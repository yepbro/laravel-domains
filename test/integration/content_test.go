// Integration tests of the content store (T-046; spec 5.7, 8, 11.4
// item 3): end-to-end scenarios over live PostgreSQL (the contents
// metadata, the isolated migratedSchema schema) and real pack files in
// t.TempDir — write/read with and without a dictionary, deduplication,
// references and SwapRef, pack rotation, compaction, the max_store_bytes
// limit and the reaction to disk exhaustion. The ContentStore contract
// (T-041) — on PackStore.
package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/content"
	"crawler/internal/content/contenttest"
)

// contentTestEnv — a PackStore on a live schema (the contents table from
// migration 000001) and pack files in a temp directory. A small PackSize
// by default: the rotation and compaction tests rely on it.
func contentTestEnv(t *testing.T, mutate func(*content.PackOptions)) (*content.PackStore, *pgxpool.Pool) {
	t.Helper()
	pool := migratedSchema(t)
	opts := content.PackOptions{
		Dir:           t.TempDir(),
		PackSize:      8 << 10,
		MaxStoreBytes: 1 << 20,
	}
	if mutate != nil {
		mutate(&opts)
	}
	st, err := content.OpenPackStore(pool, opts)
	if err != nil {
		t.Fatalf("OpenPackStore: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, pool
}

// contentRow — a contents row for checks directly in the DB.
type contentRow struct {
	packID int32
	off    int64
	clen   int32
	rawLen int32
	refc   int32
}

// contentRowByHash — a live or dead row (no refcount filter — the dead
// ones interest the tests too).
func contentRowByHash(t *testing.T, pool *pgxpool.Pool, h content.Hash) (contentRow, bool) {
	t.Helper()
	var r contentRow
	err := pool.QueryRow(context.Background(),
		"SELECT pack_id, off, len, raw_len, refcount FROM contents WHERE hash = $1", h[:],
	).Scan(&r.packID, &r.off, &r.clen, &r.rawLen, &r.refc)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, false
	}
	if err != nil {
		t.Fatalf("чтение contents по %s: %v", h, err)
	}
	return r, true
}

// contentPackIDs — the sorted pack_id of live and dead rows.
func contentPackIDs(t *testing.T, pool *pgxpool.Pool) []int32 {
	t.Helper()
	rows, err := pool.Query(context.Background(), "SELECT DISTINCT pack_id FROM contents ORDER BY pack_id")
	if err != nil {
		t.Fatalf("DISTINCT pack_id: %v", err)
	}
	defer rows.Close()
	var ids []int32
	for rows.Next() {
		var id int32
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("чтение pack_id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("итерация pack_id: %v", err)
	}
	return ids
}

// packFileExists — the presence of a pack file in the content directory.
func packFileExists(t *testing.T, dir string, id int32) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(dir, fmt.Sprintf("pack-%06d.pack", id)))
	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	t.Fatal(err)
	return false
}

// parkingPage — a "parking page" of a shared template: exactly these are
// compressed with the dictionary and deduplicated by hash (spec 5.7, 8).
func parkingPage(domain, note string) []byte {
	return []byte(fmt.Sprintf(`<!doctype html><html><head><title>Добро пожаловать на %s</title>
<meta name="parking" content="shared-template"></head>
<body><div class="hero">Домен продаётся</div><div class="note">%s</div>
<div class="footer">Эта страница размещена на парковке хостинга</div></body></html>`, domain, note))
}

// TestContent_StoreContract — the ContentStore contract (T-041) on
// PackStore with live PostgreSQL and files: the main check of the task.
// The set is in the contenttest package with T-228 (testing only in test
// packages).
func TestContent_StoreContract(t *testing.T) {
	skipUnlessIntegration(t)
	contenttest.StoreContract(t, func(t *testing.T) content.ContentStore {
		st, _ := contentTestEnv(t, nil)
		return st
	})
}

// TestContent_PutBatch — group commit on live PostgreSQL (T-228, review
// P3-1): the dedup batch (an unnest join with copy counters) and the
// batched INSERT with hash repeats are the only check of the batch
// statements by a real server and of the pgx argument encoding (the unit
// tests run on a fake pool). Both paths: a live hash → refcount 1+n, a new
// one with duplicates → n; the references are real — Delete × refcount
// exhausts the content.
func TestContent_PutBatch(t *testing.T) {
	skipUnlessIntegration(t)
	st, pool := contentTestEnv(t, nil)
	ctx := context.Background()

	live := parkingPage("live-batch.example", "живой до пачки")
	if _, err := st.Put(ctx, live); err != nil {
		t.Fatal(err)
	}
	fresh := parkingPage("fresh-batch.example", "новый, в пачке трижды")

	results, errs, err := st.PutBatch(ctx, [][]byte{live, live, live, fresh, fresh, fresh})
	if err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	for i, e := range errs {
		if e != nil {
			t.Fatalf("элемент %d: %v", i, e)
		}
	}
	// Created: live — dedup (no bytes), fresh — the first element writes
	// the bytes, the hash repeats do not.
	for i, want := range []bool{false, false, false, true, false, false} {
		if results[i].Created != want {
			t.Errorf("элемент %d: Created = %v, want %v", i, results[i].Created, want)
		}
	}
	for h, want := range map[content.Hash]int32{
		content.HashOf(live): 4, content.HashOf(fresh): 3,
	} {
		row, ok := contentRowByHash(t, pool, h)
		if !ok {
			t.Fatalf("%s: нет строки contents", h)
		}
		if row.refc != want {
			t.Errorf("%s: refcount = %d, want %d (каждая копия в пачке — ссылка)", h, row.refc, want)
		}
	}

	// The references are real: exhausting Delete × refcount kills the content.
	for i := 0; i < 4; i++ {
		if err := st.Delete(ctx, content.HashOf(live)); err != nil {
			t.Fatalf("Delete живого %d из 4: %v", i+1, err)
		}
	}
	if _, err := st.Get(ctx, content.HashOf(live)); !errors.Is(err, content.ErrNotFound) {
		t.Errorf("Get живого после 4 Delete: %v, want ErrNotFound", err)
	}
	for i := 0; i < 3; i++ {
		if err := st.Delete(ctx, content.HashOf(fresh)); err != nil {
			t.Fatalf("Delete свежего %d из 3: %v", i+1, err)
		}
	}
	if _, err := st.Get(ctx, content.HashOf(fresh)); !errors.Is(err, content.ErrNotFound) {
		t.Errorf("Get свежего после 3 Delete: %v, want ErrNotFound", err)
	}
}

// TestContent_Roundtrip — write/read: with and without a dictionary,
// compression shrinks the block (len < raw_len), reopening the directory
// reads the content (the cross-process semantics of
// http-worker/render-worker, spec 11.4 item 3).
func TestContent_Roundtrip(t *testing.T) {
	skipUnlessIntegration(t)

	t.Run("без словаря", func(t *testing.T) {
		st, pool := contentTestEnv(t, nil)
		ctx := context.Background()
		pages := [][]byte{
			parkingPage("example.com", "первая"),
			[]byte("<html><body>простая страница</body></html>"),
			bytes.Repeat([]byte("<div>повторяющийся блок</div>"), 500),
		}
		for i, page := range pages {
			res, err := st.Put(ctx, page)
			if err != nil {
				t.Fatalf("Put(%d): %v", i, err)
			}
			got, err := st.Get(ctx, res.Hash)
			if err != nil {
				t.Fatalf("Get(%d): %v", i, err)
			}
			if !bytes.Equal(got, page) {
				t.Errorf("Get(%d): байты не совпали (%d vs %d)", i, len(got), len(page))
			}
			row, ok := contentRowByHash(t, pool, res.Hash)
			if !ok {
				t.Fatalf("строка contents для %s не появилась", res.Hash)
			}
			if int(row.rawLen) != len(page) {
				t.Errorf("raw_len = %d, want %d", row.rawLen, len(page))
			}
			// Compression works on compressible pages (spec 5.7): large
			// repeating content takes fewer raw bytes; for small pages the
			// zstd frame overhead may exceed the benefit — the comparison
			// is only about the compressible case.
			if len(page) > 1024 && int(row.clen) >= len(page) {
				t.Errorf("сжатый блок %d байт не меньше сырых %d — сжатие не работает", row.clen, len(page))
			}
		}
	})

	t.Run("со словарём", func(t *testing.T) {
		samples := make([][]byte, 0, 4)
		for i := range 4 {
			samples = append(samples, parkingPage(fmt.Sprintf("parked-%d.example", i), fmt.Sprintf("заметка %d", i)))
		}
		dict, err := content.TrainDict(samples, content.TrainOptions{DictSize: 4 << 10})
		if err != nil {
			t.Fatalf("TrainDict: %v", err)
		}
		content.SetDict(dict)
		t.Cleanup(func() { content.SetDict(nil) })

		st, pool := contentTestEnv(t, nil)
		ctx := context.Background()
		var withDict []contentRow
		for i := range 4 {
			page := parkingPage(fmt.Sprintf("site-%d.example", i), "парковка после обучения")
			res, err := st.Put(ctx, page)
			if err != nil {
				t.Fatalf("Put(%d): %v", i, err)
			}
			got, err := st.Get(ctx, res.Hash)
			if err != nil || !bytes.Equal(got, page) {
				t.Fatalf("Get(%d): %v, совпадение=%v", i, err, bytes.Equal(got, page))
			}
			row, ok := contentRowByHash(t, pool, res.Hash)
			if !ok {
				t.Fatalf("строка contents для %s не появилась", res.Hash)
			}
			withDict = append(withDict, row)
		}
		// The dictionary must help: a shared-template page's block is
		// noticeably smaller than the raw bytes (the exact ratio is zstd's
		// domain; here it is the very fact of noticeable compression,
		// spec 5.7).
		for i, row := range withDict {
			if int(row.clen) > int(row.rawLen)/2 {
				t.Errorf("страница %d со словарём: блок %d байт при сырых %d — словарь не работает", i, row.clen, row.rawLen)
			}
		}
	})

	t.Run("переоткрытие каталога", func(t *testing.T) {
		pool := migratedSchema(t)
		dir := t.TempDir()
		opts := content.PackOptions{Dir: dir, PackSize: 8 << 10, MaxStoreBytes: 1 << 20}
		first, err := content.OpenPackStore(pool, opts)
		if err != nil {
			t.Fatalf("OpenPackStore: %v", err)
		}
		ctx := context.Background()
		page := parkingPage("reopen.example", "перезапись воркером")
		res, err := first.Put(ctx, page)
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		if err := first.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		// The second "process": the same directory, the same pool, reading
		// the block written by the first (the Chromium worker reads the
		// http worker's HTML).
		second, err := content.OpenPackStore(pool, opts)
		if err != nil {
			t.Fatalf("переоткрытие: %v", err)
		}
		t.Cleanup(func() { _ = second.Close() })
		got, err := second.Get(ctx, res.Hash)
		if err != nil || !bytes.Equal(got, page) {
			t.Fatalf("Get после переоткрытия: %v, совпадение=%v", err, bytes.Equal(got, page))
		}
	})
}

// TestContent_DedupAndRefcount — deduplication (spec 5.7): a repeat Put
// writes no bytes (Created=false), the refcount grows; Delete withdraws
// the references one by one, the last one makes the row dead (spec 11.4
// item 3).
func TestContent_DedupAndRefcount(t *testing.T) {
	skipUnlessIntegration(t)
	st, pool := contentTestEnv(t, nil)
	ctx := context.Background()

	page := parkingPage("dup.example", "парковка")
	first, err := st.Put(ctx, page)
	if err != nil {
		t.Fatalf("первый Put: %v", err)
	}
	row, ok := contentRowByHash(t, pool, first.Hash)
	if !ok {
		t.Fatalf("строка contents для %s не появилась", first.Hash)
	}
	sizeBefore := row.clen

	dup, err := st.Put(ctx, append([]byte(nil), page...)) // an independent copy of the bytes
	if err != nil {
		t.Fatalf("повторный Put: %v", err)
	}
	if dup.Hash != first.Hash {
		t.Fatalf("хэш изменился: %s vs %s", dup.Hash, first.Hash)
	}
	if dup.Created {
		t.Error("повторный Put записал байты: Created=true, want false (дедупликация)")
	}
	row, _ = contentRowByHash(t, pool, first.Hash)
	if row.refc != 2 {
		t.Errorf("refcount после дедупа = %d, want 2", row.refc)
	}
	if row.clen != sizeBefore {
		t.Errorf("блок перезаписан: len %d → %d, want без изменений", sizeBefore, row.clen)
	}

	// Withdrawal of references: while any remain the content is alive; the last one makes the row dead.
	if err := st.Delete(ctx, first.Hash); err != nil {
		t.Fatalf("Delete первой из двух ссылок: %v", err)
	}
	row, _ = contentRowByHash(t, pool, first.Hash)
	if row.refc != 1 {
		t.Errorf("refcount после одного Delete = %d, want 1", row.refc)
	}
	if _, err := st.Get(ctx, first.Hash); err != nil {
		t.Fatalf("Get при одной живой ссылке: %v", err)
	}
	if err := st.Delete(ctx, first.Hash); err != nil {
		t.Fatalf("Delete последней ссылки: %v", err)
	}
	if _, err := st.Get(ctx, first.Hash); !errors.Is(err, content.ErrNotFound) {
		t.Errorf("Get после исчерпания ссылок: %v, want ErrNotFound", err)
	}
	if err := st.Delete(ctx, first.Hash); !errors.Is(err, content.ErrNotFound) {
		t.Errorf("повторный Delete: %v, want ErrNotFound (симптом ошибки учёта)", err)
	}
	// Revival: a new Put writes a fresh block (T-042), refcount 1.
	res, err := st.Put(ctx, page)
	if err != nil {
		t.Fatalf("Put после исчерпания ссылок: %v", err)
	}
	if res.Hash != first.Hash {
		t.Fatalf("хэш воскресшего контента изменился: %s", res.Hash)
	}
	row, ok = contentRowByHash(t, pool, first.Hash)
	if !ok || row.refc != 1 {
		t.Errorf("воскрешение: строка %+v (found=%v), want refcount 1", row, ok)
	}
}

// TestContent_SwapRef — rewriting the reference when the result is
// overwritten (T-044; spec 5.7, 8): swap scenarios on the live contents
// table.
func TestContent_SwapRef(t *testing.T) {
	skipUnlessIntegration(t)
	st, pool := contentTestEnv(t, nil)
	ctx := context.Background()
	var noHash content.Hash

	a, err := st.Put(ctx, parkingPage("swap.example", "старый контент"))
	if err != nil {
		t.Fatalf("Put A: %v", err)
	}
	b, err := st.Put(ctx, parkingPage("swap.example", "новый контент"))
	if err != nil {
		t.Fatalf("Put B: %v", err)
	}

	t.Run("перезапись со сменой контента", func(t *testing.T) {
		if err := st.SwapRef(ctx, a.Hash, b.Hash); err != nil {
			t.Fatalf("SwapRef A→B: %v", err)
		}
		rowA, ok := contentRowByHash(t, pool, a.Hash)
		if !ok {
			t.Fatal("строка старого контента A исчезла после свопа")
		}
		rowB, ok := contentRowByHash(t, pool, b.Hash)
		if !ok {
			t.Fatal("строка нового контента B не записана")
		}
		if rowA.refc != 0 || rowB.refc != 2 {
			t.Errorf("refcount после свопа: A=%d (want 0), B=%d (want 2)", rowA.refc, rowB.refc)
		}
		if _, err := st.Get(ctx, a.Hash); !errors.Is(err, content.ErrNotFound) {
			t.Errorf("Get старого контента после свопа: %v, want ErrNotFound", err)
		}
		if _, err := st.Get(ctx, b.Hash); err != nil {
			t.Errorf("Get нового контента после свопа: %v", err)
		}
	})

	t.Run("первый контент результата", func(t *testing.T) {
		c, err := st.Put(ctx, parkingPage("swap.example", "результат без контента ранее"))
		if err != nil {
			t.Fatalf("Put C: %v", err)
		}
		if err := st.SwapRef(ctx, noHash, c.Hash); err != nil {
			t.Fatalf("SwapRef 0→C: %v", err)
		}
		rowC, ok := contentRowByHash(t, pool, c.Hash)
		if !ok {
			t.Fatalf("строка contents для %s не появилась", c.Hash)
		}
		if rowC.refc != 2 {
			t.Errorf("refcount C после 0→C = %d, want 2", rowC.refc)
		}
	})

	t.Run("результат без контента", func(t *testing.T) {
		if err := st.SwapRef(ctx, b.Hash, noHash); err != nil {
			t.Fatalf("SwapRef B→0: %v", err)
		}
		rowB, ok := contentRowByHash(t, pool, b.Hash)
		if !ok {
			t.Fatal("строка контента B исчезла после свопа B→0")
		}
		if rowB.refc != 1 {
			t.Errorf("refcount B после B→0 = %d, want 1", rowB.refc)
		}
	})

	t.Run("контент не сменился — no-op", func(t *testing.T) {
		before, ok := contentRowByHash(t, pool, b.Hash)
		if !ok {
			t.Fatal("строка контента B исчезла до no-op свопа")
		}
		if err := st.SwapRef(ctx, b.Hash, b.Hash); err != nil {
			t.Fatalf("SwapRef B→B: %v", err)
		}
		after, ok := contentRowByHash(t, pool, b.Hash)
		if !ok {
			t.Fatal("строка контента B исчезла после no-op свопа")
		}
		if before.refc != after.refc {
			t.Errorf("no-op своп изменил refcount: %d → %d", before.refc, after.refc)
		}
	})

	t.Run("нецелостность: новый не записан", func(t *testing.T) {
		ghost := content.HashOf([]byte("не было Put-а"))
		err := st.SwapRef(ctx, b.Hash, ghost)
		if !errors.Is(err, content.ErrRefIntegrity) {
			t.Fatalf("SwapRef на незаписанный контент: %v, want ErrRefIntegrity", err)
		}
		rowB, ok := contentRowByHash(t, pool, b.Hash)
		if !ok {
			t.Fatal("строка контента B исчезла при откаченном свопе")
		}
		if rowB.refc != 1 {
			t.Errorf("откаченный своп изменил refcount B: %d, want 1", rowB.refc)
		}
	})
}

// TestContent_PackRotation — rotation by pack_size (spec 5.7): packs
// appear, the content is readable from all of them, the newest has the
// maximum id.
func TestContent_PackRotation(t *testing.T) {
	skipUnlessIntegration(t)
	st, pool := contentTestEnv(t, nil)
	ctx := context.Background()

	var hashes []content.Hash
	for i := 0; len(contentPackIDs(t, pool)) < 3; i++ {
		page := parkingPage(fmt.Sprintf("rotate-%d.example", i), fmt.Sprintf("страница %d", i))
		res, err := st.Put(ctx, page)
		if err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
		hashes = append(hashes, res.Hash)
		if i > 100 {
			t.Fatal("ротация не наступила за 100 записей — pack_size не работает")
		}
	}
	ids := contentPackIDs(t, pool)
	if len(ids) < 3 || ids[0] != 1 {
		t.Fatalf("pack_id после серии записей: %v, want минимум [1 2 3]", ids)
	}
	for _, id := range ids {
		if !packFileExists(t, st.Dir(), id) {
			t.Errorf("файл пака %d отсутствует", id)
		}
	}
	// The content is read from every pack (blocks at the committed offsets).
	for i, h := range hashes {
		if _, err := st.Get(ctx, h); err != nil {
			t.Errorf("Get(%d) из пака: %v", i, err)
		}
	}
	// The newest pack keeps accepting writes.
	res, err := st.Put(ctx, parkingPage("last.example", "запись после ротации"))
	if err != nil {
		t.Fatalf("Put после ротации: %v", err)
	}
	row, ok := contentRowByHash(t, pool, res.Hash)
	if !ok {
		t.Fatalf("строка contents для %s не появилась", res.Hash)
	}
	if row.packID != ids[len(ids)-1] {
		t.Errorf("новая запись попала в пак %d, want новейший %d", row.packID, ids[len(ids)-1])
	}
}

// TestContent_Compaction — compaction (spec 5.7): a live share below the
// threshold — the pack is rewritten and deleted, the live content is
// readable, the space is freed; above the threshold — untouched.
func TestContent_Compaction(t *testing.T) {
	skipUnlessIntegration(t)
	st, pool := contentTestEnv(t, nil)
	ctx := context.Background()

	// Packs 1 and 2: five pages in the first, the sixth rotates into the second.
	byPack := make(map[int32][]content.Hash)
	var i int
	for len(contentPackIDs(t, pool)) < 2 {
		res, err := st.Put(ctx, parkingPage(fmt.Sprintf("compact-%d.example", i), fmt.Sprintf("страница %d", i)))
		if err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
		row, ok := contentRowByHash(t, pool, res.Hash)
		if !ok {
			t.Fatalf("строка для %s не появилась", res.Hash)
		}
		byPack[row.packID] = append(byPack[row.packID], res.Hash)
		i++
	}
	first := byPack[1]
	if len(first) < 2 {
		t.Fatalf("в паке 1 контента %d, want минимум 2 для доли живых", len(first))
	}
	before := len(first)

	// One live, the rest dead: the live share << 50%.
	for _, h := range first[1:] {
		if err := st.Delete(ctx, h); err != nil {
			t.Fatalf("Delete %s: %v", h, err)
		}
	}
	live := first[0]
	info, err := st.CompactScan(ctx, 1)
	if err != nil {
		t.Fatalf("CompactScan: %v", err)
	}
	if info.Ratio >= 0.5 {
		t.Fatalf("доля живых пака 1 = %.2f, want < 0.5 (удалено %d из %d)", info.Ratio, before-1, before)
	}

	stats, err := content.CompactOnce(ctx, st, content.CompactOptions{Threshold: 0.5})
	if err != nil {
		t.Fatalf("CompactOnce: %v", err)
	}
	if stats.Compacted < 1 || stats.FreedBytes <= 0 {
		t.Fatalf("CompactOnce: %+v, want Compacted ≥ 1 и FreedBytes > 0", stats)
	}
	if packFileExists(t, st.Dir(), 1) {
		t.Error("файл пака 1 не удалён после компактификации")
	}
	// The live content has moved and is readable; the dead rows are purged.
	row, ok := contentRowByHash(t, pool, live)
	if !ok {
		t.Fatal("живая строка потеряна компактификацией")
	}
	if row.packID == 1 {
		t.Errorf("живая строка осталась в удалённом паке 1")
	}
	if got, err := st.Get(ctx, live); err != nil || len(got) == 0 {
		t.Errorf("Get живого после компактификации: %v", err)
	}
	if ids := contentPackIDs(t, pool); len(ids) == 0 || ids[0] == 1 {
		t.Errorf("мёртвые строки пака 1 не удалены: pack_id %v", ids)
	}
	// Writing continues, the space is freed and accounted for (T-046).
	if _, err := st.Put(ctx, parkingPage("after-compact.example", "запись после компактификации")); err != nil {
		t.Fatalf("Put после компактификации: %v", err)
	}

	// A pack with a live share above the threshold is not compacted.
	t.Run("доля живых выше порога", func(t *testing.T) {
		st2, pool2 := contentTestEnv(t, nil)
		var keep []content.Hash
		for j := 0; len(contentPackIDs(t, pool2)) < 2; j++ {
			res, err := st2.Put(ctx, parkingPage(fmt.Sprintf("live-%d.example", j), fmt.Sprintf("страница %d", j)))
			if err != nil {
				t.Fatalf("Put(%d): %v", j, err)
			}
			row, ok := contentRowByHash(t, pool2, res.Hash)
			if !ok {
				t.Fatalf("строка contents для %s не появилась", res.Hash)
			}
			if row.packID == 1 {
				keep = append(keep, res.Hash)
			}
		}
		if len(keep) == 0 {
			t.Fatal("пак 1 пуст — нечего беречь")
		}
		stats, err := content.CompactOnce(ctx, st2, content.CompactOptions{Threshold: 0.5})
		if err != nil {
			t.Fatalf("CompactOnce: %v", err)
		}
		if stats.Compacted != 0 {
			t.Errorf("компактифицирован живой пак: %+v, want Compacted 0", stats)
		}
		if !packFileExists(t, st2.Dir(), 1) {
			t.Error("пак 1 с живыми данными удалён")
		}
	})
}

// TestContent_MaxStoreBytes — the single content limit (spec 5.5, 5.7):
// exceeding it — ErrTooLarge as a whole, exactly at the boundary — passes.
func TestContent_MaxStoreBytes(t *testing.T) {
	skipUnlessIntegration(t)
	const limit = 64
	st, pool := contentTestEnv(t, func(o *content.PackOptions) { o.MaxStoreBytes = limit })
	ctx := context.Background()

	if _, err := st.Put(ctx, bytes.Repeat([]byte("x"), limit+1)); !errors.Is(err, content.ErrTooLarge) {
		t.Fatalf("Put сверх лимита: %v, want ErrTooLarge", err)
	}
	res, err := st.Put(ctx, bytes.Repeat([]byte("y"), limit))
	if err != nil {
		t.Fatalf("Put ровно в лимит: %v", err)
	}
	if !res.Created {
		t.Error("Put в лимит не записал контент")
	}
	if _, ok := contentRowByHash(t, pool, content.HashOf(bytes.Repeat([]byte("x"), limit+1))); ok {
		t.Error("строка сверхлимитного контента появилась в БД")
	}
}

// TestContent_DiskFullPause — the reaction to disk exhaustion (spec 8):
// an unreachable threshold — Put returns ErrStoreFull (a write pause),
// the alert event carries a real statfs snapshot; a sane threshold — the
// writing goes on. The mockable statfs and the threshold semantics are the
// unit tests of disk_test.go; here — a live file system and the end-to-end
// connection with Put.
func TestContent_DiskFullPause(t *testing.T) {
	skipUnlessIntegration(t)

	var (
		mu     sync.Mutex
		events []content.StoreFullEvent
	)
	st, _ := contentTestEnv(t, func(o *content.PackOptions) {
		o.DiskFreePercent = 101 // there is never more than 100% free space — always a pause
		o.OnStoreFull = func(ev content.StoreFullEvent) {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, ev)
		}
	})
	ctx := context.Background()

	_, err := st.Put(ctx, parkingPage("full.example", "запись в полный диск"))
	if !errors.Is(err, content.ErrStoreFull) {
		t.Fatalf("Put при недостижимом пороге: %v, want ErrStoreFull", err)
	}
	if !strings.Contains(err.Error(), "content.disk_free_percent") {
		t.Errorf("ошибка не называет порог: %q", err.Error())
	}
	mu.Lock()
	if len(events) != 1 {
		t.Fatalf("событий алерта %d, want 1", len(events))
	}
	ev := events[0]
	mu.Unlock()
	if ev.Source != content.StoreFullPut || ev.Recovered {
		t.Errorf("событие: %+v, want Source=%s Recovered=false", ev, content.StoreFullPut)
	}
	if ev.Space.Total == 0 {
		t.Error("событие без реального снимка statfs: Total = 0")
	}

	// A live threshold: enough space, writing goes on (the same directory).
	st2, _ := contentTestEnv(t, func(o *content.PackOptions) { o.DiskFreePercent = 0.0001 })
	if _, err := st2.Put(ctx, parkingPage("ok.example", "места достаточно")); err != nil {
		t.Fatalf("Put при живом пороге: %v", err)
	}
}

// TestContent_RefcountFSM — the refcount automaton invariants on live
// PostgreSQL (T-265, TZ-fsm spec §3 FR-11; the specification is the doc
// comment of internal/content/pack.go): reviving a tombstone row goes
// through the ON CONFLICT DO UPDATE branch — refcount 0→1, the placement
// moved to a fresh block (EXCLUDED), Created = false; a row deleted
// entirely (the retention/compaction view) revives through the INSERT
// branch — Created = true; a double Delete of a dead one — ErrNotFound.
// The unit pass over the transition table — internal/content
// (TestPackRefcountFSM).
func TestContent_RefcountFSM(t *testing.T) {
	skipUnlessIntegration(t)
	st, pool := contentTestEnv(t, nil)
	ctx := context.Background()
	page := parkingPage("fsm.example", "автомат refcount")

	res, err := st.Put(ctx, page)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !res.Created {
		t.Error("первый Put: Created = false, want true (вставка)")
	}
	if _, err := st.Put(ctx, append([]byte(nil), page...)); err != nil {
		t.Fatalf("Put-дедуп: %v", err)
	}
	oldRow, ok := contentRowByHash(t, pool, res.Hash)
	if !ok || oldRow.refc != 2 {
		t.Fatalf("после дедупа: строка %+v (found=%v), want refcount 2", oldRow, ok)
	}

	// live(2) → dead·tombstone: both references withdrawn, the row stays.
	for i := 0; i < 2; i++ {
		if err := st.Delete(ctx, res.Hash); err != nil {
			t.Fatalf("Delete %d: %v", i+1, err)
		}
	}
	row, ok := contentRowByHash(t, pool, res.Hash)
	if !ok || row.refc != 0 {
		t.Fatalf("tombstone: строка %+v (found=%v), want refcount 0", row, ok)
	}
	if err := st.Delete(ctx, res.Hash); !errors.Is(err, content.ErrNotFound) {
		t.Errorf("повторный Delete мёртвой: %v, want ErrNotFound", err)
	}

	// T6: the tombstone revival — DO UPDATE, the EXCLUDED placement.
	again, err := st.Put(ctx, page)
	if err != nil {
		t.Fatalf("Put воскрешения: %v", err)
	}
	if again.Created {
		t.Error("воскрешение tombstone: Created = true, want false (DO UPDATE)")
	}
	row, _ = contentRowByHash(t, pool, res.Hash)
	if row.refc != 1 {
		t.Errorf("после воскрешения: refcount = %d, want 1", row.refc)
	}
	if row.packID == oldRow.packID && row.off == oldRow.off {
		t.Error("воскрешение не перевело размещение на свежий блок (EXCLUDED)")
	}
	if got, err := st.Get(ctx, res.Hash); err != nil || string(got) != string(page) {
		t.Errorf("Get воскрешённого: %v", err)
	}

	// The "no row" view (the retention path T-166 / the compaction purge
	// T-045): the row is deleted entirely — Get/Delete as for a dead one,
	// the revival through the INSERT branch, Created = true.
	if _, err := pool.Exec(ctx, "DELETE FROM contents WHERE hash = $1", res.Hash[:]); err != nil {
		t.Fatalf("удаление строки: %v", err)
	}
	if _, ok := contentRowByHash(t, pool, res.Hash); ok {
		t.Fatal("строка обязана быть удалена")
	}
	if _, err := st.Get(ctx, res.Hash); !errors.Is(err, content.ErrNotFound) {
		t.Errorf("Get отсутствующей: %v, want ErrNotFound", err)
	}
	if err := st.Delete(ctx, res.Hash); !errors.Is(err, content.ErrNotFound) {
		t.Errorf("Delete отсутствующей: %v, want ErrNotFound", err)
	}
	fresh, err := st.Put(ctx, page)
	if err != nil {
		t.Fatalf("Put после удаления строки: %v", err)
	}
	if !fresh.Created {
		t.Error("воскрешение отсутствующей строки: Created = false, want true (INSERT)")
	}
	if row, ok := contentRowByHash(t, pool, res.Hash); !ok || row.refc != 1 {
		t.Errorf("после INSERT-воскрешения: строка %+v (found=%v), want refcount 1", row, ok)
	}
}

// TestContent_RefcountClampDesync — the GREATEST(refcount − n, 0) clamp on
// desync against live PostgreSQL (T-265, the refcount automaton invariant
// I1, transition T9): two aged rows reference content with counter 1 —
// the aggregated withdrawal of two references clamps to zero, not minus,
// the row is deleted in the same transaction (retention skips the
// tombstone), the pass neither fails nor triggers the CHECK
// contents_refcount_check (migration 000019). The unit pass —
// internal/content (TestRetentionOnce_ClampDesync).
func TestContent_RefcountClampDesync(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	var zone int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name) VALUES ('clamp.test') RETURNING id").Scan(&zone); err != nil {
		t.Fatalf("insert zones: %v", err)
	}
	var id int64
	if err := pool.QueryRow(ctx,
		"INSERT INTO domains (zone_id, label) VALUES ($1, 'clamp') RETURNING id", zone,
	).Scan(&id); err != nil {
		t.Fatalf("insert domains: %v", err)
	}

	// The desync: 2 holders, counter 1 (one reference already withdrawn by
	// another path — the accounting-error scenario that the clamp forgives
	// with zero).
	h := make([]byte, 16)
	h[15] = 0x42
	if _, err := pool.Exec(ctx, `
		INSERT INTO contents (hash, pack_id, off, len, raw_len, refcount)
		VALUES ($1, 1, 0, 10, 20, 1)`, h); err != nil {
		t.Fatalf("посев contents: %v", err)
	}
	for sub := int16(0); sub < 2; sub++ {
		if _, err := pool.Exec(ctx, `
			INSERT INTO subdomain_results (zone_id, domain_id, subdomain_id, scheme, checked_at, content_hash)
			VALUES ($1, $2, $3, 2, now() - interval '2 hours', $4)`, zone, id, sub, h); err != nil {
			t.Fatalf("посев результата sub=%d: %v", sub, err)
		}
	}

	stats, err := content.RetentionOnce(ctx, pool, content.RetentionOptions{
		Retention: time.Hour,
		BatchSize: 100,
	})
	if err != nil {
		t.Fatalf("RetentionOnce при рассинхроне: %v", err)
	}
	if stats.Rows != 2 || stats.Refs != 2 || stats.Contents != 1 || stats.FreedBytes != 10 {
		t.Fatalf("stats = %+v, want Rows=2 Refs=2 Contents=1 FreedBytes=10 (клэмп 1−2 → 0)", stats)
	}
	if stats.BadHashes != 0 || stats.Missing != 0 || stats.Skipped || stats.SkippedPending {
		t.Errorf("аномалий быть не должно: %+v", stats)
	}
	var n int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM contents WHERE hash = $1", h).Scan(&n); err != nil {
		t.Fatalf("чтение contents: %v", err)
	}
	if n != 0 {
		t.Errorf("contents после клэмпа: строк %d, want 0 (refcount дошёл до нуля — удалена)", n)
	}
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM subdomain_results WHERE content_hash IS NOT NULL").Scan(&n); err != nil {
		t.Fatalf("чтение subdomain_results: %v", err)
	}
	if n != 0 {
		t.Errorf("ссылок осталось %d, want 0 (обе записи очищены)", n)
	}
}

// TestSchema_ContentsRefcountCheck — the companion invariant test of the
// contents_refcount_check CHECK (T-265, FR-11, migration 000019):
// refcount < 0 is rejected by the schema (23514) on INSERT and UPDATE, a
// tombstone refcount = 0 is legal (the dead view #1, T-042), the
// constraint is added NOT VALID (convalidated = false — VALIDATE per the
// migration regulations, docs/schema.md "CHECK invariants"); contents is
// not partitioned — a single object.
func TestSchema_ContentsRefcountCheck(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	// Legal rows: a live refcount = 1 and a tombstone refcount = 0.
	ins := func(hb byte, refc int) error {
		_, err := pool.Exec(ctx, `
			INSERT INTO contents (hash, pack_id, off, len, raw_len, refcount)
			VALUES ($1, 1, 0, 10, 20, $2)`, refcTestHash(hb), refc)
		return err
	}
	if err := ins(0x01, 1); err != nil {
		t.Fatalf("живая строка refcount 1: %v", err)
	}
	if err := ins(0x02, 0); err != nil {
		t.Fatalf("tombstone refcount 0: %v (нулевая строка легальна — мёртвая до компактификации T-045)", err)
	}

	// The invariant violation: an INSERT with refcount = −1 — 23514 from the schema.
	if err := ins(0x03, -1); err == nil || !strings.Contains(err.Error(), "contents_refcount_check") {
		t.Errorf("INSERT refcount −1: err = %v, want violation contents_refcount_check", err)
	}
	// An UPDATE of an existing row to minus is rejected too (NOT VALID
	// watches new and modified rows).
	if _, err := pool.Exec(ctx,
		"UPDATE contents SET refcount = -1 WHERE hash = $1", refcTestHash(0x01)); err == nil ||
		!strings.Contains(err.Error(), "contents_refcount_check") {
		t.Errorf("UPDATE в минус: err = %v, want violation contents_refcount_check", err)
	}

	// NOT VALID: the constraint in the test schema (filtered by
	// current_schema — the schemas of neighboring runs live in the DB) is
	// not validated yet; contents is not partitioned (migration 000001) —
	// a single object.
	rows, err := pool.Query(ctx,
		`SELECT count(*) FILTER (WHERE convalidated) AS validated, count(*) AS total
		 FROM pg_constraint
		 WHERE conname = 'contents_refcount_check'
		   AND connamespace = current_schema()::regnamespace`)
	if err != nil {
		t.Fatalf("pg_constraint: %v", err)
	}
	defer rows.Close()
	var validated, total int
	if !rows.Next() || rows.Scan(&validated, &total) != nil {
		t.Fatalf("чтение pg_constraint: %v", rows.Err())
	}
	if total != 1 {
		t.Fatalf("ограничение в схеме теста на %d объектах, want 1 (миграция 000019)", total)
	}
	if validated != 0 {
		t.Errorf("convalidated = true, want false (NOT VALID, VALIDATE — по регламенту)")
	}
}

// refcTestHash — a 16-octet hash with a filler byte (the seeding of
// TestSchema_ContentsRefcountCheck).
func refcTestHash(b byte) []byte {
	h := make([]byte, 16)
	h[15] = b
	return h
}

// retentionPinSelectSQL — the full text of retentionSelectSQL with the
// parameters as literals (T-271; the anchor is
// internal/content/retention.go, the duplication is the rqBatchSQL
// style): EXPLAIN with parameters requires a prepared statement, the plan
// shape check is indifferent to literals. The schema codes as literals
// (FlagRenderPending = 2, StatusDeleted = 6 — the rqBatchSQL style).
const retentionPinSelectSQL = "SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, r.content_hash, r.checked_at " +
	"FROM subdomain_results AS r " +
	"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id " +
	"WHERE r.checked_at < now() - interval '6 hours' " +
	"AND (r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant) " +
	"> ('1970-01-01'::timestamptz, 0::int2, 0::int8, 0::int2, 0::int2) " +
	"AND (r.content_hash IS NOT NULL OR r.headers IS NOT NULL OR r.cookies IS NOT NULL " +
	"OR r.tls IS NOT NULL OR r.redirects IS NOT NULL OR r.addrs IS NOT NULL OR r.cname IS NOT NULL) " +
	"AND (r.rendered OR (d.flags & 2) = 0) " +
	"AND d.status <> 6 " +
	"AND NOT EXISTS (SELECT 1 FROM laravel_domains AS ld " +
	"WHERE ld.zone_id = r.zone_id AND ld.domain_id = r.domain_id) " +
	"ORDER BY r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant " +
	"LIMIT 10000"

// TestContent_RetentionSelectPlanPinned — pinning the plan shape of the
// retention batch selection (T-271; the incident of Nov 08–09, 2026 — the
// plan degraded to a seq scan of 9M rows + a hash join of domains + a
// Sort of ~1 GiB on top of 2 GiB shared_buffers, knocking PostgreSQL down
// with OOM kills, batches were not committed — the cursor did not move):
// inside the pinning transaction (SET LOCAL seq/hash/merge/bitmap off —
// the same literals as internal/content/retention.go) EXPLAIN shows the
// index access shape to subdomain_results + Limit, without Hash/Merge
// Join and Seq Scan; an Incremental Sort is allowed (enable_sort is not
// switched off — ORDER BY (checked_at, PK) keeps it on top of the index
// order). Bitmap is switched off by the T-271 tuning: the bitmap path
// does not preserve the index order → an explicit Sort, the shape is
// nondeterministic.
func TestContent_RetentionSelectPlanPinned(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	// Seeding: a zone, a live domain, an aged row with content — the plan
	// shape does not depend on the volume (the pinning is SET LOCAL), but
	// an empty table would drive the planner into zero estimates by its
	// statistics.
	var zone int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name) VALUES ('pin.test') RETURNING id").Scan(&zone); err != nil {
		t.Fatalf("insert zones: %v", err)
	}
	var id int64
	if err := pool.QueryRow(ctx,
		"INSERT INTO domains (zone_id, label) VALUES ($1, 'pin') RETURNING id", zone,
	).Scan(&id); err != nil {
		t.Fatalf("insert domains: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO subdomain_results (zone_id, domain_id, subdomain_id, scheme, final_url, content_hash, checked_at)
		VALUES ($1, $2, 0, 2, 'https://pin.test/', $3, now() - interval '2 hours')`,
		zone, id, refcTestHash(0x77)); err != nil {
		t.Fatalf("посев результата: %v", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, pin := range []string{
		"SET LOCAL enable_seqscan = off",
		"SET LOCAL enable_hashjoin = off",
		"SET LOCAL enable_mergejoin = off",
		"SET LOCAL enable_bitmapscan = off",
	} {
		if _, err := tx.Exec(ctx, pin); err != nil {
			t.Fatalf("%s: %v", pin, err)
		}
	}
	plan := explainPlan(t, tx, ctx, retentionPinSelectSQL)
	if !planIdxScanRe.MatchString(plan) {
		t.Errorf("закреплённый план без индексного доступа к subdomain_results:\n%s", plan)
	}
	if !strings.Contains(plan, "Limit") {
		t.Errorf("закреплённый план без Limit (ранний останов пачки):\n%s", plan)
	}
	for _, bad := range []string{"Hash Join", "Merge Join", "Seq Scan"} {
		if strings.Contains(plan, bad) {
			t.Errorf("закреплённый план содержит %q (SET LOCAL не подействовал):\n%s", bad, plan)
		}
	}
	// The bitmap path is a different diagnosis than the un-switched-off
	// GUCs: the pinning works but the shape is not fully closed — bitmap
	// does not preserve the index order, the ORDER BY is satisfied by an
	// explicit Sort (the T-271 tuning).
	for _, bad := range []string{"Bitmap Index Scan", "Bitmap Heap Scan"} {
		if strings.Contains(plan, bad) {
			t.Errorf("закреплённый план содержит %q (bitmap-путь ломает порядок курсора — форма не закреплена до конца):\n%s", bad, plan)
		}
	}
}

// TestContent_RetentionCatchUpFromZeroCursor — the main test gap of T-271,
// closed by T-273: a catch-up pass from a ZERO cursor must finish within
// the SelectTimeout with a clean skip prefix and drain the whole candidate
// set. The production mechanics of the incident of Nov 09–10, 2026 (the
// scale is a production proportion in miniature, the property matters
// more than the absolute millions): retention had been down since the
// night of Nov 08 → the cursor at zero → every select went "from the
// beginning of the table" through millions of already clean rows (the
// "anything to purge" predicate is outside the index — a heap visit of
// each); the 600 s deadline killed the pass BEFORE the commit → the
// cursor did not move → the identical doomed scan (an eternal loop).
// Here: 200k clean aged rows (a clean skip prefix by checked_at) ahead of
// 30k candidates (jsonb) and 5k fresh ones — one RetentionOnce pass must
// purge all candidates and fit into the SelectTimeout squeezed through
// the options (30 s per batch against the production 3600 s), and a
// repeat pass must see an empty set (the drain is complete). A contract
// test in essence: the cost of the production loop is not reproduced on
// the fixture (the test does not care whether the pass took seconds or
// hours as long as it fits the cap) — it pins down that a pass from a
// zero cursor FINISHES and moves the drain to the end of the set; the
// plan shape and the fact of running at scale are checked by
// TestRenderLateSelectPlanUnpinned and the EXPLAIN test of the pinned
// retention selection above.
func TestContent_RetentionCatchUpFromZeroCursor(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	// Two zones → dedicated partitions of subdomain_results (production —
	// ~677; partitioning is part of the cost: an Append over the leaves,
	// PK point reads).
	var z1, z2 int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name) VALUES ('cup-a.test') RETURNING id").Scan(&z1); err != nil {
		t.Fatalf("insert zones: %v", err)
	}
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name) VALUES ('cup-b.test') RETURNING id").Scan(&z2); err != nil {
		t.Fatalf("insert zones: %v", err)
	}
	for _, z := range []int16{z1, z2} {
		if _, err := pool.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE subdomain_results_cup_%d PARTITION OF subdomain_results FOR VALUES IN (%d)", z, z)); err != nil {
			t.Fatalf("партиция зоны %d: %v", z, err)
		}
	}

	const (
		cleanPrefix = 200000 // clean aged rows BEHIND the cost cursor
		dirty       = 30000  // candidates: aged with jsonb
		fresh       = 5000   // younger than the window — outside the drain
		total       = cleanPrefix + dirty + fresh
	)
	// A domain per row — the production proportion (43M domains at 11.3M
	// rows: most domains have no rows).
	if _, err := pool.Exec(ctx, `
		INSERT INTO domains (zone_id, id, label)
		SELECT CASE WHEN g % 2 = 0 THEN $1::int2 ELSE $2::int2 END, g, 'cup' || g
		FROM generate_series(1, $3) g`, z1, z2, total); err != nil {
		t.Fatalf("посев domains: %v", err)
	}
	// The rows in checked_at order: a clean prefix [−35h, −7.2h] →
	// candidates [−7h, −6.2h] (the retention window of 6 h — aged) →
	// fresh [−1h, −59min]. The candidates are distinguishable by the jsonb
	// header (a trim without references — contents is not needed); the
	// clean prefix — a passport without jsonb, the way retention left them
	// before the downtime.
	if _, err := pool.Exec(ctx, `
		INSERT INTO subdomain_results
		    (zone_id, domain_id, subdomain_id, variant, scheme, checked_at, headers)
		SELECT CASE WHEN g % 2 = 0 THEN $1::int2 ELSE $2::int2 END, g, (g % 63)::smallint, 0, 2,
		       CASE
		       WHEN g <= $3 THEN now() - interval '35 hours' + (g * interval '0.5 seconds')
		       WHEN g <= $4 THEN now() - interval '7 hours' + ((g - $3) * interval '0.1 seconds')
		       ELSE now() - interval '1 hour' + ((g - $4) * interval '0.1 seconds')
		       END,
		       CASE WHEN g > $3 THEN '{"cup":1}'::jsonb END
		FROM generate_series(1, $5) g`, z1, z2, cleanPrefix, cleanPrefix+dirty, total); err != nil {
		t.Fatalf("посев subdomain_results: %v", err)
	}
	if _, err := pool.Exec(ctx, "ANALYZE subdomain_results"); err != nil {
		t.Fatalf("ANALYZE: %v", err)
	}

	// A catch-up pass from the zero cursor: the SelectTimeout is squeezed
	// through the options (30 s per batch — generous against the actual
	// single-digit seconds, many times less than the production 3600 s:
	// the property "the pass makes it" is checked, not waited out for an
	// hour).
	start := time.Now()
	stats, err := content.RetentionOnce(ctx, pool, content.RetentionOptions{
		Retention:     6 * time.Hour,
		BatchSize:     10000,
		SelectTimeout: 30 * time.Second,
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("catch-up с нулевого курсора не завершился в SelectTimeout: %v (проход %v)", err, elapsed)
	}
	if stats.Rows != dirty {
		t.Fatalf("очищено строк = %d, want %d (весь бэклог одним проходом)", stats.Rows, dirty)
	}
	if stats.Batches != dirty/10000 {
		t.Fatalf("пачек = %d, want %d", stats.Batches, dirty/10000)
	}
	if stats.BadHashes != 0 || stats.Missing != 0 || stats.Skipped || stats.SkippedPending {
		t.Errorf("аномалий быть не должно: %+v", stats)
	}
	// The drain is complete: no candidates left (the fresh ones untouched —
	// their headers are in place, the age window has not expired).
	var n int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM subdomain_results WHERE headers IS NOT NULL").Scan(&n); err != nil {
		t.Fatalf("чтение остатков: %v", err)
	}
	if n != fresh {
		t.Errorf("строк с jsonb после прохода = %d, want %d (только свежие)", n, fresh)
	}
	if elapsed > time.Minute {
		t.Errorf("catch-up-проход на фикстуре занял %v — деградация формы плана (бюджет теста: минута)", elapsed)
	}

	// A repeat pass — the set is exhausted (the cursor/cleanup advanced):
	// an empty drain in one select.
	stats, err = content.RetentionOnce(ctx, pool, content.RetentionOptions{
		Retention:     6 * time.Hour,
		BatchSize:     10000,
		SelectTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("повторный проход: %v", err)
	}
	if stats.Rows != 0 || stats.Batches != 0 {
		t.Fatalf("повторный проход = %+v, want пустой (множество исчерпано)", stats)
	}
}

// TestContent_RetentionCursorRestart — the main test of T-274: the
// persistent drain cursor survives an http-worker restart. Circle 1
// drains the aging window and commits the position to
// retention_drain_cursor (migration 000020, an upsert in the batch's
// transaction); the "restart" (a new RetentionLoop) reads the state and
// continues from it: a row inserted BEHIND the cursor is not visited
// (the clean prefix is not walked — the restart penalty is removed), an
// aged row AHEAD of the cursor is drained normally. A full RetentionOnce
// (a zero cursor, the runbooks T-167 path A) purges the laggard too — no
// data loss, only the pass cost changes. Along the way — the single-row
// contract of the table (CHECK id = 1).
func TestContent_RetentionCursorRestart(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	// A second state row is impossible by construction (the migration's CHECK).
	if _, err := pool.Exec(ctx,
		"INSERT INTO retention_drain_cursor (id, checked_at, zone_id, domain_id, subdomain_id, variant) "+
			"VALUES (2, now(), 1, 1, 0, 0)"); err == nil {
		t.Fatal("вторая строка retention_drain_cursor прошла вопреки CHECK (id = 1)")
	}

	var z int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name) VALUES ('cur-a.test') RETURNING id").Scan(&z); err != nil {
		t.Fatalf("insert zones: %v", err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(
		"CREATE TABLE subdomain_results_cur_%d PARTITION OF subdomain_results FOR VALUES IN (%d)", z, z)); err != nil {
		t.Fatalf("партиция зоны %d: %v", z, err)
	}
	// Two domains: circle 1 drains the first one's rows; the second — the
	// rows "after the restart" (one behind the cursor, one ahead).
	if _, err := pool.Exec(ctx,
		"INSERT INTO domains (zone_id, id, label) VALUES ($1, 1, 'first'), ($1, 2, 'second')", z); err != nil {
		t.Fatalf("посев domains: %v", err)
	}
	// The first pass's candidates (the 6 h window): the checked_at order
	// d1/sub0 (−10 h) → d1/sub1 (−9 h) — the cursor will stop at d1/sub1.
	if _, err := pool.Exec(ctx, `
		INSERT INTO subdomain_results (zone_id, domain_id, subdomain_id, variant, scheme, checked_at, headers)
		VALUES
		  ($1, 1, 0, 0, 2, now() - interval '10 hours', '{"a":1}'),
		  ($1, 1, 1, 0, 2, now() - interval '9 hours',  '{"a":1}')`, z); err != nil {
		t.Fatalf("посев subdomain_results: %v", err)
	}

	// Circle 1 ("the previous process"): the starting pass drains the
	// window and commits the cursor with the batch.
	loop := func() (content.RetentionStats, error) {
		passed := make(chan content.RetentionStats, 1)
		opts := content.RetentionOptions{
			Retention: 6 * time.Hour,
			BatchSize: 100,
			OnPass:    func(s content.RetentionStats, _ time.Duration) { passed <- s },
		}
		loopCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- content.RetentionLoop(loopCtx, pool, opts) }()
		select {
		case s := <-passed:
			cancel()
			<-done
			return s, nil
		case <-time.After(time.Minute):
			cancel()
			return content.RetentionStats{}, errors.New("стартовый проход цикла не завершился")
		}
	}
	if s, err := loop(); err != nil {
		t.Fatalf("цикл 1: %v", err)
	} else if s.Rows != 2 {
		t.Fatalf("цикл 1: Rows = %d, want 2 (всё окно старения)", s.Rows)
	}

	// The state is committed: the position is the last row of pass 1.
	var (
		curChecked      time.Time
		curZone, curSub int16
		curDomain       int64
		curVariant      int16
	)
	if err := pool.QueryRow(ctx,
		"SELECT checked_at, zone_id, domain_id, subdomain_id, variant FROM retention_drain_cursor WHERE id = 1",
	).Scan(&curChecked, &curZone, &curDomain, &curSub, &curVariant); err != nil {
		t.Fatalf("строка персистентного курсора: %v", err)
	}
	if curZone != z || curDomain != 1 || curSub != 1 || curVariant != 0 {
		t.Fatalf("курсор = %d/%d/%d/%d, want позиция последней строки прохода 1 (%d/1/1/0)",
			curZone, curDomain, curSub, curVariant, z)
	}

	// "Downtime + restart": candidates appeared behind and ahead of the cursor.
	if _, err := pool.Exec(ctx, `
		INSERT INTO subdomain_results (zone_id, domain_id, subdomain_id, variant, scheme, checked_at, headers)
		VALUES
		  -- позади курсора (−9,5 ч < −9 ч): рестарт с курсора её НЕ видит
		  ($1, 2, 0, 0, 2, now() - interval '9 hours 30 minutes', '{"b":1}'),
		  -- впереди курсора (−7 ч): остаревшая — дренаж подберёт
		  ($1, 2, 1, 0, 2, now() - interval '7 hours', '{"b":1}')`, z); err != nil {
		t.Fatalf("посев после рестарта: %v", err)
	}

	// Circle 2 ("the restart"): continues from the saved cursor.
	if s, err := loop(); err != nil {
		t.Fatalf("цикл 2: %v", err)
	} else if s.Rows != 1 {
		t.Fatalf("цикл 2: Rows = %d, want 1 (только запись впереди курсора — рестарт не ходит с нуля)", s.Rows)
	}
	var behind, ahead int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FILTER (WHERE domain_id = 2 AND subdomain_id = 0), "+
			"count(*) FILTER (WHERE domain_id = 2 AND subdomain_id = 1) "+
			"FROM subdomain_results WHERE headers IS NOT NULL").Scan(&behind, &ahead); err != nil {
		t.Fatalf("остатки после рестарта: %v", err)
	}
	if behind != 1 || ahead != 0 {
		t.Fatalf("позади курсора headers=%d (want 1 — не посещалась), впереди=%d (want 0 — вычищена)", behind, ahead)
	}

	// No loss: a full pass from the zero cursor purges the laggard too;
	// a one-off pass does not write the circle state — circle 2's cursor
	// row (the d2/sub1 position) stays in place.
	if _, err := pool.Exec(ctx, "ANALYZE subdomain_results"); err != nil {
		t.Fatalf("ANALYZE: %v", err)
	}
	stats, err := content.RetentionOnce(ctx, pool, content.RetentionOptions{
		Retention:     6 * time.Hour,
		BatchSize:     100,
		SelectTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("RetentionOnce: %v", err)
	}
	if stats.Rows != 1 {
		t.Fatalf("RetentionOnce: Rows = %d, want 1 (запись позади курсора — идемпотентность)", stats.Rows)
	}
	if err := pool.QueryRow(ctx,
		"SELECT zone_id, domain_id, subdomain_id FROM retention_drain_cursor WHERE id = 1",
	).Scan(&curZone, &curDomain, &curSub); err != nil {
		t.Fatalf("строка курсора после RetentionOnce: %v", err)
	}
	if curDomain != 2 || curSub != 1 {
		t.Fatalf("строка курсора = %d/%d, want позиция цикла 2 (2/1) — разовый проход её не трогает", curDomain, curSub)
	}
}

// planIdxScanRe — the index access shape (Index Scan / Index Only Scan) in
// the plan-pinning EXPLAIN tests (T-271): it covers both the PG16 leaf
// partitions' entries and the PG17+ parents. The anchor at the start of
// the node's line (^, ->): "Bitmap Index Scan"/"Bitmap Heap Scan" contain
// the "Index Scan" substring, and without the anchor a bitmap plan passed
// the check (the T-271 review, P3-2) — the bitmap nodes themselves are
// forbidden in the tests' bad lists.
var planIdxScanRe = regexp.MustCompile(`(?m)^\s*(?:->\s*)?(Index Scan|Index Only Scan)\b`)

// explainPlan — the EXPLAIN (COSTS OFF) text of a query inside the pinning
// transaction (T-271; extracted from the inline blocks of
// render_requeue_test.go for the pinning tests of both cursor selections).
func explainPlan(t *testing.T, tx pgx.Tx, ctx context.Context, sql string) string {
	t.Helper()
	rows, err := tx.Query(ctx, "EXPLAIN (COSTS OFF) "+sql)
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("EXPLAIN: чтение: %v", err)
		}
		plan.WriteString(line)
		plan.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	return plan.String()
}

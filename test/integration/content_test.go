// Интеграционные тесты хранилища контента (T-046; ТЗ 5.7, 8, 11.4 п. 3):
// сквозные сценарии поверх живого PostgreSQL (метаданные contents,
// изолированная схема migratedSchema) и реальных пак-файлов в t.TempDir —
// запись/чтение со словарём и без, дедупликация, ссылки и SwapRef,
// ротация паков, компактификация, лимит max_store_bytes и реакция на
// переполнение диска. Контракт ContentStore (T-041) — на PackStore.
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

// contentTestEnv — PackStore на живой схеме (таблица contents из миграции
// 000001) и пак-файлах во временном каталоге. Маленький PackSize по
// умолчанию: тесты ротации и компактификации полагаются на неё.
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

// contentRow — строка contents для проверок напрямую в БД.
type contentRow struct {
	packID int32
	off    int64
	clen   int32
	rawLen int32
	refc   int32
}

// contentRowByHash — живая или мёртвая строка (без фильтра refcount —
// тестам интересны именно мёртвые тоже).
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

// contentPackIDs — отсортированные pack_id живых и мёртвых строк.
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

// packFileExists — presence пак-файла в каталоге контента.
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

// parkingPage — «парковочная страница» общего шаблона: именно такие
// сжимаются словарём и дедуплицируются хэшем (ТЗ 5.7, 8).
func parkingPage(domain, note string) []byte {
	return []byte(fmt.Sprintf(`<!doctype html><html><head><title>Добро пожаловать на %s</title>
<meta name="parking" content="shared-template"></head>
<body><div class="hero">Домен продаётся</div><div class="note">%s</div>
<div class="footer">Эта страница размещена на парковке хостинга</div></body></html>`, domain, note))
}

// TestContent_StoreContract — контракт ContentStore (T-041) на PackStore
// с живыми PostgreSQL и файлами: главная проверка задачи. Набор — в
// пакете contenttest с T-228 (testing только в тестовых пакетах).
func TestContent_StoreContract(t *testing.T) {
	skipUnlessIntegration(t)
	contenttest.StoreContract(t, func(t *testing.T) content.ContentStore {
		st, _ := contentTestEnv(t, nil)
		return st
	})
}

// TestContent_PutBatch — групповая фиксация на живой PostgreSQL (T-228,
// ревью P3-1): дедуп-пачка (unnest-джойн с счётчиками копий) и пакетный
// INSERT с повторами хэша — единственная проверка операторов пачки
// реальным сервером и pgx-кодирования аргументов (юнит-тесты идут на
// фейковом пуле). Оба пути: живой хэш → refcount 1+n, новый с дублями →
// n; ссылки настоящие — Delete × refcount исчерпывает контент.
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
	// Created: live — дедуп (байтов нет), fresh — первый элемент пишет
	// байты, повторы хэша — нет.
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

	// Ссылки настоящие: исчерпание Delete × refcount убивает контент.
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

// TestContent_Roundtrip — запись/чтение: со словарём и без, сжатие
// уменьшает блок (len < raw_len), переоткрытие каталога читает контент
// (межпроцессная семантика http-worker/render-worker, ТЗ 11.4 п. 3).
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
			// Сжатие работает на сжимаемых страницах (ТЗ 5.7): крупный
			// повторяющийся контент занимает меньше сырых байтов; для
			// маленьких страниц zstd-оверхед кадра может превысить выгоду
			// — сравнение только про сжимаемый случай.
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
		// Словарь обязан помогать: блок страницы общего шаблона заметно
		// меньше сырых байтов (точный коэффициент — зона zstd, здесь —
		// сам факт заметного сжатия, ТЗ 5.7).
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

		// Второй «процесс»: тот же каталог, тот же пул, чтение блока,
		// записанного первым (Chromium-воркер читает HTML http-воркера).
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

// TestContent_DedupAndRefcount — дедупликация (ТЗ 5.7): повторный Put
// не пишет байтов (Created=false), refcount растёт; Delete снимает
// ссылки по одной, последняя делает строку мёртвой (ТЗ 11.4 п. 3).
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

	dup, err := st.Put(ctx, append([]byte(nil), page...)) // независимая копия байтов
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

	// Снятие ссылок: пока есть — контент жив; последняя — строка мертва.
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
	// Воскрешение: новый Put пишет свежий блок (T-042), refcount 1.
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

// TestContent_SwapRef — перезапись ссылки при перезаписи результата
// (T-044; ТЗ 5.7, 8): сценарии свопа на живой таблице contents.
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

// TestContent_PackRotation — ротация по pack_size (ТЗ 5.7): паки
// появляются, контент читается из всех, новейший — максимальный id.
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
	// Контент читается из каждого пака (блоки по зафиксированным смещениям).
	for i, h := range hashes {
		if _, err := st.Get(ctx, h); err != nil {
			t.Errorf("Get(%d) из пака: %v", i, err)
		}
	}
	// Новейший пак продолжает принимать записи.
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

// TestContent_Compaction — компактификация (ТЗ 5.7): доля живых ниже
// порога — пак переписывается и удаляется, живой контент читаем, место
// освобождено; выше порога — не трогается.
func TestContent_Compaction(t *testing.T) {
	skipUnlessIntegration(t)
	st, pool := contentTestEnv(t, nil)
	ctx := context.Background()

	// Паки 1 и 2: пять страниц в первом, шестая ротирует во второй.
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

	// Один живой, остальные мёртвые: доля живых << 50%.
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
	// Живой контент переехал и читается; мёртвые строки вычищены.
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
	// Запись продолжается, место освобождено и учитывается (T-046).
	if _, err := st.Put(ctx, parkingPage("after-compact.example", "запись после компактификации")); err != nil {
		t.Fatalf("Put после компактификации: %v", err)
	}

	// Пак с живыми данными выше порога не компактифицируется.
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

// TestContent_MaxStoreBytes — лимит одного контента (ТЗ 5.5, 5.7):
// превышение — ErrTooLarge целиком, граница — проходит.
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

// TestContent_DiskFullPause — реакция на переполнение диска (ТЗ 8):
// порог недостижимо высок — Put возвращает ErrStoreFull (пауза записи),
// событие алерта несёт реальный снимок statfs; читаемый порог — запись
// идёт. Заглушаемый statfs и семантика порога — юнит-тесты disk_test.go,
// здесь — живая файловая система и сквозная связь с Put.
func TestContent_DiskFullPause(t *testing.T) {
	skipUnlessIntegration(t)

	var (
		mu     sync.Mutex
		events []content.StoreFullEvent
	)
	st, _ := contentTestEnv(t, func(o *content.PackOptions) {
		o.DiskFreePercent = 101 // выше 100% места не бывает — пауза всегда
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

	// Живой порог: места достаточно, запись идёт (тот же каталог).
	st2, _ := contentTestEnv(t, func(o *content.PackOptions) { o.DiskFreePercent = 0.0001 })
	if _, err := st2.Put(ctx, parkingPage("ok.example", "места достаточно")); err != nil {
		t.Fatalf("Put при живом пороге: %v", err)
	}
}

// TestContent_RefcountFSM — инварианты автомата refcount на живой
// PostgreSQL (T-265, ТЗ-fsm §3 ФТ-11; спецификация — doc-комментарий
// internal/content/pack.go): воскрешение tombstone-строки идёт веткой
// ON CONFLICT DO UPDATE — refcount 0→1, размещение переведено на свежий
// блок (EXCLUDED), Created = false; строка, удалённая целиком
// (представление retention/компактификации), воскресает веткой INSERT —
// Created = true; двойной Delete мёртвого — ErrNotFound. Юнит-проход по
// таблице переходов — internal/content (TestPackRefcountFSM).
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

	// live(2) → dead·tombstone: обе ссылки сняты, строка остаётся.
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

	// П6: воскрешение tombstone — DO UPDATE, размещение EXCLUDED.
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

	// Представление «строки нет» (путь retention T-166 / вычистки
	// компактификации T-045): строка удалена целиком — Get/Delete как у
	// мёртвого, воскрешение веткой INSERT, Created = true.
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

// TestContent_RefcountClampDesync — клэмп GREATEST(refcount − n, 0) при
// рассинхроне на живой PostgreSQL (T-265, инвариант И1 автомата
// refcount, переход П9): две остаревшие записи ссылаются на контент со
// счётчиком 1 — агрегированное срезание двух ссылок клэмпится в ноль, а
// не в минус, строка удаляется той же транзакцией (retention минует
// tombstone), проход не падает и не триггерит CHECK
// contents_refcount_check (миграция 000019). Юнит-проход —
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

	// Рассинхрон: держателей 2, счётчик 1 (одна ссылка уже снята иным
	// путём — сценарий ошибки учёта, которую клэмп прощает нулём).
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

// TestSchema_ContentsRefcountCheck — парный тест-инвариант CHECK
// contents_refcount_check (T-265, ФТ-11, миграция 000019): refcount < 0
// отвергается схемой (23514) на INSERT и UPDATE, tombstone refcount = 0
// легален (мёртвое представление № 1, T-042), ограничение добавлено
// NOT VALID (convalidated = false — VALIDATE по регламенту миграций,
// docs/schema.md «CHECK-инварианты»); contents не секционирована —
// объект один.
func TestSchema_ContentsRefcountCheck(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	// Легальные строки: живая refcount = 1 и tombstone refcount = 0.
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

	// Нарушение инварианта: INSERT с refcount = −1 — 23514 схемой.
	if err := ins(0x03, -1); err == nil || !strings.Contains(err.Error(), "contents_refcount_check") {
		t.Errorf("INSERT refcount −1: err = %v, want violation contents_refcount_check", err)
	}
	// UPDATE существующей строки в минус — тоже отвергается (NOT VALID
	// бдит новые и изменяемые строки).
	if _, err := pool.Exec(ctx,
		"UPDATE contents SET refcount = -1 WHERE hash = $1", refcTestHash(0x01)); err == nil ||
		!strings.Contains(err.Error(), "contents_refcount_check") {
		t.Errorf("UPDATE в минус: err = %v, want violation contents_refcount_check", err)
	}

	// NOT VALID: ограничение в схеме теста (фильтр по current_schema —
	// в БД живут схемы соседних прогонов) ещё не валидировано; contents
	// не секционирована (миграция 000001) — объект один.
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

// refcTestHash — 16-октетный хэш с байтом-заполнителем (посев
// TestSchema_ContentsRefcountCheck).
func refcTestHash(b byte) []byte {
	h := make([]byte, 16)
	h[15] = b
	return h
}

// retentionPinSelectSQL — полный текст retentionSelectSQL с литералами
// параметров (T-271; якорь — internal/content/retention.go, дублирование
// — стиль rqBatchSQL): EXPLAIN с параметрами требует подготовленного
// оператора, проверке формы плана литералы безразличны. Коды схемы —
// литералами (FlagRenderPending = 2, StatusDeleted = 6 — стиль
// rqBatchSQL).
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

// TestContent_RetentionSelectPlanPinned — закрепление формы плана
// выборки пачки retention (T-271; инцидент 08–09.11.2026 — деградация
// плана до seq scan 9 млн строк + hash join domains + Sort ≈1 ГиБ
// поверх shared_buffers 2 ГиБ клала PostgreSQL OOM-киллами, пачки не
// коммитились — курсор не двигался): внутри транзакции закрепления
// (SET LOCAL seq/hash/merge/bitmap off — те же литералы
// internal/content/retention.go) EXPLAIN показывает индексную форму
// доступа к subdomain_results + Limit, без Hash/Merge Join и Seq Scan;
// Incremental Sort допустим (enable_sort не отключается — ORDER BY
// (checked_at, PK) держит его поверх порядка индекса). Bitmap выключен
// доводкой T-271: битмап-путь не сохраняет порядок индекса → явный
// Sort, форма недетерминирована.
func TestContent_RetentionSelectPlanPinned(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	// Посев: зона, живой домен, остаревшая запись с контентом — форма
	// плана от объёма не зависит (закрепление — SET LOCAL), но пустая
	// таблица статистикой вводила бы планировщик в нулевые оценки.
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
	// Bitmap-путь — другой диагноз, чем невыключенные GUC: закрепление
	// действует, но форма не закрыта до конца — битмап не сохраняет
	// порядок индекса, ORDER BY закрывается явным Sort (доводка T-271).
	for _, bad := range []string{"Bitmap Index Scan", "Bitmap Heap Scan"} {
		if strings.Contains(plan, bad) {
			t.Errorf("закреплённый план содержит %q (bitmap-путь ломает порядок курсора — форма не закреплена до конца):\n%s", bad, plan)
		}
	}
}

// TestContent_RetentionCatchUpFromZeroCursor — главный тест-пробел T-271,
// закрытый T-273: catch-up-проход с НУЛЕВОГО курсора обязан завершаться
// в SelectTimeout при чистом skip-префиксе и дренировать всё множество
// кандидатов. Прод-механика инцидента 09–10.11.2026 (масштаб —
// прод-пропорция в миниатюре, свойство важнее абсолютных миллионов):
// retention стоял с ночи 08.11 → курсор нулевой → каждая выборка идёт
// «с начала таблицы» через миллионы уже чистых строк (предикат «есть
// что чистить» вне индекса — heap-визит каждой); дедлайн 600 с убивал
// проход ДО коммита → курсор не двигался → идентичный обречённый скан
// (вечная петля). Здесь: 200 тыс. чистых остаревших строк (skip-префикс
// по checked_at) впереди 30 тыс. кандидатов (jsonb) и 5 тыс. свежих —
// один проход RetentionOnce обязан очистить всех кандидатов и уложиться
// в ужиженный через опции SelectTimeout (30 с на пачку против продовых
// 3600 с), а повторный — увидеть пустое множество (дренаж завершён).
// Контракт-тест по сути: стоимость прод-петли на фикстуре не
// воспроизводится (тесту без разницы, секунды или часы шёл проход,
// пока укладывается в потолок) — он фиксирует, что проход с нулевого
// курсора ЗАВЕРШАЕТСЯ и двигает дренаж до конца множества; форму плана
// и факт исполнения на объёме проверяет TestRenderLateSelectPlanUnpinned
// и EXPLAIN-тест закреплённой выборки retention выше.
func TestContent_RetentionCatchUpFromZeroCursor(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	// Две зоны → выделенные партиции subdomain_results (прод — ~677;
	// партиционирование — часть стоимости: Append по листам, PK-пробы).
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
		cleanPrefix = 200000 // чистые остаревшие строки ПОЗАДИ курсора стоимости
		dirty       = 30000  // кандидаты: остаревшие с jsonb
		fresh       = 5000   // моложе окна — вне дренажа
		total       = cleanPrefix + dirty + fresh
	)
	// Домен на строку — прод-пропорция (43 млн domains при 11,3 млн
	// записей: большинство доменов без записей).
	if _, err := pool.Exec(ctx, `
		INSERT INTO domains (zone_id, id, label)
		SELECT CASE WHEN g % 2 = 0 THEN $1::int2 ELSE $2::int2 END, g, 'cup' || g
		FROM generate_series(1, $3) g`, z1, z2, total); err != nil {
		t.Fatalf("посев domains: %v", err)
	}
	// Строки по checked_at-порядку: чистый префикс [−35ч, −7,2ч] →
	// кандидаты [−7ч, −6,2ч] (окно retention 6 ч — остаревшие) → свежие
	// [−1ч, −59м]. Кандидаты отличимы заголовком jsonb (трим без ссылок
	// — contents не нужен), чистый префикс — паспорт без jsonb, как их
	// оставил retention до простоя.
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

	// Catch-up-проход с нулевого курсора: SelectTimeout ужижен через
	// опции (30 с на пачку — щедро против фактических единиц секунд,
	// многократно меньше продовых 3600 с: свойство «проход успевает»
	// проверяется, а не ждётся час).
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
	// Дренаж завершён: кандидатов не осталось (свежие не тронуты —
	// у них headers на месте, окно возраста не вышло).
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

	// Повторный проход — множество исчерпано (курсор/очистка
	// продвинулись): пустой дренаж за одну выборку.
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

// planIdxScanRe — индексная форма доступа (Index Scan / Index Only
// Scan) в EXPLAIN-тестах закрепления планов (T-271): покрывает обе
// записи и листовые партиции PG16, и родителей PG17+. Якорь на начало
// строки узла (^, ->): «Bitmap Index Scan»/«Bitmap Heap Scan» содержат
// подстроку «Index Scan», и без якоря bitmap-план проходил проверку
// (ревью T-271, P3-2) — сами bitmap-узлы запрещены в bad-списках тестов.
var planIdxScanRe = regexp.MustCompile(`(?m)^\s*(?:->\s*)?(Index Scan|Index Only Scan)\b`)

// explainPlan — текст EXPLAIN (COSTS OFF) запроса внутри транзакции
// закрепления (T-271; вынесено из inline-блоков render_requeue_test.go
// для тестов закрепления обоих курсорных выборок).
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

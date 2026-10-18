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
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/content"
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
		"SELECT pack_id, len, raw_len, refcount FROM contents WHERE hash = $1", h[:],
	).Scan(&r.packID, &r.clen, &r.rawLen, &r.refc)
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
// с живыми PostgreSQL и файлами: главная проверка задачи.
func TestContent_StoreContract(t *testing.T) {
	skipUnlessIntegration(t)
	content.StoreContract(t, func(t *testing.T) content.ContentStore {
		st, _ := contentTestEnv(t, nil)
		return st
	})
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

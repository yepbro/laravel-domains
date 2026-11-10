// Тесты триажа мутантов CI-прогона 09.11.2026 (половина A): добивки
// zonestats.go — ошибки Scan/rows.Err каждого читателя и писателя
// (строки не той ширины, деградированные наборы), нулевые значения на
// ошибках, сбой Begin/Commit, счётчики-мISMATCH перезаписей, суммы
// FILTER-колонок по нескольким зонам, заполнение «Old := New» для
// отсутствующих корзин, дрейф-отчёт (тишина без дрейфа, ровно пять
// строк топа, точная сумма), Histорgram-корзины метрик цикла и
// пред-отменённый ctx стартового прохода (gauge «статус повис»).
package monitor

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"crawler/internal/metrics"
)

// zsLogger — текстовый логгер цикла в буфер (уровень Debug).
func zsLogger(buf *zonestatsSyncBuf) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// TestZoneStatsScanErrors — ошибка Scan каждой позиции прохода (строка
// не той ширины) обёрнута контекстом шага; проход падает.
func TestZoneStatsScanErrors(t *testing.T) {
	// Шаги с «узкой» строкой: Scan каждого читателя/писателя обязан
	// провалиться и уронить проход с контекстом «чтение».
	for _, tc := range []struct {
		name string
		step int
		rows [][]any
		want string
	}{
		{"старые зоны", 0, [][]any{{int16(7), int64(1)}}, "чтение зон: строка"},
		{"старые счётчики", 1, [][]any{{"status_alive"}}, "чтение счётчиков: строка"},
		{"суточная строка", 2, [][]any{{int64(1), int64(2)}}, "суточная строка проверок: чтение"},
		{"зрелая корзина", 4, [][]any{{int64(1), int64(2)}}, "зрелая корзина ретрая: чтение"},
		{"per-zone агрегат", 12, [][]any{{int16(7), int64(1)}}, "агрегат: чтение"},
		{"laravel-агрегат", 13, [][]any{{int16(7)}}, "laravel-агрегат: чтение"},
		{"история", 14, [][]any{{int64(1)}}, "история: чтение"},
		{"часовые корзины", 15, [][]any{{int64(1), int64(2)}}, "часовые корзины: чтение"},
		{"перезапись зон", 16, [][]any{{int16(7), int64(1)}}, "зоны: чтение"},
		{"upsert счётчиков", 17, [][]any{{"status_alive"}}, "глобальные: чтение"},
		{"очистка корзин", 20, [][]any{{int64(1), int64(2)}}, "очистка корзин ретрая: чтение"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			steps := stepsOfPass(
				zoneValsOf(7, 10, 4, 2, 3, 1),
				zoneValsOf(7, 10, 4, 2, 3, 1),
				aggRowOf(7, 10, 4, 2, 3, make([]int64, len(statusAggCols)), 0),
				zonestatsRows(),
				fullCounterRows(nil),
				fullCounterRows(nil))
			steps[tc.step].rows = zonestatsRows(tc.rows...)
			pool := &zonestatsFakePool{steps: steps}
			_, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("шаг %d: ошибка = %v, want контекст %q", tc.step, err, tc.want)
			}
		})
	}
}

// TestZoneStatsRowsErr — rows.Err после прочитанной строки роняет
// читателя с контекстом шага (обрыв соединения — не «строки нет»);
// пустой ответ скалярных агрегатов истории/часовых корзин — обрыв
// протокола.
func TestZoneStatsRowsErr(t *testing.T) {
	// Строка есть, после неё — ошибка итерации.
	rowErr := func(row []any) *zonestatsFakeRows {
		return &zonestatsFakeRows{rows: [][]any{row}, err: errors.New("обрыв соединения")}
	}
	for _, tc := range []struct {
		name string
		step int
		rows *zonestatsFakeRows
		want string
	}{
		{"суточная строка", 2, rowErr([]any{int64(7)}), "суточная строка проверок"},
		{"зрелая корзина", 4, rowErr([]any{int64(7)}), "зрелая корзина ретрая"},
		{"история", 14, rowErr([]any{int64(1), int64(2)}), "история"},
		{"часовые корзины", 15, rowErr([]any{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6)}), "часовые корзины"},
		{"перезапись зон", 16, rowErr(zoneValsOf(7, 10, 4, 2, 3, 1)), "зоны"},
		{"upsert счётчиков", 17, rowErr(counterRowOf("status_alive", 5)), "глобальные"},
		{"очистка корзин", 20, rowErr([]any{int64(1)}), "очистка корзин ретрая"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			steps := stepsOfPass(
				zoneValsOf(7, 10, 4, 2, 3, 1),
				zoneValsOf(7, 10, 4, 2, 3, 1),
				aggRowOf(7, 10, 4, 2, 3, make([]int64, len(statusAggCols)), 0),
				zonestatsRows(),
				fullCounterRows(nil),
				fullCounterRows(nil))
			steps[tc.step].rows = tc.rows
			pool := &zonestatsFakePool{steps: steps}
			_, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{})
			if err == nil || !strings.Contains(err.Error(), tc.want) ||
				!strings.Contains(err.Error(), "обрыв соединения") {
				t.Errorf("шаг %d: ошибка = %v, want %q с исходной", tc.step, err, tc.want)
			}
		})
	}
	// Пустой ответ скалярного агрегата (история/часовые корзины без
	// GROUP BY обязаны отвечать строкой) — обрыв протокола.
	for _, tc := range []struct {
		name string
		step int
		want string
	}{
		{"история", 14, "история: пустой ответ агрегата"},
		{"часовые корзины", 15, "часовые корзины: пустой ответ агрегата"},
	} {
		steps := stepsOfPass(
			zoneValsOf(7, 10, 4, 2, 3, 1),
			zoneValsOf(7, 10, 4, 2, 3, 1),
			aggRowOf(7, 10, 4, 2, 3, make([]int64, len(statusAggCols)), 0),
			zonestatsRows(),
			fullCounterRows(nil),
			fullCounterRows(nil))
		steps[tc.step].rows = zonestatsRows()
		pool := &zonestatsFakePool{steps: steps}
		if _, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{}); err == nil ||
			!strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: ошибка = %v, want %q", tc.name, err, tc.want)
		}
	}
}

// TestZoneStatsWriteCountMismatch — RETURNING с недостающими строками
// роняет перезапись зон/счётчиков (строки обязательны).
func TestZoneStatsWriteCountMismatch(t *testing.T) {
	// Две зоны в чтении, RETURNING — одна: «перезаписано 1 из 2».
	steps := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, make([]int64, len(statusAggCols)), 0),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))
	steps[0].rows = zonestatsRows(zoneValsOf(7, 10, 4, 2, 3, 1), zoneValsOf(9, 2, 2, 0, 0, 0))
	steps[12].rows = zonestatsRows(
		aggRowOf(7, 10, 4, 2, 3, make([]int64, len(statusAggCols)), 0),
		aggRowOf(9, 2, 2, 0, 0, make([]int64, len(statusAggCols)), 0))
	steps[16].rows = zonestatsRows(zoneValsOf(7, 10, 4, 2, 3, 1)) // зона 9 потеряна
	pool := &zonestatsFakePool{steps: steps}
	if _, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{}); err == nil ||
		!strings.Contains(err.Error(), "перезаписано 1 из 2") {
		t.Errorf("ошибка зон = %v, want «перезаписано 1 из 2»", err)
	}

	// Upsert счётчиков с неполной легендой — «N из M строк».
	steps2 := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, make([]int64, len(statusAggCols)), 0),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))
	steps2[17].rows = zonestatsRows(counterRowOf("status_alive", 5))
	pool2 := &zonestatsFakePool{steps: steps2}
	if _, err := ZoneStatsOnce(context.Background(), pool2, ZoneStatsOptions{}); err == nil ||
		!strings.Contains(err.Error(), "строк") {
		t.Errorf("ошибка счётчиков = %v, want число строк", err)
	}
}

// TestZoneStatsBeginError — сбой открытия транзакции — ошибка прохода.
func TestZoneStatsBeginError(t *testing.T) {
	pool := &zonestatsFakePool{beginErr: errors.New("пул исчерпан")}
	if _, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{}); err == nil ||
		!strings.Contains(err.Error(), "транзакция") {
		t.Errorf("ошибка Begin = %v, want контекст транзакции", err)
	}
}

// TestZoneStatsSumsMultipleZones — значения суточных строк и корзин —
// СУММЫ FILTER-колонок по всем зонам (мутанты присваивания теряли все,
// кроме последней зоны).
func TestZoneStatsSumsMultipleZones(t *testing.T) {
	statuses := make([]int64, len(statusAggCols))
	steps := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 0, 3, 1, 2, 4, 5),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))
	steps[0].rows = zonestatsRows(zoneValsOf(7, 10, 4, 2, 3, 1), zoneValsOf(9, 2, 2, 0, 0, 0))
	steps[12].rows = zonestatsRows(
		aggRowOf(7, 10, 4, 2, 3, statuses, 0, 3, 1, 2, 4, 5),
		aggRowOf(9, 2, 2, 0, 0, make([]int64, len(statusAggCols)), 0, 30, 10, 20, 40, 50))
	steps[16].rows = zonestatsRows(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(9, 2, 2, 0, 0, 0))
	pool := &zonestatsFakePool{steps: steps}
	res, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}
	calls := pool.calls()
	// Суточные строки: checks 3+30, temp 1+10.
	if got := calls[18].args[0].(int64); got != 33 {
		t.Errorf("checks:СУММА = %d, want 33 (3+30)", got)
	}
	if got := calls[19].args[0].(int64); got != 11 {
		t.Errorf("temp_error:СУММА = %d, want 11 (1+10)", got)
	}
	// Корзины ретрая: зрелая 4+40, сегодняшняя 2+20.
	if got := calls[21].args[0].(int64); got != 44 {
		t.Errorf("зрелая корзина = %d, want 44", got)
	}
	if got := calls[22].args[0].(int64); got != 22 {
		t.Errorf("сегодняшняя корзина = %d, want 22", got)
	}
	// «Статус повис» — тоже сумма по зонам.
	if res.NewDoneStale != 55 {
		t.Errorf("NewDoneStale = %d, want 55 (5+50)", res.NewDoneStale)
	}
}

// TestZoneStatsFillOldEqualsNew — корзины, которых не было: позиция —
// заполнение (Old := New), не дрейф.
func TestZoneStatsFillOldEqualsNew(t *testing.T) {
	statuses := make([]int64, len(statusAggCols))
	steps := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 0),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))
	// Все старые корзины/суточные — пустые (строк нет); новые значения
	// шагов ненулевые (заготовленные нули stepsOfPass заменяем).
	steps[18].rows = zonestatsRows([]any{int64(7)})
	steps[19].rows = zonestatsRows([]any{int64(3)})
	steps[21].rows = zonestatsRows([]any{int64(9)})
	steps[22].rows = zonestatsRows([]any{int64(4)})
	steps[24].rows = zonestatsRows([]any{int64(8)})
	steps[25].rows = zonestatsRows([]any{int64(2)})
	steps[27].rows = zonestatsRows([]any{int64(6)})
	steps[28].rows = zonestatsRows([]any{int64(5)})
	steps[30].rows = zonestatsRows([]any{int64(11)})
	steps[31].rows = zonestatsRows([]any{int64(1)})
	res, err := ZoneStatsOnce(context.Background(), &zonestatsFakePool{steps: steps}, ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}
	byName := make(map[string]CounterPosition, len(res.Positions))
	for _, p := range res.Positions {
		byName[p.Counter] = p
	}
	for name, wantNew := range map[string]int64{
		"daily:checks": 7, "daily:temp_error": 3,
		"retry_mature:past": 9, "retry_mature:today": 4,
		"reanalyze:prev": 8, "reanalyze:cur": 2,
		"rendered:prev": 6, "rendered:cur": 5,
		"results:prev": 11, "results:cur": 1,
	} {
		p := byName[name]
		if p.Old != wantNew || p.New != wantNew || p.Drift() != 0 {
			t.Errorf("%s = %+v, want old=new=%d (заполнение, не дрейф)", name, p, wantNew)
		}
	}
}

// TestZoneStatsAggErrorsZeroValues — ошибки читателей агрегатов
// возвращают нулевые значения (пин нулей на ошибке).
func TestZoneStatsAggErrorsZeroValues(t *testing.T) {
	// История: Query-ошибка — нули.
	errTx := func() *zonestatsFakeTx {
		pool := &zonestatsFakePool{steps: []zonestatsStep{{err: errors.New("бд недоступна")}}}
		tx, _ := pool.Begin(context.Background())
		return tx.(*zonestatsFakeTx)
	}
	if a, l, err := readHistoryAgg(context.Background(), errTx()); err == nil || a != 0 || l != 0 {
		t.Errorf("история: (%d/%d, %v), want нули и ошибка", a, l, err)
	}
	// Часовые корзины: Query-ошибка — нулевая структура (пин полей).
	if h, err := readHourlyAgg(context.Background(), errTx()); err == nil || h.prevResults != 0 || h.curReanalyze != 0 {
		t.Errorf("часовые корзины: (%+v, %v), want нули и ошибка", h, err)
	}
	// Суточное значение: Query-ошибка — 0/false.
	if v, exists, err := readDailyValue(context.Background(), errTx(), readDailyChecksSQL, "суточная"); err == nil || v != 0 || exists {
		t.Errorf("readDailyValue: (%v/%v, %v), want 0/false и ошибка", v, exists, err)
	}
	// Перезапись суточной строки: пустой ответ upsert — ошибка и 0.
	upEmpty := &zonestatsFakePool{steps: []zonestatsStep{{rows: zonestatsRows()}}}
	upTx, _ := upEmpty.Begin(context.Background())
	if v, err := writeDailyCounter(context.Background(), upTx, recountDailyChecksSQL, 5, time.Unix(0, 0), "суточная"); err == nil || v != 0 {
		t.Errorf("writeDailyCounter: (%v, %v), want 0 и ошибка пустого upsert", v, err)
	}
	// Ошибка Query у writeDailyCounter — тоже 0.
	werr := &zonestatsFakePool{steps: []zonestatsStep{{err: errors.New("бд недоступна")}}}
	wtx, _ := werr.Begin(context.Background())
	if v, err := writeDailyCounter(context.Background(), wtx, recountDailyChecksSQL, 5, time.Unix(0, 0), "суточная"); err == nil || v != 0 {
		t.Errorf("writeDailyCounter (Query-сбой): (%v, %v), want 0 и ошибка", v, err)
	}
	// Очистка корзин: Query-ошибка — ошибка с контекстом.
	cerr := &zonestatsFakePool{steps: []zonestatsStep{{err: errors.New("бд недоступна")}}}
	ctxTx, _ := cerr.Begin(context.Background())
	if err := cleanupBaskets(context.Background(), ctxTx, retryMatureCleanupSQL, "очистка корзин ретрая"); err == nil ||
		!strings.Contains(err.Error(), "очистка корзин ретрая") {
		t.Errorf("cleanupBaskets: %v, want контекст и ошибка", err)
	}
}

// TestAbsDiff — |a − b| обеих ориентаций и нуля.
func TestAbsDiff(t *testing.T) {
	for _, c := range [][3]int64{{5, 3, 2}, {3, 5, 2}, {0, 0, 0}, {-4, 3, 7}} {
		if got := absDiff(c[0], c[1]); got != c[2] {
			t.Errorf("absDiff(%d, %d) = %d, want %d", c[0], c[1], got, c[2])
		}
	}
}

// TestZoneStatsLoopNoDriftNoWarn — стационарные проходы (без дрейфа) не
// поднимают Warn «реконсиляция: дрейф» (мутант вырезания return при
// пустом drifted спамил бы positions=0).
func TestZoneStatsLoopNoDriftNoWarn(t *testing.T) {
	slogBuf := &zonestatsSyncBuf{}
	logger := zsLogger(slogBuf)
	reg := metrics.New("monitor")
	statuses := make([]int64, len(statusAggCols))
	pass := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 0),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))
	pool := &zonestatsFakePool{steps: append(append([]zonestatsStep{}, pass...), pass...)}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ZoneStatsLoop(ctx, pool, reg, ZoneStatsOptions{Interval: 2 * time.Millisecond, Log: logger})
	}()
	waitCond(t, 2*time.Second, func() bool { return pool.stats() >= 64 }, "два прохода")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ZoneStatsLoop: %v", err)
	}
	if log := slogBuf.String(); strings.Contains(log, "реконсиляция: дрейф") {
		t.Errorf("стационарные проходы подняли Warn дрейфа:\n%s", log)
	}
}

// TestZoneStatsLoopDriftWarnExact — дрейф нескольких позиций: Warn с
// точной суммой |Δ| и ровно пятью строками топа (шестая и дальше —
// счётчиком, не строками).
func TestZoneStatsLoopDriftWarnExact(t *testing.T) {
	slogBuf := &zonestatsSyncBuf{}
	logger := zsLogger(slogBuf)
	reg := metrics.New("monitor")
	statuses := make([]int64, len(statusAggCols))
	base := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 0),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))
	// Второй проход: 7 позиций зон с дрейфом 2 каждая (все колонки двух
	// зон + третья зона), сумма |Δ| = 14; single-дрейф вариант ниже.
	drift := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 0),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))
	drift[0].rows = zonestatsRows(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(9, 0, 0, 0, 0, 0),
		zoneValsOf(11, 0, 0, 0, 0, 0))
	drift[12].rows = zonestatsRows(
		aggRowOf(7, 10, 4, 2, 3, statuses, 0),
		aggRowOf(9, 2, 2, 2, 2, make([]int64, len(statusAggCols)), 0),
		aggRowOf(11, 4, 0, 0, 0, make([]int64, len(statusAggCols)), 0))
	drift[16].rows = zonestatsRows(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(9, 2, 2, 2, 2, 0),
		zoneValsOf(11, 4, 0, 0, 0, 0))
	pool := &zonestatsFakePool{steps: append(base, drift...)}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ZoneStatsLoop(ctx, pool, reg, ZoneStatsOptions{Interval: 2 * time.Millisecond, Log: logger})
	}()
	waitCond(t, 2*time.Second, func() bool { return slogBuf.contains("реконсиляция: дрейф") }, "Warn дрейфа")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ZoneStatsLoop: %v", err)
	}
	log := slogBuf.String()
	// Дрейф: четыре колонки зоны 9 (по 2) + total зоны 11 (4) = 12.
	if !strings.Contains(log, "sum_abs=12") {
		t.Errorf("Warn без точной суммы дрейфа (want sum_abs=12):\n%s", log)
	}
	// Топ — ровно пять позиций (шестая дрейфующая — за скобками топа).
	for _, want := range []string{"zone:9:total: 0 → 2", "zone:11:total: 0 → 4"} {
		if !strings.Contains(log, want) {
			t.Errorf("Warn без %q:\n%s", want, log)
		}
	}
	if n := strings.Count(log, " → "); n != 5 {
		t.Errorf("строк топа = %d, want 5:\n%s", n, log)
	}
	if strings.Contains(log, "positions=6") {
		t.Errorf("топ не обрезан пятью строками:\n%s", log)
	}
	// Одна дрейфующая позиция — Warn всё равно поднимается (граница
	// «ровно одна» у мутанта len(drifted) == 1 глотала бы его).
	slogBuf2 := &zonestatsSyncBuf{}
	logger2 := zsLogger(slogBuf2)
	single := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 0),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))
	singleDrift := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 6, 2, 3, 1),
		aggRowOf(7, 10, 6, 2, 3, statuses, 0),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))
	pool2 := &zonestatsFakePool{steps: append(single, singleDrift...)}
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() {
		done2 <- ZoneStatsLoop(ctx2, pool2, reg, ZoneStatsOptions{Interval: 2 * time.Millisecond, Log: logger2})
	}()
	waitCond(t, 2*time.Second, func() bool { return slogBuf2.contains("реконсиляция: дрейф") }, "Warn единственного дрейфа")
	cancel2()
	if err := <-done2; err != nil {
		t.Fatalf("ZoneStatsLoop: %v", err)
	}
}

// TestZoneStatsLoopCancelledCtx — пред-отменённый ctx: стартовый проход
// выполняется (источник жив), gauge «статус повис» выставляется, выход
// nil (мутант case true&& не выставлял бы gauge).
func TestZoneStatsLoopCancelledCtx(t *testing.T) {
	reg := metrics.New("monitor")
	statuses := make([]int64, len(statusAggCols))
	pool := &zonestatsFakePool{steps: stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 0, 0, 0, 0, 0, 6),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- ZoneStatsLoop(ctx, pool, reg, ZoneStatsOptions{Interval: time.Hour}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("цикл: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("цикл не вышел по пред-отменённому ctx")
	}
	if got := historyGaugeValue(t, reg, "crawler_domains_new_done_stale"); got != 6 {
		t.Errorf("gauge «статус повис» после стартового прохода = %v, want 6", got)
	}
}

// TestZoneStatsLoopMetricsBuckets — корзины гистограммы прохода:
// экспонента 0.5с ×2, ровно 11 штук (замер §12 — десятки секунд).
func TestZoneStatsLoopMetricsBuckets(t *testing.T) {
	reg := metrics.New("monitor")
	m := newZoneStatsLoopMetrics(reg)
	m.observe(1.0) // серия материализуется — Gather видит семейство
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range families {
		if mf.GetName() != "crawler_counters_recount_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			got := m.GetHistogram().GetBucket()
			if len(got) != 11 {
				t.Errorf("корзин = %d, want 11", len(got))
			}
			want := 0.5
			for i, b := range got {
				if b.GetUpperBound() != want {
					t.Errorf("корзина %d = %v, want %v", i, b.GetUpperBound(), want)
				}
				want *= 2
			}
		}
	}
}

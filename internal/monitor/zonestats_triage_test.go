// The triage tests of the mutants of the CI run of Nov 09 2026 (half A):
// the finishing work on zonestats.go — the Scan/rows.Err errors of each
// reader and writer
// (rows of the wrong width, degraded sets), zero values on
// errors, a Begin/Commit failure, the write count mismatches, the sums
// of the FILTER columns over several zones, the "Old := New" fill-in for
// absent baskets, the drift report (silence without drift, exactly five
// top lines, the exact sum), the histogram buckets of the loop metrics
// and the pre-cancelled ctx of the starting pass (the "status hung"
// gauge).
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

// zsLogger — the loop's text logger into a buffer (the Debug level).
func zsLogger(buf *zonestatsSyncBuf) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// TestZoneStatsScanErrors — a Scan error of each pass position (a row
// of the wrong width) is wrapped with the step context; the pass fails.
func TestZoneStatsScanErrors(t *testing.T) {
	// The steps with a "narrow" row: the Scan of each reader/writer must
	// fail and fail the pass with the "reading" context.
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

// TestZoneStatsRowsErr — rows.Err after a read row fails the
// reader with the step context (a connection break is not "the row is
// missing");
// an empty answer of the scalar history/hourly basket aggregates — a
// protocol break.
func TestZoneStatsRowsErr(t *testing.T) {
	// A row exists, an iteration error follows it.
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
	// An empty answer of a scalar aggregate (history/hourly baskets
	// without GROUP BY must answer with a row) — a protocol break.
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

// TestZoneStatsWriteCountMismatch — a RETURNING with missing rows
// fails the zone/counter overwrite (the rows are mandatory).
func TestZoneStatsWriteCountMismatch(t *testing.T) {
	// Two zones in the read, RETURNING — one: "overwritten 1 of 2".
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
	steps[16].rows = zonestatsRows(zoneValsOf(7, 10, 4, 2, 3, 1)) // zone 9 is lost
	pool := &zonestatsFakePool{steps: steps}
	if _, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{}); err == nil ||
		!strings.Contains(err.Error(), "перезаписано 1 из 2") {
		t.Errorf("ошибка зон = %v, want «перезаписано 1 из 2»", err)
	}

	// A counter upsert with an incomplete legend — "N of M rows".
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

// TestZoneStatsBeginError — a transaction open failure is a pass error.
func TestZoneStatsBeginError(t *testing.T) {
	pool := &zonestatsFakePool{beginErr: errors.New("пул исчерпан")}
	if _, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{}); err == nil ||
		!strings.Contains(err.Error(), "транзакция") {
		t.Errorf("ошибка Begin = %v, want контекст транзакции", err)
	}
}

// TestZoneStatsSumsMultipleZones — the values of the daily rows and the
// baskets are the SUMS of the FILTER columns over all zones (assignment
// mutants lost all but the last zone).
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
	// The daily rows: checks 3+30, temp 1+10.
	if got := calls[18].args[0].(int64); got != 33 {
		t.Errorf("checks:СУММА = %d, want 33 (3+30)", got)
	}
	if got := calls[19].args[0].(int64); got != 11 {
		t.Errorf("temp_error:СУММА = %d, want 11 (1+10)", got)
	}
	// The retry baskets: the mature one 4+40, today's 2+20.
	if got := calls[21].args[0].(int64); got != 44 {
		t.Errorf("зрелая корзина = %d, want 44", got)
	}
	if got := calls[22].args[0].(int64); got != 22 {
		t.Errorf("сегодняшняя корзина = %d, want 22", got)
	}
	// The "status hung" — also a sum over zones.
	if res.NewDoneStale != 55 {
		t.Errorf("NewDoneStale = %d, want 55 (5+50)", res.NewDoneStale)
	}
}

// TestZoneStatsFillOldEqualsNew — baskets that did not exist: the
// position is a fill-in (Old := New), not drift.
func TestZoneStatsFillOldEqualsNew(t *testing.T) {
	statuses := make([]int64, len(statusAggCols))
	steps := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 0),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))
	// All the old baskets/dailies are empty (no rows); the new values
	// of the steps are non-zero (we replace the preset zeros of
	// stepsOfPass).
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

// TestZoneStatsAggErrorsZeroValues — the aggregate reader errors
// return zero values (a zero pin on error).
func TestZoneStatsAggErrorsZeroValues(t *testing.T) {
	// History: a Query error — zeros.
	errTx := func() *zonestatsFakeTx {
		pool := &zonestatsFakePool{steps: []zonestatsStep{{err: errors.New("бд недоступна")}}}
		tx, _ := pool.Begin(context.Background())
		return tx.(*zonestatsFakeTx)
	}
	if a, l, err := readHistoryAgg(context.Background(), errTx()); err == nil || a != 0 || l != 0 {
		t.Errorf("история: (%d/%d, %v), want нули и ошибка", a, l, err)
	}
	// The hourly baskets: a Query error — a zero struct (a field pin).
	if h, err := readHourlyAgg(context.Background(), errTx()); err == nil || h.prevResults != 0 || h.curReanalyze != 0 {
		t.Errorf("часовые корзины: (%+v, %v), want нули и ошибка", h, err)
	}
	// The daily value: a Query error — 0/false.
	if v, exists, err := readDailyValue(context.Background(), errTx(), readDailyChecksSQL, "суточная"); err == nil || v != 0 || exists {
		t.Errorf("readDailyValue: (%v/%v, %v), want 0/false и ошибка", v, exists, err)
	}
	// The daily row overwrite: an empty upsert answer — an error and 0.
	upEmpty := &zonestatsFakePool{steps: []zonestatsStep{{rows: zonestatsRows()}}}
	upTx, _ := upEmpty.Begin(context.Background())
	if v, err := writeDailyCounter(context.Background(), upTx, recountDailyChecksSQL, 5, time.Unix(0, 0), "суточная"); err == nil || v != 0 {
		t.Errorf("writeDailyCounter: (%v, %v), want 0 и ошибка пустого upsert", v, err)
	}
	// A Query error of writeDailyCounter — also 0.
	werr := &zonestatsFakePool{steps: []zonestatsStep{{err: errors.New("бд недоступна")}}}
	wtx, _ := werr.Begin(context.Background())
	if v, err := writeDailyCounter(context.Background(), wtx, recountDailyChecksSQL, 5, time.Unix(0, 0), "суточная"); err == nil || v != 0 {
		t.Errorf("writeDailyCounter (Query-сбой): (%v, %v), want 0 и ошибка", v, err)
	}
	// The basket cleanup: a Query error — an error with context.
	cerr := &zonestatsFakePool{steps: []zonestatsStep{{err: errors.New("бд недоступна")}}}
	ctxTx, _ := cerr.Begin(context.Background())
	if err := cleanupBaskets(context.Background(), ctxTx, retryMatureCleanupSQL, "очистка корзин ретрая"); err == nil ||
		!strings.Contains(err.Error(), "очистка корзин ретрая") {
		t.Errorf("cleanupBaskets: %v, want контекст и ошибка", err)
	}
}

// TestAbsDiff — |a − b| of both orientations and zero.
func TestAbsDiff(t *testing.T) {
	for _, c := range [][3]int64{{5, 3, 2}, {3, 5, 2}, {0, 0, 0}, {-4, 3, 7}} {
		if got := absDiff(c[0], c[1]); got != c[2] {
			t.Errorf("absDiff(%d, %d) = %d, want %d", c[0], c[1], got, c[2])
		}
	}
}

// TestZoneStatsLoopNoDriftNoWarn — steady-state passes (without drift)
// do not raise the drift Warn (a return cut-out mutant with an empty
// drifted would spam positions=0).
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

// TestZoneStatsLoopDriftWarnExact — the drift of several positions: a
// Warn with the exact |Δ| sum and exactly five top lines (the sixth and
// beyond — by the counter, not lines).
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
	// The second pass: 7 zone positions with a drift of 2 each (all the
	// columns of two zones + a third zone), the |Δ| sum = 14; the
	// single-drift variant below.
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
	// The drift: four columns of zone 9 (2 each) + the total of zone 11
	// (4) = 12.
	if !strings.Contains(log, "sum_abs=12") {
		t.Errorf("Warn без точной суммы дрейфа (want sum_abs=12):\n%s", log)
	}
	// The top is exactly five positions (the sixth drifting one is
	// outside the top).
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
	// One drifting position — the Warn is raised anyway (a
	// len(drifted) == 1 mutant boundary would swallow it).
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

// TestZoneStatsLoopCancelledCtx — a pre-cancelled ctx: the starting pass
// runs (the source is alive), the "status hung" gauge is set, the exit
// is nil (a case true&& mutant would not set the gauge).
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

// TestZoneStatsLoopMetricsBuckets — the pass histogram buckets:
// an exponent of 0.5 s ×2, exactly 11 of them (measurement §12 — tens
// of seconds).
func TestZoneStatsLoopMetricsBuckets(t *testing.T) {
	reg := metrics.New("monitor")
	m := newZoneStatsLoopMetrics(reg)
	m.observe(1.0) // the series materializes — Gather sees the family
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

// The second round of mutant triage (catching up with the monitor run
// of 0.901): direct tests of reportDrift and the machines at the actual
// boundaries, VerifyCounters on a fake pool (the unit level: the errors
// of each cross-check step, the row Close flags, the tolerances), pins
// of zeros on every error branch of the reconciliation readers, source
// errors with the original text (psi/redis/renderdebt/unitevents),
// readMemInfo with foreign lines, the withDefaults boundaries at unit
// values (1ns — a working value), the exact parser values on
// base/bitSize injections and the full numeral agreement range.
package monitor

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"crawler/internal/metrics"
	"crawler/internal/queue"
)

// --- reportDrift: direct calls (without loop races) ----------------------------

// TestReportDriftDirect — the first pass is silent; a steady state
// without drift is silent; N drift positions — a Warn with the exact sum
// and exactly five top lines (the sixth and beyond — by the position
// counter, not lines).
func TestReportDriftDirect(t *testing.T) {
	slogBuf := &zonestatsSyncBuf{}
	logger := zsLogger(slogBuf)
	reg := metrics.New("monitor")
	m := newZoneStatsLoopMetrics(reg)
	zero := []CounterPosition{{Counter: "zone:7:total", Old: 5, New: 5}}

	// The first pass (fillFirst) — complete silence even with drift.
	reportDrift(logger, m, ZoneStatsResult{Positions: []CounterPosition{
		{Counter: "x", Old: 1, New: 2}}}, true)
	if log := slogBuf.String(); log != "" {
		t.Fatalf("fillFirst writes a log:\n%s", log)
	}
	// Steady state (no drift) — silence.
	reportDrift(logger, m, ZoneStatsResult{Positions: zero}, false)
	if log := slogBuf.String(); log != "" {
		t.Fatalf("a stationary pass without drift writes a Warn:\n%s", log)
	}
	// The streaming positions (daily/retry/hourly) do not raise a Warn —
	// but are written to the gauge.
	reportDrift(logger, m, ZoneStatsResult{Positions: []CounterPosition{
		{Counter: dailyChecksCounter, Old: 7, New: 9}}}, false)
	if log := slogBuf.String(); strings.Contains(log, "level=WARN") {
		t.Fatalf("a streaming position in the Warn:\n%s", log)
	}
	if v := driftGaugeValue(t, reg, dailyChecksCounter); v != 2 {
		t.Errorf("the streaming position gauge = %v, want 2", v)
	}

	// Seven drifting positions by 2 each: a Warn with sum_abs=14 and
	// exactly five top lines.
	slogBuf = &zonestatsSyncBuf{}
	logger = zsLogger(slogBuf)
	positions := make([]CounterPosition, 7)
	for i := range positions {
		positions[i] = CounterPosition{Counter: "zone:" + strconv.Itoa(i) + ":total", Old: 0, New: 2}
	}
	reportDrift(logger, m, ZoneStatsResult{Positions: positions}, false)
	log := slogBuf.String()
	if !strings.Contains(log, "reconciliation: counter drift") {
		t.Fatalf("the drift Warn was not raised:\n%s", log)
	}
	if !strings.Contains(log, "sum_abs=14") {
		t.Errorf("the Warn without the exact sum (want sum_abs=14):\n%s", log)
	}
	if !strings.Contains(log, "positions=7") {
		t.Errorf("the Warn without the position counter (want positions=7):\n%s", log)
	}
	if n := strings.Count(log, " → "); n != 5 {
		t.Errorf("top lines = %d, want exactly 5:\n%s", n, log)
	}
	if strings.Contains(log, "zone:6:total") || strings.Contains(log, "zone:5:total") {
		t.Errorf("the top is longer than five lines:\n%s", log)
	}
}

// --- VerifyCounters: unit level on a fake pool -----------------------------

// verifyRow — a verifyZonesSQL row: id + 5 pairs (stored, the live aggregate).
func verifyRow(id int16, stored, live [5]int64) []any {
	row := []any{id}
	for i := range stored {
		row = append(row, stored[i], live[i])
	}
	return row
}

// verifySteps — the steps of one cross-check pass: zones
// (verifyZonesSQL), history (historyAggSQL), the aggregate
// (recountAggQuerySQL), the stored counters (storedCountersSQL).
func verifySteps(zones, agg, history, stored *zonestatsFakeRows) []zonestatsStep {
	// The order of the cross-check queries: zones → the per-zone
	// aggregate → history → the stored counters.
	return []zonestatsStep{{rows: zones}, {rows: agg}, {rows: history}, {rows: stored}}
}

func TestVerifyCountersCleanAndDiff(t *testing.T) {
	statuses := make([]int64, len(statusAggCols))
	statuses[4] = 5
	aggRow := aggRowOf(7, 10, 4, 2, 3, statuses, 2)
	// Matching — an empty result. We keep the steps locally: the pool
	// makes them up as the pass goes, pool.steps is empty after the call.
	steps := verifySteps(
		zonestatsRows(verifyRow(7, [5]int64{10, 4, 2, 3, 1}, [5]int64{10, 4, 2, 3, 1})),
		zonestatsRows(aggRow),
		zonestatsRows([]any{int64(0), int64(0)}),
		zonestatsRows(counterRowOf("render_pending", 2), counterRowOf("status_alive", 5)),
	)
	pool := &zonestatsFakePool{steps: steps}
	out, err := VerifyCounters(context.Background(), pool, VerifyOptions{})
	if err != nil {
		t.Fatalf("VerifyCounters: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("mismatches at equality: %+v", out)
	}
	// All the row sets are Closed on the happy path too (zones and history
	// are closed by explicit calls after reading — not only by the error
	// branches above).
	for i, step := range steps {
		if !step.rows.closed {
			t.Errorf("the rows of step %d are not closed by Close", i)
		}
	}

	// A zone/global counter mismatch — positions with Old/New.
	pool2 := &zonestatsFakePool{steps: verifySteps(
		zonestatsRows(verifyRow(7, [5]int64{10, 4, 0, 3, 1}, [5]int64{10, 6, 2, 3, 1})),
		zonestatsRows(aggRow),
		zonestatsRows([]any{int64(1), int64(2)}),
		zonestatsRows(counterRowOf("render_pending", 5)),
	)}
	out, err = VerifyCounters(context.Background(), pool2, VerifyOptions{})
	if err != nil {
		t.Fatalf("VerifyCounters: %v", err)
	}
	byName := map[string]CounterPosition{}
	for _, p := range out {
		byName[p.Counter] = p
	}
	if p := byName["zone:7:pending"]; p.Old != 4 || p.New != 6 {
		t.Errorf("zone:7:pending = %+v, want old=4 new=6", p)
	}
	if p := byName["zone:7:queued"]; p.Old != 0 || p.New != 2 {
		t.Errorf("zone:7:queued = %+v, want old=0 new=2", p)
	}
	if p := byName["global:laravel_appeared_total"]; p.Old != 0 || p.New != 1 {
		t.Errorf("the appeared cumulative = %+v, want old=0 (no row) new=1", p)
	}
	if p := byName["global:laravel_lost_total"]; p.Old != 0 || p.New != 2 {
		t.Errorf("the lost cumulative = %+v, want old=0 new=2", p)
	}
	if p := byName["global:render_pending"]; p.Old != 5 || p.New != 2 {
		t.Errorf("render_pending = %+v, want old=5 new=2", p)
	}

	// Tolerance: a mismatch within Tolerance is not observed.
	pool3 := &zonestatsFakePool{steps: verifySteps(
		zonestatsRows(verifyRow(7, [5]int64{10, 4, 2, 3, 1}, [5]int64{10, 4, 2, 3, 1})),
		zonestatsRows(aggRow),
		zonestatsRows([]any{int64(0), int64(0)}),
		zonestatsRows(counterRowOf("render_pending", 3)),
	)}
	out, _ = VerifyCounters(context.Background(), pool3, VerifyOptions{Tolerance: 1})
	if len(out) != 0 {
		t.Errorf("a mismatch within the tolerance is observed: %+v", out)
	}
}

func TestVerifyCountersErrors(t *testing.T) {
	statuses := make([]int64, len(statusAggCols))
	aggRow := aggRowOf(7, 10, 4, 2, 3, statuses, 0)
	hist := zonestatsRows([]any{int64(0), int64(0)})
	stored := zonestatsRows()
	// An error of each step — with the step context.
	for _, tc := range []struct {
		name string
		step int
		err  error
		want string
	}{
		{"zones", 0, errors.New("db unavailable"), "counter verify: zones"},
		{"aggregate", 1, errors.New("db unavailable"), "aggregate"},
		{"history", 2, errors.New("db unavailable"), "history"},
		{"stored", 3, errors.New("db unavailable"), "stored"},
	} {
		steps := verifySteps(
			zonestatsRows(verifyRow(7, [5]int64{1, 1, 1, 1, 1}, [5]int64{1, 1, 1, 1, 1})),
			zonestatsRows(aggRow), hist, stored)
		steps[tc.step].err = tc.err
		if _, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps}, VerifyOptions{}); err == nil ||
			!strings.Contains(err.Error(), tc.want) ||
			!strings.Contains(err.Error(), "db unavailable") {
			t.Errorf("%s: %v, want the %q context with the original", tc.name, err, tc.want)
		}
	}
	// An empty history answer — a protocol break.
	steps := verifySteps(
		zonestatsRows(verifyRow(7, [5]int64{1, 1, 1, 1, 1}, [5]int64{1, 1, 1, 1, 1})),
		zonestatsRows(aggRow), zonestatsRows(), stored)
	if _, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps}, VerifyOptions{}); err == nil ||
		!strings.Contains(err.Error(), "empty response") {
		t.Errorf("an empty history = %v, want a protocol break", err)
	}
	// Malformed rows (Scan errors) of each set — with the "reading"
	// context (fresh sets per iteration: the row iterator is one-shot).
	narrow := [][]any{{int16(7)}, {int64(1)}, {int64(9)}, {"name"}}
	for i, row := range narrow {
		steps := verifySteps(
			zonestatsRows(verifyRow(7, [5]int64{1, 1, 1, 1, 1}, [5]int64{1, 1, 1, 1, 1})),
			zonestatsRows(aggRow),
			zonestatsRows([]any{int64(0), int64(0)}),
			zonestatsRows(counterRowOf("render_pending", 0)))
		steps[i].rows = zonestatsRows(row)
		if _, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps}, VerifyOptions{}); err == nil ||
			!strings.Contains(err.Error(), "scan") {
			t.Errorf("step %d: %v, want the scan context", i, err)
		}
	}
	// rows.Err after the zone/aggregate/history/stored rows — with the
	// original one (fresh sets per iteration).
	for i := 0; i < 4; i++ {
		steps := verifySteps(
			zonestatsRows(verifyRow(7, [5]int64{1, 1, 1, 1, 1}, [5]int64{1, 1, 1, 1, 1})),
			zonestatsRows(aggRow),
			zonestatsRows([]any{int64(0), int64(0)}),
			zonestatsRows(counterRowOf("render_pending", 0)))
		steps[i].rows = &zonestatsFakeRows{rows: steps[i].rows.rows, err: errors.New("drop")}
		_, err := VerifyCounters(context.Background(), &zonestatsFakePool{steps: steps}, VerifyOptions{})
		if err == nil || !strings.Contains(err.Error(), "drop") {
			t.Errorf("the rows.Err of step %d = %v, want the original", i, err)
		}
		// The rows are closed even on an iteration error.
		if !steps[i].rows.closed && i != 0 {
			t.Errorf("step %d: the rows are not closed", i)
		}
	}
}

// --- reconciliation readers: zero pins of every error branch -------------------

// TestZoneStatsReaderErrorPins — each reader on each error returns zero
// values (a pin), the errors — with the original reason.
func TestZoneStatsReaderErrorPins(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")

	// readHistoryAgg: an empty answer, Scan, rows.Err — zeros.
	p := &zonestatsFakePool{steps: []zonestatsStep{{rows: zonestatsRows()}}}
	tx, _ := p.Begin(ctx)
	if a, l, err := readHistoryAgg(ctx, tx); err == nil || a != 0 || l != 0 ||
		!strings.Contains(err.Error(), "empty response") {
		t.Errorf("history/empty: (%v/%v, %v)", a, l, err)
	}
	p = &zonestatsFakePool{steps: []zonestatsStep{{rows: &zonestatsFakeRows{
		rows: [][]any{{int64(1)}}, scanErr: boom}}}}
	tx, _ = p.Begin(ctx)
	if a, l, err := readHistoryAgg(ctx, tx); err == nil || a != 0 || l != 0 ||
		!strings.Contains(err.Error(), "scan") {
		t.Errorf("history/Scan: (%v/%v, %v)", a, l, err)
	}
	p = &zonestatsFakePool{steps: []zonestatsStep{{rows: &zonestatsFakeRows{
		rows: [][]any{{int64(1), int64(2)}}, err: boom}}}}
	tx, _ = p.Begin(ctx)
	if a, l, err := readHistoryAgg(ctx, tx); err == nil || a != 0 || l != 0 {
		t.Errorf("history/rows.Err: (%v/%v, %v)", a, l, err)
	}

	// readDailyValue: rowsErr before the row, empty-ok (zero/false), Scan,
	// rowsErr after the row.
	for _, tc := range []struct {
		rows  *zonestatsFakeRows
		wantV int64
		wantE bool
	}{
		{&zonestatsFakeRows{err: boom}, 0, true},
		{zonestatsRows(), 0, false},
		{&zonestatsFakeRows{rows: [][]any{{int64(7)}}, scanErr: boom}, 0, true},
		{&zonestatsFakeRows{rows: [][]any{{int64(7)}}, err: boom}, 0, true},
	} {
		p := &zonestatsFakePool{steps: []zonestatsStep{{rows: tc.rows}}}
		tx, _ := p.Begin(ctx)
		v, exists, err := readDailyValue(ctx, tx, readDailyChecksSQL, "the daily checks row")
		if tc.wantE && (err == nil || v != 0 || exists) {
			t.Errorf("readDailyValue %+v: (%v/%v, %v), want 0/false and an error", tc.rows, v, exists, err)
		}
		if !tc.wantE && (err != nil || v != 0 || exists) {
			t.Errorf("readDailyValue empty: (%v/%v, %v), want 0/false without an error", v, exists, err)
		}
	}

	// writeDailyCounter: an empty upsert answer, Scan, rows.Err — zero.
	dailyRows := []*zonestatsFakeRows{
		zonestatsRows(),
		{rows: [][]any{{int64(1), int64(2)}}, scanErr: boom},
		{rows: [][]any{{int64(1)}}, err: boom},
	}
	for _, rows := range dailyRows {
		p := &zonestatsFakePool{steps: []zonestatsStep{{rows: rows}}}
		tx, _ := p.Begin(ctx)
		if v, err := writeDailyCounter(ctx, tx, recountDailyChecksSQL, 5, time.Unix(0, 0), "the daily checks row"); err == nil || v != 0 {
			t.Errorf("writeDailyCounter %+v: (%v, %v), want 0 and an error", rows, v, err)
		}
	}

	// cleanupBaskets: Scan and rows.Err — errors with context.
	cleanupRows := []*zonestatsFakeRows{
		{rows: [][]any{{int64(1), int64(2)}}, scanErr: boom},
		{rows: [][]any{{int64(1)}}, err: boom},
	}
	for _, rows := range cleanupRows {
		p := &zonestatsFakePool{steps: []zonestatsStep{{rows: rows}}}
		tx, _ := p.Begin(ctx)
		if err := cleanupBaskets(ctx, tx, retryMatureCleanupSQL, "the retry basket cleanup"); err == nil ||
			!strings.Contains(err.Error(), "the retry basket cleanup") {
			t.Errorf("cleanupBaskets %+v: %v, want the context", rows, err)
		}
	}

	// readZonePositions/readCounterValues/readZoneAgg/readLaravelAgg:
	// a Query error and rows.Err — nil values with the original reason.
	p = &zonestatsFakePool{steps: []zonestatsStep{{err: boom}}}
	tx, _ = p.Begin(ctx)
	if m, err := readZonePositions(ctx, tx, nil); err == nil || m != nil {
		t.Errorf("zone read/Query: (%v, %v), want nil and an error", m, err)
	}
	p = &zonestatsFakePool{steps: []zonestatsStep{{err: boom}}}
	tx, _ = p.Begin(ctx)
	if m, err := readCounterValues(ctx, tx, CounterNames()); err == nil || m != nil {
		t.Errorf("counter read/Query: (%v, %v), want nil and an error", m, err)
	}
	p = &zonestatsFakePool{steps: []zonestatsStep{{err: boom}}}
	tx, _ = p.Begin(ctx)
	if m, err := readZoneAgg(ctx, tx); err == nil || m != nil {
		t.Errorf("aggregate/Query: (%v, %v), want nil and an error", m, err)
	}
	p = &zonestatsFakePool{steps: []zonestatsStep{{err: boom}}}
	tx, _ = p.Begin(ctx)
	if m, err := readLaravelAgg(ctx, tx); err == nil || m != nil {
		t.Errorf("laravel aggregate/Query: (%v, %v), want nil and an error", m, err)
	}
	// rows.Err of each — an error.
	rowErr := func(rows [][]any) *zonestatsFakeRows {
		return &zonestatsFakeRows{rows: rows, err: boom}
	}
	zRow := rowErr([][]any{zoneValsOf(7, 1, 1, 1, 1, 1)})
	p = &zonestatsFakePool{steps: []zonestatsStep{{rows: zRow}}}
	tx, _ = p.Begin(ctx)
	if m, err := readZonePositions(ctx, tx, nil); err == nil || m != nil {
		t.Errorf("zone read/rows.Err: (%v, %v)", m, err)
	}
	cRow := rowErr([][]any{counterRowOf("status_alive", 1)})
	p = &zonestatsFakePool{steps: []zonestatsStep{{rows: cRow}}}
	tx, _ = p.Begin(ctx)
	if m, err := readCounterValues(ctx, tx, CounterNames()); err == nil || m != nil {
		t.Errorf("counter read/rows.Err: (%v, %v)", m, err)
	}
	statuses := make([]int64, len(statusAggCols))
	aggRow := aggRowOf(7, 10, 4, 2, 3, statuses, 0)
	aRow := rowErr([][]any{aggRow})
	p = &zonestatsFakePool{steps: []zonestatsStep{{rows: aRow}}}
	tx, _ = p.Begin(ctx)
	if m, err := readZoneAgg(ctx, tx); err == nil || m != nil {
		t.Errorf("aggregate/rows.Err: (%v, %v)", m, err)
	}
	lRow := rowErr([][]any{{int16(7), int64(1)}})
	p = &zonestatsFakePool{steps: []zonestatsStep{{rows: lRow}}}
	tx, _ = p.Begin(ctx)
	if m, err := readLaravelAgg(ctx, tx); err == nil || m != nil {
		t.Errorf("laravel aggregate/rows.Err: (%v, %v)", m, err)
	}
}

// --- load: exact values and missed boundaries --------------------------------

// TestReadMemInfoForeignLines — foreign meminfo lines (without a colon,
// an empty value) are skipped, they do not break the parsing; the values
// are distinguishable by the base (19kB ≠ base-9).
func TestReadMemInfoForeignLines(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "meminfo")
	content := "a foreign line without a colon\n" +
		"MemTotal: 19 kB\n" +
		"EmptyValue:\n" +
		"MemAvailable: 7 kB\n"
	if err := writeFileForTest(p, content); err != nil {
		t.Fatal(err)
	}
	tt, a, st, sf, err := readMemInfo(p)
	if err != nil || tt != 19*1024 || a != 7*1024 || st != 0 || sf != 0 {
		t.Errorf("foreign lines: (%v/%v/%v/%v, %v), want 19456/7168/0/0",
			tt, a, st, sf, err)
	}
	// A 2^63 KiB value in a foreign field — valid for uint64
	// (a bitSize 63 mutant must accept it).
	if err := writeFileForTest(p, "MemTotal: 19 kB\nMemAvailable: 7 kB\nHuge: 9223372036854775808 kB\n"); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := readMemInfo(p); err != nil {
		t.Errorf("a foreign field 2^63: %v, want valid (uint64)", err)
	}
	// No file — an error with zeros (a pin).
	tt, a, st, sf, err = readMemInfo(filepath.Join(dir, "missing"))
	if err == nil || tt != 0 || a != 0 || st != 0 || sf != 0 {
		t.Errorf("no file: (%v/%v/%v/%v, %v), want zeros and an error", tt, a, st, sf, err)
	}
}

// writeFileForTest — a fixture file (an os.WriteFile wrapper).
func writeFileForTest(p, content string) error {
	return os.WriteFile(p, []byte(content), 0o644)
}

// TestReadLoadAvgMissingFilePins — a missing file: zeros (a pin of the
// values on a read error).
func TestReadLoadAvgMissingFilePins(t *testing.T) {
	l1, l5, l15, err := readLoadAvg(filepath.Join(t.TempDir(), "missing"))
	if err == nil || l1 != 0 || l5 != 0 || l15 != 0 {
		t.Errorf("no file: (%v/%v/%v, %v), want zeros and an error", l1, l5, l15, err)
	}
}

// TestReadProcsMissingRoot — a missing root: an error with the procs
// context (not an empty snapshot).
func TestReadProcsMissingRoot(t *testing.T) {
	if m, err := readProcs(filepath.Join(t.TempDir(), "missing")); err == nil ||
		!strings.Contains(err.Error(), "processes") {
		t.Errorf("no root: (%v, %v), want the \"processes\" error", m, err)
	}
}

// TestReadCPUStatMissingFileContext — a missing file: an error with the
// "/proc/stat" context (not the "no cpu line" error).
func TestReadCPUStatMissingFileContext(t *testing.T) {
	if _, err := readCPUStat(filepath.Join(t.TempDir(), "missing")); err == nil ||
		!strings.Contains(err.Error(), "/proc/stat") {
		t.Errorf("no file: %v, want the /proc/stat context", err)
	}
}

// TestParseProcStatLineLeadingParen — a name at the very start of the
// line (open == 0) — a valid row, not an empty contribution.
func TestParseProcStatLineLeadingParen(t *testing.T) {
	line := "(weird) R 1 2 3 4 5 6 7 8 9 10 3 4"
	if comm, ticks := parseProcStatLine([]byte(line)); comm != "weird" || ticks != 7 {
		t.Errorf("open==0: (%q, %d), want weird and 7", comm, ticks)
	}
	// open < 0 → false: lines without an opening paren — no contribution.
	if comm, _ := parseProcStatLine([]byte("7 no paren R 1 2 3 4 5 6 7 8 9 10 3 4")); comm != "" {
		t.Errorf("without a parenthesis = %q, want an empty contribution", comm)
	}
}

// --- slowqueries: unit boundaries and the full numeral range ------------------

// TestSlowQueriesPluralFullRange — the numeral agreement over the range
// 0..3000 (n%99/n%101 mutants are distinguishable at n > 1300).
func TestSlowQueriesPluralFullRange(t *testing.T) {
	plural := func(n int) string {
		switch {
		case n%10 == 1 && n%100 != 11:
			return "slow query"
		case n%10 >= 2 && n%10 <= 4 && (n%100 < 10 || n%100 >= 20):
			return "slow queries"
		}
		return "slow queries"
	}
	for n := 0; n <= 3000; n++ {
		if got := slowQueriesPhrase(n); got != plural(n) {
			t.Fatalf("slowQueriesPhrase(%d) = %q, want %q", n, got, plural(n))
		}
	}
}

// TestSlowQueriesUnitBoundaries — unit values of the working parameters
// are not replaced by defaults (the "≤ 0" boundaries).
func TestSlowQueriesUnitBoundaries(t *testing.T) {
	if got := (SlowQueryOptions{MinDuration: 1}).withDefaults().MinDuration; got != 1 {
		t.Errorf("MinDuration=1: %v, want as set", got)
	}
	pool := &zonestatsFakePool{steps: []zonestatsStep{{rows: zonestatsRows()}}}
	if _, err := (PGSlowQueries{Pool: pool, Limit: 1}).SlowQueries(context.Background(), time.Second); err != nil {
		t.Fatal(err)
	}
	if got := pool.calls()[0].args[2]; got != 1 {
		t.Errorf("Limit=1: %v, want 1", got)
	}
}

// TestSlowQueryLoopStartupAndMetrics — the starting pass runs right away
// (before the first tick) and writes metrics; a 1ns interval is a
// working value.
func TestSlowQueryLoopStartupAndMetrics(t *testing.T) {
	reg := metrics.New("monitor")
	t0 := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	q := slowQueryAt(11, "scheduler", 42.3, t0, "SELECT 1")
	src := &fakeSlowSource{passes: [][]SlowQuery{{q}}}
	var reports []string
	opts := SlowQueryOptions{
		Interval: time.Hour, // the tick never comes: only the starting pass
		Metrics:  reg,
		Notify:   func(text string) { reports = append(reports, text) },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- SlowQueryLoop(ctx, src, opts) }()
	waitSlowPasses(t, src, 1, cancel, done)
	if len(reports) != 1 {
		t.Fatalf("reports = %d, want 1 (the starting pass before a tick)", len(reports))
	}
	// The header ends with a newline (the query lines start on a new line).
	if !strings.Contains(reports[0], "(threshold 3s)\n") {
		t.Errorf("the header without a newline:\n%q", reports[0])
	}
	// The starting pass metrics: the gauge and the counter.
	if v, ok := gaugeLabels(t, reg, "crawler_pg_slow_queries", nil); !ok || v != 1 {
		t.Errorf("crawler_pg_slow_queries = (%v, %v), want 1", v, ok)
	}
	if v, ok := slowCounterValue(t, reg, "crawler_pg_slow_queries_total", map[string]string{"app": "scheduler"}); !ok || v != 1 {
		t.Errorf("total{app=scheduler} = (%v, %v), want 1", v, ok)
	}

	// A 1ns interval is working (not "disabled"): the loop lives and
	// repeats the passes, the starting pass report arrives at
	// Interval=1ns too.
	src2 := &fakeSlowSource{passes: [][]SlowQuery{{q}}}
	var reports2 []string
	opts2 := SlowQueryOptions{
		Interval: 1,
		Notify:   func(text string) { reports2 = append(reports2, text) },
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- SlowQueryLoop(ctx2, src2, opts2) }()
	waitSlowPasses(t, src2, 1, cancel2, done2)
	if len(reports2) != 1 {
		t.Errorf("Interval=1ns: reports = %d, want 1 (not disabled)", len(reports2))
	}
}

// TestPGSlowQueriesRows — the snapshot rows are read into the result (not swallowed).
func TestPGSlowQueriesRows(t *testing.T) {
	pool := &zonestatsFakePool{steps: []zonestatsStep{
		{rows: zonestatsRows(slowScanRow(11, "scheduler", 42.3, "SELECT 1"),
			slowScanRow(12, "", 7.8, ""))}}}
	out, err := (PGSlowQueries{Pool: pool}).SlowQueries(context.Background(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || out[0].PID != 11 || out[1].App != "" || out[1].Query != "" {
		t.Errorf("the snapshot = %+v, want both rows as is", out)
	}
}

// --- psi/redis/renderdebt/unitevents: the original error text ----------------------

// TestOSPSISourceReadErrorText — a directory instead of a file: the error
// carries the original read text (it does not turn into a PSI format error).
func TestOSPSISourceReadErrorText(t *testing.T) {
	dir := t.TempDir()
	_, err := (OSPSI{Path: dir}).PSI(context.Background())
	if err == nil || !strings.Contains(err.Error(), "PSI: ") ||
		strings.Contains(err.Error(), "line is missing") {
		t.Errorf("a directory: %v, want the original read error (not the PSI format)", err)
	}
}

// TestParsePSIMissingFieldKeepsError — a missing field is a format error
// (not a quiet zero snapshot).
func TestParsePSIMissingFieldKeepsError(t *testing.T) {
	if st, err := parsePSI("some avg60=1 avg300=1 total=0\n"); err == nil || st != (PSIStat{}) {
		t.Errorf("no avg10: (%+v, %v), want zeros and an error", st, err)
	}
	if st, err := parsePSI("some avg10=1 total=0\n"); err == nil || st != (PSIStat{}) {
		t.Errorf("no avg60: (%+v, %v), want zeros and an error", st, err)
	}
}

// TestInfoSecondsMissingField — a missing INFO field is an error (not a
// silent zero).
func TestInfoSecondsMissingField(t *testing.T) {
	if v, err := infoSeconds(map[string]string{"other": "1"}, "uptime_in_seconds"); err == nil || v != 0 {
		t.Errorf("no field: (%v, %v), want 0 and an error", v, err)
	}
}

// TestInfoBytesMissingFieldAndBitSize — a missing field and the bitSize
// boundary (2^62 is valid for int64/64, rejected at 63).
func TestInfoBytesMissingFieldAndBitSize(t *testing.T) {
	if v, err := infoBytes(map[string]string{}, "maxmemory"); err == nil || v != 0 {
		t.Errorf("no field: (%v, %v), want 0 and an error", v, err)
	}
	if v, err := infoBytes(map[string]string{"k": "4611686018427387904"}, "k"); err != nil || v != 4611686018427387904 {
		t.Errorf("2^62: (%v, %v), want as is", v, err)
	}
	if v, err := infoSeconds(map[string]string{"k": "4611686018427387904"}, "k"); err != nil || v != 4611686018427387904 {
		t.Errorf("2^62 s: (%v, %v), want as is", v, err)
	}
}

// TestRedisUptimeSourceError — an INFO read error — with the original text.
func TestRedisUptimeSourceError(t *testing.T) {
	if _, err := redisUptimeOf(context.Background(), errInfoSource{}); err == nil ||
		!strings.Contains(err.Error(), "connection refused") {
		t.Errorf("an INFO error: %v, want the original", err)
	}
}

// errInfoSource — an INFO source with a preset error.
type errInfoSource struct{}

func (errInfoSource) Info(context.Context) (string, error) {
	return "", errors.New("connection refused")
}

// redisUptimeOf — an uptime reading wrapper (assembled from INFO).
func redisUptimeOf(ctx context.Context, src interface {
	Info(context.Context) (string, error)
}) (RedisUptime, error) {
	raw, err := src.Info(ctx)
	if err != nil {
		return RedisUptime{}, err
	}
	fields := parseRedisInfo(raw)
	secs, err := infoSeconds(fields, "uptime_in_seconds")
	if err != nil {
		return RedisUptime{}, err
	}
	return RedisUptime{Seconds: secs}, nil
}

// parseRedisInfo — parsing INFO into a field map (a local copy for the test).
func parseRedisInfo(raw string) map[string]string {
	fields := map[string]string{}
	for _, line := range strings.Split(raw, "\n") {
		if k, v, ok := strings.Cut(line, ":"); ok {
			fields[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return fields
}

// TestRenderDebtOfErrors — source errors with the original text: the pool
// not set, Redis not set, a counter error.
func TestRenderDebtOfErrors(t *testing.T) {
	if _, err := renderDebtOf(context.Background(), nil, nil); err == nil ||
		!strings.Contains(err.Error(), "pool is not set") {
		t.Errorf("the pool: %v, want \"the DB pool is not set\"", err)
	}
	pool := &zonestatsFakePool{steps: []zonestatsStep{{err: errors.New("db unavailable")}}}
	if _, err := renderDebtOf(context.Background(), pool, &renderLensSource{}); err == nil ||
		!strings.Contains(err.Error(), "db unavailable") {
		t.Errorf("the counter: %v, want the original", err)
	}
}

// TestRenderDebtLoopUnitIntervalAndGauge — a 1ns interval is a working
// value (the starting pass runs and writes the debt gauge; the loop
// lives until cancellation).
func TestRenderDebtLoopUnitIntervalAndGauge(t *testing.T) {
	src := &fakeDebtSource{debt: RenderDebt{Flags: 3, Lag: 1}}
	reg := metrics.New("monitor")
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		done <- RenderDebtLoop(ctx, src, RenderDebtOptions{Interval: 1, Metrics: reg})
	}()
	// The starting pass must complete before cancellation: we wait for the gauge.
	deadline := time.Now().Add(2 * time.Second)
	var v float64
	var ok bool
	for time.Now().Before(deadline) {
		if v, ok = gaugeLabels(t, reg, "crawler_render_pending_domains", nil); ok {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("the loop: %v", err)
	}
	if !ok || v != 3 {
		t.Errorf("the render debt gauge = (%v, %v), want 3 (the starting pass at Interval=1ns)", v, ok)
	}
}

// --- unitevents: boundaries ------------------------------------------------------------

// TestParseSystemctlShowNRestartsBoundaries — an NRestarts before the
// first Id is skipped (it does not break the parsing), values in base 10.
func TestParseSystemctlShowNRestartsBoundaries(t *testing.T) {
	out := "NRestarts=99\n" +
		"Id=a.service\nNRestarts=123\n" +
		"NRestarts=77\n" +
		"Id=b.service\nNRestarts=9223372036854775808\n"
	units := parseSystemctlShow(out)
	if len(units) != 2 {
		t.Fatalf("units = %d, want 2 (a line before Id does not break the parse)", len(units))
	}
	if units[0].NRestarts != 77 {
		t.Errorf("NRestarts = %d, want 77 (the last line of the block)", units[0].NRestarts)
	}
	if units[1].NRestarts != 9223372036854775808 {
		t.Errorf("NRestarts 2^63 = %d, want as is (uint64)", units[1].NRestarts)
	}
}

// TestParseMemoryEventsForeignLine — a line without a value does not
// break the parsing (continue), the keys after it are read.
func TestParseMemoryEventsForeignLine(t *testing.T) {
	ev, err := parseMemoryEvents("low\noom 4\n")
	if err != nil || ev.OOM != 4 {
		t.Errorf("a line without a value: (%+v, %v), want oom=4", ev, err)
	}
}

// TestOSUnitEventsCancelledCtx — a cancelled ctx: a context error with
// the original one (not an empty snapshot).
func TestOSUnitEventsCancelledCtx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if units, err := (OSUnitEvents{}).UnitEvents(ctx); err == nil || units != nil {
		t.Errorf("a ctx cancellation: (%v, %v), want nil and an error", units, err)
	}
}

// --- streamgroups: several streams ------------------------------------------------

// TestStreamGroupsSkipEmptyThenRead — an empty stream is skipped, the
// one after it is read (continue, not break).
func TestStreamGroupsSkipEmptyThenRead(t *testing.T) {
	group := redis.XInfoGroup{Name: "g", Lag: 3, Pending: 1, EntriesRead: 10}
	rdb := &fakeGroupsRedis{
		groups: map[string][]redis.XInfoGroup{
			alertStreams[0]: {},      // empty — skipped
			alertStreams[1]: {group}, // read after the empty one
			queue.StreamDNS: {},
		},
		xlen: map[string]int64{alertStreams[1]: 5},
	}
	st, err := (RedisStreamGroups{RDB: rdb}).StreamGroups(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(st) != 1 || st[0].Stream != alertStreams[1] {
		t.Errorf("the snapshot = %+v, want only the second stream's groups", st)
	}
}

// --- alertstate: missing boundaries --------------------------------------------------

// TestNewDoneStaleInvalidSnapshots — invalid snapshot stamps: the first
// pass increments the series (no base), a repeat of the same invalid
// stamp is a new snapshot (an invalid stamp does not match the previous
// valid one).
func TestNewDoneStaleInvalidSnapshots(t *testing.T) {
	invalid := staleRow{value: 5} // snap is invalid
	// The first pass — the series grows even without a valid stamp.
	tr := &newDoneStaleTrack{minPasses: 2}
	if s := tr.decide("", time.Time{}, invalid); !strings.Contains(s.detail, "1/2") {
		t.Errorf("the first invalid snapshot: %s, want the streak 1/2", s.detail)
	}
	// A repeat of the invalid one is growth too: an invalid stamp is not
	// counted as matching the previous one (!Valid).
	if s := tr.decide("", time.Time{}, invalid); !strings.Contains(s.detail, "2/2") {
		t.Errorf("an invalid repeat: %s, want the streak 2/2 (an invalid stamp ≠ any)", s.detail)
	}
	// A valid stamp equal to the zero time: the first pass — growth
	// (no base — the series is incremented).
	zero := staleRow{value: 5, snap: sql.NullTime{Time: time.Time{}, Valid: true}}
	tr2 := &newDoneStaleTrack{minPasses: 2}
	if s := tr2.decide("", time.Time{}, zero); !strings.Contains(s.detail, "1/2") {
		t.Errorf("a valid zero-time snapshot: %s, want the streak 1/2", s.detail)
	}
}

// TestCounterGrowthFirstPassBase — a counter's first pass is the base
// (series 0), not growth.
func TestCounterGrowthFirstPassBase(t *testing.T) {
	tr := &counterGrowthTrack{minPasses: 2}
	s := tr.decide("", time.Time{}, 100)
	if !strings.Contains(s.detail, "0/2") {
		t.Errorf("the counter base: %s, want the streak 0/2", s.detail)
	}
	s = tr.decide("", time.Time{}, 150)
	if !strings.Contains(s.detail, "1/2") {
		t.Errorf("counter growth: %s, want the streak 1/2", s.detail)
	}
}

// TestRenderDebtTrackThresholdExact — a debt exactly at the threshold
// with a growth series — NOT a firing (strict >); a debt below the
// threshold with a series — ok.
func TestRenderDebtTrackThresholdExact(t *testing.T) {
	d := func(total int64) RenderDebt { return RenderDebt{Flags: total} }
	// Exactly the threshold: 5 → 10 (series 2), total == domains → ok.
	tr := &renderDebtTrack{domains: 10, passes: 2}
	_ = tr.decide("", time.Time{}, d(5))
	_ = tr.decide("", time.Time{}, d(8))
	if s := tr.decide("", time.Time{}, d(10)); s.status != AlertStatusOK {
		t.Errorf("the debt exactly at the threshold: %s, want ok (a strict >)", s.status)
	}
	// Below the threshold with a full series — ok (a "true" mutant would fire).
	tr2 := &renderDebtTrack{domains: 10, passes: 2}
	_ = tr2.decide("", time.Time{}, d(6))
	_ = tr2.decide("", time.Time{}, d(7))
	if s := tr2.decide("", time.Time{}, d(8)); s.status != AlertStatusOK {
		t.Errorf("the debt below the threshold with a streak: %s, want ok", s.status)
	}
	// The ceil boundary of the clearing threshold: the threshold 9 →
	// clearing < 9 (ceil(8.1)); the level 8 clears, 9 holds.
	tr3 := &renderDebtTrack{domains: 9, passes: 1}
	_ = tr3.decide("", time.Time{}, d(2))  // the base
	_ = tr3.decide("", time.Time{}, d(15)) // growth → firing
	if s := tr3.decide("", time.Time{}, d(9)); s.status != AlertStatusFiring {
		t.Errorf("a hold at 9 (the threshold 9): %s, want firing", s.status)
	}
	if s := tr3.decide("", time.Time{}, d(8)); s.status != AlertStatusOK {
		t.Errorf("clearing at 8 (the clearing threshold <9): %s, want ok (ceil)", s.status)
	}
}

// TestRenderFbDepthBelowWatermarkAfterDrop — the depth below the
// watermark with a full growth series — ok (a "true" mutant would fire).
func TestRenderFbDepthBelowWatermarkAfterDrop(t *testing.T) {
	tr := &renderFbDepthTrack{wm: 100, passes: 2}
	_ = tr.decide("", time.Time{}, 105) // the base
	_ = tr.decide("", time.Time{}, 90)  // a drop (a loss)
	_ = tr.decide("", time.Time{}, 91)  // growth 1
	if s := tr.decide("", time.Time{}, 92); s.status != AlertStatusOK {
		t.Errorf("below the threshold with a streak: %s (%s), want ok", s.status, s.detail)
	}
}

// TestQueueFreezeMovingCounter — the ok detail names the moving-groups
// counter: a group with a moving ID is counted.
func TestQueueFreezeMovingCounter(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	m := env.newQueueFreezeMachine()
	moving := []StreamGroupState{{Stream: "q:dns", Group: "g", Depth: 5, LastDeliveredID: "1-0"}}
	s := m.transition(env.Alerts, context.Background(), obs(moving))
	if !strings.Contains(s.Detail, "1 of 1") {
		t.Errorf("a moving group: %q, want \"1 of 1\"", s.Detail)
	}
	// Several groups: an empty one before a frozen one does not break the
	// parsing — the frozen one behind the empty one is seen.
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	env2 := newAlertsTestEnv(t, func(o *AlertsOptions) {
		o.QueueFreeze = 15 * time.Minute
		o.Now = func() time.Time { return now }
	})
	m2 := env2.newQueueFreezeMachine()
	frozen := []StreamGroupState{
		{Stream: "q:dns", Group: "empty", Depth: 0, LastDeliveredID: "0-0"},
		{Stream: "q:http", Group: "worker", Depth: 40, LastDeliveredID: "5-1"},
	}
	_ = m2.transition(env2.Alerts, context.Background(), obs(frozen))
	now = now.Add(16 * time.Minute)
	s2 := m2.transition(env2.Alerts, context.Background(), obs(frozen))
	if s2.Status != AlertStatusFiring || !strings.Contains(s2.Detail, "q:http/worker") {
		t.Errorf("frozen after the empty one: %s (%s), want firing with q:http/worker", s2.Status, s2.Detail)
	}
}

// --- load: an exact report with sensitive values -------------------------------

// TestLoadReportLoopMetricsAndDebt — the second pass: the cpu instruments
// are written (there is a delta), the render debt line from the working
// source is in the report.
func TestLoadReportLoopMetricsAndDebt(t *testing.T) {
	reg := metrics.New("monitor")
	st := LoadStat{
		Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{User: 200_000, Total: 1_000_000, Cores: 4},
		MemTotal: 8 << 30, MemAvailable: 4 << 30,
		Procs: map[string]ProcAgg{"a": {Ticks: 5000}},
	}
	st2 := LoadStat{
		Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{User: 300_000, Total: 1_040_000, Cores: 4},
		MemTotal: 8 << 30, MemAvailable: 4 << 30,
		Procs: map[string]ProcAgg{"a": {Ticks: 14_000}},
	}
	two := &fakeLoadSource{states: []LoadStat{st, st2}}
	fq := &switchQueuesSource{passes: []map[string]QueueStat{
		{queue.StreamDNS: {Len: 1, Read: 5000}},
		{queue.StreamDNS: {Len: 1, Read: 5100}},
	}}
	debt := &fakeDebtSource{debt: RenderDebt{Flags: 7, Lag: 3}}
	reports := make(chan string, 8)
	opts := LoadReportOptions{
		Interval:   20 * time.Millisecond,
		Metrics:    reg,
		Queues:     fq,
		RenderDebt: debt,
		Notify:     func(text string) { reports <- text },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- LoadReportLoop(ctx, two, opts) }()
	<-reports // the base
	var r string
	select {
	case r = <-reports:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("the second report did not arrive")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("the loop: %v", err)
	}
	// The second pass CPU instruments: a delta of 40000 (user 100000,
	// idle 0) — busy 100%, user 250%.
	if v, ok := gaugeLabels(t, reg, "crawler_os_cpu_percent", map[string]string{"kind": "busy"}); !ok || v != 100 {
		t.Errorf("cpu{kind=busy} = (%v, %v), want 100", v, ok)
	}
	if v, ok := gaugeLabels(t, reg, "crawler_os_cpu_percent", map[string]string{"kind": "user"}); !ok || v != 250 {
		t.Errorf("cpu{kind=user} = (%v, %v), want 250", v, ok)
	}
	if !strings.Contains(r, "📬 Render debt: flags 7 + lag 3 = 10") {
		t.Errorf("the report without the debt line:\n%s", r)
	}
}

// TestReadMemInfoZeroTotal — MemTotal: 0 — an error (an empty value of a
// required field, a weakening mutant would accept it).
func TestReadMemInfoZeroTotal(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "meminfo")
	if err := os.WriteFile(p, []byte("MemTotal: 0 kB\nMemAvailable: 1 kB\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := readMemInfo(p); err == nil {
		t.Error("MemTotal 0: want an error (the value of a required field)")
	}
}

// TestRedisQueuesLenNoSuchKeyComplete — a textual "no such key": the
// stream is present in the snapshot ({0 0}), the map is complete.
func TestRedisQueuesLenNoSuchKeyComplete(t *testing.T) {
	rdb := &fakeGroupsRedis{
		groups:   map[string][]redis.XInfoGroup{},
		xlen:     map[string]int64{},
		lensErrs: map[string]error{queue.StreamDNS: redis.Nil},
	}
	q, err := (RedisQueues{RDB: rdb}).Queues(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(q) != len(reportStreams) {
		t.Errorf("streams = %d, want %d (a missing one — a zero contribution)", len(q), len(reportStreams))
	}
}

// TestLoadReportLoopNanosecondInterval — a 1ns interval is a working
// value (not "disabled"): the starting pass runs.
func TestLoadReportLoopNanosecondInterval(t *testing.T) {
	st := LoadStat{Load1: 1, Load5: 1, Load15: 1, CPU: CPUStat{Cores: 1},
		MemTotal: 1, MemAvailable: 1}
	src := &fakeLoadSource{states: []LoadStat{st}}
	// The counter is incremented from the loop goroutine (Notify) and
	// polled here before the stop — atomically only (race-detector).
	var reports atomic.Int64
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		done <- LoadReportLoop(ctx, src, LoadReportOptions{
			Interval: 1,
			Notify:   func(string) { reports.Add(1) },
		})
	}()
	deadline := time.Now().Add(2 * time.Second)
	for reports.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("the loop: %v", err)
	}
	if reports.Load() < 1 {
		t.Error("Interval=1ns: the starting report was not built (the loop was taken as disabled)")
	}
}

// TestZoneStatsLoopMetricsNilReceiver — nil loop metric receivers are
// no-ops without a panic.
func TestZoneStatsLoopMetricsNilReceiver(t *testing.T) {
	var m *zoneStatsLoopMetrics
	m.observe(0)
	m.skip()
	m.setNewDoneStale(0)
}

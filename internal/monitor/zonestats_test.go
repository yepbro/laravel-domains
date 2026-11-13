// Unit tests of the combined reconciliation of the pipeline counters
// (T-178, the incremental-counters spec
// §7) on a fake pgx pool with transactions: the exact SQL texts and
// parameters
// of the pass statements (reading old values → aggregates → overwriting
// zones with an unnest batch → the counter upsert), the drift (old/new by
// positions), the step errors and the ctx cancellation, the
// ZoneStatsLoop loop (an immediate
// first pass without drift noise, a Warn+gauge on drift, single-flight
// T-161, a clean exit), the VerifyCounters cross-check. The behavior
// against a live
// DB — the integration tests (test/integration/counters_test.go).
package monitor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/model"
)

// --- a fake pool with transactions -----------------------------------------

// zonestatsCall — a recorded SQL call with arguments.
type zonestatsCall struct {
	sql  string
	args []any
}

// zonestatsFakeRows — pgx.Rows with preset rows; the Scan is
// positional, in the SQL column order.
type zonestatsFakeRows struct {
	rows    [][]any
	i       int
	closed  bool
	err     error
	scanErr error
}

func zonestatsRows(rows ...[]any) *zonestatsFakeRows {
	return &zonestatsFakeRows{rows: rows}
}

func (r *zonestatsFakeRows) Close() { r.closed = true }

func (r *zonestatsFakeRows) Err() error { return r.err }

func (r *zonestatsFakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *zonestatsFakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *zonestatsFakeRows) Next() bool {
	if r.closed || r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *zonestatsFakeRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if r.closed || r.i == 0 || r.i > len(r.rows) {
		return errors.New("zonestatsFakeRows: Scan without Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("zonestatsFakeRows: %d destinations, %d columns", len(dest), len(row))
	}
	for i, d := range dest {
		target := reflect.ValueOf(d).Elem()
		if row[i] == nil {
			target.Set(reflect.Zero(target.Type()))
			continue
		}
		target.Set(reflect.ValueOf(row[i]))
	}
	return nil
}

func (r *zonestatsFakeRows) Values() ([]any, error) { return r.rows[r.i-1], nil }

func (r *zonestatsFakeRows) RawValues() [][]byte { return nil }

// Conn and TypeMap — the part of pgx.Rows not used by the positional Scan.
func (r *zonestatsFakeRows) Conn() *pgx.Conn      { return nil }
func (r *zonestatsFakeRows) TypeMap() *pgtype.Map { return nil }

// zonestatsStep — a preset result of one pass Query.
type zonestatsStep struct {
	rows *zonestatsFakeRows
	err  error
}

// zonestatsFakePool — ZoneStatsDB without a DB: the queue of Query
// results (in
// the order of the pass statements) and errors, the call protocol, the
// transaction counters. Everything is under a lock: the loop runs in a
// goroutine, the test waits for
// progress by the counters. delay — the pause of each Query (a slow pass
// for the T-161 single-flight test).
type zonestatsFakePool struct {
	mu        sync.Mutex
	steps     []zonestatsStep
	commitErr error
	beginErr  error
	delay     time.Duration

	queries   []zonestatsCall
	begins    int
	commits   int
	rollbacks int
}

func (p *zonestatsFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom must not be called")
}

func (p *zonestatsFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("Exec must not be called")
}

func (p *zonestatsFakePool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	if d := p.delaySnapshot(); d > 0 {
		time.Sleep(d)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, zonestatsCall{sql: sql, args: args})
	if len(p.steps) == 0 {
		return zonestatsRows(), nil
	}
	step := p.steps[0]
	p.steps = p.steps[1:]
	if step.err != nil {
		return nil, step.err
	}
	if step.rows == nil {
		return zonestatsRows(), nil
	}
	return step.rows, nil
}

func (p *zonestatsFakePool) Begin(context.Context) (pgx.Tx, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.begins++
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	return &zonestatsFakeTx{pool: p}, nil
}

// stats — a snapshot of the query count under the lock (waiting for the loop's progress).
func (p *zonestatsFakePool) stats() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queries)
}

// calls — a snapshot of the call protocol under the lock.
func (p *zonestatsFakePool) calls() []zonestatsCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]zonestatsCall(nil), p.queries...)
}

// txCounts — the transaction counters.
func (p *zonestatsFakePool) txCounts() (begins, commits, rollbacks int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.begins, p.commits, p.rollbacks
}

func (p *zonestatsFakePool) delaySnapshot() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.delay
}

func (p *zonestatsFakePool) setDelay(d time.Duration) {
	p.mu.Lock()
	p.delay = d
	p.mu.Unlock()
}

// zonestatsFakeTx — a transaction over the fake pool (Query/Exec — into
// the pool protocol; Commit/Rollback — counters). The pass does not use
// the other pgx.Tx methods.
type zonestatsFakeTx struct {
	pool *zonestatsFakePool
	pgx.Tx
}

func (t *zonestatsFakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.pool.Query(ctx, sql, args...)
}

func (t *zonestatsFakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return t.pool.Exec(ctx, sql, args...)
}

func (t *zonestatsFakeTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.pool.commits++
	return t.pool.commitErr
}

func (t *zonestatsFakeTx) Rollback(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.pool.rollbacks++
	return nil
}

// --- pass presets ------------------------------------------------------
//
// The steps of one pass (11 statements): the old zones, the old counters,
// the old daily checks and temp_error rows (T-179/T-182), the per-zone
// aggregate (16 columns: zone, 4 states, 8 statuses, the bit, the daily
// window, the temp numerator), the laravel aggregate, the history, the
// zone overwrite
// (RETURNING of the new values), the counter upsert (RETURNING of the new
// values), the upsert of both daily rows (in the checks: →
// temp_error: order — shared with the record.go increments).

// aggRowOf — a row of the per-zone aggregate: zone, total, pending, queued,
// checked, then the status legend in the statusAggCols order, then the bit,
// then the daily checks window, its temp numerator (T-179/T-182),
// the retry maturity baskets (T-202: today's/matured episodes
// of temp_error × done) and the "status hung" domains (FR-2.2: new ∧ done ∧
// older than a day; the variadic arguments default to zeros so that
// the base passes of the tests do not have to state them explicitly:
// [checks] →
// [checks, temp] → [checks, temp, retryToday, retryPast,
// newDoneStale]).
func aggRowOf(zone int16, total, pending, queued, checked int64, statuses []int64, render int64, daily ...int64) []any {
	if len(statuses) != len(statusAggCols) {
		panic("aggRowOf: the status legend is incomplete")
	}
	if len(daily) > 5 {
		panic("aggRowOf: five daily columns (T-179/T-182/T-202/FR-2.2)")
	}
	var today, temp, retryToday, retryPast, newDoneStale int64
	if len(daily) > 0 {
		today = daily[0]
	}
	if len(daily) > 1 {
		temp = daily[1]
	}
	if len(daily) > 2 {
		retryToday = daily[2]
	}
	if len(daily) > 3 {
		retryPast = daily[3]
	}
	if len(daily) > 4 {
		newDoneStale = daily[4]
	}
	row := []any{zone, total, pending, queued, checked}
	for _, s := range statuses {
		row = append(row, s)
	}
	return append(row, render, today, temp, retryToday, retryPast, newDoneStale)
}

// zoneValsOf — a row of readZonesSQL/recountZonesSQL RETURNING.
func zoneValsOf(id int16, total, pending, queued, checked, laravel int64) []any {
	return []any{id, total, pending, queued, checked, laravel}
}

// counterRowOf — a row of readCountersSQL/recountCountersSQL RETURNING.
func counterRowOf(name string, v int64) []any { return []any{name, v} }

// stepsOfPass — the queue of the results of one full pass. The steps:
// the old zones, the old counters, the old daily checks and
// temp_error rows (T-179/T-182), the old retry maturity baskets (T-202;
// empty sets by default — no rows, the pass creates them: a fill-in
// without drift), the old hourly tempo baskets (T-203, the same block),
// the old reanalysis tail baskets (T-204, the same block), the per-zone
// aggregate, the laravel aggregate, the history, the hourly basket
// aggregate (the two-live-hours window — zero tempo and tails), the zone
// overwrite (RETURNING of the new
// values = zoneNew), the counter upsert (RETURNING of the new values),
// the upsert of the daily rows, the cleanup of the stale retry baskets and
// the upsert of both of its baskets (the values 0 — tests with a non-zero
// window mutate the step, as they mutate the aggregate), the cleanup and
// the upserts of the reanalysis tail baskets
// (prev/cur, zeros), the cleanups and upserts of the hourly baskets
// (rendered prev/cur,
// then results prev/cur — all zeros).
func stepsOfPass(zoneOld, zoneNew, agg []any, laravel *zonestatsFakeRows, oldCounters, newCounters *zonestatsFakeRows) []zonestatsStep {
	return []zonestatsStep{
		{rows: zonestatsRows(zoneOld)}, // 0: old zones
		{rows: oldCounters},            // 1: old counters
		{},                             // 2: the old checks row
		{},                             // 3: the old temp row
		{},                             // 4: the old mature basket
		{},                             // 5: the old today basket
		{}, {}, {}, {},                 // 6–9: the old hourly tempo baskets
		{}, {}, // 10–11: the old reanalysis tail baskets
		{rows: zonestatsRows(agg)},                       // 12: the per-zone aggregate
		{rows: laravel},                                  // 13: the laravel aggregate
		{rows: zonestatsRows([]any{int64(0), int64(0)})}, // 14: the history
		{rows: zonestatsRows([]any{int64(0), int64(0), int64(0), int64(0), int64(0), int64(0)})}, // 15: the hourly baskets (a zero window)
		{rows: zonestatsRows(zoneNew)},         // 16: the zone overwrite
		{rows: newCounters},                    // 17: the counter upsert
		{rows: zonestatsRows([]any{int64(0)})}, // 18: the checks row
		{rows: zonestatsRows([]any{int64(0)})}, // 19: the temp row
		{},                                     // 20: the retry basket cleanup
		{rows: zonestatsRows([]any{int64(0)})}, // 21: the mature basket
		{rows: zonestatsRows([]any{int64(0)})}, // 22: the today basket
		{},                                     // 23: the tail basket cleanup
		{rows: zonestatsRows([]any{int64(0)})}, // 24: the tails prev
		{rows: zonestatsRows([]any{int64(0)})}, // 25: the tails cur
		{},                                     // 26: the rendered basket cleanup
		{rows: zonestatsRows([]any{int64(0)})}, // 27: rendered prev
		{rows: zonestatsRows([]any{int64(0)})}, // 28: rendered cur
		{},                                     // 29: the results basket cleanup
		{rows: zonestatsRows([]any{int64(0)})}, // 30: results prev
		{rows: zonestatsRows([]any{int64(0)})}, // 31: results cur
	}
}

// fullCounterRows — the full answer of the counter upsert/read: the
// CounterNames legend with the values from the map (absence — 0).
func fullCounterRows(values map[string]int64) *zonestatsFakeRows {
	rows := make([][]any, 0, len(CounterNames()))
	for _, name := range CounterNames() {
		rows = append(rows, counterRowOf(name, values[name]))
	}
	return zonestatsRows(rows...)
}

// zonestatsFixedClock — a clock with a fixed "now".
func zonestatsFixedClock() (time.Time, func() time.Time) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	return now, func() time.Time { return now }
}

// --- ZoneStatsOnce ---------------------------------------------------------

// TestZoneStatsOnceSQL — the sequence of the pass statements in one
// transaction with the exact texts: reading the old zones, the counters,
// both daily rows, both maturity baskets, the four hourly tempo baskets
// and
// the two reanalysis tail baskets → the aggregate (the only domains
// scan)
// → laravel → history → the hourly basket aggregate → the zone
// overwrite
// (the locks first) → the counter upsert → the upsert of the daily rows
// (checks:, then temp_error:) → the cleanup of the stale retry baskets →
// the upsert of the retry baskets (the mature one, then today's) → the
// reanalysis tail baskets
// (cleanup → prev → cur) → the hourly tempo baskets
// (rendered: cleanup → prev → cur, then results: in the same order) →
// Commit.
func TestZoneStatsOnceSQL(t *testing.T) {
	now, clock := zonestatsFixedClock()
	statuses := make([]int64, len(statusAggCols))
	statuses[4] = 5 // alive
	statuses[5] = 1 // temp_error
	pool := &zonestatsFakePool{steps: stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 2, 8, 1, 3, 2, 1),
		zonestatsRows([]any{int16(7), int64(1)}),
		fullCounterRows(map[string]int64{"status_alive": 5, "render_pending": 2}),
		fullCounterRows(map[string]int64{"status_alive": 5, "render_pending": 2}),
	)}
	// The daily rows (T-179/T-182): the old checks 7 / temp_error 2
	// (the increments outran the live window by one temp_error retry), the
	// new ones are
	// the sums of the aggregate's FILTER columns (8 and 1). The retry
	// baskets (T-202):
	// the old mature 1 / today's 4 (the drain decrements and the events
	// of the day), the new ones are the sums of the basket FILTER columns
	// (2 and 3). The hourly
	// tempo baskets (T-203): the old rendered prev 5 / cur 2, results
	// prev 11 / cur 6 (the event increments), the new ones are the FILTER
	// positions
	// of the window aggregate (prev 4/9, cur 2/5 — the events beyond the
	// live rows).
	// The reanalysis tail baskets (T-204): the old prev 3 / cur 1, the new
	// ones are the
	// NULL rows of the window (prev 2 / cur 1).
	pool.steps[2].rows = zonestatsRows([]any{int64(7)})
	pool.steps[3].rows = zonestatsRows([]any{int64(2)})
	pool.steps[4].rows = zonestatsRows([]any{int64(1)})
	pool.steps[5].rows = zonestatsRows([]any{int64(4)})
	pool.steps[6].rows = zonestatsRows([]any{int64(5)})
	pool.steps[7].rows = zonestatsRows([]any{int64(2)})
	pool.steps[8].rows = zonestatsRows([]any{int64(11)})
	pool.steps[9].rows = zonestatsRows([]any{int64(6)})
	pool.steps[10].rows = zonestatsRows([]any{int64(3)})
	pool.steps[11].rows = zonestatsRows([]any{int64(1)})
	pool.steps[15].rows = zonestatsRows([]any{int64(9), int64(4), int64(5), int64(2), int64(2), int64(1)})
	pool.steps[18].rows = zonestatsRows([]any{int64(8)})
	pool.steps[19].rows = zonestatsRows([]any{int64(1)})
	pool.steps[21].rows = zonestatsRows([]any{int64(2)})
	pool.steps[22].rows = zonestatsRows([]any{int64(3)})
	pool.steps[24].rows = zonestatsRows([]any{int64(2)})
	pool.steps[25].rows = zonestatsRows([]any{int64(1)})
	pool.steps[27].rows = zonestatsRows([]any{int64(4)})
	pool.steps[28].rows = zonestatsRows([]any{int64(2)})
	pool.steps[30].rows = zonestatsRows([]any{int64(9)})
	pool.steps[31].rows = zonestatsRows([]any{int64(5)})

	res, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{Now: clock})
	if err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}

	calls := pool.calls()
	if len(calls) != 32 {
		t.Fatalf("queries = %d, want 32 (reads ×12, aggregates ×4, the zone overwrite, the counters upsert, daily ×2, retry baskets ×3, tails ×3, hourly ×6)", len(calls))
	}
	wantSQL := []string{
		readZonesSQL, readCountersSQL, readDailyChecksSQL, readDailyTempSQL, readRetryPastSQL, readRetryTodaySQL,
		readHourlyRenderedPrevSQL, readHourlyRenderedCurSQL, readHourlyResultsPrevSQL, readHourlyResultsCurSQL,
		readHourlyReanalyzePrevSQL, readHourlyReanalyzeCurSQL,
		recountAggQuerySQL, laravelAggQuerySQL, historyAggSQL, hourlyAggSQL,
		recountZonesSQL, recountCountersSQL, recountDailyChecksSQL, recountDailyTempSQL,
		retryMatureCleanupSQL, recountRetryPastSQL, recountRetryTodaySQL,
		hourlyReanalyzeCleanupSQL, recountHourlyReanalyzePrevSQL, recountHourlyReanalyzeCurSQL,
		hourlyRenderedCleanupSQL, recountHourlyRenderedPrevSQL, recountHourlyRenderedCurSQL,
		hourlyResultsCleanupSQL, recountHourlyResultsPrevSQL, recountHourlyResultsCurSQL,
	}
	for i, want := range wantSQL {
		if calls[i].sql != want {
			t.Errorf("query %d:\n%s\nwant:\n%s", i, calls[i].sql, want)
		}
	}
	// The counter legend is complete, in a deterministic order.
	if !reflect.DeepEqual(calls[1].args, []any{CounterNames()}) {
		t.Errorf("the counter read args = %v, want %v", calls[1].args, CounterNames())
	}
	// The zone overwrite: the unnest batch — all the parameters in the SQL
	// order:
	// $1 ids, $2 total, $3 pending, $4 queued, $5 checked, $6 the moment,
	// $7 laravel — from the laravel aggregate (step 13), not from the zone
	// RETURNING
	// (a mixed-up columns test must catch it, review R2).
	wantZone := []any{[]int16{7}, []int64{10}, []int64{4}, []int64{2}, []int64{3}, now, []int64{1}}
	if !reflect.DeepEqual(calls[16].args, wantZone) {
		t.Errorf("the zone unnest batch = %#v\nwant %#v", calls[16].args, wantZone)
	}
	// The upsert: the full legend names + the values + the moment.
	if got := calls[17].args[0]; !reflect.DeepEqual(got, CounterNames()) {
		t.Errorf("upsert names = %v, want the CounterNames legend", got)
	}
	if calls[17].args[2] != any(now) {
		t.Errorf("updated_at upsert = %#v, want %v", calls[17].args[2], now)
	}
	// The upsert values match the aggregate: the legend in order.
	wantVals := calls[17].args[1].([]int64)
	if wantVals[len(statusAggCols)] != 2 { // render_pending after the statuses
		t.Errorf("render_pending in the upsert = %d, want 2", wantVals[len(statusAggCols)])
	}
	// The "status hung" domains (FR-2.2): the absolute level at the tail
	// of the legend (after the laravel cumulatives) — the sum of the
	// f_new_done_stale FILTER column of the aggregate.
	if wantVals[len(statusAggCols)+3] != 1 {
		t.Errorf("new_done_stale in the upsert = %d, want 1", wantVals[len(statusAggCols)+3])
	}
	// The daily rows (T-179/T-182): the values are the sums of the
	// aggregate's FILTER columns, the moment is the pass clock; the day
	// names are computed by the DB — current_date in the statement
	// texts, no name parameter; the write order
	// "checks: → temp_error:" is shared with the record.go increments.
	if !reflect.DeepEqual(calls[18].args, []any{int64(8), any(now)}) {
		t.Errorf("the checks row upsert args = %#v, want the value 8 + the moment", calls[18].args)
	}
	if !reflect.DeepEqual(calls[19].args, []any{int64(1), any(now)}) {
		t.Errorf("the temp row upsert args = %#v, want the value 1 + the moment", calls[19].args)
	}
	if !strings.Contains(recountDailyChecksSQL, "'checks:' || to_char(current_date, 'YYYY-MM-DD')") {
		t.Errorf("recountDailyChecksSQL without the day name from the DB:\n%s", recountDailyChecksSQL)
	}
	if !strings.Contains(recountDailyTempSQL, "'temp_error:' || to_char(current_date, 'YYYY-MM-DD')") {
		t.Errorf("recountDailyTempSQL without the day name from the DB:\n%s", recountDailyTempSQL)
	}
	// The retry maturity baskets (T-202): the cleanup — no parameters (the
	// name range is computed by the DB), the upserts — the value + the
	// moment; the basket names are
	// current_date − 1 / current_date in the statement texts; the order
	// "cleanup → mature → today's" is shared with the delta writers.
	if len(calls[20].args) != 0 {
		t.Errorf("the basket cleanup args = %#v, want no parameters", calls[20].args)
	}
	if !reflect.DeepEqual(calls[21].args, []any{int64(2), any(now)}) {
		t.Errorf("the mature basket upsert args = %#v, want the value 2 + the moment", calls[21].args)
	}
	if !reflect.DeepEqual(calls[22].args, []any{int64(3), any(now)}) {
		t.Errorf("today's basket upsert args = %#v, want the value 3 + the moment", calls[22].args)
	}
	if !strings.Contains(recountRetryPastSQL, "'retry_mature:' || to_char(current_date - 1, 'YYYY-MM-DD')") {
		t.Errorf("recountRetryPastSQL without the basket name from the DB:\n%s", recountRetryPastSQL)
	}
	if !strings.Contains(recountRetryTodaySQL, "'retry_mature:' || to_char(current_date, 'YYYY-MM-DD')") {
		t.Errorf("recountRetryTodaySQL without the basket name from the DB:\n%s", recountRetryTodaySQL)
	}
	if !strings.Contains(retryMatureCleanupSQL, "< 'retry_mature:' || to_char(current_date - 1, 'YYYY-MM-DD')") {
		t.Errorf("retryMatureCleanupSQL without the cleanup boundary from the DB:\n%s", retryMatureCleanupSQL)
	}
	// The reanalysis tail baskets (T-204): the cleanup — no parameters,
	// the bound from the DB; the upserts — a FILTER position + the moment;
	// the order
	// "cleanup → prev → cur" between the retry baskets and the rendered
	// tempo ones —
	// shared with the writers (record.go/renderworker/analyzer).
	if len(calls[23].args) != 0 {
		t.Errorf("the tail basket cleanup args = %#v, want no parameters", calls[23].args)
	}
	if !reflect.DeepEqual(calls[24].args, []any{int64(2), any(now)}) {
		t.Errorf("the tail prev upsert args = %#v, want the value 2 + the moment", calls[24].args)
	}
	if !reflect.DeepEqual(calls[25].args, []any{int64(1), any(now)}) {
		t.Errorf("the tail cur upsert args = %#v, want the value 1 + the moment", calls[25].args)
	}
	for _, s := range []struct {
		sql  string
		want string
	}{
		{recountHourlyReanalyzePrevSQL, "'reanalyze:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24')"},
		{recountHourlyReanalyzeCurSQL, "'reanalyze:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24')"},
	} {
		if !strings.Contains(s.sql, s.want) {
			t.Errorf("a tail basket upsert without the hour name from the DB:\n%s", s.sql)
		}
	}
	if !strings.Contains(hourlyReanalyzeCleanupSQL, "< 'reanalyze:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24')") {
		t.Errorf("hourlyReanalyzeCleanupSQL without the cleanup boundary from the DB:\n%s", hourlyReanalyzeCleanupSQL)
	}
	if !strings.Contains(hourlyAggSQL, "prev_reanalyze") || !strings.Contains(hourlyAggSQL, "rules_version IS NULL") {
		t.Errorf("hourlyAggSQL without the re-analysis tail FILTER positions:\n%s", hourlyAggSQL)
	}
	// The hourly tempo baskets (T-203): the cleanups — no parameters, the
	// name bounds from the DB; the upserts — a FILTER position + the
	// moment; the order
	// "rendered: (cleanup → prev → cur) → results: (the same)" — shared
	// with
	// the writers (record.go: results: last; renderworker: rendered:).
	for _, i := range []int{26, 29} {
		if len(calls[i].args) != 0 {
			t.Errorf("the hourly basket cleanup args (step %d) = %#v, want no parameters", i, calls[i].args)
		}
	}
	if !reflect.DeepEqual(calls[27].args, []any{int64(4), any(now)}) {
		t.Errorf("the rendered prev upsert args = %#v, want the value 4 + the moment", calls[27].args)
	}
	if !reflect.DeepEqual(calls[28].args, []any{int64(2), any(now)}) {
		t.Errorf("the rendered cur upsert args = %#v, want the value 2 + the moment", calls[28].args)
	}
	if !reflect.DeepEqual(calls[30].args, []any{int64(9), any(now)}) {
		t.Errorf("the results prev upsert args = %#v, want the value 9 + the moment", calls[30].args)
	}
	if !reflect.DeepEqual(calls[31].args, []any{int64(5), any(now)}) {
		t.Errorf("the results cur upsert args = %#v, want the value 5 + the moment", calls[31].args)
	}
	for _, s := range []struct {
		sql  string
		want string
	}{
		{recountHourlyRenderedPrevSQL, "'rendered:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24')"},
		{recountHourlyRenderedCurSQL, "'rendered:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24')"},
		{recountHourlyResultsPrevSQL, "'results:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24')"},
		{recountHourlyResultsCurSQL, "'results:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24')"},
	} {
		if !strings.Contains(s.sql, s.want) {
			t.Errorf("an hourly basket upsert without the hour name from the DB:\n%s", s.sql)
		}
	}
	if !strings.Contains(hourlyRenderedCleanupSQL, "< 'rendered:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24')") {
		t.Errorf("hourlyRenderedCleanupSQL without the cleanup boundary from the DB:\n%s", hourlyRenderedCleanupSQL)
	}
	if !strings.Contains(hourlyResultsCleanupSQL, "< 'results:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24')") {
		t.Errorf("hourlyResultsCleanupSQL without the cleanup boundary from the DB:\n%s", hourlyResultsCleanupSQL)
	}

	begins, commits, _ := pool.txCounts()
	if begins != 1 || commits != 1 {
		t.Errorf("transactions: begin=%d commit=%d, want 1/1", begins, commits)
	}
	if res.Zones != 1 {
		t.Errorf("Zones = %d, want 1", res.Zones)
	}
	// The "status hung" domains (FR-2.2): the result field is the sum
	// of the aggregate's FILTER column (the value of the loop's gauge).
	if res.NewDoneStale != 1 {
		t.Errorf("NewDoneStale = %d, want 1", res.NewDoneStale)
	}
	// The drift positions: 5 zone columns + the delta-driven globals +
	// the daily rows (T-179/T-182) + the retry baskets (T-202) + the
	// hourly tempo baskets (T-203) and the reanalysis tail ones (T-204).
	wantPositions := 5 + len(driftCounterNames()) + len(dailyStreamingPositions()) + len(retryMatureStreamingPositions()) + len(hourlyStreamingPositions())
	if len(res.Positions) != wantPositions {
		t.Fatalf("positions = %d, want %d", len(res.Positions), wantPositions)
	}
	byName := make(map[string]CounterPosition, len(res.Positions))
	for _, p := range res.Positions {
		byName[p.Counter] = p
	}
	if p := byName["zone:7:pending"]; p.Old != 4 || p.New != 4 || p.Drift() != 0 {
		t.Errorf("zone:7:pending = %+v, want old=new=4", p)
	}
	if _, ok := byName["global:status_alive"]; ok {
		t.Error("status_* in the drift positions: only the reconciliation writes the status distribution")
	}
	if p := byName["global:render_pending"]; p.Old != 2 || p.New != 2 {
		t.Errorf("global:render_pending = %+v, want old=new=2", p)
	}
	// A missing counter row — an old value of 0 (the first pass fills it).
	if p := byName["global:laravel_appeared_total"]; p.Old != 0 || p.New != 0 {
		t.Errorf("global:laravel_appeared_total = %+v, want old=new=0 (no row)", p)
	}
	// The daily positions: the drift |7 − 8| = 1 — the record events
	// beyond the live
	// window (a day repeat, TZ_slow_done_today §5.1); the temp numerator
	// |2 − 1| — a retry closed the error, the event stayed in the history
	// (TZ_slow_dns_share §5.1).
	if p := byName["daily:checks"]; p.Old != 7 || p.New != 8 || p.Drift() != 1 {
		t.Errorf("daily:checks = %+v, want old=7 new=8 drift=1", p)
	}
	if p := byName["daily:temp_error"]; p.Old != 2 || p.New != 1 || p.Drift() != 1 {
		t.Errorf("daily:temp_error = %+v, want old=2 new=1 drift=1", p)
	}
	// The retry baskets (T-202): the drift is the streaming difference of
	// events and domains
	// (the drain drift |1 − 2|, the events of the day |4 − 3|), the
	// positions
	// are observable, they are not in the Warn.
	if p := byName["retry_mature:past"]; p.Old != 1 || p.New != 2 || p.Drift() != 1 {
		t.Errorf("retry_mature:past = %+v, want old=1 new=2 drift=1", p)
	}
	if p := byName["retry_mature:today"]; p.Old != 4 || p.New != 3 || p.Drift() != 1 {
		t.Errorf("retry_mature:today = %+v, want old=4 new=3 drift=1", p)
	}
	// The reanalysis tail baskets (T-204): the drift is the streaming
	// difference
	// of the transition events and the live NULL rows (prev |3 − 2|), cur
	// without
	// a mismatch; the positions are observable, they are not in the Warn.
	if p := byName["reanalyze:prev"]; p.Old != 3 || p.New != 2 || p.Drift() != 1 {
		t.Errorf("reanalyze:prev = %+v, want old=3 new=2 drift=1", p)
	}
	if p := byName["reanalyze:cur"]; p.Old != 1 || p.New != 1 || p.Drift() != 0 {
		t.Errorf("reanalyze:cur = %+v, want old=new=1", p)
	}
	// The hourly tempo baskets (T-203): the drift is the streaming
	// difference of the
	// insertion/render events and the live window rows; the basket did not
	// exist — a fill-in
	// (here all exist — the old values are set above).
	if p := byName["rendered:prev"]; p.Old != 5 || p.New != 4 || p.Drift() != 1 {
		t.Errorf("rendered:prev = %+v, want old=5 new=4 drift=1", p)
	}
	if p := byName["rendered:cur"]; p.Old != 2 || p.New != 2 || p.Drift() != 0 {
		t.Errorf("rendered:cur = %+v, want old=new=2", p)
	}
	if p := byName["results:prev"]; p.Old != 11 || p.New != 9 || p.Drift() != 2 {
		t.Errorf("results:prev = %+v, want old=11 new=9 drift=2", p)
	}
	if p := byName["results:cur"]; p.Old != 6 || p.New != 5 || p.Drift() != 1 {
		t.Errorf("results:cur = %+v, want old=6 new=5 drift=1", p)
	}
}

// TestZoneStatsOnceDrift — the drift: old ≠ new for a zone column and
// a global counter; Drift() is |Δ|; a zone new in the aggregate without
// an old row — an old value of 0.
func TestZoneStatsOnceDrift(t *testing.T) {
	statuses := make([]int64, len(statusAggCols))
	// Zone 9 in the old read with zeros — a "new zone" (the counters were
	// zeroed by a restart on the migration).
	pool := &zonestatsFakePool{steps: stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 6, 2, 3, 1),
		aggRowOf(7, 10, 6, 2, 3, statuses, 4),
		zonestatsRows(),
		fullCounterRows(map[string]int64{"render_pending": 2}),
		fullCounterRows(map[string]int64{"render_pending": 4}),
	)}
	pool.steps[0].rows = zonestatsRows(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(9, 0, 0, 0, 0, 0))
	pool.steps[12].rows = zonestatsRows(
		aggRowOf(7, 10, 6, 2, 3, statuses, 4),
		aggRowOf(9, 2, 2, 0, 0, make([]int64, len(statusAggCols)), 0))
	pool.steps[16].rows = zonestatsRows(
		zoneValsOf(7, 10, 6, 2, 3, 1),
		zoneValsOf(9, 2, 2, 0, 0, 0))

	res, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}
	if res.Zones != 2 {
		t.Fatalf("Zones = %d, want 2", res.Zones)
	}
	byName := make(map[string]CounterPosition, len(res.Positions))
	for _, p := range res.Positions {
		byName[p.Counter] = p
	}
	if p := byName["zone:7:pending"]; p.Old != 4 || p.New != 6 || p.Drift() != 2 {
		t.Errorf("zone:7:pending = %+v, want old=4 new=6 drift=2", p)
	}
	// Zone 9 came from the zeros (a restart on the migration) — an old
	// value of 0.
	if p := byName["zone:9:total"]; p.Old != 0 || p.New != 2 || p.Drift() != 2 {
		t.Errorf("zone:9:total = %+v, want old=0 new=2", p)
	}
	if p := byName["global:render_pending"]; p.Old != 2 || p.New != 4 || p.Drift() != 2 {
		t.Errorf("global:render_pending = %+v, want old=2 new=4", p)
	}
	drifted := 0
	for _, p := range res.Positions {
		if p.Drift() > 0 {
			drifted++
		}
	}
	if drifted != 4 { // the pending of zone 7, the total+pending of zone 9, render_pending
		t.Errorf("positions with drift = %d, want 4", drifted)
	}
}

// TestZoneStatsOnceNewDoneStale — the "status hung" domains (FR-2.2,
// T-256):
// the NewDoneStale field is the sum of the f_new_done_stale FILTER
// column over the zones,
// the new_done_stale row is in the unnest-upsert of the global scalars
// (an absolute
// like status_*, it is not a drift position: written by one
// reconciliation).
func TestZoneStatsOnceNewDoneStale(t *testing.T) {
	statuses := make([]int64, len(statusAggCols))
	pool := &zonestatsFakePool{steps: stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 0, 0, 0, 0, 0, 2),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil),
	)}
	pool.steps[0].rows = zonestatsRows(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(9, 2, 2, 0, 0, 0))
	pool.steps[12].rows = zonestatsRows(
		aggRowOf(7, 10, 4, 2, 3, statuses, 0, 0, 0, 0, 0, 2),
		aggRowOf(9, 2, 2, 0, 0, make([]int64, len(statusAggCols)), 0, 0, 0, 0, 0, 5))
	pool.steps[16].rows = zonestatsRows(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(9, 2, 2, 0, 0, 0))

	res, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}
	if res.NewDoneStale != 7 {
		t.Errorf("NewDoneStale = %d, want 7 (the sum 2 + 5 over the zones)", res.NewDoneStale)
	}
	// The new_done_stale row is the last in the unnest-upsert legend with
	// the sum value (the CounterNames order: the statuses,
	// render_pending,
	// the cumulatives).
	calls := pool.calls()
	vals := calls[17].args[1].([]int64)
	if got := vals[len(statusAggCols)+3]; got != 7 {
		t.Errorf("new_done_stale in the upsert = %d, want 7", got)
	}
	// The level is not a drift position (written by one
	// reconciliation, like status_*).
	for _, p := range res.Positions {
		if strings.Contains(p.Counter, "new_done_stale") {
			t.Errorf("new_done_stale in the drift positions: %+v", p)
		}
	}
}

// TestZoneStatsOnceDailyChecks — the daily rows of the pass (T-179/T-182):
// the day row does not exist yet — the pass creates it with the window
// value, the fill-in
// is not counted as drift (Old := New; the "0 against in-flight
// DNS writes" mismatch is unrelated to the increments); an existing row
// with
// a mismatch is drift, the daily:checks/daily:temp_error positions. The
// temp_error numerator
// additionally diverges from the window by the retries that closed
// the error (the event stays in the history, TZ_slow_dns_share §5.1) —
// the window 1
// against the increments 2.
func TestZoneStatsOnceDailyChecks(t *testing.T) {
	statuses := make([]int64, len(statusAggCols))
	statuses[4] = 3 // alive
	statuses[5] = 1 // temp_error

	dailyOf := func(res ZoneStatsResult, name string) CounterPosition {
		t.Helper()
		for _, p := range res.Positions {
			if p.Counter == name {
				return p
			}
		}
		t.Fatalf("the position %s is missing from the pass totals", name)
		return CounterPosition{}
	}

	// No day rows, the live window 3 / the temp window 1 (the first pass
	// after
	// midnight): the creation is not drift.
	create := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 0, 3, 1),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))
	// stepsOfPass sows empty old daily rows (steps 2–3) —
	// we keep them; the new values of the steps 18–19 are the windows 3
	// and 1.
	create[18].rows = zonestatsRows([]any{int64(3)})
	create[19].rows = zonestatsRows([]any{int64(1)})
	res, err := ZoneStatsOnce(context.Background(), &zonestatsFakePool{steps: create}, ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}
	if p := dailyOf(res, "daily:checks"); p.New != 3 || p.Drift() != 0 {
		t.Errorf("a day row creation: daily:checks = %+v, want new=3 drift=0 (a fill-in)", p)
	}
	if p := dailyOf(res, "daily:temp_error"); p.New != 1 || p.Drift() != 0 {
		t.Errorf("a temp row creation: daily:temp_error = %+v, want new=1 drift=0 (a fill-in)", p)
	}

	// The rows exist (the increments outran the window by two repeats;
	// temp_error
	// was closed by a retry — the event stayed): the drifts 2 and 1.
	drift := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 0, 3, 1),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))
	drift[2].rows = zonestatsRows([]any{int64(5)})
	drift[3].rows = zonestatsRows([]any{int64(2)})
	drift[18].rows = zonestatsRows([]any{int64(3)})
	drift[19].rows = zonestatsRows([]any{int64(1)})
	res, err = ZoneStatsOnce(context.Background(), &zonestatsFakePool{steps: drift}, ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}
	if p := dailyOf(res, "daily:checks"); p.Old != 5 || p.New != 3 || p.Drift() != 2 {
		t.Errorf("a day row drift: daily:checks = %+v, want old=5 new=3 drift=2", p)
	}
	if p := dailyOf(res, "daily:temp_error"); p.Old != 2 || p.New != 1 || p.Drift() != 1 {
		t.Errorf("a temp row drift: daily:temp_error = %+v, want old=2 new=1 drift=1", p)
	}
}

// TestZoneStatsOnceZoneFilter — the zone filter (recount --zone): the
// read
// of the old zones is limited, the overwrite is only of the chosen
// zone.
func TestZoneStatsOnceZoneFilter(t *testing.T) {
	zone := int16(7)
	statuses := make([]int64, len(statusAggCols))
	pool := &zonestatsFakePool{steps: stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 2),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil),
	)}

	if _, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{Zone: &zone}); err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}
	calls := pool.calls()
	if got, ok := calls[0].args[0].(*int16); !ok || got == nil || *got != zone {
		t.Errorf("the old-zone read zone filter = %#v, want *int16(7)", calls[0].args[0])
	}
	if got := calls[16].args[0]; !reflect.DeepEqual(got, []int16{7}) {
		t.Errorf("the overwrite zone unnest = %#v, want [7] (the zone overwrite — step 16)", got)
	}
}

// TestZoneStatsOnceEmpty — no zones: empty reads, a zero result,
// the transaction runs (the counter rows are created by the pass's
// upsert).
func TestZoneStatsOnceEmpty(t *testing.T) {
	// The zone batch is empty — the overwrite statement is not run (the
	// no-op skip in writeZonePositions), the steps: reads ×12, the
	// aggregates ×4,
	// the counter upsert, the dailies ×2, the retry baskets ×3, the tails
	// ×3,
	// the hourly ×6.
	pool := &zonestatsFakePool{steps: []zonestatsStep{
		{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {},
		{rows: zonestatsRows()}, {rows: zonestatsRows()},
		{rows: zonestatsRows([]any{int64(0), int64(0)})},
		{rows: zonestatsRows([]any{int64(0), int64(0), int64(0), int64(0), int64(0), int64(0)})},
		{rows: fullCounterRows(nil)},
		{rows: zonestatsRows([]any{int64(0)})},
		{rows: zonestatsRows([]any{int64(0)})},
		{},
		{rows: zonestatsRows([]any{int64(0)})},
		{rows: zonestatsRows([]any{int64(0)})},
		{},
		{rows: zonestatsRows([]any{int64(0)})},
		{rows: zonestatsRows([]any{int64(0)})},
		{},
		{rows: zonestatsRows([]any{int64(0)})},
		{rows: zonestatsRows([]any{int64(0)})},
		{},
		{rows: zonestatsRows([]any{int64(0)})},
		{rows: zonestatsRows([]any{int64(0)})},
	}}

	res, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("ZoneStatsOnce: %v", err)
	}
	if res.Zones != 0 {
		t.Errorf("Zones = %d, want 0", res.Zones)
	}
	if n := pool.stats(); n != 31 {
		t.Errorf("queries = %d, want 31 (the zone overwrite skipped)", n)
	}
	wantPositions := len(driftCounterNames()) + len(dailyStreamingPositions()) + len(retryMatureStreamingPositions()) + len(hourlyStreamingPositions())
	if len(res.Positions) != wantPositions {
		t.Errorf("positions = %d, want globals + daily + retry baskets + hourly + tails (%d)", len(res.Positions), wantPositions)
	}
}

// TestZoneStatsOnceErrors — the error of each step is wrapped with the
// task context; the ctx cancellation is preserved by errors.Is; Commit
// can fail too.
func TestZoneStatsOnceErrors(t *testing.T) {
	for i := 0; i < 32; i++ {
		steps := stepsOfPass(
			zoneValsOf(7, 10, 4, 2, 3, 1),
			zoneValsOf(7, 10, 4, 2, 3, 1),
			aggRowOf(7, 10, 4, 2, 3, make([]int64, len(statusAggCols)), 0),
			zonestatsRows(),
			fullCounterRows(nil),
			fullCounterRows(nil))
		steps[i].err = errors.New("db unavailable")
		pool := &zonestatsFakePool{steps: steps}
		_, err := ZoneStatsOnce(context.Background(), pool, ZoneStatsOptions{})
		if err == nil || !strings.Contains(err.Error(), "monitor: counter reconciliation") ||
			!strings.Contains(err.Error(), "db unavailable") {
			t.Errorf("step %d: error = %v, want the task context and the original", i, err)
		}
		if _, _, rollbacks := pool.txCounts(); rollbacks != 1 {
			t.Errorf("step %d: rollbacks = %d, want 1 (a failure before Commit)", i, rollbacks)
		}
	}

	cancelSteps := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, make([]int64, len(statusAggCols)), 0),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))
	cancelSteps[12].err = context.Canceled // the aggregate step — the heavy read
	cancelPool := &zonestatsFakePool{steps: cancelSteps}
	if _, err := ZoneStatsOnce(context.Background(), cancelPool, ZoneStatsOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}

	commitPool := &zonestatsFakePool{
		commitErr: errors.New("fix failed"),
		steps: stepsOfPass(
			zoneValsOf(7, 10, 4, 2, 3, 1),
			zoneValsOf(7, 10, 4, 2, 3, 1),
			aggRowOf(7, 10, 4, 2, 3, make([]int64, len(statusAggCols)), 0),
			zonestatsRows(),
			fullCounterRows(nil),
			fullCounterRows(nil)),
	}
	if _, err := ZoneStatsOnce(context.Background(), commitPool, ZoneStatsOptions{}); err == nil ||
		!strings.Contains(err.Error(), "commit") {
		t.Errorf("error = %v, want a commit failure", err)
	}
}

func TestZoneStatsOnceNilPool(t *testing.T) {
	if _, err := ZoneStatsOnce(context.Background(), nil, ZoneStatsOptions{}); err == nil ||
		!strings.Contains(err.Error(), "pool is not set") {
		t.Errorf("error = %v, want \"the pool is not set\"", err)
	}
}

// --- ZoneStatsOptions ------------------------------------------------------

// TestZoneStatsOptionsDefaults — zero and negative values are the
// default; the set ones are not wiped, Now is passed as is. The default
// is pinned by the 30m literal (T-199: the operator's decision of Oct 26
// by the measurement of the slow window after 3bffc62 — 48% of the slow
// DB time at 5 min;
// the former 5m — T-161, measurement §12).
func TestZoneStatsOptionsDefaults(t *testing.T) {
	if DefaultZoneStatsInterval != 30*time.Minute {
		t.Errorf("DefaultZoneStatsInterval = %v, want 30m (T-199)", DefaultZoneStatsInterval)
	}
	got := (ZoneStatsOptions{}).withDefaults()
	if got.Interval != DefaultZoneStatsInterval {
		t.Errorf("Interval = %v, want the default %v", got.Interval, DefaultZoneStatsInterval)
	}
	got = (ZoneStatsOptions{Interval: -time.Second}).withDefaults()
	if got.Interval != DefaultZoneStatsInterval {
		t.Errorf("Interval = %v after a negative one, want the default %v", got.Interval, DefaultZoneStatsInterval)
	}
	got = (ZoneStatsOptions{Interval: time.Nanosecond}).withDefaults()
	if got.Interval != time.Nanosecond {
		t.Errorf("Interval = %v after 1ns, want 1ns (only ≤ 0 is replaced)", got.Interval)
	}
	now := func() time.Time { return time.Unix(0, 0) }
	got = ZoneStatsOptions{Interval: 30 * time.Second, Now: now}.withDefaults()
	if got.Interval != 30*time.Second || got.Now == nil || !got.Now().Equal(time.Unix(0, 0)) {
		t.Errorf("withDefaults overwrites the set values: %+v", got)
	}
}

// --- ZoneStatsLoop ---------------------------------------------------------

// driftGaugeValue — the current value of crawler_counters_drift{counter}.
func driftGaugeValue(t *testing.T, reg *metrics.Registry, counter string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "crawler_counters_drift" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "counter" && l.GetValue() == counter {
					return m.GetGauge().GetValue()
				}
			}
		}
	}
	return 0
}

// TestZoneStatsLoopImmediatePass — the first pass runs right at start,
// without waiting for a tick; the drift of the first pass (the old zeros)
// is without
// a Warn and metrics (a fill-in, the incremental-counters spec §7.1.3).
func TestZoneStatsLoopImmediatePass(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	reg := metrics.New("monitor")

	statuses := make([]int64, len(statusAggCols))
	// There is a drift (the old zeros, the new values) — the first pass is
	// silent.
	pool := &zonestatsFakePool{steps: stepsOfPass(
		zoneValsOf(7, 0, 0, 0, 0, 0),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 5, 0, 0, 0, 0, 3),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(map[string]int64{"status_alive": 5}))}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ZoneStatsLoop(ctx, pool, reg, ZoneStatsOptions{
			Interval: time.Hour, // the tick never comes: only the starting pass
			Log:      logger,
		})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for pool.stats() < 32 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("the starting pass was not performed")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	if n := pool.stats(); n != 32 {
		t.Errorf("queries = %d, want 32: without a tick there are no repeated passes", n)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ZoneStatsLoop: %v, want nil on a ctx cancellation", err)
	}
	log := buf.String()
	if strings.Contains(log, "level=WARN") {
		t.Errorf("the first pass produced a Warn (a fill-in drift is not an alarm):\n%s", log)
	}
	if driftGaugeValue(t, reg, "zone:7:total") != 0 {
		t.Errorf("the first pass drift gauge is not zero/missing: the first pass metrics are not raised")
	}
	// The "status hung" gauge (FR-2.2) — a level, not a drift: the first
	// (filling) pass must set the value of the FILTER column.
	if got := historyGaugeValue(t, reg, "crawler_domains_new_done_stale"); got != 3 {
		t.Errorf("the crawler_domains_new_done_stale gauge after the first pass = %v, want 3", got)
	}
}

// zonestatsSyncBuf — a bytes.Buffer under a lock: the loop writes the
// log in its
// goroutine, the test reads it before the loop finishes.
type zonestatsSyncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *zonestatsSyncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *zonestatsSyncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *zonestatsSyncBuf) contains(s string) bool {
	return strings.Contains(b.String(), s)
}

// waitCond — waiting for a condition with a timeout.
func waitCond(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("the condition was not met within %s: %s", timeout, what)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestZoneStatsLoopFailedFirstPass — a failing first pass (the DB not
// ready) does not clear "first" (review R1 P3-5): the next successful
// pass
// fills the zeros without a Warn — otherwise a restart on a raised DB
// would report
// the fill-in drift as an anomaly.
func TestZoneStatsLoopFailedFirstPass(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	reg := metrics.New("monitor")

	statuses := make([]int64, len(statusAggCols))
	fill := stepsOfPass(
		zoneValsOf(7, 0, 0, 0, 0, 0),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 0),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))
	// The first pass: exactly twelve read steps up to the aggregate
	// failure
	// (we do not keep a tail — otherwise the second pass would eat the
	// preset shifted).
	fail := []zonestatsStep{
		{rows: zonestatsRows(zoneValsOf(7, 0, 0, 0, 0, 0))}, // the old zones
		{rows: fullCounterRows(nil)},                        // the old counters
		{},                                                  // the old checks row (T-179)
		{},                                                  // the old temp row (T-182)
		{},                                                  // the old mature basket (T-202)
		{},                                                  // the old today basket (T-202)
		{}, {}, {}, {},                                      // the old hourly tempo baskets (T-203)
		{}, {}, // the old reanalysis tail baskets (T-204)
		{err: errors.New("db unavailable")}, // the aggregate — the failure
	}
	pool := &zonestatsFakePool{steps: append(fail, fill...)}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ZoneStatsLoop(ctx, pool, reg, ZoneStatsOptions{
			Interval: 2 * time.Millisecond,
			Log:      logger,
		})
	}()

	waitCond(t, 2*time.Second, func() bool { return pool.stats() >= 38 }, "a pass after a failed start")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ZoneStatsLoop: %v", err)
	}
	log := buf.String()
	if !strings.Contains(log, "level=WARN") || !strings.Contains(log, "db unavailable") {
		t.Errorf("a failed start without a Warn with the reason:\n%s", log)
	}
	if strings.Contains(log, "reconciliation: counter drift") {
		t.Errorf("the filling pass after the failure is reported as drift:\n%s", log)
	}
	if driftGaugeValue(t, reg, "zone:7:total") != 0 {
		t.Errorf("the drift gauge raised by the filling pass after the failure")
	}
}

// TestZoneStatsLoopTickerDriftWarn — a pass by tick with a non-zero drift
// (not the first): a Warn with the position count and the |Δ| gauge for
// all the positions
// (steady state — 0). The sync is by the Warn itself in the log (not by
// the query
// counter): the third pass must not overwrite the gauge with zeros
// before
// the checks — after the Warn the loop is cancelled and waited for
// (a flake fix, review P3-1).
func TestZoneStatsLoopTickerDriftWarn(t *testing.T) {
	slogBuf := &zonestatsSyncBuf{}
	logger := slog.New(slog.NewTextHandler(slogBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	reg := metrics.New("monitor")

	statuses := make([]int64, len(statusAggCols))
	noDrift := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, statuses, 5),
		zonestatsRows(),
		fullCounterRows(map[string]int64{"render_pending": 5}),
		fullCounterRows(map[string]int64{"render_pending": 5}))
	withDrift := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 5, 2, 3, 1),
		aggRowOf(7, 10, 5, 2, 3, statuses, 6),
		zonestatsRows(),
		fullCounterRows(map[string]int64{"render_pending": 5}),
		fullCounterRows(map[string]int64{"render_pending": 6}))
	// The daily rows of the second pass — a non-zero "drift" (streaming
	// positions, T-179/T-202): the gauge exposes them, but no Warn is
	// raised on them (noise under live load — reportDrift). The retry
	// baskets, the hourly tempo baskets and the tail ones — the same
	// streaming
	// semantics (T-202/T-203/T-204): the old basket values also
	// diverge from the new window — a gauge without a Warn.
	withDrift[2].rows = zonestatsRows([]any{int64(12)})
	withDrift[3].rows = zonestatsRows([]any{int64(8)})
	withDrift[4].rows = zonestatsRows([]any{int64(9)})
	withDrift[5].rows = zonestatsRows([]any{int64(6)})
	withDrift[6].rows = zonestatsRows([]any{int64(7)})
	withDrift[7].rows = zonestatsRows([]any{int64(3)})
	withDrift[8].rows = zonestatsRows([]any{int64(10)})
	withDrift[9].rows = zonestatsRows([]any{int64(5)})
	withDrift[10].rows = zonestatsRows([]any{int64(4)})
	withDrift[11].rows = zonestatsRows([]any{int64(2)})
	withDrift[15].rows = zonestatsRows([]any{int64(6), int64(2), int64(3), int64(1), int64(1), int64(1)})
	withDrift[18].rows = zonestatsRows([]any{int64(7)})
	withDrift[19].rows = zonestatsRows([]any{int64(5)})
	withDrift[21].rows = zonestatsRows([]any{int64(4)})
	withDrift[22].rows = zonestatsRows([]any{int64(2)})
	withDrift[24].rows = zonestatsRows([]any{int64(1)})
	withDrift[25].rows = zonestatsRows([]any{int64(1)})
	withDrift[27].rows = zonestatsRows([]any{int64(2)})
	withDrift[28].rows = zonestatsRows([]any{int64(3)})
	withDrift[30].rows = zonestatsRows([]any{int64(6)})
	withDrift[31].rows = zonestatsRows([]any{int64(1)})
	pool := &zonestatsFakePool{steps: append(noDrift, withDrift...)}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ZoneStatsLoop(ctx, pool, reg, ZoneStatsOptions{
			Interval: 2 * time.Millisecond,
			Log:      logger,
		})
	}()

	// The second pass (by tick) carries a drift — we wait for the Warn
	// itself: reportDrift
	// has already set the gauge before writing the line.
	waitCond(t, 2*time.Second, func() bool { return slogBuf.contains("reconciliation: counter drift") }, "the drift Warn")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ZoneStatsLoop: %v", err)
	}

	log := slogBuf.String()
	for _, want := range []string{
		"counter reconciliation loop", // the start
		"level=WARN",                  // the second pass's drift
		"positions", "sum_abs",
		"zone:7:pending: 4 → 5",
		"global:render_pending: 5 → 6",
	} {
		if !strings.Contains(log, want) {
			t.Errorf("the log does not contain %q:\n%s", want, log)
		}
	}
	// The daily positions — outside the Warn (streaming, reportDrift): in
	// the drift
	// top they must not be, the gauge meanwhile exposes |12 − 7| and
	// |8 − 5|. The retry baskets (T-202) and the hourly tempo baskets
	// (T-203) are
	// the same streaming semantics: |9 − 4|, |6 − 2|, |7 − 2|, |3 − 3|,
	// |10 − 6|, |5 − 1| outside the Warn, the gauge is maintained.
	streaming := append(append(dailyStreamingPositions(), retryMatureStreamingPositions()...), hourlyStreamingPositions()...)
	for _, s := range streaming {
		if strings.Contains(log, s) {
			t.Errorf("the streaming position %s in the drift Warn (noise):\n%s", s, log)
		}
	}
	if v := driftGaugeValue(t, reg, "zone:7:pending"); v != 1 {
		t.Errorf("drift{zone:7:pending} = %v, want 1", v)
	}
	if v := driftGaugeValue(t, reg, "zone:7:queued"); v != 0 {
		t.Errorf("drift{zone:7:queued} = %v, want 0 (a stationary position)", v)
	}
	if v := driftGaugeValue(t, reg, "global:render_pending"); v != 1 {
		t.Errorf("drift{global:render_pending} = %v, want 1", v)
	}
	if v := driftGaugeValue(t, reg, dailyChecksCounter); v != 5 {
		t.Errorf("drift{daily:checks} = %v, want 5 (the gauge is kept outside the Warn too)", v)
	}
	if v := driftGaugeValue(t, reg, dailyTempCounter); v != 3 {
		t.Errorf("drift{daily:temp_error} = %v, want 3 (the gauge is kept outside the Warn too)", v)
	}
	if v := driftGaugeValue(t, reg, retryMaturePastCounter); v != 5 {
		t.Errorf("drift{retry_mature:past} = %v, want 5 (the gauge is kept outside the Warn too)", v)
	}
	if v := driftGaugeValue(t, reg, retryMatureTodayCounter); v != 4 {
		t.Errorf("drift{retry_mature:today} = %v, want 4 (the gauge is kept outside the Warn too)", v)
	}
	// The hourly tempo baskets (T-203): the gauge is maintained outside
	// the Warn too.
	if v := driftGaugeValue(t, reg, hourlyRenderedPrevCounter); v != 5 {
		t.Errorf("drift{rendered:prev} = %v, want 5 (the gauge is kept outside the Warn too)", v)
	}
	if v := driftGaugeValue(t, reg, hourlyRenderedCurCounter); v != 0 {
		t.Errorf("drift{rendered:cur} = %v, want 0 (old = new 3)", v)
	}
	if v := driftGaugeValue(t, reg, hourlyResultsPrevCounter); v != 4 {
		t.Errorf("drift{results:prev} = %v, want 4 (the gauge is kept outside the Warn too)", v)
	}
	if v := driftGaugeValue(t, reg, hourlyResultsCurCounter); v != 4 {
		t.Errorf("drift{results:cur} = %v, want 4 (the gauge is kept outside the Warn too)", v)
	}
}

// TestZoneStatsLoopCancelDuringPass — a ctx cancellation during a pass
// is not
// an alarm: no Warn, a clean nil (the OnRun contract, T-012).
func TestZoneStatsLoopCancelDuringPass(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	steps := stepsOfPass(
		zoneValsOf(7, 10, 4, 2, 3, 1),
		zoneValsOf(7, 10, 4, 2, 3, 1),
		aggRowOf(7, 10, 4, 2, 3, make([]int64, len(statusAggCols)), 0),
		zonestatsRows(),
		fullCounterRows(nil),
		fullCounterRows(nil))
	steps[2].err = context.Canceled
	pool := &zonestatsFakePool{steps: steps}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled before the start: the pass will fail with the cancellation

	done := make(chan error, 1)
	go func() { done <- ZoneStatsLoop(ctx, pool, nil, ZoneStatsOptions{Log: logger}) }()

	if err := <-done; err != nil {
		t.Fatalf("ZoneStatsLoop: %v, want nil on a ctx cancellation", err)
	}
	if strings.Contains(buf.String(), "level=WARN") {
		t.Errorf("a cancellation during the pass — a Warn:\n%s", buf.String())
	}
}

func TestZoneStatsLoopNilPool(t *testing.T) {
	err := ZoneStatsLoop(context.Background(), nil, nil, ZoneStatsOptions{})
	if err == nil || !strings.Contains(err.Error(), "pool is not set") {
		t.Errorf("error = %v, want \"the pool is not set\"", err)
	}
}

// TestZoneStatsLoopPoolWithoutTx — a pool without transactions (db.DB) is
// a start error: the pass requires one snapshot. The wrapper hides the
// fake's Begin
// (embedding would promote it too — the assertion would pass).
func TestZoneStatsLoopPoolWithoutTx(t *testing.T) {
	inner := &zonestatsFakePool{}
	pool := struct{ db.DB }{DB: inner} // only db.DB, without Begin
	err := ZoneStatsLoop(context.Background(), pool, nil, ZoneStatsOptions{})
	if err == nil || !strings.Contains(err.Error(), "ZoneStatsDB") {
		t.Errorf("error = %v, want \"a pool without transactions\"", err)
	}
}

// --- single-flight (T-161) --------------------------------------------------

// zonestatsSkippedValue — the current value of the
// crawler_zonestats_skipped_ticks_total counter from the registry.
func zonestatsSkippedValue(t *testing.T, reg *metrics.Registry) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "crawler_zonestats_skipped_ticks_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			return m.GetCounter().GetValue()
		}
	}
	return 0
}

// zonestatsHistogramSamples — the number of observations of
// crawler_counters_recount_seconds (every pass must land).
func zonestatsHistogramSamples(t *testing.T, reg *metrics.Registry) uint64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "crawler_counters_recount_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			return m.GetHistogram().GetSampleCount()
		}
	}
	return 0
}

// passSteps — presets of N "quiet" passes (a zero drift).
func passSteps(n int) []zonestatsStep {
	statuses := make([]int64, len(statusAggCols))
	out := make([]zonestatsStep, 0, n*32)
	for i := 0; i < n; i++ {
		out = append(out, stepsOfPass(
			zoneValsOf(7, 10, 4, 2, 3, 1),
			zoneValsOf(7, 10, 4, 2, 3, 1),
			aggRowOf(7, 10, 4, 2, 3, statuses, 0),
			zonestatsRows(),
			fullCounterRows(nil),
			fullCounterRows(nil))...)
	}
	return out
}

// TestZoneStatsLoopSingleFlight — a pass longer than the interval: the
// accumulated tick does not start a new pass right behind the slow one —
// a Warn "longer
// than the interval", the skip counter, the duration histogram; the next
// pass — by the next tick, the loop lives.
func TestZoneStatsLoopSingleFlight(t *testing.T) {
	slogBuf := &zonestatsSyncBuf{}
	logger := slog.New(slog.NewTextHandler(slogBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	reg := metrics.New("monitor")
	pool := &zonestatsFakePool{steps: passSteps(12)}
	pool.setDelay(70 * time.Millisecond) // 27 queries ≈ 1.9 s per pass > the 50 ms interval

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ZoneStatsLoop(ctx, pool, reg, ZoneStatsOptions{
			Interval: 50 * time.Millisecond,
			Log:      logger,
		})
	}()

	waitCond(t, 3*time.Second, func() bool { return slogBuf.contains("longer than the interval") }, "the slow pass Warn")
	if got := zonestatsSkippedValue(t, reg); got != 1 {
		t.Errorf("skipped_ticks = %v, want 1", got)
	}
	// The single-flight invariant: at the moment of the Warn the tick is
	// skipped — the second pass
	// did not start BEFORE the drain. The exact query count is not
	// checked: under
	// the load of the run between the Warn and the check the next tick
	// (150 ms) could arrive and start pass 2 — that is the normal mode.
	if n := pool.stats(); n < 27 {
		t.Errorf("queries = %d after the drain, want ≥ 27 (the first pass completed)", n)
	}

	// Pass 2 is fast: the normal mode is restored, the loop lives.
	// There is deliberately no second skipped == 1 check (review R2 P3-1):
	// between the first check and setDelay(0) the ticker could have
	// started pass 2
	// with the old delay — an honest skipped = 2, that is the normal mode;
	// the single-flight invariant ("no more often than the interval") is
	// checked above.
	pool.setDelay(0)
	waitCond(t, 2*time.Second, func() bool { return pool.stats() >= 33 }, "a pass after a skip")
	if got := zonestatsSkippedValue(t, reg); got < 1 {
		t.Errorf("skipped_ticks = %v after a fast pass, want ≥ 1", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ZoneStatsLoop: %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ZoneStatsLoop did not finish after the ctx cancellation")
	}
	if got := zonestatsHistogramSamples(t, reg); got < 2 {
		t.Errorf("histogram observations = %d, want ≥ 2 (one per pass)", got)
	}
}

// TestZoneStatsLoopNilRegistry — the loop without a registry lives: the
// loop's purpose is
// reconciliation, the metrics are observability (T-161).
func TestZoneStatsLoopNilRegistry(t *testing.T) {
	pool := &zonestatsFakePool{steps: passSteps(1)}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- ZoneStatsLoop(ctx, pool, nil, ZoneStatsOptions{
			Interval: time.Hour, // the tick never comes: only the starting pass
		})
	}()

	waitCond(t, 2*time.Second, func() bool { return pool.stats() >= 27 }, "the starting pass without a registry")
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("ZoneStatsLoop: %v, want nil without a registry", err)
	}
}

// --- the SQL texts (the contract) ---------------------------------------------------

// TestRecountSQLContracts — the code literals in the aggregate and the
// overwrite statements: the schema codes are inlined into the text (the
// lease.go style), the
// check_state/status filters correspond to model.*; the unnest value
// batches.
func TestRecountSQLContracts(t *testing.T) {
	for _, s := range []struct{ code, name string }{
		{fmt.Sprintf("check_state = %d", int16(model.CheckStatePending)), "pending FILTER"},
		{fmt.Sprintf("check_state = %d", int16(model.CheckStateQueued)), "queued FILTER"},
		{fmt.Sprintf("check_state = %d", int16(model.CheckStateDone)), "checked FILTER"},
		{fmt.Sprintf("status <> %d", int16(model.StatusDeleted)), "the base set"},
	} {
		if !strings.Contains(domainsAggSQL, s.code) {
			t.Errorf("domainsAggSQL does not contain %s: %q", s.name, s.code)
		}
	}
	// The status legend is complete and consistent with statusAggCols (one
	// source of SQL generation and reading, review P3-4).
	if len(statusAggCols) != len(model.Statuses) {
		t.Fatalf("statusAggCols = %d positions, want the model.Statuses legend (%d)", len(statusAggCols), len(model.Statuses))
	}
	for _, c := range statusAggCols {
		want := fmt.Sprintf("count(*) FILTER (WHERE status = %d) AS %s", int16(c.status), c.col)
		if !strings.Contains(domainsAggSQL, want) {
			t.Errorf("domainsAggSQL without the %s column of the status %v", c.col, c.status)
		}
	}
	if !strings.Contains(domainsAggSQL, fmt.Sprintf("(flags & %d) <> 0", int16(model.FlagRenderPending))) {
		t.Error("domainsAggSQL without the render_pending bit (the 000015 backfill predicate)")
	}
	// f_render — without a status filter (the invariant of the 000015
	// backfill).
	if strings.Contains(domainsAggSQL, fmt.Sprintf("status <> %d AND (flags", int16(model.StatusDeleted))) {
		t.Error("domainsAggSQL filters the render bit by status — the predicate must match the 000015 backfill (no filter)")
	}
	// The daily windows (T-179/T-182): one window predicate, the temp
	// numerator is
	// the same window × the temp_error status.
	if !strings.Contains(domainsAggSQL, "count(*) FILTER (WHERE last_checked_at = current_date) AS f_checks_today") {
		t.Error("domainsAggSQL without the daily window f_checks_today (T-179)")
	}
	if !strings.Contains(domainsAggSQL, fmt.Sprintf("count(*) FILTER (WHERE last_checked_at = current_date AND status = %d) AS f_temp_today", int16(model.StatusTempError))) {
		t.Errorf("domainsAggSQL without the temp numerator f_temp_today (T-182): status %d", int16(model.StatusTempError))
	}
	// The retry maturity baskets (T-202): the mature temp_error × done
	// candidates
	// by episode dates — today's (they mature at midnight) and the matured
	// cohort (older than the current day); the scheduler guard predicate is
	// status × done × the date window.
	if !strings.Contains(domainsAggSQL, fmt.Sprintf(
		"count(*) FILTER (WHERE status = %d AND check_state = %d AND last_checked_at = current_date) AS f_retry_mature_today",
		int16(model.StatusTempError), int16(model.CheckStateDone))) {
		t.Error("domainsAggSQL without the f_retry_mature_today basket (T-202)")
	}
	if !strings.Contains(domainsAggSQL, fmt.Sprintf(
		"count(*) FILTER (WHERE status = %d AND check_state = %d AND last_checked_at < current_date) AS f_retry_mature_past",
		int16(model.StatusTempError), int16(model.CheckStateDone))) {
		t.Error("domainsAggSQL without the f_retry_mature_past basket (T-202)")
	}
	// The "status hung" domains (FR-2.2, T-256): new × done × older than a
	// day is
	// the same date boundary as the mature retry basket (the precision of
	// the
	// date column, the "older than 24 h" implementation).
	if !strings.Contains(domainsAggSQL, fmt.Sprintf(
		"count(*) FILTER (WHERE status = %d AND check_state = %d AND last_checked_at < current_date) AS f_new_done_stale",
		int16(model.StatusNew), int16(model.CheckStateDone))) {
		t.Error("domainsAggSQL without f_new_done_stale (FR-2.2, T-256)")
	}
	for _, sql := range []string{recountAggQuerySQL, verifyZonesSQL} {
		if !strings.Contains(sql, "GROUP BY zone_id") {
			t.Errorf("the aggregate without a partitionwise GROUP BY zone_id: %s", sql)
		}
	}
	// The overwrite: the zones are locked before the counters (the
	// anti-deadlock order,
	// review P3-2) — the unnest batches of both statements.
	for _, sql := range []string{recountZonesSQL, recountCountersSQL} {
		if !strings.Contains(sql, "unnest($1") {
			t.Errorf("the statement without an unnest batch: %s", sql)
		}
	}
	if !strings.Contains(recountCountersSQL, "ON CONFLICT (name) DO UPDATE") {
		t.Error("recountCountersSQL without the counter row upsert (the first pass creates the rows)")
	}
}

// --- VerifyCounters ---------------------------------------------------------

// TestVerifyCounters — the check without writing: the mismatches of the
// zones and the globals
// (stored ≠ the aggregate) with the tolerance; consistency — an empty
// result.
func TestVerifyCounters(t *testing.T) {
	statuses := make([]int64, len(statusAggCols))
	statuses[4] = 6 // alive
	pool := &zonestatsFakePool{steps: []zonestatsStep{
		// verifyZonesSQL: id, total, live_total, pending, live_pending, …
		{rows: zonestatsRows(
			[]any{int16(7), int64(10), int64(10), int64(4), int64(4), int64(2), int64(2), int64(3), int64(3), int64(1), int64(1)},
			[]any{int16(9), int64(5), int64(6), int64(1), int64(1), int64(0), int64(0), int64(4), int64(4), int64(0), int64(0)},
		)},
		// recountAggQuerySQL: the live per-zone aggregate (the alive=6 status).
		{rows: zonestatsRows(aggRowOf(7, 10, 4, 2, 3, statuses, 3))},
		// historyAggSQL: the live cumulatives.
		{rows: zonestatsRows([]any{int64(10), int64(2)})},
		// storedCountersSQL: the stored ones.
		{rows: zonestatsRows(
			counterRowOf("status_alive", 5),
			counterRowOf("render_pending", 2),
			counterRowOf("laravel_appeared_total", 10),
			counterRowOf("laravel_lost_total", 2))},
	}}
	// The second check (the tolerance) — fresh step portions: the preset
	// rows
	// are iterated once (i in *zonestatsFakeRows), the pointers cannot be
	// reused.
	replay := func() []zonestatsStep {
		return []zonestatsStep{
			{rows: zonestatsRows(
				[]any{int16(7), int64(10), int64(10), int64(4), int64(4), int64(2), int64(2), int64(3), int64(3), int64(1), int64(1)},
				[]any{int16(9), int64(5), int64(6), int64(1), int64(1), int64(0), int64(0), int64(4), int64(4), int64(0), int64(0)},
			)},
			{rows: zonestatsRows(aggRowOf(7, 10, 4, 2, 3, statuses, 3))},
			{rows: zonestatsRows([]any{int64(10), int64(2)})},
			{rows: zonestatsRows(
				counterRowOf("status_alive", 5),
				counterRowOf("render_pending", 2),
				counterRowOf("laravel_appeared_total", 10),
				counterRowOf("laravel_lost_total", 2))},
		}
	}
	pool.steps = append(pool.steps, replay()...)

	mismatch, err := VerifyCounters(context.Background(), pool, VerifyOptions{})
	if err != nil {
		t.Fatalf("VerifyCounters: %v", err)
	}
	names := make([]string, len(mismatch))
	for i, p := range mismatch {
		names[i] = p.Counter
	}
	// zone:9:total (5→6) and global:render_pending (2→3); status_alive
	// (5→6) is outside the check — the interval freshness, not drift.
	if len(mismatch) != 2 {
		t.Fatalf("mismatches = %v (%d), want 2 positions", names, len(mismatch))
	}
	byName := map[string]CounterPosition{}
	for _, p := range mismatch {
		byName[p.Counter] = p
	}
	if p := byName["zone:9:total"]; p.Old != 5 || p.New != 6 {
		t.Errorf("zone:9:total = %+v, want old=5 new=6", p)
	}
	if p := byName["global:render_pending"]; p.Old != 2 || p.New != 3 {
		t.Errorf("global:render_pending = %+v, want old=2 new=3", p)
	}

	// The tolerance closes the single mismatches.
	if mismatch, err = VerifyCounters(context.Background(), pool, VerifyOptions{Tolerance: 1}); err != nil || len(mismatch) != 0 {
		t.Errorf("verify with a tolerance of 1: %v (%d mismatches), want empty", err, len(mismatch))
	}
}

func TestVerifyCountersNilPool(t *testing.T) {
	if _, err := VerifyCounters(context.Background(), nil, VerifyOptions{}); err == nil ||
		!strings.Contains(err.Error(), "pool is not set") {
		t.Errorf("error = %v, want \"the pool is not set\"", err)
	}
}

// TestGlobalCounters — the global sums function: the statuses are summed
// by the legend, the render bit is summed, the cumulatives pass
// through.
func TestGlobalCounters(t *testing.T) {
	s1 := make([]int64, len(statusAggCols))
	s1[4] = 3 // alive
	s2 := make([]int64, len(statusAggCols))
	s2[4] = 2
	s2[6] = 1 // deleted
	agg := []zoneAggRow{
		{zone: 7, statuses: s1, render: 4},
		{zone: 9, statuses: s2, render: 1},
	}
	got := globalCounters(agg, 12, 3)
	if got["status_alive"] != 5 || got["status_deleted"] != 1 || got["status_new"] != 0 {
		t.Errorf("statuses = %v, want alive=5 deleted=1 new=0", got)
	}
	if got["render_pending"] != 5 || got["laravel_appeared_total"] != 12 || got["laravel_lost_total"] != 3 {
		t.Errorf("scalars = %v, want render=5 appeared=12 lost=3", got)
	}
	// The full name legend.
	if len(got) != len(CounterNames()) {
		t.Errorf("the legend = %d names, want %d", len(got), len(CounterNames()))
	}
}

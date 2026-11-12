// Unit tests of the Laravel findings gray zone counter (T-115) on a fake
// pgx pool: the exact SQL text and parameters (the real range boundaries),
// the threshold validation, the defaults, the query errors and answer-shape
// degradations (empty, an unreadable row, an iteration error), a ctx cancellation, the loop
// NearStatsLoop (an immediate first pass, the gauge updated, a Warn and
// surviving an error, a successful pass under cancellation, a clean exit, no-op
// without a registry). Behavior against a live DB — the dev environment (crawlerctl
// near + the monitor /metrics).
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

	"crawler/internal/metrics"
)

// --- the fake pool (Query — a count aggregate) --------------------------------

// nearstatsCall — a recorded SQL call with arguments.
type nearstatsCall struct {
	sql  string
	args []any
}

// nearstatsFakePool — a db.DB without a DB: the queue of the count(*)
// aggregate answers and errors, the call protocol. Under a lock: the NearStatsLoop runs
// in a goroutine.
type nearstatsFakePool struct {
	mu     sync.Mutex
	counts []int64 // the queue of count(*) values (exhausted — 0)
	errs   []error // the error queue (nil — a call without an error)

	// Answer-shape degradations (the alertsPool style): an empty answer (Next
	// false), a row with an unreadable type (a Scan error), an iteration error
	// (rows.Err).
	empty   bool
	badRow  bool
	rowsErr error
	// ignoreCtx — answer success without checking ctx: the scenario "the pass
	// is successful, there was no cancellation during it" for the loop classification.
	// Immutable after construction (set by a composite literal before
	// the loop goroutine starts) — the queues lock does not protect it and must not.
	ignoreCtx bool

	queries []nearstatsCall
}

func (p *nearstatsFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom must not be called")
}

func (p *nearstatsFakePool) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, fmt.Errorf("nearstatsFakePool: unexpected Exec: %s", sql)
}

func (p *nearstatsFakePool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if !p.ignoreCtx {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, nearstatsCall{sql: sql, args: args})
	if err := popErr(&p.errs); err != nil {
		return nil, err
	}
	if p.empty {
		return &nearstatsRows{err: p.rowsErr}, nil
	}
	if p.badRow {
		return &nearstatsRows{rows: [][]any{{"bytes"}}, err: p.rowsErr}, nil
	}
	n := int64(0)
	if len(p.counts) > 0 {
		n = p.counts[0]
		p.counts = p.counts[1:]
	}
	return &nearstatsRows{rows: [][]any{{n}}, err: p.rowsErr}, nil
}

// stats — a snapshot of the call count (waiting for loop progress).
func (p *nearstatsFakePool) stats() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queries)
}

// calls — a snapshot of the call protocol.
func (p *nearstatsFakePool) calls() []nearstatsCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]nearstatsCall(nil), p.queries...)
}

// nearstatsRows — pgx.Rows with one aggregate row. err — the rows.Err
// answer (nil by default); the row value type is checked in Scan.
type nearstatsRows struct {
	rows   [][]any
	i      int
	closed bool
	err    error
}

func (r *nearstatsRows) Close()                                       { r.closed = true }
func (r *nearstatsRows) Err() error                                   { return r.err }
func (r *nearstatsRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *nearstatsRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *nearstatsRows) Next() bool {
	if r.closed || r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *nearstatsRows) Scan(dest ...any) error {
	if r.closed || r.i == 0 || r.i > len(r.rows) {
		return errors.New("nearstatsRows: Scan without Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("nearstatsRows: %d destinations, %d columns", len(dest), len(row))
	}
	n, ok := row[0].(int64)
	if !ok {
		return fmt.Errorf("nearstatsRows: column 0: %T is not int64", row[0])
	}
	*dest[0].(*int64) = n
	return nil
}

func (r *nearstatsRows) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *nearstatsRows) RawValues() [][]byte    { return nil }
func (r *nearstatsRows) Conn() *pgx.Conn        { return nil }
func (r *nearstatsRows) TypeMap() *pgtype.Map   { return nil }

// --- the expected SQL text (the contract) ------------------------------------

const wantNearStatsSQL = "SELECT count(*) FROM laravel_findings WHERE score >= $1::real AND score < $2::real"

// --- NearStatsOnce ---------------------------------------------------------

// TestNearStatsOnceSQL — one query with exact parameters: the gray zone
// boundaries [min/2, min) are cast to real.
func TestNearStatsOnceSQL(t *testing.T) {
	pool := &nearstatsFakePool{counts: []int64{42}}
	n, err := NearStatsOnce(context.Background(), pool, 0.6)
	if err != nil {
		t.Fatalf("NearStatsOnce: %v", err)
	}
	calls := pool.calls()
	if len(calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(calls))
	}
	if calls[0].sql != wantNearStatsSQL {
		t.Errorf("SQL:\n%s\nwant:\n%s", calls[0].sql, wantNearStatsSQL)
	}
	wantArgs := []any{float32(0.3), float32(0.6)}
	if !reflect.DeepEqual(calls[0].args, wantArgs) {
		t.Errorf("args = %v, want %v", calls[0].args, wantArgs)
	}
	if n != 42 {
		t.Errorf("count = %d, want 42", n)
	}
}

// TestNearStatsOnceThreshold — a threshold outside (0, 1) — an error before the DB
// (half of a negative or >= 1 is meaningless).
func TestNearStatsOnceThreshold(t *testing.T) {
	pool := &nearstatsFakePool{}
	for _, m := range []float64{0, -0.1, 1, 1.5} {
		n, err := NearStatsOnce(context.Background(), pool, m)
		if err == nil {
			t.Errorf("NearStatsOnce(%v) = nil error, want a threshold error", m)
		}
		if n != 0 {
			t.Errorf("NearStatsOnce(%v) = %d, want 0 on error", m, n)
		}
	}
	if pool.stats() != 0 {
		t.Error("Query must not be called with an invalid threshold")
	}
}

// TestNearStatsOnceNilPool — the pool not set: an error, not a panic; the value
// of the error is strictly 0.
func TestNearStatsOnceNilPool(t *testing.T) {
	n, err := NearStatsOnce(context.Background(), nil, 0.6)
	if err == nil {
		t.Fatal("NearStatsOnce(nil pool) = nil error, want an error")
	}
	if n != 0 {
		t.Errorf("the value = %d, want 0 on error", n)
	}
}

// TestNearStatsOnceErrors — the aggregate answer degradations: a query
// error, an empty answer, an unreadable row, an iteration error — each with the
// task context in the text and a zero value (not ±1).
func TestNearStatsOnceErrors(t *testing.T) {
	cases := []struct {
		name    string
		pool    *nearstatsFakePool
		wantErr string
	}{
		{
			"a query error",
			&nearstatsFakePool{errs: []error{errors.New("db unavailable")}},
			"gray zone counter: db unavailable",
		},
		{"an empty aggregate response", &nearstatsFakePool{empty: true}, "empty aggregate response"},
		{"an unreadable row", &nearstatsFakePool{badRow: true}, "scan"},
		{
			"an iteration error",
			&nearstatsFakePool{rowsErr: errors.New("connection reset")},
			"connection reset",
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			n, err := NearStatsOnce(context.Background(), tt.pool, 0.6)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tt.wantErr)
			}
			if n != 0 {
				t.Errorf("the value = %d, want 0 on error", n)
			}
		})
	}
}

// TestNearStatsOptionsDefaults — a zero interval — an hour; the "≤ 0" boundary:
// a positive nanosecond interval is not replaced by the default.
func TestNearStatsOptionsDefaults(t *testing.T) {
	if got := (NearStatsOptions{}).withDefaults().Interval; got != DefaultNearStatsInterval {
		t.Errorf("the interval default = %v, want %v", got, DefaultNearStatsInterval)
	}
	if got := (NearStatsOptions{Interval: time.Minute}).withDefaults().Interval; got != time.Minute {
		t.Errorf("an explicit interval = %v, want time.Minute", got)
	}
	if got := (NearStatsOptions{Interval: time.Nanosecond}).withDefaults().Interval; got != time.Nanosecond {
		t.Errorf("the interval 1ns = %v, want 1ns (only ≤ 0 is replaced)", got)
	}
}

// nearGaugeValue — the current gray zone gauge value from the registry.
func nearGaugeValue(t *testing.T, reg *metrics.Registry) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "crawler_laravel_near_findings" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if g := m.GetGauge(); g != nil {
				return g.GetValue()
			}
		}
	}
	t.Fatal("the crawler_laravel_near_findings series was not found")
	return -1
}

// TestNearStatsLoopImmediatePass — the first pass right away: the gauge gets
// the value before the first tick; a cancellation — a clean nil.
func TestNearStatsLoopImmediatePass(t *testing.T) {
	pool := &nearstatsFakePool{counts: []int64{7}}
	reg := metrics.New("monitor-test")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- NearStatsLoop(ctx, pool, reg, NearStatsOptions{MinScore: 0.6})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for pool.stats() == 0 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("the first pass was not performed")
		}
		time.Sleep(time.Millisecond)
	}
	if got := nearGaugeValue(t, reg); got != 7 {
		t.Errorf("gauge = %v, want 7", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("NearStatsLoop: %v, want nil on cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the loop did not finish after the ctx cancellation")
	}
}

// TestNearStatsLoopNoop — a nil registry or pool — a no-op without a panic
// (isolated builds, the CollectQueueLoop style).
func TestNearStatsLoopNoop(t *testing.T) {
	pool := &nearstatsFakePool{}
	if err := NearStatsLoop(context.Background(), pool, nil, NearStatsOptions{MinScore: 0.6}); err != nil {
		t.Fatalf("a nil registry: %v, want nil", err)
	}
	reg := metrics.New("monitor-test")
	if err := NearStatsLoop(context.Background(), nil, reg, NearStatsOptions{MinScore: 0.6}); err != nil {
		t.Fatalf("a nil pool: %v, want nil", err)
	}
	if pool.stats() != 0 {
		t.Error("there must be no passes")
	}
}

// TestNearStatsLoopErrorSurvives — a pass error does not kill the loop:
// a Warn into the log (a ctx cancellation is not at fault — it classifies only
// errors under an already cancelled context); the next pass counts anew.
func TestNearStatsLoopErrorSurvives(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := &nearstatsFakePool{errs: []error{errors.New("db unavailable")}}
	reg := metrics.New("monitor-test")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- NearStatsLoop(ctx, pool, reg, NearStatsOptions{
			Interval: time.Millisecond, MinScore: 0.6, Log: logger,
		})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for pool.stats() < 2 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("the loop did not reach the second pass: %d", pool.stats())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("NearStatsLoop: %v, want nil (a pass error is not the loop's death)", err)
	}
	log := buf.String()
	if !strings.Contains(log, "level=WARN") || !strings.Contains(log, "db unavailable") {
		t.Errorf("a pass error with a live ctx without a Warn:\n%s", log)
	}
	if got := nearGaugeValue(t, reg); got != 0 {
		// The value queue is empty (counts not set) — a successful pass
		// returned 0; what matters more is that the gauge exists and the loop is alive.
		t.Logf("the gauge = %v after the recovery", got)
	}
}

// TestNearStatsLoopPassDuringCancel — a successful pass under an already cancelled
// ctx (the pool ignores the cancellation) — a normal result in the gauge, not the
// silence of the "cancellation during the pass" branch: the cancellation classifies only errors.
func TestNearStatsLoopPassDuringCancel(t *testing.T) {
	pool := &nearstatsFakePool{counts: []int64{42}, ignoreCtx: true}
	reg := metrics.New("monitor-test")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		done <- NearStatsLoop(ctx, pool, reg, NearStatsOptions{MinScore: 0.6})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("NearStatsLoop: %v, want nil on cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the loop did not finish after the ctx cancellation")
	}
	if got := nearGaugeValue(t, reg); got != 42 {
		t.Errorf("the gauge = %v, want 42: a successful pass under cancellation is the normal outcome", got)
	}
}

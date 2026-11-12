// Unit tests of the daily domain recheck (T-030, spec 5.3, 3.3, 11.4
// item 8) on a fake pgx pool: the exact SQL text and parameters (the
// now()−interval boundary, the batch size, the check_state/status codes), defaults,
// an empty set, repeated passes, errors and ctx cancellation, the
// RecheckDaily cycle (an immediate drain, surviving an error, a clean exit).
// Behavior against a live DB — integration tests of stage 2.
package scheduler

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

	"crawler/internal/model"
)

// --- fake pool ---------------------------------------------------------

// recheckCall — a recorded SQL call with arguments.
type recheckCall struct {
	sql  string
	args []any
}

// recheckFakePool — a db.DB without a DB: the queue of Query results (the
// number of moved rows — the zone_id rows of the pass statement's outer
// SELECT, T-178) and errors, the call protocol. Everything under a lock: the
// RecheckDaily loop runs in a goroutine; the test waits for progress by
// counters.
type recheckFakePool struct {
	mu       sync.Mutex
	tags     []int64 // the queue of response row counts (ran out — 0: an empty set)
	execErrs []error // the queue of Query errors (nil — a call without an error)
	queries  []recheckCall
}

func (p *recheckFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom must not be called")
}

func (p *recheckFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("Exec must not be called")
}

func (p *recheckFakePool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, recheckCall{sql: sql, args: args})
	if len(p.execErrs) > 0 {
		err := p.execErrs[0]
		p.execErrs = p.execErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	n := int64(0)
	if len(p.tags) > 0 {
		n = p.tags[0]
		p.tags = p.tags[1:]
	}
	rows := make([][]any, n)
	for i := range rows {
		rows[i] = []any{int16(1)} // zone_id: the value does not matter for row counting
	}
	return retryRows(rows...), nil
}

// stats — a snapshot of the call count under the lock (waiting for loop progress).
func (p *recheckFakePool) stats() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queries)
}

// calls — a snapshot of the call protocol under the lock.
func (p *recheckFakePool) calls() []recheckCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]recheckCall(nil), p.queries...)
}

const wantRecheckUpdate = "WITH b AS (SELECT zone_id, id FROM domains " +
	"WHERE check_state = 2 AND status <> 6 AND status <> 7 AND last_checked_at < $2 " +
	"ORDER BY last_checked_at, zone_id, id LIMIT $3), " +
	"upd AS (UPDATE domains AS d " +
	"SET check_state = $1, queued_at = NULL " +
	"FROM b WHERE d.zone_id = b.zone_id AND d.id = b.id " +
	"AND d.check_state = 2 AND d.status <> 6 AND d.status <> 7 AND d.last_checked_at < $2 " +
	"RETURNING d.zone_id), " +
	"zc AS (UPDATE zones AS z " +
	"SET domains_checked = z.domains_checked - c.n, " +
	"domains_pending = z.domains_pending + c.n " +
	"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c " +
	"WHERE z.id = c.zone_id) " +
	"SELECT zone_id FROM upd"

// recheckFixedClock — a clock with a fixed "now" (the pass threshold
// reproducible to the nanosecond).
func recheckFixedClock() (time.Time, func() time.Time) {
	now := time.Date(2026, 10, 17, 3, 0, 0, 0, time.UTC)
	return now, func() time.Time { return now }
}

// --- RecheckOnce -----------------------------------------------------------

// TestRecheckOnceSQL — one UPDATE statement with exact parameters:
// the target state pending, the done/not-deleted selection, the
// now() − interval boundary and the batch size.
func TestRecheckOnceSQL(t *testing.T) {
	now, clock := recheckFixedClock()
	pool := &recheckFakePool{tags: []int64{42}}

	res, err := RecheckOnce(context.Background(), pool, RecheckOptions{
		Interval:  2190 * time.Hour,
		BatchSize: 7000,
		Now:       clock,
	})
	if err != nil {
		t.Fatalf("RecheckOnce: %v", err)
	}

	calls := pool.calls()
	if len(calls) != 1 {
		t.Fatalf("calls = %d, want 1: a pass is one statement", len(calls))
	}
	if calls[0].sql != wantRecheckUpdate {
		t.Errorf("pass SQL:\n%s\nwant:\n%s", calls[0].sql, wantRecheckUpdate)
	}
	wantArgs := []any{
		int16(model.CheckStatePending), // SET check_state
		now.Add(-2190 * time.Hour),     // the now() − interval boundary
		7000,                           // the batch LIMIT
	}
	if !reflect.DeepEqual(calls[0].args, wantArgs) {
		t.Errorf("args = %v, want %v", calls[0].args, wantArgs)
	}
	if res.Domains != 42 {
		t.Errorf("result = %+v, want Domains=42 (the outer SELECT rows)", res)
	}
}

// TestRecheckOnceDefaults — zero options: the interval and batch are the defaults
// (4380 h ≈ 6 months, spec 3.3); the clocks are fixed: the threshold is counted
// exactly.
func TestRecheckOnceDefaults(t *testing.T) {
	now, clock := recheckFixedClock()
	pool := &recheckFakePool{}

	if _, err := RecheckOnce(context.Background(), pool, RecheckOptions{Now: clock}); err != nil {
		t.Fatalf("RecheckOnce: %v", err)
	}

	args := pool.calls()[0].args
	if args[2] != DefaultRecheckBatch {
		t.Errorf("LIMIT = %v, want the default %d", args[2], DefaultRecheckBatch)
	}
	if got, ok := args[1].(time.Time); !ok || !got.Equal(now.Add(-DefaultRecheckInterval)) {
		t.Errorf("the threshold = %v, want %v (now − the default %s)", args[1], now.Add(-DefaultRecheckInterval), DefaultRecheckInterval)
	}
}

// TestRecheckOnceEmpty — an exhausted set: RowsAffected 0, a zero
// outcome, one statement.
func TestRecheckOnceEmpty(t *testing.T) {
	pool := &recheckFakePool{} // the tags queue is empty — UPDATE 0

	res, err := RecheckOnce(context.Background(), pool, RecheckOptions{})
	if err != nil {
		t.Fatalf("RecheckOnce: %v", err)
	}
	if res != (RecheckResult{}) {
		t.Errorf("result = %+v, want zero", res)
	}
	if n := pool.stats(); n != 1 {
		t.Errorf("statements = %d, want 1", n)
	}
}

// TestRecheckOnceRepeatPasses — moved rows are not selected
// again (the selection condition is in the SQL itself): each pass is its own statement;
// after exhaustion — a zero outcome.
func TestRecheckOnceRepeatPasses(t *testing.T) {
	pool := &recheckFakePool{tags: []int64{5000, 3, 0}}

	first, err := RecheckOnce(context.Background(), pool, RecheckOptions{})
	if err != nil {
		t.Fatalf("the first pass: %v", err)
	}
	second, err := RecheckOnce(context.Background(), pool, RecheckOptions{})
	if err != nil {
		t.Fatalf("the second pass: %v", err)
	}
	third, err := RecheckOnce(context.Background(), pool, RecheckOptions{})
	if err != nil {
		t.Fatalf("the third pass: %v", err)
	}
	if first.Domains != 5000 || second.Domains != 3 || third.Domains != 0 {
		t.Errorf("passes = %d, %d, %d; want 5000, 3, 0",
			first.Domains, second.Domains, third.Domains)
	}
	if n := pool.stats(); n != 3 {
		t.Errorf("statements = %d, want 3 (one per pass)", n)
	}
}

// TestRecheckOnceError — a statement error is wrapped with the task
// context; ctx cancellation is preserved via errors.Is.
func TestRecheckOnceError(t *testing.T) {
	pool := &recheckFakePool{execErrs: []error{errors.New("db unavailable")}}
	_, err := RecheckOnce(context.Background(), pool, RecheckOptions{})
	if err == nil ||
		!strings.Contains(err.Error(), "scheduler: domain recheck") ||
		!strings.Contains(err.Error(), "db unavailable") {
		t.Errorf("err = %v, want the task context and the original error", err)
	}

	cancelPool := &recheckFakePool{execErrs: []error{context.Canceled}}
	if _, err := RecheckOnce(context.Background(), cancelPool, RecheckOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func TestRecheckOnceNilPool(t *testing.T) {
	if _, err := RecheckOnce(context.Background(), nil, RecheckOptions{}); err == nil ||
		!strings.Contains(err.Error(), "pool not set") {
		t.Errorf("err = %v, want \"pool not set\"", err)
	}
}

// --- RecheckOptions --------------------------------------------------------

// recheckCheckDefaults — the values after withDefaults (Now is a function;
// compared separately: a struct with a function field is not compared in full).
func recheckCheckDefaults(t *testing.T, got RecheckOptions, wantInterval time.Duration, wantBatch int, wantSchedule time.Duration) {
	t.Helper()
	if got.Interval != wantInterval || got.BatchSize != wantBatch || got.Schedule != wantSchedule {
		t.Errorf("withDefaults = Interval=%v BatchSize=%v Schedule=%v, want %v/%v/%v",
			got.Interval, got.BatchSize, got.Schedule, wantInterval, wantBatch, wantSchedule)
	}
}

func TestRecheckOptionsDefaults(t *testing.T) {
	// The defaults are pinned by spec literals (3.3: 6 months; 5.3: batches, a day):
	// comparing with the very constant does not notice mutations.
	if DefaultRecheckInterval != 4380*time.Hour {
		t.Errorf("DefaultRecheckInterval = %v, want 4380h (≈6 months)", DefaultRecheckInterval)
	}
	if DefaultRecheckBatch != 5000 {
		t.Errorf("DefaultRecheckBatch = %d, want 5000", DefaultRecheckBatch)
	}
	if DefaultRecheckSchedule != 24*time.Hour {
		t.Errorf("DefaultRecheckSchedule = %v, want a day", DefaultRecheckSchedule)
	}

	// Zero and negative values — the defaults.
	recheckCheckDefaults(t, (RecheckOptions{}).withDefaults(),
		DefaultRecheckInterval, DefaultRecheckBatch, DefaultRecheckSchedule)
	recheckCheckDefaults(t, (RecheckOptions{Interval: -time.Hour, BatchSize: -1, Schedule: -time.Minute}).withDefaults(),
		DefaultRecheckInterval, DefaultRecheckBatch, DefaultRecheckSchedule)

	// Replacement boundaries: 1 ns and 1 are positive values, not defaults.
	recheckCheckDefaults(t, (RecheckOptions{Interval: time.Nanosecond, BatchSize: 1, Schedule: time.Nanosecond}).withDefaults(),
		time.Nanosecond, 1, time.Nanosecond)

	// Given values are not overwritten; Now is passed as is.
	now := func() time.Time { return time.Unix(0, 0) }
	got := RecheckOptions{
		Interval:  time.Hour,
		BatchSize: 10,
		Schedule:  time.Minute,
		Now:       now,
	}.withDefaults()
	recheckCheckDefaults(t, got, time.Hour, 10, time.Minute)
	if got.Now == nil || !got.Now().Equal(time.Unix(0, 0)) {
		t.Errorf("withDefaults overwrites Now: nil=%v, value %v", got.Now == nil, got.Now())
	}
}

// --- RecheckDaily ----------------------------------------------------------

// TestRecheckDailyImmediateDrain — the first drain runs immediately at
// start, without waiting a day: it picks up what ripened during the downtime.
func TestRecheckDailyImmediateDrain(t *testing.T) {
	pool := &recheckFakePool{tags: []int64{7}} // an incomplete batch — the drain is finished

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- RecheckDaily(ctx, pool, RecheckOptions{
			Schedule: time.Hour, // the tick never comes: only the starting drain
		})
	}()

	// The starting drain: one statement and silence until cancellation.
	deadline := time.Now().Add(2 * time.Second)
	for pool.stats() < 1 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("the initial drain did not run")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	if n := pool.stats(); n != 1 {
		t.Errorf("statements = %d, want 1: without a tick there are no repeated drains", n)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("RecheckDaily: %v, want nil on ctx cancellation", err)
	}
}

// TestRecheckDailyLoop — a full drain in batches until exhaustion; a batch
// error interrupts only its own drain (a Warn); the next tick continues;
// ctx cancellation — a clean nil.
func TestRecheckDailyLoop(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	// Drain 1 (start): a DB error — a Warn. Drain 2 (tick): a full batch +
	// an incomplete one — an Info with the sum. Then — empty sets.
	pool := &recheckFakePool{
		tags:     []int64{DefaultRecheckBatch, 3},
		execErrs: []error{errors.New("db unavailable")},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RecheckDaily(ctx, pool, RecheckOptions{
			Schedule: 2 * time.Millisecond,
			Log:      logger,
		})
	}()

	// Three statements: the first drain's failure and both batches of the second.
	deadline := time.Now().Add(2 * time.Second)
	for pool.stats() < 3 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("the loop did not reach the third statement: %d", pool.stats())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("RecheckDaily: %v, want nil on ctx cancellation", err)
	}

	log := buf.String()
	for _, want := range []string{
		"daily domain recheck", // the start
		"level=WARN", // a drain error did not kill the loop
		"db unavailable",
		fmt.Sprintf("domains=%d", DefaultRecheckBatch+3), // the drain outcome
	} {
		if !strings.Contains(log, want) {
			t.Errorf("log does not contain %q:\n%s", want, log)
		}
	}
}

// TestRecheckDailyCancelDuringDrain — ctx cancellation during a drain is not
// an alarm: no Warn, a clean nil (the OnRun contract, T-012).
func TestRecheckDailyCancelDuringDrain(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := &recheckFakePool{execErrs: []error{context.Canceled}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled before the start: the drain's very first statement will fail

	done := make(chan error, 1)
	go func() { done <- RecheckDaily(ctx, pool, RecheckOptions{Log: logger}) }()

	if err := <-done; err != nil {
		t.Fatalf("RecheckDaily: %v, want nil on ctx cancellation", err)
	}
	if strings.Contains(buf.String(), "level=WARN") {
		t.Errorf("a cancellation during the drain — Warn:\n%s", buf.String())
	}
}

// TestRecheckDailyDrainTotals — the drain's Info outcome: a positive sum
// is logged (exactly 1 is also positive); a zero one — is not. The drain
// runs with ctx already cancelled: the fake returns no errors; the cancellation
// does not hinder the passes — the outcome must be tallied.
func TestRecheckDailyDrainTotals(t *testing.T) {
	tests := []struct {
		name string
		tags []int64
		want string // a substring of the drain's Info outcome; "" — the outcome is not logged
	}{
		{"an incomplete batch of one", []int64{1}, "domains=1"},
		{"a zero drain", []int64{0}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))
			pool := &recheckFakePool{tags: tt.tags}

			ctx, cancel := context.WithCancel(context.Background())
			cancel() // the starting drain is synchronous and independent of the tick
			done := make(chan error, 1)
			go func() { done <- RecheckDaily(ctx, pool, RecheckOptions{Log: logger}) }()

			if err := <-done; err != nil {
				t.Fatalf("RecheckDaily: %v, want nil", err)
			}
			log := buf.String()
			if tt.want == "" {
				if strings.Contains(log, "daily domain recheck drain") {
					t.Errorf("a zero drain summarizes:\n%s", log)
				}
			} else if !strings.Contains(log, tt.want) {
				t.Errorf("log does not contain %q:\n%s", tt.want, log)
			}
		})
	}
}

func TestRecheckDailyNilPool(t *testing.T) {
	err := RecheckDaily(context.Background(), nil, RecheckOptions{})
	if err == nil || !strings.Contains(err.Error(), "pool not set") {
		t.Errorf("err = %v, want \"pool not set\"", err)
	}
}

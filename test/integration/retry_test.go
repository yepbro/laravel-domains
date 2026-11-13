// Integration tests of the temp_error re-check pass RetryOnce
// (T-031; the candidate selection by a seq scan with the plan pinning
// SET LOCAL — T-191, the guard by the retry_mature maturity buckets —
// T-202, moving the bucket decrement out of the zone requeue operator
// — T-267) against live PostgreSQL: the retry_test.go unit tests check
// the SQL contracts and the sequence on fakes; here — the real
// behavior on a migrated schema: a mature temp_error cohort is drained
// by batches (a requeue is assigned by temp_attempts < N, a
// finalization into delegated_no_address on exhaustion), the
// transitioned ones are not selected again; the bucket decrements zero
// the guard — the heavy selection is not run at all (a pool wrapper
// with a call counter — the T-191 §7.2 spec prescription); today's
// episodes do not open the guard; missing bucket rows — an honest
// zero; the bucket reconciliation agrees with the live window. The
// pinned plan shape is checked by EXPLAIN in the same transaction with
// SET LOCAL (a seq scan over domains, the index forms disabled). The
// call level — like the neighbors: library calls without starting
// services; isolation — withTestSchema.
package integration

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/migrate"
	"crawler/internal/model"
	"crawler/internal/monitor"
	"crawler/internal/scheduler"
)

// retrySeqScanRe — a seq scan over domains (the d alias) in EXPLAIN:
// PG16 prints the leaf partition ("Seq Scan on domains_default d"),
// PG17+ — the parent ("Seq Scan on d").
var retrySeqScanRe = regexp.MustCompile(`Seq Scan on (\w+ )?d\b`)

// retryBatchPrefix — the beginning of retrySelectSQL (a literal from
// internal/scheduler/retry.go, the duplication — the schema_test.go
// style): the pool wrapper counts the heavy candidate selection calls
// — the T-202 guard must keep it behind a zero sum of maturity
// buckets. The selection runs INSIDE the plan pinning transaction, so
// the counter lives in the transaction wrapper.
const retryBatchPrefix = "SELECT d.zone_id, d.id, d.temp_attempts FROM domains d"

// retryBatchSQL — the full selection text for EXPLAIN (the LIMIT — as
// a literal: a parameter in EXPLAIN requires a prepared statement, the
// plan shape check does not care; the codes — status temp_error 5,
// check_state done 2).
const retryBatchSQL = retryBatchPrefix +
	" WHERE d.status = 5 AND d.check_state = 2 AND d.last_checked_at < DATE '2020-01-01' LIMIT 100"

// retryTracePool — a RetryDB pool wrapper: counts the heavy candidate
// selection calls (a pool wrapper with a call counter — the T-191 §7.2
// spec prescription).
type retryTracePool struct {
	*pgxpool.Pool

	mu    sync.Mutex
	batch int
}

func (p *retryTracePool) countBatch() {
	p.mu.Lock()
	p.batch++
	p.mu.Unlock()
}

func (p *retryTracePool) batchCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.batch
}

// Begin wraps the transaction with the selection counter.
func (p *retryTracePool) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &retryTraceTx{Tx: tx, pool: p}, nil
}

// retryTraceTx — a transaction wrapper: the heavy selection runs
// inside the plan pinning transaction (retryCandidates, T-191).
type retryTraceTx struct {
	pgx.Tx

	pool *retryTracePool
}

func (t *retryTraceTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if strings.HasPrefix(sql, retryBatchPrefix) {
		t.pool.countBatch()
	}
	return t.Tx.Query(ctx, sql, args...)
}

// retryMatureFailPool — a RetryDB pool wrapper: the transaction whose
// Exec of the tail maturity bucket decrement (retryRequeueMatureSQL,
// the second requeue assignment operator, T-267) fails — a check of
// the atomicity of the retry half: both the transition and the
// decrement roll back.
type retryMatureFailPool struct {
	*pgxpool.Pool
}

// retryMatureDecMark — the bucket decrement identifier in the SQL text
// (a literal duplication — the schema_test.go style: the constant is
// not exported).
const retryMatureDecMark = "FROM unnest($1::date[]) AS d"

func (p *retryMatureFailPool) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &retryMatureFailTx{Tx: tx}, nil
}

// retryMatureFailTx — a transaction wrapper: the Exec of the maturity
// bucket decrement fails, everything else is delegated.
type retryMatureFailTx struct {
	pgx.Tx
}

func (t *retryMatureFailTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.Contains(sql, "retry_mature") && strings.Contains(sql, retryMatureDecMark) {
		return pgconn.CommandTag{}, errors.New("bucket decrement failure (the scenario)")
	}
	return t.Tx.Exec(ctx, sql, args...)
}

// TestRetryTempErrorPass — the re-check pass on live PostgreSQL: a
// mature temp_error cohort is drained by batches (the iterations
// converge, the transitioned ones are not selected again), the
// maturity bucket decrements (T-202) zero the guard after the drain —
// the heavy selection is not run at all; today's episodes (immature)
// do not open the guard; missing bucket rows — an honest zero, not an
// error; the bucket reconciliation agrees with the live window and
// cleans up stale ones.
func TestRetryTempErrorPass(t *testing.T) {
	skipUnlessIntegration(t)
	ctx := context.Background()

	dsn, cleanup := withTestSchema(t)
	t.Cleanup(cleanup)
	if err := migrate.Up(ctx, dsn); err != nil {
		t.Fatalf("applying migrations to the isolated schema: %v", err)
	}
	raw, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting to the test schema: %v", err)
	}
	t.Cleanup(raw.Close)
	pool := &retryTracePool{Pool: raw}

	// The plan shape is pinned (§4.2 of the T-191 spec): inside the
	// SET LOCAL transaction (all index forms disabled) EXPLAIN shows a
	// seq scan over domains, no index nodes — the planner is forbidden
	// the degenerate range scan over the partial domains_recheck_idx.
	t.Run("the selection plan pinned by SET LOCAL", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		for _, pin := range []string{
			"SET LOCAL enable_indexscan = off",
			"SET LOCAL enable_bitmapscan = off",
			"SET LOCAL enable_indexonlyscan = off",
		} {
			if _, err := tx.Exec(ctx, pin); err != nil {
				t.Fatalf("%s: %v", pin, err)
			}
		}
		rows, err := tx.Query(ctx, "EXPLAIN (COSTS OFF) "+retryBatchSQL)
		if err != nil {
			t.Fatalf("EXPLAIN: %v", err)
		}
		defer rows.Close()
		var plan strings.Builder
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatalf("EXPLAIN: reading: %v", err)
			}
			plan.WriteString(line)
			plan.WriteByte('\n')
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("EXPLAIN: %v", err)
		}
		if !retrySeqScanRe.MatchString(plan.String()) {
			t.Errorf("the pinned plan has no seq scan over domains (the d alias):\n%s", plan.String())
		}
		if strings.Contains(plan.String(), "Index") || strings.Contains(plan.String(), "Bitmap") {
			t.Errorf("the pinned plan has an index form (SET LOCAL did not take effect):\n%s", plan.String())
		}
	})

	// The temp_error cohort: mature re-check candidates (done + older
	// than the window) — the requeue domains (1 attempt < N) and final
	// (3 attempts = N); the young and the unfinished are not candidates
	// (the RetryAfter window and check_state). last_checked_at — a
	// date literal in the past (the default window is 24 h, spec 5.3).
	// The maturity bucket of the past day is placed by hand (the value
	// — like the T-202 reconciliation: two mature candidates; the seed
	// is a direct INSERT, a direct INSERT does not keep the record
	// increments).
	var zone int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name) VALUES ('retry.test') RETURNING id",
	).Scan(&zone); err != nil {
		t.Fatalf("insert zones: %v", err)
	}
	seed := func(label string, status model.Status, state model.CheckState, checked string, attempts int16) {
		t.Helper()
		if _, err := pool.Exec(ctx,
			`INSERT INTO domains (zone_id, label, status, check_state, last_checked_at, temp_attempts)
			 VALUES ($1, $2, $3, $4, $5::date, $6)`,
			zone, label, int16(status), int16(state), checked, attempts,
		); err != nil {
			t.Fatalf("insert domains %s: %v", label, err)
		}
	}
	seed("requeue", model.StatusTempError, model.CheckStateDone, "2020-01-01", 1)
	seed("final", model.StatusTempError, model.CheckStateDone, "2020-01-01", int16(scheduler.DefaultRetryMaxRetries))
	seed("young", model.StatusTempError, model.CheckStateDone, "2999-01-01", 0)      // younger than the window — not a candidate
	seed("pending", model.StatusTempError, model.CheckStatePending, "2020-01-01", 0) // awaiting the dispatcher — not a candidate
	if _, err := pool.Exec(ctx,
		"INSERT INTO crawler_counters (name, value) "+
			"VALUES ('retry_mature:' || to_char(current_date - 1, 'YYYY-MM-DD'), 2)",
	); err != nil {
		t.Fatalf("insert crawler_counters: %v", err)
	}

	// retryMatureSum — the guard sum of the past-day buckets (the same
	// predicate as the scheduler retryMatureSQL).
	retryMatureSum := func(t *testing.T) int64 {
		t.Helper()
		var sum int64
		if err := pool.QueryRow(ctx,
			"SELECT coalesce(sum(value), 0) FROM crawler_counters "+
				"WHERE name >= 'retry_mature:' AND name < 'retry_mature:' || to_char(current_date, 'YYYY-MM-DD')",
		).Scan(&sum); err != nil {
			t.Fatalf("the guard sum of the buckets: %v", err)
		}
		return sum
	}

	// The drain by batches of 1 (T-031): BatchSize = 1 — every batch is
	// complete, the loop keeps iterating until exhaustion; the
	// transitioned (pending) and the immature ones are not selected
	// again. There are two iterations with the heavy selection (one per
	// candidate): each transition decrements the bucket (2 → 1 → 0),
	// the zero guard stops the drain BEFORE the next selection (T-202)
	// — an empty heavy selection does not happen here; it is the
	// backup stop of the drain when the buckets drift up.
	t.Run("the drain iterations converge: requeue and finalization", func(t *testing.T) {
		var logBuf strings.Builder
		logger := slog.New(slog.NewTextHandler(&logBuf, nil))

		drainCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- scheduler.RetryTempErrors(drainCtx, pool, scheduler.RetryOptions{
				BatchSize: 1,
				Interval:  time.Hour, // the tick never fires: the drain must handle everything
				Log:       logger,
			})
		}()

		// Two complete batches of 1; we wait for two heavy selections,
		// there must be no third (the guard is closed by the decrements;
		// Interval = an hour).
		deadline := time.Now().Add(10 * time.Second)
		for pool.batchCalls() < 2 {
			if time.Now().After(deadline) {
				cancel()
				t.Fatalf("the drain did not reach exhaustion: %d heavy selections", pool.batchCalls())
			}
			time.Sleep(5 * time.Millisecond)
		}
		time.Sleep(50 * time.Millisecond) // the drain must stop without a third selection
		if n := pool.batchCalls(); n != 2 {
			t.Fatalf("%d heavy selections, want 2: the zero guard closes the drain before the selection", n)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("RetryTempErrors: %v", err)
		}

		// The cohort state: a requeue — pending and +1 attempt; a final —
		// delegated_no_address and the counter reset; the young and the
		// unfinished ones are untouched.
		type domRow struct {
			status    model.Status
			state     model.CheckState
			attempts  int16
			lastCheck string
		}
		got := make(map[string]domRow)
		rows, err := pool.Query(ctx,
			`SELECT label, status, check_state, temp_attempts, last_checked_at::text FROM domains`,
		)
		if err != nil {
			t.Fatalf("select domains: %v", err)
		}
		defer rows.Close()
		for rows.Next() {
			var label string
			var r domRow
			var status, state int16
			if err := rows.Scan(&label, &status, &state, &r.attempts, &r.lastCheck); err != nil {
				t.Fatalf("select domains: reading: %v", err)
			}
			r.status, r.state = model.Status(status), model.CheckState(state)
			got[label] = r
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("select domains: %v", err)
		}
		if r := got["requeue"]; r.status != model.StatusTempError || r.state != model.CheckStatePending || r.attempts != 2 {
			t.Errorf("requeue = status %d state %d attempts %d, want temp_error/pending/2 (a retry assigned)", r.status, r.state, r.attempts)
		}
		if r := got["final"]; r.status != model.StatusDelegatedNoAddress || r.state != model.CheckStateDone || r.attempts != 0 {
			t.Errorf("final = status %d state %d attempts %d, want delegated_no_address/done/0 (the outcome by temp_attempts)", r.status, r.state, r.attempts)
		}
		if r := got["young"]; r.status != model.StatusTempError || r.state != model.CheckStateDone || r.attempts != 0 {
			t.Errorf("young touched: %+v, want temp_error/done/0 (younger than the window)", r)
		}
		if r := got["pending"]; r.status != model.StatusTempError || r.state != model.CheckStatePending || r.attempts != 0 {
			t.Errorf("pending touched: %+v, want temp_error/pending/0 (not a candidate)", r)
		}

		// The zone counter deltas (the zc CTE of retryRequeueSQL, T-178):
		// the only done → pending transition — via requeue.
		var checked, pend int64
		if err := pool.QueryRow(ctx,
			"SELECT domains_checked, domains_pending FROM zones WHERE id = $1", zone,
		).Scan(&checked, &pend); err != nil {
			t.Fatalf("select zones: %v", err)
		}
		if checked != -1 || pend != 1 {
			t.Errorf("zone counters = checked %d pending %d, want -1 and 1 (the done → pending transition)", checked, pend)
		}

		// The maturity buckets (T-202/T-267): the decrements of the tail
		// requeue assignment operator went into the episode date bucket
		// (2020-01-01, by −1 per requeue and finalization — the key is
		// derived from the dates of the transitioned rows passed by the
		// batch from the RETURNING of the first operator) — the guard sum
		// agrees with the live number of mature candidates (0).
		var stale int64
		if err := pool.QueryRow(ctx,
			"SELECT value FROM crawler_counters WHERE name = 'retry_mature:2020-01-01'",
		).Scan(&stale); err != nil {
			t.Fatalf("the episode date bucket: %v", err)
		}
		if stale != -2 {
			t.Errorf("the 2020-01-01 bucket = %d, want -2 (the requeue and finalization decrements)", stale)
		}
		if got := retryMatureSum(t); got != 0 {
			t.Errorf("the guard sum of the buckets = %d, want 0 (the mature exhausted, the drain will not open)", got)
		}

		// The drain outcome — the sum of the batches: one of each kind.
		for _, want := range []string{"requeued=1", "finalized=1"} {
			if !strings.Contains(logBuf.String(), want) {
				t.Errorf("the drain log does not contain %q:\n%s", want, logBuf.String())
			}
		}
	})

	// The deadlock 40P01 regression of the counters: the batch decisions
	// (the requeue assignment and the finalization) run in SEPARATE
	// transactions. The holder session holds SELECT FOR UPDATE of the
	// finalization candidate domains row — the way the result recording
	// transaction of the http-worker holds its own domains rows until
	// it reaches the tail counters
	// (checks:/temp_error:/retry_mature:). The retry half must commit
	// and release its locks (domains/zones/the retry_mature bucket —
	// the transition and the tail decrement as TWO operators of one
	// transaction, T-267) WITHOUT waiting for the finalization: in a
	// common batch transaction it would hang on a foreign domains row
	// while holding the counter bucket — the "counters → domains" edge
	// of the deadlock cycle with the result recording.
	t.Run("batch halves — independent transactions (the 40P01 regression)", func(t *testing.T) {
		seed("mixreq", model.StatusTempError, model.CheckStateDone, "2020-01-01", 0) // requeue
		seed("mixfin", model.StatusTempError, model.CheckStateDone, "2020-01-01", int16(scheduler.DefaultRetryMaxRetries))
		// The guard fixture is self-sufficient: the tails of the drain
		// subtest above — the yesterday bucket = 2 (the guard seed) and
		// the 2020-01-01 decrements = −2 (their sum is zero, but a new
		// pass must rely only on its own bucket) — are zeroed; the
		// episode date bucket of the candidates := 2. The rows are not
		// deleted: the reconciliation subtest below reads the yesterday
		// bucket.
		if _, err := pool.Exec(ctx,
			"UPDATE crawler_counters SET value = 0 WHERE name LIKE 'retry_mature:%'",
		); err != nil {
			t.Fatalf("zeroing the maturity buckets: %v", err)
		}
		if _, err := pool.Exec(ctx,
			"INSERT INTO crawler_counters (name, value, updated_at) "+
				"VALUES ('retry_mature:2020-01-01', 2, now()) "+
				"ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value",
		); err != nil {
			t.Fatalf("resetting the maturity bucket: %v", err)
		}

		// The holder: the finalization candidate domains row is captured
		// before the pass (an image of the result recording transaction).
		holder, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("beginning the holder: %v", err)
		}
		defer func() { _ = holder.Rollback(ctx) }()
		if _, err := holder.Exec(ctx,
			"SELECT id FROM domains WHERE zone_id = $1 AND label = $2 FOR UPDATE", zone, "mixfin",
		); err != nil {
			t.Fatalf("the holder FOR UPDATE: %v", err)
		}

		type onceResult struct {
			res scheduler.RetryResult
			err error
		}
		done := make(chan onceResult, 1)
		go func() {
			res, err := scheduler.RetryOnce(ctx, pool, scheduler.RetryOptions{})
			done <- onceResult{res, err}
		}()

		// While the holder has not released the finalization row, the
		// retry half is already committed and visible from another
		// session: retryRequeue is committed BEFORE waiting on
		// retryFinalize. Before the fix (a common batch transaction) the
		// requeue transition would hang together with the finalization —
		// the poll would hit the deadline.
		deadline := time.Now().Add(10 * time.Second)
		var state int16
		for time.Now().Before(deadline) {
			if err := pool.QueryRow(ctx,
				"SELECT check_state FROM domains WHERE zone_id = $1 AND label = $2", zone, "mixreq",
			).Scan(&state); err != nil {
				t.Fatalf("reading mixreq: %v", err)
			}
			if state == int16(model.CheckStatePending) {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		if state != int16(model.CheckStatePending) {
			t.Fatalf("the requeue half not committed before the finalization: check_state mixreq = %d", state)
		}

		// The holder releases the row — the finalization goes through,
		// the pass completes with the pair {1, 1}.
		if err := holder.Rollback(ctx); err != nil {
			t.Fatalf("rolling back the holder: %v", err)
		}
		select {
		case r := <-done:
			if r.err != nil {
				t.Fatalf("RetryOnce: %v", r.err)
			}
			if want := (scheduler.RetryResult{Requeued: 1, Finalized: 1}); r.res != want {
				t.Errorf("outcome = %+v, want %+v", r.res, want)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("RetryOnce did not finish after the row was released")
		}
		var finStatus int16
		if err := pool.QueryRow(ctx,
			"SELECT status FROM domains WHERE zone_id = $1 AND label = $2", zone, "mixfin",
		).Scan(&finStatus); err != nil {
			t.Fatalf("reading mixfin: %v", err)
		}
		if finStatus != int16(model.StatusDelegatedNoAddress) {
			t.Errorf("mixfin status = %d, want delegated_no_address (the finalization after the release)", finStatus)
		}
	})

	// The atomicity of the retry half (T-267): the transition and the
	// tail bucket decrement are TWO operators of ONE transaction; a
	// decrement failure rolls the half back as a whole — the domain
	// stays a candidate (done), the zone deltas and the buckets are
	// untouched. In contrast to the DNS stage result recording, where
	// the operators are spread across autocommits and the bucket is
	// caught up by the reconciliation (counters_test.go, the ±1
	// window).
	t.Run("a bucket decrement failure rolls back the requeue half", func(t *testing.T) {
		seed("mixfail", model.StatusTempError, model.CheckStateDone, "2020-01-01", 0)
		if _, err := pool.Exec(ctx,
			"INSERT INTO crawler_counters (name, value, updated_at) "+
				"VALUES ('retry_mature:2020-01-01', 1, now()) "+
				"ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value",
		); err != nil {
			t.Fatalf("resetting the maturity bucket: %v", err)
		}
		fail := &retryMatureFailPool{Pool: raw}

		var checkedBefore, pendBefore int64
		if err := pool.QueryRow(ctx,
			"SELECT domains_checked, domains_pending FROM zones WHERE id = $1", zone,
		).Scan(&checkedBefore, &pendBefore); err != nil {
			t.Fatalf("select zones before: %v", err)
		}
		if _, err := scheduler.RetryOnce(ctx, fail, scheduler.RetryOptions{BatchSize: 1}); err == nil ||
			!strings.Contains(err.Error(), "maturity bucket decrement") {
			t.Fatalf("RetryOnce = %v, want a maturity bucket decrement error", err)
		}
		var state, attempts int16
		if err := pool.QueryRow(ctx,
			"SELECT check_state, temp_attempts FROM domains WHERE zone_id = $1 AND label = $2", zone, "mixfail",
		).Scan(&state, &attempts); err != nil {
			t.Fatalf("reading mixfail: %v", err)
		}
		if model.CheckState(state) != model.CheckStateDone || attempts != 0 {
			t.Fatalf("mixfail = state %d attempts %d, want done/0 (the transition rolled back together with the decrement)",
				state, attempts)
		}
		var checked, pend int64
		if err := pool.QueryRow(ctx,
			"SELECT domains_checked, domains_pending FROM zones WHERE id = $1", zone,
		).Scan(&checked, &pend); err != nil {
			t.Fatalf("select zones: %v", err)
		}
		if checked != checkedBefore || pend != pendBefore {
			t.Errorf("the zone deltas changed on the rollback: checked %d pending %d, want unchanged (%d/%d)",
				checked, pend, checkedBefore, pendBefore)
		}
		var basket int64
		if err := pool.QueryRow(ctx,
			"SELECT value FROM crawler_counters WHERE name = 'retry_mature:2020-01-01'",
		).Scan(&basket); err != nil {
			t.Fatalf("the episode date bucket: %v", err)
		}
		if basket != 1 {
			t.Errorf("the 2020-01-01 bucket = %d, want 1 (the decrement rolled back together with the transition)", basket)
		}
		// The candidate is intact — the next pass (the live pool)
		// handles it: the transition and the decrement go together, the
		// zone deltas land.
		res, err := scheduler.RetryOnce(ctx, pool, scheduler.RetryOptions{BatchSize: 1})
		if err != nil {
			t.Fatalf("a repeated pass: %v", err)
		}
		if res.Requeued != 1 {
			t.Errorf("a repeated pass = %+v, want requeued=1 (the candidate survived in done)", res)
		}
		if err := pool.QueryRow(ctx,
			"SELECT domains_checked, domains_pending FROM zones WHERE id = $1", zone,
		).Scan(&checked, &pend); err != nil {
			t.Fatalf("select zones after the pass: %v", err)
		}
		if checked != checkedBefore-1 || pend != pendBefore+1 {
			t.Errorf("the zone deltas after the pass: checked %d pending %d, want %d and %d (done → pending)",
				checked, pend, checkedBefore-1, pendBefore+1)
		}
	})

	// Steady state (the guard trust boundary, the T-202 TZ): no mature
	// rows — the heavy selection is not run at all, the tick stands for
	// one bucket sum.
	t.Run("no mature ones — the heavy selection does not run", func(t *testing.T) {
		before := pool.batchCalls()
		res, err := scheduler.RetryOnce(ctx, pool, scheduler.RetryOptions{})
		if err != nil {
			t.Fatalf("RetryOnce: %v", err)
		}
		if res != (scheduler.RetryResult{}) {
			t.Fatalf("outcome %+v, want empty (the guard: no mature ones)", res)
		}
		if n := pool.batchCalls(); n != before {
			t.Fatalf("the heavy selection ran %d times with a zero bucket sum (was %d)", n-before, before)
		}
	})

	// The immature (today's episodes, T-202): the current-day bucket is
	// not in the guard — the episodes mature by midnight, the heavy
	// selection does not open, although the domain formally answered
	// the status criterion.
	t.Run("today's episodes — the guard does not let them through", func(t *testing.T) {
		// The episode date — the current_date of the DB ITSELF (the bind
		// parameter $5::date accepts only a literal, "current_date" as a
		// string is not parsed; the value from the DB — the timezone is
		// aligned with the reconciliation FILTER).
		var today string
		if err := pool.QueryRow(ctx,
			"SELECT to_char(current_date, 'YYYY-MM-DD')",
		).Scan(&today); err != nil {
			t.Fatalf("current_date: %v", err)
		}
		seed("fresh", model.StatusTempError, model.CheckStateDone, today, 0)
		if _, err := pool.Exec(ctx,
			"INSERT INTO crawler_counters (name, value) "+
				"VALUES ('retry_mature:' || to_char(current_date, 'YYYY-MM-DD'), 1)",
		); err != nil {
			t.Fatalf("inserting the day bucket: %v", err)
		}
		before := pool.batchCalls()
		res, err := scheduler.RetryOnce(ctx, pool, scheduler.RetryOptions{})
		if err != nil {
			t.Fatalf("RetryOnce: %v", err)
		}
		if res != (scheduler.RetryResult{}) {
			t.Fatalf("outcome %+v, want empty (today's episode is immature)", res)
		}
		if n := pool.batchCalls(); n != before {
			t.Fatalf("the heavy selection ran with immature buckets: %d calls (was %d)", n-before, before)
		}
	})

	// The reconciliation (T-202) — the authority of the buckets: the
	// today bucket := the live window (fresh — one temp_error × done
	// today), the mature one — 0, the stale bucket of the old episode
	// dates (the tail of the 2020-01-01 decrements) is removed; the
	// guard sum stays zero (the today bucket is not in the sum).
	t.Run("the bucket reconciliation converges with the live window", func(t *testing.T) {
		if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
			t.Fatalf("ZoneStatsOnce: %v", err)
		}
		var today, past int64
		if err := pool.QueryRow(ctx,
			"SELECT value FROM crawler_counters WHERE name = 'retry_mature:' || to_char(current_date, 'YYYY-MM-DD')",
		).Scan(&today); err != nil {
			t.Fatalf("the day bucket: %v", err)
		}
		if today != 1 {
			t.Errorf("the day bucket after the pass = %d, want 1 (fresh — one episode today)", today)
		}
		if err := pool.QueryRow(ctx,
			"SELECT value FROM crawler_counters WHERE name = 'retry_mature:' || to_char(current_date - 1, 'YYYY-MM-DD')",
		).Scan(&past); err != nil {
			t.Fatalf("the mature bucket: %v", err)
		}
		if past != 0 {
			t.Errorf("the mature bucket after the pass = %d, want 0 (no live mature ones)", past)
		}
		var stale int
		if err := pool.QueryRow(ctx,
			"SELECT count(*) FROM crawler_counters WHERE name = 'retry_mature:2020-01-01'",
		).Scan(&stale); err != nil {
			t.Fatalf("the stale bucket: %v", err)
		}
		if stale != 0 {
			t.Error("the stale 2020-01-01 bucket alive after the pass (the T-202 cleanup did not fire)")
		}
		if got := retryMatureSum(t); got != 0 {
			t.Errorf("the guard sum of the buckets = %d, want 0 (today's bucket is not in the sum)", got)
		}
	})

	// Missing bucket rows — an honest zero, not an error: the bucket
	// rows are created by the first day increment or the reconciliation
	// pass, nobody guarantees them (unlike the status_* of T-178).
	t.Run("no buckets — an honest zero", func(t *testing.T) {
		if _, err := pool.Exec(ctx, "DELETE FROM crawler_counters WHERE name LIKE 'retry_mature:%'"); err != nil {
			t.Fatal(err)
		}
		before := pool.batchCalls()
		res, err := scheduler.RetryOnce(ctx, pool, scheduler.RetryOptions{})
		if err != nil {
			t.Fatalf("RetryOnce: %v (missing buckets are not an error: coalesce gives zero)", err)
		}
		if res != (scheduler.RetryResult{}) {
			t.Fatalf("outcome %+v, want empty", res)
		}
		if n := pool.batchCalls(); n != before {
			t.Fatalf("the heavy selection ran without buckets: %d calls (was %d)", n-before, before)
		}
	})
}

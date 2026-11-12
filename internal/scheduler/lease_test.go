// Unit tests of returning domains with an expired lease (T-029; the T-200 form —
// a batch SELECT by cursor + a move by PK probes, spec
// TZ_slow_lease_return.md §7.1) on a fake pgx pool with transactions
// (the retry_test.go style): the exact SQL texts of both statements and parameters
// (the now() − lease boundary, the queued guard, the queued_at reset, the
// check_state/status codes, the composite cursor), the pair of statements in one
// transaction (Begin → SELECT → UPDATE → Commit), counting by RETURNING,
// an empty batch does not move the cursor, the guard filtered out a worker-written
// result, errors and ctx cancellation, the LeaseWatch cycle (an immediate drain,
// the interval, the cursor lives between batches/drains, a restart resets it,
// surviving an error, a clean exit). Behavior against a live DB —
// integration tests (test/integration/lease_test.go).
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

// --- fake pool with transactions ------------------------------------------
//
// Names prefixed with lease: fakes of other scheduler tasks live
// in the package in parallel (retryFakePool T-191); no common helpers
// are introduced here (except retryRows — the positional Scan is reused).

// leaseCall — a recorded SQL call with arguments.
type leaseCall struct {
	sql  string
	args []any
}

// leaseBatch — a stubbed pass response: the SELECT batch (rows in
// index key order) and the move outcome translated — RETURNING rows
// (the guard could have filtered out part of the batch: translated < len(rows)).
type leaseBatch struct {
	rows       []leaseRow
	translated int64
}

// leaseNullBatch — a repair pass stub (leasenull.go): the
// SELECT batch (keys of queued/NULL rows) and the move outcome.
type leaseNullBatch struct {
	rows       []leaseNullRow
	translated int64
}

// leaseFakePool — a LeaseDB without a DB: the queue of SELECT batches (ran out —
// an empty batch), per-step pass errors, the call protocol and the
// transaction operation order. Everything under a lock: the LeaseWatch loop runs in
// a goroutine; the test waits for progress by counters. The repair pass
// of the guard (FR-1.2) — the same fields with the null prefix: the main
// pass counters are checked by the tests separately.
type leaseFakePool struct {
	mu         sync.Mutex
	batches    []leaseBatch // the FIFO of SELECT batches
	selectErrs []error      // the FIFO of SELECT errors (nil — no error)
	updateErrs []error      // the FIFO of UPDATE errors (nil — no error)
	beginErr   error
	commitErrs []error // the FIFO of Commit errors (ran out — a commit without an error)

	nullBatches     []leaseNullBatch // the FIFO of guard batches
	nullSelectErrs  []error
	nullUpdateErrs  []error
	nullSelectCalls []leaseCall
	nullUpdateCalls []leaseCall

	ops         []string    // the operation order: begin/select/update/commit
	begins      int         // transactions opened
	commits     int         // committed
	selectCalls []leaseCall // the batch SELECT (cursor check)
	updateCalls []leaseCall // the move UPDATE
}

func (p *leaseFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom must not be called")
}

func (p *leaseFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("Exec must not be called")
}

func (p *leaseFakePool) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("Query via the pool must not be called — both statements are in one transaction")
}

func (p *leaseFakePool) Begin(context.Context) (pgx.Tx, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.begins++
	p.ops = append(p.ops, "begin")
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	tx := &leaseFakeTx{pool: p}
	return tx, nil
}

// nextErrLocked — the next error of the FIFO queue (a nil element — a step without
// an error); ok — the queue is non-empty.
func nextErrLocked(q *[]error) (error, bool) {
	if len(*q) == 0 {
		return nil, false
	}
	err := (*q)[0]
	*q = (*q)[1:]
	return err, true
}

// nextBatchLocked — the next FIFO batch (ran out — empty: the set
// is exhausted).
func (p *leaseFakePool) nextBatchLocked() leaseBatch {
	if len(p.batches) == 0 {
		return leaseBatch{}
	}
	b := p.batches[0]
	p.batches = p.batches[1:]
	return b
}

// nextNullBatchLocked — the next guard FIFO batch (ran out —
// empty).
func (p *leaseFakePool) nextNullBatchLocked() leaseNullBatch {
	if len(p.nullBatches) == 0 {
		return leaseNullBatch{}
	}
	b := p.nullBatches[0]
	p.nullBatches = p.nullBatches[1:]
	return b
}

// stats — a snapshot of the counters under the lock (waiting for loop progress).
func (p *leaseFakePool) stats() (begins, selects, updates, commits int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.begins, len(p.selectCalls), len(p.updateCalls), p.commits
}

// opsSnapshot — a snapshot of the operation order under the lock.
func (p *leaseFakePool) opsSnapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.ops...)
}

// selectSnapshot — a snapshot of the SELECT protocol under the lock.
func (p *leaseFakePool) selectSnapshot() []leaseCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]leaseCall(nil), p.selectCalls...)
}

// updateSnapshot — a snapshot of the UPDATE protocol under the lock.
func (p *leaseFakePool) updateSnapshot() []leaseCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]leaseCall(nil), p.updateCalls...)
}

// nullSelectSnapshot — a snapshot of the guard SELECT protocol under the lock.
func (p *leaseFakePool) nullSelectSnapshot() []leaseCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]leaseCall(nil), p.nullSelectCalls...)
}

// nullUpdateSnapshot — a snapshot of the guard UPDATE protocol under the lock.
func (p *leaseFakePool) nullUpdateSnapshot() []leaseCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]leaseCall(nil), p.nullUpdateCalls...)
}

// leaseFakeTx — the fake pool's transaction: the batch SELECT and the move UPDATE
// (both must run in it — the pool does not accept statements directly); it remembers
// the batch of this SELECT: the UPDATE must hit exactly its keys. The
// repair pass statements of the guard (FR-1.2) — by the same transaction.
type leaseFakeTx struct {
	pgx.Tx // the other pgx.Tx methods are not used by the pass

	pool      *leaseFakePool
	batch     leaseBatch // the SELECT batch of this transaction
	nullBatch leaseNullBatch
	committed bool
}

func (t *leaseFakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	switch sql {
	case leaseSelectSQL:
		t.pool.selectCalls = append(t.pool.selectCalls, leaseCall{sql: sql, args: args})
		if err, ok := nextErrLocked(&t.pool.selectErrs); ok && err != nil {
			return nil, err
		}
		t.batch = t.pool.nextBatchLocked()
		t.pool.ops = append(t.pool.ops, "select")
		rows := make([][]any, len(t.batch.rows))
		for i, r := range t.batch.rows {
			rows[i] = []any{r.zone, r.id, r.queuedAt}
		}
		return retryRows(rows...), nil
	case leaseNullSelectSQL:
		t.pool.nullSelectCalls = append(t.pool.nullSelectCalls, leaseCall{sql: sql, args: args})
		if err, ok := nextErrLocked(&t.pool.nullSelectErrs); ok && err != nil {
			return nil, err
		}
		t.nullBatch = t.pool.nextNullBatchLocked()
		t.pool.ops = append(t.pool.ops, "null_select")
		rows := make([][]any, len(t.nullBatch.rows))
		for i, r := range t.nullBatch.rows {
			rows[i] = []any{r.zone, r.id}
		}
		return retryRows(rows...), nil
	case leaseNullUpdateSQL:
		t.pool.nullUpdateCalls = append(t.pool.nullUpdateCalls, leaseCall{sql: sql, args: args})
		if err, ok := nextErrLocked(&t.pool.nullUpdateErrs); ok && err != nil {
			return nil, err
		}
		if len(args) != 3 {
			return nil, fmt.Errorf("leaseFakeTx: the guard move: %d parameters, need 3", len(args))
		}
		zones, okZ := args[1].([]int16)
		ids, okI := args[2].([]int64)
		if !okZ || !okI {
			return nil, fmt.Errorf("leaseFakeTx: the guard move: keys %T/%T, need []int16/[]int64", args[1], args[2])
		}
		if len(zones) != len(t.nullBatch.rows) || len(ids) != len(t.nullBatch.rows) {
			return nil, fmt.Errorf("leaseFakeTx: the guard move: %d keys, %d selected — it must punch through the batch",
				len(zones), len(t.nullBatch.rows))
		}
		for i, r := range t.nullBatch.rows {
			if zones[i] != r.zone || ids[i] != r.id {
				return nil, fmt.Errorf("leaseFakeTx: the guard move: key %d = (%d,%d), selected (%d,%d)",
					i, zones[i], ids[i], r.zone, r.id)
			}
		}
		t.pool.ops = append(t.pool.ops, "null_update")
		rows := make([][]any, t.nullBatch.translated)
		for i := range rows {
			rows[i] = []any{int16(1)} // zone_id: the value does not matter for row counting
		}
		return retryRows(rows...), nil
	case leaseUpdateSQL:
		t.pool.updateCalls = append(t.pool.updateCalls, leaseCall{sql: sql, args: args})
		if err, ok := nextErrLocked(&t.pool.updateErrs); ok && err != nil {
			return nil, err
		}
		// The move contract: the unnest keys are exactly the SELECT batch of this same
		// transaction, in selection order.
		if len(args) != 5 {
			return nil, fmt.Errorf("leaseFakeTx: the move: %d parameters, need 5", len(args))
		}
		zones, okZ := args[1].([]int16)
		ids, okI := args[2].([]int64)
		if !okZ || !okI {
			return nil, fmt.Errorf("leaseFakeTx: the move: keys %T/%T, need []int16/[]int64", args[1], args[2])
		}
		if len(zones) != len(t.batch.rows) || len(ids) != len(t.batch.rows) {
			return nil, fmt.Errorf("leaseFakeTx: the move: %d keys, %d selected — it must punch through the batch",
				len(zones), len(t.batch.rows))
		}
		for i, r := range t.batch.rows {
			if zones[i] != r.zone || ids[i] != r.id {
				return nil, fmt.Errorf("leaseFakeTx: the move: key %d = (%d,%d), selected (%d,%d)",
					i, zones[i], ids[i], r.zone, r.id)
			}
		}
		t.pool.ops = append(t.pool.ops, "update")
		rows := make([][]any, t.batch.translated)
		for i := range rows {
			rows[i] = []any{int16(1)} // zone_id: the value does not matter for row counting
		}
		return retryRows(rows...), nil
	default:
		return nil, fmt.Errorf("leaseFakeTx: unexpected SQL in a transaction: %s", sql)
	}
}

func (t *leaseFakeTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.committed = true
	t.pool.commits++
	t.pool.ops = append(t.pool.ops, "commit")
	if err, ok := nextErrLocked(&t.pool.commitErrs); ok && err != nil {
		return err
	}
	return nil
}

func (t *leaseFakeTx) Rollback(context.Context) error { return nil }

// --- batch stubs -------------------------------------------------------

// leaseSeedBatch — a batch of n rows: queuedAt strictly grows with i (the last
// row is the key maximum: we check the cursor after the batch by it); ids in
// order, one zone.
func leaseSeedBatch(n int, translated int64, base time.Time) leaseBatch {
	rows := make([]leaseRow, n)
	for i := range rows {
		rows[i] = leaseRow{
			zone:     1,
			id:       int64(i + 1),
			queuedAt: base.Add(time.Duration(i) * time.Minute),
		}
	}
	return leaseBatch{rows: rows, translated: translated}
}

// --- expected SQL texts (contract, T-200 spec §§4.1–4.2) ------------------

const wantLeaseSelect = "SELECT zone_id, id, queued_at FROM domains " +
	"WHERE check_state = 1 AND status <> 6 " +
	"AND queued_at < COALESCE($1::timestamptz, now()) - make_interval(secs => $2) " +
	"AND (queued_at, zone_id, id) > ($3::timestamptz, $4::int2, $5::int8) " +
	"ORDER BY queued_at, zone_id, id LIMIT $6"

const wantLeaseUpdate = "WITH upd AS (UPDATE domains AS t " +
	"SET check_state = $1, queued_at = NULL " +
	"FROM unnest($2::int2[], $3::int8[]) AS u(zone_id, id) " +
	"WHERE t.zone_id = u.zone_id AND t.id = u.id " +
	"AND t.check_state = 1 AND t.status <> 6 " +
	"AND t.queued_at < COALESCE($4::timestamptz, now()) - make_interval(secs => $5) " +
	"RETURNING t.zone_id), " +
	"zc AS (UPDATE zones AS z " +
	"SET domains_queued = z.domains_queued - c.n, " +
	"domains_pending = z.domains_pending + c.n " +
	"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c " +
	"WHERE z.id = c.zone_id) " +
	"SELECT zone_id FROM upd"

// leaseFixedClock — a clock with a fixed "now" (the pass threshold
// reproducible to the nanosecond).
func leaseFixedClock() (time.Time, func() time.Time) {
	now := time.Date(2026, 10, 17, 4, 30, 0, 0, time.UTC)
	return now, func() time.Time { return now }
}

// leaseCursorArgs — the cursor arguments of the SELECT call (positions 2–4).
func leaseCursorArgs(c leaseCall) (time.Time, int16, int64) {
	q, _ := c.args[2].(time.Time)
	z, _ := c.args[3].(int16)
	i, _ := c.args[4].(int64)
	return q, z, i
}

// --- LeaseOnce -----------------------------------------------------------

// TestLeaseOnceSQL — the contract of both statements (spec §§4.1–4.2): the batch SELECT
// with the composite cursor and the server clock threshold, UPDATE … FROM unnest with
// guard re-checks and the queued_at reset; exact parameters, the operation order
// of one transaction (Begin → SELECT → UPDATE → Commit), counting by RETURNING,
// the cursor — past the last row of the batch.
func TestLeaseOnceSQL(t *testing.T) {
	now, clock := leaseFixedClock()
	base := time.Date(2026, 10, 17, 4, 0, 0, 0, time.UTC)
	pool := &leaseFakePool{batches: []leaseBatch{leaseSeedBatch(3, 2, base)}}

	var cursor LeaseCursor
	res, err := LeaseOnce(context.Background(), pool, &cursor, LeaseOptions{
		Timeout:   15 * time.Minute,
		BatchSize: 8000,
		Now:       clock,
	})
	if err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}

	if ops := pool.opsSnapshot(); !reflect.DeepEqual(ops, []string{"begin", "select", "update", "commit"}) {
		t.Errorf("the operation order = %v, want [begin select update commit] — the statement pair in one transaction", ops)
	}

	sel := pool.selectSnapshot()
	if len(sel) != 1 {
		t.Fatalf("SELECT calls = %d, want 1", len(sel))
	}
	if sel[0].sql != wantLeaseSelect {
		t.Errorf("selection SQL:\n%s\nwant:\n%s", sel[0].sql, wantLeaseSelect)
	}
	wantSelArgs := []any{
		&now,                            // the test clocks' "now" (in production NULL → the DB's now())
		(15 * time.Minute).Seconds(),    // the lease term — make_interval
		time.Time{}, int16(0), int64(0), // a zero cursor — from the beginning of the index
		8000, // the batch LIMIT
	}
	if !reflect.DeepEqual(sel[0].args, wantSelArgs) {
		t.Errorf("selection args = %v, want %v", sel[0].args, wantSelArgs)
	}

	upd := pool.updateSnapshot()
	if len(upd) != 1 {
		t.Fatalf("UPDATE-calls = %d, want 1", len(upd))
	}
	if upd[0].sql != wantLeaseUpdate {
		t.Errorf("move SQL:\n%s\nwant:\n%s", upd[0].sql, wantLeaseUpdate)
	}
	wantUpdArgs := []any{
		int16(model.CheckStatePending), // SET check_state
		[]int16{1, 1, 1},               // the zones of the selected batch
		[]int64{1, 2, 3},               // the domains of the selected batch
		&now,                           // the same "now" — the threshold is one
		(15 * time.Minute).Seconds(),
	}
	if !reflect.DeepEqual(upd[0].args, wantUpdArgs) {
		t.Errorf("move args = %v, want %v", upd[0].args, wantUpdArgs)
	}

	// Counting by RETURNING: 2 of the selected 3 moved (the guard filtered out one).
	if res.Domains != 2 || res.Selected != 3 {
		t.Errorf("result = %+v, want Domains=2 (RETURNING), Selected=3", res)
	}
	// The cursor — the last row of the batch (the minute grid of the seed stub).
	want := leaseSeedBatch(3, 0, base).rows[2]
	if cursor.queuedAt != want.queuedAt || cursor.zone != want.zone || cursor.id != want.id {
		t.Errorf("cursor = (%v,%d,%d), want the batch's last row (%v,%d,%d)",
			cursor.queuedAt, cursor.zone, cursor.id, want.queuedAt, want.zone, want.id)
	}
}

// TestLeaseOnceClockIsServer — without test clocks the "now" is not passed
// at all (NULL) to either the selection or the move: the threshold is counted by the DB server's now(),
// not by the scheduler process's clock.
func TestLeaseOnceClockIsServer(t *testing.T) {
	pool := &leaseFakePool{batches: []leaseBatch{leaseSeedBatch(1, 1, time.Now())}}
	if _, err := LeaseOnce(context.Background(), pool, nil, LeaseOptions{}); err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}
	selArgs := pool.selectSnapshot()[0].args
	if got, ok := selArgs[0].(*time.Time); !ok || got != nil {
		t.Errorf("the selection \"now\" = %v (%T), want a nil *time.Time — the DB server clock", selArgs[0], selArgs[0])
	}
	if got, ok := selArgs[1].(float64); !ok || got != (30*time.Minute).Seconds() {
		t.Errorf("the expiry = %v, want %v seconds of the default", selArgs[1], (30 * time.Minute).Seconds())
	}
	updArgs := pool.updateSnapshot()[0].args
	if got, ok := updArgs[3].(*time.Time); !ok || got != nil {
		t.Errorf("the move \"now\" = %v (%T), want nil *time.Time", updArgs[3], updArgs[3])
	}
}

// TestLeaseOnceDefaults — zero options: the lease term and batch are the defaults
// (30 minutes per spec 5.3, 5000); the clocks are fixed: the "now" and the term go
// into the SQL as parameters.
func TestLeaseOnceDefaults(t *testing.T) {
	now, clock := leaseFixedClock()
	pool := &leaseFakePool{batches: []leaseBatch{leaseSeedBatch(1, 1, time.Now())}}

	if _, err := LeaseOnce(context.Background(), pool, nil, LeaseOptions{Now: clock}); err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}

	args := pool.selectSnapshot()[0].args
	if args[5] != DefaultLeaseBatch {
		t.Errorf("LIMIT = %v, want the default %d", args[5], DefaultLeaseBatch)
	}
	if got, ok := args[0].(*time.Time); !ok || !got.Equal(now) {
		t.Errorf("\"now\" = %v, want the fixed clock %v", args[0], now)
	}
	if got, ok := args[1].(float64); !ok || got != DefaultLeaseTimeout.Seconds() {
		t.Errorf("the expiry = %v, want the default %s in seconds", args[1], DefaultLeaseTimeout)
	}
}

// TestLeaseOnceEmpty — an exhausted set: an empty batch — only the SELECT
// (nothing to move, the UPDATE is not executed); the transaction is not committed,
// the outcome is zero, the cursor does not move.
func TestLeaseOnceEmpty(t *testing.T) {
	pool := &leaseFakePool{} // the batch queue is empty

	var cursor LeaseCursor
	res, err := LeaseOnce(context.Background(), pool, &cursor, LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}
	if res != (LeaseResult{}) {
		t.Errorf("result = %+v, want zero", res)
	}
	if _, selects, updates, commits := pool.stats(); selects != 1 || updates != 0 || commits != 0 {
		t.Errorf("selects=%d updates=%d commits=%d, want 1/0/0 — an empty batch without a move",
			selects, updates, commits)
	}
	if (cursor != LeaseCursor{}) {
		t.Errorf("cursor = %+v, want zero — an empty batch does not advance", cursor)
	}
	if ops := pool.opsSnapshot(); !reflect.DeepEqual(ops, []string{"begin", "select"}) {
		t.Errorf("the operation order = %v, want [begin select]", ops)
	}
}

// TestLeaseOnceCursorMoves — the cursor moves past the last row
// of a non-empty batch and is passed to the next pass (the SELECT args); an empty
// batch does not touch it.
func TestLeaseOnceCursorMoves(t *testing.T) {
	base := time.Date(2026, 10, 17, 3, 0, 0, 0, time.UTC)
	first := leaseSeedBatch(2, 2, base)                 // rows base+0, base+1m
	second := leaseSeedBatch(3, 3, base.Add(time.Hour)) // rows base+1h+{0,1,2}m
	pool := &leaseFakePool{batches: []leaseBatch{first, second}}

	var cursor LeaseCursor
	if _, err := LeaseOnce(context.Background(), pool, &cursor, LeaseOptions{}); err != nil {
		t.Fatalf("the first pass: %v", err)
	}
	want := first.rows[len(first.rows)-1]
	if cursor.queuedAt != want.queuedAt || cursor.id != want.id {
		t.Fatalf("the cursor after the batch = (%v,%d), want the last row (%v,%d)",
			cursor.queuedAt, cursor.id, want.queuedAt, want.id)
	}

	// The second pass receives the first pass's cursor in the SELECT arguments (a snapshot
	// of the cursor before the second pass — after that it moves past the second batch).
	cursorAfterFirst := cursor
	if _, err := LeaseOnce(context.Background(), pool, &cursor, LeaseOptions{}); err != nil {
		t.Fatalf("the second pass: %v", err)
	}
	q, z, i := leaseCursorArgs(pool.selectSnapshot()[1])
	if q != cursorAfterFirst.queuedAt || z != cursorAfterFirst.zone || i != cursorAfterFirst.id {
		t.Errorf("the second SELECT's cursor = (%v,%d,%d), want the passed (%v,%d,%d)",
			q, z, i, cursorAfterFirst.queuedAt, cursorAfterFirst.zone, cursorAfterFirst.id)
	}

	// An empty batch (the queue ran out) does not move the cursor.
	if _, err := LeaseOnce(context.Background(), pool, &cursor, LeaseOptions{}); err != nil {
		t.Fatalf("the third pass: %v", err)
	}
	if cursor.queuedAt != second.rows[len(second.rows)-1].queuedAt {
		t.Errorf("the cursor after an empty batch = %v, want the previous %v",
			cursor.queuedAt, second.rows[len(second.rows)-1].queuedAt)
	}
}

// TestLeaseOnceGuardFiltered — a result write between the selection and
// the lock (a race with a worker): the move hits the ENTIRE selected batch
// (the unnest keys — the selected set), but the count is by RETURNING: rows filtered out
// by the guard do not get into Domains; the cursor moves past the batch anyway
// (the filtered-out ones' fate changed — they do not exist under the old key).
func TestLeaseOnceGuardFiltered(t *testing.T) {
	base := time.Date(2026, 10, 17, 5, 0, 0, 0, time.UTC)
	pool := &leaseFakePool{batches: []leaseBatch{leaseSeedBatch(5, 1, base)}}

	var cursor LeaseCursor
	res, err := LeaseOnce(context.Background(), pool, &cursor, LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}
	if res.Domains != 1 || res.Selected != 5 {
		t.Fatalf("result = %+v, want Domains=1 (RETURNING after the guard), Selected=5", res)
	}
	upd := pool.updateSnapshot()
	if got := len(upd[0].args[1].([]int16)); got != 5 {
		t.Errorf("move unnest keys = %d, want 5 — the whole selected batch is punched through", got)
	}
	want := leaseSeedBatch(5, 0, base).rows[4]
	if cursor.queuedAt != want.queuedAt || cursor.id != want.id {
		t.Errorf("cursor = (%v,%d), want past the batch's last row (%v,%d)",
			cursor.queuedAt, cursor.id, want.queuedAt, want.id)
	}
}

// TestLeaseOnceRepeatPasses — moved rows are not selected again
// (the selection condition is in the SQL itself): each pass is its own transaction; after
// exhaustion — a zero outcome.
func TestLeaseOnceRepeatPasses(t *testing.T) {
	base := time.Date(2026, 10, 17, 6, 0, 0, 0, time.UTC)
	pool := &leaseFakePool{batches: []leaseBatch{
		leaseSeedBatch(5, 5, base),
		leaseSeedBatch(7, 7, base.Add(time.Hour)),
	}}

	var cursor LeaseCursor
	first, err := LeaseOnce(context.Background(), pool, &cursor, LeaseOptions{})
	if err != nil {
		t.Fatalf("the first pass: %v", err)
	}
	second, err := LeaseOnce(context.Background(), pool, &cursor, LeaseOptions{})
	if err != nil {
		t.Fatalf("the second pass: %v", err)
	}
	third, err := LeaseOnce(context.Background(), pool, &cursor, LeaseOptions{})
	if err != nil {
		t.Fatalf("the third pass: %v", err)
	}
	if first.Domains != 5 || second.Domains != 7 || third.Domains != 0 {
		t.Errorf("passes = %d, %d, %d; want 5, 7, 0",
			first.Domains, second.Domains, third.Domains)
	}
	if _, selects, _, commits := pool.stats(); selects != 3 || commits != 2 {
		t.Errorf("selects=%d commits=%d, want 3/2 (an empty pass without a commit)", selects, commits)
	}
}

// TestLeaseOnceNilCursor — a nil cursor: a one-off pass from the beginning of the index
// without persisting (the ReanalyzeTick style) — works and does not panic; each
// call starts from the zero cursor.
func TestLeaseOnceNilCursor(t *testing.T) {
	pool := &leaseFakePool{batches: []leaseBatch{
		leaseSeedBatch(2, 2, time.Now()),
		leaseSeedBatch(2, 2, time.Now().Add(time.Hour)),
	}}
	for i := 0; i < 2; i++ {
		if _, err := LeaseOnce(context.Background(), pool, nil, LeaseOptions{}); err != nil {
			t.Fatalf("pass %d with a nil cursor: %v", i+1, err)
		}
	}
	for i, call := range pool.selectSnapshot() {
		if q, z, id := leaseCursorArgs(call); !q.IsZero() || z != 0 || id != 0 {
			t.Errorf("SELECT %d with a nil cursor: the cursor (%v,%d,%d), want zero", i, q, z, id)
		}
	}
}

// TestLeaseOnceError — pass step errors (selection, move, transaction,
// commit) are wrapped with the task context; a failure does NOT move the cursor —
// a repeated pass takes the batch again; ctx cancellation is preserved via errors.Is.
func TestLeaseOnceError(t *testing.T) {
	base := time.Date(2026, 10, 17, 7, 0, 0, 0, time.UTC)

	selectPool := &leaseFakePool{
		batches:    []leaseBatch{leaseSeedBatch(2, 2, base)},
		selectErrs: []error{errors.New("db unavailable")},
	}
	var cursor LeaseCursor
	_, err := LeaseOnce(context.Background(), selectPool, &cursor, LeaseOptions{})
	if err == nil ||
		!strings.Contains(err.Error(), "scheduler: expired lease return") ||
		!strings.Contains(err.Error(), "db unavailable") {
		t.Errorf("the selection error = %v, want the task context and the original error", err)
	}
	if (cursor != LeaseCursor{}) {
		t.Errorf("the cursor advanced on a selection error: %+v", cursor)
	}

	cancelPool := &leaseFakePool{
		batches:    []leaseBatch{leaseSeedBatch(2, 2, base)},
		updateErrs: []error{context.Canceled},
	}
	if _, err := LeaseOnce(context.Background(), cancelPool, &cursor, LeaseOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("the move error = %v, want context.Canceled", err)
	}
	if (cursor != LeaseCursor{}) {
		t.Errorf("the cursor advanced on a move error: %+v", cursor)
	}

	beginPool := &leaseFakePool{beginErr: errors.New("no connections")}
	if _, err := LeaseOnce(context.Background(), beginPool, &cursor, LeaseOptions{}); err == nil ||
		!strings.Contains(err.Error(), "transaction") {
		t.Errorf("the Begin error = %v, want the transaction context", err)
	}

	commitPool := &leaseFakePool{
		batches:    []leaseBatch{leaseSeedBatch(2, 2, base)},
		commitErrs: []error{errors.New("connection refused")},
	}
	if _, err := LeaseOnce(context.Background(), commitPool, &cursor, LeaseOptions{}); err == nil ||
		!strings.Contains(err.Error(), "commit") {
		t.Errorf("the Commit error = %v, want the commit context", err)
	}
	if (cursor != LeaseCursor{}) {
		t.Errorf("the cursor advanced on a commit failure: %+v — the batch must be retried", cursor)
	}
}

func TestLeaseOnceNilPool(t *testing.T) {
	if _, err := LeaseOnce(context.Background(), nil, nil, LeaseOptions{}); err == nil ||
		!strings.Contains(err.Error(), "pool not set") {
		t.Errorf("err = %v, want \"pool not set\"", err)
	}
}

// --- LeaseOptions ---------------------------------------------------------

func TestLeaseOptionsDefaults(t *testing.T) {
	// The batch default is pinned by the spec 5.3 literal (the 1000–10,000 corridor):
	// comparing with the very constant does not notice mutations.
	if DefaultLeaseBatch != 5000 {
		t.Errorf("DefaultLeaseBatch = %d, want 5000", DefaultLeaseBatch)
	}

	// Zero and negative values — the defaults; the loop interval is
	// half the lease term (the "return speed vs load" balance,
	// see LeaseWatch), including from the default term.
	got := (LeaseOptions{}).withDefaults()
	if got.Timeout != DefaultLeaseTimeout || got.BatchSize != DefaultLeaseBatch {
		t.Errorf("withDefaults = Timeout=%v BatchSize=%v, want %v/%v",
			got.Timeout, got.BatchSize, DefaultLeaseTimeout, DefaultLeaseBatch)
	}
	if want := DefaultLeaseTimeout / 2; got.Interval != want {
		t.Errorf("Interval = %v, want %v (Timeout/2)", got.Interval, want)
	}

	got = (LeaseOptions{Timeout: -time.Minute, BatchSize: -1, Interval: -time.Second}).withDefaults()
	if got.Timeout != DefaultLeaseTimeout || got.BatchSize != DefaultLeaseBatch ||
		got.Interval != DefaultLeaseTimeout/2 {
		t.Errorf("withDefaults(negatives) = %+v, want the defaults", got)
	}

	// A short term — the interval shrinks with it: the loop does not
	// "oversleep" the configurable lease.
	got = (LeaseOptions{Timeout: 2 * time.Minute}).withDefaults()
	if got.Interval != time.Minute {
		t.Errorf("Interval = %v, want 1m (Timeout/2 at Timeout=2m)", got.Interval)
	}

	// A degenerate Timeout of 1 ns would give 0 by division — the ticker needs a
	// positive interval.
	if got := (LeaseOptions{Timeout: time.Nanosecond}).withDefaults(); got.Interval <= 0 {
		t.Errorf("Interval = %v at Timeout=1ns, want > 0", got.Interval)
	}

	// Replacement boundaries: 1 ns and 1 are positive values, not defaults.
	if got := (LeaseOptions{Timeout: time.Nanosecond, BatchSize: 1}).withDefaults(); got.Timeout != time.Nanosecond || got.BatchSize != 1 {
		t.Errorf("withDefaults(Timeout=1ns, BatchSize=1) = %+v, want no replacement", got)
	}
	// The 1 ns interval is checked with a Timeout of 4 ns: replacing it with the default would
	// give 2 ns (Timeout/2), while a degenerate Timeout of 1 ns coincides with
	// the ticker's internal minimum and does not reveal the replacement.
	if got := (LeaseOptions{Timeout: 4 * time.Nanosecond, Interval: time.Nanosecond}).withDefaults(); got.Interval != time.Nanosecond {
		t.Errorf("Interval 1ns = %v, want no replacement", got.Interval)
	}

	// Given values are not overwritten; Now is passed as is.
	now := func() time.Time { return time.Unix(0, 0) }
	got = LeaseOptions{
		Timeout:   time.Hour,
		BatchSize: 10,
		Interval:  time.Minute,
		Now:       now,
	}.withDefaults()
	if got.Timeout != time.Hour || got.BatchSize != 10 || got.Interval != time.Minute {
		t.Errorf("withDefaults overwrites values: %+v", got)
	}
	if got.Now == nil || !got.Now().Equal(time.Unix(0, 0)) {
		t.Errorf("withDefaults overwrites Now: nil=%v, value %v", got.Now == nil, got.Now())
	}
}

// --- LeaseWatch -----------------------------------------------------------

// TestLeaseWatchImmediateDrain — the first drain runs immediately at
// start, without waiting for a tick: it picks up what hung during the service
// downtime (spec 3.4, 11.4 item 9).
func TestLeaseWatchImmediateDrain(t *testing.T) {
	pool := &leaseFakePool{batches: []leaseBatch{leaseSeedBatch(1, 1, time.Now())}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- LeaseWatch(ctx, pool, LeaseOptions{
			Interval: time.Hour, // the tick never comes: only the starting drain
		})
	}()

	// The starting drain: one transaction and silence until cancellation.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, selects, _, commits := pool.stats(); selects >= 1 && commits >= 1 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("the initial drain did not run")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	if _, selects, updates, commits := pool.stats(); selects != 1 || updates != 1 || commits != 1 {
		t.Errorf("selects=%d updates=%d commits=%d, want 1/1/1: without a tick there are no repeated drains",
			selects, updates, commits)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("LeaseWatch: %v, want nil on ctx cancellation", err)
	}
}

// TestLeaseWatchLoop — a full drain in batches until exhaustion; a batch
// error interrupts only its own drain (a Warn); the next tick continues;
// the default interval is half the lease term; ctx cancellation — a clean nil.
func TestLeaseWatchLoop(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	// Drain 1 (start): a DB error — a Warn. Drain 2 (tick): a full batch +
	// an incomplete one — an Info with the sum. Then — empty sets.
	base := time.Date(2026, 10, 17, 8, 0, 0, 0, time.UTC)
	pool := &leaseFakePool{
		batches: []leaseBatch{
			leaseSeedBatch(DefaultLeaseBatch, DefaultLeaseBatch, base),
			leaseSeedBatch(3, 3, base.Add(time.Hour)),
		},
		selectErrs: []error{errors.New("db unavailable")},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- LeaseWatch(ctx, pool, LeaseOptions{
			Timeout: 40 * time.Millisecond, // the interval default: 20 ms
			Log:     logger,
		})
	}()

	// Three SELECT passes: the first drain's failure and both batches of the second.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, selects, _, _ := pool.stats(); selects >= 3 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("the loop did not reach the third pass: selects=%d", len(pool.selectSnapshot()))
		}
		time.Sleep(time.Millisecond)
	}
	// The cancellation race: between observing the third SELECT and committing
	// the second batch's transaction there are microseconds, but a cancellation in this window
	// interrupted the drain silently (ctx.Err() — without a Warn/Info) and the final
	// log line was not written. We wait for both commits; the pause is for writing
	// the Info after the commit.
	for {
		if _, _, _, commits := pool.stats(); commits >= 2 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			begins, selects, updates, commits := pool.stats()
			t.Fatalf("the drain did not commit both batches: begins=%d selects=%d updates=%d commits=%d",
				begins, selects, updates, commits)
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(25 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("LeaseWatch: %v, want nil on ctx cancellation", err)
	}

	log := buf.String()
	for _, want := range []string{
		"returning domains with an expired lease", // the start
		"interval=20ms", // the default: Timeout/2
		"level=WARN",    // a drain error did not kill the loop
		"db unavailable",
		fmt.Sprintf("domains=%d", DefaultLeaseBatch+3), // the drain outcome
	} {
		if !strings.Contains(log, want) {
			t.Errorf("log does not contain %q:\n%s", want, log)
		}
	}
}

// TestLeaseWatchGuardFilteredFullBatch — a full selection batch with a zero
// move (the guard filtered everything out — results written between the selection and
// the lock) does NOT stop the drain: the continuation criterion is that the selection
// came back full, the cursor moved past the batch; there is no reason to return
// to what has been covered.
func TestLeaseWatchGuardFilteredFullBatch(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	base := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	pool := &leaseFakePool{
		batches: []leaseBatch{
			leaseSeedBatch(DefaultLeaseBatch, 0, base), // the guard filtered out everything
			leaseSeedBatch(3, 3, base.Add(time.Hour)),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- LeaseWatch(ctx, pool, LeaseOptions{Log: logger})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, selects, updates, _ := pool.stats(); selects >= 2 && updates >= 2 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("the drain did not continue after a zero move: selects=%d updates=%d",
				len(pool.selectSnapshot()), len(pool.updateSnapshot()))
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("LeaseWatch: %v", err)
	}
	if !strings.Contains(buf.String(), "domains=3") {
		t.Errorf("the drain did not reach the second batch (result 3):\n%s", buf.String())
	}
}

// TestLeaseWatchCursorLives — the drain cursor survives the batches of one
// drain and the drains themselves: the second batch starts past the last row
// of the first; the first pass of the next drain — past the last row
// of the previous drain (not from the beginning of the index).
func TestLeaseWatchCursorLives(t *testing.T) {
	base := time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC)
	full := leaseSeedBatch(2, 2, base)                     // rows base, base+1m
	partial := leaseSeedBatch(1, 1, base.Add(time.Minute)) // row base+2m
	pool := &leaseFakePool{batches: []leaseBatch{full, partial}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- LeaseWatch(ctx, pool, LeaseOptions{
			BatchSize: 2,
			Timeout:   40 * time.Millisecond, // the interval default 20 ms — we wait for a tick, not 15 min
		})
	}()

	// Drain 1: a full batch (2 == BatchSize) + an incomplete one (1) — a stop.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, selects, _, commits := pool.stats(); selects >= 2 && commits >= 2 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("the first drain did not reach the second batch")
		}
		time.Sleep(time.Millisecond)
	}

	// Drain 2 (tick): the first pass continues with the previous drain's cursor.
	deadline = time.Now().Add(2 * time.Second)
	for {
		if _, selects, _, _ := pool.stats(); selects >= 3 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("the second drain did not start")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("LeaseWatch: %v", err)
	}

	calls := pool.selectSnapshot()
	// Batch 1 — the zero cursor (start).
	if q, z, i := leaseCursorArgs(calls[0]); !q.IsZero() || z != 0 || i != 0 {
		t.Errorf("the batch 1 cursor = (%v,%d,%d), want zero", q, z, i)
	}
	// Batch 2 — past the last row of batch 1 (within the drain).
	want1 := full.rows[1]
	if q, z, i := leaseCursorArgs(calls[1]); q != want1.queuedAt || z != want1.zone || i != want1.id {
		t.Errorf("the batch 2 cursor = (%v,%d,%d), want (%v,%d,%d) — past batch 1",
			q, z, i, want1.queuedAt, want1.zone, want1.id)
	}
	// Drain 2 — past the last row of the previous drain (between drains).
	want2 := partial.rows[0]
	if q, z, i := leaseCursorArgs(calls[2]); q != want2.queuedAt || z != want2.zone || i != want2.id {
		t.Errorf("the drain 2 cursor = (%v,%d,%d), want (%v,%d,%d) — the cursor survives drains",
			q, z, i, want2.queuedAt, want2.zone, want2.id)
	}
}

// TestLeaseWatchRestartResetsCursor — a process restart (a new launch of the loop)
// starts from the zero cursor: the initial full drain is preserved
// (T-012/T-029) — the dead prefix behind the cursor is deliberately reread
// (spec §5.1).
func TestLeaseWatchRestartResetsCursor(t *testing.T) {
	base := time.Date(2026, 10, 17, 11, 0, 0, 0, time.UTC)
	run := func() []leaseCall {
		pool := &leaseFakePool{batches: []leaseBatch{leaseSeedBatch(1, 1, base)}}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- LeaseWatch(ctx, pool, LeaseOptions{Interval: time.Hour}) }()

		deadline := time.Now().Add(2 * time.Second)
		for {
			if _, selects, _, _ := pool.stats(); selects >= 1 {
				break
			}
			if time.Now().After(deadline) {
				cancel()
				t.Fatal("the initial drain did not run")
			}
			time.Sleep(time.Millisecond)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("LeaseWatch: %v", err)
		}
		return pool.selectSnapshot()
	}

	first := run()
	if q, z, i := leaseCursorArgs(first[0]); !q.IsZero() || z != 0 || i != 0 {
		t.Fatalf("the first run: the initial cursor (%v,%d,%d), want zero", q, z, i)
	}
	second := run()
	if q, z, i := leaseCursorArgs(second[0]); !q.IsZero() || z != 0 || i != 0 {
		t.Errorf("restart: the initial cursor (%v,%d,%d), want zero — the full start drain is preserved", q, z, i)
	}
}

// TestLeaseWatchCancelDuringDrain — ctx cancellation during a drain is not
// an alarm: no Warn, a clean nil (the OnRun contract, T-012).
func TestLeaseWatchCancelDuringDrain(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := &leaseFakePool{selectErrs: []error{context.Canceled}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled before the start: the drain's very first SELECT will fail

	done := make(chan error, 1)
	go func() { done <- LeaseWatch(ctx, pool, LeaseOptions{Log: logger}) }()

	if err := <-done; err != nil {
		t.Fatalf("LeaseWatch: %v, want nil on ctx cancellation", err)
	}
	if strings.Contains(buf.String(), "level=WARN") {
		t.Errorf("a cancellation during the drain — Warn:\n%s", buf.String())
	}
}

// TestLeaseWatchDrainTotals — the drain's Info outcome: a positive sum
// is logged (exactly 1 is also positive); a zero one — is not.
func TestLeaseWatchDrainTotals(t *testing.T) {
	tests := []struct {
		name        string
		batches     []leaseBatch
		wantSelects int    // the number of SELECT passes until the starting drain stops
		want        string // a substring of the drain's Info outcome; "" — the outcome is not logged
	}{
		{"an incomplete batch of one", []leaseBatch{leaseSeedBatch(1, 1, time.Now())}, 1, "domains=1"},
		{"a zero drain", nil, 1, ""},
		{"a full batch, the guard filtered everything out", []leaseBatch{leaseSeedBatch(DefaultLeaseBatch, 0, time.Now())}, 2, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))
			pool := &leaseFakePool{batches: append([]leaseBatch(nil), tt.batches...)}

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- LeaseWatch(ctx, pool, LeaseOptions{Log: logger}) }()

			// The starting drain is synchronous and independent of the tick: we wait
			// for the expected number of passes and let it finish.
			deadline := time.Now().Add(2 * time.Second)
			for {
				if _, selects, _, _ := pool.stats(); selects >= tt.wantSelects {
					break
				}
				if time.Now().After(deadline) {
					cancel()
					t.Fatal("the initial drain did not run")
				}
				time.Sleep(time.Millisecond)
			}
			time.Sleep(10 * time.Millisecond)
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("LeaseWatch: %v, want nil", err)
			}
			log := buf.String()
			if tt.want == "" {
				if strings.Contains(log, "expired lease return drain") {
					t.Errorf("a zero drain summarizes:\n%s", log)
				}
			} else if !strings.Contains(log, tt.want) {
				t.Errorf("log does not contain %q:\n%s", tt.want, log)
			}
		})
	}
}

func TestLeaseWatchNilPool(t *testing.T) {
	err := LeaseWatch(context.Background(), nil, LeaseOptions{})
	if err == nil || !strings.Contains(err.Error(), "pool not set") {
		t.Errorf("err = %v, want \"pool not set\"", err)
	}
}

// Unit tests of temp_error rechecks (T-031, spec 5.3, 6.2; the guard
// over maturity buckets — T-202, the selection plan pinning — T-191,
// separate decision transactions — the 40P01 counters deadlock regression,
// moving the bucket decrement out of the retry zones statement — T-267)
// on a fake pgx pool with transactions: the exact SQL texts and parameters
// (the guard read of the bucket sum / the candidate SELECT in the plan
// pinning transaction / the retry UPDATE + the tail bucket decrement /
// the final status UPDATE with the decrement in the statement itself),
// the SET LOCAL ×3 → SELECT sequence, the
// retry_after window, the attempt counter boundaries (before N / at N / after N),
// an empty set, guard=0 without the heavy selection, guards, errors and
// ctx cancellation; the RetryTempErrors cycle. Behavior against a live DB —
// integration tests (test/integration/retry_test.go).
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
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/model"
)

// --- fake pool with transactions -----------------------------------------
//
// Names prefixed with retry: files of other scheduler tasks appear
// in the package in parallel (T-027 dispatch*, T-030 recheck*); no common helpers
// are introduced here.

// retryCall — a recorded SQL call with arguments.
type retryCall struct {
	sql  string
	args []any
}

// retryDay — the episode date of the moved rows in the RETURNING stubs
// of the retry assignment (the third column, T-267 — the target of the
// maturity bucket decrement).
var retryDay = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// retryFakeRows — pgx.Rows with pre-made rows; Scan is positional,
// in the SQL column order.
type retryFakeRows struct {
	rows   [][]any
	i      int
	closed bool
	err    error // Err() after the rows are exhausted
}

func retryRows(rows ...[]any) *retryFakeRows {
	return &retryFakeRows{rows: rows}
}

func (r *retryFakeRows) Close() { r.closed = true }

func (r *retryFakeRows) Err() error { return r.err }

func (r *retryFakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *retryFakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *retryFakeRows) Next() bool {
	if r.closed || r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *retryFakeRows) Scan(dest ...any) error {
	if r.closed || r.i == 0 || r.i > len(r.rows) {
		return errors.New("retryFakeRows: Scan without Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("retryFakeRows: %d sinks, %d columns", len(dest), len(row))
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

func (r *retryFakeRows) Values() ([]any, error) { return r.rows[r.i-1], nil }

func (r *retryFakeRows) RawValues() [][]byte { return nil }

// Conn and TypeMap — part of pgx.Rows not used by the positional Scan.
func (r *retryFakeRows) Conn() *pgx.Conn      { return nil }
func (r *retryFakeRows) TypeMap() *pgtype.Map { return nil }

// retryFakePool — a RetryDB without a DB: the guard read of the maturity bucket sum
// (T-202) via the pool; the queues of stubbed candidate SELECT results
// (in the plan pinning transaction) and UPDATE … RETURNING (in the
// update transaction), per-step pass errors, the call protocol. Everything under
// a lock: the RetryTempErrors loop runs in a goroutine; the test waits for
// progress by counters.
type retryFakePool struct {
	mu          sync.Mutex
	guardVals   []int64          // the FIFO of guard bucket-sum values; exhausted — keeps the last one; not set at all — 1 (mature ones exist: the pass proceeds to the selection)
	guardEmpty  bool             // the guard returns an empty set — a protocol break (the SQL always answers with a row)
	guardErrs   []error          // the queue of guard read errors (nil — a read without an error)
	selects     []*retryFakeRows // the queue of candidate SELECT results (ran out — an empty set)
	requeues    []*retryFakeRows // the queue of retry UPDATE results
	finalizes   []*retryFakeRows // the queue of final status UPDATE results
	selectErrs  []error          // the queue of candidate SELECT errors (nil — a selection without an error)
	planErr     error            // the plan pinning error (SET LOCAL)
	requeueErr  error
	finalizeErr error
	beginErr    error   // the Begin error while the beginErrs queue is empty
	beginErrs   []error // the FIFO of Begin errors (nil — a successful Begin)
	commitErrs  []error // the FIFO of Commit errors (ran out — a commit without an error)

	guardQueries []retryCall // guard reads via the pool
	selectCalls  int         // heavy candidate SELECTs (in pinning transactions)
	begins       int
	seq          int // the monotonic order of Begin/Commit events (the tx-split regression)
	txs          []*retryFakeTx

	matureErr    error   // the tail bucket decrement error (retryRequeueMatureSQL, T-267)
	requeueErrs  []error // the FIFO of one-off retry UPDATE errors (nil — an attempt without an error; 40P01 fault injection, T-268)
	finalizeErrs []error // the FIFO of one-off final status UPDATE errors
	matureErrs   []error // the FIFO of one-off bucket decrement errors
}

func (p *retryFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom must not be called")
}

func (p *retryFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("Exec must not be called")
}

// nextGuardLocked — the bucket sum value for the next guard read.
func (p *retryFakePool) nextGuardLocked() int64 {
	if len(p.guardVals) == 0 {
		return 1 // the fake's default: mature ones exist, the pass proceeds to the selection
	}
	v := p.guardVals[0]
	if len(p.guardVals) > 1 {
		p.guardVals = p.guardVals[1:]
	}
	return v
}

// Query — the guard read of the maturity bucket sum (T-202): the pass's only
// SELECT outside transactions; the heavy candidate selection runs in
// the plan pinning transaction (retryFakeTx.Query).
func (p *retryFakePool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.guardQueries = append(p.guardQueries, retryCall{sql: sql, args: args})
	if sql != retryMatureSQL {
		return nil, fmt.Errorf("retryFakePool: unexpected SQL via the pool: %s", sql)
	}
	if len(p.guardErrs) > 0 {
		err := p.guardErrs[0]
		p.guardErrs = p.guardErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	if p.guardEmpty {
		return retryRows(), nil // an empty set — a protocol break (coalesce answers with a row)
	}
	return retryRows([]any{p.nextGuardLocked()}), nil
}

func (p *retryFakePool) Begin(context.Context) (pgx.Tx, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.begins++
	if err := p.nextBeginErrLocked(); err != nil {
		return nil, err
	}
	p.seq++
	tx := &retryFakeTx{pool: p, beginSeq: p.seq}
	p.txs = append(p.txs, tx)
	return tx, nil
}

// nextBeginErrLocked — the next Begin's error: the beginErrs FIFO (nil —
// a successful Begin); an empty queue — the constant beginErr.
func (p *retryFakePool) nextBeginErrLocked() error {
	if len(p.beginErrs) > 0 {
		err := p.beginErrs[0]
		p.beginErrs = p.beginErrs[1:]
		return err
	}
	return p.beginErr
}

// stats — a snapshot of the counters under the lock (waiting for progress in loop tests).
type retryStats struct {
	guards  int // counter guard reads
	selects int // heavy candidate SELECTs
	begins  int
	commits int
}

func (p *retryFakePool) stats() retryStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := retryStats{
		guards:  len(p.guardQueries),
		selects: p.selectCalls,
		begins:  p.begins,
	}
	for _, tx := range p.txs {
		if tx.committed {
			s.commits++
		}
	}
	return s
}

// retryFakeTx — the fake pool's transaction: remembers the calls, the outcome and its
// place in the pool's Begin/Commit event order.
type retryFakeTx struct {
	pgx.Tx // the other pgx.Tx methods are not used by the pass

	pool       *retryFakePool
	calls      []retryCall
	committed  bool
	rolledBack bool
	beginSeq   int // the pool's Begin event number (0 — no transaction)
	commitSeq  int // the pool's Commit event number (0 — no commit)
}

// isSelect — the candidate selection transaction (plan pinning), not
// the update one: pass errors do not have to roll it back (the SELECT reads
// committed data).
func (t *retryFakeTx) isSelect() bool {
	for _, c := range t.calls {
		if c.sql == retrySelectSQL {
			return true
		}
	}
	return false
}

// nextErrLocked — the next call's error from the FIFO queue of one-off
// errors (nil — an attempt without an error); the queue is empty — nil.
func (p *retryFakePool) nextErrLocked(queue *[]error) error {
	if len(*queue) == 0 {
		return nil
	}
	err := (*queue)[0]
	*queue = (*queue)[1:]
	return err
}

// Exec — the selection plan pinning (T-191): SET LOCAL ×3 before the SELECT —
// the sequence is checked by TestRetryOnceSelectPlan; the tail
// decrement of the retry assignment's maturity buckets (retryRequeueMatureSQL,
// T-267) — the second statement of the decision transaction.
func (t *retryFakeTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.calls = append(t.calls, retryCall{sql: sql, args: args})
	switch sql {
	case retryPlanNoIndexScanSQL, retryPlanNoBitmapScanSQL, retryPlanNoIOSQL:
		if t.pool.planErr != nil {
			return pgconn.CommandTag{}, t.pool.planErr
		}
		return pgconn.NewCommandTag("SET"), nil
	case retryRequeueMatureSQL:
		if err := t.pool.nextErrLocked(&t.pool.matureErrs); err != nil {
			return pgconn.CommandTag{}, err
		}
		if t.pool.matureErr != nil {
			return pgconn.CommandTag{}, t.pool.matureErr
		}
		return pgconn.NewCommandTag("INSERT 0 1"), nil
	default:
		return pgconn.CommandTag{}, fmt.Errorf("retryFakeTx: unexpected SQL Exec: %s", sql)
	}
}

func (t *retryFakeTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.calls = append(t.calls, retryCall{sql: sql, args: args})
	switch sql {
	case retrySelectSQL:
		t.pool.selectCalls++
		if len(t.pool.selectErrs) > 0 {
			err := t.pool.selectErrs[0]
			t.pool.selectErrs = t.pool.selectErrs[1:]
			if err != nil {
				return nil, err
			}
		}
		if len(t.pool.selects) > 0 {
			rows := t.pool.selects[0]
			t.pool.selects = t.pool.selects[1:]
			return rows, nil
		}
		return retryRows(), nil
	case retryRequeueSQL:
		if err := t.pool.nextErrLocked(&t.pool.requeueErrs); err != nil {
			return nil, err
		}
		if t.pool.requeueErr != nil {
			return nil, t.pool.requeueErr
		}
		if len(t.pool.requeues) > 0 {
			rows := t.pool.requeues[0]
			t.pool.requeues = t.pool.requeues[1:]
			return rows, nil
		}
		return retryRows(), nil
	case retryFinalizeSQL:
		if err := t.pool.nextErrLocked(&t.pool.finalizeErrs); err != nil {
			return nil, err
		}
		if t.pool.finalizeErr != nil {
			return nil, t.pool.finalizeErr
		}
		if len(t.pool.finalizes) > 0 {
			rows := t.pool.finalizes[0]
			t.pool.finalizes = t.pool.finalizes[1:]
			return rows, nil
		}
		return retryRows(), nil
	default:
		return nil, fmt.Errorf("retryFakeTx: unexpected SQL in a transaction: %s", sql)
	}
}

func (t *retryFakeTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if len(t.pool.commitErrs) > 0 {
		err := t.pool.commitErrs[0]
		t.pool.commitErrs = t.pool.commitErrs[1:]
		if err != nil {
			return err
		}
	}
	t.committed = true
	t.pool.seq++
	t.commitSeq = t.pool.seq
	return nil
}

func (t *retryFakeTx) Rollback(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if !t.committed {
		t.rolledBack = true
	}
	return nil
}

// compile-time: the fake pool is a RetryDB for tests without a DB.
var _ RetryDB = (*retryFakePool)(nil)

// --- expected SQL texts (contract) -------------------------------------

const wantRetrySelect = "SELECT d.zone_id, d.id, d.temp_attempts FROM domains d " +
	"WHERE d.status = $1 " +
	"AND d.check_state = $2 " +
	"AND d.last_checked_at < $3 " +
	"LIMIT $4"

const wantRetryRequeue = "WITH upd AS (UPDATE domains d " +
	"SET check_state = $3, temp_attempts = d.temp_attempts + 1 " +
	"FROM unnest($1::int2[], $2::int8[]) AS k(zone_id, domain_id) " +
	"WHERE d.zone_id = k.zone_id AND d.id = k.domain_id " +
	"AND d.status = $4 AND d.check_state = $5 " +
	"RETURNING d.zone_id, d.id, d.last_checked_at), " +
	"zc AS (UPDATE zones AS z " +
	"SET domains_checked = z.domains_checked - c.n, " +
	"domains_pending = z.domains_pending + c.n " +
	"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c " +
	"WHERE z.id = c.zone_id) " +
	"SELECT u.zone_id, u.id, u.last_checked_at FROM upd AS u"

// wantRetryRequeueMature — the tail decrement of the maturity buckets
// of the retry assignment (T-267): the second statement of the decision transaction over
// the episode dates of the moved rows (an unnest batch of dates); ORDER BY —
// the determination of bucket capture by ascending date.
const wantRetryRequeueMature = "INSERT INTO crawler_counters (name, value, updated_at) " +
	"SELECT 'retry_mature:' || to_char(d, 'YYYY-MM-DD'), -count(*), now() " +
	"FROM unnest($1::date[]) AS d " +
	"GROUP BY d ORDER BY d " +
	"ON CONFLICT (name) DO UPDATE " +
	"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()"

const wantRetryFinalize = "WITH upd AS (UPDATE domains d " +
	"SET status = $3, temp_attempts = 0 " +
	"FROM unnest($1::int2[], $2::int8[]) AS k(zone_id, domain_id) " +
	"WHERE d.zone_id = k.zone_id AND d.id = k.domain_id " +
	"AND d.status = $4 AND d.check_state = $5 " +
	"RETURNING d.zone_id, d.id, d.last_checked_at), " +
	"rm AS (INSERT INTO crawler_counters (name, value, updated_at) " +
	"SELECT 'retry_mature:' || to_char(u.last_checked_at, 'YYYY-MM-DD'), -count(*), now() " +
	"FROM upd AS u GROUP BY u.last_checked_at " +
	"ORDER BY u.last_checked_at " +
	"ON CONFLICT (name) DO UPDATE " +
	"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()) " +
	"SELECT u.zone_id, u.id FROM upd AS u"

// retryCutoffArg — the third argument of the candidate SELECT: the window boundary.
func retryCutoffArg(t *testing.T, args []any) time.Time {
	t.Helper()
	cutoff, ok := args[2].(time.Time)
	if !ok {
		t.Fatalf("the window boundary argument = %T, want time.Time", args[2])
	}
	return cutoff
}

// retrySelectCall — the heavy selection call from the transaction (after SET LOCAL ×3).
func retrySelectCall(t *testing.T, tx *retryFakeTx) retryCall {
	t.Helper()
	for _, c := range tx.calls {
		if c.sql == retrySelectSQL {
			return c
		}
	}
	t.Fatalf("transaction %p has no candidate SELECT: %v", tx, tx.calls)
	return retryCall{}
}

// --- the SQL contract (T-191/T-202) ----------------------------------------------

// TestRetrySQLContract — the pass statements' literals: the selection without ORDER BY
// (the candidate order does not matter — sorting would hold index access),
// the plan pinning SET LOCALs, the guard over the maturity bucket sum with the
// current_date boundary from the DB; the bucket decrement — in finalization (the rm CTE of the
// statement itself) and as a separate statement of the retry assignment (T-267);
// the last_checked_at key — the same as the result write increment's.
func TestRetrySQLContract(t *testing.T) {
	if !strings.Contains(retrySelectSQL, "WHERE d.status = $1") ||
		!strings.Contains(retrySelectSQL, "AND d.check_state = $2") ||
		!strings.Contains(retrySelectSQL, "AND d.last_checked_at < $3") ||
		!strings.Contains(retrySelectSQL, "LIMIT $4") ||
		strings.Contains(retrySelectSQL, "ORDER BY") {
		t.Fatalf("retrySelectSQL diverged from the T-191 contract (no ORDER BY, LIMIT $4):\n%s", retrySelectSQL)
	}
	if retryPlanNoIndexScanSQL != "SET LOCAL enable_indexscan = off" ||
		retryPlanNoBitmapScanSQL != "SET LOCAL enable_bitmapscan = off" ||
		retryPlanNoIOSQL != "SET LOCAL enable_indexonlyscan = off" {
		t.Fatalf("the selection plan pinning diverged from the T-191 contract:\n%s\n%s\n%s",
			retryPlanNoIndexScanSQL, retryPlanNoBitmapScanSQL, retryPlanNoIOSQL)
	}
	// The maturity buckets guard (T-202): the sum over the PK prefix range;
	// the strict < cuts off the current day's bucket (today's episodes
	// are immature); coalesce — an honest zero when there are no rows; the boundary name is
	// computed by the DB (the timezone of the reconciliation FILTER window, the T-179 style).
	wantGuard := "SELECT coalesce(sum(value), 0) FROM crawler_counters " +
		"WHERE name >= 'retry_mature:' " +
		"AND name < 'retry_mature:' || to_char(current_date, 'YYYY-MM-DD')"
	if retryMatureSQL != wantGuard {
		t.Fatalf("retryMatureSQL diverged from the T-202 contract:\n%s\nwant:\n%s", retryMatureSQL, wantGuard)
	}
	// The finalization maturity bucket decrement — in the statement itself
	// (the rm CTE, T-202): rm references upd (the only CTE —
	// execution is guaranteed after the move), no zones; the key is derived
	// from the moved rows' last_checked_at — the target of the same
	// increment the result writes make (their
	// statement's current_date). ORDER BY determines the bucket row capture by
	// ascending date — a common order with the reconciliation pass
	// (cleaning old → yesterday → today), without an AB-BA cycle on
	// mixed batch dates (review T-202, P2).
	const wantBasketCTE = ", " +
		"rm AS (INSERT INTO crawler_counters (name, value, updated_at) " +
		"SELECT 'retry_mature:' || to_char(u.last_checked_at, 'YYYY-MM-DD'), -count(*), now() " +
		"FROM upd AS u GROUP BY u.last_checked_at " +
		"ORDER BY u.last_checked_at " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()) "
	if !strings.Contains(retryFinalizeSQL, "RETURNING d.zone_id, d.id, d.last_checked_at") ||
		!strings.Contains(retryFinalizeSQL, wantBasketCTE) {
		t.Errorf("retryFinalizeSQL without the maturity bucket decrement (the rm CTE, T-202):\n%s", retryFinalizeSQL)
	}

	// The retry assignment — WITHOUT counters in the statement (T-267): the statement
	// is strictly "domains → zones" — unreferenced DML-CTEs of zones and the counter
	// execute in an unpredictable order (the PG documentation); the inversion
	// rm → zc of the Nov 07 incident created the "counters → zones" edge against
	// zone holders (runbooks.md §2.8); the decrement — the second statement
	// of the same transaction over the moved rows' dates.
	if strings.Contains(retryRequeueSQL, "crawler_counters") {
		t.Errorf("retryRequeueSQL writes counters in the zones statement (T-267 — moved out to a second statement):\n%s", retryRequeueSQL)
	}
	if retryRequeueMatureSQL != wantRetryRequeueMature {
		t.Errorf("retryRequeueMatureSQL diverged from the T-267 contract:\n%s\nwant:\n%s",
			retryRequeueMatureSQL, wantRetryRequeueMature)
	}
}

// --- RetryOnce ------------------------------------------------------------

// TestRetryOnceGuardZero — the guard over maturity buckets (T-202): at a zero
// (and negative — a visible decrement drift, reconciliation will even it out)
// sum the heavy candidate selection is not executed at all and the transaction does not
// open — steady state without mature ones costs one sum over an index
// range.
func TestRetryOnceGuardZero(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value int64
	}{
		{"bucket sum 0", 0},
		{"a negative bucket sum (decrement drift)", -1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pool := &retryFakePool{guardVals: []int64{tt.value}}

			res, err := RetryOnce(context.Background(), pool, RetryOptions{})
			if err != nil {
				t.Fatalf("RetryOnce: %v", err)
			}
			if res != (RetryResult{}) {
				t.Errorf("result = %+v, want zero: no candidates by the guard", res)
			}
			if len(pool.guardQueries) != 1 || pool.guardQueries[0].sql != retryMatureSQL {
				t.Errorf("the guard read = %v, want a single bucket sum query", pool.guardQueries)
			}
			if st := pool.stats(); st.selects != 0 || st.begins != 0 {
				t.Errorf("heavy selection/transactions at sum %d: selects=%d begins=%d, want 0 and 0",
					tt.value, st.selects, st.begins)
			}
		})
	}
}

// TestRetryOnceGuardEmpty — an empty guard response is impossible from a live DB
// (the coalesce in the SQL always answers with a row): a protocol break is a pass
// error, not "no mature ones".
func TestRetryOnceGuardEmpty(t *testing.T) {
	pool := &retryFakePool{guardEmpty: true}

	_, err := RetryOnce(context.Background(), pool, RetryOptions{})
	if err == nil ||
		!strings.Contains(err.Error(), "scheduler: temp_error rechecks") ||
		!strings.Contains(err.Error(), "empty aggregate response") {
		t.Errorf("err = %v, want the guard's empty response", err)
	}
	if st := pool.stats(); st.selects != 0 || st.begins != 0 {
		t.Errorf("heavy selection/transactions at an empty guard: selects=%d begins=%d", st.selects, st.begins)
	}
}

// TestRetryOnceEmpty — the buckets see the mature ones, the selection is empty (bucket
// drift or scattered candidates): an empty outcome; the update transaction does not
// open.
func TestRetryOnceEmpty(t *testing.T) {
	pool := &retryFakePool{} // the guard default 1, the SELECT queue is empty

	res, err := RetryOnce(context.Background(), pool, RetryOptions{})
	if err != nil {
		t.Fatalf("RetryOnce: %v", err)
	}
	if res != (RetryResult{}) {
		t.Errorf("result = %+v, want zero: nothing to assign", res)
	}
	st := pool.stats()
	if st.selects != 1 {
		t.Errorf("SELECTs = %d, want 1 (one idle pass over the counter)", st.selects)
	}
	if st.begins != 1 || len(pool.txs) != 1 {
		t.Fatalf("transactions = %d, want 1 (selection only)", st.begins)
	}
	tx := pool.txs[0]
	if !tx.committed {
		t.Error("the selection transaction is not committed")
	}
	if !tx.isSelect() {
		t.Errorf("the only transaction contains an UPDATE: %v", tx.calls)
	}
}

func TestRetryOnceSQLAndFlow(t *testing.T) {
	pool := &retryFakePool{
		guardVals: []int64{491210},
		selects: []*retryFakeRows{retryRows(
			[]any{int16(1), int64(10), int16(0)}, // 0 < 3 attempts → retry
			[]any{int16(1), int64(11), int16(2)}, // 2 < 3 → retry
			[]any{int16(2), int64(20), int16(2)}, // 2 < 3 → retry
			[]any{int16(2), int64(21), int16(3)}, // 3 = N → final status
			[]any{int16(3), int64(30), int16(4)}, // 4 > N → final status
		)},
		requeues: []*retryFakeRows{retryRows(
			// The guard blocked domain 1/11 (it left done in time) — not counted.
			// The third column — the episode date (last_checked_at): the target
			// of the bucket decrement (T-267).
			[]any{int16(1), int64(10), retryDay},
			[]any{int16(2), int64(20), retryDay},
		)},
		finalizes: []*retryFakeRows{retryRows(
			[]any{int16(2), int64(21)},
			[]any{int16(3), int64(30)},
		)},
	}

	res, err := RetryOnce(context.Background(), pool, RetryOptions{MaxRetries: 3, BatchSize: 10})
	if err != nil {
		t.Fatalf("RetryOnce: %v", err)
	}

	// The guard: a maturity bucket sum read via the pool, without parameters.
	if len(pool.guardQueries) != 1 {
		t.Fatalf("guard reads = %d, want 1", len(pool.guardQueries))
	}
	if pool.guardQueries[0].sql != retryMatureSQL || len(pool.guardQueries[0].args) != 0 {
		t.Errorf("the guard read = %+v, want %s without parameters", pool.guardQueries[0], retryMatureSQL)
	}

	// Three transactions: the selection (plan pinning + SELECT) and two batch
	// decisions — the retry assignment and the final status separately (the 40P01
	// deadlock regression, retryDecide).
	if pool.begins != 3 || len(pool.txs) != 3 {
		t.Fatalf("transactions = %d, want 3 (selection + the retry + the final status)", pool.begins)
	}
	selTx, requeueTx, finalizeTx := pool.txs[0], pool.txs[1], pool.txs[2]

	// The candidate selection: the temp_error status, the done state, the batch limit.
	sel := retrySelectCall(t, selTx)
	if sel.sql != wantRetrySelect {
		t.Errorf("selection SQL:\n%s\nwant:\n%s", sel.sql, wantRetrySelect)
	}
	gotStatus, gotState, gotLimit := sel.args[0], sel.args[1], sel.args[3]
	if gotStatus != int16(model.StatusTempError) || gotState != int16(model.CheckStateDone) {
		t.Errorf("selection filters = status %v, state %v, want temp_error (%d) and done (%d)",
			gotStatus, gotState, model.StatusTempError, model.CheckStateDone)
	}
	if gotLimit != 10 {
		t.Errorf("the batch limit = %v, want 10", gotLimit)
	}

	// The decisions — each in its own transaction; the retry is TWO statements
	// (the move + the tail bucket decrement, T-267); the final status — one.
	if len(requeueTx.calls) != 2 {
		t.Fatalf("calls in the retry transaction = %d, want 2 (the move + the bucket decrement)", len(requeueTx.calls))
	}
	if requeueTx.calls[0].sql != wantRetryRequeue {
		t.Errorf("retry SQL:\n%s\nwant:\n%s", requeueTx.calls[0].sql, wantRetryRequeue)
	}
	if want := []any{
		[]int16{1, 1, 2}, []int64{10, 11, 20},
		int16(model.CheckStatePending), int16(model.StatusTempError), int16(model.CheckStateDone),
	}; !reflect.DeepEqual(requeueTx.calls[0].args, want) {
		t.Errorf("retry args = %v, want %v", requeueTx.calls[0].args, want)
	}
	if requeueTx.calls[1].sql != wantRetryRequeueMature {
		t.Errorf("bucket decrement SQL:\n%s\nwant:\n%s", requeueTx.calls[1].sql, wantRetryRequeueMature)
	}
	// The decrement — by the episode dates of the moved rows (both dates equal —
	// one bucket in the batch); the guard has no empty keys — the statement
	// is executed.
	if want := []any{[]time.Time{retryDay, retryDay}}; !reflect.DeepEqual(requeueTx.calls[1].args, want) {
		t.Errorf("bucket decrement args = %v, want %v", requeueTx.calls[1].args, want)
	}
	if len(finalizeTx.calls) != 1 {
		t.Fatalf("calls in the final status transaction = %d, want 1 (the decrement is inside the statement itself)", len(finalizeTx.calls))
	}
	if finalizeTx.calls[0].sql != wantRetryFinalize {
		t.Errorf("final status SQL:\n%s\nwant:\n%s", finalizeTx.calls[0].sql, wantRetryFinalize)
	}
	if want := []any{
		[]int16{2, 3}, []int64{21, 30},
		int16(model.StatusDelegatedNoAddress), int16(model.StatusTempError), int16(model.CheckStateDone),
	}; !reflect.DeepEqual(finalizeTx.calls[0].args, want) {
		t.Errorf("final status args = %v, want %v", finalizeTx.calls[0].args, want)
	}

	// The counters — by the rows actually affected (the guard blocked 1/11).
	if want := (RetryResult{Requeued: 2, Finalized: 2}); res != want {
		t.Errorf("result = %+v, want %+v", res, want)
	}
	for _, tx := range []*retryFakeTx{selTx, requeueTx, finalizeTx} {
		if !tx.committed || tx.rolledBack {
			t.Errorf("transaction %p: committed=%v rolledBack=%v, want a commit without a rollback", tx, tx.committed, tx.rolledBack)
		}
	}
}

// TestRetryOnceSplitTransactions — the 40P01 counters deadlock regression
// (the retry wave incident: 9 deadlocks in 3 h, the upsert checks:YYYY-MM-DD in
// the http-worker result write transaction): the batch decisions run in
// SEPARATE transactions — the final status transaction's Begin is strictly
// AFTER the retry transaction's Commit (the first one's locks — domains/zones/
// buckets — are released before the second captures its domains rows); neither
// transaction contains both statements. A shared transaction would hold
// the zones and retry_mature:{dates} buckets on top of re-capturing
// domains rows — the "counters → domains" edge against the
// "domains → … → counters" transactions of http-worker/monitor closed
// the cycle.
func TestRetryOnceSplitTransactions(t *testing.T) {
	pool := &retryFakePool{
		selects: []*retryFakeRows{retryRows(
			[]any{int16(1), int64(10), int16(0)}, // 0 < 3 attempts → retry
			[]any{int16(2), int64(20), int16(3)}, // 3 = N → final status
		)},
		requeues:  []*retryFakeRows{retryRows([]any{int16(1), int64(10), retryDay})},
		finalizes: []*retryFakeRows{retryRows([]any{int16(2), int64(20)})},
	}

	if _, err := RetryOnce(context.Background(), pool, RetryOptions{}); err != nil {
		t.Fatalf("RetryOnce: %v", err)
	}

	if len(pool.txs) != 3 {
		t.Fatalf("transactions = %d, want 3 (selection + two decisions)", len(pool.txs))
	}
	selTx, requeueTx, finalizeTx := pool.txs[0], pool.txs[1], pool.txs[2]

	// Each decision — only its own statements: mixing both decisions in
	// one tx is exactly the lock order inversion.
	for _, tx := range []*retryFakeTx{requeueTx, finalizeTx} {
		if tx.isSelect() {
			t.Errorf("the decision transaction contains a candidate SELECT: %v", tx.calls)
		}
	}
	// The retry half — the move and the tail bucket decrement (by two
	// statements, T-267: the statements of a transaction execute in textual
	// order — the order of one statement's unreferenced DML-CTEs PG does not
	// guarantee it).
	if len(requeueTx.calls) != 2 ||
		requeueTx.calls[0].sql != retryRequeueSQL ||
		requeueTx.calls[1].sql != retryRequeueMatureSQL {
		t.Errorf("the retry transaction = %v, want retryRequeueSQL + retryRequeueMatureSQL", requeueTx.calls)
	}
	if len(finalizeTx.calls) != 1 || finalizeTx.calls[0].sql != retryFinalizeSQL {
		t.Errorf("the final status transaction = %v, want retryFinalizeSQL only", finalizeTx.calls)
	}

	// The pool's event order: the retry's Commit strictly before the final
	// status's Begin — the first half's counters/zones do not survive the
	// second half's domains row capture.
	if requeueTx.commitSeq == 0 || finalizeTx.beginSeq == 0 ||
		requeueTx.commitSeq > finalizeTx.beginSeq {
		t.Errorf("the event order: the retry commit %d, the final begin %d — want the commit before the begin",
			requeueTx.commitSeq, finalizeTx.beginSeq)
	}
	for _, tx := range []*retryFakeTx{selTx, requeueTx, finalizeTx} {
		if !tx.committed || tx.rolledBack {
			t.Errorf("transaction %p: committed=%v rolledBack=%v, want a commit without a rollback", tx, tx.committed, tx.rolledBack)
		}
	}
}

// TestRetryOnceSelectPlan — pinning the selection plan form (T-191): the batch
// is read in an explicit transaction strictly by the SET LOCAL ×3 →
// SELECT sequence (a replica of the spec §4.2 diagram; without pinning the planner takes
// the partial domains_recheck_idx — query #1 of the slow-query log of Oct 25);
// the selection goes through the transaction, not the pool.
func TestRetryOnceSelectPlan(t *testing.T) {
	pool := &retryFakePool{
		selects: []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(0)})},
	}

	if _, err := RetryOnce(context.Background(), pool, RetryOptions{}); err != nil {
		t.Fatalf("RetryOnce: %v", err)
	}
	if len(pool.txs) != 2 {
		t.Fatalf("transactions = %d, want 2", len(pool.txs))
	}
	calls := pool.txs[0].calls
	if len(calls) != 4 {
		t.Fatalf("calls in the selection transaction = %d, want 4 (SET LOCAL ×3 + SELECT): %v", len(calls), calls)
	}
	if want := []string{retryPlanNoIndexScanSQL, retryPlanNoBitmapScanSQL, retryPlanNoIOSQL, retrySelectSQL}; true {
		for i, sql := range want {
			if calls[i].sql != sql {
				t.Errorf("step %d = %s, want %s (the SET LOCAL ×3 → SELECT order)", i, calls[i].sql, sql)
			}
		}
	}
	if st := pool.stats(); st.guards != 1 {
		t.Errorf("guard reads = %d, want 1 (the guard before the selection transaction)", st.guards)
	}
}

// TestRetryOnceCutoff — the window boundary: cutoff = the pass moment − RetryAfter
// (24 h from spec 5.3 by default); the comparison is strict — "older".
func TestRetryOnceCutoff(t *testing.T) {
	before := time.Now()

	tests := []struct {
		name       string
		opts       RetryOptions
		wantWindow time.Duration
	}{
		{"the 24h default", RetryOptions{}, DefaultRetryAfter},
		{"an explicit 2h window", RetryOptions{RetryAfter: 2 * time.Hour}, 2 * time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := &retryFakePool{}
			if _, err := RetryOnce(context.Background(), pool, tt.opts); err != nil {
				t.Fatalf("RetryOnce: %v", err)
			}
			sel := retrySelectCall(t, pool.txs[0])
			cutoff := retryCutoffArg(t, sel.args)
			want := before.Add(-tt.wantWindow)
			// The pass moment is not deterministic — we check with a tolerance.
			if cutoff.Before(want.Add(-time.Minute)) || cutoff.After(time.Now().Add(-tt.wantWindow)) {
				t.Errorf("the window boundary = %s, want ≈ now − %s", cutoff, tt.wantWindow)
			}
		})
	}
}

// TestRetryOnceAttemptBoundaries — splitting the batch by the counter: fewer
// than N — a retry; N and more — the final status (spec 5.3: "no more than N
// times in a row"); the default N = 3.
func TestRetryOnceAttemptBoundaries(t *testing.T) {
	tests := []struct {
		name        string
		opts        RetryOptions
		attempts    int16
		wantRequeue bool
	}{
		{"0 of 3", RetryOptions{}, 0, true},
		{"N−1 of 3", RetryOptions{}, 2, true},
		{"N of 3", RetryOptions{}, 3, false},
		{"N+1 of 3 (an overrun guard)", RetryOptions{}, 4, false},
		{"N−1 of 1", RetryOptions{MaxRetries: 1}, 0, true},
		{"N of 1", RetryOptions{MaxRetries: 1}, 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := &retryFakePool{
				selects: []*retryFakeRows{retryRows([]any{int16(7), int64(70), tt.attempts})},
				requeues: []*retryFakeRows{retryRows(
					[]any{int16(7), int64(70), retryDay})},
				finalizes: []*retryFakeRows{retryRows(
					[]any{int16(7), int64(70)})},
			}

			res, err := RetryOnce(context.Background(), pool, tt.opts)
			if err != nil {
				t.Fatalf("RetryOnce: %v", err)
			}
			tx := pool.txs[1] // the update transaction — the second one
			if tt.wantRequeue {
				if res != (RetryResult{Requeued: 1}) {
					t.Errorf("result = %+v, want one check retried", res)
				}
				if len(tx.calls) != 2 || tx.calls[0].sql != wantRetryRequeue ||
					tx.calls[1].sql != retryRequeueMatureSQL {
					t.Errorf("calls = %d, want the move + the bucket decrement", len(tx.calls))
				}
			} else {
				if res != (RetryResult{Finalized: 1}) {
					t.Errorf("result = %+v, want a single final status", res)
				}
				if len(tx.calls) != 1 || tx.calls[0].sql != wantRetryFinalize {
					t.Errorf("calls = %d, want the final status only", len(tx.calls))
				}
			}
		})
	}
}

// TestRetryOnceDefaultBatch — a zero batch size is replaced by the default.
func TestRetryOnceDefaultBatch(t *testing.T) {
	pool := &retryFakePool{}

	if _, err := RetryOnce(context.Background(), pool, RetryOptions{}); err != nil {
		t.Fatalf("RetryOnce: %v", err)
	}
	if got := retrySelectCall(t, pool.txs[0]).args[3]; got != DefaultRetryBatch {
		t.Errorf("the batch limit = %v, want the default %d", got, DefaultRetryBatch)
	}
}

// TestRetryOnceRepeatPasses — an assigned retry moves the domain to
// pending: a repeated pass does not select it and does not open the update
// transaction.
func TestRetryOnceRepeatPasses(t *testing.T) {
	pool := &retryFakePool{
		selects:  []*retryFakeRows{retryRows([]any{int16(3), int64(30), int16(0)})},
		requeues: []*retryFakeRows{retryRows([]any{int16(3), int64(30), retryDay})},
	}

	first, err := RetryOnce(context.Background(), pool, RetryOptions{})
	if err != nil {
		t.Fatalf("the first pass: %v", err)
	}
	if first.Requeued != 1 {
		t.Fatalf("the first pass = %+v, want 1 retry", first)
	}

	second, err := RetryOnce(context.Background(), pool, RetryOptions{})
	if err != nil {
		t.Fatalf("the second pass: %v", err)
	}
	if second != (RetryResult{}) {
		t.Errorf("the second pass = %+v, want zero", second)
	}
	if st := pool.stats(); st.selects != 2 {
		t.Errorf("SELECTs = %d, want 2 (one per pass)", st.selects)
	}
	// The first pass: the selection and update transactions; the second — only
	// the selection (empty): rows moved to pending are not candidates.
	if st := pool.stats(); st.begins != 3 {
		t.Errorf("transactions = %d, want 3: a repeated pass does not open an update", st.begins)
	}
}

// TestRetryOnceGuardedAway — the UPDATE guard blocked all candidates
// (the domains left done bypassing the pass): the counters are zero; the
// update transaction commits without effect — an empty key set does not
// open the tail bucket decrement (T-267).
func TestRetryOnceGuardedAway(t *testing.T) {
	pool := &retryFakePool{
		selects:  []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(0)})},
		requeues: []*retryFakeRows{retryRows()}, // RETURNING is empty
	}

	res, err := RetryOnce(context.Background(), pool, RetryOptions{})
	if err != nil {
		t.Fatalf("RetryOnce: %v", err)
	}
	if res != (RetryResult{}) {
		t.Errorf("result = %+v, want zero: the guard fought off the candidate", res)
	}
	if !pool.txs[1].committed {
		t.Error("the update transaction is not committed: an empty RETURNING is not an error")
	}
	if len(pool.txs[1].calls) != 1 || pool.txs[1].calls[0].sql != retryRequeueSQL {
		t.Errorf("the retry transaction = %v, want retryRequeueSQL only (the decrement by non-empty keys)", pool.txs[1].calls)
	}
}

func TestRetryOnceErrors(t *testing.T) {
	newPool := func() *retryFakePool {
		return &retryFakePool{
			selects:  []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(0)})},
			requeues: []*retryFakeRows{retryRows([]any{int16(1), int64(10), retryDay})},
		}
	}

	tests := []struct {
		name    string
		pool    *retryFakePool
		context string // the expected message fragment
	}{
		{"the guard error", func() *retryFakePool {
			p := &retryFakePool{guardErrs: []error{errors.New("db unavailable")}}
			return p
		}(), "maturity buckets"},
		{"the plan pinning error", func() *retryFakePool {
			p := newPool()
			p.planErr = errors.New("db unavailable")
			return p
		}(), "plan pinning"},
		{"the SELECT error", func() *retryFakePool {
			p := newPool()
			p.selectErrs = []error{errors.New("db unavailable")}
			return p
		}(), "candidate selection"},
		{"the Begin error", func() *retryFakePool {
			p := newPool()
			p.beginErr = errors.New("db unavailable")
			return p
		}(), "selection transaction"},
		{"the retry error", func() *retryFakePool {
			p := newPool()
			p.requeueErr = errors.New("db unavailable")
			return p
		}(), "retry assignment"},
		{"the bucket decrement error", func() *retryFakePool {
			p := newPool()
			p.matureErr = errors.New("db unavailable")
			return p
		}(), "maturity bucket decrement"},
		{"the final error", func() *retryFakePool {
			// A candidate with exhausted attempts — the final status
			// branch runs.
			p := newPool()
			p.selects = []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(DefaultRetryMaxRetries)})}
			p.finalizeErr = errors.New("db unavailable")
			return p
		}(), "final status"},
		{"the decision Begin error", func() *retryFakePool {
			// The selection passes; the retry assignment transaction does not
			// open (after the decision split — an independent
			// failure point).
			p := newPool()
			p.beginErrs = []error{nil, errors.New("db unavailable")}
			return p
		}(), "decision transaction (retry assignment)"},
		{"the selection Commit error", func() *retryFakePool {
			p := newPool()
			p.commitErrs = []error{errors.New("db unavailable")}
			return p
		}(), "selection commit"},
		{"the retry Commit error", func() *retryFakePool {
			p := newPool()
			p.commitErrs = []error{nil, errors.New("db unavailable")}
			return p
		}(), "decision commit (retry assignment)"},
		{"the final Commit error", func() *retryFakePool {
			// A mixed batch: the retry commit passes; the final status
			// commit — does not; the committed half is not
			// rolled back (separate decision transactions).
			p := newPool()
			p.selects = []*retryFakeRows{retryRows(
				[]any{int16(1), int64(10), int16(0)},
				[]any{int16(2), int64(20), int16(DefaultRetryMaxRetries)},
			)}
			p.commitErrs = []error{nil, nil, errors.New("db unavailable")}
			return p
		}(), "decision commit (final status)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := RetryOnce(context.Background(), tt.pool, RetryOptions{})
			if err == nil {
				t.Fatal("an error was expected")
			}
			if !strings.Contains(err.Error(), "scheduler: temp_error rechecks") ||
				!strings.Contains(err.Error(), tt.context) ||
				!strings.Contains(err.Error(), "db unavailable") {
				t.Errorf("message = %q, want the %q context and the original error", err, tt.context)
			}
			// Separate decision transactions (the 40P01 regression): on a pass
			// error only the half BEFORE the failure point can be committed —
			// the rolled-back decision transaction must
			// be the last (the halves' commits are a prefix; after the failure
			// there are no new transactions). The selection transaction is only
			// the plan pinning shell (the SELECT reads committed
			// data); its commit before the error is not a half-state;
			// the deferred rollback must fire for all
			// uncommitted ones.
			for i, tx := range tt.pool.txs {
				if !tx.committed && !tx.rolledBack {
					t.Errorf("the transaction is not rolled back on the %q error", tt.name)
				}
				if !tx.committed && !tx.isSelect() && i != len(tt.pool.txs)-1 {
					t.Errorf("after the rolled back decision transaction another one is open (case %q)", tt.name)
				}
			}
		})
	}
}

// TestRetryOnceMatureRollback — a failure of the SECOND statement of the retry
// half (the tail bucket decrement, T-267) rolls back the half
// IN FULL: the move and the decrement are one transaction (spec 3.4); the candidates
// remain the next pass's candidates. The contrast with the DNS stage
// result write increment — there the statements are split by autocommits:
// the transition survives the increment failure; reconciliation catches the bucket up.
func TestRetryOnceMatureRollback(t *testing.T) {
	pool := &retryFakePool{
		selects: []*retryFakeRows{retryRows(
			[]any{int16(1), int64(10), int16(0)}, // a retry — the move passes, the decrement fails
		)},
		requeues:  []*retryFakeRows{retryRows([]any{int16(1), int64(10), retryDay})},
		matureErr: errors.New("db unavailable"),
	}

	_, err := RetryOnce(context.Background(), pool, RetryOptions{})
	if err == nil || !strings.Contains(err.Error(), "maturity bucket decrement") ||
		!errors.Is(err, pool.matureErr) {
		t.Fatalf("err = %v, want \"maturity bucket decrement\" and the original", err)
	}
	if len(pool.txs) != 2 {
		t.Fatalf("transactions = %d, want 2 (selection + half of the retry)", len(pool.txs))
	}
	half := pool.txs[1]
	if half.committed || !half.rolledBack {
		t.Error("half of the retry is committed or not rolled back on the decrement failure")
	}
	if len(half.calls) != 2 || half.calls[0].sql != retryRequeueSQL || half.calls[1].sql != retryRequeueMatureSQL {
		t.Errorf("the retry transaction = %v, want the move + the failed bucket decrement", half.calls)
	}
}

// TestRetryOnceHalfCommittedOnError — a failure of the batch's second half does not
// roll back the committed first one (separate decision transactions):
// the guards of both UPDATEs are idempotent; rows moved by the first half are not
// the next pass's candidates; the unfinished half — are candidates
// (spec 3.4). The former "all or nothing" contract is replaced by an
// "idempotent pass repeat" — the batch atomicity is not critical.
func TestRetryOnceHalfCommittedOnError(t *testing.T) {
	pool := &retryFakePool{
		selects: []*retryFakeRows{retryRows(
			[]any{int16(1), int64(10), int16(0)}, // a retry — commits
			[]any{int16(2), int64(20), int16(3)}, // the final — Commit fails
		)},
		requeues: []*retryFakeRows{retryRows([]any{int16(1), int64(10), retryDay})},
		// The selection and the retry commit; the final's commit — an error.
		commitErrs: []error{nil, nil, errors.New("db unavailable")},
	}

	res, err := RetryOnce(context.Background(), pool, RetryOptions{})
	if err == nil || !strings.Contains(err.Error(), "decision commit (final status)") {
		t.Fatalf("err = %v, want the final status commit failure", err)
	}
	if res != (RetryResult{}) {
		t.Errorf("the result on error = %+v, want zero (the caller sees only the error)", res)
	}
	if len(pool.txs) != 3 {
		t.Fatalf("transactions = %d, want 3", len(pool.txs))
	}
	if !pool.txs[1].committed || pool.txs[1].rolledBack {
		t.Error("half of the retry is not committed: the second half's failure does not roll back the first")
	}
	if pool.txs[2].committed || !pool.txs[2].rolledBack {
		t.Error("half of the final status is committed or not rolled back on the Commit failure")
	}
}

// TestRetryOnceCtxCanceled — ctx cancellation goes through the pass steps
// as an error (errors.Is); the update transaction is not committed.
func TestRetryOnceCtxCanceled(t *testing.T) {
	guardPool := &retryFakePool{guardErrs: []error{context.Canceled}}
	if _, err := RetryOnce(context.Background(), guardPool, RetryOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("the guard error = %v, want context.Canceled", err)
	}

	selectPool := &retryFakePool{selectErrs: []error{context.Canceled}}
	if _, err := RetryOnce(context.Background(), selectPool, RetryOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("the selection error = %v, want context.Canceled", err)
	}

	requeuePool := &retryFakePool{
		selects:  []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(0)})},
		requeues: []*retryFakeRows{retryRows()},
	}
	requeuePool.requeueErr = context.Canceled
	if _, err := RetryOnce(context.Background(), requeuePool, RetryOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("the retry error = %v, want context.Canceled", err)
	}
	if requeuePool.txs[1].committed {
		t.Error("the update transaction is committed on a context cancellation")
	}
}

// TestRetryOnceRowsErrors — row read errors on both pass steps:
// the Scan of candidates and RETURNING keys (an extra column breaks the positional
// Scan); rows.Err after the rows are exhausted — exact message contexts,
// the original error is proxied (errors.Is).
func TestRetryOnceRowsErrors(t *testing.T) {
	// The candidate Scan: the second row with two columns instead of three.
	scanPool := &retryFakePool{selects: []*retryFakeRows{retryRows(
		[]any{int16(1), int64(10), int16(0)},
		[]any{int16(2), int64(20)},
	)}}
	if _, err := RetryOnce(context.Background(), scanPool, RetryOptions{}); err == nil ||
		!strings.Contains(err.Error(), "candidate read") {
		t.Errorf("the candidate Scan error = %v, want \"candidate read\"", err)
	}

	// rows.Err after reading the candidates.
	boom := errors.New("db unavailable")
	candRows := retryRows([]any{int16(1), int64(10), int16(0)})
	candRows.err = boom
	candPool := &retryFakePool{selects: []*retryFakeRows{candRows}}
	_, err := RetryOnce(context.Background(), candPool, RetryOptions{})
	if err == nil || !strings.Contains(err.Error(), "candidate selection") || !errors.Is(err, boom) {
		t.Errorf("the candidates rows.Err error = %v, want \"candidate selection\" and the original", err)
	}

	// The RETURNING key Scan: the second row with one column instead of three
	// (the move returns the zone, the domain and the episode date — T-267).
	keyScanPool := &retryFakePool{
		selects:  []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(0)})},
		requeues: []*retryFakeRows{retryRows([]any{int16(1), int64(10), retryDay}, []any{int16(2)})},
	}
	if _, err := RetryOnce(context.Background(), keyScanPool, RetryOptions{}); err == nil ||
		!strings.Contains(err.Error(), "key read") {
		t.Errorf("the key Scan error = %v, want \"key read\"", err)
	}

	// rows.Err after reading the RETURNING keys.
	keyRows := retryRows([]any{int16(1), int64(10), retryDay})
	keyRows.err = boom
	keyPool := &retryFakePool{
		selects:  []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(0)})},
		requeues: []*retryFakeRows{keyRows},
	}
	_, err = RetryOnce(context.Background(), keyPool, RetryOptions{})
	if err == nil || !strings.Contains(err.Error(), "retry assignment") || !errors.Is(err, boom) {
		t.Errorf("the update rows.Err error = %v, want \"retry assignment\" and the original", err)
	}
}

// TestRetryOnceNilPool — the pool must be set.
func TestRetryOnceNilPool(t *testing.T) {
	if _, err := RetryOnce(context.Background(), nil, RetryOptions{}); err == nil ||
		!strings.Contains(err.Error(), "pool not set") {
		t.Errorf("err = %v, want \"pool not set\"", err)
	}
}

// retryDeadlockValue — the current value of the
// crawler_db_deadlock_retried_total counter from the registry (the backlogSkippedValue style).
func retryDeadlockValue(t *testing.T, reg *metrics.Registry) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "crawler_db_deadlock_retried_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			return m.GetCounter().GetValue()
		}
	}
	return 0
}

// TestRetryOnceDeadlockRetry — 40P01 fault injection (T-268): a decision
// transaction deadlock is absorbed by a retry IN FULL — the failed attempt
// is rolled back (no move, the bucket decrement never opened); the second
// passes both statements and commits; the metric grows on every retry.
func TestRetryOnceDeadlockRetry(t *testing.T) {
	reg := metrics.New("scheduler")
	pool := &retryFakePool{
		selects:  []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(0)})},
		requeues: []*retryFakeRows{retryRows([]any{int16(1), int64(10), retryDay})},
		// The first retry UPDATE attempt fails with a deadlock; the retry — passes.
		requeueErrs: []error{&pgconn.PgError{Code: "40P01", Message: "deadlock detected"}},
	}

	res, err := RetryOnce(context.Background(), pool, RetryOptions{Metrics: reg})
	if err != nil {
		t.Fatalf("RetryOnce: %v, want nil — the deadlock is absorbed", err)
	}
	if res.Requeued != 1 || res.Finalized != 0 {
		t.Fatalf("result = %+v, want one domain retried", res)
	}
	// Transactions: the selection + TWO decision attempts — the first rolled back
	// (no move), the second committed.
	if len(pool.txs) != 3 {
		t.Fatalf("transactions = %d, want 3 (selection + the attempt + the retry)", len(pool.txs))
	}
	failTx, okTx := pool.txs[1], pool.txs[2]
	if failTx.committed || !failTx.rolledBack {
		t.Error("the first attempt is not rolled back on the deadlock")
	}
	if len(failTx.calls) != 1 || failTx.calls[0].sql != retryRequeueSQL {
		t.Errorf("the attempt = %v, want the failed move only (the decrement never opened)", failTx.calls)
	}
	if !okTx.committed || okTx.rolledBack {
		t.Error("the retry is not committed")
	}
	if len(okTx.calls) != 2 || okTx.calls[0].sql != retryRequeueSQL || okTx.calls[1].sql != retryRequeueMatureSQL {
		t.Errorf("the retry = %v, want the move + the bucket decrement", okTx.calls)
	}
	if got := retryDeadlockValue(t, reg); got != 1 {
		t.Errorf("crawler_db_deadlock_retried_total = %v, want 1", got)
	}
}

// TestRetryOnceDeadlockExhausted — a persistent deadlock: after the retries are
// exhausted the error goes out (the pass fails, the drain continues); all attempts
// are rolled back; each one recorded the metric.
func TestRetryOnceDeadlockExhausted(t *testing.T) {
	reg := metrics.New("scheduler")
	pool := &retryFakePool{
		selects: []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(0)})},
		// A deadlock on every retry UPDATE attempt.
		requeueErr: &pgconn.PgError{Code: "40P01", Message: "deadlock detected"},
	}

	res, err := RetryOnce(context.Background(), pool, RetryOptions{Metrics: reg})
	if err == nil || !db.IsDeadlock(err) ||
		!strings.Contains(err.Error(), "retry assignment") {
		t.Fatalf("err = %v, want 40P01 \"retry assignment\" outward", err)
	}
	if res != (RetryResult{}) {
		t.Errorf("the result on error = %+v, want zero", res)
	}
	// The selection + the first attempt + MaxDeadlockRetries retries — all
	// rolled back; none reached the bucket decrement.
	if len(pool.txs) != 2+db.MaxDeadlockRetries {
		t.Fatalf("transactions = %d, want %d (selection + attempts)", len(pool.txs), 2+db.MaxDeadlockRetries)
	}
	for _, tx := range pool.txs[1:] {
		if tx.committed || !tx.rolledBack {
			t.Errorf("decision attempt %p: committed=%v rolledBack=%v, want a rollback", tx, tx.committed, tx.rolledBack)
		}
		if len(tx.calls) != 1 || tx.calls[0].sql != retryRequeueSQL {
			t.Errorf("the attempt %p = %v, want the failed move only", tx, tx.calls)
		}
	}
	if got := retryDeadlockValue(t, reg); got != db.MaxDeadlockRetries {
		t.Errorf("crawler_db_deadlock_retried_total = %v, want %d", got, db.MaxDeadlockRetries)
	}
}

// --- RetryOptions ----------------------------------------------------------

func TestRetryOptionsDefaults(t *testing.T) {
	// The defaults are pinned by spec 5.3 literals: the retry window of 24 hours, the batch
	// of 1000 — comparing with the very constant does not notice mutations.
	if DefaultRetryAfter != 24*time.Hour {
		t.Errorf("DefaultRetryAfter = %v, want 24h", DefaultRetryAfter)
	}
	if DefaultRetryBatch != 1000 {
		t.Errorf("DefaultRetryBatch = %d, want 1000", DefaultRetryBatch)
	}

	got := RetryOptions{}.withDefaults()
	want := RetryOptions{
		RetryAfter: DefaultRetryAfter,
		MaxRetries: DefaultRetryMaxRetries,
		BatchSize:  DefaultRetryBatch,
		// The interval is not longer than the retry window: 24 h > 1 h → an hour.
		Interval: DefaultRetryInterval,
	}
	if got != want {
		t.Errorf("withDefaults = %+v, want %+v", got, want)
	}

	// A short window shrinks the interval: the loop does not oversleep its retry_after.
	short := RetryOptions{RetryAfter: time.Minute}.withDefaults()
	if short.Interval != time.Minute {
		t.Errorf("the interval at retry_after 1m = %s, want 1m (the min of the window and the hour)", short.Interval)
	}

	set := RetryOptions{
		RetryAfter: time.Hour, MaxRetries: 5, BatchSize: 7, Interval: 9 * time.Hour,
	}.withDefaults()
	if set.RetryAfter != time.Hour || set.MaxRetries != 5 || set.BatchSize != 7 || set.Interval != 9*time.Hour {
		t.Errorf("withDefaults overwrites the given values: %+v", set)
	}

	// Replacement boundaries: 1 ns is a positive value, not replaced by a
	// default (the window and the interval — independently of each other).
	if got := (RetryOptions{RetryAfter: time.Nanosecond}).withDefaults().RetryAfter; got != time.Nanosecond {
		t.Errorf("RetryAfter 1ns = %v, want no replacement", got)
	}
	if got := (RetryOptions{Interval: time.Nanosecond}).withDefaults().Interval; got != time.Nanosecond {
		t.Errorf("Interval 1ns = %v, want no replacement", got)
	}
}

// --- RetryTempErrors --------------------------------------------------------

// TestRetryTempErrorsLoop — the first pass immediately; a pass error does not
// stop the loop; ctx cancellation returns nil (the OnRun contract, T-012).
func TestRetryTempErrorsLoop(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := &retryFakePool{
		// Pass 1 (immediately after the start) — work; pass 2 — a DB error;
		// then — empty sets.
		selects: []*retryFakeRows{
			retryRows([]any{int16(1), int64(10), int16(0)}),
			retryRows(),
		},
		selectErrs: []error{nil, errors.New("db unavailable")},
		requeues: []*retryFakeRows{retryRows(
			[]any{int16(1), int64(10), retryDay},
		)},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RetryTempErrors(ctx, pool, RetryOptions{
			// 10 ms: the window between observing the third selection (polling
			// 1 ms) and cancel is certainly shorter than the tick — the scenario's fourth pass
			// only occasionally manages to run (see the asserts below).
			Interval: 10 * time.Millisecond,
			Log:      logger,
		})
	}()

	// We wait for three passes (immediately + at least two ticks) and stop.
	deadline := time.Now().Add(2 * time.Second)
	for pool.stats().selects < 3 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("the loop did not reach the third pass: %+v", pool.stats())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("RetryTempErrors: %v, want nil on ctx cancellation", err)
	}

	st := pool.stats()
	// Transactions: the working pass — the selection + the update; the failing and
	// empty ones — only the selection (the update does not open); a failing
	// selection is not committed. The cancellation race: between observing the third
	// selection and cancel, the tick could have started (and, more rarely, finished)
	// the cyclic scenario's fourth pass — a working one: +1 begin and
	// possibly +1 commit; one interrupted midway — +1 begin without a commit.
	// The pass discipline is the same in all outcomes: commits ≤ begins,
	// the spread is exactly that extra pass.
	if st.begins < 4 || st.begins > 5 {
		t.Errorf("transactions %d, want 4–5 (the tick racing the cancellation)", st.begins)
	}
	if st.commits < 3 || st.commits > 4 || st.commits > st.begins {
		t.Errorf("commits %d at transactions %d, want 3–4 and no more than begins",
			st.commits, st.begins)
	}

	log := buf.String()
	for _, want := range []string{
		"background temp_error rechecks", // the start
		"temp_error recheck drain",  // the first drain's outcome
		"requeued=1",    // the pass counters
		"db unavailable", // the second pass's error
	} {
		if !strings.Contains(log, want) {
			t.Errorf("log does not contain %q:\n%s", want, log)
		}
	}
}

// TestRetryTempErrorsDrain — ripened candidates are processed by one
// drain until exhaustion (a full batch — the next pass immediately, without
// waiting for a tick), not one batch per interval (S-03).
func TestRetryTempErrorsDrain(t *testing.T) {
	pool := &retryFakePool{
		// Two full batches of 1 domain each, then exhaustion.
		selects: []*retryFakeRows{
			retryRows([]any{int16(1), int64(10), int16(0)}),
			retryRows([]any{int16(2), int64(20), int16(0)}),
			retryRows(),
		},
		requeues: []*retryFakeRows{
			retryRows([]any{int16(1), int64(10), retryDay}),
			retryRows([]any{int16(2), int64(20), retryDay}),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- RetryTempErrors(ctx, pool, RetryOptions{
			BatchSize: 1,
			Interval:  time.Hour, // the tick never comes: the drain must process everything
			Log:       slog.New(slog.DiscardHandler),
		})
	}()

	// Three SELECTs (two full batches + an empty one) must pass without a tick.
	deadline := time.Now().Add(2 * time.Second)
	for pool.stats().selects < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("the drain did not reach exhaustion: %+v", pool.stats())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("RetryTempErrors: %v", err)
	}
	st := pool.stats()
	// Both batches in one drain: three selections + two updates, all
	// committed.
	if st.begins != 5 || st.commits != 5 {
		t.Errorf("transactions %d, commits %d, want 5 and 5", st.begins, st.commits)
	}
}

// TestRetryTempErrorsDrainTotals — the drain sums the batch results:
// the Info outcome is the accumulated sum of all batches (including exactly 1);
// a zero drain does not log the outcome. The drain runs with ctx already cancelled:
// the fake returns no errors; the cancellation does not hinder the passes — the outcome must
// be tallied.
func TestRetryTempErrorsDrainTotals(t *testing.T) {
	tests := []struct {
		name      string
		selects   []*retryFakeRows
		requeues  []*retryFakeRows
		finalizes []*retryFakeRows
		opts      RetryOptions
		want      string // a substring of the drain's Info outcome; "" — the outcome is not logged
	}{
		{
			name: "two full retry batches — the sum 2",
			selects: []*retryFakeRows{
				retryRows([]any{int16(1), int64(10), int16(0)}),
				retryRows([]any{int16(2), int64(20), int16(0)}),
			},
			requeues: []*retryFakeRows{
				retryRows([]any{int16(1), int64(10), retryDay}),
				retryRows([]any{int16(2), int64(20), retryDay}),
			},
			opts: RetryOptions{BatchSize: 1},
			want: "requeued=2",
		},
		{
			name: "two full final batches — the sum 2",
			selects: []*retryFakeRows{
				retryRows([]any{int16(1), int64(10), int16(3)}),
				retryRows([]any{int16(2), int64(20), int16(3)}),
			},
			finalizes: []*retryFakeRows{
				retryRows([]any{int16(1), int64(10)}),
				retryRows([]any{int16(2), int64(20)}),
			},
			opts: RetryOptions{BatchSize: 1},
			want: "finalized=2",
		},
		{
			name:      "one full final batch — the sum 1",
			selects:   []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(3)})},
			finalizes: []*retryFakeRows{retryRows([]any{int16(1), int64(10)})},
			opts:      RetryOptions{BatchSize: 1},
			want:      "finalized=1",
		},
		{
			name: "an empty drain — no final",
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))
			pool := &retryFakePool{
				selects:   tt.selects,
				requeues:  tt.requeues,
				finalizes: tt.finalizes,
			}

			ctx, cancel := context.WithCancel(context.Background())
			cancel() // the starting drain is synchronous and independent of the tick
			done := make(chan error, 1)
			opts := tt.opts
			opts.Log = logger
			go func() { done <- RetryTempErrors(ctx, pool, opts) }()

			if err := <-done; err != nil {
				t.Fatalf("RetryTempErrors: %v, want nil", err)
			}
			log := buf.String()
			if tt.want == "" {
				if strings.Contains(log, "temp_error recheck drain") {
					t.Errorf("a zero drain summarizes:\n%s", log)
				}
			} else if !strings.Contains(log, tt.want) {
				t.Errorf("log does not contain %q:\n%s", tt.want, log)
			}
		})
	}
}

// TestRetryTempErrorsCancelDuringDrain — ctx cancellation during a drain is
// part of the stop: no Warn and no Info outcome (an unfinished drain does not
// draw the line); a clean nil (the OnRun contract, T-012).
func TestRetryTempErrorsCancelDuringDrain(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	// Batch 1 — full (the drain continues); batch 2 — ctx cancellation.
	pool := &retryFakePool{
		selects: []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(0)})},
		requeues: []*retryFakeRows{
			retryRows([]any{int16(1), int64(10), retryDay}),
		},
		selectErrs: []error{nil, context.Canceled},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		done <- RetryTempErrors(ctx, pool, RetryOptions{BatchSize: 1, Log: logger})
	}()

	if err := <-done; err != nil {
		t.Fatalf("RetryTempErrors: %v, want nil on ctx cancellation", err)
	}
	log := buf.String()
	if strings.Contains(log, "level=WARN") || strings.Contains(log, "requeued=") {
		t.Errorf("a cancellation during the drain — neither an alarm nor a final:\n%s", log)
	}
}

// TestRetryTempErrorsNilLog — a nil logger is replaced by a stub: the start
// record and the loop records go to slog.DiscardHandler without panicking on
// a nil sink.
func TestRetryTempErrorsNilLog(t *testing.T) {
	pool := &retryFakePool{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the starting drain is empty, an immediate clean exit

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("a panic at Log=nil: %v", r)
		}
	}()
	if err := RetryTempErrors(ctx, pool, RetryOptions{}); err != nil {
		t.Fatalf("RetryTempErrors: %v, want nil", err)
	}
	if st := pool.stats(); st.selects != 1 {
		t.Errorf("SELECTs = %d, want 1: the initial drain ran", st.selects)
	}
}

// TestRetryTempErrorsGuardDrainStop — the guard stops the drain without
// the heavy selection: the bucket sum is 0 after the cohort is exhausted (the decrements
// zeroed it) — the next drain pass reads only the bucket sum.
func TestRetryTempErrorsGuardDrainStop(t *testing.T) {
	pool := &retryFakePool{
		// Batch 1 — full; then the bucket sum is 0 (guardVals: a sticky
		// last value — 0 is pinned by the second element).
		guardVals: []int64{1, 0},
		selects:   []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(0)})},
		requeues:  []*retryFakeRows{retryRows([]any{int16(1), int64(10), retryDay})},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the starting drain is synchronous
	done := make(chan error, 1)
	go func() {
		done <- RetryTempErrors(ctx, pool, RetryOptions{BatchSize: 1, Log: slog.New(slog.DiscardHandler)})
	}()

	if err := <-done; err != nil {
		t.Fatalf("RetryTempErrors: %v", err)
	}
	st := pool.stats()
	if st.selects != 1 {
		t.Errorf("SELECTs = %d, want 1: the second drain iteration — the guard", st.selects)
	}
	if st.begins != 2 || st.commits != 2 {
		t.Errorf("transactions %d, commits %d, want 2 and 2 (selection + update)", st.begins, st.commits)
	}
}

func TestRetryTempErrorsNilPool(t *testing.T) {
	err := RetryTempErrors(context.Background(), nil, RetryOptions{})
	if err == nil || !strings.Contains(err.Error(), "pool not set") {
		t.Errorf("err = %v, want \"pool not set\"", err)
	}
}

// TestRetryTempErrorsDrainContinues — the T-268 acceptance criterion: a
// single batch error no longer wrecks the hourly drain (the Nov 07 16:04 incident —
// 118k domains waited for the next tick): the batch is retried; the drain
// reaches the set's exhaustion within the same tick.
func TestRetryTempErrorsDrainContinues(t *testing.T) {
	// syncBuf: the logger writes from the loop goroutine, the test reads from its own —
	// the buffer is under a lock (the backlog_test style, the -race race).
	buf := &syncBuf{}
	logger := slog.New(slog.NewTextHandler(buf, nil))
	// Batch 1 — a guard failure (the DB blinked); batch 2 — full (BatchSize 1);
	// batch 3 — empty (the set is exhausted, the drain is finished).
	pool := &retryFakePool{
		guardErrs: []error{errors.New("the db blipped")},
		selects: []*retryFakeRows{
			retryRows([]any{int16(1), int64(10), int16(0)}),
			retryRows(),
		},
		requeues: []*retryFakeRows{retryRows([]any{int16(1), int64(10), retryDay})},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RetryTempErrors(ctx, pool, RetryOptions{
			BatchSize: 1,
			Interval:  time.Hour, // the tick never comes: the same drain must continue
			Log:       logger,
		})
	}()

	// We wait for the drain's Info outcome — the proof that it reached the end,
	// not returned on the first batch's error.
	deadline := time.Now().Add(2 * time.Second)
	for !buf.contains("requeued=1") {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("the drain did not reach the final:\n%s", buf.String())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("RetryTempErrors: %v", err)
	}

	log := buf.String()
	for _, want := range []string{
		"batch failed — the drain continues", // the batch failure's WARN
		"the db blipped",
		"requeued=1", // the same drain's Info outcome
	} {
		if !strings.Contains(log, want) {
			t.Errorf("log does not contain %q:\n%s", want, log)
		}
	}
	if strings.Contains(log, "failed batch limit") {
		t.Errorf("a single failure stopped the drain:\n%s", log)
	}
	// Three passes completed: the failing one (the guard, without a transaction), the working
	// one (the selection + the decision) and the empty one (the selection).
	if st := pool.stats(); st.guards != 3 || st.selects != 2 || st.begins != 3 || st.commits != 3 {
		t.Errorf("stats = guards=%d selects=%d begins=%d commits=%d, want 3/2/3/3",
			st.guards, st.selects, st.begins, st.commits)
	}
}

// TestRetryTempErrorsDrainFailLimit — the cap on CONSECUTIVELY failed batches
// (DefaultRetryDrainFails, T-268): a persistent failure stops the drain
// until the next tick — without hammering the DB in vain; the failure counter is reset
// by a success (checked by TestRetryTempErrorsDrainContinues: after a failure
// the drain finished its work).
func TestRetryTempErrorsDrainFailLimit(t *testing.T) {
	// syncBuf — as in DrainContinues: the loop writes, the test reads.
	buf := &syncBuf{}
	logger := slog.New(slog.NewTextHandler(buf, nil))
	// All batch guard reads fail (a triple FIFO — by the number of attempts
	// of the limit) — the drain must stop at the limit, not keep
	// hammering.
	dbDown := errors.New("db unavailable")
	pool := &retryFakePool{guardErrs: []error{dbDown, dbDown, dbDown}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RetryTempErrors(ctx, pool, RetryOptions{
			Interval: time.Hour,
			Log:      logger,
		})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for !buf.contains("failed batch limit") {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("the drain did not stop at the failure limit:\n%s", buf.String())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("RetryTempErrors: %v", err)
	}

	// Exactly DefaultRetryDrainFails attempts — no more: a persistent failure
	// does not hammer the DB until the tick.
	if got := len(pool.guardQueries); got != DefaultRetryDrainFails {
		t.Errorf("guard reads = %d, want %d (failed batch limit)", got, DefaultRetryDrainFails)
	}
	if st := pool.stats(); st.begins != 0 || st.selects != 0 {
		t.Errorf("transactions/selections at a guard failure = %d/%d, want 0/0", st.begins, st.selects)
	}
}

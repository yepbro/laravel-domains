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
		return errors.New("retryFakeRows: Scan без Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("retryFakeRows: приёмников %d, колонок %d", len(dest), len(row))
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
	return 0, errors.New("CopyFrom не должен вызываться")
}

func (p *retryFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("Exec не должен вызываться")
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
		return nil, fmt.Errorf("retryFakePool: неожидаемый SQL через пул: %s", sql)
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
		return pgconn.CommandTag{}, fmt.Errorf("retryFakeTx: неожидаемый SQL Exec: %s", sql)
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
		return nil, fmt.Errorf("retryFakeTx: неожидаемый SQL в транзакции: %s", sql)
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
		t.Fatalf("аргумент границы окна = %T, want time.Time", args[2])
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
	t.Fatalf("в транзакции %p нет SELECT кандидатов: %v", tx, tx.calls)
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
		t.Fatalf("retrySelectSQL разошёлся с контрактом T-191 (без ORDER BY, LIMIT $4):\n%s", retrySelectSQL)
	}
	if retryPlanNoIndexScanSQL != "SET LOCAL enable_indexscan = off" ||
		retryPlanNoBitmapScanSQL != "SET LOCAL enable_bitmapscan = off" ||
		retryPlanNoIOSQL != "SET LOCAL enable_indexonlyscan = off" {
		t.Fatalf("закрепление плана выборки разошлось с контрактом T-191:\n%s\n%s\n%s",
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
		t.Fatalf("retryMatureSQL разошёлся с контрактом T-202:\n%s\nwant:\n%s", retryMatureSQL, wantGuard)
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
		t.Errorf("retryFinalizeSQL без декремента корзины зрелости (CTE rm, T-202):\n%s", retryFinalizeSQL)
	}

	// The retry assignment — WITHOUT counters in the statement (T-267): the statement
	// is strictly "domains → zones" — unreferenced DML-CTEs of zones and the counter
	// execute in an unpredictable order (the PG documentation); the inversion
	// rm → zc of the Nov 07 incident created the "counters → zones" edge against
	// zone holders (runbooks.md §2.8); the decrement — the second statement
	// of the same transaction over the moved rows' dates.
	if strings.Contains(retryRequeueSQL, "crawler_counters") {
		t.Errorf("retryRequeueSQL пишет счётчики в операторе зон (T-267 — вынесено вторым оператором):\n%s", retryRequeueSQL)
	}
	if retryRequeueMatureSQL != wantRetryRequeueMature {
		t.Errorf("retryRequeueMatureSQL разошёлся с контрактом T-267:\n%s\nwant:\n%s",
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
		{"сумма корзин 0", 0},
		{"сумма корзин отрицательная (дрейф декрементов)", -1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pool := &retryFakePool{guardVals: []int64{tt.value}}

			res, err := RetryOnce(context.Background(), pool, RetryOptions{})
			if err != nil {
				t.Fatalf("RetryOnce: %v", err)
			}
			if res != (RetryResult{}) {
				t.Errorf("итог = %+v, want нулевой: кандидатов нет по guard", res)
			}
			if len(pool.guardQueries) != 1 || pool.guardQueries[0].sql != retryMatureSQL {
				t.Errorf("guard-чтение = %v, want один запрос суммы корзин", pool.guardQueries)
			}
			if st := pool.stats(); st.selects != 0 || st.begins != 0 {
				t.Errorf("тяжёлая выборка/транзакции при сумме %d: selects=%d begins=%d, want 0 и 0",
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
		!strings.Contains(err.Error(), "scheduler: повторные проверки temp_error") ||
		!strings.Contains(err.Error(), "пустой ответ агрегата") {
		t.Errorf("ошибка = %v, want пустой ответ guard", err)
	}
	if st := pool.stats(); st.selects != 0 || st.begins != 0 {
		t.Errorf("тяжёлая выборка/транзакции при пустом guard: selects=%d begins=%d", st.selects, st.begins)
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
		t.Errorf("итог = %+v, want нулевой: назначать нечего", res)
	}
	st := pool.stats()
	if st.selects != 1 {
		t.Errorf("SELECT'ов = %d, want 1 (один холостой проход по счётчику)", st.selects)
	}
	if st.begins != 1 || len(pool.txs) != 1 {
		t.Fatalf("транзакций = %d, want 1 (только выборка)", st.begins)
	}
	tx := pool.txs[0]
	if !tx.committed {
		t.Error("транзакция выборки не зафиксирована")
	}
	if !tx.isSelect() {
		t.Errorf("единственная транзакция содержит UPDATE: %v", tx.calls)
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
		t.Fatalf("guard-чтений = %d, want 1", len(pool.guardQueries))
	}
	if pool.guardQueries[0].sql != retryMatureSQL || len(pool.guardQueries[0].args) != 0 {
		t.Errorf("guard-чтение = %+v, want %s без параметров", pool.guardQueries[0], retryMatureSQL)
	}

	// Three transactions: the selection (plan pinning + SELECT) and two batch
	// decisions — the retry assignment and the final status separately (the 40P01
	// deadlock regression, retryDecide).
	if pool.begins != 3 || len(pool.txs) != 3 {
		t.Fatalf("транзакций = %d, want 3 (выборка + повтор + итоговый статус)", pool.begins)
	}
	selTx, requeueTx, finalizeTx := pool.txs[0], pool.txs[1], pool.txs[2]

	// The candidate selection: the temp_error status, the done state, the batch limit.
	sel := retrySelectCall(t, selTx)
	if sel.sql != wantRetrySelect {
		t.Errorf("SQL выборки:\n%s\nwant:\n%s", sel.sql, wantRetrySelect)
	}
	gotStatus, gotState, gotLimit := sel.args[0], sel.args[1], sel.args[3]
	if gotStatus != int16(model.StatusTempError) || gotState != int16(model.CheckStateDone) {
		t.Errorf("фильтры выборки = статус %v, состояние %v, want temp_error (%d) и done (%d)",
			gotStatus, gotState, model.StatusTempError, model.CheckStateDone)
	}
	if gotLimit != 10 {
		t.Errorf("лимит пачки = %v, want 10", gotLimit)
	}

	// The decisions — each in its own transaction; the retry is TWO statements
	// (the move + the tail bucket decrement, T-267); the final status — one.
	if len(requeueTx.calls) != 2 {
		t.Fatalf("вызовов в транзакции повтора = %d, want 2 (перевод + декремент корзин)", len(requeueTx.calls))
	}
	if requeueTx.calls[0].sql != wantRetryRequeue {
		t.Errorf("SQL повтора:\n%s\nwant:\n%s", requeueTx.calls[0].sql, wantRetryRequeue)
	}
	if want := []any{
		[]int16{1, 1, 2}, []int64{10, 11, 20},
		int16(model.CheckStatePending), int16(model.StatusTempError), int16(model.CheckStateDone),
	}; !reflect.DeepEqual(requeueTx.calls[0].args, want) {
		t.Errorf("аргументы повтора = %v, want %v", requeueTx.calls[0].args, want)
	}
	if requeueTx.calls[1].sql != wantRetryRequeueMature {
		t.Errorf("SQL декремента корзин:\n%s\nwant:\n%s", requeueTx.calls[1].sql, wantRetryRequeueMature)
	}
	// The decrement — by the episode dates of the moved rows (both dates equal —
	// one bucket in the batch); the guard has no empty keys — the statement
	// is executed.
	if want := []any{[]time.Time{retryDay, retryDay}}; !reflect.DeepEqual(requeueTx.calls[1].args, want) {
		t.Errorf("аргументы декремента корзин = %v, want %v", requeueTx.calls[1].args, want)
	}
	if len(finalizeTx.calls) != 1 {
		t.Fatalf("вызовов в транзакции итогового статуса = %d, want 1 (декремент — в самом операторе)", len(finalizeTx.calls))
	}
	if finalizeTx.calls[0].sql != wantRetryFinalize {
		t.Errorf("SQL итогового статуса:\n%s\nwant:\n%s", finalizeTx.calls[0].sql, wantRetryFinalize)
	}
	if want := []any{
		[]int16{2, 3}, []int64{21, 30},
		int16(model.StatusDelegatedNoAddress), int16(model.StatusTempError), int16(model.CheckStateDone),
	}; !reflect.DeepEqual(finalizeTx.calls[0].args, want) {
		t.Errorf("аргументы итогового статуса = %v, want %v", finalizeTx.calls[0].args, want)
	}

	// The counters — by the rows actually affected (the guard blocked 1/11).
	if want := (RetryResult{Requeued: 2, Finalized: 2}); res != want {
		t.Errorf("итог = %+v, want %+v", res, want)
	}
	for _, tx := range []*retryFakeTx{selTx, requeueTx, finalizeTx} {
		if !tx.committed || tx.rolledBack {
			t.Errorf("транзакция %p: committed=%v rolledBack=%v, want фиксация без отката", tx, tx.committed, tx.rolledBack)
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
		t.Fatalf("транзакций = %d, want 3 (выборка + два решения)", len(pool.txs))
	}
	selTx, requeueTx, finalizeTx := pool.txs[0], pool.txs[1], pool.txs[2]

	// Each decision — only its own statements: mixing both decisions in
	// one tx is exactly the lock order inversion.
	for _, tx := range []*retryFakeTx{requeueTx, finalizeTx} {
		if tx.isSelect() {
			t.Errorf("транзакция решения содержит SELECT кандидатов: %v", tx.calls)
		}
	}
	// The retry half — the move and the tail bucket decrement (by two
	// statements, T-267: the statements of a transaction execute in textual
	// order — the order of one statement's unreferenced DML-CTEs PG does not
	// guarantee it).
	if len(requeueTx.calls) != 2 ||
		requeueTx.calls[0].sql != retryRequeueSQL ||
		requeueTx.calls[1].sql != retryRequeueMatureSQL {
		t.Errorf("транзакция повтора = %v, want retryRequeueSQL + retryRequeueMatureSQL", requeueTx.calls)
	}
	if len(finalizeTx.calls) != 1 || finalizeTx.calls[0].sql != retryFinalizeSQL {
		t.Errorf("транзакция итогового статуса = %v, want только retryFinalizeSQL", finalizeTx.calls)
	}

	// The pool's event order: the retry's Commit strictly before the final
	// status's Begin — the first half's counters/zones do not survive the
	// second half's domains row capture.
	if requeueTx.commitSeq == 0 || finalizeTx.beginSeq == 0 ||
		requeueTx.commitSeq > finalizeTx.beginSeq {
		t.Errorf("порядок событий: commit повтора %d, begin итога %d — want commit прежде begin",
			requeueTx.commitSeq, finalizeTx.beginSeq)
	}
	for _, tx := range []*retryFakeTx{selTx, requeueTx, finalizeTx} {
		if !tx.committed || tx.rolledBack {
			t.Errorf("транзакция %p: committed=%v rolledBack=%v, want фиксация без отката", tx, tx.committed, tx.rolledBack)
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
		t.Fatalf("транзакций = %d, want 2", len(pool.txs))
	}
	calls := pool.txs[0].calls
	if len(calls) != 4 {
		t.Fatalf("вызовов в транзакции выборки = %d, want 4 (SET LOCAL ×3 + SELECT): %v", len(calls), calls)
	}
	if want := []string{retryPlanNoIndexScanSQL, retryPlanNoBitmapScanSQL, retryPlanNoIOSQL, retrySelectSQL}; true {
		for i, sql := range want {
			if calls[i].sql != sql {
				t.Errorf("шаг %d = %s, want %s (порядок SET LOCAL ×3 → SELECT)", i, calls[i].sql, sql)
			}
		}
	}
	if st := pool.stats(); st.guards != 1 {
		t.Errorf("guard-чтений = %d, want 1 (guard — до транзакции выборки)", st.guards)
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
		{"дефолт 24ч", RetryOptions{}, DefaultRetryAfter},
		{"явное окно 2ч", RetryOptions{RetryAfter: 2 * time.Hour}, 2 * time.Hour},
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
				t.Errorf("граница окна = %s, want ≈ now − %s", cutoff, tt.wantWindow)
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
		{"0 из 3", RetryOptions{}, 0, true},
		{"N−1 из 3", RetryOptions{}, 2, true},
		{"N из 3", RetryOptions{}, 3, false},
		{"N+1 из 3 (защита от перебора)", RetryOptions{}, 4, false},
		{"N−1 из 1", RetryOptions{MaxRetries: 1}, 0, true},
		{"N из 1", RetryOptions{MaxRetries: 1}, 1, false},
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
					t.Errorf("итог = %+v, want повтор одной проверки", res)
				}
				if len(tx.calls) != 2 || tx.calls[0].sql != wantRetryRequeue ||
					tx.calls[1].sql != retryRequeueMatureSQL {
					t.Errorf("вызовы = %d, want перевод + декремент корзин", len(tx.calls))
				}
			} else {
				if res != (RetryResult{Finalized: 1}) {
					t.Errorf("итог = %+v, want один итоговый статус", res)
				}
				if len(tx.calls) != 1 || tx.calls[0].sql != wantRetryFinalize {
					t.Errorf("вызовы = %d, want только итоговый статус", len(tx.calls))
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
		t.Errorf("лимит пачки = %v, want дефолт %d", got, DefaultRetryBatch)
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
		t.Fatalf("первый проход: %v", err)
	}
	if first.Requeued != 1 {
		t.Fatalf("первый проход = %+v, want 1 повтор", first)
	}

	second, err := RetryOnce(context.Background(), pool, RetryOptions{})
	if err != nil {
		t.Fatalf("второй проход: %v", err)
	}
	if second != (RetryResult{}) {
		t.Errorf("второй проход = %+v, want нулевой", second)
	}
	if st := pool.stats(); st.selects != 2 {
		t.Errorf("SELECT'ов = %d, want 2 (по одному на проход)", st.selects)
	}
	// The first pass: the selection and update transactions; the second — only
	// the selection (empty): rows moved to pending are not candidates.
	if st := pool.stats(); st.begins != 3 {
		t.Errorf("транзакций = %d, want 3: повторный проход не открывает обновление", st.begins)
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
		t.Errorf("итог = %+v, want нулевой: охрана отбила кандидата", res)
	}
	if !pool.txs[1].committed {
		t.Error("транзакция обновления не зафиксирована: пустой RETURNING — не ошибка")
	}
	if len(pool.txs[1].calls) != 1 || pool.txs[1].calls[0].sql != retryRequeueSQL {
		t.Errorf("транзакция повтора = %v, want только retryRequeueSQL (декремент — по непустым ключам)", pool.txs[1].calls)
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
		{"ошибка guard", func() *retryFakePool {
			p := &retryFakePool{guardErrs: []error{errors.New("бд недоступна")}}
			return p
		}(), "корзины зрелости"},
		{"ошибка закрепления плана", func() *retryFakePool {
			p := newPool()
			p.planErr = errors.New("бд недоступна")
			return p
		}(), "закрепление плана"},
		{"ошибка SELECT", func() *retryFakePool {
			p := newPool()
			p.selectErrs = []error{errors.New("бд недоступна")}
			return p
		}(), "выборка кандидатов"},
		{"ошибка Begin", func() *retryFakePool {
			p := newPool()
			p.beginErr = errors.New("бд недоступна")
			return p
		}(), "транзакция выборки"},
		{"ошибка повтора", func() *retryFakePool {
			p := newPool()
			p.requeueErr = errors.New("бд недоступна")
			return p
		}(), "назначение повтора"},
		{"ошибка декремента корзин", func() *retryFakePool {
			p := newPool()
			p.matureErr = errors.New("бд недоступна")
			return p
		}(), "декремент корзин зрелости"},
		{"ошибка итога", func() *retryFakePool {
			// A candidate with exhausted attempts — the final status
			// branch runs.
			p := newPool()
			p.selects = []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(DefaultRetryMaxRetries)})}
			p.finalizeErr = errors.New("бд недоступна")
			return p
		}(), "итоговый статус"},
		{"ошибка Begin решения", func() *retryFakePool {
			// The selection passes; the retry assignment transaction does not
			// open (after the decision split — an independent
			// failure point).
			p := newPool()
			p.beginErrs = []error{nil, errors.New("бд недоступна")}
			return p
		}(), "транзакция решения (назначение повтора)"},
		{"ошибка Commit выборки", func() *retryFakePool {
			p := newPool()
			p.commitErrs = []error{errors.New("бд недоступна")}
			return p
		}(), "фиксация выборки"},
		{"ошибка Commit повтора", func() *retryFakePool {
			p := newPool()
			p.commitErrs = []error{nil, errors.New("бд недоступна")}
			return p
		}(), "фиксация решения (назначение повтора)"},
		{"ошибка Commit итога", func() *retryFakePool {
			// A mixed batch: the retry commit passes; the final status
			// commit — does not; the committed half is not
			// rolled back (separate decision transactions).
			p := newPool()
			p.selects = []*retryFakeRows{retryRows(
				[]any{int16(1), int64(10), int16(0)},
				[]any{int16(2), int64(20), int16(DefaultRetryMaxRetries)},
			)}
			p.commitErrs = []error{nil, nil, errors.New("бд недоступна")}
			return p
		}(), "фиксация решения (итоговый статус)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := RetryOnce(context.Background(), tt.pool, RetryOptions{})
			if err == nil {
				t.Fatal("ожидалась ошибка")
			}
			if !strings.Contains(err.Error(), "scheduler: повторные проверки temp_error") ||
				!strings.Contains(err.Error(), tt.context) ||
				!strings.Contains(err.Error(), "бд недоступна") {
				t.Errorf("сообщение = %q, want контекст %q и исходная ошибка", err, tt.context)
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
					t.Errorf("транзакция не откачена при ошибке %q", tt.name)
				}
				if !tx.committed && !tx.isSelect() && i != len(tt.pool.txs)-1 {
					t.Errorf("после откаченной транзакции решений открыта ещё одна (кейс %q)", tt.name)
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
		matureErr: errors.New("бд недоступна"),
	}

	_, err := RetryOnce(context.Background(), pool, RetryOptions{})
	if err == nil || !strings.Contains(err.Error(), "декремент корзин зрелости") ||
		!errors.Is(err, pool.matureErr) {
		t.Fatalf("ошибка = %v, want «декремент корзин зрелости» и исходная", err)
	}
	if len(pool.txs) != 2 {
		t.Fatalf("транзакций = %d, want 2 (выборка + половина повтора)", len(pool.txs))
	}
	half := pool.txs[1]
	if half.committed || !half.rolledBack {
		t.Error("половина повтора зафиксирована или не откачена при сбое декремента")
	}
	if len(half.calls) != 2 || half.calls[0].sql != retryRequeueSQL || half.calls[1].sql != retryRequeueMatureSQL {
		t.Errorf("транзакция повтора = %v, want перевод + упавший декремент корзин", half.calls)
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
		commitErrs: []error{nil, nil, errors.New("бд недоступна")},
	}

	res, err := RetryOnce(context.Background(), pool, RetryOptions{})
	if err == nil || !strings.Contains(err.Error(), "фиксация решения (итоговый статус)") {
		t.Fatalf("ошибка = %v, want сбой фиксации итогового статуса", err)
	}
	if res != (RetryResult{}) {
		t.Errorf("итог при ошибке = %+v, want нулевой (вызыватель видит только ошибку)", res)
	}
	if len(pool.txs) != 3 {
		t.Fatalf("транзакций = %d, want 3", len(pool.txs))
	}
	if !pool.txs[1].committed || pool.txs[1].rolledBack {
		t.Error("половина повтора не зафиксирована: сбой второй половины не откатывает первую")
	}
	if pool.txs[2].committed || !pool.txs[2].rolledBack {
		t.Error("половина итогового статуса зафиксирована или не откачена при сбое Commit")
	}
}

// TestRetryOnceCtxCanceled — ctx cancellation goes through the pass steps
// as an error (errors.Is); the update transaction is not committed.
func TestRetryOnceCtxCanceled(t *testing.T) {
	guardPool := &retryFakePool{guardErrs: []error{context.Canceled}}
	if _, err := RetryOnce(context.Background(), guardPool, RetryOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка guard = %v, want context.Canceled", err)
	}

	selectPool := &retryFakePool{selectErrs: []error{context.Canceled}}
	if _, err := RetryOnce(context.Background(), selectPool, RetryOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка выборки = %v, want context.Canceled", err)
	}

	requeuePool := &retryFakePool{
		selects:  []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(0)})},
		requeues: []*retryFakeRows{retryRows()},
	}
	requeuePool.requeueErr = context.Canceled
	if _, err := RetryOnce(context.Background(), requeuePool, RetryOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка повтора = %v, want context.Canceled", err)
	}
	if requeuePool.txs[1].committed {
		t.Error("транзакция обновления закоммичена при отмене контекста")
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
		!strings.Contains(err.Error(), "чтение кандидатов") {
		t.Errorf("ошибка Scan кандидатов = %v, want «чтение кандидатов»", err)
	}

	// rows.Err after reading the candidates.
	boom := errors.New("бд недоступна")
	candRows := retryRows([]any{int16(1), int64(10), int16(0)})
	candRows.err = boom
	candPool := &retryFakePool{selects: []*retryFakeRows{candRows}}
	_, err := RetryOnce(context.Background(), candPool, RetryOptions{})
	if err == nil || !strings.Contains(err.Error(), "выборка кандидатов") || !errors.Is(err, boom) {
		t.Errorf("ошибка rows.Err кандидатов = %v, want «выборка кандидатов» и исходная", err)
	}

	// The RETURNING key Scan: the second row with one column instead of three
	// (the move returns the zone, the domain and the episode date — T-267).
	keyScanPool := &retryFakePool{
		selects:  []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(0)})},
		requeues: []*retryFakeRows{retryRows([]any{int16(1), int64(10), retryDay}, []any{int16(2)})},
	}
	if _, err := RetryOnce(context.Background(), keyScanPool, RetryOptions{}); err == nil ||
		!strings.Contains(err.Error(), "чтение ключей") {
		t.Errorf("ошибка Scan ключей = %v, want «чтение ключей»", err)
	}

	// rows.Err after reading the RETURNING keys.
	keyRows := retryRows([]any{int16(1), int64(10), retryDay})
	keyRows.err = boom
	keyPool := &retryFakePool{
		selects:  []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(0)})},
		requeues: []*retryFakeRows{keyRows},
	}
	_, err = RetryOnce(context.Background(), keyPool, RetryOptions{})
	if err == nil || !strings.Contains(err.Error(), "назначение повтора") || !errors.Is(err, boom) {
		t.Errorf("ошибка rows.Err обновления = %v, want «назначение повтора» и исходная", err)
	}
}

// TestRetryOnceNilPool — the pool must be set.
func TestRetryOnceNilPool(t *testing.T) {
	if _, err := RetryOnce(context.Background(), nil, RetryOptions{}); err == nil ||
		!strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
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
		t.Fatalf("RetryOnce: %v, want nil — дедлок поглощён", err)
	}
	if res.Requeued != 1 || res.Finalized != 0 {
		t.Fatalf("итог = %+v, want повтор одного домена", res)
	}
	// Transactions: the selection + TWO decision attempts — the first rolled back
	// (no move), the second committed.
	if len(pool.txs) != 3 {
		t.Fatalf("транзакций = %d, want 3 (выборка + попытка + повтор)", len(pool.txs))
	}
	failTx, okTx := pool.txs[1], pool.txs[2]
	if failTx.committed || !failTx.rolledBack {
		t.Error("первая попытка не откачена при дедлоке")
	}
	if len(failTx.calls) != 1 || failTx.calls[0].sql != retryRequeueSQL {
		t.Errorf("попытка = %v, want только упавший перевод (декремент не открывался)", failTx.calls)
	}
	if !okTx.committed || okTx.rolledBack {
		t.Error("повтор не зафиксирован")
	}
	if len(okTx.calls) != 2 || okTx.calls[0].sql != retryRequeueSQL || okTx.calls[1].sql != retryRequeueMatureSQL {
		t.Errorf("повтор = %v, want перевод + декремент корзин", okTx.calls)
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
		!strings.Contains(err.Error(), "назначение повтора") {
		t.Fatalf("ошибка = %v, want 40P01 «назначение повтора» наружу", err)
	}
	if res != (RetryResult{}) {
		t.Errorf("итог при ошибке = %+v, want нулевой", res)
	}
	// The selection + the first attempt + MaxDeadlockRetries retries — all
	// rolled back; none reached the bucket decrement.
	if len(pool.txs) != 2+db.MaxDeadlockRetries {
		t.Fatalf("транзакций = %d, want %d (выборка + попытки)", len(pool.txs), 2+db.MaxDeadlockRetries)
	}
	for _, tx := range pool.txs[1:] {
		if tx.committed || !tx.rolledBack {
			t.Errorf("попытка решения %p: committed=%v rolledBack=%v, want откат", tx, tx.committed, tx.rolledBack)
		}
		if len(tx.calls) != 1 || tx.calls[0].sql != retryRequeueSQL {
			t.Errorf("попытка %p = %v, want только упавший перевод", tx, tx.calls)
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
		t.Errorf("DefaultRetryAfter = %v, want 24ч", DefaultRetryAfter)
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
		t.Errorf("интервал при retry_after 1м = %s, want 1м (min окна и часа)", short.Interval)
	}

	set := RetryOptions{
		RetryAfter: time.Hour, MaxRetries: 5, BatchSize: 7, Interval: 9 * time.Hour,
	}.withDefaults()
	if set.RetryAfter != time.Hour || set.MaxRetries != 5 || set.BatchSize != 7 || set.Interval != 9*time.Hour {
		t.Errorf("withDefaults затирает заданные значения: %+v", set)
	}

	// Replacement boundaries: 1 ns is a positive value, not replaced by a
	// default (the window and the interval — independently of each other).
	if got := (RetryOptions{RetryAfter: time.Nanosecond}).withDefaults().RetryAfter; got != time.Nanosecond {
		t.Errorf("RetryAfter 1нс = %v, want без замены", got)
	}
	if got := (RetryOptions{Interval: time.Nanosecond}).withDefaults().Interval; got != time.Nanosecond {
		t.Errorf("Interval 1нс = %v, want без замены", got)
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
		selectErrs: []error{nil, errors.New("бд недоступна")},
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
			t.Fatalf("цикл не дошёл до третьего прохода: %+v", pool.stats())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("RetryTempErrors: %v, want nil при отмене ctx", err)
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
		t.Errorf("транзакций %d, want 4–5 (гонка тика с отменой)", st.begins)
	}
	if st.commits < 3 || st.commits > 4 || st.commits > st.begins {
		t.Errorf("коммитов %d при транзакциях %d, want 3–4 и не больше begins",
			st.commits, st.begins)
	}

	log := buf.String()
	for _, want := range []string{
		"фоновые повторные проверки temp_error", // the start
		"дренаж повторных проверок temp_error",  // the first drain's outcome
		"requeued=1",    // the pass counters
		"бд недоступна", // the second pass's error
	} {
		if !strings.Contains(log, want) {
			t.Errorf("лог не содержит %q:\n%s", want, log)
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
			t.Fatalf("дренаж не дошёл до исчерпания: %+v", pool.stats())
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
		t.Errorf("транзакций %d, коммитов %d, want 5 и 5", st.begins, st.commits)
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
			name: "две полные пачки повторов — сумма 2",
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
			name: "две полные пачки итогов — сумма 2",
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
			name:      "одна полная пачка итогов — сумма 1",
			selects:   []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(3)})},
			finalizes: []*retryFakeRows{retryRows([]any{int16(1), int64(10)})},
			opts:      RetryOptions{BatchSize: 1},
			want:      "finalized=1",
		},
		{
			name: "пустой дренаж — без итога",
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
				if strings.Contains(log, "дренаж повторных проверок temp_error") {
					t.Errorf("нулевой дренаж подводит итог:\n%s", log)
				}
			} else if !strings.Contains(log, tt.want) {
				t.Errorf("лог не содержит %q:\n%s", tt.want, log)
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
		t.Fatalf("RetryTempErrors: %v, want nil при отмене ctx", err)
	}
	log := buf.String()
	if strings.Contains(log, "level=WARN") || strings.Contains(log, "requeued=") {
		t.Errorf("отмена во время дренажа — не тревога и не итог:\n%s", log)
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
			t.Fatalf("паника при Log=nil: %v", r)
		}
	}()
	if err := RetryTempErrors(ctx, pool, RetryOptions{}); err != nil {
		t.Fatalf("RetryTempErrors: %v, want nil", err)
	}
	if st := pool.stats(); st.selects != 1 {
		t.Errorf("SELECT'ов = %d, want 1: стартовый дренаж выполнен", st.selects)
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
		t.Errorf("SELECT'ов = %d, want 1: вторая итерация дренажа — guard", st.selects)
	}
	if st.begins != 2 || st.commits != 2 {
		t.Errorf("транзакций %d, коммитов %d, want 2 и 2 (выборка + обновление)", st.begins, st.commits)
	}
}

func TestRetryTempErrorsNilPool(t *testing.T) {
	err := RetryTempErrors(context.Background(), nil, RetryOptions{})
	if err == nil || !strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
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
		guardErrs: []error{errors.New("бд моргнула")},
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
			t.Fatalf("дренаж не дошёл до итога:\n%s", buf.String())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("RetryTempErrors: %v", err)
	}

	log := buf.String()
	for _, want := range []string{
		"пачка не прошла — дренаж продолжается", // the batch failure's WARN
		"бд моргнула",
		"requeued=1", // the same drain's Info outcome
	} {
		if !strings.Contains(log, want) {
			t.Errorf("лог не содержит %q:\n%s", want, log)
		}
	}
	if strings.Contains(log, "предел упавших пачек") {
		t.Errorf("единичный сбой остановил дренаж:\n%s", log)
	}
	// Three passes completed: the failing one (the guard, without a transaction), the working
	// one (the selection + the decision) and the empty one (the selection).
	if st := pool.stats(); st.guards != 3 || st.selects != 2 || st.begins != 3 || st.commits != 3 {
		t.Errorf("статистика = guards=%d selects=%d begins=%d commits=%d, want 3/2/3/3",
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
	dbDown := errors.New("бд недоступна")
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
	for !buf.contains("предел упавших пачек") {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("дренаж не остановился на пределе сбоёв:\n%s", buf.String())
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
		t.Errorf("guard-чтений = %d, want %d (предел упавших пачек)", got, DefaultRetryDrainFails)
	}
	if st := pool.stats(); st.begins != 0 || st.selects != 0 {
		t.Errorf("транзакций/выборок при сбое guard = %d/%d, want 0/0", st.begins, st.selects)
	}
}

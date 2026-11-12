// Unit tests of the "hung status" compensator (T-270, spec tasks/TZ-fsm.md §3
// FR-2.2) on a fake pgx pool with transactions (the retry_test.go style,
// the positional Scan — the shared retryRows): the exact SQL texts of the guard/selection/
// move and parameters (the now() − 24 h threshold, the batch LIMIT, the guards
// new ∧ done ∧ age, the queued_at reset, the zone deltas checked −n /
// pending +n), the selection plan pinning SET LOCAL ×3, SEPARATE
// transactions of the selection and the move, counting by RETURNING, a zero guard — a pass
// without touching domains (a cheap empty-set probe), an empty batch —
// a no-op (the idempotency of a repeated pass), step errors, a 40P01 retry
// of the move; the crawler_scheduler_stale_requeued_total counter grows by the
// moves; the StaleRequeueLoop cycle drain in batches until exhaustion, surviving
// an error. Behavior against a live DB (including the lost
// q:http and the new_done_stale gauge) — integration tests
// (test/integration/stalerequeue_test.go).
package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/metrics"
	"crawler/internal/model"
)

// --- stubs -------------------------------------------------------------

// staleNow — the fixed pass clocks (the lease test style): the threshold
// is computed from them; the test checks the exact value.
var staleNow = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)

// staleSeedBatch — a batch stub: n rows, translated RETURNING rows
// (the guard could have filtered out part).
func staleSeedBatch(rows [][2]int64, translated int64) (*retryFakeRows, *retryFakeRows) {
	sel := make([][]any, len(rows))
	for i, r := range rows {
		sel[i] = []any{int16(r[0]), r[1]}
	}
	upd := make([][]any, translated)
	for i := range upd {
		upd[i] = []any{int16(1)}
	}
	return retryRows(sel...), retryRows(upd...)
}

// staleFakePool — a StaleRequeueDB without a DB: the guard read of the
// new_done_stale level via the pool; the queues of stubbed SELECT results of the
// batch (in the plan pinning transaction) and UPDATE … RETURNING (in
// the move transaction), per-step pass errors, the call protocol and the
// operation order. Everything under a lock: the StaleRequeueLoop loop runs in
// a goroutine; the test waits for progress by counters.
type staleFakePool struct {
	mu sync.Mutex

	guardVal   int64            // the new_done_stale level (0 — the guard is closed)
	guardErrs  []error          // the FIFO of guard errors (nil — a read without an error)
	guardEmpty bool             // the guard returns an empty set — a protocol break
	selects    []*retryFakeRows // the FIFO of batch SELECT results (ran out — an empty set)
	updates    []*retryFakeRows // the FIFO of UPDATE RETURNING results
	selectErrs []error          // the FIFO of SELECT errors (nil — no error)
	planErr    error            // the plan pinning error (SET LOCAL)
	updateErrs []error          // the FIFO of one-off UPDATE errors (nil — an attempt without an error)
	beginErr   error            // the Begin error
	commitErrs []error          // the FIFO of Commit errors (ran out — a commit without an error)

	guardQueries []retryCall // guard reads via the pool
	selectCalls  []retryCall // the batch SELECT (in pinning transactions)
	updateCalls  []retryCall // the move UPDATE (in move transactions)
	planCalls    []retryCall // the plan pinning SET LOCALs
	ops          []string    // the operation order: begin/plan/select/update/commit
	begins       int
	commits      int
}

func (p *staleFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

func (p *staleFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("Exec не должен вызываться")
}

// Query — the guard read of the new_done_stale level: the pass's only SELECT
// outside transactions; the batch selection runs in the plan pinning transaction.
func (p *staleFakePool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.guardQueries = append(p.guardQueries, retryCall{sql: sql, args: args})
	if sql != staleRequeueGuardSQL {
		return nil, errStaleUnexpected("пул", sql)
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
	return retryRows([]any{p.guardVal}), nil
}

func (p *staleFakePool) Begin(context.Context) (pgx.Tx, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.begins++
	p.ops = append(p.ops, "begin")
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	return &staleFakeTx{pool: p}, nil
}

// nextErrLocked — an error from the FIFO queue of one-off errors (nil —
// an attempt without an error); the queue is empty — nil.
func (p *staleFakePool) nextErrLocked(queue *[]error) error {
	if len(*queue) == 0 {
		return nil
	}
	err := (*queue)[0]
	*queue = (*queue)[1:]
	return err
}

func errStaleUnexpected(where, sql string) error {
	return errors.New("staleFakePool: неожидаемый SQL через " + where + ": " + sql)
}

// snapshots of the protocol under the lock (waiting for loop progress in tests).

type staleStats struct {
	guards  int // guard reads of the level
	selects int // batch selections
	begins  int
	commits int
}

func (p *staleFakePool) stats() staleStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return staleStats{guards: len(p.guardQueries), selects: len(p.selectCalls), begins: p.begins, commits: p.commits}
}

func (p *staleFakePool) opsSnapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.ops...)
}

func (p *staleFakePool) selectSnapshot() []retryCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]retryCall(nil), p.selectCalls...)
}

func (p *staleFakePool) updateSnapshot() []retryCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]retryCall(nil), p.updateCalls...)
}

func (p *staleFakePool) planSnapshot() []retryCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]retryCall(nil), p.planCalls...)
}

// staleFakeTx — the fake pool's transaction: plan pinning (Exec) and
// either the batch selection or the move (Query) — by the SQL.
type staleFakeTx struct {
	pgx.Tx // the other pgx.Tx methods are not used by the pass
	pool   *staleFakePool
}

func (t *staleFakeTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	switch sql {
	case staleRequeuePlanNoIndexScanSQL, staleRequeuePlanNoBitmapScanSQL, staleRequeuePlanNoIOSQL:
		t.pool.planCalls = append(t.pool.planCalls, retryCall{sql: sql, args: args})
		t.pool.ops = append(t.pool.ops, "plan")
		if t.pool.planErr != nil {
			return pgconn.CommandTag{}, t.pool.planErr
		}
		return pgconn.NewCommandTag("SET"), nil
	default:
		return pgconn.CommandTag{}, errStaleUnexpected("Exec транзакции", sql)
	}
}

func (t *staleFakeTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	switch sql {
	case staleRequeueSelectSQL:
		t.pool.selectCalls = append(t.pool.selectCalls, retryCall{sql: sql, args: args})
		t.pool.ops = append(t.pool.ops, "select")
		if err := t.pool.nextErrLocked(&t.pool.selectErrs); err != nil {
			return nil, err
		}
		if len(t.pool.selects) > 0 {
			rows := t.pool.selects[0]
			t.pool.selects = t.pool.selects[1:]
			return rows, nil
		}
		return retryRows(), nil // the set is exhausted
	case staleRequeueUpdateSQL:
		t.pool.updateCalls = append(t.pool.updateCalls, retryCall{sql: sql, args: args})
		t.pool.ops = append(t.pool.ops, "update")
		if err := t.pool.nextErrLocked(&t.pool.updateErrs); err != nil {
			return nil, err
		}
		if len(t.pool.updates) > 0 {
			rows := t.pool.updates[0]
			t.pool.updates = t.pool.updates[1:]
			return rows, nil
		}
		return retryRows(), nil
	default:
		return nil, errStaleUnexpected("Query транзакции", sql)
	}
}

func (t *staleFakeTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.pool.ops = append(t.pool.ops, "commit")
	if len(t.pool.commitErrs) > 0 {
		err := t.pool.commitErrs[0]
		t.pool.commitErrs = t.pool.commitErrs[1:]
		if err != nil {
			return err
		}
	}
	t.pool.commits++
	return nil
}

// Rollback — a no-op: a rollback after Commit is empty; the interrupted transaction
// changed nothing (the retryFakeTx style).
func (t *staleFakeTx) Rollback(context.Context) error { return nil }

// --- expected SQL texts (the R15/FR-2.2 contract) ----------------------------

const wantStaleRequeueGuard = "SELECT coalesce((SELECT value FROM crawler_counters WHERE name = 'new_done_stale'), 0)"

const wantStaleRequeueSelect = "SELECT d.zone_id, d.id FROM domains AS d " +
	"WHERE d.status = 0 AND d.check_state = 2 AND d.last_checked_at < $1 " +
	"LIMIT $2"

const wantStaleRequeueUpdate = "WITH upd AS (UPDATE domains AS t " +
	"SET check_state = $1, queued_at = NULL " +
	"FROM unnest($2::int2[], $3::int8[]) AS u(zone_id, id) " +
	"WHERE t.zone_id = u.zone_id AND t.id = u.id " +
	"AND t.check_state = 2 AND t.status = 0 AND t.last_checked_at < $4 " +
	"RETURNING t.zone_id), " +
	"zc AS (UPDATE zones AS z " +
	"SET domains_checked = z.domains_checked - c.n, " +
	"domains_pending = z.domains_pending + c.n " +
	"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c " +
	"WHERE z.id = c.zone_id) " +
	"SELECT zone_id FROM upd"

// --- StaleRequeueOnce -------------------------------------------------------

// TestStaleRequeueOnceSQL — the contract of all pass statements: the guard —
// a point lookup of the level; the batch SELECT — new ∧ done ∧ age without
// sorting (an early LIMIT stop); UPDATE … FROM unnest with guard re-checks
// and zone deltas checked −n / pending +n; exact parameters (the
// now() − 24 h threshold, the LIMIT, the codes as literals), SEPARATE transactions of the
// selection (plan pinning SET LOCAL ×3 before the SELECT) and the move; counting by
// RETURNING.
func TestStaleRequeueOnceSQL(t *testing.T) {
	sel, upd := staleSeedBatch([][2]int64{{1, 1}, {1, 2}, {2, 3}}, 2)
	pool := &staleFakePool{guardVal: 1, selects: []*retryFakeRows{sel}, updates: []*retryFakeRows{upd}}

	res, err := StaleRequeueOnce(context.Background(), pool, StaleRequeueOptions{
		BatchSize: 7000,
		Now:       func() time.Time { return staleNow },
	})
	if err != nil {
		t.Fatalf("StaleRequeueOnce: %v", err)
	}

	want := []string{"begin", "plan", "plan", "plan", "select", "commit", "begin", "update", "commit"}
	if got := pool.opsSnapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("порядок операций = %v, want %v — выборка и перевод раздельными транзакциями", got, want)
	}

	guards := pool.guardQueries
	if len(guards) != 1 || guards[0].sql != wantStaleRequeueGuard || len(guards[0].args) != 0 {
		t.Errorf("guard-чтение = %+v, want SQL уровня new_done_stale без аргументов", guards)
	}

	sels := pool.selectSnapshot()
	if len(sels) != 1 {
		t.Fatalf("SELECT-вызовов пачки = %d, want 1", len(sels))
	}
	if sels[0].sql != wantStaleRequeueSelect {
		t.Errorf("SQL выборки:\n%s\nwant:\n%s", sels[0].sql, wantStaleRequeueSelect)
	}
	cutoff := staleNow.Add(-StaleRequeueWindow)
	if len(sels[0].args) != 2 || !sels[0].args[0].(time.Time).Equal(cutoff) || sels[0].args[1] != 7000 {
		t.Errorf("аргументы выборки = %v, want [порог %v, LIMIT 7000]", sels[0].args, cutoff)
	}

	upds := pool.updateSnapshot()
	if len(upds) != 1 {
		t.Fatalf("UPDATE-вызовов перевода = %d, want 1", len(upds))
	}
	if upds[0].sql != wantStaleRequeueUpdate {
		t.Errorf("SQL перевода:\n%s\nwant:\n%s", upds[0].sql, wantStaleRequeueUpdate)
	}
	wantUpdArgs := []any{
		int16(model.CheckStatePending), // SET check_state
		[]int16{1, 1, 2},               // the zones of the selected batch
		[]int64{1, 2, 3},               // the domains of the selected batch
		cutoff,                         // the threshold — the same as the selection's
	}
	if !reflect.DeepEqual(upds[0].args, wantUpdArgs) {
		t.Errorf("аргументы перевода = %v, want %v", upds[0].args, wantUpdArgs)
	}

	// Counting by RETURNING: 2 of the selected 3 moved (the guard filtered out one).
	if res.Domains != 2 || res.Selected != 3 {
		t.Errorf("итог = %+v, want Domains=2 (RETURNING), Selected=3", res)
	}

	// The plan pinning — only in the selection transaction, by three SET LOCALs
	// before the SELECT (T-191): not inherited by the move.
	if plans := pool.planSnapshot(); len(plans) != 3 ||
		plans[0].sql != staleRequeuePlanNoIndexScanSQL ||
		plans[1].sql != staleRequeuePlanNoBitmapScanSQL ||
		plans[2].sql != staleRequeuePlanNoIOSQL {
		t.Errorf("закрепление плана = %v, want SET LOCAL ×3 до SELECT", plans)
	}
}

// TestStaleRequeueOnceWindow — the 24 h threshold: the constant equals the gauge
// window (FR-2.2); the selection parameter is now() − 24 h from the pass's time source.
func TestStaleRequeueOnceWindow(t *testing.T) {
	if StaleRequeueWindow != 24*time.Hour {
		t.Fatalf("StaleRequeueWindow = %v, want 24 ч (окно gauge new_done_stale)", StaleRequeueWindow)
	}
	sel, upd := staleSeedBatch([][2]int64{{1, 1}}, 1)
	pool := &staleFakePool{guardVal: 1, selects: []*retryFakeRows{sel}, updates: []*retryFakeRows{upd}}
	if _, err := StaleRequeueOnce(context.Background(), pool, StaleRequeueOptions{
		Now: func() time.Time { return staleNow },
	}); err != nil {
		t.Fatalf("StaleRequeueOnce: %v", err)
	}
	sels := pool.selectSnapshot()
	if len(sels) != 1 {
		t.Fatalf("SELECT-вызовов = %d, want 1", len(sels))
	}
	if got := sels[0].args[0].(time.Time); !got.Equal(staleNow.Add(-24 * time.Hour)) {
		t.Errorf("порог выборки = %v, want %v (now − 24 ч)", got, staleNow.Add(-24*time.Hour))
	}
}

// TestStaleRequeueOnceGuardClosed — a cheap empty-set probe:
// the new_done_stale level is zero (steady state) — the pass performs ONE
// counter lookup and opens no transactions: domains is not scanned at all.
func TestStaleRequeueOnceGuardClosed(t *testing.T) {
	pool := &staleFakePool{guardVal: 0}

	res, err := StaleRequeueOnce(context.Background(), pool, StaleRequeueOptions{})
	if err != nil {
		t.Fatalf("StaleRequeueOnce: %v", err)
	}
	if res != (StaleRequeueResult{}) {
		t.Errorf("итог = %+v, want нулевой", res)
	}
	st := pool.stats()
	if st.begins != 0 || st.selects != 0 || st.commits != 0 {
		t.Fatalf("операции = %+v, want ноль транзакций — guard закрыт, domains не тронут", st)
	}
	if st.guards != 1 {
		t.Errorf("guard-чтений = %d, want 1 (одна дешёвая проба)", st.guards)
	}
}

// TestStaleRequeueOnceEmptyBatch — the guard is open, the selection is empty (level
// drift until reconciliation): the move transaction does not open; the outcome
// is zero — a repeated pass is a no-op (idempotency).
func TestStaleRequeueOnceEmptyBatch(t *testing.T) {
	pool := &staleFakePool{guardVal: 1} // selects/updates are empty

	res, err := StaleRequeueOnce(context.Background(), pool, StaleRequeueOptions{})
	if err != nil {
		t.Fatalf("StaleRequeueOnce: %v", err)
	}
	if res != (StaleRequeueResult{}) {
		t.Errorf("итог = %+v, want нулевой", res)
	}
	want := []string{"begin", "plan", "plan", "plan", "select", "commit"}
	if got := pool.opsSnapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("порядок операций = %v, want %v — пустая пачка без перевода", got, want)
	}
}

// TestStaleRequeueOnceIdempotent — a repeated pass after a moved
// batch: the moved rows stop matching the selection (the selection is empty) —
// a no-op, the counter does not grow.
func TestStaleRequeueOnceIdempotent(t *testing.T) {
	sel, upd := staleSeedBatch([][2]int64{{1, 1}, {1, 2}}, 2)
	reg := metrics.New("scheduler")
	pool := &staleFakePool{guardVal: 1, selects: []*retryFakeRows{sel}, updates: []*retryFakeRows{upd}}

	first, err := StaleRequeueOnce(context.Background(), pool, StaleRequeueOptions{Metrics: reg})
	if err != nil {
		t.Fatalf("StaleRequeueOnce: %v", err)
	}
	second, err := StaleRequeueOnce(context.Background(), pool, StaleRequeueOptions{Metrics: reg})
	if err != nil {
		t.Fatalf("повторный StaleRequeueOnce: %v", err)
	}
	if first.Domains != 2 || second.Domains != 0 {
		t.Fatalf("проходы = %+v, %+v; want Domains 2, 0 (идемпотентность)", first, second)
	}
	if got := staleRequeuedValue(t, reg); got != 2 {
		t.Errorf("счётчик после повторного прохода = %v, want 2 (пустой проход не считается)", got)
	}
}

// TestStaleRequeueOnceDefaults — zero options: the LIMIT is the default batch;
// the threshold — from the system clock (Now nil does not hinder the pass).
func TestStaleRequeueOnceDefaults(t *testing.T) {
	sel, upd := staleSeedBatch([][2]int64{{1, 1}}, 1)
	pool := &staleFakePool{guardVal: 1, selects: []*retryFakeRows{sel}, updates: []*retryFakeRows{upd}}
	if _, err := StaleRequeueOnce(context.Background(), pool, StaleRequeueOptions{}); err != nil {
		t.Fatalf("StaleRequeueOnce: %v", err)
	}
	sels := pool.selectSnapshot()
	if len(sels) != 1 || sels[0].args[1] != DefaultStaleRequeueBatch {
		t.Fatalf("LIMIT = %v, want дефолт %d", sels, DefaultStaleRequeueBatch)
	}
	if DefaultStaleRequeueBatch != 1000 {
		t.Errorf("DefaultStaleRequeueBatch = %d, want 1000 (задача T-270)", DefaultStaleRequeueBatch)
	}
	if DefaultStaleRequeueInterval != time.Hour {
		t.Errorf("DefaultStaleRequeueInterval = %v, want 1 ч (задача T-270)", DefaultStaleRequeueInterval)
	}
}

// TestStaleRequeueOnceError — pass step errors are wrapped with the
// compensator context; a guard protocol break (an empty set) and a nil pool
// — are errors.
func TestStaleRequeueOnceError(t *testing.T) {
	ctx := context.Background()

	guardPool := &staleFakePool{guardErrs: []error{errors.New("бд недоступна")}}
	if _, err := StaleRequeueOnce(ctx, guardPool, StaleRequeueOptions{}); err == nil ||
		!strings.Contains(err.Error(), "компенсатор «статус повис»") ||
		!strings.Contains(err.Error(), "бд недоступна") {
		t.Errorf("ошибка guard = %v, want контекст компенсатора и исходная ошибка", err)
	}

	emptyPool := &staleFakePool{guardEmpty: true}
	if _, err := StaleRequeueOnce(ctx, emptyPool, StaleRequeueOptions{}); err == nil ||
		!strings.Contains(err.Error(), "пустой ответ агрегата") {
		t.Errorf("ошибка guard-протокола = %v, want обрыв протокола", err)
	}

	planPool := &staleFakePool{guardVal: 1, planErr: errors.New("set local отклонён")}
	if _, err := StaleRequeueOnce(ctx, planPool, StaleRequeueOptions{}); err == nil ||
		!strings.Contains(err.Error(), "закрепление плана") {
		t.Errorf("ошибка закрепления = %v, want контекст закрепления", err)
	}

	selectPool := &staleFakePool{guardVal: 1, selectErrs: []error{context.Canceled}}
	if _, err := StaleRequeueOnce(ctx, selectPool, StaleRequeueOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка выборки = %v, want context.Canceled", err)
	}

	sel, _ := staleSeedBatch([][2]int64{{1, 1}}, 1)
	updatePool := &staleFakePool{guardVal: 1, selects: []*retryFakeRows{sel}, updateErrs: []error{errors.New("deadlock и не только")}}
	if _, err := StaleRequeueOnce(ctx, updatePool, StaleRequeueOptions{}); err == nil ||
		!strings.Contains(err.Error(), "перевод пачки") {
		t.Errorf("ошибка перевода = %v, want контекст перевода", err)
	}

	commitPool := &staleFakePool{guardVal: 1, commitErrs: []error{errors.New("connection refused")}}
	if _, err := StaleRequeueOnce(ctx, commitPool, StaleRequeueOptions{}); err == nil ||
		!strings.Contains(err.Error(), "фиксация выборки") {
		t.Errorf("ошибка Commit выборки = %v, want контекст фиксации", err)
	}

	beginPool := &staleFakePool{guardVal: 1, beginErr: errors.New("нет соединений")}
	if _, err := StaleRequeueOnce(ctx, beginPool, StaleRequeueOptions{}); err == nil ||
		!strings.Contains(err.Error(), "транзакция выборки") {
		t.Errorf("ошибка Begin = %v, want контекст транзакции", err)
	}

	if _, err := StaleRequeueOnce(ctx, nil, StaleRequeueOptions{}); err == nil ||
		!strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
	}
}

// TestStaleRequeueOnceDeadlockRetry — a 40P01 of the move transaction
// is retried in full (T-268): the guards are idempotent, what the first
// attempt moved is not moved again — a repeated Begin of the same statement.
func TestStaleRequeueOnceDeadlockRetry(t *testing.T) {
	sel, upd := staleSeedBatch([][2]int64{{1, 1}}, 1)
	pool := &staleFakePool{
		guardVal:   1,
		selects:    []*retryFakeRows{sel},
		updates:    []*retryFakeRows{upd},
		updateErrs: []error{&pgconn.PgError{Code: "40P01"}},
	}

	res, err := StaleRequeueOnce(context.Background(), pool, StaleRequeueOptions{})
	if err != nil {
		t.Fatalf("StaleRequeueOnce после 40P01: %v", err)
	}
	if res.Domains != 1 {
		t.Fatalf("итог = %+v, want Domains=1 — повтор перевёл пачку", res)
	}
	if got := len(pool.updateSnapshot()); got != 2 {
		t.Errorf("UPDATE-попыток = %d, want 2 (повтор после дедлока)", got)
	}
}

// TestStaleRequeueOnceMetrics — the crawler_scheduler_stale_requeued_total counter
// grows by the rows actually moved (by RETURNING, not by the selection);
// without a registry (nil Metrics) the pass works.
func TestStaleRequeueOnceMetrics(t *testing.T) {
	reg := metrics.New("scheduler")
	sel, upd := staleSeedBatch([][2]int64{{1, 1}, {1, 2}, {2, 3}}, 2) // 2 moved (the guard filtered out one)
	pool := &staleFakePool{guardVal: 1, selects: []*retryFakeRows{sel}, updates: []*retryFakeRows{upd}}

	if _, err := StaleRequeueOnce(context.Background(), pool, StaleRequeueOptions{Metrics: reg}); err != nil {
		t.Fatalf("StaleRequeueOnce: %v", err)
	}
	if got := staleRequeuedValue(t, reg); got != 2 {
		t.Errorf("счётчик = %v, want 2 (по RETURNING)", got)
	}

	// Without a registry (nil Metrics) and with a closed guard — a no-op; the pass runs.
	emptyPool := &staleFakePool{guardVal: 0}
	if _, err := StaleRequeueOnce(context.Background(), emptyPool, StaleRequeueOptions{}); err != nil {
		t.Fatalf("StaleRequeueOnce без реестра: %v", err)
	}
}

// staleRequeuedValue — the compensator counter's value in the registry (the style
// leaseNullReturnedValue).
func staleRequeuedValue(t *testing.T, reg *metrics.Registry) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "crawler_scheduler_stale_requeued_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			return m.GetCounter().GetValue()
		}
	}
	return 0
}

// --- the StaleRequeueLoop cycle ---------------------------------------------------

// TestStaleRequeueLoopDrain — the starting drain in batches until exhaustion:
// a full batch continues the drain, an incomplete one stops it; the counter grows
// by the outcome; without a tick there are no new selections; ctx cancellation — a clean nil.
func TestStaleRequeueLoopDrain(t *testing.T) {
	reg := metrics.New("scheduler")
	fullSel, fullUpd := staleSeedBatch([][2]int64{{1, 1}, {1, 2}}, 2)
	tailSel, tailUpd := staleSeedBatch([][2]int64{{2, 3}}, 1)
	pool := &staleFakePool{
		guardVal: 1,
		selects:  []*retryFakeRows{fullSel, tailSel}, // BatchSize=2: full, then incomplete
		updates:  []*retryFakeRows{fullUpd, tailUpd},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- StaleRequeueLoop(ctx, pool, StaleRequeueOptions{
			BatchSize: 2,
			Interval:  time.Hour, // the tick never comes: only the starting drain
			Metrics:   reg,
		})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		st := pool.stats()
		if st.selects >= 2 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("дренаж не дошёл до второй пачки: %+v", st)
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	if got := staleRequeuedValue(t, reg); got != 3 {
		t.Errorf("счётчик дренажа = %v, want 3", got)
	}
	// Both batches — by one starting drain: an incomplete batch stops it.
	if got := len(pool.selectSnapshot()); got != 2 {
		t.Errorf("SELECT-вызовов = %d, want 2 (неполная пачка останавливает)", got)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("StaleRequeueLoop: %v, want nil при отмене ctx", err)
	}
}

// TestStaleRequeueLoopError — a pass error interrupts only the current
// drain: the loop lives, the next tick retries (the OnRun contract); a Warn in
// the log.
func TestStaleRequeueLoopError(t *testing.T) {
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	sel, upd := staleSeedBatch([][2]int64{{1, 1}}, 1)
	pool := &staleFakePool{
		guardVal:  1,
		selects:   []*retryFakeRows{sel},
		updates:   []*retryFakeRows{upd},
		guardErrs: []error{errors.New("бд недоступна")}, // the first pass fails
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- StaleRequeueLoop(ctx, pool, StaleRequeueOptions{
			Interval: 20 * time.Millisecond,
			Log:      logger,
		})
	}()

	// The first drain fails; the tick retries and finishes the batch.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if st := pool.stats(); st.commits >= 2 { // the selection and the move are pushed through to commits
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("очередной тик не повторил дренаж: %+v", pool.stats())
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("StaleRequeueLoop: %v, want nil при отмене ctx", err)
	}

	log := buf.String()
	for _, want := range []string{
		"level=WARN",
		"бд недоступна",
	} {
		if !strings.Contains(log, want) {
			t.Errorf("лог не содержит %q:\n%s", want, log)
		}
	}
}

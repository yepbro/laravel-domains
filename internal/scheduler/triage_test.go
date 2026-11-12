// Tests of triaging the surviving mutants of the CI run of Nov 10, 2026 (part B,
// score 0.857): scheduler coverage top-ups by triage groups —
// option defaults (single values and literal pins of constants),
// step errors of the compensator and retry passes (swallowing returns:
// Scan/rows.Err of reads, per-statement plan pinning, transaction
// Begin/Commit, nil pools of loops), transaction rollbacks (the defer Rollback must
// be called even after Commit — a no-op), drain loop boundaries
// (the failed-batch counter reset by a success; a stop by a full batch with
// a zero move; the drain continuing after a fully moved
// batch), nil metric sinks and the backlog pass histogram buckets.
// Counters on errors, logging and equivalent mutants —
// the blacklist (docs/mutation-testing.md §4).
package scheduler

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/metrics"
)

// --- triage fake pool wrappers -----------------------------------------
//
// The packages' base fakes cannot: fail an individual plan pinning
// operation (planErr is static across all three SET LOCALs), return rows with
// a Scan/Err error (stubs are assembled from typed batches) or
// count rollbacks. The wrappers delegate everything that is not
// substituted to the base fake: the call protocol and statistics stay honest.

// triStalePool — a staleFakePool with substitutions for stalerequeue.go.
type triStalePool struct {
	*staleFakePool
	failNthPlan  int // 0 — no substitution; N — the error of the batch's Nth SET LOCAL
	beginFailNth int // 0 — no substitution; N — the error of the pass's Nth Begin
	guardRows    pgx.Rows
	selectRows   pgx.Rows
	updateRows   pgx.Rows

	mu        sync.Mutex
	begins    int
	rollbacks int
}

func (p *triStalePool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if sql == staleRequeueGuardSQL && p.guardRows != nil {
		return p.guardRows, nil
	}
	return p.staleFakePool.Query(ctx, sql, args...)
}

func (p *triStalePool) Begin(ctx context.Context) (pgx.Tx, error) {
	p.mu.Lock()
	p.begins++
	n := p.begins
	p.mu.Unlock()
	if p.beginFailNth != 0 && n == p.beginFailNth {
		return nil, errors.New("нет соединений")
	}
	tx, err := p.staleFakePool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &triStaleTx{staleFakeTx: tx.(*staleFakeTx), pool: p}, nil
}

func (p *triStalePool) rollbackCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rollbacks
}

// triStaleTx — the wrapper's transaction: a point SET LOCAL failure,
// a substitution of selection/move rows, rollback counting.
type triStaleTx struct {
	*staleFakeTx
	pool *triStalePool
	n    int
}

func (t *triStaleTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.n++
	if t.pool.failNthPlan != 0 && t.n == t.pool.failNthPlan {
		return pgconn.CommandTag{}, errors.New("set local отклонён")
	}
	return t.staleFakeTx.Exec(ctx, sql, args...)
}

func (t *triStaleTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	switch sql {
	case staleRequeueSelectSQL:
		if t.pool.selectRows != nil {
			return t.pool.selectRows, nil
		}
	case staleRequeueUpdateSQL:
		if t.pool.updateRows != nil {
			return t.pool.updateRows, nil
		}
	}
	return t.staleFakeTx.Query(ctx, sql, args...)
}

func (t *triStaleTx) Rollback(ctx context.Context) error {
	t.pool.mu.Lock()
	t.pool.rollbacks++
	t.pool.mu.Unlock()
	return t.staleFakeTx.Rollback(ctx)
}

// triRetryPool — a retryFakePool with substitutions for retry.go.
type triRetryPool struct {
	*retryFakePool
	failNthPlan int
	guardRows   pgx.Rows
}

func (p *triRetryPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if sql == retryMatureSQL && p.guardRows != nil {
		return p.guardRows, nil
	}
	return p.retryFakePool.Query(ctx, sql, args...)
}

func (p *triRetryPool) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := p.retryFakePool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if ft, ok := tx.(*retryFakeTx); ok {
		return &triRetryTx{retryFakeTx: ft, pool: p}, nil
	}
	return tx, nil
}

// triRetryTx — a point failure of the selection transaction's Nth SET LOCAL.
type triRetryTx struct {
	*retryFakeTx
	pool *triRetryPool
	n    int
}

func (t *triRetryTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.n++
	if t.pool.failNthPlan != 0 && t.n == t.pool.failNthPlan {
		return pgconn.CommandTag{}, errors.New("set local отклонён")
	}
	return t.retryFakeTx.Exec(ctx, sql, args...)
}

// triLeasePool — a leaseFakePool with row substitutions and rollback counting
// (lease.go/leasenull.go).
type triLeasePool struct {
	*leaseFakePool
	selectRows     pgx.Rows
	updateRows     pgx.Rows
	nullSelectRows pgx.Rows
	nullUpdateRows pgx.Rows

	mu        sync.Mutex
	rollbacks int
}

func (p *triLeasePool) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := p.leaseFakePool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &triLeaseTx{Tx: tx, pool: p}, nil
}

func (p *triLeasePool) rollbackCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rollbacks
}

// triLeaseTx — a substitution of the statement's response and rollback counting; the rest —
// to the base fake (the protocol and the batch key validations).
type triLeaseTx struct {
	pgx.Tx
	pool *triLeasePool
}

func (t *triLeaseTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	switch sql {
	case leaseSelectSQL:
		if t.pool.selectRows != nil {
			return t.pool.selectRows, nil
		}
	case leaseUpdateSQL:
		if t.pool.updateRows != nil {
			return t.pool.updateRows, nil
		}
	case leaseNullSelectSQL:
		if t.pool.nullSelectRows != nil {
			return t.pool.nullSelectRows, nil
		}
	case leaseNullUpdateSQL:
		if t.pool.nullUpdateRows != nil {
			return t.pool.nullUpdateRows, nil
		}
	}
	return t.Tx.Query(ctx, sql, args...)
}

func (t *triLeaseTx) Rollback(ctx context.Context) error {
	t.pool.mu.Lock()
	t.pool.rollbacks++
	t.pool.mu.Unlock()
	return t.Tx.Rollback(ctx)
}

// triRecheckPool — a recheckFakePool with a substitution of the UPDATE response rows.
type triRecheckPool struct {
	*recheckFakePool
	rows pgx.Rows
}

func (p *triRecheckPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if p.rows != nil {
		return p.rows, nil
	}
	return p.recheckFakePool.Query(ctx, sql, args...)
}

// --- stalerequeue.go -------------------------------------------------------

// TestStaleRequeueTriageDefaults — the compensator's option defaults: zero
// BatchSize/Interval — DefaultStaleRequeueBatch/Interval; single
// (1) values are legitimate explicit ones and are not replaced by defaults (the "≤ 1"
// mutant would replace them).
func TestStaleRequeueTriageDefaults(t *testing.T) {
	o := StaleRequeueOptions{}.withDefaults()
	if o.BatchSize != DefaultStaleRequeueBatch {
		t.Errorf("BatchSize по умолчанию = %d, хочу %d", o.BatchSize, DefaultStaleRequeueBatch)
	}
	if o.Interval != DefaultStaleRequeueInterval {
		t.Errorf("Interval по умолчанию = %s, хочу %s", o.Interval, DefaultStaleRequeueInterval)
	}
	o = StaleRequeueOptions{BatchSize: 1, Interval: time.Nanosecond}.withDefaults()
	if o.BatchSize != 1 || o.Interval != time.Nanosecond {
		t.Errorf("единичные значения заменены дефолтами: %+v (дефолт — только для ≤ 0)", o)
	}
}

// TestStaleRequeueTriageErrors — pass step errors must
// be returned: the guard reads (Scan, the trailing Err), plan pinning
// (each of the three SET LOCALs separately), the selection and move reads
// (Scan/Err), the move transaction's Begin, the move's Commit, the loop's
// nil pool. Swallowing any return would silently carry the pass to the end.
func TestStaleRequeueTriageErrors(t *testing.T) {
	ctx := context.Background()

	// Guard: the response width does not match the receivers — a read error.
	p := &triStalePool{staleFakePool: &staleFakePool{},
		guardRows: retryRows([]any{int64(1), int64(2)})}
	if _, err := StaleRequeueOnce(ctx, p, StaleRequeueOptions{}); err == nil {
		t.Error("ошибка Scan guard проглочена")
	}

	// Guard: the row is read, but the reading broke off after it.
	p = &triStalePool{staleFakePool: &staleFakePool{},
		guardRows: &retryFakeRows{rows: [][]any{{int64(5)}}, err: errors.New("обрыв соединения")}}
	if _, err := StaleRequeueOnce(ctx, p, StaleRequeueOptions{}); err == nil {
		t.Error("хвостовая ошибка rows.Err() guard проглочена (уровень 5 потерян)")
	}

	// Plan pinning: each SET LOCAL failing individually — an error
	// (the fake's static planErr is caught by neighboring returns — here
	// exactly one operation of the series fails).
	for n := 1; n <= 3; n++ {
		p := &triStalePool{staleFakePool: &staleFakePool{guardVal: 1}, failNthPlan: n}
		if _, err := StaleRequeueOnce(ctx, p, StaleRequeueOptions{}); err == nil {
			t.Errorf("сбой SET LOCAL #%d проглочен", n)
		}
	}

	// The batch selection: Scan and the trailing Err.
	p = &triStalePool{staleFakePool: &staleFakePool{guardVal: 1},
		selectRows: retryRows([]any{int16(1)})} // fewer columns than receivers
	if _, err := StaleRequeueOnce(ctx, p, StaleRequeueOptions{}); err == nil {
		t.Error("ошибка Scan выборки проглочена")
	}
	p = &triStalePool{staleFakePool: &staleFakePool{guardVal: 1},
		selectRows: &retryFakeRows{rows: [][]any{{int16(1), int64(1)}}, err: errors.New("обрыв")}}
	if _, err := StaleRequeueOnce(ctx, p, StaleRequeueOptions{}); err == nil {
		t.Error("хвостовая ошибка rows.Err() выборки проглочена")
	}

	// The move: Scan and the trailing Err (the RETURNING rows).
	p = &triStalePool{staleFakePool: &staleFakePool{guardVal: 1,
		selects: []*retryFakeRows{retryRows([]any{int16(1), int64(1)})}},
		updateRows: retryRows([]any{int16(1), int64(2)})}
	if _, err := StaleRequeueOnce(ctx, p, StaleRequeueOptions{}); err == nil {
		t.Error("ошибка Scan перевода проглочена")
	}
	p = &triStalePool{staleFakePool: &staleFakePool{guardVal: 1,
		selects: []*retryFakeRows{retryRows([]any{int16(1), int64(1)})}},
		updateRows: &retryFakeRows{rows: [][]any{{int16(1)}}, err: errors.New("обрыв")}}
	if _, err := StaleRequeueOnce(ctx, p, StaleRequeueOptions{}); err == nil {
		t.Error("хвостовая ошибка rows.Err() перевода проглочена")
	}

	// The move transaction's Begin (the pass's second Begin).
	sel, upd := staleSeedBatch([][2]int64{{1, 1}}, 1)
	p = &triStalePool{staleFakePool: &staleFakePool{guardVal: 1,
		selects: []*retryFakeRows{sel}, updates: []*retryFakeRows{upd}},
		beginFailNth: 2}
	if _, err := StaleRequeueOnce(ctx, p, StaleRequeueOptions{}); err == nil {
		t.Error("сбой Begin транзакции перевода проглочен")
	}

	// The move's Commit (the pass's second Commit).
	sel, upd = staleSeedBatch([][2]int64{{1, 1}}, 1)
	p = &triStalePool{staleFakePool: &staleFakePool{guardVal: 1,
		selects: []*retryFakeRows{sel}, updates: []*retryFakeRows{upd},
		commitErrs: []error{nil, errors.New("connection refused")}}}
	if _, err := StaleRequeueOnce(ctx, p, StaleRequeueOptions{}); err == nil {
		t.Error("сбой Commit перевода проглочен")
	}

	// The loop's nil pool — an immediate error (not a silent hang).
	loopErr := make(chan error, 1)
	go func() { loopErr <- StaleRequeueLoop(ctx, nil, StaleRequeueOptions{}) }()
	select {
	case err := <-loopErr:
		if err == nil {
			t.Error("StaleRequeueLoop с nil-пулом не вернул ошибку")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("StaleRequeueLoop с nil-пулом не завершился ошибкой")
	}
}

// TestStaleRequeueTriageRollback — both pass transactions must
// close with a rollback (after Commit — a no-op, but the call happens): a leak
// of an unclosed transaction holds a pool connection.
func TestStaleRequeueTriageRollback(t *testing.T) {
	sel, upd := staleSeedBatch([][2]int64{{1, 1}}, 1)
	p := &triStalePool{staleFakePool: &staleFakePool{guardVal: 1,
		selects: []*retryFakeRows{sel}, updates: []*retryFakeRows{upd}}}
	if _, err := StaleRequeueOnce(context.Background(), p, StaleRequeueOptions{}); err != nil {
		t.Fatalf("StaleRequeueOnce: %v", err)
	}
	if n := p.rollbackCount(); n != 2 {
		t.Errorf("откатов %d, хочу 2 (выборка и перевод — каждый закрывается)", n)
	}
}

// staleDrainQuiet — starting StaleRequeueLoop and waiting for the drain
// to settle (the statistics progress has stopped): the Interval tick is an hour — after
// the starting drain the loop waits; all passes are in memory — deterministic.
func staleDrainQuiet(t *testing.T, p *triStalePool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- StaleRequeueLoop(ctx, p, StaleRequeueOptions{BatchSize: 1, Interval: time.Hour})
	}()
	deadline := time.Now().Add(2 * time.Second)
	last := staleStats{}
	for quiet := 0; quiet < 20; {
		st := p.stats()
		if st == last {
			quiet++
		} else {
			quiet = 0
			last = st
		}
		if time.Now().After(deadline) {
			t.Fatalf("дренаж не успокоился: %+v", st)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("StaleRequeueLoop: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("StaleRequeueLoop не завершился после отмены ctx")
	}
}

// TestStaleRequeueTriageDrain — the drain stopping: a full batch with
// a ZERO move (the guard/guards filtered out everything) — the set is exhausted,
// no second pass; a full batch with a full move — there are more
// beyond it; the drain continues with the next batch.
func TestStaleRequeueTriageDrain(t *testing.T) {
	// A full batch (BatchSize 1), an empty move: one pass, one SELECT.
	selZero, _ := staleSeedBatch([][2]int64{{1, 1}}, 0)
	p := &triStalePool{staleFakePool: &staleFakePool{guardVal: 1,
		selects: []*retryFakeRows{selZero}}}
	staleDrainQuiet(t, p)
	if st := p.stats(); st.selects != 1 || st.guards != 1 {
		t.Errorf("нулевая пачка: проходов (select=%d, guard=%d), хочу 1/1 — множество исчерпано", st.selects, st.guards)
	}

	// A full batch with a move: the drain continues — the second (empty)
	// selection closes the drain.
	selFull, updFull := staleSeedBatch([][2]int64{{1, 1}}, 1)
	p = &triStalePool{staleFakePool: &staleFakePool{guardVal: 1,
		selects: []*retryFakeRows{selFull}, updates: []*retryFakeRows{updFull}}}
	staleDrainQuiet(t, p)
	if st := p.stats(); st.selects != 2 || st.guards != 2 {
		t.Errorf("полная переведённая пачка: проходов (select=%d, guard=%d), хочу 2/2 — дренаж продолжает", st.selects, st.guards)
	}
}

// --- retry.go --------------------------------------------------------------

// TestRetryTriageDefaults — a literal pin of the failed-batch limit
// (T-268: three in a row — a persistent failure; no DB hammering until the tick).
func TestRetryTriageDefaults(t *testing.T) {
	if DefaultRetryDrainFails != 3 {
		t.Errorf("DefaultRetryDrainFails = %d, хочу ровно 3 (T-268)", DefaultRetryDrainFails)
	}
}

// TestRetryTriageErrors — step errors: plan pinning (each SET
// LOCAL separately), the bucket guard reads (Scan, the trailing Err).
func TestRetryTriageErrors(t *testing.T) {
	ctx := context.Background()

	for n := 1; n <= 3; n++ {
		p := &triRetryPool{retryFakePool: &retryFakePool{}, failNthPlan: n}
		if _, err := RetryOnce(ctx, p, RetryOptions{}); err == nil {
			t.Errorf("сбой SET LOCAL #%d проглочен", n)
		}
	}

	p := &triRetryPool{retryFakePool: &retryFakePool{},
		guardRows: retryRows([]any{int64(1), int64(2)})}
	if _, err := RetryOnce(ctx, p, RetryOptions{}); err == nil {
		t.Error("ошибка Scan guard корзин проглочена")
	}
	p = &triRetryPool{retryFakePool: &retryFakePool{},
		guardRows: &retryFakeRows{rows: [][]any{{int64(1)}}, err: errors.New("обрыв")}}
	if _, err := RetryOnce(ctx, p, RetryOptions{}); err == nil {
		t.Error("хвостовая ошибка rows.Err() guard проглочена")
	}
}

// retryDrainRun — starting RetryTempErrors until the drain settles.
func retryDrainRun(t *testing.T, p *retryFakePool, o RetryOptions) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- RetryTempErrors(ctx, p, o) }()
	deadline := time.Now().Add(2 * time.Second)
	last := retryStats{}
	for quiet := 0; quiet < 20; {
		st := p.stats()
		if st == last {
			quiet++
		} else {
			quiet = 0
			last = st
		}
		if time.Now().After(deadline) {
			t.Fatalf("дренаж не успокоился: %+v", st)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RetryTempErrors: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RetryTempErrors не завершился после отмены ctx")
	}
}

// TestRetryTriageDrainFailCounter — the failed-batch counter is reset
// by EVERY success: after a successful batch the limit is exactly three failures
// in a row (a reset to 0, not ±1 — otherwise the limit fires earlier/later).
func TestRetryTriageDrainFailCounter(t *testing.T) {
	dbDown := errors.New("бд недоступна")
	full := retryRows([]any{int16(1), int64(10), int16(0)})
	p := &retryFakePool{
		// A success, then a persistent guard failure.
		guardErrs: []error{nil, dbDown, dbDown, dbDown, dbDown, dbDown},
		selects:   []*retryFakeRows{full},
		requeues:  []*retryFakeRows{retryRows([]any{int16(1), int64(10), retryDay})},
	}
	retryDrainRun(t, p, RetryOptions{BatchSize: 1, Interval: time.Hour})
	if got := len(p.guardQueries); got != 4 {
		t.Errorf("guard-чтений = %d, хочу 4 (успех + ровно 3 падения подряд до предела)", got)
	}
}

// TestRetryTriageDrainConsecutiveReset — a failure/success alternation: the counter
// is exactly of CONSECUTIVE failures (a success resets it) — the drain finishes
// to the end (the outcome requeued=3, without the limit).
func TestRetryTriageDrainConsecutiveReset(t *testing.T) {
	dbDown := errors.New("бд недоступна")
	cand := func() *retryFakeRows { return retryRows([]any{int16(1), int64(10), int16(0)}) }
	ret := func() *retryFakeRows { return retryRows([]any{int16(1), int64(10), retryDay}) }
	p := &retryFakePool{
		// a failure, a success, a failure, a success, a failure, a success, then empty — the outcome.
		guardErrs: []error{dbDown, nil, dbDown, nil, dbDown, nil},
		selects:   []*retryFakeRows{cand(), cand(), cand()},
		requeues:  []*retryFakeRows{ret(), ret(), ret()},
	}
	retryDrainRun(t, p, RetryOptions{BatchSize: 1, Interval: time.Hour})
	if got := len(p.guardQueries); got != 7 {
		t.Errorf("guard-чтений = %d, хочу 7 (дренаж доработал до пустой пачки)", got)
	}
}

// --- lease.go / leasenull.go ----------------------------------------------

// TestLeaseTriageErrors — the main pass's read errors: the Scan and
// trailing Err of the batch selection and the move — pass errors.
func TestLeaseTriageErrors(t *testing.T) {
	ctx := context.Background()
	now, clock := leaseFixedClock()
	base := time.Date(2026, 10, 17, 4, 0, 0, 0, time.UTC)
	opts := LeaseOptions{Timeout: 15 * time.Minute, BatchSize: 8, Now: clock}
	good := func() pgx.Rows { return retryRows([]any{int16(1), int64(1), base}) }

	p := &triLeasePool{leaseFakePool: &leaseFakePool{}, selectRows: retryRows([]any{int16(1)})}
	if _, err := LeaseOnce(ctx, p, &LeaseCursor{}, opts); err == nil {
		t.Error("ошибка Scan выборки lease проглочена")
	}
	p = &triLeasePool{leaseFakePool: &leaseFakePool{},
		selectRows: &retryFakeRows{rows: [][]any{{int16(1), int64(1), base}}, err: errors.New("обрыв")}}
	if _, err := LeaseOnce(ctx, p, &LeaseCursor{}, opts); err == nil {
		t.Error("хвостовая ошибка rows.Err() выборки lease проглочена")
	}
	p = &triLeasePool{leaseFakePool: &leaseFakePool{}, selectRows: good(),
		updateRows: retryRows([]any{int16(1), int64(2)})}
	if _, err := LeaseOnce(ctx, p, &LeaseCursor{}, opts); err == nil {
		t.Error("ошибка Scan перевода lease проглочена")
	}
	p = &triLeasePool{leaseFakePool: &leaseFakePool{}, selectRows: good(),
		updateRows: &retryFakeRows{rows: [][]any{{int16(1)}}, err: errors.New("обрыв")}}
	if _, err := LeaseOnce(ctx, p, &LeaseCursor{}, opts); err == nil {
		t.Error("хвостовая ошибка rows.Err() перевода lease проглочена")
	}
	_ = now
}

// TestLeaseTriageRollback — the pass transaction closes with a rollback.
func TestLeaseTriageRollback(t *testing.T) {
	_, clock := leaseFixedClock()
	base := time.Date(2026, 10, 17, 4, 0, 0, 0, time.UTC)
	p := &triLeasePool{leaseFakePool: &leaseFakePool{
		batches: []leaseBatch{leaseSeedBatch(2, 2, base)}}}
	if _, err := LeaseOnce(context.Background(), p, &LeaseCursor{},
		LeaseOptions{Timeout: 15 * time.Minute, BatchSize: 8, Now: clock}); err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}
	if n := p.rollbackCount(); n != 1 {
		t.Errorf("откатов %d, хочу 1 (транзакция прохода закрывается)", n)
	}
}

// TestLeaseNullTriageErrors — the guard repair pass's read errors.
func TestLeaseNullTriageErrors(t *testing.T) {
	ctx := context.Background()
	good := func() pgx.Rows { return retryRows([]any{int16(1), int64(1)}) }

	p := &triLeasePool{leaseFakePool: &leaseFakePool{}, nullSelectRows: retryRows([]any{int16(1)})}
	if _, err := LeaseNullOnce(ctx, p, LeaseOptions{}); err == nil {
		t.Error("ошибка Scan выборки страховки проглочена")
	}
	p = &triLeasePool{leaseFakePool: &leaseFakePool{},
		nullSelectRows: &retryFakeRows{rows: [][]any{{int16(1), int64(1)}}, err: errors.New("обрыв")}}
	if _, err := LeaseNullOnce(ctx, p, LeaseOptions{}); err == nil {
		t.Error("хвостовая ошибка rows.Err() выборки страховки проглочена")
	}
	p = &triLeasePool{leaseFakePool: &leaseFakePool{}, nullSelectRows: good(),
		nullUpdateRows: retryRows([]any{int16(1), int64(2)})}
	if _, err := LeaseNullOnce(ctx, p, LeaseOptions{}); err == nil {
		t.Error("ошибка Scan перевода страховки проглочена")
	}
	p = &triLeasePool{leaseFakePool: &leaseFakePool{}, nullSelectRows: good(),
		nullUpdateRows: &retryFakeRows{rows: [][]any{{int16(1)}}, err: errors.New("обрыв")}}
	if _, err := LeaseNullOnce(ctx, p, LeaseOptions{}); err == nil {
		t.Error("хвостовая ошибка rows.Err() перевода страховки проглочена")
	}
}

// TestLeaseNullTriageRollback — the guard transaction closes with a rollback.
func TestLeaseNullTriageRollback(t *testing.T) {
	p := &triLeasePool{leaseFakePool: &leaseFakePool{
		nullBatches: []leaseNullBatch{leaseNullSeedBatch(2, 2)}}}
	if _, err := LeaseNullOnce(context.Background(), p, LeaseOptions{}); err != nil {
		t.Fatalf("LeaseNullOnce: %v", err)
	}
	if n := p.rollbackCount(); n != 1 {
		t.Errorf("откатов %d, хочу 1", n)
	}
}

// TestLeaseTriageSelectSwallow — swallowing the selection error returns
// (Scan/rows.Err) must crash the pass BEFORE the move: the move response
// is substituted with success here, so a swallowed error would carry
// the mutant to the commit and nil (the cases above caught it only by a repeated
// batch validation error in the move fake).
func TestLeaseTriageSelectSwallow(t *testing.T) {
	ctx := context.Background()
	_, clock := leaseFixedClock()
	opts := LeaseOptions{Timeout: 15 * time.Minute, BatchSize: 8, Now: clock}

	// A Scan error (one column for three receivers): the "batch selection" error.
	p := &triLeasePool{leaseFakePool: &leaseFakePool{},
		selectRows: retryRows([]any{int16(1)}), updateRows: retryRows()}
	if _, err := LeaseOnce(ctx, p, &LeaseCursor{}, opts); err == nil ||
		!strings.Contains(err.Error(), "выборка пачки") {
		t.Errorf("глушение Scan выборки: %v, want ошибку выборки пачки", err)
	}

	// A trailing rows.Err: the same step error, not a success with a batch.
	p = &triLeasePool{leaseFakePool: &leaseFakePool{},
		selectRows: &retryFakeRows{
			rows: [][]any{{int16(1), int64(1), time.Date(2026, 10, 17, 4, 0, 0, 0, time.UTC)}},
			err:  errors.New("обрыв"),
		},
		updateRows: retryRows()}
	if _, err := LeaseOnce(ctx, p, &LeaseCursor{}, opts); err == nil ||
		!strings.Contains(err.Error(), "выборка пачки") {
		t.Errorf("глушение rows.Err() выборки: %v, want ошибку выборки пачки", err)
	}
}

// TestLeaseNullTriageSelectSwallow — the same for the guard repair
// pass's selection: the move is substituted with success — swallowing the
// Scan/rows.Err errors would carry the mutant to the commit and nil.
func TestLeaseNullTriageSelectSwallow(t *testing.T) {
	ctx := context.Background()

	p := &triLeasePool{leaseFakePool: &leaseFakePool{},
		nullSelectRows: retryRows([]any{int16(1)}), nullUpdateRows: retryRows()}
	if _, err := LeaseNullOnce(ctx, p, LeaseOptions{}); err == nil ||
		!strings.Contains(err.Error(), "выборка пачки") {
		t.Errorf("глушение Scan выборки страховки: %v, want ошибку выборки пачки", err)
	}

	p = &triLeasePool{leaseFakePool: &leaseFakePool{},
		nullSelectRows: &retryFakeRows{
			rows: [][]any{{int16(1), int64(1)}},
			err:  errors.New("обрыв"),
		},
		nullUpdateRows: retryRows()}
	if _, err := LeaseNullOnce(ctx, p, LeaseOptions{}); err == nil ||
		!strings.Contains(err.Error(), "выборка пачки") {
		t.Errorf("глушение rows.Err() выборки страховки: %v, want ошибку выборки пачки", err)
	}
}

// --- recheck.go ------------------------------------------------------------

// TestRecheckTriageRowsErrors — errors of reading the recheck's UPDATE response:
// the Scan and the trailing Err — pass errors (swallowing would silently lose
// the moved count).
func TestRecheckTriageRowsErrors(t *testing.T) {
	p := &triRecheckPool{recheckFakePool: &recheckFakePool{},
		rows: retryRows([]any{int16(1), int64(2)})} // more columns than the receiver
	if _, err := RecheckOnce(context.Background(), p, RecheckOptions{}); err == nil {
		t.Error("ошибка Scan перепроверки проглочена")
	}
	p = &triRecheckPool{recheckFakePool: &recheckFakePool{},
		rows: &retryFakeRows{rows: [][]any{{int16(1)}}, err: errors.New("обрыв")}}
	if _, err := RecheckOnce(context.Background(), p, RecheckOptions{}); err == nil {
		t.Error("хвостовая ошибка rows.Err() перепроверки проглочена")
	}
}

// --- backlog.go ------------------------------------------------------------

// TestBacklogTriageNilMetrics — the loop's nil metric sinks — a no-op:
// the constructor without a registry and observations on nil do not panic (the
// "nil — no-op" contract; isolated assemblies without Prometheus).
func TestBacklogTriageNilMetrics(t *testing.T) {
	if m := newBacklogLoopMetrics(nil); m != nil {
		t.Errorf("newBacklogLoopMetrics(nil) = %v, хочу nil", m)
	}
	var m *backlogLoopMetrics
	m.observe(1.5) // does not panic
	m.skip()
}

// TestBacklogTriageHistogramBuckets — the pass duration buckets are
// exponential 0.5s…512s (11 of them, start 0.5, factor 2): the
// "pass longer than interval" observability contract is visible in the gathered metric.
func TestBacklogTriageHistogramBuckets(t *testing.T) {
	reg := metrics.New("scheduler-triage")
	m := newBacklogLoopMetrics(reg)
	if m == nil {
		t.Fatal("newBacklogLoopMetrics(reg) = nil")
	}
	m.observe(0.7)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "crawler_backlog_pass_seconds" {
			continue
		}
		var finite []float64
		for _, b := range mf.GetMetric()[0].GetHistogram().GetBucket() {
			if !math.IsInf(b.GetUpperBound(), 1) {
				finite = append(finite, b.GetUpperBound())
			}
		}
		want := []float64{0.5, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512}
		if !reflect.DeepEqual(finite, want) {
			t.Fatalf("конечные границы = %v, хочу %v (0.5…512, фактор 2, 11 корзин)", finite, want)
		}
		if c := mf.GetMetric()[0].GetHistogram().GetSampleCount(); c != 1 {
			t.Errorf("наблюдение 0.7с не учтено: count=%d", c)
		}
		return
	}
	t.Fatal("метрика crawler_backlog_pass_seconds не найдена в реестре")
}

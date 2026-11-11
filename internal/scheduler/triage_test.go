// Тесты триажа выживших мутантов CI-прогона 10.11.2026 (часть B,
// score 0.857): добивки покрытия scheduler по группам разбора —
// дефолты опций (единичные значения и литеральные пины констант),
// ошибки шагов проходов компенсаторов и ретрая (глушение возвратов:
// Scan/rows.Err чтений, закрепление плана по-операторно, Begin/Commit
// транзакций, nil-пул циклов), откат транзакций (defer Rollback обязан
// вызываться даже после Commit — no-op), границы дренажных циклов
// (обнуление счётчика упавших пачек успехом, останов полной пачкой с
// нулевым переводом, продолжение дренажа после полностью переведённой
// пачки), nil-приёмники метрик и корзины гистограммы прохода backlog.
// Счётчики при ошибках, журналирование и эквивалентные мутанты —
// блэклист (docs/mutation-testing.md §4).
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

// --- обёртки фейковых пулов триажа -----------------------------------------
//
// Базовые фейки пакетов не умеют: падать отдельной операцией закрепления
// плана (planErr статичен на все три SET LOCAL), возвращать строки с
// ошибкой Scan/Err (заготовки собираются из типизированных пачек) и
// считать откаты. Обёртки делегируют базовому фейку всё, что не
// подменяется: протокол вызовов и статистика остаются честными.

// triStalePool — staleFakePool с подменами для stalerequeue.go.
type triStalePool struct {
	*staleFakePool
	failNthPlan  int // 0 — без подмены; N — ошибка N-го SET LOCAL пачки
	beginFailNth int // 0 — без подмены; N — ошибка N-го Begin прохода
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

// triStaleTx — транзакция обёртки: точечный сбой SET LOCAL, подмена
// строк выборки/перевода, подсчёт откатов.
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

// triRetryPool — retryFakePool с подменами для retry.go.
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

// triRetryTx — точечный сбой N-го SET LOCAL транзакции выборки.
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

// triLeasePool — leaseFakePool с подменами строк и подсчётом откатов
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

// triLeaseTx — подмена ответа оператора и подсчёт откатов; остальное —
// базовому фейку (протокол и валидации ключей пачки).
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

// triRecheckPool — recheckFakePool с подменой строк ответа UPDATE.
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

// TestStaleRequeueTriageDefaults — дефолты опций компенсатора: нулевые
// BatchSize/Interval — DefaultStaleRequeueBatch/Interval; единичные
// (1) значения — законные явные, дефолтом не заменяются (мутант «≤ 1»
// подменял бы их).
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

// TestStaleRequeueTriageErrors — ошибки шагов прохода обязаны
// возвращаться: чтения guard (Scan, хвостовой Err), закрепление плана
// (каждое из трёх SET LOCAL отдельно), чтения выборки и перевода
// (Scan/Err), Begin транзакции перевода, Commit перевода, nil-пул
// цикла. Глушение любого возврата молча доводило бы проход до конца.
func TestStaleRequeueTriageErrors(t *testing.T) {
	ctx := context.Background()

	// Guard: ширина ответа не совпадает с приёмником — ошибка чтения.
	p := &triStalePool{staleFakePool: &staleFakePool{},
		guardRows: retryRows([]any{int64(1), int64(2)})}
	if _, err := StaleRequeueOnce(ctx, p, StaleRequeueOptions{}); err == nil {
		t.Error("ошибка Scan guard проглочена")
	}

	// Guard: строка прочитана, но чтение оборвалось после неё.
	p = &triStalePool{staleFakePool: &staleFakePool{},
		guardRows: &retryFakeRows{rows: [][]any{{int64(5)}}, err: errors.New("обрыв соединения")}}
	if _, err := StaleRequeueOnce(ctx, p, StaleRequeueOptions{}); err == nil {
		t.Error("хвостовая ошибка rows.Err() guard проглочена (уровень 5 потерян)")
	}

	// Закрепление плана: сбой каждого SET LOCAL по отдельности — ошибка
	// (статичный planErr фейка ловится соседними возвратами — здесь
	// падает ровно одна операция серии).
	for n := 1; n <= 3; n++ {
		p := &triStalePool{staleFakePool: &staleFakePool{guardVal: 1}, failNthPlan: n}
		if _, err := StaleRequeueOnce(ctx, p, StaleRequeueOptions{}); err == nil {
			t.Errorf("сбой SET LOCAL #%d проглочен", n)
		}
	}

	// Выборка пачки: Scan и хвостовой Err.
	p = &triStalePool{staleFakePool: &staleFakePool{guardVal: 1},
		selectRows: retryRows([]any{int16(1)})} // колонок меньше приёмников
	if _, err := StaleRequeueOnce(ctx, p, StaleRequeueOptions{}); err == nil {
		t.Error("ошибка Scan выборки проглочена")
	}
	p = &triStalePool{staleFakePool: &staleFakePool{guardVal: 1},
		selectRows: &retryFakeRows{rows: [][]any{{int16(1), int64(1)}}, err: errors.New("обрыв")}}
	if _, err := StaleRequeueOnce(ctx, p, StaleRequeueOptions{}); err == nil {
		t.Error("хвостовая ошибка rows.Err() выборки проглочена")
	}

	// Перевод: Scan и хвостовой Err (строки RETURNING).
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

	// Begin транзакции перевода (второй Begin прохода).
	sel, upd := staleSeedBatch([][2]int64{{1, 1}}, 1)
	p = &triStalePool{staleFakePool: &staleFakePool{guardVal: 1,
		selects: []*retryFakeRows{sel}, updates: []*retryFakeRows{upd}},
		beginFailNth: 2}
	if _, err := StaleRequeueOnce(ctx, p, StaleRequeueOptions{}); err == nil {
		t.Error("сбой Begin транзакции перевода проглочен")
	}

	// Commit перевода (второй Commit прохода).
	sel, upd = staleSeedBatch([][2]int64{{1, 1}}, 1)
	p = &triStalePool{staleFakePool: &staleFakePool{guardVal: 1,
		selects: []*retryFakeRows{sel}, updates: []*retryFakeRows{upd},
		commitErrs: []error{nil, errors.New("connection refused")}}}
	if _, err := StaleRequeueOnce(ctx, p, StaleRequeueOptions{}); err == nil {
		t.Error("сбой Commit перевода проглочен")
	}

	// nil-пул цикла — немедленная ошибка (не молчаливое зависание).
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

// TestStaleRequeueTriageRollback — обе транзакции прохода обязаны
// закрываться откатом (после Commit — no-op, но вызов есть): утечка
// незакрытой транзакции держит соединение пула.
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

// staleDrainQuiet — запуск StaleRequeueLoop и ожидание успокоения
// дренажа (прогресс статистики остановился): тик Interval час — после
// стартового дренажа цикл ждёт, все проходы в памяти — детерминированно.
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

// TestStaleRequeueTriageDrain — остановка дренажа: полная пачка с
// НУЛЕВЫМ переводом (guard/охраны отсеяли всё) — множество исчерпано,
// второго прохода нет; полная пачка с полным переводом — за ней ещё
// есть, дренаж продолжает следующей пачкой.
func TestStaleRequeueTriageDrain(t *testing.T) {
	// Полная пачка (BatchSize 1), перевод пуст: один проход, один SELECT.
	selZero, _ := staleSeedBatch([][2]int64{{1, 1}}, 0)
	p := &triStalePool{staleFakePool: &staleFakePool{guardVal: 1,
		selects: []*retryFakeRows{selZero}}}
	staleDrainQuiet(t, p)
	if st := p.stats(); st.selects != 1 || st.guards != 1 {
		t.Errorf("нулевая пачка: проходов (select=%d, guard=%d), хочу 1/1 — множество исчерпано", st.selects, st.guards)
	}

	// Полная пачка с переводом: дренаж продолжает — вторая (пустая)
	// выборка закрывает дренаж.
	selFull, updFull := staleSeedBatch([][2]int64{{1, 1}}, 1)
	p = &triStalePool{staleFakePool: &staleFakePool{guardVal: 1,
		selects: []*retryFakeRows{selFull}, updates: []*retryFakeRows{updFull}}}
	staleDrainQuiet(t, p)
	if st := p.stats(); st.selects != 2 || st.guards != 2 {
		t.Errorf("полная переведённая пачка: проходов (select=%d, guard=%d), хочу 2/2 — дренаж продолжает", st.selects, st.guards)
	}
}

// --- retry.go --------------------------------------------------------------

// TestRetryTriageDefaults — литеральный пин предела упавших пачек
// (T-268: три подряд — устойчивый сбой, до тика не молоть БД).
func TestRetryTriageDefaults(t *testing.T) {
	if DefaultRetryDrainFails != 3 {
		t.Errorf("DefaultRetryDrainFails = %d, хочу ровно 3 (T-268)", DefaultRetryDrainFails)
	}
}

// TestRetryTriageErrors — ошибки шагов: закрепление плана (каждое SET
// LOCAL отдельно), чтения guard корзин (Scan, хвостовой Err).
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

// retryDrainRun — запуск RetryTempErrors до успокоения дренажа.
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

// TestRetryTriageDrainFailCounter — счётчик упавших пачек обнуляется
// КАЖДЫМ успехом: после успешной пачки предел — ровно три падения
// подряд (сброс в 0, не в ±1 — иначе предел срабатывает раньше/позже).
func TestRetryTriageDrainFailCounter(t *testing.T) {
	dbDown := errors.New("бд недоступна")
	full := retryRows([]any{int16(1), int64(10), int16(0)})
	p := &retryFakePool{
		// Успех, затем устойчивый сбой guard.
		guardErrs: []error{nil, dbDown, dbDown, dbDown, dbDown, dbDown},
		selects:   []*retryFakeRows{full},
		requeues:  []*retryFakeRows{retryRows([]any{int16(1), int64(10), retryDay})},
	}
	retryDrainRun(t, p, RetryOptions{BatchSize: 1, Interval: time.Hour})
	if got := len(p.guardQueries); got != 4 {
		t.Errorf("guard-чтений = %d, хочу 4 (успех + ровно 3 падения подряд до предела)", got)
	}
}

// TestRetryTriageDrainConsecutiveReset — чередование сбой/успех: счётчик
// именно ПОСЛЕДОВАТЕЛЬНЫХ падений (успех обнуляет) — дренаж дорабатывает
// до конца (итог requeued=3, без предела).
func TestRetryTriageDrainConsecutiveReset(t *testing.T) {
	dbDown := errors.New("бд недоступна")
	cand := func() *retryFakeRows { return retryRows([]any{int16(1), int64(10), int16(0)}) }
	ret := func() *retryFakeRows { return retryRows([]any{int16(1), int64(10), retryDay}) }
	p := &retryFakePool{
		// сбой, успех, сбой, успех, сбой, успех, затем пусто — итог.
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

// TestLeaseTriageErrors — ошибки чтений основного прохода: Scan и
// хвостовой Err выборки пачки и перевода — ошибки прохода.
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

// TestLeaseTriageRollback — транзакция прохода закрывается откатом.
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

// TestLeaseNullTriageErrors — ошибки чтений ремонтного прохода страховки.
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

// TestLeaseNullTriageRollback — транзакция страховки закрывается откатом.
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

// TestLeaseTriageSelectSwallow — глушение возвратов ошибок выборки
// (Scan/rows.Err) обязано ронять проход ДО перевода: ответ перевода
// здесь подменён успехом, поэтому проглоченная ошибка доводила бы
// мутанта до фиксации и nil (кейсы выше ловили её только повторной
// ошибкой валидации пачки в фейке перевода).
func TestLeaseTriageSelectSwallow(t *testing.T) {
	ctx := context.Background()
	_, clock := leaseFixedClock()
	opts := LeaseOptions{Timeout: 15 * time.Minute, BatchSize: 8, Now: clock}

	// Scan-ошибка (одна колонка на три приёмника): ошибка «выборка пачки».
	p := &triLeasePool{leaseFakePool: &leaseFakePool{},
		selectRows: retryRows([]any{int16(1)}), updateRows: retryRows()}
	if _, err := LeaseOnce(ctx, p, &LeaseCursor{}, opts); err == nil ||
		!strings.Contains(err.Error(), "выборка пачки") {
		t.Errorf("глушение Scan выборки: %v, want ошибку выборки пачки", err)
	}

	// Хвостовая rows.Err: та же ошибка шага, не успех с пачкой.
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

// TestLeaseNullTriageSelectSwallow — то же для выборки ремонтного
// прохода страховки: перевод подменён успехом — глушение ошибок
// Scan/rows.Err доводило бы мутанта до фиксации и nil.
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

// TestRecheckTriageRowsErrors — ошибки чтения ответа UPDATE перепроверки:
// Scan и хвостовой Err — ошибки прохода (глушение молча теряло бы
// счёт переведённых).
func TestRecheckTriageRowsErrors(t *testing.T) {
	p := &triRecheckPool{recheckFakePool: &recheckFakePool{},
		rows: retryRows([]any{int16(1), int64(2)})} // колонок больше приёмника
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

// TestBacklogTriageNilMetrics — nil-приёмники метрик цикла — no-op:
// конструктор без реестра и наблюдения на nil не паникуют (контракт
// «nil — no-op», изолированные сборки без Prometheus).
func TestBacklogTriageNilMetrics(t *testing.T) {
	if m := newBacklogLoopMetrics(nil); m != nil {
		t.Errorf("newBacklogLoopMetrics(nil) = %v, хочу nil", m)
	}
	var m *backlogLoopMetrics
	m.observe(1.5) // не паникует
	m.skip()
}

// TestBacklogTriageHistogramBuckets — корзины длительности прохода —
// экспоненциальные 0.5с…512с (11 штук, старт 0.5, фактор 2): контракт
// наблюдаемости «проход дольше интервала» виден в собранной метрике.
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

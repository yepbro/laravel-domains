// Юнит-тесты компенсатора «статус повис» (T-270, ТЗ tasks/TZ-fsm.md §3
// ФТ-2.2) на фейковом pgx-пуле с транзакциями (стиль retry_test.go,
// позиционный Scan — общий retryRows): точные SQL-тексты guard/выборки/
// перевода и параметры (порог now() − 24 ч, LIMIT пачки, охраны
// new ∧ done ∧ возраст, сброс queued_at, дельты зон checked −n /
// pending +n), закрепление плана выборки SET LOCAL ×3, РАЗДЕЛЬНЫЕ
// транзакции выборки и перевода, счёт по RETURNING, guard ноль — проход
// без касания domains (дешёвая проба пустого множества), пустая пачка —
// no-op (идемпотентность повторного прохода), ошибки шагов, 40P01-повтор
// перевода, счётчик crawler_scheduler_stale_requeued_total растёт по
// переводам, дренаж цикла StaleRequeueLoop пачками до исчерпания с
// переживанием ошибки. Поведение против живой БД (включая потерянную
// q:http и gauge new_done_stale) — интеграционные тесты
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

// --- заготовки -------------------------------------------------------------

// staleNow — фиксированные часы прохода (стиль lease-тестов): порог
// вычисляется от них, тест сверяет точное значение.
var staleNow = time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)

// staleSeedBatch — заготовка пачки: n строк, translated строк RETURNING
// (guard мог отсеять часть).
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

// staleFakePool — StaleRequeueDB без БД: guard-чтение уровня
// new_done_stale через пул, очереди заготовленных результатов SELECT
// пачки (в транзакции закрепления плана) и UPDATE … RETURNING (в
// транзакции перевода), ошибки по шагам прохода, протокол вызовов и
// порядок операций. Всё под замком: цикл StaleRequeueLoop работает в
// горутине, тест ожидает прогресс по счётчикам.
type staleFakePool struct {
	mu sync.Mutex

	guardVal   int64            // уровень new_done_stale (0 — guard закрыт)
	guardErrs  []error          // FIFO ошибок guard (nil — чтение без ошибки)
	guardEmpty bool             // guard возвращает пустой набор — обрыв протокола
	selects    []*retryFakeRows // FIFO результатов SELECT пачки (кончилась — пустой набор)
	updates    []*retryFakeRows // FIFO результатов UPDATE RETURNING
	selectErrs []error          // FIFO ошибок SELECT (nil — без ошибки)
	planErr    error            // ошибка закрепления плана (SET LOCAL)
	updateErrs []error          // FIFO одноразовых ошибок UPDATE (nil — попытка без ошибки)
	beginErr   error            // ошибка Begin
	commitErrs []error          // FIFO ошибок Commit (кончилась — фиксация без ошибки)

	guardQueries []retryCall // guard-чтения через пул
	selectCalls  []retryCall // SELECT пачки (в транзакциях закрепления)
	updateCalls  []retryCall // UPDATE перевода (в транзакциях перевода)
	planCalls    []retryCall // SET LOCAL закрепления плана
	ops          []string    // порядок операций: begin/plan/select/update/commit
	begins       int
	commits      int
}

func (p *staleFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

func (p *staleFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("Exec не должен вызываться")
}

// Query — guard-чтение уровня new_done_stale: единственный SELECT прохода
// мимо транзакций; выборка пачки идёт в транзакции закрепления плана.
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
		return retryRows(), nil // пустой набор — обрыв протокола (coalesce отвечает строкой)
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

// nextErrLocked — ошибка из FIFO-очереди одноразовых ошибок (nil —
// попытка без ошибки); очередь пуста — nil.
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

// снимки протокола под замком (ожидание прогресса цикла в тестах).

type staleStats struct {
	guards  int // guard-чтений уровня
	selects int // выборок пачки
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

// staleFakeTx — транзакция фейкового пула: закрепление плана (Exec) и
// выборка пачки ЛИБО перевод (Query) — по SQL.
type staleFakeTx struct {
	pgx.Tx // остальные методы pgx.Tx проход не использует
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
		return retryRows(), nil // множество исчерпано
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

// Rollback — no-op: откат после Commit пуст, прерванная транзакция
// ничего не меняла (стиль retryFakeTx).
func (t *staleFakeTx) Rollback(context.Context) error { return nil }

// --- ожидаемые SQL-тексты (контракт Р15/ФТ-2.2) ----------------------------

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

// TestStaleRequeueOnceSQL — контракт всех операторов прохода: guard —
// точечный лукап уровня, SELECT пачки — new ∧ done ∧ возраст без
// сортировки (ранний останов LIMIT), UPDATE … FROM unnest с guard-охранами
// и дельтами зон checked −n / pending +n; точные параметры (порог
// now() − 24 ч, LIMIT, коды литералами), РАЗДЕЛЬНЫЕ транзакции выборки
// (закрепление плана SET LOCAL ×3 до SELECT) и перевода, счёт по
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
		[]int16{1, 1, 2},               // зоны выбранной пачки
		[]int64{1, 2, 3},               // домены выбранной пачки
		cutoff,                         // порог — тот же, что у выборки
	}
	if !reflect.DeepEqual(upds[0].args, wantUpdArgs) {
		t.Errorf("аргументы перевода = %v, want %v", upds[0].args, wantUpdArgs)
	}

	// Счёт по RETURNING: переведено 2 из выбранных 3 (guard отсеял одну).
	if res.Domains != 2 || res.Selected != 3 {
		t.Errorf("итог = %+v, want Domains=2 (RETURNING), Selected=3", res)
	}

	// Закрепление плана — только в транзакции выборки, тремя SET LOCAL
	// до SELECT (T-191): переводу не наследуется.
	if plans := pool.planSnapshot(); len(plans) != 3 ||
		plans[0].sql != staleRequeuePlanNoIndexScanSQL ||
		plans[1].sql != staleRequeuePlanNoBitmapScanSQL ||
		plans[2].sql != staleRequeuePlanNoIOSQL {
		t.Errorf("закрепление плана = %v, want SET LOCAL ×3 до SELECT", plans)
	}
}

// TestStaleRequeueOnceWindow — порог 24 ч: константа равна окну gauge
// (ФТ-2.2), параметр выборки — now() − 24 ч от источника времени прохода.
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

// TestStaleRequeueOnceGuardClosed — дешёвая проба пустого множества:
// уровень new_done_stale ноль (стационар) — проход читает ОДИН лукап
// счётчика и не открывает транзакций: domains не сканируется вовсе.
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

// TestStaleRequeueOnceEmptyBatch — guard открыт, выборка пуста (дрейф
// уровня до реконсиляции): транзакция перевода не открывается, итог
// нулевой — повторный проход no-op (идемпотентность).
func TestStaleRequeueOnceEmptyBatch(t *testing.T) {
	pool := &staleFakePool{guardVal: 1} // selects/updates пусты

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

// TestStaleRequeueOnceIdempotent — повторный проход после переведённой
// пачки: переведённые перестают удовлетворять отбору (выборка пуста) —
// no-op, счётчик не растёт.
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

// TestStaleRequeueOnceDefaults — нулевые опции: LIMIT — дефолтная пачка,
// порог — от системных часов (Now nil не мешает проходу).
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

// TestStaleRequeueOnceError — ошибки шагов прохода обёртываются с
// контекстом компенсатора; обрыв протокола guard (пустой набор) и nil-пул
// — ошибки.
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

// TestStaleRequeueOnceDeadlockRetry — 40P01 транзакции перевода
// повторяется целиком (T-268): охраны идемпотентны, переведённое первой
// попыткой не переведётся повторно — повторный Begin того же оператора.
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

// TestStaleRequeueOnceMetrics — счётчик crawler_scheduler_stale_requeued_total
// растёт на фактически переведённые (по RETURNING, не по выборке);
// без реестра (nil Metrics) проход работает.
func TestStaleRequeueOnceMetrics(t *testing.T) {
	reg := metrics.New("scheduler")
	sel, upd := staleSeedBatch([][2]int64{{1, 1}, {1, 2}, {2, 3}}, 2) // переведено 2 (guard отсеял одну)
	pool := &staleFakePool{guardVal: 1, selects: []*retryFakeRows{sel}, updates: []*retryFakeRows{upd}}

	if _, err := StaleRequeueOnce(context.Background(), pool, StaleRequeueOptions{Metrics: reg}); err != nil {
		t.Fatalf("StaleRequeueOnce: %v", err)
	}
	if got := staleRequeuedValue(t, reg); got != 2 {
		t.Errorf("счётчик = %v, want 2 (по RETURNING)", got)
	}

	// Без реестра (nil Metrics) и с закрытым guard — no-op, проход выполняется.
	emptyPool := &staleFakePool{guardVal: 0}
	if _, err := StaleRequeueOnce(context.Background(), emptyPool, StaleRequeueOptions{}); err != nil {
		t.Fatalf("StaleRequeueOnce без реестра: %v", err)
	}
}

// staleRequeuedValue — значение счётчика компенсатора в реестре (стиль
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

// --- цикл StaleRequeueLoop ---------------------------------------------------

// TestStaleRequeueLoopDrain — стартовый дренаж пачками до исчерпания:
// полная пачка продолжает дренаж, неполная останавливает; счётчик растёт
// на итог; без тика новых выборок нет; отмена ctx — чистый nil.
func TestStaleRequeueLoopDrain(t *testing.T) {
	reg := metrics.New("scheduler")
	fullSel, fullUpd := staleSeedBatch([][2]int64{{1, 1}, {1, 2}}, 2)
	tailSel, tailUpd := staleSeedBatch([][2]int64{{2, 3}}, 1)
	pool := &staleFakePool{
		guardVal: 1,
		selects:  []*retryFakeRows{fullSel, tailSel}, // BatchSize=2: полная, затем неполная
		updates:  []*retryFakeRows{fullUpd, tailUpd},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- StaleRequeueLoop(ctx, pool, StaleRequeueOptions{
			BatchSize: 2,
			Interval:  time.Hour, // тик не наступит: только стартовый дренаж
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
	// Обе пачки — одним стартовым дренажём: неполная пачка останавливает.
	if got := len(pool.selectSnapshot()); got != 2 {
		t.Errorf("SELECT-вызовов = %d, want 2 (неполная пачка останавливает)", got)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("StaleRequeueLoop: %v, want nil при отмене ctx", err)
	}
}

// TestStaleRequeueLoopError — ошибка прохода прерывает только текущий
// дренаж: цикл живёт, очередной тик повторяет (контракт OnRun); Warn в
// журнале.
func TestStaleRequeueLoopError(t *testing.T) {
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	sel, upd := staleSeedBatch([][2]int64{{1, 1}}, 1)
	pool := &staleFakePool{
		guardVal:  1,
		selects:   []*retryFakeRows{sel},
		updates:   []*retryFakeRows{upd},
		guardErrs: []error{errors.New("бд недоступна")}, // первый проход падает
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

	// Первый дренаж падает, тик повторяет и доводит пачку.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if st := pool.stats(); st.commits >= 2 { // выборка и перевод дожаты до фиксаций
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

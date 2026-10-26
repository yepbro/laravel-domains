// Юнит-тесты возврата доменов с истёкшим lease (T-029; форма T-200 —
// SELECT пачки по курсору + перевод PK-пробами, ТЗ
// TZ_slow_lease_return.md §7.1) на фейковом pgx-пуле с транзакциями
// (стиль retry_test.go): точные SQL-тексты обоих операторов и параметры
// (граница now() − lease, охрана queued, сброс queued_at, коды
// check_state/status, композитный курсор), пара операторов в одной
// транзакции (Begin → SELECT → UPDATE → Commit), счёт по RETURNING,
// пустая пачка не двигает курсор, guard отсеял записанный воркером
// результат, ошибки и отмена ctx, цикл LeaseWatch (немедленный дренаж,
// интервал, курсор живёт между пачками/дренажами, рестарт сбрасывает,
// переживание ошибки, чистый выход). Поведение против живой БД —
// интеграционные тесты (test/integration/lease_test.go).
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

// --- фейковый пул с транзакциями ------------------------------------------
//
// Имена с префиксом lease: в пакете параллельно живут фейки других задач
// планировщика (retryFakePool T-191), общих хелперов здесь не заводят
// (кроме retryRows — переиспользуется позиционный Scan).

// leaseCall — записанный SQL-вызов с аргументами.
type leaseCall struct {
	sql  string
	args []any
}

// leaseBatch — заготовленный ответ прохода: пачка SELECT (строки в
// порядке индексного ключа) и итог перевода translated — строк RETURNING
// (guard мог отсеять часть пачки: translated < len(rows)).
type leaseBatch struct {
	rows       []leaseRow
	translated int64
}

// leaseFakePool — LeaseDB без БД: очередь пачек SELECT (кончилась —
// пустая пачка), ошибки по шагам прохода, протокол вызовов и порядок
// операций транзакции. Всё под замком: цикл LeaseWatch работает в
// горутине, тест ожидает прогресс по счётчикам.
type leaseFakePool struct {
	mu         sync.Mutex
	batches    []leaseBatch // FIFO пачек SELECT
	selectErrs []error      // FIFO ошибок SELECT (nil — без ошибки)
	updateErrs []error      // FIFO ошибок UPDATE (nil — без ошибки)
	beginErr   error
	commitErrs []error // FIFO ошибок Commit (кончилась — фиксация без ошибки)

	ops         []string    // порядок операций: begin/select/update/commit
	begins      int         // транзакций открыто
	commits     int         // зафиксировано
	selectCalls []leaseCall // SELECT пачки (проверка курсора)
	updateCalls []leaseCall // UPDATE перевода
}

func (p *leaseFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

func (p *leaseFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("Exec не должен вызываться")
}

func (p *leaseFakePool) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("Query через пул не должен вызываться — оба оператора в транзакции")
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

// nextErrLocked — следующая ошибка FIFO-очереди (nil-элемент — шаг без
// ошибки), ok — очередь непуста.
func nextErrLocked(q *[]error) (error, bool) {
	if len(*q) == 0 {
		return nil, false
	}
	err := (*q)[0]
	*q = (*q)[1:]
	return err, true
}

// nextBatchLocked — следующая пачка FIFO (кончилась — пустая: множество
// исчерпано).
func (p *leaseFakePool) nextBatchLocked() leaseBatch {
	if len(p.batches) == 0 {
		return leaseBatch{}
	}
	b := p.batches[0]
	p.batches = p.batches[1:]
	return b
}

// stats — снимок счётчиков под замком (ожидание прогресса цикла).
func (p *leaseFakePool) stats() (begins, selects, updates, commits int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.begins, len(p.selectCalls), len(p.updateCalls), p.commits
}

// opsSnapshot — снимок порядка операций под замком.
func (p *leaseFakePool) opsSnapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.ops...)
}

// selectSnapshot — снимок протокола SELECT под замком.
func (p *leaseFakePool) selectSnapshot() []leaseCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]leaseCall(nil), p.selectCalls...)
}

// updateSnapshot — снимок протокола UPDATE под замком.
func (p *leaseFakePool) updateSnapshot() []leaseCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]leaseCall(nil), p.updateCalls...)
}

// leaseFakeTx — транзакция фейкового пула: SELECT пачки и UPDATE перевода
// (оба обязаны идти в ней — пул напрямую операторов не принимает), помнит
// пачку этого SELECT: UPDATE обязан пробивать ровно её ключи.
type leaseFakeTx struct {
	pgx.Tx // остальные методы pgx.Tx проход не использует

	pool      *leaseFakePool
	batch     leaseBatch // пачка SELECT этой транзакции
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
	case leaseUpdateSQL:
		t.pool.updateCalls = append(t.pool.updateCalls, leaseCall{sql: sql, args: args})
		if err, ok := nextErrLocked(&t.pool.updateErrs); ok && err != nil {
			return nil, err
		}
		// Контракт перевода: unnest-ключи — ровно пачка SELECT этой же
		// транзакции, в порядке выборки.
		if len(args) != 5 {
			return nil, fmt.Errorf("leaseFakeTx: перевод: параметров %d, нужно 5", len(args))
		}
		zones, okZ := args[1].([]int16)
		ids, okI := args[2].([]int64)
		if !okZ || !okI {
			return nil, fmt.Errorf("leaseFakeTx: перевод: ключи %T/%T, нужно []int16/[]int64", args[1], args[2])
		}
		if len(zones) != len(t.batch.rows) || len(ids) != len(t.batch.rows) {
			return nil, fmt.Errorf("leaseFakeTx: перевод: ключей %d, выбрано %d — пробивать обязано пачку",
				len(zones), len(t.batch.rows))
		}
		for i, r := range t.batch.rows {
			if zones[i] != r.zone || ids[i] != r.id {
				return nil, fmt.Errorf("leaseFakeTx: перевод: ключ %d = (%d,%d), выбран (%d,%d)",
					i, zones[i], ids[i], r.zone, r.id)
			}
		}
		t.pool.ops = append(t.pool.ops, "update")
		rows := make([][]any, t.batch.translated)
		for i := range rows {
			rows[i] = []any{int16(1)} // zone_id: значение для счёта строк не важно
		}
		return retryRows(rows...), nil
	default:
		return nil, fmt.Errorf("leaseFakeTx: неожидаемый SQL в транзакции: %s", sql)
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

// --- заготовки пачек -------------------------------------------------------

// leaseSeedBatch — пачка n строк: queuedAt строго растёт по i (последняя
// строка — максимум ключа: курсор после пачки проверяем по ней), id по
// порядку, зона одна.
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

// --- ожидаемые SQL-тексты (контракт, ТЗ T-200 §§4.1–4.2) ------------------

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

// leaseFixedClock — часы с фиксированным «сейчас» (порог прохода
// воспроизводим до наносекунды).
func leaseFixedClock() (time.Time, func() time.Time) {
	now := time.Date(2026, 10, 17, 4, 30, 0, 0, time.UTC)
	return now, func() time.Time { return now }
}

// leaseCursorArgs — аргументы-курсор вызова SELECT (позиции 2–4).
func leaseCursorArgs(c leaseCall) (time.Time, int16, int64) {
	q, _ := c.args[2].(time.Time)
	z, _ := c.args[3].(int16)
	i, _ := c.args[4].(int64)
	return q, z, i
}

// --- LeaseOnce -----------------------------------------------------------

// TestLeaseOnceSQL — контракт обоих операторов (ТЗ §§4.1–4.2): SELECT пачки
// с композитным курсором и порогом серверных часов, UPDATE … FROM unnest с
// guard-охранами и сбросом queued_at; точные параметры, порядок операций
// одной транзакции (Begin → SELECT → UPDATE → Commit), счёт по RETURNING,
// курсор — за последнюю строку пачки.
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
		t.Errorf("порядок операций = %v, want [begin select update commit] — пара операторов в одной транзакции", ops)
	}

	sel := pool.selectSnapshot()
	if len(sel) != 1 {
		t.Fatalf("SELECT-вызовов = %d, want 1", len(sel))
	}
	if sel[0].sql != wantLeaseSelect {
		t.Errorf("SQL выборки:\n%s\nwant:\n%s", sel[0].sql, wantLeaseSelect)
	}
	wantSelArgs := []any{
		&now,                            // «сейчас» тестовых часов (в бою NULL → now() БД)
		(15 * time.Minute).Seconds(),    // срок аренды — make_interval
		time.Time{}, int16(0), int64(0), // нулевой курсор — с начала индекса
		8000, // LIMIT пачки
	}
	if !reflect.DeepEqual(sel[0].args, wantSelArgs) {
		t.Errorf("аргументы выборки = %v, want %v", sel[0].args, wantSelArgs)
	}

	upd := pool.updateSnapshot()
	if len(upd) != 1 {
		t.Fatalf("UPDATE-вызовов = %d, want 1", len(upd))
	}
	if upd[0].sql != wantLeaseUpdate {
		t.Errorf("SQL перевода:\n%s\nwant:\n%s", upd[0].sql, wantLeaseUpdate)
	}
	wantUpdArgs := []any{
		int16(model.CheckStatePending), // SET check_state
		[]int16{1, 1, 1},               // зоны выбранной пачки
		[]int64{1, 2, 3},               // домены выбранной пачки
		&now,                           // то же «сейчас» — порог един
		(15 * time.Minute).Seconds(),
	}
	if !reflect.DeepEqual(upd[0].args, wantUpdArgs) {
		t.Errorf("аргументы перевода = %v, want %v", upd[0].args, wantUpdArgs)
	}

	// Счёт по RETURNING: переведено 2 из выбранных 3 (guard отсеял одну).
	if res.Domains != 2 || res.Selected != 3 {
		t.Errorf("итог = %+v, want Domains=2 (RETURNING), Selected=3", res)
	}
	// Курсор — последняя строка пачки (минутная сетка seed-заготовки).
	want := leaseSeedBatch(3, 0, base).rows[2]
	if cursor.queuedAt != want.queuedAt || cursor.zone != want.zone || cursor.id != want.id {
		t.Errorf("курсор = (%v,%d,%d), want последняя строка пачки (%v,%d,%d)",
			cursor.queuedAt, cursor.zone, cursor.id, want.queuedAt, want.zone, want.id)
	}
}

// TestLeaseOnceClockIsServer — без тестовых часов «сейчас» не передаётся
// вовсе (NULL) ни в выборку, ни в перевод: порог считает now() сервера БД,
// а не часы процесса планировщика.
func TestLeaseOnceClockIsServer(t *testing.T) {
	pool := &leaseFakePool{batches: []leaseBatch{leaseSeedBatch(1, 1, time.Now())}}
	if _, err := LeaseOnce(context.Background(), pool, nil, LeaseOptions{}); err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}
	selArgs := pool.selectSnapshot()[0].args
	if got, ok := selArgs[0].(*time.Time); !ok || got != nil {
		t.Errorf("«сейчас» выборки = %v (%T), want nil *time.Time — часы сервера БД", selArgs[0], selArgs[0])
	}
	if got, ok := selArgs[1].(float64); !ok || got != (30*time.Minute).Seconds() {
		t.Errorf("срок = %v, want %v секунд дефолта", selArgs[1], (30 * time.Minute).Seconds())
	}
	updArgs := pool.updateSnapshot()[0].args
	if got, ok := updArgs[3].(*time.Time); !ok || got != nil {
		t.Errorf("«сейчас» перевода = %v (%T), want nil *time.Time", updArgs[3], updArgs[3])
	}
}

// TestLeaseOnceDefaults — нулевые опции: срок аренды и пачка — дефолты
// (30 минут по ТЗ 5.3, 5000), часы фиксированные: «сейчас» и срок уходят
// в SQL параметрами.
func TestLeaseOnceDefaults(t *testing.T) {
	now, clock := leaseFixedClock()
	pool := &leaseFakePool{batches: []leaseBatch{leaseSeedBatch(1, 1, time.Now())}}

	if _, err := LeaseOnce(context.Background(), pool, nil, LeaseOptions{Now: clock}); err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}

	args := pool.selectSnapshot()[0].args
	if args[5] != DefaultLeaseBatch {
		t.Errorf("LIMIT = %v, want дефолт %d", args[5], DefaultLeaseBatch)
	}
	if got, ok := args[0].(*time.Time); !ok || !got.Equal(now) {
		t.Errorf("«сейчас» = %v, want фиксированные часы %v", args[0], now)
	}
	if got, ok := args[1].(float64); !ok || got != DefaultLeaseTimeout.Seconds() {
		t.Errorf("срок = %v, want дефолт %s в секундах", args[1], DefaultLeaseTimeout)
	}
}

// TestLeaseOnceEmpty — исчерпанное множество: пустая пачка — только SELECT
// (переводить нечего, UPDATE не выполняется), транзакция не фиксируется,
// итог нулевой, курсор не двигается.
func TestLeaseOnceEmpty(t *testing.T) {
	pool := &leaseFakePool{} // очередь пачек пуста

	var cursor LeaseCursor
	res, err := LeaseOnce(context.Background(), pool, &cursor, LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}
	if res != (LeaseResult{}) {
		t.Errorf("итог = %+v, want нулевой", res)
	}
	if _, selects, updates, commits := pool.stats(); selects != 1 || updates != 0 || commits != 0 {
		t.Errorf("selects=%d updates=%d commits=%d, want 1/0/0 — пустая пачка без перевода",
			selects, updates, commits)
	}
	if (cursor != LeaseCursor{}) {
		t.Errorf("курсор = %+v, want нулевой — пустая пачка не двигает", cursor)
	}
	if ops := pool.opsSnapshot(); !reflect.DeepEqual(ops, []string{"begin", "select"}) {
		t.Errorf("порядок операций = %v, want [begin select]", ops)
	}
}

// TestLeaseOnceCursorMoves — курсор двигается за последнюю строку
// непустой пачки и передаётся следующим проходом (args SELECT), пустая
// пачка его не трогает.
func TestLeaseOnceCursorMoves(t *testing.T) {
	base := time.Date(2026, 10, 17, 3, 0, 0, 0, time.UTC)
	first := leaseSeedBatch(2, 2, base)                 // строки base+0, base+1m
	second := leaseSeedBatch(3, 3, base.Add(time.Hour)) // строки base+1h+{0,1,2}m
	pool := &leaseFakePool{batches: []leaseBatch{first, second}}

	var cursor LeaseCursor
	if _, err := LeaseOnce(context.Background(), pool, &cursor, LeaseOptions{}); err != nil {
		t.Fatalf("первый проход: %v", err)
	}
	want := first.rows[len(first.rows)-1]
	if cursor.queuedAt != want.queuedAt || cursor.id != want.id {
		t.Fatalf("курсор после пачки = (%v,%d), want последняя строка (%v,%d)",
			cursor.queuedAt, cursor.id, want.queuedAt, want.id)
	}

	// Второй проход получает курсор первого в аргументах SELECT (снимок
	// курсора до второго прохода — дальше он уходит за вторую пачку).
	cursorAfterFirst := cursor
	if _, err := LeaseOnce(context.Background(), pool, &cursor, LeaseOptions{}); err != nil {
		t.Fatalf("второй проход: %v", err)
	}
	q, z, i := leaseCursorArgs(pool.selectSnapshot()[1])
	if q != cursorAfterFirst.queuedAt || z != cursorAfterFirst.zone || i != cursorAfterFirst.id {
		t.Errorf("курсор второго SELECT = (%v,%d,%d), want переданный (%v,%d,%d)",
			q, z, i, cursorAfterFirst.queuedAt, cursorAfterFirst.zone, cursorAfterFirst.id)
	}

	// Пустая пачка (очередь кончилась) курсор не двигает.
	if _, err := LeaseOnce(context.Background(), pool, &cursor, LeaseOptions{}); err != nil {
		t.Fatalf("третий проход: %v", err)
	}
	if cursor.queuedAt != second.rows[len(second.rows)-1].queuedAt {
		t.Errorf("курсор после пустой пачки = %v, want прежний %v",
			cursor.queuedAt, second.rows[len(second.rows)-1].queuedAt)
	}
}

// TestLeaseOnceGuardFiltered — запись результата между выборкой и
// блокировкой (гонка с воркером): перевод пробивает ВСЮ выбранную пачку
// (ключи unnest — выбранное множество), но счёт — по RETURNING: отсеянные
// охраной строки в Domains не попадают, курсор всё равно уходит за пачку
// (судьба отсеянных изменилась — под старым ключом их нет).
func TestLeaseOnceGuardFiltered(t *testing.T) {
	base := time.Date(2026, 10, 17, 5, 0, 0, 0, time.UTC)
	pool := &leaseFakePool{batches: []leaseBatch{leaseSeedBatch(5, 1, base)}}

	var cursor LeaseCursor
	res, err := LeaseOnce(context.Background(), pool, &cursor, LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}
	if res.Domains != 1 || res.Selected != 5 {
		t.Fatalf("итог = %+v, want Domains=1 (RETURNING после guard), Selected=5", res)
	}
	upd := pool.updateSnapshot()
	if got := len(upd[0].args[1].([]int16)); got != 5 {
		t.Errorf("unnest-ключей перевода = %d, want 5 — пробивается вся выбранная пачка", got)
	}
	want := leaseSeedBatch(5, 0, base).rows[4]
	if cursor.queuedAt != want.queuedAt || cursor.id != want.id {
		t.Errorf("курсор = (%v,%d), want за последнюю строку пачки (%v,%d)",
			cursor.queuedAt, cursor.id, want.queuedAt, want.id)
	}
}

// TestLeaseOnceRepeatPasses — переведённые строки не выбираются повторно
// (условие отбора в самом SQL): каждый проход — своя транзакция, после
// опустошения — нулевой итог.
func TestLeaseOnceRepeatPasses(t *testing.T) {
	base := time.Date(2026, 10, 17, 6, 0, 0, 0, time.UTC)
	pool := &leaseFakePool{batches: []leaseBatch{
		leaseSeedBatch(5, 5, base),
		leaseSeedBatch(7, 7, base.Add(time.Hour)),
	}}

	var cursor LeaseCursor
	first, err := LeaseOnce(context.Background(), pool, &cursor, LeaseOptions{})
	if err != nil {
		t.Fatalf("первый проход: %v", err)
	}
	second, err := LeaseOnce(context.Background(), pool, &cursor, LeaseOptions{})
	if err != nil {
		t.Fatalf("второй проход: %v", err)
	}
	third, err := LeaseOnce(context.Background(), pool, &cursor, LeaseOptions{})
	if err != nil {
		t.Fatalf("третий проход: %v", err)
	}
	if first.Domains != 5 || second.Domains != 7 || third.Domains != 0 {
		t.Errorf("проходы = %d, %d, %d; want 5, 7, 0",
			first.Domains, second.Domains, third.Domains)
	}
	if _, selects, _, commits := pool.stats(); selects != 3 || commits != 2 {
		t.Errorf("selects=%d commits=%d, want 3/2 (пустой проход без фиксации)", selects, commits)
	}
}

// TestLeaseOnceNilCursor — nil-курсор: разовый проход с начала индекса
// без сохранения (стиль ReanalyzeTick) — работает и не паникает, каждый
// вызов стартует с нулевого курсора.
func TestLeaseOnceNilCursor(t *testing.T) {
	pool := &leaseFakePool{batches: []leaseBatch{
		leaseSeedBatch(2, 2, time.Now()),
		leaseSeedBatch(2, 2, time.Now().Add(time.Hour)),
	}}
	for i := 0; i < 2; i++ {
		if _, err := LeaseOnce(context.Background(), pool, nil, LeaseOptions{}); err != nil {
			t.Fatalf("проход %d с nil-курсором: %v", i+1, err)
		}
	}
	for i, call := range pool.selectSnapshot() {
		if q, z, id := leaseCursorArgs(call); !q.IsZero() || z != 0 || id != 0 {
			t.Errorf("SELECT %d с nil-курсором: курсор (%v,%d,%d), want нулевой", i, q, z, id)
		}
	}
}

// TestLeaseOnceError — ошибки шагов прохода (выборка, перевод, транзакция,
// фиксация) обёртываются с контекстом задачи; провал НЕ двигает курсор —
// повтор прохода берёт пачку заново; отмена ctx сохраняется errors.Is.
func TestLeaseOnceError(t *testing.T) {
	base := time.Date(2026, 10, 17, 7, 0, 0, 0, time.UTC)

	selectPool := &leaseFakePool{
		batches:    []leaseBatch{leaseSeedBatch(2, 2, base)},
		selectErrs: []error{errors.New("бд недоступна")},
	}
	var cursor LeaseCursor
	_, err := LeaseOnce(context.Background(), selectPool, &cursor, LeaseOptions{})
	if err == nil ||
		!strings.Contains(err.Error(), "scheduler: возврат истёкших lease") ||
		!strings.Contains(err.Error(), "бд недоступна") {
		t.Errorf("ошибка выборки = %v, want контекст задачи и исходная ошибка", err)
	}
	if (cursor != LeaseCursor{}) {
		t.Errorf("курсор сдвинулся при ошибке выборки: %+v", cursor)
	}

	cancelPool := &leaseFakePool{
		batches:    []leaseBatch{leaseSeedBatch(2, 2, base)},
		updateErrs: []error{context.Canceled},
	}
	if _, err := LeaseOnce(context.Background(), cancelPool, &cursor, LeaseOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка перевода = %v, want context.Canceled", err)
	}
	if (cursor != LeaseCursor{}) {
		t.Errorf("курсор сдвинулся при ошибке перевода: %+v", cursor)
	}

	beginPool := &leaseFakePool{beginErr: errors.New("нет соединений")}
	if _, err := LeaseOnce(context.Background(), beginPool, &cursor, LeaseOptions{}); err == nil ||
		!strings.Contains(err.Error(), "транзакция") {
		t.Errorf("ошибка Begin = %v, want контекст транзакции", err)
	}

	commitPool := &leaseFakePool{
		batches:    []leaseBatch{leaseSeedBatch(2, 2, base)},
		commitErrs: []error{errors.New("connection refused")},
	}
	if _, err := LeaseOnce(context.Background(), commitPool, &cursor, LeaseOptions{}); err == nil ||
		!strings.Contains(err.Error(), "фиксация") {
		t.Errorf("ошибка Commit = %v, want контекст фиксации", err)
	}
	if (cursor != LeaseCursor{}) {
		t.Errorf("курсор сдвинулся при провале фиксации: %+v — пачка обязана повториться", cursor)
	}
}

func TestLeaseOnceNilPool(t *testing.T) {
	if _, err := LeaseOnce(context.Background(), nil, nil, LeaseOptions{}); err == nil ||
		!strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
	}
}

// --- LeaseOptions ---------------------------------------------------------

func TestLeaseOptionsDefaults(t *testing.T) {
	// Дефолт пачки пинируется литералом ТЗ 5.3 (коридор 1000–10 000):
	// сравнение с самой константой мутации не замечает.
	if DefaultLeaseBatch != 5000 {
		t.Errorf("DefaultLeaseBatch = %d, want 5000", DefaultLeaseBatch)
	}

	// Нулевые и отрицательные значения — дефолты; интервал цикла —
	// половина срока аренды (баланс «скорость возврата vs нагрузка»,
	// см. LeaseWatch), в том числе от дефолтного срока.
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
		t.Errorf("withDefaults(отрицательные) = %+v, want дефолты", got)
	}

	// Короткий срок — интервал ужимается вместе с ним: цикл не
	// «переспит» настраиваемый lease.
	got = (LeaseOptions{Timeout: 2 * time.Minute}).withDefaults()
	if got.Interval != time.Minute {
		t.Errorf("Interval = %v, want 1m (Timeout/2 при Timeout=2m)", got.Interval)
	}

	// Вырожденный Timeout 1 нс делением дал бы 0 — тикеру нужен
	// положительный интервал.
	if got := (LeaseOptions{Timeout: time.Nanosecond}).withDefaults(); got.Interval <= 0 {
		t.Errorf("Interval = %v при Timeout=1ns, want > 0", got.Interval)
	}

	// Границы замен: 1 нс и 1 — положительные значения, не дефолты.
	if got := (LeaseOptions{Timeout: time.Nanosecond, BatchSize: 1}).withDefaults(); got.Timeout != time.Nanosecond || got.BatchSize != 1 {
		t.Errorf("withDefaults(Timeout=1нс, BatchSize=1) = %+v, want без замены", got)
	}
	// Интервал 1 нс проверяется при Timeout 4 нс: подмена дефолтом дала
	// бы 2 нс (Timeout/2), а вырожденный Timeout 1 нс совпадает с
	// внутренним минимумом тикера и подмену не выявляет.
	if got := (LeaseOptions{Timeout: 4 * time.Nanosecond, Interval: time.Nanosecond}).withDefaults(); got.Interval != time.Nanosecond {
		t.Errorf("Interval 1нс = %v, want без замены", got.Interval)
	}

	// Заданные значения не затираются, Now передаётся как есть.
	now := func() time.Time { return time.Unix(0, 0) }
	got = LeaseOptions{
		Timeout:   time.Hour,
		BatchSize: 10,
		Interval:  time.Minute,
		Now:       now,
	}.withDefaults()
	if got.Timeout != time.Hour || got.BatchSize != 10 || got.Interval != time.Minute {
		t.Errorf("withDefaults затирает значения: %+v", got)
	}
	if got.Now == nil || !got.Now().Equal(time.Unix(0, 0)) {
		t.Errorf("withDefaults затирает Now: nil=%v, значение %v", got.Now == nil, got.Now())
	}
}

// --- LeaseWatch -----------------------------------------------------------

// TestLeaseWatchImmediateDrain — первый дренаж выполняется сразу при
// старте, не дожидаясь тика: подбирает зависшее за простой сервиса
// (ТЗ 3.4, 11.4 п. 9).
func TestLeaseWatchImmediateDrain(t *testing.T) {
	pool := &leaseFakePool{batches: []leaseBatch{leaseSeedBatch(1, 1, time.Now())}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- LeaseWatch(ctx, pool, LeaseOptions{
			Interval: time.Hour, // тик не наступит: только стартовый дренаж
		})
	}()

	// Стартовый дренаж: одна транзакция и тишина до отмены.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, selects, _, commits := pool.stats(); selects >= 1 && commits >= 1 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("стартовый дренаж не выполнен")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	if _, selects, updates, commits := pool.stats(); selects != 1 || updates != 1 || commits != 1 {
		t.Errorf("selects=%d updates=%d commits=%d, want 1/1/1: без тика повторных дренажей нет",
			selects, updates, commits)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("LeaseWatch: %v, want nil при отмене ctx", err)
	}
}

// TestLeaseWatchLoop — полный дренаж пачками до исчерпания, ошибка
// пачки прерывает только свой дренаж (Warn), очередной тик продолжает;
// интервал по умолчанию — половина срока аренды; отмена ctx — чистый nil.
func TestLeaseWatchLoop(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	// Дренаж 1 (старт): ошибка БД — Warn. Дренаж 2 (тик): полная пачка +
	// неполная — Info с суммой. Дальше — пустые множества.
	base := time.Date(2026, 10, 17, 8, 0, 0, 0, time.UTC)
	pool := &leaseFakePool{
		batches: []leaseBatch{
			leaseSeedBatch(DefaultLeaseBatch, DefaultLeaseBatch, base),
			leaseSeedBatch(3, 3, base.Add(time.Hour)),
		},
		selectErrs: []error{errors.New("бд недоступна")},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- LeaseWatch(ctx, pool, LeaseOptions{
			Timeout: 40 * time.Millisecond, // интервал-дефолт: 20 мс
			Log:     logger,
		})
	}()

	// Три прохода SELECT: провал первого дренажа и обе пачки второго.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, selects, _, _ := pool.stats(); selects >= 3 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("цикл не дошёл до третьего прохода: selects=%d", len(pool.selectSnapshot()))
		}
		time.Sleep(time.Millisecond)
	}
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("LeaseWatch: %v, want nil при отмене ctx", err)
	}

	log := buf.String()
	for _, want := range []string{
		"возврат доменов с истёкшим lease", // старт
		"interval=20ms", // дефолт: Timeout/2
		"level=WARN",    // ошибка дренажа не убила цикл
		"бд недоступна",
		fmt.Sprintf("domains=%d", DefaultLeaseBatch+3), // итог дренажа
	} {
		if !strings.Contains(log, want) {
			t.Errorf("лог не содержит %q:\n%s", want, log)
		}
	}
}

// TestLeaseWatchGuardFilteredFullBatch — полная пачка выборки с нулевым
// переводом (guard отсеял всё — результаты записаны между выборкой и
// блокировкой) НЕ останавливает дренаж: критерий продолжения — выборка
// прошла целиком, курсор ушёл за пачку, возвращаться к пройденному
// незачем.
func TestLeaseWatchGuardFilteredFullBatch(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	base := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	pool := &leaseFakePool{
		batches: []leaseBatch{
			leaseSeedBatch(DefaultLeaseBatch, 0, base), // всё отсеял guard
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
			t.Fatalf("дренаж не продолжился после нулевого перевода: selects=%d updates=%d",
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
		t.Errorf("дренаж не дошёл до второй пачки (итог 3):\n%s", buf.String())
	}
}

// TestLeaseWatchCursorLives — курсор дренажа переживает пачки одного
// дренажа и сами дренажи: вторая пачка стартует за последней строкой
// первой, первый проход следующего дренажа — за последней строкой
// прошлого дренажа (не с начала индекса).
func TestLeaseWatchCursorLives(t *testing.T) {
	base := time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC)
	full := leaseSeedBatch(2, 2, base)                     // строки base, base+1m
	partial := leaseSeedBatch(1, 1, base.Add(time.Minute)) // строка base+2m
	pool := &leaseFakePool{batches: []leaseBatch{full, partial}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- LeaseWatch(ctx, pool, LeaseOptions{
			BatchSize: 2,
			Timeout:   40 * time.Millisecond, // интервал-дефолт 20 мс — тик ждём, а не 15 мин
		})
	}()

	// Дренаж 1: полная пачка (2 == BatchSize) + неполная (1) — стоп.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, selects, _, commits := pool.stats(); selects >= 2 && commits >= 2 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("первый дренаж не дошёл до второй пачки")
		}
		time.Sleep(time.Millisecond)
	}

	// Дренаж 2 (тик): первый проход продолжает курсором прошлого дренажа.
	deadline = time.Now().Add(2 * time.Second)
	for {
		if _, selects, _, _ := pool.stats(); selects >= 3 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("второй дренаж не начался")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("LeaseWatch: %v", err)
	}

	calls := pool.selectSnapshot()
	// Пачка 1 — нулевой курсор (старт).
	if q, z, i := leaseCursorArgs(calls[0]); !q.IsZero() || z != 0 || i != 0 {
		t.Errorf("курсор пачки 1 = (%v,%d,%d), want нулевой", q, z, i)
	}
	// Пачка 2 — за последнюю строку пачки 1 (внутри дренажа).
	want1 := full.rows[1]
	if q, z, i := leaseCursorArgs(calls[1]); q != want1.queuedAt || z != want1.zone || i != want1.id {
		t.Errorf("курсор пачки 2 = (%v,%d,%d), want (%v,%d,%d) — за пачку 1",
			q, z, i, want1.queuedAt, want1.zone, want1.id)
	}
	// Дренаж 2 — за последнюю строку прошлого дренажа (между дренажами).
	want2 := partial.rows[0]
	if q, z, i := leaseCursorArgs(calls[2]); q != want2.queuedAt || z != want2.zone || i != want2.id {
		t.Errorf("курсор дренажа 2 = (%v,%d,%d), want (%v,%d,%d) — курсор переживает дренажи",
			q, z, i, want2.queuedAt, want2.zone, want2.id)
	}
}

// TestLeaseWatchRestartResetsCursor — рестарт процесса (новый запуск цикла)
// начинает с нулевого курсора: стартовый полный дренаж сохраняется
// (T-012/T-029) — мёртвый префикс за курсором перечитывается осознанно
// (ТЗ §5.1).
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
				t.Fatal("стартовый дренаж не выполнен")
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
		t.Fatalf("первый запуск: стартовый курсор (%v,%d,%d), want нулевой", q, z, i)
	}
	second := run()
	if q, z, i := leaseCursorArgs(second[0]); !q.IsZero() || z != 0 || i != 0 {
		t.Errorf("рестарт: стартовый курсор (%v,%d,%d), want нулевой — полный дренаж старта сохраняется", q, z, i)
	}
}

// TestLeaseWatchCancelDuringDrain — отмена ctx во время дренажа — не
// тревога: без Warn, чистый nil (контракт OnRun, T-012).
func TestLeaseWatchCancelDuringDrain(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := &leaseFakePool{selectErrs: []error{context.Canceled}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // отменён до старта: первый же SELECT дренажа провалится

	done := make(chan error, 1)
	go func() { done <- LeaseWatch(ctx, pool, LeaseOptions{Log: logger}) }()

	if err := <-done; err != nil {
		t.Fatalf("LeaseWatch: %v, want nil при отмене ctx", err)
	}
	if strings.Contains(buf.String(), "level=WARN") {
		t.Errorf("отмена во время дренажа — Warn:\n%s", buf.String())
	}
}

// TestLeaseWatchDrainTotals — Info-итог дренажа: положительная сумма
// логируется (ровно 1 — тоже положительная), нулевая — нет.
func TestLeaseWatchDrainTotals(t *testing.T) {
	tests := []struct {
		name        string
		batches     []leaseBatch
		wantSelects int    // проходов SELECT до остановки стартового дренажа
		want        string // подстрока Info-итога дренажа; "" — итог не логируется
	}{
		{"неполная пачка из одного", []leaseBatch{leaseSeedBatch(1, 1, time.Now())}, 1, "domains=1"},
		{"нулевой дренаж", nil, 1, ""},
		{"полная пачка, всё отсеял guard", []leaseBatch{leaseSeedBatch(DefaultLeaseBatch, 0, time.Now())}, 2, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, nil))
			pool := &leaseFakePool{batches: append([]leaseBatch(nil), tt.batches...)}

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- LeaseWatch(ctx, pool, LeaseOptions{Log: logger}) }()

			// Стартовый дренаж синхронен и от тика не зависит: ждём
			// ожидаемое число проходов, даём завершиться.
			deadline := time.Now().Add(2 * time.Second)
			for {
				if _, selects, _, _ := pool.stats(); selects >= tt.wantSelects {
					break
				}
				if time.Now().After(deadline) {
					cancel()
					t.Fatal("стартовый дренаж не выполнен")
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
				if strings.Contains(log, "дренаж возврата истёкших lease") {
					t.Errorf("нулевой дренаж подводит итог:\n%s", log)
				}
			} else if !strings.Contains(log, tt.want) {
				t.Errorf("лог не содержит %q:\n%s", tt.want, log)
			}
		})
	}
}

func TestLeaseWatchNilPool(t *testing.T) {
	err := LeaseWatch(context.Background(), nil, LeaseOptions{})
	if err == nil || !strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
	}
}

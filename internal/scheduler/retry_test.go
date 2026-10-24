// Юнит-тесты повторных проверок temp_error (T-031, ТЗ 5.3, 6.2) на
// фейковом pgx-пуле с транзакциями: точные SQL-тексты и параметры
// (SELECT кандидатов / UPDATE повтора / UPDATE итогового статуса),
// окно retry_after, границы счётчика попыток (до N / на N / после N),
// пустой набор, охраны, ошибки и отмена ctx, цикл RetryTempErrors.
// Поведение против живой БД — интеграционные тесты (T-032+).
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

	"crawler/internal/model"
)

// --- фейковый пул с транзакциями -----------------------------------------
//
// Имена с префиксом retry: в пакете параллельно появляются файлы других
// задач планировщика (T-027 dispatch*, T-030 recheck*), общих хелперов
// здесь не заводят.

// retryCall — записанный SQL-вызов с аргументами.
type retryCall struct {
	sql  string
	args []any
}

// retryFakeRows — pgx.Rows с заготовленными строками; Scan позиционный,
// в порядке колонок SQL.
type retryFakeRows struct {
	rows   [][]any
	i      int
	closed bool
	err    error // Err() после исчерпания строк
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

// Conn и TypeMap — часть pgx.Rows, не используемая позиционным Scan.
func (r *retryFakeRows) Conn() *pgx.Conn      { return nil }
func (r *retryFakeRows) TypeMap() *pgtype.Map { return nil }

// retryFakePool — RetryDB без БД: очереди заготовленных результатов
// SELECT кандидатов и UPDATE … RETURNING (по шагу), ошибки по шагам
// прохода, протокол вызовов. Всё под замком: цикл RetryTempErrors
// работает в горутине, тест ожидает прогресс по счётчикам.
type retryFakePool struct {
	mu          sync.Mutex
	selects     []*retryFakeRows // очередь результатов SELECT (кончилась — пустой набор)
	requeues    []*retryFakeRows // очередь результатов UPDATE повтора
	finalizes   []*retryFakeRows // очередь результатов UPDATE итогового статуса
	selectErrs  []error          // очередь ошибок SELECT (nil — проход без ошибки)
	requeueErr  error
	finalizeErr error
	beginErr    error
	commitErr   error

	queries []retryCall // SELECT'ы через пул
	begins  int
	txs     []*retryFakeTx
}

func (p *retryFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

func (p *retryFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("Exec не должен вызываться")
}

func (p *retryFakePool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, retryCall{sql: sql, args: args})
	if len(p.selectErrs) > 0 {
		err := p.selectErrs[0]
		p.selectErrs = p.selectErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	if len(p.selects) > 0 {
		rows := p.selects[0]
		p.selects = p.selects[1:]
		return rows, nil
	}
	return retryRows(), nil
}

func (p *retryFakePool) Begin(context.Context) (pgx.Tx, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.begins++
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	tx := &retryFakeTx{pool: p}
	p.txs = append(p.txs, tx)
	return tx, nil
}

// stats — снимок счётчиков под замком (ожидание прогресса в тестах цикла).
type retryStats struct {
	selects int
	begins  int
	commits int
}

func (p *retryFakePool) stats() retryStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := retryStats{selects: len(p.queries), begins: p.begins}
	for _, tx := range p.txs {
		if tx.committed {
			s.commits++
		}
	}
	return s
}

// retryFakeTx — транзакция фейкового пула: помнит вызовы и исход.
type retryFakeTx struct {
	pgx.Tx // остальные методы pgx.Tx проход не использует

	pool       *retryFakePool
	calls      []retryCall
	committed  bool
	rolledBack bool
}

func (t *retryFakeTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.calls = append(t.calls, retryCall{sql: sql, args: args})
	switch sql {
	case retryRequeueSQL:
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
		return nil, fmt.Errorf("retryFakeTx: неожиданный SQL в транзакции: %s", sql)
	}
}

func (t *retryFakeTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.pool.commitErr != nil {
		return t.pool.commitErr
	}
	t.committed = true
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

// compile-time: фейковый пул — RetryDB для тестов без БД.
var _ RetryDB = (*retryFakePool)(nil)

// --- ожидаемые SQL-тексты (контракт) -------------------------------------

const wantRetrySelect = "SELECT d.zone_id, d.id, d.temp_attempts FROM domains d " +
	"WHERE d.status = $1 " +
	"AND d.check_state = $2 " +
	"AND d.last_checked_at < $3 " +
	"ORDER BY d.zone_id, d.id LIMIT $4"

const wantRetryRequeue = "WITH upd AS (UPDATE domains d " +
	"SET check_state = $3, temp_attempts = d.temp_attempts + 1 " +
	"FROM unnest($1::int2[], $2::int8[]) AS k(zone_id, domain_id) " +
	"WHERE d.zone_id = k.zone_id AND d.id = k.domain_id " +
	"AND d.status = $4 AND d.check_state = $5 " +
	"RETURNING d.zone_id, d.id), " +
	"zc AS (UPDATE zones AS z " +
	"SET domains_checked = z.domains_checked - c.n, " +
	"domains_pending = z.domains_pending + c.n " +
	"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c " +
	"WHERE z.id = c.zone_id) " +
	"SELECT u.zone_id, u.id FROM upd AS u"

const wantRetryFinalize = "UPDATE domains d " +
	"SET status = $3, temp_attempts = 0 " +
	"FROM unnest($1::int2[], $2::int8[]) AS k(zone_id, domain_id) " +
	"WHERE d.zone_id = k.zone_id AND d.id = k.domain_id " +
	"AND d.status = $4 AND d.check_state = $5 " +
	"RETURNING d.zone_id, d.id"

// retryCutoffArg — третий аргумент SELECT кандидатов: граница окна.
func retryCutoffArg(t *testing.T, args []any) time.Time {
	t.Helper()
	cutoff, ok := args[2].(time.Time)
	if !ok {
		t.Fatalf("аргумент границы окна = %T, want time.Time", args[2])
	}
	return cutoff
}

// --- RetryOnce ------------------------------------------------------------

func TestRetryOnceEmpty(t *testing.T) {
	pool := &retryFakePool{} // очередь SELECT пуста — кандидатов нет

	res, err := RetryOnce(context.Background(), pool, RetryOptions{})
	if err != nil {
		t.Fatalf("RetryOnce: %v", err)
	}
	if res != (RetryResult{}) {
		t.Errorf("итог = %+v, want нулевой: назначать нечего", res)
	}
	if len(pool.queries) != 1 || pool.queries[0].sql != wantRetrySelect {
		t.Errorf("SELECT = %v, want один запрос кандидатов", pool.queries)
	}
	if pool.begins != 0 {
		t.Errorf("транзакций = %d, want 0: пустой набор не открывает транзакцию", pool.begins)
	}
}

func TestRetryOnceSQLAndFlow(t *testing.T) {
	pool := &retryFakePool{
		selects: []*retryFakeRows{retryRows(
			[]any{int16(1), int64(10), int16(0)}, // попыток 0 < 3 → повтор
			[]any{int16(1), int64(11), int16(2)}, // 2 < 3 → повтор
			[]any{int16(2), int64(20), int16(2)}, // 2 < 3 → повтор
			[]any{int16(2), int64(21), int16(3)}, // 3 = N → итоговый статус
			[]any{int16(3), int64(30), int16(4)}, // 4 > N → итоговый статус
		)},
		requeues: []*retryFakeRows{retryRows(
			// Охрана отбила домен 1/11 (успел уйти из done) — не считается.
			[]any{int16(1), int64(10)},
			[]any{int16(2), int64(20)},
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

	// Выборка кандидатов: статус temp_error, состояние done, лимит пачки.
	if len(pool.queries) != 1 {
		t.Fatalf("SELECT'ов = %d, want 1", len(pool.queries))
	}
	if pool.queries[0].sql != wantRetrySelect {
		t.Errorf("SQL выборки:\n%s\nwant:\n%s", pool.queries[0].sql, wantRetrySelect)
	}
	gotStatus, gotState, gotLimit := pool.queries[0].args[0], pool.queries[0].args[1], pool.queries[0].args[3]
	if gotStatus != int16(model.StatusTempError) || gotState != int16(model.CheckStateDone) {
		t.Errorf("фильтры выборки = статус %v, состояние %v, want temp_error (%d) и done (%d)",
			gotStatus, gotState, model.StatusTempError, model.CheckStateDone)
	}
	if gotLimit != 10 {
		t.Errorf("лимит пачки = %v, want 10", gotLimit)
	}

	// Транзакция: назначение повтора + итоговый статус.
	if pool.begins != 1 || len(pool.txs) != 1 {
		t.Fatalf("транзакций = %d, want 1", pool.begins)
	}
	tx := pool.txs[0]
	if len(tx.calls) != 2 {
		t.Fatalf("вызовов в транзакции = %d, want 2 (повтор + итоговый статус)", len(tx.calls))
	}
	if tx.calls[0].sql != wantRetryRequeue {
		t.Errorf("SQL повтора:\n%s\nwant:\n%s", tx.calls[0].sql, wantRetryRequeue)
	}
	if want := []any{
		[]int16{1, 1, 2}, []int64{10, 11, 20},
		int16(model.CheckStatePending), int16(model.StatusTempError), int16(model.CheckStateDone),
	}; !reflect.DeepEqual(tx.calls[0].args, want) {
		t.Errorf("аргументы повтора = %v, want %v", tx.calls[0].args, want)
	}
	if tx.calls[1].sql != wantRetryFinalize {
		t.Errorf("SQL итогового статуса:\n%s\nwant:\n%s", tx.calls[1].sql, wantRetryFinalize)
	}
	if want := []any{
		[]int16{2, 3}, []int64{21, 30},
		int16(model.StatusDelegatedNoAddress), int16(model.StatusTempError), int16(model.CheckStateDone),
	}; !reflect.DeepEqual(tx.calls[1].args, want) {
		t.Errorf("аргументы итогового статуса = %v, want %v", tx.calls[1].args, want)
	}

	// Счётчики — по фактически затронутым строкам (охрана отбила 1/11).
	if want := (RetryResult{Requeued: 2, Finalized: 2}); res != want {
		t.Errorf("итог = %+v, want %+v", res, want)
	}
	if !tx.committed || tx.rolledBack {
		t.Errorf("транзакция: committed=%v rolledBack=%v, want фиксация без отката", tx.committed, tx.rolledBack)
	}
}

// TestRetryOnceCutoff — граница окна: cutoff = момент прохода − RetryAfter
// (по умолчанию 24 ч из ТЗ 5.3); сравнение строгое — «старше».
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
			cutoff := retryCutoffArg(t, pool.queries[0].args)
			want := before.Add(-tt.wantWindow)
			// Момент прохода не детерминирован — сверяем с допуском.
			if cutoff.Before(want.Add(-time.Minute)) || cutoff.After(time.Now().Add(-tt.wantWindow)) {
				t.Errorf("граница окна = %s, want ≈ now − %s", cutoff, tt.wantWindow)
			}
		})
	}
}

// TestRetryOnceAttemptBoundaries — деление пачки по счётчику: строго
// меньше N — повтор, N и больше — итоговый статус (ТЗ 5.3: «не более N
// раз подряд»); дефолт N = 3.
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
					[]any{int16(7), int64(70)})},
				finalizes: []*retryFakeRows{retryRows(
					[]any{int16(7), int64(70)})},
			}

			res, err := RetryOnce(context.Background(), pool, tt.opts)
			if err != nil {
				t.Fatalf("RetryOnce: %v", err)
			}
			tx := pool.txs[0]
			if tt.wantRequeue {
				if res != (RetryResult{Requeued: 1}) {
					t.Errorf("итог = %+v, want повтор одной проверки", res)
				}
				if len(tx.calls) != 1 || tx.calls[0].sql != wantRetryRequeue {
					t.Errorf("вызовы = %d, want только назначение повтора", len(tx.calls))
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

// TestRetryOnceDefaultBatch — нулевой размер пачки заменяется дефолтом.
func TestRetryOnceDefaultBatch(t *testing.T) {
	pool := &retryFakePool{}

	if _, err := RetryOnce(context.Background(), pool, RetryOptions{}); err != nil {
		t.Fatalf("RetryOnce: %v", err)
	}
	if got := pool.queries[0].args[3]; got != DefaultRetryBatch {
		t.Errorf("лимит пачки = %v, want дефолт %d", got, DefaultRetryBatch)
	}
}

// TestRetryOnceRepeatPasses — назначенный повтор переводит домен в
// pending: повторный проход его не выбирает и транзакцию не открывает.
func TestRetryOnceRepeatPasses(t *testing.T) {
	pool := &retryFakePool{
		selects:  []*retryFakeRows{retryRows([]any{int16(3), int64(30), int16(0)})},
		requeues: []*retryFakeRows{retryRows([]any{int16(3), int64(30)})},
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
	if len(pool.queries) != 2 {
		t.Errorf("SELECT'ов = %d, want 2 (по одному на проход)", len(pool.queries))
	}
	if pool.begins != 1 {
		t.Errorf("транзакций = %d, want 1: повторный проход не открывает", pool.begins)
	}
}

// TestRetryOnceGuardedAway — охрана UPDATE отбила всех кандидатов
// (домены ушли из done мимо прохода): счётчики нулевые, транзакция
// фиксируется без эффекта.
func TestRetryOnceGuardedAway(t *testing.T) {
	pool := &retryFakePool{
		selects:  []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(0)})},
		requeues: []*retryFakeRows{retryRows()}, // RETURNING пуст
	}

	res, err := RetryOnce(context.Background(), pool, RetryOptions{})
	if err != nil {
		t.Fatalf("RetryOnce: %v", err)
	}
	if res != (RetryResult{}) {
		t.Errorf("итог = %+v, want нулевой: охрана отбила кандидата", res)
	}
	if !pool.txs[0].committed {
		t.Error("транзакция не зафиксирована: пустой RETURNING — не ошибка")
	}
}

func TestRetryOnceErrors(t *testing.T) {
	newPool := func() *retryFakePool {
		return &retryFakePool{
			selects:  []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(0)})},
			requeues: []*retryFakeRows{retryRows([]any{int16(1), int64(10)})},
		}
	}

	tests := []struct {
		name    string
		pool    *retryFakePool
		context string // ожидаемый фрагмент сообщения
	}{
		{"ошибка SELECT", &retryFakePool{selectErrs: []error{errors.New("бд недоступна")}}, "выборка кандидатов"},
		{"ошибка Begin", func() *retryFakePool {
			p := newPool()
			p.beginErr = errors.New("бд недоступна")
			return p
		}(), "транзакция"},
		{"ошибка повтора", func() *retryFakePool {
			p := newPool()
			p.requeueErr = errors.New("бд недоступна")
			return p
		}(), "назначение повтора"},
		{"ошибка итога", func() *retryFakePool {
			// Кандидат с исчерпанными попытками — выполняется ветка
			// итогового статуса.
			p := newPool()
			p.selects = []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(DefaultRetryMaxRetries)})}
			p.finalizeErr = errors.New("бд недоступна")
			return p
		}(), "итоговый статус"},
		{"ошибка Commit", func() *retryFakePool {
			p := newPool()
			p.commitErr = errors.New("бд недоступна")
			return p
		}(), "фиксация"},
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
			for _, tx := range tt.pool.txs {
				if tx.committed {
					t.Errorf("транзакция закоммичена при ошибке %q", tt.name)
				}
				// Отложенный откат обязан сработать: прерванный проход
				// не оставляет половинчатых изменений (ТЗ 3.4).
				if !tx.rolledBack {
					t.Errorf("транзакция не откачена при ошибке %q", tt.name)
				}
			}
		})
	}
}

// TestRetryOnceCtxCanceled — отмена контекста проходит через шаги прохода
// как ошибка (errors.Is), транзакция не фиксируется.
func TestRetryOnceCtxCanceled(t *testing.T) {
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
	if requeuePool.txs[0].committed {
		t.Error("транзакция закоммичена при отмене контекста")
	}
}

// TestRetryOnceRowsErrors — ошибки чтения строк на обоих шагах прохода:
// Scan кандидатов и ключей RETURNING (лишняя колонка ломает позиционный
// Scan), rows.Err после исчерпания строк — точные контексты сообщений,
// исходная ошибка проксируется (errors.Is).
func TestRetryOnceRowsErrors(t *testing.T) {
	// Scan кандидатов: вторая строка с двумя колонками вместо трёх.
	scanPool := &retryFakePool{selects: []*retryFakeRows{retryRows(
		[]any{int16(1), int64(10), int16(0)},
		[]any{int16(2), int64(20)},
	)}}
	if _, err := RetryOnce(context.Background(), scanPool, RetryOptions{}); err == nil ||
		!strings.Contains(err.Error(), "чтение кандидатов") {
		t.Errorf("ошибка Scan кандидатов = %v, want «чтение кандидатов»", err)
	}

	// rows.Err после чтения кандидатов.
	boom := errors.New("бд недоступна")
	candRows := retryRows([]any{int16(1), int64(10), int16(0)})
	candRows.err = boom
	candPool := &retryFakePool{selects: []*retryFakeRows{candRows}}
	_, err := RetryOnce(context.Background(), candPool, RetryOptions{})
	if err == nil || !strings.Contains(err.Error(), "выборка кандидатов") || !errors.Is(err, boom) {
		t.Errorf("ошибка rows.Err кандидатов = %v, want «выборка кандидатов» и исходная", err)
	}

	// Scan ключей RETURNING: вторая строка с одной колонкой вместо двух.
	keyScanPool := &retryFakePool{
		selects:  []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(0)})},
		requeues: []*retryFakeRows{retryRows([]any{int16(1), int64(10)}, []any{int16(2)})},
	}
	if _, err := RetryOnce(context.Background(), keyScanPool, RetryOptions{}); err == nil ||
		!strings.Contains(err.Error(), "чтение ключей") {
		t.Errorf("ошибка Scan ключей = %v, want «чтение ключей»", err)
	}

	// rows.Err после чтения ключей RETURNING.
	keyRows := retryRows([]any{int16(1), int64(10)})
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

// TestRetryOnceNilPool — пул обязан быть задан.
func TestRetryOnceNilPool(t *testing.T) {
	if _, err := RetryOnce(context.Background(), nil, RetryOptions{}); err == nil ||
		!strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
	}
}

// --- RetryOptions ----------------------------------------------------------

func TestRetryOptionsDefaults(t *testing.T) {
	// Дефолты пинятся литералами ТЗ 5.3: окно повтора 24 часа, пачка
	// 1000 — сравнение с самой константой мутации не замечает.
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
		// Интервал не длиннее окна повтора: 24 ч > 1 ч → час.
		Interval: DefaultRetryInterval,
	}
	if got != want {
		t.Errorf("withDefaults = %+v, want %+v", got, want)
	}

	// Короткое окно ужимает интервал: цикл не переспит свой retry_after.
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

	// Границы замен: 1 нс — положительное значение, дефолтом не
	// заменяется (окно и интервал — независимо друг от друга).
	if got := (RetryOptions{RetryAfter: time.Nanosecond}).withDefaults().RetryAfter; got != time.Nanosecond {
		t.Errorf("RetryAfter 1нс = %v, want без замены", got)
	}
	if got := (RetryOptions{Interval: time.Nanosecond}).withDefaults().Interval; got != time.Nanosecond {
		t.Errorf("Interval 1нс = %v, want без замены", got)
	}
}

// --- RetryTempErrors --------------------------------------------------------

// TestRetryTempErrorsLoop — первый проход сразу, ошибка прохода не
// останавливает цикл, отмена ctx возвращает nil (контракт OnRun, T-012).
func TestRetryTempErrorsLoop(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	pool := &retryFakePool{
		// Проход 1 (сразу после старта) — работа; проход 2 — ошибка БД;
		// дальше — пустые наборы.
		selects: []*retryFakeRows{
			retryRows([]any{int16(1), int64(10), int16(0)}),
			retryRows(),
		},
		selectErrs: []error{nil, errors.New("бд недоступна")},
		requeues: []*retryFakeRows{retryRows(
			[]any{int16(1), int64(10)},
		)},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RetryTempErrors(ctx, pool, RetryOptions{
			Interval: 2 * time.Millisecond,
			Log:      logger,
		})
	}()

	// Ждём трёх проходов (сразу + минимум два тика) и останавливаем.
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
	if st.begins != 1 || st.commits != 1 {
		t.Errorf("транзакций %d, коммитов %d, want 1 и 1: ошибочный и пустые проходы не открывают", st.begins, st.commits)
	}

	log := buf.String()
	for _, want := range []string{
		"фоновые повторные проверки temp_error", // старт
		"дренаж повторных проверок temp_error",  // итог первого дренажа
		"requeued=1",    // счётчики прохода
		"бд недоступна", // ошибка второго прохода
	} {
		if !strings.Contains(log, want) {
			t.Errorf("лог не содержит %q:\n%s", want, log)
		}
	}
}

// TestRetryTempErrorsDrain — созревшие кандидаты разбираются одним
// дренажом до исчерпания (полная пачка — следующий проход сразу, без
// ожидания тика), а не по пачке за интервал (S-03).
func TestRetryTempErrorsDrain(t *testing.T) {
	pool := &retryFakePool{
		// Две полные пачки по 1 домену, затем исчерпание.
		selects: []*retryFakeRows{
			retryRows([]any{int16(1), int64(10), int16(0)}),
			retryRows([]any{int16(2), int64(20), int16(0)}),
			retryRows(),
		},
		requeues: []*retryFakeRows{
			retryRows([]any{int16(1), int64(10)}),
			retryRows([]any{int16(2), int64(20)}),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- RetryTempErrors(ctx, pool, RetryOptions{
			BatchSize: 1,
			Interval:  time.Hour, // тик не наступит: всё должен разобрать дренаж
			Log:       slog.New(slog.DiscardHandler),
		})
	}()

	// Три SELECT (две полные пачки + пустая) обязаны пройти без тика.
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
	if st.begins != 2 || st.commits != 2 {
		t.Errorf("транзакций %d, коммитов %d, want 2 и 2 (обе пачки в одном дренаже)", st.begins, st.commits)
	}
}

// TestRetryTempErrorsDrainTotals — дренаж суммирует результаты пачек:
// Info-итог — накопленная сумма всех пачек (включая ровно 1), нулевой
// дренаж итога не логирует. Дренаж выполняется при уже отменённом ctx:
// фейк ошибок не возвращает, отмена не мешает проходам — итог обязан
// быть подведён.
func TestRetryTempErrorsDrainTotals(t *testing.T) {
	tests := []struct {
		name      string
		selects   []*retryFakeRows
		requeues  []*retryFakeRows
		finalizes []*retryFakeRows
		opts      RetryOptions
		want      string // подстрока Info-итога дренажа; "" — итог не логируется
	}{
		{
			name: "две полные пачки повторов — сумма 2",
			selects: []*retryFakeRows{
				retryRows([]any{int16(1), int64(10), int16(0)}),
				retryRows([]any{int16(2), int64(20), int16(0)}),
			},
			requeues: []*retryFakeRows{
				retryRows([]any{int16(1), int64(10)}),
				retryRows([]any{int16(2), int64(20)}),
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
			cancel() // стартовый дренаж синхронен и от тика не зависит
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

// TestRetryTempErrorsCancelDuringDrain — отмена ctx во время дренажа —
// часть остановки: без Warn и без Info-итога (незавершённый дренаж не
// подводит черту), чистый nil (контракт OnRun, T-012).
func TestRetryTempErrorsCancelDuringDrain(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	// Пачка 1 — полная (продолжение дренажа), пачка 2 — отмена ctx.
	pool := &retryFakePool{
		selects: []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(0)})},
		requeues: []*retryFakeRows{
			retryRows([]any{int16(1), int64(10)}),
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

// TestRetryTempErrorsNilLog — nil-логгер заменяется заглушкой: стартовая
// запись и записи цикла уходят в slog.DiscardHandler, не паникуя на
// nil-приёмнике.
func TestRetryTempErrorsNilLog(t *testing.T) {
	pool := &retryFakePool{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // стартовый дренаж пуст, сразу чистый выход

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

func TestRetryTempErrorsNilPool(t *testing.T) {
	err := RetryTempErrors(context.Background(), nil, RetryOptions{})
	if err == nil || !strings.Contains(err.Error(), "пул не задан") {
		t.Errorf("ошибка = %v, want «пул не задан»", err)
	}
}

// Юнит-тесты повторных проверок temp_error (T-031, ТЗ 5.3, 6.2; guard
// корзинами зрелости — T-202, закрепление плана выборки — T-191) на
// фейковом pgx-пуле с транзакциями: точные SQL-тексты и параметры
// (guard-чтение суммы корзин / SELECT кандидатов в транзакции
// закрепления плана / UPDATE повтора / UPDATE итогового статуса с
// декрементом корзины), последовательность SET LOCAL ×3 → SELECT, окно
// retry_after, границы счётчика попыток (до N / на N / после N),
// пустой набор, guard=0 без тяжёлой выборки, охраны, ошибки и отмена
// ctx, цикл RetryTempErrors. Поведение против живой БД —
// интеграционные тесты (test/integration/retry_test.go).
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

// retryFakePool — RetryDB без БД: guard-чтение суммы корзин зрелости
// (T-202) через пул, очереди заготовленных результатов SELECT кандидатов
// (в транзакции закрепления плана) и UPDATE … RETURNING (в транзакции
// обновления), ошибки по шагам прохода, протокол вызовов. Всё под
// замком: цикл RetryTempErrors работает в горутине, тест ожидает
// прогресс по счётчикам.
type retryFakePool struct {
	mu          sync.Mutex
	guardVals   []int64          // FIFO значений суммы корзин guard; исчерпана — держится последнее, не задана вовсе — 1 (зрелые есть: проход идёт к выборке)
	guardEmpty  bool             // guard возвращает пустой набор — обрыв протокола (SQL отвечает строкой всегда)
	guardErrs   []error          // очередь ошибок guard-чтения (nil — чтение без ошибки)
	selects     []*retryFakeRows // очередь результатов SELECT кандидатов (кончилась — пустой набор)
	requeues    []*retryFakeRows // очередь результатов UPDATE повтора
	finalizes   []*retryFakeRows // очередь результатов UPDATE итогового статуса
	selectErrs  []error          // очередь ошибок SELECT кандидатов (nil — выборка без ошибки)
	planErr     error            // ошибка закрепления плана (SET LOCAL)
	requeueErr  error
	finalizeErr error
	beginErr    error
	commitErrs  []error // FIFO ошибок Commit (кончилась — фиксация без ошибки)

	guardQueries []retryCall // guard-чтения через пул
	selectCalls  int         // тяжёлые SELECT кандидатов (в транзакциях закрепления)
	begins       int
	txs          []*retryFakeTx
}

func (p *retryFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

func (p *retryFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("Exec не должен вызываться")
}

// nextGuardLocked — значение суммы корзин для очередного guard-чтения.
func (p *retryFakePool) nextGuardLocked() int64 {
	if len(p.guardVals) == 0 {
		return 1 // дефолт фейка: зрелые есть, проход идёт к выборке
	}
	v := p.guardVals[0]
	if len(p.guardVals) > 1 {
		p.guardVals = p.guardVals[1:]
	}
	return v
}

// Query — guard-чтение суммы корзин зрелости (T-202): единственный
// SELECT прохода мимо транзакций; тяжёлая выборка кандидатов идёт в
// транзакции закрепления плана (retryFakeTx.Query).
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
		return retryRows(), nil // пустой набор — обрыв протокола (coalesce отвечает строкой)
	}
	return retryRows([]any{p.nextGuardLocked()}), nil
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
	guards  int // guard-чтений счётчика
	selects int // тяжёлых SELECT кандидатов
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

// retryFakeTx — транзакция фейкового пула: помнит вызовы и исход.
type retryFakeTx struct {
	pgx.Tx // остальные методы pgx.Tx проход не использует

	pool       *retryFakePool
	calls      []retryCall
	committed  bool
	rolledBack bool
}

// isSelect — транзакция выборки кандидатов (закрепление плана), а не
// обновления: ошибки прохода не обязаны её откатывать (SELECT читает
// зафиксированные данные).
func (t *retryFakeTx) isSelect() bool {
	for _, c := range t.calls {
		if c.sql == retrySelectSQL {
			return true
		}
	}
	return false
}

// Exec — закрепление плана выборки (T-191): SET LOCAL ×3 до SELECT —
// последовательность проверяет TestRetryOnceSelectPlan.
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
	"WHERE z.id = c.zone_id), " +
	"rm AS (INSERT INTO crawler_counters (name, value, updated_at) " +
	"SELECT 'retry_mature:' || to_char(u.last_checked_at, 'YYYY-MM-DD'), -count(*), now() " +
	"FROM upd AS u GROUP BY u.last_checked_at " +
	"ORDER BY u.last_checked_at " +
	"ON CONFLICT (name) DO UPDATE " +
	"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()) " +
	"SELECT u.zone_id, u.id FROM upd AS u"

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

// retryCutoffArg — третий аргумент SELECT кандидатов: граница окна.
func retryCutoffArg(t *testing.T, args []any) time.Time {
	t.Helper()
	cutoff, ok := args[2].(time.Time)
	if !ok {
		t.Fatalf("аргумент границы окна = %T, want time.Time", args[2])
	}
	return cutoff
}

// retrySelectCall — вызов тяжёлой выборки из транзакции (после SET LOCAL ×3).
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

// --- контракт SQL (T-191/T-202) ----------------------------------------------

// TestRetrySQLContract — литералы операторов прохода: выборка без ORDER BY
// (порядок кандидатов не важен — сортировка держала бы индексный доступ),
// SET LOCAL закрепления плана, guard суммой корзин зрелости с границей
// current_date от БД; декремент корзин (CTE rm) — в обоих решениях пачки,
// ключ last_checked_at — тот же, что у инкремента записи результата.
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
	// Guard корзин зрелости (T-202): сумма по диапазону PK-префикса,
	// строгое < отсекает корзину текущих суток (сегодняшние эпизоды
	// незрелые), coalesce — честный ноль при отсутствии строк, имя границы
	// вычисляет БД (таймзона окна FILTER реконсиляции, стиль T-179).
	wantGuard := "SELECT coalesce(sum(value), 0) FROM crawler_counters " +
		"WHERE name >= 'retry_mature:' " +
		"AND name < 'retry_mature:' || to_char(current_date, 'YYYY-MM-DD')"
	if retryMatureSQL != wantGuard {
		t.Fatalf("retryMatureSQL разошёлся с контрактом T-202:\n%s\nwant:\n%s", retryMatureSQL, wantGuard)
	}
	// Декремент корзины зрелости — в назначении повтора и финализации
	// (CTE rm, T-202): оба решения завершают кандидату; ключ выводится из
	// last_checked_at переведённых строк — адресат того же инкремента,
	// что пишут записи результата (current_date их транзакции). ORDER BY
	// детерминирует захват строк корзин по возрастанию даты — общий
	// порядок с проходом реконсиляции (очистка старых → вчерашняя →
	// сегодняшняя), без AB-BA цикла на смешанных датах пачки (ревью
	// T-202, P2).
	const wantBasketCTE = ", " +
		"rm AS (INSERT INTO crawler_counters (name, value, updated_at) " +
		"SELECT 'retry_mature:' || to_char(u.last_checked_at, 'YYYY-MM-DD'), -count(*), now() " +
		"FROM upd AS u GROUP BY u.last_checked_at " +
		"ORDER BY u.last_checked_at " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()) "
	for _, sql := range []struct {
		name string
		text string
	}{
		{"retryRequeueSQL", retryRequeueSQL},
		{"retryFinalizeSQL", retryFinalizeSQL},
	} {
		if !strings.Contains(sql.text, "RETURNING d.zone_id, d.id, d.last_checked_at") ||
			!strings.Contains(sql.text, wantBasketCTE) {
			t.Errorf("%s без декремента корзины зрелости (CTE rm, T-202):\n%s", sql.name, sql.text)
		}
	}
}

// --- RetryOnce ------------------------------------------------------------

// TestRetryOnceGuardZero — guard корзинами зрелости (T-202): при нулевой
// (и отрицательной — видимый дрейф декрементов, реконсиляция выровняет)
// сумме тяжёлая выборка кандидатов не выполняется вовсе и транзакция не
// открывается — стационар без зрелых стоит одну сумму индексного
// диапазона.
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

// TestRetryOnceGuardEmpty — пустой ответ guard невозможен у живой БД
// (coalesce в SQL отвечает строкой всегда): обрыв протокола — ошибка
// прохода, а не «зрелых нет».
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

// TestRetryOnceEmpty — корзины видят зрелых, выборка пуста (дрейф корзин
// или рассеянные кандидаты): пустой итог, транзакция обновления не
// открывается.
func TestRetryOnceEmpty(t *testing.T) {
	pool := &retryFakePool{} // guard по умолчанию 1, очередь SELECT пуста

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

	// Guard: чтение суммы корзин зрелости через пул, без параметров.
	if len(pool.guardQueries) != 1 {
		t.Fatalf("guard-чтений = %d, want 1", len(pool.guardQueries))
	}
	if pool.guardQueries[0].sql != retryMatureSQL || len(pool.guardQueries[0].args) != 0 {
		t.Errorf("guard-чтение = %+v, want %s без параметров", pool.guardQueries[0], retryMatureSQL)
	}

	// Две транзакции: выборка (закрепление плана + SELECT) и обновление.
	if pool.begins != 2 || len(pool.txs) != 2 {
		t.Fatalf("транзакций = %d, want 2 (выборка + обновление)", pool.begins)
	}
	selTx, updTx := pool.txs[0], pool.txs[1]

	// Выборка кандидатов: статус temp_error, состояние done, лимит пачки.
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

	// Обновление: назначение повтора + итоговый статус.
	if len(updTx.calls) != 2 {
		t.Fatalf("вызовов в транзакции обновления = %d, want 2 (повтор + итоговый статус)", len(updTx.calls))
	}
	if updTx.calls[0].sql != wantRetryRequeue {
		t.Errorf("SQL повтора:\n%s\nwant:\n%s", updTx.calls[0].sql, wantRetryRequeue)
	}
	if want := []any{
		[]int16{1, 1, 2}, []int64{10, 11, 20},
		int16(model.CheckStatePending), int16(model.StatusTempError), int16(model.CheckStateDone),
	}; !reflect.DeepEqual(updTx.calls[0].args, want) {
		t.Errorf("аргументы повтора = %v, want %v", updTx.calls[0].args, want)
	}
	if updTx.calls[1].sql != wantRetryFinalize {
		t.Errorf("SQL итогового статуса:\n%s\nwant:\n%s", updTx.calls[1].sql, wantRetryFinalize)
	}
	if want := []any{
		[]int16{2, 3}, []int64{21, 30},
		int16(model.StatusDelegatedNoAddress), int16(model.StatusTempError), int16(model.CheckStateDone),
	}; !reflect.DeepEqual(updTx.calls[1].args, want) {
		t.Errorf("аргументы итогового статуса = %v, want %v", updTx.calls[1].args, want)
	}

	// Счётчики — по фактически затронутым строкам (охрана отбила 1/11).
	if want := (RetryResult{Requeued: 2, Finalized: 2}); res != want {
		t.Errorf("итог = %+v, want %+v", res, want)
	}
	for _, tx := range []*retryFakeTx{selTx, updTx} {
		if !tx.committed || tx.rolledBack {
			t.Errorf("транзакция %p: committed=%v rolledBack=%v, want фиксация без отката", tx, tx.committed, tx.rolledBack)
		}
	}
}

// TestRetryOnceSelectPlan — закрепление формы плана выборки (T-191): пачка
// читается в явной транзакции строго последовательностью SET LOCAL ×3 →
// SELECT (реплика диаграммы §4.2 ТЗ; без закрепления планировщик берёт
// частичный domains_recheck_idx — запрос №1 журнала медленных 25.10);
// выборка идёт через транзакцию, не пул.
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
			sel := retrySelectCall(t, pool.txs[0])
			cutoff := retryCutoffArg(t, sel.args)
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
			tx := pool.txs[1] // транзакция обновления — вторая
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
	if got := retrySelectCall(t, pool.txs[0]).args[3]; got != DefaultRetryBatch {
		t.Errorf("лимит пачки = %v, want дефолт %d", got, DefaultRetryBatch)
	}
}

// TestRetryOnceRepeatPasses — назначенный повтор переводит домен в
// pending: повторный проход его не выбирает и транзакцию обновления не
// открывает.
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
	if st := pool.stats(); st.selects != 2 {
		t.Errorf("SELECT'ов = %d, want 2 (по одному на проход)", st.selects)
	}
	// Первый проход: транзакции выборки и обновления; второй — только
	// выборка (пустая): переведённые в pending не кандидаты.
	if st := pool.stats(); st.begins != 3 {
		t.Errorf("транзакций = %d, want 3: повторный проход не открывает обновление", st.begins)
	}
}

// TestRetryOnceGuardedAway — охрана UPDATE отбила всех кандидатов
// (домены ушли из done мимо прохода): счётчики нулевые, транзакция
// обновления фиксируется без эффекта.
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
	if !pool.txs[1].committed {
		t.Error("транзакция обновления не зафиксирована: пустой RETURNING — не ошибка")
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
		{"ошибка итога", func() *retryFakePool {
			// Кандидат с исчерпанными попытками — выполняется ветка
			// итогового статуса.
			p := newPool()
			p.selects = []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(DefaultRetryMaxRetries)})}
			p.finalizeErr = errors.New("бд недоступна")
			return p
		}(), "итоговый статус"},
		{"ошибка Commit выборки", func() *retryFakePool {
			p := newPool()
			p.commitErrs = []error{errors.New("бд недоступна")}
			return p
		}(), "фиксация выборки"},
		{"ошибка Commit обновления", func() *retryFakePool {
			p := newPool()
			p.commitErrs = []error{nil, errors.New("бд недоступна")}
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
				if tx.committed && !tx.isSelect() {
					t.Errorf("транзакция обновления закоммичена при ошибке %q", tt.name)
					continue
				}
				// Отложенный откат обязан сработать: прерванный проход не
				// оставляет половинчатых изменений (ТЗ 3.4). Транзакция
				// выборки — только оболочка закрепления плана (SELECT по
				// зафиксированным данным), её фиксация до ошибки — не
				// половинчатость.
				if !tx.committed && !tx.rolledBack {
					t.Errorf("транзакция не откачена при ошибке %q", tt.name)
				}
			}
		})
	}
}

// TestRetryOnceCtxCanceled — отмена контекста проходит через шаги прохода
// как ошибка (errors.Is), транзакция обновления не фиксируется.
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
			// 10 мс: окно между наблюдением третьей выборки (поллинг
			// 1 мс) и cancel заведомо меньше тика — четвёртый проход
			// сценария успевает лишь изредка (см. ассерты ниже).
			Interval: 10 * time.Millisecond,
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
	// Транзакции: рабочий проход — выборка + обновление; ошибочный и
	// пустой — только выборка (обновление не открывается); ошибочная
	// выборка не фиксируется. Гонка отмены: между наблюдением третьей
	// выборки и cancel тик мог успеть начать (и, реже, завершить)
	// четвёртый проход циклического сценария — рабочий: +1 begin и
	// возможно +1 commit; прерванный на середине — +1 begin без commit.
	// Дисциплина проходов во всех исходах одна: commits ≤ begins,
	// разброс — ровно на этот лишний проход.
	if st.begins < 4 || st.begins > 5 {
		t.Errorf("транзакций %d, want 4–5 (гонка тика с отменой)", st.begins)
	}
	if st.commits < 3 || st.commits > 4 || st.commits > st.begins {
		t.Errorf("коммитов %d при транзакциях %d, want 3–4 и не больше begins",
			st.commits, st.begins)
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
	// Обе пачки в одном дренаже: три выборки + два обновления, все
	// зафиксированы.
	if st.begins != 5 || st.commits != 5 {
		t.Errorf("транзакций %d, коммитов %d, want 5 и 5", st.begins, st.commits)
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

// TestRetryTempErrorsGuardDrainStop — guard останавливает дренаж без
// тяжёлой выборки: сумма корзин 0 после исчерпания когорты (декременты
// занулили) — следующий проход дренажа читает только сумму корзин.
func TestRetryTempErrorsGuardDrainStop(t *testing.T) {
	pool := &retryFakePool{
		// Пачка 1 — полная; дальше сумма корзин 0 (guardVals: липкое
		// последнее значение — вторым элементом фиксируется 0).
		guardVals: []int64{1, 0},
		selects:   []*retryFakeRows{retryRows([]any{int16(1), int64(10), int16(0)})},
		requeues:  []*retryFakeRows{retryRows([]any{int16(1), int64(10)})},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // стартовый дренаж синхронен
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

// Юнит-тесты коллектора глубины backlog (T-077, ТЗ 5.9) на фейковом
// pgx-пуле: точный SQL-текст и параметр (исключение deleted), экспорт
// обеих серий gauge через Gather (включая нулевые), дефолты, ошибки,
// nil-зависимости, цикл BacklogLoop (немедленный первый проход,
// переживание ошибки, чистый выход по отмене ctx). Поведение против
// живой БД — интеграционные тесты.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/metrics"
	"crawler/internal/model"
)

// --- фейковый пул ---------------------------------------------------------

// backlogCall — записанный SQL-вызов с аргументами.
type backlogCall struct {
	sql  string
	args []any
}

// backlogFakeRows — pgx.Rows с заготовленными строками; Scan позиционный,
// в порядке колонок SQL (check_state, count).
type backlogFakeRows struct {
	rows   [][]any
	i      int
	closed bool
	err    error // Err() после исчерпания строк
}

func backlogRows(rows ...[]any) *backlogFakeRows {
	return &backlogFakeRows{rows: rows}
}

func (r *backlogFakeRows) Close() { r.closed = true }

func (r *backlogFakeRows) Err() error { return r.err }

func (r *backlogFakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *backlogFakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *backlogFakeRows) Next() bool {
	if r.closed || r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *backlogFakeRows) Scan(dest ...any) error {
	if r.closed || r.i == 0 || r.i > len(r.rows) {
		return errors.New("backlogFakeRows: Scan без Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("backlogFakeRows: приёмников %d, колонок %d", len(dest), len(row))
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

func (r *backlogFakeRows) Values() ([]any, error) { return r.rows[r.i-1], nil }

func (r *backlogFakeRows) RawValues() [][]byte { return nil }

// Conn и TypeMap — часть pgx.Rows, не используемая позиционным Scan.
func (r *backlogFakeRows) Conn() *pgx.Conn      { return nil }
func (r *backlogFakeRows) TypeMap() *pgtype.Map { return nil }

// backlogFakePool — db.DB без БД: ответ Query задаётся hook (пул
// используется циклом в горутине, тест меняет поведение проходов).
type backlogFakePool struct {
	mu    sync.Mutex
	hook  func() (pgx.Rows, error)
	calls []backlogCall
}

func (p *backlogFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

func (p *backlogFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("Exec не должен вызываться")
}

func (p *backlogFakePool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.mu.Lock()
	p.calls = append(p.calls, backlogCall{sql: sql, args: args})
	hook := p.hook
	p.mu.Unlock()
	return hook()
}

// setHook меняет поведение ответов (тест подменяет сценарий на живом
// цикле; доступ под замком — Query работает в горутине цикла).
func (p *backlogFakePool) setHook(f func() (pgx.Rows, error)) {
	p.mu.Lock()
	p.hook = f
	p.mu.Unlock()
}

// --- ожидаемый SQL-текст (контракт) ---------------------------------------

const wantBacklogSQL = "SELECT check_state, count(*) FROM domains " +
	"WHERE status <> $1 GROUP BY check_state"

// backlogGaugeValue — текущее значение серии crawler_backlog_domains.
func backlogGaugeValue(t *testing.T, reg *metrics.Registry, state string) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "crawler_backlog_domains" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				// Имя метки — state (docs/services.md, ТЗ 5.9): не type.
				if l.GetName() == metrics.LabelState && l.GetValue() == state {
					return m.GetGauge().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

// --- BacklogOnce -----------------------------------------------------------

func TestBacklogOnce(t *testing.T) {
	reg := metrics.New("scheduler")
	pool := &backlogFakePool{hook: func() (pgx.Rows, error) {
		// done в агрегате игнорируется: прогресс — счётчики зон T-078.
		return backlogRows(
			[]any{int16(model.CheckStatePending), int64(10)},
			[]any{int16(model.CheckStateQueued), int64(7)},
			[]any{int16(model.CheckStateDone), int64(350)},
		), nil
	}}

	res, err := BacklogOnce(context.Background(), pool, NewBacklogGauge(reg))
	if err != nil {
		t.Fatalf("BacklogOnce: %v", err)
	}
	if res.Pending != 10 || res.Queued != 7 {
		t.Errorf("итог = %+v, want pending 10, queued 7", res)
	}

	if len(pool.calls) != 1 || pool.calls[0].sql != wantBacklogSQL {
		t.Fatalf("SQL = %q, want точный контракт", pool.calls[0].sql)
	}
	if want := []any{int16(model.StatusDeleted)}; len(pool.calls[0].args) != 1 ||
		pool.calls[0].args[0] != want[0] {
		t.Errorf("args = %v, want %v (исключение deleted, §13.1)", pool.calls[0].args, want)
	}

	for state, want := range map[string]float64{
		model.CheckStatePending.String(): 10,
		model.CheckStateQueued.String():  7,
	} {
		if v, ok := backlogGaugeValue(t, reg, state); !ok || v != want {
			t.Errorf("crawler_backlog_domains{state=%q} = (%v, %v), want (%v, true)", state, v, ok, want)
		}
	}
}

// Отсутствующее в агрегате состояние — экспортируется нулём: пустой
// backlog не должен пропадать из /metrics до следующего непустого.
func TestBacklogOnceEmptyStates(t *testing.T) {
	reg := metrics.New("scheduler")
	pool := &backlogFakePool{hook: func() (pgx.Rows, error) {
		return backlogRows(), nil // БД пуста — GROUP BY без строк
	}}

	res, err := BacklogOnce(context.Background(), pool, NewBacklogGauge(reg))
	if err != nil {
		t.Fatalf("BacklogOnce: %v", err)
	}
	if res.Pending != 0 || res.Queued != 0 {
		t.Errorf("итог = %+v, want нули", res)
	}
	for _, state := range []string{model.CheckStatePending.String(), model.CheckStateQueued.String()} {
		if v, ok := backlogGaugeValue(t, reg, state); !ok || v != 0 {
			t.Errorf("crawler_backlog_domains{state=%q} = (%v, %v), want (0, true)", state, v, ok)
		}
	}
}

func TestBacklogOnceErrors(t *testing.T) {
	reg := metrics.New("scheduler")
	boom := errors.New("бд недоступна")
	pool := &backlogFakePool{hook: func() (pgx.Rows, error) { return nil, boom }}

	if _, err := BacklogOnce(context.Background(), pool, NewBacklogGauge(reg)); err == nil ||
		!errors.Is(err, boom) {
		t.Fatalf("ошибка = %v, want исходная ошибка БД", err)
	}
	// Сбой прохода gauge не трогает: серии остаются с прошлым значением
	// или отсутствуют — но не «обнуляются» (ложное «backlog пуст»).
	if _, ok := backlogGaugeValue(t, reg, model.CheckStatePending.String()); ok {
		t.Error("сбойный проход не должен писать gauge")
	}

	if _, err := BacklogOnce(context.Background(), nil, NewBacklogGauge(reg)); err == nil ||
		err.Error() != "scheduler: глубина backlog: пул не задан" {
		t.Fatalf("nil-пул = %v, want ошибка", err)
	}
	if _, err := BacklogOnce(context.Background(), pool, nil); err == nil ||
		err.Error() != "scheduler: глубина backlog: gauge не задан" {
		t.Fatalf("nil-gauge = %v, want ошибка", err)
	}
}

func TestBacklogOptionsDefaults(t *testing.T) {
	if got := (BacklogOptions{}).withDefaults().Interval; got != DefaultBacklogInterval {
		t.Errorf("Interval = %v, want %v", got, DefaultBacklogInterval)
	}
	if got := (BacklogOptions{Interval: time.Second}).withDefaults().Interval; got != time.Second {
		t.Errorf("кастомный Interval затёрт: %v", got)
	}
}

// --- BacklogLoop -----------------------------------------------------------

func TestBacklogLoop(t *testing.T) {
	reg := metrics.New("scheduler")
	pool := &backlogFakePool{}
	pool.setHook(func() (pgx.Rows, error) {
		return backlogRows(
			[]any{int16(model.CheckStatePending), int64(3)},
			[]any{int16(model.CheckStateQueued), int64(4)},
		), nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- BacklogLoop(ctx, pool, reg, BacklogOptions{
			Interval: time.Millisecond,
			Log:      nil,
		})
	}()

	// Первый проход — сразу при старте, без ожидания тика.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := backlogGaugeValue(t, reg, model.CheckStatePending.String()); ok && v == 3 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if v, ok := backlogGaugeValue(t, reg, model.CheckStatePending.String()); !ok || v != 3 {
		t.Fatalf("стартовый проход: pending = (%v, %v), want (3, true)", v, ok)
	}

	// Сбой прохода не останавливает цикл; восстановление — обновление.
	pool.setHook(func() (pgx.Rows, error) { return nil, errors.New("бд недоступна") })
	time.Sleep(20 * time.Millisecond) // несколько неудачных тиков
	if v, _ := backlogGaugeValue(t, reg, model.CheckStatePending.String()); v != 3 {
		t.Errorf("при сбоях pending = %v, want последнее известное 3", v)
	}
	pool.setHook(func() (pgx.Rows, error) {
		return backlogRows(
			[]any{int16(model.CheckStatePending), int64(5)},
			[]any{int16(model.CheckStateQueued), int64(0)},
		), nil
	})
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v, _ := backlogGaugeValue(t, reg, model.CheckStatePending.String()); v == 5 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if v, _ := backlogGaugeValue(t, reg, model.CheckStatePending.String()); v != 5 {
		t.Fatal("цикл не восстановился после ошибки")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("BacklogLoop: %v, want nil (чистый выход по отмене)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("BacklogLoop не завершился после отмены ctx")
	}
}

func TestBacklogLoopValidation(t *testing.T) {
	pool := &backlogFakePool{hook: func() (pgx.Rows, error) { return backlogRows(), nil }}
	if err := BacklogLoop(context.Background(), nil, metrics.New("scheduler"), BacklogOptions{}); err == nil ||
		err.Error() != "scheduler: цикл backlog: пул не задан" {
		t.Fatalf("nil-пул = %v, want ошибка", err)
	}
	if err := BacklogLoop(context.Background(), pool, nil, BacklogOptions{}); err == nil ||
		err.Error() != "scheduler: цикл backlog: реестр метрик не задан" {
		t.Fatalf("nil-реестр = %v, want ошибка", err)
	}
}

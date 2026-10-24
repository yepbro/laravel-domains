// Юнит-тесты коллектора глубины backlog (T-077, ТЗ 5.9) на фейковом
// pgx-пуле: точный SQL-текст и параметр (исключение deleted), экспорт
// обеих серий gauge через Gather (включая нулевые), дефолты, ошибки,
// nil-зависимости, цикл BacklogLoop (немедленный первый проход,
// переживание ошибки, чистый выход по отмене ctx). Поведение против
// живой БД — интеграционные тесты.
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

// callsCount — снимок числа вызовов под замком (ожидание прогресса
// цикла в тестах).
func (p *backlogFakePool) callsCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls)
}

// syncBuf — bytes.Buffer под замком: slog пишет из горутины цикла, а
// тест читает лог ДО её завершения (ожидание Warn вживую) — без
// синхронизации это гонка.
type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *syncBuf) contains(s string) bool {
	return strings.Contains(b.String(), s)
}

// --- ожидаемый SQL-текст (контракт) ---------------------------------------

const wantBacklogSQL = "SELECT coalesce(sum(domains_pending), 0), coalesce(sum(domains_queued), 0) FROM zones"

// backlogPair — строка ответа нового прохода (pending, queued): суммы
// счётчиков справочника зон (T-178).
func backlogPair(pending, queued int64) *backlogFakeRows {
	return backlogRows([]any{pending, queued})
}

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
		return backlogPair(10, 7), nil
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
	if len(pool.calls[0].args) != 0 {
		t.Errorf("args = %v, want пусто (суммы справочника, T-178)", pool.calls[0].args)
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

// Пустой справочник — нули: пустой backlog обязан экспортироваться, а
// не пропадать из /metrics до следующего непустого (агрегат без GROUP BY
// всегда отвечает одной строкой с нулями).
func TestBacklogOnceEmptyStates(t *testing.T) {
	reg := metrics.New("scheduler")
	pool := &backlogFakePool{hook: func() (pgx.Rows, error) {
		return backlogPair(0, 0), nil
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
	// Дефолт пинится литералом (T-161, замер §12: мутация константы
	// не должна незамеченной менять дефолт): сравнение с самой
	// константой мутации не замечает.
	if DefaultBacklogInterval != 5*time.Minute {
		t.Errorf("DefaultBacklogInterval = %v, want 5m", DefaultBacklogInterval)
	}
	if got := (BacklogOptions{}).withDefaults().Interval; got != 5*time.Minute {
		t.Errorf("Interval = %v, want 5m", got)
	}
	if got := (BacklogOptions{Interval: time.Second}).withDefaults().Interval; got != time.Second {
		t.Errorf("кастомный Interval затёрт: %v", got)
	}
	// 1 нс — положительное значение, дефолтом не заменяется.
	if got := (BacklogOptions{Interval: time.Nanosecond}).withDefaults().Interval; got != time.Nanosecond {
		t.Errorf("Interval 1нс = %v, want без замены", got)
	}
}

// TestBacklogOnceRowsErrors — ошибки чтения ответа: Scan (лишняя колонка
// ломает позиционный приём), rows.Err после строки и пустой ответ
// (агрегат обязан отвечать одной строкой) — точные контексты
// сообщений, исходная ошибка проксируется (errors.Is).
func TestBacklogOnceRowsErrors(t *testing.T) {
	reg := metrics.New("scheduler")

	// Scan: строка с тремя колонками вместо двух.
	scanPool := &backlogFakePool{hook: func() (pgx.Rows, error) {
		return backlogRows([]any{int64(1), int64(2), int64(3)}), nil
	}}
	if _, err := BacklogOnce(context.Background(), scanPool, NewBacklogGauge(reg)); err == nil ||
		!strings.Contains(err.Error(), "чтение") {
		t.Errorf("ошибка Scan = %v, want контекст «чтение»", err)
	}

	// rows.Err после строки ответа.
	boom := errors.New("бд недоступна")
	errRows := backlogPair(3, 4)
	errRows.err = boom
	errPool := &backlogFakePool{hook: func() (pgx.Rows, error) { return errRows, nil }}
	if _, err := BacklogOnce(context.Background(), errPool, NewBacklogGauge(reg)); err == nil ||
		!errors.Is(err, boom) {
		t.Errorf("ошибка rows.Err = %v, want исходная %v", err, boom)
	}

	// Пустой ответ — аномалия формы (агрегат без GROUP BY отвечает
	// одной строкой).
	emptyPool := &backlogFakePool{hook: func() (pgx.Rows, error) { return backlogRows(), nil }}
	if _, err := BacklogOnce(context.Background(), emptyPool, NewBacklogGauge(reg)); err == nil ||
		!strings.Contains(err.Error(), "пустой ответ агрегата") {
		t.Errorf("пустой ответ = %v, want «пустой ответ агрегата»", err)
	}
}

// --- BacklogLoop -----------------------------------------------------------

func TestBacklogLoop(t *testing.T) {
	reg := metrics.New("scheduler")
	pool := &backlogFakePool{}
	pool.setHook(func() (pgx.Rows, error) {
		return backlogPair(3, 4), nil
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
		return backlogPair(5, 0), nil
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

// TestBacklogLoopImmediateFirstPass — первый проход выполняется сразу
// при старте, не дожидаясь тика: с часовым интервалом gauge обязан
// заполниться (и только один раз) до любого тика.
func TestBacklogLoopImmediateFirstPass(t *testing.T) {
	reg := metrics.New("scheduler")
	pool := &backlogFakePool{}
	pool.setHook(func() (pgx.Rows, error) {
		return backlogPair(3, 4), nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- BacklogLoop(ctx, pool, reg, BacklogOptions{
			Interval: time.Hour, // тик не наступит: только стартовый проход
			Log:      slog.New(slog.DiscardHandler),
		})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := backlogGaugeValue(t, reg, model.CheckStatePending.String()); ok && v == 3 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if v, ok := backlogGaugeValue(t, reg, model.CheckStatePending.String()); !ok || v != 3 {
		cancel()
		t.Fatalf("pending = (%v, %v), want (3, true): проход не выполнен без тика", v, ok)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("BacklogLoop: %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("BacklogLoop не завершился после отмены ctx")
	}
	if n := pool.callsCount(); n != 1 {
		t.Errorf("проходов = %d, want 1 (стартовый, без тика)", n)
	}
}

// TestBacklogLoopLogging — записи цикла: успешный проход — Debug,
// сбой прохода при живом ctx — Warn с исходной ошибкой.
func TestBacklogLoopLogging(t *testing.T) {
	slogBuf := &syncBuf{}
	logger := slog.New(slog.NewTextHandler(slogBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	reg := metrics.New("scheduler")
	pool := &backlogFakePool{}
	pool.setHook(func() (pgx.Rows, error) {
		return backlogPair(3, 4), nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- BacklogLoop(ctx, pool, reg, BacklogOptions{
			Interval: 5 * time.Millisecond,
			Log:      logger,
		})
	}()

	// Стартовый проход — Debug с итогом.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := backlogGaugeValue(t, reg, model.CheckStatePending.String()); ok && v == 3 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// Сбойный проход при живом ctx — Warn. Гонка первого варианта:
	// ожидание числа вызовов/ошибочных вызовов НЕ гарантирует, что Warn
	// уже записан — cancel() встраивался между возвратом hook и выбором
	// ветки switch (тихая ветка «отмена во время прохода»), Warn терялся
	// (флак ~1/20). Детерминизм: ждём сам Warn в логе (syncBuf под
	// замком), запись Warn строго раньше нашей cancel.
	pool.setHook(func() (pgx.Rows, error) { return nil, errors.New("бд недоступна") })
	deadline = time.Now().Add(2 * time.Second)
	for !slogBuf.contains("бд недоступна") {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("сбойный проход не выполнен или Warn не записан:\n%s", slogBuf)
		}
		time.Sleep(2 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("BacklogLoop: %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("BacklogLoop не завершился после отмены ctx")
	}

	log := slogBuf.String()
	for _, want := range []string{
		"level=DEBUG", // итог успешного прохода
		"level=WARN",  // сбой прохода не убил цикл
		"бд недоступна",
	} {
		if !strings.Contains(log, want) {
			t.Errorf("лог не содержит %q:\n%s", want, log)
		}
	}
}

// TestBacklogLoopCancelDuringPass — ctx отменён до старта: единственный
// стартовый проход выполняется и логируется Debug — отмена не часть
// прохода, начатого до неё; выход чистый.
func TestBacklogLoopCancelDuringPass(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	reg := metrics.New("scheduler")
	pool := &backlogFakePool{hook: func() (pgx.Rows, error) {
		return backlogPair(3, 4), nil
	}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := BacklogLoop(ctx, pool, reg, BacklogOptions{Log: logger}); err != nil {
		t.Fatalf("BacklogLoop: %v, want nil при отмене ctx", err)
	}
	if !strings.Contains(buf.String(), "level=DEBUG") {
		t.Errorf("стартовый проход не логируется при отменённом ctx:\n%s", buf.String())
	}
	if v, ok := backlogGaugeValue(t, reg, model.CheckStatePending.String()); !ok || v != 3 {
		t.Errorf("pending = (%v, %v), want (3, true): стартовый проход выполнен", v, ok)
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

// --- single-flight (T-161) --------------------------------------------------

// backlogSkippedValue — текущее значение счётчика
// crawler_backlog_skipped_ticks_total из реестра.
func backlogSkippedValue(t *testing.T, reg *metrics.Registry) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "crawler_backlog_skipped_ticks_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			return m.GetCounter().GetValue()
		}
	}
	return 0
}

// backlogHistogramSamples — число наблюдений crawler_backlog_pass_seconds
// (каждый проход обязан попасть в гистограмму).
func backlogHistogramSamples(t *testing.T, reg *metrics.Registry) uint64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "crawler_backlog_pass_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			return m.GetHistogram().GetSampleCount()
		}
	}
	return 0
}

// TestBacklogLoopSingleFlight — проход дольше интервала (замер §12:
// агрегат по 47 млн строк): накопившийся тик не запускает новый проход
// сразу за медленным — Warn «дольше интервала», счётчик пропусков,
// гистограмма длительности; следующий проход — по следующему тику,
// цикл живёт.
//
// Раскладка без реальных секунд ожидания: интервал 50 мс, медленный
// стартовый проход 120 мс (тик 50 мс буферизуется, тик 100 мс тонет —
// канал тикера ёмкостью 1, тик 150 мс ещё не наступил к моменту дренажа
// в 120 мс): дренаж снимает тик без гонки на границе, проход 2 — не
// раньше 150 мс.
func TestBacklogLoopSingleFlight(t *testing.T) {
	slogBuf := &syncBuf{}
	logger := slog.New(slog.NewTextHandler(slogBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	reg := metrics.New("scheduler")
	pool := &backlogFakePool{}
	pool.setHook(func() (pgx.Rows, error) {
		time.Sleep(120 * time.Millisecond) // медленный агрегат (замер §12)
		return backlogPair(3, 4), nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- BacklogLoop(ctx, pool, reg, BacklogOptions{
			Interval: 50 * time.Millisecond,
			Log:      logger,
		})
	}()

	// Warn приходит в конце медленного прохода (после дренажа тика).
	deadline := time.Now().Add(2 * time.Second)
	for !slogBuf.contains("дольше интервала") {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("медленный проход не зафиксирован:\n%s", slogBuf)
		}
		time.Sleep(time.Millisecond)
	}
	if got := backlogSkippedValue(t, reg); got != 1 {
		t.Errorf("skipped_ticks = %v, want 1", got)
	}
	// Момент наблюдения Warn — до тика 150 мс: второй проход ещё не
	// мог начаться (единственный buffered-тик снят дренажом).
	if n := pool.callsCount(); n != 1 {
		t.Errorf("проходов = %d после дренажа, want 1 (тик пропущен)", n)
	}

	// Проход 2 быстрый: обычный режим восстанавливается, цикл живёт.
	pool.setHook(func() (pgx.Rows, error) {
		return backlogPair(5, 6), nil
	})
	deadline = time.Now().Add(2 * time.Second)
	for pool.callsCount() < 2 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("проход после пропуска не начался: %d", pool.callsCount())
		}
		time.Sleep(time.Millisecond)
	}
	if got := backlogSkippedValue(t, reg); got != 1 {
		t.Errorf("skipped_ticks = %v после быстрого прохода, want 1", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("BacklogLoop: %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("BacklogLoop не завершился после отмены ctx")
	}
	if got := backlogHistogramSamples(t, reg); got < 2 {
		t.Errorf("наблюдений гистограммы = %d, want ≥ 2 (по одному на проход)", got)
	}
}

// TestBacklogLoopFastPassNoSkip — проход короче интервала не_warn'ит и
// не двигает счётчик пропусков: single-flight касается только хвоста
// «проход дольше интервала».
func TestBacklogLoopFastPassNoSkip(t *testing.T) {
	reg := metrics.New("scheduler")
	pool := &backlogFakePool{}
	pool.setHook(func() (pgx.Rows, error) {
		return backlogPair(3, 4), nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- BacklogLoop(ctx, pool, reg, BacklogOptions{
			Interval: 20 * time.Millisecond, // запас против CI-задержек прохода
			Log:      slog.New(slog.DiscardHandler),
		})
	}()

	// Несколько быстрых проходов.
	deadline := time.Now().Add(2 * time.Second)
	for pool.callsCount() < 3 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("проходы не идут")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("BacklogLoop: %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("BacklogLoop не завершился после отмены ctx")
	}
	if got := backlogSkippedValue(t, reg); got != 0 {
		t.Errorf("skipped_ticks = %v, want 0 (быстрые проходы не пропускаются)", got)
	}
	if got := backlogHistogramSamples(t, reg); got < 3 {
		t.Errorf("наблюдений гистограммы = %d, want ≥ 3", got)
	}
}

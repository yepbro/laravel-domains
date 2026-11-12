// Unit tests of the backlog depth collector (T-077, spec 5.9) on a fake
// pgx pool: the exact SQL text and parameter (excluding deleted), the export
// of both gauge series via Gather (including zeros), defaults, errors,
// nil dependencies, the BacklogLoop cycle (an immediate first pass,
// surviving an error, a clean exit on ctx cancellation). Behavior against
// a live DB — integration tests.
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

// --- fake pool ---------------------------------------------------------

// backlogCall — a recorded SQL call with its arguments.
type backlogCall struct {
	sql  string
	args []any
}

// backlogFakeRows — pgx.Rows with pre-made rows; Scan is positional,
// in the order of the SQL columns (check_state, count).
type backlogFakeRows struct {
	rows   [][]any
	i      int
	closed bool
	err    error // Err() after the rows are exhausted
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

// Conn and TypeMap — part of pgx.Rows not used by the positional Scan.
func (r *backlogFakeRows) Conn() *pgx.Conn      { return nil }
func (r *backlogFakeRows) TypeMap() *pgtype.Map { return nil }

// backlogFakePool — a db.DB without a DB: the Query response is set by a hook (the pool
// is used by the loop in a goroutine; the test changes pass behavior).
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

// setHook changes response behavior (the test swaps the scenario on the live
// loop; access under a lock — Query runs in the loop's goroutine).
func (p *backlogFakePool) setHook(f func() (pgx.Rows, error)) {
	p.mu.Lock()
	p.hook = f
	p.mu.Unlock()
}

// callsCount — a snapshot of the call count under the lock (waiting for loop
// progress in tests).
func (p *backlogFakePool) callsCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls)
}

// syncBuf — a bytes.Buffer under a lock: slog writes from the loop goroutine, and
// the test reads the log BEFORE it finishes (waiting for a Warn live) — without
// synchronization this is a race.
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

// --- expected SQL text (contract) ---------------------------------------

const wantBacklogSQL = "SELECT coalesce(sum(domains_pending), 0), coalesce(sum(domains_queued), 0) FROM zones"

// backlogPair — a row of the new pass's response (pending, queued): sums of
// the zone reference table counters (T-178).
func backlogPair(pending, queued int64) *backlogFakeRows {
	return backlogRows([]any{pending, queued})
}

// backlogGaugeValue — the current value of the crawler_backlog_domains series.
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
				// The label name is state (docs/services.md, spec 5.9): not type.
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

// An empty reference table — zeros: an empty backlog must be exported, and
// not disappear from /metrics until the next non-empty one (an aggregate without GROUP BY
// always answers with a single row of zeros).
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
	// A pass failure does not touch the gauge: the series keep their previous value
	// or are absent — but not "reset to zero" (a false "backlog is empty").
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
	// The default is pinned by a literal (T-161, measurement §12: a mutation of the constant
	// must not change the default unnoticed): comparing with the very
	// constant does not notice mutations.
	if DefaultBacklogInterval != 5*time.Minute {
		t.Errorf("DefaultBacklogInterval = %v, want 5m", DefaultBacklogInterval)
	}
	if got := (BacklogOptions{}).withDefaults().Interval; got != 5*time.Minute {
		t.Errorf("Interval = %v, want 5m", got)
	}
	if got := (BacklogOptions{Interval: time.Second}).withDefaults().Interval; got != time.Second {
		t.Errorf("кастомный Interval затёрт: %v", got)
	}
	// 1 ns — a positive value, not replaced by the default.
	if got := (BacklogOptions{Interval: time.Nanosecond}).withDefaults().Interval; got != time.Nanosecond {
		t.Errorf("Interval 1нс = %v, want без замены", got)
	}
}

// TestBacklogOnceRowsErrors — response read errors: Scan (an extra column
// breaks the positional receiving), rows.Err after a row, and an empty response
// (the aggregate must answer with a single row) — exact message
// contexts, the original error is proxied (errors.Is).
func TestBacklogOnceRowsErrors(t *testing.T) {
	reg := metrics.New("scheduler")

	// Scan: a row with three columns instead of two.
	scanPool := &backlogFakePool{hook: func() (pgx.Rows, error) {
		return backlogRows([]any{int64(1), int64(2), int64(3)}), nil
	}}
	if _, err := BacklogOnce(context.Background(), scanPool, NewBacklogGauge(reg)); err == nil ||
		!strings.Contains(err.Error(), "чтение") {
		t.Errorf("ошибка Scan = %v, want контекст «чтение»", err)
	}

	// rows.Err after a response row.
	boom := errors.New("бд недоступна")
	errRows := backlogPair(3, 4)
	errRows.err = boom
	errPool := &backlogFakePool{hook: func() (pgx.Rows, error) { return errRows, nil }}
	if _, err := BacklogOnce(context.Background(), errPool, NewBacklogGauge(reg)); err == nil ||
		!errors.Is(err, boom) {
		t.Errorf("ошибка rows.Err = %v, want исходная %v", err, boom)
	}

	// An empty response is a form anomaly (an aggregate without GROUP BY answers
	// with a single row).
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

	// The first pass — immediately at start, without waiting for a tick.
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

	// A pass failure does not stop the loop; recovery — an update.
	pool.setHook(func() (pgx.Rows, error) { return nil, errors.New("бд недоступна") })
	time.Sleep(20 * time.Millisecond) // several failed ticks
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

// TestBacklogLoopImmediateFirstPass — the first pass runs immediately
// at start, without waiting for a tick: with an hourly interval the gauge must
// be filled (and only once) before any tick.
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
			Interval: time.Hour, // the tick never comes: only the starting pass
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

// TestBacklogLoopLogging — loop records: a successful pass — Debug,
// a pass failure with a live ctx — Warn with the original error.
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

	// The starting pass — Debug with the outcome.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := backlogGaugeValue(t, reg, model.CheckStatePending.String()); ok && v == 3 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// A failing pass with a live ctx — Warn. The race of the first variant:
	// waiting for the call/failing-call counts does NOT guarantee the Warn
	// is already written — cancel() was embedded between the hook's return and the
	// switch branch choice (the quiet "cancellation during a pass" branch), the Warn was lost
	// (a flake of ~1/20). Determinism: we wait for the Warn itself in the log (syncBuf under
	// a lock), the Warn record strictly before our cancel.
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
		"level=DEBUG", // the outcome of a successful pass
		"level=WARN",  // a pass failure did not kill the loop
		"бд недоступна",
	} {
		if !strings.Contains(log, want) {
			t.Errorf("лог не содержит %q:\n%s", want, log)
		}
	}
}

// TestBacklogLoopCancelDuringPass — ctx cancelled before the start: the only
// starting pass runs and is logged as Debug — the cancellation is not part
// of a pass begun before it; the exit is clean.
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

// backlogSkippedValue — the current value of the
// crawler_backlog_skipped_ticks_total counter from the registry.
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

// backlogHistogramSamples — the number of crawler_backlog_pass_seconds observations
// (every pass must land in the histogram).
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

// TestBacklogLoopSingleFlight — a pass longer than the interval (measurement §12:
// the aggregate over 47M rows): the accumulated tick does not start a new pass
// right after the slow one — a Warn "longer than the interval", a skip counter,
// a duration histogram; the next pass — on the next tick,
// the loop lives.
//
// The layout without real seconds of waiting: an interval of 50 ms, a slow
// starting pass of 120 ms (the 50 ms tick is buffered, the 100 ms tick sinks —
// the ticker channel has capacity 1, the 150 ms tick has not yet arrived by the moment of the
// drain at 120 ms): the drain removes the tick without a race at the boundary, pass 2 is not
// earlier than 150 ms.
func TestBacklogLoopSingleFlight(t *testing.T) {
	slogBuf := &syncBuf{}
	logger := slog.New(slog.NewTextHandler(slogBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	reg := metrics.New("scheduler")
	pool := &backlogFakePool{}
	pool.setHook(func() (pgx.Rows, error) {
		time.Sleep(120 * time.Millisecond) // a slow aggregate (measurement §12)
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

	// The Warn comes at the end of the slow pass (after the tick drain).
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
	// The moment of observing the Warn — before the 150 ms tick: the second pass could not
	// have started yet (the only buffered tick was removed by the drain).
	if n := pool.callsCount(); n != 1 {
		t.Errorf("проходов = %d после дренажа, want 1 (тик пропущен)", n)
	}

	// Pass 2 is fast: the normal mode is restored, the loop lives.
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

// TestBacklogLoopFastPassNoSkip — a pass shorter than the interval does not Warn
// and does not move the skip counter: single-flight concerns only the tail
// "pass longer than the interval".
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
			Interval: 20 * time.Millisecond, // headroom against CI delays of the pass
			Log:      slog.New(slog.DiscardHandler),
		})
	}()

	// Several fast passes.
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

// Юнит-тесты рендер-долга (T-175): снимок (счётчик crawler_counters из
// пула + лаг q:render), сводка одной строкой, деградации источников
// (обязана быть ошибкой, не тихим нулём), цикл одним проходом на фейках
// с проверкой gauge crawler_render_pending_domains. Живые БД/Redis —
// вне юнит-тестов (герметичность, стиль alerts_test.go).
package monitor

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"crawler/internal/metrics"
	"crawler/internal/queue"
)

// TestRenderDebtValue — арифметика и сводка одной строкой (тот же текст
// в журнале цикла, детали алерта и строке отчёта нагрузки).
func TestRenderDebtValue(t *testing.T) {
	d := RenderDebt{Flags: 443000, Lag: 99120}
	if d.Total() != 542120 {
		t.Errorf("Total = %d, want 542120", d.Total())
	}
	want := "флаги 443000 + лаг 99120 = 542120"
	if d.String() != want {
		t.Errorf("String = %q, want %q", d.String(), want)
	}
	if zero := (RenderDebt{}).String(); zero != "флаги 0 + лаг 0 = 0" {
		t.Errorf("нулевой долг = %q", zero)
	}
}

// TestRenderDebtReader — источник читает счётчик из пула и лаг q:render;
// nil-зависимости и ошибки — ошибки источника (не нули).
func TestRenderDebtReader(t *testing.T) {
	ctx := context.Background()

	pool := &alertsPool{debt: []int64{443000}}
	lener := &fakeLener{lens: map[string]int64{queue.StreamRender: 99120}}
	d, err := (RenderDebtReader{Pool: pool, Redis: lener}).RenderDebt(ctx)
	if err != nil {
		t.Fatalf("RenderDebt: %v", err)
	}
	if d.Flags != 443000 || d.Lag != 99120 || d.Total() != 542120 {
		t.Fatalf("снимок = %+v, want флаги 443000 лаг 99120", d)
	}
	// SQL — точный контракт (фейк падает на неожидаемом).
	if calls := pool.calls(); len(calls) != 1 || calls[0].sql != renderDebtFlagsSQL {
		t.Fatalf("запросов = %+v, want один renderDebtFlagsSQL", calls)
	}
	if streams := lener.streams(); len(streams) != 1 || streams[0] != queue.StreamRender {
		t.Fatalf("Len звали для %v, want только q:render", streams)
	}

	// nil-зависимости.
	if _, err := (RenderDebtReader{}).RenderDebt(ctx); err == nil {
		t.Error("пул nil: хочу ошибку")
	}
	if _, err := (RenderDebtReader{Pool: pool}).RenderDebt(ctx); err == nil {
		t.Error("Redis nil: хочу ошибку")
	}

	// Ошибки источника — ошибки (молчаливый ноль сделал бы gauge ложью).
	poolErr := &alertsPool{debtErrs: []error{errors.New("бд недоступна")}}
	if _, err := (RenderDebtReader{Pool: poolErr, Redis: lener}).RenderDebt(ctx); err == nil ||
		!strings.Contains(err.Error(), "счётчик") {
		t.Errorf("ошибка запроса счётчика: %v, want ошибка про счётчик", err)
	}

	poolEmpty := &alertsPool{debtEmpty: true}
	_, err = (RenderDebtReader{Pool: poolEmpty, Redis: lener}).RenderDebt(ctx)
	if err == nil || !strings.Contains(err.Error(), "render_pending") {
		t.Errorf("строки счётчика нет: %v, want ошибка про строку render_pending", err)
	}

	poolBad := &alertsPool{debtBadRow: true}
	if _, err := (RenderDebtReader{Pool: poolBad, Redis: lener}).RenderDebt(ctx); err == nil {
		t.Error("кривая строка счётчика: хочу ошибку чтения")
	}

	poolRows := &alertsPool{debt: []int64{1}, debtRowsErr: errors.New("итерация")}
	if _, err := (RenderDebtReader{Pool: poolRows, Redis: lener}).RenderDebt(ctx); err == nil {
		t.Error("ошибка итерации счётчика: хочу ошибку")
	}

	lenerErr := &fakeLener{lens: map[string]int64{}, errs: []error{errors.New("redis: xlen")}}
	if _, err := (RenderDebtReader{Pool: pool, Redis: lenerErr}).RenderDebt(ctx); err == nil ||
		!strings.Contains(err.Error(), "q:render") {
		t.Errorf("ошибка XLEN: %v, want ошибка про q:render", err)
	}
}

// TestRenderDebtLoopOnePass — отмена до старта: стартовый проход
// выполняется, gauge пишется флагами (лаг — территория
// crawler_queue_depth), сводка — Debug (журнал не флудит), цикл выходит
// nil; сбой источника — Warn без записи gauge.
func TestRenderDebtLoopOnePass(t *testing.T) {
	pool := &alertsPool{debt: []int64{443000}}
	lener := &fakeLener{lens: map[string]int64{queue.StreamRender: 99120}}
	reg := metrics.New("monitor")
	var buf bytes.Buffer
	opts := RenderDebtOptions{
		Interval: time.Minute,
		Log:      slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Metrics:  reg,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		done <- RenderDebtLoop(ctx, RenderDebtReader{Pool: pool, Redis: lener}, opts)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("цикл: %v (отмена — чистый nil)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("цикл не вышел по отмене")
	}

	v, ok := gaugeLabels(t, reg, "crawler_render_pending_domains", map[string]string{})
	if !ok || v != 443000 {
		t.Fatalf("gauge = %v (found=%v), want 443000 (только флаги; лаг — crawler_queue_depth)", v, ok)
	}
	if !strings.Contains(buf.String(), "рендер-долг: флаги 443000 + лаг 99120 = 542120") {
		t.Errorf("сводка прохода не в журнале (Debug):\n%s", buf.String())
	}
	if strings.Count(buf.String(), "рендер-долг: флаги") != 1 {
		t.Errorf("сводка пишется не одной строкой на проход:\n%s", buf.String())
	}

	// Сбой источника — Warn каждого прохода, цикл живёт, gauge держит
	// последнее значение (hold-last): источник с постоянной ошибкой и
	// живым ctx (отмена глушит Warn — часть остановки, стиль
	// ZoneStatsLoop), проходы считаются по вызовам источника.
	failing := &fakeRenderDebtSource{err: errors.New("бд недоступна")}
	var buf2 bytes.Buffer
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() {
		done2 <- RenderDebtLoop(ctx2, failing, RenderDebtOptions{
			Interval: time.Millisecond,
			Log:      slog.New(slog.NewTextHandler(&buf2, nil)),
			Metrics:  reg,
		})
	}()
	deadline := time.After(3 * time.Second)
	for failing.callsCount() < 2 {
		select {
		case <-deadline:
			cancel2()
			t.Fatal("проходов со сбоем нет — цикл не живёт")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel2()
	if err := <-done2; err != nil {
		t.Fatalf("цикл со сбоем: %v (отмена — чистый nil)", err)
	}
	if !strings.Contains(buf2.String(), "level=WARN") {
		t.Errorf("сбой источника без Warn:\n%s", buf2.String())
	}
	v, ok = gaugeLabels(t, reg, "crawler_render_pending_domains", map[string]string{})
	if !ok || v != 443000 {
		t.Errorf("gauge после сбоя = %v (found=%v), want прежние 443000 (hold-last)", v, ok)
	}

	// nil-источник — ошибка цикла (контракт OnRun).
	if err := RenderDebtLoop(context.Background(), nil, RenderDebtOptions{}); err == nil {
		t.Error("источник nil: хочу ошибку")
	}
}

// fakeRenderDebtSource — RenderDebtSource по заготовке с постоянной
// ошибкой и счётчиком вызовов (синхронизация теста с горутиной цикла).
type fakeRenderDebtSource struct {
	mu    sync.Mutex
	debt  RenderDebt
	err   error
	calls int
}

func (f *fakeRenderDebtSource) RenderDebt(context.Context) (RenderDebt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.debt, f.err
}

func (f *fakeRenderDebtSource) callsCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

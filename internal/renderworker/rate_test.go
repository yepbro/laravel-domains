// Юнит-тесты темпа потребления воркера (T-240, ТЗР этапа 35 §4.5 п.1,
// ФТ-13): скользящее окно rateWindow (потолок записей, выход меток по
// времени, пустое чтение бюджета не расходует) и проводка потолка в цикл
// Run — пачка чтения урезается до свободных слотов окна, исчерпанное окно
// держит паузу чтения до выхода старейшей записи. Воркер B (сборка
// q:render-fallback) ограничивается тем же параметром без ветвлений по
// движку/стриму — потолок действует, когда задан (Options.Rate).
package renderworker

import (
	"context"
	"sync"
	"testing"
	"time"

	"crawler/internal/queue"
)

// --- механика окна ------------------------------------------------------------

// TestRenderWorker_RateWindowMechanics — окно темпа чистыми метками
// времени (без реальных пауз): бюджет, расход пачками, потолок, выход
// меток по истечении окна, no-op пустого чтения.
func TestRenderWorker_RateWindowMechanics(t *testing.T) {
	start := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	w := newRateWindow(3, 10*time.Second)

	// Пустое окно — полный бюджет.
	if a := w.allowance(start); a != 3 {
		t.Errorf("allowance пустого окна = %d, хочу 3", a)
	}
	// Пустое чтение — no-op: обращение к пустой очереди потолка не
	// расходует (простой не жжёт бюджет).
	w.record(start, 0)
	if a := w.allowance(start); a != 3 {
		t.Errorf("после record(0) allowance = %d, хочу 3 (пустое чтение — no-op)", a)
	}

	// Пачка из двух записей — слот один.
	w.record(start, 2)
	if a := w.allowance(start); a != 1 {
		t.Errorf("после record(2) allowance = %d, хочу 1", a)
	}

	// Ещё запись — потолок исчерпан, retry ведёт к старейшей метке окна.
	w.record(start.Add(2*time.Second), 1)
	now := start.Add(2 * time.Second)
	if a := w.allowance(now); a != 0 {
		t.Errorf("у потолка allowance = %d, хочу 0", a)
	}
	if d := w.retry(now); d != 8*time.Second {
		t.Errorf("retry = %v, хочу 8s (старейшая метка start + окно 10s − now)", d)
	}

	// Окно скользит: первая пачка (start) вышла по возрасту — слот
	// освободился, жива только пачка start+2s.
	if a := w.allowance(start.Add(10 * time.Second)); a != 2 {
		t.Errorf("через окно allowance = %d, хочу 2 (пачка start истекла, start+2s жива)", a)
	}
	if d := w.retry(start.Add(10 * time.Second)); d != 2*time.Second {
		t.Errorf("retry = %v, хочу 2s (до выхода пачки start+2s)", d)
	}

	// Обе пачки вышли — окно пустое, полный бюджет.
	if a := w.allowance(start.Add(12 * time.Second)); a != 3 {
		t.Errorf("после выхода всех меток allowance = %d, хочу 3", a)
	}
}

// TestRenderWorker_RateOptionsDefaults — Rate — параметр сборки, а не
// этапа: дефолт 0 («без потолка», поведение основной сборки q:render не
// меняется), заданное значение not затирается, отрицательный сбрасывается.
func TestRenderWorker_RateOptionsDefaults(t *testing.T) {
	if o := (Options{}).withDefaults(); o.Rate != 0 {
		t.Errorf("дефолт Rate = %d, хочу 0 (основная сборка читает без потолка)", o.Rate)
	}
	if o := (Options{Rate: 7}).withDefaults(); o.Rate != 7 {
		t.Errorf("withDefaults затёр Rate: %d, хочу 7", o.Rate)
	}
	if o := (Options{Rate: -1}).withDefaults(); o.Rate != 0 {
		t.Errorf("отрицательный Rate = %d, хочу 0", o.Rate)
	}
}

// --- проводка потолка в цикл Run ----------------------------------------------

// rateQueue — queue.Queue для тестов темпа: фиксирует запрошенный размер
// каждой пачки чтения, раздаёт доставки по n, первые emptyReads чтений
// возвращают пусто без блока (пустая очередь снаружи XREADGROUP-блока —
// для проверки, что простой не расходует бюджет), исчерпание — блок до
// отмены ctx (как реальная очередь); Ack считает подтверждения и
// сигнализирует.
type rateQueue struct {
	mu         sync.Mutex
	deliveries []queue.Delivery
	emptyReads int
	batches    []int
	acked      int
	ackCh      chan struct{}
}

func newRateQueue(emptyReads int, ids ...string) *rateQueue {
	q := &rateQueue{emptyReads: emptyReads, ackCh: make(chan struct{}, 64)}
	for _, id := range ids {
		q.deliveries = append(q.deliveries, rwDelivery(id))
	}
	return q
}

var _ queue.Queue = (*rateQueue)(nil)

func (q *rateQueue) Publish(context.Context, string, queue.Message) (string, error) {
	return "entry", nil // публикация тестам темпа не нужна (строк нет — успех без публикации)
}

func (q *rateQueue) Consume(ctx context.Context, _, _, _ string, n int) ([]queue.Delivery, error) {
	q.mu.Lock()
	q.batches = append(q.batches, n)
	var out []queue.Delivery
	served := false
	if q.emptyReads > 0 {
		q.emptyReads--
		served = true // пустое чтение: возвращаемся сразу, блок не нужен
	} else if n > 0 && len(q.deliveries) > 0 {
		take := min(n, len(q.deliveries))
		out = append(out, q.deliveries[:take]...)
		q.deliveries = q.deliveries[take:]
		served = true
	}
	q.mu.Unlock()
	if !served {
		<-ctx.Done() // пустая очередь: блок XREADGROUP до отмены
		return nil, ctx.Err()
	}
	return out, nil
}

func (q *rateQueue) Ack(ctx context.Context, _, _ string, ids ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.mu.Lock()
	q.acked += len(ids)
	q.mu.Unlock()
	for range ids {
		q.ackCh <- struct{}{}
	}
	return nil
}

// batchAt — запрошенный размер i-й пачки чтения (0 — чтения не было).
func (q *rateQueue) batchAt(i int) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	if i >= len(q.batches) {
		return 0
	}
	return q.batches[i]
}

// consumesCount — сколько чтений состоялось.
func (q *rateQueue) consumesCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.batches)
}

// left — нерозданных доставок.
func (q *rateQueue) left() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.deliveries)
}

// waitConsume — ждать i-го по счёту чтения.
func (q *rateQueue) waitConsume(t *testing.T, i int, d time.Duration) int {
	t.Helper()
	deadline := time.After(d)
	for {
		if n := q.consumesCount(); n > i {
			return q.batchAt(i)
		}
		select {
		case <-deadline:
			t.Fatalf("чтение №%d не состоялось (%d чтений)", i+1, q.consumesCount())
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// waitAcks — ждать want подтверждений.
func (q *rateQueue) waitAcks(t *testing.T, want int, d time.Duration) {
	t.Helper()
	deadline := time.After(d)
	for {
		q.mu.Lock()
		n := q.acked
		q.mu.Unlock()
		if n >= want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("подтверждений %d, хочу ≥ %d", n, want)
		case <-q.ackCh:
		}
	}
}

// newRateWorker — воркер на очереди темпа (строк в БД нет — обработки
// быстрые: доставка подтверждается штатным отказом без рендера).
func newRateWorker(t *testing.T, q *rateQueue) *Worker {
	t.Helper()
	pool := newRWFakePool()
	store := &rwFakeStore{j: pool.j, p: pool}
	renderer := &rwFakeRenderer{j: pool.j}
	w, err := New(pool, q, store, renderer, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return w
}

// TestRenderWorker_RunRateCeiling — проводка потолка в Run (T-240, ФТ-13):
// пачка чтения урезается до свободных слотов окна (первая — Rate, не
// Consume), исчерпанное окно держит паузу чтения (внутри окна новых
// чтений нет), после скольжения окна чтение возобновляется и дорабатывает
// очередь. Окно сжато (renderRateWindow — тестовый шов, образец
// renderFailWaitBase), реальная пауза — сотни миллисекунд.
func TestRenderWorker_RunRateCeiling(t *testing.T) {
	oldWindow := renderRateWindow
	renderRateWindow = 500 * time.Millisecond
	t.Cleanup(func() { renderRateWindow = oldWindow })

	q := newRateQueue(0, "r1", "r2", "r3", "r4", "r5", "r6")
	w := newRateWorker(t, q)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, w, Options{Rate: 3, Consume: 6}) }()

	// Первая пачка урезана потолком: 3 записи, не Consume=6.
	if got := q.waitConsume(t, 0, 5*time.Second); got != 3 {
		t.Errorf("первая пачка чтения = %d, хочу 3 (потолок окна; не Consume=6)", got)
	}
	q.waitAcks(t, 3, 5*time.Second)

	// Потолок исчерпан: до истечения окна (500 мс) чтений больше нет —
	// за окно потреблено не больше потолка.
	time.Sleep(200 * time.Millisecond)
	if n := q.consumesCount(); n != 1 {
		t.Errorf("чтений внутри окна = %d, хочу 1 — потолок Rate=3 держит чтение до скольжения окна", n)
	}

	// Окно скользнуло — чтение возобновилось и дорабатывает остаток:
	// все 6 доставок розданы и подтверждены.
	q.waitAcks(t, 6, 10*time.Second)
	if left := q.left(); left != 0 {
		t.Errorf("нерозданных доставок %d, хочу 0 (воркер дорабатывает очередь в темпе потолка)", left)
	}
	rwStopLoop(t, cancel, done)
}

// TestRenderWorker_RunRateIdleKeepsBudget — пустая очередь не расходует
// потолок (ФТ-13: батч-режим «собрать и доработать», простой — в блоке
// XREADGROUP): после пустых чтений следующий запрос берёт полный бюджет
// окна, без паузы на «потраченное» простое чтение.
func TestRenderWorker_RunRateIdleKeepsBudget(t *testing.T) {
	oldWindow := renderRateWindow
	renderRateWindow = 500 * time.Millisecond
	t.Cleanup(func() { renderRateWindow = oldWindow })

	// Два пустых чтения, потом три доставки: Rate=3, Consume=6.
	q := newRateQueue(2, "r1", "r2", "r3")
	w := newRateWorker(t, q)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, w, Options{Rate: 3, Consume: 6}) }()

	// Пустые чтения немедленно сменяются рабочим (бюджет цел — паузы
	// темпа между ними нет); рабочий запрос берёт полный потолок, а не
	// остаток «за вычетом пустых чтений».
	if got := q.waitConsume(t, 0, 5*time.Second); got != 3 {
		t.Errorf("первая пачка = %d, хочу 3 (урезана потолком, не Consume=6)", got)
	}
	if got := q.waitConsume(t, 1, 5*time.Second); got != 3 {
		t.Errorf("вторая пачка = %d, хочу 3 (пустая очередь — no-op темпа)", got)
	}
	if got := q.waitConsume(t, 2, 5*time.Second); got != 3 {
		t.Errorf("третья пачка = %d, хочу 3 (полный бюджет после простоя)", got)
	}
	q.waitAcks(t, 3, 5*time.Second)
	rwStopLoop(t, cancel, done)
}

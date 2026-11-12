// Unit tests of the worker's consumption rate (T-240, render spec stage 35 §4.5 item 1,
// FR-13): the rateWindow sliding window (an entries cap, marks leaving by
// time, an empty read not spending the budget) and the cap wiring into the
// Run loop — the read chunk is trimmed to the window's free slots, an exhausted window
// holds the read pause until the oldest entry leaves. Worker B (the
// q:render-fallback build) is limited by the same parameter without branches by
// engine/stream — the cap acts when set (Options.Rate).
package renderworker

import (
	"context"
	"sync"
	"testing"
	"time"

	"crawler/internal/queue"
)

// --- window mechanics ---------------------------------------------------------

// TestRenderWorker_RateWindowMechanics — the rate window with pure timestamps
// (no real pauses): the budget, spending in chunks, the cap, marks
// leaving on the window's expiry, the no-op of an empty read.
func TestRenderWorker_RateWindowMechanics(t *testing.T) {
	start := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	w := newRateWindow(3, 10*time.Second)

	// An empty window — the full budget.
	if a := w.allowance(start); a != 3 {
		t.Errorf("allowance пустого окна = %d, хочу 3", a)
	}
	// An empty read — a no-op: calling an empty queue does not spend the
	// cap (idleness does not burn the budget).
	w.record(start, 0)
	if a := w.allowance(start); a != 3 {
		t.Errorf("после record(0) allowance = %d, хочу 3 (пустое чтение — no-op)", a)
	}

	// A chunk of two entries — one slot.
	w.record(start, 2)
	if a := w.allowance(start); a != 1 {
		t.Errorf("после record(2) allowance = %d, хочу 1", a)
	}

	// One more entry — the cap is exhausted, retry leads to the window's oldest mark.
	w.record(start.Add(2*time.Second), 1)
	now := start.Add(2 * time.Second)
	if a := w.allowance(now); a != 0 {
		t.Errorf("у потолка allowance = %d, хочу 0", a)
	}
	if d := w.retry(now); d != 8*time.Second {
		t.Errorf("retry = %v, хочу 8s (старейшая метка start + окно 10s − now)", d)
	}

	// The window slides: the first chunk (start) left by age — the slot
	// freed up, only the start+2s chunk is alive.
	if a := w.allowance(start.Add(10 * time.Second)); a != 2 {
		t.Errorf("через окно allowance = %d, хочу 2 (пачка start истекла, start+2s жива)", a)
	}
	if d := w.retry(start.Add(10 * time.Second)); d != 2*time.Second {
		t.Errorf("retry = %v, хочу 2s (до выхода пачки start+2s)", d)
	}

	// Both chunks left — the window is empty, the full budget.
	if a := w.allowance(start.Add(12 * time.Second)); a != 3 {
		t.Errorf("после выхода всех меток allowance = %d, хочу 3", a)
	}
}

// TestRenderWorker_RateOptionsDefaults — Rate is a build parameter, not a
// stage one: the default 0 ("without a cap", the main q:render build's behavior does not
// change), a set value is not wiped, a negative one is reset.
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

// --- cap wiring into the Run loop ---------------------------------------------

// rateQueue — a queue.Queue for the rate tests: records the requested size
// of every read chunk, hands out deliveries by n, the first emptyReads reads
// return empty without a block (an empty queue outside the XREADGROUP block —
// to check that idleness does not spend the budget), exhaustion — a block until
// ctx is cancelled (like a real queue); Ack counts acknowledgments and
// signals.
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
	return "entry", nil // the rate tests do not need a publication (no rows — a success without publication)
}

func (q *rateQueue) Consume(ctx context.Context, _, _, _ string, n int) ([]queue.Delivery, error) {
	q.mu.Lock()
	q.batches = append(q.batches, n)
	var out []queue.Delivery
	served := false
	if q.emptyReads > 0 {
		q.emptyReads--
		served = true // an empty read: we return immediately, no block needed
	} else if n > 0 && len(q.deliveries) > 0 {
		take := min(n, len(q.deliveries))
		out = append(out, q.deliveries[:take]...)
		q.deliveries = q.deliveries[take:]
		served = true
	}
	q.mu.Unlock()
	if !served {
		<-ctx.Done() // an empty queue: an XREADGROUP block until cancellation
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

// batchAt — the requested size of the i-th read chunk (0 — there was no read).
func (q *rateQueue) batchAt(i int) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	if i >= len(q.batches) {
		return 0
	}
	return q.batches[i]
}

// consumesCount — how many reads happened.
func (q *rateQueue) consumesCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.batches)
}

// left — the undelivered (unhanded-out) deliveries.
func (q *rateQueue) left() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.deliveries)
}

// waitConsume — wait for the i-th read in order.
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

// waitAcks — wait for want acknowledgments.
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

// newRateWorker — a worker over the rate queue (no rows in the DB — processing
// is fast: the delivery is acknowledged by a regular rejection without a render).
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

// TestRenderWorker_RunRateCeiling — the cap wiring into Run (T-240, FR-13):
// the read chunk is trimmed to the window's free slots (the first — Rate, not
// Consume), an exhausted window holds the read pause (inside the window there are no new
// reads), after the window slides the reading resumes and finishes
// the queue. The window is shrunk (renderRateWindow — a test seam, the
// renderFailWaitBase pattern), the real pause is hundreds of milliseconds.
func TestRenderWorker_RunRateCeiling(t *testing.T) {
	oldWindow := renderRateWindow
	renderRateWindow = 500 * time.Millisecond
	t.Cleanup(func() { renderRateWindow = oldWindow })

	q := newRateQueue(0, "r1", "r2", "r3", "r4", "r5", "r6")
	w := newRateWorker(t, q)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, w, Options{Rate: 3, Consume: 6}) }()

	// The first chunk is trimmed by the cap: 3 entries, not Consume=6.
	if got := q.waitConsume(t, 0, 5*time.Second); got != 3 {
		t.Errorf("первая пачка чтения = %d, хочу 3 (потолок окна; не Consume=6)", got)
	}
	q.waitAcks(t, 3, 5*time.Second)

	// The cap is exhausted: until the window expires (500 ms) there are no more reads —
	// no more than the cap is consumed over the window.
	time.Sleep(200 * time.Millisecond)
	if n := q.consumesCount(); n != 1 {
		t.Errorf("чтений внутри окна = %d, хочу 1 — потолок Rate=3 держит чтение до скольжения окна", n)
	}

	// The window slid — the reading resumed and finishes the rest:
	// all 6 deliveries are handed out and acknowledged.
	q.waitAcks(t, 6, 10*time.Second)
	if left := q.left(); left != 0 {
		t.Errorf("нерозданных доставок %d, хочу 0 (воркер дорабатывает очередь в темпе потолка)", left)
	}
	rwStopLoop(t, cancel, done)
}

// TestRenderWorker_RunRateIdleKeepsBudget — an empty queue does not spend the
// cap (FR-13: the "collect and rework" batch mode, idleness — in the
// XREADGROUP block): after empty reads the next request takes the window's full
// budget, without a pause for the "spent" idle read.
func TestRenderWorker_RunRateIdleKeepsBudget(t *testing.T) {
	oldWindow := renderRateWindow
	renderRateWindow = 500 * time.Millisecond
	t.Cleanup(func() { renderRateWindow = oldWindow })

	// Two empty reads, then three deliveries: Rate=3, Consume=6.
	q := newRateQueue(2, "r1", "r2", "r3")
	w := newRateWorker(t, q)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, w, Options{Rate: 3, Consume: 6}) }()

	// The empty reads are immediately replaced by a working one (the budget is intact — no
	// rate pause between them); the working request takes the full cap, not
	// the remainder "minus the empty reads".
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

// Unit tests of counting tarpit-classified deliveries (T-261, TZ-fsm §3
// FR-7): the tarpit-finalization decision is made by the cardinality of the
// render:tarpit:{task_id} set (the Options.Tarpits seam, the production build —
// queue.TarpitCounter over Redis), not by the PEL delivery number.
// Checked: mixed series (an infra failure between tarpits does not move the
// counter — no finalization until the tarpit threshold), SADD idempotency by
// the delivery id (a replay of the number after the T-222 attempt refund), degradations
// (a Redis failure / the counter not mounted / a task without a task_id / losing
// the set) and the episode budget through the fallback (one task_id for the A and
// B streams). The Redis specifics of the key — test/integration/render_tarpit_test.go;
// the behavior before T-261 on a nil counter — tarpit_test.go. The fake
// environment — worker_test.go.
package renderworker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"crawler/internal/config"
	"crawler/internal/metrics"
	"crawler/internal/queue"
)

// rwFakeTarpits — a TarpitStore without Redis: sets of deliveries per task
// (SADD semantics: a repeated member does not grow the cardinality), TTLs per key,
// a call journal. err — a failure of all calls (a Redis loss); flush — losing
// the sets (a cleanup/TTL expiry with the storage alive).
type rwFakeTarpits struct {
	mu    sync.Mutex
	sets  map[string]map[string]struct{}
	ttls  map[string]time.Duration
	calls []string // the "taskID|delivery" journal in call order
	err   error
}

func newRWFakeTarpits() *rwFakeTarpits {
	return &rwFakeTarpits{
		sets: make(map[string]map[string]struct{}),
		ttls: make(map[string]time.Duration),
	}
}

func (f *rwFakeTarpits) Track(_ context.Context, taskID, delivery string, ttl time.Duration) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return 0, f.err
	}
	if f.sets[taskID] == nil {
		f.sets[taskID] = make(map[string]struct{})
	}
	f.sets[taskID][delivery] = struct{}{}
	f.ttls[taskID] = ttl
	f.calls = append(f.calls, taskID+"|"+delivery)
	return len(f.sets[taskID]), nil
}

// count — the task set's cardinality under the lock.
func (f *rwFakeTarpits) count(taskID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sets[taskID])
}

// has — a member of the task's set (an exact check of "the delivery is counted").
func (f *rwFakeTarpits) has(taskID, delivery string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.sets[taskID][delivery]
	return ok
}

// callsCount — the number of Track calls (the degradations must not make them).
func (f *rwFakeTarpits) callsCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// ttlOf — the last written TTL of the task's key.
func (f *rwFakeTarpits) ttlOf(taskID string) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ttls[taskID]
}

// flush — losing the sets (an operator's Redis cleanup, a restart without
// persistence): the TTLs and members disappear, the storage is alive.
func (f *rwFakeTarpits) flush() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sets = make(map[string]map[string]struct{})
}

var _ TarpitStore = (*rwFakeTarpits)(nil)

// rwTaskIDDelivery — a valid delivery with the Result message of the task
// taskID of the stream stream: the task identifier survives the stream record and
// is carried over by the fallback publication (T-238), the counter is counted by it.
func rwTaskIDDelivery(t *testing.T, stream, id, taskID string) queue.Delivery {
	t.Helper()
	task := rwTask
	task.TaskID = taskID
	payload, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	return queue.Delivery{Stream: stream, ID: id, Payload: payload}
}

// newRWTarpitFixture — a fixture with the counter mounted and separate
// limits (TimeoutAttempts 2 < MaxAttempts 5): in the tests only the
// tarpit branch can finalize, the task class branch T-172 (the delivery number ≥
// MaxAttempts) does not interfere.
func newRWTarpitFixture(t *testing.T, tp TarpitStore) *rwFixture {
	t.Helper()
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer,
		Options{TimeoutAttempts: 2, MaxAttempts: 5, Tarpits: tp})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return f
}

// TestRenderWorker_TarpitCountMixedSeries — the T-261 fix: all deliveries grow the
// PEL delivery number, the tarpit set — only tarpit-classified ones. The mixed series "an infra failure → a tarpit on
// delivery #2" no longer finalizes (before T-261 attempts=2 ≥ the limit
// would have finalized with a single tarpit): the finalization — only after
// the second tarpit-classified delivery.
func TestRenderWorker_TarpitCountMixedSeries(t *testing.T) {
	tp := newRWFakeTarpits()
	reg := metrics.New("render-worker")
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer,
		Options{TimeoutAttempts: 2, MaxAttempts: 5, Tarpits: tp, Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	// Delivery #1 — an infrastructure failure (the DB unavailable): a retryable
	// error, the tarpit counter does not see it.
	f.pool.queryErr = errors.New("the database is unavailable")
	if err := f.w.processDelivery(ctx, rwTaskIDDelivery(t, queue.StreamRender, "e1", "t1"), 1); err == nil {
		t.Fatal("an infra failure — a re-delivery error")
	}
	if n := tp.callsCount(); n != 0 {
		t.Errorf("Track was called %d times — an infra delivery is not tarpit-classified", n)
	}
	f.pool.queryErr = nil

	// Delivery #2 — a tarpit: the count 1 < 2, NO finalization (the delivery
	// number 2 ≥ TimeoutAttempts — the behavior before T-261 would have finalized here).
	if err := f.w.processDelivery(ctx, rwTaskIDDelivery(t, queue.StreamRender, "e1", "t1"), 2); err == nil {
		t.Fatal("tarpit #1 of 2 — a re-delivery error (a mixed series does not finalize an attempt earlier)")
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Fatalf("there must be no publications: %+v", pubs)
	}

	// Delivery #3 — a tarpit: the count 2 ≥ 2, a finalization without a render.
	if err := f.w.processDelivery(ctx, rwTaskIDDelivery(t, queue.StreamRender, "e1", "t1"), 3); err != nil {
		t.Fatalf("tarpit #2 of 2 — a finalization: %v", err)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("the publications %+v, one q:analyze is needed (the finalization)", pubs)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.rendered || row.failedAt == nil {
		t.Errorf("the row after the finalization: rendered=%v failedAt=%v — expecting false/marker", row.rendered, row.failedAt)
	}
	// The set counted exactly the tarpit deliveries: the infra failure is not in it.
	if got := tp.count("t1"); got != 2 {
		t.Errorf("the t1 set cardinality = %d, want 2 (only tarpit deliveries)", got)
	}
	if tp.has("t1", "e1#1") {
		t.Error("the infra delivery e1#1 must not get into the tarpit set")
	}
	if !tp.has("t1", "e1#2") || !tp.has("t1", "e1#3") {
		t.Error("the tarpit deliveries e1#2/e1#3 must be in the set")
	}
	if got := tp.ttlOf("t1"); got != DefaultTarpitTTL {
		t.Errorf("the key TTL = %s, want %s (DefaultTarpitTTL)", got, DefaultTarpitTTL)
	}
	if got := rwTarpitMetric(t, reg, config.EngineChromium); got != 1 {
		t.Errorf("crawler_render_tarpit_finalized_total = %v, want 1 (the finalization is the tarpit branch, not T-172)", got)
	}
}

// TestRenderWorker_TarpitCountPureSeriesBoundary — the boundary has not shifted:
// with the counter mounted, a pure tarpit series (a timeout on every
// delivery) finalizes exactly on delivery # TimeoutAttempts — as before
// T-261: the threshold delivery counts itself (SADD before the SCARD comparison),
// for a pure series the delivery number and the tarpit count coincide.
func TestRenderWorker_TarpitCountPureSeriesBoundary(t *testing.T) {
	tp := newRWFakeTarpits()
	f := newRWTarpitFixture(t, tp)
	ctx := context.Background()
	del := rwTaskIDDelivery(t, queue.StreamRender, "e1", "t1")

	// Delivery #1 (TimeoutAttempts 2): the count 1 — a retry, as before T-261.
	if err := f.w.processDelivery(ctx, del, 1); err == nil {
		t.Fatal("a pure series, delivery #1 — a re-delivery error")
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Fatalf("there must be no publications: %+v", pubs)
	}
	// Delivery #2: the count 2 — a finalization exactly on the threshold delivery.
	if err := f.w.processDelivery(ctx, del, 2); err != nil {
		t.Fatalf("a pure series, delivery #2 — a finalization: %v", err)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("the publications %+v, one q:analyze is needed", pubs)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.rendered || row.failedAt == nil {
		t.Errorf("the row after the finalization: rendered=%v failedAt=%v — expecting false/marker", row.rendered, row.failedAt)
	}
}

// TestRenderWorker_TarpitCountIdempotentReplay — one delivery, one set
// member: a replay of the same delivery number (the T-222/FR-6.2 attempt
// refund, reprocessing of a stuck one) does not grow the cardinality — SADD is
// idempotent by "{entry-id}#{number}". Below the threshold both processings are retryable, the first
// NEW number finalizes.
func TestRenderWorker_TarpitCountIdempotentReplay(t *testing.T) {
	tp := newRWFakeTarpits()
	f := newRWTarpitFixture(t, tp)
	ctx := context.Background()
	del := rwTaskIDDelivery(t, queue.StreamRender, "e1", "t1")

	// The first processing of delivery #2 — a tarpit, the count 1.
	if err := f.w.processDelivery(ctx, del, 2); err == nil {
		t.Fatal("tarpit #1 of 2 — a re-delivery error")
	}
	// A replay of the same number (the processing happened, no XACK — the
	// delivery counter returned the same number): the count stays 1, the duplicate does not finalize.
	if err := f.w.processDelivery(ctx, del, 2); err == nil {
		t.Fatal("the delivery #2 replay — the count did not grow (1 < 2), it must be a re-delivery error")
	}
	if got := tp.count("t1"); got != 1 {
		t.Errorf("the set cardinality after the replay = %d, want 1 (SADD is idempotent)", got)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Fatalf("there must be no publications: %+v", pubs)
	}
	// A new delivery #3 — a new member, the count 2 ≥ 2, a finalization.
	if err := f.w.processDelivery(ctx, del, 3); err != nil {
		t.Fatalf("delivery #3 — a finalization: %v", err)
	}
	if got := tp.count("t1"); got != 2 {
		t.Errorf("the set cardinality = %d, want 2", got)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Errorf("the publications %+v, one q:analyze is needed", pubs)
	}
}

// TestRenderWorker_TarpitCounterRedisLoss — the counter access degradations:
// a Redis failure (D1) — the decision by the PEL delivery number, the behavior
// before T-261 (a finalization on delivery #TimeoutAttempts, the processing does not crash);
// losing the set with Redis alive (D3) — the count starts over, the finalization
// is postponed until a new threshold and does not happen before the threshold.
func TestRenderWorker_TarpitCounterRedisLoss(t *testing.T) {
	t.Run("a Redis failure — the decision by the delivery number", func(t *testing.T) {
		tp := newRWFakeTarpits()
		tp.err = errors.New("redis: connection reset by peer")
		f := newRWTarpitFixture(t, tp)

		// Delivery #2 (TimeoutAttempts) with the counter unavailable
		// finalizes by the delivery number — as before T-261; at #1 — it does not.
		del := rwTaskIDDelivery(t, queue.StreamRender, "e1", "t1")
		if err := f.w.processDelivery(context.Background(), del, 1); err == nil {
			t.Fatal("delivery #1 at a Redis failure — a re-delivery error (the number is below the threshold)")
		}
		if err := f.w.processDelivery(context.Background(), del, 2); err != nil {
			t.Fatalf("delivery #2 at a Redis failure — a finalization by the delivery number (the degradation): %v", err)
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
			t.Fatalf("the publications %+v, one q:analyze is needed", pubs)
		}
		if row := rwRowSnapshot(f.pool, rwKey); row.failedAt == nil {
			t.Error("the render_failed_at marker must be in place — the finalization happened")
		}
	})
	t.Run("a set loss — the count anew", func(t *testing.T) {
		tp := newRWFakeTarpits()
		f := newRWTarpitFixture(t, tp)
		ctx := context.Background()
		del := rwTaskIDDelivery(t, queue.StreamRender, "e1", "t1")

		// Tarpit #1 (delivery #2): the count 1, a retry.
		if err := f.w.processDelivery(ctx, del, 2); err == nil {
			t.Fatal("tarpit #1 of 2 — a re-delivery error")
		}
		// The set is lost (a cleanup/a Redis restart): the count starts
		// over — delivery #3 does not finalize (the new count 1 < 2).
		tp.flush()
		if err := f.w.processDelivery(ctx, del, 3); err == nil {
			t.Fatal("after the set loss the count starts anew — delivery #3 does not finalize (1 < 2)")
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
			t.Fatalf("there must be no publications: %+v", pubs)
		}
		// The next tarpit brings the new count to the threshold — a finalization.
		if err := f.w.processDelivery(ctx, del, 4); err != nil {
			t.Fatalf("the new count reached the threshold — a finalization: %v", err)
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 1 {
			t.Errorf("publications = %d, 1 q:analyze is needed", len(pubs))
		}
	})
}

// TestRenderWorker_TarpitCounterNotMounted — the counter is not mounted
// (Options.Tarpits = nil, isolated builds) or a message without a task_id
// (the formats before T-024): the D2 degradation — the decision by the PEL delivery
// number, Track is not called at all.
func TestRenderWorker_TarpitCounterNotMounted(t *testing.T) {
	t.Run("the counter is not mounted", func(t *testing.T) {
		f := newRWTarpitFixture(t, nil)
		if err := f.w.processDelivery(context.Background(),
			rwTaskIDDelivery(t, queue.StreamRender, "e1", "t1"), 2); err != nil {
			t.Fatalf("a nil counter: delivery #2 — a finalization by the number (the behavior before T-261): %v", err)
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 1 {
			t.Errorf("publications = %d, 1 q:analyze is needed", len(pubs))
		}
	})
	t.Run("a message without a task_id", func(t *testing.T) {
		tp := newRWFakeTarpits()
		f := newRWTarpitFixture(t, tp)
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
			t.Fatalf("without a task_id: delivery #2 — a finalization by the number (there is no set identifier): %v", err)
		}
		if n := tp.callsCount(); n != 0 {
			t.Errorf("Track was called %d times — without a task_id the counter is not addressable", n)
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 1 {
			t.Errorf("publications = %d, 1 q:analyze is needed", len(pubs))
		}
	})
}

// TestRenderWorker_TarpitEpisodeBudgetThroughFallback — the tarpit budget is
// one per task (task_id), not per stream (invariant I4 of outcomestate.go):
// the fallback publication (T-238) moves the task_id into q:render-fallback, a
// worker B tarpit counts into the same set — an episode that exhausted tarpits on
// engine A is finalized on the first tarpit of engine B (the second engine
// confirmed the page tarpit). The members differ by the streams' entry-ids.
// Workers A and B — separate instances (like the production builds of one binary:
// the fbDedup fallback dedup filter — in process memory, B's is empty).
func TestRenderWorker_TarpitEpisodeBudgetThroughFallback(t *testing.T) {
	tp := newRWFakeTarpits()
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")
	newWorker := func(stream, engine string) *Worker {
		t.Helper()
		w, err := New(f.pool, f.q, f.store, f.renderer, Options{
			Stream: stream, TimeoutAttempts: 2, MaxAttempts: 5, Tarpits: tp,
			Fallback: FallbackOptions{Enabled: true}, Engine: engine,
		})
		if err != nil {
			t.Fatalf("New(%s): %v", stream, err)
		}
		return w
	}
	wA := newWorker(queue.StreamRender, rwFBCandidateEngine)
	wB := newWorker(queue.StreamRenderFallback, config.EngineChromium)
	ctx := context.Background()

	// Worker A (q:render): tarpit #1 — a retry, #2 — the threshold, a fallback
	// publication instead of the marker (T-238).
	a1 := rwTaskIDDelivery(t, queue.StreamRender, "e1", "tp1")
	if err := wA.processDelivery(ctx, a1, 1); err == nil {
		t.Fatal("tarpit #1 of 2 — a re-delivery error")
	}
	if err := wA.processDelivery(ctx, a1, 2); err != nil {
		t.Fatalf("tarpit #2 of 2 — the fallback publication: %v", err)
	}
	fbs := rwFallbackPublishes(t, f)
	if len(fbs) != 1 {
		t.Fatalf("q:render-fallback publications = %d, 1 is needed", len(fbs))
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("worker B sets the marker after the fallback is exhausted (FR-10) — it must not be here")
	}

	// Worker B (q:render-fallback, the same task): the very first tarpit sees
	// the cardinality 3 ≥ 2 — the episode's budget is exhausted on engine A, a finalization.
	b1 := rwTaskIDDelivery(t, queue.StreamRenderFallback, "f1", "tp1")
	if err := wB.processDelivery(ctx, b1, 1); err != nil {
		t.Fatalf("the worker B tarpit — a finalization (the episode budget is shared): %v", err)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 2 || pubs[1].stream != queue.StreamAnalyze {
		t.Fatalf("the publications %+v: the fallback (worker A) + q:analyze (the B finalization)", pubs)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt == nil {
		t.Error("the worker B finalization sets the render_failed_at marker")
	}
	// The streams' members differ by entry-id: 3 deliveries, no double counting.
	if got := tp.count("tp1"); got != 3 {
		t.Errorf("the episode set cardinality = %d, want 3 (e1#1, e1#2, f1#1)", got)
	}
	for _, m := range []string{"e1#1", "e1#2", "f1#1"} {
		if !tp.has("tp1", m) {
			t.Errorf("the member %s is missing from the episode set", m)
		}
	}
}

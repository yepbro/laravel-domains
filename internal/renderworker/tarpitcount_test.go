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
	f.pool.queryErr = errors.New("база недоступна")
	if err := f.w.processDelivery(ctx, rwTaskIDDelivery(t, queue.StreamRender, "e1", "t1"), 1); err == nil {
		t.Fatal("инфра-сбой — ошибка повторной доставки")
	}
	if n := tp.callsCount(); n != 0 {
		t.Errorf("Track вызван %d раз — инфра-доставка не тарпит-классифицирована", n)
	}
	f.pool.queryErr = nil

	// Delivery #2 — a tarpit: the count 1 < 2, NO finalization (the delivery
	// number 2 ≥ TimeoutAttempts — the behavior before T-261 would have finalized here).
	if err := f.w.processDelivery(ctx, rwTaskIDDelivery(t, queue.StreamRender, "e1", "t1"), 2); err == nil {
		t.Fatal("тарпит №1 из 2 — ошибка повторной доставки (смешанная серия не финализирует на попытку раньше)")
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Fatalf("публикаций быть не должно: %+v", pubs)
	}

	// Delivery #3 — a tarpit: the count 2 ≥ 2, a finalization without a render.
	if err := f.w.processDelivery(ctx, rwTaskIDDelivery(t, queue.StreamRender, "e1", "t1"), 3); err != nil {
		t.Fatalf("тарпит №2 из 2 — финализация: %v", err)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("публикации %+v, нужна одна q:analyze (финализация)", pubs)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.rendered || row.failedAt == nil {
		t.Errorf("строка после финализации: rendered=%v failedAt=%v — жду false/маркер", row.rendered, row.failedAt)
	}
	// The set counted exactly the tarpit deliveries: the infra failure is not in it.
	if got := tp.count("t1"); got != 2 {
		t.Errorf("мощность множества t1 = %d, хочу 2 (только тарпит-доставки)", got)
	}
	if tp.has("t1", "e1#1") {
		t.Error("инфра-доставка e1#1 не должна попадать в тарпит-множество")
	}
	if !tp.has("t1", "e1#2") || !tp.has("t1", "e1#3") {
		t.Error("тарпит-доставки e1#2/e1#3 обязаны быть в множестве")
	}
	if got := tp.ttlOf("t1"); got != DefaultTarpitTTL {
		t.Errorf("TTL ключа = %s, хочу %s (DefaultTarpitTTL)", got, DefaultTarpitTTL)
	}
	if got := rwTarpitMetric(t, reg, config.EngineChromium); got != 1 {
		t.Errorf("crawler_render_tarpit_finalized_total = %v, хочу 1 (финализация — тарпит-ветка, не T-172)", got)
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
		t.Fatal("чистая серия, доставка №1 — ошибка повторной доставки")
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Fatalf("публикаций быть не должно: %+v", pubs)
	}
	// Delivery #2: the count 2 — a finalization exactly on the threshold delivery.
	if err := f.w.processDelivery(ctx, del, 2); err != nil {
		t.Fatalf("чистая серия, доставка №2 — финализация: %v", err)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("публикации %+v, нужна одна q:analyze", pubs)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.rendered || row.failedAt == nil {
		t.Errorf("строка после финализации: rendered=%v failedAt=%v — жду false/маркер", row.rendered, row.failedAt)
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
		t.Fatal("тарпит №1 из 2 — ошибка повторной доставки")
	}
	// A replay of the same number (the processing happened, no XACK — the
	// delivery counter returned the same number): the count stays 1, the duplicate does not finalize.
	if err := f.w.processDelivery(ctx, del, 2); err == nil {
		t.Fatal("replay доставки №2 — счёт не вырос (1 < 2), обязана быть ошибка повторной доставки")
	}
	if got := tp.count("t1"); got != 1 {
		t.Errorf("мощность множества после replay = %d, хочу 1 (SADD идемпотентен)", got)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Fatalf("публикаций быть не должно: %+v", pubs)
	}
	// A new delivery #3 — a new member, the count 2 ≥ 2, a finalization.
	if err := f.w.processDelivery(ctx, del, 3); err != nil {
		t.Fatalf("доставка №3 — финализация: %v", err)
	}
	if got := tp.count("t1"); got != 2 {
		t.Errorf("мощность множества = %d, хочу 2", got)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Errorf("публикации %+v, нужна одна q:analyze", pubs)
	}
}

// TestRenderWorker_TarpitCounterRedisLoss — the counter access degradations:
// a Redis failure (D1) — the decision by the PEL delivery number, the behavior
// before T-261 (a finalization on delivery #TimeoutAttempts, the processing does not crash);
// losing the set with Redis alive (D3) — the count starts over, the finalization
// is postponed until a new threshold and does not happen before the threshold.
func TestRenderWorker_TarpitCounterRedisLoss(t *testing.T) {
	t.Run("сбой Redis — решение по номеру доставки", func(t *testing.T) {
		tp := newRWFakeTarpits()
		tp.err = errors.New("redis: connection reset by peer")
		f := newRWTarpitFixture(t, tp)

		// Delivery #2 (TimeoutAttempts) with the counter unavailable
		// finalizes by the delivery number — as before T-261; at #1 — it does not.
		del := rwTaskIDDelivery(t, queue.StreamRender, "e1", "t1")
		if err := f.w.processDelivery(context.Background(), del, 1); err == nil {
			t.Fatal("доставка №1 при сбое Redis — ошибка повторной доставки (номер ниже порога)")
		}
		if err := f.w.processDelivery(context.Background(), del, 2); err != nil {
			t.Fatalf("доставка №2 при сбое Redis — финализация по номеру доставки (деградация): %v", err)
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
			t.Fatalf("публикации %+v, нужна одна q:analyze", pubs)
		}
		if row := rwRowSnapshot(f.pool, rwKey); row.failedAt == nil {
			t.Error("маркер render_failed_at обязан стоять — финализация состоялась")
		}
	})
	t.Run("потеря множества — счёт заново", func(t *testing.T) {
		tp := newRWFakeTarpits()
		f := newRWTarpitFixture(t, tp)
		ctx := context.Background()
		del := rwTaskIDDelivery(t, queue.StreamRender, "e1", "t1")

		// Tarpit #1 (delivery #2): the count 1, a retry.
		if err := f.w.processDelivery(ctx, del, 2); err == nil {
			t.Fatal("тарпит №1 из 2 — ошибка повторной доставки")
		}
		// The set is lost (a cleanup/a Redis restart): the count starts
		// over — delivery #3 does not finalize (the new count 1 < 2).
		tp.flush()
		if err := f.w.processDelivery(ctx, del, 3); err == nil {
			t.Fatal("после потери множества счёт заново — delivery №3 не финализирует (1 < 2)")
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
			t.Fatalf("публикаций быть не должно: %+v", pubs)
		}
		// The next tarpit brings the new count to the threshold — a finalization.
		if err := f.w.processDelivery(ctx, del, 4); err != nil {
			t.Fatalf("новый счёт достиг порога — финализация: %v", err)
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 1 {
			t.Errorf("публикаций = %d, нужна 1 q:analyze", len(pubs))
		}
	})
}

// TestRenderWorker_TarpitCounterNotMounted — the counter is not mounted
// (Options.Tarpits = nil, isolated builds) or a message without a task_id
// (the formats before T-024): the D2 degradation — the decision by the PEL delivery
// number, Track is not called at all.
func TestRenderWorker_TarpitCounterNotMounted(t *testing.T) {
	t.Run("счётчик не смонтирован", func(t *testing.T) {
		f := newRWTarpitFixture(t, nil)
		if err := f.w.processDelivery(context.Background(),
			rwTaskIDDelivery(t, queue.StreamRender, "e1", "t1"), 2); err != nil {
			t.Fatalf("nil-счётчик: доставка №2 — финализация по номеру (поведение до T-261): %v", err)
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 1 {
			t.Errorf("публикаций = %d, нужна 1 q:analyze", len(pubs))
		}
	})
	t.Run("сообщение без task_id", func(t *testing.T) {
		tp := newRWFakeTarpits()
		f := newRWTarpitFixture(t, tp)
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
			t.Fatalf("без task_id: доставка №2 — финализация по номеру (идентификатора множества нет): %v", err)
		}
		if n := tp.callsCount(); n != 0 {
			t.Errorf("Track вызван %d раз — без task_id счётчик не адресуется", n)
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 1 {
			t.Errorf("публикаций = %d, нужна 1 q:analyze", len(pubs))
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
		t.Fatal("тарпит №1 из 2 — ошибка повторной доставки")
	}
	if err := wA.processDelivery(ctx, a1, 2); err != nil {
		t.Fatalf("тарпит №2 из 2 — публикация фолбэка: %v", err)
	}
	fbs := rwFallbackPublishes(t, f)
	if len(fbs) != 1 {
		t.Fatalf("публикации q:render-fallback = %d, нужна 1", len(fbs))
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("маркер ставит воркер B после исчерпания фолбэка (ФТ-10) — тут его быть не должно")
	}

	// Worker B (q:render-fallback, the same task): the very first tarpit sees
	// the cardinality 3 ≥ 2 — the episode's budget is exhausted on engine A, a finalization.
	b1 := rwTaskIDDelivery(t, queue.StreamRenderFallback, "f1", "tp1")
	if err := wB.processDelivery(ctx, b1, 1); err != nil {
		t.Fatalf("тарпит воркера B — финализация (бюджет эпизода общий): %v", err)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 2 || pubs[1].stream != queue.StreamAnalyze {
		t.Fatalf("публикации %+v: фолбэк (воркер A) + q:analyze (финализация B)", pubs)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt == nil {
		t.Error("маркер render_failed_at ставит финализация воркера B")
	}
	// The streams' members differ by entry-id: 3 deliveries, no double counting.
	if got := tp.count("tp1"); got != 3 {
		t.Errorf("мощность множества эпизода = %d, хочу 3 (e1#1, e1#2, f1#1)", got)
	}
	for _, m := range []string{"e1#1", "e1#2", "f1#1"} {
		if !tp.has("tp1", m) {
			t.Errorf("член %s отсутствует во множестве эпизода", m)
		}
	}
}

// Юнит-тесты счёта тарпит-классифицированных доставок (T-261, ТЗ-fsm §3
// ФТ-7): решение тарпит-финализации принимается по мощности множества
// render:tarpit:{task_id} (шов Options.Tarpits, боевая сборка —
// queue.TarpitCounter поверх Redis), а не по номеру доставки PEL.
// Проверяются: смешанные серии (инфра-сбой между тарпитами не двигает
// счётчик — финализации нет до порога тарпитов), идемпотентность SADD по
// id доставки (replay номера после возврата попытки T-222), деградации
// (сбой Redis / счётчик не смонтирован / задача без task_id / потеря
// множества) и бюджет эпизода сквозь фолбэк (task_id один на стримы A и
// B). Redis-специфика ключа — test/integration/render_tarpit_test.go;
// поведение до T-261 на nil-счётчике — tarpit_test.go. Фейковое
// окружение — worker_test.go.
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

// rwFakeTarpits — TarpitStore без Redis: множество доставок по задачам
// (SADD-семантика: повторный член мощность не растит), TTL по ключам,
// журнал вызовов. err — сбой всех вызовов (потеря Redis); flush — потеря
// множества (очистка/истечение TTL при живом хранилище).
type rwFakeTarpits struct {
	mu    sync.Mutex
	sets  map[string]map[string]struct{}
	ttls  map[string]time.Duration
	calls []string // журнал «taskID|delivery» в порядке вызовов
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

// count — мощность множества задачи под замком.
func (f *rwFakeTarpits) count(taskID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sets[taskID])
}

// has — член множества задачи (точная проверка «доставка посчитана»).
func (f *rwFakeTarpits) has(taskID, delivery string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.sets[taskID][delivery]
	return ok
}

// callsCount — число вызовов Track (деградации обязаны их не делать).
func (f *rwFakeTarpits) callsCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// ttlOf — последний записанный TTL ключа задачи.
func (f *rwFakeTarpits) ttlOf(taskID string) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ttls[taskID]
}

// flush — потеря множеств (очистка Redis оператором, рестарт без
// персистентности): TTL и члены исчезают, хранилище живо.
func (f *rwFakeTarpits) flush() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sets = make(map[string]map[string]struct{})
}

var _ TarpitStore = (*rwFakeTarpits)(nil)

// rwTaskIDDelivery — валидная доставка с Result-сообщением задачи
// taskID стрима stream: идентификатор задачи переживает запись стрима и
// переносится публикацией фолбэка (T-238), счётчик считается по нему.
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

// newRWTarpitFixture — фикстура со смонтированным счётчиком и раздельными
// лимитами (TimeoutAttempts 2 < MaxAttempts 5): финализировать в тестах
// может только тарпит-ветка, ветка класса задачи T-172 (номер доставки ≥
// MaxAttempts) не вмешивается.
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

// TestRenderWorker_TarpitCountMixedSeries — фикс T-261: номер доставки
// PEL растят все доставки, тарпит-множество — только
// тарпит-классифицированные. Смешанная серия «инфра-сбой → тарпит на
// доставке №2» больше не финализирует (до T-261 attempts=2 ≥ лимита
// финализировало бы с единственным тарпитом): финализация — только после
// второй тарпит-классифицированной доставки.
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

	// Доставка №1 — инфраструктурный сбой (БД недоступна): повторимая
	// ошибка, тарпит-счётчик её не видит.
	f.pool.queryErr = errors.New("база недоступна")
	if err := f.w.processDelivery(ctx, rwTaskIDDelivery(t, queue.StreamRender, "e1", "t1"), 1); err == nil {
		t.Fatal("инфра-сбой — ошибка повторной доставки")
	}
	if n := tp.callsCount(); n != 0 {
		t.Errorf("Track вызван %d раз — инфра-доставка не тарпит-классифицирована", n)
	}
	f.pool.queryErr = nil

	// Доставка №2 — тарпит: счёт 1 < 2, финализации НЕТ (номер доставки
	// 2 ≥ TimeoutAttempts — поведение до T-261 финализировало бы здесь).
	if err := f.w.processDelivery(ctx, rwTaskIDDelivery(t, queue.StreamRender, "e1", "t1"), 2); err == nil {
		t.Fatal("тарпит №1 из 2 — ошибка повторной доставки (смешанная серия не финализирует на попытку раньше)")
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Fatalf("публикаций быть не должно: %+v", pubs)
	}

	// Доставка №3 — тарпит: счёт 2 ≥ 2, финализация без рендера.
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
	// Множество посчитало ровно тарпит-доставки: инфра-сбоя в нём нет.
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

// TestRenderWorker_TarpitCountPureSeriesBoundary — граница не сместилась:
// со смонтированным счётчиком чистая тарпит-серия (таймаут на каждой
// доставке) финализирует ровно на доставке № TimeoutAttempts — как до
// T-261: пороговая доставка считает саму себя (SADD до сравнения SCARD),
// номер доставки и число тарпитов для чистой серии совпадают.
func TestRenderWorker_TarpitCountPureSeriesBoundary(t *testing.T) {
	tp := newRWFakeTarpits()
	f := newRWTarpitFixture(t, tp)
	ctx := context.Background()
	del := rwTaskIDDelivery(t, queue.StreamRender, "e1", "t1")

	// Доставка №1 (TimeoutAttempts 2): счёт 1 — повтор, как до T-261.
	if err := f.w.processDelivery(ctx, del, 1); err == nil {
		t.Fatal("чистая серия, доставка №1 — ошибка повторной доставки")
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Fatalf("публикаций быть не должно: %+v", pubs)
	}
	// Доставка №2: счёт 2 — финализация ровно на пороговой доставке.
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

// TestRenderWorker_TarpitCountIdempotentReplay — одна доставка один член
// множества: replay того же номера доставки (возврат попытки T-222/ФТ-6.2,
// повторная обработка зависшей) мощность не растит — SADD идемпотентен
// по «{entry-id}#{номер}». До порога обе обработки повторяемы, финалирует
// первый НОВЫЙ номер.
func TestRenderWorker_TarpitCountIdempotentReplay(t *testing.T) {
	tp := newRWFakeTarpits()
	f := newRWTarpitFixture(t, tp)
	ctx := context.Background()
	del := rwTaskIDDelivery(t, queue.StreamRender, "e1", "t1")

	// Первая обработка доставки №2 — тарпит, счёт 1.
	if err := f.w.processDelivery(ctx, del, 2); err == nil {
		t.Fatal("тарпит №1 из 2 — ошибка повторной доставки")
	}
	// Replay того же номера (обработка прошла, XACK не наступил — счётчик
	// доставок вернул тот же номер): счёт остаётся 1, дубль не финализирует.
	if err := f.w.processDelivery(ctx, del, 2); err == nil {
		t.Fatal("replay доставки №2 — счёт не вырос (1 < 2), обязана быть ошибка повторной доставки")
	}
	if got := tp.count("t1"); got != 1 {
		t.Errorf("мощность множества после replay = %d, хочу 1 (SADD идемпотентен)", got)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Fatalf("публикаций быть не должно: %+v", pubs)
	}
	// Новая доставка №3 — новый член, счёт 2 ≥ 2, финализация.
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

// TestRenderWorker_TarpitCounterRedisLoss — деградации доступа к счётчику:
// сбой Redis (Д1) — решение по номеру доставки PEL, поведению до T-261
// (финализация на delivery №TimeoutAttempts, обработка не падает);
// потеря множества при живом Redis (Д3) — счёт заново, финализация
// откладывается до нового порога, раньше порога не случается.
func TestRenderWorker_TarpitCounterRedisLoss(t *testing.T) {
	t.Run("сбой Redis — решение по номеру доставки", func(t *testing.T) {
		tp := newRWFakeTarpits()
		tp.err = errors.New("redis: connection reset by peer")
		f := newRWTarpitFixture(t, tp)

		// Доставка №2 (TimeoutAttempts) при недоступном счётчике
		// финализирует по номеру доставки — как до T-261; при №1 — нет.
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

		// Тарпит №1 (доставка №2): счёт 1, повтор.
		if err := f.w.processDelivery(ctx, del, 2); err == nil {
			t.Fatal("тарпит №1 из 2 — ошибка повторной доставки")
		}
		// Множество потеряно (очистка/рестарт Redis): счёт начинается
		// заново — delivery №3 не финализирует (новый счёт 1 < 2).
		tp.flush()
		if err := f.w.processDelivery(ctx, del, 3); err == nil {
			t.Fatal("после потери множества счёт заново — delivery №3 не финализирует (1 < 2)")
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
			t.Fatalf("публикаций быть не должно: %+v", pubs)
		}
		// Следующий тарпит доводит новый счёт до порога — финализация.
		if err := f.w.processDelivery(ctx, del, 4); err != nil {
			t.Fatalf("новый счёт достиг порога — финализация: %v", err)
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 1 {
			t.Errorf("публикаций = %d, нужна 1 q:analyze", len(pubs))
		}
	})
}

// TestRenderWorker_TarpitCounterNotMounted — счётчик не смонтирован
// (Options.Tarpits = nil, изолированные сборки) или сообщение без task_id
// (форматы до T-024): деградация Д2 — решение по номеру доставки PEL,
// Track не вызывается вовсе.
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

// TestRenderWorker_TarpitEpisodeBudgetThroughFallback — бюджет тарпитов
// один на задачу (task_id), не на стрим (инвариант И4 outcomestate.go):
// публикация фолбэка (T-238) переносит task_id в q:render-fallback, тарпит
// воркера B считается в то же множество — эпизод, исчерпавший тарпиты на
// движке A, на первом же тарпите движка B финализируется (второй движок
// подтвердил тарпит страницы). Члены различаются entry-id стримов.
// Воркеры A и B — отдельные экземпляры (как боевые сборки одного бинаря:
// фильтр дубля фолбэка fbDedup — в памяти процесса, у B он пуст).
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

	// Воркер A (q:render): тарпит №1 — повтор, №2 — порог, публикация
	// фолбэка вместо маркера (T-238).
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

	// Воркер B (q:render-fallback, та же задача): первый же тарпит видит
	// мощность 3 ≥ 2 — бюджет эпизода исчерпан на движке A, финализация.
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
	// Члены стримов различаются entry-id: доставок 3, двойного счёта нет.
	if got := tp.count("tp1"); got != 3 {
		t.Errorf("мощность множества эпизода = %d, хочу 3 (e1#1, e1#2, f1#1)", got)
	}
	for _, m := range []string{"e1#1", "e1#2", "f1#1"} {
		if !tp.has("tp1", m) {
			t.Errorf("член %s отсутствует во множестве эпизода", m)
		}
	}
}

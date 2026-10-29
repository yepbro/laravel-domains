// Юнит-тесты политики таймаутов рендера (T-148): таймаут страницы
// (render.timeout, предсказуемый тарпит) не жжёт общий лимит доставок
// redis.max_attempts как случайная ошибка. Пока номер доставки (счётчик
// PEL) ниже render.timeout_attempts — тарпит живёт общим путём повторных
// доставок (ошибка без XACK); на исчерпании лимита запись финализируется
// без рендера: итог — сырой HTML HTTP-фазы (rendered = false), публикация
// q:analyze, подтверждение доставки — q:dead остаётся инфраструктурным
// ошибкам. Фейковое окружение — worker_test.go.
package renderworker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/render"
)

// rwTarpitErr — ошибка рендера, неотличимая от боевой по классификации
// (session.wrap: ClassTask + context.DeadlineExceeded в цепочке — так
// помечается истечение общего таймаута страницы render.timeout).
func rwTarpitErr(url string) error {
	return &render.Error{Class: render.ClassTask, URL: url,
		Err: fmt.Errorf("таймаут страницы %s: %w: %w",
			25*time.Second, context.DeadlineExceeded, errors.New("navigation timeout"))}
}

// TestRenderWorker_TarpitFinalizedWithoutRender — суть T-148: таймаут
// страницы на доставке с номером ≥ TimeoutAttempts финализирует запись
// без рендера, а не уводит её в q:dead: обработка успешна (Processing
// подтверждает доставку), строка остаётся с сырым HTML (rendered = false,
// content_hash не меняется), публикуется q:analyze (Fresh=true —
// доработка текущего эпизода), Put/транзакция не выполняются.
func TestRenderWorker_TarpitFinalizedWithoutRender(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	// Доставка №2 из 2 (DefaultTimeoutAttempts): клейм после первого
	// таймаута — ProcessingAttempt подтверждает после nil-возврата.
	err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e1"), 2, f.w.processDelivery)
	if err != nil {
		t.Fatalf("тарпит на исчерпании лимита обязан финализироваться без ошибки (не q:dead): %v", err)
	}

	acked := f.q.ackedSnapshot()
	if len(acked) != 1 || acked[0] != "e1" {
		t.Errorf("XACK = %v, нужна e1 (финализация подтверждает доставку)", acked)
	}
	row := rwRowSnapshot(f.pool, rwKey)
	if row.rendered {
		t.Error("rendered должен остаться false — итог записи сырой HTML HTTP-фазы")
	}
	if row.failedAt == nil {
		t.Error("render_failed_at обязан ставиться финализацией (T-210) — иначе проход доработки переиздаёт запись")
	}
	if row.hash == nil || string(row.hash) != string(raw[:]) {
		t.Errorf("content_hash = %v, нужна прежняя ссылка на сырой HTML %s", row.hash, raw)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("публикации %+v, нужна одна q:analyze (конвейер продолжается анализом)", pubs)
	}
	if !pubs[0].msg.Fresh {
		t.Error("q:analyze Fresh=true — финализация дорабатывает текущий эпизод, как успешный рендер")
	}
	if pubs[0].msg.ZoneID != rwTask.ZoneID || pubs[0].msg.DomainID != rwTask.DomainID ||
		pubs[0].msg.SubdomainID != rwTask.SubdomainID || pubs[0].msg.Variant != rwTask.Variant {
		t.Errorf("q:analyze ключ %+v не совпадает с задачей", pubs[0].msg)
	}
	if puts := f.store.putsSnapshot(); len(puts) != 0 {
		t.Errorf("Put не должен вызываться: %d", len(puts))
	}
	if i := f.pool.j.indexOf(rwEvBegin); i >= 0 {
		t.Errorf("транзакция не должна открываться (событие на %d)", i)
	}
	// Refcount не тронут: сырой HTML остаётся итоговым контентом записи.
	if refc := rwRefc(f.pool)[raw]; refc != 1 {
		t.Errorf("refcount сырого HTML = %d, нужен 1 (ссылка жива)", refc)
	}
}

// TestRenderWorker_TarpitBelowLimitRetries — раздельные лимиты: тарпит
// на доставке с номером ниже TimeoutAttempts — обычная повторимая
// ошибка (без XACK, вернётся клеймом), как до T-148: первая попытка
// может таймаутнуться от затора, повтор даёт странице шанс.
func TestRenderWorker_TarpitBelowLimitRetries(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e1"), 1, f.w.processDelivery)
	if err == nil {
		t.Fatal("тарпит до исчерпания лимита — ошибка: доставка вернётся повторной доставкой")
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 0 {
		t.Errorf("XACK = %v — до исчерпания лимита подтверждения нет", acked)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикаций быть не должно: %+v", pubs)
	}
}

// TestRenderWorker_RetryableErrorKeepsDeadPath — повторимые ошибки
// (браузер, БД, хранилище, публикация) не финализируются никогда, на
// любом номере доставки: прежний путь — без XACK, исчерпание
// redis.max_attempts, q:dead (T-025).
func TestRenderWorker_RetryableErrorKeepsDeadPath(t *testing.T) {
	cases := []struct {
		name string
		set  func(f *rwFixture)
	}{
		{"браузер", func(f *rwFixture) {
			f.renderer.err = &render.Error{Class: render.ClassBrowser, URL: "https://spa.example/", Err: errors.New("canceled")}
		}},
		{"контекст", func(f *rwFixture) {
			f.renderer.err = &render.Error{Class: render.ClassContext, URL: "https://spa.example/", Err: errors.New("Session not found")}
		}},
		{"отмена вызывающего контекста (не тарпит)", func(f *rwFixture) {
			f.renderer.err = &render.Error{Class: render.ClassTask, URL: "https://spa.example/",
				Err: fmt.Errorf("задача отменена: %w: %w", context.Canceled, errors.New("canceled"))}
		}},
		{"сбой БД", func(f *rwFixture) {
			f.pool.queryErr = errors.New("база недоступна")
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newRWFixture(t)
			raw := rwTestHash(1)
			f.pool.seedRow(rwKey, "https://spa.example/", &raw)
			c.set(f)

			// Номер доставки выше лимита тарпитов (3 ≥ 2) — ошибка всё
			// равно возвращается: финализация только для таймаутов.
			err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e9"), 3, f.w.processDelivery)
			if err == nil {
				t.Fatal("повторимая ошибка обязана возвращаться (путь q:dead прежний)")
			}
			if acked := f.q.ackedSnapshot(); len(acked) != 0 {
				t.Errorf("XACK = %v — повторимая ошибка не подтверждается", acked)
			}
			if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
				t.Errorf("публикаций быть не должно: %+v", pubs)
			}
		})
	}
}

// TestRenderWorker_TarpitFinalizeNoRow — строки нет (перепроверка
// перезаписала) или у неё нет итогового URL: финализация — штатный отказ
// без публикации, доставка подтверждается (как ProcessResult).
func TestRenderWorker_TarpitFinalizeNoRow(t *testing.T) {
	f := newRWFixture(t)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	// Строки нет вовсе.
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("строки нет — не ошибка: %v", err)
	}
	// Строка без итогового URL.
	f.pool.seedRow(rwKey, "", nil)
	if err := f.w.processDelivery(context.Background(), rwDelivery("e2"), 2); err != nil {
		t.Fatalf("без итогового URL — не ошибка: %v", err)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикаций быть не должно: %+v", pubs)
	}
}

// TestRenderWorker_TarpitFinalizePublishError — сбой публикации q:analyze
// при финализации: ошибка без XACK — доставка вернётся и финализация
// повторится (at-least-once, дубль публикации ограничен попытками).
func TestRenderWorker_TarpitFinalizePublishError(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")
	f.q.publishErrs = []error{errors.New("redis недоступен")}

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err == nil {
		t.Fatal("сбой публикации q:analyze при финализации — ошибка (повторная доставка)")
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 0 {
		t.Errorf("XACK = %v — подтверждения быть не должно", acked)
	}
}

// TestRenderWorker_TarpitAttemptsBoundary — граница лимита тарпитов
// настраивается раздельно (Options.TimeoutAttempts): при 1 — финализация
// уже на первой доставке («1 попытка + сразу финализация»), при 3 —
// вторая доставка ещё повторяема, третья финализирует.
func TestRenderWorker_TarpitAttemptsBoundary(t *testing.T) {
	newFix := func(t *testing.T, limit int) *rwFixture {
		t.Helper()
		f := newRWFixture(t)
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.err = rwTarpitErr("https://spa.example/")
		var err error
		f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{TimeoutAttempts: limit})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return f
	}
	t.Run("лимит 1 — финализация на первой доставке", func(t *testing.T) {
		f := newFix(t, 1)
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
			t.Fatalf("лимит 1: первая доставка — тарпит — финализация: %v", err)
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 1 {
			t.Errorf("публикаций q:analyze = %d, нужна 1", len(pubs))
		}
	})
	t.Run("лимит 3 — вторая доставка ещё повторяема", func(t *testing.T) {
		f := newFix(t, 3)
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err == nil {
			t.Fatal("лимит 3: вторая доставка ниже лимита — ошибка повторной доставки")
		}
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 3); err != nil {
			t.Fatalf("лимит 3: третья доставка — финализация: %v", err)
		}
	})
}

// TestRenderWorker_TarpitMetrics — финализация без рендера считается
// задачей этапа, ошибкой класса task и отдельным счётчиком тарпитов.
func TestRenderWorker_TarpitMetrics(t *testing.T) {
	f := newRWFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("финализация: %v", err)
	}
	stage := map[string]string{"stage": "render"}
	if got := rwMetricValue(t, reg, "crawler_stage_tasks_total", stage); got != 1 {
		t.Errorf("crawler_stage_tasks_total = %v, want 1 (финализация — обработанная задача)", got)
	}
	labels := map[string]string{"stage": "render", "type": render.ClassTask.String()}
	if got := rwMetricValue(t, reg, "crawler_stage_errors_total", labels); got != 1 {
		t.Errorf("crawler_stage_errors_total{task} = %v, want 1", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_tarpit_finalized_total", nil); got != 1 {
		t.Errorf("crawler_render_tarpit_finalized_total = %v, want 1", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "true"}); got != 0 {
		t.Errorf("crawler_render_records_total{rendered=true} = %v, want 0 (DOM не записан)", got)
	}
}

// TestRenderWorker_TarpitTimeoutAttemptsDefault — дефолт воркера и
// конфигурации согласованы (T-148): render.timeout_attempts по умолчанию
// равен DefaultTimeoutAttempts.
func TestRenderWorker_TarpitTimeoutAttemptsDefault(t *testing.T) {
	o := Options{}.withDefaults()
	if o.TimeoutAttempts != DefaultTimeoutAttempts {
		t.Errorf("withDefaults: timeout_attempts=%d, дефолт %d", o.TimeoutAttempts, DefaultTimeoutAttempts)
	}
}

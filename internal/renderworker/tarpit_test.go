// Unit tests of the render timeout policy (T-148): a page timeout
// (render.timeout, a predictable tarpit) does not burn the overall redis.max_attempts
// delivery limit like a random error. While the delivery number (the
// PEL counter) is below render.timeout_attempts — a tarpit lives the common path of re-
// deliveries (an error without XACK); on the limit's exhaustion the entry is finalized
// without a render: the result is the raw HTML of the HTTP phase (rendered = false), a
// q:analyze publication, the delivery acknowledgment — q:dead stays for infrastructure
// errors. The fake environment — worker_test.go.
package renderworker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"crawler/internal/config"
	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/render"
)

// rwTarpitMetric — the value of crawler_render_tarpit_finalized_total{engine}
// of the build (S-P2-4): engine — the worker build's engine (the fixture's default —
// chromium, "" → chromium).
func rwTarpitMetric(t *testing.T, reg *metrics.Registry, engine string) float64 {
	t.Helper()
	return rwMetricValue(t, reg, "crawler_render_tarpit_finalized_total", map[string]string{"engine": engine})
}

// rwTarpitErr — a render error indistinguishable from the production one by classification
// (session.wrap: ClassTask + context.DeadlineExceeded in the chain — that is how
// the expiry of the overall page timeout render.timeout is marked).
func rwTarpitErr(url string) error {
	return &render.Error{Class: render.ClassTask, URL: url,
		Err: fmt.Errorf("таймаут страницы %s: %w: %w",
			25*time.Second, context.DeadlineExceeded, errors.New("navigation timeout"))}
}

// TestRenderWorker_TarpitFinalizedWithoutRender — the essence of T-148: a page
// timeout on a delivery with a number ≥ TimeoutAttempts finalizes the entry
// without a render instead of taking it to q:dead: the processing is successful (Processing
// acknowledges the delivery), the row stays with the raw HTML (rendered = false,
// content_hash unchanged), q:analyze is published (Fresh=true —
// reworking the current episode), Put/the transaction are not executed.
func TestRenderWorker_TarpitFinalizedWithoutRender(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	// Delivery #2 of 2 (DefaultTimeoutAttempts): a claim after the first
	// timeout — ProcessingAttempt acknowledges after a nil return.
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
	// The refcount is untouched: the raw HTML stays the entry's final content.
	if refc := rwRefc(f.pool)[raw]; refc != 1 {
		t.Errorf("refcount сырого HTML = %d, нужен 1 (ссылка жива)", refc)
	}
}

// TestRenderWorker_TarpitBelowLimitRetries — separate limits: a tarpit
// on a delivery with a number below TimeoutAttempts — an ordinary retryable
// error (without an XACK, it will return with a claim), as before T-148: the first attempt
// may time out because of a jam, the repeat gives the page a chance.
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

// TestRenderWorker_RetryableErrorKeepsDeadPath — retryable errors
// (browser, DB, storage, publication) are never finalized, on
// any delivery number: the former path — without an XACK, the
// redis.max_attempts exhaustion, q:dead (T-025).
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

			// The delivery number is above the tarpit limit (3 ≥ 2) — the error is
			// returned anyway: the finalization is only for timeouts.
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

// TestRenderWorker_TarpitFinalizeNoRow — there is no row (the recheck
// rewrote it) or it has no final URL: the finalization — a regular
// rejection without a publication, the delivery is acknowledged (like ProcessResult).
func TestRenderWorker_TarpitFinalizeNoRow(t *testing.T) {
	f := newRWFixture(t)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	// There is no row at all.
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("строки нет — не ошибка: %v", err)
	}
	// A row without a final URL.
	f.pool.seedRow(rwKey, "", nil)
	if err := f.w.processDelivery(context.Background(), rwDelivery("e2"), 2); err != nil {
		t.Fatalf("без итогового URL — не ошибка: %v", err)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикаций быть не должно: %+v", pubs)
	}
}

// TestRenderWorker_TarpitFinalizePublishError — a q:analyze publication
// failure during finalization: an error without an XACK — the delivery will return and the
// finalization will repeat (at-least-once, a publication duplicate is bounded by the attempts).
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

// TestRenderWorker_TarpitAttemptsBoundary — the tarpit limit boundary is
// configured separately (Options.TimeoutAttempts): at 1 — the finalization
// already on the first delivery ("1 attempt + immediate finalization"), at 3 —
// the second delivery is still retryable, the third finalizes.
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

// TestRenderWorker_TarpitMetrics — a finalization without a render is counted
// as a stage task, a task class error and a separate tarpit counter
// with the build's engine label (S-P2-4: a tarpit is an engine-branched failure,
// another engine's series is not created).
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
	if got := rwTarpitMetric(t, reg, config.EngineChromium); got != 1 {
		t.Errorf("crawler_render_tarpit_finalized_total{engine=%s} = %v, want 1", config.EngineChromium, got)
	}
	if got := rwTarpitMetric(t, reg, config.EngineMoli); got != -1 {
		t.Errorf("crawler_render_tarpit_finalized_total{engine=%s} = %v, want -1 (серия чужого движка)", config.EngineMoli, got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "true"}); got != 0 {
		t.Errorf("crawler_render_records_total{rendered=true} = %v, want 0 (DOM не записан)", got)
	}
}

// TestRenderWorker_TarpitTimeoutAttemptsDefault — the worker's default and
// the configuration are consistent (T-148): render.timeout_attempts by default
// equals DefaultTimeoutAttempts.
func TestRenderWorker_TarpitTimeoutAttemptsDefault(t *testing.T) {
	o := Options{}.withDefaults()
	if o.TimeoutAttempts != DefaultTimeoutAttempts {
		t.Errorf("withDefaults: timeout_attempts=%d, дефолт %d", o.TimeoutAttempts, DefaultTimeoutAttempts)
	}
}

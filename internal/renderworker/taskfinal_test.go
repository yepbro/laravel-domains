// Unit tests of the last attempt × task class policy (T-172): a render error
// of the task class (a connection refusal, a load error — a host property, not
// infrastructure) on the delivery with the number redis.max_attempts finalizes
// the entry without a render following the tarpit pattern T-148 instead of burying in
// q:dead: the result is an honest raw HTML of the HTTP phase (rendered = false),
// a q:analyze publication (Fresh=true), the delivery acknowledgment. Below
// the last attempt the error is retryable (host recovery is observed:
// the T-172 measurement — 6.6% of ERR_CONNECTION_REFUSED retries succeed);
// the infrastructure class (DB, storage, publication, browser, ctx cancellation)
// dies the former way — q:dead stays for real infrastructure
// errors. The fake environment — worker_test.go.
package renderworker

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crawler/internal/config"
	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/render"
)

// TestRenderWorker_TaskFinalizedOnLastAttempt — the essence of T-172, the scenario
// of the Oct 23 incident (a mixed series): a timeout on the first delivery (a tarpit
// below the T-148 limit — a retryable error), a connection refusal on the second
// (an early attempt — retryable), a refusal on the third = the last
// (redis.max_attempts) — a finalization without a render, not q:dead: the processing
// is successful (Processing acknowledges the delivery), the row stays with the raw
// HTML (rendered = false, content_hash unchanged), q:analyze is
// published (Fresh=true), Put/the transaction are not executed.
func TestRenderWorker_TaskFinalizedOnLastAttempt(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)

	// Delivery 1: a page timeout — a tarpit below TimeoutAttempts (2),
	// the T-148 territory: a re-delivery error.
	f.renderer.err = rwTarpitErr("https://spa.example/")
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err == nil {
		t.Fatal("таймаут ниже тарпит-лимита — ошибка повторной доставки (T-148)")
	}

	// Delivery 2: a connection refusal — an early attempt, the host may come up
	// (the measurement: recovery is observed) — a re-delivery error.
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED")
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err == nil {
		t.Fatal("отказ соединения до последней попытки — ошибка (хосту даётся шанс подняться)")
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Fatalf("публикаций до финализации быть не должно: %+v", pubs)
	}

	// Delivery 3 = MaxAttempts: the same error — a finalization without a render.
	err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e1"), 3, f.w.processDelivery)
	if err != nil {
		t.Fatalf("ошибка класса задачи на последней попытке обязана финализироваться без ошибки (не q:dead): %v", err)
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

// TestRenderWorker_TaskBelowLastAttemptRetries — the retries on early
// attempts are untouched: a task class error below MaxAttempts — an ordinary
// retryable error (without an XACK, it will return with a claim), as before T-172.
func TestRenderWorker_TaskBelowLastAttemptRetries(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED")

	for attempt := 1; attempt <= 2; attempt++ {
		err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e1"), attempt, f.w.processDelivery)
		if err == nil {
			t.Fatalf("attempts=%d < MaxAttempts=3: ошибка повторяема (путь q:dead не заменён раньше времени)", attempt)
		}
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 0 {
		t.Errorf("XACK = %v — до последней попытки подтверждения нет", acked)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикаций быть не должно: %+v", pubs)
	}
}

// TestRenderWorker_TaskFinalizeBoundary — the boundary is configured by the
// Options.MaxAttempts field (the production build passes redis.max_attempts): at 1
// the finalization is already on the first delivery, at 5 — the fourth is still retryable.
func TestRenderWorker_TaskFinalizeBoundary(t *testing.T) {
	newFix := func(t *testing.T, max int) *rwFixture {
		t.Helper()
		f := newRWFixture(t)
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED")
		var err error
		f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{MaxAttempts: max})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return f
	}
	t.Run("лимит 1 — финализация на первой доставке", func(t *testing.T) {
		f := newFix(t, 1)
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
			t.Fatalf("лимит 1: первая доставка — задача-класс — финализация: %v", err)
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 1 {
			t.Errorf("публикаций q:analyze = %d, нужна 1", len(pubs))
		}
	})
	t.Run("лимит 5 — четвёртая доставка ещё повторяема", func(t *testing.T) {
		f := newFix(t, 5)
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 4); err == nil {
			t.Fatal("лимит 5: четвёртая доставка ниже лимита — ошибка повторной доставки")
		}
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 5); err != nil {
			t.Fatalf("лимит 5: пятая доставка — финализация: %v", err)
		}
	})
}

// TestRenderWorker_TaskFinalizeInfraStillDies — the infrastructure class
// is not finalized on the last attempt either: it dies the former way
// (without an XACK → q:dead) — the browser, the context, the caller context's
// cancellation (not a host property), the DB/storage/publication errors (not render
// errors — no render.Error in the chain), an empty result
// (a T-064 contract violation — for the operator to triage). The publication case —
// the T-225 path (Commit went through, Publish fell): the row is written, but the task
// is not finished — it must die with an error, without a render_failed_at marker.
func TestRenderWorker_TaskFinalizeInfraStillDies(t *testing.T) {
	cases := []struct {
		name string
		set  func(f *rwFixture)
		// pubAttempted — the case tries to publish itself (the fake records
		// even a failed attempt): the "no publications" check does not apply.
		pubAttempted bool
	}{
		{"браузер", func(f *rwFixture) {
			f.renderer.err = &render.Error{Class: render.ClassBrowser, URL: "https://spa.example/", Err: errors.New("canceled")}
		}, false},
		{"контекст", func(f *rwFixture) {
			f.renderer.err = &render.Error{Class: render.ClassContext, URL: "https://spa.example/", Err: errors.New("Session not found")}
		}, false},
		{"отмена вызывающего контекста (класс task, но не свойство хоста)", func(f *rwFixture) {
			f.renderer.err = &render.Error{Class: render.ClassTask, URL: "https://spa.example/",
				Err: fmt.Errorf("задача отменена: %w: %w", context.Canceled, errors.New("canceled"))}
		}, false},
		{"сбой БД (не ошибка рендера)", func(f *rwFixture) {
			f.pool.queryErr = errors.New("база недоступна")
		}, false},
		{"сбой хранилища (не ошибка рендера)", func(f *rwFixture) {
			f.renderer.output = rwOutput("<html>dom</html>")
			f.store.putErr = errors.New("диск полон")
		}, false},
		{"пустой результат рендера (контракт T-064)", func(f *rwFixture) {
			f.renderer.output = render.RenderOutput{FinalURL: "https://spa.example/x"} // the DOM is empty
		}, false},
		{"сбой публикации q:analyze (Commit прошёл, T-225)", func(f *rwFixture) {
			f.renderer.output = rwOutput("<html>dom</html>")
			f.q.publishErrs = []error{errors.New("redis недоступен")}
		}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newRWFixture(t)
			raw := rwTestHash(1)
			f.pool.seedRow(rwKey, "https://spa.example/", &raw)
			c.set(f)

			// The last delivery (3 = MaxAttempts): the infrastructure class
			// must return — the q:dead path is the former one.
			err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e9"), 3, f.w.processDelivery)
			if err == nil {
				t.Fatal("инфраструктурная ошибка обязана возвращаться и на последней попытке (путь q:dead)")
			}
			if acked := f.q.ackedSnapshot(); len(acked) != 0 {
				t.Errorf("XACK = %v — инфраструктурная ошибка не подтверждается", acked)
			}
			if !c.pubAttempted {
				if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
					t.Errorf("публикаций быть не должно: %+v", pubs)
				}
			}
			if c.pubAttempted {
				// The T-225 path: the row is committed by the render (the Commit before
				// the publication), but not finalized "without a render" —
				// no render_failed_at marker, a re-delivery
				// will rewrite and publish again.
				row := rwRowSnapshot(f.pool, rwKey)
				if !row.rendered {
					t.Error("строка должна быть записана (Commit до сбоя публикации)")
				}
				if row.failedAt != nil {
					t.Error("маркер render_failed_at не ставится: сбой публикации — не финализация без рендера")
				}
			}
		})
	}
}

// TestRenderWorker_TaskFinalizeNoRow — there is no row (the recheck
// rewrote it) or it has no final URL: the finalization — a regular
// rejection without a publication, the delivery is acknowledged (like ProcessResult).
func TestRenderWorker_TaskFinalizeNoRow(t *testing.T) {
	f := newRWFixture(t)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED")

	// There is no row at all.
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 3); err != nil {
		t.Fatalf("строки нет — не ошибка: %v", err)
	}
	// A row without a final URL.
	f.pool.seedRow(rwKey, "", nil)
	if err := f.w.processDelivery(context.Background(), rwDelivery("e2"), 3); err != nil {
		t.Fatalf("без итогового URL — не ошибка: %v", err)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикаций быть не должно: %+v", pubs)
	}
}

// TestRenderWorker_TaskFinalizePublishError — a q:analyze publication
// failure during finalization: an error without an XACK — the delivery will return and the
// finalization will repeat (at-least-once, a publication duplicate is bounded by the attempts,
// the analyzer is idempotent by the key).
func TestRenderWorker_TaskFinalizePublishError(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED")
	f.q.publishErrs = []error{errors.New("redis недоступен")}

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 3); err == nil {
		t.Fatal("сбой публикации q:analyze при финализации — ошибка (повторная доставка)")
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 0 {
		t.Errorf("XACK = %v — подтверждения быть не должно", acked)
	}
	// The repeat is idempotent: the same finalization, the publication goes through.
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 3); err != nil {
		t.Fatalf("повторная доставка после сбоя публикации обязана финализироваться: %v", err)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 2 || pubs[0].stream != queue.StreamAnalyze || pubs[1].stream != queue.StreamAnalyze {
		t.Errorf("публикации %+v, нужны две q:analyze (до сбоя и на повторе)", pubs)
	}
	if !pubs[1].msg.Fresh {
		t.Error("q:analyze Fresh=true — доработка текущего эпизода")
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.rendered {
		t.Error("rendered должен остаться false")
	}
}

// TestRenderWorker_TaskFinalizeMetrics — a finalization on the last
// attempt is counted as a stage task, a task class error and a separate
// counter with a reason label (the net::ERR_* code); the tarpit and
// certificate counters are not touched.
func TestRenderWorker_TaskFinalizeMetrics(t *testing.T) {
	f := newRWFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED")

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 3); err != nil {
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
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total",
		map[string]string{"reason": "net::ERR_CONNECTION_REFUSED"}); got != 1 {
		t.Errorf("crawler_render_task_finalized_total{reason=net::ERR_CONNECTION_REFUSED} = %v, want 1", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total",
		map[string]string{"reason": "net::ERR_NAME_NOT_RESOLVED"}); got != -1 {
		t.Errorf("чужая метка причины = %v, want −1 (нет серии)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_tarpit_finalized_total",
		map[string]string{"engine": config.EngineChromium}); got != -1 {
		t.Errorf("crawler_render_tarpit_finalized_total = %v, want −1 (не тарпит, серия не создаётся)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_cert_finalized_total", nil); got != 0 {
		t.Errorf("crawler_render_cert_finalized_total = %v, want 0 (не cert)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "true"}); got != 0 {
		t.Errorf("crawler_render_records_total{rendered=true} = %v, want 0 (DOM не записан)", got)
	}
}

// TestRenderWorker_TaskFinalizeReason — the reason label: the
// net::ERR_* load error code, "other" for a task without a code (a page failure),
// "timeout" for a timeout not caught by the tarpit limit (the guard for builds
// with TimeoutAttempts above MaxAttempts — the config forbids it).
func TestRenderWorker_TaskFinalizeReason(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED"), "net::ERR_CONNECTION_REFUSED"},
		{rwCertErr("https://spa.example/", "net::ERR_NAME_NOT_RESOLVED"), "net::ERR_NAME_NOT_RESOLVED"},
		{&render.Error{Class: render.ClassTask, URL: "https://spa.example/", Err: errors.New("session crashed")}, "other"},
		{rwTarpitErr("https://spa.example/"), "timeout"},
		{&render.Error{Class: render.ClassContext, URL: "https://spa.example/", Err: errors.New("x")}, "other"},
	}
	for _, c := range cases {
		if got := taskFinalizeReason(c.err); got != c.want {
			t.Errorf("taskFinalizeReason(%v) = %q, want %q", c.err, got, c.want)
		}
	}
}

// TestRenderWorker_TaskFinalizeTarpitPriority — a tarpit on the last
// attempt is finalized by the T-148 tarpit branch (the tarpit counter), not by
// the T-172 policy: the branch order of processDelivery is preserved.
func TestRenderWorker_TaskFinalizeTarpitPriority(t *testing.T) {
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

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 3); err != nil {
		t.Fatalf("финализация: %v", err)
	}
	if got := rwMetricValue(t, reg, "crawler_render_tarpit_finalized_total",
		map[string]string{"engine": config.EngineChromium}); got != 1 {
		t.Errorf("crawler_render_tarpit_finalized_total = %v, want 1 (таймаут — территория T-148)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total", nil); got != -1 {
		t.Errorf("crawler_render_task_finalized_total имеет серии без причины: %v", got)
	}
}

// TestRenderWorker_DateInvalidCertFromFirstDelivery — an expired
// certificate (the T-172 extension): finalized by the cert policy from the first
// delivery (the measurement: 0 successful retries of 321), not by the last
// attempt policy and not q:dead.
func TestRenderWorker_DateInvalidCertFromFirstDelivery(t *testing.T) {
	f := newRWFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CERT_DATE_INVALID")

	if err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e1"), 1, f.w.processDelivery); err != nil {
		t.Fatalf("DATE_INVALID первой доставки — cert-финализация (не q:dead): %v", err)
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 1 || acked[0] != "e1" {
		t.Errorf("XACK = %v, нужна e1", acked)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Errorf("публикации %+v, нужна одна q:analyze", pubs)
	}
	if got := rwMetricValue(t, reg, "crawler_render_cert_finalized_total", nil); got != 1 {
		t.Errorf("crawler_render_cert_finalized_total = %v, want 1 (cert-класс)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total", nil); got != -1 {
		t.Errorf("crawler_render_task_finalized_total не должен расти: %v", got)
	}
}

// TestRenderWorker_MaxAttemptsDefault — the worker's default is consistent with
// the queue's (queue.DefaultMaxAttempts): the production build passes
// redis.max_attempts, a default mismatch would silently shift the boundary
// of finalization against the Reliab burying.
func TestRenderWorker_MaxAttemptsDefault(t *testing.T) {
	o := Options{}.withDefaults()
	if o.MaxAttempts != queue.DefaultMaxAttempts {
		t.Errorf("withDefaults: max_attempts=%d, дефолт очереди %d", o.MaxAttempts, queue.DefaultMaxAttempts)
	}
}

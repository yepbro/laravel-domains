// Unit tests of the DNS refusal policy at render (T-211, from the T-207 outcomes):
// ERR_NAME_NOT_RESOLVED — the death of the host at the DNS level. The entry got into
// q:render through a live HTTP episode (the HTTP phase resolved the name itself), and
// the browser resolves with the same system unbound — a refusal at render means
// the host's DNS died between the episode and the render; DNS recovery is a scale of days,
// the retry window (3 deliveries, ~20 min in the PEL) does not cover it (the
// T-207 measurement: 252 finalizations per ~7 h, each ate all 3 deliveries).
// The DNS class is finalized without a render from the very first delivery — following
// the cert class T-159 pattern (without the T-148 tarpit limit): the result is the raw
// HTML of the HTTP phase (rendered = false), a q:analyze publication,
// the delivery acknowledgment — q:dead stays for infrastructure errors.
// ERR_CONNECTION_REFUSED is not in the class: recovery is observed
// (the T-172 measurement: 6.6% of retries succeed) — retryable until the last delivery
// (T-172). The fake environment — worker_test.go.
package renderworker

import (
	"context"
	"errors"
	"testing"

	"github.com/chromedp/chromedp"

	"crawler/internal/config"
	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/render"
)

// TestRenderWorker_DNSFinalizedWithoutRender — the essence of T-211: a DNS refusal on
// the FIRST delivery finalizes the entry without a render (a dead host does not eat
// 3 deliveries and ~20 min of PEL): the processing is successful (Processing acknowledges
// the delivery), the row stays with the raw HTML (rendered = false,
// content_hash unchanged), q:analyze is published (Fresh=true —
// reworking the current episode), Put/the transaction are not executed; the
// render_failed_at marker (T-210) closes the pass's re-publications until the
// content changes.
func TestRenderWorker_DNSFinalizedWithoutRender(t *testing.T) {
	for _, code := range dnsErrorCodes {
		t.Run(code, func(t *testing.T) {
			f := newRWFixture(t)
			raw := rwTestHash(1)
			f.pool.seedRow(rwKey, "https://spa.example/", &raw)
			f.renderer.err = rwCertErr("https://spa.example/", code)

			// The first delivery (attempts=1) — the finalization right away, not an
			// error of a re-delivery: ProcessingAttempt acknowledges after
			// a nil return.
			err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e1"), 1, f.w.processDelivery)
			if err != nil {
				t.Fatalf("DNS-отказ первой доставки обязан финализироваться без ошибки (не q:dead): %v", err)
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
		})
	}
}

// TestRenderWorker_DNSFinalizeAnyAttempt — the finalization does not depend on the
// delivery number (like the cert class): a claim with a high attempts (a late
// fix deploy, a mixed delivery history) also finalizes instead of
// taking to q:dead.
func TestRenderWorker_DNSFinalizeAnyAttempt(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_NAME_NOT_RESOLVED")

	for attempt := 1; attempt <= 3; attempt++ {
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), attempt); err != nil {
			t.Fatalf("attempts=%d: DNS-класс финализируется на любом номере доставки: %v", attempt, err)
		}
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 3 {
		t.Errorf("публикаций q:analyze = %d, нужно 3 (по одной на доставку)", len(pubs))
	}
	if acks := len(f.q.ackedSnapshot()); acks != 0 {
		t.Errorf("XACK = %d — processDelivery не подтверждает сам (XACK — Processing)", acks)
	}
}

// TestRenderWorker_DNSFinalizeNoRow — there is no row (the recheck
// rewrote it) or it has no final URL: the finalization — a regular
// rejection without a publication, the delivery is acknowledged (like ProcessResult).
func TestRenderWorker_DNSFinalizeNoRow(t *testing.T) {
	f := newRWFixture(t)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_NAME_NOT_RESOLVED")

	// There is no row at all.
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
		t.Fatalf("строки нет — не ошибка: %v", err)
	}
	// A row without a final URL.
	f.pool.seedRow(rwKey, "", nil)
	if err := f.w.processDelivery(context.Background(), rwDelivery("e2"), 1); err != nil {
		t.Fatalf("без итогового URL — не ошибка: %v", err)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("публикаций быть не должно: %+v", pubs)
	}
}

// TestRenderWorker_DNSFinalizePublishError — a q:analyze publication
// failure during finalization: an error without an XACK — the delivery will return and the
// finalization will repeat (at-least-once, a publication duplicate is bounded by the attempts,
// the analyzer is idempotent by the key).
func TestRenderWorker_DNSFinalizePublishError(t *testing.T) {
	f := newRWFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_NAME_NOT_RESOLVED")
	f.q.publishErrs = []error{errors.New("redis недоступен")}

	// A re-delivery after the publication failure (a claim, attempts grew).
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err == nil {
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

// TestRenderWorker_DNSNotClassified — the former path is preserved for
// errors outside the DNS class: a connection refusal (the T-211 decision by the
// T-172 measurement: 6.6% of retries succeed — retryable until the last delivery, on
// the last one the T-172 policy finalizes them, not DNS), a non-resolve code in
// a foreign class (the context died — infrastructure), a page
// timeout (the T-148 territory).
func TestRenderWorker_DNSNotClassified(t *testing.T) {
	// A connection refusal below the last attempt — a retryable error:
	// exactly the T-211 decision — CONNECTION_REFUSED is not in the DNS class.
	t.Run("отказ соединения ниже последней попытки (решение T-211)", func(t *testing.T) {
		f := newRWFixture(t)
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED")

		err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e9"), 2, f.w.processDelivery)
		if err == nil {
			t.Fatal("отказ соединения до последней попытки — ошибка (хосту даётся шанс подняться, замер T-172)")
		}
		if acked := f.q.ackedSnapshot(); len(acked) != 0 {
			t.Errorf("XACK = %v — ошибка вне DNS-класса не подтверждается", acked)
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
			t.Errorf("публикаций быть не должно: %+v", pubs)
		}
		if row := rwRowSnapshot(f.pool, rwKey); row.rendered {
			t.Error("rendered должен остаться false")
		}
	})

	// A connection refusal on the last attempt — the T-172 policy
	// (the task class) finalizes it, not DNS: the counters distinguish the policies.
	t.Run("отказ соединения на последней попытке — T-172, не DNS", func(t *testing.T) {
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

		if err := f.w.processDelivery(context.Background(), rwDelivery("e9"), 3); err != nil {
			t.Fatalf("последняя попытка — финализация T-172: %v", err)
		}
		if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total",
			map[string]string{"reason": "net::ERR_CONNECTION_REFUSED"}); got != 1 {
			t.Errorf("crawler_render_task_finalized_total{refused} = %v, want 1 (политика T-172)", got)
		}
		if got := rwMetricValue(t, reg, "crawler_render_dns_finalized_total", nil); got != 0 {
			t.Errorf("crawler_render_dns_finalized_total = %v, want 0 (refused — не DNS-класс)", got)
		}
	})

	// A non-resolve code in a foreign class is not finalized on the last
	// delivery either: the context class — infrastructure (a retry makes sense).
	t.Run("код нерезолва в чужом классе", func(t *testing.T) {
		f := newRWFixture(t)
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.err = &render.Error{
			Class: render.ClassContext, URL: "https://spa.example/",
			Err: &chromedp.LoadError{ErrorText: "net::ERR_NAME_NOT_RESOLVED"}}

		err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e9"), 2, f.w.processDelivery)
		if err == nil {
			t.Fatal("код DNS в чужом классе обязан возвращаться (повтор на новом контексте осмыслен)")
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
			t.Errorf("публикаций быть не должно: %+v", pubs)
		}
	})

	// A page timeout stays the territory of T-148: the DNS recognition
	// (ProcessResult checks it after cert, before the tarpit) must not
	// swallow timeouts — below the tarpit limit it is a retryable error.
	t.Run("таймаут ниже тарпит-лимита (территория T-148)", func(t *testing.T) {
		f := newRWFixture(t)
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.err = rwTarpitErr("https://spa.example/")

		err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e1"), 1, f.w.processDelivery)
		if err == nil {
			t.Fatal("таймаут ниже тарпит-лимита — ошибка повторной доставки (T-148), не DNS-финализация")
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
			t.Errorf("публикаций быть не должно: %+v", pubs)
		}
	})
}

// TestRenderWorker_DNSMetrics — a DNS refusal finalization is counted
// as a stage task, a task class error and a separate DNS class counter;
// the tarpit, certificate and task class counters are not touched.
func TestRenderWorker_DNSMetrics(t *testing.T) {
	f := newRWFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_NAME_NOT_RESOLVED")

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
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
	if got := rwMetricValue(t, reg, "crawler_render_dns_finalized_total", nil); got != 1 {
		t.Errorf("crawler_render_dns_finalized_total = %v, want 1", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_cert_finalized_total", nil); got != 0 {
		t.Errorf("crawler_render_cert_finalized_total = %v, want 0 (DNS ≠ cert)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_tarpit_finalized_total",
		map[string]string{"engine": config.EngineChromium}); got != -1 {
		t.Errorf("crawler_render_tarpit_finalized_total = %v, want −1 (DNS ≠ тарпит, серия не создаётся)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total", nil); got != -1 {
		t.Errorf("crawler_render_task_finalized_total не должен иметь серий: %v", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "true"}); got != 0 {
		t.Errorf("crawler_render_records_total{rendered=true} = %v, want 0 (DOM не записан)", got)
	}
}

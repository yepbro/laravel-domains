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
				t.Fatalf("the first delivery DNS failure must be finalized without an error (not q:dead): %v", err)
			}

			acked := f.q.ackedSnapshot()
			if len(acked) != 1 || acked[0] != "e1" {
				t.Errorf("XACK = %v, e1 is needed (the finalization acknowledges the delivery)", acked)
			}
			row := rwRowSnapshot(f.pool, rwKey)
			if row.rendered {
				t.Error("rendered must stay false — the record outcome is the raw HTML of the HTTP phase")
			}
			if row.failedAt == nil {
				t.Error("render_failed_at must be set by the finalization (T-210) — otherwise the rework pass republishes the record")
			}
			if row.hash == nil || string(row.hash) != string(raw[:]) {
				t.Errorf("content_hash = %v, the previous raw HTML reference is needed %s", row.hash, raw)
			}
			pubs := f.q.publishesSnapshot()
			if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
				t.Fatalf("the publications %+v, one q:analyze is needed (the pipeline continues with analysis)", pubs)
			}
			if !pubs[0].msg.Fresh {
				t.Error("q:analyze Fresh=true — the finalization reworks the current episode, like a successful render")
			}
			if pubs[0].msg.ZoneID != rwTask.ZoneID || pubs[0].msg.DomainID != rwTask.DomainID ||
				pubs[0].msg.SubdomainID != rwTask.SubdomainID || pubs[0].msg.Variant != rwTask.Variant {
				t.Errorf("the q:analyze key %+v does not match the task", pubs[0].msg)
			}
			if puts := f.store.putsSnapshot(); len(puts) != 0 {
				t.Errorf("Put must not be called: %d", len(puts))
			}
			if i := f.pool.j.indexOf(rwEvBegin); i >= 0 {
				t.Errorf("a transaction must not open (an event on %d)", i)
			}
			// The refcount is untouched: the raw HTML stays the entry's final content.
			if refc := rwRefc(f.pool)[raw]; refc != 1 {
				t.Errorf("the raw HTML refcount = %d, 1 needed (the reference is alive)", refc)
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
			t.Fatalf("attempts=%d: the DNS class is finalized at any delivery number: %v", attempt, err)
		}
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 3 {
		t.Errorf("q:analyze publications = %d, 3 needed (one per delivery)", len(pubs))
	}
	if acks := len(f.q.ackedSnapshot()); acks != 0 {
		t.Errorf("XACK = %d — processDelivery does not acknowledge itself (the XACK is Processings)", acks)
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
		t.Fatalf("no row — not an error: %v", err)
	}
	// A row without a final URL.
	f.pool.seedRow(rwKey, "", nil)
	if err := f.w.processDelivery(context.Background(), rwDelivery("e2"), 1); err != nil {
		t.Fatalf("no final URL — not an error: %v", err)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("there must be no publications: %+v", pubs)
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
	f.q.publishErrs = []error{errors.New("redis unavailable")}

	// A re-delivery after the publication failure (a claim, attempts grew).
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err == nil {
		t.Fatal("a q:analyze publication failure at the finalization — an error (a re-delivery)")
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 0 {
		t.Errorf("XACK = %v — there must be no acknowledgment", acked)
	}
	// The repeat is idempotent: the same finalization, the publication goes through.
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 3); err != nil {
		t.Fatalf("the re-delivery after a publication failure must be finalized: %v", err)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 2 || pubs[0].stream != queue.StreamAnalyze || pubs[1].stream != queue.StreamAnalyze {
		t.Errorf("the publications %+v, two q:analyze needed (before the failure and on the repeat)", pubs)
	}
	if !pubs[1].msg.Fresh {
		t.Error("q:analyze Fresh=true — the current episode rework")
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.rendered {
		t.Error("rendered must stay false")
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
	t.Run("a connection refusal below the last attempt (the T-211 decision)", func(t *testing.T) {
		f := newRWFixture(t)
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED")

		err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e9"), 2, f.w.processDelivery)
		if err == nil {
			t.Fatal("a connection refusal before the last attempt — an error (the host is given a chance to come up, the T-172 measurement)")
		}
		if acked := f.q.ackedSnapshot(); len(acked) != 0 {
			t.Errorf("XACK = %v — an error outside the DNS class is not acknowledged", acked)
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
			t.Errorf("there must be no publications: %+v", pubs)
		}
		if row := rwRowSnapshot(f.pool, rwKey); row.rendered {
			t.Error("rendered must stay false")
		}
	})

	// A connection refusal on the last attempt — the T-172 policy
	// (the task class) finalizes it, not DNS: the counters distinguish the policies.
	t.Run("a connection refusal on the last attempt — T-172, not DNS", func(t *testing.T) {
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
			t.Fatalf("the last attempt — the T-172 finalization: %v", err)
		}
		if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total",
			map[string]string{"reason": "net::ERR_CONNECTION_REFUSED"}); got != 1 {
			t.Errorf("crawler_render_task_finalized_total{refused} = %v, want 1 (the T-172 policy)", got)
		}
		if got := rwMetricValue(t, reg, "crawler_render_dns_finalized_total", nil); got != 0 {
			t.Errorf("crawler_render_dns_finalized_total = %v, want 0 (refused is not the DNS class)", got)
		}
	})

	// A non-resolve code in a foreign class is not finalized on the last
	// delivery either: the context class — infrastructure (a retry makes sense).
	t.Run("a non-resolve code in a foreign class", func(t *testing.T) {
		f := newRWFixture(t)
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.err = &render.Error{
			Class: render.ClassContext, URL: "https://spa.example/",
			Err: &chromedp.LoadError{ErrorText: "net::ERR_NAME_NOT_RESOLVED"}}

		err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e9"), 2, f.w.processDelivery)
		if err == nil {
			t.Fatal("a DNS code in a foreign class must be returned (a retry on a new context is meaningful)")
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
			t.Errorf("there must be no publications: %+v", pubs)
		}
	})

	// A page timeout stays the territory of T-148: the DNS recognition
	// (ProcessResult checks it after cert, before the tarpit) must not
	// swallow timeouts — below the tarpit limit it is a retryable error.
	t.Run("a timeout below the tarpit limit (the T-148 territory)", func(t *testing.T) {
		f := newRWFixture(t)
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.err = rwTarpitErr("https://spa.example/")

		err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e1"), 1, f.w.processDelivery)
		if err == nil {
			t.Fatal("a timeout below the tarpit limit — a re-delivery error (T-148), not a DNS finalization")
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
			t.Errorf("there must be no publications: %+v", pubs)
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
		t.Fatalf("finalization: %v", err)
	}
	stage := map[string]string{"stage": "render"}
	if got := rwMetricValue(t, reg, "crawler_stage_tasks_total", stage); got != 1 {
		t.Errorf("crawler_stage_tasks_total = %v, want 1 (a finalization is a processed task)", got)
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
		t.Errorf("crawler_render_tarpit_finalized_total = %v, want −1 (DNS ≠ tarpit, the series is not created)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total", nil); got != -1 {
		t.Errorf("crawler_render_task_finalized_total must have no series: %v", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "true"}); got != 0 {
		t.Errorf("crawler_render_records_total{rendered=true} = %v, want 0 (the DOM is not written)", got)
	}
}

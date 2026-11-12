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
		Err: fmt.Errorf("page timeout %s: %w: %w",
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
		t.Fatalf("the tarpit at the limit exhaustion must be finalized without an error (not q:dead): %v", err)
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
		t.Fatal("a tarpit before the limit exhaustion — an error: the delivery will come back with a re-delivery")
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 0 {
		t.Errorf("XACK = %v — before the limit exhaustion there is no acknowledgment", acked)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("there must be no publications: %+v", pubs)
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
		{"browser", func(f *rwFixture) {
			f.renderer.err = &render.Error{Class: render.ClassBrowser, URL: "https://spa.example/", Err: errors.New("canceled")}
		}},
		{"context", func(f *rwFixture) {
			f.renderer.err = &render.Error{Class: render.ClassContext, URL: "https://spa.example/", Err: errors.New("Session not found")}
		}},
		{"the caller context cancellation (not a tarpit)", func(f *rwFixture) {
			f.renderer.err = &render.Error{Class: render.ClassTask, URL: "https://spa.example/",
				Err: fmt.Errorf("the task is cancelled: %w: %w", context.Canceled, errors.New("canceled"))}
		}},
		{"a DB failure", func(f *rwFixture) {
			f.pool.queryErr = errors.New("the database is unavailable")
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
				t.Fatal("a retryable error must be returned (the q:dead path is the same as before)")
			}
			if acked := f.q.ackedSnapshot(); len(acked) != 0 {
				t.Errorf("XACK = %v — a retryable error is not acknowledged", acked)
			}
			if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
				t.Errorf("there must be no publications: %+v", pubs)
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
		t.Fatalf("no row — not an error: %v", err)
	}
	// A row without a final URL.
	f.pool.seedRow(rwKey, "", nil)
	if err := f.w.processDelivery(context.Background(), rwDelivery("e2"), 2); err != nil {
		t.Fatalf("no final URL — not an error: %v", err)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("there must be no publications: %+v", pubs)
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
	f.q.publishErrs = []error{errors.New("redis unavailable")}

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err == nil {
		t.Fatal("a q:analyze publication failure at the finalization — an error (a re-delivery)")
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 0 {
		t.Errorf("XACK = %v — there must be no acknowledgment", acked)
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
	t.Run("limit 1 — a finalization on the first delivery", func(t *testing.T) {
		f := newFix(t, 1)
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
			t.Fatalf("limit 1: the first delivery — a tarpit — a finalization: %v", err)
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 1 {
			t.Errorf("q:analyze publications = %d, 1 is needed", len(pubs))
		}
	})
	t.Run("limit 3 — the second delivery is still retryable", func(t *testing.T) {
		f := newFix(t, 3)
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err == nil {
			t.Fatal("limit 3: the second delivery is below the limit — a re-delivery error")
		}
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 3); err != nil {
			t.Fatalf("limit 3: the third delivery — a finalization: %v", err)
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
	if got := rwTarpitMetric(t, reg, config.EngineChromium); got != 1 {
		t.Errorf("crawler_render_tarpit_finalized_total{engine=%s} = %v, want 1", config.EngineChromium, got)
	}
	if got := rwTarpitMetric(t, reg, config.EngineMoli); got != -1 {
		t.Errorf("crawler_render_tarpit_finalized_total{engine=%s} = %v, want -1 (a series of a foreign engine)", config.EngineMoli, got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "true"}); got != 0 {
		t.Errorf("crawler_render_records_total{rendered=true} = %v, want 0 (the DOM is not written)", got)
	}
}

// TestRenderWorker_TarpitTimeoutAttemptsDefault — the worker's default and
// the configuration are consistent (T-148): render.timeout_attempts by default
// equals DefaultTimeoutAttempts.
func TestRenderWorker_TarpitTimeoutAttemptsDefault(t *testing.T) {
	o := Options{}.withDefaults()
	if o.TimeoutAttempts != DefaultTimeoutAttempts {
		t.Errorf("withDefaults: timeout_attempts=%d, the default %d", o.TimeoutAttempts, DefaultTimeoutAttempts)
	}
}

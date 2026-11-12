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
		t.Fatal("a timeout below the tarpit limit — a re-delivery error (T-148)")
	}

	// Delivery 2: a connection refusal — an early attempt, the host may come up
	// (the measurement: recovery is observed) — a re-delivery error.
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED")
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err == nil {
		t.Fatal("a connection refusal before the last attempt — an error (the host is given a chance to come up)")
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Fatalf("there must be no publications before the finalization: %+v", pubs)
	}

	// Delivery 3 = MaxAttempts: the same error — a finalization without a render.
	err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", rwDelivery("e1"), 3, f.w.processDelivery)
	if err != nil {
		t.Fatalf("the task class error on the last attempt must be finalized without an error (not q:dead): %v", err)
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
			t.Fatalf("attempts=%d < MaxAttempts=3: the error is retryable (the q:dead path is not replaced prematurely)", attempt)
		}
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 0 {
		t.Errorf("XACK = %v — before the last attempt there is no acknowledgment", acked)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("there must be no publications: %+v", pubs)
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
	t.Run("limit 1 — a finalization on the first delivery", func(t *testing.T) {
		f := newFix(t, 1)
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
			t.Fatalf("limit 1: the first delivery — the task class — a finalization: %v", err)
		}
		if pubs := f.q.publishesSnapshot(); len(pubs) != 1 {
			t.Errorf("q:analyze publications = %d, 1 is needed", len(pubs))
		}
	})
	t.Run("limit 5 — the fourth delivery is still retryable", func(t *testing.T) {
		f := newFix(t, 5)
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 4); err == nil {
			t.Fatal("limit 5: the fourth delivery is below the limit — a re-delivery error")
		}
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 5); err != nil {
			t.Fatalf("limit 5: the fifth delivery — a finalization: %v", err)
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
		{"browser", func(f *rwFixture) {
			f.renderer.err = &render.Error{Class: render.ClassBrowser, URL: "https://spa.example/", Err: errors.New("canceled")}
		}, false},
		{"context", func(f *rwFixture) {
			f.renderer.err = &render.Error{Class: render.ClassContext, URL: "https://spa.example/", Err: errors.New("Session not found")}
		}, false},
		{"the caller context cancellation (the task class, but not a host property)", func(f *rwFixture) {
			f.renderer.err = &render.Error{Class: render.ClassTask, URL: "https://spa.example/",
				Err: fmt.Errorf("the task is cancelled: %w: %w", context.Canceled, errors.New("canceled"))}
		}, false},
		{"a DB failure (not a render error)", func(f *rwFixture) {
			f.pool.queryErr = errors.New("the database is unavailable")
		}, false},
		{"a store failure (not a render error)", func(f *rwFixture) {
			f.renderer.output = rwOutput("<html>dom</html>")
			f.store.putErr = errors.New("the disk is full")
		}, false},
		{"an empty render result (the T-064 contract)", func(f *rwFixture) {
			f.renderer.output = render.RenderOutput{FinalURL: "https://spa.example/x"} // the DOM is empty
		}, false},
		{"a q:analyze publication failure (the Commit passed, T-225)", func(f *rwFixture) {
			f.renderer.output = rwOutput("<html>dom</html>")
			f.q.publishErrs = []error{errors.New("redis unavailable")}
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
				t.Fatal("an infra error must be returned on the last attempt too (the q:dead path)")
			}
			if acked := f.q.ackedSnapshot(); len(acked) != 0 {
				t.Errorf("XACK = %v — an infra error is not acknowledged", acked)
			}
			if !c.pubAttempted {
				if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
					t.Errorf("there must be no publications: %+v", pubs)
				}
			}
			if c.pubAttempted {
				// The T-225 path: the row is committed by the render (the Commit before
				// the publication), but not finalized "without a render" —
				// no render_failed_at marker, a re-delivery
				// will rewrite and publish again.
				row := rwRowSnapshot(f.pool, rwKey)
				if !row.rendered {
					t.Error("the row must be written (the Commit precedes the publication failure)")
				}
				if row.failedAt != nil {
					t.Error("the render_failed_at marker is not set: a publication failure is not a finalize-without-render")
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
		t.Fatalf("no row — not an error: %v", err)
	}
	// A row without a final URL.
	f.pool.seedRow(rwKey, "", nil)
	if err := f.w.processDelivery(context.Background(), rwDelivery("e2"), 3); err != nil {
		t.Fatalf("no final URL — not an error: %v", err)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("there must be no publications: %+v", pubs)
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
	f.q.publishErrs = []error{errors.New("redis unavailable")}

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 3); err == nil {
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
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total",
		map[string]string{"reason": "net::ERR_CONNECTION_REFUSED"}); got != 1 {
		t.Errorf("crawler_render_task_finalized_total{reason=net::ERR_CONNECTION_REFUSED} = %v, want 1", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total",
		map[string]string{"reason": "net::ERR_NAME_NOT_RESOLVED"}); got != -1 {
		t.Errorf("a foreign reason label = %v, want −1 (no series)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_tarpit_finalized_total",
		map[string]string{"engine": config.EngineChromium}); got != -1 {
		t.Errorf("crawler_render_tarpit_finalized_total = %v, want −1 (not a tarpit, the series is not created)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_cert_finalized_total", nil); got != 0 {
		t.Errorf("crawler_render_cert_finalized_total = %v, want 0 (not cert)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_records_total", map[string]string{"rendered": "true"}); got != 0 {
		t.Errorf("crawler_render_records_total{rendered=true} = %v, want 0 (the DOM is not written)", got)
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
		t.Fatalf("finalization: %v", err)
	}
	if got := rwMetricValue(t, reg, "crawler_render_tarpit_finalized_total",
		map[string]string{"engine": config.EngineChromium}); got != 1 {
		t.Errorf("crawler_render_tarpit_finalized_total = %v, want 1 (a timeout is the T-148 territory)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total", nil); got != -1 {
		t.Errorf("crawler_render_task_finalized_total has series without a reason: %v", got)
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
		t.Fatalf("the first delivery DATE_INVALID — a cert finalization (not q:dead): %v", err)
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 1 || acked[0] != "e1" {
		t.Errorf("XACK = %v, e1 is needed", acked)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Errorf("the publications %+v, one q:analyze is needed", pubs)
	}
	if got := rwMetricValue(t, reg, "crawler_render_cert_finalized_total", nil); got != 1 {
		t.Errorf("crawler_render_cert_finalized_total = %v, want 1 (the cert class)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total", nil); got != -1 {
		t.Errorf("crawler_render_task_finalized_total must not grow: %v", got)
	}
}

// TestRenderWorker_MaxAttemptsDefault — the worker's default is consistent with
// the queue's (queue.DefaultMaxAttempts): the production build passes
// redis.max_attempts, a default mismatch would silently shift the boundary
// of finalization against the Reliab burying.
func TestRenderWorker_MaxAttemptsDefault(t *testing.T) {
	o := Options{}.withDefaults()
	if o.MaxAttempts != queue.DefaultMaxAttempts {
		t.Errorf("withDefaults: max_attempts=%d, the queue default %d", o.MaxAttempts, queue.DefaultMaxAttempts)
	}
}

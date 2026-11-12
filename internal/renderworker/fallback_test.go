// Unit tests of the fallback publication from the finalization paths without a render
// (T-238, spec §4.4 items 1–2/5, FR-7/8/10/11/14): the tarpit T-148 and the task
// class T-172 on the last attempt with render.fallback.enabled on
// publish the task to q:render-fallback instead of the
// render_failed_at marker (the marker and q:analyze — to worker B after the fallback is
// exhausted, FR-10); the DNS class T-211 and the cert class T-159 get no
// fallback (engine-independent reasons, FR-7); the fallback off —
// yesterday's behavior (the marker + q:analyze); a publication failure — an error
// without a marker, the delivery to the redelivery; a re-delivery of an already
// published row does not duplicate the stream (the filter by the result
// row identifier). The fake environment — worker_test.go.
package renderworker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/config"
	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/render"
)

// rwFBCandidateEngine — the candidate engine of the worker A build in the
// fallback tests (S-P1-1): the fallbackArmed engine guard does not publish with
// chromium (acceptance step 1, spec §6.2, the §6.4 rollback), so the
// armed fallback fixtures build the primary worker with a candidate engine
// — the engine here is only a field, the fake decides the render.
const rwFBCandidateEngine = config.EngineMoli

// newRWFallbackFixture — a fixture with the fallback on
// (FallbackOptions.Enabled, T-238) and the build's candidate engine (S-P1-1):
// the other options — defaults (TimeoutAttempts 2, MaxAttempts 3, the Stream
// q:render); the zero gate/budget thresholds (T-239) — the states are off,
// the decision publishes without caps (the always-publish stub behavior for
// the T-238 tests).
func newRWFallbackFixture(t *testing.T) *rwFixture {
	t.Helper()
	f := newRWFixture(t)
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{Fallback: FallbackOptions{Enabled: true}, Engine: rwFBCandidateEngine})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return f
}

// rwFallbackPublishes — the publications to q:render-fallback from all
// recorded ones (the checks helper).
func rwFallbackPublishes(t *testing.T, f *rwFixture) []rwPublishRec {
	t.Helper()
	var out []rwPublishRec
	for _, p := range f.q.publishesSnapshot() {
		if p.stream == queue.StreamRenderFallback {
			out = append(out, p)
		}
	}
	return out
}

// TestRenderWorker_TarpitFallbackPublish — a tarpit on the limit's
// exhaustion with the fallback on: instead of the marker and q:analyze the task
// is published to q:render-fallback (Fresh=true, the row key), the row is not
// touched (rendered=false, the old content_hash, render_failed_at
// NULL — "awaiting fallback", spec §4.4 item 1), the delivery is acknowledged,
// Put/the transaction are not executed, the
// crawler_render_fallback_published_total{timeout} metric grows, the
// finalization counters — no.
func TestRenderWorker_TarpitFallbackPublish(t *testing.T) {
	f := newRWFallbackFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{Fallback: FallbackOptions{Enabled: true}, Engine: rwFBCandidateEngine, Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	// Delivery #2 of 2 (DefaultTimeoutAttempts).
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("a tarpit with the fallback — a publication, not an error: %v", err)
	}
	pubs := rwFallbackPublishes(t, f)
	if len(pubs) != 1 {
		t.Fatalf("q:render-fallback publications: %d, 1 is needed (in total %+v)", len(pubs), f.q.publishesSnapshot())
	}
	if !pubs[0].msg.Fresh {
		t.Error("the fallback message Fresh=true — the current episode rework, like the finalization")
	}
	if pubs[0].msg.ZoneID != rwTask.ZoneID || pubs[0].msg.DomainID != rwTask.DomainID ||
		pubs[0].msg.SubdomainID != rwTask.SubdomainID || pubs[0].msg.Variant != rwTask.Variant {
		t.Errorf("the fallback key %+v does not match the task", pubs[0].msg)
	}
	// Neither q:analyze nor a marker: the entry waits for worker B (FR-10).
	for _, p := range f.q.publishesSnapshot() {
		if p.stream == queue.StreamAnalyze {
			t.Errorf("q:analyze is published by worker B after the fallback, not the primary: %+v", p)
		}
	}
	row := rwRowSnapshot(f.pool, rwKey)
	if row.failedAt != nil {
		t.Error("the render_failed_at marker must not be set — the record waits for the fallback (FR-10), worker B will set the marker")
	}
	if row.rendered {
		t.Error("rendered must stay false")
	}
	if row.hash == nil || string(row.hash) != string(raw[:]) {
		t.Errorf("content_hash = %v, the previous raw HTML reference is needed %s", row.hash, raw)
	}
	if puts := f.store.putsSnapshot(); len(puts) != 0 {
		t.Errorf("Put must not be called: %d", len(puts))
	}
	if i := f.pool.j.indexOf(rwEvBegin); i >= 0 {
		t.Errorf("a transaction must not open (an event on %d)", i)
	}
	if refc := rwRefc(f.pool)[raw]; refc != 1 {
		t.Errorf("the raw HTML refcount = %d, 1 needed (the reference is alive)", refc)
	}
	// The metrics: a publication with the timeout reason, no finalizations.
	if got := rwMetricValue(t, reg, "crawler_render_fallback_published_total",
		map[string]string{"reason": "timeout"}); got != 1 {
		t.Errorf("crawler_render_fallback_published_total{timeout} = %v, want 1", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_tarpit_finalized_total",
		map[string]string{"engine": rwFBCandidateEngine}); got != -1 {
		t.Errorf("crawler_render_tarpit_finalized_total = %v, want -1 (not a finalization — the series is not created)", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total", nil); got != -1 {
		t.Errorf("crawler_render_task_finalized_total must have no series: %v", got)
	}
}

// TestRenderWorker_TaskFallbackPublish — the task class on the last
// attempt with the fallback on: the same publication to q:render-fallback
// instead of the marker, the reason label — the net::ERR_* code (the
// taskFinalizeReason vocabulary, spec §4.6 item 2).
func TestRenderWorker_TaskFallbackPublish(t *testing.T) {
	f := newRWFallbackFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{Fallback: FallbackOptions{Enabled: true}, Engine: rwFBCandidateEngine, Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED")

	// Delivery #3 of 3 (DefaultMaxAttempts): after the tarpit branch —
	// the T-172 last attempt branch.
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 3); err != nil {
		t.Fatalf("the task class on the last attempt with the fallback — a publication, not an error: %v", err)
	}
	pubs := rwFallbackPublishes(t, f)
	if len(pubs) != 1 {
		t.Fatalf("q:render-fallback publications: %d, 1 is needed (in total %+v)", len(pubs), f.q.publishesSnapshot())
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("the render_failed_at marker must not be set — the record waits for the fallback (FR-10)")
	}
	if got := rwMetricValue(t, reg, "crawler_render_fallback_published_total",
		map[string]string{"reason": "net::ERR_CONNECTION_REFUSED"}); got != 1 {
		t.Errorf("crawler_render_fallback_published_total{net::ERR_CONNECTION_REFUSED} = %v, want 1", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total", nil); got != -1 {
		t.Errorf("crawler_render_task_finalized_total must have no series: %v", got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_cert_finalized_total", nil); got != 0 {
		t.Errorf("crawler_render_cert_finalized_total = %v, want 0 (not a cert finalization)", got)
	}
}

// TestRenderWorker_FallbackDisabledTodayBehavior — the fallback off
// (the default, FR-14): the finalizations work as today — the
// render_failed_at marker, a q:analyze publication, not a single entry in
// q:render-fallback.
func TestRenderWorker_FallbackDisabledTodayBehavior(t *testing.T) {
	f := newRWFixture(t) // Options{} — the fallback is off
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("a tarpit without the fallback — a finalization: %v", err)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("the publications %+v, one q:analyze is needed (the behavior before T-238)", pubs)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt == nil {
		t.Error("the render_failed_at marker must be set (T-210) with the fallback off")
	}
	if fb := rwFallbackPublishes(t, f); len(fb) != 0 {
		t.Errorf("there must be no q:render-fallback publications: %+v", fb)
	}
}

// TestRenderWorker_FallbackChromiumEngineNotArmed — the engine guard
// of the publication (S-P1-1, spec §6.2 item 2/§6.4 item 1, FR-14): on the build engine
// chromium — including an empty Options.Engine value (builds without
// configuration) — no fallback is published even with
// render.fallback.enabled on: the tarpit T-148 and the task class T-172 on
// the last delivery are finalized as with the fallback off
// (the render_failed_at marker + q:analyze), the publications metric creates no
// series.
// Acceptance step 1 (enabling the fallback without changing the engine) leaves
// q:render-fallback empty; a rollback to chromium puts the publication out by itself.
func TestRenderWorker_FallbackChromiumEngineNotArmed(t *testing.T) {
	cases := []struct {
		name   string
		engine string
	}{
		{"the chromium engine", config.EngineChromium},
		{"no engine set — the chromium default", ""},
	}
	paths := []struct {
		name    string
		err     error
		attempt int
		reason  string
	}{
		{
			name:    "the tarpit T-148 (the render.timeout_attempts exhaustion)",
			err:     rwTarpitErr("https://spa.example/"),
			attempt: 2, // DefaultTimeoutAttempts
			reason:  "timeout",
		},
		{
			name:    "the task class T-172 (the last delivery)",
			err:     rwCertErr("https://spa.example/", "net::ERR_CONNECTION_REFUSED"),
			attempt: 3, // DefaultMaxAttempts
			reason:  "net::ERR_CONNECTION_REFUSED",
		},
	}
	for _, c := range cases {
		for _, p := range paths {
			t.Run(c.name+" × "+p.name, func(t *testing.T) {
				f := newRWFixture(t)
				reg := metrics.New("render-worker")
				var err error
				f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
					Fallback: FallbackOptions{Enabled: true},
					Engine:   c.engine,
					Metrics:  reg,
				})
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				raw := rwTestHash(1)
				f.pool.seedRow(rwKey, "https://spa.example/", &raw)
				f.renderer.err = p.err

				if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), p.attempt); err != nil {
					t.Fatalf("chromium: the candidate — a normal finalization, not an error: %v", err)
				}
				if fb := rwFallbackPublishes(t, f); len(fb) != 0 {
					t.Errorf("there must be no q:render-fallback publications: %+v", fb)
				}
				pubs := f.q.publishesSnapshot()
				if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
					t.Fatalf("the publications %+v, one q:analyze is needed (as with the fallback off)", pubs)
				}
				if row := rwRowSnapshot(f.pool, rwKey); row.failedAt == nil {
					t.Error("the render_failed_at marker must be set — chromium does not publish, the finalization as today")
				}
				if got := rwMetricValue(t, reg, "crawler_render_fallback_published_total",
					map[string]string{"reason": p.reason}); got != -1 {
					t.Errorf("crawler_render_fallback_published_total{%s} = %v, want -1 (there were no publications)", p.reason, got)
				}
			})
		}
	}
}

// TestRenderWorker_DNSCertNoFallback — the engine-independent reasons
// (FR-7): the DNS class T-211 and the cert class T-159 are finalized as today
// — the marker + q:analyze — and get no fallback even with the flag on:
// Chromium will not fix a host DNS refusal or certificate error (spec §4.4
// item 2).
func TestRenderWorker_DNSCertNoFallback(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"the DNS class (T-211)", rwCertErr("https://spa.example/", "net::ERR_NAME_NOT_RESOLVED")},
		{"the cert class (T-159)", rwCertErr("https://spa.example/", "net::ERR_CERT_AUTHORITY_INVALID")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newRWFallbackFixture(t)
			raw := rwTestHash(1)
			f.pool.seedRow(rwKey, "https://spa.example/", &raw)
			f.renderer.err = c.err

			// Both classes are finalized from the very first delivery.
			if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
				t.Fatalf("an engine-independent class — a finalization from the first delivery: %v", err)
			}
			if fb := rwFallbackPublishes(t, f); len(fb) != 0 {
				t.Errorf("no fallback is due: %+v", fb)
			}
			pubs := f.q.publishesSnapshot()
			if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
				t.Fatalf("the publications %+v, one q:analyze is needed (the finalization as today)", pubs)
			}
			if row := rwRowSnapshot(f.pool, rwKey); row.failedAt == nil {
				t.Error("the render_failed_at marker must be set (T-210)")
			}
		})
	}
}

// TestRenderWorker_FallbackNoRecursionWorkerB — the recursion ban
// (FR-10, spec §4.5 item 2): worker B — the same binary with the consumption
// stream q:render-fallback; its engine-branched failure on the attempts' exhaustion
// is finalized with the marker as today, a fallback of the fallback is not published —
// the stream is always one, the engine is always chromium.
func TestRenderWorker_FallbackNoRecursionWorkerB(t *testing.T) {
	f := newRWFallbackFixture(t)
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
		Fallback: FallbackOptions{Enabled: true},
		Stream:   queue.StreamRenderFallback, // the worker B build (T-236)
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("worker B: the tarpit at the exhaustion — a finalization (not a recursive fallback): %v", err)
	}
	if fb := rwFallbackPublishes(t, f); len(fb) != 0 {
		t.Errorf("a recursive fallback is forbidden: %+v", fb)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("the publications %+v, one q:analyze is needed (the worker B finalization, spec §4.5 item 2)", pubs)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt == nil {
		t.Error("worker B sets the marker after the fallback is exhausted (FR-10)")
	}
}

// TestRenderWorker_FallbackPublishErrorNoMarker — the transactionality
// of the "publication → acknowledgment" composition (spec §4.4 item 1, FR-10): a
// q:render-fallback publication failure — an error without an XACK (the delivery to
// the redelivery), the marker is NOT set, the row did not change; a re-delivery
// publishes (the duplicate record is removed on failure — the task is not lost
// between the filter and the stream).
func TestRenderWorker_FallbackPublishErrorNoMarker(t *testing.T) {
	f := newRWFallbackFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")
	f.q.publishErrs = []error{errors.New("redis unavailable")}

	// A publication failure: an error, the delivery will return (a redelivery).
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err == nil {
		t.Fatal("a q:render-fallback publication failure — an error (a re-delivery)")
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("the render_failed_at marker is not set: the record is not finalized — the publication did not happen")
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.rendered {
		t.Error("rendered must stay false")
	}

	// A re-delivery: the publication goes through, still no marker.
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("the repeat after a publication failure must publish the fallback: %v", err)
	}
	pubs := f.q.publishesSnapshot()
	fb := 0
	for _, p := range pubs {
		switch p.stream {
		case queue.StreamRenderFallback:
			fb++
		case queue.StreamAnalyze:
			t.Error("q:analyze is published by worker B, not the primary worker")
		}
	}
	if fb != 2 {
		// The first (failed) attempt is also recorded by the fake: there is one
		// successful publication — the dedup filter did not eat the retry after the failure.
		t.Errorf("q:render-fallback publication attempts = %d, 2 needed (a failure + a success), one in the stream", fb)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("the marker is not set after a successful fallback publication either (FR-10)")
	}
}

// TestRenderWorker_FallbackDuplicateDelivery — the publication
// idempotency (spec §4.4 item 5, FR-11): a re-delivery of an already published
// row (an XACK failure after the publication) does not give a second entry in the stream —
// the filter by the result row identifier; the delivery is acknowledged
// without a marker and without q:analyze, the publications metric does not grow (there
// was no publication).
func TestRenderWorker_FallbackDuplicateDelivery(t *testing.T) {
	f := newRWFallbackFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{Fallback: FallbackOptions{Enabled: true}, Engine: rwFBCandidateEngine, Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	// The first delivery: a fallback publication.
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("the fallback publication: %v", err)
	}
	// A duplicate: the same key, the same content (the XACK lost after the publication).
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("a duplicate delivery is acknowledged without an error: %v", err)
	}
	pubs := f.q.publishesSnapshot()
	fb := 0
	for _, p := range pubs {
		if p.stream == queue.StreamRenderFallback {
			fb++
		}
	}
	if fb != 1 {
		t.Errorf("q:render-fallback records = %d, 1 is needed (a duplicate delivery does not duplicate the stream)", fb)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("the marker is not set on the publication or on the duplicate (FR-10)")
	}
	if got := rwMetricValue(t, reg, "crawler_render_fallback_published_total",
		map[string]string{"reason": "timeout"}); got != 1 {
		t.Errorf("the publication metric = %v, want 1 (a duplicate does not count as a publication)", got)
	}
}

// TestRenderWorker_FallbackNewEpisodeNotFiltered — the filter identifier —
// the row key + the episode's checked_at: a new episode (the recheck
// T-058 rewrote the row) — a legitimate new candidate, the filter does not
// know it; in particular the content-addressed content_hash does NOT identify
// the episode — a new episode with byte-identical content (the hash matched,
// the T-238 P1 review) must go through the fallback again.
func TestRenderWorker_FallbackNewEpisodeNotFiltered(t *testing.T) {
	f := newRWFallbackFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("the fallback publication (episode 1): %v", err)
	}
	// A new episode: the T-058 row rewrite — a new checked_at; the content
	// is byte-identical (the content_hash matched — the content dedup T-056):
	// the old filter key must not recognize the row.
	fresh := time.Now().Add(time.Hour)
	f.pool.rows[rwKey].checkedAt = fresh

	if err := f.w.processDelivery(context.Background(), rwDelivery("e2"), 2); err != nil {
		t.Fatalf("the fallback publication (episode 2, the same content): %v", err)
	}
	fb := 0
	for _, p := range f.q.publishesSnapshot() {
		if p.stream == queue.StreamRenderFallback {
			fb++
		}
	}
	if fb != 2 {
		t.Errorf("q:render-fallback records = %d, 2 needed (the episodes differ by checked_at, not by the hash)", fb)
	}
}

// TestRenderWorker_FallbackRenderedRowSkipsPublish — the row is already
// rendered (worker B reworked it between the publication and the late
// delivery duplicate; a concurrent render): no publication needed — the delivery
// is acknowledged without a marker, without q:analyze and without a second entry in
// the fallback (the window of an extra repeated render is closed, the T-238 P3 review).
func TestRenderWorker_FallbackRenderedRowSkipsPublish(t *testing.T) {
	f := newRWFallbackFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.pool.rows[rwKey].rendered = true // worker B reworked it
	f.renderer.err = rwTarpitErr("https://spa.example/")

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("a rendered row — not an error: %v", err)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("there must be no publications (the row is ready): %+v", pubs)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("a rendered row needs no marker")
	}
}

// TestRenderWorker_FallbackNoRowNoURL — there is no row or no final
// URL: the fallback has nothing to rework — a regular rejection without
// publications (like the finalization), the delivery is acknowledged. Both levels:
// the processDelivery composite (the early ProcessResult rejection) and the direct
// publishFallback guards (the row disappeared between the render and the publication).
func TestRenderWorker_FallbackNoRowNoURL(t *testing.T) {
	f := newRWFallbackFixture(t)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	// The composite: ProcessResult exits before the render — not an error.
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("no row — not an error: %v", err)
	}
	f.pool.seedRow(rwKey, "", nil)
	if err := f.w.processDelivery(context.Background(), rwDelivery("e2"), 2); err != nil {
		t.Fatalf("no final URL — not an error: %v", err)
	}
	// The direct publishFallback guards (the recheck removed the row after
	// the render error): (false, nil) — the caller finalizes, the same
	// rejection there.
	delete(f.pool.rows, rwKey)
	handled, err := f.w.publishFallback(context.Background(), rwTask, errors.New("tarpit"))
	if err != nil || handled {
		t.Fatalf("publishFallback without a row = (%v, %v), want (false, nil)", handled, err)
	}
	f.pool.seedRow(rwKey, "", nil)
	handled, err = f.w.publishFallback(context.Background(), rwTask, errors.New("tarpit"))
	if err != nil || handled {
		t.Fatalf("publishFallback without a final URL = (%v, %v), want (false, nil)", handled, err)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("there must be no publications: %+v", pubs)
	}
}

// The state table of the decision function (candidate × gate × budget) —
// TestRenderWorker_FallbackDecision, fallbackgate_test.go (T-239):
// the always-publish T-238 stub test is replaced by the gate/budget implementation.

// TestRenderWorker_FallbackDedupEviction — the filter's capacity: beyond it
// the oldest key is evicted (its duplicate becomes
// allowed — suppressed by consumer B, FR-11), the recent ones stay.
func TestRenderWorker_FallbackDedupEviction(t *testing.T) {
	d := newFallbackDedup()
	if !d.claim("a") {
		t.Fatal("the first latching of a must pass")
	}
	for i := 0; i < fallbackDedupCap; i++ {
		if !d.claim(fmt.Sprintf("k%d", i)) {
			t.Fatalf("the latching of k%d must not evict the neighbors", i)
		}
	}
	if !d.claim("a") {
		t.Error("a is evicted (the oldest) — a repeated latching passes, a duplicate becomes allowed (FR-11)")
	}
	if !d.claim("b") {
		t.Fatal("the first latching of b must pass")
	}
	if d.claim("b") {
		t.Fatal("the repeated latching of b — a duplicate, it must be rejected")
	}
}

// TestRenderWorker_FallbackDedupRelease — release removes the record
// of a failed publication: a re-delivery keeps the right to
// a publication (otherwise the task would be lost between the filter and the stream);
// an unknown key — a no-op.
func TestRenderWorker_FallbackDedupRelease(t *testing.T) {
	d := newFallbackDedup()
	d.release("unknown") // a no-op, does not panic
	if !d.claim("k") {
		t.Fatal("the first latching must pass")
	}
	d.release("k")
	if !d.claim("k") {
		t.Fatal("after a release the repeated publication must pass")
	}
	if d.claim("k") {
		t.Fatal("without a release the repeated latching is a duplicate")
	}
}

// TestRenderWorker_FallbackDedupKey — the result row identifier:
// the row key + the episode's checked_at (stable over the episode's life, changes
// on a rewrite): different rows, different episodes — different identifiers.
func TestRenderWorker_FallbackDedupKey(t *testing.T) {
	ts := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	other := rwTask
	other.DomainID++
	if fallbackDedupKey(rwTask, ts) == fallbackDedupKey(other, ts) {
		t.Error("different records — different identifiers")
	}
	if fallbackDedupKey(rwTask, ts) == fallbackDedupKey(rwTask, ts.Add(time.Second)) {
		t.Error("different episodes (checked_at) — different identifiers")
	}
	if fallbackDedupKey(rwTask, ts) == fallbackDedupKey(rwTask, time.Time{}) {
		t.Error("a zero checked_at is also a value of the identifier (it does not match)")
	}
}

// TestRenderWorker_FallbackConsumedMetrics — the accounting of the fallback
// consumption by worker B (T-245, spec §4.6 item 2, FR-15): the
// crawler_render_fallback_consumed_total counter grows only in the build with
// the q:render-fallback stream (Options.Stream, FR-12) and only on
// ack-able deliveries — by the outcomes: rendered (a successful rework:
// the DOM written, q:analyze), finalized (a finalization with the marker —
// the fallback exhaustion, FR-10), skipped (no row — without side
// effects). The main build (q:render) does not touch the counter: the
// closed vocabulary's series are pre-created with zeros in all builds (S-P1-4) and
// stay zeros — only the reworking
// worker counts the fallback consumption. The balance
// published = consumed + the q:render-fallback remainder — acceptance step 4
// of spec §6.2 (the integration cross-check — TestAccept03_RenderFallback).
func TestRenderWorker_FallbackConsumedMetrics(t *testing.T) {
	// consumedSeries — the outcome series' value (-1 — no series).
	consumedSeries := func(t *testing.T, g prometheus.Gatherer, result string) float64 {
		t.Helper()
		return rwMetricValue(t, g, "crawler_render_fallback_consumed_total", map[string]string{"result": result})
	}

	t.Run("a rework success — rendered", func(t *testing.T) {
		f := newRWFixture(t)
		reg := metrics.New("render-fallback-worker")
		var err error
		f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
			Stream:  queue.StreamRenderFallback, // the worker B build (T-236)
			Metrics: reg,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.output = rwOutput("<html><body>dom</body></html>")

		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
			t.Fatalf("processDelivery: %v", err)
		}
		if got := consumedSeries(t, reg, fbResultRendered); got != 1 {
			t.Errorf("crawler_render_fallback_consumed_total{rendered} = %v, want 1", got)
		}
		for _, result := range []string{fbResultFinalized, fbResultSkipped} {
			if got := consumedSeries(t, reg, result); got != 0 {
				t.Errorf("crawler_render_fallback_consumed_total{%s} = %v, want 0 (the series is pre-created as zero — S-P1-4, there was no outcome)", result, got)
			}
		}
	})

	t.Run("the fallback exhaustion — finalized", func(t *testing.T) {
		f := newRWFixture(t)
		reg := metrics.New("render-fallback-worker")
		var err error
		f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
			Stream:  queue.StreamRenderFallback, // the worker B build (T-236)
			Metrics: reg,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.err = rwTarpitErr("https://spa.example/")

		// A tarpit on exhaustion: worker B does not publish a recursive fallback
		// (FR-10) — a finalization with the marker (TestRenderWorker_
		// FallbackNoRecursionWorkerB) = the finalized consumption.
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
			t.Fatalf("processDelivery: %v", err)
		}
		// The cert class — another fbConsume call in processDelivery (the same
		// finalized semantics, the T-159 path from the first delivery).
		f.renderer.err = rwCertErr("https://spa.example/", "net::ERR_CERT_AUTHORITY_INVALID")
		if err := f.w.processDelivery(context.Background(), rwDelivery("e2"), 1); err != nil {
			t.Fatalf("processDelivery (cert): %v", err)
		}
		if got := consumedSeries(t, reg, fbResultFinalized); got != 2 {
			t.Errorf("crawler_render_fallback_consumed_total{finalized} = %v, want 2 (a tarpit + a cert)", got)
		}
		if got := consumedSeries(t, reg, fbResultRendered); got != 0 {
			t.Errorf("crawler_render_fallback_consumed_total{rendered} = %v, want 0 (the series is pre-created as zero, there was no outcome)", got)
		}
	})

	t.Run("a delivery error is not counted", func(t *testing.T) {
		f := newRWFixture(t)
		reg := metrics.New("render-fallback-worker")
		var err error
		f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
			Stream:  queue.StreamRenderFallback, // the worker B build (T-236)
			Metrics: reg,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		// The browser class (the engine unavailable) — an infrastructure error:
		// a delivery without an XACK stays in the PEL (a claim/bury — the queue),
		// it is not counted as consumption — the key invariant of the
		// published = consumed + remainder balance (acceptance step 4, spec §6.2).
		f.renderer.err = &render.Error{Class: render.ClassBrowser, URL: "https://spa.example/",
			Err: errors.New("chromium is unavailable")}

		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err == nil {
			t.Fatal("a browser class error — a delivery error (a re-delivery), not a success")
		}
		for _, result := range []string{fbResultRendered, fbResultFinalized, fbResultSkipped} {
			if got := consumedSeries(t, reg, result); got != 0 {
				t.Errorf("crawler_render_fallback_consumed_total{%s} = %v, want 0 (a failed delivery is not counted; the series is pre-created as zero)", result, got)
			}
		}
	})

	t.Run("no row — skipped", func(t *testing.T) {
		f := newRWFixture(t)
		reg := metrics.New("render-fallback-worker")
		var err error
		f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
			Stream:  queue.StreamRenderFallback, // the worker B build (T-236)
			Metrics: reg,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		// We do not seed the row: the recheck rewrote the episode between the publication
		// and the consumption — the delivery is acknowledged without a render and a marker,
		// but it is counted as consumption (the balance does not diverge).
		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
			t.Fatalf("processDelivery: %v", err)
		}
		if got := consumedSeries(t, reg, fbResultSkipped); got != 1 {
			t.Errorf("crawler_render_fallback_consumed_total{skipped} = %v, want 1", got)
		}
	})

	t.Run("the primary build (q:render) does not count the consumption", func(t *testing.T) {
		f := newRWFixture(t)
		reg := metrics.New("render-worker")
		var err error
		f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
			// Stream — the default q:render (the main build).
			Metrics: reg,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		raw := rwTestHash(1)
		f.pool.seedRow(rwKey, "https://spa.example/", &raw)
		f.renderer.output = rwOutput("<html><body>dom</body></html>")

		if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
			t.Fatalf("processDelivery: %v", err)
		}
		for _, result := range []string{fbResultRendered, fbResultFinalized, fbResultSkipped} {
			if got := consumedSeries(t, reg, result); got != 0 {
				t.Errorf("crawler_render_fallback_consumed_total{%s} = %v, want 0 (only worker B counts; the series are pre-created as zeros in all builds)", result, got)
			}
		}
	})
}

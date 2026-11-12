// Unit tests of the fallback health gate and share budget (T-239, spec §4.4
// items 3–4, FR-9): a streak of infra errors of the browser class (the main engine
// unavailable) reaching render.fallback.health_gate_streak suspends
// the fallback publication until the engine recovers — the candidate is finalized
// as today (the render_failed_at marker + q:analyze), without losses;
// the share of publications over the window beyond render.fallback.share_percent
// suspends the publications until the window boundary (the marker is set);
// the suspension events — crawler_render_fallback_suspended_total
// {gate|budget}. The "engine fell" scenario — the unit level through fakes:
// a live run requires a FALLING engine on schedule (a deterministic
// ClassBrowser streak), which only a render fake gives — the integration
// acceptance of the fallback (TestAccept03) uses the same trick for worker A.
package renderworker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/render"
)

// newRWGateFixture — a fixture with the fallback armed (Enabled + the
// q:render stream + the candidate engine of the build — S-P1-1: chromium does not
// publish the fallback) and the gate/budget states connected (T-239):
// threshold — the infra error streak threshold (render.fallback
// .health_gate_streak), ceiling — the share cap of publications over the window
// (render.fallback.share_percent; 0 — without a cap). The metrics registry —
// for checking the suspension events.
func newRWGateFixture(t *testing.T, threshold int, ceiling float64) (*rwFixture, *metrics.Registry) {
	t.Helper()
	f := newRWFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
		Fallback: FallbackOptions{
			Enabled: true,
			Gate:    FallbackGate{Threshold: threshold},
			Budget:  FallbackBudget{Ceiling: ceiling},
		},
		Engine:  rwFBCandidateEngine, // the candidate engine: the publication guard (S-P1-1)
		Metrics: reg,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return f, reg
}

// rwBrowserErr — a render error of the browser class (the main engine
// unavailable): how ClassOf classifies connection/probe failures
// (T-065) — an infra error for the health gate streak (T-239).
func rwBrowserErr(url string) error {
	return &render.Error{Class: render.ClassBrowser, URL: url,
		Err: errors.New("the browser is unavailable: the connection is lost")}
}

// rwGateSuspended — the value of crawler_render_fallback_suspended_total
// {reason=gate} (the series is pre-created with a zero — S-P1-4: 0 — there was no event).
func rwGateSuspended(t *testing.T, reg *metrics.Registry) float64 {
	t.Helper()
	return rwMetricValue(t, reg, "crawler_render_fallback_suspended_total",
		map[string]string{"reason": fbReasonGate})
}

// rwBudgetSuspended — the value of crawler_render_fallback_suspended_total
// {reason=budget} (the series is pre-created with a zero — S-P1-4: 0 — there was no event).
func rwBudgetSuspended(t *testing.T, reg *metrics.Registry) float64 {
	t.Helper()
	return rwMetricValue(t, reg, "crawler_render_fallback_suspended_total",
		map[string]string{"reason": fbReasonBudget})
}

// rwFBCount — the entries in q:render-fallback (the fixture without
// publication resets — the counter grows over the test's phases).
func rwFBCount(t *testing.T, f *rwFixture) int {
	t.Helper()
	return len(rwFallbackPublishes(t, f))
}

// TestRenderWorker_FallbackDecision — the state table of the decision function
// (candidate × gate × budget, T-239; the replacement of the always-publish stub test
// T-238): a healthy engine publishes, the streak on/beyond the threshold and a budget
// suspension reject; zero thresholds (builds without configuration) — the gate and
// budget are off, the T-238 stub behavior.
func TestRenderWorker_FallbackDecision(t *testing.T) {
	cases := []struct {
		name   string
		gate   FallbackGate
		budget FallbackBudget
		want   bool
	}{
		{"healthy (zero states)", FallbackGate{}, FallbackBudget{}, true},
		{"the gate is off (threshold 0), a streak exists", FallbackGate{Threshold: 0, streak: 5}, FallbackBudget{}, true},
		{"the streak below the threshold", FallbackGate{Threshold: 3, streak: 2}, FallbackBudget{}, true},
		{"the streak at the threshold", FallbackGate{Threshold: 3, streak: 3}, FallbackBudget{}, false},
		{"the streak beyond the threshold", FallbackGate{Threshold: 3, streak: 7}, FallbackBudget{}, false},
		{"a budget without a cap, the window was spent", FallbackGate{}, FallbackBudget{Ceiling: 0, tasks: 999, published: 999}, true},
		{"the window share at the cap — not over", FallbackGate{}, FallbackBudget{Ceiling: 30, tasks: 1000, published: 300}, true},
		{"the budget window suspended", FallbackGate{}, FallbackBudget{Ceiling: 30, hold: true}, false},
		{"the gate and the budget at once", FallbackGate{Threshold: 1, streak: 1}, FallbackBudget{Ceiling: 30, hold: true}, false},
	}
	cand := fallbackCandidate{task: rwTask, reason: "timeout"}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := fallbackPublishDecision(cand, c.gate, c.budget); got != c.want {
				t.Errorf("fallbackPublishDecision(%+v, %+v) = %v, want %v", c.gate, c.budget, got, c.want)
			}
		})
	}
}

// TestRenderWorker_FallbackGateBrowserStreakSuspends — a streak of infra errors
// of ClassBrowser closes the gate (FR-9a): below the threshold the publications go
// (a single infra error — the live flow's noise), a task class error
// does NOT reset the streak (a page property, not the engine's — even a
// healthy one has tarpits), the threshold is reached — a suspension event, the candidate
// is finalized as today (the marker + q:analyze) WITHOUT a publication to
// q:render-fallback; the event is not multiplied on infra errors beyond the threshold.
func TestRenderWorker_FallbackGateBrowserStreakSuspends(t *testing.T) {
	f, reg := newRWGateFixture(t, 3, 30)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwBrowserErr("https://spa.example/")

	// Two infra errors: the deliveries return with an error (a redelivery,
	// the browser class is not covered by a finalization), no event.
	for i := 1; i <= 2; i++ {
		if err := f.w.processDelivery(context.Background(), rwDelivery(fmt.Sprintf("e%d", i)), 1); err == nil {
			t.Fatalf("a browser class infra error — a delivery error (redelivery), delivery %d", i)
		}
	}
	if got := rwGateSuspended(t, reg); got != 0 {
		t.Errorf("crawler_render_fallback_suspended_total{gate} before the threshold = %v, want 0 (the series is pre-created as zero, S-P1-4)", got)
	}

	// A candidate between the infra errors (the streak 2 < 3): the publication goes,
	// a task class error does not reset the streak — otherwise the live
	// flow's tarpits would open the gate for a down engine.
	f.renderer.err = rwTarpitErr("https://spa.example/")
	if err := f.w.processDelivery(context.Background(), rwDelivery("t1"), 2); err != nil {
		t.Fatalf("a candidate with the gate open — a publication, not an error: %v", err)
	}
	if n := rwFBCount(t, f); n != 1 {
		t.Fatalf("q:render-fallback publications = %d, 1 is needed (the gate is open)", n)
	}

	// The third infra error — the threshold (the config default 3): the
	// suspension event, one per the "open → closed" transition.
	f.renderer.err = rwBrowserErr("https://spa.example/")
	if err := f.w.processDelivery(context.Background(), rwDelivery("e3"), 1); err == nil {
		t.Fatal("the third infra error — a delivery error (redelivery)")
	}
	if got := rwGateSuspended(t, reg); got != 1 {
		t.Errorf("crawler_render_fallback_suspended_total{gate} = %v, want 1", got)
	}

	// A candidate with the gate closed — a DIFFERENT row (not a duplicate of the
	// already published one: a duplicate is learned about before the decision, see
	// TestRenderWorker_FallbackDuplicateDuringSuspension): the finalization
	// as today — the marker
	// and q:analyze, WITHOUT a publication (the flow overflow onto the expensive engine with
	// an unhealthy main one is excluded, FR-9a), tasks without losses.
	key2 := rwRowKey{zone: 3, domain: rwKey.domain + 100, subdomain: 1, variant: 0}
	f.pool.seedRow(key2, "https://spa.example/", &raw)
	f.renderer.err = rwTarpitErr("https://spa.example/")
	if err := f.w.processDelivery(context.Background(),
		rwKeyDelivery(t, "t2", key2, "spa.example"), 2); err != nil {
		t.Fatalf("a candidate with the gate closed — a normal finalization, not an error: %v", err)
	}
	if n := rwFBCount(t, f); n != 1 {
		t.Errorf("q:render-fallback publications = %d, want 1 (the gate is closed — there must be no new one)", n)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 2 || pubs[1].stream != queue.StreamAnalyze {
		t.Fatalf("the publications %+v, the finalization q:analyze is needed (as with the fallback off)", pubs)
	}
	if row := rwRowSnapshot(f.pool, key2); row.failedAt == nil {
		t.Error("the render_failed_at marker must be set (a suspension = the finalization of today, spec §4.4 item 3)")
	}

	// An infra error beyond the threshold: the event is not multiplied (the streak 4).
	f.renderer.err = rwBrowserErr("https://spa.example/")
	_ = f.w.processDelivery(context.Background(), rwDelivery("e4"), 1)
	if got := rwGateSuspended(t, reg); got != 1 {
		t.Errorf("crawler_render_fallback_suspended_total{gate} = %v, want 1 (one event per latching)", got)
	}
}

// TestRenderWorker_FallbackGateRecoversOnSuccess — the engine's recovery
// (FR-9a): a successful render resets the streak (the engine showed a sign of life,
// the renderFailBackoff pattern — a reset by success), the gate opens and
// the following candidates are published; there is no recovery event (the metric is
// only suspensions).
func TestRenderWorker_FallbackGateRecoversOnSuccess(t *testing.T) {
	f, reg := newRWGateFixture(t, 3, 30)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwBrowserErr("https://spa.example/")

	// The threshold: the gate is closed.
	for i := 1; i <= 3; i++ {
		_ = f.w.processDelivery(context.Background(), rwDelivery(fmt.Sprintf("e%d", i)), 1)
	}
	if got := rwGateSuspended(t, reg); got != 1 {
		t.Fatalf("crawler_render_fallback_suspended_total{gate} = %v, want 1 (the threshold reached)", got)
	}

	// The recovery: a successful render of another row (the manager
	// restarted the engine, T-065 — the page rendered).
	key2 := rwRowKey{zone: 3, domain: rwKey.domain + 1, subdomain: 1, variant: 0}
	f.pool.seedRow(key2, "https://ok.example/", &raw)
	f.renderer.err = nil
	f.renderer.output = rwOutput("<html><body>dom</body></html>")
	if err := f.w.processDelivery(context.Background(), rwKeyDelivery(t, "ok1", key2, "ok.example"), 1); err != nil {
		t.Fatalf("a successful render after the recovery: %v", err)
	}
	if row := rwRowSnapshot(f.pool, key2); !row.rendered {
		t.Fatal("a successful render must write the DOM (the engine is alive)")
	}

	// The gate is open: the candidate is published to the fallback, no marker.
	f.renderer.err = rwTarpitErr("https://spa.example/")
	if err := f.w.processDelivery(context.Background(), rwDelivery("t1"), 2); err != nil {
		t.Fatalf("a candidate after the recovery — a publication, not an error: %v", err)
	}
	if n := rwFBCount(t, f); n != 1 {
		t.Errorf("q:render-fallback publications = %d, 1 is needed (the gate is opened by the recovery)", n)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("the marker is not set — the record waits for the fallback (FR-10)")
	}
	if got := rwGateSuspended(t, reg); got != 1 {
		t.Errorf("crawler_render_fallback_suspended_total{gate} = %v, want 1 (there is no recovery event)", got)
	}
}

// TestRenderWorker_FallbackBudgetWindowSuspension — the share budget window
// (FR-9b): the publications accumulate in the numerator, the acknowledged
// deliveries — in the denominator; a filled window with a share beyond the cap suspends
// the next window's publications — the candidate gets the marker (the criterion
// "after the budget is exhausted — the marker is set"), the {budget} event;
// the suspended window does not accumulate publications — the next estimate
// returns the fallback (self-reset).
func TestRenderWorker_FallbackBudgetWindowSuspension(t *testing.T) {
	// A cap of 10%: 101 publications per the window's 1000 deliveries (10.1%) —
	// an overspend; 100 (exactly 10%) — no ("beyond the cap").
	f, reg := newRWGateFixture(t, 0, 10)
	raw := rwTestHash(1)

	// Phase A — 101 candidates: each is published (the gate is off
	// by the threshold 0, the budget does not interfere before the window estimate).
	f.renderer.err = rwTarpitErr("https://spa.example/")
	pubs := 0
	for i := 0; i < 101; i++ {
		key := rwRowKey{zone: 3, domain: int64(2000 + i), subdomain: 1, variant: 0}
		f.pool.seedRow(key, "https://spa.example/", &raw)
		if err := f.w.processDelivery(context.Background(),
			rwKeyDelivery(t, fmt.Sprintf("a%d", i), key, "spa.example"), 2); err != nil {
			t.Fatalf("candidate %d: the publication: %v", i, err)
		}
		pubs++
	}
	if n := rwFBCount(t, f); n != pubs {
		t.Fatalf("q:render-fallback publications = %d, want %d", n, pubs)
	}
	if got := rwBudgetSuspended(t, reg); got != 0 {
		t.Errorf("crawler_render_fallback_suspended_total{budget} before the window closes = %v, want 0 (the series is pre-created as zero)", got)
	}

	// Phase B — filler up to 1000 acknowledged deliveries: deliveries
	// without rows are acknowledged by a regular rejection. On the thousandth the window
	// closes: 10.1% > 10% — the next window's suspension.
	filler := func(from, n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			key := rwRowKey{zone: 9, domain: int64(50000 + from + i), subdomain: 1, variant: 0}
			if err := f.w.processDelivery(context.Background(),
				rwKeyDelivery(t, fmt.Sprintf("b%d", from+i), key, "missing.example"), 1); err != nil {
				t.Fatalf("filler %d: no row — not an error: %v", from+i, err)
			}
		}
	}
	filler(0, 899)
	if got := rwBudgetSuspended(t, reg); got != 1 {
		t.Errorf("crawler_render_fallback_suspended_total{budget} = %v, want 1 (the window exceeded the cap)", got)
	}

	// Phase C — a candidate in the suspended window: a finalization with the marker
	// (like with the fallback off), no publication.
	key := rwRowKey{zone: 3, domain: 3000, subdomain: 1, variant: 0}
	f.pool.seedRow(key, "https://spa.example/", &raw)
	if err := f.w.processDelivery(context.Background(),
		rwKeyDelivery(t, "c1", key, "spa.example"), 2); err != nil {
		t.Fatalf("a candidate in the suspended window — a normal finalization, not an error: %v", err)
	}
	if n := rwFBCount(t, f); n != pubs {
		t.Errorf("q:render-fallback publications = %d, want %d (the budget is exhausted — the window holds)", n, pubs)
	}
	if row := rwRowSnapshot(f.pool, key); row.failedAt == nil {
		t.Error("after the budget is exhausted the render_failed_at marker is set (spec §4.4 item 4)")
	}
	if got := rwMetricValue(t, reg, "crawler_render_fallback_published_total",
		map[string]string{"reason": "timeout"}); got != float64(pubs) {
		t.Errorf("crawler_render_fallback_published_total{timeout} = %v, want %d", got, pubs)
	}

	// Phase D — the suspended window collects the remaining 999 acknowledged
	// deliveries (the phase C candidate is already counted) and closes without
	// publications: the estimate returns the fallback.
	filler(899, fallbackBudgetWindow-1)
	if got := rwBudgetSuspended(t, reg); got != 1 {
		t.Errorf("crawler_render_fallback_suspended_total{budget} = %v, want 1 (one event per latching)", got)
	}

	// Phase E — the next window publishes again (self-reset).
	key2 := rwRowKey{zone: 3, domain: 3001, subdomain: 1, variant: 0}
	f.pool.seedRow(key2, "https://spa.example/", &raw)
	if err := f.w.processDelivery(context.Background(),
		rwKeyDelivery(t, "e1", key2, "spa.example"), 2); err != nil {
		t.Fatalf("a candidate after the window — a publication, not an error: %v", err)
	}
	if n := rwFBCount(t, f); n != pubs+1 {
		t.Errorf("q:render-fallback publications = %d, want %d (the window resumed the publications)", n, pubs+1)
	}
	if got := rwBudgetSuspended(t, reg); got != 1 {
		t.Errorf("crawler_render_fallback_suspended_total{budget} = %v, want 1 (the resumption does not bear an event)", got)
	}
}

// TestRenderWorker_FallbackBudgetExactCeilingNotSuspended — the share exactly
// at the cap — not "beyond": the window closes without a suspension (the spec
// §5.3 deviation threshold — above 30% — zeroes the economics, exactly 30% —
// the working point).
func TestRenderWorker_FallbackBudgetExactCeilingNotSuspended(t *testing.T) {
	f, reg := newRWGateFixture(t, 0, 10)
	raw := rwTestHash(1)
	f.renderer.err = rwTarpitErr("https://spa.example/")
	// 100 publications of 1000 deliveries = exactly 10%.
	for i := 0; i < 100; i++ {
		key := rwRowKey{zone: 3, domain: int64(30000 + i), subdomain: 1, variant: 0}
		f.pool.seedRow(key, "https://spa.example/", &raw)
		if err := f.w.processDelivery(context.Background(),
			rwKeyDelivery(t, fmt.Sprintf("a%d", i), key, "spa.example"), 2); err != nil {
			t.Fatalf("candidate %d: the publication: %v", i, err)
		}
	}
	for i := 0; i < 900; i++ {
		key := rwRowKey{zone: 9, domain: int64(80000 + i), subdomain: 1, variant: 0}
		if err := f.w.processDelivery(context.Background(),
			rwKeyDelivery(t, fmt.Sprintf("b%d", i), key, "missing.example"), 1); err != nil {
			t.Fatalf("filler %d: %v", i, err)
		}
	}
	if got := rwBudgetSuspended(t, reg); got != 0 {
		t.Errorf("crawler_render_fallback_suspended_total{budget} = %v, want 0 (the share at the cap, not over; the series is pre-created as zero)", got)
	}
	// The next candidate is published: the window was not suspended.
	key := rwRowKey{zone: 3, domain: 31000, subdomain: 1, variant: 0}
	f.pool.seedRow(key, "https://spa.example/", &raw)
	if err := f.w.processDelivery(context.Background(),
		rwKeyDelivery(t, "c1", key, "spa.example"), 2); err != nil {
		t.Fatalf("a candidate after the window at the cap: %v", err)
	}
	if n := rwFBCount(t, f); n != 101 {
		t.Errorf("q:render-fallback publications = %d, want 101", n)
	}
}

// TestRenderWorker_FallbackGateNoOpDoesNotReset — the streak reset only by
// a successful render (the T-239 review, P1): a regular rejection without a
// render (no row / it slipped out from under the task — ProcessResult returned nil without
// rendering) does not count as a sign of the engine's life — a no-op between
// infra errors does not reset the streak, the gate latches through it.
func TestRenderWorker_FallbackGateNoOpDoesNotReset(t *testing.T) {
	f, reg := newRWGateFixture(t, 3, 30)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)

	// Two engine infra errors (the delivery returns with an error — ed.)
	f.renderer.err = rwBrowserErr("https://spa.example/")
	_ = f.w.processDelivery(context.Background(), rwDelivery("e1"), 1)
	_ = f.w.processDelivery(context.Background(), rwDelivery("e2"), 1)

	// A no-op between them: no row — a success without a render; no render
	// was executed, the engine showed no sign of life.
	missing := rwRowKey{zone: 9, domain: 99999, subdomain: 1, variant: 0}
	if err := f.w.processDelivery(context.Background(),
		rwKeyDelivery(t, "noop", missing, "missing.example"), 1); err != nil {
		t.Fatalf("a no-op — not an error: %v", err)
	}

	// The third infra error — the threshold is reached (the no-op did not reset the streak).
	_ = f.w.processDelivery(context.Background(), rwDelivery("e3"), 1)
	if got := rwGateSuspended(t, reg); got != 1 {
		t.Fatalf("crawler_render_fallback_suspended_total{gate} = %v, want 1 (a no-op without a render does not reset the streak)", got)
	}

	// A candidate with the gate closed — the marker, without a publication.
	f.renderer.err = rwTarpitErr("https://spa.example/")
	if err := f.w.processDelivery(context.Background(), rwDelivery("t1"), 2); err != nil {
		t.Fatalf("a candidate with the gate closed — a normal finalization: %v", err)
	}
	if n := rwFBCount(t, f); n != 0 {
		t.Errorf("q:render-fallback publications = %d, want 0", n)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt == nil {
		t.Error("the render_failed_at marker is set (a suspension = the finalization of today)")
	}
}

// TestRenderWorker_FallbackDuplicateDuringSuspension — a delivery duplicate
// of a published row is learned about BEFORE the gate/budget decision (the T-239
// review, P3): the entry is already waiting for worker B in q:render-fallback — a suspension
// does not cancel a publication that happened, the duplicate is acknowledged without a second
// entry in the stream, without a marker and without q:analyze (FR-11).
func TestRenderWorker_FallbackDuplicateDuringSuspension(t *testing.T) {
	f, _ := newRWGateFixture(t, 3, 30)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)

	// The publication happened; the XACK was lost — the delivery will return as a duplicate.
	f.renderer.err = rwTarpitErr("https://spa.example/")
	if err := f.w.processDelivery(context.Background(), rwDelivery("t1"), 2); err != nil {
		t.Fatalf("the fallback publication: %v", err)
	}
	if n := rwFBCount(t, f); n != 1 {
		t.Fatalf("q:render-fallback publications = %d, 1 is needed", n)
	}

	// The engine fell: the gate is closed by the streak.
	f.renderer.err = rwBrowserErr("https://spa.example/")
	for i := 0; i < 3; i++ {
		_ = f.w.processDelivery(context.Background(), rwDelivery(fmt.Sprintf("e%d", i)), 1)
	}

	// A duplicate with the gate closed: without a second publication, without a marker,
	// without q:analyze — worker B reworks the entry.
	f.renderer.err = rwTarpitErr("https://spa.example/")
	if err := f.w.processDelivery(context.Background(), rwDelivery("t1dup"), 2); err != nil {
		t.Fatalf("a duplicate within the suspension is acknowledged without an error: %v", err)
	}
	if n := rwFBCount(t, f); n != 1 {
		t.Errorf("q:render-fallback publications = %d, want 1 (a duplicate does not duplicate the stream)", n)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("a duplicate needs no marker — the record already waits for worker B (FR-10)")
	}
	for _, p := range f.q.publishesSnapshot() {
		if p.stream == queue.StreamAnalyze {
			t.Errorf("no q:analyze is due for a duplicate: %+v", p)
		}
	}
}

// TestRenderWorker_FallbackStatesParallel — parallel deliveries
// (the T-239 review, P3: the declared thread safety of fbMu): N candidates
// are published simultaneously — the window's numerator/denominator accounting without losses,
// no deadlocks; all deliveries finish (a Timeout through the wait group).
func TestRenderWorker_FallbackStatesParallel(t *testing.T) {
	f, reg := newRWGateFixture(t, 1000, 30) // an unreachable threshold — the gate is open
	raw := rwTestHash(1)
	f.renderer.err = rwTarpitErr("https://spa.example/")

	const n = 64
	dels := make([]queue.Delivery, n)
	for i := 0; i < n; i++ {
		key := rwRowKey{zone: 3, domain: int64(40000 + i), subdomain: 1, variant: 0}
		f.pool.seedRow(key, "https://spa.example/", &raw)
		dels[i] = rwKeyDelivery(t, fmt.Sprintf("p%d", i), key, "spa.example")
	}
	var wg sync.WaitGroup
	for _, del := range dels {
		wg.Add(1)
		go func(del queue.Delivery) {
			defer wg.Done()
			_ = f.w.processDelivery(context.Background(), del, 2)
		}(del)
	}
	wg.Wait()

	// All the publications went through: the numerator = n (the accounting without races),
	// the window's share n/n below the 30% cap never latched — the window is not closed yet.
	if got := rwFBCount(t, f); got != n {
		t.Errorf("q:render-fallback publications = %d, want %d", got, n)
	}
	if got := rwMetricValue(t, reg, "crawler_render_fallback_published_total",
		map[string]string{"reason": "timeout"}); got != float64(n) {
		t.Errorf("crawler_render_fallback_published_total{timeout} = %v, want %d", got, n)
	}
	if got := rwBudgetSuspended(t, reg); got != 0 {
		t.Errorf("crawler_render_fallback_suspended_total{budget} = %v, want 0 (the window never closed; the series is pre-created as zero)", got)
	}
	_, budget := f.w.fallbackStates()
	if budget.tasks != n || budget.published != n {
		t.Errorf("the budget window: tasks=%d published=%d, want %d/%d (the accounting without races)", budget.tasks, budget.published, n, n)
	}
}

// TestRenderWorker_FallbackWorkerBNoState — worker B's isolation (the
// T-239 review, P3): the same binary on the q:render-fallback stream keeps no
// gate/budget observations (fallbackArmed) and raises no suspension
// events — its engine (chromium) is not the main one, the fallback alerts are not
// its concern.
func TestRenderWorker_FallbackWorkerBNoState(t *testing.T) {
	f := newRWFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
		Fallback: FallbackOptions{
			Enabled: true,
			Gate:    FallbackGate{Threshold: 1}, // a threshold of 1: any infra error would close it
			Budget:  FallbackBudget{Ceiling: 30},
		},
		Stream:  queue.StreamRenderFallback, // the worker B build (T-236)
		Metrics: reg,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.err = rwBrowserErr("https://spa.example/")

	// An infra error and a candidate on worker B: no states, the
	// suspension metric does not grow.
	_ = f.w.processDelivery(context.Background(), rwDelivery("e1"), 1)
	f.renderer.err = rwTarpitErr("https://spa.example/")
	if err := f.w.processDelivery(context.Background(), rwDelivery("t1"), 2); err != nil {
		t.Fatalf("worker B: the candidate — a finalization (the recursion is forbidden): %v", err)
	}
	gate, budget := f.w.fallbackStates()
	if gate.streak != 0 || budget.tasks != 0 || budget.published != 0 || budget.hold {
		t.Errorf("the worker B states: gate=%+v budget=%+v, want zeros (the observations only in the armed build)", gate, budget)
	}
	// The series are pre-created with zeros in all builds (S-P1-4): worker B
	// has the family, but it does not grow — no suspension events.
	if got := rwMetricValue(t, reg, "crawler_render_fallback_suspended_total", nil); got != 0 {
		t.Errorf("crawler_render_fallback_suspended_total = %v, want 0 (the series are pre-created as zeros, they do not grow)", got)
	}
	if got := rwGateSuspended(t, reg); got != 0 {
		t.Errorf("crawler_render_fallback_suspended_total{gate} = %v, want 0", got)
	}
}

// TestRenderWorker_FallbackEngineDownScenario — the "engine fell" scenario
// (spec §4.4 item 3, the T-239 criterion): the main engine returns infra errors
// of the browser class — there is NO publication to q:render-fallback (the browser
// class is not a candidate, infrastructure), every delivery returns an error
// without an XACK (the tasks go to the q:render redelivery — a claim will return them after
// recovery); the streak closes the gate — a mixed candidate
// (an engine-branched task class error) is finalized with the marker,
// the deliveries without losses. Only a fake gives a live engine falling on
// schedule (see the file header), the redelivery wiring —
// queue.ProcessingAttempt: an error ⇒ no XACK, nil ⇒ XACK.
func TestRenderWorker_FallbackEngineDownScenario(t *testing.T) {
	f, reg := newRWGateFixture(t, 3, 30) // the T-233 configuration defaults
	raw := rwTestHash(1)

	// Several rows of the live flow; the engine fell — ClassBrowser on
	// every task.
	keys := []rwRowKey{
		rwKey,
		{zone: 3, domain: rwKey.domain + 1, subdomain: 1, variant: 0},
		{zone: 3, domain: rwKey.domain + 2, subdomain: 1, variant: 0},
		{zone: 3, domain: rwKey.domain + 3, subdomain: 1, variant: 0},
		{zone: 3, domain: rwKey.domain + 4, subdomain: 1, variant: 0},
	}
	for _, k := range keys {
		f.pool.seedRow(k, "https://spa.example/", &raw)
	}
	f.renderer.err = rwBrowserErr("https://spa.example/")

	// All the deliveries — errors without an XACK: the tasks go to the redelivery, no
	// fallback publications (the browser class is not a candidate, FR-7).
	for i, k := range keys {
		del := rwKeyDelivery(t, fmt.Sprintf("d%d", i), k, "spa.example")
		if err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", del, 1, f.w.processDelivery); err == nil {
			t.Fatalf("the engine fell: delivery %d must come back as an error (redelivery)", i)
		}
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 0 {
		t.Errorf("XACK = %v, want empty (the tasks in redelivery, the attempts are not burned)", acked)
	}
	if n := rwFBCount(t, f); n != 0 {
		t.Fatalf("q:render-fallback publications = %d, want 0 (the engine fell — no overflow)", n)
	}
	// The streak reached the threshold — the gate event (the engine degradation alert).
	if got := rwGateSuspended(t, reg); got != 1 {
		t.Errorf("crawler_render_fallback_suspended_total{gate} = %v, want 1 (the infra error streak)", got)
	}

	// A mixed candidate with the engine down: the engine-branched task
	// class error would reach the fallback, but the gate is closed — the finalization
	// as today (the marker + q:analyze), the delivery is acknowledged:
	// tasks without losses (a redelivery or a regular finalization).
	f.renderer.err = rwTarpitErr("https://spa.example/")
	del := rwKeyDelivery(t, "c1", keys[0], "spa.example")
	if err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker", del, 2, f.w.processDelivery); err != nil {
		t.Fatalf("a candidate with the engine down — a normal finalization, not an error: %v", err)
	}
	if n := rwFBCount(t, f); n != 0 {
		t.Errorf("q:render-fallback publications = %d, want 0 (the gate is closed)", n)
	}
	if row := rwRowSnapshot(f.pool, keys[0]); row.failedAt == nil {
		t.Error("the render_failed_at marker is set — a suspension = the finalization of today")
	}
	acked := f.q.ackedSnapshot()
	if len(acked) != 1 || acked[0] != "c1" {
		t.Errorf("XACK = %v, want [c1] (the finalization acknowledges the delivery)", acked)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("the publications %+v, one q:analyze is needed (the pipeline reaches the analysis)", pubs)
	}
}

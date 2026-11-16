// Unit tests of the per-engine URL skip-list (T-285, the consequence of
// D-026: one https://hh.ru/ navigation deterministically takes moli v1.1.25
// to ~2 GiB RSS in 8–18 s and a memcg OOM — no engine-side compensation
// before an upstream fix): before the navigation worker A checks the task's
// final URL against the patterns of the build's engine
// (render.engine_skip_patterns[render.engine], a substring match); a match —
// no navigation at all: the engine-branched finalization path from the
// FIRST delivery (the match is deterministic on the URL) — with the
// fallback on a publication to q:render-fallback (the reason "engine-skip",
// worker B reworks the page in chromium: the domain is not lost, the engine
// does not see the page-killer), a rejection (off / the worker B stream /
// the T-239 gate-budget) — the regular finalization without a render (the
// marker + q:analyze). The lists of other engines and any list on a
// chromium build are inactive (the worker receives only its own engine's
// slice; the matcher's engine guard is the second line of defense).
// The fake environment — worker_test.go.
package renderworker

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"crawler/internal/config"
	"crawler/internal/metrics"
	"crawler/internal/queue"
)

// rwSkipPattern — the pattern of the tests' skip-list (the hh.ru page-killer
// of D-026 — the trigger of the task).
const rwSkipPattern = "hh.ru"

// newRWSkipFixture — a fixture of the moli worker A build with the fallback
// on and the skip-list of rwSkipPattern: the other options — defaults
// (TimeoutAttempts 2, MaxAttempts 3, the Stream q:render); gate/budget —
// off (the always-publish stub behavior, the newRWFallbackFixture style).
func newRWSkipFixture(t *testing.T) *rwFixture {
	t.Helper()
	f := newRWFixture(t)
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
		Fallback: FallbackOptions{Enabled: true}, Engine: rwFBCandidateEngine,
		EngineSkipPatterns: []string{rwSkipPattern},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return f
}

// TestEngineSkipMatch — the pure decision of the matcher: the substring
// semantics (a host or a path pattern), the first match wins, an empty list
// and a chromium build never match (worker B / a rollback — the engine
// guard), a non-matching URL — the ordinary path.
func TestEngineSkipMatch(t *testing.T) {
	cases := []struct {
		name     string
		patterns []string
		engine   string
		url      string
		want     string
		ok       bool
	}{
		{"a host substring", []string{"hh.ru"}, config.EngineMoli, "https://hh.ru/", "hh.ru", true},
		{"a superstring host", []string{"hh.ru"}, config.EngineMoli, "https://www.hh.ru/vacancy/1", "hh.ru", true},
		{"a path pattern", []string{"/heavy/report"}, config.EngineMoli, "https://slow.example/heavy/report/2026", "/heavy/report", true},
		{"the first matching pattern is reported", []string{"slow.example", "hh.ru"}, config.EngineMoli, "https://hh.ru/", "hh.ru", true},
		{"no match — the ordinary path", []string{"hh.ru"}, config.EngineMoli, "https://spa.example/", "", false},
		{"an empty list never matches", nil, config.EngineMoli, "https://hh.ru/", "", false},
		{"a chromium build never skips", []string{"hh.ru"}, config.EngineChromium, "https://hh.ru/", "", false},
		{"lightpanda carries its own list", []string{"slow.example"}, config.EngineLightpanda, "https://slow.example/", "slow.example", true},
		{"an empty URL does not blow up", []string{"hh.ru"}, config.EngineMoli, "", "", false},
	}
	// The per-engine binding (a list of another engine is not looked at) is
	// the WIRING's contract, not the matcher's: the worker receives only its
	// own engine's slice (cmd/render-worker), the matcher is engine-blind
	// except the chromium guard — verified by the renderworker tests and the
	// wiring comment.
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := engineSkipMatch(c.patterns, c.engine, c.url)
			if ok != c.ok || got != c.want {
				t.Errorf("engineSkipMatch(%v, %q, %q) = (%q, %v), want (%q, %v)",
					c.patterns, c.engine, c.url, got, ok, c.want, c.ok)
			}
		})
	}
	// The reason vocabulary: the marker maps to "engine-skip" (a member of
	// the taskFinalizeReason dictionary alongside net::ERR_*/timeout/other).
	if got := taskFinalizeReason(&engineSkipError{url: "https://hh.ru/", pattern: rwSkipPattern}); got != engineSkipReason {
		t.Errorf("taskFinalizeReason(engineSkipError) = %q, want %q", got, engineSkipReason)
	}
}

// TestRenderWorker_EngineSkipFallbackPublish — the main acceptance: a match
// on the moli build with the fallback on — NO navigation (the renderer got
// no tasks), the task is published to q:render-fallback (Fresh=true, the
// row key), no marker, no q:analyze (worker B does both, FR-10), no
// Put/transaction, the delivery is not an error, the
// crawler_render_fallback_published_total{reason="engine-skip"} metric
// grows — from the very FIRST delivery (the match is deterministic on the
// URL, a retry would repeat it).
func TestRenderWorker_EngineSkipFallbackPublish(t *testing.T) {
	f := newRWSkipFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
		Fallback: FallbackOptions{Enabled: true}, Engine: rwFBCandidateEngine,
		EngineSkipPatterns: []string{rwSkipPattern}, Metrics: reg,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://hh.ru/", &raw)

	// Delivery #1 of 3: the skip works from the first delivery — waiting
	// for attempts is pointless, every delivery would match again. The
	// delivery goes through the queue-level wrapper (ProcessingAttempt):
	// the nil of processDelivery is the XACK precondition, the
	// acknowledgment itself is asserted below (the criterion "publication +
	// XACK, no q:analyze").
	if err := queue.ProcessingAttempt(context.Background(), f.q, "render-worker",
		rwDelivery("e1"), 1, f.w.processDelivery); err != nil {
		t.Fatalf("a skip-list match with the fallback — a publication, not an error: %v", err)
	}
	if acked := f.q.ackedSnapshot(); len(acked) != 1 || acked[0] != "e1" {
		t.Errorf("XACK = %v, want [e1] — the skipped delivery is acknowledged (the publication precedes it)", acked)
	}
	// The engine has not seen the page-killer: no navigation at all.
	if tasks := f.renderer.tasksSnapshot(); len(tasks) != 0 {
		t.Fatalf("the render got tasks %+v — a skip-list match must not navigate", tasks)
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
		t.Error("the render_failed_at marker must not be set — the record waits for the fallback (FR-10)")
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
	// The metrics: a publication with the engine-skip reason, no
	// finalizations, no stage errors (a routing decision, not a render
	// failure — R0 of the outcome policy).
	if got := rwMetricValue(t, reg, "crawler_render_fallback_published_total",
		map[string]string{"reason": engineSkipReason}); got != 1 {
		t.Errorf("crawler_render_fallback_published_total{%s} = %v, want 1", engineSkipReason, got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total",
		map[string]string{"reason": engineSkipReason, "engine": rwFBCandidateEngine}); got != -1 {
		t.Errorf("crawler_render_task_finalized_total{%s} = %v, want no series (the publication, not a finalization)",
			engineSkipReason, got)
	}
	if got := rwMetricValue(t, reg, "crawler_stage_errors_total",
		map[string]string{"stage": "render", "type": "task"}); got != -1 {
		t.Errorf("crawler_stage_errors_total{render,task} = %v, want no series (no navigation — no render error)", got)
	}
}

// TestRenderWorker_EngineSkipNoMatch — a URL outside the list: the ordinary
// path — the render runs, the DOM is written (the skip-list does not touch
// the common flow).
func TestRenderWorker_EngineSkipNoMatch(t *testing.T) {
	f := newRWSkipFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://spa.example/", &raw)
	f.renderer.output = rwOutput("<html><body>rendered</body></html>")

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
		t.Fatalf("a URL outside the list — the ordinary path: %v", err)
	}
	if tasks := f.renderer.tasksSnapshot(); len(tasks) != 1 || tasks[0].URL != "https://spa.example/" {
		t.Fatalf("the render: tasks %+v, one URL https://spa.example/ needed", tasks)
	}
	if row := rwRowSnapshot(f.pool, rwKey); !row.rendered {
		t.Error("rendered must be true — the ordinary path")
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Errorf("the publications %+v, one q:analyze needed (the ordinary path)", pubs)
	}
}

// TestRenderWorker_EngineSkipFallbackOff — the fallback off (the default):
// a match — the finalization without a render exactly as today: the
// render_failed_at marker + a q:analyze publication, no navigation, the
// delivery is acknowledged (nil), the
// crawler_render_task_finalized_total{reason="engine-skip",engine} metric
// grows.
func TestRenderWorker_EngineSkipFallbackOff(t *testing.T) {
	f := newRWFixture(t)
	reg := metrics.New("render-worker")
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
		Engine: config.EngineMoli, EngineSkipPatterns: []string{rwSkipPattern}, Metrics: reg,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://hh.ru/", &raw)

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
		t.Fatalf("the skip-list with the fallback off — a finalization, not an error: %v", err)
	}
	if tasks := f.renderer.tasksSnapshot(); len(tasks) != 0 {
		t.Fatalf("the render got tasks %+v — no navigation on a match", tasks)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("the publications %+v, one q:analyze needed (the finalization without a render)", pubs)
	}
	row := rwRowSnapshot(f.pool, rwKey)
	if row.failedAt == nil {
		t.Error("the render_failed_at marker must be set (the fallback is off — the finalization of today)")
	}
	if row.rendered {
		t.Error("rendered must stay false")
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total",
		map[string]string{"reason": engineSkipReason, "engine": config.EngineMoli}); got != 1 {
		t.Errorf("crawler_render_task_finalized_total{%s,moli} = %v, want 1", engineSkipReason, got)
	}
	if got := rwMetricValue(t, reg, "crawler_render_fallback_published_total",
		map[string]string{"reason": engineSkipReason}); got != -1 {
		t.Errorf("crawler_render_fallback_published_total{%s} = %v, want no series (the fallback is off)", engineSkipReason, got)
	}
}

// TestRenderWorker_EngineSkipChromiumInactive — the patterns handed to a
// CHROMIUM build (a miswired worker B or a rollback build): the matcher's
// engine guard holds — no skip, the ordinary path (the engine never skips
// itself: chromium is the terminal rework engine).
func TestRenderWorker_EngineSkipChromiumInactive(t *testing.T) {
	f := newRWFixture(t)
	reg := metrics.New("render-worker")
	var err error
	// The fallback is on deliberately: even the armed build with the
	// patterns of the default engine does not skip and does not publish.
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
		Fallback: FallbackOptions{Enabled: true},
		// Engine is not set: "" — chromium (withDefaults, like a build
		// without configuration).
		EngineSkipPatterns: []string{rwSkipPattern}, Metrics: reg,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://hh.ru/", &raw)
	f.renderer.output = rwOutput("<html><body>rendered</body></html>")

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
		t.Fatalf("a chromium build with patterns — the ordinary path: %v", err)
	}
	if tasks := f.renderer.tasksSnapshot(); len(tasks) != 1 || tasks[0].URL != "https://hh.ru/" {
		t.Fatalf("the render: tasks %+v — a chromium build must render the URL itself", tasks)
	}
	if row := rwRowSnapshot(f.pool, rwKey); !row.rendered {
		t.Error("rendered must be true — the ordinary path")
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Errorf("the publications %+v, one q:analyze needed", pubs)
	}
}

// TestRenderWorker_EngineSkipWorkerBNoRecursion — the worker B stream
// (q:render-fallback) with a candidate engine's list (a miswired build):
// a match finalizes without a render (the marker + q:analyze) and does NOT
// publish to q:render-fallback — a recursive fallback is forbidden
// (FR-10; the same publishFallback guard as for the tarpit/task classes).
func TestRenderWorker_EngineSkipWorkerBNoRecursion(t *testing.T) {
	f := newRWFixture(t)
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
		Stream: queue.StreamRenderFallback, Group: "render-fallback-worker",
		Fallback: FallbackOptions{Enabled: true}, Engine: rwFBCandidateEngine,
		EngineSkipPatterns: []string{rwSkipPattern},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://hh.ru/", &raw)

	// The delivery of the fallback stream: rwDelivery builds a q:render
	// stream message — the payload is what matters here (the skip check
	// does not read the stream field).
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
		t.Fatalf("a skip on the worker B stream — a finalization, not an error: %v", err)
	}
	if tasks := f.renderer.tasksSnapshot(); len(tasks) != 0 {
		t.Fatalf("the render got tasks %+v — no navigation on a match", tasks)
	}
	if n := rwFBCount(t, f); n != 0 {
		t.Errorf("q:render-fallback publications = %d, 0 needed (a recursive fallback is forbidden, FR-10)", n)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("the publications %+v, one q:analyze needed (the finalization without a render)", pubs)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt == nil {
		t.Error("the render_failed_at marker must be set (worker B has no fallback)")
	}
}

// TestRenderWorker_EngineSkipGateSuspended — the health gate (FR-9a/T-239,
// FR-12): with the gate closed by a browser-class infra error streak the
// engine-skip candidate does not overflow onto the expensive chromium —
// the finalization of today (the marker + q:analyze), no publication; the
// engine-skip does not touch the streak either (no render happened — the
// T-239 review P1 rule), and the suspension event is not multiplied.
func TestRenderWorker_EngineSkipGateSuspended(t *testing.T) {
	f, reg := newRWGateFixture(t, 1, 0)
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
		Fallback: FallbackOptions{Enabled: true,
			Gate:   FallbackGate{Threshold: 1},
			Budget: FallbackBudget{Ceiling: 0},
		},
		Engine:             rwFBCandidateEngine,
		EngineSkipPatterns: []string{rwSkipPattern},
		Metrics:            reg,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	// The gate closes on ANOTHER row (outside the skip-list — the priming
	// delivery must be a browser error, not a skip): one browser-class infra
	// error reaches the threshold 1.
	primeKey := rwRowKey{zone: 3, domain: rwKey.domain + 100, subdomain: 1, variant: 0}
	f.pool.seedRow(primeKey, "https://spa.example/", &raw)
	f.pool.seedRow(rwKey, "https://hh.ru/", &raw)
	f.renderer.err = rwBrowserErr("https://spa.example/")
	if err := f.w.processDelivery(context.Background(), rwKeyDelivery(t, "e0", primeKey, "spa.example"), 1); err == nil {
		t.Fatal("a browser class infra error — a delivery error (redelivery)")
	}
	if got := rwGateSuspended(t, reg); got != 1 {
		t.Fatalf("crawler_render_fallback_suspended_total{gate} = %v, want 1", got)
	}

	// The skip candidate with the gate closed — the finalization of today.
	f.renderer.err = nil
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
		t.Fatalf("a skip candidate with the gate closed — a normal finalization, not an error: %v", err)
	}
	if n := rwFBCount(t, f); n != 0 {
		t.Errorf("q:render-fallback publications = %d, 0 needed (the gate is closed, FR-9a)", n)
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("the publications %+v, one q:analyze needed (the priming browser error publishes nothing — it is a redelivery; only the skip's finalization publishes)", pubs)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt == nil {
		t.Error("the render_failed_at marker must be set (a suspension = the finalization of today, spec §4.4 item 3)")
	}
	if got := rwMetricValue(t, reg, "crawler_render_task_finalized_total",
		map[string]string{"reason": engineSkipReason, "engine": rwFBCandidateEngine}); got != 1 {
		t.Errorf("crawler_render_task_finalized_total{%s,moli} = %v, want 1", engineSkipReason, got)
	}
	// The skip does not touch the streak (no render — no sign of life) and
	// does not multiply the suspension event.
	if got := rwGateSuspended(t, reg); got != 1 {
		t.Errorf("crawler_render_fallback_suspended_total{gate} = %v, want 1 (one event per latching)", got)
	}
}

// TestRenderWorker_EngineSkipBudgetSuspended — the share budget (FR-9b/
// T-239): a suspended window (the previous one overspent the
// render.fallback.share_percent cap) finalizes the engine-skip candidate
// with the marker instead of publishing — the ceilings apply to the
// skip-list publications exactly as to the tarpit/task-class ones.
func TestRenderWorker_EngineSkipBudgetSuspended(t *testing.T) {
	f, reg := newRWGateFixture(t, 0, 30)
	var err error
	f.w, err = New(f.pool, f.q, f.store, f.renderer, Options{
		Fallback: FallbackOptions{Enabled: true,
			Gate:   FallbackGate{Threshold: 0},
			Budget: FallbackBudget{Ceiling: 30},
		},
		Engine:             rwFBCandidateEngine,
		EngineSkipPatterns: []string{rwSkipPattern},
		Metrics:            reg,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://hh.ru/", &raw)

	// The suspended window state (the T-239 machine: the transition into
	// hold is covered by the FallbackBudget tests; here — the decision of
	// a skip candidate against the held state).
	f.w.fbMu.Lock()
	f.w.fbBudget.hold = true
	f.w.fbMu.Unlock()

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
		t.Fatalf("a skip candidate with the budget suspended — a normal finalization, not an error: %v", err)
	}
	if n := rwFBCount(t, f); n != 0 {
		t.Errorf("q:render-fallback publications = %d, 0 needed (the window is suspended, FR-9b)", n)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt == nil {
		t.Error("the render_failed_at marker must be set (a suspension = the finalization of today, spec §4.4 item 4)")
	}
	pubs := f.q.publishesSnapshot()
	if len(pubs) != 1 || pubs[0].stream != queue.StreamAnalyze {
		t.Fatalf("the publications %+v, the finalization q:analyze is needed", pubs)
	}
}

// TestRenderWorker_EngineSkipDuplicateDelivery — a re-delivery of an
// already published row (an XACK failure after the publication): the dedup
// filter by the row key + the episode's checked_at acknowledges the
// duplicate WITHOUT a second q:render-fallback entry and without a marker
// (the same filter as for the tarpit/task-class candidates, spec §4.4
// item 5).
func TestRenderWorker_EngineSkipDuplicateDelivery(t *testing.T) {
	f := newRWSkipFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://hh.ru/", &raw)

	for i, attempts := range []int{1, 2} {
		if err := f.w.processDelivery(context.Background(), rwDelivery(fmt.Sprintf("e%d", i+1)), attempts); err != nil {
			t.Fatalf("delivery %d: %v", i+1, err)
		}
	}
	if n := rwFBCount(t, f); n != 1 {
		t.Errorf("q:render-fallback publications = %d, 1 needed (a duplicate does not duplicate the stream)", n)
	}
	for _, p := range f.q.publishesSnapshot() {
		if p.stream == queue.StreamAnalyze {
			t.Errorf("q:analyze is not published on a duplicate: %+v", p)
		}
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("the marker must not be set — the record waits for the fallback (FR-10/FR-11)")
	}
}

// TestRenderWorker_EngineSkipNoRow — no row (the recheck rewrote it) or no
// final URL: the skip path is a regular rejection without publications —
// the same no-op as the cert/DNS finalizations (finalizeNoRender), the
// delivery is acknowledged.
func TestRenderWorker_EngineSkipNoRow(t *testing.T) {
	f := newRWSkipFixture(t)

	// There is no row at all.
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err != nil {
		t.Fatalf("no row — not an error: %v", err)
	}
	// A row without a final URL: the check has nothing to match.
	f.pool.seedRow(rwKey, "", nil)
	if err := f.w.processDelivery(context.Background(), rwDelivery("e2"), 1); err != nil {
		t.Fatalf("no final URL — not an error: %v", err)
	}
	if pubs := f.q.publishesSnapshot(); len(pubs) != 0 {
		t.Errorf("there must be no publications: %+v", pubs)
	}
}

// TestRenderWorker_EngineSkipPublishError — a q:render-fallback
// publication failure at the skip: an error WITHOUT a marker and without
// q:analyze — the delivery will return and replay (nothing was changed,
// idempotent by construction); the repeat publishes (the dedup filter
// released the failed attempt's key).
func TestRenderWorker_EngineSkipPublishError(t *testing.T) {
	f := newRWSkipFixture(t)
	raw := rwTestHash(1)
	f.pool.seedRow(rwKey, "https://hh.ru/", &raw)
	f.q.publishErrs = []error{errors.New("redis unavailable")}

	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 1); err == nil {
		t.Fatal("a fallback publication failure at the skip — an error (a re-delivery)")
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("the marker must not be set on a publication failure — the delivery will replay")
	}
	for _, p := range f.q.publishesSnapshot() {
		if p.stream == queue.StreamAnalyze {
			t.Errorf("q:analyze is not published on a failed fallback attempt: %+v", p)
		}
	}

	// The repeat is idempotent: the publication goes through, still no marker.
	if err := f.w.processDelivery(context.Background(), rwDelivery("e1"), 2); err != nil {
		t.Fatalf("the re-delivery after a publication failure must publish: %v", err)
	}
	fb := 0
	for _, p := range f.q.publishesSnapshot() {
		switch p.stream {
		case queue.StreamRenderFallback:
			fb++
		case queue.StreamAnalyze:
			t.Error("q:analyze is published by worker B, not the primary worker")
		}
	}
	if fb != 2 {
		// The first (failed) attempt is also recorded by the fake: there is one
		// successful publication — the dedup filter did not eat the retry
		// after the release.
		t.Errorf("q:render-fallback publication attempts = %d, 2 needed (a failure + a success), one in the stream", fb)
	}
	if row := rwRowSnapshot(f.pool, rwKey); row.failedAt != nil {
		t.Error("the marker must not be set — the record waits for the fallback")
	}
}

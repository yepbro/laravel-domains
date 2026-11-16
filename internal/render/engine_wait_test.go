// Tests of the engine-unavailability wait of Manager.Run (T-283): an
// external engine restart (a cgroup OOM-kill of the container → a docker
// restart) is a routine event, not a task error — the failed generation
// start must be retried within the Options.EngineWait budget with a
// backoff, so the delivery held by the worker does not fail and does not
// burn queue attempts (the pre-T-283 behavior: the first start failure
// returned a ClassBrowser error at once — the delivery went to the PEL,
// claim cycles ate the attempts 2→3, dozens of "stalled delivery not
// acknowledged … connection refused" per outage, the drain froze until
// the claim timeouts). The retry budget of page EXECUTIONS
// (maxRestartsPerTask) must stay intact: a page that kills the engine
// must not multiply its executions (the T-285 argument).
package render

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// fastEngineRetry — shrinks the wait step between generation start
// attempts to a millisecond (the production engineRetryBase is a second —
// a test must not wait it out), restoring it at the end of the test.
func fastEngineRetry(t *testing.T) {
	t.Helper()
	old := engineRetryBase
	engineRetryBase = time.Millisecond
	t.Cleanup(func() { engineRetryBase = old })
}

// managerWithStarts — a manager whose generation start fails the first
// fail times (the engine container is restarting: connection refused),
// then brings up a live browser; counts the start calls.
func managerWithStarts(t *testing.T, opts Options, fail int32) (*Manager, *atomic.Int32) {
	t.Helper()
	m := NewManager(context.Background(), opts)
	var starts atomic.Int32
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		if starts.Add(1) <= fail {
			return nil, &Error{Class: ClassBrowser,
				Err: errors.New("connection refused (the engine container is restarting)")}
		}
		b, _ := liveBrowser(1)
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool { return poolOver(b, nil) }
	return m, &starts
}

func TestManagerRunWaitsEngineRecovery(t *testing.T) {
	// The acceptance of T-283: an engine outage shorter than the wait
	// budget does not fail the delivery — Run waits out the failed
	// starts, then the task renders on the recovered engine and returns
	// a result (no error, no queue re-delivery).
	fastEngineRetry(t)
	opts := managerOpts()
	opts.EngineWait = 50 * time.Millisecond
	m, starts := managerWithStarts(t, opts, 2) // two refusals, then the engine is up

	out, err := m.Run(context.Background(), RenderTask{URL: "https://a.example/"})
	if err != nil {
		t.Fatalf("Run did not wait out the engine restart: %v", err)
	}
	if out.Title != "ok" {
		t.Fatalf("the task did not render on the recovered engine: %+v", out)
	}
	if got := starts.Load(); got != 3 {
		t.Errorf("start attempts %d, want 3 (two refused + one after the recovery)", got)
	}
	if got := m.Restarts(); got != 0 {
		t.Errorf("restarts %d, want 0 (the first successful start is not a restart)", got)
	}
}

func TestManagerRunEngineWaitBudgetExhausted(t *testing.T) {
	// The engine is down longer than the budget: after the budget is
	// exhausted the start error goes upward as before (a re-delivery is
	// the queue level, R5 of the outcome policy) — the wait must not
	// hang the worker forever.
	fastEngineRetry(t)
	opts := managerOpts()
	opts.EngineWait = 20 * time.Millisecond
	m, starts := managerWithStarts(t, opts, 1<<30) // always refuses

	_, err := m.Run(context.Background(), RenderTask{URL: "https://a.example/"})
	if ClassOf(err) != ClassBrowser {
		t.Fatalf("after the budget: the class %v (%v), want browser", ClassOf(err), err)
	}
	if got := starts.Load(); got < 2 {
		t.Errorf("start attempts %d, want ≥ 2 (the failed start is retried while the budget lasts)", got)
	}
}

func TestManagerRunEngineWaitOff(t *testing.T) {
	// EngineWait=0 — the control is off: the start error is returned
	// immediately, the start is not retried (the zero behavior, like
	// MemoryThreshold=0; isolated builds and tests without the config).
	opts := managerOpts() // EngineWait is not set — 0
	m, starts := managerWithStarts(t, opts, 1<<30)

	_, err := m.Run(context.Background(), RenderTask{URL: "https://a.example/"})
	if ClassOf(err) != ClassBrowser {
		t.Fatalf("the start error: the class %v (%v), want browser", ClassOf(err), err)
	}
	if got := starts.Load(); got != 1 {
		t.Errorf("start attempts %d, want 1 (no wait — no retry)", got)
	}
}

func TestManagerRunEngineWaitCancel(t *testing.T) {
	// A graceful shutdown during the wait: the ctx cancellation exits
	// the wait at once with a CONTEXT-class error — the delivery will
	// return with a re-delivery (spec 3.4), and the worker's stage
	// error counter / the fallback gate streak must not count a
	// shutdown as an engine outage (the classification matches the
	// mid-page shutdown — ClassContext, the T-283 review P3).
	fastEngineRetry(t)
	opts := managerOpts()
	opts.EngineWait = time.Minute
	m, _ := managerWithStarts(t, opts, 1<<30)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := m.Run(ctx, RenderTask{URL: "https://a.example/"})
	if ClassOf(err) != ClassContext {
		t.Fatalf("the cancelled wait: the class %v (%v), want context (not the engine's browser class)", ClassOf(err), err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("the cancelled wait: the error %v does not carry context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("the wait did not exit on the ctx cancellation: %s", elapsed)
	}
}

func TestManagerRunEngineRecoveryCountsRestart(t *testing.T) {
	// The canonical T-283 path (the acceptance's metric criterion): the
	// engine dies under a LIVE generation, the start retries fail while
	// the container restarts, the recovery completes the replacement —
	// Restarts() (the source of
	// crawler_render_browser_restarts_total) fixes the engine restart.
	// Before the review fix (P1) the counter grew only when the start
	// succeeded while the old pool was still in the slot: the death →
	// failed start (the slot is cleared, T4) → recovery path completed
	// as T2 and was not counted.
	fastEngineRetry(t)
	opts := managerOpts()
	opts.EngineWait = 50 * time.Millisecond
	m := NewManager(context.Background(), opts)
	b1, cancel1 := liveBrowser(1)
	var starts atomic.Int32
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		switch starts.Add(1) {
		case 1:
			return b1, nil // the first generation
		case 2, 3: // the engine container is restarting: connection refused
			return nil, &Error{Class: ClassBrowser, Err: errors.New("connection refused")}
		}
		b, _ := liveBrowser(1)
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool { return poolOver(b, nil) }

	if _, err := m.Run(context.Background(), RenderTask{URL: "https://a.example/1"}); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if got := m.Restarts(); got != 0 {
		t.Fatalf("restarts after the first start: %d, want 0", got)
	}
	cancel1() // the engine died under the live generation (a cgroup OOM-kill)
	out, err := m.Run(context.Background(), RenderTask{URL: "https://a.example/2"})
	if err != nil {
		t.Fatalf("Run 2 did not wait out the engine restart: %v", err)
	}
	if out.Title != "ok" {
		t.Fatalf("the task did not render on the recovered engine: %+v", out)
	}
	if got := m.Restarts(); got != 1 {
		t.Fatalf("restarts %d, want 1 (the death → retries → recovery replacement is counted)", got)
	}
	if got := starts.Load(); got != 4 {
		t.Errorf("start attempts %d, want 4 (the live start + two refused + the recovery)", got)
	}
}

func TestManagerRunEngineWaitKeepsExecutionBudget(t *testing.T) {
	// The wait applies ONLY to failed generation STARTS (the engine is
	// unreachable), not to page executions: a page whose render kills
	// the engine (the T-285 hh.ru case, an unknown page-killer) still
	// gets at most 1+maxRestartsPerTask executions — the wait must not
	// multiply them.
	fastEngineRetry(t)
	opts := managerOpts()
	opts.EngineWait = 50 * time.Millisecond
	m := NewManager(context.Background(), opts)
	var sessions, starts atomic.Int32
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		starts.Add(1)
		b, _ := liveBrowser(1)
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool {
		p := NewPool(b)
		p.newSession = func(context.Context) (renderSession, error) {
			sessions.Add(1)
			return &fakeSession{run: func(context.Context, RenderTask) (RenderOutput, error) {
				return RenderOutput{}, &Error{Class: ClassBrowser, Err: ErrBrowserDead}
			}}, nil
		}
		return p
	}

	_, err := m.Run(context.Background(), RenderTask{URL: "u"})
	if ClassOf(err) != ClassBrowser {
		t.Fatalf("the class %v (%v), want browser", ClassOf(err), err)
	}
	if got := sessions.Load(); got != 1+maxRestartsPerTask {
		t.Errorf("task executions %d, want %d (the wait does not multiply page executions)", got, 1+maxRestartsPerTask)
	}
}

func TestEngineRetryStepGrowth(t *testing.T) {
	// The backoff ladder of the start retries: the first step is
	// engineRetryBase, then doubling up to engineRetryCap (the engine
	// container restarts in tens of seconds — probing it more often than
	// the cap is pointless). 0 — the base (the ladder's start).
	old := engineRetryBase
	engineRetryBase = 10 * time.Millisecond
	t.Cleanup(func() { engineRetryBase = old })

	if got := engineRetryStep(0); got != 10*time.Millisecond {
		t.Errorf("engineRetryStep(0) = %s, want the base 10ms", got)
	}
	if got := engineRetryStep(10 * time.Millisecond); got != 20*time.Millisecond {
		t.Errorf("engineRetryStep(10ms) = %s, want 20ms (doubling)", got)
	}
	if got := engineRetryStep(engineRetryCap); got != engineRetryCap {
		t.Errorf("engineRetryStep(%s) = %s, want the cap %s", engineRetryCap, got, engineRetryCap)
	}
}

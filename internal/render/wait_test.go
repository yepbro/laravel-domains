package render

// The unit tests of the wait strategy (wait.go, spec 5.6 / T-063): the
// phases are moved by fake events (the waitEvents channels) and fake
// timers — real time is not used, the order of the scenario steps is
// deterministic (exactly one case is ready in the select at every
// step). The live behavior against Chromium — wait_live_test.go.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/cdproto"
)

// fakeWaitTimer — a timer on a fake clock: fires only by an explicit
// fire() from the test. Reset drains an undelivered value (the Timer
// semantics of Go ≥ 1.23), the Reset/Stop counters are visible to the
// test.
type fakeWaitTimer struct {
	ch      chan time.Time
	resets  atomic.Int32
	stopped atomic.Bool
}

func (f *fakeWaitTimer) C() <-chan time.Time { return f.ch }
func (f *fakeWaitTimer) Reset(d time.Duration) {
	select {
	case <-f.ch:
	default:
	}
	f.resets.Add(1)
}
func (f *fakeWaitTimer) Stop() { f.stopped.Store(true) }
func (f *fakeWaitTimer) fire() {
	select {
	case f.ch <- time.Time{}:
	default:
	}
}

// fakeWaitClock — the timer factory: remembers the created timers by
// their durations. The tests must use plans with non-repeating phase
// durations (loadCap, quiet, settleCap are distinguishable).
type fakeWaitClock struct {
	mu     sync.Mutex
	timers map[time.Duration]*fakeWaitTimer
	total  int
}

func (c *fakeWaitClock) timer(d time.Duration) waitTimer {
	t := &fakeWaitTimer{ch: make(chan time.Time, 1)}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.timers == nil {
		c.timers = make(map[time.Duration]*fakeWaitTimer)
	}
	c.timers[d] = t
	c.total++
	return t
}

// byDur — the phase timer with the given duration (nil if not yet
// created).
func (c *fakeWaitClock) byDur(d time.Duration) *fakeWaitTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.timers[d]
}

// nTimers — how many timers have been created.
func (c *fakeWaitClock) nTimers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.total
}

// waitTestPlan — a plan with distinguishable phase durations.
func waitTestPlan() WaitPlan {
	return WaitPlan{Load: 1 * time.Second, Quiet: 2 * time.Second, Settle: 3 * time.Second}
}

// waitResult — the waitLoop outcome for a scenario.
type waitResult struct {
	out WaitOutcome
	err error
}

// startWaitLoop runs waitLoop in a goroutine; the scenario steps are
// performed by the test itself, the result is collected via waitDone.
func startWaitLoop(ctx context.Context, plan WaitPlan, evs waitEvents, clock *fakeWaitClock) <-chan waitResult {
	done := make(chan waitResult, 1)
	go func() {
		out, err := waitLoop(ctx, plan, evs, clock.timer)
		done <- waitResult{out: out, err: err}
	}()
	return done
}

func waitDone(t *testing.T, done <-chan waitResult) waitResult {
	t.Helper()
	select {
	case r := <-done:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("waitLoop did not finish")
		return waitResult{}
	}
}

// waitPhase2 waits for entering the quiet phase: all three timers have
// been created.
func waitPhase2(t *testing.T, clock *fakeWaitClock) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if clock.nTimers() >= 3 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("the quiet phase did not start: %d timers, want 3", clock.nTimers())
}

// waitResets waits until the quiet window is reset n times.
func waitResets(t *testing.T, clock *fakeWaitClock, quiet time.Duration, n int32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if q := clock.byDur(quiet); q != nil && q.resets.Load() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("the quiet window was not reset %d times", n)
}

// waitTimerByDur waits for the creation of the phase timer with the
// given duration.
func waitTimerByDur(t *testing.T, clock *fakeWaitClock, d time.Duration) *fakeWaitTimer {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if q := clock.byDur(d); q != nil {
			return q
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("the %s phase timer was not created", d)
	return nil
}

func TestDefaultWaitPlan(t *testing.T) {
	got := DefaultWaitPlan(25 * time.Second)
	want := WaitPlan{Load: 12500 * time.Millisecond, Quiet: 2 * time.Second, Settle: 5 * time.Second}
	if got != want {
		t.Fatalf("DefaultWaitPlan(25s) = %+v, want %+v", got, want)
	}
	// The caps are shares of the budget; the quiet window is the
	// request-rate constant but at most half of the phase cap
	// (otherwise it would never be confirmed).
	got = DefaultWaitPlan(10 * time.Second)
	want = WaitPlan{Load: 5 * time.Second, Quiet: 1 * time.Second, Settle: 2 * time.Second}
	if got != want {
		t.Fatalf("DefaultWaitPlan(10s) = %+v, want %+v", got, want)
	}
	// A standard budget above the threshold — the window is not
	// compressed.
	if q := DefaultWaitPlan(15 * time.Second).Quiet; q != 1500*time.Millisecond {
		t.Errorf("DefaultWaitPlan(15s).Quiet = %s, want 1.5s (half the cap)", q)
	}
	// A degenerate tiny budget: the phases are positive, the window is
	// no longer than half of the cap — the quiet confirmation remains
	// possible.
	got = DefaultWaitPlan(time.Second)
	if got.Load <= 0 || got.Quiet <= 0 || got.Settle <= 0 {
		t.Fatalf("DefaultWaitPlan(1s) with non-positive phases: %+v", got)
	}
	if got.Quiet > got.Settle/2 {
		t.Fatalf("DefaultWaitPlan(1s): the window %s > half the cap %s", got.Quiet, got.Settle/2)
	}
}

func TestWaitLoopLoadThenQuiet(t *testing.T) {
	plan := waitTestPlan()
	evs := waitEvents{load: make(chan struct{}), reqs: make(chan struct{}, waitReqsBuffer)}
	clock := &fakeWaitClock{}
	close(evs.load) // the load event has already arrived

	done := startWaitLoop(context.Background(), plan, evs, clock)
	waitPhase2(t, clock) // the load phase passed instantly

	// Silence: the window expires without a single request.
	clock.byDur(plan.Quiet).fire()

	r := waitDone(t, done)
	if r.err != nil {
		t.Fatalf("waitLoop: %v", r.err)
	}
	if !r.out.LoadFired || !r.out.Quiet {
		t.Fatalf("the outcome %+v, want {LoadFired:true Quiet:true}", r.out)
	}
	if q := clock.byDur(plan.Quiet); q.resets.Load() != 0 {
		t.Errorf("the window was reset %d times, want 0 (there were no requests)", q.resets.Load())
	}
	if c := clock.byDur(plan.Load); !c.stopped.Load() {
		t.Error("the load phase cap was not stopped after the load event")
	}
	if c := clock.byDur(plan.Settle); !c.stopped.Load() {
		t.Error("the quiet phase cap was not stopped on the window confirmation")
	}
}

func TestWaitLoopLoadCapProceeds(t *testing.T) {
	// load did not arrive: the phase cap is soft — the wait continues
	// with the quiet window.
	plan := waitTestPlan()
	evs := waitEvents{load: make(chan struct{}), reqs: make(chan struct{}, waitReqsBuffer)}
	clock := &fakeWaitClock{}

	done := startWaitLoop(context.Background(), plan, evs, clock)
	// The load phase cap expires without the load event.
	waitTimerByDur(t, clock, plan.Load).fire()
	waitPhase2(t, clock)
	clock.byDur(plan.Quiet).fire()

	r := waitDone(t, done)
	if r.err != nil {
		t.Fatalf("waitLoop: %v", r.err)
	}
	if !r.out.Quiet {
		t.Fatalf("the outcome %+v: the quiet window held out, Quiet must be true", r.out)
	}
}

func TestWaitLoopRequestsMoveWindow(t *testing.T) {
	plan := waitTestPlan()
	evs := waitEvents{load: make(chan struct{}), reqs: make(chan struct{}, waitReqsBuffer)}
	clock := &fakeWaitClock{}
	close(evs.load)

	done := startWaitLoop(context.Background(), plan, evs, clock)
	waitPhase2(t, clock)

	// Every new request restarts the window.
	evs.reqs <- struct{}{}
	waitResets(t, clock, plan.Quiet, 1)
	evs.reqs <- struct{}{}
	waitResets(t, clock, plan.Quiet, 2)

	// After the last request the window is held — done.
	clock.byDur(plan.Quiet).fire()
	r := waitDone(t, done)
	if r.err != nil {
		t.Fatalf("waitLoop: %v", r.err)
	}
	if !r.out.Quiet {
		t.Fatalf("the outcome %+v: the window held out, Quiet must be true", r.out)
	}
	if q := clock.byDur(plan.Quiet); q.resets.Load() != 2 {
		t.Errorf("window resets %d, want 2", q.resets.Load())
	}
}

func TestWaitLoopSettleCap(t *testing.T) {
	// Continuous traffic: the window does not converge, the phase is
	// cut off by the cap — the result is captured as is (not an error).
	plan := waitTestPlan()
	evs := waitEvents{load: make(chan struct{}), reqs: make(chan struct{}, waitReqsBuffer)}
	clock := &fakeWaitClock{}
	close(evs.load)

	done := startWaitLoop(context.Background(), plan, evs, clock)
	waitPhase2(t, clock)
	evs.reqs <- struct{}{}
	waitResets(t, clock, plan.Quiet, 1)

	clock.byDur(plan.Settle).fire()
	r := waitDone(t, done)
	if r.err != nil {
		t.Fatalf("waitLoop: %v (the quiet cap is not an error)", r.err)
	}
	if r.out.Quiet {
		t.Fatalf("the outcome %+v: the network did not quiet down, Quiet must be false", r.out)
	}
	if q := clock.byDur(plan.Quiet); !q.stopped.Load() {
		t.Error("the quiet window was not stopped on the cap hangup")
	}
}

func TestWaitLoopSettleCapBelowWindow(t *testing.T) {
	// A degenerate plan (the phase cap is shorter than the window): a
	// cut-off by the cap, the window never gets to fire — the behavior
	// is the same, the "bounded wait" remains bounded.
	plan := WaitPlan{Load: 1 * time.Second, Quiet: 10 * time.Second, Settle: 100 * time.Millisecond}
	evs := waitEvents{load: make(chan struct{}), reqs: make(chan struct{}, waitReqsBuffer)}
	clock := &fakeWaitClock{}
	close(evs.load)

	done := startWaitLoop(context.Background(), plan, evs, clock)
	waitPhase2(t, clock)
	clock.byDur(plan.Settle).fire()

	r := waitDone(t, done)
	if r.err != nil {
		t.Fatalf("waitLoop: %v", r.err)
	}
	if r.out.Quiet {
		t.Fatalf("the outcome %+v: the cap is shorter than the window, Quiet must be false", r.out)
	}
}

func TestWaitLoopStaleRequestsDrained(t *testing.T) {
	// The requests issued before entering the quiet phase do not
	// belong to the new window: the window is counted from entering
	// the phase, the stale signals are drained.
	plan := waitTestPlan()
	evs := waitEvents{load: make(chan struct{}), reqs: make(chan struct{}, waitReqsBuffer)}
	clock := &fakeWaitClock{}
	evs.reqs <- struct{}{}
	evs.reqs <- struct{}{}
	close(evs.load)

	done := startWaitLoop(context.Background(), plan, evs, clock)
	waitPhase2(t, clock)
	// Let the loop pick the ready cases: besides draining the stale
	// signals there are no ready ones; if the signals moved the window,
	// the counter would have grown.
	time.Sleep(50 * time.Millisecond)
	if q := clock.byDur(plan.Quiet); q != nil && q.resets.Load() != 0 {
		t.Fatalf("old signals move the window: resets %d, want 0", q.resets.Load())
	}

	clock.byDur(plan.Quiet).fire()
	r := waitDone(t, done)
	if r.err != nil || !r.out.Quiet {
		t.Fatalf("the outcome %+v, err %v: want {Quiet:true} without an error", r.out, r.err)
	}
}

func TestWaitLoopContextCanceled(t *testing.T) {
	t.Run("the load phase", func(t *testing.T) {
		plan := waitTestPlan()
		evs := waitEvents{load: make(chan struct{}), reqs: make(chan struct{}, waitReqsBuffer)}
		clock := &fakeWaitClock{}
		ctx, cancel := context.WithCancel(context.Background())

		done := startWaitLoop(ctx, plan, evs, clock)
		cancel()

		r := waitDone(t, done)
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("the error %v, want context.Canceled", r.err)
		}
		if r.out.LoadFired || r.out.Quiet {
			t.Fatalf("the outcome %+v on cancellation, want zero", r.out)
		}
	})

	t.Run("the quiet phase", func(t *testing.T) {
		plan := waitTestPlan()
		evs := waitEvents{load: make(chan struct{}), reqs: make(chan struct{}, waitReqsBuffer)}
		clock := &fakeWaitClock{}
		ctx, cancel := context.WithCancel(context.Background())
		close(evs.load)

		done := startWaitLoop(ctx, plan, evs, clock)
		waitPhase2(t, clock)
		cancel()

		r := waitDone(t, done)
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("the error %v, want context.Canceled", r.err)
		}
	})
}

func TestWaitLoopOverallDeadline(t *testing.T) {
	// The overall timeout (the hard limit of the visit) cuts the wait
	// from inside — a real context deadline; the fake timers do not
	// fire.
	plan := waitTestPlan()
	evs := waitEvents{load: make(chan struct{}), reqs: make(chan struct{}, waitReqsBuffer)}
	clock := &fakeWaitClock{}
	ctx, stop := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer stop()

	done := startWaitLoop(ctx, plan, evs, clock)
	r := waitDone(t, done)
	if !errors.Is(r.err, context.DeadlineExceeded) {
		t.Fatalf("the error %v, want context.DeadlineExceeded", r.err)
	}
	if r.out.Quiet {
		t.Fatalf("the outcome %+v: the window was not confirmed", r.out)
	}
}

// TestWaitSubscribe — the waitSubscribe subscriptions and pumps against
// the in-memory chromedp transport (cdpfake_test.go; no live browser
// needed): the load event closes load once, network requests are
// signalled into reqs with a buffer of exactly waitReqsBuffer, a
// Network.enable failure is returned as an error; a context
// cancellation and an event decoding error produce neither a load
// closure nor a false reqs signal.
func TestWaitSubscribe(t *testing.T) {
	t.Run("the load event closes load exactly once", func(t *testing.T) {
		ctx, tr := newFakeCDPContext(t)
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()
		evs, err := waitSubscribe(wctx)
		if err != nil {
			t.Fatalf("waitSubscribe: %v", err)
		}
		tr.fireSession("Page.loadEventFired", `{}`)
		select {
		case <-evs.load:
		case <-time.After(2 * time.Second):
			t.Fatal("the load event did not close the load channel")
		}
		// A repeated event — no repeated-close panic (sync.Once).
		tr.fireSession("Page.loadEventFired", `{}`)
		select {
		case <-evs.load:
		case <-time.After(50 * time.Millisecond):
		}
	})

	t.Run("a network request is signaled into reqs", func(t *testing.T) {
		ctx, tr := newFakeCDPContext(t)
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()
		evs, err := waitSubscribe(wctx)
		if err != nil {
			t.Fatalf("waitSubscribe: %v", err)
		}
		tr.fireSession("Network.requestWillBeSent", `{"requestId":"r-1"}`)
		select {
		case <-evs.reqs:
		case <-time.After(2 * time.Second):
			t.Fatal("a network request was not signaled into reqs")
		}
	})

	t.Run("a Network.enable refusal — a subscription error", func(t *testing.T) {
		ctx, tr := newFakeCDPContext(t)
		tr.setError("Network.enable", &cdproto.Error{Code: -32000, Message: "the network is unavailable"})
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()
		_, err := waitSubscribe(wctx)
		if err == nil || !strings.Contains(err.Error(), "the network is unavailable") {
			t.Fatalf("the error %v, want a Network.enable refusal", err)
		}
	})

	t.Run("a context cancellation quietly stops the pumps", func(t *testing.T) {
		ctx, _ := newFakeCDPContext(t)
		wctx, cancel := context.WithCancel(ctx)
		evs, err := waitSubscribe(wctx)
		if err != nil {
			t.Fatalf("waitSubscribe: %v", err)
		}
		cancel()
		// An iterator error is terminal: the pumps exit silently, without
		// closing load and without a false reqs signal.
		//
		// The negative-check window is generous (1 s, not 100 ms). The
		// mutant wait.go.6 (branch/if, checksum
		// bf3a81de80cc2c57e11a60cf904c367c) cuts the return at
		// err != nil and falls through into close(evs.load) — if the
		// iterator error is delivered at all, the closure arrives within
		// microseconds, but under the CPU load of the mutation run
		// (hundreds of concurrent go test) the scheduler may delay the
		// delivery beyond a short window. 1 s × ~455 mutants of the
		// package ≈ +8 min to the render mutation run — an acceptable
		// price.
		//
		// Important: this check is only a guard; the kill of the
		// wait.go.6/.7 mutants is held by the adjacent subtest with the
		// decoding error below. A ctx cancellation is in principle a
		// race in the chromedp teardown (v0.21.0): endingSession closes
		// the subscription channel via context.AfterFunc, and the select
		// of the cdp.Events iterator sees both ready branches —
		// ctx.Done() (yielding the error) and the closed channel (a
		// silent exit without an error); the choice is random, so here
		// the mutant is killed only with a ~50–90% probability
		// (measured: 35/40 on a quiet machine), whatever the window.
		select {
		case <-evs.load:
			t.Fatal("the context cancellation closed the load channel")
		case <-time.After(1 * time.Second):
		}
		n := 0
		draining := true
		for draining {
			select {
			case <-evs.reqs:
				n++
			default:
				draining = false
			}
		}
		if n != 0 {
			t.Errorf("the context cancellation gave %d false reqs signals, want 0", n)
		}
	})

	t.Run("an event decoding error stops the pumps", func(t *testing.T) {
		ctx, tr := newFakeCDPContext(t)
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()
		evs, err := waitSubscribe(wctx)
		if err != nil {
			t.Fatalf("waitSubscribe: %v", err)
		}
		// A payload not decodable into the event type: the cdp.Events
		// iterator itself yields a decoding error (jsonv2.Unmarshal) and
		// ends the iteration — a terminal error of the pumps, like the
		// ctx cancellation but WITHOUT the teardown race: the
		// subscription is alive, the channel is not closed, the
		// iterator's select has a single ready branch. Therefore the
		// kill here is deterministic and does not depend on the
		// scheduler:
		//
		//   - the original: if err != nil → return — the pumps exit
		//     silently;
		//   - the mutants wait.go.6/.7 (the cut return): fall through
		//     into the loop body — close(evs.load) / a false reqs
		//     signal.
		//
		// The check is negative (the original — silence), the window is
		// 1 s: the mutants' reaction arrives in-memory within
		// microseconds after fireSession.
		tr.fireSession("Page.loadEventFired", `[1]`)
		tr.fireSession("Network.requestWillBeSent", `[1]`)
		select {
		case <-evs.load:
			t.Fatal("the decoding error closed the load channel")
		case <-time.After(1 * time.Second):
		}
		n := 0
		draining := true
		for draining {
			select {
			case <-evs.reqs:
				n++
			default:
				draining = false
			}
		}
		if n != 0 {
			t.Errorf("the decoding error gave %d false reqs signals, want 0", n)
		}
	})
}

// TestWaitSubscribeReqsBuffer — the reqs channel buffer holds exactly
// 64 signals (waitReqsBuffer): the pump does not block the tab's event
// flow, extra signals merge (the same quiet window reset). The
// literals 64/65 are pinned deliberately: the waitReqsBuffer symbol in
// the test would follow the constant's mutant and would distinguish
// nothing. Determinism: 65 events with an interval (each manages to
// pass the delivery chain before the next), then a positive wait for
// the buffer to fill up to 64 and one more interval — the 65th event
// has been processed by then (merged at the original, fit in at the 65
// mutant); the drain fixes the outcome.
func TestWaitSubscribeReqsBuffer(t *testing.T) {
	ctx, tr := newFakeCDPContext(t)
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	evs, err := waitSubscribe(wctx)
	if err != nil {
		t.Fatalf("waitSubscribe: %v", err)
	}
	for i := 0; i < 65; i++ {
		tr.fireSession("Network.requestWillBeSent", `{"requestId":"r"}`)
		time.Sleep(2 * time.Millisecond)
	}
	// The buffer has filled (64 at the original and the mutant
	// alike)…
	deadline := time.Now().Add(2 * time.Second)
	for len(evs.reqs) < 64 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(evs.reqs) < 64 {
		t.Fatalf("the reqs buffer did not fill up: %d signals of 64", len(evs.reqs))
	}
	// …and the last event has been given the same interval as between
	// the events: at the original its send is merged (the buffer is
	// full), at the 65 mutant it has fit in.
	time.Sleep(50 * time.Millisecond)

	n := 0
	draining := true
	for draining {
		select {
		case <-evs.reqs:
			n++
		default:
			draining = false
		}
	}
	if n != 64 {
		t.Errorf("the reqs buffer held %d signals, want exactly 64 (the 65th sleeps)", n)
	}
}

// TestWaitRealTimer — the real timer wrapper: Reset restarts the
// countdown (the firing moves to the new deadline), Stop kills an
// unfired one. Real time — milliseconds, no phase-wait pauses.
func TestWaitRealTimer(t *testing.T) {
	t.Run("Reset restarts the countdown", func(t *testing.T) {
		timer := waitRealTimerFactory(10 * time.Second)
		timer.Reset(10 * time.Millisecond)
		select {
		case <-timer.C():
		case <-time.After(2 * time.Second):
			t.Fatal("after Reset the timer did not fire on the new deadline")
		}
	})
	t.Run("Stop kills an unfired timer", func(t *testing.T) {
		timer := waitRealTimerFactory(50 * time.Millisecond)
		timer.Stop()
		select {
		case <-timer.C():
			t.Fatal("the timer fired after Stop")
		case <-time.After(300 * time.Millisecond):
		}
	})
}

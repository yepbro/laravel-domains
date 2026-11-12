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
		t.Fatal("waitLoop не завершился")
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
	t.Fatalf("фаза затишья не началась: таймеров %d, хочу 3", clock.nTimers())
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
	t.Fatalf("окно затишья не сброшено %d раз", n)
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
	t.Fatalf("таймер фазы %s не создан", d)
	return nil
}

func TestDefaultWaitPlan(t *testing.T) {
	got := DefaultWaitPlan(25 * time.Second)
	want := WaitPlan{Load: 12500 * time.Millisecond, Quiet: 2 * time.Second, Settle: 5 * time.Second}
	if got != want {
		t.Fatalf("DefaultWaitPlan(25s) = %+v, хочу %+v", got, want)
	}
	// The caps are shares of the budget; the quiet window is the
	// request-rate constant but at most half of the phase cap
	// (otherwise it would never be confirmed).
	got = DefaultWaitPlan(10 * time.Second)
	want = WaitPlan{Load: 5 * time.Second, Quiet: 1 * time.Second, Settle: 2 * time.Second}
	if got != want {
		t.Fatalf("DefaultWaitPlan(10s) = %+v, хочу %+v", got, want)
	}
	// A standard budget above the threshold — the window is not
	// compressed.
	if q := DefaultWaitPlan(15 * time.Second).Quiet; q != 1500*time.Millisecond {
		t.Errorf("DefaultWaitPlan(15s).Quiet = %s, хочу 1.5s (половина потолка)", q)
	}
	// A degenerate tiny budget: the phases are positive, the window is
	// no longer than half of the cap — the quiet confirmation remains
	// possible.
	got = DefaultWaitPlan(time.Second)
	if got.Load <= 0 || got.Quiet <= 0 || got.Settle <= 0 {
		t.Fatalf("DefaultWaitPlan(1s) с неположительными фазами: %+v", got)
	}
	if got.Quiet > got.Settle/2 {
		t.Fatalf("DefaultWaitPlan(1s): окно %s > половины потолка %s", got.Quiet, got.Settle/2)
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
		t.Fatalf("исход %+v, хочу {LoadFired:true Quiet:true}", r.out)
	}
	if q := clock.byDur(plan.Quiet); q.resets.Load() != 0 {
		t.Errorf("окно сброшено %d раз, хочу 0 (запросов не было)", q.resets.Load())
	}
	if c := clock.byDur(plan.Load); !c.stopped.Load() {
		t.Error("потолок фазы загрузки не остановлен после события load")
	}
	if c := clock.byDur(plan.Settle); !c.stopped.Load() {
		t.Error("потолок фазы затишья не остановлен при подтверждении окна")
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
		t.Fatalf("исход %+v: окно затишья выдержано, Quiet должен быть true", r.out)
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
		t.Fatalf("исход %+v: окно выдержано, Quiet должен быть true", r.out)
	}
	if q := clock.byDur(plan.Quiet); q.resets.Load() != 2 {
		t.Errorf("сбросов окна %d, хочу 2", q.resets.Load())
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
		t.Fatalf("waitLoop: %v (потолок затишья — не ошибка)", r.err)
	}
	if r.out.Quiet {
		t.Fatalf("исход %+v: сеть не затихла, Quiet должен быть false", r.out)
	}
	if q := clock.byDur(plan.Quiet); !q.stopped.Load() {
		t.Error("окно затишья не остановлено при обрыве по потолку")
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
		t.Fatalf("исход %+v: потолок короче окна, Quiet должен быть false", r.out)
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
		t.Fatalf("старые сигналы двигают окно: сбросов %d, хочу 0", q.resets.Load())
	}

	clock.byDur(plan.Quiet).fire()
	r := waitDone(t, done)
	if r.err != nil || !r.out.Quiet {
		t.Fatalf("исход %+v, err %v: хочу {Quiet:true} без ошибки", r.out, r.err)
	}
}

func TestWaitLoopContextCanceled(t *testing.T) {
	t.Run("фаза загрузки", func(t *testing.T) {
		plan := waitTestPlan()
		evs := waitEvents{load: make(chan struct{}), reqs: make(chan struct{}, waitReqsBuffer)}
		clock := &fakeWaitClock{}
		ctx, cancel := context.WithCancel(context.Background())

		done := startWaitLoop(ctx, plan, evs, clock)
		cancel()

		r := waitDone(t, done)
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("ошибка %v, хочу context.Canceled", r.err)
		}
		if r.out.LoadFired || r.out.Quiet {
			t.Fatalf("исход %+v при отмене, хочу нулевой", r.out)
		}
	})

	t.Run("фаза затишья", func(t *testing.T) {
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
			t.Fatalf("ошибка %v, хочу context.Canceled", r.err)
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
		t.Fatalf("ошибка %v, хочу context.DeadlineExceeded", r.err)
	}
	if r.out.Quiet {
		t.Fatalf("исход %+v: окно не подтверждено", r.out)
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
	t.Run("событие загрузки закрывает load однократно", func(t *testing.T) {
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
			t.Fatal("событие загрузки не закрыло канал load")
		}
		// A repeated event — no repeated-close panic (sync.Once).
		tr.fireSession("Page.loadEventFired", `{}`)
		select {
		case <-evs.load:
		case <-time.After(50 * time.Millisecond):
		}
	})

	t.Run("сетевой запрос сигнализируется в reqs", func(t *testing.T) {
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
			t.Fatal("сетевой запрос не сигнализирован в reqs")
		}
	})

	t.Run("отказ Network.enable — ошибка подписки", func(t *testing.T) {
		ctx, tr := newFakeCDPContext(t)
		tr.setError("Network.enable", &cdproto.Error{Code: -32000, Message: "сеть недоступна"})
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()
		_, err := waitSubscribe(wctx)
		if err == nil || !strings.Contains(err.Error(), "сеть недоступна") {
			t.Fatalf("ошибка %v, хочу отказ Network.enable", err)
		}
	})

	t.Run("отмена контекста тихо останавливает насосы", func(t *testing.T) {
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
			t.Fatal("отмена контекста закрыла канал load")
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
			t.Errorf("отмена контекста дала %d ложных сигналов reqs, хочу 0", n)
		}
	})

	t.Run("ошибка декодирования события останавливает насосы", func(t *testing.T) {
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
			t.Fatal("ошибка декодирования закрыла канал load")
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
			t.Errorf("ошибка декодирования дала %d ложных сигналов reqs, хочу 0", n)
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
		t.Fatalf("буфер reqs не наполнился: %d сигналов из 64", len(evs.reqs))
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
		t.Errorf("буфер reqs вместил %d сигналов, хочу ровно 64 (65-й слипнут)", n)
	}
}

// TestWaitRealTimer — the real timer wrapper: Reset restarts the
// countdown (the firing moves to the new deadline), Stop kills an
// unfired one. Real time — milliseconds, no phase-wait pauses.
func TestWaitRealTimer(t *testing.T) {
	t.Run("Reset перезапускает отсчёт", func(t *testing.T) {
		timer := waitRealTimerFactory(10 * time.Second)
		timer.Reset(10 * time.Millisecond)
		select {
		case <-timer.C():
		case <-time.After(2 * time.Second):
			t.Fatal("после Reset таймер не сработал по новому сроку")
		}
	})
	t.Run("Stop гасит несработавший", func(t *testing.T) {
		timer := waitRealTimerFactory(50 * time.Millisecond)
		timer.Stop()
		select {
		case <-timer.C():
			t.Fatal("таймер сработал после Stop")
		case <-time.After(300 * time.Millisecond):
		}
	})
}

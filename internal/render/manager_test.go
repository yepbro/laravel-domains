package render

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// liveBrowser — a "live" browser without Chromium: the context has no
// chromedp data and Alive considers it alive (only the context
// cancellation and the connection loss are checked, and there is none).
// cancel lets the test kill the context midway.
func liveBrowser(contexts int) (*Browser, context.CancelFunc) {
	b := testBrowser(contexts)
	ctx, cancel := context.WithCancel(context.Background())
	b.ctx = ctx
	return b, cancel
}

// deadBrowser — a browser with a cancelled context (Alive = false).
func deadBrowser(contexts int) *Browser {
	b, cancel := liveBrowser(contexts)
	cancel()
	return b
}

// poolOver — a pool over b with fake sessions (the pool_test.go
// style); a nil run — a successful session.
func poolOver(b *Browser, run func(context.Context, RenderTask) (RenderOutput, error)) *Pool {
	p := NewPool(b)
	p.newSession = func(context.Context) (renderSession, error) {
		fs := &fakeSession{}
		if run != nil {
			fs.run = run
		}
		return fs, nil
	}
	return p
}

// managerFor — a manager with fake factories: browsers from the
// browsers slice (by the number of calls), the pool — poolOver; counts
// the starts and remembers the pools.
func managerFor(t *testing.T, opts Options, browsers ...*Browser) (*Manager, *atomic.Int32, *[]*Pool) {
	t.Helper()
	m := NewManager(context.Background(), opts)
	var calls atomic.Int32
	var pools []*Pool
	var mu sync.Mutex
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		i := int(calls.Add(1)) - 1
		if i >= len(browsers) {
			return nil, &Error{Class: ClassBrowser, Err: errors.New("no browser is due")}
		}
		return browsers[i], nil
	}
	m.newPool = func(b *Browser) *Pool {
		p := poolOver(b, nil)
		mu.Lock()
		pools = append(pools, p)
		mu.Unlock()
		return p
	}
	return m, &calls, &pools
}

func managerOpts() Options {
	return Options{
		Contexts:          1,
		Timeout:           5 * time.Second,
		RestartEveryPages: 100,
	}.withDefaults()
}

func TestManagerRunStartsBrowserOnce(t *testing.T) {
	b, _ := liveBrowser(1)
	m, calls, _ := managerFor(t, managerOpts(), b)

	for range 2 {
		out, err := m.Run(context.Background(), RenderTask{URL: "https://a.example/"})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.FinalURL != "https://a.example/" {
			t.Fatalf("result %+v", out)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("browser starts %d, want 1 (one browser is reused)", got)
	}
	if got := m.Restarts(); got != 0 {
		t.Errorf("restarts %d, want 0", got)
	}
	m.Close()
	if m.Browser() != nil {
		t.Error("after Close there must be no browser")
	}
}

func TestManagerStart(t *testing.T) {
	// Start raises the first generation immediately (the production
	// build fails at startup instead of cycling delivery attempts); a
	// live generation — a no-op.
	b, _ := liveBrowser(1)
	m, calls, _ := managerFor(t, managerOpts(), b)
	if m.Browser() != nil {
		t.Fatal("before Start there is no generation")
	}
	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("starts %d, want 1", got)
	}
	if m.Browser() != b {
		t.Fatal("Start did not bring up a generation")
	}
	if err := m.Start(); err != nil {
		t.Fatalf("a repeated Start: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("starts %d, want 1 (a live generation is not touched)", got)
	}
	m.Close()
	if err := m.Start(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Start after Close: %v, want ErrClosed", err)
	}
}

func TestManagerAlive(t *testing.T) {
	// The consumption probe: a live generation — true without extra
	// starts; after the generation's death (a Chromium restart by the
	// host) the probe replaces it itself — a reconnect, the consumption
	// pause never hangs forever.
	b1, cancel1 := liveBrowser(1)
	b2, _ := liveBrowser(1)
	m, calls, _ := managerFor(t, managerOpts(), b1, b2)
	if !m.Alive() {
		t.Fatal("before the start the probe brings up the first generation itself (true)")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("starts %d, want 1 (the probe started the first generation)", got)
	}
	for range 2 {
		if !m.Alive() {
			t.Fatal("a live generation: Alive = true")
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("starts %d, want 1 (a live generation is not restarted)", got)
	}

	cancel1() // the host restarted Chromium: the connection is lost forever
	if !m.Alive() {
		t.Fatal("the probe must swap the dead generation (true)")
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("starts %d, want 2 (the dead generation swapped by the probe)", got)
	}
	if !m.Browser().Alive() {
		t.Error("the new generation is alive")
	}

	// The first generation's pool is closed by the background Close:
	// tasks do not go into it.
	if !m.Alive() {
		t.Error("after the swap the probe sees a live generation")
	}
	m.Close()
	if m.Alive() {
		t.Error("after Close the probe is false")
	}
}

func TestManagerRestartOnPagesThreshold(t *testing.T) {
	opts := managerOpts()
	opts.RestartEveryPages = 1 // every successful page is the threshold
	b1, _ := liveBrowser(1)
	b1.opts.RestartEveryPages = 1
	b2, _ := liveBrowser(1)
	b2.opts.RestartEveryPages = 1
	m, calls, pools := managerFor(t, opts, b1, b2, nil)

	if _, err := m.Run(context.Background(), RenderTask{URL: "u1"}); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if _, err := m.Run(context.Background(), RenderTask{URL: "u2"}); err != nil {
		t.Fatalf("Run 2: %v", err)
	}

	if got := calls.Load(); got != 2 {
		t.Fatalf("starts %d, want 2 (a generation swap by the page threshold)", got)
	}
	if got := m.Restarts(); got != 1 {
		t.Errorf("restarts %d, want 1", got)
	}
	// The new generation's page counter from zero: the threshold is not
	// reached again.
	if got := m.Browser().Pages(); got != 1 {
		t.Errorf("the new generation's pages %d, want 1", got)
	}
	// The old pool is closed: new tasks do not go into it.
	_, err := (*pools)[0].Run(context.Background(), RenderTask{URL: "u"})
	if !errors.Is(err, ErrClosed) {
		t.Errorf("the old pool's error %v, want ErrClosed", err)
	}
}

func TestManagerRestartOnMemoryThreshold(t *testing.T) {
	b1, _ := liveBrowser(1)
	b1.rootPID = os.Getpid()
	if b1.RSS() <= 0 {
		t.Skip("RSS unavailable (no /proc)")
	}
	b1.opts.MemoryThreshold = 1 // byte: any live memory — over
	b2, _ := liveBrowser(1)     // threshold 0: the control is off, no restart
	b3, _ := liveBrowser(1)
	m, calls, _ := managerFor(t, managerOpts(), b1, b2, b3)

	if _, err := m.Run(context.Background(), RenderTask{URL: "u1"}); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if _, err := m.Run(context.Background(), RenderTask{URL: "u2"}); err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("starts %d, want 2 (a generation swap by the memory threshold)", got)
	}
	if got := m.Restarts(); got != 1 {
		t.Errorf("restarts %d, want 1", got)
	}
}

func TestManagerRestartOnDeadBrowserBeforeTask(t *testing.T) {
	// The generation's browser died before the task (a connection/
	// process loss): acquire replaces the generation before the task
	// meets the dead browser. The generation is planted white-box — as
	// if the browser died right after the previous restart.
	m := NewManager(context.Background(), managerOpts())
	dead := deadBrowser(1)
	m.b, m.p = dead, poolOver(dead, nil)
	var calls atomic.Int32
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		calls.Add(1)
		b, _ := liveBrowser(1)
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool { return poolOver(b, nil) }

	if _, err := m.Run(context.Background(), RenderTask{URL: "u"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("starts %d, want 1 (the dead generation swapped)", got)
	}
	if got := m.Restarts(); got != 1 {
		t.Errorf("restarts %d, want 1", got)
	}
}

func TestManagerRetryAfterBrowserDeathInTask(t *testing.T) {
	opts := managerOpts()
	b1, cancel1 := liveBrowser(1)
	b2, _ := liveBrowser(1)
	m, calls, _ := managerFor(t, opts, b1, b2)

	// The first generation's pool: the session kills the browser and
	// returns ClassBrowser.
	inner, first := m.newPool, true
	m.newPool = func(b *Browser) *Pool {
		p := inner(b)
		if first {
			first = false
			p.newSession = func(context.Context) (renderSession, error) {
				return &fakeSession{run: func(context.Context, RenderTask) (RenderOutput, error) {
					cancel1()
					return RenderOutput{}, &Error{Class: ClassBrowser, Err: ErrBrowserDead}
				}}, nil
			}
		}
		return p
	}

	out, err := m.Run(context.Background(), RenderTask{URL: "https://a.example/"})
	if err != nil {
		t.Fatalf("the retry after the browser's death did not heal: %v", err)
	}
	if out.Title != "ok" {
		t.Fatalf("result %+v", out)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("starts %d, want 2", got)
	}
	if got := m.Restarts(); got != 1 {
		t.Errorf("restarts %d, want 1", got)
	}
}

func TestManagerBoundedRestarts(t *testing.T) {
	// The sessions fail with the browser class while the browser is
	// alive: the attempts are bounded, the last error goes upward
	// (re-delivery is the queue level).
	m := NewManager(context.Background(), managerOpts())
	var sessions atomic.Int32
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		b := testBrowser(1)
		b.ctx = context.Background()
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
	// The literal pin (the ±1 mutants of the constant): an attempt +
	// exactly 2 restarts. Comparing the attempts with
	// 1+maxRestartsPerTask would mask the mutation — the expectation
	// would change along with the constant.
	if maxRestartsPerTask != 2 {
		t.Fatalf("maxRestartsPerTask = %d, want 2", maxRestartsPerTask)
	}
	if got := sessions.Load(); got != 1+maxRestartsPerTask {
		t.Errorf("attempts %d, want %d (the attempt + restarts)", got, 1+maxRestartsPerTask)
	}
}

// TestRestartBudgetCoversProbeSeries — the T-226 invariant: a series of
// connection probe failures (probeFailThreshold task executions — each
// first failure with the browser class goes into a Manager.Run retry)
// plus a restart and a successful execution must fit into the budget of
// 1+maxRestartsPerTask — otherwise the recovery from the T-218 black
// hole does not fit into a single task and requires re-delivery. The
// pin of the constants' relation (the literals themselves are pinned by
// their own tests — only the relation here).
func TestRestartBudgetCoversProbeSeries(t *testing.T) {
	if maxRestartsPerTask < probeFailThreshold {
		t.Fatalf("maxRestartsPerTask=%d < probeFailThreshold=%d: a series of probe failures eats the retry budget — the T-218 recovery does not fit into one Manager.Run",
			maxRestartsPerTask, probeFailThreshold)
	}
}

func TestManagerStartFailure(t *testing.T) {
	m := NewManager(context.Background(), managerOpts())
	var fail atomic.Bool
	fail.Store(true)
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		if fail.Load() {
			return nil, &Error{Class: ClassBrowser, Err: errors.New("no binary")}
		}
		b, _ := liveBrowser(1)
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool { return poolOver(b, nil) }

	_, err := m.Run(context.Background(), RenderTask{URL: "u"})
	if ClassOf(err) != ClassBrowser {
		t.Fatalf("the start error: the class %v (%v), want browser", ClassOf(err), err)
	}
	// The start is not postponed: the next task tries again.
	fail.Store(false)
	if _, err := m.Run(context.Background(), RenderTask{URL: "u"}); err != nil {
		t.Fatalf("Run after the fix: %v", err)
	}
}

func TestManagerClosed(t *testing.T) {
	b, _ := liveBrowser(1)
	m, calls, _ := managerFor(t, managerOpts(), b)
	m.Close()

	_, err := m.Run(context.Background(), RenderTask{URL: "u"})
	if !errors.Is(err, ErrClosed) || ClassOf(err) != ClassTask {
		t.Fatalf("the error %v (%v), want an ErrClosed of the task class", err, ClassOf(err))
	}
	if got := calls.Load(); got != 0 {
		t.Errorf("starts after Close: %d", got)
	}
	m.Close() // a repeated call is harmless
	// And it does not wedge the manager: the mutex is released, tasks
	// still get ErrClosed immediately. Cutting Unlock out would leave
	// the mutex held (Run hangs) — checked with a timeout, not to the
	// bitter end.
	errCh := make(chan error, 1)
	go func() {
		_, err := m.Run(context.Background(), RenderTask{URL: "u2"})
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("the error after a repeated Close: %v, want ErrClosed", err)
		}
		if ClassOf(err) != ClassTask {
			t.Errorf("the class %v, want task", ClassOf(err))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run hung after a repeated Close: the mutex is not released")
	}
}

func TestManagerRunRetryOnClosedPool(t *testing.T) {
	// The race "the generation was swapped right after acquire": the
	// task gets ErrClosed from the already closing pool — the manager
	// swaps the generation and retries the task instead of accumulating
	// errors until the attempts run out. The pool is closed by the
	// factory right at creation — a deterministic imitation of the
	// closure between acquire and p.Run.
	m := NewManager(context.Background(), managerOpts())
	var poolCalls atomic.Int32
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		b, _ := liveBrowser(1)
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool {
		p := poolOver(b, nil)
		if poolCalls.Add(1) == 1 {
			p.Close()
		}
		return p
	}

	out, err := m.Run(context.Background(), RenderTask{URL: "https://a.example/"})
	if err != nil {
		t.Fatalf("Run after the generation is closed: %v", err)
	}
	if out.Title != "ok" {
		t.Fatalf("result %+v", out)
	}
	if got := poolCalls.Load(); got != 2 {
		t.Errorf("generations %d, want 2 (the closed one swapped)", got)
	}
	if got := m.Restarts(); got != 1 {
		t.Errorf("restarts %d, want 1", got)
	}
}

func TestManagerCloseWaitsInflight(t *testing.T) {
	opts := managerOpts()
	m := NewManager(context.Background(), opts)
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		b, _ := liveBrowser(1)
		return b, nil
	}
	started := make(chan struct{})
	release := make(chan struct{})
	m.newPool = func(b *Browser) *Pool {
		return poolOver(b, func(context.Context, RenderTask) (RenderOutput, error) {
			close(started)
			<-release
			return RenderOutput{}, nil
		})
	}

	runDone := make(chan error, 1)
	go func() {
		_, err := m.Run(context.Background(), RenderTask{URL: "u"})
		runDone <- err
	}()
	<-started

	closed := make(chan struct{})
	go func() {
		m.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close did not wait out the in-flight task")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	if err := <-runDone; err != nil {
		t.Fatalf("the in-flight task: %v", err)
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish after the tasks")
	}
}

func TestManagerCloseRejectsTasksWhileDraining(t *testing.T) {
	// R-07: while Close is finishing the generation's tasks (Pool.Close
	// waits on wg), new tasks get ErrClosed immediately instead of
	// waiting for the drain — the mutex is not held for the whole wait.
	m := NewManager(context.Background(), managerOpts())
	started := make(chan struct{})
	release := make(chan struct{})
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		b, _ := liveBrowser(1)
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool {
		return poolOver(b, func(context.Context, RenderTask) (RenderOutput, error) {
			close(started)
			<-release
			return RenderOutput{}, nil
		})
	}

	runDone := make(chan error, 1)
	go func() {
		_, err := m.Run(context.Background(), RenderTask{URL: "u"})
		runDone <- err
	}()
	<-started

	closed := make(chan struct{})
	go func() {
		m.Close()
		close(closed)
	}()
	// Close enters the drain (the critical section is short), the task
	// holds it.
	select {
	case <-closed:
		t.Fatal("Close must not finish before the generation's tasks end")
	case <-time.After(100 * time.Millisecond):
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := m.Run(context.Background(), RenderTask{URL: "u2"})
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("the error %v, want ErrClosed immediately, without waiting for the drain", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("a new task blocks on the Close drain (is the mutex held?)")
	}

	close(release)
	if err := <-runDone; err != nil {
		t.Fatalf("the in-flight task: %v", err)
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish after the tasks")
	}
}

func TestManagerParallelStress(t *testing.T) {
	// Every successful page reaches the threshold: the tasks keep
	// swapping generations, wait for slots, land in closing pools —
	// all must finish with a result, without deadlocks.
	opts := managerOpts()
	opts.RestartEveryPages = 1
	opts.Contexts = 2
	m := NewManager(context.Background(), opts)
	var calls atomic.Int32
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		calls.Add(1)
		b, _ := liveBrowser(2)
		b.opts.RestartEveryPages = 1
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool { return poolOver(b, nil) }

	const tasks = 20
	var wg sync.WaitGroup
	errs := make(chan error, tasks)
	for range tasks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := m.Run(context.Background(), RenderTask{URL: "u"}); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("Run: %v", err)
	}
	if got := m.Restarts(); got == 0 {
		t.Error("no restarts at the threshold of 1 page")
	}
	// Every start after the first is a restart: the generation swap
	// invariant.
	if got, want := calls.Load(), int32(m.Restarts())+1; got != want {
		t.Errorf("starts %d, want restarts+1 = %d", got, want)
	}
	m.Close()
}

// wiredBrowser — a browser with the context cancellation wired up: the
// stop is visible through Alive (the TestPoolCloseStopsBrowser style),
// unlike liveBrowser whose Close does not cancel the context.
func wiredBrowser(contexts int) (*Browser, context.CancelFunc) {
	b := testBrowser(contexts)
	ctx, cancel := context.WithCancel(context.Background())
	b.ctx, b.cancel = ctx, cancel
	return b, cancel
}

func TestManagerDefaultFactories(t *testing.T) {
	// The NewManager factories: newBrowser = StartBrowser,
	// newPool = NewPool. Cutting the first out — a nil call on the
	// first start (a panic), the second — a panic after a successful
	// browser start; both are observable without substitution.
	t.Run("newBrowser — StartBrowser", func(t *testing.T) {
		// A hermetic start failure: a closed port, the connection is
		// refused instantly (TestStartBrowserRemoteUnreachable).
		opts := managerOpts()
		opts.RemoteURL = "ws://127.0.0.1:1"
		opts.DialTimeout = 500 * time.Millisecond
		m := NewManager(context.Background(), opts)
		err := m.Start()
		var e *Error
		if !errors.As(err, &e) || e.Class != ClassBrowser {
			t.Fatalf("want a browser class error from StartBrowser, got: %v", err)
		}
		m.Close()
	})
	t.Run("newPool — NewPool", func(t *testing.T) {
		// The browser is a fake, the pool is real: cutting the
		// assignment out leaves a nil factory — a panic on the first
		// start. Running tasks through the real pool is not needed:
		// sessions require a live chromedp context, the pool creation
		// is enough.
		m := NewManager(context.Background(), managerOpts())
		b, _ := liveBrowser(1)
		m.newBrowser = func(context.Context, Options) (*Browser, error) {
			return b, nil
		}
		if err := m.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		if m.Browser() != b {
			t.Fatal("Start did not bring up a generation with the real NewPool")
		}
		m.Close()
	})
}

func TestManagerRunReturnsBrowserErrorAfterClose(t *testing.T) {
	// The manager closed mid-task: the browser error goes upward as
	// is, not replaced by the ErrClosed of a repeated acquire (the
	// m.closed.Load() branch in the Run retry decision).
	m := NewManager(context.Background(), managerOpts())
	started := make(chan struct{})
	release := make(chan struct{})
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		b, _ := liveBrowser(1)
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool {
		return poolOver(b, func(context.Context, RenderTask) (RenderOutput, error) {
			close(started)
			<-release
			return RenderOutput{}, &Error{Class: ClassBrowser, Err: ErrBrowserDead}
		})
	}

	runDone := make(chan error, 1)
	go func() {
		_, err := m.Run(context.Background(), RenderTask{URL: "u"})
		runDone <- err
	}()
	<-started

	closed := make(chan struct{})
	go func() {
		m.Close()
		close(closed)
	}()
	// Wait for the Close critical section (closed is set, the
	// generation detached) before releasing the task — the
	// determinism of the checked branch.
	deadline := time.Now().Add(5 * time.Second)
	for m.Browser() != nil {
		if time.Now().After(deadline) {
			t.Fatal("Close did not enter the critical section")
		}
		time.Sleep(time.Millisecond)
	}
	close(release)

	err := <-runDone
	if ClassOf(err) != ClassBrowser {
		t.Fatalf("the class %v (%v), want browser: the task error is not replaced by ErrClosed", ClassOf(err), err)
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not finish after the task")
	}
}

func TestManagerRunNoRetryOnTaskError(t *testing.T) {
	// A page error is a regular stage result: the manager neither
	// retries the task nor restarts the browser (re-delivery is the
	// queue level, T-066).
	b, _ := liveBrowser(1)
	m := NewManager(context.Background(), managerOpts())
	boom := &Error{Class: ClassTask, Err: errors.New("the page did not load")}
	var runs atomic.Int32
	m.newBrowser = func(context.Context, Options) (*Browser, error) { return b, nil }
	m.newPool = func(b *Browser) *Pool {
		return poolOver(b, func(context.Context, RenderTask) (RenderOutput, error) {
			runs.Add(1)
			return RenderOutput{}, boom
		})
	}

	_, err := m.Run(context.Background(), RenderTask{URL: "u"})
	if !errors.Is(err, boom) {
		t.Fatalf("the error %v, want the original page error", err)
	}
	if got := runs.Load(); got != 1 {
		t.Errorf("task runs %d, want 1 (a page error — no manager retries)", got)
	}
	if got := m.Restarts(); got != 0 {
		t.Errorf("restarts %d, want 0", got)
	}
}

func TestManagerAliveAfterCloseNoRestart(t *testing.T) {
	// After Close the probe is finally dead: false without an attempt
	// to raise a new generation (the closed check precedes the swap
	// decision; the factory is not exhausted here — a restart success
	// would be visible).
	m := NewManager(context.Background(), managerOpts())
	var calls atomic.Int32
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		calls.Add(1)
		b, _ := liveBrowser(1)
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool { return poolOver(b, nil) }

	if !m.Alive() {
		t.Fatal("the probe brought up the first generation (true)")
	}
	m.Close()
	if m.Alive() {
		t.Error("after Close the probe is false")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("starts %d, want 1 (a closed manager does not bring up generations)", got)
	}
}

func TestManagerAcquireSwapsMissingPool(t *testing.T) {
	// The nil-pool guard: the generation has a browser but no pool —
	// acquire swaps the generation ("the pool is closed") instead of
	// crashing. The orphaned browser is not killed (old == nil —
	// nothing to close): the state is unreachable by regular paths (b
	// and p are set together under m.mu); the guard protects against a
	// panic, not a leak.
	m := NewManager(context.Background(), managerOpts())
	b1, _ := liveBrowser(1)
	m.b = b1 // no pool planted — a generation desync
	var calls atomic.Int32
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		calls.Add(1)
		b, _ := liveBrowser(1)
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool { return poolOver(b, nil) }

	if _, err := m.Run(context.Background(), RenderTask{URL: "u"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("starts %d, want 1 (a generation without a pool swapped)", got)
	}
}

func TestManagerMemoryRestartBoundaries(t *testing.T) {
	// The memory control boundaries: the threshold exactly 0 — the
	// control is off; a threshold above the live RSS — not reached.
	// Both without a restart at a positive process RSS (the second
	// case also pins the annotation-suppressed trade-off of §4 —
	// cutting the right half of the condition in swapReasonLocked:
	// `rss > 0 && true` would restart the browser after every task).
	probe, _ := liveBrowser(1)
	probe.rootPID = os.Getpid()
	if probe.RSS() <= 0 {
		t.Skip("RSS unavailable (no /proc)")
	}
	above := probe.RSS() + 1<<30 // clearly above the live tree's RSS
	newCase := func(t *testing.T, threshold int64) {
		t.Helper()
		b1, _ := liveBrowser(1)
		b1.rootPID = os.Getpid()
		b1.opts.MemoryThreshold = threshold
		b2, _ := liveBrowser(1)
		m, calls, _ := managerFor(t, managerOpts(), b1, b2)

		for range 2 {
			if _, err := m.Run(context.Background(), RenderTask{URL: "u"}); err != nil {
				t.Fatalf("Run: %v", err)
			}
		}
		if got := calls.Load(); got != 1 {
			t.Errorf("starts %d, want 1 (the threshold %d does not trigger a restart)", got, threshold)
		}
		if got := m.Restarts(); got != 0 {
			t.Errorf("restarts %d, want 0", got)
		}
	}
	t.Run("a zero threshold — the control is off", func(t *testing.T) { newCase(t, 0) })
	t.Run("the threshold is above the live RSS", func(t *testing.T) { newCase(t, above) })
}

func TestManagerRestartFailureDetachesGeneration(t *testing.T) {
	// A failed restart detaches the old generation immediately:
	// Browser() is nil, with no reference to the dead browser; the
	// start is not postponed — the next task tries again.
	b1, cancel1 := liveBrowser(1)
	m, calls, _ := managerFor(t, managerOpts(), b1) // the second start fails

	if _, err := m.Run(context.Background(), RenderTask{URL: "u1"}); err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if m.Browser() != b1 {
		t.Fatal("the live generation is not pinned")
	}
	cancel1() // the generation's browser died
	_, err := m.Run(context.Background(), RenderTask{URL: "u2"})
	if ClassOf(err) != ClassBrowser {
		t.Fatalf("the restart error: the class %v (%v), want browser", ClassOf(err), err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("starts %d, want 2", got)
	}
	if m.Browser() != nil {
		t.Error("after a failed restart the generation is unpinned (Browser nil)")
	}
}

func TestManagerRestartClosesOldGeneration(t *testing.T) {
	// The old generation goes to the background closure on a swap —
	// both on a successful and on a failed start of the new one: the
	// generation's browser is killed (the context cancellation), it
	// does not stay hanging.
	run := func(t *testing.T, restartFails bool) {
		opts := managerOpts() // the page threshold — in the browsers'
		// opts below: NeedsRestart reads b.opts specifically, and the
		// factory of substituted browsers does not use the manager
		// options.
		b1, _ := wiredBrowser(1)
		b1.opts.RestartEveryPages = 1
		browsers := []*Browser{b1}
		if !restartFails {
			b2, _ := wiredBrowser(1)
			b2.opts.RestartEveryPages = 1
			browsers = append(browsers, b2)
		}
		m, calls, _ := managerFor(t, opts, browsers...)

		if _, err := m.Run(context.Background(), RenderTask{URL: "u1"}); err != nil {
			t.Fatalf("Run 1: %v", err)
		}
		_, err := m.Run(context.Background(), RenderTask{URL: "u2"}) // the page threshold
		if restartFails && ClassOf(err) != ClassBrowser {
			t.Fatalf("Run 2: %v, want a browser class error", err)
		}
		if !restartFails && err != nil {
			t.Fatalf("Run 2: %v", err)
		}
		if got := calls.Load(); got != 2 {
			t.Fatalf("starts %d, want 2", got)
		}
		// The background closure of the old generation — wait for the
		// cancellation.
		deadline := time.Now().Add(2 * time.Second)
		for b1.Alive() && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if b1.Alive() {
			t.Error("the old generation's browser was not stopped on the generation swap")
		}
	}
	t.Run("a successful swap", func(t *testing.T) { run(t, false) })
	t.Run("a failed start of the new one", func(t *testing.T) { run(t, true) })
}

package render

// Unit tests of the browser generation FSM (T-263, genstate.go): the
// genStateLocked state table on cdpfake browsers (the transport fake of
// cdpfake_test.go: a real chromedp bootstrap, liveness/death visible to
// Browser.Alive), the swapReasonLocked decision table over all states and
// thresholds, a walkthrough of the lifecycle transitions with side effects
// (a restart, drain, Close terminality) and the isolation of G1 "starting"
// by the manager's mutex.

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
)

// genCDPBrowser — a generation on top of fakeCDP: a full chromedp bootstrap on
// an in-memory transport, Alive sees the live browser context. A generation's
// death in the scenarios — b.Close() (a context cancellation: the same
// Browser.Alive branch as the LostConnection of a real connection).
//
// Cleanup — t.Cleanup(b.Close), not the raw chromedp cancel functions:
// the cancelWait wrapper (NewContext) for a NON-attacher allocator
// (fakeCDPAllocator) consumes the c.allocated semaphore token exactly once
// and is closed by no one — a second cancel call hangs forever;
// Browser.Close is idempotent (the closed flag) and kills both contexts
// (the browser's and the allocator's) on the first call.
func genCDPBrowser(t *testing.T, opts Options) *Browser {
	t.Helper()
	tr := newFakeCDP()
	allocCtx, cancelAlloc := chromedp.NewAllocatorContext(context.Background(),
		&fakeCDPAllocator{tr: tr})
	bctx, cancelBrowser := chromedp.NewContext(allocCtx)
	b := &Browser{opts: opts, ctx: bctx, cancel: cancelBrowser, cancelAlloc: cancelAlloc,
		probeTimeout: testProbeTimeout}
	t.Cleanup(b.Close)
	if err := chromedp.Do(bctx); err != nil {
		t.Fatalf("bootstrapping the browser over fakeCDP: %v", err)
	}
	return b
}

// genStateOf — reading the slot's state under the manager's mutex (as
// acquire/Alive/Start read it).
func genStateOf(m *Manager) genState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.genStateLocked()
}

// genStartedWith — a manager with a raised cdpfake generation (the browser
// is returned to the scenarios for death/thresholds).
func genStartedWith(t *testing.T) (*Manager, *Browser) {
	t.Helper()
	b := genCDPBrowser(t, managerOpts())
	m, _, _ := managerFor(t, managerOpts(), b)
	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return m, b
}

func TestGenStateNames(t *testing.T) {
	// The six FSM states (T-263) with names for diagnostics; G1
	// "starting" is in the list although the predicate does not return it
	// (it exists only under m.mu, invariant I2).
	names := map[genState]string{
		genAbsent:   "missing",
		genStarting: "starting",
		genActive:   "active",
		genDraining: "draining",
		genDead:     "dead",
		genClosed:   "closed",
	}
	for g, want := range names {
		if got := g.String(); got != want {
			t.Errorf("String(%d) = %q, want %q", int(g), got, want)
		}
	}
	for i := 0; i <= int(genClosed); i++ {
		if _, ok := names[genState(i)]; !ok {
			t.Errorf("the state %d is outside the FSM list (extra/undocumented)", i)
		}
	}
}

func TestGenStateTable(t *testing.T) {
	// The state table (TZ-fsm §4): all slot states observable from outside
	// m.mu — on cdpfake browsers; G1 — a separate test.
	cases := []struct {
		name  string
		setup func(t *testing.T) *Manager
		want  genState
	}{
		{"G0 missing — before the first start", func(t *testing.T) *Manager {
			m, _, _ := managerFor(t, managerOpts())
			return m
		}, genAbsent},
		{"G0 missing — after a failed start (T4)", func(t *testing.T) *Manager {
			m, _, _ := managerFor(t, managerOpts()) // the factory has no browsers — the start fails
			if err := m.Start(); ClassOf(err) != ClassBrowser {
				t.Fatalf("Start: %v, want a browser class error", err)
			}
			return m
		}, genAbsent},
		{"G2 active — a live generation (T2)", func(t *testing.T) *Manager {
			m, _ := genStartedWith(t)
			return m
		}, genActive},
		{"G4 dead — the generation's browser is down (T6)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			b.Close()
			return m
		}, genDead},
		{"G3 draining — the generation's pool is retired (T8)", func(t *testing.T) *Manager {
			m, _ := genStartedWith(t)
			m.mu.Lock()
			m.p.retire()
			m.mu.Unlock()
			return m
		}, genDraining},
		{"G3 draining — a slot without a pool (a desync guard)", func(t *testing.T) *Manager {
			b := genCDPBrowser(t, managerOpts())
			m := NewManager(context.Background(), managerOpts())
			m.mu.Lock()
			m.b = b // no pool planted — unreachable normally (b and p are set together)
			m.mu.Unlock()
			return m
		}, genDraining},
		{"G4 dead takes precedence over G3 draining (I1)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			m.mu.Lock()
			m.p.retire()
			m.mu.Unlock()
			b.Close()
			return m
		}, genDead},
		{"G5 closed — Close with a live generation (T9)", func(t *testing.T) *Manager {
			m, _ := genStartedWith(t)
			m.Close()
			return m
		}, genClosed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := tc.setup(t)
			if got := genStateOf(m); got != tc.want {
				t.Fatalf("the generation state %s, want %s", got, tc.want)
			}
		})
	}
}

func TestSwapReasonTable(t *testing.T) {
	// The swapReasonLocked decision table: the replacement reasons by the
	// slot states + the live generation thresholds (genstate.go,
	// "Replacement reasons").
	cases := []struct {
		name       string
		setup      func(t *testing.T) *Manager
		wantSubstr string
		wantSwap   bool
	}{
		{"the first start (G0)", func(t *testing.T) *Manager {
			m, _, _ := managerFor(t, managerOpts())
			return m
		}, "the first start", true},
		{"browser unavailable (G4)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			b.Close()
			return m
		}, "browser unavailable", true},
		{"a dead browser in a closed pool — the cause G4 (I1)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			m.mu.Lock()
			m.p.retire()
			m.mu.Unlock()
			b.Close()
			return m
		}, "browser unavailable", true},
		{"pool closed (G3 in the slot, T8)", func(t *testing.T) *Manager {
			m, _ := genStartedWith(t)
			m.mu.Lock()
			m.p.retire()
			m.mu.Unlock()
			return m
		}, "pool closed", true},
		{"no swap (G2, no thresholds reached)", func(t *testing.T) *Manager {
			m, _ := genStartedWith(t)
			return m
		}, "", false},
		{"the page threshold (G2, T5)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			m.mu.Lock()
			b.opts.RestartEveryPages = 1
			b.pages.Add(1) // one successful page — the threshold
			m.mu.Unlock()
			return m
		}, "the page threshold", true},
		{"the page threshold takes precedence over the memory threshold", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			m.mu.Lock()
			b.rootPID = os.Getpid()
			m.mu.Unlock()
			if b.RSS() <= 0 {
				t.Skip("RSS unavailable (no /proc)")
			}
			m.mu.Lock()
			b.opts.RestartEveryPages = 1
			b.pages.Add(1)
			b.opts.MemoryThreshold = 1 // both thresholds reached — the first one decides
			m.mu.Unlock()
			return m
		}, "the page threshold", true},
		{"the memory threshold (G2, T5)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			m.mu.Lock()
			b.rootPID = os.Getpid()
			m.mu.Unlock()
			if b.RSS() <= 0 {
				t.Skip("RSS unavailable (no /proc)")
			}
			m.mu.Lock()
			b.opts.MemoryThreshold = 1 // bytes: any live memory is an excess
			m.mu.Unlock()
			return m
		}, "the memory threshold", true},
		{"a dead browser with a reached threshold — the state beats the threshold (I1)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			m.mu.Lock()
			b.opts.RestartEveryPages = 1
			b.pages.Add(1)
			m.mu.Unlock()
			b.Close()
			return m
		}, "browser unavailable", true},
		{"closed — no decisions (I3)", func(t *testing.T) *Manager {
			m, _ := genStartedWith(t)
			m.Close()
			return m
		}, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := tc.setup(t)
			m.mu.Lock()
			reason, swap := m.swapReasonLocked()
			m.mu.Unlock()
			if swap != tc.wantSwap {
				t.Fatalf("the swap decision %v (%q), want %v", swap, reason, tc.wantSwap)
			}
			if tc.wantSubstr != "" && !strings.Contains(reason, tc.wantSubstr) {
				t.Fatalf("the reason %q does not contain %q", reason, tc.wantSubstr)
			}
		})
	}
}

func TestGenStateLifecycle(t *testing.T) {
	// A walkthrough of the transitions: T1→T2 (a start), T6 (death) →
	// T1→T3 (a replacement by the probe, T7 drains the old one),
	// T8 (a closed pool in the slot) → T3, T9 (Close).
	// Side effects: Restarts, the retire of the old pool (the tasks'
	// ErrClosed), the ban on new generations from G5.
	b1 := genCDPBrowser(t, managerOpts())
	b2 := genCDPBrowser(t, managerOpts())
	b3 := genCDPBrowser(t, managerOpts())
	m, calls, pools := managerFor(t, managerOpts(), b1, b2, b3)

	// T1→T2: the first generation — active, no restarts.
	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := genStateOf(m); got != genActive {
		t.Fatalf("after the start the state %s, want active", got)
	}
	if got, want := calls.Load(), int32(1); got != want {
		t.Fatalf("starts %d, want %d", got, want)
	}
	if got := m.Restarts(); got != 0 {
		t.Errorf("restarts %d, want 0 (a first start is not a restart)", got)
	}

	// T6: the generation's browser dies — dead.
	b1.Close()
	if got := genStateOf(m); got != genDead {
		t.Fatalf("after the browser's death the state %s, want dead", got)
	}

	// T1→T3: the probe replaces the dead generation; the old pool is drained (T7).
	if !m.Alive() {
		t.Fatal("the probe must swap the dead generation")
	}
	if got := genStateOf(m); got != genActive {
		t.Fatalf("after the swap the state %s, want active", got)
	}
	if got, want := calls.Load(), int32(2); got != want {
		t.Fatalf("starts %d, want %d (swapping the dead generation)", got, want)
	}
	if got := m.Restarts(); got != 1 {
		t.Errorf("restarts %d, want 1", got)
	}
	if !(*pools)[0].closed.Load() {
		t.Error("the dead generation's pool is not retired")
	}
	if _, err := (*pools)[0].Run(context.Background(), RenderTask{URL: "u"}); !errors.Is(err, ErrClosed) {
		t.Errorf("a task into the draining pool: %v, want ErrClosed", err)
	}

	// T8: a closed pool in the slot with a live browser — the "pool closed" replacement.
	m.mu.Lock()
	m.p.retire()
	m.mu.Unlock()
	if got := genStateOf(m); got != genDraining {
		t.Fatalf("after the pool retire the state %s, want draining", got)
	}
	if _, err := m.Run(context.Background(), RenderTask{URL: "u"}); err != nil {
		t.Fatalf("Run after the slot's pool is closed: %v", err)
	}
	if got, want := calls.Load(), int32(3); got != want {
		t.Fatalf("starts %d, want %d (swapping a generation with a closed pool)", got, want)
	}
	if got := m.Restarts(); got != 2 {
		t.Errorf("restarts %d, want 2", got)
	}

	// T9: Close — terminal: tasks ErrClosed, the probe false, no starts.
	m.Close()
	if got := genStateOf(m); got != genClosed {
		t.Fatalf("after Close the state %s, want closed", got)
	}
	if _, err := m.Run(context.Background(), RenderTask{URL: "u"}); !errors.Is(err, ErrClosed) || ClassOf(err) != ClassTask {
		t.Fatalf("Run after Close: %v (%v), want an ErrClosed of the task class", err, ClassOf(err))
	}
	if m.Alive() {
		t.Error("the probe after Close: true, want false")
	}
	if got, want := calls.Load(), int32(3); got != want {
		t.Errorf("starts after Close %d, want %d (there are no G5 generations)", got, want)
	}
}

func TestGenStateStartingSerialized(t *testing.T) {
	// I2/G1 "starting": a generation start holds m.mu for the whole
	// StartBrowser — a concurrent probe waits for the outcome, does not
	// observe an empty slot and does not raise a second browser.
	b := genCDPBrowser(t, managerOpts())
	entered := make(chan struct{})
	release := make(chan struct{})
	m := NewManager(context.Background(), managerOpts())
	var calls atomic.Int32
	m.newBrowser = func(context.Context, Options) (*Browser, error) {
		if calls.Add(1) > 1 {
			t.Error("a double generation start — G1 is not mutex-isolated")
			return b, nil
		}
		close(entered)
		<-release
		return b, nil
	}
	m.newPool = func(b *Browser) *Pool { return poolOver(b, nil) }

	runDone := make(chan error, 1)
	go func() {
		_, err := m.Run(context.Background(), RenderTask{URL: "u"})
		runDone <- err
	}()
	<-entered // G1 "starting": m.mu is held by restartLocked

	probeDone := make(chan bool, 1)
	go func() { probeDone <- m.Alive() }()
	select {
	case alive := <-probeDone:
		t.Fatalf("the probe returned (%v) during a start: starting is not mutex-isolated", alive)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-runDone; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !<-probeDone {
		t.Fatal("after the start the probe sees a live generation")
	}
	if got := genStateOf(m); got != genActive {
		t.Fatalf("after the start the state %s, want active", got)
	}
	if got, want := calls.Load(), int32(1); got != want {
		t.Errorf("starts %d, want %d (the probe waited out the start)", got, want)
	}
}

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
		t.Fatalf("bootstrap браузера поверх fakeCDP: %v", err)
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
		genAbsent:   "отсутствует",
		genStarting: "стартующий",
		genActive:   "активный",
		genDraining: "draining",
		genDead:     "мёртв",
		genClosed:   "закрыт",
	}
	for g, want := range names {
		if got := g.String(); got != want {
			t.Errorf("String(%d) = %q, хочу %q", int(g), got, want)
		}
	}
	for i := 0; i <= int(genClosed); i++ {
		if _, ok := names[genState(i)]; !ok {
			t.Errorf("состояние %d вне перечня автомата (лишнее/незадокументированное)", i)
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
		{"Г0 отсутствует — до первого старта", func(t *testing.T) *Manager {
			m, _, _ := managerFor(t, managerOpts())
			return m
		}, genAbsent},
		{"Г0 отсутствует — после неудачного старта (П4)", func(t *testing.T) *Manager {
			m, _, _ := managerFor(t, managerOpts()) // the factory has no browsers — the start fails
			if err := m.Start(); ClassOf(err) != ClassBrowser {
				t.Fatalf("Start: %v, хочу ошибку класса browser", err)
			}
			return m
		}, genAbsent},
		{"Г2 активный — живое поколение (П2)", func(t *testing.T) *Manager {
			m, _ := genStartedWith(t)
			return m
		}, genActive},
		{"Г4 мёртв — браузер поколения погашен (П6)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			b.Close()
			return m
		}, genDead},
		{"Г3 draining — пул поколения retired (П8)", func(t *testing.T) *Manager {
			m, _ := genStartedWith(t)
			m.mu.Lock()
			m.p.retire()
			m.mu.Unlock()
			return m
		}, genDraining},
		{"Г3 draining — слот без пула (страховка рассинхрона)", func(t *testing.T) *Manager {
			b := genCDPBrowser(t, managerOpts())
			m := NewManager(context.Background(), managerOpts())
			m.mu.Lock()
			m.b = b // no pool planted — unreachable normally (b and p are set together)
			m.mu.Unlock()
			return m
		}, genDraining},
		{"Г4 мёртв приоритетнее Г3 draining (И1)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			m.mu.Lock()
			m.p.retire()
			m.mu.Unlock()
			b.Close()
			return m
		}, genDead},
		{"Г5 закрыт — Close при живом поколении (П9)", func(t *testing.T) *Manager {
			m, _ := genStartedWith(t)
			m.Close()
			return m
		}, genClosed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := tc.setup(t)
			if got := genStateOf(m); got != tc.want {
				t.Fatalf("состояние поколения %s, хочу %s", got, tc.want)
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
		{"первый старт (Г0)", func(t *testing.T) *Manager {
			m, _, _ := managerFor(t, managerOpts())
			return m
		}, "первый старт", true},
		{"браузер недоступен (Г4)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			b.Close()
			return m
		}, "браузер недоступен", true},
		{"мёртвый браузер в закрытом пуле — причина Г4 (И1)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			m.mu.Lock()
			m.p.retire()
			m.mu.Unlock()
			b.Close()
			return m
		}, "браузер недоступен", true},
		{"пул закрыт (Г3 в слоте, П8)", func(t *testing.T) *Manager {
			m, _ := genStartedWith(t)
			m.mu.Lock()
			m.p.retire()
			m.mu.Unlock()
			return m
		}, "пул закрыт", true},
		{"без замены (Г2, пороги не достигнуты)", func(t *testing.T) *Manager {
			m, _ := genStartedWith(t)
			return m
		}, "", false},
		{"порог страниц (Г2, П5)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			m.mu.Lock()
			b.opts.RestartEveryPages = 1
			b.pages.Add(1) // one successful page — the threshold
			m.mu.Unlock()
			return m
		}, "порог страниц", true},
		{"порог страниц приоритетнее порога памяти", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			m.mu.Lock()
			b.rootPID = os.Getpid()
			m.mu.Unlock()
			if b.RSS() <= 0 {
				t.Skip("RSS недоступен (нет /proc)")
			}
			m.mu.Lock()
			b.opts.RestartEveryPages = 1
			b.pages.Add(1)
			b.opts.MemoryThreshold = 1 // both thresholds reached — the first one decides
			m.mu.Unlock()
			return m
		}, "порог страниц", true},
		{"порог памяти (Г2, П5)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			m.mu.Lock()
			b.rootPID = os.Getpid()
			m.mu.Unlock()
			if b.RSS() <= 0 {
				t.Skip("RSS недоступен (нет /proc)")
			}
			m.mu.Lock()
			b.opts.MemoryThreshold = 1 // bytes: any live memory is an excess
			m.mu.Unlock()
			return m
		}, "порог памяти", true},
		{"мёртвый браузер с достигнутым порогом — состояние важнее порога (И1)", func(t *testing.T) *Manager {
			m, b := genStartedWith(t)
			m.mu.Lock()
			b.opts.RestartEveryPages = 1
			b.pages.Add(1)
			m.mu.Unlock()
			b.Close()
			return m
		}, "браузер недоступен", true},
		{"закрыт — решений нет (И3)", func(t *testing.T) *Manager {
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
				t.Fatalf("решение о смене %v (%q), хочу %v", swap, reason, tc.wantSwap)
			}
			if tc.wantSubstr != "" && !strings.Contains(reason, tc.wantSubstr) {
				t.Fatalf("причина %q не содержит %q", reason, tc.wantSubstr)
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
		t.Fatalf("после старта состояние %s, хочу активный", got)
	}
	if got, want := calls.Load(), int32(1); got != want {
		t.Fatalf("стартов %d, хочу %d", got, want)
	}
	if got := m.Restarts(); got != 0 {
		t.Errorf("перезапусков %d, хочу 0 (первый старт — не перезапуск)", got)
	}

	// T6: the generation's browser dies — dead.
	b1.Close()
	if got := genStateOf(m); got != genDead {
		t.Fatalf("после смерти браузера состояние %s, хочу мёртв", got)
	}

	// T1→T3: the probe replaces the dead generation; the old pool is drained (T7).
	if !m.Alive() {
		t.Fatal("зонд обязан заменить мёртвое поколение")
	}
	if got := genStateOf(m); got != genActive {
		t.Fatalf("после замены состояние %s, хочу активный", got)
	}
	if got, want := calls.Load(), int32(2); got != want {
		t.Fatalf("стартов %d, хочу %d (замена мёртвого поколения)", got, want)
	}
	if got := m.Restarts(); got != 1 {
		t.Errorf("перезапусков %d, хочу 1", got)
	}
	if !(*pools)[0].closed.Load() {
		t.Error("пул мёртвого поколения не выведен из работы (retire)")
	}
	if _, err := (*pools)[0].Run(context.Background(), RenderTask{URL: "u"}); !errors.Is(err, ErrClosed) {
		t.Errorf("задача в дренируемый пул: %v, хочу ErrClosed", err)
	}

	// T8: a closed pool in the slot with a live browser — the "pool closed" replacement.
	m.mu.Lock()
	m.p.retire()
	m.mu.Unlock()
	if got := genStateOf(m); got != genDraining {
		t.Fatalf("после retire пула состояние %s, хочу draining", got)
	}
	if _, err := m.Run(context.Background(), RenderTask{URL: "u"}); err != nil {
		t.Fatalf("Run после закрытия пула слота: %v", err)
	}
	if got, want := calls.Load(), int32(3); got != want {
		t.Fatalf("стартов %d, хочу %d (замена поколения с закрытым пулом)", got, want)
	}
	if got := m.Restarts(); got != 2 {
		t.Errorf("перезапусков %d, хочу 2", got)
	}

	// T9: Close — terminal: tasks ErrClosed, the probe false, no starts.
	m.Close()
	if got := genStateOf(m); got != genClosed {
		t.Fatalf("после Close состояние %s, хочу закрыт", got)
	}
	if _, err := m.Run(context.Background(), RenderTask{URL: "u"}); !errors.Is(err, ErrClosed) || ClassOf(err) != ClassTask {
		t.Fatalf("Run после Close: %v (%v), хочу ErrClosed класса task", err, ClassOf(err))
	}
	if m.Alive() {
		t.Error("зонд после Close: true, хочу false")
	}
	if got, want := calls.Load(), int32(3); got != want {
		t.Errorf("стартов после Close %d, хочу %d (из Г5 поколений нет)", got, want)
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
			t.Error("двойной старт поколения — Г1 не изолирован мьютексом")
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
		t.Fatalf("зонд вернулся (%v) во время старта: стартующий не изолирован мьютексом", alive)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-runDone; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !<-probeDone {
		t.Fatal("после старта зонд видит живое поколение")
	}
	if got := genStateOf(m); got != genActive {
		t.Fatalf("после старта состояние %s, хочу активный", got)
	}
	if got, want := calls.Load(), int32(1); got != want {
		t.Errorf("стартов %d, хочу %d (зонд ждал исхода старта)", got, want)
	}
}

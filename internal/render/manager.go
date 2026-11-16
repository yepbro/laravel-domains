package render

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// maxRestartsPerTask — the browser restart limit per task: the Chromium
// crash loop must not multiply executions of a single page; once exhausted,
// the error goes upward (re-delivery is the queue level, T-066/T-067).
// Since T-226 a ClassBrowser retry need not be accompanied by a restart
// (the first connection probe failure does not kill the generation): the
// budget of 1+maxRestartsPerTask executions must cover a series of probe
// failures plus a restart — otherwise recovery from the T-218 black hole
// does not fit into a single Manager.Run (the invariant
// maxRestartsPerTask >= probeFailThreshold pins
// TestRestartBudgetCoversProbeSeries).
const maxRestartsPerTask = 2

// engineRetryBase — the first wait step between generation start attempts
// while the engine is unreachable (T-283); the production value is a
// second, the unit tests substitute it (the engine_wait_test.go seam,
// like Browser.probeTimeout). A variable, not a constant, only for that.
var engineRetryBase = time.Second

// engineRetryCap — the ceiling of the wait step growth (T-283): an engine
// container restart (docker Restart=on-failure, a pull, a healthcheck
// warm-up) is tens of seconds — probing more often than 15 s changes
// nothing, the dial itself is bounded by DialTimeout.
const engineRetryCap = 15 * time.Second

// engineRetryStep — the next wait step between the start retries (T-283):
// doubling from engineRetryBase up to engineRetryCap; 0 — the ladder's
// start (the base). Pure, so the ladder is pinned by a unit test
// (TestEngineRetryStepGrowth).
func engineRetryStep(prev time.Duration) time.Duration {
	if prev <= 0 {
		return engineRetryBase
	}
	return min(prev*2, engineRetryCap)
}

// Manager — the Chromium worker lifecycle (T-065, spec 5.6: "restart the
// browser every N pages and when the memory threshold is exceeded"): start,
// restarts and graceful stop. It owns the current "browser + pool"
// generation; Run executes tasks — concurrently, like Pool.Run (drives the
// queue cycle, T-067). The generation states and transitions are the T-263
// FSM (genstate.go).
//
// A generation swap does not wait for the old pool's tasks: the old pool
// goes to a graceful stop in a background goroutine (it finishes its pages
// — each bounded by Options.Timeout, spec 3.4 — and closes its browser),
// and the new browser starts immediately: the restart downtime is not
// added to someone else's tasks, otherwise a memory-triggered restart
// would wait for the "heaviest" pages that consumed the budget. A task
// that did not start in the closing pool (ErrClosed) is retried on the new
// generation.
type Manager struct {
	opts    Options
	lifeCtx context.Context // lifecycle context of the browsers (StartBrowser)

	// newBrowser/newPool — factories; replaced by fakes in unit tests.
	newBrowser func(ctx context.Context, opts Options) (*Browser, error)
	newPool    func(b *Browser) *Pool

	restarts atomic.Int64
	closed   atomic.Bool

	mu sync.Mutex
	b  *Browser
	p  *Pool

	// restartPending/restartReason — the latch of a replacement in
	// flight (T-283): a live generation was detached (death, thresholds,
	// a closed pool) but the new one has not started yet (the engine
	// container is restarting — the start retries of Run/Alive). The
	// first successful start after the latch counts the restart in
	// Restarts (the canonical death → T4 retries → recovery path would
	// otherwise complete as T2 and grow nothing — the T-283 review, P1).
	// Under mu (restartLocked).
	restartPending bool
	restartReason  string
}

// NewManager — a manager over the lifecycle context ctx: all manager
// browsers start on it, cancelling ctx stops them, including the background
// generations of past restarts; do not put a deadline on ctx (the
// StartBrowser contract). opts is validated on the first start.
func NewManager(ctx context.Context, opts Options) *Manager {
	m := &Manager{opts: opts.withDefaults(), lifeCtx: ctx}
	m.newBrowser = StartBrowser
	m.newPool = NewPool
	return m
}

// Run executes a render task on the managed browser: before the task a
// live generation is guaranteed (a start or a restart by triggers — the
// page and memory thresholds, browser death), then Pool.Run. Page and
// context errors are returned as is (the pool has already made its own
// retry on a fresh context); browser death mid-task (ClassBrowser), pool
// closure by a generation swap before the task started (ErrClosed — the
// task did not run) and a context failure raced by that closure
// (ClassContext wrapped in ErrClosed by the pool — an in-place retry is
// impossible in the retiring generation, 05#P2-1) mean a restart and a
// retry, at most maxRestartsPerTask times.
//
// A failed generation START (the engine is unreachable — a container
// restart, T-283) is not a task error: within the Options.EngineWait
// budget the start is retried with a backoff (engineRetryStep), and the
// waiting does not spend the execution budget — page executions stay
// bounded by 1+maxRestartsPerTask (a page-killer must not multiply
// them). Only when the budget is exhausted (or EngineWait is off) the
// ClassBrowser error goes upward — a re-delivery is the queue level
// (R5), where the recovery claim gate of the worker (T-283) keeps the
// attempts from burning while the engine is down.
func (m *Manager) Run(ctx context.Context, task RenderTask) (RenderOutput, error) {
	// The engine-recovery budget of THIS call (T-283): the WALL time
	// since the call's start that may be spent on failed generation
	// starts (waits and the start attempts themselves — dial retries eat
	// into the deadline, they do not stack on top of it: the total hold
	// of the delivery is bounded by EngineWait + the in-flight tail, up
	// to (1+maxRestartsPerTask) dial+page segments — the binding
	// render.timeout + engine_wait < redis.claim_min_idle of
	// config.Validate covers the regular restart shape with that
	// margin). EngineWait 0 — the deadline is now, waitStart always
	// refuses at once (the pre-T-283 behavior).
	deadline := timeNow().Add(m.opts.EngineWait)
	step := time.Duration(0)
	waitStart := func() bool {
		if remain := deadline.Sub(timeNow()); remain > 0 {
			step = engineRetryStep(step)
			if step > remain {
				step = remain
			}
			t := time.NewTimer(step)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return false
			case <-t.C:
			}
			return true
		}
		return false
	}
	executions := 0 // page executions (the maxRestartsPerTask budget)
	for {
		p, err := m.acquire()
		if err != nil {
			if errors.Is(err, ErrClosed) {
				return RenderOutput{}, &Error{Class: ClassTask, URL: task.URL, Err: err}
			}
			// The engine is unreachable (a failed generation start,
			// T-283): wait for the recovery within the budget and retry
			// the START — the page has not run, the execution budget is
			// not spent. A refusal (the budget is off/exhausted) returns
			// the start error upward.
			if !waitStart() {
				if ctxErr := ctx.Err(); ctxErr != nil {
					// A graceful shutdown during the engine wait: the
					// delivery returns to the queue with a re-delivery
					// (spec 3.4); the error is the context's, not the
					// engine's — the stage error counter and the
					// fallback gate streak must not count a shutdown as
					// an engine outage (the T-283 review, P3; the
					// mid-page shutdown is classified the same way —
					// ClassContext).
					return RenderOutput{}, &Error{Class: ClassContext, URL: task.URL, Err: ctxErr}
				}
				return RenderOutput{}, err
			}
			continue
		}
		out, err := runAcquired(ctx, p, task)
		executions++
		// Equivalent mutant (T-M08): cutting out the if body when
		// err == nil falls through to the next if, where the third
		// disjunct is trivially true (ClassOf(nil) = ClassTask !=
		// ClassBrowser, and errors.Is(nil, ErrClosed) = false) — the
		// same (out, nil) with err == nil is returned.
		// mutator-disable-next-line branch/if
		if err == nil {
			return out, nil
		}
		if m.closed.Load() || executions > maxRestartsPerTask ||
			(ClassOf(err) != ClassBrowser && !errors.Is(err, ErrClosed)) {
			return out, err
		}
	}
}

// runAcquired executes the task on the pool, holding the generation slot
// taken in acquire (wg.Done) until the end of Pool.Run — see the
// rationale in acquire.
func runAcquired(ctx context.Context, p *Pool, task RenderTask) (RenderOutput, error) {
	defer p.wg.Done()
	return p.Run(ctx, task)
}

// Start starts the first generation immediately rather than lazily with
// the first task: the production build (cmd/render-worker) must fail at
// startup when Chromium is unavailable (an OnRun error → exit code 1 →
// systemd Restart=on-failure), instead of silently cycling the q:render
// delivery attempts. On a live generation — a no-op; after Close —
// ErrClosed.
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed.Load() {
		return ErrClosed
	}
	if reason, swap := m.swapReasonLocked(); swap {
		return m.restartLocked(reason)
	}
	return nil
}

// Alive — the worker consumption probe (renderworker.Options.Alive):
// whether the current generation is alive; a dead or missing one is
// replaced right here (restartLocked → a new StartBrowser), so after a
// connection loss or a Chromium restart (the host unit
// crawler-chromium.service, Restart=on-failure) the probe restores the
// generation itself — the consumption pause never hangs forever. A start
// failure — false: consumption stays paused (deliveries do not spend
// attempts, T-025), the next probe call retries the start. The manager
// closedness and the generation liveness are decided by the FSM predicate
// genStateLocked (genstate.go, T-263).
func (m *Manager) Alive() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.genStateLocked() == genClosed {
		return false
	}
	if reason, swap := m.swapReasonLocked(); swap {
		return m.restartLocked(reason) == nil
	}
	return true
}

// acquire returns the pool of the current live generation, swapping it
// when needed (the triggers — swapReason). Concurrent calls are
// serialized by the mutex: the generation swap is single; the threshold
// checks run under the same lock — RSS is checked once per task, the
// /proc scan no more often than the rssCacheTTL cache window (Browser.RSS,
// T-229).
//
// The taken pool is marked in flight (wg.Add) under the same m.mu: the
// retire of the swapped-out generation is also under m.mu, so a generation
// swap cannot slip between acquire and Pool.Run — and the background pool
// Close never waits on a zero-count WaitGroup while a task holds an
// acquired slot (Add from zero concurrently with Wait is forbidden by
// sync.WaitGroup). The slot is released in runAcquired after Pool.Run.
func (m *Manager) acquire() (*Pool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed.Load() {
		return nil, ErrClosed
	}
	if reason, swap := m.swapReasonLocked(); swap {
		if err := m.restartLocked(reason); err != nil {
			return nil, err
		}
	}
	m.p.wg.Add(1)
	return m.p, nil
}

// restartLocked starts a new generation. The old pool is taken out of
// service synchronously (retire — new tasks bypass it), while it finishes
// its pages and closes the browser in a background goroutine: graceful
// (spec 3.4) does not block the start — otherwise a memory-triggered
// restart would wait for the longest pages, the very ones that consumed
// the budget. A failed start returns the error (the task goes upward, the
// next Run attempt repeats the start), and the former pool still goes to
// closure: its browser is dead or has exhausted the threshold.
//
// The browser start runs under m.mu — deliberately (R-07): while the new
// generation starts, there is no live generation and concurrent tasks have
// nothing to acquire — their wait on the mutex is equivalent to waiting
// for the generation, and moving the start out of the critical section
// (single-flight) would save nothing while adding generation-installation
// races. The upper bound is Options.DialTimeout (connecting remote,
// including resolving the /json/version address) plus
// Options.SessionStartTimeout (the bootstrap commands, initBrowserSession
// — T-218: chromedp itself does not bound them) or reading the process
// address (local, 20 s in chromedp).
func (m *Manager) restartLocked(reason string) error {
	old := m.p
	if old != nil {
		old.retire()
		// A live generation was taken out of service (death, thresholds,
		// a closed pool): its replacement — in THIS call or on the first
		// successful start after the T4 retries (the engine container is
		// restarting, T-283) — is a restart, counted once (T-283 review,
		// P1: the canonical outage path death → failed start (the slot
		// is cleared) → recovery used to complete as T2 and grow nothing
		// in Restarts/crawler_render_browser_restarts_total). The reason
		// of the call that detached the generation is the honest one for
		// the completion log ("browser unavailable", not the "first
		// start" of the recovering call).
		m.restartPending, m.restartReason = true, reason
	}
	m.b, m.p = nil, nil
	b, err := m.newBrowser(m.lifeCtx, m.opts)
	if err != nil {
		if old != nil {
			go old.Close()
		}
		return err
	}
	m.b = b
	m.p = m.newPool(b)
	if !m.restartPending {
		// The first start of the manager (no generation has ever been
		// replaced) — not a restart.
		// Logging of the chromium start (§4 docs/mutation-testing.md).
		// mutator-disable-next-line statement/remove
		slog.Info("chromium started", "mode", m.mode(), "pid", b.rootPID)
		return nil
	}
	m.restartPending = false
	// The reason of the call that DETACHED the replaced generation: for
	// the T4→T2 completion (the recovery after failed retries, T-283) the
	// recovering call's own reason is the misleading "first start".
	reason, m.restartReason = m.restartReason, ""
	if old != nil {
		go old.Close()
	}
	n := m.restarts.Add(1)
	// Logging of the chromium restart (§4 docs/mutation-testing.md).
	// mutator-disable-next-line statement/remove
	slog.Info("chromium restarted", "reason", reason,
		"restarts", n, "pid", b.rootPID)
	return nil
}

func (m *Manager) mode() string {
	// A branch mutant changes only the start log text (remote would be
	// called local): mode() is consumed only by slog.Info in
	// restartLocked — logging, §4 docs/mutation-testing.md.
	// mutator-disable-next-line branch/if
	if m.opts.RemoteURL != "" {
		return "remote " + m.opts.RemoteURL
	}
	return "local process"
}

// Close stops the manager: new tasks are rejected (ErrClosed →
// re-delivery), the current generation finishes (graceful, spec 3.4) and
// is stopped together with the browser. Under the mutex — only the flip to
// closed and the generation detach: the long drain of the generation tasks
// (up to Options.Timeout) does not block acquire, the Alive probe or
// Browser() — new tasks get ErrClosed immediately, without waiting for the
// drain (R-07). If Close lands on a restart in progress, it waits for the
// mutex until the start completes (up to Options.DialTimeout +
// Options.SessionStartTimeout — the cap of initBrowserSession, T-218): a
// generation start is short-lived, and the browser of this not yet created
// generation is killed by the lifecycle context cancellation.
// The background closures of past generations complete independently. A
// repeated call is harmless.
func (m *Manager) Close() {
	m.mu.Lock()
	// Equivalent mutant (T-M08): falling through from a failed CAS runs
	// the tail of Close — p := m.p is always nil (the first Close
	// detached the generation, no new one is raised after closed), the
	// repeated detach and Unlock below give the same result. The
	// statement/remove of the same line is not suppressed: cutting out
	// Unlock specifically hangs the mutex — killed by a test (Run after
	// a repeated Close).
	// mutator-disable-next-line branch/if
	if !m.closed.CompareAndSwap(false, true) {
		m.mu.Unlock()
		return
	}
	p := m.p
	m.b, m.p = nil, nil
	m.mu.Unlock()
	if p != nil {
		p.Close() // outside m.mu: finishes the tasks and kills the browser
	}
}

// Restarts — the number of browser restarts (diagnostics; metrics —
// T-076+).
func (m *Manager) Restarts() int64 { return m.restarts.Load() }

// Browser — the browser of the current generation (the Pages/RSS counters
// are metrics); nil before the first start or after Close.
func (m *Manager) Browser() *Browser {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.b
}

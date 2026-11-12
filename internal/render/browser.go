package render

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/emulation"
	"github.com/chromedp/cdproto/target"
	"github.com/chromedp/chromedp"
)

// probeTimeout — the timeout of the live connection probe (probeConn) during
// error classification: with headroom for an overloaded Chromium (T-226: 2s
// hit DeadlineExceeded on a healthy but render-busy browser — and the
// probe runs before every task). The production value; the reconnect
// tests substitute the Browser.probeTimeout field with a short one.
const probeTimeout = 5 * time.Second

// tabProbeTimeout — the tab probe timeout (Session.probe): a tab does not
// decide the generation's fate, the short cap is kept (the retry decision
// on a new context does not wait for the page timeout level); the T-226
// headroom is only needed for the connection probe, which can kill a generation.
const tabProbeTimeout = 2 * time.Second

// probeFailThreshold — how many connection probe failures in a row kill
// the generation (T-226): a single timeout does not distinguish the T-218 black hole from
// an overloaded Chromium; two in a row — sufficient evidence of death.
const probeFailThreshold = 2

// rssCacheTTL — the RSS cache lifetime (T-229): the memory threshold
// check runs before every task (Manager.swapReasonLocked), and a measurement is
// a full /proc traversal; the cache limits the traversal to one per window. An error
// of up to TTL is safe: the memory-based restart is soft prevention (spec 5.6),
// the hard cap is held by the cgroup (mem_limit, spec 3.2/10.1).
const rssCacheTTL = 10 * time.Second

// Browser — the worker's single Chromium (spec 5.6: "one browser"): one
// WebSocket connection (remote) or one child process (local) and one
// service tab about:blank, on top of which the isolated
// task contexts (Session) are opened. The browser is reused by all tasks;
// restart by the page/memory counter — T-065.
type Browser struct {
	opts Options

	// ctx — the chromedp context of the browser session: it owns the connection and
	// the service tab. Cancellation (Close or connection loss) kills all
	// task contexts too — they are children.
	ctx    context.Context
	cancel context.CancelFunc
	// cancelAlloc stops the allocator: remote — closes the connection
	// after the tabs are closed, local — waits for the process to exit.
	cancelAlloc context.CancelFunc

	pages atomic.Int64 // successfully rendered pages (T-065)

	// rootPID — the PID of the main Chromium process in local mode
	// (0 — remote mode or the process is undetermined): the root of the process tree
	// for memory control (Browser.RSS, T-065). Unchanged after startup.
	rootPID int

	// rss cache (T-229): the last positive RSS measurement, the moment
	// of the snapshot and the rootPID it was taken for (a generation change —
	// a new Browser; a direct rootPID reset — tests). The /proc traversal used to run
	// on every task, now — no more often than rssCacheTTL. Under mu, like Close:
	// reads come from swapReasonLocked under the manager's mutex, but the method
	// is public (OverMemory, tests).
	rssPID int
	rss    int64
	rssAt  time.Time

	// lost — the sticky "connection is dead" mark (T-218): it is set by
	// probeConn when the live browser probe does not confirm the connection —
	// chromedp knows nothing in such a situation (WebSocket reading does
	// not react to the context or to timeouts). A half-closed socket does
	// not resurrect, so the mark is never cleared: the Manager changes the generation.
	// Since T-226 it is set by a proven transport death (the very first failure)
	// or by a series of probe timeouts — see probeConn.
	lost atomic.Bool

	// probeFails — the counter of consecutive connection probe failures
	// (T-226): success resets it, the probeFailThreshold kills the generation
	// (lost). Atomic: the probe runs before every pool task concurrently;
	// "consecutiveness" under races is approximate, any interpretation
	// is safe (success zeroes it, failures accumulate).
	probeFails atomic.Int32

	// probeTimeout — the budget of one connection probe: the production value
	// is the probeTimeout constant (set by StartBrowser), the reconnect
	// tests substitute it with a short one (a field, not config): a probe series runs
	// in real time and must not push a package beyond the budget
	// of the mutation run (MUTATE_TIMEOUT, T-226). A zero value —
	// fail-safe: every probe times out instantly.
	probeTimeout time.Duration

	mu     sync.Mutex
	closed bool
}

// StartBrowser connects to an external Chromium (Options.RemoteURL — remote
// via an own allocator on top of chromedp/remote, remotealloc.go —
// deterministic connection closing, T-218) or starts an own
// headless-shell process (an empty RemoteURL — chromedp.NewExecAllocator,
// the production render image) and initializes the browser session. The
// browser session profile is headless-shell itself without a user directory:
// it is a service one, task pages live in isolated contexts and do not
// touch it.
//
// ctx — the lifecycle context: in chromedp the browser lives as long as
// the context of the first call lives, so canceling ctx stops the browser
// (including initialization); do not put a deadline on ctx.
//
// A connection error — a *Error of class ClassBrowser: the worker may retry
// the start (T-065/T-066) without parsing error texts.
func StartBrowser(ctx context.Context, opts Options) (*Browser, error) {
	// The mutant is unobservable in CI (T-M07): the derived timeouts
	// (DialTimeout/SessionStartTimeout) are visible only on a successfully
	// started browser (the sessions' b.opts, the remote dial timeout);
	// hermetic starts fail earlier — connection refused is instant,
	// the local binary is absent. A live Chromium — the live tests behind
	// CRAWLER_RENDER_TEST=1, not part of the mutation run.
	// mutator-disable-next-line statement/remove
	opts = opts.withDefaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}

	var allocCtx context.Context
	var cancelAlloc context.CancelFunc
	mode := opts.RemoteURL
	if mode != "" {
		// External CDP server (dev stack: the compose service chromium; the production
		// systemd unit crawler-chromium, render.remote_url; the same for
		// the alternative engines moli/lightpanda, render spec §4.2): an own
		// allocator on top of chromedp/remote — a WebSocket connection with
		// deterministic socket closing on a generation change
		// (remotealloc.go, T-218); the address mode — per engine
		// (wsURLMode): chromium refines the address with a /json/version request,
		// lightpanda connects by the configuration address, moli
		// combines the path from /json/version with the configuration address.
		allocCtx, cancelAlloc = chromedp.NewAllocatorContext(ctx,
			newRemoteAllocator(opts))
	} else {
		// Production mode: an own process — only for the chromium engine
		// (Options validation requires RemoteURL for moli|lightpanda,
		// they do not reach here; render spec §4.2, FR-3). --no-sandbox is mandatory
		// in an unprivileged container (deploy/docker/Dockerfile.render,
		// USER nobody); the rest — the chromedp default set: headless, without
		// browser background services.
		mode = "локальный процесс"
		execOpts := append(chromedp.DefaultExecAllocatorOptions[:],
			chromedp.Flag("no-sandbox", true))
		if opts.ExecPath != "" {
			execOpts = append(execOpts, chromedp.ExecPath(opts.ExecPath))
		}
		allocCtx, cancelAlloc = chromedp.NewExecAllocator(ctx, execOpts...)
	}

	bctx, cancel := chromedp.NewContext(allocCtx)
	b := &Browser{opts: opts, ctx: bctx, cancel: cancel, cancelAlloc: cancelAlloc,
		probeTimeout: probeTimeout}

	// Initialization of the connection and the service tab. The first Run must run
	// on bctx itself: in chromedp the browser lives as long as the context of the first
	// call lives (the rule from the chromedp.Run doc comment) — a child context with
	// a timeout would be canceled after initialization and kill the browser,
	// so the cap is the outrunning race of initBrowserSession, not a deadline
	// on bctx itself. Connecting is bounded by Options.DialTimeout
	// (remoteDial, including the /json/version address refinement); the further
	// bootstrap (creating/attaching the service tab, enabling
	// domains) is not bounded by chromedp itself — on a stalled but accepting
	// Chromium it hangs forever, holding the manager's mutex
	// (restartLocked, T-218), — so the attempt is raced against
	// the external cap DialTimeout + SessionStartTimeout (the same
	// scale as the session's bootstrap). Local mode — reading the process address
	// (20s in chromedp), the race cap covers it too; the external boundary —
	// the caller's ctx cancellation.
	if err := initBrowserSession(bctx, opts); err != nil {
		// Cancellations on failed initialization — chromedp resource hygiene
		// (goroutines/connections/process); hermetic start failures
		// (connection refused, fork/exec of a nonexistent binary) have no
		// time to create anything — the cancellation effect is unobservable without
		// a half-started browser, which is not reproducible in CI (T-M07).
		// mutator-disable-next-line statement/remove
		cancel()
		// mutator-disable-next-line statement/remove
		cancelAlloc()
		return nil, &Error{
			Class: ClassBrowser,
			Err:   fmt.Errorf("инициализация Chromium (%s): %w", mode, err),
		}
	}

	// Local mode: remember the Chromium process — the root of the tree for
	// memory control (T-065). chromedp hands it over explicitly (Browser.Process
	// is documented exactly for collecting process metrics); in remote mode
	// there is no process nearby — memory control is disabled (RSS = -1).
	//
	// The block below runs only after the local
	// Chromium is initialized successfully (chromedp.Do above), i.e. with a live browser process — in CI
	// the start always fails, and the live tests behind CRAWLER_RENDER_TEST=1 are not
	// part of the mutation run (the precedent — NVMe, T-M01). The block's
	// mutants are killed: the nil halves of the FromContext check after a successful Do
	// (expression/remove), cutting out the Process() condition and the
	// rootPID assignment from the real process (branch/if on the if line,
	// statement/remove on the statement); the "undetermined" boundary of 0 against
	// -1/1 (numbers) changes only the Warn — rootPID stays 0 and RSS remains
	// -1; the Warn itself is logging (§4 docs/mutation-testing.md).
	if opts.RemoteURL == "" {
		// mutator-disable-next-line expression/remove
		if c := chromedp.FromContext(bctx); c != nil && c.Browser != nil {
			// mutator-disable-next-line branch/if
			if p := c.Browser.Process(); p != nil {
				// mutator-disable-next-line statement/remove
				b.rootPID = p.Pid
			}
		}
		// mutator-disable-next-line numbers/decrementer, numbers/incrementer, branch/if
		if b.rootPID == 0 {
			// mutator-disable-next-line statement/remove
			slog.Warn("процесс Chromium не определён: перезапуск по памяти отключён")
		}
	}
	return b, nil
}

// initBrowserSession — initialization of the browser session (connection +
// service tab) with an external cap: the chromedp bootstrap is not
// bounded from inside (see StartBrowser), and the start runs under the
// manager's mutex — an outrunning race, as in Session.start. An overrun is an error
// (the generation start will be retried by the next task/probe), the caller
// StartBrowser cancels everything created.
func initBrowserSession(bctx context.Context, opts Options) error {
	type result struct{ err error }
	done := make(chan result, 1)
	go func() {
		done <- result{chromedp.Do(bctx)}
	}()
	limit := opts.DialTimeout + opts.SessionStartTimeout
	timer := time.NewTimer(limit)
	defer timer.Stop()
	select {
	case r := <-done:
		return r.err
	case <-timer.C:
		return fmt.Errorf("таймаут инициализации %s", limit)
	}
}

// Alive — whether the browser session is alive: the context is not canceled, the
// connection to Chromium is not lost and the connection probe has not declared it dead (lost,
// T-218). On connection loss chromedp closes LostConnection in
// both modes (a WebSocket drop in remote, process death in local) and
// cancels the browser context; a half-closed connection (no FIN/RST —
// NAT/conntrack, a browser stalled on shutdown) is invisible
// to the library at all — probeConn catches it.
func (b *Browser) Alive() bool {
	if b == nil || b.ctx == nil || b.ctx.Err() != nil {
		return false
	}
	if b.lost.Load() {
		return false
	}
	if c := chromedp.FromContext(b.ctx); c != nil && c.Browser != nil {
		select {
		case <-c.Browser.LostConnection:
			return false
		default:
		}
	}
	return true
}

// probeConn — the probe of the connection to the browser: a live browser-level
// command (Target.getTargets) with a short timeout. Needed in addition to Alive: chromedp
// learns of a connection death only from socket errors, and WebSocket
// reading (remote.Conn.Read) does not react to the context or to
// timeouts — a half-closed connection looks alive forever
// (the Nov 02 incident: sessions hung in "context creation timeout 15s" ×
// 11,520 over 6h, the generation was never changed).
//
// A probe failure is not a death sentence for the generation (T-226, the flip side
// of the T-218 countermeasure): a timeout does not distinguish the T-218 black hole (the transport
// is "alive", no errors, no answer) from an overloaded Chromium (getTargets does not
// make it within probeTimeout), and the probe runs before every task — a false
// death on a loaded worker (a generation restart, tearing down others' tasks)
// is more expensive than an extra probe. Therefore the timeout and other unexplained failures
// accumulate: lost is set from the second in a row (a success in between resets
// the counter; the second comes with the Manager.Run task retry or the next
// task — the black hole detection degrades from probeTimeout to ~2×probeTimeout
// against the "minutes" recovery budget of the Nov 02 incident). A proven
// transport death — a cancellation error (context.Canceled: the browser context
// canceled or chromedp's LostConnection/lostError on a closed socket) —
// kills immediately, with a single failure: a half-closed socket does not resurrect, the mark
// is sticky, the Manager changes the generation.
func (b *Browser) probeConn() bool {
	if b == nil {
		return false
	}
	if b.lost.Load() {
		return false
	}
	if b.ctx == nil {
		// Without a chromedp context (the package's unit fakes) — the probe has
		// nothing to check; Alive already behaved the same way for such browsers.
		return true
	}
	c := chromedp.FromContext(b.ctx)
	if c == nil || c.Browser == nil {
		return true
	}
	ctx, stop := context.WithTimeout(b.ctx, b.probeTimeout)
	defer stop()
	if _, err := cdp.Call(ctx, c.Browser, target.GetTargets, target.GetTargetsParams{}); err != nil {
		if errors.Is(err, context.Canceled) {
			// Death is proven by the transport (a closed socket/cancellation) —
			// instantly, as before T-226.
			b.lost.Store(true)
			return false
		}
		// A timeout (the T-218 black hole or an overloaded browser — indistinguishable
		// by one answer) or a rare erroneous protocol answer on
		// a live connection: we accumulate consecutive failures.
		if b.probeFails.Add(1) >= probeFailThreshold {
			b.lost.Store(true)
		}
		return false
	}
	b.probeFails.Store(0)
	return true
}

// Pages — the number of successfully rendered pages since the browser start: the counter
// of the "every N pages" restart (spec 5.6; the Options.RestartEveryPages threshold,
// the restart itself — T-065).
func (b *Browser) Pages() int64 { return b.pages.Load() }

// NeedsRestart — whether the restart threshold by page count is reached (T-065):
// the Pages counter is zeroed by a generation change (the Manager).
func (b *Browser) NeedsRestart() bool {
	return b.Pages() >= int64(b.opts.RestartEveryPages)
}

// procRSS — the /proc traversal for memory control; the seam of the RSS cache unit
// tests (the pass counter, T-229), the production path is procTreeRSS (the
// remoteDial style).
var procRSS = procTreeRSS

// timeNow — the time for the RSS cache; the unit test seam (controlled time,
// T-229).
var timeNow = time.Now

// RSS — the total RSS of the Chromium process tree (the main process and all
// descendants — renderers, GPU: each is counted into the spec 3.2 budget), bytes;
// the root is the PID of the local mode process. -1 — unknown: remote mode
// (the browser is in a foreign container, its cap is held by the container's mem_limit)
// or /proc is unavailable. The value is cached for rssCacheTTL (T-229):
// the threshold check runs on every task, a measurement is a full /proc traversal.
func (b *Browser) RSS() int64 {
	// Equivalent mutants (T-M07): `rootPID < 0` and `<= -1` at
	// rootPID == 0 lead into procTreeRSS, whose own root <= 0 guard
	// returns the same -1; cutting out `rootPID <= 0` (expression/remove)
	// is equivalent for the same reason (the nil half remains) and along the way
	// kills the killable "b == nil → false" — the trade-off of line annotations
	// (§4 docs/mutation-testing.md, the T-M02 precedent); the nil receiver and
	// a valid pid 1 are pinned by tests (TestBrowserRSSUnknown).
	// mutator-disable-next-line expression/comparison, numbers/decrementer, expression/remove
	if b == nil || b.rootPID <= 0 {
		return -1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	// A fresh snapshot is needed if the cache is of a foreign rootPID (the first read or
	// a root change), empty, or the TTL has expired. The comparison equivalent
	// mutants (trade-off §4 docs/mutation-testing.md): `>` → `>=` on b.rss —
	// indistinguishable (the cache stores only positive measurements, b.rss == 0
	// with a valid rssPID is unreachable); the TTL boundary `<` → `<=` —
	// an update a nanosecond earlier, unobservable (the cache freshness,
	// not the restart decision). The rssPID `==` inversion and cutting out
	// the whole condition are killed by TestBrowserRSSCache (the second read of the same
	// root — from the cache, a rootPID change — a fresh snapshot).
	now := timeNow()
	// mutator-disable-next-line expression/comparison
	if b.rssPID == b.rootPID && b.rss > 0 && now.Sub(b.rssAt) < rssCacheTTL {
		return b.rss
	}
	rss := procRSS(b.rootPID)
	// Only positive measurements are cached: 0 (the root vanished from the
	// snapshot) and -1 (/proc unavailable) — not values but their absence;
	// another pass is cheaper than a sticky "unknown" until the end of the TTL.
	if rss > 0 {
		b.rss, b.rssAt, b.rssPID = rss, now, b.rootPID
	}
	return rss
}

// OverMemory — whether the restart memory threshold
// Options.MemoryThreshold is exceeded (spec 5.6). An unknown RSS and a zero threshold
// (control disabled) do not trigger: the restart is soft prevention
// of memory growth, the hard cap in any mode is held by the container's cgroup
// (mem_limit — the docker analog of MemoryMax, spec 3.2/10.1).
func (b *Browser) OverMemory() bool {
	if b == nil || b.opts.MemoryThreshold <= 0 {
		return false
	}
	rss := b.RSS()
	// Equivalent mutants (T-M07): at threshold >= 1 (the guard above) all
	// variants of the left half (`>= 0`, `true`, `> -1`, `> 1`) are indistinguishable
	// from `rss > 0`: rss == 0 and rss == -1 are discarded by the right half
	// (0/-1 >= threshold is false), and rss == 1 is unreachable — RSS from /proc
	// is a multiple of the page (>= 4096). The expression/comparison annotation along the way
	// also kills the `>= → >` boundary on the right (trade-off §4); the boundary itself, "RSS
	// equals the threshold — an excess", is pinned by TestBrowserOverMemory.
	// mutator-disable-next-line expression/comparison, expression/remove, numbers/decrementer, numbers/incrementer
	return rss > 0 && rss >= b.opts.MemoryThreshold
}

// Close stops the browser: the tabs and the isolated contexts are closed,
// then the connection (remote) or the process (local). A repeated call
// and a call on an uninitialized browser (unit tests) are harmless — the nil-guard
// here is consistent with Alive/RSS/OverMemory: all Browser methods tolerate
// the zero value.
func (b *Browser) Close() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	if b.cancel != nil {
		b.cancel()
	}
	if b.cancelAlloc != nil {
		b.cancelAlloc()
	}
}

// classifyCtx classifies a failure of an operation over a browser that is alive by its contexts
// (the task context cancellation and the page timeout are checked by the caller —
// Session.wrapTask/sessionStart). The order, without parsing error texts:
// browser death — by the connection state; tab context death — by
// its context; an unresponsive tab (chromedp does not cancel the tab
// context on its crash) — by the live probe.
func (b *Browser) classifyCtx(err error, url string, s *Session) *Error {
	switch {
	case !b.Alive():
		return &Error{Class: ClassBrowser, URL: url,
			Err: fmt.Errorf("браузер Chromium недоступен: %w", err)}
	case s != nil && s.ctx != nil && s.ctx.Err() != nil:
		return &Error{Class: ClassContext, URL: url,
			Err: fmt.Errorf("контекст вкладки отменён: %w", err)}
	case errors.Is(err, chromedp.ErrPageLoad):
		// The page did not load (net::ERR_*): the tab is alive, the profile is intact.
		return &Error{Class: ClassTask, URL: url, Err: err}
	case s == nil || !s.probe():
		// An unresponsive tab: distinguish a dead connection to the browser
		// (a half-closed socket — the probeConn probe, T-218) from a dead tab
		// on a live connection: the former — the browser class (a retry on a new
		// context of the same dead socket is pointless, the Manager will change
		// the generation), the latter — the context class (the existing retry).
		// Since T-226 the first probe failure does not yet kill the generation (an overloaded
		// browser is possible), but the class is the same: the Manager.Run retry
		// probes again — a success continues the task on the same
		// generation, a second failure in a row kills it.
		if !b.probeConn() {
			return &Error{Class: ClassBrowser, URL: url,
				Err: fmt.Errorf("браузер Chromium недоступен: %w", err)}
		}
		return &Error{Class: ClassContext, URL: url,
			Err: fmt.Errorf("контекст вкладки не отвечает: %w", err)}
	default:
		return &Error{Class: ClassTask, URL: url, Err: err}
	}
}

// setUserAgent applies the session's identifiable UA (spec §9) on top of the tab.
func setUserAgent(ctx context.Context, t *chromedp.Target, ua string) error {
	_, err := cdp.Call(ctx, t, emulation.SetUserAgentOverride,
		emulation.SetUserAgentOverrideParams{UserAgent: ua})
	return err
}

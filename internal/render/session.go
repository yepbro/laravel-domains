package render

import (
	"context"
	"fmt"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

// Session — one isolated Chromium context (CDP
// Target.createBrowserContext, chromedp.WithNewBrowserContext): an
// incognito profile with its own cookies, cache and storages plus a single
// about:blank tab. It lives for exactly one task: after a page the pool
// closes the session and the next task gets a fresh profile — no state
// survives between pages (spec 5.6, §9; the rationale for recreating
// instead of clearing is in the package doc comment).
type Session struct {
	b      *Browser
	ctx    context.Context    // the tab's chromedp context (a child of the browser's)
	cancel context.CancelFunc // closes the tab and destroys the profile

	// probeFn replaces the tab probe in unit tests; nil — the real probe.
	probeFn func() bool
}

// NewSession creates an isolated context with a tab (an incognito profile:
// own cookies/cache/storages, isolation from other sessions and from the
// browser's service tab). If Options.UserAgent is set, it is applied to
// the tab (spec §9). After the task the session must be closed with Close
// — the profile is discarded entirely.
func (b *Browser) NewSession(ctx context.Context) (*Session, error) {
	if !b.Alive() {
		return nil, &Error{Class: ClassBrowser, Err: ErrBrowserDead}
	}
	// The connection probe before creating the context (T-218): Alive
	// does not see a half-closed socket, and it shows up exactly here —
	// as a hung createBrowserContext until SessionStartTimeout (15 s of
	// useless waiting per task, the Nov 2 incident). The probe settles
	// the question within probeTimeout: a failure — the browser class,
	// Manager retries the task; the generation is swapped on the second
	// probe failure in a row or immediately on transport death (T-226:
	// a one-off timeout of an overloaded Chromium does not break the
	// generation).
	if !b.probeConn() {
		return nil, &Error{Class: ClassBrowser, Err: ErrBrowserDead}
	}
	sctx, cancel := chromedp.NewContext(b.ctx, chromedp.WithNewBrowserContext())
	s := &Session{b: b, ctx: sctx, cancel: cancel}
	if err := s.start(ctx); err != nil {
		// Close the half-initialized tab (if one is being created) and
		// throw away the partially created profile.
		cancel()
		return nil, err
	}
	return s, nil
}

// start initializes the tab (createBrowserContext + about:blank) and
// applies the User-Agent. The first call must run on the tab context
// s.ctx itself: in chromedp the tab's event pump (Target.run) is bound to
// the context of the first call — a short-lived child with a timeout would
// be cancelled right after initialization and all further tab commands
// would hang (reproducible on a live Chromium). The wait is bounded by
// SessionStartTimeout in a racing select: on exceeding it or on the caller
// context being cancelled the session is closed entirely (NewSession
// cancels the tab), not "finished up later".
func (s *Session) start(ctx context.Context) error {
	type result struct{ err error }
	done := make(chan result, 1)
	go func() {
		err := chromedp.Do(s.ctx)
		if err == nil && s.b.opts.UserAgent != "" {
			err = chromedp.Do(s.ctx, chromedp.Func(func(c context.Context, t *chromedp.Target) error {
				return setUserAgent(c, t, s.b.opts.UserAgent)
			}))
		}
		done <- result{err}
	}()

	timer := time.NewTimer(s.b.opts.SessionStartTimeout)
	defer timer.Stop()
	select {
	case r := <-done:
		if r.err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return &Error{Class: ClassTask,
				Err: fmt.Errorf("task cancelled: %w: %w", ctx.Err(), r.err)}
		}
		// Classification without the tab probe (the session is not up):
		// a dead browser versus a failed context.
		return s.b.classifyCtx(r.err, "", nil)
	case <-timer.C:
		// The context was not created within SessionStartTimeout:
		// distinguish a dead/hung connection (the probeConn probe — the
		// browser class: a retry on a new context of the same dead socket
		// is pointless, the generation is swapped by Manager) from a
		// merely slow browser with a live connection (the context class —
		// the existing retry on a new context). The Nov 2 incident:
		// without the probe this branch alone produced 11,520 WARN
		// "the render context died" messages in 6 h on a half-closed
		// connection (T-218).
		if !s.b.probeConn() {
			return &Error{Class: ClassBrowser,
				Err: fmt.Errorf("Chromium browser unavailable: context creation timeout %s",
					s.b.opts.SessionStartTimeout)}
		}
		return &Error{Class: ClassContext,
			Err: fmt.Errorf("context creation timeout %s", s.b.opts.SessionStartTimeout)}
	case <-ctx.Done():
		return &Error{Class: ClassTask,
			Err: fmt.Errorf("task cancelled: %w", ctx.Err())}
	}
}

// Run executes the task: navigation to task.URL (NavigateResponse — with
// the main navigation's response, T-064), waiting for page readiness
// (T-063: the load event with a cap + bounded network quiet) and capturing
// the result — the final URL and title (T-061), the final DOM, all cookies
// of the final page and the headers of the main navigation's response
// (T-064, extract.go). The overall page timeout is Options.Timeout
// (spec 5.6: 25 s by default); it bounds the task's actions, not the
// profile's lifetime. Expiry of the overall timeout is a task error
// (ClassTask), not a partial result: per spec §8 an interruption by the
// limit is recorded as an error code, and there is no "partial" DOM in
// the §6.1 schema — T-067 would replace the full raw HTML with an
// under-rendered DOM without a marker; the raw HTML is already saved by
// the HTTP stage and remains the result until a successful render
// (§7: render is an asynchronous addition, a failure means re-delivery by
// the queue level, T-066). The soft caps of the wait phases are not an
// error; after them the result is captured as is (wait.go).
//
// The context chain goes from the tab (s.ctx): chromedp looks up the
// session by a value in the context; a context derived from the caller
// does not see it. The cancellation and deadline of the caller ctx are
// brought in by AfterFunc: they stop the task's actions without touching
// the profile.
//
// The URL scheme is validated before any CDP commands (validateTaskURL,
// R-09): file:/data: — the browser would open them and the DOM capture
// would carry the contents of a local resource into the content store.
// With the SSRF guard enabled, an internal IP-literal host is rejected
// right alongside (validateTaskURLInternal, T-219:
// http.block_internal_addrs — a flag shared with the HTTP stage).
//
// Blocking of images/fonts/media (T-062, spec 5.6; with the
// Options.BlockStylesheet option — CSS too, T-213; with
// Options.BlockTrackerDomains — analytics domains too, T-214) is enabled
// here, before navigation (startBlock, before waitSubscribe): the
// interception sees the page's requests from the first one. The stages
// are chromedp.Run/Do over s.Context().
func (s *Session) Run(ctx context.Context, task RenderTask) (RenderOutput, error) {
	if err := validateTaskURL(task.URL); err != nil {
		return RenderOutput{}, err
	}
	// The SSRF guard (T-219): an internal IP literal — alongside the
	// scheme check, before navigating to the page; the task class.
	if s.b.opts.BlockInternalAddrs {
		if err := validateTaskURLInternal(task.URL); err != nil {
			return RenderOutput{}, err
		}
	}
	tctx, stop := context.WithTimeout(s.ctx, s.b.opts.Timeout)
	defer stop()
	stopCaller := context.AfterFunc(ctx, stop)
	defer stopCaller()

	stopBlock, err := s.startBlock(tctx)
	if err != nil {
		return RenderOutput{}, s.wrap(err, ctx, tctx, task.URL)
	}
	defer stopBlock()

	var out RenderOutput
	// T-063: the tab event subscriptions — before navigation and before
	// Network.enable: a subscription starts buffering from the moment of
	// the call; a load event that happened during NavigateResponse (its
	// own load wait) is picked by waitLoop from the buffer instantly and
	// moves on to the network quiet.
	evs, err := waitSubscribe(tctx)
	var resp *network.Response
	if err == nil {
		// NavigateResponse — navigation with a load wait (the same wait
		// as Navigate) plus the main navigation's response: chromedp
		// tracks the loader/request chain through redirects and returns
		// the final document (T-064 — the headers for the spec 5.8
		// rules). The task scheme is http/https (validateTaskURL above),
		// the navigation response is a real network one; nil (a defensive
		// case) is handled below by the resp != nil check.
		resp, err = chromedp.Run(tctx, chromedp.NavigateResponse(task.URL))
	}
	if err == nil {
		// T-063: the load event (from the buffer — instantly) and the
		// network quiet (a window with no new requests) with the WaitPlan
		// caps.
		_, err = waitLoop(tctx, DefaultWaitPlan(s.b.opts.Timeout), evs, waitRealTimerFactory)
	}
	if err == nil {
		out.Title, err = chromedp.Run(tctx, chromedp.Title())
	}
	if err == nil {
		out.FinalURL, err = chromedp.Run(tctx, chromedp.Location())
	}
	if err == nil {
		// The DOM cap ≤ 16 MiB by configuration (content.max_store_bytes);
		// int is always sufficient here.
		err = newExtractCDP(int(s.b.opts.MaxDOMBytes)).extractResult(tctx, &out) // T-064: DOM + cookies
	}
	if err == nil && resp != nil {
		out.ResponseHeaders = extractResponseHeaders(resp.Headers) // T-064
	}
	if err != nil {
		return out, s.wrap(err, ctx, tctx, task.URL)
	}
	return out, nil
}

// Close closes the tab and destroys the isolated profile
// (Target.disposeBrowserContext): the page's cookies, cache and storages
// are thrown away — page state does not survive (spec §9). Waits for the
// actual closure (a chromedp guarantee); the session must not be reused
// after Close.
func (s *Session) Close() {
	if s.cancel != nil {
		s.cancel()
	}
}

// Context — the session tab's chromedp context. The extension point of
// T-062…T-064: the stages run as chromedp.Run/Do over this context.
func (s *Session) Context() context.Context { return s.ctx }

// wrap classifies a page stage error: a cancelled caller context —
// ClassTask (a retry on a new context is pointless), an expired page
// timeout — ClassTask if the browser connection is alive (the tarpit
// policy T-148: a heavy page), and ClassBrowser if the probe found a dead
// connection (T-218: a half-closed socket gives no transport errors — a
// page timeout on it looks like a tarpit, but this is an infrastructure
// failure: Manager swaps the generation and retries the task); everything
// else — classifyCtx by the browser/tab state.
func (s *Session) wrap(err error, caller, opCtx context.Context, url string) *Error {
	switch {
	case caller.Err() != nil:
		return &Error{Class: ClassTask, URL: url,
			Err: fmt.Errorf("task cancelled: %w: %w", caller.Err(), err)}
	case opCtx.Err() != nil:
		// The connection probe is cheap on a live browser (getTargets
		// answers within milliseconds even for a slow page); probeTimeout
		// is paid only by a dead socket — exactly where the browser
		// class is needed.
		if !s.b.probeConn() {
			return &Error{Class: ClassBrowser, URL: url,
				Err: fmt.Errorf("Chromium browser unavailable: page timeout %s: %w: %w",
					s.b.opts.Timeout, opCtx.Err(), err)}
		}
		return &Error{Class: ClassTask, URL: url,
			Err: fmt.Errorf("page timeout %s: %w: %w", s.b.opts.Timeout, opCtx.Err(), err)}
	default:
		return s.b.classifyCtx(err, url, s)
	}
}

// probe — the tab probe for error classification: chromedp does not cancel
// the tab context when the tab dies (the error texts are uninformative:
// "Session not found" and the like), so responsiveness is checked with a
// live request with a short timeout (tabProbeTimeout: the tab does not
// decide the generation's fate — the cap is shorter than the connection
// probe, T-226).
func (s *Session) probe() bool {
	if s.ctx == nil || s.ctx.Err() != nil {
		return false
	}
	if s.probeFn != nil {
		return s.probeFn()
	}
	ctx, stop := context.WithTimeout(s.ctx, tabProbeTimeout)
	defer stop()
	_, err := chromedp.Run(ctx, chromedp.Evaluate[int]("1"))
	return err == nil
}

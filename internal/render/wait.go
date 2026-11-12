package render

// The strategy of waiting for page readiness after navigation (spec 5.6,
// T-063): the load event with its own cap, then bounded waiting for the
// network quiet. The combined algorithm of Session.Run:
//
//	navigation (NavigateResponse, T-064 — with the load wait and the
//	response headers) → the load event from the subscription buffer (the
//	Load cap) → the Quiet window with no new requests (the Settle cap) →
//	result capture
//
// All waits are derived from the tab context (the T-061 lesson: the
// actions context must be derived from the tab context, chromedp looks up
// the session by a value in the context).
//
// The Load and Settle phase caps are soft: the cap expired → move on and
// capture the result as is (the "bounded wait" of spec 5.6 — bounded,
// not infinite). The hard limit is the overall page timeout render.timeout
// (the wrapper of the whole visit in Session.Run): its expiry is a task
// error (see the Session.Run doc comment), not a partial result.
//
// The default values are derived from the overall timeout
// (DefaultWaitPlan); the rationale is at the constants and fields of
// WaitPlan.

import (
	"context"
	"sync"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
)

const (
	// WaitQuietWindow — the default network quiet window: the page is
	// considered fully loaded when no NEW network requests have occurred
	// for this long (Network.requestWillBeSent). 2 s — the top of the
	// recommended range (1.5–2 s): an SPA XHR chain (request → response →
	// a spawned request) fits into ~1 s on a slow host, the window takes
	// a margin; the price is a one-time addition to every page after the
	// last request, which against the overall timeout of 25 s and 2–4
	// parallel contexts does not limit throughput. A constant, not a
	// share of the budget: the window characterizes the request rate of
	// the site, not the overall timeout; degenerate small timeouts have
	// the window compressed by DefaultWaitPlan (to at most half of the
	// phase cap), otherwise a window longer than the cap would never be
	// confirmed.
	WaitQuietWindow = 2 * time.Second

	// WaitLoadShare — the share of the overall timeout (1/N) for waiting
	// for the load event: one half, 12.5 s at 25 s. A typical load fits
	// into single seconds; if Page.loadEventFired has not arrived within
	// half the budget, the page is stuck on a subresource (load waits for
	// all of them), and further waiting is of little use:
	// DOMContentLoaded has usually happened already, the DOM is fit for
	// capture, and the rest of the budget suffices for the quiet window
	// and the result capture.
	WaitLoadShare = 2

	// WaitSettleShare — the share of the overall timeout (1/N) for the
	// quiet phase: one fifth, 5 s at 25 s. If during this time the network
	// has never been quiet, the page generates a constant background
	// (polling, analytics, ad bidders) and the window will not converge —
	// further waiting only eats the budget, the result is captured as is.
	WaitSettleShare = 5
)

// WaitPlan — the caps of the readiness wait phases (spec 5.6). Zero
// values are not supported: the plan is built by DefaultWaitPlan from the
// overall timeout.
type WaitPlan struct {
	// Load — the cap for waiting for the load event (Page.loadEventFired)
	// after navigation. For an SPA load fires before the rendering
	// completes (scripts fetch the data afterwards) — this phase fixes
	// the document's baseline readiness; the rendering is awaited by the
	// quiet phase.
	Load time.Duration
	// Quiet — the quiet window: no new network requests must occur for
	// this long. Every Network.requestWillBeSent restarts the window.
	Quiet time.Duration
	// Settle — the cap of the quiet phase, counted from its start (not
	// from the last request): a guard against pages with continuous
	// background traffic.
	Settle time.Duration
}

// DefaultWaitPlan derives the plan from the overall page timeout
// render.timeout: at 25 s — load 12.5 s, a quiet window of 2 s, a settle
// cap of 5 s. Shares, not constants: small timeouts (tests, tight
// environments) compress the phases proportionally, preserving the budget
// margin for the result capture (Load + Settle = 0.7 of the timeout). The
// quiet window is the WaitQuietWindow constant, but at most half of the
// phase cap: a window longer than the cap would never be confirmed —
// compressed budgets get a proportionally compressed window instead of a
// non-working one.
func DefaultWaitPlan(total time.Duration) WaitPlan {
	return WaitPlan{
		Load:   total / WaitLoadShare,
		Quiet:  min(WaitQuietWindow, total/(WaitSettleShare*2)),
		Settle: total / WaitSettleShare,
	}
}

// WaitOutcome — how the wait ended (diagnostics; both outcomes are
// success).
type WaitOutcome struct {
	// LoadFired — the load event arrived before the phase cap. false —
	// the cap expired (the page is stuck on a subresource), the wait
	// continued by quiet.
	LoadFired bool
	// Quiet — the quiet window was held. false — the phase was cut off
	// by the Settle cap (continuous background traffic); the result is
	// captured as is anyway.
	Quiet bool
}

// waitReqsBuffer — the buffer of the network events channel: the pump
// goroutine must not block the tab's event flow. An overflow only merges
// signals — for the quiet window that is the same reset, no meaning is
// lost.
const waitReqsBuffer = 64

// waitEvents — the tab events reduced to channels for the select in
// waitLoop.
type waitEvents struct {
	// load is closed once on Page.loadEventFired.
	load chan struct{}
	// reqs is signalled on every Network.requestWillBeSent (with possible
	// signal merging, see waitReqsBuffer).
	reqs chan struct{}
}

// waitSubscribe subscribes to the tab events (load, network requests) and
// enables the Network domain. Call BEFORE navigation: a chromedp.Events
// subscription starts buffering from the moment of the call, so events
// between the subscription and Page.navigate are not lost; Network.enable
// is required for Network.requestWillBeSent to arrive.
//
// The events are reduced to channels by pump goroutines: a typed iterator
// cannot be selected in a select. The pumps end together with ctx — the
// chromedp.Events subscriptions are bound to it (endingSession), an
// unread subscription does not hold the queue. load is closed once
// (sync.Once); reqs uses a non-blocking send: the quiet window is counted
// from "any new request", merged signals give the same window reset.
func waitSubscribe(ctx context.Context) (waitEvents, error) {
	load := chromedp.Events(ctx, page.LoadEventFired)
	reqs := chromedp.Events(ctx, network.RequestWillBeSent)
	enable := chromedp.Func(func(ctx context.Context, t *chromedp.Target) error {
		_, err := cdp.Call(ctx, t, network.Enable, network.EnableParams{})
		return err
	})
	if err := chromedp.Do(ctx, enable); err != nil {
		return waitEvents{}, err
	}
	evs := waitEvents{
		load: make(chan struct{}),
		reqs: make(chan struct{}, waitReqsBuffer),
	}
	var once sync.Once
	go func() {
		for _, err := range load {
			if err != nil {
				return // ctx cancelled or the tab died
			}
			once.Do(func() { close(evs.load) })
		}
	}()
	go func() {
		for _, err := range reqs {
			if err != nil {
				return
			}
			select {
			case evs.reqs <- struct{}{}:
			default:
			}
		}
	}()
	return evs, nil
}

// waitLoop — the two phases of waiting for readiness:
//
//  1. the load event (evs.load) with the cap plan.Load;
//  2. the quiet window plan.Quiet with no new requests (evs.reqs resets
//     the window) with the cap plan.Settle.
//
// Both caps are soft (on expiry we move on); the hard limit is ctx (the
// overall page timeout): its cancellation returns an error. Timers go
// through the newTimer factory: the real passage of time is replaced by
// fakes in unit tests (wait_test.go).
func waitLoop(ctx context.Context, plan WaitPlan, evs waitEvents, newTimer waitTimerFunc) (WaitOutcome, error) {
	var out WaitOutcome

	// Phase 1: the load event with a cap.
	loadCap := newTimer(plan.Load)
	select {
	case <-evs.load:
		out.LoadFired = true
		loadCap.Stop()
	case <-loadCap.C():
		// The cap is soft: load waits for all subresources, a stuck
		// subresource must not eat the budget — DOMContentLoaded has
		// usually happened already, we continue to the quiet window.
	case <-ctx.Done():
		return out, ctx.Err()
	}

	// The quiet window starts on entering the phase: requests issued
	// during the load phase do not belong to the new window — they are
	// drained.
drain:
	for {
		select {
		case <-evs.reqs:
		default:
			break drain
		}
	}

	// Phase 2: the quiet window with a cap.
	quiet := newTimer(plan.Quiet)
	settleCap := newTimer(plan.Settle)
	for {
		select {
		case <-evs.reqs:
			// A new request — the window restarts.
			quiet.Reset(plan.Quiet)
		case <-quiet.C():
			out.Quiet = true
			settleCap.Stop()
			return out, nil
		case <-settleCap.C():
			// The cap is soft: the network does not go quiet — the result
			// is captured as is.
			quiet.Stop()
			return out, nil
		case <-ctx.Done():
			return out, ctx.Err()
		}
	}
}

// waitTimer — a phase timer; an abstraction for the fake clocks in unit
// tests.
type waitTimer interface {
	C() <-chan time.Time
	Reset(d time.Duration)
	Stop()
}

// waitTimerFunc — the phase timer factory.
type waitTimerFunc func(d time.Duration) waitTimer

// waitRealTimer — a real timer. Go ≥ 1.23: the Timer channel is
// unbuffered, Reset and Stop drain an undelivered value themselves — safe
// in the select loop above.
type waitRealTimer struct{ timer *time.Timer }

// waitRealTimerFactory — the real timer factory for Session.Run.
func waitRealTimerFactory(d time.Duration) waitTimer {
	return &waitRealTimer{timer: time.NewTimer(d)}
}

func (t *waitRealTimer) C() <-chan time.Time   { return t.timer.C }
func (t *waitRealTimer) Reset(d time.Duration) { t.timer.Reset(d) }
func (t *waitRealTimer) Stop()                 { t.timer.Stop() }

package render

// A hermetic Chromium replacement for the unit tests of tab subscriptions and
// commands: an in-memory chromedp.Transport. No live browser is needed — commands and
// events travel over channels, the transport's auto-replies service the whole chromedp
// bootstrap
// (SetDiscoverTargets → Target.targetCreated → attachToTarget → the
// Runtime/Page/Network/… enable chain). The trick — exported chromedp pieces:
// Transport (the CDP message read/write interface), NewBrowserTransport and
// NewAllocatorContext; the remote connection (remote.Conn) is substituted
// with the transport fake, just as other tests of the package substitute struct fields
// (probeFn, eventsOf in blockPump, fakeExtractCDP).
//
// What the fake tells the tests:
//   - commands — the cmds journal (method, session, params) and per-method counters;
//   - command errors — setError per method (after the bootstrap: e.g. a
//     Network.enable refusal for waitSubscribe);
//   - events — fireSession/fireBrowser: delivered to the chromedp.Events
//     subscriptions exactly like the events of a real tab.

import (
	"context"
	"sync"
	"testing"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/target"
	"github.com/chromedp/chromedp"
)

// fakeCDPSessionID/fakeCDPTargetID — the fixed identifiers of the fake's
// tab: the attachToTarget reply and the sessionId of events must match.
const (
	fakeCDPSessionID = "fake-session"
	fakeCDPTargetID  = "fake-target"
)

// fakeCDPCmd — a command that went through the transport: the identifier, method,
// session and params (json text) as-is.
type fakeCDPCmd struct {
	ID        int64
	Method    string
	SessionID string
	Params    string
}

// fakeCDPReplies — the results of the chromedp bootstrap commands: attachToTarget
// (the session identifier), the "self" Runtime.evaluate (not a Worker — the
// isWorker branch), getTargets (an empty list — the waitTargetGone on closing
// ends immediately). Everything else gets an empty result {}.
var fakeCDPReplies = map[string]string{
	"Target.attachToTarget": `{"sessionId":"` + fakeCDPSessionID + `"}`,
	"Runtime.evaluate":      `{"result":{"type":"object","className":"Object"}}`,
	"Target.getTargets":     `{"targetInfos":[]}`,
}

// fakeCDP — an in-memory CDP transport: Write accepts commands and replies to
// them (an auto-reply or a configured error), Read hands out the replies and
// events; the inbox is shared, the delivery order is FIFO.
type fakeCDP struct {
	mu     sync.Mutex
	cmds   []fakeCDPCmd
	errors map[string]*cdproto.Error

	inbox chan *cdproto.Message
}

func newFakeCDP() *fakeCDP {
	return &fakeCDP{
		errors: make(map[string]*cdproto.Error),
		inbox:  make(chan *cdproto.Message, 1024),
	}
}

// Write satisfies chromedp.Transport: writing a command. The reply is synthesized
// synchronously (no real browser): the result from fakeCDPReplies or {},
// or an error from errors. The push into the inbox respects the transport's ctx: if
// the reader has already exited (the browser lifecycle is killed), the chromedp
// write-loop does not hang on a full buffer. On the browser-level setDiscoverTargets (the
// first step of the tab's bootstrap) a page is additionally announced — chromedp
// waits exactly for this event.
func (f *fakeCDP) Write(ctx context.Context, m *cdproto.Message) error {
	f.mu.Lock()
	f.cmds = append(f.cmds, fakeCDPCmd{
		ID:        m.ID,
		Method:    string(m.Method),
		SessionID: string(m.SessionID),
		Params:    string(m.Params),
	})
	cmdErr := f.errors[string(m.Method)]
	f.mu.Unlock()

	reply := &cdproto.Message{ID: m.ID, SessionID: m.SessionID}
	if cmdErr != nil {
		reply.Error = cmdErr
	} else {
		res, ok := fakeCDPReplies[string(m.Method)]
		if !ok {
			res = "{}"
		}
		reply.Result = jsonv2.Value(res)
	}
	select {
	case f.inbox <- reply:
	case <-ctx.Done():
		return ctx.Err()
	}

	if m.SessionID == "" && string(m.Method) == "Target.setDiscoverTargets" {
		f.fireBrowser("Target.targetCreated",
			`{"targetInfo":{"targetId":"`+fakeCDPTargetID+
				`","type":"page","url":"about:blank","attached":false}}`)
	}
	return nil
}

// Read satisfies chromedp.Transport: waiting for a message (a reply/event)
// until the browser context is canceled.
func (f *fakeCDP) Read(ctx context.Context, m *cdproto.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case msg := <-f.inbox:
		*m = *msg
		return nil
	}
}

// Close satisfies chromedp.Transport.
func (f *fakeCDP) Close() error { return nil }

// fireSession — a tab session event: delivered to the chromedp.Events
// subscriptions (the params are a valid json of the target event).
func (f *fakeCDP) fireSession(method, params string) {
	f.inbox <- &cdproto.Message{
		Method:    cdproto.MethodType(method),
		SessionID: target.SessionID(fakeCDPSessionID),
		Params:    jsonv2.Value(params),
	}
}

// fireBrowser — a browser-level event (no session).
func (f *fakeCDP) fireBrowser(method, params string) {
	f.inbox <- &cdproto.Message{
		Method: cdproto.MethodType(method),
		Params: jsonv2.Value(params),
	}
}

// setError — reply with an error to the method (until the next reset).
func (f *fakeCDP) setError(method string, e *cdproto.Error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errors[method] = e
}

// callsOf — a copy of the command journal.
func (f *fakeCDP) callsOf() []fakeCDPCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeCDPCmd(nil), f.cmds...)
}

// countOf — the number of commands with the given method.
func (f *fakeCDP) countOf(method string) int {
	n := 0
	for _, c := range f.callsOf() {
		if c.Method == method {
			n++
		}
	}
	return n
}

// fakeCDPAllocator — an allocator on top of fakeCDP: the browser lives as long as
// the allocator context lives (like remote.Allocator, but the transport is in-memory).
type fakeCDPAllocator struct{ tr *fakeCDP }

func (a *fakeCDPAllocator) Allocate(ctx context.Context, opts ...chromedp.BrowserOption) (*chromedp.Browser, error) {
	wctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-ctx.Done()
		cancel()
	}()
	return chromedp.NewBrowserTransport(wctx, a.tr, opts...)
}

func (a *fakeCDPAllocator) Wait() {}

// newFakeCDPContext — a chromedp context with a raised tab on top of fakeCDP:
// chromedp.Do performs the full bootstrap (browser → targetCreated → attach →
// domains), all its commands are serviced by the auto-replies. The context
// is released by the test's Cleanup.
func newFakeCDPContext(t *testing.T) (context.Context, *fakeCDP) {
	t.Helper()
	tr := newFakeCDP()
	allocCtx, cancelAlloc := chromedp.NewAllocatorContext(context.Background(), &fakeCDPAllocator{tr: tr})
	tctx, cancel := chromedp.NewContext(allocCtx)
	t.Cleanup(func() {
		cancel()
		cancelAlloc()
	})
	if err := chromedp.Do(tctx); err != nil {
		t.Fatalf("bootstrapping the chromedp context over the fake: %v", err)
	}
	return tctx, tr
}

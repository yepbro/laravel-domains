package render

// A reproduction of the Nov 2 incident (T-218): after an external
// Chromium restart (systemctl restart crawler-chromium, the T-208
// timer) the workers accumulated websockets to :9222 (284 over ~6 h),
// Target.createBrowserContext hung until SessionStartTimeout ("the
// context creation timeout 15s" × 11,520 WARN "the render context
// died" over 6 h), rendering degraded for ~5.5 h and stood still by
// 07:25; the fresh Chromium was healthy — the workers' state was
// broken, cured by a worker restart.
//
// The mechanism (per the chromedp v0.21.0 code): the WebSocket read
// (remote.Conn.Read) reacts neither to the context nor to timeouts —
// the library learns of the connection's death only from socket
// errors. A half-closed/hung connection (no FIN/RST: NAT/conntrack,
// the browser hung on shutdown) looks alive forever: Browser.Alive() =
// true, Manager does not swap the generation, every task hangs in the
// context creation and dies by the timeout (the context class → a
// useless retry on a new context of the same dead socket). The
// WebSocket write (remote.Conn.Write) is also without a deadline: a
// chromedp pump hangs in writing until the TCP retransmissions run
// out, the upstream allocator closes the socket only after the pumps
// — the old connections are never released.
//
// Here: switchableCDP — a controllable transport (a "black hole":
// reads hang, writes are accepted silently); both halves of the fix
// are checked — the dead connection detector (probeConn: ClassBrowser
// → a generation swap by the manager, recovery without a worker
// restart) and the allocator's forced socket closure (connections do
// not accumulate).

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/chromedp"

	"crawler/internal/config"
)

// switchableCDP — a CDP transport with a switchable liveness: like
// fakeCDP it answers the chromedp bootstrap commands (unique
// sessionId/targetId — the session tabs do not conflict with the
// browser's service tab); by kill() it becomes a "black hole" — Write
// accepts commands silently (like a write into a half-closed socket's
// buffer), Read hangs until Close() (remote.Conn.Read ignores the
// context — the fidelity matters: the probe and the forced closure are
// checked exactly against such behavior).
type switchableCDP struct {
	mu     sync.Mutex
	alive  bool
	closed bool
	seq    int
	hang   map[string]bool
	fail   map[string]bool
	inbox  chan *cdproto.Message
	cmds   []fakeCDPCmd
	closeC chan struct{}
}

func newSwitchableCDP() *switchableCDP {
	return &switchableCDP{
		alive:  true,
		hang:   make(map[string]bool),
		fail:   make(map[string]bool),
		inbox:  make(chan *cdproto.Message, 1024),
		closeC: make(chan struct{}),
	}
}

// Write satisfies chromedp.Transport. The reply is synthesized
// synchronously; in the "black hole" mode and for the methods from
// hang the command is accepted silently — there will never be a reply
// (an imitation of a dead browser / a hung command with a live
// connection).
func (f *switchableCDP) Write(ctx context.Context, m *cdproto.Message) error {
	f.mu.Lock()
	f.cmds = append(f.cmds, fakeCDPCmd{
		ID:        m.ID,
		Method:    string(m.Method),
		SessionID: string(m.SessionID),
		Params:    string(m.Params),
	})
	method := string(m.Method)
	dead := !f.alive || f.hang[method]
	cmdErr := f.fail[method]
	f.seq++
	seq := f.seq
	f.mu.Unlock()
	if dead {
		return nil
	}

	reply := &cdproto.Message{ID: m.ID, SessionID: m.SessionID}
	switch {
	case cmdErr:
		// A protocol error reply with a live connection (the browser
		// answered — a transport death is not confirmed by it).
		reply.Error = &cdproto.Error{Code: -32000, Message: "the command is broken"}
	case method == "Target.attachToTarget":
		reply.Result = jsonv2.Value(fmt.Sprintf(`{"sessionId":"sw-session-%d"}`, seq))
	case method == "Target.createTarget":
		reply.Result = jsonv2.Value(fmt.Sprintf(`{"targetId":"sw-target-%d"}`, seq))
	case method == "Target.createBrowserContext":
		reply.Result = jsonv2.Value(fmt.Sprintf(`{"browserContextId":"sw-bc-%d"}`, seq))
	case method == "Runtime.evaluate":
		reply.Result = jsonv2.Value(`{"result":{"type":"object","className":"Object"}}`)
	case method == "Target.getTargets":
		reply.Result = jsonv2.Value(`{"targetInfos":[]}`)
	default:
		reply.Result = jsonv2.Value("{}")
	}
	select {
	case f.inbox <- reply:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// Read satisfies chromedp.Transport: waits for a message until the
// transport is closed. The context is NOT observed — as with
// remote.Conn.Read; without this one cannot prove that the forced
// Close is really needed.
func (f *switchableCDP) Read(_ context.Context, m *cdproto.Message) error {
	select {
	case <-f.closeC:
		return errors.New("transport closed")
	case msg := <-f.inbox:
		*m = *msg
		return nil
	}
}

// Close satisfies chromedp.Transport: unblocks a hung read (an analog
// of closing the socket — "use of closed network connection").
func (f *switchableCDP) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	close(f.closeC)
	return nil
}

// kill — the "browser death" in front of the worker's eyes: the
// connection is half-closed, the answers (including the accumulated
// ones) never arrive.
func (f *switchableCDP) kill() {
	f.mu.Lock()
	f.alive = false
	f.mu.Unlock()
	for {
		select {
		case <-f.inbox:
		default:
			return
		}
	}
}

// hangMethod — do not answer the method (a hung command with a live
// connection: e.g. createBrowserContext on an overloaded browser).
func (f *switchableCDP) hangMethod(method string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hang[method] = true
}

// failMethod — answer the method with a protocol error while the
// connection is live (the browser sent an error reply — the transport
// works).
func (f *switchableCDP) failMethod(method string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail[method] = true
}

// unhangMethod — remove the method hang (answers again).
func (f *switchableCDP) unhangMethod(method string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.hang, method)
}

func (f *switchableCDP) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// countOf — the number of commands with the given method.
func (f *switchableCDP) countOf(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.cmds {
		if c.Method == method {
			n++
		}
	}
	return n
}

// dialRecorder — a remoteDial replacement: every connection is a new
// switchableCDP; the transport log = the log of the worker's
// connections to :9222.
type dialRecorder struct {
	mu         sync.Mutex
	transports []*switchableCDP
}

func (r *dialRecorder) dial(_ context.Context, _ string, _ time.Duration) (chromedp.Transport, error) {
	tr := newSwitchableCDP()
	r.mu.Lock()
	r.transports = append(r.transports, tr)
	r.mu.Unlock()
	return tr, nil
}

func (r *dialRecorder) list() []*switchableCDP {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*switchableCDP(nil), r.transports...)
}

// overrideRemoteDial — replacing the connection seam for the test's
// duration.
func overrideRemoteDial(t *testing.T, f func(ctx context.Context, wsURL string, timeout time.Duration) (chromedp.Transport, error)) {
	t.Helper()
	old := remoteDial
	remoteDial = f
	t.Cleanup(func() { remoteDial = old })
}

// waitForFor — like waitFor (block_test.go), with a configurable
// deadline: the background connection closures take up to ~5.5 s (the
// internal chromedp timeouts: closeTargetTimeout 5 s + waitTargetGone
// 0.5 s).
func waitForFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("the condition was not reached in %s", d)
}

// bootSession — a pool session with a real context creation
// (NewSession: the connection probe + createBrowserContext + the tab)
// and an instant task: the incident reproduction is the mechanics of
// the break in the context creation, not in the page rendering.
type bootSession struct {
	real renderSession
}

func (b bootSession) Run(ctx context.Context, task RenderTask) (RenderOutput, error) {
	return RenderOutput{FinalURL: task.URL, Title: "ok"}, nil
}

func (b bootSession) Close() { b.real.Close() }

// remoteOpts — the remote mode parameters for the reconnect tests: an
// address with a ready /devtools/browser/ path (no /json/version
// request — the transport is replaced by the remoteDial seam), short
// timeouts.
func remoteOpts() Options {
	return Options{
		RemoteURL:           "ws://127.0.0.1:9222/devtools/browser/test",
		Contexts:            1,
		Timeout:             2 * time.Second,
		RestartEveryPages:   1000,
		DialTimeout:         time.Second,
		SessionStartTimeout: 300 * time.Millisecond,
	}
}

// testProbeTimeout — the test budget of the connection probe (T-226):
// the probe series in the tests runs in real time; the full production
// probeTimeout (5 s × ~10 probes) would push the batch out of the
// mutation run budget (MUTATE_TIMEOUT=60 s). The short budget does not
// change the semantics under test: a "black hole"/a hung method does
// not answer within 100 ms either.
const testProbeTimeout = 100 * time.Millisecond

// switchableBrowser — a browser over the controllable transport: the
// full chromedp bootstrap (StartBrowser does not fit — it builds the
// browser from Options itself); here the context is already raised by
// the caller for the fine-grained scenarios (the probe, the context
// creation timeout). The release — the test's Cleanup.
func switchableBrowser(t *testing.T, tr *switchableCDP, opts Options) *Browser {
	t.Helper()
	allocCtx, cancelAlloc := chromedp.NewAllocatorContext(context.Background(),
		newRemoteAllocator(opts))
	bctx, cancelBrowser := chromedp.NewContext(allocCtx)
	t.Cleanup(func() {
		cancelBrowser()
		cancelAlloc()
	})
	if err := initBrowserSession(bctx, opts); err != nil {
		t.Fatalf("bootstrapping the browser over the fake: %v", err)
	}
	return &Browser{opts: opts, ctx: bctx, cancel: cancelBrowser, cancelAlloc: cancelAlloc,
		probeTimeout: testProbeTimeout}
}

// TestManagerRecoversAfterBrowserDeath — the T-218 task sink: the
// "browser" death (a half-closed connection) → the context attempts do
// not accumulate (the browser class, not context retries on a dead
// socket) → the manager swaps the generation itself → after the
// browser's "resurrection" (a new connection) the rendering continues;
// the recovery is ≤ a minute; the connections do not grow: the old
// socket is closed forcibly, the live one is exactly one.
func TestManagerRecoversAfterBrowserDeath(t *testing.T) {
	rec := &dialRecorder{}
	overrideRemoteDial(t, rec.dial)

	m := NewManager(context.Background(), remoteOpts())
	// The start — a real StartBrowser (the full bootstrap over the
	// fake) but with the test probe budget: the scenario waits for a
	// failed probe twice; the production 5 s × 2 is incompatible with
	// the run budget (MUTATE_TIMEOUT).
	realBrowser := m.newBrowser
	m.newBrowser = func(ctx context.Context, opts Options) (*Browser, error) {
		b, err := realBrowser(ctx, opts)
		if b != nil {
			b.probeTimeout = testProbeTimeout
		}
		return b, err
	}
	// The pool with a real session creation (the probe + the context)
	// and an instant task — see bootSession.
	m.newPool = func(b *Browser) *Pool {
		p := NewPool(b)
		inner := p.newSession
		p.newSession = func(ctx context.Context) (renderSession, error) {
			s, err := inner(ctx)
			if err != nil {
				return nil, err
			}
			return bootSession{real: s}, nil
		}
		return p
	}
	t.Cleanup(m.Close)

	// A live browser: the task passes, one connection.
	if _, err := m.Run(context.Background(), RenderTask{URL: "https://a.example/"}); err != nil {
		t.Fatalf("Run on a live browser: %v", err)
	}
	if conns := rec.list(); len(conns) != 1 {
		t.Fatalf("connections %d, want 1", len(conns))
	}

	// The browser death: a host recycle, the connection is half-closed.
	started := time.Now()
	dead := rec.list()[0]
	createdBefore := dead.countOf("Target.createBrowserContext") // the live render created a context

	dead.kill()

	// The worker recovers by itself — without a service restart: the
	// connection probe classifies the death as browser, the manager
	// swaps the generation, the task passes on a new connection.
	if _, err := m.Run(context.Background(), RenderTask{URL: "https://a.example/"}); err != nil {
		t.Fatalf("the worker did not recover after the browser's death: %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Minute {
		t.Errorf("the recovery took %s, want ≤ a minute", elapsed)
	}

	conns := rec.list()
	if len(conns) != 2 {
		t.Fatalf("connections %d, want 2 (a reconnect without accumulation)", len(conns))
	}
	// After the connection's death no contexts are created on it at
	// all (before the fix — a hung createBrowserContext on every
	// attempt, the Nov 2 incident: ×11,520 WARN over 6 h).
	if n := conns[0].countOf("Target.createBrowserContext"); n != createdBefore {
		t.Errorf("createBrowserContext on a dead connection +%d after the death, want 0", n-createdBefore)
	}
	// The old socket is closed forcibly (in the background — after the
	// regular chromedp procedure, within its timeouts), the new live
	// one — exactly one.
	waitForFor(t, 10*time.Second, conns[0].isClosed)
	if conns[1].isClosed() {
		t.Error("the live connection closed")
	}
	// The generation is stable: further tasks do not spawn connections.
	for range 3 {
		if _, err := m.Run(context.Background(), RenderTask{URL: "https://a.example/"}); err != nil {
			t.Fatalf("Run after the recovery: %v", err)
		}
	}
	if got := len(rec.list()); got != 2 {
		t.Errorf("connections %d after a series of tasks, want 2", got)
	}
}

// TestRemoteAllocatorClosesConnectionOnCancel — the forced socket
// closure on the lifecycle context cancellation: a "black hole" cannot
// hold the connection (the chromedp pumps would hang in
// writing/reading — the upstream allocator would wait for them
// unboundedly).
func TestRemoteAllocatorClosesConnectionOnCancel(t *testing.T) {
	tr := newSwitchableCDP()
	overrideRemoteDial(t, func(context.Context, string, time.Duration) (chromedp.Transport, error) {
		return tr, nil
	})

	opts := remoteOpts()
	a := newRemoteAllocator(opts)
	allocCtx, cancelAlloc := chromedp.NewAllocatorContext(context.Background(), a)
	bctx, cancelBrowser := chromedp.NewContext(allocCtx)
	if err := initBrowserSession(bctx, opts); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	tr.kill()
	// The lifecycle context cancellation — the allocator contract
	// ("canceling the provided context frees all resources"): the
	// socket must close forcibly, without waiting for the hung chromedp
	// pumps.
	cancelAlloc()

	waitForFor(t, 10*time.Second, tr.isClosed)
	a.Wait() // the background connection closure has completed
	cancelBrowser()
}

// TestRemoteAllocatorEngineURLMode — the engine wiring of the allocator
// (the engines render spec §4.2 items 1/3, FR-4): the wsURLMode address
// mode by engine. The engines' behavior is confirmed on the pinned
// stack images (moli v1.1.21: the browser WS only on the
// /devtools/browser/<uuid> path, new on every start, the root — 404;
// lightpanda 1.0.2: the browser WS at the root path). lightpanda
// connects to the configuration address as is; moli takes the path
// from /json/version but the authority from the configuration address
// (the advertisement is built from the bind address); the control:
// chromium uses the webSocketDebuggerUrl from /json/version as is (the
// behavior before stage 35).
func TestRemoteAllocatorEngineURLMode(t *testing.T) {
	// lightpanda: a hostname without the /devtools/browser/ path —
	// chromium in such a configuration must resolve and refine the
	// address; for lightpanda any attempt is a regression (the resolve
	// is replaced by a failure, there is no /json/version server): the
	// connection goes only to the address as is.
	t.Run(config.EngineLightpanda, func(t *testing.T) {
		tr := newSwitchableCDP()
		var mu sync.Mutex
		dialed := ""
		overrideRemoteDial(t, func(_ context.Context, wsURL string, _ time.Duration) (chromedp.Transport, error) {
			mu.Lock()
			dialed = wsURL
			mu.Unlock()
			return tr, nil
		})
		overrideRemoteResolve(t, func(_ context.Context, host string) ([]net.IPAddr, error) {
			t.Errorf("resolving %s with the lightpanda engine: the address must not be refined", host)
			return nil, errors.New("a resolution is not expected")
		})

		opts := remoteOpts()
		opts.Engine = config.EngineLightpanda
		opts.RemoteURL = "ws://engine.example:9222"
		b, err := StartBrowser(context.Background(), opts)
		if err != nil {
			t.Fatalf("connecting to lightpanda by the address as is: %v", err)
		}
		b.Close()
		waitForFor(t, 10*time.Second, tr.isClosed)

		mu.Lock()
		got := dialed
		mu.Unlock()
		if got != opts.RemoteURL {
			t.Errorf("lightpanda: connecting to %q, want the configuration address %q as is", got, opts.RemoteURL)
		}
	})

	// moli: the browser session path — from /json/version, the
	// authority — from the configuration address: the advertisement
	// ws://0.0.0.0:9222 (the bind 0.0.0.0, moli has no advertise-host
	// flag) is unreachable; the connection goes to the operator's
	// address with the advertised path.
	t.Run(config.EngineMoli, func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/json/version" {
				http.NotFound(w, r)
				return
			}
			_, _ = fmt.Fprint(w, `{"Browser":"moli","webSocketDebuggerUrl":"ws://0.0.0.0:9222/devtools/browser/moli-uuid"}`)
		}))
		defer srv.Close()

		tr := newSwitchableCDP()
		var mu sync.Mutex
		dialed := ""
		overrideRemoteDial(t, func(_ context.Context, wsURL string, _ time.Duration) (chromedp.Transport, error) {
			mu.Lock()
			dialed = wsURL
			mu.Unlock()
			return tr, nil
		})

		opts := remoteOpts()
		opts.Engine = config.EngineMoli
		opts.RemoteURL = strings.Replace(srv.URL, "http://", "ws://", 1) // the ws scheme: the alt-engine validation requires ws/wss
		b, err := StartBrowser(context.Background(), opts)
		if err != nil {
			t.Fatalf("connecting to moli with the path from /json/version: %v", err)
		}
		b.Close()
		waitForFor(t, 10*time.Second, tr.isClosed)

		want := strings.Replace(srv.URL, "http://", "ws://", 1) + "/devtools/browser/moli-uuid"
		mu.Lock()
		got := dialed
		mu.Unlock()
		if got != want {
			t.Errorf("moli: connecting to %q, want the /json/version path with the configuration authority %q", got, want)
		}
	})

	// The control (chromium): the webSocketDebuggerUrl from
	// /json/version is used as is, the authority is not replaced — the
	// behavior before stage 35.
	t.Run(config.EngineChromium, func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/json/version" {
				http.NotFound(w, r)
				return
			}
			_, _ = fmt.Fprint(w, `{"Browser":"x","webSocketDebuggerUrl":"ws://127.0.0.1:9222/devtools/browser/adv"}`)
		}))
		defer srv.Close()

		tr := newSwitchableCDP()
		var mu sync.Mutex
		dialed := ""
		overrideRemoteDial(t, func(_ context.Context, wsURL string, _ time.Duration) (chromedp.Transport, error) {
			mu.Lock()
			dialed = wsURL
			mu.Unlock()
			return tr, nil
		})

		opts := remoteOpts()
		opts.Engine = config.EngineChromium
		opts.RemoteURL = srv.URL // http://IP:port without /devtools/browser/ — forces the refinement
		b, err := StartBrowser(context.Background(), opts)
		if err != nil {
			t.Fatalf("connecting to chromium with the address refined: %v", err)
		}
		b.Close()
		waitForFor(t, 10*time.Second, tr.isClosed)

		mu.Lock()
		got := dialed
		mu.Unlock()
		const advertised = "ws://127.0.0.1:9222/devtools/browser/adv"
		if got != advertised {
			t.Errorf("chromium: connecting to %q, want the webSocketDebuggerUrl from /json/version %q", got, advertised)
		}
	})
}

// TestBrowserProbeConnDeadConnection — the connection probe against the
// T-218 "black hole" (a half-closed socket: the transport is "alive",
// no answers arrive): a live one answers; after the death the first
// failure does not kill the generation yet — a single timeout does not
// distinguish the hole from an overloaded Chromium (T-226); the second
// in a row kills it stickily, and Alive sees it even with a (by
// chromedp's opinion) live context — the state of the Nov 2 incident.
func TestBrowserProbeConnDeadConnection(t *testing.T) {
	tr := newSwitchableCDP()
	overrideRemoteDial(t, func(context.Context, string, time.Duration) (chromedp.Transport, error) {
		return tr, nil
	})
	opts := remoteOpts()
	b := switchableBrowser(t, tr, opts)

	if !b.Alive() {
		t.Fatal("a live connection: Alive")
	}
	if !b.probeConn() {
		t.Fatal("the live connection probe failed")
	}

	tr.kill()
	// The first failure: the browser class (the probe failed), but the
	// generation is alive.
	if b.probeConn() {
		t.Fatal("the dead connection probe passed")
	}
	if !b.Alive() {
		t.Fatal(`a single "black hole" timeout killed the generation (T-226: a series is required)`)
	}
	// The second in a row — lost stickily; Manager will see the
	// generation swap.
	if b.probeConn() {
		t.Fatal("the dead connection probe passed")
	}
	if b.Alive() {
		t.Fatal("a dead connection: Alive must be false (for the Manager — a generation swap)")
	}
	// Sticking: a repeated probe does not wait for probeTimeout and
	// does not "revive".
	started := time.Now()
	if b.probeConn() {
		t.Fatal("a repeated dead connection probe passed")
	}
	if elapsed := time.Since(started); elapsed > probeTimeout/2 {
		t.Errorf("a repeated probe on a sticky lost took %s, want instant", elapsed)
	}
}

// TestProbeConnTimeoutTwoFailures — the T-226 sink: a single probe
// timeout does not kill the generation. The scenario — an "overloaded
// Chromium": the connection is alive (the transport answers other
// commands), Target.getTargets does not make it within probeTimeout.
// The other side of the T-218 countermeasure: the probe runs before
// every task, and a one-off lag of a busy browser must not cost the
// generation (a restart, breaking others' tasks) — lost is set from
// the second failure in a row, a success between failures resets the
// counter.
func TestProbeConnTimeoutTwoFailures(t *testing.T) {
	tr := newSwitchableCDP()
	overrideRemoteDial(t, func(context.Context, string, time.Duration) (chromedp.Transport, error) {
		return tr, nil
	})
	opts := remoteOpts()
	b := switchableBrowser(t, tr, opts)

	if !b.probeConn() {
		t.Fatal("the live connection probe failed")
	}

	// A single timeout: a failure without the generation's death.
	tr.hangMethod("Target.getTargets")
	started := time.Now()
	if b.probeConn() {
		t.Fatal("the hanging getTargets probe passed")
	}
	// The budget substitution pin: the probe must not wait for the
	// production probeTimeout (without substituting the field this
	// check would fail at ~5 s).
	if elapsed := time.Since(started); elapsed >= probeTimeout {
		t.Errorf("the probe waited the full production probeTimeout (%s) — the test budget is not replaced", elapsed)
	}
	if !b.Alive() {
		t.Fatal("a single probe timeout killed the generation (T-226: a false death on a loaded worker)")
	}

	// A success between failures resets the counter: the next single
	// timeout is still not a death.
	tr.unhangMethod("Target.getTargets")
	if !b.probeConn() {
		t.Fatal("the live connection probe after the reset failed")
	}
	tr.hangMethod("Target.getTargets")
	if b.probeConn() {
		t.Fatal("the hanging getTargets probe passed")
	}
	if !b.Alive() {
		t.Fatal("a timeout right after a success (the counter reset) killed the generation")
	}

	// The second failure in a row: the generation is killed stickily.
	if b.probeConn() {
		t.Fatal("the hanging getTargets probe passed")
	}
	if b.Alive() {
		t.Fatal("a double probe timeout did not kill the generation")
	}
	// Sticking: a repeated probe does not wait for probeTimeout and
	// does not "revive".
	started = time.Now()
	if b.probeConn() {
		t.Fatal("a repeated probe of the lost browser passed")
	}
	if elapsed := time.Since(started); elapsed > probeTimeout/2 {
		t.Errorf("a repeated probe on a sticky lost took %s, want instant", elapsed)
	}
}

// TestProbeConnTransportDeathInstantLost — a proven transport death (a
// closed socket: the read fails, chromedp closes LostConnection, the
// call errors wrap context.Canceled) is not weakened by the T-226
// countermeasure: lost is set by the very first failure, without a
// series, instantly.
func TestProbeConnTransportDeathInstantLost(t *testing.T) {
	tr := newSwitchableCDP()
	overrideRemoteDial(t, func(context.Context, string, time.Duration) (chromedp.Transport, error) {
		return tr, nil
	})
	opts := remoteOpts()
	b := switchableBrowser(t, tr, opts)

	// Closing the socket (an analog of FIN/RST — "use of closed network
	// connection"): chromedp learns of it from the read and closes
	// LostConnection.
	_ = tr.Close()
	c := chromedp.FromContext(b.ctx)
	waitForFor(t, 5*time.Second, func() bool {
		select {
		case <-c.Browser.LostConnection:
			return true
		default:
			return false
		}
	})

	started := time.Now()
	if b.probeConn() {
		t.Fatal("the closed connection probe passed")
	}
	if elapsed := time.Since(started); elapsed > probeTimeout/2 {
		t.Errorf("the closed connection probe waited %s — a transport death is detected instantly, without probeTimeout", elapsed)
	}
	if !b.lost.Load() {
		t.Fatal("the transport death did not stick from the first failure (the T-218 detection weakened)")
	}
	if b.Alive() {
		t.Fatal("a lost connection: Alive must be false")
	}
}

// TestProbeConnProtocolErrorSeries — a protocol error reply to
// getTargets with a live connection (the browser answered — the
// transport works) belongs to the "unexplained" failures of T-226, not
// to a proven death: a single error reply does not kill the generation
// (the class of false deaths), a series of two does.
func TestProbeConnProtocolErrorSeries(t *testing.T) {
	tr := newSwitchableCDP()
	overrideRemoteDial(t, func(context.Context, string, time.Duration) (chromedp.Transport, error) {
		return tr, nil
	})
	opts := remoteOpts()
	b := switchableBrowser(t, tr, opts)

	tr.failMethod("Target.getTargets")
	if b.probeConn() {
		t.Fatal("the probe with an error response passed")
	}
	if !b.Alive() {
		t.Fatal("a single protocol error response killed the generation (T-226: the connection is alive — the browser answered)")
	}
	if b.probeConn() {
		t.Fatal("the probe with an error response passed")
	}
	if b.Alive() {
		t.Fatal("a series of error responses did not kill the generation")
	}
}

// TestSessionStartTimeoutDeadConnection — the classification of the
// context creation timeout: a dead connection — the browser class
// (Manager swaps the generation), a hung command with a live
// connection — the context class (the existing retry on a new
// context).
func TestSessionStartTimeoutDeadConnection(t *testing.T) {
	tr := newSwitchableCDP()
	overrideRemoteDial(t, func(context.Context, string, time.Duration) (chromedp.Transport, error) {
		return tr, nil
	})
	opts := remoteOpts()
	b := switchableBrowser(t, tr, opts)

	// A dead connection: createBrowserContext hangs, the probe does not
	// answer.
	tr.kill()
	sctx, scancel := chromedp.NewContext(b.ctx, chromedp.WithNewBrowserContext())
	defer scancel()
	s := &Session{b: b, ctx: sctx, cancel: scancel}
	err := s.start(context.Background())
	var e *Error
	if !errors.As(err, &e) || e.Class != ClassBrowser {
		t.Fatalf("a context creation timeout on a dead connection: the class %v (%v), want browser", ClassOf(err), err)
	}
}

// TestSessionStartTimeoutSlowBrowser — a hung createBrowserContext with
// a live connection (the probe passes) stays the context class: a
// retry on a new context, as before T-218.
func TestSessionStartTimeoutSlowBrowser(t *testing.T) {
	tr := newSwitchableCDP()
	overrideRemoteDial(t, func(context.Context, string, time.Duration) (chromedp.Transport, error) {
		return tr, nil
	})
	opts := remoteOpts()
	b := switchableBrowser(t, tr, opts)

	tr.hangMethod("Target.createBrowserContext")
	sctx, scancel := chromedp.NewContext(b.ctx, chromedp.WithNewBrowserContext())
	defer scancel()
	s := &Session{b: b, ctx: sctx, cancel: scancel}
	err := s.start(context.Background())
	var e *Error
	if !errors.As(err, &e) || e.Class != ClassContext {
		t.Fatalf("a context creation timeout with a live connection: the class %v (%v), want context", ClassOf(err), err)
	}
}

// TestStartBrowserBootstrapTimeout — the initialization cap
// (initBrowserSession, T-218): the chromedp bootstrap commands are not
// bounded from inside — a Chromium hung at startup would hold the
// generation start (and with it the manager mutex) forever. A hung
// bootstrap command → StartBrowser fails with the browser class within
// the DialTimeout + SessionStartTimeout cap, the socket is closed;
// after the browser's "recovery" the next start passes.
func TestStartBrowserBootstrapTimeout(t *testing.T) {
	tr := newSwitchableCDP()
	var dials int
	overrideRemoteDial(t, func(context.Context, string, time.Duration) (chromedp.Transport, error) {
		dials++
		if dials == 1 {
			return tr, nil
		}
		return newSwitchableCDP(), nil
	})

	// The browser session's service tab is not created: the connection
	// is there, no answers arrive (a Chromium hung at startup).
	tr.hangMethod("Target.createTarget")

	opts := remoteOpts() // the initialization cap = 1 s + 300 ms
	started := time.Now()
	_, err := StartBrowser(context.Background(), opts)
	var e *Error
	if !errors.As(err, &e) || e.Class != ClassBrowser {
		t.Fatalf("the initialization timeout: the class %v (%v), want browser", ClassOf(err), err)
	}
	if !strings.Contains(err.Error(), "the initialization timeout") {
		t.Errorf("the error does not mention the cause: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Errorf("the start hung for %s — the cap does not work", elapsed)
	}
	// The socket of the hung connection is closed forcibly (the
	// cancellation hygiene of StartBrowser + the allocator), it does
	// not wait for the browser's "recovery".
	waitForFor(t, 10*time.Second, tr.isClosed)

	// The browser "recovered" (the hang is lifted — the new transport
	// answers): the next generation start passes, the connection is
	// alive.
	b, err := StartBrowser(context.Background(), opts)
	if err != nil {
		t.Fatalf("the start after the recovery: %v", err)
	}
	defer b.Close()
	if !b.Alive() {
		t.Error("the new generation is alive")
	}
}

// TestSessionRunPageTimeoutDeadConnection — a connection break in the
// middle of a page (T-218, the P2 of the review): a page timeout on a
// half-closed socket is not a tarpit but an infrastructure failure:
// the probe in Session.wrap reclassifies it as browser, Manager will
// swap the generation and retry the task.
func TestSessionRunPageTimeoutDeadConnection(t *testing.T) {
	tr := newSwitchableCDP()
	overrideRemoteDial(t, func(context.Context, string, time.Duration) (chromedp.Transport, error) {
		return tr, nil
	})
	opts := remoteOpts()
	b := switchableBrowser(t, tr, opts)

	s, err := b.NewSession(context.Background())
	if err != nil {
		t.Fatalf("NewSession on a live connection: %v", err)
	}
	defer s.Close()

	// A break in the middle of the task: the commands go out, no
	// answers arrive — the very first stage (startBlock/Fetch.enable)
	// hangs until the page timeout.
	tr.kill()
	_, err = s.Run(context.Background(), RenderTask{URL: "https://a.example/"})
	var e *Error
	if !errors.As(err, &e) || e.Class != ClassBrowser {
		t.Fatalf("a page timeout on a dead connection: the class %v (%v), want browser", ClassOf(err), err)
	}
}

// TestResolveRemoteWSURL — the WebSocket address refinement: a ready
// /devtools/browser/ address is used as is (no network), otherwise — a
// /json/version request; an answer without webSocketDebuggerUrl is an
// error.
func TestResolveRemoteWSURL(t *testing.T) {
	ctx := context.Background()

	ready := "ws://127.0.0.1:9222/devtools/browser/abc"
	ws, err := resolveRemoteWSURL(ctx, ready, time.Second)
	if err != nil {
		t.Fatalf("the ready address: %v", err)
	}
	if ws != ready {
		t.Errorf("the ready address changed: %s", ws)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/json/version" {
			http.NotFound(w, r)
			return
		}
		_, _ = fmt.Fprint(w, `{"Browser":"x","webSocketDebuggerUrl":"ws://127.0.0.1:9222/devtools/browser/xyz"}`)
	}))
	defer srv.Close()
	ws, err = resolveRemoteWSURL(ctx, srv.URL, time.Second)
	if err != nil {
		t.Fatalf("the address refinement: %v", err)
	}
	if ws != "ws://127.0.0.1:9222/devtools/browser/xyz" {
		t.Errorf("webSocketDebuggerUrl: %s", ws)
	}

	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{}`)
	}))
	defer empty.Close()
	if _, err := resolveRemoteWSURL(ctx, empty.URL, time.Second); err == nil {
		t.Error("a response without webSocketDebuggerUrl must be an error")
	} else if !strings.Contains(err.Error(), "webSocketDebuggerUrl") {
		t.Errorf("the error does not mention the cause: %v", err)
	}
}

// overrideRemoteResolve — replacing the resolve seam for the test's
// duration (the overrideRemoteDial style).
func overrideRemoteResolve(t *testing.T, f func(context.Context, string) ([]net.IPAddr, error)) {
	t.Helper()
	old := remoteResolve
	remoteResolve = f
	t.Cleanup(func() { remoteResolve = old })
}

// TestResolveRemoteWSURLPreferIPv4 — the address choice from the DNS
// answer (T-229): an IPv6-first order must not hand a v6 address to a
// Chromium listening on IPv4 only (the connection would go to the
// second address); v4-first and v6-only answers work with the first
// address. The /devtools/browser/ path — no network; the address is
// checked right after the resolve.
func TestResolveRemoteWSURLPreferIPv4(t *testing.T) {
	ctx := context.Background()
	v6 := net.ParseIP("2001:db8::1")
	v4 := net.ParseIP("192.0.2.10")
	const withPath = "ws://chromium.example:9222/devtools/browser/abc"
	const noPort = "ws://chromium.example/devtools/browser/abc"

	for _, tc := range []struct {
		name  string
		raw   string
		addrs []net.IPAddr
		want  string
	}{
		{"IPv6-first — IPv4 chosen", withPath, []net.IPAddr{{IP: v6}, {IP: v4}}, "ws://192.0.2.10:9222/devtools/browser/abc"},
		{"IPv4-first — the same one", withPath, []net.IPAddr{{IP: v4}, {IP: v6}}, "ws://192.0.2.10:9222/devtools/browser/abc"},
		{"only IPv6 — the first address", withPath, []net.IPAddr{{IP: v6}}, "ws://[2001:db8::1]:9222/devtools/browser/abc"},
		// Without a port: v4 — as is, v6 — in brackets (bare colons in
		// the host would be re-parsed as the port, the review P3-7).
		{"IPv4 without a port", noPort, []net.IPAddr{{IP: v6}, {IP: v4}}, "ws://192.0.2.10/devtools/browser/abc"},
		{"IPv6 without a port — in brackets", noPort, []net.IPAddr{{IP: v6}}, "ws://[2001:db8::1]/devtools/browser/abc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			overrideRemoteResolve(t, func(_ context.Context, _ string) ([]net.IPAddr, error) {
				return tc.addrs, nil
			})
			ws, err := resolveRemoteWSURL(ctx, tc.raw, time.Second)
			if err != nil {
				t.Fatalf("resolution: %v", err)
			}
			if ws != tc.want {
				t.Errorf("the address %s, want %s", ws, tc.want)
			}
		})
	}
}

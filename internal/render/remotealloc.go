package render

// The allocator of the remote mode (an external Chromium,
// Options.RemoteURL) — a functional analog of the chromedp
// remote.Allocator with one principal difference: deterministic closure
// of the connection (T-218, the Nov 2 incident). Upstream closes the
// WebSocket only after the chromedp pumps (browser.run) finish, and the
// library transport is not time-bounded: remote.Conn.Write has no
// deadline and ignores the context, remote.Conn.Read ignores the context
// entirely — on a half-closed or hung connection (NAT/conntrack between
// the worker and Chromium, a browser hung on shutdown) a pump hangs in
// writing until the TCP retransmissions run out (minutes), reading hangs
// forever. The sockets stay ESTABLISHED: the incident's workers
// accumulated 284 k of them to :9222 over ~6 h. Here, after the context
// is cancelled, the regular chromedp tab closure runs first (bounded by
// the library's own timeouts), then the socket is closed forcibly —
// regardless of the pump state.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/chromedp"
	"github.com/chromedp/chromedp/remote"

	"crawler/internal/config"
)

// remoteDial — connecting the transport to the external Chromium
// (WebSocket). The unit-test seam: replaced by a controllable fake — a
// CDP "black hole" (reads hang, writes are accepted) and a "resurrection"
// of the browser with a new connection (reconnect_test.go, T-218). The
// production path — remote.DialContext with the Options.DialTimeout
// timeout.
var remoteDial = func(ctx context.Context, wsURL string, timeout time.Duration) (chromedp.Transport, error) {
	dctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := remote.DialContext(dctx, wsURL)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// remoteResolve — resolving the Chromium host into addresses; the unit
// test seam of resolveRemoteWSURL (the address order of the DNS answer,
// T-229); the production path — net.DefaultResolver (the remoteDial
// style).
var remoteResolve = func(ctx context.Context, host string) ([]net.IPAddr, error) {
	return net.DefaultResolver.LookupIPAddr(ctx, host)
}

// remoteAllocator — the browser session allocator over an external
// Chromium: one WebSocket connection per browser generation (StartBrowser
// creates the allocator anew). Implements chromedp.Allocator and
// chromedp.Attacher — the "the browser is already running" semantics are
// mandatory (see Attaches).
type remoteAllocator struct {
	wsURL       string
	dialTimeout time.Duration
	// urlMode — the mode of preparing the connection address (the
	// engines render spec §4.2 items 1/3): the engine-specific CDP
	// nuances are fixed by allocator parameters, not by core code. Set
	// in newRemoteAllocator by Options.Engine.
	urlMode wsURLMode

	wg sync.WaitGroup
}

// wsURLMode — the mode of preparing the connection address to the
// external engine (the engines render spec §4.2 item 1: engine specifics
// live only here). The behavior is confirmed on the pinned stack images
// (moli v1.1.21, lightpanda 1.0.2): moli serves the browser WebSocket
// only on the /devtools/browser/<uuid> path generated on every process
// start (a handshake at the root — 404, at the uuid path — 101; the uuid
// changes on restart), lightpanda — at the root path of the address
// itself (a handshake at "/" — 101).
type wsURLMode int

const (
	// wsURLResolve — chromium: resolving the host into an IP and
	// refining the address with a /json/version request
	// (resolveRemoteWSURL); the advertised webSocketDebuggerUrl is used
	// as is — Chromium builds it from the same listener.
	wsURLResolve wsURLMode = iota
	// wsURLAsIs — lightpanda: the browser WebSocket lives at the root
	// path of the address — the connection goes to the configuration
	// address unchanged (an analog of the remote.NoModifyURL option of
	// chromedp/remote); the /json/version advertisement is built from the
	// bind address and is not used for connecting.
	wsURLAsIs
	// wsURLResolveAuthority — moli: the browser session path must be
	// refined with a /json/version request (the uuid path is new on every
	// start), but the authority (host:port) of the advertised URL is
	// replaced with the configuration address: moli builds the
	// advertisement from the bind address (there is no advertise-host
	// flag), and with 0.0.0.0/127.0.0.1 bound it is unreachable from
	// outside the container — see wsURLWithAuthority.
	wsURLResolveAuthority
)

// newRemoteAllocator — the allocator for Options.RemoteURL (DialTimeout
// bounds both the address refinement and the connection). The address
// mode is by engine (Options.Engine; chromium and "" — refinement, as
// before stage 35).
func newRemoteAllocator(opts Options) *remoteAllocator {
	a := &remoteAllocator{wsURL: opts.RemoteURL, dialTimeout: opts.DialTimeout, urlMode: wsURLResolve}
	switch opts.Engine {
	case config.EngineLightpanda:
		a.urlMode = wsURLAsIs
	case config.EngineMoli:
		a.urlMode = wsURLResolveAuthority
	}
	return a
}

// Allocate satisfies chromedp.Allocator: ctx is the context of the
// chromedp browser session (the context of the first Run/Do); its
// cancellation stops the session — first softly (tab closing commands),
// then forcibly (the socket). The dial part is bounded by dialTimeout
// via remoteDial.
func (a *remoteAllocator) Allocate(ctx context.Context, opts ...chromedp.BrowserOption) (*chromedp.Browser, error) {
	wsURL, err := a.dialURL(ctx)
	if err != nil {
		return nil, err
	}
	tr, err := remoteDial(ctx, wsURL, a.dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("подключение к Chromium %s: %w", a.wsURL, err)
	}
	// wctx — the session lifetime context, separate from ctx (as in
	// remote.Allocator): cancelling the client context must not tear
	// down the connection before the tabs are closed.
	wctx, wcancel := context.WithCancel(context.Background())
	a.wg.Go(func() {
		<-ctx.Done()
		// The regular path: close the tabs and isolated contexts over the
		// socket. The wait is bounded by chromedp's own timeouts
		// (closeTargetTimeout 5 s + waitTargetGone 0.5 s) even on a dead
		// connection; the result does not matter here — the socket is
		// closed forcibly either way.
		_ = chromedp.Cancel(ctx)
		// The forced finale: Close is idempotent and unblocks the pumps'
		// hung reads/writes — the socket is released within chromedp's
		// timeouts, not the TCP retransmissions.
		_ = tr.Close()
		wcancel()
	})
	return chromedp.NewBrowserTransport(wctx, tr, opts...)
}

// dialURL — the WebSocket connection address by engine mode (wsURLMode):
// chromium refines the address (host resolve + /json/version), lightpanda
// uses the configuration address as is, moli refines the path via
// /json/version but connects with the authority of the configuration
// address.
func (a *remoteAllocator) dialURL(ctx context.Context) (string, error) {
	if a.urlMode == wsURLAsIs {
		return a.wsURL, nil
	}
	ws, err := resolveRemoteWSURL(ctx, a.wsURL, a.dialTimeout)
	if err != nil || a.urlMode != wsURLResolveAuthority {
		return ws, err
	}
	return wsURLWithAuthority(ws, a.wsURL)
}

// wsURLWithAuthority — the webSocketDebuggerUrl with the authority
// (host:port) of the configuration address (the moli mode): the engine's
// browser session path (/devtools/browser/<uuid>, new on every start)
// and the scheme stay from the advertisement, while the connection
// address is the one set by the operator — the engine builds the
// advertisement from the bind address, and with 0.0.0.0/127.0.0.1 bound
// it is unreachable from outside the container. The configuration
// address has already passed validation (a non-empty host); an empty
// parse here is a defensive failure.
func wsURLWithAuthority(wsURL, cfgURL string) (string, error) {
	ws, err := url.Parse(wsURL)
	if err != nil {
		return "", fmt.Errorf("webSocketDebuggerUrl %q: %w", wsURL, err)
	}
	cfg, err := url.Parse(cfgURL)
	if err != nil {
		return "", fmt.Errorf("адрес конфигурации %q: %w", cfgURL, err)
	}
	if cfg.Host == "" {
		return "", fmt.Errorf("адрес конфигурации %q без хоста", cfgURL)
	}
	ws.Host = cfg.Host
	return ws.String(), nil
}

// Attaches satisfies chromedp.Attacher: the browser is already started
// by an external process (the crawler-chromium unit / the chromium
// compose service), the allocator only connects. The flag changes the
// semantics of the chromedp browser context: without it the context
// would be considered the browser's owner — closing would go through
// the Browser.close command (kills the shared external Chromium), and
// the session's service tab would be awaited as an event rather than
// created.
func (a *remoteAllocator) Attaches() bool { return true }

// Wait satisfies chromedp.Allocator: the allocator's background
// connection closures have completed.
func (a *remoteAllocator) Wait() { a.wg.Wait() }

// preferIPv4 — the connection address from the resolve answer (T-229):
// the first IPv4 if there is one, otherwise the first address. DNS may
// answer IPv6-first (the address sorting of a dual-stack host), while
// the Chromium of the external unit listens on IPv4 (docker publishes
// :9222 on 0.0.0.0, the crawler-chromium systemd unit — on the host
// address): a v6 address attempt to a v4-only listener would fail with
// the browser alive — the generation would not come up. Trying the
// addresses with connection attempts here is redundant: the resolve
// only builds the URL, the actual connection is remoteDial within
// Options.DialTimeout; a v6-only host keeps working with the first
// address.
//
// The input contract is net.Resolver.LookupIPAddr: err == nil guarantees
// a non-empty slice (no records — an error), indexing [0] is safe.
func preferIPv4(addrs []net.IPAddr) net.IPAddr {
	for _, a := range addrs {
		if a.IP.To4() != nil {
			return a
		}
	}
	return addrs[0]
}

// resolveRemoteWSURL — the browser WebSocket address to connect to: a
// configuration address with the /devtools/browser/ path is a ready
// address and is used as is; otherwise it is refined with a
// /json/version request (webSocketDebuggerUrl), as remote.Allocator
// does. A host that is not an IP/localhost is resolved into an IP
// (IPv4 preferred — preferIPv4): Chrome ≥66 requires the Host header of
// the connection to be an IP address or localhost.
func resolveRemoteWSURL(ctx context.Context, raw string, timeout time.Duration) (string, error) {
	// One timeout cap for the whole address refinement (the resolve and
	// the request): the input ctx is the browser lifecycle context
	// without a deadline (the StartBrowser contract), and per the docs
	// Options.DialTimeout bounds the address refinement as well
	// (render.go, Options.DialTimeout).
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("адрес Chromium %q: %w", raw, err)
	}
	if host := u.Hostname(); host != "localhost" && net.ParseIP(host) == nil {
		addrs, err := remoteResolve(rctx, host)
		if err != nil {
			return "", fmt.Errorf("резолв хоста Chromium %s: %w", host, err)
		}
		ip := preferIPv4(addrs)
		if port := u.Port(); port != "" {
			u.Host = net.JoinHostPort(ip.String(), port)
		} else if ip.IP.To4() != nil {
			u.Host = ip.String()
		} else {
			// IPv6 without a port — in brackets only: bare colons in the
			// host would be re-parsed by the "host:port" scheme
			// (Hostname "2001:db8:", Port "1"). JoinHostPort adds the
			// brackets itself; the port branch does not need this
			// (T-229, review P3-7).
			u.Host = "[" + ip.String() + "]"
		}
	}
	if strings.Contains(u.Path, "/devtools/browser/") {
		return u.String(), nil
	}
	q := *u
	q.Scheme = "http"
	if u.Scheme == "wss" || u.Scheme == "https" {
		// Secure schemes are not downgraded to a plaintext /json/version
		// request.
		q.Scheme = "https"
	}
	q.Path = "/json/version"
	req, err := http.NewRequestWithContext(rctx, http.MethodGet, q.String(), nil)
	if err != nil {
		return "", fmt.Errorf("запрос /json/version (%s): %w", q.Host, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("запрос /json/version (%s): %w", q.Host, err)
	}
	defer func() { _ = resp.Body.Close() }()
	var v struct {
		WS string `json:"webSocketDebuggerUrl"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return "", fmt.Errorf("ответ /json/version (%s): %w", q.Host, err)
	}
	if v.WS == "" {
		return "", fmt.Errorf("ответ /json/version (%s) без webSocketDebuggerUrl", q.Host)
	}
	return v.WS, nil
}

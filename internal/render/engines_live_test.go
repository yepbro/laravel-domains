package render

// Per-engine live tests of the render (T-254, layer 1) — the full CDP slice of each
// engine (chromium / moli / lightpanda) against the dev stack: the invariants of spec 5.6
// must hold on any engine (the engines render spec FR-5). The slice per engine:
//
//	(a) /json/version is available and responds (the common health endpoint, FR-4);
//	(b) an isolated context per task: a cookie does not survive a context
//	    change (Target.createBrowserContext/disposeBrowserContext);
//	(c) navigate + result capture (T-064): the final DOM after JS,
//	    the cookies of the context store (JS- and httpOnly) and the response
//	    headers of the main navigation — on the internal testbed pages *.crawl.test;
//	(d) the Fetch interception: blocking Image/Font does not let a request out of
//	    the engine (the server's counters, T-062) and the tracker domain patterns
//	    (the differential of two runs, T-214);
//	(e) the SSRF contour (T-219/T-230): a task with an internal host is rejected
//	    before the navigation, navigation to a private testbed address is cut off
//	    by the interception inside the engine itself — net::ERR_BLOCKED_BY_CLIENT;
//	(f) the UA override (spec §9): Emulation.setUserAgentOverride is visible both in
//	    navigator.userAgent and in the User-Agent of the page request on the server.
//
// Enabling — as in the neighboring live tests (CRAWLER_RENDER_TEST=1, the sample
// in browser_live_test.go); the regular make test without the stack skips them.
// Connecting an engine — the environment variable of the table row, modeled on
// CHROMIUM_WS_URL: MOLI_WS_URL / LIGHTPANDA_WS_URL (the defaults are static
// addresses of the compose services: chromium of the app profile, moli/lightpanda of the
// render-engines profile, T-241). The common CRAWLER_RENDER_REMOTE_URL
// override (the address of one engine in the other live tests) is not read by the
// slice: the table distinguishes the engines by addresses explicitly. A new engine is
// added by a row in engineLiveCases — the slice code is one.
//
// Before each engine's run — a quick /json/version probe (the same check as
// the compose healthcheck and TestChromium_CDP): unavailable at the default
// address — SKIP with a reason (the render-engines profile is not obliged to be up
// for everyone); with an explicitly set variable — Fatal (the operator pointed the test
// at a concrete address, silence would hide a configuration error). In the
// tools container of compose CHROMIUM_WS_URL is always set — an unavailable
// chromium gives a failure, not a SKIP: it is raised together with the stack
// (the app profile) and is not supposed to be unavailable in a working environment.
//
// Running (moli/lightpanda are raised in advance by the render-engines profile —
// docs/dev-environment.md, "Render engines"; --use-aliases is needed by the
// tracker slice, as in BlockLive):
//
//	docker compose --project-directory . -f deploy/docker/compose.yaml \
//	  run --use-aliases -e CRAWLER_RENDER_TEST=1 --rm tools \
//	  go test ./internal/render -run EnginesLive -v -count=1
//
// A single engine — with a Go filter: -run 'TestEnginesLiveCDPSlice/moli'.
//
// The engines' state against the slice (pinned versions and retests:
// the live run of 2026-11-06, the moli v1.1.23 retest of 2026-11-10) — moli and
// lightpanda 1.0.2, both alternatives are BROKEN against the slice; the slice must
// show this in red until the engines/core are fixed):
//
//   - moli v1.1.23 (T-272; the pinned in the repo stays v1.1.21):
//     the InvalidParams on Fetch.enable is gone — Font/Media in the patterns
//     got the fixes of lexmount/moli#2470 (our PR, the yepbro/moli fork) +
//     the paired #2476. Navigation/DOM/cookies/context isolation/UA override
//     are green. The slice is held red by the Fetch interception defects:
//     (1) with the interception enabled a synchronous XHR from the page's JS
//     fails on the client with NetworkError (the moli log: Uncaught NetworkError:
//     Failed to execute 'send' on 'XMLHttpRequest') — the request does not reach
//     the server (block_resources); the block_trackers
//     probes are switched to async XHR (T-275: real analytics is async,
//     blocked on all engines); the sync probe there is a separate
//     engine-branched case of the #2728/#2729 divergence (moli does not pause sync-XHR
//     and it goes to the network; on builds without #2729 — a
//     NetworkError failure factory);
//     (2) the fetch.failRequest of the main navigation does not finish the navigation with
//     a load error — the tab is unresponsive (classifyCtx → context),
//     ERR_BLOCKED_BY_CLIENT arrives outside the loadFailed channel, there is no
//     load code (ssrf); (3) Stylesheet is absent from supports_fetch_enable()
//     v1.1.23 (fetch_support.rs: the enum type parses, but is not in the
//     supports list) — the Stylesheet pattern brings down the whole
//     Fetch.enable with InvalidParams: BlockStylesheet (T-213, the CSS mode)
//     is unavailable on moli; not a blocker for the base profile (the option defaults to
//     false). We wait for the upstream fix → a retest with
//     the next release (T-247). The history before v1.1.23 (v1.1.21/1.1.22,
//     the run of 2026-11-06): Fetch.enable with Media/Font patterns
//     answered InvalidParams — moli distinguished a trimmed set of
//     resourceType; every task failed at startBlock (0/100 on
//     the seed hundred).
//   - lightpanda 1.0.2: one browser context per connection ("Cannot have
//     more than one browser context at a time", also recorded by
//     third-party measurements). The service tab of the starting context
//     takes the only slot → the Target.createBrowserContext of a session
//     always fails: the "a context per task" model (spec 5.6) is impossible on the
//     pinned version.
//
// A red slice = an engine regression against the stage's invariants (pinning
// a new version — the smoke procedure of FR-17/T-254,
// docs/dev-environment.md); a green chromium is the reference (the control
// of 2026-11-10: 6/6 subtests PASS; the re-check of 2026-11-11 after
// T-275 — again 6/6, the block_trackers probes async) — the moli failures are
// engine ones, not bench ones).

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"

	"crawler/internal/config"
)

// engineLiveCase — a row of the engines table: the name (the value of render.engine and
// the scene of the test), the environment variable with the address and the default
// stack address.
type engineLiveCase struct {
	name  string
	env   string
	defWS string
}

// engineLiveCases — the engines table of layer 1: one slice code × the sets of
// connection variables (a new engine — a row).
var engineLiveCases = []engineLiveCase{
	{name: config.EngineChromium, env: "CHROMIUM_WS_URL", defWS: "ws://172.28.100.14:9222"},
	{name: config.EngineMoli, env: "MOLI_WS_URL", defWS: "ws://172.28.100.17:9222"},
	{name: config.EngineLightpanda, env: "LIGHTPANDA_WS_URL", defWS: "ws://172.28.100.16:9222"},
}

// engineLiveWS — the engine's address: the table row's environment variable, otherwise
// the stack default. The second value — whether the variable is set explicitly (the
// operator pointed the slice at a concrete address).
func engineLiveWS(tc engineLiveCase) (string, bool) {
	if ws := os.Getenv(tc.env); ws != "" {
		return ws, true
	}
	return tc.defWS, false
}

// engineLiveOptions — the Options of the table row's engine (the address — engineLiveWS,
// the engine — by the Engine field: the validation and the remote allocator see it, the
// engines render spec §4.2; the package core does not distinguish engines, FR-4).
func engineLiveOptions(tc engineLiveCase) Options {
	ws, _ := engineLiveWS(tc)
	return Options{
		Engine:            tc.name,
		RemoteURL:         ws,
		Contexts:          2,
		Timeout:           30 * time.Second,
		RestartEveryPages: 1000,
	}
}

// engineLiveVersionURL — the http address of the engine's /json/version from the ws
// address of the connection (all three engines serve the common endpoint on the same
// port).
func engineLiveVersionURL(ws string) (string, error) {
	u, err := url.Parse(ws)
	if err != nil {
		return "", fmt.Errorf("разбор %q: %w", ws, err)
	}
	switch u.Scheme {
	case "ws", "http":
		u.Scheme = "http"
	case "wss", "https":
		u.Scheme = "https"
	default:
		return "", fmt.Errorf("схема %q (ожидается ws:// или wss://)", u.Scheme)
	}
	u.Path = "/json/version"
	u.RawQuery = ""
	return u.String(), nil
}

// engineLiveGate — (a) the engine's availability before the slice starts: GET
// /json/version
// with a short cap (a quick failure with a reason instead of a cascade of timeouts across
// all subtests). 200 + "Browser" in the body — the response shape of all three engines
// (as the compose healthcheck and TestChromium_CDP); the body is written to the log —
// the engine version from the response is cross-checked against the pinned at the FR-17/T-254 smoke.
func engineLiveGate(t *testing.T, tc engineLiveCase) {
	t.Helper()
	ws, explicit := engineLiveWS(tc)
	vu, err := engineLiveVersionURL(ws)
	if err != nil {
		t.Fatalf("адрес движка %s из %s: %v", ws, tc.env, err)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(vu)
	if err != nil {
		if explicit {
			t.Fatalf("движок %s недоступен: %v (%s = %s) — проверьте адрес переменной %s",
				tc.name, err, tc.env, ws, tc.env)
		}
		t.Skipf("движок %s недоступен (%s): %v — контейнер движка не поднят? "+
			"Подъём: docker compose --profile render-engines up -d moli lightpanda "+
			"(chromium — профиль app: ... up -d chromium); docs/dev-environment.md, «Движки рендера»",
			tc.name, vu, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "Browser") {
		t.Fatalf("ответ /json/version движка %s: %d %s", tc.name, resp.StatusCode, string(body))
	}
	t.Logf("движок %s, /json/version: %s", tc.name, strings.TrimSpace(string(body)))
}

// engineLiveBrowser — starting the engine's browser for a slice subtest (closing —
// Cleanup): each slice check lives with its own browser, a failure of one does not
// tear the others.
func engineLiveBrowser(t *testing.T, opts Options) *Browser {
	t.Helper()
	b, err := StartBrowser(context.Background(), opts)
	if err != nil {
		t.Fatalf("StartBrowser (%s): %v", opts.Engine, err)
	}
	t.Cleanup(b.Close)
	return b
}

// TestEnginesLiveCDPSlice — the engines table × the CDP slice (T-254, layer 1):
// one code runs (a)–(f) for each row of engineLiveCases. Chromium
// is always up (the app profile), the alternatives — the render-engines profile;
// an unavailable engine is honestly skipped by the gate (a) with a reason.
func TestEnginesLiveCDPSlice(t *testing.T) {
	skipUnlessLive(t)
	for _, tc := range engineLiveCases {
		t.Run(tc.name, func(t *testing.T) {
			engineLiveGate(t, tc) // (a) /json/version: the probe before the slice
			t.Run("context_isolation", func(t *testing.T) {
				engineLiveContextIsolation(t, engineLiveOptions(tc))
			})
			t.Run("navigate_extract", func(t *testing.T) {
				engineLiveNavigateExtract(t, engineLiveOptions(tc))
			})
			t.Run("block_resources", func(t *testing.T) {
				engineLiveBlockResources(t, engineLiveOptions(tc))
			})
			t.Run("block_trackers", func(t *testing.T) {
				blockLiveTrackerDomains(t, engineLiveOptions(tc))
			})
			t.Run("ssrf", func(t *testing.T) {
				engineLiveSSRF(t, engineLiveOptions(tc))
			})
			t.Run("ua_override", func(t *testing.T) {
				engineLiveUAOverride(t, engineLiveOptions(tc))
			})
		})
	}
}

// engineLiveContextIsolation — (b) an isolated context per task
// (the invariant of spec 5.6/§9, FR-5): a cookie put into the incognito profile
// of the first session is visible in it and does not survive a context change (a second
// NewSession = a new profile; dispose throws the stores away entirely).
// The check — after TestBrowserLiveContextIsolation, the engines — the table rows.
func engineLiveContextIsolation(t *testing.T, opts Options) {
	ctx := context.Background()

	b := engineLiveBrowser(t, opts)

	const cookieURL = "http://example.net/"
	setCookie := chromedp.Func(func(ctx context.Context, t *chromedp.Target) error {
		_, err := cdp.Call(ctx, t, network.SetCookie, network.SetCookieParams{
			Name: "isolation", Value: "1", URL: cookieURL, Path: "/",
		})
		return err
	})
	getCookieNames := func(ctx context.Context, t *chromedp.Target) ([]string, error) {
		res, err := cdp.Call(ctx, t, network.GetCookies, network.GetCookiesParams{
			URLs: []string{cookieURL},
		})
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(res.Cookies))
		for _, c := range res.Cookies {
			names = append(names, c.Name)
		}
		return names, nil
	}

	s1, err := b.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if err := chromedp.Do(s1.Context(), setCookie); err != nil {
		s1.Close()
		t.Fatalf("SetCookie: %v", err)
	}
	names, err := chromedp.Run(s1.Context(), getCookieNames)
	if err != nil {
		s1.Close()
		t.Fatalf("GetCookies (та же сессия): %v", err)
	}
	s1.Close()
	if len(names) != 1 || names[0] != "isolation" {
		t.Fatalf("в первой сессии cookies %v, хочу [isolation]", names)
	}

	s2, err := b.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession (вторая): %v", err)
	}
	names, err = chromedp.Run(s2.Context(), getCookieNames)
	s2.Close()
	if err != nil {
		t.Fatalf("GetCookies (вторая сессия): %v", err)
	}
	if len(names) != 0 {
		t.Fatalf("состояние пережило смену контекста: cookies %v", names)
	}
}

// engineLiveNavigateExtract — (c) navigation and result capture (T-064):
// the internal testbed page (spa-shell.crawl.test — the engines resolve it
// through the stack's unbound) gives the final DOM after JS, a JS cookie from the
// context store (CDP reads the store, document.cookie does not) and the headers
// of the real Caddy's main navigation response; the slice fixture covers
// httpOnly — a cookie set by the SERVER's response (document.cookie does not
// see it at all — it gets into the context store only via the engine's network
// stack) and a signature pair of headers. The testweb samples do not fit for
// httpOnly: the laravel samples' fixtures set Set-Cookie with an expired date
// (the headers were generated 2026-10-16 with an expiry of 2026-10-21 — the browser
// drops such a cookie right away), so httpOnly is covered by a fixture's own
// page with a non-expiring session cookie. The second page — by a new
// session: a context per task, as in the pool.
func engineLiveNavigateExtract(t *testing.T, opts Options) {
	ctx := context.Background()

	b := engineLiveBrowser(t, opts)

	mux := http.NewServeMux()
	mux.HandleFunc("/httponly", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// The fixture's signature header: its presence in the result proves
		// that the headers were captured from the real server response
		// (Content-Type alone could have come from a pseudo-response too).
		w.Header().Set("X-Engine-Live", "1")
		http.SetCookie(w, &http.Cookie{
			Name: "engine_httponly", Value: "1", Path: "/",
			HttpOnly: true, SameSite: http.SameSiteLaxMode,
		})
		_, _ = fmt.Fprint(w, `<html><body><script>document.cookie="engine_js=1; path=/";</script>ok</body></html>`)
	})
	base := liveServe(t, mux)

	t.Run("spa-shell.crawl.test: DOM после JS и JS-cookie", func(t *testing.T) {
		s, err := b.NewSession(ctx)
		if err != nil {
			t.Fatalf("NewSession: %v", err)
		}
		defer s.Close()
		out, err := s.Run(ctx, RenderTask{URL: "http://spa-shell.crawl.test/"})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.FinalURL != "http://spa-shell.crawl.test/" {
			t.Errorf("итоговый URL %q", out.FinalURL)
		}
		if !strings.HasPrefix(out.DOM, "<html") {
			t.Errorf("итоговый DOM не с корневого элемента: %.80q", out.DOM)
		}
		if !strings.Contains(out.DOM, `<div id="root">`) {
			t.Errorf("итоговый DOM без каркаса SPA: %.120q", out.DOM)
		}
		var js *ExtractCookie
		for i := range out.Cookies {
			if out.Cookies[i].Name == "js_session" {
				js = &out.Cookies[i]
			}
		}
		if js == nil {
			t.Fatalf("JS-cookie js_session не снята: %+v", out.Cookies)
		}
		if js.Value != "rendered-by-js" || js.Path != "/" || js.Domain != "spa-shell.crawl.test" {
			t.Errorf("js_session: %+v", js)
		}
		if ct := out.ResponseHeaders.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
			t.Errorf("Content-Type %q, хочу text/html…", ct)
		}
		if out.ResponseHeaders.Get("Server") == "" {
			t.Errorf("заголовок Server не снят: %v", out.ResponseHeaders)
		}
	})

	t.Run("фикстура: httpOnly-cookie и заголовки ответа", func(t *testing.T) {
		s, err := b.NewSession(ctx)
		if err != nil {
			t.Fatalf("NewSession: %v", err)
		}
		defer s.Close()
		page := base + "/httponly"
		out, err := s.Run(ctx, RenderTask{URL: page})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.FinalURL != page {
			t.Errorf("итоговый URL %q, хочу %q", out.FinalURL, page)
		}
		var httpOnly, js *ExtractCookie
		for i := range out.Cookies {
			switch out.Cookies[i].Name {
			case "engine_httponly":
				httpOnly = &out.Cookies[i]
			case "engine_js":
				js = &out.Cookies[i]
			}
		}
		if httpOnly == nil {
			t.Fatalf("httpOnly-cookie engine_httponly не снята из хранилища: %+v", out.Cookies)
		}
		if !httpOnly.HTTPOnly || httpOnly.Path != "/" || httpOnly.Value != "1" {
			t.Errorf("engine_httponly: %+v, хочу httpOnly cookie path=/ value=1", httpOnly)
		}
		if js == nil || js.Value != "1" {
			t.Errorf("JS-cookie engine_js: %+v, хочу value=1 (JS обязан работать)", js)
		}
		if !strings.HasPrefix(out.DOM, "<html") || !strings.Contains(out.DOM, "ok</body>") {
			t.Errorf("итоговый DOM неожиданен: %.120q", out.DOM)
		}
		if got := out.ResponseHeaders.Get("X-Engine-Live"); got != "1" {
			t.Errorf("X-Engine-Live %q, хочу 1 — заголовки не с реального ответа фикстуры", got)
		}
		if ct := out.ResponseHeaders.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
			t.Errorf("Content-Type %q, хочу text/html…", ct)
		}
	})
}

// engineLiveBlockResources — (d) the Fetch interception by resource types (T-062):
// the image and the font are not requested at all (failRequest before sending — the
// engine rejects them with net::ERR_BLOCKED_BY_CLIENT, the server counters are zero —
// evidence independent of the CDP events, the blockLiveServe fixture);
// CSS/script/XHR pass (JS must work, spec 5.6).
func engineLiveBlockResources(t *testing.T, opts Options) {
	c, pageURL := blockLiveServe(t)

	out := blockLiveRun(t, opts, pageURL)
	if out.FinalURL != pageURL {
		t.Errorf("final URL %q, хочу %q", out.FinalURL, pageURL)
	}

	blockLiveWaitReady(t, c)
	if got := c.page.Load(); got < 1 {
		t.Errorf("страница запрошена %d раз, хочу >= 1", got)
	}
	if got := c.css.Load(); got < 1 {
		t.Errorf("CSS запрошен %d раз, хочу >= 1 (не блокируется)", got)
	}
	if got := c.js.Load(); got < 1 {
		t.Errorf("скрипт запрошен %d раз, хочу >= 1 (не блокируется)", got)
	}
	if got := c.xhr.Load(); got < 1 {
		t.Errorf("XHR запрошен %d раз, хочу >= 1 (JS обязан работать)", got)
	}
	if got := c.img.Load(); got != 0 {
		t.Errorf("картинка запрошена %d раз, хочу 0 (net::ERR_BLOCKED_BY_CLIENT до отправки, T-062)", got)
	}
	if got := c.font.Load(); got != 0 {
		t.Errorf("шрифт запрошен %d раз, хочу 0 (net::ERR_BLOCKED_BY_CLIENT до отправки, T-062)", got)
	}
}

// engineLiveLookupHost — the SSRF contour resolve of the slice (the Options.LookupHost
// seam, T-230): the stack's unbound (172.28.100.10) — the same resolver the engines
// use to resolve the testbed pages (dns: unbound in compose). The resolver of the
// tools run container is external (it downloads the Go modules) and does not know the
// test zones: without the substitution the zone name resolve in the interception
// would fail with a DNS failure, the contour would fail-open the navigation, and the
// interception check would lose its meaning.
func engineLiveLookupHost(ctx context.Context, host string) ([]netip.Addr, error) {
	r := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "udp", "172.28.100.10:53")
		},
	}
	return r.LookupNetIP(ctx, "ip", host)
}

// engineLiveSSRF — (e) the render's SSRF contour (T-219/T-230) on a live engine:
// a task with an internal host (IP literals, including cloud metadata and
// the WHATWG number-like forms, loopback names — the forms of the unit test
// TestSessionRunBlocksInternalTaskURL) is rejected with the task class before
// the navigation; navigation to a private testbed address (the *.crawl.test zones live
// in 172.28.100.0/24) is cut off by the Fetch interception in the engine itself —
// fetch.failRequest gives net::ERR_BLOCKED_BY_CLIENT (the code is a LoadError,
// the task class). The zone name is resolved by the interception's resolver (the
// dnsCache of T-230, here — engineLiveLookupHost): the name stays public for the
// task and must be internal for the interception.
func engineLiveSSRF(t *testing.T, opts Options) {
	ctx := context.Background()

	// The resolve counter of the seam: the blocking verdict must be made by the
	// interception (the dnsCache via Options.LookupHost, T-230), not by an own
	// blocking of private networks in the engine, if there is one (lightpanda has the
	// --block-private-networks flag): without the resolve call the navigation would not
	// reach the interception, and the ERR_BLOCKED_BY_CLIENT code would prove nothing.
	var lookups atomic.Int32
	opts.BlockInternalAddrs = true
	opts.LookupHost = func(ctx context.Context, host string) ([]netip.Addr, error) {
		lookups.Add(1)
		return engineLiveLookupHost(ctx, host)
	}
	b := engineLiveBrowser(t, opts)
	s, err := b.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer s.Close()

	for _, u := range []string{
		"http://127.0.0.1/",
		"http://10.0.0.5/",
		"http://169.254.169.254/latest/meta-data/",
		"http://localhost/",
		"http://2130706433/",
	} {
		if _, err := s.Run(ctx, RenderTask{URL: u}); err == nil ||
			ClassOf(err) != ClassTask || !strings.Contains(err.Error(), "внутренн") {
			t.Errorf("%s: ожидался отказ SSRF-контура классом task, получено err=%v", u, err)
		}
	}

	// The interception: the zone name passes the task's address control but is
	// resolved by the interception into a private testbed address — the navigation
	// must fail.
	_, err = s.Run(ctx, RenderTask{URL: "http://spa-shell.crawl.test/"})
	if err == nil {
		t.Fatal("навигация на приватный адрес testbed прошла сквозь SSRF-контур")
	}
	if ClassOf(err) != ClassTask {
		t.Errorf("класс %v (%v), хочу task — блокировка контура это ошибка загрузки страницы", ClassOf(err), err)
	}
	code := LoadErrorCode(err)
	if code == "" {
		// Without a load code the reason is invisible (on moli, for example, a
		// Fetch.enable refusal lands here) — we print the whole error, not an empty code.
		t.Fatalf("навигация упала без кода ошибки загрузки (хочу net::ERR_BLOCKED_BY_CLIENT от failRequest перехвата): %v", err)
	}
	if code != "net::ERR_BLOCKED_BY_CLIENT" {
		t.Errorf("код ошибки загрузки %q, хочу net::ERR_BLOCKED_BY_CLIENT (fetch.failRequest перехвата, T-219)", code)
	}
	if n := lookups.Load(); n < 1 {
		t.Errorf("резолв SSRF-контура не вызывался (%d вызовов) — блокировка пришла не из перехвата, код ошибки её не доказывает (T-230)", n)
	}
}

// engineLiveUAOverride — (f) the crawler's identifiable UA (spec §9):
// the Emulation.setUserAgentOverride in the session's starting set is visible to the
// page twice — in the tab's navigator.userAgent and in the User-Agent of the HTTP
// request for the page on the fixture server (the second rules out "the engine
// overrides only the JS property": the header is set by the engine's network stack).
func engineLiveUAOverride(t *testing.T, opts Options) {
	ctx := context.Background()

	opts.UserAgent = "DomainCrawler/engine-live-test (T-254; +https://example.example)"

	var gotUA atomic.Value
	mux := http.NewServeMux()
	mux.HandleFunc("/ua", func(w http.ResponseWriter, r *http.Request) {
		gotUA.Store(r.UserAgent())
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte("<html><body>ua</body></html>"))
	})
	page := liveServe(t, mux) + "/ua"

	b := engineLiveBrowser(t, opts)
	s, err := b.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer s.Close()

	if _, err := s.Run(ctx, RenderTask{URL: page}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	ua, err := chromedp.Run(s.Context(), chromedp.Evaluate[string]("navigator.userAgent"))
	if err != nil {
		t.Fatalf("navigator.userAgent: %v", err)
	}
	if ua != opts.UserAgent {
		t.Errorf("navigator.userAgent %q, хочу %q (ТЗ §9)", ua, opts.UserAgent)
	}
	if sent, _ := gotUA.Load().(string); sent != opts.UserAgent {
		t.Errorf("User-Agent запроса страницы %q, хочу %q — override не дошёл до сетевого стека движка", sent, opts.UserAgent)
	}
}

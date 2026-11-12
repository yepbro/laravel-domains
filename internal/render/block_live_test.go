package render

// The live check of resource blocking (T-062; CSS — the T-213 option;
// tracker domains — the T-214 option) — against
// the real Chromium of the dev stack. Inside the test an HTTP server with
// request counters is raised: the page pulls an image, a font, CSS, a script and
// makes an XHR; the counters prove the fact of the request independently of our own
// CDP events (otherwise the check would be closed onto itself).
//
// The test runs in the tools run container (compose run), Chromium — in
// the neighboring chromium container; both are in the crawler_net network. httptest.NewServer
// will not do here (it listens on 127.0.0.1 — Chromium from another container cannot
// reach it): the server listens on 0.0.0.0, and the page is given this
// container's address in crawler_net — the first non-loopback IPv4 (net.InterfaceAddrs),
// no DNS needed. For T-214 the tracker domains are resolved by the DNS aliases of the
// tools service in compose (c.clarity.ms, mc.yandex.ru → the run container); the aliases
// are applied by the --use-aliases flag of compose run (see the run below), without it
// the run container does not get the service aliases — the control run of the T-214 test
// will say so explicitly.
//
// Running — as in browser_live_test.go:
//
//	docker compose --project-directory . -f deploy/docker/compose.yaml \
//	  run --use-aliases -e CRAWLER_RENDER_TEST=1 --rm tools \
//	  go test ./internal/render -run BlockLive -v -count=1

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crawler/internal/config"
)

// blockLiveCounters — the request counters of the test page's resources.
type blockLiveCounters struct {
	page    atomic.Int64
	css     atomic.Int64
	js      atomic.Int64
	xhr     atomic.Int64
	img     atomic.Int64
	font    atomic.Int64
	favicon atomic.Int64
	// The counters of the T-214 page (tracker.html): the page's own script
	// (trackerJS), the tracker domain probes via the compose DNS aliases
	// (clarityProbe — c.clarity.ms, mcProbe — mc.yandex.ru) and
	// a request with a tracker domain in the PATH but its own host (trackerTrap).
	// claritySync — a separate synchronous probe of c.clarity.ms
	// (an engine-branched case, moli#2728): the counter is separate from
	// clarityProbe, otherwise "sync gets through" on moli would mask
	// "async is blocked".
	clarityProbe atomic.Int64
	mcProbe      atomic.Int64
	trackerJS    atomic.Int64
	trackerTrap  atomic.Int64
	claritySync  atomic.Int64
}

// blockLiveServe — the test page with request counters: one for both
// blocking live tests (the T-062 default and the T-213 option). Returns the counters
// and the page address; the server lives until the end of the test (Cleanup).
func blockLiveServe(t *testing.T) (*blockLiveCounters, string) {
	t.Helper()
	c := &blockLiveCounters{}
	mux := http.NewServeMux()
	mux.HandleFunc("/page.html", func(w http.ResponseWriter, _ *http.Request) {
		c.page.Add(1)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = fmt.Fprint(w, `<!doctype html>
<html><head>
<link rel="stylesheet" href="/style.css">
<style>@font-face{font-family:BlockLive;font-display:block;
  src:url(/font.woff2) format('woff2')} body{font-family:BlockLive}</style>
</head><body>
<img src="/img.png">
текст для загрузки шрифта
<script src="/app.js"></script>
</body></html>`)
	})
	mux.HandleFunc("/style.css", func(w http.ResponseWriter, _ *http.Request) {
		c.css.Add(1)
		w.Header().Set("Content-Type", "text/css")
		_, _ = fmt.Fprint(w, "body{color:#333}")
	})
	mux.HandleFunc("/app.js", func(w http.ResponseWriter, _ *http.Request) {
		c.js.Add(1)
		w.Header().Set("Content-Type", "text/javascript")
		// A synchronous XHR: it runs while the script is parsed — by the
		// load event the xhr counter is guaranteed to be set.
		_, _ = fmt.Fprint(w, `var x=new XMLHttpRequest();
x.open('GET','/data.json',false);x.send();`)
	})
	mux.HandleFunc("/data.json", func(w http.ResponseWriter, _ *http.Request) {
		c.xhr.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"ok":true}`)
	})
	mux.HandleFunc("/img.png", func(w http.ResponseWriter, _ *http.Request) {
		c.img.Add(1)
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write([]byte{0x89, 'P', 'N', 'G'})
	})
	mux.HandleFunc("/font.woff2", func(w http.ResponseWriter, _ *http.Request) {
		c.font.Add(1)
		w.Header().Set("Content-Type", "font/woff2")
		_, _ = w.Write([]byte("wOF2"))
	})
	mux.HandleFunc("/favicon.ico", func(w http.ResponseWriter, _ *http.Request) {
		c.favicon.Add(1)
		w.Header().Set("Content-Type", "image/x-icon")
		_, _ = w.Write(nil)
	})
	// The T-214 page (block_tracker_domains). The tracker domains are resolved
	// by the Docker DNS via the compose aliases (deploy/docker/compose.yaml, the
	// tools service: www.google-analytics.com and mc.yandex.ru → this same
	// run container): the hermetic unbound does not know such names, and without
	// a resolve "blocked by the interception" would be indistinguishable from
	// a DNS failure. The probes are asynchronous XHRs with the outcome in a <body>
	// attribute (CORS: we read the status too): the control run with the option
	// off must DELIVER them (proves resolve+serving), the working one —
	// cut them off before sending (proves the blocking by the server counter).
	// Async, not sync: real web analytics (clarity, mc.yandex,
	// google-analytics) travels async/fetch/beacon, and only an async XHR
	// is blocked by the interception on all engines — under the interception
	// moli's sync XHR goes to the network past the pause (the #2728 divergence,
	// a separate engine-branched probe below). The async probe's outcome is written into the
	// attribute on the request's completion: the guarantee here is not a waiting mechanism
	// (the quiet window of wait.go is counted from the last requestWillBeSent and
	// a request completion does not hold it) but the headroom: the fixture replies within
	// milliseconds, the Quiet window is 2s; a "slow" probe would require
	// explicit polling (the blockLiveWaitReady style).
	port := 0 // the server's port — filled in after Listen
	mux.HandleFunc("/tracker.html", func(w http.ResponseWriter, _ *http.Request) {
		c.page.Add(1)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = fmt.Fprint(w, `<!doctype html>
<html><body>
<script src="/tracker-app.js"></script>
</body></html>`)
	})
	mux.HandleFunc("/tracker-app.js", func(w http.ResponseWriter, _ *http.Request) {
		c.trackerJS.Add(1)
		w.Header().Set("Content-Type", "text/javascript")
		_, _ = fmt.Fprintf(w, `
function probe(attr, url) {
  try {
    var x = new XMLHttpRequest();
    x.open('GET', url, true);
    x.onload = function () { document.body.setAttribute(attr, 'loaded ' + x.status); };
    x.onerror = function () { document.body.setAttribute(attr, 'error'); };
    x.send();
  } catch (e) {
    document.body.setAttribute(attr, 'error');
  }
}
function probeSync(attr, url) {
  try {
    var x = new XMLHttpRequest();
    x.open('GET', url, false);
    x.send();
    document.body.setAttribute(attr, 'loaded ' + x.status);
  } catch (e) {
    document.body.setAttribute(attr, 'error');
  }
}
probe('clarity-xhr', 'http://c.clarity.ms:`+"%[1]d"+`/clarity-probe');
probe('mc-xhr', 'http://mc.yandex.ru:`+"%[1]d"+`/mc-probe');
probe('path-trap', '/js/google-analytics.com.js');
probeSync('sync-xhr', 'http://c.clarity.ms:`+"%[1]d"+`/clarity-sync-probe');
`, port)
	})
	clarityProbe := func(w http.ResponseWriter, _ *http.Request) {
		c.clarityProbe.Add(1)
		w.Header().Set("Content-Type", "text/javascript")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		_, _ = fmt.Fprint(w, "// clarity.ms probe")
	}
	mux.HandleFunc("/clarity-probe", clarityProbe)
	// A separate endpoint of the sync probe: the claritySync counter must differ
	// from clarityProbe — on moli the sync goes to the network (200) while the
	// async is blocked; a shared counter would hide this divergence.
	mux.HandleFunc("/clarity-sync-probe", func(w http.ResponseWriter, _ *http.Request) {
		c.claritySync.Add(1)
		w.Header().Set("Content-Type", "text/javascript")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		_, _ = fmt.Fprint(w, "// sync probe clarity.ms (the engine-branched case)")
	})
	mux.HandleFunc("/mc-probe", func(w http.ResponseWriter, _ *http.Request) {
		c.mcProbe.Add(1)
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		_, _ = fmt.Fprint(w, "ok")
	})
	mux.HandleFunc("/js/google-analytics.com.js", func(w http.ResponseWriter, _ *http.Request) {
		c.trackerTrap.Add(1)
		w.Header().Set("Content-Type", "text/javascript")
		_, _ = fmt.Fprint(w, "// a tracker domain name in the path, the host is the test server")
	})

	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	// Before Serve starts: the handler goroutine reads port without synchronization.
	port = ln.Addr().(*net.TCPAddr).Port
	srv := &http.Server{Handler: mux}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(ln)
	}()
	t.Cleanup(func() {
		_ = srv.Close()
		<-done
	})
	return c, "http://" + net.JoinHostPort(liveIPv4(t), fmt.Sprint(port)) + "/page.html"
}

// blockLiveRun — the page in a session with the given options (the shared
// wiring of the tests): a browser start, a session, Run, closing the session.
func blockLiveRun(t *testing.T, opts Options, pageURL string) RenderOutput {
	t.Helper()
	ctx := context.Background()
	b, err := StartBrowser(ctx, opts)
	if err != nil {
		t.Fatalf("StartBrowser: %v", err)
	}
	defer b.Close()
	s, err := b.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	out, err := s.Run(ctx, RenderTask{URL: pageURL})
	s.Close()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return out
}

// blockLiveWaitReady — polling until the must-arrive resource counters
// (page/script/XHR) are ready: Navigate waits for the load event, usually they are
// already set; the polling guards against slow delivery. An expiry — an error
// right away with the reason (the waitFor style of the unit tests): further counter
// checks would give a cascade of uninformative "requested 0 times".
// The blocked counters can only stay zero: a request attempt is blocked before
// sending at any moment of the page's life, there is no race.
func blockLiveWaitReady(t *testing.T, c *blockLiveCounters) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c.page.Load() >= 1 && c.js.Load() >= 1 && c.xhr.Load() >= 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("the resources bound to get through were not requested in 10 s: page=%d js=%d xhr=%d",
		c.page.Load(), c.js.Load(), c.xhr.Load())
}

// TestBlockLiveResources — the image and the font are not requested (Fetch.failRequest
// before sending), CSS/script/XHR are requested (they pass, JS works — spec 5.6):
// the default T-062 set, CSS is not blocked by the option.
func TestBlockLiveResources(t *testing.T) {
	skipUnlessLive(t)
	c, pageURL := blockLiveServe(t)

	out := blockLiveRun(t, liveOptions(), pageURL)
	if out.FinalURL != pageURL {
		t.Errorf("final URL %q, want %q", out.FinalURL, pageURL)
	}

	blockLiveWaitReady(t, c)
	if got := c.page.Load(); got < 1 {
		t.Errorf("the page was requested %d times, want >= 1", got)
	}
	if got := c.css.Load(); got < 1 {
		t.Errorf("CSS was requested %d times, want >= 1 (not blocked)", got)
	}
	if got := c.js.Load(); got < 1 {
		t.Errorf("the script was requested %d times, want >= 1 (not blocked)", got)
	}
	if got := c.xhr.Load(); got < 1 {
		t.Errorf("XHR was requested %d times, want >= 1 (JS must work)", got)
	}
	if got := c.img.Load(); got != 0 {
		t.Errorf("the image was requested %d times, want 0 (blocked, spec 5.6)", got)
	}
	if got := c.font.Load(); got != 0 {
		t.Errorf("the font was requested %d times, want 0 (blocked, spec 5.6)", got)
	}
	// A favicon initiated by the browser (not the page) may arrive as the
	// Other type and pass — the pinned decision (see blockBlockedTypes).
	// In headless-shell there are usually no such requests; the counter is for
	// diagnostics only.
	t.Logf("favicon.ico was requested %d times (the Other type is not blocked)",
		c.favicon.Load())
}

// TestBlockLiveStylesheetBlocked — the block_stylesheet option (T-213): CSS is
// not requested at all (failRequest before sending), the page lives — the script and the
// XHR pass, JS works, the DOM is captured: a CSS block does not break the render as
// such (the option's risk — computed styles on rare sites, hence the default
// false and enabling — after an A/B).
func TestBlockLiveStylesheetBlocked(t *testing.T) {
	skipUnlessLive(t)
	c, pageURL := blockLiveServe(t)

	opts := liveOptions()
	opts.BlockStylesheet = true
	out := blockLiveRun(t, opts, pageURL)
	if out.FinalURL != pageURL {
		t.Errorf("final URL %q, want %q", out.FinalURL, pageURL)
	}

	blockLiveWaitReady(t, c)
	if got := c.page.Load(); got < 1 {
		t.Errorf("the page was requested %d times, want >= 1", got)
	}
	if got := c.css.Load(); got != 0 {
		t.Errorf("CSS was requested %d times, want 0 (blocked by the option, T-213)", got)
	}
	if got := c.js.Load(); got < 1 {
		t.Errorf("the script was requested %d times, want >= 1 (not blocked)", got)
	}
	if got := c.xhr.Load(); got < 1 {
		t.Errorf("XHR was requested %d times, want >= 1 (JS must work)", got)
	}
	if got := c.img.Load(); got != 0 {
		t.Errorf("the image was requested %d times, want 0 (blocked, spec 5.6)", got)
	}
	if got := c.font.Load(); got != 0 {
		t.Errorf("the font was requested %d times, want 0 (blocked, spec 5.6)", got)
	}
}

// domAttr — the value of a body attribute from the final DOM (the T-214 live
// test): the attributes are set by the page itself with the outcomes of its XHRs — they
// are read from the public render result. A missing attribute — an empty string.
func domAttr(dom, name string) string {
	i := strings.Index(dom, name+"=\"")
	if i < 0 {
		return ""
	}
	rest := dom[i+len(name)+2:]
	if j := strings.IndexByte(rest, '"'); j >= 0 {
		return rest[:j]
	}
	return rest
}

// TestBlockLiveTrackerDomains — the block_tracker_domains option (T-214)
// against a live Chromium (the differential — blockLiveTrackerDomains; the
// mechanics details and the probes' rationale — in the helper's doc comment).
func TestBlockLiveTrackerDomains(t *testing.T) {
	skipUnlessLive(t)
	blockLiveTrackerDomains(t, liveOptions())
}

// blockLiveTrackerDomains — the differential of two runs of one page
// (the tracker domains are resolved by the compose DNS aliases into this same run
// container, the server counters — evidence independent of the CDP events); opts —
// the browser of the run (the control one — as passed, the working one — a copy with
// BlockTrackerDomains). The parameter — the per-engine slice of T-254
// (engines_live_test.go) and a live Chromium (liveOptions):
//
//   - the control (the option off): the probes of both tracker domains REACH
//     the server and are read (loaded 200) — otherwise the test proves nothing
//     (without a resolve a DNS failure is indistinguishable from blocking);
//   - the working run (the option on): the probes are cut off BEFORE sending
//     (Fetch.failRequest — the counters are zero), the page's own script
//     works, and the XHR over the path with a tracker domain in the file name (the
//     host is our server) passes: the substring pause of the urlPattern is released by
//     the exact host check.
//
// The probes are async XHRs: real web analytics (clarity, mc.yandex,
// google-analytics) travels async/fetch/beacon, and the T-214 contract is exactly
// "tracker hits into the network are stopped on any engine". Sync XHR
// is left as a separate engine-branched probe: under the interception moli
// does NOT pause sync XHR and it goes to the network the regular way — the pause
// is architecturally impossible (an owner-loop deadlock), the failure factory was removed
// (lexmount/moli#2728, PR #2729; PR #2704 also lifts the sync-XHR ban).
// The documented divergence from Chromium: there sync XHR is paused and
// blocked fully; on moli a deliberate blocking of sync XHR is
// unavailable — hence the main probes are async, and the sync probe checks the
// expectation PER ENGINE (a switch over opts.Engine, as in the smoke contracts):
// chromium — "blocked" (error, the counter 0), moli — "went to the network,
// 200, not blocked" (the counter 1); the other engines — SKIP: the
// sync XHR semantics under the interception is not documented for them.
//
// The probes: c.clarity.ms — a subdomain of a list domain (suffix matching),
// mc.yandex.ru — an exact list host. Google analytics (google-analytics,
// googletagmanager, doubleclick) is probed by the unit tests but not live:
// the google domains are in Chromium's HSTS preload — an http probe is forcibly
// upgraded to https and fails on TLS with the test's HTTP server.
func blockLiveTrackerDomains(t *testing.T, opts Options) {
	t.Helper()

	// The control run: a fresh server — only this run's counters.
	cOff, offPage := blockLiveServe(t)
	offURL := strings.TrimSuffix(offPage, "page.html") + "tracker.html"
	offOut := blockLiveRun(t, opts, offURL)
	if got := cOff.clarityProbe.Load(); got < 1 {
		t.Fatalf("control: the clarity.ms probe did not reach the server (%d; the clarity-xhr attribute %q, mc-xhr %q) — "+
			"check the --use-aliases flag of compose run and the tools service DNS aliases (T-214)",
			got, domAttr(offOut.DOM, "clarity-xhr"), domAttr(offOut.DOM, "mc-xhr"))
	}
	if got := cOff.mcProbe.Load(); got < 1 {
		t.Fatalf("control: the mc.yandex.ru probe did not reach the server (%d) — "+
			"check the --use-aliases flag of compose run and the tools service DNS aliases (T-214)", got)
	}
	if attr := domAttr(offOut.DOM, "clarity-xhr"); attr != "loaded 200" {
		t.Errorf("control: clarity-xhr %q, want loaded 200 (no blocking)", attr)
	}
	if attr := domAttr(offOut.DOM, "mc-xhr"); attr != "loaded 200" {
		t.Errorf("control: mc-xhr %q, want loaded 200 (no blocking)", attr)
	}
	// The control sync probe reaches the server and is read on both engines: without
	// the blocking enabled the #2728 divergence does not manifest — the branching
	// per engine is needed only in the working run below.
	if attr := domAttr(offOut.DOM, "sync-xhr"); attr != "loaded 200" {
		t.Errorf("control: sync-xhr %q, want loaded 200 (no blocking)", attr)
	}

	// The working run: the option is on.
	cOn, onPage := blockLiveServe(t)
	onURL := strings.TrimSuffix(onPage, "page.html") + "tracker.html"
	onOpts := opts
	onOpts.BlockTrackerDomains = true
	onOut := blockLiveRun(t, onOpts, onURL)
	if onOut.FinalURL != onURL {
		t.Errorf("final URL %q, want %q", onOut.FinalURL, onURL)
	}

	// The must-arrive ones: the page and its own script (JS alive, the site's own
	// domain untouched) — both are synchronous/precede load, usually already set;
	// the polling guards against slow delivery (the blockLiveWaitReady style).
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) &&
		(cOn.page.Load() < 1 || cOn.trackerJS.Load() < 1) {
		time.Sleep(50 * time.Millisecond)
	}
	if got := cOn.page.Load(); got < 1 {
		t.Fatalf("the page was not requested in 10 s — the T-214 page test did not run")
	}
	if got := cOn.trackerJS.Load(); got < 1 {
		t.Fatalf("the own script was not requested in 10 s (the blocking must not touch its own domain)")
	}

	// The trackers are cut off before sending: the counters are zero and the attributes are an error.
	if got := cOn.clarityProbe.Load(); got != 0 {
		t.Errorf("the clarity.ms probe reached the server %d times, want 0 (blocked, T-214)", got)
	}
	if got := cOn.mcProbe.Load(); got != 0 {
		t.Errorf("the mc.yandex.ru probe reached the server %d times, want 0 (blocked, T-214)", got)
	}
	if attr := domAttr(onOut.DOM, "clarity-xhr"); attr != "error" {
		t.Errorf("clarity-xhr %q, want error (blocked)", attr)
	}
	if attr := domAttr(onOut.DOM, "mc-xhr"); attr != "error" {
		t.Errorf("mc-xhr %q, want error (blocked)", attr)
	}

	// A false positive: the path contains a tracker domain, the host is our
	// server; the request must arrive (the counter) and be read (200).
	if got := cOn.trackerTrap.Load(); got != 1 {
		t.Errorf("requests to /js/google-analytics.com.js: %d, want 1 (the host is not a tracker — it passes)", got)
	}
	if attr := domAttr(onOut.DOM, "path-trap"); attr != "loaded 200" {
		t.Errorf("path-trap %q, want loaded 200 (the host is not a tracker — it passes)", attr)
	}

	// The sync XHR probe — an engine-branched case (T-275): under the
	// interception moli does not pause sync XHR and it goes to the network (the documented
	// divergence lexmount/moli#2728, PR #2729): "went to the network, 200, not
	// blocked". Chromium blocks sync XHR fully — "blocked". This is exactly
	// why the main probes of the test are async (the T-214 contract on any engine),
	// and the sync one checks the behavior of the concrete engine. The branching over
	// opts.Engine, as in the smoke contracts. On moli without #2729 (v1.1.23 and
	// the integration builds before it) sync XHR under the interception was crashed
	// by the failure factory "Synchronous XMLHttpRequest interception is not
	// supported" — there this branch is red (the slice must show
	// the engine divergence/regression until the fix); on v1.1.21/1.1.22
	// it was red earlier and for another reason — InvalidParams on Fetch.enable.
	// The other engines (lightpanda): the sync XHR semantics under the
	// interception is not documented — an honest SKIP instead of an unchecked assert.
	switch opts.Engine {
	case config.EngineMoli:
		if got := cOn.claritySync.Load(); got < 1 {
			t.Errorf("the sync probe clarity.ms did not reach the server %d times, want >= 1 "+
				"(moli: sync-XHR is not blocked, discrepancy #2728)", got)
		}
		if attr := domAttr(onOut.DOM, "sync-xhr"); attr != "loaded 200" {
			t.Errorf("sync-xhr %q, want loaded 200 (moli: sync-XHR goes to the network, #2728)", attr)
		}
	case "", config.EngineChromium:
		// The %q engine in the hint — diagnostics of a single-test run with
		// CRAWLER_RENDER_REMOTE_URL on a foreign engine: liveOptions does not
		// fill in Engine, the semantics are taken to be chromium — a red here would look like
		// a blocking failure. moli — only via the per-engine slice
		// (MOLI_WS_URL, EnginesLive).
		if got := cOn.claritySync.Load(); got != 0 {
			t.Errorf("sync-the clarity.ms probe reached the server %d times, want 0 (blocked; the engine %q — for moli sync-XHR is not blocked, use the per-engine slice MOLI_WS_URL)",
				got, opts.Engine)
		}
		if attr := domAttr(onOut.DOM, "sync-xhr"); attr != "error" {
			t.Errorf("sync-xhr %q, want error (blocked; the engine %q — for moli sync-XHR is not blocked, use the per-engine slice MOLI_WS_URL)",
				attr, opts.Engine)
		}
	default:
		t.Skipf("the sync-XHR behavior under interception is not documented for the engine %q — the assert is only for chromium/moli (#2728)", opts.Engine)
	}
}

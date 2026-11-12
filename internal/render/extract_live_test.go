package render

// Live checks of the T-064 result capture against the real Chromium of the dev
// stack (the enabling and the address — as in browser_live_test.go:
// CRAWLER_RENDER_TEST=1, the compose service chromium). Two levels:
//
//   - a static page of the HTTP fixture (liveServe): the exact outerHTML
//     after the script runs (the tree is finished by JS), no cookies,
//     the response headers of the main navigation captured from a real network
//     response (data: pages with a pseudo-response are impossible after R-09:
//     validateTaskURL rejects non-http schemes, T-216);
//   - the internal page spa-shell.crawl.test (the testbed): a JS cookie,
//     set by the script synchronously during parsing (document.cookie — before
//     the load event), is visible in the CDP set; the response headers of the
//     main navigation captured.
//
// The async content of spa-shell (setTimeout 150 ms) is not checked here:
// waiting for the network quiet — T-063; without it the capture runs right after load.

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// TestExtractLiveStaticPage — the result capture on a static fixture
// page: the script changes the text synchronously — the finishing touches
// must be in the final DOM by the moment of capture (after the load event);
// no cookies (a clean isolated context, the page sets none); the headers come from
// the real response of the fixture server.
func TestExtractLiveStaticPage(t *testing.T) {
	skipUnlessLive(t)
	ctx := context.Background()

	mux := http.NewServeMux()
	mux.HandleFunc("/dom", func(w http.ResponseWriter, _ *http.Request) {
		// X-Live-Fixture — the fixture's signature header: its presence in the
		// result proves that the headers were captured from the real response
		// of the server (Content-Type alone could have come from a pseudo-response too).
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("X-Live-Fixture", "1")
		_, _ = fmt.Fprint(w, "<html><head><title>ext</title></head><body>"+
			`<div id="d">before</div>`+
			`<script>document.getElementById("d").textContent="after-js";</script>`+
			"</body></html>")
	})
	page := liveServe(t, mux) + "/dom"

	b, err := StartBrowser(ctx, liveOptions())
	if err != nil {
		t.Fatalf("StartBrowser: %v", err)
	}
	defer b.Close()

	s, err := b.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer s.Close()

	out, err := s.Run(ctx, RenderTask{URL: page})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.FinalURL != page {
		t.Errorf("итоговый URL %q, хочу %q", out.FinalURL, page)
	}
	want := "<html><head><title>ext</title></head><body>" +
		`<div id="d">after-js</div>` +
		`<script>document.getElementById("d").textContent="after-js";</script>` +
		"</body></html>"
	if out.DOM != want {
		t.Fatalf("итоговый DOM:\n got  %q\nwant %q", out.DOM, want)
	}
	if len(out.Cookies) != 0 {
		t.Errorf("cookies у страницы без них: %+v, хочу пусто", out.Cookies)
	}
	// The headers of the real main navigation response: the fixture server's
	// Content-Type and the signature X-Live-Fixture (the keys are canonical, as in the
	// HTTP stage; the full set of the network response's headers is not pinned —
	// Date etc. are added by the server itself).
	if ct := out.ResponseHeaders.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type %q, хочу text/html…", ct)
	}
	if got := out.ResponseHeaders.Get("X-Live-Fixture"); got != "1" {
		t.Errorf("X-Live-Fixture %q, хочу 1 — заголовки не с реального ответа фикстуры", got)
	}
}

func TestExtractLiveInternalPage(t *testing.T) {
	skipUnlessLive(t)
	ctx := context.Background()

	b, err := StartBrowser(ctx, liveOptions())
	if err != nil {
		t.Fatalf("StartBrowser: %v", err)
	}
	defer b.Close()

	s, err := b.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer s.Close()

	// spa-shell.crawl.test: the testbed scenario "DOM and cookies from JS" — the script
	// sets document.cookie synchronously (before load) and draws content into
	// #root on a timer (after load — the T-063 waiting territory).
	out, err := s.Run(ctx, RenderTask{URL: "http://spa-shell.crawl.test/"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.FinalURL != "http://spa-shell.crawl.test/" {
		t.Errorf("итоговый URL %q", out.FinalURL)
	}

	// The JS cookie is visible in the CDP set: the context store is read by the command,
	// not document.cookie.
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

	// The page tree after the load: the SPA skeleton is in place.
	if !strings.Contains(out.DOM, `<div id="root">`) {
		t.Errorf("итоговый DOM без каркаса SPA: %q", out.DOM)
	}
	if !strings.HasPrefix(out.DOM, "<html") {
		t.Errorf("итоговый DOM не с корневого элемента: %q", out.DOM)
	}

	// The response headers of the main navigation (spec 5.8): the keys are
	// canonical, as in the HTTP stage.
	if ct := out.ResponseHeaders.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type %q, хочу text/html…", ct)
	}
	if out.ResponseHeaders.Get("Server") == "" {
		t.Errorf("заголовок Server не снят: %v", out.ResponseHeaders)
	}
}

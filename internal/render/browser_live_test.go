package render

// Browser (live) tests — against the real Chromium of the dev stack
// (the compose service chromium, ws://172.28.100.14:9222 from the crawler_net
// network). Skipped by default; enabled by CRAWLER_RENDER_TEST=1:
//
//	docker compose --project-directory . -f deploy/docker/compose.yaml \
//	  run -e CRAWLER_RENDER_TEST=1 --rm tools \
//	  go test ./internal/render -run BrowserLive -v -count=1
//
// The Chromium address: CRAWLER_RENDER_REMOTE_URL, then CHROMIUM_WS_URL
// (set in the dev containers of compose), otherwise the default stack address.
// The flag also works in the worktree agents of the following tasks (T-062…T-067):
// the same run from a worktree directory uses the main stack.

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

func liveEnabled() bool { return os.Getenv("CRAWLER_RENDER_TEST") == "1" }

func skipUnlessLive(t *testing.T) {
	t.Helper()
	if !liveEnabled() {
		t.Skip("живой Chromium не включён: CRAWLER_RENDER_TEST=1 (см. докомментарий файла)")
	}
}

func liveOptions() Options {
	ws := os.Getenv("CRAWLER_RENDER_REMOTE_URL")
	if ws == "" {
		ws = os.Getenv("CHROMIUM_WS_URL")
	}
	if ws == "" {
		ws = "ws://172.28.100.14:9222"
	}
	return Options{
		RemoteURL:         ws,
		Contexts:          2,
		Timeout:           20 * time.Second,
		RestartEveryPages: 1000,
	}
}

// liveIPv4 — the address of the run container in crawler_net: the HTTP fixtures of the
// live tests listen on 0.0.0.0, and Chromium lives in a neighboring container of the
// stack — it needs the non-loopback address of this container (the first IPv4 from
// net.InterfaceAddrs), no DNS required.
func liveIPv4(t *testing.T) string {
	t.Helper()
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatalf("InterfaceAddrs: %v", err)
	}
	for _, a := range addrs {
		ipn, ok := a.(*net.IPNet)
		if ok && !ipn.IP.IsLoopback() && ipn.IP.To4() != nil {
			return ipn.IP.String()
		}
	}
	t.Fatal("нет не-loopback IPv4: run-контейнер обязан быть в crawler_net")
	return ""
}

// liveServe — the HTTP server of the live tests' page fixtures (the pattern of
// block_live_test.go): it listens on 0.0.0.0 — httptest.NewServer with 127.0.0.1
// will not do, Chromium from the neighboring container cannot reach it. Returns
// the base URL (http://<container-address>:<port>); the server lives until the end
// of the test (Cleanup). data: pages (the "cheap" hermeticity of the early
// live tests) are impossible after R-09: validateTaskURL rejects non-http
// schemes, so the tasks are ordinary http fixture pages (T-216).
func liveServe(t *testing.T, mux *http.ServeMux) string {
	t.Helper()
	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
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
	return "http://" + net.JoinHostPort(liveIPv4(t), fmt.Sprint(ln.Addr().(*net.TCPAddr).Port))
}

func TestBrowserLivePoolRender(t *testing.T) {
	skipUnlessLive(t)
	ctx := context.Background()

	// Fixtures: the page's title = the path without the slash (/ok-N → "ok-N").
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = fmt.Fprintf(w, "<title>%s</title>hello", strings.TrimPrefix(r.URL.Path, "/"))
	})
	base := liveServe(t, mux)

	b, err := StartBrowser(ctx, liveOptions())
	if err != nil {
		t.Fatalf("StartBrowser: %v", err)
	}
	p := NewPool(b)
	defer p.Close()

	// More tasks than slots: the fourth and onward wait for a free slot.
	const n = 4
	var wg sync.WaitGroup
	errs := make([]error, n)
	outs := make([]RenderOutput, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			outs[i], errs[i] = p.Run(ctx, RenderTask{URL: fmt.Sprintf("%s/ok-%d", base, i)})
		}()
	}
	wg.Wait()

	for i := range n {
		if errs[i] != nil {
			t.Errorf("задача %d: %v", i, errs[i])
			continue
		}
		want := fmt.Sprintf("ok-%d", i)
		if outs[i].Title != want {
			t.Errorf("задача %d: title %q, хочу %q", i, outs[i].Title, want)
		}
		if wantURL := fmt.Sprintf("%s/ok-%d", base, i); outs[i].FinalURL != wantURL {
			t.Errorf("задача %d: final URL %q, хочу %q", i, outs[i].FinalURL, wantURL)
		}
	}
	if got := b.Pages(); got != n {
		t.Errorf("счётчик страниц %d, хочу %d", got, n)
	}
	if !b.Alive() {
		t.Error("браузер должен жить после задач")
	}
}

func TestBrowserLiveContextIsolation(t *testing.T) {
	skipUnlessLive(t)
	ctx := context.Background()

	b, err := StartBrowser(ctx, liveOptions())
	if err != nil {
		t.Fatalf("StartBrowser: %v", err)
	}
	defer b.Close()

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

	// The first session puts the cookie into its own incognito profile.
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

	// A new session = a new profile: the cookie did not survive the context change.
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

func TestBrowserLiveUserAgent(t *testing.T) {
	skipUnlessLive(t)
	ctx := context.Background()

	opts := liveOptions()
	opts.UserAgent = "DomainCrawler/live-test (+https://example.example)"
	b, err := StartBrowser(ctx, opts)
	if err != nil {
		t.Fatalf("StartBrowser: %v", err)
	}
	defer b.Close()

	s, err := b.NewSession(ctx)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer s.Close()
	ua, err := chromedp.Run(s.Context(), chromedp.Evaluate[string]("navigator.userAgent"))
	if err != nil {
		t.Fatalf("navigator.userAgent: %v", err)
	}
	if ua != opts.UserAgent {
		t.Fatalf("User-Agent вкладки %q, хочу %q (ТЗ §9)", ua, opts.UserAgent)
	}
}

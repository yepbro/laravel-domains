package render

// Live tests of the wait strategy (T-063) — against the real Chromium
// of the dev stack. Enabling and the address — as in
// browser_live_test.go: CRAWLER_RENDER_TEST=1 (the address:
// CRAWLER_RENDER_REMOTE_URL, then CHROMIUM_WS_URL, by default
// ws://172.28.100.14:9222).
//
//	The pages are HTTP fixtures (liveServe in browser_live_test.go): no
//	external network needed; the "noisy" page spawns fetch network
//	requests to TEST-NET (192.0.2.1, RFC 5737) — the
//	Network.requestWillBeSent event arrives at the moment the request
//	is initiated, no response is needed. data: pages (hermeticity
//	without a server) are impossible after R-09: validateTaskURL
//	rejects non-http schemes (T-216).

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// waitLiveServe — the pages for the wait strategies on a local server.
// quiet — a quiet page (load and not a single request after); noisy —
// background traffic: a request every 300 ms to TEST-NET, no responses
// arrive (the testbed network will drop them or the router will drop
// them silently) — for the quiet window only the initiating event
// Network.requestWillBeSent matters; stuck — an endless
// parser-blocking script: the load event never fires (the script does
// not finish) and the result capture (Runtime.evaluate of the title)
// hangs on the busy renderer — the overall timeout cuts the whole
// visit.
func waitLiveServe(t *testing.T) (quiet, noisy, stuck string) {
	t.Helper()
	page := func(body string) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = fmt.Fprint(w, body)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/quiet", page(`<title>settle-quiet</title>hello`))
	mux.HandleFunc("/noisy", page(`<title>noisy</title><script>`+
		`setInterval(function(){fetch("http://192.0.2.1/ping").catch(function(){})},300)`+
		`</script>`))
	mux.HandleFunc("/stuck", page(`<title>stuck</title><script>for(;;);</script>`))
	base := liveServe(t, mux)
	return base + "/quiet", base + "/noisy", base + "/stuck"
}

func TestWaitLiveSettleQuiet(t *testing.T) {
	skipUnlessLive(t)
	ctx := context.Background()

	opts := liveOptions()
	// The liveOptions budget is 20 s: the plan — load 10 s, the window
	// 2 s, the settle cap 4 s. The quiet page must finish by the window
	// (≈2 s), not by the cap and not by the overall timeout.
	b, err := StartBrowser(ctx, opts)
	if err != nil {
		t.Fatalf("StartBrowser: %v", err)
	}
	p := NewPool(b)
	defer p.Close()

	want, _, _ := waitLiveServe(t)
	start := time.Now()
	out, err := p.Run(ctx, RenderTask{URL: want})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Title != "settle-quiet" || out.FinalURL != want {
		t.Fatalf("результат %+v", out)
	}
	plan := DefaultWaitPlan(opts.Timeout)
	if elapsed < plan.Quiet-100*time.Millisecond {
		t.Errorf("визит %s: окно затишья %s не выдержано (раньше окна)", elapsed, plan.Quiet)
	}
	if elapsed >= plan.Settle {
		t.Errorf("визит %s дошёл до потолка затишья %s — тихая страница должна заканчиваться окном", elapsed, plan.Settle)
	}
	if elapsed >= opts.Timeout {
		t.Errorf("визит %s дотянул до общего таймаута %s — тихая страница должна заканчиваться окном", elapsed, opts.Timeout)
	}
}

func TestWaitLiveSettleNoisy(t *testing.T) {
	skipUnlessLive(t)
	ctx := context.Background()

	opts := liveOptions()
	// The liveOptions budget is 20 s: the plan — load 10 s, the window
	// 2 s, the settle cap 4 s. Continuous requests keep moving the
	// window, and the visit must finish by the settle cap (≈4 s):
	// earlier — the window would "collapse" (the strategy sees no
	// requests), later — the overall timeout.
	b, err := StartBrowser(ctx, opts)
	if err != nil {
		t.Fatalf("StartBrowser: %v", err)
	}
	p := NewPool(b)
	defer p.Close()

	_, want, _ := waitLiveServe(t)
	start := time.Now()
	out, err := p.Run(ctx, RenderTask{URL: want})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Run: %v (потолок затишья — не ошибка)", err)
	}
	if out.Title != "noisy" || out.FinalURL != want {
		t.Fatalf("результат %+v", out)
	}
	plan := DefaultWaitPlan(opts.Timeout)
	if elapsed < plan.Settle-100*time.Millisecond {
		t.Errorf("визит %s закончился раньше потолка затишья %s — окно подтвердилось при непрерывных запросах", elapsed, plan.Settle)
	}
	if elapsed >= opts.Timeout {
		t.Errorf("визит %s дотянул до общего таймаута %s", elapsed, opts.Timeout)
	}
}

func TestWaitLiveOverallTimeout(t *testing.T) {
	skipUnlessLive(t)
	ctx := context.Background()

	opts := liveOptions()
	// A tiny overall timeout: the plan — load 1 s (will not fire), the
	// window 200 ms, the settle cap 400 ms; the result capture hangs on
	// the busy renderer — the overall timeout must cut the visit off
	// with a task error.
	opts.Timeout = 2 * time.Second
	b, err := StartBrowser(ctx, opts)
	if err != nil {
		t.Fatalf("StartBrowser: %v", err)
	}
	p := NewPool(b)
	defer p.Close()

	_, _, stuck := waitLiveServe(t)
	start := time.Now()
	_, err = p.Run(ctx, RenderTask{URL: stuck})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("ожидана ошибка общего таймаута, получен успех")
	}
	if ClassOf(err) != ClassTask {
		t.Errorf("класс %v (%v), хочу task — таймаут страницы без повторов контекста", ClassOf(err), err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("ошибка %v не содержит context.DeadlineExceeded", err)
	}
	if elapsed < opts.Timeout {
		t.Errorf("визит %s оборван раньше общего таймаута %s", elapsed, opts.Timeout)
	}
	// A task error — the page counter does not grow, there is no retry.
	if got := b.Pages(); got != 0 {
		t.Errorf("счётчик страниц %d, хочу 0 (неуспешная задача)", got)
	}
}

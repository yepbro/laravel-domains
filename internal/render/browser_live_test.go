package render

// Браузерные (live) тесты — против реального Chromium dev-стека
// (compose-сервис chromium, ws://172.28.100.14:9222 из сети crawler_net).
// По умолчанию пропускаются; включаются переменной CRAWLER_RENDER_TEST=1:
//
//	docker compose --project-directory . -f deploy/docker/compose.yaml \
//	  run -e CRAWLER_RENDER_TEST=1 --rm tools \
//	  go test ./internal/render -run BrowserLive -v -count=1
//
// Адрес Chromium: CRAWLER_RENDER_REMOTE_URL, затем CHROMIUM_WS_URL
// (задан в dev-контейнерах compose), иначе адрес стека по умолчанию.
// Флаг действует и в worktree-агентах следующих задач (T-062…T-067):
// тот же run из каталога worktree использует основной стек.

import (
	"context"
	"fmt"
	"os"
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

// dataURL — hermetic-страница без сети (в testbed нет произвольных DNS-имён).
func dataURL(title string) string {
	return "data:text/html,<title>" + title + "</title>hello"
}

func TestBrowserLivePoolRender(t *testing.T) {
	skipUnlessLive(t)
	ctx := context.Background()

	b, err := StartBrowser(ctx, liveOptions())
	if err != nil {
		t.Fatalf("StartBrowser: %v", err)
	}
	p := NewPool(b)
	defer p.Close()

	// Больше задач, чем слотов: четвёртая и дальше ждут освобождения.
	const n = 4
	var wg sync.WaitGroup
	errs := make([]error, n)
	outs := make([]RenderOutput, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			outs[i], errs[i] = p.Run(ctx, RenderTask{URL: dataURL(fmt.Sprintf("ok-%d", i))})
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
		if outs[i].FinalURL != dataURL(want) {
			t.Errorf("задача %d: final URL %q, хочу %q", i, outs[i].FinalURL, dataURL(want))
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

	// Первая сессия кладёт cookie в свой инкогнито-профиль.
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

	// Новая сессия = новый профиль: cookie не пережила смену контекста.
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

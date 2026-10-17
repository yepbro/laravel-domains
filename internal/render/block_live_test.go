package render

// Live-проверка блокировки ресурсов (T-062) — против реального Chromium
// dev-стека. Внутри теста поднимается HTTP-сервер со счётчиками обращений:
// страница тянет картинку, шрифт, CSS, скрипт и делает XHR; счётчики
// доказывают факт запроса независимо от наших же CDP-событий (иначе
// проверка была бы замкнута на саму себя).
//
// Тест исполняется в run-контейнере tools (compose run), Chromium — в
// соседнем контейнере chromium; оба в сети crawler_net. httptest.NewServer
// здесь не годится (слушает 127.0.0.1 — Chromium из другого контейнера не
// достучится): сервер слушает на 0.0.0.0, а странице отдаётся адрес этого
// контейнера в crawler_net — первый не-loopback IPv4 (net.InterfaceAddrs),
// DNS не нужен.
//
// Запуск — как в browser_live_test.go:
//
//	docker compose --project-directory . -f deploy/docker/compose.yaml \
//	  run -e CRAWLER_RENDER_TEST=1 --rm tools \
//	  go test ./internal/render -run BlockLive -v -count=1

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// blockLiveCounters — счётчики обращений к ресурсам тестовой страницы.
type blockLiveCounters struct {
	page    atomic.Int64
	css     atomic.Int64
	js      atomic.Int64
	xhr     atomic.Int64
	img     atomic.Int64
	font    atomic.Int64
	favicon atomic.Int64
}

// blockLiveIPv4 — адрес этого контейнера в crawler_net для Chromium.
func blockLiveIPv4(t *testing.T) string {
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

// TestBlockLiveResources — картинка и шрифт не запрошены (Fetch.failRequest
// до отправки), CSS/скрипт/XHR запрошены (проходят, JS работает — ТЗ 5.6).
func TestBlockLiveResources(t *testing.T) {
	skipUnlessLive(t)
	ctx := context.Background()

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
		// Синхронный XHR: выполняется при разборе скрипта — к моменту
		// события load счётчик xhr гарантированно установлен.
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
	pageURL := "http://" + net.JoinHostPort(blockLiveIPv4(t), fmt.Sprint(ln.Addr().(*net.TCPAddr).Port)) + "/page.html"

	b, err := StartBrowser(ctx, liveOptions())
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
	if out.FinalURL != pageURL {
		t.Errorf("final URL %q, хочу %q", out.FinalURL, pageURL)
	}

	// Navigate ждёт событие load, счётчики запрошенного обычно уже стоят;
	// опрос страхует медленную доставку. Заблокированные счётчики могут
	// только оставаться нулём: попытка запроса блокируется до отправки в
	// любой момент жизни страницы, гонки нет.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c.page.Load() >= 1 && c.css.Load() >= 1 && c.js.Load() >= 1 && c.xhr.Load() >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
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
		t.Errorf("картинка запрошена %d раз, хочу 0 (заблокирована, ТЗ 5.6)", got)
	}
	if got := c.font.Load(); got != 0 {
		t.Errorf("шрифт запрошен %d раз, хочу 0 (заблокирован, ТЗ 5.6)", got)
	}
	// Favicon, инициированный браузером (не страницей), может прийти типом
	// Other и пройти — зафиксированное решение (см. blockBlockedTypes).
	// В headless-shell таких запросов обычно нет; счётчик только для
	// диагностики.
	t.Logf("favicon.ico запрошен %d раз (тип Other не блокируется)",
		c.favicon.Load())
}

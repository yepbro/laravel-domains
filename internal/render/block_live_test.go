package render

// Live-проверка блокировки ресурсов (T-062; CSS — опция T-213;
// трекер-домены — опция T-214) — против
// реального Chromium dev-стека. Внутри теста поднимается HTTP-сервер со
// счётчиками обращений: страница тянет картинку, шрифт, CSS, скрипт и
// делает XHR; счётчики доказывают факт запроса независимо от наших же
// CDP-событий (иначе проверка была бы замкнута на саму себя).
//
// Тест исполняется в run-контейнере tools (compose run), Chromium — в
// соседнем контейнере chromium; оба в сети crawler_net. httptest.NewServer
// здесь не годится (слушает 127.0.0.1 — Chromium из другого контейнера не
// достучится): сервер слушает на 0.0.0.0, а странице отдаётся адрес этого
// контейнера в crawler_net — первый не-loopback IPv4 (net.InterfaceAddrs),
// DNS не нужен. Для T-214 трекер-домены резолвятся DNS-алиасами сервиса
// tools в compose (c.clarity.ms, mc.yandex.ru → run-контейнер); алиасы
// применяет флаг --use-aliases у compose run (см. запуск ниже), без него
// run-контейнер алиасы служб не получает — контрольный прогон теста T-214
// скажет об этом явно.
//
// Запуск — как в browser_live_test.go:
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
	// Счётчики страницы T-214 (tracker.html): собственный скрипт
	// (trackerJS), зонды трекер-доменов через DNS-алиасы compose
	// (clarityProbe — c.clarity.ms, mcProbe — mc.yandex.ru) и
	// запрос с доменом трекера в ПУТИ, но своим хостом (trackerTrap).
	clarityProbe atomic.Int64
	mcProbe      atomic.Int64
	trackerJS    atomic.Int64
	trackerTrap  atomic.Int64
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

// blockLiveServe — тестовая страница со счётчиками обращений: одна на оба
// live-теста блокировки (дефолт T-062 и опция T-213). Возвращает счётчики
// и адрес страницы; сервер живёт до конца теста (Cleanup).
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
	// Страница T-214 (block_tracker_domains). Трекер-домены резолвит
	// Docker DNS по алиасам compose (deploy/docker/compose.yaml, сервис
	// tools: www.google-analytics.com и mc.yandex.ru → этот же
	// run-контейнер): герметичный unbound такие имена не знает, а без
	// резолва «заблокировано перехватом» было бы неотличимо от
	// DNS-отказа. Зонды — синхронные XHR с итогом в атрибуте <body>
	// (CORS: читаем и статус): контрольный прогон с выключенной опцией
	// обязан их ДОГРУЗИТЬ (доказывает резолв+раздачу), рабочий —
	// оборвать до отправки (доказывает блокировку счётчиком сервера).
	port := 0 // порт сервера — заполняется после Listen
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
`, port)
	})
	clarityProbe := func(w http.ResponseWriter, _ *http.Request) {
		c.clarityProbe.Add(1)
		w.Header().Set("Content-Type", "text/javascript")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		_, _ = fmt.Fprint(w, "// зонд clarity.ms")
	}
	mux.HandleFunc("/clarity-probe", clarityProbe)
	mux.HandleFunc("/mc-probe", func(w http.ResponseWriter, _ *http.Request) {
		c.mcProbe.Add(1)
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		_, _ = fmt.Fprint(w, "ok")
	})
	mux.HandleFunc("/js/google-analytics.com.js", func(w http.ResponseWriter, _ *http.Request) {
		c.trackerTrap.Add(1)
		w.Header().Set("Content-Type", "text/javascript")
		_, _ = fmt.Fprint(w, "// имя трекер-домена в пути, хост — тестовый сервер")
	})

	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	// До старта Serve: handler-горутина читает port без синхронизации.
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
	return c, "http://" + net.JoinHostPort(blockLiveIPv4(t), fmt.Sprint(port)) + "/page.html"
}

// blockLiveRun — страница в сессии с данными опциями (общая обвязка
// тестов): старт браузера, сессия, Run, снятие сессии.
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

// blockLiveWaitReady — опрос до готовности счётчиков ресурсов, обязанных
// дойти (страница/скрипт/XHR): Navigate ждёт событие load, обычно они уже
// стоят; опрос страхует медленную доставку. Истечение срока — ошибка
// сразу с причиной (стиль waitFor юнит-тестов): дальнейшие проверки
// счётчиков дали бы каскад неинформативных «запрошен 0 раз».
// Заблокированные счётчики могут только оставаться нулём: попытка
// запроса блокируется до отправки в любой момент жизни страницы, гонки
// нет.
func blockLiveWaitReady(t *testing.T, c *blockLiveCounters) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if c.page.Load() >= 1 && c.js.Load() >= 1 && c.xhr.Load() >= 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("ресурсы, обязанные дойти, не запрошены за 10 с: page=%d js=%d xhr=%d",
		c.page.Load(), c.js.Load(), c.xhr.Load())
}

// TestBlockLiveResources — картинка и шрифт не запрошены (Fetch.failRequest
// до отправки), CSS/скрипт/XHR запрошены (проходят, JS работает — ТЗ 5.6):
// дефолтный набор T-062, CSS опцией не блокируется.
func TestBlockLiveResources(t *testing.T) {
	skipUnlessLive(t)
	c, pageURL := blockLiveServe(t)

	out := blockLiveRun(t, liveOptions(), pageURL)
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

// TestBlockLiveStylesheetBlocked — опция block_stylesheet (T-213): CSS не
// запрошен вовсе (failRequest до отправки), страница живёт — скрипт и XHR
// проходят, JS работает, DOM снимается: блок CSS не ломает рендер как
// таковой (риск опции — computed styles у редких сайтов, поэтому дефолт
// false и включение — после A/B).
func TestBlockLiveStylesheetBlocked(t *testing.T) {
	skipUnlessLive(t)
	c, pageURL := blockLiveServe(t)

	opts := liveOptions()
	opts.BlockStylesheet = true
	out := blockLiveRun(t, opts, pageURL)
	if out.FinalURL != pageURL {
		t.Errorf("final URL %q, хочу %q", out.FinalURL, pageURL)
	}

	blockLiveWaitReady(t, c)
	if got := c.page.Load(); got < 1 {
		t.Errorf("страница запрошена %d раз, хочу >= 1", got)
	}
	if got := c.css.Load(); got != 0 {
		t.Errorf("CSS запрошен %d раз, хочу 0 (заблокирован опцией, T-213)", got)
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
}

// domAttr — значение атрибута тела из итогового DOM (live-тест T-214):
// атрибуты выставляет сама страница по итогам своих XHR — читаются из
// публичного результата рендера. Отсутствие атрибута — пустая строка.
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

// TestBlockLiveTrackerDomains — опция block_tracker_domains (T-214)
// против живого Chromium, дифференциалом двух прогонов одной страницы
// (трекер-домены резолвятся DNS-алиасами compose в этот же run-контейнер,
// счётчики сервера — независимое от CDP-событий доказательство):
//
//   - контроль (опция выключена): зонды обоих трекер-доменов ДОХОДЯТ до
//     сервера и читаются (loaded 200) — иначе тест ничего не доказывает
//     (без резолва DNS-отказ неотличим от блокировки);
//   - рабочий прогон (опция включена): зонды оборваны ДО отправки
//     (Fetch.failRequest — счётчики ноль), собственный скрипт страницы
//     работает, а XHR по пути с доменом трекера в имени файла (хост —
//     наш сервер) проходит: подстрочную паузу urlPattern отпускает
//     точная хостовая проверка.
//
// Зонды: c.clarity.ms — поддомен домена списка (суффиксный матчинг),
// mc.yandex.ru — точный хост списка. Google-аналитика (google-analytics,
// googletagmanager, doubleclick) зондируется юнит-тестами, но не live:
// google-домены в HSTS-preload Chromium — http-зонд принудительно
// апгрейдится на https и падает на TLS с HTTP-сервером теста.
func TestBlockLiveTrackerDomains(t *testing.T) {
	skipUnlessLive(t)

	// Контрольный прогон: свежий сервер — счётчики только этого прогона.
	cOff, offPage := blockLiveServe(t)
	offURL := strings.TrimSuffix(offPage, "page.html") + "tracker.html"
	offOut := blockLiveRun(t, liveOptions(), offURL)
	if got := cOff.clarityProbe.Load(); got < 1 {
		t.Fatalf("контроль: зонд clarity.ms не дошёл до сервера (%d; атрибут clarity-xhr %q, mc-xhr %q) — "+
			"проверьте флаг --use-aliases у compose run и DNS-алиасы сервиса tools (T-214)",
			got, domAttr(offOut.DOM, "clarity-xhr"), domAttr(offOut.DOM, "mc-xhr"))
	}
	if got := cOff.mcProbe.Load(); got < 1 {
		t.Fatalf("контроль: зонд mc.yandex.ru не дошёл до сервера (%d) — "+
			"проверьте флаг --use-aliases у compose run и DNS-алиасы сервиса tools (T-214)", got)
	}
	if attr := domAttr(offOut.DOM, "clarity-xhr"); attr != "loaded 200" {
		t.Errorf("контроль: clarity-xhr %q, хочу loaded 200 (блокировки нет)", attr)
	}
	if attr := domAttr(offOut.DOM, "mc-xhr"); attr != "loaded 200" {
		t.Errorf("контроль: mc-xhr %q, хочу loaded 200 (блокировки нет)", attr)
	}

	// Рабочий прогон: опция включена.
	cOn, onPage := blockLiveServe(t)
	onURL := strings.TrimSuffix(onPage, "page.html") + "tracker.html"
	opts := liveOptions()
	opts.BlockTrackerDomains = true
	onOut := blockLiveRun(t, opts, onURL)
	if onOut.FinalURL != onURL {
		t.Errorf("final URL %q, хочу %q", onOut.FinalURL, onURL)
	}

	// Обязанные дойти: страница и собственный скрипт (JS жив, свой домен
	// не задет) — оба синхронны/предшествуют load, обычно уже стоят;
	// опрос страхует медленную доставку (стиль blockLiveWaitReady).
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) &&
		(cOn.page.Load() < 1 || cOn.trackerJS.Load() < 1) {
		time.Sleep(50 * time.Millisecond)
	}
	if got := cOn.page.Load(); got < 1 {
		t.Fatalf("страница не запрошена за 10 с — тест страницы T-214 не выполнился")
	}
	if got := cOn.trackerJS.Load(); got < 1 {
		t.Fatalf("собственный скрипт не запрошен за 10 с (блокировка не должна трогать свой домен)")
	}

	// Трекеры оборваны до отправки: счётчики ноль и атрибуты — ошибка.
	if got := cOn.clarityProbe.Load(); got != 0 {
		t.Errorf("зонд clarity.ms дошёл до сервера %d раз, хочу 0 (заблокирован, T-214)", got)
	}
	if got := cOn.mcProbe.Load(); got != 0 {
		t.Errorf("зонд mc.yandex.ru дошёл до сервера %d раз, хочу 0 (заблокирован, T-214)", got)
	}
	if attr := domAttr(onOut.DOM, "clarity-xhr"); attr != "error" {
		t.Errorf("clarity-xhr %q, хочу error (заблокирован)", attr)
	}
	if attr := domAttr(onOut.DOM, "mc-xhr"); attr != "error" {
		t.Errorf("mc-xhr %q, хочу error (заблокирован)", attr)
	}

	// Ложное срабатывание: путь содержит домен трекера, хост — наш
	// сервер; запрос обязан дойти (счётчик) и прочитаться (200).
	if got := cOn.trackerTrap.Load(); got != 1 {
		t.Errorf("запросов к /js/google-analytics.com.js: %d, хочу 1 (хост не трекер — проходит)", got)
	}
	if attr := domAttr(onOut.DOM, "path-trap"); attr != "loaded 200" {
		t.Errorf("path-trap %q, хочу loaded 200 (хост не трекер — проходит)", attr)
	}
}

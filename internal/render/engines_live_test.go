package render

// Per-engine живые тесты рендера (T-254, слой 1) — полный CDP-срез каждого
// движка (chromium / moli / lightpanda) против dev-стека: инварианты ТЗ 5.6
// обязаны выполняться на любом движке (ТЗР движков ФТ-5). Срез на движок:
//
//	(а) /json/version доступен и отвечает (общий health-эндпоинт, ФТ-4);
//	(б) изолированный контекст на задачу: cookie не переживает смену
//	    контекста (Target.createBrowserContext/disposeBrowserContext);
//	(в) navigate + съём результата (T-064): итоговый DOM после JS,
//	    cookies хранилища контекста (JS- и httpOnly) и заголовки ответа
//	    основной навигации — на внутренних страницах testbed *.crawl.test;
//	(г) перехват Fetch: блокировка Image/Font не выпускает запрос из
//	    движка (счётчики сервера, T-062) и доменные паттерны трекеров
//	    (дифференциал двух прогонов, T-214);
//	(д) SSRF-контур (T-219/T-230): задача с внутренним хостом отвергается
//	    до перехода, навигация на приватный адрес testbed обрывается
//	    перехватом в самом движке — net::ERR_BLOCKED_BY_CLIENT;
//	(е) UA-override (ТЗ §9): Emulation.setUserAgentOverride виден и в
//	    navigator.userAgent, и в User-Agent запроса страницы на сервере.
//
// Включение — как у соседних live-тестов (CRAWLER_RENDER_TEST=1, образец
// browser_live_test.go); штатный make test без стека их пропускает.
// Подключение движка — переменная окружения строки таблицы, по образцу
// CHROMIUM_WS_URL: MOLI_WS_URL / LIGHTPANDA_WS_URL (дефолты — статические
// адреса compose-сервисов: chromium профиля app, moli/lightpanda профиля
// render-engines, T-241). Общего оверрайда CRAWLER_RENDER_REMOTE_URL
// (адрес одного движка в остальных live-тестах) срез не читает: таблица
// различает движки адресами явно. Новый движок добавляется строкой
// engineLiveCases — код среза один.
//
// До прогона каждого движка — быстрый зонд /json/version (та же проверка,
// что healthcheck compose и TestChromium_CDP): недоступен при дефолтном
// адресе — SKIP с причиной (профиль render-engines не обязан стоять у
// каждого), при явно заданной переменной — Fatal (оператор направил тест
// на конкретный адрес, тишина скрывала бы ошибку конфигурации). В
// tools-контейнере compose всегда задан CHROMIUM_WS_URL — недоступный
// chromium даёт отказ, а не SKIP: он поднимается вместе со стеком
// (профиль app) и быть недоступным в рабочем окружении не обязан.
//
// Запуск (moli/lightpanda предварительно подняты профилем render-engines —
// docs/dev-environment.md, «Движки рендера»; --use-aliases нужен срезу
// трекеров, как у BlockLive):
//
//	docker compose --project-directory . -f deploy/docker/compose.yaml \
//	  run --use-aliases -e CRAWLER_RENDER_TEST=1 --rm tools \
//	  go test ./internal/render -run EnginesLive -v -count=1
//
// Один движок — фильтром Go: -run 'TestEnginesLiveCDPSlice/moli'.
//
// Состояние pinned-версий на момент задачи (live-прогон 2026-11-06:
// moli v1.1.21, lightpanda 1.0.2 — обе альтернативы СЛОМАНЫ против среза,
// срез обязан показывать это красным до починки движков/ядра):
//
//   - moli v1.1.21: Fetch.enable с паттернами по типам Media/Font
//     (базовый набор T-062) отвечает InvalidParams — moli различает
//     урезанный набор resourceType (Document/Image/Script/WebSocket/Ping/
//     CSPViolationReport/EventSource/Fetch/Other; Stylesheet/Media/Font/
//     TextTrack/Manifest/SignedExchange/Preflight — нет). Ядро включает
//     перехват паттернами Image+Media+Font → каждая задача падает на
//     startBlock: engine=moli в dev-профиле неработоспособен (ФТ-5 на
//     pinned-версии не выполняется — ресёрх этапа 35 переоценил срез).
//   - lightpanda 1.0.2: один browser context на соединение («Cannot have
//     more than one browser context at a time», зафиксировано и
//     сторонними замерами). Служебная вкладка стартового контекста
//     занимает единственный слот → Target.createBrowserContext сессии
//     всегда падает: модель «контекст на задачу» (ТЗ 5.6) на pinned-
//     версии невозможна.
//
// Красный срез = регрессия движка против инвариантов этапа (пиновка
// новой версией — процедура smoke ФТ-17/T-254,
// docs/dev-environment.md); зелёный chromium — эталон.

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

// engineLiveCase — строка таблицы движков: имя (значение render.engine и
// сцена теста), переменная окружения с адресом и адрес стека по умолчанию.
type engineLiveCase struct {
	name  string
	env   string
	defWS string
}

// engineLiveCases — таблица движков слоя 1: один код среза × наборы
// переменных подключения (новый движок — строка).
var engineLiveCases = []engineLiveCase{
	{name: config.EngineChromium, env: "CHROMIUM_WS_URL", defWS: "ws://172.28.100.14:9222"},
	{name: config.EngineMoli, env: "MOLI_WS_URL", defWS: "ws://172.28.100.17:9222"},
	{name: config.EngineLightpanda, env: "LIGHTPANDA_WS_URL", defWS: "ws://172.28.100.16:9222"},
}

// engineLiveWS — адрес движка: переменная окружения строки таблицы, иначе
// дефолт стека. Второе значение — задана ли переменная явно (оператор
// направил срез на конкретный адрес).
func engineLiveWS(tc engineLiveCase) (string, bool) {
	if ws := os.Getenv(tc.env); ws != "" {
		return ws, true
	}
	return tc.defWS, false
}

// engineLiveOptions — Options движка строки таблицы (адрес — engineLiveWS,
// движок — полем Engine: его видят валидация и remote-аллокатор, ТЗР
// движков §4.2; ядро пакета движок не различает, ФТ-4).
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

// engineLiveVersionURL — http-адрес /json/version движка из ws-адреса
// подключения (все три движка отдают общий эндпоинт на том же порту).
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

// engineLiveGate — (а) доступность движка до старта среза: GET /json/version
// с коротким потолком (быстрый отказ с причиной вместо каскада таймаутов по
// всем подтестам). 200 + «Browser» в теле — форма ответа всех трёх движков
// (как healthcheck compose и TestChromium_CDP); тело пишется в лог —
// версия движка из ответа сверяется с pinned при smoke ФТ-17/T-254.
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

// engineLiveBrowser — старт браузера движка для подтеста среза (закрытие —
// Cleanup): каждая проверка среза живёт своим браузером, сбой одной не
// рвёт остальные.
func engineLiveBrowser(t *testing.T, opts Options) *Browser {
	t.Helper()
	b, err := StartBrowser(context.Background(), opts)
	if err != nil {
		t.Fatalf("StartBrowser (%s): %v", opts.Engine, err)
	}
	t.Cleanup(b.Close)
	return b
}

// TestEnginesLiveCDPSlice — таблица движков × срез CDP (T-254, слой 1):
// один код прогоняет (а)–(е) для каждой строки engineLiveCases. Хромиум
// поднят всегда (профиль app), альтернативы — профиль render-engines;
// недоступный движок честно скипается гейтом (а) с причиной.
func TestEnginesLiveCDPSlice(t *testing.T) {
	skipUnlessLive(t)
	for _, tc := range engineLiveCases {
		t.Run(tc.name, func(t *testing.T) {
			engineLiveGate(t, tc) // (а) /json/version: зонд до среза
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

// engineLiveContextIsolation — (б) изолированный контекст на задачу
// (инвариант ТЗ 5.6/§9, ФТ-5): cookie, положенная в инкогнито-профиль
// первой сессии, видна в ней же и не переживает смену контекста (второй
// NewSession = новый профиль; dispose выкидывает хранилища целиком).
// Проверка — по TestBrowserLiveContextIsolation, движок — строки таблицы.
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

// engineLiveNavigateExtract — (в) переход и съём результата (T-064):
// внутренняя страница testbed (spa-shell.crawl.test — движки резолвят её
// через unbound стека) даёт итоговый DOM после JS, JS-cookie из хранилища
// контекста (CDP читает хранилище, document.cookie — нет) и заголовки
// ответа основной навигации реального Caddy; фикстура среза покрывает
// httpOnly — cookie, выставленную ответом СЕРВЕРА (document.cookie её не
// видит вовсе — в хранилище контекста попадает только сетевым стеком
// движка) и фирменной парой заголовков. Образцы testweb для httpOnly не
// годятся: фикстуры laravel-сэмплов ставят Set-Cookie с истёкшим сроком
// (заголовки генерились 2026-10-16 с датой истечения 2026-10-21 — браузер
// такую cookie отбрасывает сразу), поэтому httpOnly — собственной
// страницей фикстуры с бессрочной session-cookie. Вторая страница — новой
// сессией: контекст на задачу, как в пуле.
func engineLiveNavigateExtract(t *testing.T, opts Options) {
	ctx := context.Background()

	b := engineLiveBrowser(t, opts)

	mux := http.NewServeMux()
	mux.HandleFunc("/httponly", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// Фирменный заголовок фикстуры: его наличие в результате доказывает,
		// что заголовки сняты с реального ответа сервера (равно как
		// Content-Type мог бы дать и псевдо-ответ).
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

// engineLiveBlockResources — (г) перехват Fetch по типам ресурсов (T-062):
// картинка и шрифт не запрошены вовсе (failRequest до отправки — движок
// отвергает их с net::ERR_BLOCKED_BY_CLIENT, счётчики сервера ноль —
// независимое от CDP-событий доказательство, фикстура blockLiveServe),
// CSS/скрипт/XHR проходят (JS обязан работать, ТЗ 5.6).
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

// engineLiveLookupHost — резолв SSRF-контура среза (шов Options.LookupHost,
// T-230): unbound стека (172.28.100.10) — тот же резолвер, которым движки
// резолвят страницы testbed (dns: unbound в compose). Резолвер run-контейнера
// tools внешний (он качает модули Go) и тестовых зон не знает: без подмены
// резолв имени зоны в перехвате падал бы с DNS-отказом, контур fail-open'ил
// навигацию, и проверка перехвата теряла смысл.
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

// engineLiveSSRF — (д) SSRF-контур рендера (T-219/T-230) на живом движке:
// задача с внутренним хостом (IP-литералы, включая cloud metadata и
// числоподобные формы WHATWG, loopback-имена — формы юнит-теста
// TestSessionRunBlocksInternalTaskURL) отвергается классом task до
// перехода; навигация на приватный адрес testbed (зоны *.crawl.test живут
// в 172.28.100.0/24) обрывается перехватом Fetch в самом движке —
// fetch.failRequest даёт net::ERR_BLOCKED_BY_CLIENT (код — LoadError,
// класс task). Резолв имени зоны идёт резолвером перехвата (dnsCache
// T-230, здесь — engineLiveLookupHost): имя публичным для задачи
// остаётся, внутренним для перехвата быть обязано.
func engineLiveSSRF(t *testing.T, opts Options) {
	ctx := context.Background()

	// Счётчик резолвов шва: вердикт о блокировке обязан вынести перехват
	// (dnsCache через Options.LookupHost, T-230), а не собственная блокировка
	// приватных сетей движка, если такая есть (у lightpanda — флаг
	// --block-private-networks): без вызова резолва навигация не дошла бы
	// до перехвата, и код ERR_BLOCKED_BY_CLIENT ничего не доказывал бы.
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

	// Перехват: имя зоны проходит адресный контроль задачи, но резолвится
	// перехватом в приватный адрес testbed — навигация обязана упасть.
	_, err = s.Run(ctx, RenderTask{URL: "http://spa-shell.crawl.test/"})
	if err == nil {
		t.Fatal("навигация на приватный адрес testbed прошла сквозь SSRF-контур")
	}
	if ClassOf(err) != ClassTask {
		t.Errorf("класс %v (%v), хочу task — блокировка контура это ошибка загрузки страницы", ClassOf(err), err)
	}
	code := LoadErrorCode(err)
	if code == "" {
		// Без кода загрузки причина невидима (на moli, например, сюда падает
		// отказ Fetch.enable) — печатаем ошибку целиком, а не пустой код.
		t.Fatalf("навигация упала без кода ошибки загрузки (хочу net::ERR_BLOCKED_BY_CLIENT от failRequest перехвата): %v", err)
	}
	if code != "net::ERR_BLOCKED_BY_CLIENT" {
		t.Errorf("код ошибки загрузки %q, хочу net::ERR_BLOCKED_BY_CLIENT (fetch.failRequest перехвата, T-219)", code)
	}
	if n := lookups.Load(); n < 1 {
		t.Errorf("резолв SSRF-контура не вызывался (%d вызовов) — блокировка пришла не из перехвата, код ошибки её не доказывает (T-230)", n)
	}
}

// engineLiveUAOverride — (е) идентифицируемый UA краулера (ТЗ §9):
// Emulation.setUserAgentOverride в стартовом наборе сессии виден странице
// дважды — в navigator.userAgent вкладки и в User-Agent HTTP-запроса
// страницы на сервере фикстуры (второй снимает «движок подменяет только
// JS-свойство»: заголовок ставит сетевой стек движка).
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

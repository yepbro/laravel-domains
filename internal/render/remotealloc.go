package render

// Аллокатор remote-режима (внешний Chromium, Options.RemoteURL) —
// функциональный аналог remote.Allocator chromedp с одним принципиальным
// отличием: детерминированным закрытием соединения (T-218, инцидент
// 02.11). Upstream закрывает WebSocket только после завершения насосов
// chromedp (browser.run), а транспорт библиотеки не ограничен во
// времени: remote.Conn.Write не имеет дедлайна и игнорирует контекст,
// remote.Conn.Read игнорирует контекст вовсе — на полузакрытом или
// подвисшем соединении (NAT/conntrack между воркером и Chromium,
// подвисший при остановке браузер) насос висит в записи до исчерпания
// TCP-ретрансмиссий (минуты), чтение — бесконечно. Сокеты при этом
// остаются ESTABLISHED: у воркеров инцидента их накопилось 284 к :9222
// за ~6 ч. Здесь после отмены контекста сначала выполняется штатное
// закрытие вкладок chromedp (ограничено таймаутами самой библиотеки),
// затем сокет закрывается принудительно — независимо от состояния
// насосов.

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
)

// remoteDial — подключение транспорта к внешнему Chromium (WebSocket).
// Шов юнит-тестов: подменяется управляемым фейком — «чёрная дыра» CDP
// (чтение висит, записи принимаются) и «воскрешение» браузера новым
// соединением (reconnect_test.go, T-218). Продуктовый путь —
// remote.DialContext с таймаутом Options.DialTimeout.
var remoteDial = func(ctx context.Context, wsURL string, timeout time.Duration) (chromedp.Transport, error) {
	dctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := remote.DialContext(dctx, wsURL)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// remoteAllocator — аллокатор браузерной сессии поверх внешнего
// Chromium: одно WebSocket-соединение на поколение браузера (StartBrowser
// создаёт аллокатор заново). Реализует chromedp.Allocator и
// chromedp.Attacher — семантика «браузер уже запущен» обязательна (см.
// Attaches).
type remoteAllocator struct {
	wsURL       string
	dialTimeout time.Duration

	wg sync.WaitGroup
}

// newRemoteAllocator — аллокатор для Options.RemoteURL (DialTimeout —
// бюджет и уточнения адреса, и подключения).
func newRemoteAllocator(opts Options) *remoteAllocator {
	return &remoteAllocator{wsURL: opts.RemoteURL, dialTimeout: opts.DialTimeout}
}

// Allocate удовлетворяет chromedp.Allocator: ctx — контекст браузерной
// сессии chromedp (контекст первого Run/Do); его отмена останавливает
// сессию — сначала мягко (команды закрытия вкладок), затем принудительно
// (сокет). dial-часть ограничена dialTimeout через remoteDial.
func (a *remoteAllocator) Allocate(ctx context.Context, opts ...chromedp.BrowserOption) (*chromedp.Browser, error) {
	wsURL, err := resolveRemoteWSURL(ctx, a.wsURL, a.dialTimeout)
	if err != nil {
		return nil, err
	}
	tr, err := remoteDial(ctx, wsURL, a.dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("подключение к Chromium %s: %w", a.wsURL, err)
	}
	// wctx — контекст жизни сессии, отдельный от ctx (как у
	// remote.Allocator): отмена клиентского контекста не должна рвать
	// соединение до закрытия вкладок.
	wctx, wcancel := context.WithCancel(context.Background())
	a.wg.Go(func() {
		<-ctx.Done()
		// Штатный путь: закрыть вкладки и изолированные контексты по
		// сокету. Ожидание ограничено таймаутами самой chromedp
		// (closeTargetTimeout 5 с + waitTargetGone 0.5 с) и на мёртвом
		// соединении; результат здесь не важен — сокет закрывается
		// принудительно в любом случае.
		_ = chromedp.Cancel(ctx)
		// Принудительный финал: Close идемпотентен и разблокирует
		// зависшие чтение/запись насосов — сокет освобождается в
		// границы таймаутов chromedp, а не TCP-ретрансмиссий.
		_ = tr.Close()
		wcancel()
	})
	return chromedp.NewBrowserTransport(wctx, tr, opts...)
}

// Attaches удовлетворяет chromedp.Attacher: браузер уже запущен внешним
// процессом (юнит crawler-chromium / compose-сервис chromium), аллокатор
// лишь подключается. Признак меняет семантику браузерного контекста
// chromedp: без него контекст считался бы владельцем браузера — закрытие
// шло бы командой Browser.close (гасит общий внешний Chromium), а
// служебная вкладка сессии ждалась бы событием, а не создавалась.
func (a *remoteAllocator) Attaches() bool { return true }

// Wait удовлетворяет chromedp.Allocator: фоновые закрытия соединений
// аллокатора завершены.
func (a *remoteAllocator) Wait() { a.wg.Wait() }

// resolveRemoteWSURL — WebSocket-адрес браузера для подключения: адрес
// конфигурации с путём /devtools/browser/ — готовый адрес и используется
// как есть; иначе уточняется запросом /json/version
// (webSocketDebuggerUrl), как это делает remote.Allocator. Хост, не
// являющийся IP/localhost, резолвится в IP: Chrome ≥66 требует, чтобы
// Host-заголовок подключения был IP-адресом или localhost.
func resolveRemoteWSURL(ctx context.Context, raw string, timeout time.Duration) (string, error) {
	// Один потолок timeout на всё уточнение адреса (резолв и запрос):
	// входной ctx — контекст жизненного цикла браузера, без дедлайна
	// (контракт StartBrowser), а Options.DialTimeout по доке ограничивает
	// и уточнение адреса (render.go, Options.DialTimeout).
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("адрес Chromium %q: %w", raw, err)
	}
	if host := u.Hostname(); host != "localhost" && net.ParseIP(host) == nil {
		addrs, err := net.DefaultResolver.LookupIPAddr(rctx, host)
		if err != nil {
			return "", fmt.Errorf("резолв хоста Chromium %s: %w", host, err)
		}
		if port := u.Port(); port != "" {
			u.Host = net.JoinHostPort(addrs[0].IP.String(), port)
		} else {
			u.Host = addrs[0].IP.String()
		}
	}
	if strings.Contains(u.Path, "/devtools/browser/") {
		return u.String(), nil
	}
	q := *u
	q.Scheme = "http"
	if u.Scheme == "wss" || u.Scheme == "https" {
		// Защищённые схемы не понижаются до plaintext-запроса /json/version.
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

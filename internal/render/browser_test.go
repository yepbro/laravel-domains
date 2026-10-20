package render

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
)

// Стартовые ошибки классифицируются как ClassBrowser — без живого Chromium:
// удалённый режим бьётся в закрытый порт (отказ соединения мгновенный),
// локальный — в несуществующий бинарк. Оба случая детерминированы.

func TestStartBrowserRemoteUnreachable(t *testing.T) {
	_, err := StartBrowser(context.Background(), Options{
		RemoteURL:         "ws://127.0.0.1:1",
		DialTimeout:       500 * time.Millisecond,
		Contexts:          1,
		Timeout:           time.Second,
		RestartEveryPages: 1,
	})
	if err == nil {
		t.Fatal("ожидана ошибка недоступного Chromium")
	}
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("ошибка не *render.Error: %v", err)
	}
	if e.Class != ClassBrowser {
		t.Errorf("класс %v, хочу browser", e.Class)
	}
}

func TestStartBrowserLocalExecMissing(t *testing.T) {
	_, err := StartBrowser(context.Background(), Options{
		ExecPath:          "/nonexistent/chromium",
		Contexts:          1,
		Timeout:           time.Second,
		RestartEveryPages: 1,
	})
	if err == nil {
		t.Fatal("ожидана ошибка запуска несуществующего бинарка")
	}
	var e *Error
	if !errors.As(err, &e) || e.Class != ClassBrowser {
		t.Fatalf("хочу *render.Error класса browser, получено: %v", err)
	}
	// Сообщение называет режим и заданный путь: путь в тексте появляется
	// только если ExecPath действительно дошёл до аллокатора (иначе chromedp
	// ищет бинар в PATH и падает другим текстом — «google-chrome … PATH»).
	msg := err.Error()
	if !strings.Contains(msg, "локальный процесс") {
		t.Errorf("ошибка не называет локальный режим: %v", msg)
	}
	if !strings.Contains(msg, "/nonexistent/chromium") {
		t.Errorf("ошибка не называет заданный ExecPath: %v", msg)
	}
}

// TestProbeTimeout пинит литерал зонда вкладки: короткий (решение о повторе
// контекста не ждёт уровня таймаута страницы), но и не нулевой/трёхсекундный.
func TestProbeTimeout(t *testing.T) {
	if probeTimeout != 2*time.Second {
		t.Errorf("probeTimeout = %s, хочу 2s", probeTimeout)
	}
}

func TestStartBrowserValidate(t *testing.T) {
	// Неверные опции отклоняются до каких-либо подключений.
	_, err := StartBrowser(context.Background(), Options{
		RemoteURL: "ws://127.0.0.1:1", DialTimeout: time.Second,
		Contexts: 0, Timeout: time.Second, RestartEveryPages: 1,
	})
	if err == nil {
		t.Fatal("ожидана ошибка валидации")
	}
	var e *Error
	if errors.As(err, &e) {
		t.Errorf("ошибка валидации не должна быть классифицирована: %v", err)
	}
}

// TestBrowserCloseNil — Close терпим к nil, как Alive/RSS/OverMemory:
// контракт «вызов на неинициализированный браузер безвреден» (R-05).
func TestBrowserCloseNil(t *testing.T) {
	var b *Browser
	b.Close() // не паникует
	b.Close() // повторный вызов безвреден
}

// TestBrowserAliveNil — nil-браузер мёртв (первая половина nil-guard).
func TestBrowserAliveNil(t *testing.T) {
	var b *Browser
	if b.Alive() {
		t.Error("nil-браузер не жив")
	}
}

// TestBrowserAliveChromedpContext — Alive по состоянию chromedp-контекста
// без живого Chromium: контекст NewContext ещё без браузерной сессии
// (Browser == nil до первого Run) жив; закрытый LostConnection — мёртв
// даже при живом контексте.
func TestBrowserAliveChromedpContext(t *testing.T) {
	b := testBrowser(1)
	if b.Alive() {
		t.Error("браузер без контекста (юнит-тесты) не жив")
	}

	bctx, cancel := chromedp.NewContext(context.Background())
	defer cancel()
	b2 := testBrowser(1)
	b2.ctx = bctx
	if !b2.Alive() {
		t.Error("живой контекст без браузерной сессии: Alive")
	}

	lost := make(chan struct{})
	close(lost)
	chromedp.FromContext(bctx).Browser = &chromedp.Browser{LostConnection: lost}
	if b2.Alive() {
		t.Error("потерянное соединение при живом контексте: не Alive")
	}
}

// TestBrowserCloseIdempotent — повторный Close не повторяет остановку:
// cancel-хуки браузера дёргаются ровно один раз.
func TestBrowserCloseIdempotent(t *testing.T) {
	b := testBrowser(1)
	var cancels, allocCancels int
	b.cancel = func() { cancels++ }
	b.cancelAlloc = func() { allocCancels++ }
	b.Close()
	b.Close()
	if cancels != 1 {
		t.Errorf("cancel вызван %d раз, хочу 1", cancels)
	}
	if allocCancels != 1 {
		t.Errorf("cancelAlloc вызван %d раз, хочу 1", allocCancels)
	}
}

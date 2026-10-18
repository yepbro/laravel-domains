package render

import (
	"context"
	"errors"
	"testing"
	"time"
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

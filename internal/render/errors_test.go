package render

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
)

func TestClassString(t *testing.T) {
	cases := map[Class]string{
		ClassTask:    "task",
		ClassContext: "context",
		ClassBrowser: "browser",
		Class(99):    "class(99)",
	}
	for c, want := range cases {
		if got := c.String(); got != want {
			t.Errorf("Class(%d).String() = %q, хочу %q", int(c), got, want)
		}
	}
}

func TestErrorFormatUnwrapClassOf(t *testing.T) {
	sentinel := errors.New("причина")
	e := &Error{Class: ClassContext, URL: "https://a.example/", Err: sentinel}

	if got := e.Error(); got != `render context: причина (url "https://a.example/")` {
		t.Errorf("Error() = %q", got)
	}
	if !errors.Is(e, sentinel) {
		t.Error("errors.Is не проходит к причине")
	}
	var target *Error
	if !errors.As(e, &target) || target.Class != ClassContext {
		t.Error("errors.As не достал класс")
	}
	// Класс читается и через сторонние обёртки (fmt.Errorf %w).
	if got := ClassOf(fmt.Errorf("обёртка: %w", e)); got != ClassContext {
		t.Errorf("ClassOf через обёртку = %v", got)
	}
	// Без URL — краткий формат.
	if got := (&Error{Class: ClassBrowser, Err: ErrClosed}).Error(); got != "render browser: "+ErrClosed.Error() {
		t.Errorf("Error() без URL = %q", got)
	}
	// Посторонняя ошибка — ошибка страницы: без ретраев контекста.
	if got := ClassOf(sentinel); got != ClassTask {
		t.Errorf("ClassOf(чужая) = %v, хочу task", got)
	}
	if got := ClassOf(nil); got != ClassTask {
		t.Errorf("ClassOf(nil) = %v, хочу task", got)
	}
}

// TestLoadErrorCode — код ошибки загрузки страницы читается из цепочки
// структурно (поле chromedp.LoadError.ErrorText, не разбор текста ошибки;
// потребитель — политика cert-класса renderworker, T-159): прямо, через
// обёртку render.Error и fmt.Errorf; без LoadError в цепочке — пусто.
func TestLoadErrorCode(t *testing.T) {
	loadErr := &chromedp.LoadError{ErrorText: "net::ERR_CERT_AUTHORITY_INVALID"}

	if got := LoadErrorCode(loadErr); got != "net::ERR_CERT_AUTHORITY_INVALID" {
		t.Errorf("LoadErrorCode(LoadError) = %q", got)
	}
	// Боевой путь: classifyCtx оборачивает сбой навигации в render.Error.
	e := &Error{Class: ClassTask, URL: "https://a.example/", Err: loadErr}
	if got := LoadErrorCode(e); got != "net::ERR_CERT_AUTHORITY_INVALID" {
		t.Errorf("LoadErrorCode(render.Error) = %q", got)
	}
	// Произвольные обёртки %w — как у воркера поверх render.Error.
	if got := LoadErrorCode(fmt.Errorf("renderworker: рендер: %w", e)); got != "net::ERR_CERT_AUTHORITY_INVALID" {
		t.Errorf("LoadErrorCode через обёртки = %q", got)
	}
	for name, err := range map[string]error{
		"без ошибки":            nil,
		"не ошибка загрузки":    errors.New("таймаут страницы"),
		"маркер загрузки":       chromedp.ErrPageLoad,
		"ошибка другого класса": &Error{Class: ClassBrowser, Err: errors.New("canceled")},
	} {
		if got := LoadErrorCode(err); got != "" {
			t.Errorf("LoadErrorCode(%s) = %q, хочу пусто", name, got)
		}
	}
}

// livePair — браузер и сессия с живыми (не отменёнными) контекстами для
// классификации; без реального Chromium (классификация ходит только по
// состоянию контекстов и зонду, зонд подменяется probeFn).
func livePair() (*Browser, *Session) {
	b := &Browser{opts: Options{}.withDefaults(), ctx: context.Background()}
	return b, &Session{b: b, ctx: context.Background()}
}

func canceledCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestClassifyCtx(t *testing.T) {
	err := errors.New("сбой")

	t.Run("браузер мёртв", func(t *testing.T) {
		b, _ := livePair()
		b.ctx = canceledCtx()
		if got := ClassOf(b.classifyCtx(err, "u", nil)); got != ClassBrowser {
			t.Errorf("класс %v, хочу browser", got)
		}
	})

	t.Run("контекст вкладки отменён", func(t *testing.T) {
		b, s := livePair()
		s.ctx = canceledCtx()
		if got := ClassOf(b.classifyCtx(err, "u", s)); got != ClassContext {
			t.Errorf("класс %v, хочу context", got)
		}
	})

	t.Run("ошибка загрузки страницы", func(t *testing.T) {
		b, s := livePair()
		s.probeFn = func() bool { return true } // не должен понадобиться
		loadErr := &chromedp.LoadError{ErrorText: "net::ERR_NAME_NOT_RESOLVED"}
		if got := ClassOf(b.classifyCtx(loadErr, "u", s)); got != ClassTask {
			t.Errorf("класс %v, хочу task", got)
		}
	})

	t.Run("вкладка не отвечает — зонд", func(t *testing.T) {
		b, s := livePair()
		s.probeFn = func() bool { return false }
		if got := ClassOf(b.classifyCtx(err, "u", s)); got != ClassContext {
			t.Errorf("класс %v, хочу context", got)
		}
	})

	t.Run("зонд жив — ошибка страницы", func(t *testing.T) {
		b, s := livePair()
		s.probeFn = func() bool { return true }
		if got := ClassOf(b.classifyCtx(err, "u", s)); got != ClassTask {
			t.Errorf("класс %v, хочу task", got)
		}
	})

	t.Run("сессия без контекста", func(t *testing.T) {
		// Сессия, не дошедшая до создания вкладки (ctx nil): nil-guard
		// классификации обязан пропустить её в зонд, а не упасть.
		b, _ := livePair()
		s := &Session{b: b}
		if got := ClassOf(b.classifyCtx(err, "u", s)); got != ClassContext {
			t.Errorf("класс %v, хочу context (зонд отвергает)", got)
		}
	})

	t.Run("сессии нет (создание контекста)", func(t *testing.T) {
		b, _ := livePair()
		if got := ClassOf(b.classifyCtx(err, "u", nil)); got != ClassContext {
			t.Errorf("класс %v, хочу context", got)
		}
	})
}

func TestSessionWrap(t *testing.T) {
	err := errors.New("сбой")

	t.Run("отмена вызывающего контекста", func(t *testing.T) {
		_, s := livePair()
		got := s.wrap(err, canceledCtx(), context.Background(), "u")
		if got.Class != ClassTask || !errors.Is(got.Err, context.Canceled) {
			t.Errorf("класс %v / %v, хочу task с context.Canceled", got.Class, got.Err)
		}
	})

	t.Run("таймаут страницы — ошибка задачи", func(t *testing.T) {
		_, s := livePair()
		caller := context.Background()
		opCtx, stop := context.WithTimeout(caller, time.Millisecond)
		defer stop()
		<-opCtx.Done() // гарантированно истёк

		got := s.wrap(err, caller, opCtx, "u")
		if got.Class != ClassTask || !errors.Is(got.Err, context.DeadlineExceeded) {
			t.Errorf("класс %v / %v, хочу task с DeadlineExceeded", got.Class, got.Err)
		}
	})

	t.Run("живые контексты — классификация браузера", func(t *testing.T) {
		b, s := livePair()
		b.ctx = canceledCtx()
		if got := ClassOf(s.wrap(err, context.Background(), context.Background(), "u")); got != ClassBrowser {
			t.Errorf("класс %v, хочу browser", got)
		}
	})
}

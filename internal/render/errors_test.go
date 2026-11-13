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
			t.Errorf("Class(%d).String() = %q, want %q", int(c), got, want)
		}
	}
}

func TestErrorFormatUnwrapClassOf(t *testing.T) {
	sentinel := errors.New("cause")
	e := &Error{Class: ClassContext, URL: "https://a.example/", Err: sentinel}

	if got := e.Error(); got != `render context: cause (url "https://a.example/")` {
		t.Errorf("Error() = %q", got)
	}
	if !errors.Is(e, sentinel) {
		t.Error("errors.Is does not reach the cause")
	}
	var target *Error
	if !errors.As(e, &target) || target.Class != ClassContext {
		t.Error("errors.As did not extract the class")
	}
	// The class is read through third-party wrappers too (fmt.Errorf %w).
	if got := ClassOf(fmt.Errorf("wrapper: %w", e)); got != ClassContext {
		t.Errorf("ClassOf through the wrapper = %v", got)
	}
	// Without a URL — the short format.
	if got := (&Error{Class: ClassBrowser, Err: ErrClosed}).Error(); got != "render browser: "+ErrClosed.Error() {
		t.Errorf("Error() without a URL = %q", got)
	}
	// A foreign error — a page error: no context retries.
	if got := ClassOf(sentinel); got != ClassTask {
		t.Errorf("ClassOf(a foreign error) = %v, want task", got)
	}
	if got := ClassOf(nil); got != ClassTask {
		t.Errorf("ClassOf(nil) = %v, want task", got)
	}
}

// TestLoadErrorCode — the page load error code is read from the chain
// structurally (the chromedp.LoadError.ErrorText field, not error text
// parsing; the consumer is the cert-class policy of renderworker, T-159):
// directly, through a render.Error wrapper and fmt.Errorf; no LoadError
// in the chain — empty.
func TestLoadErrorCode(t *testing.T) {
	loadErr := &chromedp.LoadError{ErrorText: "net::ERR_CERT_AUTHORITY_INVALID"}

	if got := LoadErrorCode(loadErr); got != "net::ERR_CERT_AUTHORITY_INVALID" {
		t.Errorf("LoadErrorCode(LoadError) = %q", got)
	}
	// The production path: classifyCtx wraps the navigation failure in render.Error.
	e := &Error{Class: ClassTask, URL: "https://a.example/", Err: loadErr}
	if got := LoadErrorCode(e); got != "net::ERR_CERT_AUTHORITY_INVALID" {
		t.Errorf("LoadErrorCode(render.Error) = %q", got)
	}
	// Arbitrary %w wrappers — as the worker does on top of render.Error.
	if got := LoadErrorCode(fmt.Errorf("renderworker: render: %w", e)); got != "net::ERR_CERT_AUTHORITY_INVALID" {
		t.Errorf("LoadErrorCode through the wrappers = %q", got)
	}
	for name, err := range map[string]error{
		"no error":                  nil,
		"not a load error":          errors.New("page timeout"),
		"a load marker":             chromedp.ErrPageLoad,
		"an error of another class": &Error{Class: ClassBrowser, Err: errors.New("canceled")},
	} {
		if got := LoadErrorCode(err); got != "" {
			t.Errorf("LoadErrorCode(%s) = %q, want empty", name, got)
		}
	}
}

// livePair — a browser and a session with live (not canceled) contexts for
// classification; no real Chromium (classification only walks the state of
// the contexts and the probe, the probe is substituted by probeFn).
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
	err := errors.New("failure")

	t.Run("the browser is dead", func(t *testing.T) {
		b, _ := livePair()
		b.ctx = canceledCtx()
		if got := ClassOf(b.classifyCtx(err, "u", nil)); got != ClassBrowser {
			t.Errorf("the class %v, want browser", got)
		}
	})

	t.Run("the tab context is cancelled", func(t *testing.T) {
		b, s := livePair()
		s.ctx = canceledCtx()
		if got := ClassOf(b.classifyCtx(err, "u", s)); got != ClassContext {
			t.Errorf("the class %v, want context", got)
		}
	})

	t.Run("a page load error", func(t *testing.T) {
		b, s := livePair()
		s.probeFn = func() bool { return true } // must not be needed
		loadErr := &chromedp.LoadError{ErrorText: "net::ERR_NAME_NOT_RESOLVED"}
		if got := ClassOf(b.classifyCtx(loadErr, "u", s)); got != ClassTask {
			t.Errorf("the class %v, want task", got)
		}
	})

	t.Run("the tab is not responding — the probe", func(t *testing.T) {
		b, s := livePair()
		s.probeFn = func() bool { return false }
		if got := ClassOf(b.classifyCtx(err, "u", s)); got != ClassContext {
			t.Errorf("the class %v, want context", got)
		}
	})

	t.Run("the probe is alive — a page error", func(t *testing.T) {
		b, s := livePair()
		s.probeFn = func() bool { return true }
		if got := ClassOf(b.classifyCtx(err, "u", s)); got != ClassTask {
			t.Errorf("the class %v, want task", got)
		}
	})

	t.Run("a session without a context", func(t *testing.T) {
		// A session that never reached tab creation (ctx nil): the nil-guard
		// of classification must pass it to the probe, not crash.
		b, _ := livePair()
		s := &Session{b: b}
		if got := ClassOf(b.classifyCtx(err, "u", s)); got != ClassContext {
			t.Errorf("the class %v, want context (the probe rejects)", got)
		}
	})

	t.Run("no session (context creation)", func(t *testing.T) {
		b, _ := livePair()
		if got := ClassOf(b.classifyCtx(err, "u", nil)); got != ClassContext {
			t.Errorf("the class %v, want context", got)
		}
	})
}

func TestSessionWrap(t *testing.T) {
	err := errors.New("failure")

	t.Run("the caller's context cancellation", func(t *testing.T) {
		_, s := livePair()
		got := s.wrap(err, canceledCtx(), context.Background(), "u")
		if got.Class != ClassTask || !errors.Is(got.Err, context.Canceled) {
			t.Errorf("the class %v / %v, want task with context.Canceled", got.Class, got.Err)
		}
	})

	t.Run("page timeout — a task error", func(t *testing.T) {
		_, s := livePair()
		caller := context.Background()
		opCtx, stop := context.WithTimeout(caller, time.Millisecond)
		defer stop()
		<-opCtx.Done() // expired for sure

		got := s.wrap(err, caller, opCtx, "u")
		if got.Class != ClassTask || !errors.Is(got.Err, context.DeadlineExceeded) {
			t.Errorf("the class %v / %v, want task with DeadlineExceeded", got.Class, got.Err)
		}
	})

	t.Run("live contexts — the browser classification", func(t *testing.T) {
		b, s := livePair()
		b.ctx = canceledCtx()
		if got := ClassOf(s.wrap(err, context.Background(), context.Background(), "u")); got != ClassBrowser {
			t.Errorf("the class %v, want browser", got)
		}
	})
}

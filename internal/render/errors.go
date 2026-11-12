package render

import (
	"errors"
	"fmt"

	"github.com/chromedp/chromedp"
)

// Class — a render error class; determines the failure response (spec 5.6, §8):
// the worker must not diagnose Chromium from the library's error texts.
type Class int

const (
	// ClassTask — a page error: Options.Timeout exceeded, the caller's
	// context canceled, the page failed to load (net::ERR_*).
	// Retrying on a new context is pointless; redelivery is
	// the queue's level (T-066).
	ClassTask Class = iota
	// ClassContext — the isolated context died (the tab or the render
	// crashed, the profile is unresponsive): the Pool retries the task once on
	// a new context.
	ClassContext
	// ClassBrowser — Chromium is unavailable or the connection is lost: a
	// browser restart is required (T-065); the Pool does not retry.
	ClassBrowser
)

// String — the class name for logs and error messages.
func (c Class) String() string {
	switch c {
	case ClassTask:
		return "task"
	case ClassContext:
		return "context"
	case ClassBrowser:
		return "browser"
	default:
		return fmt.Sprintf("class(%d)", int(c))
	}
}

// Error — a render error with classification. errors.As(*Error) yields the class,
// Unwrap — the original cause (including context.Canceled/DeadlineExceeded).
type Error struct {
	// Class — the response to the error, see the constants above.
	Class Class
	// URL — the task's page (diagnostics).
	URL string
	// Err — the original error.
	Err error
}

// Error implements error.
func (e *Error) Error() string {
	if e.URL != "" {
		return fmt.Sprintf("render %s: %v (url %q)", e.Class, e.Err, e.URL)
	}
	return fmt.Sprintf("render %s: %v", e.Class, e.Err)
}

// Unwrap gives access to the original error (errors.Is/As pass through).
func (e *Error) Unwrap() error { return e.Err }

// ClassOf returns the error's class. All errors of this package are *Error;
// foreign errors are treated as page errors (no retries).
func ClassOf(err error) Class {
	var e *Error
	if errors.As(err, &e) {
		return e.Class
	}
	return ClassTask
}

// LoadErrorCode — the page load error code from the error chain: the browser
// text from net/base/net_error_list.h (e.g. net::ERR_NAME_NOT_RESOLVED
// or net::ERR_CERT_AUTHORITY_INVALID) with which chromedp marks a navigation
// failure (the LoadError type, errors.Is with ErrPageLoad; classifyCtx assigns
// it to the task class). An empty string — there is no page load error in the
// chain. The code is the LoadError.ErrorText field, not a substring of the
// error text: exact comparison is stable against the library's message format.
// Consumers — the host permanent certificate failure (T-159) and DNS failure
// (T-211) policies in renderworker: the code itself makes no decision.
func LoadErrorCode(err error) string {
	var le *chromedp.LoadError
	if errors.As(err, &le) {
		return le.ErrorText
	}
	return ""
}

// ErrClosed — the pool or the browser is stopped, the task did not run.
// Guarantees moving the task to the redelivery queue (T-066).
var ErrClosed = errors.New("render stopped")

// ErrBrowserDead — the browser session is not initialized or is lost
// (checked before creating a session, so as not to run commands doomed to fail).
var ErrBrowserDead = errors.New("the Chromium browser session is unavailable")

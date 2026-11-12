package render

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
)

// Start errors are classified as ClassBrowser — without a live Chromium:
// the remote mode hits a closed port (the connection refusal is instant),
// the local one — a nonexistent binary. Both cases are deterministic.

func TestStartBrowserRemoteUnreachable(t *testing.T) {
	_, err := StartBrowser(context.Background(), Options{
		RemoteURL:         "ws://127.0.0.1:1",
		DialTimeout:       500 * time.Millisecond,
		Contexts:          1,
		Timeout:           time.Second,
		RestartEveryPages: 1,
	})
	if err == nil {
		t.Fatal("an unavailable-Chromium error was expected")
	}
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("the error is not *render.Error: %v", err)
	}
	if e.Class != ClassBrowser {
		t.Errorf("the class %v, want browser", e.Class)
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
		t.Fatal("a launch error for a non-existent binary was expected")
	}
	var e *Error
	if !errors.As(err, &e) || e.Class != ClassBrowser {
		t.Fatalf("want a *render.Error of the browser class, got: %v", err)
	}
	// The message names the mode and the given path: the path appears in the
	// text only if ExecPath actually reached the allocator (otherwise chromedp
	// looks for the binary in PATH and fails with a different text — "google-chrome … PATH").
	msg := err.Error()
	if !strings.Contains(msg, "local process") {
		t.Errorf("the error does not mention the local mode: %v", msg)
	}
	if !strings.Contains(msg, "/nonexistent/chromium") {
		t.Errorf("the error does not mention the given ExecPath: %v", msg)
	}
}

// TestProbeTimeout pins the probe literals: the connection (probeConn) — with
// headroom for an overloaded Chromium (T-226: 2s produced false generation
// deaths); the tab (Session.probe) — short, the generation does not decide.
func TestProbeTimeout(t *testing.T) {
	if probeTimeout != 5*time.Second {
		t.Errorf("probeTimeout = %s, want 5s", probeTimeout)
	}
	if tabProbeTimeout != 2*time.Second {
		t.Errorf("tabProbeTimeout = %s, want 2s", tabProbeTimeout)
	}
}

func TestStartBrowserValidate(t *testing.T) {
	// Invalid options are rejected before any connections.
	_, err := StartBrowser(context.Background(), Options{
		RemoteURL: "ws://127.0.0.1:1", DialTimeout: time.Second,
		Contexts: 0, Timeout: time.Second, RestartEveryPages: 1,
	})
	if err == nil {
		t.Fatal("a validation error was expected")
	}
	var e *Error
	if errors.As(err, &e) {
		t.Errorf("the validation error must not be classified: %v", err)
	}
}

// TestBrowserCloseNil — Close tolerates nil, as Alive/RSS/OverMemory do:
// the "a call on an uninitialized browser is harmless" contract (R-05).
func TestBrowserCloseNil(t *testing.T) {
	var b *Browser
	b.Close() // does not panic
	b.Close() // a repeated call is harmless
}

// TestBrowserAliveNil — a nil browser is dead (the first half of the nil-guard).
func TestBrowserAliveNil(t *testing.T) {
	var b *Browser
	if b.Alive() {
		t.Error("a nil browser is not alive")
	}
}

// TestBrowserAliveChromedpContext — Alive by the chromedp context state
// without a live Chromium: a NewContext context without a browser session yet
// (Browser == nil before the first Run) is alive; a closed LostConnection — dead
// even with a live context.
func TestBrowserAliveChromedpContext(t *testing.T) {
	b := testBrowser(1)
	if b.Alive() {
		t.Error("a browser without a context (unit tests) is not alive")
	}

	bctx, cancel := chromedp.NewContext(context.Background())
	defer cancel()
	b2 := testBrowser(1)
	b2.ctx = bctx
	if !b2.Alive() {
		t.Error("a live context without a browser session: Alive")
	}

	lost := make(chan struct{})
	close(lost)
	chromedp.FromContext(bctx).Browser = &chromedp.Browser{LostConnection: lost}
	if b2.Alive() {
		t.Error("a lost connection with a live context: not Alive")
	}
}

// TestBrowserCloseIdempotent — a repeated Close does not repeat the stop:
// the browser's cancel hooks fire exactly once.
func TestBrowserCloseIdempotent(t *testing.T) {
	b := testBrowser(1)
	var cancels, allocCancels int
	b.cancel = func() { cancels++ }
	b.cancelAlloc = func() { allocCancels++ }
	b.Close()
	b.Close()
	if cancels != 1 {
		t.Errorf("cancel called %d times, want 1", cancels)
	}
	if allocCancels != 1 {
		t.Errorf("cancelAlloc called %d times, want 1", allocCancels)
	}
}

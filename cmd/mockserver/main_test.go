// The tests of the bounds of the "nasty" mockserver handlers (CLI-09):
// the request parameters are capped at the top (the allocation by ?rate=,
// holding the goroutine by ?delay=), /slow listens to the client
// disconnect.
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestTarpitRate(t *testing.T) {
	cases := map[string]int{
		"":             512,           // the default — below the 2 KB/s of the main spec
		"abc":          512,           // junk — the default
		"0":            512,           // non-positive — the default
		"-5":           512,           // negative — the default
		"4096":         4096,          // a normal value — as is
		"2000000000":   tarpitMaxRate, // a gigabyte/s — truncated to the limit
		"999999999999": tarpitMaxRate, // even more — truncated
	}
	for raw, want := range cases {
		if got := tarpitRate(raw); got != want {
			t.Errorf("tarpitRate(%q) = %d, want %d", raw, got, want)
		}
	}
	if tarpitMaxRate <= 0 || tarpitMaxRate > 1<<21 {
		t.Errorf("tarpitMaxRate = %d — предел должен быть мал против гигабайтных заказов", tarpitMaxRate)
	}
}

func TestSlowDelay(t *testing.T) {
	cases := map[string]time.Duration{
		"":         30 * time.Second, // the default of the slow-headers scenario
		"junk":     30 * time.Second,
		"0s":       30 * time.Second,
		"-1m":      30 * time.Second,
		"45s":      45 * time.Second,
		"5m":       5 * time.Minute,
		"9999h":    slowMaxDelay, // a day of waiting — truncated
		"1000000h": slowMaxDelay,
	}
	for raw, want := range cases {
		if got := slowDelay(raw); got != want {
			t.Errorf("slowDelay(%q) = %v, want %v", raw, got, want)
		}
	}
}

// TestSlowHandlerClientGone — a client disconnect (the ctx cancellation of
// the request) returns the handler goroutine immediately, not after the
// delay expires: no response is written (the body is empty), ServeHTTP
// finishes fast.
func TestSlowHandlerClientGone(t *testing.T) {
	mux := http.NewServeMux()
	registerRoutes(mux)

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/slow?delay=30m", nil)
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		mux.ServeHTTP(rec, req)
		close(done)
	}()
	cancel()

	select {
	case <-done:
		if rec.Body.Len() != 0 {
			t.Errorf("после отмены клиентом тело = %q, хочу ответ не писался", rec.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("обработчик /slow держит горутину после отключения клиента (ctx не учтён)")
	}
}

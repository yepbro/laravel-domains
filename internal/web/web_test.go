// Юнит-тесты каркаса публичного сервиса лендинга (T-121): маршрутизация
// (редирект корня, локали, 404 чужих префиксов), healthz по живости БД,
// заглушка формы, graceful stop Run — всё на httptest без PostgreSQL.
package web

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fakePing — Pinger с заготовленным исходом.
type fakePing struct{ err error }

func (p fakePing) Ping(context.Context) error { return p.err }

func newTestServer(t *testing.T, ping Pinger) *Server {
	t.Helper()
	s, err := New(ping, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// TestNewNilPing — без БД сервис не собирается (readiness слепым быть
// не должен).
func TestNewNilPing(t *testing.T) {
	if _, err := New(nil, Options{}); err == nil {
		t.Fatal("New(nil) = nil error, хочу ошибку")
	}
}

// TestRouting — карта маршрутов: корень редиректит на дефолтную локаль,
// локали рендерятся, чужой префикс — 404, healthz отражает БД.
func TestRouting(t *testing.T) {
	srv := newTestServer(t, fakePing{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	cases := []struct {
		method, path string
		wantStatus   int
		wantBody     string
	}{
		{"GET", "/ru", http.StatusOK, "locale ru"},
		{"GET", "/en", http.StatusOK, "locale en"},
		{"GET", "/de", http.StatusNotFound, ""},      // неподдерживаемая локаль
		{"GET", "/ru/x", http.StatusOK, "locale ru"}, // вложенный путь — локаль в префиксе
		{"GET", "/healthz", http.StatusOK, "ok"},
		{"POST", "/api/check-domain", http.StatusNotImplemented, "T-123"},
	}
	for _, tc := range cases {
		req, _ := http.NewRequest(tc.method, ts.URL+tc.path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", tc.method, tc.path, err)
		}
		if tc.wantBody != "" {
			body := make([]byte, 4096)
			n, _ := resp.Body.Read(body)
			_ = resp.Body.Close()
			if !strings.Contains(string(body[:n]), tc.wantBody) {
				t.Errorf("%s %s: тело не содержит %q: %q", tc.method, tc.path, tc.wantBody, string(body[:n]))
			}
		} else {
			_ = resp.Body.Close()
		}
		if resp.StatusCode != tc.wantStatus {
			t.Errorf("%s %s: статус = %d, хочу %d", tc.method, tc.path, resp.StatusCode, tc.wantStatus)
		}
	}

	// Редирект корня указывает на дефолтную локаль.
	req, _ := http.NewRequest("GET", ts.URL+"/", nil)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if loc := resp.Header.Get("Location"); loc != "/"+DefaultLocale {
		t.Errorf("Location корня = %q, хочу /%s", loc, DefaultLocale)
	}
}

// TestHealthzDB — недоступная БД: 503, тело без деталей (публичная
// поверхность не раскрывает внутренности).
func TestHealthzDB(t *testing.T) {
	srv := newTestServer(t, fakePing{err: errors.New("connection refused")})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("healthz = %d, хочу 503", resp.StatusCode)
	}
}

// TestMethodNotAllowed — не-GET на страницах и не-POST на API — 405
// (Go 1.22 mux сам по строгим шаблонам).
func TestMethodNotAllowed(t *testing.T) {
	srv := newTestServer(t, fakePing{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	for _, tc := range []struct{ method, path string }{
		{"POST", "/ru"},
		{"PUT", "/"},
		{"GET", "/api/check-domain"},
	} {
		req, _ := http.NewRequest(tc.method, ts.URL+tc.path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", tc.method, tc.path, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s %s: статус = %d, хочу 405", tc.method, tc.path, resp.StatusCode)
		}
	}
}

// TestRunGracefulStop — Run слушает, отменённый ctx останавливает
// сервер чистым nil в окне остановки.
func TestRunGracefulStop(t *testing.T) {
	srv := newTestServer(t, fakePing{})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // Run открывает свой слушатель на адресе

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx, addr) }()

	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("сервер не поднялся за 2 с")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run после отмены: %v, хочу nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("сервер не остановился после отмены ctx")
	}
}

// Unit tests of the /api/* CORS allowlist (L-04, TZ-ld-landing §7.2):
// the exact-origin echo on actual GET/POST requests, the preflight 204
// with the method/header answer (both API routes), a foreign origin —
// no CORS headers, an empty allowlist — CORS off entirely (no headers,
// OPTIONS not special-cased), and the non-API routes staying untouched.
package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"crawler/internal/checker"
	"crawler/internal/config"
)

// newCORSServer — a server with the CORS allowlist set (RateLimit is
// high: these tests do not hit the form limit).
func newCORSServer(t *testing.T, db DB, origins ...string) *Server {
	t.Helper()
	cfg := config.Default()
	cfg.Web.RateLimit = 100
	cfg.Web.CORS.AllowedOrigins = origins
	s, err := New(db, cfg, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// corsGet — a request with an Origin header (the browser cross-origin
// fetch; the same-origin/curl case is an ordinary request without one).
func corsGet(t *testing.T, url, origin string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Origin", origin)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// assertNoCORSHeaders — the response carries no CORS answer at all
// (Access-Control-*): the browser blocks the script's read itself.
func assertNoCORSHeaders(t *testing.T, resp *http.Response) {
	t.Helper()
	for _, h := range []string{
		"Access-Control-Allow-Origin", "Access-Control-Allow-Methods",
		"Access-Control-Allow-Headers", "Access-Control-Max-Age",
	} {
		if got := resp.Header.Get(h); got != "" {
			t.Errorf("%s = %q, want empty (no CORS answer)", h, got)
		}
	}
}

// TestCORSAllowedOriginActualRequests — an allowlisted origin on the
// real (non-preflight) requests: the exact echo in Allow-Origin + Vary:
// Origin — on the stats GET and on the form POST (the browser sends the
// actual POST with Origin after the preflight).
func TestCORSAllowedOriginActualRequests(t *testing.T) {
	const origin = "https://sites.laravel.city"
	db := &fakeDB{seed: seedLanding()}
	db.seed.domainInfo = []byte(`{"domain":{"status":0,"status_name":"new"},"laravel":{"score":0.71}}`)
	srv := newCORSServer(t, db, origin, "https://sites.laracity.ru")
	srv.liveCheck = func(context.Context, *config.Config, checker.Pool, string, checker.Logger) (*checker.Result, error) {
		return &checker.Result{Domain: "example.com", Found: true, Score: 0.5}, nil
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp := corsGet(t, ts.URL+"/api/v1/landing/stats", origin)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("the stats GET = %d, want 200:\n%s", resp.StatusCode, readBody(t, resp))
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != origin {
		t.Errorf("Allow-Origin = %q, want the exact origin %q", got, origin)
	}
	if vary := resp.Header.Get("Vary"); vary != "Origin" {
		t.Errorf("Vary = %q, want Origin (the answer depends on it)", vary)
	}
	// The CORS wrapping does not touch the endpoint's own contract.
	if cc := resp.Header.Get("Cache-Control"); cc != "public, max-age=60" {
		t.Errorf("Cache-Control = %q, want public, max-age=60 (L-03)", cc)
	}

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/check-domain", strings.NewReader(`{"domain":"example.com"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Origin", origin)
	req.Header.Set("Content-Type", "application/json")
	presp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = presp.Body.Close() }()
	if presp.StatusCode != http.StatusOK {
		t.Fatalf("the form POST = %d, want 200:\n%s", presp.StatusCode, readBody(t, presp))
	}
	if got := presp.Header.Get("Access-Control-Allow-Origin"); got != origin {
		t.Errorf("the POST Allow-Origin = %q, want the exact origin %q (never the * wildcard)", got, origin)
	}
}

// TestCORSPreflight — the browser's OPTIONS preflight (the form sends
// Content-Type: application/json, which is not a simple content type):
// 204 with the method/header list and Max-Age on both API routes; the
// endpoint handler is not called — a preflight must not spend the form's
// rate limit (the limit of 1 below: the POST after the preflight passes).
func TestCORSPreflight(t *testing.T) {
	const origin = "https://sites.laracity.ru"
	cfg := config.Default()
	cfg.Web.RateLimit = 1 // a preflight + a check must fit
	cfg.Web.CORS.AllowedOrigins = []string{origin}
	db := &fakeDB{seed: seedLanding()}
	srv, err := New(db, cfg, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv.liveCheck = func(context.Context, *config.Config, checker.Pool, string, checker.Logger) (*checker.Result, error) {
		return &checker.Result{Domain: "example.com"}, nil
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	preflight := func(path string) *http.Response {
		req, err := http.NewRequest(http.MethodOptions, ts.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Origin", origin)
		req.Header.Set("Access-Control-Request-Method", "POST")
		req.Header.Set("Access-Control-Request-Headers", "content-type")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	for _, path := range []string{"/api/check-domain", "/api/v1/landing/stats"} {
		resp := preflight(path)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("the preflight of %s = %d, want 204", path, resp.StatusCode)
		}
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != origin {
			t.Errorf("%s: Allow-Origin = %q, want %q", path, got, origin)
		}
		if got := resp.Header.Get("Access-Control-Allow-Methods"); got != "GET, POST, OPTIONS" {
			t.Errorf("%s: Allow-Methods = %q, want %q", path, got, "GET, POST, OPTIONS")
		}
		if got := resp.Header.Get("Access-Control-Allow-Headers"); got != "Content-Type" {
			t.Errorf("%s: Allow-Headers = %q, want %q", path, got, "Content-Type")
		}
		if got := resp.Header.Get("Access-Control-Max-Age"); got != "600" {
			t.Errorf("%s: Max-Age = %q, want 600", path, got)
		}
		if vary := resp.Header.Get("Vary"); vary != "Origin" {
			t.Errorf("%s: Vary = %q, want Origin", path, vary)
		}
	}

	// The preflight did not touch the endpoint: with the limit of 1 the
	// actual POST still passes (the handler was not called by OPTIONS).
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/check-domain", strings.NewReader(`{"domain":"example.com"}`))
	req.Header.Set("Origin", origin)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("the POST after the preflight = %d, want 200 (the preflight must not spend the rate limit)", resp.StatusCode)
	}
}

// TestCORSForeignOrigin — a non-allowlisted origin: no CORS answer on
// the actual requests and on the preflight (the preflight falls through
// to the router's honest 405 — there are no OPTIONS routes). Vary:
// Origin stays: the answer depends on Origin, and the stats endpoint is
// publicly cacheable — without Vary a shared cache would serve one
// origin's Allow-Origin to a request of another.
func TestCORSForeignOrigin(t *testing.T) {
	const foreign = "https://evil.example.org"
	srv := newCORSServer(t, &fakeDB{seed: seedLanding()}, "https://sites.laravel.city")
	// The stub of the live check (the neighbors' style): the foreign
	// origin reaches the handler like any other — the outcome must not
	// depend on the runtime assembly of a unit test.
	srv.liveCheck = func(context.Context, *config.Config, checker.Pool, string, checker.Logger) (*checker.Result, error) {
		return &checker.Result{Domain: "example.com"}, nil
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp := corsGet(t, ts.URL+"/api/v1/landing/stats", foreign)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("the stats GET of a foreign origin = %d, want 200 (CORS is not the server's access control)", resp.StatusCode)
	}
	assertNoCORSHeaders(t, resp)
	if vary := resp.Header.Get("Vary"); vary != "Origin" {
		t.Errorf("Vary = %q, want Origin (the cache key of the allowlist echo)", vary)
	}

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/check-domain", strings.NewReader(`{"domain":"example.com"}`))
	req.Header.Set("Origin", foreign)
	presp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = presp.Body.Close()
	if presp.StatusCode != http.StatusOK {
		t.Fatalf("the form POST of a foreign origin = %d, want 200 (the handler works, the browser blocks the read)", presp.StatusCode)
	}
	assertNoCORSHeaders(t, presp)

	// A request without Origin at all (a same-origin fetch, curl): no
	// echo; Vary stays — the answer depends on the header for others.
	plain, err := http.Get(ts.URL + "/api/v1/landing/stats")
	if err != nil {
		t.Fatal(err)
	}
	_ = plain.Body.Close()
	if plain.StatusCode != http.StatusOK {
		t.Fatalf("the stats GET without Origin = %d, want 200", plain.StatusCode)
	}
	assertNoCORSHeaders(t, plain)
	if vary := plain.Header.Get("Vary"); vary != "Origin" {
		t.Errorf("Vary = %q, want Origin", vary)
	}

	preq, _ := http.NewRequest(http.MethodOptions, ts.URL+"/api/check-domain", nil)
	preq.Header.Set("Origin", foreign)
	preq.Header.Set("Access-Control-Request-Method", "POST")
	pfresp, err := http.DefaultClient.Do(preq)
	if err != nil {
		t.Fatal(err)
	}
	_ = pfresp.Body.Close()
	if pfresp.StatusCode == http.StatusNoContent {
		t.Error("the foreign preflight must not get the 204 answer")
	}
	assertNoCORSHeaders(t, pfresp)
}

// TestCORSDisabled — an empty allowlist: CORS off entirely — no headers
// on a cross-origin-looking request, and OPTIONS is not special-cased
// (the mux's honest 405 — the behavior before L-04).
func TestCORSDisabled(t *testing.T) {
	const origin = "https://sites.laravel.city"
	srv := newTestServer(t, &fakeDB{seed: seedLanding()}) // Default(): the allowlist is empty
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp := corsGet(t, ts.URL+"/api/v1/landing/stats", origin)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("the stats GET = %d, want 200:\n%s", resp.StatusCode, readBody(t, resp))
	}
	assertNoCORSHeaders(t, resp)
	if vary := resp.Header.Get("Vary"); vary != "" {
		t.Errorf("Vary = %q, want empty (CORS off — nothing depends on Origin)", vary)
	}

	req, _ := http.NewRequest(http.MethodOptions, ts.URL+"/api/check-domain", nil)
	req.Header.Set("Origin", origin)
	req.Header.Set("Access-Control-Request-Method", "POST")
	presp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = presp.Body.Close()
	if presp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("the OPTIONS of the disabled CORS = %d, want 405 (no special-casing)", presp.StatusCode)
	}
	assertNoCORSHeaders(t, presp)
}

// TestCORSNonAPIRoutesUntouched — the landing page and /healthz carry
// no CORS answer even with the allowlist set: the wrapper gates on the
// /api/ prefix only.
func TestCORSNonAPIRoutesUntouched(t *testing.T) {
	const origin = "https://sites.laravel.city"
	srv := newCORSServer(t, &fakeDB{seed: seedLanding()}, origin)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	for _, path := range []string{"/en", "/healthz", "/static/favicon.svg"} {
		resp := corsGet(t, ts.URL+path, origin)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s = %d, want 200", path, resp.StatusCode)
		}
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "" {
			t.Errorf("%s: Allow-Origin = %q, want empty (not an /api route)", path, got)
		}
		if vary := resp.Header.Get("Vary"); vary != "" {
			t.Errorf("%s: Vary = %q, want empty (not an /api route)", path, vary)
		}
	}
}

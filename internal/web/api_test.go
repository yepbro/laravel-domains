// Unit tests of the manual check form (T-123): the input validator
// table (URL/IDN/junk — modeled on the larasites CheckDomainRequest),
// the token bucket rate limit (4/min, 429 + Retry-After, recovery), the
// e2e handler with a fake checker (the liveCheck seam): the JSON
// response, the stored section, the web_checks record, the 422
// validation errors.
package web

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"crawler/internal/checker"
	"crawler/internal/config"
	"crawler/internal/rules"
)

// TestValidateCheckInput — the validator table: trim, a dot, a
// scheme+port, IDN, junk.
func TestValidateCheckInput(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"example.com", "example.com", false},
		{"  Example.COM.  ", "example.com", false},
		{"https://example.com/page?q=1", "example.com", false},
		{"http://example.com:8080/", "example.com", false},
		{"домен.рф", "xn--d1acufc.xn--p1ai", false},
		{"", "", true},
		{"   ", "", true},
		{"not a domain", "", true},
		{"https://", "", true},
		{"user@example.com", "", true},
		{"-", "", true},
		{"single", "", true},
	}
	for _, tc := range cases {
		got, err := validateCheckInput(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("validate(%q) = %q, want an error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("validate(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("validate(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestRateLimit — 4 requests pass, the fifth is rejected; the window
// restores one token per quarter minute.
func TestRateLimit(t *testing.T) {
	l := newLimiter(4, time.Minute)
	now := time.Now()
	for i := 0; i < 4; i++ {
		if !l.allow("1.2.3.4", now) {
			t.Fatalf("request %d rejected, want a pass (the limit 4)", i+1)
		}
	}
	if l.allow("1.2.3.4", now) {
		t.Fatal("the fifth request in a minute passed, want a refusal")
	}
	if l.allow("5.6.7.8", now) {
		// Another IP — its own bucket.
	} else {
		t.Fatal("a foreign IP rejected by the shared bucket")
	}
	if !l.allow("1.2.3.4", now.Add(16*time.Second)) {
		t.Fatal("a quarter minute later the bucket has not restored one token")
	}
}

// TestClientIP — the visitor address behind the reverse proxy (T-124):
// X-Real-IP is trusted only from a loopback peer (TCP) or a unix-socket
// peer (T-221) and only for a valid IP; from outside and from a foreign
// address the header is ignored — a forgery does not spend someone
// else's rate limit bucket. A unix-socket peer without a valid header —
// the unixLocalKey label key (not an IP: it is not written into the
// web_checks log). X-Forwarded-For (the fallback without X-Real-IP)
// takes the last entry — our proxy appended it; the client start of the
// list is not trusted (P1-1 of the T-221 review).
func TestClientIP(t *testing.T) {
	req := func(remote, real, xff string, unix bool) *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/api/check-domain", nil)
		r.RemoteAddr = remote
		if unix {
			// Positive unix-peer recognition — by the connection's local
			// address from the context (http.Server puts it there; the
			// RemoteAddr in that case is "@"/a path, and a path with a
			// colon would even parse as host:port by SplitHostPort).
			r = r.WithContext(context.WithValue(r.Context(), http.LocalAddrContextKey,
				&net.UnixAddr{Name: "@", Net: "unix"}))
		}
		if real != "" {
			r.Header.Set("X-Real-IP", real)
		}
		if xff != "" {
			r.Header.Set("X-Forwarded-For", xff)
		}
		return r
	}
	for _, tt := range []struct {
		name                string
		remote, realIP, xff string
		unix                bool
		want                string
		wantIP              bool
	}{
		{"a direct address without a header", "203.0.113.7:44321", "", "", false, "203.0.113.7", true},
		{"nginx loopback + X-Real-IP", "127.0.0.1:51234", "198.51.100.9", "", false, "198.51.100.9", true},
		{"nginx ipv6-loopback + X-Real-IP", "[::1]:51234", "198.51.100.9", "", false, "198.51.100.9", true},
		{"loopback + a junk header", "127.0.0.1:51234", "not-an-ip", "", false, "127.0.0.1", true},
		{"loopback + a header with a port", "127.0.0.1:51234", "198.51.100.9:443", "", false, "127.0.0.1", true},
		{"a foreign address + a forged header", "203.0.113.7:44321", "198.51.100.9", "", false, "203.0.113.7", true},
		// A unix socket (T-221): the peer is trusted, like loopback TCP.
		{"a unix peer + X-Real-IP", "@", "198.51.100.9", "", true, "198.51.100.9", true},
		{"a named unix peer + X-Real-IP", "/run/client.sock", "198.51.100.9", "", true, "198.51.100.9", true},
		{"a unix peer, a socket path with a colon", "/run/web:s.sock", "198.51.100.9", "", true, "198.51.100.9", true},
		{"a unix peer + a single-entry XFF", "@", "", "198.51.100.10", true, "198.51.100.10", true},
		{"a unix peer + an ipv6 XFF", "@", "", "2001:db8::1", true, "2001:db8::1", true},
		{"a unix peer + an XFF with client junk at the start", "@", "", "junk, 6.6.6.6, 198.51.100.10", true, "198.51.100.10", true},
		{"a unix peer + an XFF with a forged first entry", "@", "", "1.2.3.4, 198.51.100.10", true, "198.51.100.10", true},
		{"a unix peer without headers", "@", "", "", true, unixLocalKey, false},
		{"a named unix peer without headers", "/run/client.sock", "", "", true, unixLocalKey, false},
		{"a unix peer + a junk X-Real-IP, a valid XFF", "@", "not-an-ip", "198.51.100.10", true, "198.51.100.10", true},
		{"a unix peer + junk at the end of the XFF", "@", "", "198.51.100.10, junk", true, unixLocalKey, false},
		// A forgery over TCP does not extend trust: the XFF of a foreign
		// peer and of a loopback peer without X-Real-IP is not read (the
		// TCP branch of T-124); a RemoteAddr without an IP (no unix
		// connection context) gets no trust and is not written into the
		// log.
		{"a foreign TCP address + a forged XFF", "203.0.113.7:44321", "", "198.51.100.9", false, "203.0.113.7", true},
		{"loopback TCP without X-Real-IP (the XFF is not read)", "127.0.0.1:51234", "", "198.51.100.9", false, "127.0.0.1", true},
		{"a path with a colon without the unix context — not trusted", "/run/web:s.sock", "", "198.51.100.9", false, "/run/web", false},
	} {
		got, isIP := clientIP(req(tt.remote, tt.realIP, tt.xff, tt.unix))
		if got != tt.want || isIP != tt.wantIP {
			t.Errorf("%s: clientIP = (%q, %v), want (%q, %v)", tt.name, got, isIP, tt.want, tt.wantIP)
		}
	}
}

// TestCheckDomainE2E — a fake checker: a 200 + JSON with
// found/score/records, a web_checks record, repeated requests within
// the limit.
func TestCheckDomainE2E(t *testing.T) {
	db := &fakeDB{seed: seedLanding()}
	db.seed.domainInfo = []byte(`{"domain":{"status":0,"status_name":"new","last_checked_at":"2026-10-18"},
		"laravel":{"score":0.71,"ver_min":"9.19","ver_max":"11"}}`)
	cfg := config.Default()
	cfg.Web.RateLimit = 10
	srv, err := New(db, cfg, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	score := 0.7231
	srv.liveCheck = func(context.Context, *config.Config, checker.Pool, string, checker.Logger) (*checker.Result, error) {
		return &checker.Result{
			Domain: "example.com", Zone: "com", Apex: "delegated",
			Found: true, Score: score, VerMin: "9.19", VerMax: "12",
			Records: []checker.SubRecord{{
				Label: "www", Host: "www.example.com", Variant: "main", Scheme: "https",
				Status: "200", FinalURL: "https://example.com/",
				Signals: []rules.Evidence{{Rule: "cookie_xsrf_token", Weight: 0.25, Evidence: "XSRF-TOKEN"}},
			}},
		}, nil
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/api/check-domain", "application/json",
		strings.NewReader(`{"domain":"Example.COM."}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body := make([]byte, 1<<16)
	n, _ := resp.Body.Read(body)
	got := string(body[:n])
	for _, want := range []string{
		`"found":true`, `"score":0.723`, `"ver_min":"9.19"`, `"ver_max":"12"`,
		`"subdomain":"www"`, `"final_url":"https://example.com/"`,
		`"rule":"cookie_xsrf_token"`, `"stored"`, `"status":"new"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the response does not contain %s:\n%s", want, got)
		}
	}
	// web_checks is written: the fake carries no INSERT semantics — we
	// record the access (the contract appears with the integration; here
	// it is the seam) and that the write goes through Exec, not Query
	// (T-221: Query without reading rows swallowed the error — the log
	// silently went empty).
	if len(db.queries) == 0 {
		t.Error("no DB calls (stored/web_checks)")
	}
	if got := db.webCheckInserts(); got != 1 {
		t.Errorf("the INSERT web_checks via Exec ran %d times, want 1", got)
	}
}

// TestCheckDomainInvalid — junk input: a 422 in the landing.js format.
func TestCheckDomainInvalid(t *testing.T) {
	srv := newTestServer(t, &fakeDB{seed: seedLanding()})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/api/check-domain", "application/json",
		strings.NewReader(`{"domain":"not a domain"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", resp.StatusCode)
	}
	body := make([]byte, 256)
	n, _ := resp.Body.Read(body)
	if !strings.Contains(string(body[:n]), `"invalid"`) {
		t.Errorf("the 422 body is not in the form format: %s", string(body[:n]))
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("the 422 Content-Type = %q, want application/json (T-227)", ct)
	}
}

// TestCheckDomainRateLimited — a 429 with Retry-After.
func TestCheckDomainRateLimited(t *testing.T) {
	db := &fakeDB{seed: seedLanding()}
	cfg := config.Default()
	cfg.Web.RateLimit = 1
	srv, err := New(db, cfg, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv.liveCheck = func(context.Context, *config.Config, checker.Pool, string, checker.Logger) (*checker.Result, error) {
		return &checker.Result{Domain: "example.com"}, nil
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	post := func() *http.Response {
		resp, err := http.Post(ts.URL+"/api/check-domain", "application/json",
			strings.NewReader(`{"domain":"example.com"}`))
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	first := post()
	defer func() { _ = first.Body.Close() }()
	if first.StatusCode != http.StatusOK {
		t.Fatalf("the first request = %d, want 200", first.StatusCode)
	}
	second := post()
	defer func() { _ = second.Body.Close() }()
	if second.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("the second request = %d, want 429 (the limit 1)", second.StatusCode)
	}
	if ra := second.Header.Get("Retry-After"); ra == "" {
		t.Error("a 429 without Retry-After")
	}
	// A JSON body with a correct Content-Type (T-227): http.Error used
	// to serve JSON with text/plain — landing.js looks at the status,
	// but this is the uniformity contract for all form outcomes.
	if ct := second.Header.Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("the 429 Content-Type = %q, want application/json", ct)
	}
	if !strings.Contains(readBody(t, second), `"rate_limited"`) {
		t.Error("the 429 body without error:rate_limited")
	}
}

// readBody — the response body in full (closes Body).
func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

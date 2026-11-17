// Unit tests of the manual check form (T-123): the input validator
// table (URL/IDN/junk — modeled on the larasites CheckDomainRequest),
// the token bucket rate limit (4/min, 429 + Retry-After, recovery), the
// e2e handler with a fake checker (the liveCheck seam): the JSON
// response, the stored section, the web_checks record (the raw input vs
// the punycode — the schema contract of migrations/000012, T-297), the
// 422 validation errors.
package web

import (
	"context"
	"encoding/json"
	"errors"
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

// TestJournalRawDomain — the raw form input for the web_checks.domain
// column: as entered, capped at 512 bytes on a rune boundary (validation
// trims the input, so a padded raw can be longer than any legal name —
// the audit keeps the beginning, where the typos are; migrations/000012:
// «domain — as entered by the operator (before normalization)», T-297).
func TestJournalRawDomain(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"as entered", "  Example.COM.  ", "  Example.COM.  "},
		{"short", "example.com", "example.com"},
		{"empty", "", ""},
		// 600 bytes of padding survive validation (trimmed there) — the
		// journal keeps the first 512 bytes of the raw input.
		{"padded cut", strings.Repeat(" ", 600) + "example.com", strings.Repeat(" ", 512)},
		// A multi-byte rune split by the byte cut: 511 ASCII bytes + Λ
		// (2 bytes each) — the cut at 512 lands inside the first Λ, the
		// helper steps back to the rune boundary (511 bytes).
		{"rune boundary", strings.Repeat("a", 511) + "ΛΛΛ", strings.Repeat("a", 511)},
		// The cut exactly on a rune boundary is kept as is.
		{"boundary exact", strings.Repeat("a", 510) + "ΛΛΛ", strings.Repeat("a", 510) + "Λ"},
	}
	for _, tc := range cases {
		if got := journalRawDomain(tc.in); got != tc.want {
			t.Errorf("%s: journalRawDomain() = %q (%d bytes), want %q (%d bytes)",
				tc.name, got, len(got), tc.want, len(tc.want))
		}
	}
	if got := journalRawDomain(strings.Repeat("a", 512)); len(got) != 512 {
		t.Errorf("the cap itself: %d bytes, want exactly 512", len(got))
	}
	if got := journalRawDomain(strings.Repeat("a", 513)); len(got) != 512 {
		t.Errorf("one over the cap: %d bytes, want 512", len(got))
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
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	got := string(body)
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
	rows := db.webCheckRows()
	if len(rows) != 1 {
		t.Fatalf("the INSERT web_checks via Exec ran %d times, want 1", len(rows))
	}
	// The journal contract (migrations/000012, T-297): domain — the raw
	// input as entered by the operator (the case, the trailing dot),
	// punycode_domain — the idn.Normalize form; found/score — the live
	// verdict shown to the visitor.
	row := rows[0]
	if row[0] != "Example.COM." || row[1] != "example.com" {
		t.Errorf("the journal (domain, punycode_domain) = (%q, %q), want (%q, %q) — the raw input must be preserved",
			row[0], row[1], "Example.COM.", "example.com")
	}
	if row[3] != true {
		t.Errorf("the journal found = %v, want true", row[3])
	}
	if got, want := row[4].(float64), score; got != want {
		t.Errorf("the journal score = %v, want %v", got, want)
	}
}

// TestCheckDomainJournalTruncatesRaw — a padded raw input (validation
// trims it, so it passes) reaches the journal capped at 512 bytes of the
// input as entered: the audit keeps the beginning (the typos are at the
// start), the punycode column still carries the normalized name (T-297).
func TestCheckDomainJournalTruncatesRaw(t *testing.T) {
	db := &fakeDB{seed: seedLanding()}
	cfg := config.Default()
	cfg.Web.RateLimit = 10
	srv, err := New(db, cfg, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv.liveCheck = fakeLiveCheck()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	raw := strings.Repeat(" ", 600) + "example.com"
	resp, err := http.Post(ts.URL+"/api/check-domain", "application/json",
		strings.NewReader(`{"domain":"`+raw+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (the padding is trimmed by validation)", resp.StatusCode)
	}
	rows := db.webCheckRows()
	if len(rows) != 1 {
		t.Fatalf("the INSERT web_checks ran %d times, want 1", len(rows))
	}
	if got, want := rows[0][0].(string), strings.Repeat(" ", 512); got != want {
		t.Errorf("the journal domain = %d bytes, want the raw input capped at 512 bytes (%d bytes)",
			len(got), len(want))
	}
	if got, want := rows[0][1].(string), "example.com"; got != want {
		t.Errorf("the journal punycode_domain = %q, want %q", got, want)
	}
}

// TestCheckDomainStoredOnlyJournal — the stored_only branch (the live
// check failed, the domain is known — a 200 built from the stored
// episode): the journal must reflect the answer the visitor got, not the
// zero defaults of the failed live branch (T-297): found/score come from
// the stored Laravel verdict, the duration stays 0 (there was no live
// measurement), while the JSON keeps found=false/score=0 — the top-level
// fields are the live-check contract of the landing client.
func TestCheckDomainStoredOnlyJournal(t *testing.T) {
	db := &fakeDB{seed: seedLanding()}
	db.seed.domainInfo = []byte(`{"domain":{"status":0,"status_name":"new","last_checked_at":"2026-11-01"},
		"laravel":{"score":0.71,"ver_min":"9.19","ver_max":"11"}}`)
	cfg := config.Default()
	cfg.Web.RateLimit = 10
	srv, err := New(db, cfg, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv.liveCheck = func(context.Context, *config.Config, checker.Pool, string, checker.Logger) (*checker.Result, error) {
		return nil, errors.New("the pipeline is unavailable")
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
		t.Fatalf("status = %d, want 200 (stored_only)", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	// The JSON is decoded into the response type (the review P3-3: a
	// substring check would pass on `"score":0.71` of the stored block —
	// `"score":0` is its prefix, so the top-level fields were not pinned).
	var got checkResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode the response: %v\n%s", err, body)
	}
	if got.Found {
		t.Errorf("the top-level found = true, want false (the live check failed)")
	}
	if got.Score != 0 {
		t.Errorf("the top-level score = %v, want 0 (the live-check contract of the landing client)", got.Score)
	}
	if got.Stored == nil || got.Stored.Score == nil || *got.Stored.Score != 0.71 {
		t.Errorf("the stored block = %+v, want the stored Laravel score 0.71", got.Stored)
	}

	rows := db.webCheckRows()
	if len(rows) != 1 {
		t.Fatalf("the INSERT web_checks ran %d times, want 1", len(rows))
	}
	row := rows[0]
	if row[0] != "Example.COM." || row[1] != "example.com" {
		t.Errorf("the journal (domain, punycode_domain) = (%q, %q), want the raw input and its punycode",
			row[0], row[1])
	}
	if row[3] != true {
		t.Errorf("the journal found = %v, want true (the visitor got a stored Laravel verdict)", row[3])
	}
	if got, want := row[4].(float64), 0.71; got != want {
		t.Errorf("the journal score = %v, want %v (the stored score shown in the answer)", got, want)
	}
	if got, want := row[5].(int32), int32(0); got != want {
		t.Errorf("the journal duration_ms = %d, want %d (no live measurement)", got, want)
	}
}

// TestCheckDomainStoredNoLaravelJournal — the second stored_only shape
// (the review P3-4): the domain is known to the pipeline, but there is
// no Laravel verdict (the laravel block of domain_info is absent →
// Stored.Score == nil). The journal must record found=false/score=0 —
// not "any stored episode means found" — pinning the
// jFound = Stored.Score != nil mapping of the fix.
func TestCheckDomainStoredNoLaravelJournal(t *testing.T) {
	db := &fakeDB{seed: seedLanding()}
	// No "laravel" key: a known domain, not a Laravel site.
	db.seed.domainInfo = []byte(`{"domain":{"status_name":"checked","last_checked_at":"2026-11-02"}}`)
	cfg := config.Default()
	cfg.Web.RateLimit = 10
	srv, err := New(db, cfg, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv.liveCheck = func(context.Context, *config.Config, checker.Pool, string, checker.Logger) (*checker.Result, error) {
		return nil, errors.New("the pipeline is unavailable")
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/api/check-domain", "application/json",
		strings.NewReader(`{"domain":"example.com"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (stored_only without a Laravel verdict)", resp.StatusCode)
	}

	rows := db.webCheckRows()
	if len(rows) != 1 {
		t.Fatalf("the INSERT web_checks ran %d times, want 1", len(rows))
	}
	row := rows[0]
	if row[3] != false {
		t.Errorf("the journal found = %v, want false (a known domain without a Laravel verdict)", row[3])
	}
	if got, want := row[4].(float64), 0.0; got != want {
		t.Errorf("the journal score = %v, want %v", got, want)
	}
	if got, want := row[5].(int32), int32(0); got != want {
		t.Errorf("the journal duration_ms = %d, want %d (no live measurement)", got, want)
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

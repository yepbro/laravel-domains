// Unit tests of the public landing framework and data (T-121/T-122):
// routing, healthz, the ru/en render on a fake pool with live SQL data
// semantics, SQL contracts, the graceful stop — httptest without
// PostgreSQL.
package web

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/config"
)

// --- fake pool ---------------------------------------------------------------

// webSeed — the landing data in the fake.
type webSeed struct {
	zones      int64
	total      int64
	checked    int64
	laravel    int64
	last24h    int64
	cloud      [][]any // (name, unicode, laravel)
	table      [][]any // (name, unicode, checked, laravel)
	latest     [][]any // (name, mask, ver_min, ver_max, updated)
	subdomains [][]any // (id, label)
	domainInfo []byte  // the domain_info(text) response, or nil (the domain is unknown)
}

// fakeDB — Pinger + Querier + Execer with the live landing SQL data
// semantics.
type fakeDB struct {
	seed webSeed
	ping error
	// execErr — an Exec failure (the INSERT into web_checks, T-221);
	// nil — success.
	execErr error

	mu      sync.Mutex
	queries []string
	execs   []string
}

func (d *fakeDB) Ping(context.Context) error { return d.ping }

func (d *fakeDB) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom must not be called")
}

func (d *fakeDB) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.execs = append(d.execs, sql)
	if d.execErr != nil {
		return pgconn.CommandTag{}, d.execErr
	}
	return pgconn.CommandTag{}, nil
}

// webCheckInserts — how many INSERT web_checks went through Exec.
func (d *fakeDB) webCheckInserts() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, sql := range d.execs {
		if strings.Contains(sql, "INSERT INTO web_checks") {
			n++
		}
	}
	return n
}

func (d *fakeDB) Query(ctx context.Context, sql string, _ ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.queries = append(d.queries, sql)
	switch sql {
	case statsSQL:
		return webRowsOf([][]any{{d.seed.zones, d.seed.total, d.seed.checked, d.seed.laravel}}), nil
	case last24hSQL:
		return webRowsOf([][]any{{d.seed.last24h}}), nil
	case cloudSQL:
		return webRowsOf(d.seed.cloud), nil
	case tableSQL:
		return webRowsOf(d.seed.table), nil
	case latestSQL:
		return webRowsOf(d.seed.latest), nil
	case subdomainsSQL:
		return webRowsOf(d.seed.subdomains), nil
	case "SELECT domain_info($1::text)":
		if d.seed.domainInfo == nil {
			return webRowsOf(nil), nil // NULL — the domain is unknown
		}
		return webRowsOf([][]any{{d.seed.domainInfo}}), nil
	default:
		return nil, fmt.Errorf("fakeDB: an unexpected SQL: %s", sql)
	}
}

// webRows — pgx.Rows with pre-stocked rows (a positional Scan by the
// value types, the anAggRows style).
type webRows struct {
	rows [][]any
	i    int
}

func webRowsOf(rows [][]any) *webRows { return &webRows{rows: rows} }

func (r *webRows) Close()                        {}
func (r *webRows) Err() error                    { return nil }
func (r *webRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (r *webRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *webRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *webRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) || len(dest) != len(r.rows[r.i-1]) {
		return fmt.Errorf("webRows: Scan without Next or the arity %d ≠ columns", len(dest))
	}
	row := r.rows[r.i-1]
	for i, d := range dest {
		switch dst := d.(type) {
		case *int64:
			*dst = row[i].(int64)
		case *int16:
			*dst = row[i].(int16)
		case *string:
			*dst = row[i].(string)
		case *time.Time:
			*dst = row[i].(time.Time)
		case *[]byte:
			*dst = row[i].([]byte)
		default:
			return fmt.Errorf("webRows: dest[%d] %T", i, d)
		}
	}
	return nil
}

func (r *webRows) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *webRows) RawValues() [][]byte    { return nil }
func (r *webRows) Conn() *pgx.Conn        { return nil }
func (r *webRows) TypeMap() *pgtype.Map   { return nil }

// seedLanding — typical page data (zones/finds).
func seedLanding() webSeed {
	return webSeed{
		zones: 12, total: 1_000_000, checked: 800_000, laravel: 4_242, last24h: 17,
		cloud: [][]any{
			{"com", "", int64(2000)},
			{"de", "", int64(300)},
			{"рф-зона", "xn--", int64(40)},
		},
		table: [][]any{
			{"com", "", int64(800_000), int64(2000)},
			{"de", "", int64(90_000), int64(300)},
		},
		latest: [][]any{
			{"shop.example.com", int64(0b11), "9.19", "12", time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)},
		},
		subdomains: [][]any{{int16(0), ""}, {int16(1), "www"}, {int16(4), "api"}},
	}
}

func newTestServer(t *testing.T, db DB) *Server {
	t.Helper()
	cfg := config.Default()
	cfg.Web.RateLimit = 100 // the routing tests do not hit the limit
	s, err := New(db, cfg, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// --- framework ---------------------------------------------------------------

// TestNewNilDB — without a DB the service is not assembled.
func TestNewNilDB(t *testing.T) {
	cfg := config.Default()
	if _, err := New(nil, cfg, Options{}); err == nil {
		t.Fatal("New(nil) = nil error, want an error")
	}
	if _, err := New(&fakeDB{}, nil, Options{}); err == nil {
		t.Fatal("New(nil cfg) = nil error, want an error")
	}
}

// TestRoutingLanding — the locale landing renders with data, the root
// redirects, a foreign prefix — 404, static assets are served.
func TestRoutingLanding(t *testing.T) {
	srv := newTestServer(t, &fakeDB{seed: seedLanding()})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	for locale, want := range map[string][]string{
		"ru": {"We find Laravel", "Statistics", `data-count="4242"`, "shop.example.com", "We scan millions"},
		"en": {"We find Laravel", "Statistics", `data-count="4242"`, "shop.example.com"},
	} {
		resp, err := http.Get(ts.URL + "/" + locale)
		if err != nil {
			t.Fatalf("/%s: %v", locale, err)
		}
		body := make([]byte, 1<<20)
		n, _ := resp.Body.Read(body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("/%s: status = %d", locale, resp.StatusCode)
			continue
		}
		for _, w := range want {
			if !strings.Contains(string(body[:n]), w) {
				t.Errorf("/%s: the body does not contain %q", locale, w)
			}
		}
	}

	// A foreign locale — 404; static assets — 200 with a type; the root —
	// Location.
	resp, _ := http.Get(ts.URL + "/de")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("/de = %d, want 404", resp.StatusCode)
	}
	_ = resp.Body.Close()

	resp, err := http.Get(ts.URL + "/static/favicon.svg")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("favicon = %d, want 200 (go:embed)", resp.StatusCode)
	}

	req, _ := http.NewRequest("GET", ts.URL+"/", nil)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if loc := resp.Header.Get("Location"); loc != "/"+DefaultLocale {
		t.Errorf("the root Location = %q, want /%s", loc, DefaultLocale)
	}
}

// TestLandingDBError — a data failure: a 500 without internal details.
func TestLandingDBError(t *testing.T) {
	srv := newTestServer(t, brokenDB{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/ru")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("the DB failure: status = %d, want 500", resp.StatusCode)
	}
}

// brokenDB — a Querier failing on every query.
type brokenDB struct{}

func (brokenDB) Ping(context.Context) error { return nil }
func (brokenDB) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("no")
}
func (brokenDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("no")
}
func (brokenDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("the db is unavailable")
}

// TestHealthzDB — an unavailable DB: a 503.
func TestHealthzDB(t *testing.T) {
	srv := newTestServer(t, &fakeDB{ping: errors.New("connection refused")})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("healthz = %d, want 503", resp.StatusCode)
	}
}

// TestMethodNotAllowed — a non-GET on the pages and a non-POST on the
// API — 405.
func TestMethodNotAllowed(t *testing.T) {
	srv := newTestServer(t, &fakeDB{seed: seedLanding()})
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
			t.Errorf("%s %s: status = %d, want 405", tc.method, tc.path, resp.StatusCode)
		}
	}
}

// TestRunGracefulStop — Run listens, a canceled ctx stops the server
// with a clean nil within the stop window.
func TestRunGracefulStop(t *testing.T) {
	srv := newTestServer(t, &fakeDB{seed: seedLanding()})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

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
			t.Fatal("the server did not start within 2 s")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after the cancellation: %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the server did not stop after the ctx cancellation")
	}
}

// TestRunUnixSocket — a listen with an absolute path = a unix socket
// (the deploy variant behind nginx without a TCP port, T-124): the
// server answers, a clean stop removes the socket file. An orphaned
// file (a crash of the previous run — SIGKILL, the defer did not fire)
// is removed before Listen: otherwise a restart by Restart=on-failure
// would loop on address already in use.
func TestRunUnixSocket(t *testing.T) {
	srv := newTestServer(t, &fakeDB{seed: seedLanding()})
	sock := filepath.Join(t.TempDir(), "web.sock")

	// An "orphaned" socket of the previous run: a regular file on the
	// Listen path.
	if err := os.WriteFile(sock, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx, sock) }()

	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		},
	}}
	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err := client.Get("http://unix/healthz")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("the unix socket did not start within 2 s")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run(unix) after the cancellation: %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the unix socket did not stop after the ctx cancellation")
	}
	if _, err := os.Stat(sock); !os.IsNotExist(err) {
		t.Errorf("the socket file %s was not removed after the stop (stat err: %v)", sock, err)
	}
}

// TestRobotsSitemap — the search engine service files at the root
// (T-124): robots.txt — static assets from embed; sitemap.xml — a
// minimal set of locales with an absolute URL (the host — the request
// Host when web.public_host is empty; behind the reverse proxy the
// scheme is carried by X-Forwarded-Proto — T-227 trusts it only from a
// loopback/unix peer); a POST on the routes — 405.
func TestRobotsSitemap(t *testing.T) {
	srv := newTestServer(t, &fakeDB{seed: seedLanding()})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/robots.txt")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("robots.txt = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Errorf("robots.txt Content-Type = %q, want text/plain", ct)
	}
	for _, want := range []string{"User-agent: *", "Disallow: /api/"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("robots.txt does not contain %q", want)
		}
	}

	// sitemap: the URLs of both locales with the request host; nothing
	// extra.
	resp, err = http.Get(ts.URL + "/sitemap.xml")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("sitemap.xml = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/xml; charset=utf-8" {
		t.Errorf("sitemap.xml Content-Type = %q, want application/xml", ct)
	}
	s := string(body)
	if !strings.Contains(s, `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`) {
		t.Errorf("sitemap.xml without the sitemap 0.9 schema:\n%s", s)
	}
	for _, locale := range Locales {
		if !strings.Contains(s, "<loc>"+ts.URL+"/"+locale+"</loc>") {
			t.Errorf("sitemap.xml without the locale URL /%s:\n%s", locale, s)
		}
	}
	if n := strings.Count(s, "<loc>"); n != len(Locales) {
		t.Errorf("sitemap.xml holds %d URLs, want %d (locales only):\n%s", n, len(Locales), s)
	}

	// Behind the reverse proxy the scheme is carried by X-Forwarded-Proto
	// (the public entry — TLS), the Host is taken from the request
	// (public_host is not set — the fallback). The header is read only
	// from a trusted peer (T-227): by default httptest.NewRequest sets
	// the RemoteAddr to 192.0.2.1:1234 (TEST-NET-1, an untrusted peer) —
	// hence the peer is set explicitly to a loopback address, like the
	// real nginx on this host. A foreign header value is ignored (the
	// connection scheme stays).
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/sitemap.xml", nil)
	req.RemoteAddr = "127.0.0.1:60001"
	req.Host = "crawler.example.com"
	req.Header.Set("X-Forwarded-Proto", "https")
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("sitemap.xml behind the proxy = %d, want 200", rec.Code)
	}
	if want := "<loc>https://crawler.example.com/ru</loc>"; !strings.Contains(rec.Body.String(), want) {
		t.Errorf("sitemap.xml behind the proxy without %q:\n%s", want, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/sitemap.xml", nil)
	req.RemoteAddr = "127.0.0.1:60002"
	req.Host = "crawler.example.com"
	req.Header.Set("X-Forwarded-Proto", "javascript") // junk — not a web scheme
	srv.Handler().ServeHTTP(rec, req)
	if want := "<loc>http://crawler.example.com/ru</loc>"; !strings.Contains(rec.Body.String(), want) {
		t.Errorf("sitemap.xml with a junk scheme without %q:\n%s", want, rec.Body.String())
	}

	// GET-only routes: a POST is served by the mux as a 405.
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/robots.txt", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST /robots.txt = %d, want 405", resp.StatusCode)
	}
}

// TestSitemapCanonicalHost — the sitemap <loc> does not depend on the
// Host and X-Forwarded-Proto of untrusted peers (T-227): the host — the
// canonical web.public_host (an empty field — the fallback to the
// request Host); the scheme — the TLS fact or XFP only from a trusted
// peer (loopback TCP/a unix socket).
func TestSitemapCanonicalHost(t *testing.T) {
	// body — the sitemap body for a request with the given public_host,
	// peer, Host and XFP; unix — a request "from nginx behind a unix
	// socket" (the local address context, as in TestClientIP);
	// directTLS — a direct TLS connection.
	body := func(publicHost, remote, host, xfp string, unix, directTLS bool) string {
		cfg := config.Default()
		cfg.Web.PublicHost = publicHost
		srv, err := New(&fakeDB{seed: seedLanding()}, cfg, Options{})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		req := httptest.NewRequest(http.MethodGet, "/sitemap.xml", nil)
		req.RemoteAddr = remote
		req.Host = host
		if unix {
			req = req.WithContext(context.WithValue(req.Context(), http.LocalAddrContextKey,
				&net.UnixAddr{Name: "@", Net: "unix"}))
		}
		if directTLS {
			req.TLS = &tls.ConnectionState{}
		}
		if xfp != "" {
			req.Header.Set("X-Forwarded-Proto", xfp)
		}
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		return rec.Body.String()
	}
	const evil = `evil.example.org`
	for _, tc := range []struct {
		name       string
		publicHost string
		remote     string
		host       string
		xfp        string
		unix       bool
		directTLS  bool
		want       string
	}{
		{
			// The canonical host is set: a direct request with foreign
			// Host/XFP affects neither the host nor the scheme (the
			// default — http, a connection without TLS).
			name:       "canonical + an untrusted peer with foreign Host/XFP",
			publicHost: "crawler.example.com", remote: "203.0.113.9:40001", host: evil,
			xfp:  "https",
			want: "<loc>http://crawler.example.com/ru</loc>",
		},
		{
			// The canonical host + a trusted loopback peer with XFP
			// https: the scheme is https, the request Host is ignored.
			name:       "canonical + a loopback proxy with XFP https",
			publicHost: "crawler.example.com", remote: "127.0.0.1:40002", host: evil,
			xfp:  "https",
			want: "<loc>https://crawler.example.com/ru</loc>",
		},
		{
			// The canonical host + a trusted unix-socket peer: the same
			// result.
			name:       "canonical + a unix proxy with XFP https",
			publicHost: "crawler.example.com", remote: "@", host: evil,
			xfp: "https", unix: true,
			want: "<loc>https://crawler.example.com/ru</loc>",
		},
		{
			// The canonical host + an XFP list: the last entry (ours),
			// the start of the list came from the client.
			name:       "canonical + an XFP list (client, proxy)",
			publicHost: "crawler.example.com", remote: "127.0.0.1:40003", host: evil,
			xfp:  "http, https",
			want: "<loc>https://crawler.example.com/ru</loc>",
		},
		{
			// A direct TLS connection: https regardless of the XFP (web
			// does not listen this way — the publicScheme branch for the
			// future, T-227).
			name:       "canonical + a direct TLS with XFP http",
			publicHost: "crawler.example.com", remote: "203.0.113.9:40006", host: evil,
			xfp: "http", directTLS: true,
			want: "<loc>https://crawler.example.com/ru</loc>",
		},
		{
			// No canonical host: the fallback to the request Host behind
			// a trusted peer (the public entry passes the Host as
			// $host).
			name:   "the Host fallback + a loopback proxy with XFP https",
			remote: "127.0.0.1:40004", host: "crawler.example.com",
			xfp:  "https",
			want: "<loc>https://crawler.example.com/ru</loc>",
		},
		{
			// No canonical host, the peer is untrusted: the XFP is not
			// read — the default connection scheme (http).
			name:   "the Host fallback + an untrusted peer with XFP https",
			remote: "203.0.113.9:40005", host: "crawler.example.com",
			xfp:  "https",
			want: "<loc>http://crawler.example.com/ru</loc>",
		},
	} {
		got := body(tc.publicHost, tc.remote, tc.host, tc.xfp, tc.unix, tc.directTLS)
		if !strings.Contains(got, tc.want) {
			t.Errorf("%s: the sitemap without %q:\n%s", tc.name, tc.want, got)
		}
		if strings.Contains(got, evil) {
			t.Errorf("%s: the sitemap depends on the Host of an untrusted party (%s):\n%s", tc.name, evil, got)
		}
	}
}

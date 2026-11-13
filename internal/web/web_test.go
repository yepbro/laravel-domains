// Unit tests of the public API framework and data (T-121/T-122, L-08):
// routing (the former landing routes — honest 404s), healthz, the SQL
// data semantics on a fake pool, the graceful stop — httptest without
// PostgreSQL.
package web

import (
	"context"
	"errors"
	"fmt"
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

// TestLandingRoutesRemoved — L-08 (TZ-ld-landing §7.3): the landing lives
// in the static ld-landing repo, the former landing routes of the service
// are the router's honest 404 — no redirect, no page, no SEO service
// files, no static assets.
func TestLandingRoutesRemoved(t *testing.T) {
	srv := newTestServer(t, &fakeDB{seed: seedLanding()})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// No redirects: the old GET / → /en 302 is gone together with the
	// landing (the static sites of ld-landing own their redirects).
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	for _, path := range []string{
		"/", "/ru", "/en", "/de", "/ru/about",
		"/robots.txt", "/sitemap.xml",
		"/static/favicon.svg", "/static/app.css", "/static/landing.js",
	} {
		req, err := http.NewRequest(http.MethodGet, ts.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		// No redirects: the old GET / → /en 302 is gone together with the
		// landing (the static sites of ld-landing own their redirects).
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404 (the landing is static now, L-08)", path, resp.StatusCode)
		}
		if loc := resp.Header.Get("Location"); loc != "" {
			t.Errorf("GET %s: Location = %q, want empty (no redirects)", path, loc)
		}
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

// TestMethodNotAllowed — a method mismatch on a registered path — the
// mux's honest 405 (Go 1.22: the path patterns are method-scoped; before
// L-08 a GET on the form endpoint needed an explicit handler — the
// landing catch-all GET / would answer a misleading 404).
func TestMethodNotAllowed(t *testing.T) {
	srv := newTestServer(t, &fakeDB{seed: seedLanding()})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	for _, tc := range []struct{ method, path string }{
		{"GET", "/api/check-domain"},
		{"POST", "/healthz"},
		{"POST", "/api/v1/landing/stats"},
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
		// The mux's own 405 carries the allowed methods (Go 1.22) — the
		// contract the automatic answer is relied on for (the explicit
		// GET handler of the landing era is gone with the landing).
		if allow := resp.Header.Get("Allow"); allow == "" {
			t.Errorf("%s %s: the 405 without an Allow header", tc.method, tc.path)
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

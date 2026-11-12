// Tests of the form on a unix socket (T-221): a unix-socket peer is a
// trusted proxy (the deploy variant of deploy/nginx/README.md §"The
// unix-socket variant"; the peer's RemoteAddr is "@" for an unnamed
// autobind client or the socket path, never an IP). The criteria:
// X-Real-IP/X-Forwarded-For give independent rate limit buckets (before
// T-221 — one bucket for all visitors), a peer without a valid header —
// the shared label bucket without writing a non-IP into inet (the
// INSERT is not attempted, a Warn in the log), an INSERT error is
// logged, the form metrics are registered and served by the registry.
package web

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/checker"
	"crawler/internal/config"
	"crawler/internal/metrics"
)

// newUnixTestServer — an httptest server on a unix socket in a
// temporary directory; returns a client with a dialer to the socket.
func newUnixTestServer(t *testing.T, h http.Handler) *http.Client {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "web.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	ts := httptest.NewUnstartedServer(h)
	ts.Listener = ln
	ts.Start()
	t.Cleanup(ts.Close)
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		},
	}}
}

// postFormUnix — a POST of the form through the client with proxy
// headers (X-Real-IP/X-Forwarded-For); an empty value — the header is
// not set.
func postFormUnix(t *testing.T, c *http.Client, realIP, xff string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://unix/api/check-domain",
		strings.NewReader(`{"domain":"example.com"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if realIP != "" {
		req.Header.Set("X-Real-IP", realIP)
	}
	if xff != "" {
		req.Header.Set("X-Forwarded-For", xff)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("POSTing the form: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

// fakeLiveCheck — the live check seam: an always successful result.
func fakeLiveCheck() checker.LiveFunc {
	return func(context.Context, *config.Config, checker.Pool, string, checker.Logger) (*checker.Result, error) {
		return &checker.Result{Domain: "example.com", Found: true, Score: 0.5}, nil
	}
}

// serveOn — a direct call of the server handler with the given
// RemoteAddr.
func serveOn(srv *Server, remote, body string) int {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/check-domain", strings.NewReader(body))
	req.RemoteAddr = remote
	srv.Handler().ServeHTTP(rec, req)
	return rec.Code
}

// TestCheckDomainUnixSocketBuckets — the T-221 criterion: on a unix
// socket two different X-Real-IPs get independent buckets (the first
// exhausted the limit — the second passes); X-Forwarded-For without
// X-Real-IP works the same way; an "@" peer without headers — the
// shared unixLocalKey label bucket, the web_checks INSERT is not
// attempted for it at all (inet NOT NULL), the skip is visible as a
// Warn in the service log.
func TestCheckDomainUnixSocketBuckets(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	db := &fakeDB{seed: seedLanding()}
	cfg := config.Default()
	cfg.Web.RateLimit = 1 // one request per bucket: independence is visible at once
	srv, err := New(db, cfg, Options{Log: log})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv.liveCheck = fakeLiveCheck()
	client := newUnixTestServer(t, srv.Handler())

	// Independent buckets by X-Real-IP: the first IP exhausted the limit
	// — the second passes (before T-221 the "@" peer was not trusted and
	// all visitors shared one bucket: 4 checks/min for the whole site).
	if st := postFormUnix(t, client, "198.51.100.1", ""); st != http.StatusOK {
		t.Fatalf("the first request with X-Real-IP A: %d, want 200", st)
	}
	if st := postFormUnix(t, client, "198.51.100.1", ""); st != http.StatusTooManyRequests {
		t.Fatalf("a repeat with X-Real-IP A: %d, want 429 (the limit 1 is exhausted)", st)
	}
	if st := postFormUnix(t, client, "198.51.100.2", ""); st != http.StatusOK {
		t.Fatalf("a request with X-Real-IP B: %d, want 200 (an independent bucket)", st)
	}
	// X-Forwarded-For without X-Real-IP — the fallback address source:
	// the key is the LAST entry (our proxy appended it; the client start
	// of the list is not trusted — P1-1 of the T-221 review).
	if st := postFormUnix(t, client, "", "6.6.6.6, 198.51.100.3"); st != http.StatusOK {
		t.Fatalf("a request with X-Forwarded-For C: %d, want 200 (its own bucket by the last entry)", st)
	}
	// A forged first entry of the XFF gives no new bucket: the same
	// address at the end of the list — the limit is exhausted.
	if st := postFormUnix(t, client, "", "1.2.3.4, 198.51.100.3"); st != http.StatusTooManyRequests {
		t.Fatalf("forging the start of the XFF: %d, want 429 (one bucket — by the last entry)", st)
	}
	if got := db.webCheckInserts(); got != 3 {
		t.Fatalf("the INSERT web_checks ran %d times, want 3 (one per valid IP)", got)
	}

	// An "@" peer without headers: the shared label bucket — the first
	// passes, the second exhausts it; the DB write is not attempted
	// (inet NOT NULL), the skip of each request is a Warn in the log.
	warnsBefore := strings.Count(buf.String(), "web_checks")
	if st := postFormUnix(t, client, "", ""); st != http.StatusOK {
		t.Fatalf("the first request without headers: %d, want 200 (the bucket key is fresh)", st)
	}
	if st := postFormUnix(t, client, "", ""); st != http.StatusTooManyRequests {
		t.Fatalf("the second request without headers: %d, want 429 (the shared bucket key)", st)
	}
	if got := db.webCheckInserts(); got != 3 {
		t.Fatalf("the INSERT web_checks after a non-IP peer: %d calls, still want 3 (no write is attempted)", got)
	}
	// The log skip — a Warn per each completed request (the first passed
	// the limit and reached the log; the second — a 429, it did not
	// reach the log).
	if got, want := strings.Count(buf.String(), "web_checks")-warnsBefore, 1; got != want {
		t.Errorf("%d journal-skip Warns, want %d (not silently):\n%s", got, want, buf.String())
	}
}

// TestCheckDomainUnixJournalError — a web_checks INSERT failure (a
// unix peer with a valid X-Real-IP): the response to the visitor is not
// broken, the failure is logged with a Warn (before T-221 it went
// through Query without reading rows — silently).
func TestCheckDomainUnixJournalError(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	db := &fakeDB{seed: seedLanding(), execErr: errors.New("inet violation")}
	cfg := config.Default()
	cfg.Web.RateLimit = 10
	srv, err := New(db, cfg, Options{Log: log})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv.liveCheck = fakeLiveCheck()
	client := newUnixTestServer(t, srv.Handler())

	if st := postFormUnix(t, client, "198.51.100.7", ""); st != http.StatusOK {
		t.Fatalf("the response on a journal failure: %d, want 200 (best-effort)", st)
	}
	if !strings.Contains(buf.String(), "web_checks") {
		t.Errorf("the INSERT web_checks failure is not logged:\n%s", buf.String())
	}
	if got := db.webCheckInserts(); got != 1 {
		t.Fatalf("INSERT web_checks attempts %d, want 1", got)
	}
}

// TestRecordCheckClientCanceled — a visitor who left (the request
// context canceled after the live check) does not look like a log
// failure: the web_checks INSERT lives in its own window without the
// client cancellation (P3-5 of the T-221 review) — the audit of
// "who/what/when checked" does not depend on the disconnect.
func TestRecordCheckClientCanceled(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	db := &fakeDB{seed: seedLanding()}
	srv, err := New(db, config.Default(), Options{Log: log})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	srv.recordCheck(ctx, "example.com", true, 0.5, 10, "198.51.100.1")

	if got := db.webCheckInserts(); got != 1 {
		t.Fatalf("the INSERT web_checks with the client context canceled: %d, want 1 (the journal lives in its own window)", got)
	}
	if buf.Len() != 0 {
		t.Errorf("the client cancellation reflected as a journal failure (it must not be):\n%s", buf.String())
	}
}

// counterByLabels — the value of the name counter with the exact label
// set, except the service one; -1 if the row is not found (the
// retentionmetrics_test style).
func counterByLabels(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
	nextMetric:
		for _, m := range mf.GetMetric() {
			got := make(map[string]string)
			for _, lp := range m.GetLabel() {
				if lp.GetName() == metrics.LabelService {
					continue
				}
				got[lp.GetName()] = lp.GetValue()
			}
			for k, v := range labels {
				if got[k] != v {
					continue nextMetric
				}
			}
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
		}
	}
	return -1
}

// TestCheckDomainMetrics — the form metrics (T-221) are registered in
// the service registry and served by /metrics (:9107): the request
// outcomes by result (ok/rate_limited/invalid/failed/busy), the
// failures by stage (live/journal/non_ip), the duration — as a
// histogram.
func TestCheckDomainMetrics(t *testing.T) {
	reg := metrics.New("web")
	db := &fakeDB{seed: seedLanding(), execErr: errors.New("insert failed")}
	cfg := config.Default()
	cfg.Web.RateLimit = 1
	cfg.Web.LiveConcurrency = 1
	srv, err := New(db, cfg, Options{Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv.liveCheck = fakeLiveCheck()

	// ok + a log failure (journal): the first request from IP A.
	if st := serveOn(srv, "203.0.113.1:1000", `{"domain":"example.com"}`); st != http.StatusOK {
		t.Fatalf("the ok request: %d, want 200", st)
	}
	// rate_limited: a repeat from the same IP A.
	if st := serveOn(srv, "203.0.113.1:1000", `{"domain":"example.com"}`); st != http.StatusTooManyRequests {
		t.Fatalf("a repeat: %d, want 429", st)
	}
	// invalid: a junk domain from IP B (the validation after the limit).
	if st := serveOn(srv, "203.0.113.2:1000", `{"domain":"not a domain"}`); st != http.StatusUnprocessableEntity {
		t.Fatalf("junk input: %d, want 422", st)
	}

	// busy: the semaphore slot is taken by a blocked pipeline (IP C),
	// a request from IP D — a fast 503; the release — the holder answers
	// 200.
	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	done := make(chan int, 1)
	srv.liveCheck = func(context.Context, *config.Config, checker.Pool, string, checker.Logger) (*checker.Result, error) {
		entered <- struct{}{}
		<-block
		return &checker.Result{Domain: "example.com"}, nil
	}
	go func() { done <- serveOn(srv, "203.0.113.4:1000", `{"domain":"example.com"}`) }()
	<-entered
	if st := serveOn(srv, "203.0.113.5:1000", `{"domain":"example.com"}`); st != http.StatusServiceUnavailable {
		t.Fatalf("a request over a busy slot: %d, want 503 busy", st)
	}
	close(block)
	if st := <-done; st != http.StatusOK {
		t.Fatalf("the slot holder: %d, want 200", st)
	}

	// failed + a live error: a pipeline failure without stored data (a
	// TCP peer), then a unix-socket peer without headers — the same
	// outcome plus non_ip.
	failing, err := New(db, cfg, Options{Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	failing.liveCheck = func(context.Context, *config.Config, checker.Pool, string, checker.Logger) (*checker.Result, error) {
		return nil, errors.New("the pipeline is unavailable")
	}
	if st := serveOn(failing, "203.0.113.3:1000", `{"domain":"example.com"}`); st != http.StatusBadGateway {
		t.Fatalf("a pipeline failure: %d, want 502", st)
	}
	if st := serveOn(failing, "@", `{"domain":"example.com"}`); st != http.StatusBadGateway {
		t.Fatalf("a unix peer without headers: %d, want 502 (a pipeline failure, no stored data)", st)
	}

	// stored_only: the live check failed, the domain is known to the
	// pipeline — a 200 from the stored data (P3-3 of the T-221 review).
	seed := seedLanding()
	seed.domainInfo = []byte(`{"domain":{"status":0,"status_name":"new","last_checked_at":"2026-11-01"}}`)
	stored, err := New(&fakeDB{seed: seed}, cfg, Options{Metrics: reg})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stored.liveCheck = failing.liveCheck
	if st := serveOn(stored, "203.0.113.6:1000", `{"domain":"example.com"}`); st != http.StatusOK {
		t.Fatalf("a pipeline failure with stored data: %d, want 200 (stored_only)", st)
	}

	for _, tc := range []struct {
		name string
		got  float64
		want float64
	}{
		{"checks result=ok", counterByLabels(t, reg, "crawler_web_checks_total", map[string]string{"result": "ok"}), 2},
		{"checks result=rate_limited", counterByLabels(t, reg, "crawler_web_checks_total", map[string]string{"result": "rate_limited"}), 1},
		{"checks result=invalid", counterByLabels(t, reg, "crawler_web_checks_total", map[string]string{"result": "invalid"}), 1},
		{"checks result=busy", counterByLabels(t, reg, "crawler_web_checks_total", map[string]string{"result": "busy"}), 1},
		{"checks result=failed", counterByLabels(t, reg, "crawler_web_checks_total", map[string]string{"result": "failed"}), 2},
		{"checks result=stored_only", counterByLabels(t, reg, "crawler_web_checks_total", map[string]string{"result": "stored_only"}), 1},
		{"errors type=journal", counterByLabels(t, reg, "crawler_web_check_errors_total", map[string]string{"type": "journal"}), 3},
		{"errors type=live", counterByLabels(t, reg, "crawler_web_check_errors_total", map[string]string{"type": "live"}), 3},
		{"errors type=non_ip", counterByLabels(t, reg, "crawler_web_check_errors_total", map[string]string{"type": "non_ip"}), 1},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %v, want %v", tc.name, tc.got, tc.want)
		}
	}

	// Duration — a histogram: observed by everyone who reached the live
	// check (2 ok + 2 failed + 1 stored_only), 429/422/busy were not
	// observed.
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	histogramObserved := false
	for _, mf := range families {
		if mf.GetName() != "crawler_web_check_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if h := m.GetHistogram(); h != nil && h.GetSampleCount() == 5 {
				histogramObserved = true
			}
		}
	}
	if !histogramObserved {
		t.Error("crawler_web_check_seconds without 5 observations (2 ok + 2 failed + 1 stored_only)")
	}

	// Serving the registry by the /metrics handler (like the framework
	// on :9107).
	rec := httptest.NewRecorder()
	metrics.Handler(reg).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		"crawler_web_checks_total", "crawler_web_check_errors_total", "crawler_web_check_seconds",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics does not serve %s", want)
		}
	}
}

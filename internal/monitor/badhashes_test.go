// Unit tests of the junk content_hash alert of ingest cleanup (T-099) and its
// HTTP source: the spike threshold over a window, the basing of the first observation
// (a monitoring start later than the accumulated counter — not a spike), a restart
// of ingest (the counter started anew — no false firing), the window aging
// without a continuing flow, the source degradation, the exposition parsing
// of Prometheus and the probe behavior over HTTP (statuses, a missing metric,
// a ctx cancellation). The live probe against the ingest /metrics — the integration level.
package monitor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeBadHashes — a counter source per a stub: the value changes between
// passes, an error queue (empty — an answer without an error).
type fakeBadHashes struct {
	mu    sync.Mutex
	val   int64
	errs  []error
	calls int
}

func (f *fakeBadHashes) BadHashes(context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if err := popErr(&f.errs); err != nil {
		return 0, err
	}
	return f.val, nil
}

func (f *fakeBadHashes) set(v int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.val = v
}

// --- the alert -----------------------------------------------------------------

// TestAlertsBadHashesThreshold — accumulating deltas up to the threshold by the window:
// jumps below the threshold one by one do not fire; a sum over the window ≥
// AlertBadHashesThreshold — firing; without a continuing flow the window ages
// and the alert is cleared.
func TestAlertsBadHashesThreshold(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	m := env.newBadHashesMachine()

	// The first observation — the base: what accumulated before the monitoring
	// start (100) does not count as a spike.
	// Growths of 5 and 3 — a sum of 8 < 10: ok.
	// 4 more — the sum of 12 ≥ 10: a spike.
	steps := []struct {
		cur  int64
		want string
	}{
		{100, AlertStatusOK},
		{105, AlertStatusOK},
		{108, AlertStatusOK},
		{112, AlertStatusFiring},
	}
	for i, s := range steps {
		got := m.transition(env.Alerts, ctx, obs(s.cur))
		if got.Status != s.want {
			t.Errorf("step %d (counter %d): status = %s (%s), want %s", i, s.cur, got.Status, got.Detail, s.want)
		}
		if i == len(steps)-1 {
			for _, want := range []string{"112", "12", "10"} { // the counter, the sum, the threshold
				if !strings.Contains(got.Detail, want) {
					t.Errorf("the detail %q does not contain %s", got.Detail, want)
				}
			}
		}
	}

	// It holds for the next pass (the sum is still in the window)…
	if got := m.transition(env.Alerts, ctx, obs(int64(112))); got.Status != AlertStatusFiring {
		t.Errorf("the window still holds the sum: status = %s (%s), want firing", got.Status, got.Detail)
	}

	// …and ages without new junk rows: after the window of zero deltas
	// the sum zeroes out — clearing.
	for range AlertBadHashesWindow + 2 {
		if got := m.transition(env.Alerts, ctx, obs(int64(112))); got.Status == AlertStatusOK {
			return
		}
	}
	t.Fatal("the alert was not cleared after the window aging without new junk rows")
}

// TestBadHashesWindow — the delta ring size (deltaRing): the window of exactly 15 values —
// the spike is held by the sum until the eviction by the last (15th) zero
// delta. The size is pinned by a literal at the start: a boundary from the
// constant would mutate together with the test and "follow" the mutant. The write step
// is not pinned separately: any step coprime with the length gives the same
// set of last deltas (see the annotation in push).
func TestBadHashesWindow(t *testing.T) {
	if AlertBadHashesWindow != 15 {
		t.Fatalf("AlertBadHashesWindow = %d, want 15 (a window of 15 alert passes)", AlertBadHashesWindow)
	}
	w := &deltaRing{}
	if sum := w.push(0); sum != 0 {
		t.Fatalf("the first observation (the base): the sum = %d, want 0", sum)
	}
	if sum := w.push(10); sum != 10 {
		t.Fatalf("a spike of +10: the sum = %d, want 10", sum)
	}
	// The window of 15: the spike holds while 14 subsequent deltas have not evicted
	// it from the ring (without counter growth the deltas are zero).
	for i := 1; i <= AlertBadHashesWindow-1; i++ {
		if sum := w.push(10); sum != 10 {
			t.Fatalf("after %d zero deltas: the sum = %d, want 10 (the window still holds)", i, sum)
		}
	}
	// The 15th zero delta overwrites the spike cell — the sum is 0.
	if sum := w.push(10); sum != 0 {
		t.Fatalf("the window freed up: the sum = %d, want 0", sum)
	}
}

// TestAlertsBadHashesExactThreshold — a sum of exactly AlertBadHashesThreshold is
// already a spike (≥, not "strictly greater"); the detail names the window — 15
// passes of the test env interval (DefaultAlertsInterval 5m) = 1h15m.
func TestAlertsBadHashesExactThreshold(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	m := env.newBadHashesMachine()

	_ = m.transition(env.Alerts, ctx, obs(int64(0)))   // the base
	s := m.transition(env.Alerts, ctx, obs(int64(10))) // a single delta of exactly 10
	if s.Status != AlertStatusFiring {
		t.Fatalf("the sum exactly 10: status = %s (%s), want firing", s.Status, s.Detail)
	}
	for _, want := range []string{"1h15m0s", "10"} { // the window of 15 × 5m, the threshold
		if !strings.Contains(s.Detail, want) {
			t.Errorf("the detail %q does not contain %s", s.Detail, want)
		}
	}
}

// TestAlertsBadHashesRestart — an ingest restart resets the counter: a decrease
// gives a zero delta, no false spike (the counter is counted anew).
func TestAlertsBadHashesRestart(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	m := env.newBadHashesMachine()

	_ = m.transition(env.Alerts, ctx, obs(int64(50)))                                      // the base
	if got := m.transition(env.Alerts, ctx, obs(int64(50))); got.Status != AlertStatusOK { // silence
		t.Fatalf("silence: status = %s (%s), want ok", got.Status, got.Detail)
	}

	// An ingest restart: the counter is below the previous one — a decrease gives a zero
	// delta, no false spike (the counter is counted anew).
	if got := m.transition(env.Alerts, ctx, obs(int64(3))); got.Status != AlertStatusOK {
		t.Errorf("a source restart: status = %s (%s), want ok without a false spike", got.Status, got.Detail)
	}

	// New growth after the restart is counted from the new base: +17 ≥ the threshold.
	if got := m.transition(env.Alerts, ctx, obs(int64(20))); got.Status != AlertStatusFiring {
		t.Errorf("growth after the restart: status = %s (%s), want firing", got.Status, got.Detail)
	}
}

// TestAlertsBadHashesDegradation — without a source the alert is "not measured"
// quietly; a source error (ingest unavailable) — a Warn and "not measured",
// the next pass is alive.
func TestAlertsBadHashesDegradation(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	if s := snapshotOf(t, env.Pass(context.Background()), AlertBadHashes); s.Status != AlertStatusNotMeasured {
		t.Fatalf("without a source: status = %s, want not_measured", s.Status)
	}

	src := &fakeBadHashes{errs: []error{errors.New("connection refused")}}
	env.deps.BadHashes = src
	s := snapshotOf(t, env.Pass(context.Background()), AlertBadHashes)
	if s.Status != AlertStatusNotMeasured {
		t.Fatalf("a source error: status = %s, want not_measured", s.Status)
	}
	if !strings.Contains(env.buf.String(), "level=WARN") || !strings.Contains(env.buf.String(), "connection refused") {
		t.Errorf("a source error without a Warn:\n%s", env.buf.String())
	}

	s = snapshotOf(t, env.Pass(context.Background()), AlertBadHashes)
	if s.Status != AlertStatusOK {
		t.Errorf("after the error status = %s, want ok (the loop lives)", s.Status)
	}
}

// --- the HTTP source -------------------------------------------------------------

// TestIngestBadHashesHTTP — the probe reads the counter from the /metrics exposition:
// the real format (HELP/TYPE, neighboring metrics with labels) and the
// status/network errors.
func TestIngestBadHashesHTTP(t *testing.T) {
	const exposition = "# HELP crawler_service_up The service is up.\n" +
		"# TYPE crawler_service_up gauge\n" +
		"crawler_service_up{service=\"ingest\"} 1\n" +
		"# HELP crawler_ingest_bad_hashes_total Results removed by the cleanup.\n" +
		"# TYPE crawler_ingest_bad_hashes_total counter\n" +
		"crawler_ingest_bad_hashes_total{service=\"ingest\"} 42\n"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(exposition))
	}))
	defer srv.Close()

	got, err := (IngestBadHashes{URL: srv.URL}).BadHashes(context.Background())
	if err != nil {
		t.Fatalf("BadHashes: %v", err)
	}
	if got != 42 {
		t.Errorf("the counter = %d, want 42", got)
	}

	// An invalid URL (a control character) — a request build error before
	// the network; the error value is strictly 0 (not ±1).
	got, err = (IngestBadHashes{URL: "http://ingest:9101/\n"}).BadHashes(context.Background())
	if err == nil || got != 0 {
		t.Errorf("a URL error: (%d, %v), want (0, an error)", got, err)
	}

	// A missing metric (an old ingest) — a source error with the name.
	noMetric := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("crawler_service_up{service=\"ingest\"} 1\n"))
	}))
	defer noMetric.Close()
	if n, err := (IngestBadHashes{URL: noMetric.URL}).BadHashes(context.Background()); err == nil ||
		!strings.Contains(err.Error(), badHashesMetricName) || n != 0 {
		t.Errorf("no metric: (%d, %v), want (0, an error with the metric name)", n, err)
	}

	// Non-200 — an error with the status.
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer bad.Close()
	if n, err := (IngestBadHashes{URL: bad.URL}).BadHashes(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "503") || n != 0 {
		t.Errorf("status 503: (%d, %v), want (0, an error with the status)", n, err)
	}

	// An unreachable server — a transport error.
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := dead.URL
	dead.Close()
	if n, err := (IngestBadHashes{URL: url}).BadHashes(context.Background()); err == nil || n != 0 {
		t.Errorf("a closed server: (%d, %v), want (0, an error)", n, err)
	}

	// A ctx cancellation — a context error, not a hang.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (IngestBadHashes{URL: srv.URL}).BadHashes(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("a ctx cancellation: error = %v, want context.Canceled", err)
	}
}

// roundTripFunc — a transport with a fixed answer, no network.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// closeTrackingBody — a response body with a Close mark.
type closeTrackingBody struct {
	io.Reader
	closed bool
}

func (b *closeTrackingBody) Close() error {
	b.closed = true
	return nil
}

// TestIngestBadHashesClosesBody — the response body is closed after parsing:
// the only resource of the method; a leak would hold the ingest connections.
func TestIngestBadHashesClosesBody(t *testing.T) {
	body := &closeTrackingBody{Reader: strings.NewReader(
		"crawler_ingest_bad_hashes_total{service=\"ingest\"} 42\n")}
	cl := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       body,
			Header:     make(http.Header),
		}, nil
	})}

	got, err := (IngestBadHashes{URL: "http://ingest:9101/metrics", Client: cl}).BadHashes(context.Background())
	if err != nil {
		t.Fatalf("BadHashes: %v", err)
	}
	if got != 42 {
		t.Errorf("the counter = %d, want 42", got)
	}
	if !body.closed {
		t.Error("the response body was not closed")
	}
}

// TestParseBadHashesExposition — a table parse of the format: labels, scientific
// notation, a timestamp after the value, comments and junk — miss.
func TestParseBadHashesExposition(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		want    int64
		wantErr string // "" — success
		wantHas string // the offending value in the error text ("" — does not matter)
	}{
		{
			"a row with labels",
			"crawler_ingest_bad_hashes_total{service=\"ingest\"} 42\n",
			42, "", "",
		},
		{
			"scientific notation",
			"crawler_ingest_bad_hashes_total 1.7e+06\n",
			1700000, "", "",
		},
		{
			"a timestamp after the value is ignored",
			"crawler_ingest_bad_hashes_total{service=\"ingest\"} 7 1697760000000\n",
			7, "", "",
		},
		{
			"neighboring metrics and HELP/TYPE skipped",
			"# HELP crawler_stage_tasks_total Tasks.\n" +
				"# TYPE crawler_stage_tasks_total counter\n" +
				"crawler_stage_tasks_total{service=\"dns-worker\",stage=\"dns\"} 3\n" +
				"# TYPE crawler_ingest_bad_hashes_total counter\n" +
				"crawler_ingest_bad_hashes_total{service=\"ingest\"} 5\n",
			5, "", "",
		},
		// Zero is a valid counter (an ingest restart): the "v < 0" boundary
		// is inclusive; negatives and non-numbers — an error naming
		// the offending value.
		{"a zero value", "crawler_ingest_bad_hashes_total 0\n", 0, "", ""},
		{"minus one", "crawler_ingest_bad_hashes_total -1\n", 0, "is not a counter", "-1"},
		{"a name without a value", "crawler_ingest_bad_hashes_total\n", 0, "is missing", ""},
		{"a comment in the second field", "crawler_ingest_bad_hashes_total #42\n", 0, "is not a counter", "#42"},
		{"no metric", "crawler_service_up 1\n", 0, "is missing", ""},
		{"a non-numeric value", "crawler_ingest_bad_hashes_total abc\n", 0, "is not a counter", "abc"},
		{"a negative value", "crawler_ingest_bad_hashes_total -3\n", 0, "is not a counter", "-3"},
		{"a foreign name prefix", "crawler_ingest_bad_hashes_total_extra 9\n", 0, "is missing", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseBadHashesExposition(strings.NewReader(tt.body))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want it to contain %q", err, tt.wantErr)
				}
				if got != 0 {
					t.Fatalf("the value on error = %d, want 0", got)
				}
				if tt.wantHas != "" && !strings.Contains(err.Error(), tt.wantHas) {
					t.Errorf("the error text %q does not name the offending value %q", err.Error(), tt.wantHas)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseBadHashesExposition: %v", err)
			}
			if got != tt.want {
				t.Errorf("the value = %d, want %d", got, tt.want)
			}
		})
	}
}

// failReader — a reader with an unrecoverable error (a transport break during
// the /metrics body read).
type failReader struct{ err error }

func (r *failReader) Read([]byte) (int, error) { return 0, r.err }

// TestParseBadHashesExpositionReadError — a body read error bubbles out of the
// Scanner: "exposition read" with the original reason and a zero value.
func TestParseBadHashesExpositionReadError(t *testing.T) {
	got, err := parseBadHashesExposition(&failReader{err: errors.New("connection reset")})
	if err == nil ||
		!strings.Contains(err.Error(), "exposition read") ||
		!strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("error = %v, want \"exposition read\" with the reason", err)
	}
	if got != 0 {
		t.Errorf("the value = %d, want 0 on error", got)
	}
}

// TestParseBadHashesExpositionBodyLimit — the body read cap: a metric
// on the last line exactly at the badHashesBodyLimit boundary is read (the limit
// is inclusive); beyond it — cut off ("the metric is missing").
// The limit itself is pinned by the 4<<20 literal: a constant in the check would
// mutate together with the mutant and "follow" it (T-M01, alerts).
func TestParseBadHashesExpositionBodyLimit(t *testing.T) {
	if badHashesBodyLimit != 4<<20 {
		t.Fatalf("badHashesBodyLimit = %d, want 4<<20 (4 MiB)", badHashesBodyLimit)
	}
	const filler = "crawler_service_up{service=\"ingest\"} 1\n"
	const metric = "crawler_ingest_bad_hashes_total{service=\"ingest\"} 42\n"

	// Exactly at the limit: filler lines, a pad of the exact length and the
	// metric on the last line — the total size of badHashesBodyLimit.
	var b strings.Builder
	for b.Len()+len(filler)+len(metric) <= badHashesBodyLimit {
		b.WriteString(filler)
	}
	rest := badHashesBodyLimit - b.Len() - len(metric)
	switch {
	case rest == 1:
		b.WriteString("\n") // an empty line — also past the parser
	case rest >= 2:
		b.WriteString("#" + strings.Repeat("x", rest-2) + "\n")
	}
	b.WriteString(metric)
	if b.Len() != badHashesBodyLimit {
		t.Fatalf("the body at the boundary = %d bytes, want %d", b.Len(), badHashesBodyLimit)
	}
	got, err := parseBadHashesExposition(strings.NewReader(b.String()))
	if err != nil || got != 42 {
		t.Fatalf("at the limit boundary: (%d, %v), want (42, nil)", got, err)
	}

	// Beyond the limit: the metric in the tail beyond badHashesBodyLimit — not read.
	var over strings.Builder
	for over.Len()+len(filler) <= badHashesBodyLimit {
		over.WriteString(filler)
	}
	over.WriteString(filler) // the tail beyond the limit
	over.WriteString(metric)
	if _, err := parseBadHashesExposition(strings.NewReader(over.String())); err == nil ||
		!strings.Contains(err.Error(), "is missing") {
		t.Fatalf("beyond the limit: %v, want \"the metric is missing\"", err)
	}
}

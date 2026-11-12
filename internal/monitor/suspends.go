// A source of render engine selection observability alerts (T-246, the
// render spec §4.6 item 3): the HTTP /metrics exposition of the
// render-worker is a registry shared per service (spec 5.9, metrics.New),
// the monitor cannot have its own copy of the counters (different
// processes). The package's second cross-service probe, modeled on
// IngestBadHashes (T-099): the same addresses Prometheus scrapes, with
// no intermediate DB — the counters live with the data owner. Two
// branches are read: the share-budget suspension events of
// crawler_render_fallback_suspended_total{reason="budget"} (T-239,
// FR-9b — the degradation of the primary engine by publication share)
// and the browser-class errors of crawler_stage_errors_total
// {stage="render",type="browser"} (S-P2-6, an operator decision — a rate
// detector: sustained growth — the engine is hung/unavailable). The
// health gate events (reason="gate") are not counted by the alert: gate
// latching is a counter and a Warn log of the worker, the detector looks
// at the root cause — a streak of browser-class infra errors.
//
// Degradation: the render-worker is stopped or the exposition is foreign
// (no render-stage marker crawler_stage_tasks_total{stage="render"}) — a
// source error, the alert is "not measured" (Warn), not a false firing;
// the death of the worker itself is visible via the framework's
// crawler_service_up. The suspended{reason="budget"} series is absent
// from the render-worker exposition — 0, not an error (S-P1-4, the
// T-250 decision — pre-creation): the series is pre-created at zero
// since T-250; for older builds a lazy family until the first event
// means the same zero — the alert is neither muted as "not measured" nor
// loses the first 0→1 event. An empty monitor.render_metrics_url (not
// configured) — the source is not connected at all, the alert is
// "not measured" quietly (the ingest probe style).
package monitor

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// suspendedMetricName is the name of the suspension events metric in
	// the render-worker exposition (T-239).
	suspendedMetricName = "crawler_render_fallback_suspended_total"

	// suspendedBudgetPair is the exact label-value pair of the share
	// budget branch (the reason vocabulary is closed: gate|budget).
	suspendedBudgetPair = `reason="budget"`

	// renderStageTasksMetric / renderStagePair is the render-worker
	// marker: the crawler_stage_tasks_total{stage="render"} series is
	// pre-created in the worker's NewMetrics and distinguishes its
	// exposition from a foreign one (the http-worker exposes
	// stage="http").
	renderStageTasksMetric = "crawler_stage_tasks_total"
	renderStagePair        = `stage="render"`

	// browserErrorsMetric / browserTypePair is the stage error counter by
	// render.Class classes (internal/renderworker/metrics.go); the
	// stage="render",type="browser" branch — engine infra errors (T-063):
	// the family is lazy, a worker without browser errors has no series.
	browserErrorsMetric = "crawler_stage_errors_total"
	browserTypePair     = `type="browser"`

	// suspendedBodyLimit is the cap on reading the /metrics body: the
	// worker registry carries the Go/runtime and process collectors
	// (tens of KiB), the cap is protection against an inadequate
	// response, not a size estimate (the badHashesBodyLimit style).
	suspendedBodyLimit = 4 << 20
)

// RenderSuspends is a FallbackBudgetSource on top of GET /metrics of the
// render-worker: the address is monitor.render_metrics_url (dev:
// http://render-worker:9105/metrics, production: the render-worker
// loopback per METRICS_ADDR).
type RenderSuspends struct {
	// URL is the address of the render-worker /metrics exposition.
	URL string
	// Client is the HTTP client; nil — a local one with the timeout below.
	Client *http.Client
}

// compile-time: RenderSuspends is both a FallbackBudgetSource and a
// RenderBrowserErrorsSource.
var (
	_ FallbackBudgetSource      = RenderSuspends{}
	_ RenderBrowserErrorsSource = RenderSuspends{}
)

// rendersProbeTimeout is the budget of a single read (parity with
// ingestProbeTimeout: the alert loop lives on its own period, the probe
// must not delay it with a network outage). A configuration budget, not
// logic — the hermeticity of unit tests (docs/mutation-testing.md §4).
// mutator-disable-next-line arithmetic/base, numbers/decrementer, numbers/incrementer
const rendersProbeTimeout = 5 * time.Second

// fetch — GET of the render-worker /metrics exposition: the body on a
// 200, the shared transport part of the budget and browser-error probes.
func (s RenderSuspends) fetch(ctx context.Context) (io.ReadCloser, error) {
	cl := s.Client
	if cl == nil {
		cl = &http.Client{Timeout: rendersProbeTimeout}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("monitor: probe %s: %w", s.URL, err)
	}
	resp, err := cl.Do(req)
	if err != nil {
		return nil, fmt.Errorf("monitor: probe %s: %w", s.URL, err)
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("monitor: probe %s: status %s", s.URL, resp.Status)
	}
	return resp.Body, nil
}

// FallbackBudget reads the current value of the share budget events
// counter from the render-worker exposition.
func (s RenderSuspends) FallbackBudget(ctx context.Context) (int64, error) {
	body, err := s.fetch(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = body.Close() }()
	return parseSuspendExposition(body)
}

// RenderBrowserErrors reads the current value of the browser-class error
// counter of the render stage from the same exposition (S-P2-6): the
// same monitor.render_metrics_url address, a separate request — the
// probes are independent, the alert loop period absorbs the cost.
func (s RenderSuspends) RenderBrowserErrors(ctx context.Context) (int64, error) {
	body, err := s.fetch(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = body.Close() }()
	return parseBrowserErrorsExposition(body)
}

// parseSuspendExposition extracts the value of the reason="budget"
// branch counter from the textual Prometheus exposition: the lines
// "name{labels} value", '#'-lines (HELP/TYPE) are skipped. Extracted as
// a pure function for a table test without HTTP (the
// parseBadHashesExposition style).
//
// No series — 0, not an error, when the exposition belongs to the
// render-worker (the marker crawler_stage_tasks_total{stage="render"},
// pre-created in NewMetrics): the suspended{reason="budget"} series is
// pre-created at zero since T-250 (S-P1-4); for older builds the family
// is lazy — until the first event it is the same honest zero, the alert
// is not switched to "not measured" and does not lose the first 0→1
// event. Only a foreign exposition remains an error (no marker — the
// address is not pointed at the render-worker).
func parseSuspendExposition(r io.Reader) (int64, error) {
	body, err := readExposition(r)
	if err != nil {
		return 0, err
	}
	v, found, err := findPromSeries(body, suspendedMetricName, suspendedBudgetPair)
	if err != nil {
		return 0, err
	}
	if found {
		return int64(v), nil
	}
	if renderExposition(body) {
		return 0, nil // render-worker, no series yet — 0 (S-P1-4)
	}
	return 0, fmt.Errorf("monitor: the %s{%s} metric is missing from the render-worker exposition",
		suspendedMetricName, suspendedBudgetPair)
}

// readExposition — the whole exposition body into memory (the
// suspendedBodyLimit cap): the repeated line search needs a buffer,
// an io.Reader is single-pass.
func readExposition(r io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, suspendedBodyLimit))
	if err != nil {
		return nil, fmt.Errorf("monitor: exposition read: %w", err)
	}
	return body, nil
}

// findPromSeries — the value of the first sample of metric that contains
// all the pairs label pairs (a subset: extra sample labels do not get in
// the way — "the family exists, the needed branch does not" is
// distinguished by the filter that follows the name).
// '#'-lines and empty ones are skipped; a non-number/negative value is
// a "not a counter" error (the parseBadHashesExposition style).
func findPromSeries(body []byte, metric string, pairs ...string) (float64, bool, error) {
	sc := bufio.NewScanner(bytes.NewReader(body))
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		// Skipping '#'-lines and empty ones is equivalent to comparing the
		// name: Cut on a field with '#' yields a name with '#', unequal to
		// the metric (the parseBadHashesExposition style).
		// mutator-disable-next-line expression/remove
		if len(fields) < 2 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		name, labels, _ := strings.Cut(fields[0], "{")
		if name != metric {
			continue
		}
		// Labels up to '}': pairs are searched by exact comparison of a
		// list element (the vocabularies are closed, there are no values
		// with commas/quotes in them).
		labelPart, _, _ := strings.Cut(labels, "}")
		if !promHasPairs(labelPart, pairs...) {
			continue
		}
		v, err := strconv.ParseFloat(fields[1], 64)
		if err != nil || v < 0 {
			return 0, false, fmt.Errorf("monitor: %s{%s}: the value %q is not a counter",
				metric, labelPart, fields[1])
		}
		return v, true, nil
	}
	if err := sc.Err(); err != nil {
		return 0, false, fmt.Errorf("monitor: exposition read: %w", err)
	}
	return 0, false, nil
}

// promHasPairs — whether all the name="value" label pairs are present in
// the comma-separated list.
func promHasPairs(labelPart string, pairs ...string) bool {
	for _, want := range pairs {
		found := false
		for _, pair := range strings.Split(labelPart, ",") {
			if pair == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// renderExposition — the render-worker marker: the
// crawler_stage_tasks_total{stage="render"} series is pre-created in the
// worker's NewMetrics (T-077) and lives in the exposition from the first
// start — other services expose their own stage (the http-worker —
// stage="http") or do not expose the family at all. No marker = a
// foreign exposition.
func renderExposition(body []byte) bool {
	_, found, _ := findPromSeries(body, renderStageTasksMetric, renderStagePair)
	return found
}

// errNotRenderExposition — the exposition is not the render-worker's
// (no crawler_stage_tasks_total{stage="render"} marker): the
// monitor.render_metrics_url address is pointed at a foreign service.
// A quiet "not measured" without Warn (S-P2-6): the probe is alive at
// that moment (200 OK) — this is a configuration problem, not a source
// failure; the reason is visible in the alert's detail.
var errNotRenderExposition = errors.New("monitor: the exposition is not a render worker")

// parseBrowserErrorsExposition — the value of the
// crawler_stage_errors_total{stage="render",type="browser"} counter: the
// streak of engine infra errors, the root cause of the health gate
// latching (T-239). The family is lazy: a render-worker without browser
// errors has no series — 0 with the render-stage marker (like suspended
// after the T-250 pre-creation, S-P1-4); a foreign exposition (no
// marker) — errNotRenderExposition.
func parseBrowserErrorsExposition(r io.Reader) (int64, error) {
	body, err := readExposition(r)
	if err != nil {
		return 0, err
	}
	v, found, err := findPromSeries(body, browserErrorsMetric, renderStagePair, browserTypePair)
	if err != nil {
		return 0, err
	}
	if found {
		return int64(v), nil
	}
	if renderExposition(body) {
		return 0, nil // render-worker, no browser errors yet — 0
	}
	return 0, errNotRenderExposition
}

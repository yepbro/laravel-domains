// The render engines panel and fallback (render spec §4.6 item 4,
// T-246; the S-P3-2 completeness): render rate, success and tarpit
// rate per engines (the engine labels of stage counters,
// T-235/S-P2-4), the RSS of the engine's workers, the fallback rate —
// published/stage pages — and consumption (consumed/the
// q:render-fallback depth).
//
// The sources are HTTP /metrics exposures of render workers
// (dashboard.render_metrics_urls: the main build — the stage engine,
// the worker B build — the fallback consumption): the same addresses
// Prometheus scrapes (the precedent — the monitor ingest probe,
// T-099), without an intermediate DB — the counters live with the
// data owner, and the per-engine attribution exists only there (the
// DB schema does not know engines, render spec §4.8). The
// q:render-fallback depth — the same common depth computation as the
// queues section (c.lens, GroupDepth via the gate), not a build-B
// metric: with B stopped the series is absent, and the queue
// remainder is exactly what the operator needs to see.
//
// The rate (renders/s, publications/s) — the delta of cumulatives
// between collector passes in the Collector's memory: the window =
// the refresh period (5 s at the default), the front smooths with the
// polling history. A worker restart (the cumulative dropped) yields a
// zero delta — the series are not broken, no spike.
//
// Degradation — like the snapshot sections (§5.9): the address list
// is empty — Render = nil ("n/a", not an error — Health style without
// probes); an error of any worker degrades the whole section (the
// reason is in errors): a half-empty panel would be misleading (an
// engine "missing" from the aggregate is indistinguishable from a
// zero flow).
package dashboard

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"crawler/internal/queue"
)

// The names of the stage render instruments in the exposure
// (internal/renderworker/metrics.go): a fixed slice — a full-fledged
// exposure parser (expfmt) does not pay for the dependency (the
// precedent — monitor/badhashes.go).
const (
	renderRecordsMetric   = "crawler_render_records_total"            // {rendered,engine} (T-235)
	renderFinalizedMetric = "crawler_render_task_finalized_total"     // {reason,engine} (T-172/T-235)
	renderTarpitMetric    = "crawler_render_tarpit_finalized_total"   // {engine} (T-148/S-P2-4)
	fbPublishedMetric     = "crawler_render_fallback_published_total" // {reason} (T-238)
	fbConsumedMetric      = "crawler_render_fallback_consumed_total"  // {result} (T-245)
	stageTasksMetric      = "crawler_stage_tasks_total"               // {stage} — the fallback rate denominator (S-P3-2)
	processRSSMetric      = "process_resident_memory_bytes"           // the standard process collector — the worker RSS (S-P3-2)
)

// renderBodyLimit — the ceiling for reading the /metrics body: the
// worker registry carries Go/runtime and process collectors (tens of
// KiB), the ceiling — protection against an inadequate response, not
// a size estimate (monitor.badHashesBodyLimit style).
const renderBodyLimit = 4 << 20

// promSample — an exposition text format sample: the name, labels,
// value.
type promSample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// parsePromExposition — a minimal exposition text format parse
// (samples with and without labels; HELP/TYPE skipped). An own parser
// instead of expfmt: the prometheus/common dependency in the module
// graph is indirect, the panel needs only the fixed instrument slice
// (the approach — fixturesParseProm Metrics of the T-244 engine
// comparison tool, own copy: the main package is not imported).
func parsePromExposition(r io.Reader) ([]promSample, error) {
	var out []promSample
	sc := bufio.NewScanner(io.LimitReader(r, renderBodyLimit))
	for n := 0; sc.Scan(); n++ {
		line := strings.TrimRight(sc.Text(), "\r")
		if line == "" || line[0] == '#' {
			continue
		}
		name, labels := "", ""
		rest := line
		if i := strings.IndexByte(line, '{'); i >= 0 {
			end := promLabelsEnd(line, i)
			if end < 0 {
				return nil, fmt.Errorf("metrics: line %d: labels without a closing brace", n+1)
			}
			name = line[:i]
			labels = line[i+1 : end]
			rest = line[end+1:]
		} else {
			fields := strings.Fields(line)
			if len(fields) == 0 {
				continue
			}
			name = fields[0]
			rest = strings.TrimPrefix(strings.TrimSpace(line), name)
		}
		// The value — the first token after the name/labels; the
		// timestamp and OpenMetrics exemplar tail are not our case and
		// are cut off.
		if i := strings.Index(rest, " #"); i >= 0 {
			rest = rest[:i]
		}
		valueFields := strings.Fields(rest)
		if len(valueFields) == 0 {
			return nil, fmt.Errorf("metrics: line %d: no value", n+1)
		}
		v, err := strconv.ParseFloat(valueFields[0], 64)
		if err != nil {
			return nil, fmt.Errorf("metrics: line %d: the value %q: %w", n+1, valueFields[0], err)
		}
		parsed, err := parsePromLabels(labels)
		if err != nil {
			return nil, fmt.Errorf("metrics: line %d: %w", n+1, err)
		}
		out = append(out, promSample{Name: name, Labels: parsed, Value: v})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("reading the exposition: %w", err)
	}
	return out, nil
}

// promLabelsEnd — the index of the '}' closing the label list (start —
// the index of the opening '{'); '}' and '{' inside value quotes are
// part of the value.
func promLabelsEnd(s string, start int) int {
	inQuote := false
	for i := start + 1; i < len(s); i++ {
		switch {
		case s[i] == '\\' && inQuote && i+1 < len(s):
			i++
		case s[i] == '"':
			inQuote = !inQuote
		case s[i] == '}' && !inQuote:
			return i
		}
	}
	return -1
}

// parsePromLabels — a list of name="value" labels separated by commas
// (an empty string — no labels). The separator is a comma outside
// quotes: a comma inside a value is legal (text format).
func parsePromLabels(s string) (map[string]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	labels := map[string]string{}
	var b strings.Builder
	inQuote := false
	flush := func() error { return promLabelPair(b.String(), labels) }
	for i := 0; i < len(s); i++ {
		switch {
		case s[i] == '\\' && inQuote && i+1 < len(s):
			b.WriteByte(s[i])
			b.WriteByte(s[i+1])
			i++
		case s[i] == '"':
			inQuote = !inQuote
			b.WriteByte(s[i])
		case s[i] == ',' && !inQuote:
			if err := flush(); err != nil {
				return nil, err
			}
			b.Reset()
		default:
			b.WriteByte(s[i])
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return labels, nil
}

// promLabelPair — one name="value" pair into the label map.
func promLabelPair(pair string, labels map[string]string) error {
	pair = strings.TrimSpace(pair)
	if pair == "" {
		return nil
	}
	k, v, found := strings.Cut(pair, "=")
	if !found || len(v) < 2 || v[0] != '"' || v[len(v)-1] != '"' {
		return fmt.Errorf("the label %q without a quoted value", pair)
	}
	labels[strings.TrimSpace(k)] = promUnescape(v[1 : len(v)-1])
	return nil
}

// promUnescape — the text format label value escapes: \\, \" and \n;
// an unknown escape is kept literally (a backslash + the character).
func promUnescape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i+1 >= len(s) {
			b.WriteByte(s[i])
			continue
		}
		switch s[i+1] {
		case '\\', '"', 'n':
			b.WriteByte(s[i+1])
			i++
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

// renderCounters — the panel's aggregated cumulatives across all
// workers: summing per engines/outcomes (several builds of one engine
// — one browser stack of the class, the panel shows the engine, not
// the worker).
type renderCounters struct {
	rendered  map[string]float64 // engine → rendered="true" (T-235)
	finalized map[string]float64 // engine → Σ task_finalized over reasons
	tarpits   map[string]float64 // engine → tarpit_finalized (T-148/S-P2-4)
	published float64            // Σ fallback_published over reasons (T-238)
	consumed  map[string]float64 // result → fallback_consumed (T-245)
	tasks     float64            // Σ stage_tasks{stage="render"} — stage pages (S-P3-2)
	rss       map[string]float64 // engine → Σ RSS of the engine's workers (S-P3-2)
}

// collectRenderCounters — a scrape of all exposures in parallel (the
// probeAll style: the probeTimeout client timeout, not the sum of N
// requests) and a reduction of the samples into an aggregate.
func (c *Collector) collectRenderCounters(ctx context.Context) (renderCounters, error) {
	urls := c.opt.RenderMetrics
	if len(urls) == 0 {
		return renderCounters{}, nil
	}
	bodies := make([][]byte, len(urls))
	errs := make([]error, len(urls))
	var wg sync.WaitGroup
	for i, raw := range urls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bodies[i], errs[i] = c.fetchRenderMetrics(ctx, raw)
		}()
	}
	wg.Wait()
	var failed []error
	for _, err := range errs {
		if err != nil {
			failed = append(failed, err)
		}
	}
	if len(failed) > 0 {
		return renderCounters{}, errors.Join(failed...)
	}
	agg := renderCounters{
		rendered:  map[string]float64{},
		finalized: map[string]float64{},
		tarpits:   map[string]float64{},
		consumed:  map[string]float64{},
		rss:       map[string]float64{},
	}
	for _, body := range bodies {
		samples, err := parsePromExposition(bytes.NewReader(body))
		if err != nil {
			return renderCounters{}, err
		}
		agg.add(samples)
	}
	return agg, nil
}

// fetchRenderMetrics — a GET of one exposure (the probe client: the
// probeTimeout timeout, no redirects — outgoing connections to the
// configured addresses only, dashboard spec §9.3).
func (c *Collector) fetchRenderMetrics(ctx context.Context, raw string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return nil, fmt.Errorf("exposition %s: %w", raw, err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("exposition %s: %w", raw, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("exposition %s: status %s", raw, resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, renderBodyLimit))
	if err != nil {
		return nil, fmt.Errorf("exposition %s: read: %w", raw, err)
	}
	return body, nil
}

// add — a reduction of one exposure's samples into the aggregate:
// familiar instruments by name, branches — by labels
// (rendered="true"; engine for the render share, tarpits and the task
// class finalizations; reason of publications is not distinguished —
// the public needs the rate; result for consumption; stage="render"
// for stage pages). The exposure RSS
// (process_resident_memory_bytes, unlabeled) is attributed to the
// build engine of the worker — the engine label of the pre-created
// rendered="true" series (NewMetrics, T-235: the series is in the
// exposure from the first start); a build without the label (older
// than T-235) — the RSS is not attributed, skipped.
func (a *renderCounters) add(samples []promSample) {
	engine := ""
	for _, s := range samples {
		if s.Name == renderRecordsMetric && s.Labels["rendered"] == "true" {
			if e := s.Labels["engine"]; e != "" {
				engine = e
				break
			}
		}
	}
	for _, s := range samples {
		switch s.Name {
		case renderRecordsMetric:
			if s.Labels["rendered"] == "true" {
				if e := s.Labels["engine"]; e != "" {
					a.rendered[e] += s.Value
				}
			}
		case renderFinalizedMetric:
			if e := s.Labels["engine"]; e != "" {
				a.finalized[e] += s.Value
			}
		case renderTarpitMetric:
			if e := s.Labels["engine"]; e != "" {
				a.tarpits[e] += s.Value
			}
		case fbPublishedMetric:
			a.published += s.Value
		case fbConsumedMetric:
			if r := s.Labels["result"]; r != "" {
				a.consumed[r] += s.Value
			}
		case stageTasksMetric:
			if s.Labels["stage"] == "render" {
				a.tasks += s.Value
			}
		case processRSSMetric:
			if engine != "" {
				a.rss[engine] += s.Value
			}
		}
	}
}

// renderRateState — the panel rate memory between passes: the previous
// aggregate and moment. Lives in the Collector (not in the snapshot),
// the first pass — the baseline (rates 0), a worker restart (the
// cumulative dropped) — a zero delta.
type renderRateState struct {
	seen      bool
	at        time.Time
	rendered  map[string]float64
	published float64
	consumed  float64
}

// rateOf — the cumulative delta over Δt; prev < cur — growth (divided
// by the window), otherwise 0 (no baseline / restart / stagnation).
func rateOf(prev, cur float64, dt time.Duration) float64 {
	if dt <= 0 || cur <= prev {
		return 0
	}
	return (cur - prev) / dt.Seconds()
}

// observe — the pass rates: the aggregate deltas to the previous pass
// and a baseline update. An engine missing from the baseline has no
// rate (zero) but is based on the current value.
func (s *renderRateState) observe(cur renderCounters, now time.Time) renderRates {
	dt := time.Duration(0)
	if s.seen {
		dt = now.Sub(s.at)
	}
	if dt < 0 {
		dt = 0
	}
	out := renderRates{
		rendered: make(map[string]float64, len(cur.rendered)),
	}
	for e, v := range cur.rendered {
		out.rendered[e] = rateOf(s.rendered[e], v, dt)
	}
	out.published = rateOf(s.published, cur.published, dt)
	var consumed float64
	for _, v := range cur.consumed {
		consumed += v
	}
	out.consumed = rateOf(s.consumed, consumed, dt)

	s.seen = true
	s.at = now
	s.rendered = make(map[string]float64, len(cur.rendered))
	for e, v := range cur.rendered {
		s.rendered[e] = v
	}
	s.published = cur.published
	s.consumed = consumed
	return out
}

// renderRates — the rates of one pass (renders/s per engines,
// publications/s, consumption/s).
type renderRates struct {
	rendered  map[string]float64
	published float64
	consumed  float64
}

// collectRender — the engines panel section: the exposure aggregate +
// rates + the q:render-fallback depth (the same lens as the queues
// section; a depth error does not take down the section — Depth = nil
// "n/a", the reason is visible in the errors of the queues section).
// A nil return without an error — addresses not set ("n/a").
func (c *Collector) collectRender(ctx context.Context) (*RenderJSON, error) {
	if len(c.opt.RenderMetrics) == 0 {
		return nil, nil
	}
	agg, err := c.collectRenderCounters(ctx)
	if err != nil {
		return nil, err
	}
	rates := c.rates.observe(agg, c.opt.Now())

	engines := make([]string, 0, len(agg.rendered)+len(agg.finalized)+len(agg.tarpits)+len(agg.rss))
	seenEngine := map[string]bool{}
	for _, m := range []map[string]float64{agg.rendered, agg.finalized, agg.tarpits, agg.rss} {
		for e := range m {
			if !seenEngine[e] {
				seenEngine[e] = true
				engines = append(engines, e)
			}
		}
	}
	sort.Strings(engines)

	out := &RenderJSON{
		Engines: make([]RenderEngineJSON, 0, len(engines)),
		Fallback: RenderFallbackJSON{
			PublishedTotal:   int64(agg.published),
			PublishedPerSec:  rates.published,
			ConsumedPerSec:   rates.consumed,
			ConsumedByResult: make(map[string]int64, len(agg.consumed)),
		},
	}
	for _, e := range engines {
		row := RenderEngineJSON{
			Engine:         e,
			RenderedTotal:  int64(agg.rendered[e]),
			FinalizedTotal: int64(agg.finalized[e]),
			TarpitTotal:    int64(agg.tarpits[e]),
			RSSBytes:       int64(agg.rss[e]),
			RenderedPerSec: rates.rendered[e],
		}
		// Success — the same denominator (rendered+finalized, the
		// operator decision on S-P2-4): the tarpit is a separate
		// column.
		if denom := agg.rendered[e] + agg.finalized[e]; denom > 0 {
			pct := 100 * agg.rendered[e] / denom
			row.SuccessPct = &pct
		}
		// The tarpit rate — the share of tarpit finalizations among all
		// finalizations of the engine with the engine label
		// (cumulatives, like success: a fresh worker without
		// finalizations — nil "—").
		if denom := agg.rendered[e] + agg.finalized[e] + agg.tarpits[e]; denom > 0 {
			pct := 100 * agg.tarpits[e] / denom
			row.TarpitPct = &pct
		}
		out.Engines = append(out.Engines, row)
	}
	for r, v := range agg.consumed {
		out.Fallback.ConsumedByResult[r] = int64(v)
		out.Fallback.ConsumedTotal += int64(v)
	}
	// The fallback rate — the share of publications of stage pages:
	// the numerator — worker A published, the denominator — Σ
	// stage_tasks{stage="render"} of all exposures (workers A+B, the
	// same units as the T-239 budget window); nil — the stage has not
	// processed a single page yet.
	if agg.tasks > 0 {
		pct := 100 * agg.published / agg.tasks
		out.Fallback.RatePct = &pct
	}

	// The q:render-fallback depth — the same lens and budget as the
	// queues section: an error — Depth nil ("n/a"), the section alive.
	if c.lens != nil {
		qctx, cancel := context.WithTimeout(ctx, queueFetchTimeout)
		depth, derr := c.lens.Len(qctx, queue.StreamRenderFallback)
		cancel()
		if derr == nil {
			out.Fallback.Depth = &depth
		}
	}
	return out, nil
}

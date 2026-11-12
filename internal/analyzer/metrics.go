// Instruments of the analyze stage (T-077, spec 5.9): throughput is a
// task counter and a duration histogram. There are no separate
// error/status counters: the analysis is passive (no outbound requests,
// spec 5.8), a delivery failure is a queue-level retry, and its results
// (findings, is_laravel) are DB data, not process metrics. The exception
// is a find removal by re-analysis and the point content drain (T-187):
// the event changes the lifetime retention state (disk), observability is mandatory.
package analyzer

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// analyzeSecondsBuckets is the analyze task duration buckets: RE2 matching
// over a bounded input takes microseconds (T-069), a couple of DB
// operators take milliseconds; the buckets also cover anomalies (content
// at the limit bounds, a slow DB).
var analyzeSecondsBuckets = []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5}

// Metrics holds the analyze stage instruments; NewMetrics builds it from
// the service registry; nil — all methods are no-ops (isolated builds).
// Thread-safe: deliveries run in parallel, the prometheus instruments
// are synchronized.
type Metrics struct {
	tasks   prometheus.Counter  // crawler_stage_tasks_total{stage="analyze"}
	seconds prometheus.Observer // crawler_stage_seconds{stage="analyze"}
	// Find removals by re-analysis and point content drains (T-187):
	// the events are rare (rule change waves); observability covers "the
	// mechanism works" and "how much content returned for cleanup".
	unfound       prometheus.Counter // crawler_analyzer_laravel_unfound_total
	unfoundRows   prometheus.Counter // crawler_analyzer_unfound_drained_rows_total
	unfoundErrors prometheus.Counter // crawler_analyzer_unfound_drain_errors_total
}

// NewMetrics registers the stage instruments in the reg registry
// (curried by service and stage); nil — nil. Idempotent (T-076).
func NewMetrics(reg *metrics.Registry) *Metrics {
	if reg == nil {
		return nil
	}
	return &Metrics{
		tasks: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_stage_tasks_total",
			Help: "Pipeline stage tasks processed: dns — domains, http — domains, render/analyze — subdomain×variant rows.",
		}, metrics.LabelStage).WithLabelValues(metrics.StageAnalyze),
		seconds: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_stage_seconds",
			Help:    "Duration of processing one stage task (a domain/row episode).",
			Buckets: analyzeSecondsBuckets,
		}, metrics.LabelStage).WithLabelValues(metrics.StageAnalyze),
		unfound: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_analyzer_laravel_unfound_total",
			Help: "Laravel finds removed by re-analysis (a delivery without fresh, T-187): the domain left laravel_domains without a fresh HTTP episode; each is answered by the domain content point drain (content.DrainDomain).",
		}).WithLabelValues(),
		unfoundRows: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_analyzer_unfound_drained_rows_total",
			Help: "subdomain_results rows cleaned by the point drain on a find removal by re-analysis (T-187): without the drain they would stay behind the retention cursor until rare catch-up circles.",
		}).WithLabelValues(),
		unfoundErrors: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_analyzer_unfound_drain_errors_total",
			Help: "Domain content point drain failures (T-187): the rows stayed behind the retention cursor (rare T-185 catch-up circles or the T-030 recheck will pick them up); growth = investigate the analyzer Warn in the log.",
		}).WithLabelValues(),
	}
}

// Task — a q:analyze task was processed successfully (including an
// orderly refusal — the row is gone or slipped out from under the task):
// +1 stage task, the duration into the histogram.
func (m *Metrics) Task(d time.Duration) {
	if m == nil {
		return
	}
	m.tasks.Inc()
	m.seconds.Observe(d.Seconds())
}

// Unfound — a find removal by re-analysis (T-187): a transition event.
func (m *Metrics) Unfound() {
	if m == nil {
		return
	}
	m.unfound.Inc()
}

// UnfoundDrain — rows cleaned by the point drain (T-187).
func (m *Metrics) UnfoundDrain(rows int64) {
	if m == nil || rows <= 0 {
		return
	}
	m.unfoundRows.Add(float64(rows))
}

// UnfoundDrainError — the point drain did not happen (T-187).
func (m *Metrics) UnfoundDrainError() {
	if m == nil {
		return
	}
	m.unfoundErrors.Inc()
}

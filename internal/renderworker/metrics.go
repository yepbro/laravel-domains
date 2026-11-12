// Render stage metrics (T-077, spec 5.9): throughput (the task counter
// and the duration histogram), errors by render.Class classes
// (task/context/browser — the stage's reaction to a failure, T-061/T-063: a retry
// of the page, of the context, or a browser restart), the render share — the
// rendered=true branch of the crawler_render_records_total counter (the false branch
// is incremented by the HTTP stage after writing the result; the share — by the rate ratio
// in PromQL), tarpit finalizations without a render (T-148), finalizations on a
// permanent host certificate error (T-159), finalizations on a host DNS
// refusal (T-211), finalizations of a task-class error on the last attempt
// with a reason label (T-172), publications to the fallback queue with a
// reason label (T-238, spec §4.6 item 2 — the numerator of the fallback rate, FR-15),
// fallback publication suspension events with a gate|budget reason label
// (T-239, FR-9, spec §4.6 item 2), fallback consumption by worker B with an
// outcome label rendered|finalized|skipped (T-245, spec §4.6 item 2, FR-11/FR-15 —
// the balance published = consumed + the q:render-fallback remainder, step 4
// of the spec acceptance §6.2); the engine label on the render share,
// tarpit finalizations (S-P2-4: a tarpit is an engine-branched failure) and
// task class finalizations (T-235, spec §4.6 item 1, FR-15:
// cardinality 3 — chromium/moli/lightpanda, the value — the engine
// of the worker's build, Options.Engine ← render.engine; existing
// aggregations without engine do not change); pre-creation of zero series of
// the closed vocabularies suspended{gate,budget} and
// consumed{rendered,finalized,skipped} (S-P1-4): the families are visible in
// the exposition from the first start, the counter's first event is not lost.
package renderworker

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/config"
	"crawler/internal/metrics"
	"crawler/internal/render"
)

// The package-level constants below — the vocabulary of suspension reasons.
const (
	// fbReasonGate — the health gate (FR-9a): the streak of infra errors of the
	// browser class reached render.fallback.health_gate_streak — the main
	// engine is unhealthy, the fallback publication is suspended until
	// recovery (T-239).
	fbReasonGate = "gate"
	// fbReasonBudget — the share budget (FR-9b): the share of publications over the window
	// went beyond the render.fallback.share_percent cap — the next window
	// without publications (T-239).
	fbReasonBudget = "budget"
)

// The vocabulary of fallback consumption outcomes (T-245, FR-15): the result label
// of the crawler_render_fallback_consumed_total counter — what the
// q:render-fallback delivery ended with at worker B.
const (
	// fbResultRendered — the rework succeeded (spec §4.5 item 2): the DOM is written,
	// rendered = true, a q:analyze publication.
	fbResultRendered = "rendered"
	// fbResultFinalized — the fallback is exhausted (FR-10): the entry is finalized
	// with the render_failed_at marker, a q:analyze publication (the tarpit paths
	// T-148 / task class T-172 / cert T-159 / DNS T-211 of worker B).
	fbResultFinalized = "finalized"
	// fbResultSkipped — a regular rejection without side effects: there is no row
	// or no final URL (a recheck rewrote the episode) — the delivery
	// is acknowledged without a render and a marker.
	fbResultSkipped = "skipped"
)

// renderSecondsBuckets — the render task duration buckets: a page
// renders in an isolated context with a readiness wait of up to
// render.timeout (25 s by default, spec 5.6/5.10), fast static
// pages — a few seconds.
var renderSecondsBuckets = []float64{0.5, 1, 2.5, 5, 10, 15, 20, 25, 30}

// Metrics — the render stage metrics; built by NewMetrics from the registry
// of the service, nil — all methods are no-ops (isolated builds).
// Thread-safe: deliveries go in parallel, the prometheus metrics
// are synchronized.
type Metrics struct {
	tasks    prometheus.Counter     // crawler_stage_tasks_total{stage="render"}
	seconds  prometheus.Observer    // crawler_stage_seconds{stage="render"}
	errors   *prometheus.CounterVec // crawler_stage_errors_total{stage,type}
	rendered prometheus.Counter     // crawler_render_records_total{rendered="true",engine} (T-235)
	tarpits  *prometheus.CounterVec // crawler_render_tarpit_finalized_total{engine} (T-148/S-P2-4)
	certs    prometheus.Counter     // crawler_render_cert_finalized_total (T-159)
	dns      prometheus.Counter     // crawler_render_dns_finalized_total (T-211)
	tasksFin *prometheus.CounterVec // crawler_render_task_finalized_total{reason,engine} (T-172/T-235)
	fallback *prometheus.CounterVec // crawler_render_fallback_published_total{reason} (T-238)
	suspend  *prometheus.CounterVec // crawler_render_fallback_suspended_total{reason} (T-239)
	consumed *prometheus.CounterVec // crawler_render_fallback_consumed_total{result} (T-245)
	engine   string                 // the engine label value — the engine of the worker's build (T-235)
}

// NewMetrics registers the stage metrics in the reg registry (curried by
// service and stage); nil — nil. Idempotent (T-076). engine — the value
// of the engine label (T-235, spec §4.6 item 1, FR-15) on crawler_render_records_total
// and crawler_render_task_finalized_total: the engine of the worker's build
// (Options.Engine ← render.engine, the config.Engine* vocabulary), cardinality
// 3 — chromium/moli/lightpanda; the vocabulary and the "engine × remote_url ×
// memory threshold" bundle were already checked by config.Validate and render.Options.validate,
// the worker does not re-check the value — an unknown engine is rejected by the render build
// before the worker starts. An empty value — chromium, like
// render.Options.Engine (builds without configuration); the normalization here —
// the label cardinality is owned by the metrics owner. The closed vocabularies
// of suspension events (gate|budget) and fallback consumption
// (rendered|finalized|skipped) are pre-created with zero series (S-P1-4,
// an operator decision): the families are visible in the exposition from the first start —
// alerts/the panel do not wait for the first event and do not lose it (before
// pre-creation the first point of the lazy counter grounded the probe's prev and
// the 0→1 event was missed); the open vocabularies (reason codes net::ERR_* —
// fallback{reason}, tasksFin{reason,engine}) cannot and need not be pre-created:
// the codes are not enumerable, a series is born by the first finalization with
// that code.
func NewMetrics(reg *metrics.Registry, engine string) *Metrics {
	if reg == nil {
		return nil
	}
	if engine == "" {
		engine = config.EngineChromium
	}
	m := &Metrics{
		engine: engine,
		tasks: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_stage_tasks_total",
			Help: "Pipeline stage tasks processed: dns — domains, http — domains, render/analyze — subdomain×variant rows.",
		}, metrics.LabelStage).WithLabelValues(metrics.StageRender),
		seconds: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_stage_seconds",
			Help:    "Duration of processing one stage task (a domain/record episode).",
			Buckets: renderSecondsBuckets,
		}, metrics.LabelStage).WithLabelValues(metrics.StageRender),
		errors: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_stage_errors_total",
			Help: "Stage errors by type: http — check classes (timeout, redirects, tls, body, network), render — render classes (task, context, browser).",
		}, metrics.LabelStage, metrics.LabelType),
		rendered: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_records_total",
			Help: "Result records by the rendered flag: false is written by the HTTP stage, true — by the render-worker; the render share is the rate ratio. The engine label is the render engine of the worker build (chromium|moli|lightpanda, render.engine, T-235): the HTTP stage false branch has no label, aggregations and the render share without engine do not change.",
		}, "rendered", metrics.LabelEngine).WithLabelValues("true", engine),
		tarpits: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_tarpit_finalized_total",
			Help: "Records finalized without render after render.timeout_attempts is exhausted (T-148): the outcome is the raw HTML of the HTTP phase, the record goes to q:analyze; growth — the share of timeout tarpits at render. The engine label is the render engine of the worker build (chromium|moli|lightpanda, render.engine, S-P2-4): a tarpit is an engine-branched failure (a page stuck on a lightweight engine may open in chromium), the label attributes it to the engine; aggregations without engine do not change.",
		}, metrics.LabelEngine),
		certs: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_cert_finalized_total",
			Help: "Records finalized without render due to a permanent TLS certificate error of the host — net::ERR_CERT_AUTHORITY_INVALID / net::ERR_CERT_COMMON_NAME_INVALID (T-159): the outcome is the raw HTML of the HTTP phase, the record goes to q:analyze, the cert class does not reach q:dead.",
		}).WithLabelValues(),
		dns: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_dns_finalized_total",
			Help: "Records finalized without render due to a host DNS failure — net::ERR_NAME_NOT_RESOLVED (T-211): the outcome is the raw HTML of the HTTP phase, the record goes to q:analyze, the DNS class does not reach q:dead.",
		}).WithLabelValues(),
		tasksFin: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_task_finalized_total",
			Help: "Records finalized without render due to an error of the render task class on the last delivery (T-172): the outcome is the raw HTML of the HTTP phase, the record goes to q:analyze, it does not reach q:dead. The reason label is the net::ERR_* code (e.g. net::ERR_CONNECTION_REFUSED), timeout or other: the composition of what used to carry records to q:dead. The engine label is the render engine of the worker build (chromium|moli|lightpanda, render.engine, T-235): aggregations without engine do not change.",
		}, "reason", metrics.LabelEngine),
		fallback: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_fallback_published_total",
			Help: "Tasks published to the fallback queue q:render-fallback from the finalize-without-render paths (T-238, spec 4.4): engine-branched causes (the tarpit T-148, the task class T-172 on the last delivery) are reworked by chromium worker B, the record stays without the render_failed_at marker. The reason label is the vocabulary of crawler_render_task_finalized_total (the net::ERR_* code, timeout, other); the numerator of the fallback rate (FR-15).",
		}, "reason"),
		suspend: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_fallback_suspended_total",
			Help: "Fallback publication suspension events (T-239, FR-9), not a state — each latching is one point: reason=gate — the health gate, the streak of browser-class infra errors (the primary engine unavailable) reached render.fallback.health_gate_streak, publication is suspended until recovery (a successful render resets the streak; task/context class errors do not touch the streak); reason=budget — the share budget, the share of publications over a window of 1000 stage deliveries exceeded the render.fallback.share_percent cap, the next window goes without publications. While the suspension holds, fallback candidates are finalized normally with the render_failed_at marker (as with the fallback off); the browser class waits in the q:render redelivery within the redis.max_attempts retry window. Growth — a signal of primary engine degradation (alerts of spec 4.6 item 3). The series of the closed gate|budget vocabulary are pre-created as zeros (S-P1-4): the family is in the exposition from the first start, only the armed build counts events (the candidate engine, q:render — fallbackArmed).",
		}, "reason"),
		consumed: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_render_fallback_consumed_total",
			Help: "q:render-fallback tasks consumed by chromium worker B (T-245, spec 4.5/4.6 item 2, FR-11/FR-15), by the outcome of a delivery that finished processing without error: result=rendered — the rework succeeded (the DOM written, rendered=true, publication to q:analyze, spec 4.5 item 2); result=finalized — the fallback is exhausted, the record was closed via finalization (the render_failed_at marker + publication to q:analyze, FR-10: the tarpit T-148 / task class T-172 / cert T-159 / DNS T-211 paths; the marker is placed while the row belongs to the episode — a race with the recheck T-058 closes the record without the marker); result=skipped — a normal refusal without side effects (no row, no final URL, or the UPDATE guard rejected it — the row slipped out from under the task). Counting is in the handler before XACK at the queue level: an Ack failure or a duplicate delivery yields a duplicate — an allowance of units at-least-once (like the budget window T-239); failed deliveries are not counted — they live in the PEL until claimed or buried into q:dead. The acceptance balance check (spec 6.2 item 4): crawler_render_fallback_published_total (worker A, T-238) = the sum of this counter + the q:render-fallback remainder — crawler_queue_depth{queue=\"q:render-fallback\"} of worker B (the group backlog: the unread lag + the PEL; held up to the MAXLEN cap of the stream) + increase(crawler_queue_dead_buried_total{queue=\"q:render-fallback\"}) + increase(crawler_queue_dead_unidentified_total{queue=\"q:render-fallback\"}) (unidentifiable records without a task_id, T-260 — stationary zero). Only the worker B build counts (Options.Stream = q:render-fallback, FR-12): the series of the closed vocabulary of the three outcomes are pre-created as zeros in all builds (S-P1-4 — the family is in the exposition from the first start), only worker B grows them: in the primary build they live at zeros.",
		}, "result"),
	}
	// Pre-creation of zero series of the closed vocabularies (S-P1-4): both
	// families are exported from the start (with zeros), the budget alert
	// and the panel do not wait for the first event; the series grow only in
	// the armed build (suspend — the candidate engine, consumed — worker
	// B), the other builds keep them at zeros.
	m.suspend.WithLabelValues(fbReasonGate).Add(0)
	m.suspend.WithLabelValues(fbReasonBudget).Add(0)
	m.consumed.WithLabelValues(fbResultRendered).Add(0)
	m.consumed.WithLabelValues(fbResultFinalized).Add(0)
	m.consumed.WithLabelValues(fbResultSkipped).Add(0)
	return m
}

// Task — the q:render task processed successfully (including a regular rejection —
// no row or it slipped out from under the task): +1 stage task, the duration
// into the histogram.
func (m *Metrics) Task(d time.Duration) {
	if m == nil {
		return
	}
	m.tasks.Inc()
	m.seconds.Observe(d.Seconds())
}

// Rendered — the DOM is written into the row (rendered = true, spec 5.6): +1 into the
// rendered=true branch of the render share counter (with the build's engine label, T-235).
func (m *Metrics) Rendered() {
	if m == nil {
		return
	}
	m.rendered.Inc()
}

// TarpitFinalized — the entry is finalized without a render after the
// timeout limit is exhausted (T-148): +1 into the tarpit finalization counter with the
// build's engine label (S-P2-4) — an engine-branched failure is attributed
// to the engine (the material of the engine's tarpit rate on the panel).
func (m *Metrics) TarpitFinalized() {
	if m == nil {
		return
	}
	m.tarpits.WithLabelValues(m.engine).Inc()
}

// CertFinalized — the entry is finalized without a render on a permanent
// host certificate error (T-159): +1 into the cert finalization counter — the material
// of the criterion "the daily q:dead growth from the cert class = 0" (counter growth
// with a zero q:dead growth — the policy works).
func (m *Metrics) CertFinalized() {
	if m == nil {
		return
	}
	m.certs.Inc()
}

// DNSFinalized — the entry is finalized without a render on a host DNS refusal
// (T-211): +1 into the DNS finalization counter — the material of the criterion "−2
// deliveries and −20 min of PEL on a dead host" (the T-207 measurement: 252+186
// finalizations per day; counter growth with a zero q:dead growth —
// the policy works).
func (m *Metrics) DNSFinalized() {
	if m == nil {
		return
	}
	m.dns.Inc()
}

// TaskFinalized — the entry is finalized without a render on a task class
// error on the last delivery (T-172): +1 into the finalization counter with a
// reason label (the net::ERR_* / timeout / other code) and the build's engine
// label (T-235) — the material of the criterion "the daily q:dead growth from
// q:render = 0" and the visibility of the reason composition (what exactly used to reach
// q:dead before).
func (m *Metrics) TaskFinalized(reason string) {
	if m == nil {
		return
	}
	m.tasksFin.WithLabelValues(reason, m.engine).Inc()
}

// FallbackPublished — the task is published to q:render-fallback from the finalization
// path without a render (T-238): +1 with a reason label (the vocabulary
// taskFinalizeReason — the net::ERR_* / timeout / other code, the same as for
// TaskFinalized). The numerator of the fallback rate (FR-15: the share of publications
// against the stage flow — the regular health indicator of the main engine).
// A delivery duplicate (the entry is already in the stream) is not counted by the method —
// no publication happened (publishFallback).
func (m *Metrics) FallbackPublished(reason string) {
	if m == nil {
		return
	}
	m.fallback.WithLabelValues(reason).Inc()
}

// FallbackSuspended — a fallback publication suspension event (T-239,
// FR-9, spec §4.6 item 2): +1 with a reason label (the closed vocabulary
// fbReasonGate/fbReasonBudget — "gate"/"budget"). An event, not
// a state: one point per suspension latching — the gate closed
// on a streak of infra errors of the browser class (the engine is unhealthy) or the budget window
// overspent the share cap; while the suspension holds,
// candidates are finalized regularly with the marker. The material of the
// main engine degradation alerts (spec §4.6 item 3).
func (m *Metrics) FallbackSuspended(reason string) {
	if m == nil {
		return
	}
	m.suspend.WithLabelValues(reason).Inc()
}

// FallbackConsumed — the q:render-fallback delivery processed by worker B
// without an error (T-245, spec §4.6 item 2, FR-15): +1 with an outcome label (the closed
// vocabulary fbResult*): rendered — the rework succeeded, finalized — the entry
// was closed by the finalization path after the fallback was exhausted (FR-10), skipped —
// a regular rejection without side effects. The denominator of the balance published =
// consumed + the q:render-fallback remainder + those buried in q:dead (step 4
// of the spec acceptance §6.2): a delivery without an error leaves the stream's PEL with an XACK
// at the queue level (counting before it — a duplicate from an XACK failure is allowed, a few
// at-least-once units), a failed one — stays in the PEL until a claim or a bury (into the balance
// as a member of crawler_queue_dead_buried_total). Called only by the worker B build
// (Options.Stream = q:render-fallback, worker.fbConsume).
func (m *Metrics) FallbackConsumed(result string) {
	if m == nil {
		return
	}
	m.consumed.WithLabelValues(result).Inc()
}

// Error — the render task is not acknowledged (it will come back with a re-delivery):
// +1 into the stage error counter by the render.ClassOf class.
func (m *Metrics) Error(class render.Class) {
	if m == nil {
		return
	}
	m.errors.WithLabelValues(metrics.StageRender, class.String()).Inc()
}

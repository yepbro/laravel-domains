// Pipeline state snapshot (dashboard spec §4.4, §5, T-129): a single
// snapshot of all dashboard data sections. Collected by the background
// loop (collector.go) and atomically replaces the previous one; HTTP
// handlers serve the latest ready snapshot — the DB load does not
// depend on the number of open tabs (dashboard spec §3.2.3).
//
// Degradation (dashboard spec §5.9): sections are collected
// independently, a section error (DB, Redis) does not take down the
// rest — the section gets null in JSON, the reason is in the errors
// map with text; the interface shows "n/a". Values inside sections
// that could not be determined (a rate without a window, an ETA
// without a rate) are null by the same rules as in the CLI
// (internal/status).
//
// Liveness probes (dashboard spec §5.8) are not a section with an
// error: an unavailable or slow service is data ("name + up/down"),
// not degradation; the down reason sits in the probe row itself.
package dashboard

import (
	"time"

	"crawler/internal/status"
)

// LaravelItemJSON — a "recent finds" row (dashboard spec §5.6):
// domain, zone, score, the version range (empty — undetermined),
// updated_at (RFC3339).
type LaravelItemJSON struct {
	Domain    string  `json:"domain"`
	Zone      string  `json:"zone"`
	Score     float32 `json:"score"`
	VerMin    string  `json:"ver_min,omitempty"`
	VerMax    string  `json:"ver_max,omitempty"`
	UpdatedAt string  `json:"updated_at"`
}

// LaravelJSON — the Laravel finds section (dashboard spec §5.6):
// total, over the last 24 hours, the latest 10.
type LaravelJSON struct {
	Total   int64             `json:"total"`
	Last24h int64             `json:"last_24h"`
	Recent  []LaravelItemJSON `json:"recent"`
}

// IngestFileJSON — a "recent ingest files" row (dashboard spec §5.7):
// the file, the add/delete action (empty — NULL), counters,
// processed_at (RFC3339; empty — NULL).
type IngestFileJSON struct {
	Path        string `json:"path"`
	Action      string `json:"action,omitempty"`
	LinesTotal  int64  `json:"lines_total"`
	Added       int64  `json:"added"`
	Existed     int64  `json:"existed"`
	Deleted     int64  `json:"deleted"`
	Rejected    int64  `json:"rejected"`
	ProcessedAt string `json:"processed_at"`
}

// ProbeJSON — a service health row (dashboard spec §5.8): the probe
// name from dashboard.probes, up/down and the down reason
// (unavailable, timeout, non-200) — the operator needs the "why", not
// only the fact.
type ProbeJSON struct {
	Name  string `json:"name"`
	Up    bool   `json:"up"`
	Error string `json:"error,omitempty"`
}

// RenderEngineJSON — an engines-panel row (render spec §4.6 item 4,
// T-246): the rate, success and tarpit rate of one render engine per
// stage counters with the engine label (T-235): rendered —
// crawler_render_records_total {rendered="true",engine}, finalized — Σ
// crawler_render_task_finalized_total over reasons (the engine-
// dependent task class T-172; the success denominator — the operator
// decision on S-P2-4), tarpits —
// crawler_render_tarpit_finalized_total (T-148, an engine-dependent
// refusal, the engine label from T-250); cert/DNS finalizations are
// engine-independent per the render spec and do not enter the engine
// share. The cumulative — since the worker restarts (process
// metrics), the rate — between collector passes.
type RenderEngineJSON struct {
	// Engine — the render worker build engine (chromium|moli|lightpanda,
	// render.engine).
	Engine string `json:"engine"`
	// RenderedTotal — the engine's successful renders since the worker
	// restart.
	RenderedTotal int64 `json:"rendered_total"`
	// FinalizedTotal — finalizations of the task class with the engine
	// label.
	FinalizedTotal int64 `json:"finalized_total"`
	// TarpitTotal — the engine's tarpit finalizations (T-148, the
	// engine label — S-P2-4/T-250): an engine-dependent refusal, the
	// tarpit-rate material.
	TarpitTotal int64 `json:"tarpit_total"`
	// SuccessPct — the share of successful among engine-attributed
	// finalizations (rendered/(rendered+finalized)); nil — no
	// finalizations yet (a young worker).
	SuccessPct *float64 `json:"success_pct"`
	// TarpitPct — the tarpit rate: the share of tarpit finalizations
	// among all engine finalizations
	// (tarpits/(rendered+finalized+tarpits)); nil — no finalizations
	// yet. Does not change the success denominator (the operator
	// decision on S-P2-4) — the columns are independent.
	TarpitPct *float64 `json:"tarpit_pct"`
	// RSSBytes — the sum of process_resident_memory_bytes of the
	// engine's workers (S-P3-2): the memory (RSS) from spec §4.6
	// item 4; 0 — an exposure without the engine label (a build older
	// than T-235).
	RSSBytes int64 `json:"rss_bytes"`
	// RenderedPerSec — the rate of successful renders between collector
	// passes (0 — the first pass or a worker restart: no delta
	// baseline).
	RenderedPerSec float64 `json:"rendered_per_sec"`
}

// RenderFallbackJSON — the fallback block of the engines panel
// (T-246): publications by worker A
// (crawler_render_fallback_published_total, T-238), consumption by
// worker B (crawler_render_fallback_consumed_total per outcomes,
// T-245), the fallback rate (the share of publications of stage
// pages, S-P3-2) and the remainder — the q:render-fallback depth (the
// same common depth computation as the queues section).
type RenderFallbackJSON struct {
	// PublishedTotal — publications into q:render-fallback since the
	// worker A restart.
	PublishedTotal int64 `json:"published_total"`
	// PublishedPerSec — the publication rate between passes (0 —
	// without a baseline).
	PublishedPerSec float64 `json:"published_per_sec"`
	// RatePct — the fallback rate: the share of publications of stage
	// pages — the numerator is published, the denominator is Σ
	// crawler_stage_tasks_total{stage="render"} of all exposures (the
	// same units as the T-239 budget window); nil — the stage has no
	// pages yet. The acceptance checkpoints (spec §5.3) compare the
	// share against thresholds, not the absolute rate.
	RatePct *float64 `json:"rate_pct"`
	// ConsumedTotal — consumed by worker B since the restart.
	ConsumedTotal int64 `json:"consumed_total"`
	// ConsumedPerSec — the consumption rate between passes (0 —
	// without a baseline); a pair to PublishedPerSec — comparing the
	// rates is exactly the "worker B cannot keep up" diagnostics (the
	// render_fallback_depth alert, T-246).
	ConsumedPerSec float64 `json:"consumed_per_sec"`
	// ConsumedByResult — consumption by the delivered outcomes
	// (rendered|finalized|skipped); the series are pre-created with
	// zeros since T-250, an older exposure may lack an outcome — the
	// front reads via || 0.
	ConsumedByResult map[string]int64 `json:"consumed_by_result"`
	// Depth — the q:render-fallback depth (the backlog of the
	// render-fallback-worker group); nil — Redis unavailable (the
	// depth "n/a", the section alive).
	Depth *int64 `json:"depth"`
}

// Snapshot — a state snapshot (dashboard spec §5): a document ready
// for serving. The common zones/totals/queues/throughput/rates
// sections (§5.1–5.4) — status.SummaryJSON (the CLI --json wire
// contract, dashboard spec §6.2): embedding lifts the
// zones/totals/queues/throughput/rates fields to the top level.
// nil sections — collection errors (§5.9), the reasons are in errors;
// built_at — the collection moment (RFC3339), the front shows the
// snapshot freshness.
type Snapshot struct {
	status.SummaryJSON
	Statuses map[string]int64  `json:"statuses"` // §5.5: spec 6.2 names → counter; nil — a section error
	Laravel  *LaravelJSON      `json:"laravel"`  // §5.6; nil — a section error
	Ingest   []IngestFileJSON  `json:"ingest"`   // §5.7; nil — a section error
	Health   []ProbeJSON       `json:"health"`   // §5.8; nil — probes not configured ("n/a", not an error)
	Errors   map[string]string `json:"errors"`   // §5.9: section → reason; empty while all are alive
	BuiltAt  string            `json:"built_at"`

	// QueueWatermarks — the effective backpressure thresholds of the
	// working streams (§5.4): for each stream except q:dead (not
	// limited by the gate) —
	// queue.StreamWatermark(scheduler.queue_high_watermark,
	// redis.maxlen, stream), the same single computation as in the
	// publication gate (T-026) and the monitor queue overflow alert
	// (T-080): the threshold is no higher than half the stream MAXLEN
	// limit (T-141: q:dns — 125 chunks, single items — 75,000 at
	// defaults). The front needs it to highlight a stream depth
	// approaching the threshold; 0 — the gate is off. A configuration
	// value (recomputed on every pass), not a section and cannot
	// degrade.
	QueueWatermarks map[string]int64 `json:"queue_watermarks"`

	// Render — the render engines panel and fallback (render spec
	// §4.6 item 4, T-246 + S-P3-2): the rate/success/tarpit rate/RSS
	// per engine from the render workers' /metrics exposures
	// (dashboard.render_metrics_urls) and the fallback rate —
	// published/stage pages, consumed/q:render-fallback depth. nil —
	// addresses not set ("n/a", not an error — Health style) or the
	// section degraded (the reason is in errors).
	Render *RenderJSON `json:"render"`
}

// RenderJSON — the engines panel section: rows per engines
// (rate/success) and the fallback block.
type RenderJSON struct {
	// Engines — the engines seen in the worker exposures (order — by
	// name, deterministic JSON); empty — workers without renders.
	Engines []RenderEngineJSON `json:"engines"`
	// Fallback — the fallback published/consumed/depth.
	Fallback RenderFallbackJSON `json:"fallback"`
}

// rfc3339 — the common format of snapshot time moments.
func rfc3339(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// rfc3339Zero — like rfc3339, but a zero time (NULL schema columns)
// — an empty string: "0001-01-01T00:00:00Z" in the interface is
// noise.
func rfc3339Zero(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return rfc3339(t)
}

// SnapshotSource — the source of the ready snapshot for /api/status:
// the T-129 collection loop (*Collector). A nil answer — no snapshot
// yet: until the first successful collection /api/status answers 503
// (dashboard spec §5.9). A separate interface rather than *Collector
// directly: the T-128 framework and its tests work without collection.
type SnapshotSource interface {
	Current() *Snapshot
}

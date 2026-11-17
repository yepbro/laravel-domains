// Metrics of the retention cycle (T-166, spec 5.9): counters of purged rows
// and bytes, a pass duration histogram, and an interlock skip counter —
// the material for the "disk is growing" and "the analyzer cannot keep up"
// alerts.
//
// Architecturally it is push, not poll: pass outcomes are the return values
// of RetentionOnce; there is no long-lived state. The RetentionLoop cycle
// hands the outcome over via the RetentionOptions hooks — OnPass (a
// successful/skipped pass) and OnError (a failed pass: the partial work
// since T-295, the duration since T-322) — and the collector turns it into
// instruments — the same seam as
// CleanupOptions.OnPass (T-020/T-099); the retention.go package picks up no
// dependency on metrics.
package content

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// retentionSecondsBuckets — the pass duration buckets: a batch of up to
// 50k rows (content.retention_batch) takes seconds on a live DB, draining
// ~13 batches takes tens of seconds; the tail (the T-167 backfill,
// VACUUM neighborhood) is covered by minutes.
var retentionSecondsBuckets = []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300}

// RetentionMetrics — the instruments of the retention cycle: counters grow
// monotonically with pass outcomes; the histogram is observed on every
// drain pass — ok and error alike (T-322: a failed pass burned the same
// wall clock); skips and ctx-cancelled passes are not observed.
type RetentionMetrics struct {
	rows          prometheus.Counter     // crawler_content_retention_rows_total
	refs          prometheus.Counter     // crawler_content_retention_refs_total
	contents      prometheus.Counter     // crawler_content_retention_contents_total
	freed         prometheus.Counter     // crawler_content_retention_freed_bytes_total
	skips         prometheus.Counter     // crawler_content_retention_skips_total
	pendingErrors prometheus.Counter     // crawler_content_retention_pending_errors_total
	badHashes     prometheus.Counter     // crawler_content_retention_bad_hashes_total
	missing       prometheus.Counter     // crawler_content_retention_missing_refs_total
	errors        prometheus.Counter     // crawler_content_retention_errors_total (T-273)
	seconds       prometheus.Observer    // crawler_content_retention_pass_seconds
	passes        *prometheus.CounterVec // crawler_content_retention_passes_total{result}
	deferred      *prometheus.CounterVec // crawler_content_retention_deferred_total{stream}
}

// NewRetentionMetrics registers the retention instruments in the reg
// registry; nil in — nil out (a service without a registry — isolated
// builds). Idempotent (T-076): a repeated call shares the same collectors.
func NewRetentionMetrics(reg *metrics.Registry) *RetentionMetrics {
	if reg == nil {
		return nil
	}
	return &RetentionMetrics{
		rows: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_rows_total",
			Help: "subdomain_results rows purged by retention (T-166): the content_hash and/or the heavy jsonb of non-findings older than content.retention are trimmed; the row passport stays.",
		}).WithLabelValues(),
		refs: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_refs_total",
			Help: "Trimmed content_hash references (T-166): each corresponds to −1 contents.refcount in the same transaction.",
		}).WithLabelValues(),
		contents: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_contents_total",
			Help: "contents rows deleted after losing their last reference (T-166); the physical block space is reclaimed by pack compaction — convergence is visible via crawler_content_* / disk_avail.",
		}).WithLabelValues(),
		freed: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_freed_bytes_total",
			Help: "Space (compressed bytes) of blocks that lost their references (T-166): a counter of the claim to the space, the actual freeing is pack compaction (T-045).",
		}).WithLabelValues(),
		skips: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_skips_total",
			Help: "Retention skips by interlock: the q:analyze depth is at the content.retention_analyze_watermark threshold — the analyzer cannot keep up, the bodies and headers/cookies are still needed; growth = analysis falls behind, the disk keeps growing.",
		}).WithLabelValues(),
		pendingErrors: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_pending_errors_total",
			Help: "Retention skips by an error reading the unconsumed q:render/q:analyze keys (conservatively: the queue records are untouched, the pass is postponed); growth = the guard cannot read the streams — Redis/access, investigate immediately: without the guard the purge window for waiting records is open.",
		}).WithLabelValues(),
		badHashes: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_bad_hashes_total",
			Help: "References trimmed by retention with a wrong-length content_hash (16 octets, spec 6.1): their refcount was never changed; must not grow at all — growth = a result writer bug (T-056), an analogue of crawler_ingest_bad_hashes_total.",
		}).WithLabelValues(),
		missing: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_missing_refs_total",
			Help: "References trimmed by retention without a contents row — a refcount accounting divergence (T-044); must not grow in a healthy system: growth = an accounting bug, investigate via the T-044 invariants.",
		}).WithLabelValues(),
		errors: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_errors_total",
			Help: "Failed retention passes (T-273): the pass aborted on an error — batches committed before it stay in force (the cursor advanced partially; their rows/refs/bytes are credited in the work counters via the same observation, T-295; the pass duration is observed in crawler_content_retention_pass_seconds, T-322), the rest of the set repeats after a backoff. Before T-273 errors were visible only in the journal (the 2026-11-09–10 incident: a day of WARNs with zero pass metrics); a growing series = the drain stands still, watch the content disk and the Warn heading in journalctl.",
		}).WithLabelValues(),
		seconds: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_content_retention_pass_seconds",
			Help:    "Duration of one retention pass (drain in batches, T-166). Failed passes are observed with their duration too (T-322): the error burned the same wall clock — often the whole selection cap — so pass_seconds_count keeps growing while the drain is being attempted, next to a growing errors_total (not metric silence). Skips (the interlock, the guard) are not observed — including a mid-drain skip: its partial work is credited in the work counters, its duration is not (the outcome taxonomy, not the amount of work); a ctx-cancelled pass is not observed either (the hook is not called).",
			Buckets: retentionSecondsBuckets,
		}).WithLabelValues(),
		passes: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_passes_total",
			Help: "Retention passes by outcome: ok — the drain completed (including an empty one), skipped — the q:analyze interlock (the bodies are still needed), skipped_pending — an error reading the unconsumed q:render/q:analyze (the pass is postponed); error passes are not counted here — crawler_content_retention_errors_total counts them (T-273).",
		}, "result"),
		deferred: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_deferred_total",
			Help: "Records excluded by retention from the purge: the messages wait for stream consumers (q:render — SPA rendering, q:analyze — analysis; a record can wait for both and is counted in each). Grows when the queues stall, alongside the queue metrics — a signal to drain the consumers (runbooks §5.2).",
		}, "stream"),
	}
}

// Observe — the outcome of one pass (wired in via the
// RetentionOptions.OnPass hook): counters and duration. A zero outcome
// changes nothing; a nil receiver is a no-op (a service without a registry)
// so that the build can wire the hook unconditionally. Skipped is counted by
// the passes{result="skipped"} label and the skips counter, SkippedPending —
// by the passes{result="skipped_pending"} label and the pendingErrors
// counter; skips in the middle of a drain (an interlock/error after some
// batches) do not lose the work already done — Rows/Refs/Contents/Freed/
// Deferred are credited in them too, since the batches are committed in the
// DB; a FAILED pass credits its partial work the same way — via
// ObserveError (T-295, review 09#P3-1) — and its duration lands in the
// histogram like a successful one (T-322, review 09#P3-4).
func (m *RetentionMetrics) Observe(res RetentionStats, d time.Duration) {
	if m == nil {
		return
	}
	if res.Skipped {
		m.skips.Inc()
		m.passes.WithLabelValues("skipped").Inc()
		m.observeWork(res)
		return
	}
	if res.SkippedPending {
		m.pendingErrors.Inc()
		m.passes.WithLabelValues("skipped_pending").Inc()
		m.observeWork(res)
		return
	}
	m.passes.WithLabelValues("ok").Inc()
	m.seconds.Observe(d.Seconds())
	m.observeWork(res)
}

// ObserveError — a failed pass (T-273; the partial statistics — T-295,
// review 09#P3-1; the duration — T-322, review 09#P3-4): +1 to
// crawler_content_retention_errors_total, the work counters of the
// batches committed before the error point — they are already in the DB
// (the cursor advanced partially), losing them undercounted rows/refs/
// bytes against the disk reality — and the pass duration in the
// crawler_content_retention_pass_seconds histogram, the same observation
// as a successful pass (the scheduler.backlog / monitor.zonestats
// precedent — took is measured after the outcome switch): a failed pass
// burned real wall clock, often the whole selection timeout cap (the
// 2026-11-10/11 incident), and without it pass_seconds_count understates
// the pass tempo exactly while errors grow. Wired in via the
// RetentionOptions.OnError hook (the error value is not classified — the
// text is visible in the journal Warn). The pass is not counted in
// passes_total: errors_total is its label. A nil receiver is a no-op.
func (m *RetentionMetrics) ObserveError(_ error, partial RetentionStats, d time.Duration) {
	if m == nil {
		return
	}
	m.errors.Inc()
	m.seconds.Observe(d.Seconds())
	m.observeWork(partial)
}

// observeWork — the counters of the work done by the pass (the common part
// of the ok/skipped/skipped_pending branches; BadHashes/Missing — only when
// present).
func (m *RetentionMetrics) observeWork(res RetentionStats) {
	m.rows.Add(float64(res.Rows))
	m.refs.Add(float64(res.Refs))
	m.contents.Add(float64(res.Contents))
	m.freed.Add(float64(res.FreedBytes))
	for stream, n := range res.Deferred {
		if n > 0 {
			m.deferred.WithLabelValues(stream).Add(float64(n))
		}
	}
	if res.BadHashes > 0 {
		m.badHashes.Add(float64(res.BadHashes))
	}
	if res.Missing > 0 {
		m.missing.Add(float64(res.Missing))
	}
}

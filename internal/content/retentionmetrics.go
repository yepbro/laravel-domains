// Metrics of the retention cycle (T-166, spec 5.9): counters of purged rows
// and bytes, a pass duration histogram, and an interlock skip counter —
// the material for the "disk is growing" and "the analyzer cannot keep up"
// alerts.
//
// Architecturally it is push, not poll: pass outcomes are the return values
// of RetentionOnce; there is no long-lived state. The RetentionLoop cycle
// hands the outcome over via the RetentionOptions.OnPass hook, and the
// collector turns it into instruments — the same seam as
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
// monotonically with pass outcomes; the histogram is observed on every pass.
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
			Help: "Строки subdomain_results, очищенные retention (T-166): срезаны content_hash и/или тяжёлые jsonb не-находок старше content.retention; паспорт строки остаётся.",
		}).WithLabelValues(),
		refs: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_refs_total",
			Help: "Срезанные ссылки content_hash (T-166): каждой соответствует −1 contents.refcount той же транзакцией.",
		}).WithLabelValues(),
		contents: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_contents_total",
			Help: "Удалённые строки contents, лишившиеся последней ссылки (T-166); физическое место блоков возвращает компактификация паков — сходимость видно по crawler_content_* / disk_avail.",
		}).WithLabelValues(),
		freed: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_freed_bytes_total",
			Help: "Место (сжатые байты) блоков, лишившихся ссылок (T-166): счётчик права на место, фактическое освобождение — компактификация паков (T-045).",
		}).WithLabelValues(),
		skips: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_skips_total",
			Help: "Пропуски retention по интерлоку: глубина q:analyze у порога content.retention_analyze_watermark — анализатор не успевает, тела и headers/cookies ещё нужны; рост = анализ отстаёт, диск продолжает расти.",
		}).WithLabelValues(),
		pendingErrors: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_pending_errors_total",
			Help: "Пропуски retention по ошибке чтения непотреблённых ключей q:render/q:analyze (консервативно: записи очередей не тронуты, проход отложен); рост = защита не может прочитать стримы — Redis/доступ, разбираться сразу: без защиты окно чистки ждущих записей открыто.",
		}).WithLabelValues(),
		badHashes: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_bad_hashes_total",
			Help: "Срезанные retention ссылки с content_hash неверной длины (16 октет, ТЗ 6.1): refcount по ним не менялся; в норме не растёт вовсе — рост = ошибка писателя результатов (T-056), аналог crawler_ingest_bad_hashes_total.",
		}).WithLabelValues(),
		missing: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_missing_refs_total",
			Help: "Срезанные retention ссылки без строки contents — расхождение учёта refcount (T-044); в норме не растёт: рост = баг учёта, разбираться по инвариантам T-044.",
		}).WithLabelValues(),
		errors: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_errors_total",
			Help: "Ошибочные проходы retention (T-273): проход прерван на ошибке — пачки, зафиксированные до неё, остаются в силе (курсор продвинулся частично), остаток множества повторится после backoff. До T-273 ошибки были видны только в журнале (инцидент 09–10.11.2026: сутки WARN-ов при нулевых метриках проходов); рост серии = дренаж стоит, контролировать диск контента и заголовок Warn в journalctl.",
		}).WithLabelValues(),
		seconds: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_content_retention_pass_seconds",
			Help:    "Длительность одного прохода retention (дренаж пачками, T-166).",
			Buckets: retentionSecondsBuckets,
		}).WithLabelValues(),
		passes: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_passes_total",
			Help: "Проходы retention по итогам: ok — дренаж выполнен (в т.ч. пустой), skipped — интерлок q:analyze (тела ещё нужны), skipped_pending — ошибка чтения непотреблённых q:render/q:analyze (проход отложен); error-проходы здесь не считаются — их считает crawler_content_retention_errors_total (T-273).",
		}, "result"),
		deferred: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_content_retention_deferred_total",
			Help: "Записи, исключённые retention из чистки: сообщения ждут потребителей стрима (q:render — рендер SPA, q:analyze — анализ; запись может ждать оба — считается в каждом). Растёт при застое очередей рядом с queue-метриками — сигнал дренировать потребителей (runbooks §5.2).",
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
// DB.
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

// ObserveError — a failed pass (T-273): +1 to
// crawler_content_retention_errors_total; wired in via the
// RetentionOptions.OnError hook (the error value is not classified —
// the text is visible in the journal Warn). A nil receiver is a no-op.
func (m *RetentionMetrics) ObserveError(error) {
	if m == nil {
		return
	}
	m.errors.Inc()
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

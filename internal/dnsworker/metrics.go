// DNS stage instruments (T-077, spec 5.9): throughput (a counter of
// finished episodes and a histogram of their durations) and the final
// domain statuses of the terminal DNS branches (not_delegated /
// delegated_no_address / temp_error / invalid, §6.2/§7). The statuses of
// the "addresses present" branch are not written (the outcome belongs to
// the HTTP stage) — http-worker counts them with the same
// crawler_domain_status_total counter. invalid is poison row diagnostics
// (T-109), not a fact about the domain. Exchange errors are not typed by
// a separate counter: the DNS stage does not classify resolver failures
// (transport after retries and any response codes reduce to temp_error,
// T-036) — the status counter is the honest typing of the outcome;
// typing an episode failure is queue territory (redeliveries, q:dead).
package dnsworker

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
	"crawler/internal/model"
)

// dnsSecondsBuckets — DNS episode duration buckets: UDP exchanges take
// milliseconds, but an episode is a sequence of exchanges (apex,
// subdomains, wildcard) under the concurrency/QPS limits (T-035), and a
// failing resolver burns timeouts and retries — up to tens of seconds.
var dnsSecondsBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120}

// Metrics — the DNS stage instruments; built by NewMetrics from the
// service registry, nil (no registry — isolated builds) makes every
// method a no-op. Safe for concurrent use: episodes run in parallel and
// the prometheus instruments are synchronized.
type Metrics struct {
	tasks     prometheus.Counter     // crawler_stage_tasks_total{stage="dns"}
	seconds   prometheus.Observer    // crawler_stage_seconds{stage="dns"}
	statuses  *prometheus.CounterVec // crawler_domain_status_total{type}
	dbRetried prometheus.Counter     // crawler_db_deadlock_retried_total (T-268)
}

// NewMetrics registers the DNS stage instruments in the reg registry
// (curried by service and stage); nil in — nil out. Idempotent (T-076):
// a repeated call shares the same collectors.
func NewMetrics(reg *metrics.Registry) *Metrics {
	if reg == nil {
		return nil
	}
	return &Metrics{
		tasks: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_stage_tasks_total",
			Help: "Обработанные задачи этапов конвейера: dns — домены, http — домены, render/analyze — записи субдомен×вариант.",
		}, metrics.LabelStage).WithLabelValues(metrics.StageDNS),
		seconds: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_stage_seconds",
			Help:    "Длительность обработки одной задачи этапа (эпизод домена/записи).",
			Buckets: dnsSecondsBuckets,
		}, metrics.LabelStage).WithLabelValues(metrics.StageDNS),
		statuses: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_domain_status_total",
			Help: "Финальные статусы доменов по итогам эпизодов (ТЗ 6.2): DNS-этап — not_delegated/delegated_no_address/temp_error/invalid (диагноз ядовитой строки, T-109), HTTP-этап — alive/no_http/temp_error.",
		}, metrics.LabelType),
		// The 40P01 retry counter (T-268): a single name/HELP for all
		// services — the internal/metrics constructor.
		dbRetried: metrics.NewDBDeadlockRetried(reg),
	}
}

// Episode — a domain episode finished successfully: +1 stage task, the
// duration goes into the histogram. Called after ProcessDomain finishes
// (including the redelivery of an already recorded outcome — at-least-once
// counts re-done work as honest work).
func (m *Metrics) Episode(d time.Duration) {
	if m == nil {
		return
	}
	m.tasks.Inc()
	m.seconds.Observe(d.Seconds())
}

// DomainStatus — the domain received a terminal DNS stage status
// (not_delegated/delegated_no_address/temp_error, §6.2; invalid — the
// poison row diagnosis, T-109): +1 to the status counter. The status of
// a living domain (addresses present) is not written — the outcome
// belongs to the HTTP stage (T-039, §7).
func (m *Metrics) DomainStatus(status model.Status) {
	if m == nil {
		return
	}
	m.statuses.WithLabelValues(status.String()).Inc()
}

// DBDeadlockRetried — a DB operation was retried after a 40P01 deadlock
// (T-268): +1 to the retry counter; the db.RetryDeadlock hook (attempt —
// the retry number, one series, the label is reserved for the service).
func (m *Metrics) DBDeadlockRetried(attempt int) {
	if m == nil {
		return
	}
	m.dbRetried.Inc()
}

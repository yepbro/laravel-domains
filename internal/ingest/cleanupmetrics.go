// The metric of the background cleanup of deleted domains (T-099, spec
// 5.9): the crawler_ingest_bad_hashes_total counter — results removed by
// a pass with a wrong-length content_hash (CleanupResult.BadHashes). In
// the normal case the writer invariant keeps 16 octets (spec 6.1, T-056)
// and the value never grows at all: any growth is a writer bug, not load
// noise; the monitoring alert (internal/monitor, T-099) catches a spike
// via the exposition of this counter.
//
// Architecturally it is not a poll but a push: the cleanup pass results
// are the return values of CleanupOnce; there is no long-lived state with
// snapshots (like LimitStats/FallbackStats of the DNS collectors) —
// nothing to poll. The CleanupDeleted loop hands each pass's result over
// the CleanupOptions.OnPass hook; the collector only converts it into a
// counter — the same seam as AlertsOptions.OnPass in monitoring
// ("log + field"); cleanup.go takes no metric dependencies into the
// package.
package ingest

import (
	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// CleanupMetrics — the deleted-domain cleanup instruments: the junk
// content_hash counter (created once, its values grow monotonically with
// the pass results).
type CleanupMetrics struct {
	badHashes prometheus.Counter // crawler_ingest_bad_hashes_total
}

// NewCleanupMetrics registers the cleanup instruments in the reg
// registry; nil in — nil out (a service without a registry — isolated
// builds — raises no metrics). Idempotent (T-076): a repeated call
// shares the same counter.
func NewCleanupMetrics(reg *metrics.Registry) *CleanupMetrics {
	if reg == nil {
		return nil
	}
	return &CleanupMetrics{
		badHashes: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_ingest_bad_hashes_total",
			Help: "Удалённые очисткой deleted-доменов результаты с content_hash неверной длины (16 октет, ТЗ 6.1): refcount по ним не менялся, рост — ошибка писателя результатов (T-056); алерт ingest_bad_hashes мониторит всплески.",
		}).WithLabelValues(),
	}
}

// Observe — the result of one cleanup pass (wired by the
// CleanupOptions.OnPass hook): adds the pass's BadHashes to the counter.
// A zero result does not change the value; a nil receiver is a no-op (a
// service without a registry), so cmd/ingest can wire the hook
// unconditionally.
func (m *CleanupMetrics) Observe(res CleanupResult) {
	if m == nil || res.BadHashes <= 0 {
		return
	}
	m.badHashes.Add(float64(res.BadHashes))
}

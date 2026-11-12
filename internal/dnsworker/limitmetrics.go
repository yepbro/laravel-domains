// Collector of the DNS limiter state (T-077, spec 5.9): periodic polling
// of LimitResolver.Stats into gauges — the adaptive downscaling of
// concurrency/QPS by the timeout share (T-035, spec 5.4) must be
// observable. Without the export, the pipeline could silently run at 1%
// of its planned capacity (the resolver times out, the limits are at the
// floor) while Prometheus would show only the growth of the q:dns depth
// and of the temp_error share — with no cause.
//
// Architecturally — modeled on the queue depth collector
// (metrics.CollectQueueLoop): the loop lives in the service carrying the
// limiter (dns-worker), the state source is injected as the
// LimitStatsFunc (the Stats method of the decorator) — the package does
// not create reverse dependencies dnsclient → metrics. Data and
// instruments are separated as in dnsclient: LimitStats is a snapshot
// without metrics, and this file is export only.
package dnsworker

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/dnsclient"
	"crawler/internal/metrics"
)

// DefaultLimitCollectInterval — the limiter state polling period: like
// the queue collector, it equals the typical Prometheus scrape interval
// (dev config deploy/docker/prometheus) — every scrape sees a fresh
// value, and there is no reason to poll more often: the adaptive changes
// the limits on the scale of an exchange window, not milliseconds.
const DefaultLimitCollectInterval = 15 * time.Second

// The values of the kind label: configured — the configured caps
// (dns.concurrency, dns.max_qps, spec 5.10), effective — the actual ones
// after the adaptive downscaling. The pair of series in one metric gives
// a PromQL-ready degradation share:
// crawler_dns_limit_qps{kind="effective"} / {kind="configured"}.
const (
	limitKindEffective  = "effective"
	limitKindConfigured = "configured"
)

// LimitStatsFunc — the source of the limiter state: satisfied by the
// Stats method of the *dnsclient.LimitResolver decorator (a method
// value); unit tests substitute a closure over a prepared snapshot.
type LimitStatsFunc func() dnsclient.LimitStats

// limitGauges — the collector instruments (created once, the values are
// overwritten by every pass).
type limitGauges struct {
	concurrency *prometheus.GaugeVec // crawler_dns_limit_concurrency{kind}
	qps         *prometheus.GaugeVec // crawler_dns_limit_qps{kind}
	ratio       prometheus.Gauge     // crawler_dns_limit_timeout_ratio
	inflight    prometheus.Gauge     // crawler_dns_inflight
}

// newLimitGauges registers the instruments in the reg registry; nil in —
// nil out (a service without a registry — isolated builds — has no
// collector). Idempotent (T-076): a repeated call shares the same
// vectors.
func newLimitGauges(reg *metrics.Registry) *limitGauges {
	if reg == nil {
		return nil
	}
	return &limitGauges{
		concurrency: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_dns_limit_concurrency",
			Help: "Потолок параллелизма DNS-обменов: effective — действующий после адаптивного снижения (T-035), configured — dns.concurrency.",
		}, metrics.LabelKind),
		qps: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_dns_limit_qps",
			Help: "Средний темп DNS-обменов в секунду: effective — действующий после адаптивного снижения (T-035), configured — dns.max_qps.",
		}, metrics.LabelKind),
		ratio: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_dns_limit_timeout_ratio",
			Help: "Доля таймаутов в скользящем окне адаптива лимитёра DNS (0, пока окно наполняется).",
		}).WithLabelValues(),
		inflight: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_dns_inflight",
			Help: "DNS-обменов в нижнем резолвере прямо сейчас (занятые слоты параллелизма).",
		}).WithLabelValues(),
	}
}

// collectLimitPass — one pass: a source snapshot → the gauge values.
func collectLimitPass(g *limitGauges, stats LimitStatsFunc) {
	st := stats()
	g.concurrency.WithLabelValues(limitKindEffective).Set(float64(st.Concurrency))
	g.concurrency.WithLabelValues(limitKindConfigured).Set(float64(st.ConcurrencyCfg))
	g.qps.WithLabelValues(limitKindEffective).Set(st.QPS)
	g.qps.WithLabelValues(limitKindConfigured).Set(st.QPSCfg)
	ratio := 0.0
	if st.WindowSize > 0 { // the window is filling — the share per what has been collected
		ratio = float64(st.WindowTimeouts) / float64(st.WindowSize)
	}
	g.ratio.Set(ratio)
	g.inflight.Set(float64(st.InFlight))
}

// CollectLimitLoop — the periodic loop polling the DNS limiter state:
// the first pass runs immediately at startup, then on a ticker with the
// interval period (0 — DefaultLimitCollectInterval). A ctx cancellation
// is a clean exit (graceful shutdown, spec 3.4); nil dependencies are a
// no-op: a service without a registry must not crash at startup. Runs
// until ctx is cancelled and returns nil — the same contract as
// metrics.CollectQueueLoop (the service's background collector, T-012).
func CollectLimitLoop(
	ctx context.Context,
	reg *metrics.Registry,
	stats LimitStatsFunc,
	interval time.Duration,
	log *slog.Logger,
) error {
	if stats == nil {
		return nil // before registration: no instruments without a source
	}
	g := newLimitGauges(reg)
	if g == nil {
		return nil
	}
	if interval <= 0 {
		interval = DefaultLimitCollectInterval
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("коллектор лимитёра DNS", "interval", interval.String())

	pass := func() { collectLimitPass(g, stats) }
	pass()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pass()
		}
	}
}

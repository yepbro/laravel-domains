// Collector of DNS switches to the fallback resolvers (T-107, spec 5.9):
// periodic polling of FallbackResolver.Stats into counters. A switch is
// normal degradation (spec 5.4: "public resolvers are fallback only"),
// but it must be observable: without the metric, a local unbound failure
// is visible only indirectly — through the growing latency of episodes
// (each exchange pays for the failed one's retry cycle) and through the
// limiter metrics, which stay "healthy" while the fallback is alive (the
// adaptive T-035 sees a successful outcome, and that is correct: the
// pipeline is working).
//
// Architecturally — the same pattern as the limiter collector
// (limitmetrics.go): the loop lives in the service carrying the chain
// (dns-worker), the state source is injected as the FallbackStatsFunc
// (the Stats method of the decorator) — the package does not create
// reverse dependencies dnsclient → metrics. Counters, not gauges: the
// values accumulate in the source, and the collector adds the delta
// between passes (the Prometheus semantics of _total and rate() are
// preserved).
package dnsworker

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/dnsclient"
	"crawler/internal/metrics"
)

// FallbackStatsFunc — the source of switch state: satisfied by the Stats
// method of the *dnsclient.FallbackResolver decorator (a method value);
// unit tests substitute a closure over a prepared snapshot.
type FallbackStatsFunc func() dnsclient.FallbackStats

// fallbackCounters — the collector instruments (created once, the values
// grow monotonically by the deltas of the passes).
type fallbackCounters struct {
	switches  prometheus.Counter // crawler_dns_fallback_switches_total
	exhausted prometheus.Counter // crawler_dns_fallback_exhausted_total
}

// newFallbackCounters registers the instruments in the reg registry;
// nil in — nil out (a service without a registry has no collector).
// Idempotent (T-076): a repeated call shares the same counters.
func newFallbackCounters(reg *metrics.Registry) *fallbackCounters {
	if reg == nil {
		return nil
	}
	return &fallbackCounters{
		switches: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_dns_fallback_switches_total",
			Help: "DNS-обменов, отвеченных резервным резолвером: основной не ответил транспортно, сработало переключение dns.fallback_resolvers (T-107).",
		}).WithLabelValues(),
		exhausted: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_dns_fallback_exhausted_total",
			Help: "DNS-обменов, провалившихся на всей цепи резолверов (основной + резервные): каждый — будущий temp_error (T-036/T-039).",
		}).WithLabelValues(),
	}
}

// collectFallbackPass — one pass: a snapshot delta → the counters. A
// negative delta (the source counter is smaller than before — the chain
// was recreated in a test or by an in-process restart) is added in full:
// a Prometheus counter is monotonic, it cannot be rewound.
func collectFallbackPass(c *fallbackCounters, stats FallbackStatsFunc, prev *dnsclient.FallbackStats) {
	st := stats()
	if d := st.Switches - prev.Switches; d > 0 {
		c.switches.Add(float64(d))
	} else if d < 0 {
		c.switches.Add(float64(st.Switches))
	}
	if d := st.Exhausted - prev.Exhausted; d > 0 {
		c.exhausted.Add(float64(d))
	} else if d < 0 {
		c.exhausted.Add(float64(st.Exhausted))
	}
	*prev = st
}

// CollectFallbackLoop — the periodic loop polling the switch state: the
// first pass runs immediately at startup, then on a ticker with the
// interval period (0 — DefaultLimitCollectInterval, the same scrape
// interval as the limiter collector). A ctx cancellation is a clean exit
// (graceful shutdown, spec 3.4); nil dependencies — a no-op. Runs until
// ctx is cancelled and returns nil — the service background collector
// contract (T-012).
func CollectFallbackLoop(
	ctx context.Context,
	reg *metrics.Registry,
	stats FallbackStatsFunc,
	interval time.Duration,
	log *slog.Logger,
) error {
	if stats == nil {
		return nil // before registration: no instruments without a source
	}
	c := newFallbackCounters(reg)
	if c == nil {
		return nil
	}
	if interval <= 0 {
		interval = DefaultLimitCollectInterval
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("коллектор переключений DNS на резерв", "interval", interval.String())

	var prev dnsclient.FallbackStats
	pass := func() { collectFallbackPass(c, stats, &prev) }
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

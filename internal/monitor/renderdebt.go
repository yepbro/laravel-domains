// Render debt (T-175, spec 5.9): observability of render work backlog
// growth without manual SQL. A debt of 443 k (the q:render stream +
// render_pending flags, the Oct 24 freeze) was found by the operator by
// hand: counting by bitmask is a full seq scan of 41.8 M rows (tens of
// seconds of DB CPU), the T-162 "frozen queue" alert catches stalled
// deliveries but not accumulation — the queue moves and grows at the
// same time.
//
// Debt = flags + lag:
//
//   - flags — domains with the render_pending bit (bit 1 of domains.flags,
//     T-066: the render queue is overloaded, the domain is finished later) —
//     the incremental counter crawler_counters['render_pending']
//     (migration 000015): +1/−1 in the bit transition transaction
//     (internal/httpworker/renderq.go), the read is a point Lookup by PK;
//     the partial index (flags & 2) <> 0 was rejected — it breaks HOT
//     updates of mass flags updates (docs/schema.md "HOT updates",
//     the rationale — migration 000015);
//   - lag — the depth of the q:render stream (the unconsumed remainder +
//     pending, the backpressure gate formula): the same StreamLener the
//     queue alerts use; mirrored in metrics —
//     crawler_queue_depth{queue="q:render"} (the render-worker collector,
//     T-077), only read here.
//
// Three consumers of one snapshot (renderDebtOf — the semantics do not
// diverge): the RenderDebtLoop loop (gauge + a summary line in the log),
// the "Render debt" line of the load report (load.go, T-173) and the
// render_debt accumulation alert (alerts.go): debt above the threshold
// AND growing for N passes in a row — accumulation, not a one-off
// overflow spike.
package monitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/queue"
)

// DefaultRenderDebtInterval — the render debt loop period: 5 minutes
// (the old parity with the zone recount ended — T-199 thinned the
// reconciliation out to 30 min). A pass is a point Lookup by the counter
// PK + XLEN of q:render (milliseconds — it cannot get more expensive),
// so the period is chosen not by cost but by freshness: the debt is a
// slow value (the q:render drain ≈1–2 entries/s, measurement §12,
// docs/render.md), it does not change faster than the five-minute
// window; the 30s..1h corridor follows the zone_stats_interval example.
// The period concerns ONLY the gauge and the summary of this loop: the
// render_debt alert is evaluated by an AlertsLoop pass per
// monitor.alerts_interval (T-175 review, P3-6; T-206).
const DefaultRenderDebtInterval = 5 * time.Minute

// renderDebtFlagsSQL reads the render_pending flags counter: a point
// Lookup by the crawler_counters PK (migration 000015). An empty result
// means the counter row is missing (the migration creates it; it can
// disappear only by a manual table edit, runbooks §6) — a source error,
// not zero: a silent zero would make the debt gauge a lie.
const renderDebtFlagsSQL = "SELECT value FROM crawler_counters WHERE name = 'render_pending'"

// RenderDebt is a render debt snapshot: the render_pending flags
// (counter) + the lag of the q:render stream.
type RenderDebt struct {
	// Flags counts domains with the render_pending bit (crawler_counters).
	Flags int64
	// Lag is the depth of the q:render stream (the consumer group
	// backlog: min(lag, XLEN) + pending — the BackpressureGate.Len
	// formula, T-168).
	Lag int64
}

// Total is the debt in full: flags + lag (work waiting for render).
func (d RenderDebt) Total() int64 { return d.Flags + d.Lag }

// String is a one-line summary (the loop log, the alert detail, the
// load report line — the same text).
func (d RenderDebt) String() string {
	return fmt.Sprintf("flags %d + lag %d = %d", d.Flags, d.Lag, d.Total())
}

// RenderDebtSource is a render debt source; an interface for
// substitution in unit tests (the real read is DB + Redis).
// StreamGroupsSource style.
type RenderDebtSource interface {
	// RenderDebt returns a debt snapshot. An error means "not measured",
	// with the reason attached.
	RenderDebt(ctx context.Context) (RenderDebt, error)
}

// RenderDebtReader is a RenderDebtSource on top of the DB pool (the
// flags counter) and a stream depth source (the q:render lag).
type RenderDebtReader struct {
	// Pool is the DB of the crawler_counters counter; nil — a source error.
	Pool db.DB
	// Redis is the depth of the q:render stream (StreamLener: the
	// monitor's backpressure gate); nil — a source error.
	Redis StreamLener
}

// compile-time: RenderDebtReader is a RenderDebtSource.
var _ RenderDebtSource = RenderDebtReader{}

// RenderDebt reads both debt components. The snapshot is not atomic
// across DB and Redis (two stores) — and does not have to be: the
// components converge to zero independently, a fraction of a second
// between the reads is invisible to the growth alert
// (the threshold is tens of thousands, the window is passes).
func (r RenderDebtReader) RenderDebt(ctx context.Context) (RenderDebt, error) {
	return renderDebtOf(ctx, r.Pool, r.Redis)
}

// renderDebtOf is the shared debt snapshot: the gauge loop, the load
// report and the alert read it the same way (a semantics divergence
// would make the three numbers mutually contradictory).
func renderDebtOf(ctx context.Context, pool db.DB, redis StreamLener) (RenderDebt, error) {
	if pool == nil {
		return RenderDebt{}, errors.New("monitor: render debt: the DB pool is not set")
	}
	if redis == nil {
		return RenderDebt{}, errors.New("monitor: render debt: the q:render depth source is not set")
	}
	rows, err := pool.Query(ctx, renderDebtFlagsSQL)
	if err != nil {
		return RenderDebt{}, fmt.Errorf("monitor: render debt: counter: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		// An iteration error with zero rows read — not "the row is
		// missing" (T-175 review, P3-2: diagnostics must not send people
		// to the runbooks on a connection failure).
		if err := rows.Err(); err != nil {
			return RenderDebt{}, fmt.Errorf("monitor: render debt: counter: %w", err)
		}
		return RenderDebt{}, errors.New("monitor: render debt: the 'render_pending' row is missing from crawler_counters (migration 000015; manual operations — runbooks §6)")
	}
	var d RenderDebt
	if err := rows.Scan(&d.Flags); err != nil {
		return RenderDebt{}, fmt.Errorf("monitor: render debt: counter: scan: %w", err)
	}
	if err := rows.Err(); err != nil {
		return RenderDebt{}, fmt.Errorf("monitor: render debt: counter: %w", err)
	}
	lag, err := redis.Len(ctx, queue.StreamRender)
	if err != nil {
		return RenderDebt{}, fmt.Errorf("monitor: render debt: the q:render depth: %w", err)
	}
	d.Lag = lag
	return d, nil
}

// --------------------------------------------------------------- metrics

// renderDebtMetrics is the loop instrument: the
// crawler_render_pending_domains gauge (flags). The lag is not
// duplicated — the render-worker collector publishes it
// (crawler_queue_depth{queue="q:render"}, T-077); a nil receiver is a
// no-op (a loop without a registry, the zoneStatsLoopMetrics style).
type renderDebtMetrics struct {
	flags *prometheus.GaugeVec
}

func newRenderDebtMetrics(reg *metrics.Registry) *renderDebtMetrics {
	if reg == nil {
		return nil
	}
	return &renderDebtMetrics{
		flags: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_render_pending_domains",
			Help: "Render debt (T-175): domains with the render_pending bit (the crawler_counters['render_pending'] counter, the bit transitions are maintained by migration 000015). The q:render stream lag — crawler_queue_depth{queue=\"q:render\"}; the debt = the sum. Growth — accumulation (the render_debt alert), triage — docs/runbooks.md.",
		}),
	}
}

func (m *renderDebtMetrics) observe(d RenderDebt) {
	if m == nil {
		return
	}
	m.flags.WithLabelValues().Set(float64(d.Flags))
}

// ------------------------------------------------------------------- loop

// RenderDebtOptions is the loop options; zero values are replaced by
// defaults (withDefaults, the ZoneStatsOptions style).
type RenderDebtOptions struct {
	// Interval is the loop period (DefaultRenderDebtInterval); the
	// working value is monitor.render_debt_interval from the config.
	Interval time.Duration
	// Log is the loop logger; nil — records suppressed (slog.DiscardHandler).
	Log *slog.Logger
	// Metrics is the service registry; nil — no gauge.
	Metrics *metrics.Registry
}

func (o RenderDebtOptions) withDefaults() RenderDebtOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultRenderDebtInterval
	}
	return o
}

// RenderDebtLoop is the periodic render debt loop (T-175): a snapshot
// right at startup (a fresh gauge after a restart) and then by ticker.
// A pass is milliseconds (PK + XLEN), the single-flight mechanics of
// T-161 (resetting an accumulated tick) is not needed — a pass is
// shorter than any interval of the corridor. A source error — Warn, the
// loop keeps living (the OnRun contract, T-012): the next pass will
// retry. The pass summary is Debug (not Info: the loop is healthy
// whenever DB and Redis are alive — a five-minute Info would be
// background noise, the T-175 criterion "the monitor log does not
// flood"; the operator sees the debt in the T-173 load report line and
// the render_debt alert). A ctx cancellation — a clean nil (graceful
// shutdown, spec 3.4).
func RenderDebtLoop(ctx context.Context, src RenderDebtSource, opts RenderDebtOptions) error {
	if src == nil {
		return errors.New("monitor: render debt: the source is not set")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	m := newRenderDebtMetrics(o.Metrics)
	log.Info("render debt loop", "interval", o.Interval.String())

	pass := func() {
		d, err := src.RenderDebt(ctx)
		if err != nil {
			if ctx.Err() == nil {
				log.Warn("render debt pass", "err", err)
			}
			return
		}
		m.observe(d)
		log.Debug("render debt: " + d.String())
	}

	pass()
	ticker := time.NewTicker(o.Interval)
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

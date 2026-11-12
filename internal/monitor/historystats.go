// The Laravel detection history counters (T-118): the cumulative metrics
// crawler_laravel_appeared_total / crawler_laravel_lost_total.
// The events (the rows of the append-only laravel_history journal, written by the analyzer
// on fresh HTTP episodes) increment the crawler_counters rows
// in the same transaction (T-178, analyzer/aggregate.go); the authoritative
// value is periodically rewritten by the combined reconciliation
// (zonestats.go, the laravel_history aggregate) — the hourly seq scan
// of the journal left the loop; the pass reads two PK points (the incremental-counters spec §8.5).
// A gauge with counter semantics is correct for rate()/increase() in
// Prometheus (the values are monotonic; the T-118 decision — the metrics are attributed
// to internal/monitor, not the analyzer registry).
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
)

// DefaultHistoryStatsInterval — the HistoryStatsLoop cycle period: 1h
// (the freshness of the cumulatives between reconciliations; the pass is point reads —
// the interval sets not the price but the frequency of Debug lines).
const DefaultHistoryStatsInterval = time.Hour

// historyStatsSQL — reading both counter rows with one statement:
// FILTER positions instead of two Lookups; a missing row (before the first
// reconciliation) honestly gives 0 — like the empty journal before.
const historyStatsSQL = "SELECT coalesce(max(value) FILTER (WHERE name = 'laravel_appeared_total'), 0), " +
	"coalesce(max(value) FILTER (WHERE name = 'laravel_lost_total'), 0) " +
	"FROM crawler_counters WHERE name IN ('laravel_appeared_total', 'laravel_lost_total')"

// HistoryStatsOptions — the HistoryStatsLoop parameters; zero values are
// replaced by defaults (the NearStatsOptions style).
type HistoryStatsOptions struct {
	// Interval — the cycle period (DefaultHistoryStatsInterval).
	Interval time.Duration
	// Log — the loop logger; nil — entries suppressed (slog.DiscardHandler).
	Log *slog.Logger
}

func (o HistoryStatsOptions) withDefaults() HistoryStatsOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultHistoryStatsInterval
	}
	return o
}

// HistoryStats — the result of one pass: the cumulative transition numbers.
type HistoryStats struct {
	Appeared int64
	Lost     int64
}

// HistoryStatsOnce performs one pass: the appeared/lost numbers by one
// aggregate (the NearStatsOnce style — a db.DB without QueryRow). Idempotent
// (spec 3.4): the journal is append-only; a repeat gives the same values.
func HistoryStatsOnce(ctx context.Context, pool db.DB) (HistoryStats, error) {
	if pool == nil {
		return HistoryStats{}, errors.New("monitor: detection history counters: pool not set")
	}
	rows, err := pool.Query(ctx, historyStatsSQL)
	if err != nil {
		return HistoryStats{}, fmt.Errorf("monitor: detection history counters: %w", err)
	}
	defer rows.Close()
	var st HistoryStats
	if !rows.Next() {
		return HistoryStats{}, fmt.Errorf("monitor: detection history counters: empty aggregate response")
	}
	if err := rows.Scan(&st.Appeared, &st.Lost); err != nil {
		return HistoryStats{}, fmt.Errorf("monitor: detection history counters: scan: %w", err)
	}
	if err := rows.Err(); err != nil {
		return HistoryStats{}, fmt.Errorf("monitor: detection history counters: %w", err)
	}
	return st, nil
}

// NewHistoryGauges creates the crawler_laravel_appeared_total /
// crawler_laravel_lost_total gauge pair in the reg registry (the service label
// is substituted by the factory); nil — nil: a service without a registry does not raise the loop.
// Idempotent (T-076). The names with the _total suffix — counter semantics
// (monotonic values); the gauge type — the values are set by a pass from the DB.
func NewHistoryGauges(reg *metrics.Registry) (appeared, lost *prometheus.GaugeVec) {
	if reg == nil {
		return nil, nil
	}
	appeared = reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_laravel_appeared_total",
		Help: "Cumulative number of laravel_domains row appearances (laravel_history appeared, T-118): a monotonic counter from the append-only journal.",
	})
	lost = reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_laravel_lost_total",
		Help: "Cumulative number of laravel_domains row disappearances (laravel_history NOT appeared, T-118): a monotonic counter from the append-only journal.",
	})
	return appeared, lost
}

// mutator-disable-regexp log\.Info\("Laravel statement/remove
// mutator-disable-regexp log\.Debug\("history statement/remove
// Justification: the service entries of the history loop (the Info start, the Debug result
// of a successful pass) — they do not change the counter behavior, damped per
// docs/mutation-testing.md §4 (logging is not tested).

// HistoryStatsLoop — a periodic loop: HistoryStatsOnce by ticker,
// the values — into the gauge pair. The first pass — right at start (fresh
// values after an idle/restart); a pass error interrupts only that pass
// (a Warn, the loop keeps living — the OnRun contract, T-012); the gauge holds the last
// successful value. A ctx cancellation — a clean nil (graceful shutdown, spec 3.4).
// A nil gauge/pool — no-op (the NearStatsLoop style).
func HistoryStatsLoop(ctx context.Context, pool db.DB, reg *metrics.Registry, opts HistoryStatsOptions) error {
	appearedG, lostG := NewHistoryGauges(reg)
	// The gauge pair is born and dies together: NewHistoryGauges returns
	// (nil, nil) at a nil registry or two live pointers — the conditions
	// appearedG == nil and lostG == nil are inseparable on any input;
	// the twin mutants `false ||` are equivalent (the duplicate is a safety net
	// for future constructor edits).
	// mutator-disable-next-line expression/remove
	if appearedG == nil || lostG == nil || pool == nil {
		return nil
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("Laravel detection history counters loop",
		"interval", o.Interval.String())

	pass := func() {
		st, err := HistoryStatsOnce(ctx, pool)
		switch {
		// The branch body is empty (only a comment): branch/case replaces it
		// with a noop EmptyStmt whose print is identical to the original —
		// a mutant with an empty diff, not killable by a test; suppressing generation is
		// the only alternative to the checksum blacklist (§4).
		// mutator-disable-next-line branch/case
		case err != nil && ctx.Err() != nil:
			// A cancellation during the pass — part of the shutdown, not an alarm.
		case err != nil:
			log.Warn("history counters pass", "err", err)
		default:
			appearedG.WithLabelValues().Set(float64(st.Appeared))
			lostG.WithLabelValues().Set(float64(st.Lost))
			log.Debug("history counters pass",
				"appeared", st.Appeared, "lost", st.Lost)
		}
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

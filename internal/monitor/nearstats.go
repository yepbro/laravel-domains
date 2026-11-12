// The counter of Laravel findings in the threshold gray zone (T-115, from the analysis
// of larasites of 2026-10-19): laravel_findings stores all rule firings
// regardless of the outcome (T-071); findings with a score below the
// analyzer.laravel_min_score threshold — material for calibrating the weights and the
// threshold (T-116/T-117) and an indicator of the rule set quality in production: a stable
// gray zone is the norm (weak signs of frequent non-Laravel stacks);
// a spike is a signal of precision degradation after a rule change.
//
// The metric — the crawler_laravel_near_findings gauge: the current number of findings
// with min_score/2 <= score < min_score (half the threshold — the lower boundary
// of "near-firings": below is the noise of single weak rules, similarly
// to one.log of the old implementation — "hosts with a single sign").
// The command line parsing the same rows — crawlerctl near (T-115):
// the gauge answers "how many", the CLI — "which exactly"; the SQL of both
// sides keeps one range semantics; the query text here is the single
// source of truth about the boundaries for the metric.
//
// The period is an hour, not the 30–60 s of zone recounts (T-078): a pass counts
// the aggregate over laravel_findings (a sequential partition read; on
// the production corpus — tens of millions of rows), and the gray zone is
// calibration material, not an operational signal: an hour of freshness is enough;
// the pass price is like one zonestats pass, but 60× rarer.
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

// DefaultNearStatsInterval — the NearStatsLoop cycle period: 1h
// (the pass price justification — in the package comment).
const DefaultNearStatsInterval = time.Hour

// nearStatsSQL — one pass: the number of gray zone findings. The boundaries —
// parameters (real in SQL, float32 in the table): the threshold is read from
// the configuration at every loop run; a change of analyzer.laravel_min_score
// is picked up by a monitor restart; a parameter, not a literal, because
// the value is the operator's configuration, not the schema code (unlike the
// check_state/status codes in the scheduler passes).
const nearStatsSQL = "SELECT count(*) FROM laravel_findings WHERE score >= $1::real AND score < $2::real"

// NearStatsOptions — the NearStatsLoop parameters; zero values are
// replaced by defaults (withDefaults, the ZoneStatsOptions style).
type NearStatsOptions struct {
	// Interval — the cycle period (DefaultNearStatsInterval).
	Interval time.Duration
	// MinScore — the is_laravel threshold (analyzer.laravel_min_score); the gray
	// zone is [MinScore/2, MinScore). Validated by NearStatsOnce.
	MinScore float64
	// Log — the loop logger; nil — entries suppressed (slog.DiscardHandler).
	Log *slog.Logger
}

func (o NearStatsOptions) withDefaults() NearStatsOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultNearStatsInterval
	}
	return o
}

// NearStatsOnce performs one pass: the number of laravel_findings findings in
// the gray zone [minScore/2, minScore) by one query (the
// analyzer.ZoneIDByName style — a db.DB without QueryRow). Idempotent (spec 3.4):
// a repeat gives the same value at the same table state.
func NearStatsOnce(ctx context.Context, pool db.DB, minScore float64) (int64, error) {
	if pool == nil {
		return 0, errors.New("monitor: gray zone counter: pool not set")
	}
	if !(minScore > 0 && minScore < 1) {
		return 0, fmt.Errorf("monitor: gray zone counter: threshold %v outside (0, 1)", minScore)
	}
	lo, hi := minScore/2, minScore
	rows, err := pool.Query(ctx, nearStatsSQL, float32(lo), float32(hi))
	if err != nil {
		return 0, fmt.Errorf("monitor: gray zone counter: %w", err)
	}
	defer rows.Close()
	var n int64
	if !rows.Next() {
		return 0, fmt.Errorf("monitor: gray zone counter: empty aggregate response")
	}
	if err := rows.Scan(&n); err != nil {
		return 0, fmt.Errorf("monitor: gray zone counter: scan: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("monitor: gray zone counter: %w", err)
	}
	return n, nil
}

// NewNearGauge creates the crawler_laravel_near_findings gauge in the reg registry
// (the service label is substituted by the factory); nil — nil: a service without a
// registry (isolated builds) does not raise the loop (the metrics.NewQueueGauge style).
// Idempotent (T-076).
func NewNearGauge(reg *metrics.Registry) *prometheus.GaugeVec {
	if reg == nil {
		return nil
	}
	return reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_laravel_near_findings",
		Help: "Laravel findings in the threshold gray zone: laravel_findings with min_score/2 <= score < min_score (rule calibration material, T-115).",
	})
}

// mutator-disable-regexp log\.Info\("Laravel statement/remove
// mutator-disable-regexp log\.Debug\("gray statement/remove
// Justification: the service entries of the gray zone loop (the Info start, the Debug result
// of a successful pass) — they do not change the counter behavior, damped per
// docs/mutation-testing.md §4 (logging is not tested).

// NearStatsLoop — a periodic recount loop: NearStatsOnce by ticker,
// the value — into the gauge. The first pass — right at start (a fresh value
// after an idle/restart, the ZoneStatsLoop style); a pass error
// interrupts only that pass (a Warn, the loop keeps living — the OnRun contract, T-012);
// the gauge holds the last successful value. A ctx cancellation — a clean nil
// (graceful shutdown, spec 3.4). A nil gauge/pool — no-op: a run without
// a registry must not crash (the CollectQueueLoop style).
func NearStatsLoop(ctx context.Context, pool db.DB, reg *metrics.Registry, opts NearStatsOptions) error {
	gauge := NewNearGauge(reg)
	if gauge == nil || pool == nil {
		return nil
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("Laravel finding gray zone counter loop",
		"interval", o.Interval.String(), "min_score", o.MinScore)

	pass := func() {
		n, err := NearStatsOnce(ctx, pool, o.MinScore)
		switch {
		// The branch body is empty (only a comment): branch/case replaces it
		// with a noop EmptyStmt whose print is identical to the original —
		// a mutant with an empty diff, not killable by a test; suppressing generation is
		// the only alternative to the checksum blacklist (§4).
		// mutator-disable-next-line branch/case
		case err != nil && ctx.Err() != nil:
			// A cancellation during the pass — part of the shutdown, not an alarm.
		case err != nil:
			log.Warn("gray zone counter pass", "err", err)
		default:
			gauge.WithLabelValues().Set(float64(n))
			log.Debug("gray zone counter pass", "near", n)
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

// Scheduler: backlog depth collector (T-077, spec 5.9): periodic
// reads of zone counter sums → gauge crawler_backlog_domains
// {state=pending|queued} (T-178: the reference table instead of a pass over domains).
//
// Architectural decision (where the background SQL poll lives): in the
// scheduler service — it owns the domain states (moves pending → queued
// in EnqueueBatch T-028, returns queued → pending by lease T-029, and
// done → pending via rechecks T-030/T-031), has a DB pool from the very
// start and is not busy with exchanges; monitor is untouched (its territory is
// counter reconciliation T-178 and alerts T-080). The length of the q:* streams is a
// separate metric, crawler_queue_depth, at the consumers (internal/metrics/
// collect.go); the backlog from the DB is the "true" depth: the Redis queue
// is truncated by MAXLEN and loses tasks on failure (the scheduler restores them,
// spec 3.4), while pending/queued in the DB is the full volume of unfinished work,
// including rows that have not yet been enqueued.
//
// The counters track the current state (gauge) and do not accumulate:
// backlog is a level, not an event; alerts T-080 (progress
// stall, overflow) read the level. deleted are excluded by the base
// counter set (§13.1 — deleted domains are not checked), done is not
// counted: progress over zones is tracked by the domains_checked counters (monitor,
// T-078).
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/model"
)

// DefaultBacklogInterval — poll period: 5 minutes. The original 30 s (the lower
// bound of the spec 5.9 corridor, 30–60 s) were designed for a small corpus: measurement
// §12 docs/load-measurements.md — a GROUP BY pass over 47M rows took 17+ s
// every 30 s, and together with zone recalculation and status aggregates it was most
// of the postgres CPU; T-178 moved the pass to reference-table counter sums
// (milliseconds), the period stayed at 5 minutes: the dashboard depth gauge
// tolerates such freshness (enqueue T-161), and the interval sets the balance
// between freshness and load. The spec corridor remains available via the
// scheduler.backlog_interval config for small corpora.
const DefaultBacklogInterval = 5 * time.Minute

// backlogSQL — backlog depth from the zone reference table: a sum of incremental
// counters (T-178, incremental-counters spec §8.4) — one cheap pass over thousands
// of zone rows instead of a GROUP BY over 47M domains rows (measurement §12 — 17+ s every
// 5 min). The counters keep transition deltas (incremental-counters spec §6) and the monitor
// reconciliation (incremental-counters spec §7); deleted are excluded by the base column set
// (status <> deleted, incremental-counters spec §4.1).
const backlogSQL = "SELECT coalesce(sum(domains_pending), 0), coalesce(sum(domains_queued), 0) FROM zones"

// BacklogOptions — loop parameters; zero values are replaced
// with defaults (withDefaults, the EnqueueOptions style).
type BacklogOptions struct {
	// Interval — period of the BacklogLoop cycle (DefaultBacklogInterval).
	Interval time.Duration
	// Log — loop logger; nil — records suppressed (slog.DiscardHandler).
	Log *slog.Logger
}

func (o BacklogOptions) withDefaults() BacklogOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultBacklogInterval
	}
	return o
}

// BacklogResult — the outcome of a single BacklogOnce pass.
type BacklogResult struct {
	// Pending — domains awaiting scheduler selection (check_state =
	// pending): the entire accumulated volume of the loop's work.
	Pending int64
	// Queued — domains in progress (check_state = queued, lease in effect):
	// enqueued or processed by the stages.
	Queued int64
}

// NewBacklogGauge creates the backlog depth gauge crawler_backlog_domains
// {state} in the reg registry (the service label is substituted by the factory); nil — nil.
// Idempotent (T-076).
func NewBacklogGauge(reg *metrics.Registry) *prometheus.GaugeVec {
	if reg == nil {
		return nil
	}
	return reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_backlog_domains",
		Help: "Глубина backlog: домены в ожидании выборки (pending) и в работе с lease (queued) — суммы счётчиков зон (дельты переходов + реконсиляция, T-178), без удалённых.",
	}, metrics.LabelState)
}

// backlogLoopMetrics — loop instruments (T-161): pass duration
// (observability of "pass longer than interval" without gdb on pg_stat_activity)
// and a skipped-ticks counter; nil sink — no-op (loop without a registry,
// the monitor.collectorMetrics style).
type backlogLoopMetrics struct {
	seconds prometheus.ObserverVec
	skipped *prometheus.CounterVec
}

// newBacklogLoopMetrics — instruments in the reg registry (nil — nil). The pass is
// a read of the zone reference table (T-178): default buckets with headroom.
func newBacklogLoopMetrics(reg *metrics.Registry) *backlogLoopMetrics {
	if reg == nil {
		return nil
	}
	return &backlogLoopMetrics{
		seconds: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_backlog_pass_seconds",
			Help:    "Длительность прохода глубины backlog (суммы счётчиков зон по справочнику, T-077/T-178).",
			Buckets: prometheus.ExponentialBuckets(0.5, 2, 11),
		}),
		skipped: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_backlog_skipped_ticks_total",
			Help: "Пропущенные тики цикла backlog: проход длился дольше интервала, накопившийся тик не запускает новый проход (single-flight, T-161).",
		}),
	}
}

// observe — pass duration recording; nil — no-op.
func (m *backlogLoopMetrics) observe(seconds float64) {
	if m == nil {
		return
	}
	m.seconds.WithLabelValues().Observe(seconds)
}

// skip — tick skipping; nil — no-op.
func (m *backlogLoopMetrics) skip() {
	if m == nil {
		return
	}
	m.skipped.WithLabelValues().Inc()
}

// BacklogOnce — a single pass: zone counter sums (pending/queued,
// T-178) → gauge of both series (empty reference table — zeros: an empty backlog
// must be exported, not disappear until the next non-empty one).
func BacklogOnce(ctx context.Context, pool db.DB, gauge *prometheus.GaugeVec) (BacklogResult, error) {
	if pool == nil {
		return BacklogResult{}, errors.New("scheduler: глубина backlog: пул не задан")
	}
	if gauge == nil {
		return BacklogResult{}, errors.New("scheduler: глубина backlog: gauge не задан")
	}

	rows, err := pool.Query(ctx, backlogSQL)
	if err != nil {
		return BacklogResult{}, fmt.Errorf("scheduler: глубина backlog: %w", err)
	}
	defer rows.Close()

	var out BacklogResult
	if !rows.Next() {
		return BacklogResult{}, fmt.Errorf("scheduler: глубина backlog: пустой ответ агрегата")
	}
	if err := rows.Scan(&out.Pending, &out.Queued); err != nil {
		return BacklogResult{}, fmt.Errorf("scheduler: глубина backlog: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return BacklogResult{}, fmt.Errorf("scheduler: глубина backlog: %w", err)
	}

	gauge.WithLabelValues(model.CheckStatePending.String()).Set(float64(out.Pending))
	gauge.WithLabelValues(model.CheckStateQueued.String()).Set(float64(out.Queued))
	return out, nil
}

// BacklogLoop — a periodic loop of passes: the first one runs immediately at start
// (fresh depth after an idle period/restart), then by ticker. A pass
// error interrupts only that pass: a Warn is logged, the loop keeps living (the
// OnRun contract, T-012) — the next pass will retry; meanwhile the gauge returns
// the last known values. ctx cancellation — a clean nil (graceful
// shutdown, spec 3.4). nil registry/pool — a start error: a collector without
// write points is meaningless (the NewRecorder T-058 style).
//
// Single-flight (T-161): passes run in the loop's own goroutine —
// a new pass structurally cannot start before the previous one
// has finished. The tail of the "pass longer than interval" picture (measurement §12: 17+ s
// over 47M rows): the ticker buffers one tick, and an accumulated tick
// would start a new pass right after the slow one — the aggregate would hammer the DB
// continuously. Therefore a pass longer than the interval drops the accumulated
// tick (the next pass runs on the next tick: passes no more often than
// the interval), while Warn and the crawler_backlog_skipped_total counter raise
// the noise.
func BacklogLoop(ctx context.Context, pool db.DB, reg *metrics.Registry, opts BacklogOptions) error {
	gauge := NewBacklogGauge(reg)
	if pool == nil {
		return errors.New("scheduler: цикл backlog: пул не задан")
	}
	if gauge == nil {
		return errors.New("scheduler: цикл backlog: реестр метрик не задан")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	m := newBacklogLoopMetrics(reg)
	// Logging mutant (T-M06): the startup Info does not change the observable
	// behavior of the loop — suppressed per §4 docs/mutation-testing.md
	// (logging is not tested).
	// mutator-disable-next-line statement/remove
	log.Info("цикл глубины backlog", "interval", o.Interval.String())

	// Ticker before the starting pass: ticks are counted from the loop start,
	// so the accumulated-tick drain inside pass sees the same ticker.
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	pass := func() {
		started := time.Now()
		res, err := BacklogOnce(ctx, pool, gauge)
		switch {
		// The branch body is empty (comment only): branch/case replaces it
		// with a noop-EmptyStmt with identical printing — a mutant with an empty
		// diff cannot be killed by a test; suppressing generation is the only
		// alternative to the checksum blacklist (§4 docs/mutation-testing.md;
		// precedent — monitor/zonestats.go, T-M01).
		// mutator-disable-next-line branch/case
		case err != nil && ctx.Err() != nil:
			// Cancellation during a pass is part of the stop, not an alarm.
		case err != nil:
			log.Warn("проход глубины backlog", "err", err)
		default:
			log.Debug("проход глубины backlog",
				"pending", res.Pending, "queued", res.Queued)
		}
		took := time.Since(started)
		m.observe(took.Seconds())
		if took > o.Interval {
			// Single-flight: drop the tick accumulated during the slow
			// pass — otherwise a new pass would start right after this one
			// (select/default — the tick may be absent: the Warn is honest
			// about duration, the drain is a no-op in that case).
			select {
			case <-ticker.C:
			default:
			}
			log.Warn("проход глубины backlog дольше интервала — накопившийся тик пропущен",
				"took", took.String(), "interval", o.Interval.String())
			m.skip()
		}
	}

	pass()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pass()
		}
	}
}

// The scheduler service (spec 5.3, 4.2): the main loop of enqueueing
// domains for checking — selecting pending batches with zone interleaving
// (internal/scheduler DispatchBatch, T-027), moving them to queued with a
// lease and publishing DNSBatch batches into q:dns
// (EnqueueBatch/EnqueueLoop, T-028) — and the background loops in the same
// OnRun: the daily recheck of done domains (RecheckDaily, T-030), the
// rechecks of temp_error (RetryTempErrors, T-031), the refund of domains
// with an expired lease (LeaseWatch, T-029) and the compensator of the
// "status hung" new∧done — an early recheck of those that lost q:http
// (StaleRequeueLoop, T-270). The publishing — under the backpressure gate
// (WatermarkPublisher, T-026). The queue is Redis Streams (internal/queue,
// the queue.NewClient/RedisQueue client); the framework is
// internal/service: migrations, the DB pool (OnDBPool), the /livez +
// /healthz probes, /metrics, graceful shutdown (spec 3.4).
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/scheduler"
	"crawler/internal/service"
)

func main() {
	// A configuration error — before the logger is initialized: stderr and exit code 1.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[scheduler] configuration: %v", err)
	}
	logger := service.NewLogger("scheduler", cfg)

	// The metrics registry is created here (T-077): the collector of the
	// backlog depth (run below) needs the instruments before OnRun starts,
	// the framework serves the same registry via /metrics
	// (Options.Metrics).
	reg := metrics.New("scheduler")

	// The pool comes from the framework after the migrations (OnDBPool)
	// and is guaranteed to be ready before OnRun starts: postgres.dsn is
	// required (config.Validate).
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "scheduler",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9102"),
		Metrics:     reg,
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		// The composite AfterConnect hook of the pool (db.JoinHooks,
		// T-192): the aggregate backlog collector (T-077) does not tolerate
		// parallel workers (T-171) — the enqueue path is the index scans of
		// domains_pending_idx without parallel plans, a session SET does
		// not slow it down, and the seq passes (backlog, retry temp_error)
		// stop taking all the cores away from the pipeline; the scheduler
		// transactions take the row locks of domains (enqueue, the lease
		// refund) — the cap of lock_timeout 30 s (T-192) truncates the lock
		// convoy (the 25.10 incident: waits up to 39 min) by a rollback and
		// a graceful retry of the enqueue path.
		DBAfterConnect: db.JoinHooks(db.NoParallelGather, db.LockTimeout(db.DefaultLockTimeout)),
		OnRun:          func(ctx context.Context) error { return run(ctx, cfg, logger, pool, reg) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — the working logic of scheduler: the main loop of enqueueing
// batches into q:dns and the background recheck loops (T-030/T-031) by
// goroutines in the same OnRun — they return the domains to pending, the
// main loop will pick them up with the next batch; next to them — the
// collector of the backlog depth (T-077, spec 5.9): a gauge of
// pending/queued from the DB, it lives with the owner of the domains
// states, — the loop of the release of the confirmed stream prefix
// (queue.TrimLoop, T-141: XACK does not remove the records from the
// streams, XTRIM MINID by the oldest unacknowledged record of the group
// keeps the history from growing to the MAXLEN cap and the Redis budget,
// spec 3.2; the scheduler is the single owner of the queue streams, the
// loop is cheap: three commands per stream once per redis.trim_interval)
// and the consumer hygiene loop of the groups (queue.CleanupConsumersLoop,
// T-155: after worker restarts their pid consumers stay in the groups
// forever; strictly empty (pending = 0) and inactive longer than
// redis.consumer_idle ones are removed with XGROUP DELCONSUMER — the same
// stream janitors at the same trim_interval pace).
// Shutdown — ctx cancellation (graceful shutdown, spec 3.4): the loop
// finishes the current batch, the background ones — the current pass, all
// return nil.
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, reg *metrics.Registry) error {
	if pool == nil {
		return errors.New("scheduler: the DB pool is not provided (postgres.dsn is required)")
	}

	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	// Publishing the batches — under the backpressure gate (T-026): the
	// q:dns depth below scheduler.queue_high_watermark; MAXLEN/reliability
	// — T-025 in RedisQueue.Publish.
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(cfg.Scheduler.QueueHighWatermark),
		MaxLen:    int64(cfg.Redis.MaxLen), // the trim limits — for the gate threshold (T-026/T-025)
		Log:       logger,
	})
	dnsq := queue.NewWatermarkPublisher(queue.NewRedisQueue(rdb, queue.RedisOptions{
		MaxLen: int64(cfg.Redis.MaxLen), // XADD MAXLEN ~ on every stream (T-025)
	}), gate)

	// The zone weights of the selection (spec 5.3: "optionally in the
	// config", T-106): the names from scheduler.zone_weights are resolved
	// into zone_id by the reference table once at startup — like the rest
	// of the config, the weights are not changed on the fly. A name may be
	// missing from the reference table (ingest creates the zone with the
	// first file, spec 5.1) — a Warn: the zone is checked with weight 1
	// until a restart with the zone present. The inactive zones are
	// resolved too: their weight takes effect when the zone is enabled
	// (crawlerctl zones enable, T-084) without a restart.
	zoneWeights, unknownZones, err := scheduler.ResolveZoneWeights(ctx, pool, cfg.Scheduler.ZoneWeights)
	if err != nil {
		return err
	}
	if len(unknownZones) > 0 {
		logger.Warn("zone weights: not in the zones reference table (ingest creates it with the first file) — checked with weight 1 until a restart with the zone present",
			"zones", strings.Join(unknownZones, ", "))
	}

	bg := make(chan error, 7)
	go func() {
		bg <- scheduler.RecheckDaily(ctx, pool, scheduler.RecheckOptions{
			Interval: cfg.Scheduler.RecheckInterval.D(),
			Log:      logger,
		})
	}()
	go func() {
		bg <- scheduler.RetryTempErrors(ctx, pool, scheduler.RetryOptions{
			RetryAfter: cfg.Scheduler.TempErrorRetryAfter.D(),
			MaxRetries: cfg.Scheduler.TempErrorMaxRetries,
			Metrics:    reg, // the counter of the 40P01 retries of the decision transactions (T-268)
			Log:        logger,
		})
	}()
	go func() {
		bg <- scheduler.LeaseWatch(ctx, pool, scheduler.LeaseOptions{
			Timeout: cfg.Scheduler.Lease.D(),
			Log:     logger,
			// The counter of the repair pass of queued/NULL queued_at
			// (T-255, FR-1.2): a non-zero steady state is a signal to the
			// operator.
			Metrics: reg,
		})
	}()
	go func() {
		// The "status hung" compensator (T-270, FR-2.2): an hourly pass
		// returns new∧done older than 24 h (a lost q:http task) to
		// pending; a guard by the new_done_stale row — steady state is one
		// PK lookup per tick, the counter
		// crawler_scheduler_stale_requeued_total is the signal of new
		// losses.
		bg <- scheduler.StaleRequeueLoop(ctx, pool, scheduler.StaleRequeueOptions{
			Log:     logger,
			Metrics: reg,
		})
	}()
	go func() {
		// The period — scheduler.backlog_interval (T-161: the default
		// 5 min, the §12 measurement; the 30 s of the spec 5.9 corridor
		// are available via the config).
		bg <- scheduler.BacklogLoop(ctx, pool, reg, scheduler.BacklogOptions{
			Interval: cfg.Scheduler.BacklogInterval.D(),
			Log:      logger,
		})
	}()
	// The release of the confirmed stream prefix (T-141, spec 4.2
	// "Supplement 2026-10-23"): XTRIM MINID by the oldest record that must
	// stay — next to the other janitor loops of the scheduler.
	go func() {
		bg <- queue.TrimLoop(ctx, rdb, queue.TrimOptions{
			Interval: cfg.Redis.TrimInterval.D(),
			MaxLen:   int64(cfg.Redis.MaxLen), // the limit for the stuck prefix signal
			Log:      logger,
		})
	}()
	// The consumer hygiene of the groups (T-155): the strictly empty
	// (pending = 0 — DELCONSUMER with pending loses the PEL forever) and
	// inactive longer than redis.consumer_idle pid consumers of the dead
	// ones after worker restarts are purged at the same janitor pace as
	// the prefix release (redis.trim_interval).
	go func() {
		bg <- queue.CleanupConsumersLoop(ctx, rdb, queue.ConsumerOptions{
			Interval: cfg.Redis.TrimInterval.D(),
			Idle:     cfg.Redis.ConsumerIdle.D(),
			Log:      logger,
		})
	}()

	// The main loop lives until the ctx cancellation; after it the
	// background loops already finish on the same cancellation — we
	// collect their results.
	err = scheduler.EnqueueLoop(ctx, pool, dnsq, scheduler.EnqueueOptions{
		BatchSize:   cfg.Scheduler.BatchSize,
		ZoneWeights: zoneWeights,
		Log:         logger,
	})
	<-bg
	<-bg
	<-bg
	<-bg
	<-bg
	<-bg
	<-bg
	return err
}

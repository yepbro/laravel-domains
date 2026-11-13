// The dashboard service — the operator web dashboard of the pipeline
// (dashboard spec, stage 15): a single page of the pipeline state with
// password access (HTTP Basic, dashboard spec §7), strictly read-only
// (dashboard spec §1.3). The framework is internal/service (migrations,
// the DB pool via OnDBPool, the probes and /metrics on loopback
// 127.0.0.1:9108 — dashboard spec §4.2, the next one after web :9107);
// the public listener dashboard.listen — its own http.Server inside OnRun
// with a graceful stop on ctx cancellation (the cmd/web precedent,
// T-121). Next to the listener — the background snapshot building loop
// (dashboard spec §4.4, T-129): the dashboard.refresh period, an atomic
// replacement, /api/status serves the latest ready snapshot. The front is
// internal/dashboard/assets (T-130), the deployment — T-131.
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
	"crawler/internal/dashboard"
	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/service"
)

func main() {
	// A configuration error — before the logger is initialized: stderr and exit code 1.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[dashboard] configuration: %v", err)
	}
	logger := service.NewLogger("dashboard", cfg)
	// Dashboard spec §7.7: the operator credentials are required — empty
	// username/password_hash (or an unparsable hash) reject the startup
	// BEFORE the framework starts: the migrations and the listeners are
	// not brought up. The common configValidate deliberately does not
	// check them — otherwise the requirement would affect all the services
	// of the project.
	if _, err := dashboard.ParseCredentials(cfg); err != nil {
		logger.Error("dashboard credentials", "err", err)
		os.Exit(1)
	}

	// Metrics are loopback (dashboard spec §4.2: the public entry point is
	// one — dashboard.listen); the counters/histogram of the snapshot
	// building (dashboard spec §10) are registered before OnRun starts,
	// the framework serves the same registry via /metrics (the
	// cmd/monitor precedent). The pool comes from the framework after the
	// migrations (OnDBPool): postgres.dsn is required (config.Validate),
	// the readiness /healthz of the public mux pings it.
	reg := metrics.New("dashboard")
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "dashboard",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", "127.0.0.1:9108"),
		Metrics:     reg,
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		// The snapshot building — per-second aggregates over domains (T-171;
		// the heavy sections are additionally TTL-cached, T-161): without
		// parallel workers the passes do not take all the cores away from
		// the pipeline; the snapshot freshness is sufficient for the
		// operator (the frequency — dashboard.refresh/TTL).
		DBAfterConnect: db.NoParallelGather,
		OnRun:          func(ctx context.Context) error { return run(ctx, cfg, logger, pool, reg) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — the public listener and the snapshot building loop inside OnRun:
// the server is in the foreground, the collector — as a background loop
// (the cmd/monitor style: an early stop of one stops the other by the
// derived ctx cancellation); the completion is returned to the framework.
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, reg *metrics.Registry) error {
	if pool == nil {
		return errors.New("dashboard: the DB pool is not provided (postgres.dsn is required)")
	}

	// The stream depths — via the same narrow API as monitor and
	// crawlerctl status (dashboard spec §4.3.2): a Redis client by the
	// redis section of the configuration, a gate without a threshold —
	// the collector needs only Len.
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{Log: logger})

	coll, err := dashboard.NewCollector(pool, gate, cfg.Dashboard, dashboard.CollectorOptions{
		Log:            logger,
		Metrics:        reg,
		QueueWatermark: int64(cfg.Scheduler.QueueHighWatermark), // queue highlighting, §5.4
		QueueMaxLen:    int64(cfg.Redis.MaxLen),                 // the effective threshold: a clamp by MAXLEN, as by the gate and the monitor
		RenderMetrics:  cfg.Dashboard.RenderMetrics,             // the engine panel (T-246): the /metrics exposures of the render workers
	})
	if err != nil {
		return err
	}
	srv, err := dashboard.New(pool, cfg, dashboard.Options{
		Log:       logger,
		Snapshots: coll, // /api/status serves the latest ready snapshot
	})
	if err != nil {
		return err
	}

	// Derived cancellation: an early exit of its own loop stops the
	// background one too — otherwise waiting for the result would hang
	// forever (the cmd/monitor style).
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	bg := make(chan error, 1)
	go func() { bg <- coll.Loop(ctx) }()

	err = srv.Run(ctx, cfg.Dashboard.Listen)
	cancel()
	if bgErr := <-bg; err == nil {
		err = bgErr
	}
	return err
}

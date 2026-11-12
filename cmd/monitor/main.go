// The monitor service (spec 5.9): the background loop of the
// reconciliation of the incremental counters of the pipeline
// (internal/monitor.ZoneStatsLoop, T-178: one combined pass — the zone
// columns + the global crawler_counters scalars — once per
// monitor.zone_stats_interval, the only full domains pass in the steady
// state) and the evaluation of the alerts of the minimal set of spec 5.9
// (internal/monitor.AlertsLoop, T-080): the share of DNS errors, the
// queue overflow, the progress standstill, the free space of the content
// directory, the NVMe wear (SMART), a spike of the junk content_hash of
// the ingest cleanup (T-099, a /metrics probe), OOM and the restarts of
// the crawler units (T-145, systemctl + cgroup v2), the untriaged q:dead
// and the Redis memory (T-154, XLEN + INFO memory); the missing
// thresholds of T-162 — the absolute disk minimum (Warn/escalation), the
// MemoryHigh throttling, a frozen queue (last-delivered-id), the PSI CPU;
// the accumulation of render debt — T-175 (the render_pending flags by
// the counter + the q:render lag); the render fallback queue — T-246
// (the growth of the q:render-fallback depth + the share budget events
// from the /metrics probe of the render worker); the alert transitions
// are delivered to Telegram (TelegramNotifier → telegram-notify.sh
// --message). Two more loops — the system load report (T-173) and the
// slow PostgreSQL queries (T-177: pg_stat_activity → Telegram by the same
// delivery), plus the loop of the render debt gauge (T-175) and the
// cumulatives of the appeared/lost detection history from the counter
// rows (T-118/T-178).
// The framework is internal/service: migrations, the DB pool (OnDBPool),
// the /livez + /healthz probes, /metrics, graceful shutdown (spec 3.4).
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/monitor"
	"crawler/internal/queue"
	"crawler/internal/service"
)

func main() {
	// A configuration error — before the logger is initialized: stderr and exit code 1.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[monitor] конфигурация: %v", err)
	}
	logger := service.NewLogger("monitor", cfg)
	// The monitoring address — monitor.listen from the configuration
	// (spec 5.10): it takes precedence over METRICS_ADDR —
	// docs/services.md (the priority was brought to the documentation;
	// before, the variable erroneously beat the config). METRICS_ADDR —
	// the guard for an empty monitor.listen (in practice it is not empty:
	// Default/Validate); the metric addresses of the other services are
	// set only by the variable (spec 5.9).
	//
	// The pool comes from the framework after the migrations (OnDBPool)
	// and is guaranteed to be ready before OnRun starts: postgres.dsn is
	// required (config.Validate).
	listen := cfg.Monitor.Listen
	if listen == "" {
		listen = service.EnvOr("METRICS_ADDR", ":9100")
	}
	// The metrics registry (T-115): the gauge of the Laravel near-find
	// grey zone (crawler_laravel_near_findings) is registered before OnRun
	// starts, the framework serves the same registry via /metrics (the
	// cmd/analyzer style).
	reg := metrics.New("monitor")
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "monitor",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: listen,
		Metrics:     reg,
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		// The recomputation of the zone counters and the alert windows —
		// the aggregates over the whole domains (T-171): without parallel
		// workers the pass does not take all the cores away from the
		// pipeline; a freshness of the counters in tens of seconds is
		// sufficient (the threshold of the progress standstill alert —
		// 5 recomputation periods).
		DBAfterConnect: db.NoParallelGather,
		OnRun:          func(ctx context.Context) error { return run(ctx, cfg, logger, pool, reg) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — the working logic of monitor: the recomputation of the zone
// counters (spec 5.9, T-078) by a background loop and the alert
// evaluation (spec 5.9, T-080) by the foreground loop — the progress
// standstill alert reads zones.updated_at, which is written exactly by
// the background loop. Both finish on the ctx cancellation (graceful
// shutdown, spec 3.4); after its own loop we collect the result of the
// background one (the cmd/scheduler style).
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, reg *metrics.Registry) error {
	if pool == nil {
		return errors.New("monitor: пул БД не получен (postgres.dsn обязателен)")
	}

	// The depth of the streams for the queue alert — via BackpressureGate
	// (T-026): the same narrow API over go-redis as by the scheduler and
	// crawlerctl status (T-079); the gate itself waits for nothing — only
	// Len.
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(cfg.Scheduler.QueueHighWatermark),
		Log:       logger,
	})

	// Derived cancellation: an early exit of its own loop stops the
	// background one too — otherwise <-bg would wait forever. The
	// cancellation executes BEFORE the waiting (the defer remains the
	// guard for the panic path): if AlertsLoop ever learns to return an
	// error without cancelling the parent ctx, a hang will not happen.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The background loops: the reconciliation of the incremental
	// counters (T-178; the period — monitor.zone_stats_interval, the only
	// full domains pass in the steady state; the registry — the pass
	// duration, the skipped ticks and the crawler_counters_drift drift
	// gauge), the gauge of the Laravel near-find grey zone (T-115) and the
	// cumulatives of the appeared/lost detection history (T-118; the
	// counter rows — T-178). The fourth loop — the system load report
	// (T-173), the fifth — the slow PostgreSQL queries (T-177): both hook
	// in below, after the Telegram delivery is built. The sixth — the
	// render debt (T-175): the gauge crawler_render_pending_domains and a
	// summary into the log (the flag counter from the DB + the q:render
	// lag by the same gate).
	bg := make(chan error, 6)
	go func() {
		bg <- monitor.ZoneStatsLoop(ctx, pool, reg, monitor.ZoneStatsOptions{
			Interval: cfg.Monitor.ZoneStatsInterval.D(),
			Log:      logger,
		})
	}()
	go func() {
		bg <- monitor.NearStatsLoop(ctx, pool, reg, monitor.NearStatsOptions{
			MinScore: cfg.Analyzer.LaravelMinScore,
			Log:      logger,
		})
	}()
	go func() {
		bg <- monitor.HistoryStatsLoop(ctx, pool, reg, monitor.HistoryStatsOptions{
			Log: logger,
		})
	}()

	// The render debt (T-175): once per monitor.render_debt_interval — a
	// snapshot of the debt (the crawler_counters counter via the pool +
	// the q:render lag by the same gate as the queue alerts) into the
	// gauge crawler_render_pending_domains and a summary line into the
	// log; the accumulation alert and the load report line read the same
	// source.
	go func() {
		bg <- monitor.RenderDebtLoop(ctx, monitor.RenderDebtReader{Pool: pool, Redis: gate},
			monitor.RenderDebtOptions{
				Interval: cfg.Monitor.RenderDebtInterval.D(),
				Log:      logger,
				Metrics:  reg,
			})
	}()

	// The alerts period — monitor.alerts_interval (T-206): the loop is
	// decoupled from the reconciliation (zone_stats_interval, since T-199
	// — 30 min), the default 5 min — the former frequency, the detection
	// windows in the passes returned to the former minutes. The progress
	// standstill threshold is counted in the RECONCILIATION periods (the
	// ZoneStatsInterval of the options below): ZoneStatsLoop writes
	// zones.updated_at — a threshold of 5 × 5 min with a 30-min label
	// write would fire forever.
	//
	// The q:dead depth (T-154) — via the same gate wrapped by
	// NewDeadDepthMetrics: Len(q:dead) is mirrored into
	// crawler_queue_depth{queue="q:dead"} (monitor — the designated
	// observer of the quarantine, it has no consumers), the working
	// streams go in transit — the workers publish their depth themselves
	// (T-077).
	//
	// The source of the junk content_hash (T-099) — a probe of the
	// /metrics exposure of the ingest service: the counter grows during
	// ingest, the monitor reads it by the same address Prometheus
	// scrapes. An empty address — the alert "not measured" (silently),
	// not a Warn by the probe on every pass.
	deps := monitor.AlertsDeps{Pool: pool, Redis: monitor.NewDeadDepthMetrics(reg, gate)}
	if cfg.Monitor.IngestMetricsURL != "" {
		deps.BadHashes = monitor.IngestBadHashes{URL: cfg.Monitor.IngestMetricsURL}
	}
	// The source of the share budget events of the fallback (T-246): a
	// probe of the /metrics exposure of the render worker (the counter
	// suspended{reason="budget"} grows during the worker, the monitor
	// reads it by the same address Prometheus scrapes). An empty address —
	// the alert "not measured" silently (not a Warn by the probe on every
	// pass), the ingest-probe style. The same probe feeds the browser
	// error alert (S-P2-6): one address — two independent reads, the
	// alerts loop period absorbs the cost.
	if cfg.Monitor.RenderMetricsURL != "" {
		probe := monitor.RenderSuspends{URL: cfg.Monitor.RenderMetricsURL}
		deps.FallbackBudget = probe
		deps.RenderBrowserErrors = probe
	}
	// The unit events (T-145): systemctl show + cgroup v2 memory.events
	// (unitevents.go), wrapped into metrics — one read per pass feeds the
	// unit_oom and memory_high alerts (T-162: the growth of the high
	// counter) and the gauges
	// crawler_unit_oom_events/crawler_unit_memory_high_events/
	// crawler_unit_restarts. In a dev container without systemd the
	// source degrades silently (as SMART).
	deps.UnitEvents = monitor.NewUnitEventsMetrics(reg, monitor.OSUnitEvents{})
	// The Redis memory (T-154): INFO memory of the same client as the
	// gate, wrapped into metrics — one read feeds the redis_memory alert
	// and the gauges crawler_redis_memory_ratio/_bytes.
	deps.RedisMemory = monitor.NewRedisMemoryMetrics(reg, monitor.RedisInfo{RDB: rdb})
	// The Redis uptime (T-270): INFO server of the same client — the
	// redis_restart alert (a restart without persistence loses the
	// undelivered stream records: the main cause of the "status hung"
	// loss class, FR-2.2).
	deps.RedisUptime = monitor.RedisInfo{RDB: rdb}
	// The stream groups for the frozen queue alert (T-162): XINFO GROUPS
	// + XLEN of the same client (the last-delivered-id and the group depth
	// with the T-168 clamp); a stream without groups — an empty
	// contribution.
	deps.StreamGroups = monitor.RedisStreamGroups{RDB: rdb}
	// The PSI CPU (T-162): /proc/pressure/cpu of the host, wrapped into
	// metrics — the gauge crawler_psi_cpu_some_percent and the psi_cpu
	// alert by one read; a kernel without PSI degrades silently.
	deps.PSI = monitor.NewPSIMetrics(reg, monitor.OSPSI{})
	// The delivery of the alerts to Telegram (T-162): the state
	// transitions go by exec-ing telegram-notify.sh --message; the
	// secrets are taken from the environment of the unit (EnvironmentFile
	// /etc/crawler/crawler.env), no secrets or script — the delivery is
	// off, the alerts stay Warn in the log.
	telegram := monitor.NewTelegramNotifier(monitor.TelegramOptions{
		Enabled: cfg.Monitor.Alerts.Telegram,
		Script:  cfg.Monitor.Alerts.TelegramScript,
		Log:     logger,
	})

	// The system load report to the operator (T-173): once per
	// monitor.load_report_interval — a snapshot of /proc (the system + a
	// "htop"-style top of the processes), the space of the content
	// directory and the queue depths — the group consumption backlogs,
	// the same common calculation as crawlerctl status (queue.GroupDepth,
	// T-188), — with the consumption paces: into Telegram by the same
	// delivery as the alerts (no secrets/script — only the log and the
	// crawler_os_* gauges).
	// A period of 0 — the loop is off (LoadReportLoop exits silently).
	go func() {
		bg <- monitor.LoadReportLoop(ctx, monitor.OSLoad{}, monitor.LoadReportOptions{
			Interval:   cfg.Monitor.LoadReportInterval.D(),
			Log:        logger,
			Metrics:    reg,
			Notify:     telegram.Send,
			Queues:     monitor.RedisQueues{RDB: rdb},
			RenderDebt: monitor.RenderDebtReader{Pool: pool, Redis: gate},
			Space:      monitor.OSStatfs{},
			Dirs:       []string{cfg.Content.Dir},
		})
	}()

	// The slow PostgreSQL queries (T-177): the loop polls pg_stat_activity
	// — the client statements running longer than
	// monitor.slow_query_min_duration go to Telegram by the same delivery
	// as the alerts and the load report (a debounce by pid+query_start;
	// the reconciliation aggregate of monitor — an UPDATE over 47 million
	// rows every 30 min (T-199), the §12 measurement — excluded by the
	// application_name of the pool). The full post-mortem trail — the
	// server log (log_min_duration_statement, deploy/postgres). 0 — the
	// loop is off (SlowQueryLoop exits silently). The Telegram delivery —
	// only with monitor.slow_query_telegram (off by default: the
	// diagnostics are already visible in the monitor log, the metrics and
	// the server log); without the flag Notify is nil — the loop is bound
	// to Warn into the log and the metrics.
	var slowQueryNotify func(string)
	if cfg.Monitor.SlowQueryTelegram {
		slowQueryNotify = telegram.Send
	}
	go func() {
		bg <- monitor.SlowQueryLoop(ctx, monitor.PGSlowQueries{
			Pool:       pool,
			ExcludeApp: "monitor",
		}, monitor.SlowQueryOptions{
			Interval:    cfg.Monitor.SlowQueryInterval.D(),
			MinDuration: cfg.Monitor.SlowQueryMinDuration.D(),
			Log:         logger,
			Metrics:     reg,
			Notify:      slowQueryNotify,
		})
	}()

	err := monitor.AlertsLoop(ctx, deps, monitor.AlertsOptions{
		Interval:          cfg.Monitor.AlertsInterval.D(),
		ZoneStatsInterval: cfg.Monitor.ZoneStatsInterval.D(),
		QueueWatermark:    int64(cfg.Scheduler.QueueHighWatermark),
		QueueMaxLen:       int64(cfg.Redis.MaxLen),
		ContentDir:        cfg.Content.Dir,
		DiskFreePercent:   cfg.Content.DiskFreePercent,
		DiskFreeMinBytes:  cfg.Monitor.Alerts.DiskFreeMinBytes.B(),
		DiskFreeCritBytes: cfg.Monitor.Alerts.DiskFreeCritBytes.B(),
		QueueFreeze:       cfg.Monitor.Alerts.QueueFreeze.D(),
		PSICPUPercent:     cfg.Monitor.Alerts.PSICPUPercent,
		PSIWindow:         cfg.Monitor.Alerts.PSIWindow.D(),
		MemHighMinPasses:  cfg.Monitor.Alerts.MemHighPasses,
		RenderDebtDomains: cfg.Monitor.Alerts.RenderDebtDomains,
		RenderDebtPasses:  cfg.Monitor.Alerts.RenderDebtPasses,
		Log:               logger,
		OnPass:            telegram.OnPass,
	})
	// Its own loop has finished — we cancel the background ones before
	// waiting for the results (the cancellation is idempotent even with
	// the already stopped loops). We do not hide their failure behind a
	// nil of the alerts — we collect the results.
	cancel()
	for i := 0; i < 6; i++ {
		bgErr := <-bg
		if err == nil {
			err = bgErr
		}
	}
	return err
}

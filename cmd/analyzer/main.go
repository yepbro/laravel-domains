// The analyzer service (spec 5.8, §7; T-071): the Laravel analysis stage.
// It consumes the q:analyze tasks (Result — the key of the
// subdomain_results row, T-024; published by the HTTP stage T-058,
// render-worker T-067 and the reanalysis T-074) with a consumer group;
// the input of the passive analysis is read from the DB (the
// headers/cookies jsonb, content_hash) and the content store (HTML/DOM,
// spec 5.7), the rules are applied from the current snapshot of the rule
// set (Reloader, T-068: SIGHUP reloads the analyzer.rules_dir directory
// without a restart — the OnSIGHUP hook of the framework), the hit rules
// are written to laravel_findings (all of them, regardless of the
// threshold; is_laravel = score ≥ threshold, §13.1) in one transaction
// with the subdomain_results.rules_version stamp; the q:analyze
// acknowledgment — by the Processing pattern (XACK only after the DB
// write, T-025). The background reanalysis loop (T-074) enqueues the
// records with an outdated rules_version into q:analyze without a
// re-download. The framework is internal/service: migrations, the DB pool
// (OnDBPool), the /livez + /healthz probes, /metrics, graceful shutdown.
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/analyzer"
	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/rules"
	"crawler/internal/service"
)

func main() {
	// A configuration error — before the logger is initialized: stderr and exit code 1.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[analyzer] конфигурация: %v", err)
	}
	logger := service.NewLogger("analyzer", cfg)

	// The rule set is required already at startup (T-068): an analyzer
	// without rules would silently not find Laravel — a load error stops
	// the service (exit code 1, systemd Restart=on-failure). The snapshot
	// for each delivery is taken by the worker via Set(); SIGHUP reloads
	// the directory on the live service (HandleSIGHUP — the OnSIGHUP hook
	// of the framework, make rules-reload).
	reloader, err := rules.NewReloader(cfg.Analyzer.RulesDir, logger)
	if err != nil {
		log.Fatalf("[analyzer] правила %s: %v", cfg.Analyzer.RulesDir, err)
	}

	// The pool comes from the framework after the migrations (OnDBPool)
	// and is guaranteed to be ready before OnRun starts: postgres.dsn is
	// required (config.Validate); it is used both by the content store
	// (the contents rows, T-042) and by reading subdomain_results/writing
	// findings. The metrics registry — right here (T-077): the stage
	// instruments and the collector of the q:analyze depth receive it
	// before OnRun starts, the framework serves the same registry via
	// /metrics.
	reg := metrics.New("analyzer")
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "analyzer",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9106"),
		Metrics:     reg,
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		OnRun:       func(ctx context.Context) error { return run(ctx, cfg, logger, pool, reloader, reg) },
		OnSIGHUP:    reloader.HandleSIGHUP,
	}); err != nil {
		os.Exit(1)
	}
}

// run — the working logic of analyzer: the content store (reading — the
// same PackStore that the HTTP/render stages write with: the zstd
// dictionary T-043 must match, otherwise unpacking foreign blocks is
// impossible) → queue → the q:analyze worker (the foreground) with the
// stage instruments (T-077) and the collector of the q:analyze depth
// (the queue depth metric, spec 5.9) + the background reanalysis loop
// (T-074). Shutdown — ctx cancellation (graceful shutdown, spec 3.4):
// the started deliveries finish or are refunded to the queue for
// redelivery (Processing, T-025); ReanalyzeLoop on cancellation returns
// a clean nil; the background loops (ReanalyzeLoop, the q:analyze
// collector) are stopped by the derived cancellation and joined before
// the defer-closes of store/rdb (T-232).
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, reloader *rules.Reloader, reg *metrics.Registry) error {
	if pool == nil {
		return errors.New("analyzer: пул БД не получен (postgres.dsn обязателен)")
	}

	// The content store (spec 5.7, T-042): the stage only reads (the
	// content for matching the html/meta targets), the writes and the
	// DiskWatchLoop are the territory of the writing stages; this worker
	// does not need the Put limits, the same directory and file locks are
	// opened.
	if _, err := content.SetDictFile(cfg.Content.ZstdDictionary, logger); err != nil {
		return err
	}
	store, err := content.OpenPackStore(pool, content.PackOptions{Dir: cfg.Content.Dir})
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	// The dictionary against the newest pack (T-043): the previous
	// registry dictionary keeps the old frames readable (T-103); a
	// mismatch without a file in the registry is visible as a Warn at
	// startup, not as read errors.
	content.WarnDictMismatch(store.Dir(), logger)

	// Queue: the worker only consumes q:analyze and XACKs (the end of the
	// pipeline §7); the reanalysis loop needs the publishing under the
	// backpressure gate (WatermarkPublisher, T-026) — one Redis client per
	// service.
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{
		MaxLen: int64(cfg.Redis.MaxLen), // XADD MAXLEN ~ on every stream (T-025)
	})

	// Refunding the stuck q:analyze deliveries (T-025): RecoverLoop in the
	// worker — the XAUTOCLAIM claim, the attempt exhaustion — q:dead; the
	// reliability and pace parameters — the redis section (max_attempts,
	// claim_min_idle, recover_batch, recover_interval — T-153). OnBury —
	// the counter of deaths crawler_queue_dead_buried_total (T-154): a
	// bury must not be silent (the 22.10 post-mortem).
	rel := queue.NewReliab(rdb, q, queue.ReliabOptions{
		MaxAttempts: cfg.Redis.MaxAttempts,
		MinIdle:     cfg.Redis.ClaimMinIdle.D(),
		OnBury:      metrics.NewDeadBuriedHook(reg),
		// OnBuryUnidentified — the quarantine of unidentifiable records
		// (without a task_id): crawler_queue_dead_unidentified_total
		// (T-260, FR-6.1) — the terminal bury branch.
		OnBuryUnidentified: metrics.NewDeadUnidentifiedHook(reg),
	})

	// The backpressure gate (T-026) for the publishing of the reanalysis
	// loop below: the worker itself only consumes q:analyze and XACKs
	// (the end of the pipeline §7).
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(cfg.Scheduler.QueueHighWatermark),
		MaxLen:    int64(cfg.Redis.MaxLen), // the trim limits — for the gate threshold (T-026/T-025)
		Log:       logger,
	})
	publish := queue.NewWatermarkPublisher(q, gate)

	// The join of the background loops (T-232, the http-worker invariant
	// T-223): the derived cancellation + a WaitGroup. The framework
	// cancels the ctx of OnRun only after it returns (internal/service:
	// cancelRun after onRunDone) — an early exit of run from the
	// constructor below (analyzer.New) or a panic must stop the loops
	// themselves, otherwise the defer-closes of rdb/store (the LIFO
	// below) would close the resources in the middle of a reanalysis pass
	// (the DB pool, publishing q:analyze via rdb) or a queue depth request
	// (gate.Len → rdb); before T-232 both loops lived fire-and-forget.
	// The join is a defer registered AFTER the defer-closes above →
	// it executes BEFORE them (LIFO). A WaitGroup, not a channel of a
	// capacity: the join waits for exactly what was started (Add/Done at
	// the launch site) — an early exit with a part of the loops does not
	// hang on the "missing" receptions. The invariants are pinned by the
	// structural test TestRunBackgroundJoin (main_test.go, the checker —
	// internal/bgjoin).
	ctx, cancel := context.WithCancel(ctx)
	var bg sync.WaitGroup
	defer func() {
		// Normally the ctx is already cancelled by the framework by the
		// moment run returns; an early exit/a panic stop the still living
		// loops here — before the defer-closes (LIFO).
		cancel()
		bg.Wait()
	}()

	// The background reanalysis loop (T-074, spec 5.8): an initial drain +
	// a ticker + an out-of-band pass on a rule set change
	// (Reloader.Subscribe); the loop logs the pass errors as Warn itself
	// and returns only on the ctx cancellation — it lives until the
	// service stops, next to the worker.
	bg.Add(1)
	go func() {
		defer bg.Done()
		if err := analyzer.ReanalyzeLoop(ctx, pool, publish, reloader, analyzer.ReanalyzeOptions{
			Log: logger,
		}); err != nil {
			logger.Warn("переанализ: цикл остановлен", slog.Any("err", err))
		}
	}()

	// The depth of the consumed q:analyze stream → crawler_queue_depth
	// (T-077): the depth source — the gate (the same Redis client), it
	// finishes on the ctx cancellation.
	bg.Add(1)
	go func() {
		defer bg.Done()
		_ = metrics.CollectQueueLoop(ctx, reg, gate.Len,
			[]string{queue.StreamAnalyze}, metrics.DefaultQueueCollectInterval, logger)
	}()

	// The worker options — one per build (the render-worker style): the
	// Worker keeps them for ProcessResult, the Run loop takes the same
	// Group/Consume.
	opts := analyzer.Options{
		MinScore: cfg.Analyzer.LaravelMinScore,
		Recover:  rel, // refunding the stuck q:analyze tasks (T-025)
		// The pace of the stuck refunds (T-153): the claim batch/interval —
		// redis.recover_*.
		RecoverBatch:    cfg.Redis.RecoverBatch,
		RecoverInterval: cfg.Redis.RecoverInterval.D(),
		// The point drain of a domain's content when a find is removed by
		// the reanalysis (T-187): the window is the same as that of the
		// http-worker retention loop (content.retention), the purge is
		// content.DrainDomain over the service pool; retention = 0 — the
		// content is always stored, there is no drain.
		Retention:   cfg.Content.Retention.D(),
		DrainDomain: content.DrainDomain,
		Metrics:     reg, // the stage instruments: the tasks and the durations (T-077)
		Log:         logger,
	}
	w, err := analyzer.New(pool, q, store, reloader, opts)
	if err != nil {
		return err
	}
	// The main loop lives until the ctx cancellation (graceful shutdown);
	// the join of the background loops — the defer above: any exit of run
	// (graceful, early from the constructor, a panic) first stops and waits
	// for the loops, and only then the LIFO closes rdb/store.
	return analyzer.Run(ctx, w, opts)
}

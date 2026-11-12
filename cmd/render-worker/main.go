// The render-worker service (spec 5.6, §5.7, §7; T-067): the render
// stage. It consumes the tasks of the consumption stream (the default
// q:render; Result — the key of the subdomain_results row,
// T-024; published by the HTTP stage on the SPA verdicts T-057/T-066 and
// the renderq.go finishing pass) with a consumer group; the final URL is
// read from the row, the page is rendered by the Chromium browser under
// the generation manager (internal/render.Manager, T-065: one browser
// T-061, the resource blocking T-062, the readiness waiting T-063, the
// DOM/cookie/header capture T-064, the restarts by pages/memory and the
// reconnect after losing Chromium), the DOM replaces the raw HTML in the
// content store (ContentStore.Put, T-056/T-042), the row gets
// rendered = true, the final render URLs/cookies/headers and a new
// content reference in one transaction with the refcount swap of the old
// one (SwapRefTx, T-044); after the commit the record is published to
// q:analyze for a reanalysis of the already final content (§7). The
// q:render acknowledgment — by the Processing pattern (XACK only after
// the DB write, T-025); the repeated deliveries are idempotent
// (an overwrite, spec 3.4). The q:analyze publishing — under the
// backpressure gate (WatermarkPublisher, T-026). With
// render.fallback.enabled on (T-238) the engine-branched finalizations
// (the tarpit T-148, the task class T-172) are published to
// q:render-fallback instead of the marker — the finishing by the
// chromium worker B (the render spec stage 35 §4.4). The worker B — the
// same binary with another build (T-240, FR-12/§4.5):
// RENDER_WORKER_STREAM=q:render-fallback switches the consumption stream
// and the group (render-fallback-worker), the pace —
// render.fallback.worker_rate over the sliding window
// renderworker.RateWindow (FR-13); the deployment — docs/deploy.md §2.3.
// The browser restart — the manager T-065;
// with Chromium unavailable the loop only pauses the consumption (the
// Manager.Alive probe — the reconnect and the restart are done by the
// probe itself).
// The framework is internal/service: migrations, the DB pool (OnDBPool),
// the /livez + /healthz probes and /metrics, graceful shutdown.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/render"
	"crawler/internal/renderworker"
	"crawler/internal/service"
)

func main() {
	// A configuration error — before the logger is initialized: stderr and exit code 1.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[render-worker] configuration: %v", err)
	}
	logger := service.NewLogger("render-worker", cfg)

	// The pool comes from the framework after the migrations (OnDBPool)
	// and is guaranteed to be ready before OnRun starts: postgres.dsn is
	// required (config.Validate); it is used both by the content store
	// (the contents rows, T-042) and by the reference replacement in
	// subdomain_results (T-067). The metrics registry — right here
	// (T-077): the instruments of the render stage and the collector of
	// the q:render depth receive it before OnRun starts, the framework
	// serves the same registry via /metrics.
	reg := metrics.New("render-worker")
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "render-worker",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9105"),
		Metrics:     reg,
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		OnRun:       func(ctx context.Context) error { return run(ctx, cfg, logger, pool, reg) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — the working logic of render-worker: Chromium under the generation
// manager (T-065: external by render.remote_url or its own process T-061,
// the restarts by pages/memory and the replacement of a dead generation;
// the task contexts — isolated profiles) → the content store (the same as
// the HTTP stage: the packs T-042 + the zstd dictionary T-043 + the disk
// control T-046) → the q:render loop with the stage instruments (T-077)
// and the collector of the q:render depth (the queue depth metric,
// spec 5.9). Shutdown — ctx cancellation (graceful shutdown, spec 3.4):
// the tasks finish (each is bounded by render.timeout) or are refunded to
// the queue for redelivery (Processing, T-025); the background loops
// (DiskWatchLoop, the q:render collector) are stopped by the derived
// cancellation and joined before the defer-closes of rdb/store (T-232).
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, reg *metrics.Registry) error {
	if pool == nil {
		return errors.New("render-worker: the DB pool is not provided (postgres.dsn is required)")
	}

	// Chromium — the generation manager (T-065): it owns the current
	// "browser + pool", restarts the browser by the pages/RSS thresholds
	// and replaces the generation after losing Chromium (a reconnect). The
	// OnRun ctx — the lifecycle context of all the generations (T-061: the
	// browser lives while the context of the startup call lives). The
	// startup — synchronously: a startup error — an OnRun error (exit
	// code 1, systemd Restart=on-failure), without a browser the stage
	// does not work; the further restarts — without stopping the
	// consumption.
	manager := render.NewManager(ctx, render.NewOptions(*cfg))
	defer manager.Close()
	if err := manager.Start(); err != nil {
		return err
	}

	// The content store (T-056, spec 5.7) — the same as the HTTP stage:
	// the zstd dictionary (T-043) is activated before opening, a missing
	// file — not an error; the limits — the content section
	// (max_store_bytes — ErrTooLarge on a bloated DOM,
	// disk_free_percent — ErrStoreFull, spec 8).
	if _, err := content.SetDictFile(cfg.Content.ZstdDictionary, logger); err != nil {
		return err
	}
	store, err := content.OpenPackStore(pool, content.PackOptions{
		Dir:             cfg.Content.Dir,
		PackSize:        cfg.Content.PackSize.B(),
		MaxStoreBytes:   cfg.Content.MaxStoreBytes.B(),
		DiskFreePercent: cfg.Content.DiskFreePercent,
		// OnStoreFull — the hook of the T-080 alert (the metrics): before
		// it the "space below the threshold" events are visible in the
		// Warn logs of DiskWatchLoop.
	})
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	// The dictionary against the newest pack (T-043): the previous
	// registry dictionary keeps the old frames readable (T-103); a
	// mismatch without a file in the registry is visible right at the
	// startup.
	content.WarnDictMismatch(store.Dir(), logger)

	// Queue: the consumption of the worker stream (stream, the default
	// q:render) and the XACK pass through transparently, the publishing of
	// q:analyze — under the backpressure gate (T-026: the threshold is
	// common for the gated streams; as by dns-worker with q:http and
	// http-worker with q:analyze; the capacity gate of q:render is
	// separate, render.queue_high_watermark, T-174).
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()

	// The join of the background loops (T-232, the http-worker invariant
	// T-223): the derived cancellation + a WaitGroup. The framework
	// cancels the ctx of OnRun only after it returns (internal/service:
	// cancelRun after onRunDone) — an early exit of run from the
	// constructor below (renderworker.New) or a panic must stop the loops
	// themselves, otherwise the defer-closes of rdb/store (the LIFO
	// below) would close the resources in the middle of a DiskWatchLoop
	// pass (store) or a q:render depth request (gate.Len → rdb); before
	// T-232 both loops lived fire-and-forget. The join is a defer
	// registered AFTER the defer-closes above → it executes BEFORE them
	// and before the closing of the Chromium manager (LIFO). A WaitGroup,
	// not a channel of a capacity: the join waits for exactly what was
	// started (Add/Done at the launch site) — an early exit with a part of
	// the loops does not hang on the "missing" receptions. The loop errors
	// are discarded deliberately: each loop logs its own failures.
	// The invariants are pinned by the structural test
	// TestRunBackgroundJoin (main_test.go, the checker —
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

	// The free space control of the content directory (T-046, spec 8):
	// a periodic watcher by a goroutine (it does not depend on the task
	// loop), a Warn and an event on entering the write pause/its
	// recovery; it finishes on the ctx cancellation.
	bg.Add(1)
	go func() {
		defer bg.Done()
		_ = content.DiskWatchLoop(ctx, store, content.DiskWatchOptions{Log: logger})
	}()

	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(cfg.Scheduler.QueueHighWatermark),
		MaxLen:    int64(cfg.Redis.MaxLen), // the trim limits — for the gate threshold (T-026/T-025)
		Log:       logger,
	})
	q := queue.NewWatermarkPublisher(queue.NewRedisQueue(rdb, queue.RedisOptions{
		MaxLen: int64(cfg.Redis.MaxLen), // XADD MAXLEN ~ on every stream (T-025)
	}), gate)

	// Refunding the stuck deliveries of the consumption stream (T-025):
	// RecoverLoop in the worker — the XAUTOCLAIM claim, the attempt
	// exhaustion — q:dead; the reliability and pace parameters — the redis
	// section (max_attempts, claim_min_idle, recover_batch,
	// recover_interval — T-153). OnBury — the counter of deaths
	// crawler_queue_dead_buried_total (T-154): a bury must not be silent
	// (the 22.10 post-mortem).
	rel := queue.NewReliab(rdb, q, queue.ReliabOptions{
		MaxAttempts: cfg.Redis.MaxAttempts,
		MinIdle:     cfg.Redis.ClaimMinIdle.D(),
		OnBury:      metrics.NewDeadBuriedHook(reg),
		// OnBuryUnidentified — the quarantine of unidentifiable records
		// (without a task_id): crawler_queue_dead_unidentified_total
		// (T-260, FR-6.1) — the terminal bury branch.
		OnBuryUnidentified: metrics.NewDeadUnidentifiedHook(reg),
	})

	// The consumption stream of the worker (T-236, spec §4.1 item
	// 3/FR-12): the default — q:render; the fallback worker (spec §4.5) —
	// the same binary with another build: the q:render-fallback stream
	// (the constant queue.StreamRenderFallback — the task T-237). The
	// build switch — the environment variable RENDER_WORKER_STREAM (like
	// METRICS_ADDR — a launch parameter, not configuration fields: the
	// consumption stream is a build parameter, spec §4.1 item 3; the
	// worker B profile — deploy/docker/config.render-fallback.yaml,
	// docs/deploy.md §2.3). Only the render-worker streams are allowed
	// (q:render/q:render-fallback): a foreign stream would run the render
	// with a foreign consumer group. The group — the canonical pair of
	// the stream (queue.WorkingGroups, the invariant "one consumer group
	// per working stream", T-155): the main build — DefaultGroup, the
	// worker B — render-fallback-worker.
	stream := service.EnvOr("RENDER_WORKER_STREAM", renderworker.DefaultStream)
	if stream != renderworker.DefaultStream && stream != queue.StreamRenderFallback {
		return fmt.Errorf("render-worker: the consumption stream %q is invalid: the render-worker streams are q:render (the primary build) and q:render-fallback (worker B, FR-12/T-237)", stream)
	}
	group := queue.WorkingGroups()[stream]

	// The depth of the consumed stream (stream) → crawler_queue_depth
	// (T-077): the depth source — the gate (the same Redis client), it
	// finishes on the ctx cancellation.
	bg.Add(1)
	go func() {
		defer bg.Done()
		_ = metrics.CollectQueueLoop(ctx, reg, gate.Len,
			[]string{stream}, metrics.DefaultQueueCollectInterval, logger)
	}()

	// The worker options — one per build (the http-worker style): the
	// Worker keeps them for ProcessResult, the Run loop takes the same
	// Group/Consume/Alive.
	// The consumption pace (T-240, FR-13): the ceiling is enabled on the
	// fallback stream — the worker B build: render.fallback.worker_rate
	// (T-233) records over the sliding window renderworker.RateWindow,
	// the batch mode "collect and finish" without scaling up the
	// chromium capacity. The main build (q:render) reads without a
	// ceiling; the worker B keeps the pace even with
	// render.fallback.enabled off — it finishes what accumulated in
	// q:render-fallback down to empty (FR-14).
	rate := 0
	if stream == queue.StreamRenderFallback {
		rate = cfg.Render.Fallback.WorkerRate
	}
	opts := renderworker.Options{
		// The consumption stream (T-236, FR-12): the reading of the Run
		// loop and the claim of RecoverLoop — one value with the depth
		// collector above;
		// the default q:render, the fallback worker build overrides it.
		Stream: stream,
		// The group — the canonical pair of the stream
		// (queue.WorkingGroups): render-worker (the main build) /
		// render-fallback-worker (B).
		Group:   group,
		Recover: rel, // refunding the stuck tasks (T-025)
		// The pace of the stuck refunds (T-153): the batch/interval of the
		// claim — redis.recover_*; the invariant with claim_min_idle (the
		// render is the slowest sequential traversal) —
		// docs/configuration.md.
		RecoverBatch:    cfg.Redis.RecoverBatch,
		RecoverInterval: cfg.Redis.RecoverInterval.D(),
		Metrics:         reg, // the stage instruments: the tasks, the durations, the class errors, rendered (T-077)
		// The engine of the build (render.engine, T-234) → the engine label
		// of the stage metrics (T-235, FR-15): the same value as in
		// render.NewOptions above (the render manager), — the engine of the
		// build is one, the metric labels distinguish the engines of the
		// canary windows.
		Engine: cfg.Render.Engine,
		Log:    logger,
		// The browser probe: a dead generation the manager replaces right
		// in the probe (Manager.Alive, T-065 — the reconnect after losing
		// Chromium), here — only the consumption pause while the start does
		// not succeed: the deliveries stay in the stream, not in the PEL,
		// and do not burn attempts.
		Alive: manager.Alive,
		// The deliveries in progress — no less than the pool slots:
		// without this render.contexts > 4 would not speed the stage up
		// (the loop parallelism would stay 4, the extra pool slots would
		// idle; the T-142 review).
		Concurrency: max(renderworker.DefaultConcurrency, cfg.Render.Contexts),
		// The tarpit budget (T-148): the page timeouts do not burn the
		// common delivery limit — after render.timeout_attempts timeouts
		// the record is finalized without a render (the raw HTML +
		// q:analyze), not q:dead.
		TimeoutAttempts: cfg.Render.TimeoutAttempts,
		// The last attempt × the task class (T-172): a render error of the
		// task class on delivery #redis.max_attempts finalizes the record
		// without a render instead of q:dead — the same value as Reliab
		// above (the burial boundary), so that the policy makes it in time
		// before the transfer.
		MaxAttempts: cfg.Redis.MaxAttempts,
		// The tarpit counter (T-261, FR-7): the decision of the tarpit
		// finalization — by the number of the tarpit-classified deliveries
		// of the episode (the size of the Redis set
		// render:tarpit:{task_id} ≥ render.timeout_attempts, a TTL of
		// 48 h), not by the delivery number in the PEL: mixed series (an
		// infra failure between tarpits) do not finalize on an attempt
		// earlier. The same Redis client as the queue; losing Redis — a
		// degradation to the delivery number (the behavior before
		// T-261).
		Tarpits: queue.NewTarpitCounter(rdb),
		// The fallback publishing (T-238, spec 4.4, FR-14):
		// render.fallback.enabled (T-233) — the T-148 tarpits and the
		// T-172 task class on the last delivery are published to
		// q:render-fallback instead of the render_failed_at marker; false
		// (the default) — the finalizations as today.
		// The gate/budget of the decision (T-239, FR-9): the threshold of
		// the infra-error streak of the browser class and the cap of the
		// publishing share per window — from render.fallback (T-233); the
		// states (the streak, the window) are kept by the worker.
		// The pace of the worker B — T-240; the worker B (the
		// q:render-fallback stream) does not publish the fallback — the
		// recursion is forbidden (FR-10), checked in publishFallback by
		// Stream; the chromium engine of the build (including an empty
		// render.engine value) does not publish either (S-P1-1,
		// spec §6.2 item 2/§6.4 item 1) — the step 1 of the acceptance and
		// a rollback suppress the publishing without changing the flag.
		Fallback: renderworker.FallbackOptions{
			Enabled: cfg.Render.Fallback.Enabled,
			Gate: renderworker.FallbackGate{
				Threshold: cfg.Render.Fallback.HealthGateStreak,
			},
			Budget: renderworker.FallbackBudget{
				Ceiling: cfg.Render.Fallback.SharePercent,
			},
		},
		// The consumption pace ceiling (T-240, FR-13; the comment by rate
		// above): the worker B — render.fallback.worker_rate over the
		// renderworker.RateWindow window, the main build — 0 (without a
		// ceiling).
		Rate: rate,
	}
	w, err := renderworker.New(pool, q, store, manager, opts)
	if err != nil {
		return err
	}
	// The main loop lives until the ctx cancellation (graceful shutdown);
	// the join of the background loops — the defer above: any exit of run
	// (graceful, early from the constructor, a panic) first stops and
	// waits for the loops, and only then the LIFO closes rdb/store/the
	// Chromium manager.
	return renderworker.Run(ctx, w, opts)
}

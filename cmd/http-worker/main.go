// The http-worker service (spec 5.5, 4.2, 3.4): the HTTP stage of the
// domain check. It consumes the q:http tasks (HTTPTask T-024: a domain +
// the DNS-live subdomains with addresses and CNAME) with a consumer
// group; for each subdomain — the https and http checks by the redirect
// chains (internal/httpfetch: the transport with the limits T-049, the
// redirects and meta refresh T-048, the TLS attributes T-050, the
// encodings T-051, the honest UA T-052), in parallel inside the task with
// the common cache "final URL → result" (spec 5.5 item 6); the
// variant-logic T-054, the aliases T-055 (a comparison by the hashes),
// the SPA heuristic T-057 — internal/httpworker (T-053). Every downloaded
// page is immediately stored by an early Put into the ContentStore
// (T-147: the packs T-042, the zstd dictionary T-043, the dedup by hash
// §8; the bodies are not held in the slot memory — the disk control
// T-046 — DiskWatchLoop); the reconciliation of the references of the
// early Puts — ContentSaver (T-056) in the Options.OnResult hook before
// the XACK: the originals inherit their references, the extra ones are
// released with Delete, then the domain result is written into
// subdomain_results in one transaction with the swap of the content
// references (Recorder T-058: the status alive/no_http/temp_error, the
// domain done, §7) and each record is published to q:analyze (Result,
// T-024) — the whole chain before the XACK (spec 4.2), the q:http
// acknowledgment — by the Processing pattern (T-025).
// The SPA records are published to q:render (T-066, renderq.go): a free
// queue — right after the commit, an overflow — the render_pending flag
// by the same transaction without delaying the XACK (§5.6), the
// finishing — the background RequeueRenderPending pass (a repeated SPA
// verdict over the stored content).
// The redirect hosts are resolved "via the same resolver" (spec 5.5
// item 2): dnsclient with the T-035 limits → the httpworker.DNSLookup
// adapter → the CachingResolver cache. The framework is
// internal/service: migrations, the DB pool, the /livez + /healthz
// probes, /metrics, graceful shutdown.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/dnsclient"
	"crawler/internal/httpfetch"
	"crawler/internal/httpworker"
	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/service"
)

// afterConnectHooks — the AfterConnect hooks of the http-worker pool in
// the order of application (T-201): the seriality of the scanning passes
// of the service (db.NoParallelGather) and the cap of the row locks of
// the write pool (db.LockTimeout). A named list — the seam of the wiring
// test (main_test.go): before T-201 the hook wiring was assembled in main
// without a seam and was not pinned by any test at all; the composition —
// db.JoinHooks in main.
var afterConnectHooks = []db.AfterConnectFunc{
	// T-201: the retention drain (RetentionLoop, retentionSelectSQL —
	// the time-form T-185) on a starved CPU assembled a Gather Merge —
	// 2 parallel workers (the EXPLAIN ANALYZE of 26.10) — on the same
	// service where the HTTP slots work; the seriality gives away the
	// wall of the background pass in exchange for the CPU fairness (the
	// pace is set by retention_batch/the interval, the disk T-166 is
	// drained for hours).
	db.NoParallelGather,
	// T-192: see the DBAfterConnect comment in main.
	db.LockTimeout(db.DefaultLockTimeout),
}

// scanGate — the uniqueness semaphore of the heavy background scans of
// the service (db.ScanGate, T-271): the selections of the retention
// drain and the late render re-publisher must not coincide in PostgreSQL
// — the incident of 08–09.11.2026 (five PG OOM-kills overnight: two
// degraded cursor SELECTs of ≈1 GiB of working structures on top of
// 2 GiB shared_buffers; the plans themselves are pinned with SET LOCAL
// by the selections, the gate — the guard against a coincidence of the
// residually heavy scans). A named variable — the seam of the wiring
// test (main_test.go, the afterConnectHooks style): the wiring — the
// Gate field in the options of both loops in run.
var scanGate = make(db.ScanGate, 1)

func main() {
	// A configuration error — before the logger is initialized: stderr and exit code 1.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[http-worker] конфигурация: %v", err)
	}
	logger := service.NewLogger("http-worker", cfg)

	// The soft memory cap of the Go runtime (T-163, http.mem_limit): the
	// GC keeps the heap below the threshold itself, without bringing anon
	// up to the MemoryHigh/MemoryMax of the unit — the reclaim storms of
	// the kernel and an OOM-kill are costlier than the extra GC cycles
	// (the fatals of T-157: 120,095 MemoryHigh events, the peak
	// 900.5 MiB = MemoryMax). It is set before the working allocations;
	// the page cache of the pack writes is not part of the runtime
	// accounting — the cap is below MemoryHigh with a margin (the default
	// 768 MiB against 850M/900M). 0 — off.
	if limit := cfg.HTTP.MemLimit; limit > 0 {
		debug.SetMemoryLimit(limit.B())
		logger.Info("мягкий потолок памяти Go-рантайма", "limit", limit.String())
	}

	// The pool comes from the framework after the migrations (OnDBPool)
	// and is guaranteed to be ready before OnRun starts: postgres.dsn is
	// required (config.Validate); it is used both by the content store
	// (the contents rows, T-042/T-056) and by writing the result
	// (subdomain_results/domains, T-058). The metrics registry — right
	// here (T-077): the instruments of the HTTP stage and the collector
	// of the q:http depth receive it before OnRun starts, the framework
	// serves the same registry via /metrics.
	reg := metrics.New("http-worker")
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "http-worker",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9104"),
		Metrics:     reg,
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		// The composite AfterConnect hook of the pool (db.JoinHooks,
		// T-192): NoParallelGather (T-201) — the aggregate/scanning
		// operators of the background loops of the service (the retention
		// drain, the EXPLAIN of 26.10: Parallel Index Scan + Gather Merge)
		// do not tolerate parallel workers on the CPUs of the HTTP slots;
		// the point operators of the slots (PK, unnest) do not use the
		// parallelism — the pool behavior changes only for the background
		// passes (the render finishing pass T-066 is already serial by its
		// nature). LockTimeout (T-192) — the session cap of lock_timeout
		// 30 s: writing the results and reading the counters (FOR UPDATE)
		// take the row locks of domains; the 25.10 lock convoy (the holder
		// ≈37 min, the victims up to 39 min) is truncated by a transaction
		// rollback and a task delivery retry (at-least-once, spec §3.4).
		DBAfterConnect: db.JoinHooks(afterConnectHooks...),
		OnRun:          func(ctx context.Context) error { return run(ctx, cfg, logger, pool, reg) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — the working logic of http-worker: a DNS client (the resolution
// of the redirect hosts, spec 5.5 item 2) under the T-035 limits → the
// httpfetch transport with the T-049 limits and TLS without the
// certificate check (spec 5.5 item 3: the content is needed always, the
// certificate check result is preserved by TLSInfo T-050) → the content
// store (T-056/T-147: the packs T-042 + the dictionary T-043 + the disk
// control T-046 — the early Puts of the pages and the reference
// reconciliation) → the worker (the final URL cache, the variants, the
// aliases, the SPA) with the stage instruments (T-077) and the collector
// of the q:http depth (the queue depth metric, spec 5.9) → the q:http
// loop. Shutdown — ctx cancellation (graceful shutdown, spec 3.4): the
// started tasks finish or are refunded to the queue for redelivery
// (Processing, T-025).
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, reg *metrics.Registry) error {
	if pool == nil {
		return errors.New("http-worker: пул БД не получен (postgres.dsn обязателен)")
	}

	client, err := dnsclient.NewClient(ctx, dnsclient.NewOptions(cfg.DNS))
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	resolve, err := dnsclient.NewLimitResolver(client, dnsclient.NewLimitOptions(cfg.DNS))
	if err != nil {
		return err
	}

	// The stage transport: NewOptions always enables the limits (T-049)
	// and substitutes the default honest UA (T-052); the resolution of the
	// redirect hosts — by the same resolver of the DNS stage with a cache
	// (spec 5.5 item 2, CachingResolver T-047, the httpworker.DNSLookup
	// adapter); InsecureSkipVerify — spec 5.5 item 3. Waiting for a
	// per-IP semaphore slot — into the :9104 histogram (T-165):
	// NewMetrics is idempotent (T-076), the worker below shares the same
	// collectors; the same met holds the counter of the 40P01 retries of
	// the write transaction
	// (crawler_db_deadlock_retried_total, T-268) — it is passed to the
	// result recorder below.
	met := httpworker.NewMetrics(reg)
	opts := httpfetch.NewOptions(cfg.HTTP)
	opts.Limits.OnPerIPWait = met.PerIPWait
	opts.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	opts.Resolver = httpfetch.NewCachingResolver(
		httpworker.NewDNSLookup(resolve, cfg.DNS.IPv6), httpfetch.CacheOptions{})
	fetcher := httpfetch.NewFetcher(opts)
	defer fetcher.CloseIdleConnections()

	// The content store (T-056, spec 5.7): the zstd dictionary (T-043) is
	// activated before opening — the blocks are compressed with the active
	// dictionary (CompressWithDict), its ID lands in the header of the
	// created packs; a missing file — not an error (compression without a
	// dictionary). The limits — the content section: max_store_bytes
	// (ErrTooLarge), disk_free_percent (ErrStoreFull, spec 8), pack_size
	// (the rotation).
	if _, err := content.SetDictFile(cfg.Content.ZstdDictionary, logger); err != nil {
		return err
	}
	store, err := content.OpenPackStore(pool, content.PackOptions{
		Dir:             cfg.Content.Dir,
		PackSize:        cfg.Content.PackSize.B(),
		MaxStoreBytes:   cfg.Content.MaxStoreBytes.B(),
		DiskFreePercent: cfg.Content.DiskFreePercent,
		// OnStoreFull — the hook of the T-080 alert (the metrics): before
		// it the "space below the threshold" events are visible in the Warn
		// logs of DiskWatchLoop and ContentSaver.
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

	// The Redis client (q:http/q:analyze/q:render) — before the
	// background loops are started: its defer-close must stand in the LIFO
	// earlier than the join (below), the gate and the queues are assembled
	// further down the wiring.
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()

	// The join of the background loops (T-223): the derived cancellation +
	// a WaitGroup. The framework cancels the ctx of OnRun only after it
	// returns (internal/service: cancelRun after onRunDone) — an early
	// exit of run from the constructors below or a panic must stop the
	// loops themselves, otherwise the defer-closes of rdb/store/the
	// transport would close the resources in the middle of a
	// compaction/retention pass; before T-223 CompactLoop and
	// RetentionLoop lived fire-and-forget even on a graceful stop — the
	// three receptions of the bg channel did not cover them, a fourth
	// sender would hang on the channel of capacity 3. The join is a defer
	// registered AFTER the defer-closes above → it executes BEFORE them
	// (LIFO). A WaitGroup, not a channel of a capacity (the
	// scheduler/monitor style): the join waits for exactly what was
	// started (Add/Done at the launch site) — an early exit with a part of
	// the loops does not hang on the "missing" receptions, and the
	// "capacity = senders = receptions" coupling does not need to be kept
	// by hand. The loop errors are discarded deliberately: each loop logs
	// its own failures. The invariants are pinned by the structural test
	// TestRunBackgroundJoin (main_test.go).
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

	// The background compaction of the packs (T-045, spec 5.7): the only
	// mechanism of physically freeing the space and deleting the dead
	// contents rows — without it ErrStoreFull is never lifted. The loop is
	// one per store — here, next to DiskWatchLoop (the passes do not need
	// parallelism; a simultaneous start in a second writer is formally
	// allowed but useless). It finishes on the ctx cancellation.
	bg.Add(1)
	go func() {
		defer bg.Done()
		_ = content.CompactLoop(ctx, store, content.CompactOptions{Log: logger})
	}()

	saver, err := httpworker.NewContentSaver(store, logger)
	if err != nil {
		return err
	}

	// Queue: the consumption of q:http and the publishing of q:analyze —
	// under the backpressure gate (T-026: the threshold is common for the
	// gated streams; Consume/Ack pass through transparently, as by
	// dns-worker). The q:render publishing goes past the
	// WatermarkPublisher, through the same RedisQueue (T-066, §5.6):
	// waiting for the capacity would delay the XACK of the q:http task —
	// on an overflow the domain gets the render_pending flag instead of
	// the publishing (renderq.go; the capacity threshold —
	// render.queue_high_watermark, T-174, below).
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(cfg.Scheduler.QueueHighWatermark),
		MaxLen:    int64(cfg.Redis.MaxLen), // the trim limits — for the gate threshold (T-026/T-025)
		Log:       logger,
	})
	rawQ := queue.NewRedisQueue(rdb, queue.RedisOptions{
		MaxLen: int64(cfg.Redis.MaxLen), // XADD MAXLEN ~ on every stream (T-025)
	})
	q := queue.NewWatermarkPublisher(rawQ, gate)

	// Refunding the stuck q:http deliveries (T-025): RecoverLoop in the
	// worker — the XAUTOCLAIM claim, the attempt exhaustion — q:dead; the
	// reliability and pace parameters — the redis section (max_attempts,
	// claim_min_idle, recover_batch, recover_interval — T-153). OnBury —
	// the counter of deaths crawler_queue_dead_buried_total (T-154): a
	// bury must not be silent (the 22.10 post-mortem).
	rel := queue.NewReliab(rdb, rawQ, queue.ReliabOptions{
		MaxAttempts: cfg.Redis.MaxAttempts,
		MinIdle:     cfg.Redis.ClaimMinIdle.D(),
		OnBury:      metrics.NewDeadBuriedHook(reg),
		// OnBuryUnidentified — the quarantine of unidentifiable records
		// (without a task_id): crawler_queue_dead_unidentified_total
		// (T-260, FR-6.1) — the terminal bury branch.
		OnBuryUnidentified: metrics.NewDeadUnidentifiedHook(reg),
	})

	// The render queue: a non-blocking capacity check (the depth via the
	// gate T-026) + publishing without waiting for the capacity. The
	// capacity threshold — a separate render.queue_high_watermark (T-174,
	// renderQueueWatermark below): the common scheduler.queue_high_watermark
	// kept the gate open at a lag of 98.4k out of 100k (the equilibrium of
	// 24.10, the stage 25 of tasks.md) — the inflow equaled the
	// consumption, the debt drain was zero.
	renderQ, err := httpworker.NewRenderQueue(rawQ, gate, renderQueueWatermark(cfg), logger)
	if err != nil {
		return err
	}

	// The depth of the consumed q:http stream → crawler_queue_depth
	// (T-077): the depth source — the gate (the same Redis client), it
	// finishes on the ctx cancellation.
	bg.Add(1)
	go func() {
		defer bg.Done()
		_ = metrics.CollectQueueLoop(ctx, reg, gate.Len,
			[]string{queue.StreamHTTP}, metrics.DefaultQueueCollectInterval, logger)
	}()

	recorder, err := httpworker.NewRenderRecorder(pool, q, renderQ, met, logger)
	if err != nil {
		return err
	}

	// The render finishing pass (T-066, §5.6 "they are finished later"):
	// the domains with the render_pending flag → a repeated SPA verdict
	// over the stored content → publishing to q:render when the queue is
	// free.
	// The zone priority (T-184, the decision (a) of T-176): a non-empty
	// render.requeue_deprioritized_zones (production — the parking .рф,
	// xn--p1ai) is drained gets-the-leftovers — the batch of the pass is
	// collected first by the domains of the other zones; the observability
	// — the deprioritized field of the "render finishing pass" line in the
	// log.
	// By a separate goroutine in the same OnRun (the CleanupDeleted sample
	// T-020/T-021): it does not depend on the q:http task loop, it
	// finishes on the ctx cancellation.
	bg.Add(1)
	go func() {
		defer bg.Done()
		_ = httpworker.RequeueRenderPending(ctx, pool, store, renderQ, httpworker.RequeueOptions{
			Log:                logger,
			DeprioritizedZones: cfg.Render.RequeueDeprioritizedZones,
		})
	}()

	// The late render re-publisher (T-259, the TZ-fsm spec §3 FR-5.3): a
	// background pass re-publishes to q:render the "forever young" records
	// (not rendered, without a marker, with a render target) older than
	// 6 h — without the render_pending bit on the domain (the finishing
	// pass above owns the debt) and without unconsumed messages of
	// q:render/q:render-fallback (an interlock by the snapshot of the
	// streams — the retentionPending adapter, the same one as the
	// retention guard). It heals the loss of the MAXLEN-truncated
	// fallback tasks and the lost publications: the state of the record
	// stops depending on the survival of the message in the stream. A
	// cursor by PK, the batch/interval — the loop options; the
	// observability — crawler_render_late_republish_total
	// (httpworker.Metrics). The selection — a heavy scan under the common
	// scanGate gate (T-271: not to coincide with the retention drain in
	// PostgreSQL).
	bg.Add(1)
	go func() {
		defer bg.Done()
		_ = httpworker.RenderLateLoop(ctx, pool, store, renderQ,
			retentionPending{rdb: rdb}, met, httpworker.LateOptions{
				Log:  logger,
				Gate: scanGate,
				// The ceiling of the re-publisher batch selection (T-274, the
				// value of T-273): a knob in case of an underestimation —
				// a ceiling change without a deploy.
				SelectTimeout: cfg.Render.LateSelectTimeout.D(),
			})
	}()

	// The content retention and the jsonb trim of the non-finds (T-166,
	// spec 5.7: the content is stored while the rules need to be
	// reapplied; the operator decision of 2026-10-23 — the disk without
	// new capacities): the records older than content.retention, not
	// Laravel finds and not waiting for a render lose their content_hash
	// (−1 contents.refcount by the same transaction, the zero rows are
	// deleted) and the heavy jsonb; the passport of the row stays for
	// domain_info and the dashboard. The interlock — the q:analyze depth
	// (the gate T-026): if the analyzer does not keep up, the bodies are
	// still needed, the pass is skipped; a threshold of 0 in the config —
	// the common scheduler.queue_high_watermark (the publishing gate of
	// q:analyze works by the same "strictly below the threshold"
	// criterion).
	// The precise guard (T-166/T-167): the records whose messages are not
	// yet consumed by q:render/q:analyze are excluded from the batches by
	// the snapshot of the streams — retentionPending below, by the same
	// Redis client as the gate.
	// The loop right here where the compaction is: the purge only
	// prepares the dead blocks, the hourly compaction pass returns the
	// space; a second owner does not need it (the guards make a parallel
	// start safe but useless). It finishes on the ctx cancellation. The
	// batch selection — a heavy scan under the common scanGate gate
	// (T-271: not to coincide with the late render re-publisher in
	// PostgreSQL) in a tx wrapper of plan pinning with a
	// statement_timeout cap.
	retentionWatermark := int64(cfg.Content.RetentionAnalyzeWatermark)
	if retentionWatermark <= 0 {
		retentionWatermark = int64(cfg.Scheduler.QueueHighWatermark)
	}
	// One retention instruments instance for both hooks: the pass totals
	// (OnPass) and the errors (OnError —
	// crawler_content_retention_errors_total, T-273: before this the pass
	// errors were visible only in journalctl, the :9104 metrics stood at
	// zero with live WARNs).
	retentionMet := content.NewRetentionMetrics(reg)
	bg.Add(1)
	go func() {
		defer bg.Done()
		_ = content.RetentionLoop(ctx, pool, content.RetentionOptions{
			Retention: cfg.Content.Retention.D(),
			Interval:  cfg.Content.RetentionInterval.D(),
			BatchSize: cfg.Content.RetentionBatch,
			// The ceiling of the batch selection (T-274): the default
			// 7200 s — a doubled margin over the measurements of
			// 10–11.11.2026 of the first catch-up pass under load
			// (3600 s of T-273 sufficed only for a quiet system); 0 in the
			// config — the code default (withDefaults).
			SelectTimeout:    cfg.Content.RetentionSelectTimeout.D(),
			Depth:            analyzeDepth{gate: gate},
			Pending:          retentionPending{rdb: rdb},
			AnalyzeWatermark: retentionWatermark,
			Gate:             scanGate,
			Log:              logger,
			OnPass:           retentionMet.Observe,
			OnError:          retentionMet.ObserveError,
		})
	}()

	opts2 := httpworker.Options{
		// The task parallelism of the Run loop (T-139): the sliding
		// consumption of q:http, the values — http.task_* from the config
		// (the defaults by the measurements of T-092; the transport caps —
		// http.concurrency/per_ip_limit/max_rps above, in
		// httpfetch.NewOptions).
		Concurrency:  cfg.HTTP.TaskConcurrency,
		Consume:      cfg.HTTP.TaskConsume,
		PerDomain:    cfg.HTTP.TaskPerDomain,
		MaxRedirects: cfg.HTTP.MaxRedirects, // 0 — do not follow (T-006/T-048)
		// The dead-host cutoff (T-164): dead apex+www do not burn the task
		// slots on the subdomains — http.dead_host_cutoff.
		DeadHostCutoff: cfg.HTTP.DeadHostCutoff,
		Store:          store, // the early storing of the page bodies (T-147)
		Recover:        rel,   // refunding the stuck q:http tasks (T-025)
		// The pace of the stuck refunds (T-153): the batch/interval of the
		// claim — redis.recover_*.
		RecoverBatch:    cfg.Redis.RecoverBatch,
		RecoverInterval: cfg.Redis.RecoverInterval.D(),
		Metrics:         reg, // the stage instruments: the tasks, the errors, the statuses, the response time, rendered (T-077)
		Log:             logger,
		// The OnResult chain (inside the task processing, before the XACK,
		// spec 4.2): storing the content (T-056) → writing subdomain_results
		// with the contents reference swap (T-058/T-044) → publishing to
		// q:analyze (T-058, §5.5 item 9). A failure of any link — a
		// delivery without an acknowledgment, a repeated delivery replays
		// the chain idempotently (spec 3.4).
		OnResult: func(c context.Context, res *httpworker.DomainResult) error {
			if err := saver.Save(c, res); err != nil {
				return err
			}
			_, err := recorder.RecordAndPublish(c, res)
			return err
		},
	}
	w, err := httpworker.New(httpworker.ChainFetch(fetcher), opts2)
	if err != nil {
		return err
	}

	// The main loop lives until the ctx cancellation (graceful shutdown);
	// the join of the background loops — the defer above: any exit of run
	// (graceful, early from the constructor, a panic) first stops and
	// waits for the loops, and only then the LIFO closes rdb/store/the
	// transport connections.
	return httpworker.Run(ctx, q, w, opts2)
}

// renderQueueWatermark — the effective capacity threshold of q:render
// (T-174): render.queue_high_watermark, 0 — the common
// scheduler.queue_high_watermark (the semantics of
// content.retention_analyze_watermark in run above); then the clamp
// min(watermark, MAXLEN/2) — queue.StreamWatermark, the same single
// calculation as by the publishing gate (T-026) and the stream
// thresholds: the gate must close BEFORE the MAXLEN trim — the trim
// removes the records regardless of reading, and a threshold above the
// limit would turn the soft regulation into a silent loss of the
// undelivered render tasks (they have no render_pending flag — the
// publishing went through). On the defaults the clamp is not active:
// 10k < 75k = DefaultStreamMaxLen(q:render)/2 (T-025); with an explicit
// 0 it caps the common threshold of 100k to 75k — the former T-066
// wiring compared the raw value, and the gate stayed open at a lag of
// 98.4k (the equilibrium of 24.10) even where the publishing gate by the
// clamp would already have closed.
func renderQueueWatermark(cfg *config.Config) int64 {
	wm := int64(cfg.Render.QueueHighWatermark)
	if wm <= 0 {
		wm = int64(cfg.Scheduler.QueueHighWatermark)
	}
	return queue.StreamWatermark(wm, int64(cfg.Redis.MaxLen), queue.StreamRender)
}

// analyzeDepth — the q:analyze depth for the retention interlock
// (T-166): an adapter of the backpressure gate (T-026) to the narrow
// content.RetentionDepth interface — the content package does not know
// the Redis specifics of the gate (the RenderLen style, renderq.go).
type analyzeDepth struct{ gate *queue.BackpressureGate }

func (a analyzeDepth) AnalyzeDepth(ctx context.Context) (int64, error) {
	return a.gate.Len(ctx, queue.StreamAnalyze)
}

// retentionPendingRedis — a window into go-redis for reading the
// unconsumed tail of the stream: XINFO GROUPS (the last-delivered-id of
// the groups) and XRANGE. Narrow, as the redisDepth of the gate (T-026):
// *redis.Client satisfies it, the unit test (main_test.go) substitutes a
// fake; the internal interface of the internal/queue package is not
// extended — there are parallel tasks there.
type retentionPendingRedis interface {
	XInfoGroups(ctx context.Context, key string) *redis.XInfoGroupsCmd
	XRangeN(ctx context.Context, key, start, stop string, count int64) *redis.XMessageSliceCmd
}

// compile-time: the queue client — the source of the retention guard.
var _ retentionPendingRedis = (*redis.Client)(nil)

// The constants of the stream tail reading.
const (
	// pendingFieldData — the only field of a stream record: the JSON
	// message (fieldData redisq.go; the literal here — the field is not
	// exported, it is duplicated with an anchor comment).
	pendingFieldData = "data"

	// pendingScanBatch — the records in one XRANGE: the tail at a standstill
	// — tens of thousands (the q:render lag ≈99k, the runbooks §5), the
	// pagination keeps the peak slice moderate; the final key set holds the
	// whole tail anyway — the price of the precise guard.
	pendingScanBatch = 1000
)

// retentionPending — the keys of the records whose messages wait for
// consumers, for the precise retention guard (content.RetentionPending,
// T-166/T-167): an adapter over the Redis client of the gate (it reads
// the depth by the same client — BackpressureGate.Len, one point of
// truth about the streams). It reads the tail not delivered to a group:
// XRANGE from the last-delivered-id of the group (exclusively, "(id)")
// to '+'. XREADGROUP ">" is forbidden — it would deliver the records
// into the pending of the group and shift the last-delivered-id, breaking
// the real consumer. The stream has no groups — everything is
// undelivered: XRANGE from the beginning (the same semantics as "depth =
// XLEN" by the gate). The messages are parsed as queue.Result (the
// format of T-024 — the publishing by record.go and renderSPAResults); a
// foreign body is skipped: only marshalMessage writes into the queue
// streams (the validated formats).
type retentionPending struct {
	rdb retentionPendingRedis
}

func (p retentionPending) PendingKeys(ctx context.Context, stream string) (map[content.RetentionPendingKey]struct{}, error) {
	keys := make(map[content.RetentionPendingKey]struct{})
	groups, err := p.rdb.XInfoGroups(ctx, stream).Result()
	if err != nil {
		// The stream does not exist — "no such key" (not an empty list), as
		// by the gate: no publications, no groups — there is nothing to
		// wait for consumers.
		if errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "no such key") {
			return keys, nil
		}
		return nil, fmt.Errorf("http-worker: защита retention: группы %s: %w", stream, err)
	}
	if len(groups) == 0 {
		if err := scanPendingKeys(ctx, p.rdb, stream, "-", keys); err != nil {
			return nil, err
		}
		return keys, nil
	}
	// The tail undelivered to at least one group — from the minimal
	// last-delivered-id: the interval of each group — (its id, +∞), the
	// union of the intervals with different edges — the interval from the
	// minimum; one XRANGE instead of one per group (in production a group
	// is one — a degenerate case).
	minID := ""
	for _, g := range groups {
		if minID == "" || pendingIDLess(g.LastDeliveredID, minID) {
			minID = g.LastDeliveredID
		}
	}
	// The exclusive lower edge "(" + id: the records strictly after the
	// last-delivered-id; a just created group has the id "0-0" — the whole
	// stream.
	if err := scanPendingKeys(ctx, p.rdb, stream, "("+minID, keys); err != nil {
		return nil, err
	}
	return keys, nil
}

// pendingIDLess — a comparison of the "ms-seq" stream record identifiers
// numerically by the parts: the lexicographic comparison is wrong
// ("999-0" is greater than "1000-0" — the same remark by trackProgress,
// redisq.go; the LastDeliveredID of a new group "0-0" is shorter than the
// real ids).
func pendingIDLess(a, b string) bool {
	ams, aseq, aerr := splitPendingID(a)
	bms, bseq, berr := splitPendingID(b)
	if aerr != nil || berr != nil {
		return a < b // unparsable — lexicographically: determinism over accuracy
	}
	if ams != bms {
		return ams < bms
	}
	return aseq < bseq
}

// splitPendingID parses "ms-seq" into a pair of int64.
func splitPendingID(id string) (ms, seq int64, err error) {
	s1, s2, ok := strings.Cut(id, "-")
	if !ok {
		return 0, 0, fmt.Errorf("http-worker: защита retention: идентификатор записи %q не вида ms-seq", id)
	}
	if ms, err = strconv.ParseInt(s1, 10, 64); err != nil {
		return 0, 0, err
	}
	if seq, err = strconv.ParseInt(s2, 10, 64); err != nil {
		return 0, 0, err
	}
	return ms, seq, nil
}

// scanPendingKeys reads up the stream tail from start (inclusive) to '+'
// in pendingScanBatch chunks, parsing the data field into a record key.
func scanPendingKeys(ctx context.Context, rdb retentionPendingRedis, stream, start string, keys map[content.RetentionPendingKey]struct{}) error {
	for {
		msgs, err := rdb.XRangeN(ctx, stream, start, "+", pendingScanBatch).Result()
		if err != nil {
			return fmt.Errorf("http-worker: защита retention: xrange %s: %w", stream, err)
		}
		for _, m := range msgs {
			var raw []byte
			switch v := m.Values[pendingFieldData].(type) {
			case string:
				raw = []byte(v)
			case []byte:
				raw = v
			default:
				continue // no data field — not a queue message
			}
			var r queue.Result
			if json.Unmarshal(raw, &r) != nil {
				continue // a foreign body: only marshalMessage writes the queue streams
			}
			if r.ZoneID <= 0 || r.DomainID <= 0 {
				continue // zero identifiers — not a Result (validate requires ≥ 1)
			}
			keys[content.RetentionPendingKey{
				Zone: r.ZoneID, Domain: r.DomainID,
				Sub: r.SubdomainID, Variant: int16(r.Variant),
			}] = struct{}{}
		}
		if int64(len(msgs)) < pendingScanBatch {
			return nil // the tail is read in full
		}
		start = "(" + msgs[len(msgs)-1].ID // the next page — after the last one
	}
}

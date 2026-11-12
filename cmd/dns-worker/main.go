// The dns-worker service (spec 5.4, 4.2, 3.4): the DNS stage of the
// domain check. It consumes the q:dns batches (DNSBatch) with a consumer
// group, for each domain it performs an episode — the apex
// (internal/dnsworker CheckApex, T-036), the subdomains (CheckSubdomains,
// T-037) and the wildcard detect (DetectWildcard/MarkWildcardSubdomains,
// T-038) — and writes the result (dns_mask, the status, done) with
// publishing q:http tasks by the found addresses (T-039). The exchanges
// go through internal/dnsclient: a resolver chain — a local client with
// its own pool of UDP sockets (T-034) and the reserve clients of
// dns.fallback_resolvers under the switchover decorator
// (FallbackResolver, T-107), on top — the concurrency/QPS limits with an
// adaptive reduction (LimitResolver, T-035); the startup — a synthetic
// probe of the chain (a fail-fast on dead resolvers, T-107); the episode
// concurrency — dns.concurrency. The q:http publishing — under the
// backpressure gate (WatermarkPublisher, T-026). The q:dns
// acknowledgment — by the Processing pattern (XACK only after the DB
// write, T-025). The framework is internal/service: migrations, the DB
// pool (OnDBPool), the /livez + /healthz probes, /metrics, graceful
// shutdown.
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
	"crawler/internal/db"
	"crawler/internal/dnsclient"
	"crawler/internal/dnsworker"
	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/service"
)

func main() {
	// A configuration error — before the logger is initialized: stderr and exit code 1.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[dns-worker] конфигурация: %v", err)
	}
	logger := service.NewLogger("dns-worker", cfg)

	// The pool comes from the framework after the migrations (OnDBPool)
	// and is guaranteed to be ready before OnRun starts: postgres.dsn is
	// required (config.Validate).
	// The metrics registry — right here (T-077): the instruments of the
	// DNS stage and the collector of the q:dns depth receive it before
	// OnRun starts, the framework serves the same registry via /metrics.
	reg := metrics.New("dns-worker")
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "dns-worker",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9103"),
		Metrics:     reg,
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		// The AfterConnect hook of the write pool (T-192): the session cap
		// of lock_timeout 30 s — the transactions of writing the results
		// and publishing take the row locks of domains; the 25.10 lock
		// convoy (the holder ≈37 min, the victims up to 39 min) is
		// truncated by a transaction rollback and a batch delivery retry
		// (at-least-once, spec §3.4).
		DBAfterConnect: db.LockTimeout(db.DefaultLockTimeout),
		OnRun:          func(ctx context.Context) error { return run(ctx, cfg, logger, pool, reg) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — the working logic of dns-worker: the resolver chain (the local
// client T-034 + the reserves of dns.fallback_resolvers, T-107) with a
// startup probe (a fail-fast: all dead — a startup error), under it the
// T-035 limits, the dnsworker.Run loop of the worker with the stage
// instruments (T-077), the collectors of the q:dns depth and the
// switchovers to the reserve (the queue depth metric — spec 5.9: the
// backlog of the stream this worker consumes). Shutdown — ctx
// cancellation (graceful shutdown, spec 3.4): the started episodes finish
// or are refunded to the queue for redelivery (Processing, T-025);
// the background collectors are stopped by the derived cancellation and
// joined before the defer-closes of rdb/the resolvers (T-232).
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, reg *metrics.Registry) error {
	if pool == nil {
		return errors.New("dns-worker: пул БД не получен (postgres.dsn обязателен)")
	}

	// The resolver chain (T-107): the primary — the first of dns.resolvers
	// (a local unbound, spec 5.4), the reserves — dns.fallback_resolvers
	// in the config order. Each address — its own client with its own pool
	// of sockets (the T-034 transport "one client — one resolver"); the
	// pools of the reserves are opened with the same size: in the
	// switchover mode the reserve holds the planned concurrency of the
	// pipeline in full, while an idle connected UDP socket consumes
	// nothing (no packets).
	opt := dnsclient.NewOptions(cfg.DNS)
	client, err := dnsclient.NewClient(ctx, opt)
	if err != nil {
		return err
	}
	clients := []*dnsclient.Client{client}
	// Closing the clients of the chain — a single defer registered BEFORE
	// the loop of the reserves: a startup error in the middle of the loop
	// must not leave open socket pools (an exit from main would close them
	// by the runtime, but the correctness of the error path does not rely
	// on that).
	defer func() {
		for _, c := range clients {
			_ = c.Close()
		}
	}()
	chain := []dnsclient.FallbackTarget{{Addr: opt.Addr, Resolver: client}}
	for _, addr := range cfg.DNS.FallbackResolvers {
		fbopt := opt
		fbopt.Addr = addr
		fc, err := dnsclient.NewClient(ctx, fbopt)
		if err != nil {
			return fmt.Errorf("dns-worker: резервный резолвер %s: %w", addr, err)
		}
		clients = append(clients, fc)
		chain = append(chain, dnsclient.FallbackTarget{Addr: addr, Resolver: fc})
	}

	// The startup synthetic probe of the chain (T-107/DNS-12): a socket
	// pool is fail-fast only by the address, a dead server without the
	// probe would sound with the first production temp_error exchange. It
	// goes to the clients directly, bypassing the limiter (the
	// slot/token/window of the adaptive — not for a service exchange).
	// A partial failure — a Warn in the log (the chain keeps working by
	// switching), nobody answers — a startup error: a pipeline without
	// resolvers puts the domains into temp_error (T-031), it is cheaper to
	// fall down noticeably.
	if err := dnsclient.ProbeChain(ctx, chain, logger); err != nil {
		return err
	}

	fallback, err := dnsclient.NewFallbackResolver(chain[0], chain[1:]...)
	if err != nil {
		return err
	}
	resolve, err := dnsclient.NewLimitResolver(fallback, dnsclient.NewLimitOptions(cfg.DNS))
	if err != nil {
		return err
	}

	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	// The q:http publishing — under the backpressure gate (T-026: the
	// threshold is common for the gated streams; the capacity gate of
	// q:render is separate, render.queue_high_watermark, T-174); the
	// consumption of q:dns and the XACK pass through transparently.
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(cfg.Scheduler.QueueHighWatermark),
		MaxLen:    int64(cfg.Redis.MaxLen), // the trim limits — for the gate threshold (T-026/T-025)
		Log:       logger,
	})
	q := queue.NewWatermarkPublisher(queue.NewRedisQueue(rdb, queue.RedisOptions{
		MaxLen: int64(cfg.Redis.MaxLen), // XADD MAXLEN ~ on every stream (T-025)
	}), gate)

	// The join of the background loops (T-232, the http-worker invariant
	// T-223): the derived cancellation + a WaitGroup. The framework
	// cancels the ctx of OnRun only after it returns (internal/service:
	// cancelRun after onRunDone); a panic in the worker loop must stop
	// the collectors itself, otherwise the defer-closes of rdb/the chain
	// clients (the LIFO below) would close the resources in the middle of
	// a q:dns depth request (gate.Len → rdb); before T-232 all three
	// collectors lived fire-and-forget. The join is a defer registered
	// AFTER the defer-closes above → it executes BEFORE them (LIFO). A
	// WaitGroup, not a channel of a capacity: the join waits for exactly
	// what was started (Add/Done at the launch site). The invariants are
	// pinned by the structural test TestRunBackgroundJoin (main_test.go,
	// the checker — internal/bgjoin).
	ctx, cancel := context.WithCancel(ctx)
	var bg sync.WaitGroup
	defer func() {
		// Normally the ctx is already cancelled by the framework by the
		// moment run returns; an early panic stops the still living
		// collectors here — before the defer-closes (LIFO).
		cancel()
		bg.Wait()
	}()

	// Refunding the stuck q:dns deliveries (T-025): RecoverLoop in the
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

	// The depth of the consumed q:dns stream → crawler_queue_depth
	// (T-077): the depth source — the gate (the same Redis client), it
	// finishes on the ctx cancellation.
	bg.Add(1)
	go func() {
		defer bg.Done()
		_ = metrics.CollectQueueLoop(ctx, reg, gate.Len,
			[]string{queue.StreamDNS}, metrics.DefaultQueueCollectInterval, logger)
	}()

	// The state of the DNS limiter → gauges (T-035 → T-077): the effective
	// caps after the adaptive, the share of timeouts in the window, the
	// exchanges in flight — a quiet degradation of the resolver (a
	// reduction to the floor) is visible by the metric, not only by the
	// growth of temp_error (DNS-06).
	bg.Add(1)
	go func() {
		defer bg.Done()
		_ = dnsworker.CollectLimitLoop(ctx, reg, resolve.Stats, 0, logger)
	}()

	// The switchovers to the reserve → counters (T-107 → spec 5.9): the
	// pace of crawler_dns_fallback_switches_total — a degradation of the
	// local unbound with a live reserve (the limiter is "healthy" at that
	// moment — the adaptive sees the final success),
	// crawler_dns_fallback_exhausted_total — nobody answered, the
	// tomorrow temp_errors.
	bg.Add(1)
	go func() {
		defer bg.Done()
		_ = dnsworker.CollectFallbackLoop(ctx, reg, fallback.Stats, 0, logger)
	}()

	// The main loop lives until the ctx cancellation (graceful shutdown);
	// the join of the background collectors — the defer above: any exit of
	// run (graceful, a panic) first stops and waits for them, and only
	// then the LIFO closes rdb and the clients of the resolver chain.
	return dnsworker.Run(ctx, pool, q, resolve, dnsworker.Options{
		Concurrency: cfg.DNS.Concurrency, // the episode concurrency = dns.concurrency (spec 5.10)
		AskAAAA:     cfg.DNS.IPv6,        // AAAA is disabled by the config without IPv6 (spec 5.4)
		Recover:     rel,                 // refunding the stuck q:dns batches (T-025)
		// The pace of the stuck refunds (T-153): the batch/interval of the
		// claim — redis.recover_*.
		RecoverBatch:    cfg.Redis.RecoverBatch,
		RecoverInterval: cfg.Redis.RecoverInterval.D(),
		Metrics:         reg, // the stage instruments: the episodes, the durations, the statuses (T-077)
		Log:             logger,
	})
}

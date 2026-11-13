# Go Service Framework

The `internal/service` package (T-001, T-007, T-012) is the common
scaffolding for all long-lived services of the project (everything except
the `crawlerctl` CLI and testbed utilities). `service.Run` performs the
standard startup and shutdown sequence on behalf of the service, while the
service supplies only its working logic.

## Lifecycle

```
config.Load (in main)
  → service.Run(Options)
      logger (slog) + slog.SetDefault
      → PostgreSQL migrations (advisory lock, spec 6.3)
      → DB pool (Options.OnDBPool, internal/db)
      → HTTP /livez + /healthz + /metrics (Options.MetricsAddr)
      → Options.OnRun(ctx)                — working logic
      ⟵ SIGTERM/SIGINT | OnRun error | HTTP error | OnRun completion
      shutdown (spec 3.4):
        1. cancel the working-logic ctx and wait for it to return
           (budget Options.ShutdownTimeout, default 10 s);
           a second signal — forced exit without waiting;
        2. the metrics HTTP server (waits for scrapes already in progress);
        3. the DB pool closes last.
```

`Run` logs lifecycle errors itself (structured, with service context) and
returns them to the calling `main` — for which `os.Exit(1)` is enough:
all `cmd/*/main.go` are built the same way.

## Logging

`service.NewLogger(name, cfg)` — a structured logger based on the stdlib
`log/slog` (no new dependencies): text format on stderr, level from the
`log.level` configuration key (`CRAWLER_LOG_LEVEL`, default `info`),
every record tagged with a `service=<name>` attribute. `Run` makes the
logger the default (`slog.SetDefault`), so migrations and any service code
via `slog.Default`/`slog.Info` write into the common stream. If `main`
created the logger itself (to log before `Run`), it passes it in
`Options.Logger`.

## Options for the Working Logic

| Option | Purpose |
|---|---|
| `OnRun(ctx) error` | the working logic (workers of stages 2+): the context is cancelled on SIGTERM/SIGINT — on cancellation, finish the current task or return it to the queue (spec 3.4) and return. Returning without a signal stops the service: `nil` — cleanly (one-off tasks), an error — with exit code 1 |
| `OnDBPool(pool)` | get the PostgreSQL pool (after migrations); the framework closes the pool last at shutdown |
| `DBAfterConnect(hook)` | AfterConnect hook with session settings for each pool connection (T-171/T-192/T-201): `db.NoParallelGather` — monitoring pools, scheduler and http-worker (retention drain), `db.LockTimeout(db.DefaultLockTimeout)` — write pools of dns-worker/http-worker/scheduler (cap on row-lock wait of 30 s), composition — `db.JoinHooks`; nil — none ([db.md](db.md) §"AfterConnect hooks") |
| `OnSIGHUP()` | reload Laravel rules without a restart (`make rules-reload`, spec 5.8) |
| `ExtraMux(mux)` | extra handlers for the metrics HTTP server |
| `Metrics` | the service's ready Prometheus registry (T-077): main creates the registry for the working-logic instruments before `OnRun` starts, the framework serves it via `/metrics`; nil — the framework creates the registry itself |
| `Logger` | a ready logger (otherwise the framework will create one) |
| `ShutdownTimeout` | shutdown budget (otherwise 10 s) |

Schema of a service with a DB:

```go
cfg, err := config.Load()
if err != nil {
    log.Fatalf("[scheduler] configuration: %v", err)
}
logger := service.NewLogger("scheduler", cfg)
if err := service.Run(service.Options{
    Name:        "scheduler",
    Config:      cfg,
    Logger:      logger,
    MetricsAddr: service.EnvOr("METRICS_ADDR", ":9102"),
    OnDBPool:    func(pool *pgxpool.Pool) { svc = newWorker(pool, cfg, logger) },
    OnRun:       func(ctx context.Context) error { return svc.Run(ctx) },
}); err != nil {
    os.Exit(1)
}
```

## Metrics

Spec 5.9: a Prometheus HTTP endpoint on every service. The framework brings
up `/livez`, `/healthz` and `/metrics` on `Options.MetricsAddr` (the
address of `monitor` — `monitor.listen` from the configuration, spec 5.10,
takes precedence over `METRICS_ADDR`; addresses of the other services —
only `METRICS_ADDR`, dev ports `:9101`…`:9108`).
Prometheus scrapes each service separately (dev config
`deploy/docker/prometheus/prometheus.yml`, profile `monitoring`):
`monitor.listen` is the HTTP endpoint address of the monitor service
itself, not an aggregator of other services' metrics.

| Service | Address | Set by |
|---|---|---|
| monitor | `:9100` | `monitor.listen` (spec 5.10), takes precedence over `METRICS_ADDR` |
| ingest | `:9101` | `METRICS_ADDR` |
| scheduler | `:9102` | `METRICS_ADDR` |
| dns-worker | `:9103` | `METRICS_ADDR` |
| http-worker | `:9104` | `METRICS_ADDR` |
| render-worker | `:9105` | `METRICS_ADDR` |
| analyzer | `:9106` | `METRICS_ADDR` |
| web | `127.0.0.1:9107` | `METRICS_ADDR` (loopback: the landing page is public on `web.listen`, metrics are not, spec §9) |
| dashboard | `127.0.0.1:9108` | `METRICS_ADDR` (loopback: the dashboard is public on `dashboard.listen`, metrics are not, dashboard spec §4.2) |

### Liveness and Readiness Probes (T-110)

| Probe | Semantics | What it checks | Consumer |
|---|---|---|---|
| `/livez` | liveness: the process is alive | the process's HTTP loop responds; does not touch dependencies | compose-healthcheck of the application services (`docker ps`, `up --wait`), the operator |
| `/healthz` | readiness: dependencies | the PostgreSQL pool with a short Ping (503 when unavailable, C-03); a service without a pool — always 200 | operator (`curl 127.0.0.1:91xx/healthz` in the systemd deploy, `dc exec` in dev) |

`/livez` deliberately does not touch the DB/Redis: their unavailability is
not process death, and restarting workers on a PostgreSQL failure would
mean treating the symptom. The framework keeps no separate "background
loop has not panicked" signal: a panic/error/termination of `OnRun`
stops the whole process anyway (`Run` returns, the orchestrator restarts
per policy) — the state "loop dead, process alive" does not outlive the
shutdown budget. A panic of the delivery goroutine (processing a
q:http/q:render task) is not at the `OnRun` level and is accounted for by
the recover of both workers (q:http — since T-262, parity with
renderworker): an Error log with the stack, the circuit-breaker counter +1,
and delivery without XACK returns via claim — the unit survives the panic
of a single task (specification and coverage boundaries —
`internal/httpworker/readerstate.go`, `internal/renderworker/
readerstate.go`).

`/healthz` is not in the compose-healthcheck (T-110): workers pull tasks
from Redis streams themselves — there is no "admit traffic" router, and
with the DB unavailable a wall of unhealthy workers would merely duplicate
the status of postgres itself (which has its own healthcheck). Redis is
also not part of the probe: Redis clients live inside the workers' `OnRun`
(invisible to the framework), there is no cheap readiness check, and
pulling the dependency into the probe path would change the readiness
semantics; Redis availability for the pipeline is visible via
`crawler_queue_depth` and logs. For monitoring, the liveness of a service
is captured by the `up` metric of Prometheus scrapes (`/metrics`); it does
not read the probes. The systemd deploy has no healthchecks or watchdog
(`Restart=on-failure` by exit code, T-085) — both probes are available to
the operator there on loopback.

The metrics registry is `metrics.New(name)` (T-076): a dedicated
`*prometheus.Registry` per service (same-named metrics of different
services do not conflict) with the framework metrics —
`crawler_service_up{service}`,
`crawler_build_info{service,mode,version,revision}` (mode: `dev-stub` —
dev builds without injection, `prod` — production builds via `-ldflags -X
…buildMode=prod`; version/revision — the version and short commit hash of
the build, injected by the same `-X …buildVersion=…`/`…buildRevision=…`
from git: `git describe --tags --always --dirty` and `git rev-parse --short
HEAD`, with git unavailable — `unknown`; dev builds — `dev` and an empty
revision; an empty label value in PromQL is equivalent to the label being
absent) and the Go/runtime and
process collectors (uptime is derived from `process_start_time_seconds` in
PromQL).

The registry is the metric factory of the working logic: `Counter`/`Gauge`/`Histogram`
(+ `Must` variants) create a vector with a unified first label `service` and
return it curried — the `service` value is substituted
automatically, the caller passes only the values of the remaining labels:

```go
tasks := registry.MustCounter(prometheus.CounterOpts{
    Name: "crawler_stage_tasks_total",
    Help: "Pipeline domains processed, by stage.",
}, metrics.LabelStage)
tasks.WithLabelValues("dns").Inc() // the service label is filled in automatically
```

Metric names start with `crawler_`, counters carry the `_total` suffix;
HELP/TYPE are exported by Prometheus from `Name`/`Help` — the metric
documentation is written where the instrument is created. Unified label
names are package constants: `LabelService` (never passed manually),
`LabelStage`
(pipeline stage: dns/http/render/analyze), `LabelQueue` (queue name:
`q:dns`/`q:http`/`q:render`/`q:analyze`/`q:dead`), `LabelType` (error
type).

Re-registering an identical metric (same name, help and labels) is not
an error: the already registered vector is returned, so instrument
constructors can be called repeatedly in tests (the Prometheus
`AlreadyRegisteredError` does not panic; for histograms — the buckets of
the first call win, `Buckets` is not part of the descriptor). A descriptor
conflict
— same name with a different help, label set or type — is an error
(`Must` variants panic): that is a programming error, not a normal case.
The reserved `service` label and label duplicates are rejected with an
error before the vector is created.

The stage instruments themselves (spec 5.9: per-stage throughput, queue
lengths, errors by type, render share, response time, backlog depth,
worker memory) are registered by the services through the `NewMetrics`
constructors of their worker packages (`Options.Metrics` — the service
registry; `nil` — isolated builds without metrics) and collectors
(`internal/metrics/collect.go`, `internal/scheduler/backlog.go`).

### Working Logic Metrics (T-077)

| Metric | Type | Labels | Exported by | Increment point |
|---|---|---|---|---|
| `crawler_stage_tasks_total` | counter | `stage` | dns-worker (`dns`), http-worker (`http`), render-worker (`render`), analyzer (`analyze`) | a completed stage task: a domain (dns, http) or a subdomain×variant record (render, analyze) |
| `crawler_stage_seconds` | histogram | `stage` | the same four | duration of the same task (per-stage buckets) |
| `crawler_stage_errors_total` | counter | `stage`, `type` | http-worker (`timeout`/`redirects`/`tls`/`body`/`network` — ErrorClass classes, §8), render-worker (`task`/`context`/`browser` — render.Class classes) | every failed protocol check / unconfirmed render delivery |
| `crawler_domain_status_total` | counter | `type` | dns-worker (`not_delegated`/`delegated_no_address`/`temp_error`/`invalid` — poisoned-row diagnosis, T-109), http-worker (`alive`/`no_http`/`temp_error`) | the final domain status of a terminal episode branch (spec 6.2) |
| `crawler_laravel_near_findings` | gauge | — | monitor (T-115) | Laravel findings in the gray zone of the threshold `[min_score/2, min_score)` — rule calibration material |
| `crawler_laravel_appeared_total` / `crawler_laravel_lost_total` | gauge | — | monitor (T-118) | cumulative appearances/disappearances of `laravel_domains` rows (the `laravel_history` journal, fresh HTTP episodes); monotonic — counter semantics |
| `crawler_redis_memory_bytes` | gauge | `kind` | monitor (T-154) | the same pass as `crawler_redis_memory_ratio`: `kind=used` — `used_memory`, `kind=max` — `maxmemory` (0 — no cap) from `INFO memory` |
| `crawler_http_response_seconds` | histogram | — | http-worker | every reachable uncached check: time to the final page's headers (ResponseMS, spec 5.5 item 7); cache hits do not touch the network |
| `crawler_http_per_ip_wait_seconds` | histogram | — | http-worker (T-165) | every successful acquisition of a per-IP connection semaphore slot with the pure queue wait time (an instant slot — microseconds; cancelled waits are not observed — error classification counts those); the unit is a dial (a task makes several connections); the bucket `le="2"` is the boundary of the head-of-line criterion for parked IPs: the share of waits > 2 s = `1 − rate(…_bucket{le="2"})/rate(…_count)`, the norm is < 5 % on the production mix; material for `http.per_ip_overrides` |
| `crawler_http_dead_host_cutoff_tasks_total` | counter | — | http-worker (T-164) | q:http tasks completed with the dead-host cutoff: apex and www failed on the connect phase (connection refused/connect timeout, both protocols), the remaining subdomains were not checked; control of the task mean ≤ 7 s criterion — `crawler_stage_seconds{stage="http"}` |
| `crawler_http_dead_host_cutoff_skipped_checks_total` | counter | — | http-worker (T-164) | checks (subdomain × protocol) not performed because of the cutoff — the task's saved pool; they do not count as stage errors (`crawler_stage_errors_total` has no `cutoff` series); the share of false cutoffs (<1 %, the criterion) is measured by a control sample over the `dead_host_cutoff` bit in `domains.flags` |
| `crawler_render_records_total` | counter | `rendered`, `engine` | http-worker (`false` — result records, without `engine`), render-worker (`true` — replaced with DOM) | render share = `rate(…{rendered="true"}) / rate(…{rendered="false"})`; the `engine` label (T-235, FR-15) is the render engine of the render-worker build (`render.engine`, chromium/moli/lightpanda, cardinality 3): existing aggregations and the render share without `engine` do not change (a new label does not affect groupings without it) |
| `crawler_render_tarpit_finalized_total` | counter | `engine` | render-worker (T-148, tarpit counting — T-261) | a record finalized without render due to exhaustion of the tarpit budget — the count of tarpit-classified deliveries of the task (the set `render:tarpit:{task_id}`, SCARD ≥ `render.timeout_attempts`; before T-261 the threshold was the PEL delivery number): the outcome is the raw HTML of the HTTP phase, the `render_failed_at` stamp (T-210 — the rework pass does not republish the record until the content changes), publication to q:analyze, XACK (tarpits do not reach q:dead); growth — the share of timeout tarpits at render; the `engine` label (S-P2-4) is the render engine of the worker build (`render.engine`, chromium/moli/lightpanda): a tarpit is an engine-branched failure (a page stuck on a lightweight engine may open in chromium), the label attributes it to the engine — material for the engine tarpit rate (the "Render engines" panel); aggregations without `engine` do not change |
| `crawler_render_cert_finalized_total` | counter | — | render-worker (T-159) | a record finalized without render due to a permanent TLS certificate error of the host (`net::ERR_CERT_AUTHORITY_INVALID`/`ERR_CERT_COMMON_NAME_INVALID`, with T-172 also `ERR_CERT_DATE_INVALID` — measurement: 0 successful retries out of 321), from the first delivery: the cert class does not burn delivery attempts and does not reach q:dead (the `render_failed_at` stamp, T-210); growth with zero q:dead growth — the policy works |
| `crawler_render_dns_finalized_total` | counter | — | render-worker (T-211) | a record finalized without render due to a host DNS failure (`net::ERR_NAME_NOT_RESOLVED` — the name does not resolve; the record reached q:render via a live HTTP episode, a DNS recovery on a scale of days would not fit the retry window; measurement T-207: 252 finalizations in ~7 h), from the first delivery: the DNS class does not burn delivery attempts and does not reach q:dead (the `render_failed_at` stamp, T-210); `ERR_CONNECTION_REFUSED` is not in the class — recovery is observed (6.6% of retries, T-172), it stays re-tryable until the last delivery |
| `crawler_render_task_finalized_total` | counter | `reason`, `engine` | render-worker (T-172) | a record finalized without render due to an error of the render task class (connection refusal, load error — a property of the host) on the last delivery of `redis.max_attempts`: the outcome is the raw HTML of the HTTP phase, the `render_failed_at` stamp (T-210), publication to q:analyze, XACK; the reason label is the `net::ERR_*` code (e.g. `net::ERR_CONNECTION_REFUSED`), `timeout` or `other`: the composition of what used to carry records to q:dead; the `engine` label (T-235, FR-15) is the render engine of the worker build (`render.engine`, chromium/moli/lightpanda), aggregations without `engine` do not change |
| `crawler_render_fallback_published_total` | counter | `reason` | render-worker (T-238) | a task published to the fallback queue `q:render-fallback` from the finalize-without-render path (engine-branched causes — the tarpit T-148, the task class T-172 on the last delivery — with `render.fallback.enabled` on): the record stays without the `render_failed_at` marker, chromium worker B reworks it; the reason label is the same `net::ERR_*`/`timeout`/`other` vocabulary, the numerator of the fallback rate (FR-15); a duplicate delivery is not counted (no publication was performed) |
| `crawler_render_fallback_suspended_total` | counter | `reason` | render-worker (T-239) | fallback publication suspension events (FR-9), one per latching, not a state: `gate` — the health gate, the streak of browser-class infra errors (the primary engine unavailable) since the last successful render reached `render.fallback.health_gate_streak`, publication suspended until recovery (success resets the streak; task/context class errors do not touch the streak); `budget` — the share of publications over a window of 1000 confirmed deliveries exceeded the cap `render.fallback.share_percent`, the next window goes without publications (self-release on closing). While the suspension holds, fallback candidates are finalized normally with the `render_failed_at` marker (as with the fallback off) — tasks are not lost; growth — a signal of primary engine degradation (alerts of spec 4.6 item 3). The series of the closed vocabulary `gate`/`budget` are pre-created as zeros (S-P1-4): the family is in the exposition from the first start — the budget alert is not "not measured" on a quiet production and does not lose the first 0→1 event; only the armed build grows the series (the candidate engine, the q:render stream) |
| `crawler_render_fallback_consumed_total` | counter | `result` | render-worker, worker B build (T-245) | `q:render-fallback` tasks consumed by chromium worker B (the consumption stream `RENDER_WORKER_STREAM=q:render-fallback`), by the outcome of a delivery that finished processing without error: `rendered` — the rework succeeded (the DOM written, `rendered=true`, publication to q:analyze); `finalized` — the fallback is exhausted, the record was closed via finalization (the `render_failed_at` marker + q:analyze, FR-10: worker B sets the marker, not the primary; the tarpit T-148 / task class T-172 / cert T-159 / DNS T-211 paths — the marker is placed while the row belongs to the episode, a race with the recheck T-058 closes the record without the marker); `skipped` — a normal refusal without side effects (no row, no final URL, or the UPDATE guard rejected it — the row slipped out from under the task). Counting is up to XACK at the queue level: an Ack failure/duplicate delivery yields a duplicate (an allowance of units at-least-once, like the budget window T-239); failed deliveries are not counted — they live in the PEL until claimed or buried into q:dead; the series of the three outcomes are pre-created as zeros in all builds (S-P1-4), only worker B grows them — the primary q:render build stays at zeros. The denominator of the acceptance balance check (render spec §6.2 item 4) — see the `crawler_queue_depth` row |
| `crawler_ingest_bad_hashes_total` | counter | — | ingest | the outcome of each CleanupDeleted pass (the `CleanupOptions.OnPass` hook, T-099): deleted results with a wrong-length `content_hash`; normally it does not grow at all — growth = a result writer error (T-056) |
| `crawler_render_late_republish_total` | counter | — | http-worker (T-259) | q:render republications by the late republisher: "forever young" records (not rendered, without the marker, with a render target) older than 6 h without the `render_pending` bit and without unconsumed `q:render`/`q:render-fallback` messages — tasks lost to MAXLEN truncation and publication failures; stationary it does not grow, growth after an incident — the drain of losses, a fall to zero — the drain is complete (render.md §"Late republisher") |
| `crawler_render_late_errors_total` | counter | — | http-worker (T-273) | failed passes of the late republisher (`RenderLateLoop`, next to a Warn): the selection/snapshot/publication of a batch did not finish — the cursor does not move, the batch repeats after a backoff; series growth with zero republications = the loop is not working (the error text — a Warn in journalctl), with `render_pending` > 0 the render debt will not return to q:render |
| `crawler_content_retention_rows_total` / `_refs_total` / `_contents_total` / `_freed_bytes_total` | counter | — | http-worker (T-166) | the outcome of each RetentionLoop pass (the `RetentionOptions.OnPass` hook): `subdomain_results` rows cleared of content/jsonb, truncated `content_hash` references, deleted `contents` rows and their compressed bytes (the space physically returns with pack compaction T-045 — convergence is visible via `disk_avail`); growth stopping with a working loop = candidates ran out (a plateau) |
| `crawler_content_retention_skips_total` | counter | — | http-worker (T-166) | pass skips by the interlock: the q:analyze depth at the `content.retention_analyze_watermark` threshold — the analyzer cannot keep up, bodies and headers/cookies are still needed; growth = analysis is falling behind, the disk keeps growing |
| `crawler_content_retention_pending_errors_total` | counter | — | http-worker (T-166/T-167) | pass skips due to an error reading unconsumed q:render/q:analyze keys (conservatively — queue records untouched); growth = the guard cannot read the streams, investigate immediately: without it the cleanup window of waiting records is open |
| `crawler_content_retention_deferred_total` | counter | `stream` | http-worker (T-166/T-167, fallback — T-238) | records excluded from cleanup: messages waiting for stream consumers (`q:render` — SPA render, `q:render-fallback` — rework by chromium worker B, `q:analyze` — analysis; a record may wait on several — counted in each); grows on queue stalls — a signal to drain the consumers (runbooks §5.2) |
| `crawler_content_retention_pass_seconds` / `crawler_content_retention_passes_total{result}` | histogram / counter | `result` | http-worker (T-166) | the duration and count of retention passes (`ok` — the drain was performed, `skipped` — the interlock, `skipped_pending` — a guard read error; alerts over `rate(passes{result="skipped"})` and `rate(passes{result="skipped_pending"})`) |
| `crawler_content_retention_errors_total` | counter | — | http-worker (T-273) | failed retention passes (`RetentionLoop`, the `OnError` hook, next to a Warn): the pass aborted on an error — batches committed before it stay in force (the cursor advanced partially), the remainder repeats after a backoff; series growth with zero `passes{result="ok"}` = the drain is stalled, check the Warn heading in journalctl and the content disk |
| `crawler_content_retention_bad_hashes_total` / `crawler_content_retention_missing_refs_total` | counter | — | http-worker (T-166) | accounting anomalies: truncated references with a wrong-length hash / without a `contents` row; normally they do not grow at all — growth = a writer error (T-056) or a refcount mismatch (T-044) |
| `crawler_analyzer_laravel_unfound_total` | counter | — | analyzer (T-187) | Laravel finding removals by re-analysis (a delivery without fresh): the domain left `laravel_domains` without a fresh HTTP episode; each is answered by a point drain of the domain's content (`content.DrainDomain`) |
| `crawler_analyzer_unfound_drained_rows_total` | counter | — | analyzer (T-187) | `subdomain_results` rows cleared by the point drain upon a finding removal by re-analysis: without the drain they would be left behind the retention cursor until the rare catch-up circles (docs/content.md) |
| `crawler_analyzer_unfound_drain_errors_total` | counter | — | analyzer (T-187) | point drain failures (a Warn in the log): the domain's records are left behind the cursor — the rare catch-up circles T-185 or the recheck T-030 will pick them up; growth = investigate the failure cause |
| `crawler_unit_oom_events` | gauge | `unit`, `kind` | monitor (T-145) | the alerts pass (5 min, T-206): memory.events counters of cgroup v2 of the `crawler-*.service` units (kind=oom/oom_kill); reset by cgroup recreation on a unit restart — a gauge, not a counter; growth = OOM events (the unit_oom alert) |
| `crawler_web_checks_total` | counter | `result` | web (T-221) | every `POST /api/check-domain` request by outcome: `ok` — the live check succeeded (200), `stored_only` — the live check was failing, the answer came from the stored data (200), `failed` — the live check was failing with nothing stored (502), `busy` — overload of the live-check semaphore (503, T-220), `rate_limited` — the form rate limit (429), `invalid` — junk input (422) |
| `crawler_web_check_errors_total` | counter | `type` | web (T-221) | form failures by class: `live` — the live-check pipeline failed (overload is the `busy` outcome, not an error), `journal` — the `web_checks` INSERT failed (a manual-check journal row lost — before T-221 it was lost silently), `non_ip` — the client address is not an IP (a unix-socket peer without a valid proxy header), the journal row is skipped (a Warn in journald) |
| `crawler_web_check_seconds` | histogram | — | web (T-221) | the total processing time of a form request after the limit and validation (stored read, live check, journal write; 429/422/the 503 overload are not observed — no work was performed) |
| `crawler_unit_memory_high_events` | gauge | `unit` | monitor (T-162) | the same pass as unit_oom: the memory.events high counter (hits over the soft MemoryHigh cap — reclaim throttling); accumulates over the cgroup lifetime, growth is what is meaningful — the `memory_high` alert on growth over ≥2 passes in a row (T-162: the harbinger of http-worker fatals, T-157) |
| `crawler_psi_cpu_some_percent` | gauge | `kind` | monitor (T-162) | the alerts pass: the some share of CPU stall time from the host's `/proc/pressure/cpu` (kind=avg10/avg60/avg300); a sustained avg300 >90 % — scheduler starvation (the `psi_cpu` alert, T-162), a kernel without PSI — no series |
| `crawler_unit_restarts` | gauge | `unit` | monitor (T-145) | the same pass: `NRestarts` of the units (crash restarts since the last start/`reset-failed`); the source of both — `systemctl show` + cgroupfs reading (internal/monitor/unitevents.go) |
| `crawler_queue_depth` | gauge | `queue` | dns-worker (`q:dns`), http-worker (`q:http`), render-worker (`q:render`; the worker B fallback build — `q:render-fallback`, T-236/T-240), analyzer (`q:analyze`), monitor (`q:dead`, T-154) | the depth collector of the consumed stream every 15 s (the consumption group backlog: `min(lag, XLEN)` + `pending` — the lag is bounded by the stream length against the Redis 7.0.x phantom after XTRIM of undelivered entries, T-168; `metrics.CollectQueueLoop`, the source — the backpressure gate T-026); `q:dead` is published by monitor from the `queue_dead` alert pass (the quarantine has no groups — XLEN, the `monitor.NewDeadDepthMetrics` wrapper); the `queue="q:render-fallback"` series — the remainder for the fallback balance check (T-245: `published = consumed + remainder + buried` — the formula and applicability boundaries in the decisions below, step 4 of the render spec acceptance §6.2) |
| `crawler_render_pending_domains` | gauge | — | monitor (T-175) | the render-debt loop (`monitor.render_debt_interval`, 5 min): domains with the `render_pending` bit — the `crawler_counters['render_pending']` counter (migration 000015) tracks the bit transitions in the transition transaction; the authoritative value is periodically rewritten by the T-178 reconciliation (observability without a seq scan over 41.8M rows; the `q:render` lag — `crawler_queue_depth{queue="q:render"}`, the debt = the sum — the `render_debt` alert and the load report row T-173) |
| `crawler_domains_new_done_stale` | gauge | — | monitor (T-256) | "status hung" domains (FSM spec §3 FR-2.2): `status = new ∧ check_state = done ∧ last_checked_at` older than a day — the DNS stage wrote the "has addresses" branch (O6 of the status decision table, `internal/model/enums.go`), the HTTP outcome did not arrive (a lost `q:http` task). The value is the FILTER column `f_new_done_stale` of the reconciliation pass (the `crawler_counters['new_done_stale']` row), refreshed once per `monitor.zone_stats_interval`; stationary 0 normally, non-zero — the `domains_new_done_stale` alert (2 non-zero reconciliation snapshots in a row): the cohort is treated by the T-270 compensator (`scheduler/stalerequeue.go`, the `crawler_scheduler_stale_requeued_total` counter), the gauge is a signal about new losses |
| `crawler_queue_dead_buried_total` | counter | `queue` | dns-worker, http-worker, render-worker, analyzer (T-154) | every translation into q:dead of a task with a task_id (`Reliab.bury` after publishing Dead and XACK of the original record; the `ReliabOptions.OnBury` hook ← `metrics.NewDeadBuriedHook`) — the `queue` label is the stream source of the dead task; translations of unidentifiable records (without `task_id`) are counted by the neighboring `crawler_queue_dead_unidentified_total`; the death rate — `rate()`, the untriaged remainder — the monitor's q:dead depth |
| `crawler_queue_dead_unidentified_total` | counter | `queue` | dns-worker, http-worker, render-worker, analyzer (T-260) | quarantine of unidentifiable records: a PEL record without `task_id` (a body outside the package formats) exhausted its attempts and was moved into q:dead with a synthetic `task_id` `unknown:<stream>:<entry-id>`, without subject fields (`Reliab.bury`, the terminal branch of FR-6.1 of the FSM spec; the `ReliabOptions.OnBuryUnidentified` hook ← `metrics.NewDeadUnidentifiedHook`) — before T-260 such records spun in an endless claim loop in the PEL; stationary zero, growth — records are published bypassing the package contract (a manual XADD, a writer bug): find the source by `entry_id`, triage — runbooks.md §2 |
| `crawler_db_deadlock_retried_total` | counter | — | dns-worker, http-worker, scheduler (T-268) | DB statement retries after a 40P01 deadlock (`db.RetryDeadlock`, a guard on top of the counter lock-ordering invariant T-267): http-worker — the whole result-write transaction (`RecordAndPublish`), dns-worker — the autocommit statements of episode recording (`RecordEpisode`: the transition to done, the `retry_mature` bucket increment, the invalid diagnosis) without a repeated DNS episode, scheduler — the temp_error retry decision transaction (`retryDecide`); the retry drain survives a batch error (up to 3 in a row). Single events are normal (the guard works, the task does not go into redelivery as a full episode); sustained growth — a new lock cycle: triage in runbooks.md §2.8 |
| `crawler_redis_memory_ratio` | gauge | — | monitor (T-154) | the alerts pass: `used_memory/maxmemory` from `INFO memory` (the source wrapper `monitor.NewRedisMemoryMetrics`); with noeviction — a proxy metric of upcoming OOM write refusals (the `redis_memory` alert at ≥ 0.8); `maxmemory` unset — the series is not written |
| `crawler_backlog_domains` | gauge | `state` | scheduler (`pending`, `queued`) | sums of the zones' incremental counters (T-178: `sum(zones.domains_pending/queued)`, the base set `status <> deleted`) with the period `scheduler.backlog_interval` (default 5 min) — `scheduler.BacklogLoop`: reading the reference table instead of traversing 47M rows (ingest spec §8.4) |
| `crawler_backlog_pass_seconds` / `crawler_backlog_skipped_ticks_total` | histogram / counter | — | scheduler (T-161) | every backlog pass: the duration (a pass is reference-table sums, T-178) and the skipped single-flight ticks (a pass longer than the interval — the accumulated tick does not start a new pass, a Warn in the journal); growing skips — raise `scheduler.backlog_interval` |
| `crawler_scheduler_lease_null_returned_total` | counter | — | scheduler (T-255) | the terminal guard of the T-028 invariant "queued ⇒ queued_at NOT NULL": a repair pass of the LeaseWatch loop (`LeaseNullOnce`, FR-1.2) returned `check_state=queued` rows with NULL `queued_at` to pending (without the guard — an eternal hang: NULL is filtered out by comparing lease-pass age; the automaton specification — `internal/scheduler/checkstate.go`). Each event is one returned row; stationary zero, a non-zero growth rate — manual SQL or a writer bug setting queued without the stamp: triage in runbooks.md §2.9 |
| `crawler_scheduler_stale_requeued_total` | counter | — | scheduler (T-270) | the "status hung" compensator (FR-2.2 of the FSM spec, transition R15 of the `check_state` specification): the hourly `StaleRequeueLoop` cycle (`StaleRequeueOnce`) returned to pending the domains `status=new ∧ check_state=done` with `last_checked_at` older than a day — the q:http task was lost after XADD (a Redis restart without persistence, an OOM window); neither the lease pass (queued only) nor the age recheck (recheck_interval, months) returns the row; the selection is a seq scan with early stop under the guard of the `new_done_stale` row (stationarily one PK lookup per tick, domains is not scanned). Each event is one moved row (via RETURNING after the guards); stationary zero, growth — q:http losses (the level — the `crawler_domains_new_done_stale` gauge, the moment signal — the `redis_restart` alert); at the day boundary a non-empty q:http yields a normal midnight influx of live deliveries (the window matures in cohorts at midnight by the date column — a duplicate episode is extinguished by asymmetry A3, not losses): triage in runbooks.md §2.10 |
| `crawler_counters_recount_seconds` / `crawler_zonestats_skipped_ticks_total` | histogram / counter | — | monitor (T-161/T-178) | every pass of the combined counter reconciliation (the aggregates domains/laravel_domains/laravel_history → zones + crawler_counters): the duration (buckets 0.5 s…~8.5 min — measurement §12: tens of seconds over 47M rows) and the skipped single-flight ticks; `crawler_counters_recount_seconds` is a rename of `crawler_zonestats_pass_seconds` (T-178) |
| `crawler_counters_drift` | gauge | `counter` | monitor (T-178) | drift of the incremental counters at the reconciliation pass: \|old − new\| over the positions `zone:<id>:<column>` / `global:<name>` / `daily:checks` (status rows are outside the drift — written by a single reconciliation); the stationary value tends to zero (a pass is three statements under READ COMMITTED, a single transient drift under load is normal), a sustained non-zero one — a missed transition or manual SQL bypassing the code (a Warn in the journal, triage — `docs/runbooks.md`); exceptions — streaming positions: `daily:checks`/`daily:temp_error` (T-179/T-182: HTTP result-write events against the domains of the day across all pipeline branches), `retry_mature:past`/`retry_mature:today` (T-202: temp_error episode/retry events against the live domains of the maturity filter), `rendered:prev`/`rendered:cur`/`results:prev`/`results:cur` (T-203: row-insert/transition-to-rendered events against the live rows of the rate window; prev/cur — the previous and current calendar hour) and `reanalyze:prev`/`reanalyze:cur` (T-204: write/render/stamp events against the live NULL rows of the window — the re-analysis tail buckets) — non-zero under live load, observed as gauges, not included in the Warn |
| `crawler_pg_slow_queries` | gauge | — | monitor (T-177) | the slow-query loop pass: client PostgreSQL statements running longer than `monitor.slow_query_min_duration` at the moment of the `pg_stat_activity` snapshot (monitor's own aggregates excluded by `application_name`); hold-last on a source failure |
| `crawler_pg_slow_queries_total` | counter | `app` | monitor (T-177) | detected slow queries by the service's `application_name` (new statements over the threshold, pid+query_start debounce); they duplicate the Telegram message as Prometheus labels |
| `crawler_dashboard_snapshots_total` | counter | `result` | dashboard (`ok`/`error`, dashboard spec §10) | every snapshot-build loop pass: `error` — a pass with degraded sections (the snapshot is published, the causes — a Warn in the log and the `errors` map of /api/status) |
| `crawler_dashboard_snapshot_seconds` | histogram | — | dashboard | the duration of the same pass (buckets 50 ms…~3 min: a pass is the full `internal/status` aggregates) |

Decisions (fixed):

- **Queue depth** lives with the stream consumers: the consumption
  group backlog (XINFO GROUPS + XLEN via the backpressure gate; every
  service already has a Redis client; the group lag is bounded by the
  stream length — a clamp against the phantom lag of Redis 7.0.x after
  trimming undelivered entries,
  T-168), and the binding to a consumer guarantees "a queue is being
  read ⇔ its depth is published". A "raw" XLEN does not fit: it also
  counts confirmed history before the MAXLEN trim. All depth readers
  (the gate, `crawlerctl status`, `crawler_queue_depth`, the
  `queue_frozen` alert
  T-162 and the "📬 Queues" row of the load report T-173 — since T-188) count
  one and the same backlog with the shared `queue.GroupDepth`
  calculation
  (backpressure.go): observation and the gate are not allowed to
  disagree about whether there is work — the 25.10 discrepancy (the
  report printed the raw XLEN and overestimated q:render ~5× with a
  stopped TrimLoop) was closed exactly by it; a stream without groups
  and `q:dead` — XLEN.
  `q:dead` has no consumer (manual triage, spec 4.2) — since T-154 its
  depth is published by monitor itself (XLEN via the same gate wrapper,
  `NewDeadDepthMetrics`, only the `queue="q:dead"` series: duplicating
  the working streams from monitor would double the worker series in
  PromQL), cross-checked with the dashboard highlight (T-126/T-144) and
  the `queue_dead` alert;
  before an observer appeared, the depth was shown only by `crawlerctl
  status` (T-079).
- **The fallback queue remainder for the balance check** (T-245, render
  spec §6.2
  item 4) is taken with the existing depth mechanism, no separate PEL
  metric is introduced: the worker B build consumes `q:render-fallback` — its
  collector already publishes `crawler_queue_depth{queue="q:render-fallback"}`
  (the group backlog: unread lag + PEL of unconfirmed deliveries),
  and exactly this sum is the remainder of the acceptance equation — a
  PEL without lag would underestimate the remainder by the not-yet-read
  part. The full equation of step 4:

  `increase(crawler_render_fallback_published_total)` (worker A, T-238)
  = `increase(crawler_render_fallback_consumed_total)` (worker B, T-245)
  + `crawler_queue_depth{queue="q:render-fallback"}`
  + `increase(crawler_queue_dead_buried_total{queue="q:render-fallback"})`
  + `increase(crawler_queue_dead_unidentified_total{queue="q:render-fallback"})`
  — the second-to-last term: failed deliveries (browser/infrastructure
  class) at the exhaustion of `redis.max_attempts` leave for q:dead by
  bury, exiting the PEL
  without consumption (the OnBury hook is mounted on the B build too);
  the last one —
  unidentifiable records without task_id (T-260): the quarantine with a
  synthetic id leaves the PEL the same way, stationary zero (in
  q:render-fallback — only
  when publishing outside the package contract: a manual XADD or a
  writer bug).
  The check is by `increase()` deltas over a window: the counters live
  with the process,
  a restart of worker A or B resets them independently (a "head-on"
  snapshot is valid
  only between restarts); with the queue drained (worker B reworks it
  to empty, FR-14) the remainder is zero. Applicability boundaries: the
  equality holds while the depth is below the stream's MAXLEN cap
  (~15 000) —
  XADD trimming may cut unread entries when worker B lags behind
  (caught by the depth-growth alert, render spec §4.6 item 3/T-246, not
  by the balance);
  the remainder series is exported only by the worker B process — while
  it is stopped
  the equation term is absent until it returns (an independent depth
  source in
  monitor — task T-246); the stream must keep exactly one group
  (`render-fallback-worker`, invariant T-155) — an extra group would
  inflate the
  remainder by summing over all groups (`BackpressureGate.Len`).
  The integration check — TestAccept03_RenderFallback
  (test/integration/acceptance_test.go): the counters of both builds +
  `BackpressureGate.Len` + a direct XPENDING of group B.
- **Backlog depth** (pending/queued from the DB) — in the scheduler: it
  is the owner of the `domains` states (enqueue T-028, lease return T-029,
  rechecks T-030/T-031), the DB pool and the periodic loop are already
  its. Stream length and backlog are different metrics: Redis is
  trimmed by MAXLEN and lost on failure (the scheduler restores it, spec
  3.4); the DB is the full
  volume of outstanding work.
- **Worker memory** — without dedicated metrics: the Go/runtime collector
  (T-076) already exports `go_memstats_*` (heap alloc/inuse/sys, GC),
  the process collector — `process_resident_memory_bytes` (RSS — exactly
  what systemd `MemoryMax` limits, spec 3.2). Duplicating them with a
  `runtime.MemStats` snapshot makes no sense.
- Status/record counters are counted over a completed episode
  (after a successful DB write, before XACK, spec 4.2): the race with
  domain deletion (§13.1) is not distinguished; duplicate at-least-once
  deliveries count as honest work (spec 3.4).
- The DNS stage does not type exchange errors with a dedicated counter:
  resolver failures reduce to `temp_error` (T-036); the final-status
  counter is the typing of the outcome itself; the analyzer is passive
  (spec 5.8) — its
  results (findings, is_laravel) are DB data, not process metrics.

## Service Working Logic

All seven pipeline services (plus the public `web` of stage 13 — below)
are implemented and wired through `OnRun`/`OnDBPool`
(a service without `OnRun` is also valid: `/livez`, `/healthz` and `/metrics`
are up, it
lives until SIGTERM/SIGINT and shuts down correctly — the framework does
not require working logic).

`ingest` (stage 1, T-013…T-021): its
`OnRun` watches `ingest.dir` through `internal/watch` and runs ready
files through the ingest pipeline of `internal/ingest` (path parsing →
zone → parsing with sha256 → validation → staging → apply → `ingest_files`
report → archiving into `processed/`/`failed/`), plus background cleanups
— deleted domains
(T-020) and orphaned staging batches by `batch_id` age (T-098),
with one CleanupDeleted loop; the file contract and reports are in
[`incoming-files.md`](incoming-files.md). Cleanup pass totals
are exported by the `crawler_ingest_bad_hashes_total` metric (T-099):
the junk `content_hash` counter grows only on a result writer error and
is read by a monitoring alert (the alerts section below).

`scheduler` (stage 2,
T-027…T-031): its `OnRun` keeps the main batch enqueue loop — selection
of pending with zone interleaving (`DispatchBatch`, T-027; round 1 reads
only active zones with work — the `domains_pending > 0` filter, T-183:
the dispatcher becomes a reader of the zones' incremental counters,
ingest spec §8 — the column semantics match the LATERAL selection
predicate, probing an empty zone would have returned 0 rows anyway; the
top-up round quota is bounded by the batch remainder)
→ moving to `queued`
with a lease (`queued_at`) by one batch UPDATE with the pending guard
(the same statement clears the staging bit `http_published` — T-108:
every new
episode starts with a clean publication mark) and publication
of the `DNSBatch` batch to `q:dns` (`EnqueueLoop`/`EnqueueBatch`, T-028;
the "move → publish" order and guarantees — comments in
`internal/scheduler/enqueue.go`); — background loops returning domains
with an expired lease (`LeaseWatch`, T-029), the daily recheck
(`RecheckDaily`, T-030 — does not return poisoned `invalid` rows,
T-109) and re-checks of `temp_error`
(`RetryTempErrors`, T-031; the candidate batch selection is a seq scan
over
`domains` with an early stop `LIMIT` and plan pinning `SET LOCAL` in
the selection transaction: without pinning the planner lands on a partial
`domains_recheck_idx` and degenerates the "older than 24 h" window into
a marathon — slow-log query
№1 of 25.10, T-191; before the selection — a guard by the sum of the
maturity buckets `crawler_counters['retry_mature:…']` of the past days
(T-202,
refinement T-191: the `status_temp_error` counter did not distinguish
mature candidates from immature ones — at low mature density the hourly
tick read all 42.3M rows): the buckets track result-write increments
(both temp_error branches — HTTP and the DNS stage); the decrements are
retry decisions, the authority is reconciliation (the
`retry_mature:YYYY-MM-DD` rows, `docs/schema.md`); a steady state with
no mature candidates does not touch `domains` at all) and the "status
hung" compensator
(`StaleRequeueLoop`, T-270 — an early recheck of `new ∧ done` domains
older than 24 h: a `q:http` task lost after XADD is returned neither by
the lease (the row is not queued) nor by the age recheck (months);
the threshold — the gauge `new_done_stale` window (FR-2.2), the guard —
a point read of the level row: stationarily one PK lookup per hourly
tick, `domains` is not
scanned; the batch selection — the same T-191 technique: a seq scan with
an early
stop `LIMIT` under `SET LOCAL` pinning, the move — a separate
transaction of PK probes under a 40P01 retry; the zone deltas and the
counter
`crawler_scheduler_stale_requeued_total` are maintained by the move
statement,
a duplicate q:http from a revived worker is extinguished by asymmetry
A3). Batch decisions (the retry assignment and the final
status) run as separate transactions under a 40P01 retry (T-268,
`retryDecide` — a full retry, the counter
`crawler_db_deadlock_retried_total`): each observes the lock order
"domains → zones → counters" (the `docs/schema.md` invariant)
— a common transaction held the `retry_mature:` buckets on top of
re-acquiring domains rows (the "counters → domains" edge, a 40P01
deadlock cycle
with the http-worker result write — runbooks §2.8); a failure between
the
halves is safe: the guards are idempotent, the unfinished half —
candidates of the next pass; a batch error does not break the hourly
drain —
up to three failures in a row, then until the next tick (T-268, the
07.11 incident:
118k domains waited for a tick because of a single WARN). The lease
return pass is TWO statements in one
transaction: a SELECT of the batch by the `domains_lease_idx` key
(T-104) with
a composite cursor `(queued_at, zone_id, id)` and a move
`UPDATE … FROM unnest` by PK probes with guard re-checks at the moment
of row locking (T-200, slow-log query №3 of the 25–26.10 window: a
single CTE statement
held an UPDATE side scanning the entire expired range without `LIMIT`,
— the batch cost grew with the range, not with the batch; the drain
cursor
lives in the loop state and survives batches and drains — the dead
prefix
of the lease index between vacuums is read once per process lifetime,
not
by every batch of every drain; a restart resets the cursor — the initial
full drain is preserved). The same loop tick — the terminal guard of the
T-028 invariant (T-255, FR-1.2): the repair pass `LeaseNullOnce`
returns `queued` rows with NULL `queued_at` in batches (a broken
invariant — an eternal hang outside the main lease pass; the
specification of the `check_state` automaton is
`internal/scheduler/checkstate.go`) with the same
guard statement over `domains_lease_idx` (`queued_at IS NULL` — a search
condition on the leading column), the counter
`crawler_scheduler_lease_null_returned_total` and a Warn — the signal
to the operator
(runbooks §2.9). Next
to them — the backlog depth collector
`BacklogLoop` (T-077): the gauge `crawler_backlog_domains{pending|queued}`
as sums of the zones' incremental counters (T-178: the reference table
instead of
a pass over 47M rows, ingest spec §8.4) with the period
`scheduler.backlog_interval` (default 5 min; the corridor is 30 s..1 h).
The passes are single-flight (T-161): they run in the loop goroutine — a
new one does not
start until the previous one finished, and a pass longer than the
interval
drops the accumulated ticker tick; a Warn and
`crawler_backlog_skipped_ticks_total` raise the noise. State moves
(enqueue/lease/recheck/retry) maintain the zone counter deltas
`domains_pending`/`domains_queued`/`domains_checked` with the CTE of the
move statement
(T-178; the lease return — in the pass transaction T-200): the move and
the
delta are atomic, accuracy is restored by the
monitor reconciliation. The `check_state` automaton specification (the
table
"state × event × guards × target × side effects" for all
writers of the column — the scheduler, workers, ingest, crawlerctl) —
`internal/scheduler/checkstate.go` (T-255).

`dns-worker` (stage 3, T-036…T-039): its `OnRun` runs the
`dnsworker.Run` loop (`internal/dnsworker/result.go`):

- consumption of `q:dns` batches by the `dns-worker` consumer group (one
  batch record per read), each delivery processed with the
  `queue.Processing` pattern (T-025) — XACK only after the result is
  written to the DB;
- on every delivery: the `subdomains` reference table is re-read
  (`crawlerctl` changes are picked up without a restart), the batch is
  filtered by one SELECT for currency (`check_state <> done`,
  `status <> deleted`) —
  a redelivery processes only unfinished domains
  (idempotency, spec 3.4); therefore checking an added subdomain over
  already checked domains is the `done → pending` move with the
  `crawlerctl subdomains recheck` command (T-083): the domain passes the
  pipeline again,
  and the DNS stage takes subdomains from the fresh reference table;
- a domain episode (in parallel up to `dns.concurrency`): `CheckApex`
  (T-036)
  → subdomains `CheckSubdomains` and wildcard detection
  `DetectWildcard`/`MarkWildcardSubdomains` (T-037/T-038) → the decision
  (`DecideEpisode`): the terminal branches `not_delegated`/`temp_error`/
  `delegated_no_address` write `domains.status`, found addresses —
  a `q:http` task with the addresses and CNAME (without a terminal
  status: the `alive`/`no_http` outcome is set by the HTTP stage);
- the "publish `q:http` → write `done`" order (the rationale is in the
  comments of `internal/dnsworker/result.go`): a write failure returns
  the episode via redelivery, and the "`done` ⇒ the task is published"
  invariant makes the currency filter the guard against task loss. The
  retry unit is a domain, not a batch (T-108, DNS-04): right after a
  successful publication the task is marked in the domain row with the
  `http_published` bit
  (`domains.flags`, clearing — the result write and the scheduler's
  `pending → queued` move); a batch redelivery skips publishing already
  published domains by the bit and carries them to the write — no
  `q:http` duplicates (a full repeated HTTP check). The duplicate window
  shrank from "a write failure on any domain of the batch × delivery
  attempts"
  (up to `redis.max_attempts × |batch|` tasks) to a worker crash between
  the XADD
  and a single marking UPDATE (or a DB refusal of it — the mark is
  non-fatal);
- the write is one statement per domain (CTE, T-178): `dns_mask`, the
  status (terminal branches), `check_state = done`, `queued_at = NULL`,
  `last_checked_at`, the `temp_attempts` reset on a non-`temp_error`
  outcome
  (the T-031 contract); the same statement clears the `http_published`
  bit
  (T-108); guards `status <> deleted` (§13.1) and `check_state <> done`
  (idempotency); the CTE maintains the zone counter deltas by the old
  row state (the pipeline's main queued/pending → done move:
  the pre-read `FOR UPDATE` → `queued −1`/`pending −1` + `checked +1`,
  review P1-1) — the outer `SELECT count(*)` returns the number of moved
  rows;
- a domain with an empty/blank name in the batch is a poisoned row
  (`ErrEmptyDomain`, a junk/corrupted `domains` row): the terminal
  diagnosis `invalid` (T-109) with a separate UPDATE (`check_state =
  done`,
  the lease finished) + a Warn; the other domains of the batch are
  processed,
  the delivery is acknowledged. The recheck (T-030) does not return
  `invalid` —
  the row does not cycle `queued → lease → dispatch` endlessly; triage
  is
  manual (runbook 2.4);
- exchanges — the `dnsclient.FallbackResolver` chain (a local `Client`
  with a pool of UDP sockets, T-034, plus backup clients
  `dns.fallback_resolvers`,
  switching on a transport failure, T-107) under the `LimitResolver`
  (`dns.concurrency`/`dns.max_qps`, adaptive, `dns.ipv6`, T-035); the
  service start — a synthetic probe of the chain (fail-fast when all are
  dead, a Warn
  on partial failure, T-107); the `q:http` publication — under the
  backpressure gate
  (`WatermarkPublisher`, T-026).

`http-worker` (stage 5, T-053): its `OnRun` runs the
`httpworker.Run` loop (`internal/httpworker/worker.go`):

- consumption of `q:http` by the `http-worker` consumer group in batches
  of up to 16
  records (`http.task_consume`) with a sliding loop (T-139): the next
  read happens when a task slot frees up (reading runs ahead of
  processing by no more than a couple of batches), up to 192 domain
  tasks at a time per instance
  (`http.task_concurrency`; the T-139 default of 256 was lowered to 192
  for memory stability — a day on 256 slots produced two unit fatals,
  load-measurements.md §12, T-157; returning to 256 — by the measurement
  after T-163;
  the global cap on connections and RPS is held by the transport, T-049),
  each delivery — with the `queue.Processing` pattern
  (T-025): XACK after the full domain processing cycle — after collecting
  the result, saving the content (T-056), writing to the DB and
  publishing
  `q:analyze` (T-058, the `Options.OnResult` hook) — the "XACK only
  after the DB write" order (spec 4.2);
- a task is a whole domain (T-024): every subdomain × (`https`, `http`)
  is checked with the redirect chain `httpfetch.FetchChain` (T-048) on
  the transport with limits (T-049), in parallel within the task by a
  pool of 8
  checks (`http.task_per_domain`; the subdomains of one domain live on
  the same IPs — the real parallelism of connecting to one IP is already
  bounded by the per-IP semaphore of 3–4, spec 5.5); parked-owner
  prefixes may get a raised limit (`http.per_ip_overrides`, T-165:
  longest-prefix, wait observability — by the metric below);
  head-of-line
  blocking of parked IPs (the T-092 §3 finding) is visible on the
  histogram
  `crawler_http_per_ip_wait_seconds` — the share of waits > 2 s (the
  bucket
  `le="2"`) against the < 5 % criterion on the production mix;
- dead-host cutoff (T-164, `http.dead_host_cutoff`): with the flag on,
  checks go in two waves — apex and www first, and if both failed on the
  connect phase (connection refused or a connect timeout on
  both protocols — connect timeouts only, not TLS/headers/general;
  a failure of a redirect host does not count), the remaining subdomains
  of the task are not
  checked: records — per §8 ("no answer": no records), the domain status
  is decided by the completed head checks (refused → `no_http`,
  connect timeouts → `temp_error` with the T-031 recheck — a transient
  false cutoff self-heals). Observability: the
  `dead_host_cutoff` bit in `domains.flags` (set/cleared by the result
  write of the last episode), the counters
  `crawler_http_dead_host_cutoff_tasks_total` /
  `crawler_http_dead_host_cutoff_skipped_checks_total` and a journal
  record — the control sample of false cutoffs (a repeated full
  check of cutoff domains, the < 1 % criterion) is taken by them; the
  flag off — one pool wave, the previous behaviour;
- the shared "final URL → result" cache within a domain task
  (spec 5.5 item 6): the final URL of every completed check goes into
  the cache, the start URL of every next one is compared against it —
  after a redirect to the same URL bare→www does not re-download the
  page; the key is
  scheme+host+path without query/port (consistent with the variant
  criterion of
  T-054: counter-style `?i=N` variants do not break the dedup; the
  scheme is included — http and
  https of one address are different downloads);
- the body is read with a cap, converted to UTF-8 (T-051); from it are
  computed the `content.HashOf` hash (the variant/alias criterion) and
  the SPA verdict (T-057), the headers/cookies of the final response are
  carried over;
  bytes are not held in the slot memory (T-147): every downloaded
  page immediately goes with an early `Put` into `ContentStore`
  (`Options.Store`) — alias deduplication runs on hashes, the records
  do not need the bodies; the task keeps metadata and ≤
  `http.task_per_domain`
  buffers of read bodies (slot memory ≈1–1.5 MiB vs ≈3.34 before
  T-147, measurement T-092 §11); an empty body (`Content-Length: 0`) is
  valid content: the hash of empty bytes, dedup as with any page,
  the record's `content_hash` is alive (T-138);
- slot memory is observable (T-163): `/metrics` (9104) exports
  busy slots `crawler_http_tasks_inflight` (the denominator of "unit
  memory / slot" when measuring the criterion of returning to 256),
  transit bytes
  of bodies in processing `crawler_http_body_bytes_inflight` (the window
  read →
  UTF-8 → hash → SPA → early Put) and the body size distribution
  `crawler_http_body_bytes`; the soft heap cap of Go —
  `http.mem_limit` (the build calls `debug.SetMemoryLimit`): near the
  threshold
  the GC itself returns pages to the OS instead of kernel reclaim storms
  on the unit's
  MemoryHigh and OOM-kill on MemoryMax (the T-157 fatals);
- content is written with an early `Put` of every page (T-147), link
  reconciliation — `ContentSaver` in the `OnResult` hook before the
  result write
  (T-056/T-058: packs T-042, the zstd dictionary T-043, `OpenPackStore` +
  `DiskWatchLoop` T-046 in a goroutine): original records of alias
  classes
  inherit their `Put` references, the excess (collapsed variants, losing
  classes, repeated downloads) are removed with `Delete` — the dedup is
  two-level (§8): the alias classes of a task store the content once,
  identical pages of different domains — with one block by hash;
  disk overflow and the `max_store_bytes` limit do not kill the result —
  a record with `content_hash = NULL` and the "content not saved" flag
  (details in `docs/content.md`);
- the variant logic `DecideVariants` (T-054) and aliases
  `DetectAliases`
  (T-055) assemble the result records; subdomain errors are classified
  (timeout/redirect limit cycle/TLS/body/network, spec §8) and do not
  kill the
  domain — all subdomains silent ⇒ no records;
- the domain result write — `Recorder` (T-058, `record.go`) in the same
  hook after `ContentSaver`: one transaction `UPDATE domains` (the status
  `alive` with records / `temp_error` on timeouts / `no_http`, `done`,
  `queued_at = NULL`, `last_checked_at = now()`, the guard
  `status <> deleted` — §8/§13.1) → `DELETE` of the old subdomain×variant
  rows with `RETURNING content_hash` → −1 `refcount` of every old hash
  (`SwapRefTx`, T-044; the new ones already have +1 from the `Put` of
  T-056) → `INSERT`
  of the new rows in an unnest batch with all fields (§5.5 item 7:
  addresses — 16
  octets per address, IPv4 in v4-mapped form, CNAME, the redirect chain,
  the final URL, TLS/headers/cookies — jsonb, the response time, the
  error code,
  `rendered = false` — the render is T-067); after commit — publication
  of
  `q:analyze` with a `Result` message per record (§5.5 item 9) under the
  backpressure gate; the `q:render` publication for SPA records (T-066,
  `renderq.go`) is non-blocking: the stream capacity is checked before
  the transaction with one depth read (`BackpressureGate.Len`:
  `min(lag, XLEN)` + `pending` of groups — the phantom lag clamp T-168,
  XLEN — while there are no groups; the threshold — the separate
  `render.queue_high_watermark`
  (T-174; 0 — the common `scheduler.queue_high_watermark`), the
  effective
  value — with the `min(watermark, MAXLEN/2)` clamp, like the
  publication gate T-026; computed by the `renderQueueWatermark` wiring
  in `cmd/http-worker`),
  free — publication right after commit bypassing the gate; overflow —
  the `render_pending` flag by the same result transaction (with a +1
  shift of the render-debt counter `crawler_counters['render_pending']`,
  T-175 —
  migration 000015; the bit guard in the UPDATE makes RowsAffected the
  indicator of the move: a redelivery with the flag already set does not
  increment the counter), the episode finishes without delaying the XACK
  (§5.6: render does not
  delay the other stages; §7: a domain is `done` after DNS/HTTP, the
  render complements asynchronously); a redelivery overwrites the result
  idempotently (§3.4);
- domains with the `render_pending` flag are finished by the background
  pass
  `RequeueRenderPending` (T-066, a goroutine in `OnRun`, once a minute,
  batches of 100): a repeated SPA verdict over the saved content of the
  records
  (`DetectSPA` is deterministic — the same verdict as at write time;
  aliases inherit the verdict of the original); with `q:render` free —
  publishing the `Result` keys and clearing the flag with a decrement of
  the debt counter
  in one transaction (T-175; the "publish → clear" order —
  at-least-once §3.4); records with the "render impossible" stamp
  `render_failed_at` (T-210 — set by the render-worker finalizations)
  are not taken by the selection: a repeated pass over a domain whose
  flag survived
  the publication does not spend deliveries and slot time on records
  already deemed impossible; a stamp is cleared by the episode
  overwrite; the batch
  selection — without PK order, a seq scan with an early stop LIMIT and
  plan pinning `SET LOCAL` in the selection transaction; a steady state
  without
  debt does not touch `domains` at all — the guard is a point read of
  the
  debt counter (T-181; details — [`render.md`](render.md)); render
  overflow neither kills nor delays the DNS/HTTP stages.
- the same `OnRun` starts the late render republisher
  (`httpworker.RenderLateLoop`, T-259, a goroutine next to the rework
  pass, once a minute, batches of 1000 rows): it republishes to
  `q:render` the "forever young" records — `rendered = false`,
  `final_url`
  not empty, without the `render_failed_at` marker — older than 6 h,
  whose domain has no `render_pending` bit (the debt belongs to the
  rework pass)
  and no unconsumed `q:render`/`q:render-fallback` messages
  (the interlock is a stream snapshot — the `retentionPending` adapter,
  the same as
  the retention guard). It heals the loss of MAXLEN-truncated fallback
  tasks
  (T-238/T-025) and lost publications: the record state stops depending
  on the survival of a message in the stream. The SPA verdict — over
  the saved content (as in the rework pass: without a verdict the
  publishing would flood the queue with re-checking every page); the
  cursor by PK —
  a full cycle over the table on the order of a month (faster than the
  recheck of
  `scheduler.recheck_interval`); the metric
  `crawler_render_late_republish_total` — stationarily does not grow;
  pass errors — `crawler_render_late_errors_total` (T-273)
  (the automaton specification of the row — the doc comment in
  `internal/renderworker/worker.go`, FSM spec §3 FR-5; details —
  [`render.md`](render.md)).
- the same `OnRun` starts the content retention loop
  (`content.RetentionLoop`, T-166, a goroutine next to the compaction
  T-045, hourly by `content.retention_interval`): records
  of `subdomain_results` older than `content.retention`, non-Laravel
  findings and
  not waiting for render lose `content_hash` and the heavy jsonb in one
  transaction with a decrease of `contents.refcount` (the row passport
  stays); the interlock — the `q:analyze` depth near the threshold (the
  gate T-026):
  the analyzer cannot keep up — the pass is skipped; the precise guard —
  records whose messages are not yet consumed by `q:render`/`q:analyze`
  are excluded from batches by a stream snapshot (an adapter over the
  Redis client
  of the gate, fix T-166/T-167; the metrics `crawler_content_retention_*`);
  the mechanics and criteria —
  [`content.md`](content.md) "Content retention and non-find trimming".
- redirect hosts are resolved "through the same resolver" (spec 5.5
  item 2):
  `dnsclient` with the T-035 limits → the `httpworker.NewDNSLookup`
  adapter →
  the `CachingResolver` cache (T-047); A and AAAA are queried in
  parallel with a shared ctx — the resolve latency is max(A, AAAA), not
  the sum; a failure/refusal
  of one type does not kill the resolve while the other is alive; the
  result is stable:
  A addresses, then AAAA (T-101); TLS — `InsecureSkipVerify` with
  preservation of
  the certificate attributes (`TLSInfo`, T-050).

`render-worker` (stage 6, T-067): its `OnRun` brings up the Chromium
generation manager (`render.NewManager` + `Manager.Start` — external via
`render.remote_url` or its own process, T-061; restarts by pages/memory
and replacement of a dead generation — T-065), the content store (the
same as http-worker's:
`OpenPackStore` + the zstd dictionary + `DiskWatchLoop`,
T-042/T-043/T-046) and
the `renderworker.Run` loop (`internal/renderworker`):

- consumption of `q:render` by the `render-worker` consumer group —
  sliding (T-142, the T-139 pattern of http-worker): the next read
  happens when a delivery slot frees up (an XREADGROUP batch of up to 16
  records + a prefetch buffer — reading runs ahead of processing by no
  more than a couple of batches),
  in parallel up to max(4, `render.contexts`) deliveries per instance
  (the build passes `max(DefaultConcurrency, render.contexts)` — the
  pool slots do not idle when `contexts` > 4); each
  delivery — with the `queue.Processing` pattern (T-025): XACK only
  after writing the result to the DB and publishing `q:analyze`;
  consuming deliveries in a row without a single acknowledgment (a
  systematic processing failure) —
  a read pause with circuit-breaker doubling (reading new deliveries
  stops — unread ones do not spend attempts; already read
  ones stay in the PEL and return via claim by `redis.claim_min_idle`);
- a task is a `Result` key of a `subdomain_results` row (T-024;
  published by the HTTP stage on SPA verdicts T-057/T-066 and by the
  rework pass): the final
  URL is read from the row, the page is rendered in a fresh isolated
  profile (resource blocking T-062, readiness wait T-063,
  DOM/cookie/header capture T-064);
- the DOM replaces the raw HTML in the store (spec 5.6, §5.7): a `Put`
  into the same content store (+1 reference), then in one transaction
  the row receives `rendered = true`, a new `content_hash` reference,
  the render's final URLs/cookies/headers and `rules_version = NULL`
  (the content changed — the analyzer's past verdict does not reflect
  the new one), while the old reference is removed with `SwapRefTx`
  (T-044); as the tail steps of the same transaction — an increment of
  the unanalyzed-tails bucket
  `crawler_counters['reanalyze:YYYY-MM-DD"T"HH24']` (T-204: NULL returns
  the row to the unanalyzed until the republish stamp; the hour is
  `checked_at` from the UPDATE's RETURNING) and the hourly rate bucket
  `crawler_counters['rendered:YYYY-MM-DD"T"HH24']` (T-203: `+1` on the
  row moving to rendered; the hour is the same `checked_at` — the hour
  of the episode, not of the render; previously the live `rendered_hour`
  rate subquery degraded with heap fetches — the stage 27 checkpoint);
- after commit — publication of `q:analyze` with the same key (§7: the
  render is an asynchronous complement, the analysis re-runs over the
  final DOM) under the backpressure gate (`WatermarkPublisher`, T-026);
- redelivery idempotency (spec 3.4): a repeated render overwrites the
  content and refcount (the T-058 balance); the race with the recheck is
  closed with an optimistic UPDATE guard on `content_hash` — a changed
  row is not overwritten, the `Put` reference is removed;
- unavailable Chromium (the `Manager.Alive` probe) — stopping new reads
  (undelivered deliveries stay in the stream, not in the PEL);
  deliveries read into the prefetch buffer are finished and returned via
  claim on failure; the probe restarts a dead generation itself
  (reconnect after a Chromium restart, including a half-closed connection
  — the live probe `Browser.probeConn`, T-218); a browser restart every
  N pages and by memory — T-065.

The stage speed is dictated by Chromium (the `render.contexts` slots,
measurement T-092 §4: ≈5.1 s/page; the default of 8 contexts, T-169, is
a cap of ≈1.6 pages/s against a demand of ≈1.2 pages/s with the gate
free, §12); overflow of `q:render` does not block the HTTP stage: records
are marked
`render_pending`
and finished by the pass later (spec 5.6, T-066) — the
sliding consumption merely does not add a read-window barrier to the
narrow browser bottleneck (the tail of pages up to `render.timeout` does
not hold slots idle).

`analyzer` (stage 7, T-071): its `OnRun` runs the
`analyzer.Run` loop (`internal/analyzer`; the detailed pipeline is in
[`analyzer.md`](analyzer.md)):

- consumption of `q:analyze` by the `analyzer` consumer group in a
  window of up to 16 records, in parallel up to 8 deliveries per
  instance; each follows the
  `queue.Processing` pattern (T-025): XACK only after the DB write;
- the rule set must load at startup (`rules.NewReloader` over
  `analyzer.rules_dir`; an error — exit code 1): without a valid set the
  analyzer would silently miss Laravel; SIGHUP (`make rules-reload`)
  reloads the directory on the live service (`Options.OnSIGHUP` →
  `HandleSIGHUP`); every delivery takes the current snapshot via
  `Set()`;
- a task is a `Result` key of a `subdomain_results` row (T-024;
  published by the HTTP stage T-058 and the render-worker T-067): the
  analysis input (headers/cookies
  jsonb, `content_hash`) is read from the row, the content — from the
  same PackStore store (the zstd dictionary matches the writing stages);
  `Set.EvaluateCtx` with `DefaultLimits` (T-069) — all firings of any
  rule regardless of the threshold; `is_laravel = score ≥
  analyzer.laravel_min_score` (default 0.6, §13.1);
- the write is one transaction: `UPDATE subdomain_results.rules_version`
  (the set-version stamp for re-analysis, T-074; guards on
  `content_hash`, version currency (T-204: the stamp only for stale
  rows; a redelivery of the same version is a no-op) and "domain not
  deleted" —
  0 rows: success without a write, the render-worker style; `RETURNING
  checked_at` — the hour of the tails bucket) → an upsert of
  `laravel_findings`
  (score, the version range with NULL on conflict, `signals` jsonb,
  `is_laravel`, `rules_version`) or a DELETE
  on an empty result (§3.5 overwrite) → a recompute of the
  `laravel_domains` aggregate
  (T-072: the max score, the merged version range,
  `found_on_mask` — bit № = `subdomains.id` for confident firings;
  a domain advisory lock reconciles parallel deliveries); when the
  presence of the row changes and there is a fresh HTTP episode
  (`Result.Fresh` in the task — set by the HTTP/render stages), the same
  transaction writes the detection journal
  `laravel_history` appeared/lost (T-118: re-analysis over old content
  does not write history — a rule change is not a fact about the site);
  as the tail step — a decrement of the `reanalyze:` tails bucket
  (T-204: a published and stamped tail returns the bucket to zero, the
  re-analysis tick guard closes); redeliveries are
  idempotent (spec 3.4) — details in
  [`analyzer.md`](analyzer.md);
- the analyzer publishes nothing (the end of the pipeline, §7) — the
  backpressure gate is not needed, the queue is read and acknowledged
  directly.

`monitor` (stage 8, T-078/T-178): its `OnRun` runs the
`monitor.ZoneStatsLoop` loop (`internal/monitor/zonestats.go`) —
the periodic reconciliation of the pipeline's incremental counters:

- one combined pass per `monitor.zone_stats_interval` (default
  30 minutes, T-199) — the only full pass over `domains` in
  the steady state (T-178, measurement §12: the live aggregates of
  zones/statuses/backlog and
  the hourly history pass accounted for most of the postgres CPU): in
  one
  transaction the zone columns `domains_total` /
  `domains_pending` / `domains_queued` / `domains_checked` /
  `domains_laravel` are overwritten (the `domains` aggregate FILTER over
  `check_state` GROUP BY
  `zone_id` — partitionwise, plus the `laravel_domains` aggregate) along
  with the rows
  of the global scalars of `crawler_counters` (`status_<name>` per the
  status legend, the cumulative `laravel_appeared_total`/`laravel_lost_total`,
  `render_pending`), as well as the daily check rows
  `checks:YYYY-MM-DD` and its temp_error numerator
  `temp_error:YYYY-MM-DD` — via FILTER columns `last_checked_at =
  current_date` (× `status = temp_error` for the numerator) of the same
  scan
  (T-179/T-182, zero cost; the day name is computed by the DB — with
  separate upserts
  after the scalars, in the order `checks:` → `temp_error:`); the row
  upserts create them on the first pass (the daily rows — also the first
  increments of the day);
  the pass finale — the retry maturity buckets `retry_mature:YYYY-MM-DD`
  (T-202): the FILTER columns `status = temp_error × check_state = done
  ×
  episode date` of the same scan write yesterday's (the mature cohort)
  and
  today's buckets; the pass removes stale buckets — there are
  always two live rows; the order "cleanup → mature → today's" is shared
  with the result-write increments and retry decrements — no lock
  cycles; then the unanalyzed re-analysis tails buckets
  `reanalyze:YYYY-MM-DD"T"HH24` (T-204) and the hourly rate buckets
  `results:`/`rendered:YYYY-MM-DD"T"HH24` (T-203) — by a SEPARATE
  aggregate over
  the window of the two live hours of `subdomain_results` (the
  `checked_at` index,
  the prev/cur FILTER positions of all three families — for the rate the
  same predicates
  as the previous live subqueries, the stage 27 checkpoint; tails —
  `rules_version IS NULL`, the predicate of the re-analysis tick guard)
  writes
  prev and cur of each family (prev — the tail of the read window: late
  rendered
  increments and stamp decrements keep finishing it throughout the
  current hour),
  stale buckets are removed; the family order "reanalyze: → rendered:
  → results:", within each "cleanup → prev → cur", is shared with the
  writers
  (record.go: reanalyze: → results: as the last lines of the write
  transaction; render-worker: reanalyze: → rendered:; the analyzer is
  the only reanalyze: point) — no cycles;
- between passes the counters maintain the transition deltas in the
  transactions of the transitions themselves
  (T-178: the DNS stage — the main queued/pending → done move,
  the CTE with `FOR UPDATE` in the result write; scheduler enqueue/lease/
  recheck/retry — the CTE of the same statement; ingest add/delete; the
  CLI recheck
  and submodules recheck; the http-worker result write on top of done —
  no deltas (the `FOR UPDATE` pre-read); the analyzer — `domains_laravel`
  ±1
  and increments of the cumulative counters; the http-worker result
  write also
  increments the daily checks counter `+1` per episode, its
  numerator `temp_error:` `+1` per temp_error outcome — T-179/T-182) and
  the retry maturity bucket `retry_mature:` `+1` per temp_error episode
  (T-202; the same bucket is incremented by the CTE of the terminal
  temp_error
  branch of the DNS stage and decremented by retry decisions); the
  re-analysis tails bucket
  `reanalyze:` `+N` per the episode's inserted NULL rows and the hourly
  rate bucket `results:` `+N` per the episode's inserted rows
  (T-203/T-204; the `rendered:` bucket and the `+1` of the episode-hour
  tails are maintained by the
  render-worker — on moving a row to rendered with `rules_version =
  NULL`, the hour is the row's `checked_at`; the tails decrement — the
  analyzer's stamp
  on stale rows; the re-analysis tick guard — the sum of the buckets
  older than
  the current hour, T-204): a transaction rollback
  rolls the pair back. The daily rows carry the
  "work rate" semantics: increments count HTTP result-write events,
  the live window — the domains of the day across all branches (DNS
  terminal completions
  have no HTTP episode); repeated episodes give +1 on a live zero;
  the temp_error numerator diverges from the window also via retries
  that closed the error (the event stays in history). The positions
  `daily:checks` /
  `daily:temp_error` are streaming (without a stationary zero), they are
  not included in the aggregated drift Warn and are observed as gauges;
  the buckets
  `retry_mature:past`/`retry_mature:today` (T-202) — the same streaming
  semantics (episode/retry events against the live domains of the
  filter);
  the hourly rate buckets `rendered:prev`/`rendered:cur`/
  `results:prev`/`results:cur` (T-203) and the re-analysis tails
  `reanalyze:prev`/`reanalyze:cur` (T-204) — the events of row
  inserts/renders/stamps against the live rows of the window, the same
  streaming
  semantics;
- the drift |old − new| of each position — the gauge
  `crawler_counters_drift{counter}` (the stationary value tends to
  zero:
  a pass is three statements under READ COMMITTED, a single transient
  drift under live load is normal; status rows are outside the drift —
  written by one reconciliation) and a Warn on non-zero drift (the first
  successful pass of the loop does not count as drift — it fills the
  zeros after
  a migration/downtime); sustained drift — a missed move or manual
  SQL bypassing the code; triage — `docs/runbooks.md`; manual recovery —
  `crawlerctl counters recount|verify` (T-178);
- single-flight passes (T-161): in the loop goroutine a new pass does
  not start until the previous one finished, and a pass longer than the
  interval (table growth) drops the accumulated ticker tick — a Warn,
  `crawler_counters_recount_seconds` (a rename of
  `crawler_zonestats_pass_seconds`, T-178) and
  `crawler_zonestats_skipped_ticks_total` raise the noise;
- the semantics (spec 11.4 item 6 "total / checked"; the base set of ALL
  zone columns — `status <> deleted`, unification T-178, ingest spec
  §4.1):
  `domains_total` — the live domains of the zone; `domains_pending` /
  `domains_queued` — waiting for selection / in work with a lease;
  `domains_checked` — checked in the current cycle (`check_state =
  done`, spec 6.1): the age recheck (spec 5.3) moves done →
  pending, and the counters track the progress of the new cycle; all are
  counted over one base set, hence `pending + queued + checked =
  total`;
- the first pass — right at startup (fresh counters after downtime and
  filling zeros after a migration); a pass error — a Warn, the loop
  lives on;
  a ctx cancellation — a clean nil (graceful shutdown, spec 3.4). The
  counters are read by
  `crawlerctl status` (T-079), the dashboard (no domains JOIN, T-178),
  the scheduler backlog loop (reference-table sums) and the partition
  size choice
  (`docs/schema.md`). Zone progress is observed by the monitor alerts
  (T-080, the loop below); the queue/backlog/stage counters of the
  pipeline are collected by the services themselves (the table above,
  T-077).

The third loop of the same service — the Laravel detection gray-zone
counter
(`internal/monitor/nearstats.go`, T-115): the gauge
`crawler_laravel_near_findings` — the current number of
`laravel_findings` with
`analyzer.laravel_min_score/2 ≤ score < min_score` ("near-firings":
the signs are there, the threshold is not passed). The period is 1 h
(not the 30–60 s of the zone recompute):
a pass is an aggregate over large `laravel_findings` partitions, and
the rule calibration material does not need minute freshness; triage of
the same rows by name — `crawlerctl near`. A stable gray zone is normal
(weak signs of frequent non-Laravel stacks); a burst after a rule change
is
a signal of precision degradation (material T-116/T-117).

The fourth loop — the detection history counters
(`internal/monitor/historystats.go`, T-118): the pair of gauges
`crawler_laravel_appeared_total` / `crawler_laravel_lost_total` —
the cumulative counts of transitions from the append-only journal
`laravel_history`
(the appearance/disappearance of a `laravel_domains` row over fresh
HTTP episodes). The values are monotonic (the journal is append-only) —
a gauge with
counter semantics is correct for `rate()`/`increase()`; the period is
1 h,
like the gray zone: detection trends are dashboard material, not an
operational pipeline signal.

The PostgreSQL slow-query loop of the same service
(`internal/monitor/slowqueries.go`, T-177): once per
`monitor.slow_query_interval` (default 10 s) it polls
`pg_stat_activity` — client statements running longer than
`monitor.slow_query_min_duration` (3 s — the threshold of the production
`log_min_duration_statement` log, [db.md](db.md) §"Monitoring"); they go
in one message to Telegram with the same delivery as the alerts — only
when
`monitor.slow_query_telegram` is on (off by default: a slow
query is diagnostics, already duplicated by the monitor journal, metrics
and the server log, not an event requiring an "act now" reaction; without
the flag
the loop is limited to Warns and metrics): a header with the number of
new and
"total in flight" queries, the three longest rows
"duration [service] pid: text"; a Warn into the journal and the metrics
`crawler_pg_slow_queries` / `crawler_pg_slow_queries_total`. The debounce
is
pid+query_start: a statement is reported once at first observation; a
finished one leaves the base; monitor's own aggregates (an UPDATE over
47M rows every 30 min — T-199, measurement §12 — a normal load)
are excluded by the pool's `application_name`; the service labels in
the message come from there
too. The snapshot is instantaneous, not an event stream: episodes shorter
than the loop
interval are caught partially, longer ones — always; the full post-mortem
trail is the server
journal.
Reading the text of foreign sessions requires the `pg_read_all_stats`
grant for the DB role (deploy.md
§4.1) — without the grant the message honestly marks "text unavailable".

The second loop of the same service — evaluating the alerts of the
minimal spec 5.9 set
(`internal/monitor/alerts.go`, T-080; spec 8, spec 10.3), the period is
its own `monitor.alerts_interval` (default 5 minutes, T-206 —
detached from the `zone_stats_interval` reconciliation, which T-199
thinned out
to 30 min; before T-206 the pair of loops ran on one period, and the
detection windows in
the passes were stretched 6×): window conditions and pass series —
`psi_cpu`, `queue_frozen`, `memory_high`, `render_debt`,
`render_fallback_depth` (T-246) — notice
an episode 1–2 passes after its start, up to 10 minutes; the
`alerts_interval` corridor is 30s..1h, while the progress-stop threshold
stays in
reconciliation periods (see the alert below):

- **growth of the DNS error share** — the share of `temp_error` among
  the checks of the current day over the daily `crawler_counters` rows
  `temp_error:YYYY-MM-DD` /
  `checks:YYYY-MM-DD` — two point PK reads (T-182; `temp_error` is
  the DNS-stage outcome after retries are exhausted, spec 5.4; NXDOMAIN
  →
  `not_delegated` is a valid result, not an error). The semantics is the
  events of
  result writes: retries and duplicate deliveries stay in the history
  (the share is robust to them; the row absolutes are repaired by the
  reconciliation pass);
  the window is a day; the day name is computed by the DB
  (`current_date`; the day precision of
  `last_checked_at` — spec 6.1). On at a share ≥ 30 %, off below
  20 % (hysteresis); with < 100 denominator events the share is the
  noise of an empty
  window — the alert is "not measured". The former live aggregate over
  the day window
  (a `domains` scan by `last_checked_at` with `status` reads from the
  heap)
  was removed: the slow log of production on 24.10 recorded 33 evaluations
  longer than 3 s
  (median 27.7 s) — query №6; the status cannot be in the index
  (the HOT invariant, docs/schema.md);
- **queue overflow** — the depth of the working streams `q:dns`/`q:http`/
  `q:render`/`q:analyze` (group backlogs: `min(lag, XLEN)` + `pending` —
  the lag is bounded by the stream length, the phantom lag clamp T-168;
  `BackpressureGate.Len`; `q:dead` is not bounded by the gate and is not
  checked) against the stream's effective threshold —
  `queue.StreamWatermark(scheduler.queue_high_watermark, redis.maxlen,
  stream)`, no higher than half of the MAXLEN limit (T-141: `q:dns` —
  125 batches,
  per-record streams — 75 000 at defaults); the same threshold as the
  publication gate
  (T-026) and the dashboard queue highlight (`queue_watermarks`, T-144).
  It fires at len ≥ threshold — simultaneously with the gate closing on
  streams published through it (the exception is `q:render`: its capacity
  gate is separate — `render.queue_high_watermark` with the
  MAXLEN/2 clamp (T-174; 0 — common); closing does not wait — the domain
  gets the
  `render_pending` flag (T-066); the alert threshold for `q:render`
  stays common
  (75k at defaults) and fires later than the gate closes — the signal
  that debt draining cannot keep up); clearing below 90 % of the
  threshold (hysteresis
  against depth flapping near the threshold);
- **progress stop** — `zones.updated_at` (written by `ZoneStatsLoop`)
  older than 5 × `zone_stats_interval` (2.5 h at the default 30 min,
  T-199):
  a pass over tens of millions of rows takes minutes; a single missed
  period is not
  a failure. The threshold is counted in RECONCILIATION periods, not
  alert ones: the stamp
  is written by `ZoneStatsLoop` — a threshold shorter than its writing
  period (e.g.
  5 × `alerts_interval` = 25 min) would fire forever; the more frequent
  alert passes (T-206) only poll freshness more often. The
  self-control clause: the stamp
  is written by monitor itself — a full process death is visible in the
  `crawler_service_up` metric of the framework (T-012), not via this
  alert;
- **disk space shortage** — statfs of the `content.dir` directory
  against
  `content.disk_free_percent`: the same formula as the content `Put`
  pause
  (T-046) — the alert fires exactly when content writing stops
  (spec 8);
- **NVMe wear (SMART)** — `percentage_used` from the NVMe health log
  (spec
  10.3): devices are discovered in sysfs; wear is read with the ioctl of
  the admin command without nvme-cli; the alert fires at ≥ 90 % of the
  resource (values >100 %
  — the resource is exhausted — too); the detail carries the device's
  TBW. In a Docker container
  sysfs sees the host devices, but `/dev/nvme*` is not passed through —
  the alert honestly reports "not measured" (Debug, no false firing and
  no Warn):
  on a production systemd host the layer is enabled by the unit's device
  access
  (the `nvme-smart` group, T-089) next to the host smartd —
  [`disks.md`](disks.md) "SMART and TBW monitoring";
- **junk content_hash of cleanups** (T-099) — a burst of the
  `crawler_ingest_bad_hashes_total` counter: the probe reads the
  `/metrics` exposition
  of the ingest service (`monitor.ingest_metrics_url`, the same address
  Prometheus scrapes) once per alert period; the window is 15 passes
  (75 min at the default 5-min `alerts_interval`, T-206; on the 30-min
  loop of
  T-199 it stretched to 7.5 h), the threshold is 10 new junk
  rows. Normally the writer
  invariant of results holds 16 octets (spec 6.1) and the counter does
  not grow at all: growth —
  a software writer bug (T-056); no data are lost (the refcount of the
  junk reference never changed, ING-P2-4); clearing is by window aging
  without
  continuation of the flow: a single burst keeps the alert firing until
  evicted from the ring, ≈75 min at defaults. A probe error (ingest
  stopped, no metrics) —
  "not measured" with a Warn: the death of ingest itself is visible in
  `crawler_service_up`, not via this alert;
- **OOM and restarts of crawler units** (T-145) — the memory.events
  counters of
  cgroup v2 (`oom`/`oom_kill`) and `NRestarts` of all units
  `crawler-*.service`: an event is an unplanned pipeline outage
  (spec 3.4) and a symptom of missing the unit's memory budget
  ([`deploy.md`](deploy.md) §2; the http-worker's soft MemoryHigh cap —
  the same T-145). Fires on any event; one Warn per episode;
  an episode runs from the event until the unit state is reset:
  `systemctl
  reset-failed`/a fresh start zeroes NRestarts (in a live unit oom/
  oom_kill persist until the cgroup is recreated by a restart); deploy
  resets everything itself (stop → reset-failed → start,
  deploy-install.sh).
  The source — `systemctl show 'crawler-*.service' -p Id -p NRestarts`
  (manager properties are read unprivileged) + a direct read of
  `/sys/fs/cgroup/system.slice/<unit>/memory.events` (cgroup
  files are
  world-readable; systemd does not export them as D-Bus properties);
  the source wrapper mirrors the snapshot into the metrics
  `crawler_unit_oom_events`/`crawler_unit_restarts`. Source errors
  are split into classes: an environment without systemd (a dev
  container) — a silent
  "not measured" (like SMART); a production failure (systemd/D-Bus/cgroupfs
  broken) — a Warn of every pass;
- **untriaged q:dead** (T-154) — a non-zero quarantine depth for
  3 passes in a row (≈15 min at the default 5-min `alerts_interval`,
  T-206; on the 30-min loop of T-199 it stretched to ≈1.5 h):
  "sustained non-zero", not a blink of a single
  death — with live triage a single death is normal; a fired
  alert is held by any non-zero depth; clearing — an empty quarantine
  (triage + XTRIM, runbooks §2). The depth source is the same gate as in
  the queues alert: `q:dead` has no consumers, Len reads XLEN;
  the `NewDeadDepthMetrics` wrapper also mirrors it into
  `crawler_queue_depth{queue="q:dead"}`. It is consistent with the
  dashboard's q:dead
  highlight (T-126/T-144: depth > 0) and the death counters
  `crawler_queue_dead_buried_total` (tasks with task_id) and
  `crawler_queue_dead_unidentified_total` (unidentifiable records,
  T-260): the counters are the death rate,
  the depth is the sink; the 22.10 episode (5507 deaths,
  docs/load-measurements.md §11.1) stretched over hours by claim — the
  sink
  holds the signal all that time, the rate is visible retrospectively;
- **Redis memory** (T-154) — `used_memory/maxmemory` from `INFO memory`
  (the source `RedisInfo` of the same client as the depth gate; the
  `NewRedisMemoryMetrics` wrapper mirrors `crawler_redis_memory_ratio`/
  `_bytes`) ≥ 80 % — firing, < 75 % — clearing (hysteresis):
  under noeviction (the budget policy, spec 3.2) filling maxmemory is
  an XADD write refusal ("OOM command not allowed", the incidents
  T-093/22.10);
  `INFO` does not provide a direct refusal counter — the share with a
  20 % margin
  (between-trim residuals of TrimLoop, fragmentation, buffers) is a proxy
  of upcoming refusals. `maxmemory` 0 (no cap) — "not measured":
  the ratio is undefined;
- **Redis restart** (T-270, the `redis_restart` alert) —
  `uptime_in_seconds`
  from `INFO server` (the source `RedisInfo` of the same client as for
  memory)
  smaller than the `AlertRedisRestartWindow` window (15 min = 3 alert
  passes: a restart between passes cannot slip through): a restart
  without persistence
  (redis.md §5) loses ALL undelivered stream
  records — the main cause of the "status hung" loss class of FR-2.2
  (the 22.10 incident: a cohort of 1865 domains, the T-256 gauge); q:dns
  self-heals via the T-029 lease; lost q:http with a recorded done are
  returned by the T-270 compensator — the signal reaches the operator at
  the moment of the event
  (expect `new_done_stale` growth and the cohort returning via
  `crawler_scheduler_stale_requeued_total`), not as a gauge cohort a day
  later. The level is monotonic between restarts: clearing is the exit
  from
  the window itself; machine memory is not needed (no hysteresis
  required); a planned
  restart alerts the same way — the losses of undelivered work do not
  depend on intentions;
- **absolute minimum of disk space** (T-162) — the free bytes
  of the content directory against `monitor.alerts.disk_free_min_bytes`
  (15 GiB, Warn) and `disk_free_crit_bytes` (10 GiB, an escalation by a
  separate
  `disk_free_crit` alert): the percentage threshold
  `content.disk_free_percent` on a large volume is late — 10 % of 79 GB
  is 8 GB ≈ 5 h until full at a rate of ≈36 GiB/day (measurement §12);
  the absolute minimum gives headroom for triage regardless of the
  volume size.
  Firing is strict (<), no hysteresis — during an incident space
  decreases monotonically and there is nothing for flapping near the
  threshold to move (like the
  percentage one);
- **MemoryHigh throttling** (T-162, the `memory_high` alert) — growth of
  the unit's `memory.events high` counter between passes (the source —
  the same
  `systemctl show` + cgroup v2 as unit_oom; the wrapper mirrors
  `crawler_unit_memory_high_events`): every hit over the soft
  cap is a forced reclaim/delay; the counter accumulates over
  the cgroup lifetime (an http-worker with MemoryHigh=850M collects
  >100k per
  day) — the threshold is not a value but growth. Persistence — growth
  over
  `monitor.alerts.mem_high_passes` passes in a row (2 × 5 min = 10 min
  at the default alert period `alerts_interval`, T-206):
  a single burst is the normal work of the soft cap; sustained
  throttling — a precursor of http-worker fatals (T-157: memory-driven
  restarts
  before OOM-kill) and a request to revise the budget (deploy.md §2).
  Clearing —
  the first pass without growth;
- **frozen queue** (T-162, the `queue_frozen` alert) —
  the last-delivered-id of a working stream's group does not move
  for ≥ `monitor.alerts.queue_freeze_window` (15 min) with depth > 0:
  a live consumer moves the ID with every XREADGROUP (batches take
  seconds,
  spec 3.3); a standing ID with a non-empty backlog — records are not
  delivered at all: the consumer is dead or crashes on the first record
  (poison T-159).
  Immobility is evaluated only by a loop pass — the latency
  on top of the window is at most the alert loop period: at the default
  5 min
  (`alerts_interval`, T-206) a freeze is noticed ≈15–20 min from
  the start (on the 30-min loop of T-199 it was 30–60 min).
  The source — XINFO GROUPS + XLEN of the same client
  (`RedisStreamGroups`);
  the group depth — the same calculation as the gate (`queue.GroupDepth`:
  `min(lag, XLEN)` + pending, the T-168 clamp): the stuck phantom lag of
  the 22–23.10 incident (the gate closed
  for 15.5 h with 40.3M pending) would have been caught by this alert by
  the very fact
  of stopped deliveries. A source error does not touch the tracking
  memory;
  the group's first record lives only while depth > 0 — the idle of an
  empty queue does not accumulate "time without deliveries";
- **PSI CPU** (T-162, the `psi_cpu` alert) — some avg300 from
  the host's `/proc/pressure/cpu` (the source `OSPSI`; the wrapper
  mirrors
  `crawler_psi_cpu_some_percent`) above
  `monitor.alerts.psi_cpu_percent` (90 %) sustained over
  `monitor.alerts.psi_cpu_window` (30 min; the evaluation goes in alert
  loop passes every 5 min — T-206 restored the frequency; the
  sampling error of the window is ~5 min; on the 30-min loop of T-199 a
  window ≤ the period
  degenerated into "avg300 above the threshold on two adjacent passes" —
  only starvation of ≳60 min was reliably caught; episodes of 30–59 min
  were phase-dependent, see `DefaultPSIWindow`): some — a stall of at
  least
  one task (100 % utilization without a queue is the norm of a CPU-bound
  pipeline); measurement §12 — working scheduler starvation of 91–95 %
  where all stages drown together (T-158 lowered the CPU weight of
  monitor —
  its own passes do not bring starvation closer). The window discards
  one-off
  spikes (deploy, retro batches); a kernel without PSI — a silent
  "not measured" (like SMART in a container);
- **render debt accumulation** (T-175, the `render_debt` alert) — the
  debt
  (domains with the `render_pending` flag per the
  `crawler_counters['render_pending']` counter, migration 000015, plus
  the `q:render` stream
  lag; the source — the DB pool and the depth gate, the same as for the
  DNS/queue alerts) above `monitor.alerts.render_debt_domains` (50k) AND
  growing for `monitor.alerts.render_debt_passes` passes in a row
  (2 × 5 min = 10 min at the default alert period, T-206): the queue
  is overflowing and the SPA inflow outruns
  the drain — the `queue_frozen` alert (T-162) does not catch that case,
  deliveries
  keep going; the 443k debt of the 24.10 freeze was found with manual
  SQL; this alert makes accumulation visible (the series passes — the
  alert loop period
  `alerts_interval`, by default 2 × 5 min = 10 min, T-206).
  Firing is tied to growth, clearing — to the level: a fired alert
  stays on until the debt drains below 90 % of the threshold
  (hysteresis; the tracking latch — a source break into "not measured"
  does not tear an episode, review
  T-175) — debt is a level (flags + lag); the lag swings by
  hundreds-to-thousands per pass at a drain of ~1–2 pages/s, so clearing
  "by non-growth" would flap an episode 🚨/✅ at a stationary high debt.
  The growth trigger is
  any positive delta (no minimum size: the incident's accumulation rate
  of ~600/pass is comparable with the lag noise — a size threshold would
  not separate signal from noise; debt above the threshold legitimately
  holds the episode until drained, only swings change the start moment);
  stagnation without a growth series does not wake the operator — it is
  visible in the
  "Render debt" row of the T-173 load report and in the gauge
  `crawler_render_pending_domains`; the first pass after a restart is
  the base. A missing counter row (manual table editing) — "not
  measured" with a Warn, not a silent zero;
- **growth of the render fallback queue depth** (T-246, the
  `render_fallback_depth` alert) — the depth of `q:render-fallback` (the
  backlog of worker B's
  `render-fallback-worker` group — the shared `queue.GroupDepth`
  calculation
  via the same gate as the queue alerts; read directly from Redis:
  the mirror metric `crawler_queue_depth{queue="q:render-fallback"}`
  (T-245)
  is exported by the B build itself and there is no series with B
  stopped, while "B
  is stuck, the queue grows" is the alert's main scenario) against the
  stream's effective
  threshold — `queue.StreamWatermark(scheduler.queue_high_watermark,
  redis.maxlen, q:render-fallback)`; for
  `q:render-fallback` this is not the gate threshold (the stream is not
  bounded by the gate — render spec §4.3 item 3), but an observation
  threshold "the depth
  is approaching the MAXLEN limit" (15 000 → a clamp of 7 500 at the
  default
  watermark of 100k; an explicitly lowered watermark — as with q:render)
  — AND
  (growing for 2 passes in a row (10 min at the default 5-min
  `alerts_interval`;
  the render_debt mechanics: level + growth series + latch) OR staying
  above the threshold for 2 passes in a row without growth — S-P2-1, the
  operator's call: a stationary depth means a stopped B or hitting the
  stream's MAXLEN limit
  (XADD evicts the oldest undelivered records — the depth stops growing
  and tasks are lost silently), or an episode that survived a monitor
  restart and is no longer seen by the growth series; clearing in both
  cases —
  draining below 90 % of the threshold). A one-off burst of publications
  (the normal work of
  "collect and rework", FR-13) does not wake the operator: a burst that
  dissolves between passes does not build a series; sustained growth —
  the `render.fallback.worker_rate` rate below the inflow, up to
  a stopped B;
- **exceeding the fallback share budget** (T-246, the
  `render_fallback_budget` alert) — growth of the suspension-event
  counter
  `crawler_render_fallback_suspended_total{reason="budget"}` (T-239;
  the probe of the render-worker's `/metrics` at
  `monitor.render_metrics_url` — the same
  address Prometheus scrapes): the share of publications over the budget
  window
  (1000 deliveries) exceeded the `render.fallback.share_percent` cap —
  degradation of the primary engine; engine-branched finalizations
  multiply.
  Tasks are not lost (candidates are finalized normally with the
  marker, FR-9); an event is a latching, not a state: firing —
  any event within a window of 6 passes (≈30 min); clearing — by aging;
  a worker restart — a zero delta without a burst. The
  `suspended{reason="budget"}` series is pre-created as zero since T-250
  (S-P1-4):
  "no series" at the render-worker is 0, not "not measured" (the
  `crawler_stage_tasks_total{stage="render"}` marker distinguishes a
  foreign
  exposition), so the first 0→1 event is not lost. Health-gate events
  (`reason="gate"`) are not counted by this alert — the gate latch
  stays a counter and a Warn log of the worker, while the infra-error
  streak is visible to
  the `render_browser_errors` alert (S-P2-6, the operator's call —
  a rate detector):
  `crawler_stage_errors_total{stage="render",
  type="browser"}` from the same probe growing for 2 passes in a row
  (AlertRenderBrowserErrorsPasses; 10 min at the default 5-min
  `alerts_interval`) — the candidate engine is hung or unavailable, the
  browser class
  sits in q:render redelivery; a single error and a worker restart (the
  counter is process-local) stay silent; a foreign exposition (without
  the render-stage marker) —
  a silent "not measured" (the address does not point at a
  render-worker). A falling engine unit needs no separate alert:
  engine units are `crawler-*.service` (crawler-chromium today,
  moli/lightpanda per render spec §4.7 item 3) — covered by the existing
  `unit_oom` (T-145, the `crawler-*.service` pattern);
- **"status hung" domains** (T-256, the `domains_new_done_stale` alert,
  FSM spec §3 FR-2.2) — the `crawler_counters['new_done_stale']` row:
  domains `status = new ∧ check_state = done ∧ last_checked_at` older
  than a day — the DNS stage wrote the "addresses found" branch (O6 of
  the status decision
  table, `internal/model/enums.go`), while the HTTP outcome (O7–O9)
  never arrived:
  a lost `q:http` task (the automaton specification of
  `status` — the same place; the stage branches —
  `internal/dnsworker/result.go`,
  `internal/httpworker/record.go`). The source — a point PK read of
  the row (the value is written by the `f_new_done_stale` FILTER column
  of the
  reconciliation pass — the alert needs no seq scan over `domains`; the
  window depth is
  `zone_stats_interval`); the level is observable as the gauge
  `crawler_domains_new_done_stale`. Firing — 2 NEW reconciliation
  snapshots
  in a row with a non-zero value (AlertNewDoneStalePasses;
  the series grows only when the row's `updated_at` changes — alert
  passes are
  more frequent than the source, so repeated reads of one snapshot do
  not count as observations: a single boundary snapshot of the day edge
  — midnight aged the previous in-flight episodes — is overwritten with
  zero by the next pass and gives no alert, ≈1 h of a stable value);
  holding — any non-zero value; clearing — zero (the `queue_dead`
  style).
  Stationary 0 normally; the row is absent until the first
  reconciliation pass —
  a silent "not measured". The cohort is treated by the T-270
  compensator (the early
  recheck R15 of the `check_state` specification,
  `scheduler/stalerequeue.go`:
  the hourly pass returns new∧done older than 24 h to `pending` —
  `crawler_scheduler_stale_requeued_total`); the alert remains the
  signal
  to the operator about new losses (triage — operations.md §4.4).

Alert delivery to the operator (T-162, spec 5.9/10.4): state transitions
(firing/clearing) go to Telegram with the same bot and group as the
CI/CD notifications — monitor calls exec `telegram-notify.sh --message`
(the `AlertsOptions.OnPass` hook, `TelegramNotifier` in notify.go);
the secrets
`TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` are read from the unit's
EnvironmentFile. Debounce — an alert episode is delivered by exactly two
messages:
🚨 on first firing, "✅ Recovered" on clearing; repeated
passes, a return to firing after a source break and the first pass
(a monitor restart) — silence. Without the secrets or the script —
delivery is
off and the alerts remain Warns in journald (setup — operations.md
§4.3, the script — ci-cd.md §"Telegram notifications").

The front-side signal (on transitions, the style of `DiskWatchLoop` in
internal/content):
a Warn on firing, Info on clearing; "not measured" — Debug. Source
errors (DB, Redis, statfs, the ingest probe, stream groups) — a Warn
per pass, the loop lives on; the exception — sources whose unavailability
is expected in some environments (SMART without `/dev/nvme*` in a
container,
systemd unit events in a dev container, T-145; PSI on a kernel without
CONFIG_PSI, T-162) — a silent "not measured" degradation. The pass
state — an `AlertsResult` snapshot (ok/fired/not measured +
details) into the `AlertsOptions.OnPass` hook: a log line + a field; a
dedicated gauge registry in monitor is not created — the alert metrics
in Prometheus will be set up by the T-077 layer over `internal/metrics`,
to avoid duplicating registries and labels.

`web` (stage 13, T-121/T-122; a product extension outside the main
spec — ported from larasites): the public landing page. The framework is
`cmd/web`
(`config.Load` → `service.Run`, the pool via `OnDBPool`, metrics/probes —
loopback `127.0.0.1:9107`); the public listener `web.listen` —
its own `http.Server` inside OnRun with graceful stop on ctx
cancellation: the framework gives one HTTP port per service and hangs
`/metrics` on it,
which must not be exposed (spec §9) — the precedent of lifecycle
management inside OnRun. Routes: `GET /` — a redirect to the default
locale `/ru`; `GET /{ru|en}` — the landing page (T-122: the markup
ported from larasites — a hero with an SVG radar, stat cards with
count-up counters, a cloud of the top-50 zones with log scale and
collisions, a top-10 zone table with sorting, how-it-works, a manual
check form, recent findings with subdomain badges and a version range,
contacts/Telegram; SEO canonical/hreflang/OG/Twitter, a Yandex.Metrica
counter); data —
the T-120 caches + laravel_history ("findings over 24 h", T-118);
`POST
/api/check-domain` — a live check from the form (T-123; the pipeline
runs on
the long-lived circuit of `checker.Runtime`, T-220: one resolver chain
with a small UDP pool, a shared Fetcher, a rule cache, one
render.Manager —
assembled by the first check, closed on service stop; concurrent checks
are limited by `web.live_concurrency`; overload —
a fast 503 with `Retry-After`); `GET /api/v1/landing/stats` —
the landing statistics as JSON for the static landing of the separate
ld-landing repo (L-03, TZ-ld-landing §7.1: the same single
`FetchLandingData` pass — the counters, the top-50 zone cloud with the
log-scale label sizes, the top-10 table, the latest finds with badges,
the subdomain reference; serialized through a dedicated view model with
json tags — `internal/web/stats.go`, the internal structures are not
leaked, the collections are `[]` — never null; the client caching —
`Cache-Control: public, max-age=60`; a DB failure — a 500 without
details in the uniform JSON error body of the /api/* surface,
`{"error":"unavailable"}`); `GET /healthz` —
a PostgreSQL readiness ping (503); `GET /robots.txt` and
`GET /sitemap.xml` — the service files of search crawlers (T-124:
robots — statics from an embed; sitemap — a minimal locale set;
absolute URLs — from the canonical host `web.public_host` with a
fallback to the request's `Host`; the scheme — an actual TLS fact or
`X-Forwarded-Proto` from a trusted peer, T-227); `GET /static/*` —
statics (go:embed). The form visitor's address behind a proxy —
`X-Real-IP` from a TCP loopback peer or a unix-socket peer
(T-221; both trust positions — the deploy variant from
deploy/nginx/README.md; the fallback `X-Forwarded-For` without
`X-Real-IP` takes the last position of the list — the proxy appended
it); without a valid
header, a unix-socket peer is a bucket label without writing a non-IP
into
`web_checks`; the outcomes/failures/duration of the form — the metrics
`crawler_web_checks_total{result}` /
`crawler_web_check_errors_total{type}` / `crawler_web_check_seconds`
(T-221, the "Working logic metrics" table below). Templates —
`html/template` + `go:embed`; no new dependencies.

The CORS policy of the `/api/*` surface (L-04, TZ-ld-landing §7.2; the
allowlist wrapper — `internal/web/cors.go`): the static landings of the
separate ld-landing repo live on their own domains
(`sites.laravel.city`, `sites.laracity.ru`) and call this API straight
from the browser, so `/api/check-domain` and `/api/v1/landing/stats`
answer with the exact-allowlist echo when `web.cors.allowed_origins`
holds the request's `Origin`: `Access-Control-Allow-Origin: <the exact
origin>` (never `*`) and `Vary: Origin` — the latter on every `/api/*`
answer while the allowlist is set (the stats endpoint is publicly
cacheable, `max-age=60`, and a cache must key on `Origin` — otherwise
it would serve one origin's Allow-Origin to a request of another). A
preflight `OPTIONS` of an allowlisted origin — `204` with
`Access-Control-Allow-Methods: GET, POST, OPTIONS`,
`Access-Control-Allow-Headers: Content-Type` and
`Access-Control-Max-Age: 600`, without calling the endpoint (a
preflight does not spend the form's rate limit; the JSON body of the
form makes the browser preflight every check). A foreign or absent
`Origin` — no CORS headers, the request passes to the handler as is:
the browser blocks the script's read itself, and the server's controls
(the rate limit, the live-check budget) are origin-independent. An
empty/absent allowlist — CORS is off entirely: no CORS headers, and
`OPTIONS` on the API routes stays the router's 405 (the behavior before
L-04). The non-API routes (the landing pages, `/healthz`, statics) are
not wrapped at all. A note for a future shared cache: nginx
`proxy_cache` ignores `Vary` (its key is `proxy_cache_key`) — if one is
ever put in front of the API, the key must include `$http_origin`,
otherwise the exact cross-origin confusion `Vary` prevents returns.

The JSON contract of `GET /api/v1/landing/stats` (the landing API
section, L-03; the source of truth — the view model
`internal/web/stats.go`): `stats` — the counters `zones` (active),
`total`, `checked`, `laravel` (the sums over the zones reference
table) and `laravel_last_24h`, all numbers; `cloud` — the top-50 zones
by findings: `name` (ASCII/punycode, as stored), `name_unicode` (the
display form; omitted for ASCII zones), `laravel` (the tooltip
counter), `size` (the log-scale label size in rem, 0.875–1.5, already
rounded to 3 decimals — the client applies it without rescaling);
`table` — the top-10: `name`, `name_unicode`, `checked`, `laravel`,
`percent` (a percentage rounded to 2 decimals, not a 0..1 share);
`latest` — the latest finds: `name`, `badges` — the subdomain badges
`{label, host}` with a scheme-less ready host `label.domain`,
`versions` (the preformatted range "9.19–12" / "≥ 9.19"; omitted when
undetermined), `updated` (the preformatted en date "Jan 02, 2006" —
the display form of the SSR page; the locale formatting stays on the
consumer); `subdomains` — the reference table `{id, label}` (the
`found_on_mask` bits; includes the apex `{id:0, label:""}`). Empty
collections are `[]`, never null; non-GET — 405. A note for the
ld-landing client: the refresh action of the domains block
(RefreshButton, TZ-ld-landing §6) must bypass the browser cache —
`fetch` with `cache: "no-cache"` or a cache-buster — otherwise within
`max-age=60` a plain `fetch` is served from the HTTP cache without a
network round-trip and the "refresh" returns the identical body.

The landing CSS is Tailwind v4 without node: the source
`internal/web/static/app.src.css` (classes scanned by the template's
`@source`), the built bundle is not stored in git (`.gitignore` — a fresh
clone gives a page without styles, not a 500). The build: the `css`
stage in `deploy/docker/Dockerfile.go` (alpine + the standalone
tailwindcss CLI; the version — `TAILWINDCSS_VERSION` in `.env`; the
artifact is placed in the build stage before `go build` — go:embed
embeds the real CSS), or
`make web-css` for dev viewing (a one-off docker run).

Configuration — the `web` section (`listen`/`rate_limit`/`timeout`/
`live_concurrency`/`public_host`/`cors.allowed_origins`,
[`configuration.md`](configuration.md)). Deployment (T-124):
dev — the `web` compose service of the app profile (the landing port is
published on the host's `127.0.0.1`, `WEB_PORT`); production — the
`crawler-web.service` unit
(docs/deploy.md) behind a reverse proxy with TLS (the first sample —
`deploy/nginx/`).

The details of the browser layer (isolation, resource blocking,
waiting, result capture) — [`render.md`](render.md); live checks
against the `chromium` compose service — with the
`CRAWLER_RENDER_TEST=1` flag.

`dashboard` (stage 15, the dashboard spec; T-128 — the framework and
authorization): the internal operator dashboard of the pipeline — a
single at-a-glance state page (progress, speed, queues, findings, service
health), strictly read-only. The framework — `cmd/dashboard`
(`config.Load` → `service.Run`, the pool via `OnDBPool`, metrics/probes —
loopback `127.0.0.1:9108`, dashboard spec §4.2); the public listener
`dashboard.listen` —
its own `http.Server` inside OnRun with graceful stop on ctx
cancellation (the precedent of `cmd/web`, T-121) with the timeouts of
dashboard spec §6.3 (ReadHeader 5 s,
Read 10 s, Write 15 s, Idle 60 s). The startup credential check
(dashboard spec
§7.7): empty `dashboard.username`/`password_hash` (or an unparseable
hash) — a startup refusal before migrations and listeners; the common
`config.Validate`
does not check credentials (otherwise the requirement would cover all
services).

Routes (dashboard spec §6.1): `GET /` (the page), `GET /assets/*`
(statics) and
`GET /api/status` (the JSON of the latest snapshot, T-129; before the
first successful
collection — 503 `snapshot_not_ready`, §5.9) — behind authorization;
`GET /healthz` (a PostgreSQL ping, 503 on failure), `GET /livez`,
`GET /robots.txt` (`Disallow: /` — an internal service) — without
authorization. Anything but GET/HEAD — 405; all responses carry
`X-Content-Type-Options: nosniff` and the CSP of dashboard spec §6.3.

Authorization (dashboard spec §7, T-128): HTTP Basic — the name is
constant-time; the password — a PBKDF2 verification of the T-127 format
(`internal/dashboard/auth`,
`subtle.ConstantTimeCompare`, §7.4). After a successful verification a
cookie session `<expiry>.<HMAC-SHA256(expiry, the KDF-derived key)>` is
issued
(§7.5): HttpOnly, SameSite=Lax, 24 h, `Secure` by the fact of TLS — a
direct
connection or `X-Forwarded-Proto: https` from a loopback peer (T-227);
the fast signature check removes
the KDF cost (half a second per 600 000 iterations) from the periodic
polling; the key is the hash key — changing `password_hash` invalidates
all
sessions, no separate server secret is needed. Per-IP brute-force
protection
(§7.6): 5 failed verifications within a minute — 429 + `Retry-After`
until
the end of the window (in-memory, modeled on the T-123 token bucket; a
successful verification
resets the IP window); a failure slot is reserved atomically together
with the limit check and before PBKDF2 (T-231) — the window holds
parallel bursts from one
address too; a blocked address does not spend KDF, but beyond 5
concurrent
attempts from an IP the excess get 429 even with valid credentials
(after the first
successful verification the client goes with a cookie and is not touched
by the limiter); a valid
cookie is checked before the window — an operator with a live session is
not locked out by someone else's
failures from their address. The residual risk: the window is per-IP, a
distributed CPU-DoS from many addresses (5 verifications per minute
each, ~half a second
per verification) is not bounded by it — the public contour of
`dashboard.*` is worth
covering at the proxy too (`limit_req` in nginx, docs/deploy.md §6).
Failed verifications — a Warn with the IP address, without credentials
(dashboard spec §9.2). Behind a
reverse proxy the client address is carried by `X-Real-IP` (trust — only
from a
loopback peer, like the landing form T-123).

The snapshot collector (T-129, dashboard spec §4.3–4.4): a background
loop
`dashboard.Collector.Loop` (`internal/dashboard/collector.go`) next to
the public listener in `OnRun` — the first collection right at startup,
then with
the `dashboard.refresh` period (a corridor of 1 s…5 min, default 5 s);
a snapshot atomically replaces the previous one (`atomic.Pointer`);
`/api/status` serves
the last ready one — the DB load is fixed by the refresh frequency and
does not depend on the number of tabs (dashboard spec §3.2.3). Until the
first **successful** collection —
a pass in which the summary skeleton is alive (zones/totals, §5.1) —
`/api/status`
answers 503 `snapshot_not_ready` (§5.9). The readiness hierarchy — like
`crawlerctl status`'s: Redis being down from the very start does not hide
the DB summary
(queues "n/a" are served from the first pass — acceptance §12.3
works for a restart with Redis already down too); a down DB holds the
503
and the `result=error` metric until recovery. Later, degraded
snapshots are served: the replacement is atomic, a section failure does
not kill the output.

The pass price — the `internal/status` selections (the same ones
`crawlerctl status` pays on every run: the zone summary and the status
distribution — point reads of the reference table and `crawler_counters`
(T-178,
no `domains` pass); the rate — the daily checks counter
`checks:YYYY-MM-DD` with a point lookup (T-179; previously the live count
`last_checked_at = current_date` degraded with heap fetches of fresh
pages — slow-log query №1 of 24.10) and the hourly rate buckets
`results:`/`rendered:YYYY-MM-DD"T"HH24` as the sum of two live rows
(T-203;
previously live subqueries over `subdomain_results` degraded by the same
mechanism — the stage 27 checkpoint); the 24 h Laravel window — by the
index
`laravel_domains_updated_at_idx`; "recent ingest files" — a top-N
sort of the journal). Heavy sections (zones, rate, statuses) go
through the `status.Cache` TTL cache (T-161,
`dashboard.status_cache_ttl`, default 60 s — a corridor of 1–5 min):
within the TTL window the `refresh` passes reuse the last DB answer;
errors are not cached — a section degradation stays visible. Light
sections (recent findings by index, the ingest journal) bypass the
cache.

Snapshot sections (§5): zones/totals/rate/speeds/ETA and stream depths —
the shared `internal/status` library (the same SQL and calculations as
`crawlerctl status`, T-126; Redis — `BackpressureGate.Len`, like
monitor;
the service creates the Redis client itself from the `redis` section of
the configuration — like
`cmd/monitor`); the `domains` distribution by `status` (§5.5),
Laravel findings — total/over 24 h/the last 10 with a JOIN of zones by
the index
`laravel_domains_updated_at_idx` (§5.6) and the last 10 `ingest_files`
(§5.7) — also `internal/status` (one SQL source of the summary, §4.5).
The JSON shape of `/api/status` (§6.2): the shared sections — the
canonical
wire contract `status.SummaryJSON` (`crawlerctl status
--json` serves the same one — the fields do not diverge), plus
`statuses`/`laravel`/`ingest`/
`health`/`errors`/`built_at` and `queue_watermarks` — the effective
backpressure thresholds of the working streams (§5.4; since T-144
instead of the scalar
`queue_watermark`, which left the threshold unclamped): for every stream
except `q:dead` —
`queue.StreamWatermark(scheduler.queue_high_watermark,
redis.maxlen, stream)`, the same single calculation as the publication
gate
(T-026) and the monitor queue-overflow alert (T-080): a stream threshold
is no higher than half of its MAXLEN limit (T-141: at defaults `q:dns` —
125 batches, per-record streams — 75 000; 0 — the gate is off) —
configuration data,
not a degradation. For `q:render` this is the alert/highlight threshold:
the render capacity gate —
the separate `render.queue_high_watermark` (T-174,
default 10k — it closes earlier than the highlight; render-debt
observability — T-175). Unavailable sections — `null`; the reasons are
in the
`errors` map; `Cache-Control: no-store`.

The `render` section (T-246, the engines panel of render spec §4.6
item 4; completeness —
S-P3-2): the rate/success/tarpit-rate/RSS per engine and the fallback
block from the
`/metrics` expositions of the render workers (`dashboard.render_metrics_urls`
—
the same addresses Prometheus scrapes; the precedent — the ingest probe
at
monitor). JSON: `engines[]` — per engine `rendered_total` /
`finalized_total` / `tarpit_total` (cumulatives since the workers'
restart),
`success_pct` = rendered/(rendered+finalized) — the denominator without
tarpits
(the operator's call per S-P2-4); `tarpit_pct` = tarpits/(rendered+
finalized+tarpits) — the T-148 engine-branched failure as a separate
column
(the `engine` label — T-250); `null` instead of shares — no completions
yet;
`rss_bytes` — Σ `process_resident_memory_bytes` of the engine's workers
(0 —
an exposition without the `engine` label, a build older than T-235);
`rendered_per_sec`
— the rate between collector passes. The `fallback` block:
`published_total` /
`published_per_sec` (worker A); `rate_pct` — the fallback rate, the
share of
publications from the stage's pages (the denominator — Σ
`crawler_stage_tasks_total{stage="render"}` of all expositions; the same
units as the budget window T-239; the material of the spec §5.3
checkpoints);
`consumed_total` / `consumed_per_sec` / `consumed_by_result`
(rendered|finalized|skipped; the series are pre-created as zeros since
T-250);
`depth` — the `q:render-fallback` depth by the same calculation as the
queues
section (`null` — Redis unavailable). A `null` section — the addresses
are not set
("n/a", not an error) or all expositions are unavailable (the reason is
in `errors`).

Degradation (§5.9): sections are collected independently — a DB, Redis
or
probe failure does not kill the rest; the `result=error` label on
`crawler_dashboard_snapshots_total` and a Warn with the reason map raise
the noise; the loop lives on. The `/livez` probes (§5.8) from
`dashboard.probes` run
in parallel goroutines with a 2 s timeout (errgroup is not used —
`golang.org/x/sync` is an indirect dependency in go.mod; dashboard spec
§3.2.1 introduces no
new dependencies); an unavailable/slow service — data (`up:false` with
a reason), not a section error; an empty probe map — the section is
"n/a".

Own-service metrics (§10): `crawler_dashboard_snapshots_total{result}`
and `crawler_dashboard_snapshot_seconds` — the registry is created by
`cmd/dashboard`
(the table above).

The front (T-130): the `/` page and the `/assets/*` statics are
`go:embed` artifacts (`internal/dashboard/assets/`: `page.html`,
`app.js`,
`style.css`), without external CDNs and fonts (dashboard spec §3.2.2 —
an environment without
internet; the CSP of §6.3 admits only own scripts, styles and
connections);
the render is fully client-side; the server does not template HTML. The
Content-Type
of the statics is set by an explicit map (not the system mime file —
the environment
changes `.js` to `text/javascript`); a name outside the map — 404. The
markup (ru,
§6.5): the totals and speed cards — total/checked %/in work
(pending+queued)/domains per s (a day window)/HTTP responses per s and
renders per s
(an hour window)/the cycle ETA/Laravel findings total and over 24 h
(§5.1–5.2);
queues — six streams (including `q:render-fallback`, S-P3-1) with a
sparkline history and a highlight per each
working stream (without `q:dead` — dead tasks are not pipeline lag)
against its effective threshold from `queue_watermarks` — an approach
from 90 % and an excess, like the monitor's `queue_overflow` alert
(T-144:
after the MAXLEN clamp the stream thresholds differ, and the depths are
in different
units: q:dns is counted in batches; a sum of depths against one
threshold is
meaningless), and a non-zero `q:dead` (§5.4); the "Render engines"
panel (T-246, render spec §4.6 item 4; completeness — S-P3-2): a table
per
engines — the rate `Render/s`, the cumulative `Succeeded`/`Finalized`,
the shares
`Success` (rendered/(rendered+finalized)) and `Tarpit rate`
(tarpits/engine completions — the T-148 engine-branched failure as a
separate
column, S-P2-4); `RSS` — Σ of the engine workers' memory
(`process_resident_memory_bytes`); the fallback row — the
`fallback rate`
(the share of published from the stage's pages), the
publication/consumption rates and
the consumed outcomes (reworked/finalized/skipped — `|| 0` in case of
an exposition without pre-created series); the depth of
`q:render-fallback`; the zone table with
progress bars,
activity and pending/queued/done (§5.3); the status distribution —
a bar with counter legends per the legend of spec 6.2 (§5.5); the last
10
Laravel findings — domain, zone, score, version range, updated
(§5.6); the last 10 ingest files with row counters (§5.7); up/down
service health
chips with the reason in a tooltip (§5.8). Degradation
(§5.9): a section with an error in the snapshot — "n/a" with the reason
(a tooltip);
values without a window (speed/ETA) — also "n/a"; 503 before the first
snapshot —
a "snapshot is being prepared…" indicator. `app.js` polls `/api/status`
every
~5 s (the period — the page's `meta[name=dashboard-refresh]`; it may
differ from
the server's `dashboard.refresh` — the client shows the last ready
snapshot), keeps the speed and depth history in memory (~120 points ≈
10 min)
and draws sparklines as SVG polylines; 401 — stop polling and reload the
page: `fetch` does not show the browser's Basic dialog, while a repeated
request for the document itself gets `WWW-Authenticate` — the browser
offers
re-entry or silently resends the cached credentials (§6.4).
Deployment (T-131, dashboard spec §11): dev — the `dashboard` compose
service
of the app profile (a go-common clone: `SERVICE=dashboard`,
`METRICS_PORT=9108`,
`METRICS_ADDR=:9108` on the docker network for Prometheus; the port is
published on
the host's `127.0.0.1:8090` — `DASHBOARD_PORT`; `mem_limit` 256m;
credentials and
probes — the `dashboard` section in `deploy/docker/config.dev.yaml`,
the dev password — docs/dev-environment.md); production — the unit
`crawler-dashboard.service` (User=`crawler-dashboard`, Group=`crawler`,
`CRAWLER_DASHBOARD_LISTEN=127.0.0.1:8090`, `MemoryMax=128M`,
docs/deploy.md) behind a reverse proxy with TLS (dashboard spec §9.1);
Prometheus — the job
`dashboard` on :9108 (`deploy/docker/prometheus/prometheus.yml`).
Configuration — the `dashboard` section
([`configuration.md`](configuration.md)).

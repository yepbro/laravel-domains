# Service Configuration

A single YAML file + environment variable overrides (spec 5.10 of the
main spec). Package: `internal/config`; sample file:
[`config.example.yaml`](../config.example.yaml) at the repository root.

## Source Priority

Each parameter value is resolved along the chain:

```
environment variable  >  YAML file  >  default value
```

- **An empty variable value counts as unset** (YAML cannot be overridden
  with an empty stub — clear the field in the YAML itself).
- For lists (`resolvers`, `subdomains`) the variable replaces the list
  in full.
- After merging, all values are validated (see below); on any problem
  the service does not start, listing all errors at once.

## Configuration File

| What | How |
|---|---|
| Explicit path | the `CRAWLER_CONFIG` variable |
| Deprecated name | `APP_CONFIG` (used if `CRAWLER_CONFIG` is not set) |
| Auto-discovery | if neither variable is set — `config.yaml` in the current directory, when the file exists |
| No file at all | configuration from defaults + environment (validation is still mandatory: `postgres.dsn` is required) |
| Explicit path set, file missing | startup error (there are no silent defaults) |
| Unknown YAML key | startup error (typos are not ignored) |

Example of a local run from the repository root: `cp config.example.yaml config.yaml`
— the file is picked up automatically, without variables.

## Value Types

| Type | YAML form | Variable form | Example |
|---|---|---|---|
| string | YAML string | as is | `/data/incoming` |
| integer | YAML number | decimal number | `50000` |
| boolean | `true`/`false` | `strconv.ParseBool`: `true/false/1/0/T/F` | `false` |
| duration | `time.ParseDuration` string | same | `5s`, `30m`, `24h`, `4380h` |
| size | a byte count or a string with a unit | same | `2097152`, `512MB`, `1GiB` |
| float | YAML number | a number with a decimal point | `0.6` |
| list | YAML list | comma-separated items; edge whitespace is trimmed | `www,admin` |
| map | YAML mapping | `key=value` items separated by commas; edge whitespace is trimmed | `com=5,org=2` |

A duration given as a bare number (`total_timeout: 20`) is an error: the
unit would be ambiguous. Sizes: decimal units (`KB`, `MB`, `GB`) are
powers of 10³; binary ones (`KiB`, `MiB`, `GiB`) are powers of 2¹⁰.

For `subdomains`, an empty list item is the domain apex: in YAML it is
`""`, in a variable — an empty item between commas (`CRAWLER_SUBDOMAINS="www"`
does not set the apex; `",www"` does). A variable value replaces the
YAML list in full — do not forget the apex if you need it.

## Sections and Variables

### `postgres`

| YAML | Variable | Type | Default |
|---|---|---|---|
| `dsn` | `CRAWLER_POSTGRES_DSN` | string | — (**required**) |
| `pool_size` | `CRAWLER_POSTGRES_POOL_SIZE` | integer | `20` |

`pool_size` — the connection limit of **a single service** (the
`internal/db` pool, T-007); the spec recommends 20–50 connections in
total across all services, i.e. with nine services set 2–5 per service
(the production layout — docs/deploy.md §4.5). Connections are opened
lazily. See [`db.md`](db.md) for details.

### `redis`

| YAML | Variable | Type | Default |
|---|---|---|---|
| `addr` | `CRAWLER_REDIS_ADDR` | host:port | `127.0.0.1:6379` |
| `maxlen` | `CRAWLER_REDIS_MAXLEN` | integer | `0` (per-stream limits, see below) |
| `max_attempts` | `CRAWLER_REDIS_MAX_ATTEMPTS` | integer | `3` (spec 4.2) |
| `claim_min_idle` | `CRAWLER_REDIS_CLAIM_MIN_IDLE` | duration | `10m` |
| `recover_batch` | `CRAWLER_REDIS_RECOVER_BATCH` | integer | `64` |
| `recover_interval` | `CRAWLER_REDIS_RECOVER_INTERVAL` | duration | `30s` |
| `trim_interval` | `CRAWLER_REDIS_TRIM_INTERVAL` | duration | `1m` |
| `consumer_idle` | `CRAWLER_REDIS_CONSUMER_IDLE` | duration | `2h` |

`maxlen` — an explicit length limit for all streams at publication time
(`XADD MAXLEN ~`; trimming is approximate, without exact counting).
`0` — per-stream limits from the queue reliability policy (T-025,
recalculated from the T-141 measurements): `q:dns` 250 batches (an entry
≈ 64 KiB at `scheduler.batch_size` = 1000 domains, measured);
`q:http`/`q:render`/`q:analyze` 150,000 (individual entries 0.3–0.7 KiB);
`q:render-fallback` 15,000 (a share of the individual-entry flow: the
fallback collects the share of the render flow bounded by the share
budget, spec 4.3; the cap is not gated — it is bounded by the share
budget and worker B's rate); `q:dead` 10,000 (entries without the
payload of the original tasks — subject identifiers instead of the
message, T-105: a byte cap of single MiB units). The sum of the caps
≈ 337 MiB fits the Redis budget ≤ 0.5 GB (spec 3.2; the arithmetic —
[`redis.md`](redis.md) §3.1). The limits trim primarily the history of
acknowledged entries (which `trim_interval` releases anyway) and are
well above the backpressure threshold
(`scheduler.queue_high_watermark`): an untrimmed, unprocessed entry is
not lost without trace — the scheduler restores the tasks (the source
of truth is PostgreSQL, spec 3.4). The explicit limit is
1000..100000000.

`max_attempts` — the maximum number of task deliveries (the first group
read + each return of a stuck entry by claiming with `XAUTOCLAIM`),
after which the task is published to `q:dead` for manual analysis; the
counter is maintained by Redis in the group's pending list. Range
1..100; 1..3 — at least one processing before the task moves to
`q:dead`.

`claim_min_idle` — how long a stuck entry (read but not acknowledged)
must stay idle before the recovering `XAUTOCLAIM` takes it over; more
than the worst-case hold time of the delivery window by a live worker
(not of a single task: the consumption window × the stage timeout at
minimal parallelism), otherwise another worker's work is hijacked
mid-flight. The worst window — render with sliding consumption
(T-142): up to `2×16 + 8 ≈ 40` entries (an XREADGROUP batch in the
reader's hands + the prefetch buffer + in-flight deliveries); with the
defaults `render.contexts=8`/`render.timeout=25s` the window tail waits
≤ ≈2.1 min — the 10m default gives ~4.8× headroom (the numbers are
recalculated for the default of 8, T-169). With smaller
`render.contexts` or larger `render.timeout` raise the threshold
proportionally (at delivery parallelism 1 the window grows to
≈13.8 min — the default is no longer enough).

`recover_batch` / `recover_interval` — the pace of returning stuck
entries to work (T-153): each worker's background RecoverLoop claims a
batch of `recover_batch` entries per pass with `XAUTOCLAIM` every
`recover_interval` (a full batch drains immediately, without a pause;
the interval is the pause after a partial one). The PEL self-healing
time after an incident ≈ `PEL size / (recover_batch /
recover_interval)` per worker: at the default 64/30s a PEL of ~5K
entries is traversed in ≈39 min (before T-153 at 16/30 s — ≈2.6 h; in
the 22.10 incident burning through the attempts of one cohort took
12 h). The cost of raising the batch is bounded by the idempotency of
retry (spec 3.4) — a retry costs stage time, not correctness.

The invariant with `claim_min_idle`: RecoverLoop traverses the batch
sequentially and keeps its entries in the PEL for the whole traversal,
so the batch processing time — `recover_batch` × the worst-case time of
a single stage entry (individual tasks: render `render.timeout`,
HTTP `http.total_timeout`; q:dns — a batch episode) — must stay below
`claim_min_idle`, otherwise the batch tail is re-claimed by the
neighboring worker's RecoverLoop: +1 attempt per round, the same
mechanism that burned a cohort in the incident. The default of 64 is
chosen by this criterion: a typical render entry ~5 s (T-092 §4) × 64
≈ 5.3 min < 10 min — ×1.9 headroom; 128 already misses the typical case
(≈10.7 min), and a batch consisting entirely of tarpits (25 s/entry) is
re-claimed even at 64 (≈26.7 min) — a deliberate limit. When raising
`recover_batch`, raise `claim_min_idle` proportionally: e.g.
256 × 25 s ≈ 1.8 h → `claim_min_idle ≥ 2h` (at the cost of slower
recovery of the entries of fallen workers). Ranges: `recover_batch`
1..1000, `recover_interval` 1s..1h.

Cancellation in the middle of a batch (graceful shutdown, worker
restart) does not burn the attempts of the unprocessed tail (T-222):
`XAUTOCLAIM` increments the delivery counter of the whole PEL batch at
claim time — not at processing start — so the entries the traversal has
not reached get their attempt returned by the
`XCLAIM … JUSTID RETRYCOUNT <attempts-1>` command under a separate
short context (up to 5 s inside the framework's overall shutdown budget
`ShutdownTimeout`, 10 s by default, not changeable via configuration;
the loop context is already cancelled by then, and the return happens
after the current entry is finished). Reading the counters after the
claim also survives cancellation — otherwise entries without a counter
would be unrecoverable. The return runs while the batch traversal fits
into `claim_min_idle` (the invariant above): otherwise the tail may
have been re-claimed by a neighboring worker, and the return would
cancel someone else's increment — the return is skipped with a Warn.
An entry already taken up by processing spends its attempt — the
delivery happened. The price of the return is that `XCLAIM` refreshes
the delivery time: returned entries ripen for `claim_min_idle` again
until the next claim. The limits of the mechanism (best effort): a
hard kill of the process in the window between claim and return, or
Redis unavailable at shutdown, leave the attempts spent — as before
T-222.

`trim_interval` — the period for releasing the acknowledged prefix of
the streams (T-141, spec 4.2 "Supplement 2026-10-23"): `XACK` does not
remove entries from the stream, so the scheduler loop
(`queue.TrimLoop`) once per period runs `XTRIM MINID` up to the oldest
entry that must remain (the oldest pending of the group; with empty
pending — last-delivered-id) — only delivered and acknowledged data is
removed; stuck PEL entries and unprocessed tasks are not lost. The
mechanism's invariant — one consumer group per working stream (all
stages work this way): a group created later starting at "0" would see
only the untrimmed tail — that is already acknowledged work with its
result in the DB; re-delivery is not needed (the source of truth is
PostgreSQL, spec 3.4). Between passes no more than one period's worth
of acknowledged entries accumulates (a few MiB at the current stage
rates); the cost of a pass is three Redis commands per stream; range
1s..24h. Details and the budget arithmetic — [`redis.md`](redis.md) §3.

`consumer_idle` — the consumer-inactivity threshold of a group for
cleanup (T-155): every `XREADGROUP` read pins entries to a consumer
(the name is the worker process with pid), and after restarts (deploys,
crashes, OOM) consumers remain in the groups forever — 4 deploys of
wave 21 left 11 dead in `q:render` and 13 in `q:http`
([`operations.md`](operations.md) §5). The scheduler loop
(`queue.CleanupConsumersLoop`; the period is the same `trim_interval`:
the pass is cheap, a separate rate would not be operational) removes
with `XGROUP DELCONSUMER` consumers that are **strictly empty**
(pending = 0) and inactive longer than the threshold. Consumers with
pending ≠ 0 are never touched: `DELCONSUMER` removes the consumer
together with its PEL entries — undeliverable forever (the individual
tasks of `q:http`/`q:render`/`q:analyze` are not restored by lease,
only by claiming from the PEL with `XAUTOCLAIM`); a dead consumer's
dangling pending does not block claiming and is handled by
`recover_batch`/`recover_interval`. Before removal the emptiness of the
PEL is rechecked with `XPENDING` (the race "a lurking worker received a
delivery between `XINFO CONSUMERS` and `DELCONSUMER`"); a PEL carried
away anyway is detected from the `DELCONSUMER` reply and logged as a
Warn. The 2 h threshold — with headroom on top of the lull between
deliveries of an active stage (seconds); a live consumer of an empty
stream may idle longer (`XREADGROUP` without entries does not update
idle), but removing it is harmless — the name is recreated by the first
read with entries. Range 1m..30d.

Redis's own configuration (dev/production: `maxmemory`, `noeviction`,
no persistence) — [`redis.md`](redis.md).

### `ingest` (spec 5.1)

| YAML | Variable | Type | Default |
|---|---|---|---|
| `dir` | `CRAWLER_INGEST_DIR` | path | `/data/incoming` |
| `batch_size` | `CRAWLER_INGEST_BATCH_SIZE` | integer | `50000` |
| `stable_seconds` | `CRAWLER_INGEST_STABLE_SECONDS` | seconds | `10` |
| `scan_interval` | `CRAWLER_INGEST_SCAN_INTERVAL` | duration | `30s` |
| `max_error_ratio` | `CRAWLER_INGEST_MAX_ERROR_RATIO` | number | `0.1`, range (0, 1] |
| `archive_max_uncompressed` | `CRAWLER_INGEST_ARCHIVE_MAX_UNCOMPRESSED` | size | `8GiB`, range 1B..1TiB |
| `archive_max_files` | `CRAWLER_INGEST_ARCHIVE_MAX_FILES` | integer | `1000`, range 1..1000000 |
| `allowed_zone_paths` | `CRAWLER_INGEST_ALLOWED_ZONE_PATHS` | list of zone names | `[]` (the production value) |

`dir` — the watched folder (created if missing). `stable_seconds` — how
long a file's size and mtime must stay unchanged before ingest takes it
for processing (files still being appended and temporary
`*.tmp`/`*.part` files are not taken). `scan_interval` — the period of
a full folder walk as a safety net for fsnotify (T-013): it picks up
files that existed before the service started and compensates for lost
watch events.

`max_error_ratio` — the threshold for the share of rejected lines of a
file (T-016, spec 5.1): a line must be a full domain name from the
path's zone; invalid names, foreign zones and overlong lines are
rejected into the `rejected` log with a reason and do not fail the file
— only a rejection share strictly above the threshold makes it bad
(10% by default: single junk is forgiven; a file in the wrong zone or a
broken dump gives a share close to 1). For archives (T-133) the
threshold applies to a member: any member with a rejection share above
the threshold fails the whole archive.

`archive_max_uncompressed` and `archive_max_files` — limits for
ingesting `.zip`/`.tar`/`.tar.gz`/`.tgz`/`.gz` archives (T-133),
protection against zip bombs; a zero value does not disable the
protection. `archive_max_uncompressed` — a cap on the total
uncompressed volume: for tar/gz it is counted over the entire
unpacked stream (tar headers included — a metadata bomb is caught);
for zip — by the `UncompressedSize64` header before reading a member
and by actual reading (the header may lie). `archive_max_files` — a cap
on the number of entries, including directories and skipped junk (for
zip — by the central directory before reading members; for tar — over
the stream). Exceeding any limit, as well as a broken archive, sends
the archive as a whole to `failed/` with a log report. Archive ingest
semantics — [`incoming-files.md`](incoming-files.md) §9.

`allowed_zone_paths` — an allowlist of multi-label zones for ingest
directories (spec supplement tld-only-zones §5.1, §6.1). Working
directories are TLD-only (`incoming/<tld>/add|delete/`): a name's zone
is its last label, and a full TLD dump is accepted as a single file. A
directory with a dot in the zone name (`incoming/<dotted zone>/`) is
allowed only for zones from this list — testbed test zones
(`crawl.test`) that TLD semantics forbids from being a "path zone"; a
file in a directory outside the list is a file-level error (`failed/`,
lines are not parsed). Entries are valid zone names
(`idn.NormalizeZone`) in lowercase punycode with at least one dot (a
TLD zone has no business in the allowlist); normalization and dedup
happen at configuration load. The environment variable replaces the
list in full, items separated by commas. The production value is empty
(there are no test zones in production); the dev environment lists the
bench scenario zones — [`dev-environment.md`](dev-environment.md),
testbed.

### `scheduler` (spec 5.3)

| YAML | Variable | Type | Default |
|---|---|---|---|
| `recheck_interval` | `CRAWLER_SCHEDULER_RECHECK_INTERVAL` | duration | `4380h` (≈ 6 months) |
| `batch_size` | `CRAWLER_SCHEDULER_BATCH_SIZE` | integer | `1000` (spec 5.3: 1000–10000) |
| `queue_high_watermark` | `CRAWLER_SCHEDULER_QUEUE_HIGH_WATERMARK` | integer | `100000` (backpressure, spec 4.2) |
| `lease` | `CRAWLER_SCHEDULER_LEASE` | duration | `30m` |
| `temp_error_retry_after` | `CRAWLER_SCHEDULER_TEMP_ERROR_RETRY_AFTER` | duration | `24h` (at values < 24 h the maturity-bucket guard ripens in cohorts toward midnight — intra-day candidates wait for drainage until midnight, degradation ≤ a day; T-202, details in `docs/schema.md` §crawler_counters) |
| `temp_error_max_retries` | `CRAWLER_SCHEDULER_TEMP_ERROR_MAX_RETRIES` | integer | `3` (the maximum of consecutive `temp_error` retries, then the final status; spec 5.3: N) |
| `backlog_interval` | `CRAWLER_SCHEDULER_BACKLOG_INTERVAL` | duration | `5m` (allowed range 30s..1h; T-161) |
| `zone_weights` | `CRAWLER_SCHEDULER_ZONE_WEIGHTS` | map name=weight | — (all weights 1, equal round-robin order) |

`recheck_interval` — the minimum age of the last check
(`domains.last_checked_at`, spec 3.3) after which a domain is queued
again: the scheduler's daily pass moves its `check_state` from `done`
to `pending` (spec 5.3; rechecking is no more frequent than this
interval, spec 11.4 item 8). The daily pass rhythm itself is not
configurable.

`backlog_interval` — the period of the backlog-depth collector
(`scheduler.BacklogLoop`, T-077): the `crawler_backlog_domains` gauge
from `GROUP BY check_state` over all live domains. The default of
5 minutes (T-161): the original 30 s at the lower edge of the spec 5.9
range (30–60 s) target a small corpus — at 47M rows (measurement §12
[`load-measurements.md`](load-measurements.md)) a pass takes 17+ s and,
together with zone recounting and the status aggregates, accounts for
most of postgres CPU; the dashboard's depth gauge tolerates
five-minute freshness. Allowed range 30s..1h: more often than 30 s —
full unindexed `domains` passes (T-104) cost more than the metric
itself; less often than an hour — depth stops being an observable
quantity. Passes do not stack (single-flight): a pass longer than the
interval drops the accumulated ticker tick; a Warn and the
`crawler_backlog_skipped_ticks_total` counter raise the noise (T-161).

`queue_high_watermark` — the backpressure threshold (spec 4.2, 5.3):
shared by the working streams `q:dns`, `q:http`, `q:analyze` (the
`q:render` capacity gate is the separate `render.queue_high_watermark`,
T-174: publication there is non-blocking; the "publish or flag"
decision — the `render` section above). The publisher — the scheduler
for `q:dns` (T-028) and the workers for the other streams — publishes
only while the stream depth is below the threshold; upon reaching the
threshold it pauses (1 s between checks) and retries; context
cancellation is a clean exit. Depth is the consumer group's backlog
(XINFO GROUPS + XLEN: the unread remainder `lag`, clamped by the
stream length, + unacknowledged deliveries `pending`): it decreases as
entries are read and acknowledged, i.e. the gate opens together with
consumer progress. Clamping the lag by the stream length is the T-168
clamp: `lag` does not subtract entries trimmed by the trimming of
undelivered data, and Redis 7.0.x keeps a phantom lag forever (the
22–23.10 incident — runbooks §2.7); the clamp does not understate a
real backlog (while undelivered entries are in the stream,
`lag` ≤ XLEN). The effective threshold of a particular stream is at
most half of its `MAXLEN ~` limit (T-025/T-141): the gate must trip
before trimming, which removes entries regardless of whether they were
read; for `q:dns` the configured 100,000 entries is bounded to 125
batches (the limit 250, ≈ 8 MiB), for individual entries — 75,000
(the limit 150,000, ≈ 52 MiB). `q:dead` is not bounded: moving tasks
that exhausted their attempts must not be blocked by an overflow; its
byte-level protection is entries without the original tasks' payload
(T-105, single MiB units at the cap of 10,000). The threshold stays in
the `scheduler` section per its place in spec 5.10 (the fixed section
set), although it applies to streams of several stages (the `q:render`
capacity — an exception, T-174).

`lease` — the lease term: a domain in `check_state = queued` with no
result (`queued_at` older than `lease`) is returned by the scheduler's
background pass to `pending` for re-queuing (spec 5.3: "stuck" tasks —
worker death, Redis loss). The pass checks expiry twice per term (the
interval `lease`/2), so a domain is returned at most one and a half
terms after queuing. Deleted domains (`status = deleted`) are not
returned — they are excluded from the check cycle (spec 13.1).

`batch_size` — the size of the selection batch published to `q:dns`
(the stream entry unit, spec 4.2). The default of 1000 is the lower
normative bound of spec 5.3 (1000–10000) and the measured entry size
≈ 64 KiB (T-141): the batch is the unit of reprocessing when a stuck
delivery is returned (XAUTOCLAIM returns the whole batch) and of
re-selection by lease; a smaller batch is a smaller retry window.
Stream memory caps are proportional to the batch: the q:dns limit
(250 batches) is derived from the worst normative batch (10,000
domains); the "sum of caps ≤ budget" invariant for the whole range is
pinned by the tests `queue.TestStreamPolicyFitsBudget` and
`config.TestSchedulerBatchFitsStreamBudget`; the arithmetic is in
[`redis.md`](redis.md) §3.1. The queuing rate is limited to one batch
per loop interval (1 s): at a batch of 1000 that is ≈1000 domains/s
versus the DNS stage's ≈210 dom/s measured by T-092 — ×5 headroom;
raising the batch "for speed" raises the stream memory at the same
time.

`zone_weights` — zone weights for round-robin batch selection
(spec 5.3: "zone weights — optional in the config"): the key is the
zone name in lowercase punycode, as in the `zones` reference table
(zones are TLDs: `com`, `in`; IDN — `xn--p1ai`; public-suffix zones
such as `co.uk` collapse into their TLDs — spec supplement
tld-only-zones; the collapse window — [`runbooks.md`](runbooks.md) §8;
the unicode form is not allowed — the reference table does not contain
it), the value is an integer weight 1..1000. A zone with weight `w`
occupies a share of the batch proportional to `w` (stride scheduling);
no map, or weight 1 for all, means equal round-robin order (the
default behavior; a large zone does not delay the others). Names are
resolved into zone identifiers via the reference table once at
scheduler start: a name may be missing (ingest creates the zone with
its first file, spec 5.1) — a Warn is logged and the zone is checked
with weight 1 until a restart picks up the appeared zone; the weight of
an inactive zone (`zones disable`) takes effect upon its enabling
without a restart. The variable replaces the YAML map in full, items
separated by commas: `CRAWLER_SCHEDULER_ZONE_WEIGHTS="com=5, xn--p1ai=2"`.

### `dns` (spec 5.4)

| YAML | Variable | Type | Default |
|---|---|---|---|
| `resolvers` | `CRAWLER_DNS_RESOLVERS` | list of host:port | `127.0.0.1:53` (unbound) |
| `fallback_resolvers` | `CRAWLER_DNS_FALLBACK_RESOLVERS` | list of host:port | `1.1.1.1:53, 9.9.9.9:53` |
| `concurrency` | `CRAWLER_DNS_CONCURRENCY` | integer | `2000` |
| `max_qps` | `CRAWLER_DNS_MAX_QPS` | integer | `10000` |
| `ipv6` | `CRAWLER_DNS_IPV6` | boolean | `true` (`false` — AAAA is not queried) |
| `timeout` | `CRAWLER_DNS_TIMEOUT` | duration | `2s` |
| `tcp_timeout` | `CRAWLER_DNS_TCP_TIMEOUT` | duration | `5s` |
| `retries` | `CRAWLER_DNS_RETRIES` | integer | `2` (range 1..10) |
| `retry_delay` | `CRAWLER_DNS_RETRY_DELAY` | duration | `200ms` |
| `udp_pool_size` | `CRAWLER_DNS_UDP_POOL_SIZE` | integer | `2000` |

`timeout`/`tcp_timeout`/`retries`/`retry_delay`/`udp_pool_size` —
transport parameters of `internal/dnsclient` (T-034, spec 5.4):
`timeout` — a single UDP attempt of an exchange with the resolver;
`tcp_timeout` — repeating the same question over TCP when the answer
arrived with the truncation flag (TC=1); `retries` — retries beyond
the first attempt on timeout/network error (after they are exhausted
the stage consumer records a temp_error, spec 5.4 item 1);
`udp_pool_size` — the pool of pre-opened connected UDP sockets to the
first resolver from `resolvers`; by default it equals `concurrency`,
so that the pool does not limit the planned parallelism.

`concurrency`/`max_qps`/`ipv6` are applied by the limit decorator
`dnsclient.LimitResolver` (T-035, spec 5.4: "parallelism and QPS
limiting is configurable; adaptive reduction as the timeout share
grows"; spec 5.10): `concurrency` — the cap on concurrent exchanges;
`max_qps` — the average rate (a token bucket with bursts of up to
`concurrency` exchanges in a row). Adaptation: at a timeout share
≥ 10% over the window of the last 200 exchanges both limits are
reduced ×0.5 (down to a floor of base/100, but no less than 1); at a
share ≤ 2% they are restored ×1.25 up to the configured values;
between the thresholds — a deadband. Thresholds/factors are constants
of `internal/dnsclient` (the defaults are derived from the stage's
properties; the spec does not set numbers); a snapshot of the current
limits and counters — `LimitResolver.Stats()` for monitoring (T-077).

`ipv6: false` ("if the server has no IPv6, AAAA queries are disabled
via config", spec 5.4) — AAAA questions are answered locally by the
limiter with an empty NOERROR (NODATA), without going to the resolver
and without spending the limits: a crawler without IPv6 does not check
AAAA addresses over HTTP either, and resolver traffic is lower.

`fallback_resolvers` (spec 5.4: "public resolvers — only as a
fallback") — the backup chain of the `dnsclient.FallbackResolver`
decorator (T-107): each address is its own client with a socket pool
of the same size (in switching mode the backup sustains the pipeline's
planned parallelism in full; an idle connected UDP socket consumes
nothing). If the primary resolver (`resolvers[0]`) exhausted its
attempts on a network error — timeout, refusal, disconnect (in the
client contract this is exactly "local attempts exhausted" and
"unreachable") — the question is retried along the backup chain until
the first answer. A server answer with any RCODE does not count as a
switch: NXDOMAIN → `not_delegated` and SERVFAIL → `temp_error` remain
consumer semantics (T-036/T-039); ctx cancellation is not a resolver
refusal. Switching happens on every exchange, without remembering a
"dead" state: recovery of the local unbound is picked up immediately,
at the price of each exchange's latency growing by the failed one's
retry cycle (`timeout` × `retries`+1) while it is silent. At startup
the dns-worker probes the chain with a synthetic probe (the root NS,
bypassing the limiter): if part of it is silent — a Warn is logged
(the chain keeps working by switching); if all are silent — a startup
error (fail-fast: a pipeline without resolvers would dump domains into
`temp_error`). Switches are observable via the
`crawler_dns_fallback_switches_total` metric (exchanges answered by
the backup) and `crawler_dns_fallback_exhausted_total` (nobody
answered — a future `temp_error`); analysis of degraded behavior —
[`runbooks.md`](runbooks.md), section 4. unbound itself stays purely
recursive: the semantics of `forward-zone` in unbound makes forwarders
primary; the direction "recursion → forwarder" does not exist (more in
[`docker.md`](docker.md), the "Unbound" section). The defaults are
publicly reachable anycast resolvers of two independent operators
(Cloudflare, Quad9), as in spec 5.10; an empty list disables the
backup (isolated environments, the dev config — there will be no
backup probes at all).

### `http` (spec 5.5, the limits table)

| YAML | Variable | Type | Default |
|---|---|---|---|
| `concurrency` | `CRAWLER_HTTP_CONCURRENCY` | integer | `2000` |
| `per_ip_limit` | `CRAWLER_HTTP_PER_IP_LIMIT` | integer | `4` (recommendation: 3–4) |
| `per_ip_overrides` | `CRAWLER_HTTP_PER_IP_OVERRIDES` | map CIDR → integer | empty (T-165) |
| `connect_timeout` | `CRAWLER_HTTP_CONNECT_TIMEOUT` | duration | `5s` |
| `tls_handshake_timeout` | `CRAWLER_HTTP_TLS_HANDSHAKE_TIMEOUT` | duration | `5s` |
| `header_timeout` | `CRAWLER_HTTP_HEADER_TIMEOUT` | duration | `10s` |
| `total_timeout` | `CRAWLER_HTTP_TOTAL_TIMEOUT` | duration | `20s` |
| `max_redirects` | `CRAWLER_HTTP_MAX_REDIRECTS` | integer | `8` (`0` — do not follow) |
| `max_body_bytes` | `CRAWLER_HTTP_MAX_BODY_BYTES` | size | `2097152` (2 MiB) |
| `min_body_speed` | `CRAWLER_HTTP_MIN_BODY_SPEED` | size (bytes/s) | `2048` (spec 5.5: the minimum speed after the first byte) |
| `max_rps` | `CRAWLER_HTTP_MAX_RPS` | integer | `1000` |
| `user_agent` | `CRAWLER_HTTP_USER_AGENT` | string | `DomainCrawler/1.0 (+https://example.example/contact; admin@example.example)` |
| `block_internal_addrs` | `CRAWLER_HTTP_BLOCK_INTERNAL_ADDRS` | boolean | `false` (the deploy example — `true`; mandatory for Internet operation) |
| `task_concurrency` | `CRAWLER_HTTP_TASK_CONCURRENCY` | integer | `192` (T-157; the return to 256 — by measurement after T-163) |
| `task_consume` | `CRAWLER_HTTP_TASK_CONSUME` | integer | `16` (T-139) |
| `task_per_domain` | `CRAWLER_HTTP_TASK_PER_DOMAIN` | integer | `8` (T-139) |
| `dead_host_cutoff` | `CRAWLER_HTTP_DEAD_HOST_CUTOFF` | boolean | `true` (T-164) |
| `mem_limit` | `CRAWLER_HTTP_MEM_LIMIT` | size | `805306368` (768 MiB, T-163; `0` — disabled) |

`user_agent` is required: an honest User-Agent with the operator's
contacts in the customary form `Name/version (+URL; e-mail)`
(spec 5.5, §9) — from the UA a site owner can tell who is checking
them and why, and get in touch. The default is placeholders on a
reserved domain and address (RFC 2606): the operator substitutes real
contacts (a contacts page and/or e-mail) into the YAML or the
`CRAWLER_HTTP_USER_AGENT` variable. Validation: non-empty, up to
512 bytes, no line breaks or control characters (the value goes into a
header).

Minimum requests (spec 5.5, §9): the HTTP stage requests only the main
page `/` and never touches `robots.txt`; other paths inside a redirect
chain are set by the server (Location, meta refresh), not the crawler.
One request per (subdomain, variant) without repeats is sustained by
the worker's final-URL cache; the `internal/httpfetch` transport does
not retry. A side effect of the cache: on a cache hit the record's
`final_url` is the check's starting URL, not the final URL of the
primary source (query is not part of the cache key and is discarded) —
deliberate behavior (T-266, the FSM spec of
`internal/httpworker/schemestate.go`); host/path are preserved
exactly.

The section's timeouts are layered (the `internal/httpfetch`
transport, T-047): `connect_timeout` bounds establishing a TCP
connection to a single address; `tls_handshake_timeout` — the TLS
handshake; `header_timeout` — waiting for the response headers
(ResponseHeaderTimeout); `total_timeout` — the whole request,
including body reading. The connection is made to pre-resolved IPs
from the DNS stage; HTTP-redirect hosts are resolved by the same
resolver with a cache (spec 5.5 item 2). With several addresses,
connection attempts start in turn with a 300 ms pause (a
happy-eyeballs-like race, RFC 8305) — one address failing does not
burn the request's timeouts.

Limits and protection (T-049, `internal/httpfetch/limits.go`): the
body is read up to `max_body_bytes` — exceeding it is not a network
error; the body is cut off with a "data is partial" marker
(`BodyLimiter.Truncated`, the decision belongs to the consumer).
Anti-tarpit — `min_body_speed`: after the first byte, the deadline for
reading the rest is extended as reading progresses
(`now + rest/speed`); a violation is classified as a timeout and the
connection is closed. The number of concurrent connections per dialed
IP — `per_ip_limit` (waiters stand in a queue that can be interrupted
by request-context cancellation); global connections — `concurrency`;
the request rate — `max_rps` (a token bucket with bursts up to
`concurrency`, the same mechanics as `dns.max_qps`). Keep-alive is off
(a spec 5.5 variant "off or a very short idle"): one main page per
site; a per-host pool would outlive address re-resolution and keep
thousands of inactive connections at concurrency 2000.

`per_ip_overrides` (T-165) — raised per-IP limits for the prefixes of
domain parkers: a "CIDR → limit" map in YAML
(`per_ip_overrides: {"195.208.0.0/16": 12, "2a02:6b8::/32": 8}`) or via
the variable (`CRAWLER_HTTP_PER_IP_OVERRIDES=195.208.0.0/16=12,2a02:6b8::/32=8`,
replaced in full, keys normalized to the masked CIDR). Matching of the
dialed IP is by the longest matching prefix (longest-prefix,
deterministic semantics: one network cannot be listed twice —
validation rejects duplicates after masking; `195.208.0.0/16` and
`195.208.9.9/16` are one network; IPv4 and IPv6 prefixes do not cross
families). The limit is within the `per_ip_limit` range (1..64) and may
be below the default (a soft hoster); actual parallelism is still
bounded by the global `concurrency`. An empty map is the previous
behavior: a single `per_ip_limit` for all IPs. Motivation — head-of-line
blocking on parker IPs (a T-092 §3 finding, docs/load-measurements.md):
in the com/xyz mix millions of domains live on parker IPs;
`per_ip_limit=4` with the FIFO order of the semaphore queue stretches
tasks toward 20 s. Material for choosing prefixes and values — the
wait histogram `crawler_http_per_ip_wait_seconds` (:9104): any action
is justified when the share of waits > 2 s reaches 5% on the
production mix. The next step by the numbers (not implemented in
T-165; activation by the same ≥ 5% criterion) — consumption IP
diversity: the task-slot dispatcher prefers a delivery whose IP is
free under the per-IP limit (a heuristic over the head of the read
buffer), so as not to push the override limits while the queue evens
out on its own.

`block_internal_addrs` — the SSRF guard on every connection path
(T-219): with the flag `true`, internal addresses (loopback,
RFC1918/ULA, link-local — including cloud metadata 169.254.169.254,
unspecified, multicast, including the IPv4-in-IPv6 form
`::ffff:127.0.0.1`) are forbidden from any source. The single funnel of
`internal/httpfetch` (filterInternalAddrs) filters both the DNS-stage
addresses of the q:http task's starting host (FetchRequest.Addrs) and
the Location/meta refresh literals with the redirect hosts'
resolutions; the render additionally rejects tasks with internal hosts
before Chromium is engaged (the task class, `validateTaskURLInternal`:
IP literals of any form, including numeric-looking
`127.1`/`2130706433`, and loopback names), and with the flag enabled it
intercepts every request of the browser session (`blockInternal`):
navigations via HTTP/JS redirects and subresources with an internal
host are rejected before hitting the network — the price is a CDP
round trip per request, so the interception is only enabled by the
flag. Since T-230 the interception also resolves arbitrary hostnames:
a request host that resolves to an internal address at render time is
rejected by the same failRequest — time-based DNS rebinding (the name
public at the HTTP stage, internal by render time) is closed inside
Chromium itself. Resolutions go through the system resolver (the same
one Chromium uses), are cached per session with a TTL of 30 s and are
capped at 3 s per exchange (a hung resolver does not eat the page's
budget); a resolution error — the request passes (NXDOMAIN of
advertising subresources is a page norm; a resolution error ≠ an
internal address), a found internal address — rejection (fail-closed
exactly for what the guard exists for). The guard's limits: the list
of ranges is fixed by netip predicates
(loopback/RFC1918/ULA/link-local/unspecified/multicast; CGNAT
100.64.0.0/10, NAT64 etc. are not included); the residual rebinding
vector — an attacker controlling the zone distinguishes the
interception's resolution from Chromium's own resolution (two separate
DNS exchanges) and answers them with different addresses — closed by
the network policy of the Chromium container (egress without access to
internal subnets) as a layered defense that the interception
complements but does not replace. A refusal is recorded as a
resolution/dial error; the redirect chain — its completed part. Off by
default (the code default): dev/testbed benches resolve scenarios into
internal networks (172.28.100.0/24); the deploy examples
(`config.example.yaml`,
`deploy/systemd/crawler.env.example` — `CRAWLER_HTTP_BLOCK_INTERNAL_ADDRS=1`)
carry the guard enabled: for Internet production operation enabling is
mandatory — the DB, Redis and the resolver live on the network next to
the crawler; disabling — only deliberately, for dev/testbed. The public
`/api/check-domain` form runs the same pipeline (internal/checker):
with the flag on, internal addresses are unreachable for it too; the
reflection of Evidence (the triggered rules) in the response remains —
the exfiltration channel is closed at the connection level, not the
reflection level.

The `task_*` fields — task-level parallelism of the http-worker loop
(T-139), not the transport: q:http consumption is sliding; the next
stream read happens when a task slot frees up (reading runs ahead of
processing by at most a couple of `task_consume` batches — the current
one in the reader's hands + a buffer), and the loop does not wait for a
whole batch to finish — a window barrier at the tail of durations
(tarpits up to `total_timeout`, 20–25 s) held the stage at ≈2 dom/s
with the transport idle (the T-092 measurement,
docs/load-measurements.md §3). The stage's ceiling ≈
`task_concurrency` / the average task duration. The calculation at the
T-092 measurement (1.67 s → ≈150 dom/s) was not confirmed by the
re-measurement after the deploy (§11): the actual mix is longer (mean
≈5.3 s — retries, tarpits, `per_ip_limit` on parking IPs) — 256 slots
delivered **47.6 dom/s** (×25 versus the barrier); the §3.3 goal of
"≥100 dom/s" requires either more slots or shortening time-in-flight
(the `total_timeout` tail, per-IP). 256 × `task_per_domain` 8 = 2048 ≈
the transport `concurrency` of 2000 — the task window fills the
connection budget without exceeding it. The stage's memory budget =
`task_concurrency` × the peak memory of a task: the §11 measurement
(≈3.34 MiB per slot; 256 → anon ≈855 MiB with the unit's MemoryMax
900M, T-143) referred to the code before T-147's early Put — back then
a task held the bodies of the original records until acknowledgment
(each ≤ `max_body_bytes`); now bodies are not retained (Put by hash
immediately), leaving in the slot the metadata and ≤
`task_per_domain` transit buffers of bodies being read. The default
was lowered 256 → 192 (T-157): a day on 256 slots — two unit fatals
(the §12 measurement: 05:48 killed by OOM killer, 09:17 hitting the
900.5 MiB peak = MemoryMax; MemoryHigh=850M tripped 120,095 times —
reclaim storms and swap slow the worker; at 256 slots the cycle would
repeat); 192 slots are stable (the wave 21 soak: 860–893 MiB, oom=0,
NRestarts=0); throughput drops ≈22 → ≈17–19 dom/s — the 50 dom/s goal
is postponed; stability matters more. T-163 closed the remainder of
the memory profile: slot measurement instruments on :9104
(`crawler_http_tasks_inflight`, `crawler_http_body_bytes_inflight`,
`crawler_http_body_bytes`) and the soft heap cap `mem_limit` — the
return to 256 by a sampler measurement after the deploy (criteria:
≤1.5 MiB/slot, a day without MemoryHigh throttling). `task_consume` is
also the threshold of the loop's circuit breaker: that many consecutive
deliveries without a single acknowledgment pause reading with a
doubling pause (a systematic processing failure — delivery attempts
burn down into q:dead, T-025; smaller values make the breaker more
sensitive — a couple of failures already triggers a pause; change
deliberately). Raise `task_concurrency` with the http-worker unit's
memory in mind (MemoryMax 900M — T-143; the return to 256 — after
T-163, by measurement) — a re-measurement with the T-092 kit after the
deploy confirms the actual throughput and memory.

`dead_host_cutoff` — dead-host cutoff (T-164): if apex and www both
fail the connect phase (connection refused or a connect timeout —
connect timeouts specifically, not TLS/headers/total) over both
protocols, the q:http task ends without checking the remaining
subdomains: dead hosts of mass zones (the com/xyz mix) stop burning
slots with their timeout tail (the §12 measurement in
docs/load-measurements.md: mean 11.5 s against a transport of
0.54 s/request, 60% of tasks in the 10–30 s bucket). The signature is
strict: both main names × both protocols × the connect phase only (a
redirect host failing does not count — the completed part of the chain
is non-empty); any live answers from the head clear the cutoff.
Records of skipped subdomains follow §8 ("no answer": no records); the
domain's status is decided by the completed apex/www checks (refused →
no_http; connect timeouts → temp_error with the T-031 recheck — a
transient false cutoff self-heals). The default is `true`: a strict
signature and a cheap outcome of a false trip; the share of false
cutoffs (< 1%, the T-164 criterion) is measured by a control sample —
a repeated full check of cutoff domains; the sample is selected by the
`dead_host_cutoff` bit in `domains.flags` and the
`crawler_http_dead_host_cutoff_*` metrics (:9104; counters of tasks
and saved checks). `false` — all subdomains of the task are checked as
before (one pool wave, no barrier on apex/www).

`mem_limit` — a soft memory cap for the http-worker's Go runtime
(`debug.SetMemoryLimit`, T-163): approaching the cap, the GC collects
more often on its own and returns pages to the OS, never letting anon
reach the unit's MemoryHigh/MemoryMax — kernel reclaim storms
(120,095 events and two fatals in a day, §12, T-157) cost more than
extra GC cycles. The page cache of pack records is not part of the
runtime accounting — keep the value below the unit's MemoryHigh with
headroom for it (the default 768 MiB against the unit's 850M/900M in
`crawler-http-worker.service`). A live heap above the cap does not
crash the process: the GC is limited to 50% CPU (slowing down, not
dying) — a slow worker is better than a fatal and the burn-down of
delivery attempts. `0` — disabled (the Go default: no cap). The
T-163 criterion measured with the sampler: the unit's anon /
`crawler_http_tasks_inflight` ≤ 1.5 MiB; transit buffer peaks —
`crawler_http_body_bytes_inflight`; the mix — the
`crawler_http_body_bytes` histogram.

### `render` (spec 5.6)

| YAML | Variable | Type | Default |
|---|---|---|---|
| `engine` | `CRAWLER_RENDER_ENGINE` | `chromium`\|`moli`\|`lightpanda` | `chromium` (the coupling with `remote_url`/`restart_memory_bytes` — below) |
| `remote_url` | `CRAWLER_RENDER_REMOTE_URL` | URL | `""` |
| `contexts` | `CRAWLER_RENDER_CONTEXTS` | integer | `8` (T-169; the spec's recommendation of 2–4 exceeded by measurement — below; the Chromium budget — [`services.md`](services.md), the render stage) |
| `timeout` | `CRAWLER_RENDER_TIMEOUT` | duration | `25s` |
| `timeout_attempts` | `CRAWLER_RENDER_TIMEOUT_ATTEMPTS` | integer | `2` (range: 1..100; must be ≤ `redis.max_attempts`) |
| `restart_every_pages` | `CRAWLER_RENDER_RESTART_EVERY_PAGES` | integer | `500` |
| `restart_memory_bytes` | `CRAWLER_RENDER_RESTART_MEMORY_BYTES` | size | `1536MiB` (range: 256MiB–16GiB, `0` — disabled) |
| `block_stylesheet` | `CRAWLER_RENDER_BLOCK_STYLESHEET` | bool | `false` (T-213; enable only after an A/B, the computed styles risk — below) |
| `block_tracker_domains` | `CRAWLER_RENDER_BLOCK_TRACKER_DOMAINS` | bool | `true` (T-214; the domain list and the selection principle — below and in [`render.md`](render.md)) |
| `queue_high_watermark` | `CRAWLER_RENDER_QUEUE_HIGH_WATERMARK` | integer | `10000` (0 — the shared `scheduler.queue_high_watermark`) |
| `late_select_timeout` | `CRAWLER_RENDER_LATE_SELECT_TIMEOUT` | duration | `300s`, range 1m..24h, `0` — the code default (T-274) |
| `requeue_deprioritized_zones` | `CRAWLER_RENDER_REQUEUE_DEPRIORITIZED_ZONES` | list of zone names | `[]` (replaced in full; the names in lowercase punycode, as in the zones reference table) |
| `fallback.enabled` | `CRAWLER_RENDER_FALLBACK_ENABLED` | bool | `false` (enabling the fallback queue — below) |
| `fallback.worker_rate` | `CRAWLER_RENDER_FALLBACK_WORKER_RATE` | integer | `100` (entries per a 1 min window; range 1..1000000) |
| `fallback.share_percent` | `CRAWLER_RENDER_FALLBACK_SHARE_PERCENT` | number | `30` (% per window of 1000 stage deliveries; range (0..100], `0` — no cap) |
| `fallback.health_gate_streak` | `CRAWLER_RENDER_FALLBACK_HEALTH_GATE_STREAK` | integer | `3` (range 1..100) |

`engine` — the render engine (the stage 35 spec, FR-1): `chromium`
(the default) reproduces today's behavior pixel-for-pixel — its own
headless-shell of the production setup or the dev stack's external CDP
(`remote_url`); `moli`/`lightpanda` — external CDP servers: the
infrastructure brings up the engine container, the worker only
connects. The "engine × `remote_url` × `restart_memory_bytes`"
combination (FR-2/FR-3) is checked by validation at configuration
build time: for `moli`/`lightpanda` `remote_url` is required (an
external CDP server `ws://host:9222`) and `restart_memory_bytes` must
be `0` — another process's RSS is not accessible (just as memory
control already does not work today in the dev stack's remote mode);
`chromium` allows both modes — the memory threshold does not work with
an external Chromium but is not forbidden. The `internal/render` core
does not distinguish engines: engine differences are allocator wiring,
deployment and documentation. The value's case and edge whitespace are
normalized to the canonical form (`Moli` → `moli`).

`remote_url` — a dev-environment extension: the address of an external
Chromium over CDP (`ws://host:9222`); in production mode the worker
starts the browser itself and the field is empty. The User-Agent of
rendered pages is the single `http.user_agent` (spec §9); the stage
architecture (isolated contexts, the pool, errors) — in
[`render.md`](render.md).

`contexts` — concurrent Chromium pages (pool slots, spec 5.6). The
default of 8 (T-169) is a deliberate exceedance of the spec's 2–4
recommendation, by measurement: demand at a free gate ≈1.2 pages/s
([`load-measurements.md`](load-measurements.md) §12); the capacity of
4 contexts ≈0.8 pages/s (5.1 s/page, T-092 §4) — q:render drainage
matched the inflow (29–34 tasks/min) and the lag of ~99K at the 100K
watermark did not melt; 8 ≈ 1.6 pages/s covers the demand (the
precedent of raising 3→4 is T-142). Chromium memory grows sublinearly
(the base process is shared): at 4 contexts the RSS is ~550 MB (§12) —
the `restart_memory_bytes` threshold of 1.5 GiB and the cgroup ≈2 GB
(spec 3.2) hold; after the deploy — a re-measurement (RSS below the
threshold, `memory.events oom=0` for the chromium unit). With RSS
systematically living at the threshold, one Chromium is saturated —
the answer is a second Chromium instance (:9223) and a second
render-worker against it ([`deploy.md`](deploy.md) §2.2), not a higher
threshold.

`timeout` (step 2 of T-169 — a decision criterion, **not** a default):
lowering 25 s → 15–20 s is allowed only based on the render duration
histogram (`crawler_stage_seconds{stage="render"}` on the
render-worker `/metrics`, port 9105; buckets 10/15/20/25/30 s) — the
share of successful renders in the cut-off tail must not exceed
~20%: the 10–25 s bucket is the most populous; DOM coverage must not
be cut. The gain from lowering — tarpit finalization (T-148) occupies
slots for 2×`timeout` instead of 2×25 s. Interaction of the steps:
first `contexts` 8 (step 1) and a re-measurement after the render
CPUWeight (T-158) — capacity versus priority; timeout — only if the
"drainage = inflow" balance persists.

`timeout_attempts` — the render timeout policy (T-148): how many
deliveries of a `q:render` task may end in a page timeout (`timeout`)
before the record is **finalized without a render**. A heavy SPA's
timeout is a predictable page property, not a random error: burning
the shared `redis.max_attempts` limit on it (3 deliveries → `q:dead`)
means losing work (the 2026-10-23 incident: 5507 dead records of
`q:render` within hours, all of them exhaustion of attempts by
timeout). The policy:

- tarpit while the number of **tarpit-classified deliveries** of the
  task is below `timeout_attempts` — an ordinary retryable error: no
  XACK; a claim by `XAUTOCLAIM` after `redis.claim_min_idle` (10 min);
  a new attempt (a first timeout may be the result of a jam — a retry
  under changed conditions gives the page a chance);
- tarpit when the number of tarpit-classified deliveries has reached
  `timeout_attempts` — the record is finalized without a render: the
  outcome remains the **raw HTML of the HTTP phase** (`rendered =
  false` — the "no render happened" marker per the `subdomain_results`
  schema; the "render impossible" stamp `render_failed_at` of T-210
  excludes the record from re-publication by the rework pass until the
  content changes); the record is published to `q:analyze` (the
  pipeline is completed by raw-HTML analysis) and the delivery is
  acknowledged — the record does not end up in `q:dead`;
- a permanent host certificate error (`ERR_CERT_AUTHORITY_INVALID` /
  `ERR_CERT_COMMON_NAME_INVALID`, and since T-172 also
  `ERR_CERT_DATE_INVALID`) is finalized the same way but from the very
  first delivery (T-159/T-172, [`render.md`](render.md), "Queue
  Cycle");
- a host DNS failure (`ERR_NAME_NOT_RESOLVED`, T-211) is finalized the
  same way, from the first delivery: the record reached `q:render`
  through a live HTTP episode (the HTTP phase resolved the name
  itself), and the browser resolves via the same system unbound — the
  failure means the host's DNS died between the episode and the
  render, and DNS recovery (days-scale) is not covered by the retry
  window (the T-207 measurement: 252 finalizations in ~7 h, each
  eating all 3 deliveries and ~20 min of PEL); `ERR_CONNECTION_REFUSED`
  is not in the class — recovery is observed (the T-172 measurement:
  6.6% of retries succeed), so it stays retryable until the last
  delivery (the next item);
- a task-class render error (connection refusal, load error — a host
  property) on the delivery numbered `redis.max_attempts` is finalized
  without a render by the same mechanics (T-172): early attempts stay
  retryable (host recovery is observed — measurement: 6.6% of
  `ERR_CONNECTION_REFUSED` retries succeed); a host that exhausted its
  attempts gets honest raw HTML; there is no separate setting — the
  boundary is the same as the Reliab graveyard's
  (`redis.max_attempts`);
- everything else (browser, DB, storage, publication) — retryable
  errors with the former path: `redis.max_attempts` deliveries →
  `q:dead` (T-025); `q:dead` is left only to genuine infrastructure
  errors;

Tarpit counting — an application-level Redis set
`render:tarpit:{task_id}` (T-261, TZ-fsm spec §3 FR-7): each delivery
ending in a timeout adds a member "entry-id#delivery-number" (SADD is
idempotent — a replay of the number after the T-222 attempt return
does not grow the counter); the TTL of 48 h is refreshed by each mark;
the decision is SCARD ≥ `timeout_attempts`. Before T-261 the PEL
delivery number served as the threshold, and mixed series (an infra
failure between tarpits) were finalized "by attempt" earlier: all
deliveries grow the delivery number, the set only tarpits.
Degradations: loss of Redis (command errors) or the counter not
mounted — the decision is made by the PEL delivery number (the
pre-T-261 behavior); loss of the set itself with Redis alive (key
eviction, a restart without persistence) — the count starts over and
finalization is postponed until the new threshold; neither finalizes
before the threshold. The shared `redis.max_attempts` limit keeps
working by the PEL delivery number (the first delivery 1, each claim
+1) as before. The share of tarpit finalizations is visible in the
`crawler_render_tarpit_finalized_total` metric (render-worker,
/metrics); analysis of the accumulated dead records —
[`runbooks.md`](runbooks.md) §2.5. A value above `redis.max_attempts`
is meaningless (the record would die in `q:dead` before finalization)
and is rejected by validation.

`restart_every_pages` and `restart_memory_bytes` — browser restart
thresholds (spec 5.6 "restart the browser every N pages and upon
exceeding the memory threshold", T-065). The memory threshold is the
total RSS of the Chromium processes in production mode; the default
1.5 GiB with a budget of ≈2 GB (spec 3.2, `mem_limit`/cgroup
`MemoryMax`): the restart trips before cgroup-OOM. At 8 contexts the
threshold is not raised (T-169): the check runs once per task (an RSS
snapshot no older than the 10 s cache window, T-229) — pages in flight
keep growing the RSS after the last check; the 512 MiB headroom
(2048 − 1536) covers that growth, narrowing it would bring the soft
restart closer to the hard cgroup OOM; RSS systematically living above
the threshold means one saturated Chromium — the answer is a second
Chromium (:9223) and a second render-worker against it
([`deploy.md`](deploy.md) §2.2), not a raised threshold. In remote
mode (the dev stack) another container's RSS is not accessible —
memory-based control does not work, and the Chromium container's limit
holds the cap.

`block_stylesheet` — CSS blocking in the render context (T-213): at
`true` the list of blocked render types ([`render.md`](render.md),
"Blocking … T-062": Image/Media/Font — 91% of page weight) is extended
with `Stylesheet` — the remaining 5–10% of bytes plus a noticeable
share of style/layout CPU; per the 31.10 research, blocking
CSS+images gives another third off the load time. The default `false`
is a deliberate decision: on rare sites JS reads computed styles and
media queries (`getComputedStyle`, `matchMedia`) and changes the final
DOM based on unapplied styles — Laravel detection verdicts could
differ. Therefore enabling happens only after an A/B in production: a
diff of Laravel signatures before/after on an identical sample
(criterion: the share of detection disagreements < 1%) and a
throughput check (no worse than current); the mechanics and risk
boundaries — [`render.md`](render.md), the blocking section.

`block_tracker_domains` — blocking requests to web-analytics and
telemetry domains in the render context (T-214): Google Analytics,
GTM, DoubleClick, Yandex Metrica, Hotjar, Clarity, Mixpanel etc. — the
full list (15 domains) and the selection principle are in
[`render.md`](render.md), the blocking section (only specialized
analytics/telemetry, not general-purpose CDNs). Analytics arrives via
Script/XHR requests — it is not caught by types (T-062/T-213), while
it cuts traffic (5–15 requests, 100–500 KB per page per source
measurements) and the CPU of tracker timers that prolong the network
"quiet"; therefore `Fetch.enable` receives urlPattern patterns per
domain, and the verdict on each suspended request is made by an exact
host check — the site's own scripts and paths with a similar substring
(e.g. `/js/google-analytics.com.js`) are untouched. The default `true`
— a difference from `block_stylesheet` (T-213): trackers do not
participate in Laravel detection (the rules look for framework
signatures in the site's own content and scripts); the residual risk
is exotic setups collecting visible content via tag-manager tags; the
benefit is direct, no A/B is required, and the post-enablement check
is the usual stage metrics (render rate and success no worse than
current). `false` — trackers load as before.

`queue_high_watermark` — the render queue capacity threshold (T-174;
the `q:render` publication gate — T-066, the HTTP stage's
`renderq.go`, [`services.md`](services.md)): stream depth (the
consumption group's backlog: `min(lag, XLEN)` + `pending`, the T-168
clamp) ≥ the threshold — SPA result records are not published; the
domain gets the `render_pending` flag in the same transaction and is
reworked by a later pass (tasks are not lost); the
`RequeueRenderPending` pass publishes only when the queue has room —
closed gates also stop the re-flagging: consumption drains the stream,
then the flags at the same rate. A separate threshold, not the shared
`scheduler.queue_high_watermark`: with the T-168 clamp the shared
threshold of 100K kept the gates open at a lag of 98.4K out of 100K —
inflow equaled consumption (≈5.1K tasks/h, the 24.10 equilibrium —
stage 25 of `tasks/tasks.md`), and debt drainage was zero. The default
of 10K ≈ 2 h of consumption buffer (the 24.10 measurement): it closes
the gates on the current debt (a lag of 98.4K + 344.5K `render_pending`
flags) and restores the "drain — then admit" hysteresis — full
drainage ≈ 3.6 days versus "never". The effective value includes the
clamp `min(watermark, MAXLEN/2)` (`queue.StreamWatermark`, the same
unified calculation as the T-026 publication gate: the gates must
close before MAXLEN trimming — trimming removes entries regardless of
whether they were read); at the default the clamp is inactive:
10K < 75K — half of the `q:render` limit (150K, T-025). `0` — the
shared `scheduler.queue_high_watermark` (with the same clamp; the
semantics of `content.retention_analyze_watermark`). After the deploy
— operational control (the T-174 criterion): the `q:render` lag
decreases monotonically by ≥ 1.5K/h, new SPA domains get flagged and
are not lost, PSI some < 95%, and the HTTP speed does not drop.

`requeue_deprioritized_zones` — zone-based priority for render-debt
drainage (T-184, decision (a) of the T-176 measurement — the operator
on 24.10: render everything, lose nothing; zones with a high DOM
contribution go first, parking `.рф` gets the leftovers): zone names
are TLDs (punycode, as in the `zones` reference table; the IDN zone
`.рф` is `xn--p1ai`) whose debt (`render_pending`) the
`RequeueRenderPending` rework pass serves last. The mechanism is the
batch selection order: two steps without sorting — first the domains
of zones NOT in the list; upon exhaustion (the batch incomplete — the
LIMIT did not trip, the seq scan reached the end of the table) the
remainder is filled from the list's zones; both steps run under the
same T-181 plan pins (the seq scan with early stop is preserved) and
the debt queue loses no records. The `q:render` gates (T-174) meter
the publications — the priority disposes of the freed capacity;
publication of live-check SPA results and the undelivered stream tail
are not prioritized (FIFO as is). The loop does not repeat the empty
pass of an exhausted first step while the debt counter matches the
expectation (the evidenced value minus its own withdrawals);
resumption happens on any mismatch (a counter increase — a new flag
set by any zone, not only a prioritized one; with a continuous inflow
of list zones the repeats occur almost every tick — check the
slow-query log after the deploy; the mechanism and the limits —
[`render.md`](render.md), "Zone Priority of the Rework Pass"). An
empty list (the default) — the previous order without a zone filter
(T-181). A name may be missing from the reference table (ingest
creates the zone with its first file) — not an error: the filter
matches by name in the selection itself and takes effect once the zone
appears. Observability — the `deprioritized` field of the "render
rework pass" line of the http-worker log (batch domains from list
zones: `0` during the drainage window means publications go with
prioritized zones first — order control without manual SQL). The env
form is a comma-separated list:
`CRAWLER_RENDER_REQUEUE_DEPRIORITIZED_ZONES=xn--p1ai,net` (replaced in
full, the names are lowercased). The pass mechanics and the step
memory — [`render.md`](render.md), "Queue Cycle".

`late_select_timeout` — the cap on a single batch selection of the
render late re-publisher (T-274; the cap itself — T-271/T-273): the
server-side `statement_timeout` of the selection tx wrapper and the
client ctx deadline. A timeout smaller than the cost of the first
catch-up pass from a zero cursor turns slow drainage into an endless
loop of doomed retries (the T-271 regression, the 09–10.11.2026
incident). The default of 300 s: the re-publisher's selection is
adaptive (the unpinned plan form) and fast; the knob exists for the
case of underestimation — changing the cap without a deploy.

`fallback` — the Chromium fallback queue subsection (the stage 35
spec, FR-6…FR-14): tasks that did not get rendered on the primary
engine for an engine-dependent reason (a tarpit `timeout_attempts` and
the task class on the last delivery `redis.max_attempts`) are published
to the separate stream `q:render-fallback` and reworked by the second
render worker (worker B) on the chromium engine. Engine-independent
reasons — DNS failure and certificate errors — get no fallback:
Chromium will not fix them. `enabled: false` (the default) — the
primary worker finalizes without a render as today, and the subsection
affects nothing; enabling and disabling are a flag edit without
deploying new entities, and whatever accumulated in the queue is
reworked by worker B until empty. At `enabled: true` the primary
worker (T-238, [`render.md`](render.md) "Queue Cycle" → "Fallback
Publication") publishes candidates to `q:render-fallback` instead of
the `render_failed_at` marker: the record waits for rework without the
marker, and worker B sets the marker after the fallback is exhausted
(a repeated delivery does not duplicate the stream — a filter by
record id); the publication decision is subject to the health gate and
the share budget (T-239 — both caps below; a refusal means today-style
finalization with the marker), and worker B's rate (T-240) hooks into
the consumption window. Observability —
`crawler_render_fallback_published_total{reason}` and
`crawler_render_fallback_suspended_total{reason=gate|budget}`. The
caps:

- `worker_rate` — worker B's rate: entries per a 1 min sliding
  consumption window (`renderworker.RateWindow`, T-240): the batch
  mode of "collect and rework" without scaling up chromium capacity.
  The default 100 ≈ the full capacity of one chromium (8 contexts ×
  ~5 s/page ≈ 1.6 pages/s) — a starting value, calibrated by a bench
  run of stage 35. The cap bounds consumption (XREADGROUP; claiming —
  `redis.recover_*`, the recovery rate is a separate knob) and applies
  at worker B even with the fallback disabled — the accumulated work
  is reworked until empty (FR-14). The value is read by the B build
  specifically: in systemd — the shared `/etc/crawler/config.yaml`
  (deploy.md §2.3); in docker — worker B's profile
  `deploy/docker/config.render-fallback.yaml` (the key in the main
  build's config has no effect — that one does not consume
  `q:render-fallback`);
- `share_percent` — the fallback share budget: a share of fallback
  publications per window (1000 acknowledged stage deliveries, T-239)
  above the cap suspends the fallback until the end of the next window
  — tasks finalize as usual and an alert fires. The default 30 is the
  economics threshold (a fallback rate above ~30% nullifies the
  feature's benefit): the budget is a storm breaker, not a canary
  criterion. `0` — no cap;
- `health_gate_streak` — the health gate threshold: that many
  consecutive infra errors "the primary engine is unavailable" (the
  browser class; the streak counts errors since the last successful
  render — success resets it, task/context class errors do not touch
  it) suspend fallback publication until the engine recovers —
  spilling the whole flow onto the expensive chromium when the light
  one falls is excluded. A single infra error is live-flow noise and
  the threshold of 3 does not trip; a down engine produces a streak on
  every delivery — the gate closes within seconds.

The subsection's ranges are validated even at `enabled: false`:
enabling the fallback by editing a single flag must not blow up the
startup with invalid neighbors.

### `content` (spec 5.7)

| YAML | Variable | Type | Default |
|---|---|---|---|
| `dir` | `CRAWLER_CONTENT_DIR` | path | `/data/content` |
| `pack_size` | `CRAWLER_CONTENT_PACK_SIZE` | size | `512MB` (recommendation: 512 MB–1 GB) |
| `max_store_bytes` | `CRAWLER_CONTENT_MAX_STORE_BYTES` | size | `2097152` (recommendation: 1–2 MB) |
| `zstd_dictionary` | `CRAWLER_CONTENT_ZSTD_DICTIONARY` | path | `/data/content/dict.zst` (empty — no dictionary) |
| `disk_free_percent` | `CRAWLER_CONTENT_DISK_FREE_PERCENT` | number | `5`, range (0, 95] |
| `retention` | `CRAWLER_CONTENT_RETENTION` | duration | `6h`, range 1m..30d, `0` — disabled |
| `retention_interval` | `CRAWLER_CONTENT_RETENTION_INTERVAL` | duration | `1h`, range 1m..24h |
| `retention_batch` | `CRAWLER_CONTENT_RETENTION_BATCH` | number | `10000`, range 1000..100000 (the working range 10–50K) |
| `retention_analyze_watermark` | `CRAWLER_CONTENT_RETENTION_ANALYZE_WATERMARK` | number | `0` (the shared `scheduler.queue_high_watermark` threshold), an explicit value 1..10⁸ |
| `retention_select_timeout` | `CRAWLER_CONTENT_RETENTION_SELECT_TIMEOUT` | duration | `2h`, range 1m..24h, `0` — the code default (T-274) |

The dictionary is trained with the command
`crawlerctl dict train <file> [--samples PATH]…` (the dev environment —
`docs/dev-environment.md` §6); a missing file is not an error, content
is compressed without a dictionary (a log entry). On retraining, the
former dictionary is archived into the registry of former dictionaries
— the `dict-<id>.zstd` files in the active file's directory (`dir` by
default): reading a block with a dictID unknown to the process lazily
registers the dictionary from the registry, so retraining does not
make already-written content unreadable (T-103; the mechanics and the
lifetime — `docs/content.md` "The Dictionary and the Registry of
Former Dictionaries", the procedure — `docs/runbooks.md` §1). The pack
header's `dictID` is preserved for diagnostics: a mismatch between the
active dictionary and the newest pack not covered by the registry is
reported as a Warn at startup by the services and
`crawlerctl domain --content` (`WarnDictMismatch`; a dictionary from
the registry — Info).

`dir` — the directory of the content store's pack files
(`pack-NNNNNN.pack`, the lock file `.packs.lock`, the dictionary): the
format, rotation by `pack_size`, the `contents` metadata and the
limits (including the reaction to disk overflow via
`disk_free_percent`, spec 8) — [`docs/content.md`](content.md).

`retention` and the loop parameters — the lifetime window of non-find
content (T-166; the operator's decision of 2026-10-23: the disk
without new capacity): `subdomain_results` records older than the
window that are not Laravel finds and not waiting for render lose
`content_hash` (with a `contents.refcount − 1` decrement in the same
transaction, zero-refcount rows deleted) and the heavy jsonb; the
record's identity always remains — `domain_info` and the dashboard
stay alive. The steady-state content volume = the daily inflow × the
window (at 20 GiB/day: 6 h ≈ 5 GiB; 48 h would eat the whole volume).
The `retention_analyze_watermark` interlock: q:analyze depth at the
threshold — the analyzer cannot keep up, bodies and headers/cookies
are still needed, the loop pass is skipped; 0 — the shared
`scheduler.queue_high_watermark` threshold (the q:analyze publication
gate works by the same "strictly below the threshold" criterion).
`retention_select_timeout` (T-274) — the cap on a single batch
selection (the server-side statement_timeout of the tx wrapper and the
client ctx deadline): the default of 2 h is double the headroom over
the production measurements of 10–11.11.2026 of the first catch-up
pass from a zero cursor under load; the knob for the case of
underestimation — changing the cap without a deploy. The drainage
cursor is persistent (`retention_drain_cursor`, migration 000020): an
http-worker restart continues the drainage from the saved position.
The mechanics, the selection criteria, the recorded deviations
(re-analysis of non-finds — at recheck) and the metrics —
[`docs/content.md`](content.md) "Content retention and the non-find
trim (T-166)". The http-worker runs the loop; `0` — retention
disabled, the loop does not start.

### `analyzer` (spec 5.8)

| YAML | Variable | Type | Default |
|---|---|---|---|
| `rules_dir` | `CRAWLER_ANALYZER_RULES_DIR` | path | `/etc/crawler/rules` |
| `laravel_min_score` | `CRAWLER_ANALYZER_LARAVEL_MIN_SCORE` | number | `0.6`, range (0, 1] |

`rules_dir` — the directory of the Laravel detection rule YAML files:
all `*.yaml`/`*.yml` are read in name order. The directory is mounted
from the host; editing files + `make rules-reload` (SIGHUP to the
analyzer) — a reload without a restart. A reload error (no directory,
broken YAML, an invalid set) does not stop the service — the previous
set keeps working (a Warn in the log); the next attempt — the next
SIGHUP.

The rule format (`internal/rules`; the reference — spec 5.8 of the
main spec):

```yaml
rules:
  - id: cookie_xsrf_token        # unique within the directory
    description: The XSRF-TOKEN cookie # human-readable; not part of rules_version
    target: cookie_name          # cookie_name | cookie_value | header_name |
                                 # header_value | html | meta
    match: "XSRF-TOKEN"          # exact match — for names only
                                 # (cookie_name / header_name); or a regex:
    # regex: '^eyJpdiI6'
    weight: 0.25                 # (0, 1]
    ver_min: "9.19"              # optional: the implied range
    ver_max: "9.x"               # of Laravel versions ("x" = through the end of the branch)
```

Validation at load — the set is applied in full or not applied at all:
unique `id`, a known `target`, exactly one of `match`/`regex` set (for
`cookie_value`/`header_value`/`html`/`meta` — only `regex`: an exact
match makes sense for names only), a compilable `regex`, `weight` in
(0, 1], `ver_min` ≤ `ver_max` (incorrect bounds — a load error).
Typos in field names are an error (strict parsing, like the
configuration). An empty set is an error.

`rules_version` — the set's hash: FNV-32a of the canonical
representation (rules sorted by `id`), a signed value for the
`integer` column of the `laravel_findings`/`laravel_domains` tables
(more in [`schema.md`](schema.md)). Identical sets get an identical
hash regardless of file splitting and order; editing
`description`/comments does not change the version (the detection
behavior is the same — no re-analysis needed). Checking a set against
a sample corpus without the network: `make test-rules`.

#### Score and Version-Range Calculation

The calculation over the page's triggered rules —
`internal/rules/score.go` (`rules.ScoreRules`, spec 5.8):

- **Score** — "noisy-OR": `1 − Π(1 − wᵢ)` over the weights of the
  triggered rules. Weak signals accumulate but never reach 100% (a
  weight of 1 on a single rule is the only exception).
- **Version range** — the intersection of the triggered rules'
  `[ver_min, ver_max]`: the lower bound is the maximum of the
  `ver_min` values, the upper — the minimum of the `ver_max`. A rule
  without a bound does not narrow the range; the bound strings are
  kept from the rules as is (`9.x` does not become `9`). Versions are
  compared as numeric components (a missing component = 0; `x`/`*` =
  +∞ through the end of the major branch).
- **Conflict** — the intersection is empty (`ver_max < ver_min`): the
  range is considered undefined, both bounds are reset (when written
  to the DB — `ver_min`/`ver_max` NULL in
  `laravel_findings`/`laravel_domains`); the fact is visible from the
  empty bounds and the conflict marker in the analysis result.
- **`is_laravel`** — a confident trigger:
  `score ≥ laravel_min_score` (`rules.Result.IsLaravel`, default 0.6).
  The threshold gates writing the domain into `laravel_domains`;
  **all** triggers go into `laravel_findings` regardless of the
  threshold (spec 5.8, §13.1).

### `subdomains` (spec 5.10)

| YAML | Variable | Type | Default |
|---|---|---|---|
| list | `CRAWLER_SUBDOMAINS` | list of labels | `""` (the apex), `www`, `admin`, `crm`, `api`, `app` |

The initial content of the `subdomains` table; runtime management —
`crawlerctl subdomains add|disable` (T-083). No more than 63 entries
(the `dns_mask` bit mask). Case is lowercased; each label is a DNS
label without dots (`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`); duplicates
are forbidden.

### `monitor` (spec 5.9)

| YAML | Variable | Type | Default |
|---|---|---|---|
| `listen` | `CRAWLER_MONITOR_LISTEN` | [host]:port | `:9100` |
| `zone_stats_interval` | `CRAWLER_MONITOR_ZONE_STATS_INTERVAL` | duration | `30m` (allowed range 30s..1h; T-161 → T-199) |
| `alerts_interval` | `CRAWLER_MONITOR_ALERTS_INTERVAL` | duration | `5m` (allowed range 30s..1h; T-206) |
| `load_report_interval` | `CRAWLER_MONITOR_LOAD_REPORT_INTERVAL` | duration | `10m` (0 — disabled; allowed range 1m..1h; T-173) |
| `slow_query_interval` | `CRAWLER_MONITOR_SLOW_QUERY_INTERVAL` | duration | `10s` (0 — disabled; allowed range 5s..10m; T-177) |
| `slow_query_min_duration` | `CRAWLER_MONITOR_SLOW_QUERY_MIN_DURATION` | duration | `3s` (0 — the default; allowed range 1s..1h; T-177) |
| `slow_query_telegram` | `CRAWLER_MONITOR_SLOW_QUERY_TELEGRAM` | bool | `false` (T-177) |
| `render_debt_interval` | `CRAWLER_MONITOR_RENDER_DEBT_INTERVAL` | duration | `5m` (allowed range 30s..1h; T-175) |
| `ingest_metrics_url` | `CRAWLER_MONITOR_INGEST_METRICS_URL` | http(s) URL | `http://127.0.0.1:9101/metrics` |
| `render_metrics_url` | `CRAWLER_MONITOR_RENDER_METRICS_URL` | http(s) URL | `http://127.0.0.1:9105/metrics` |
| `alerts.telegram` | `CRAWLER_MONITOR_ALERTS_TELEGRAM` | bool | `true` (T-162) |
| `alerts.telegram_script` | `CRAWLER_MONITOR_ALERTS_TELEGRAM_SCRIPT` | path | `/opt/crawler/bin/telegram-notify.sh` |
| `alerts.disk_free_min_bytes` | `CRAWLER_MONITOR_ALERTS_DISK_FREE_MIN_BYTES` | size | `15GiB` |
| `alerts.disk_free_crit_bytes` | `CRAWLER_MONITOR_ALERTS_DISK_FREE_CRIT_BYTES` | size | `10GiB` |
| `alerts.queue_freeze_window` | `CRAWLER_MONITOR_ALERTS_QUEUE_FREEZE_WINDOW` | duration | `15m` (allowed range 1m..6h) |
| `alerts.psi_cpu_percent` | `CRAWLER_MONITOR_ALERTS_PSI_CPU_PERCENT` | number 0–100 | `90` |
| `alerts.psi_cpu_window` | `CRAWLER_MONITOR_ALERTS_PSI_CPU_WINDOW` | duration | `30m` (allowed range 1m..6h) |
| `alerts.mem_high_passes` | `CRAWLER_MONITOR_ALERTS_MEM_HIGH_PASSES` | integer 1..10 | `2` |
| `alerts.render_debt_domains` | `CRAWLER_MONITOR_ALERTS_RENDER_DEBT_DOMAINS` | integer 1..100000000 | `50000` (T-175) |
| `alerts.render_debt_passes` | `CRAWLER_MONITOR_ALERTS_RENDER_DEBT_PASSES` | integer 1..10 | `2` (T-175) |

`listen` — the address of the monitor service's own HTTP endpoint
(`/livez` +
`/healthz` + `/metrics`, its own metrics): every service of
the project has its own endpoint (spec 5.9; the addresses in
[`services.md`](services.md)),
Prometheus scrapes them individually, there is no aggregation through
monitor.
`zone_stats_interval` — the period of the batched recount of the zone
progress counters
`zones.domains_total` / `domains_checked` (T-078; the semantics —
[`services.md`](services.md)) and the unit of the progress-stall alert
threshold
(T-080: `zones.updated_at` older than `AlertStallFactor × interval` —
the timestamp
is written by the reconciliation loop itself, the threshold counts its
missed passes, the alert frequency does not change it).
The default of 30 minutes (T-199): the original 60 s at the upper edge
of the spec 5.9 range
(30–60 s) target a small corpus — at 47M rows (measurement §12
[`load-measurements.md`](load-measurements.md)) the UPDATE aggregate is
among the
top three postgres CPU items; T-161 raised the default to 5 minutes;
the slow-query window measurement after the 3bffc62 deploy (26.10)
showed
reconciliation as the only remaining full `domains` pass — 48% of the
total slow DB time (100 events >3 s, median 39 s,
max 501 s) at a pass every 5 minutes — and the operator spaced the
interval out to
30 minutes per the TZ_incremental spec §7.1 stub
(`tasks/TZ_incremental_counters.md`:
"after the wave's break-in the interval can be spaced out to 15–30
min"; counter drift
in steady state must remain zero — that is how you see,
when the interval can be brought back shorter); the progress-stall
threshold
5 × interval (25 min → 2.5 h).
Allowed range 30s..1h; the spec 5.9 range's 30–60 s values remain
available
for small corpora. Passes do not stack (single-flight, T-161):
a pass longer than the interval drops the accumulated ticker tick, a
Warn and
the `crawler_zonestats_skipped_ticks_total` counter raise the noise.
The thresholds
of the first alerts stayed with their own fields: the queue threshold —
`scheduler.queue_high_watermark`, the disk-space threshold (percent) —
`content.disk_free_percent` (the same one as the content-write pause,
spec 8);
the T-162 alert thresholds — the `alerts` section below; the rest —
constants
`internal/monitor` with justification (the set and the numbers —
[`services.md`](services.md)).
`alerts_interval` — the period of the alert evaluation loop (T-206,
the operator's
decision of 26.10): the default of 5 minutes — the former frequency.
Before T-161 the alert
loop ran at the original 60 s; T-161 tied "one pair of loops" of
monitoring
to a single period (`zone_stats_interval`); T-199 spaced
reconciliation out to
30 minutes — detection windows defined in passes stretched 6×
(`queue_dead` 15 → 90 min, `memory_high`/`render_debt` 10 → 60 min,
the
`bad_hashes` window 75 min → 7.5 h), and the PSI sampling (a 30 min
window)
degenerated into two adjacent passes. The decoupling restored the
effective windows and
a PSI sampling error of ~5 min; the progress-stall threshold stayed in
reconciliation
periods (`zone_stats_interval` — see above). Allowed range
30s..1h, modeled on `zone_stats_interval`; values longer than
`alerts.psi_cpu_window` degenerate the PSI window into "avg300 above
the threshold on
two adjacent passes" (see the `psi_cpu` alert below).
`ingest_metrics_url` — the address of the ingest service's `/metrics`
for the junk-`content_hash` alert probe (T-099): the same address
Prometheus scrapes;
in the docker network — by service name
(`http://ingest:9101/metrics`,
config.dev.yaml), on the production host — ingest's loopback per the
unit's `METRICS_ADDR`.
An empty string — the alert is "not measured" (the probe does not
start);
the value must be an absolute http(s) address.
`render_metrics_url` — the address of the render worker's `/metrics`
for the fallback share budget alert probe
(T-246): the suspension event counter
`crawler_render_fallback_suspended_total{reason="budget"}` (T-239)
lives in the worker process, and the monitor reads it at the same
address as
Prometheus scrapes; in the docker network —
`http://render-worker:9105/metrics`
(config.dev.yaml), on the production host — the loopback per the
unit's `METRICS_ADDR`
(9105). An empty string — the alert is quietly "not measured" (the
probe does not
start); the value must be an absolute http(s) address.
`load_report_interval` — the period of the system-load report to the
operator
(T-173): a `/proc` snapshot (CPU utilization and PSI, load average,
RAM/swap and memory
PSI, top processes by CPU and RSS — aggregates by name, an
"htop"-style view),
the free space of the content directory (statfs) and the depth of the
queues `q:*` with
the consumption rate (the entries-read delta over the window). It is
delivered to
Telegram by the same delivery as the alerts (`alerts.telegram` — the
same secrets and
script), duplicated by an Info line into the monitor log and the gauge
`crawler_os_*` in `/metrics`. The first pass after startup is the
baseline:
utilization and rates are "n/a", sizes are complete. The default of
10 minutes (144
messages/day do not drown the alerts), `0` — disable; allowed range
1m..1h.
`slow_query_interval` / `slow_query_min_duration` — the PostgreSQL
slow-query loop
(T-177): once per period monitor polls
`pg_stat_activity`, and client statements running longer than the
threshold (`3 s` by default —
the production log threshold of `log_min_duration_statement`,
[db.md](db.md) §"Monitoring") go to Telegram
by the same delivery as the alerts and the load report: a header with
the number of new and
"total in flight", the three longest lines "duration [service]
pid: text" (truncated to 160 characters; the full text — the server
log).
Debounce — pid+query_start: each statement is reported once at
first observation; monitor's own passes (aggregates over 47M
rows, measurement §12) are excluded by the pool's `application_name`,
and the service labels in
the message come from there; reading other sessions' text requires the
DB role
`pg_read_all_stats` (without it — a "text unavailable" note). The
snapshot is
instantaneous: episodes shorter than the interval are caught
partially, longer ones — always
(the full post-mortem trace — the server log). Duplicated as a Warn
into the log and
by the metrics `crawler_pg_slow_queries` / `crawler_pg_slow_queries_total`
(the `app` label). `slow_query_interval: 0` — disable (allowed range
5s..10m),
`slow_query_min_duration` — allowed range 1s..1h, `0` — the loop's
default.
`slow_query_telegram` — delivering slow queries to Telegram:
by default `false` — the loop is limited to a Warn in the log and the
metrics
(a slow query is diagnostics already duplicated by the PostgreSQL
server log,
not an event requiring a "right now" reaction, like alerts);
`true` — as messages of the same delivery as the alerts
(`alerts.telegram` —
the same secrets and script).
`render_debt_interval` — the period of the render-debt loop (T-175;
the `render_debt` alert is evaluated by the alert pass on
`alerts_interval` (T-206),
this field only sets the freshness of the gauge and the summary): once
per period
monitor reads the counter of domains with the `render_pending` flag
(`crawler_counters`, migration 000015 — the value is maintained by bit
transitions, not a periodic recount: counting by the bit mask would be
a seq scan of 41.8M
rows) and the lag of the `q:render` stream, writes the gauge
`crawler_render_pending_domains` (the flags; the lag is already
`crawler_queue_depth{queue="q:render"}`) and a summary line into the
log
(Debug — no log flooding); the same figure goes as the "Render debt"
line in the load
report (T-173) and feeds the `render_debt` alert. The pass is a point
Lookup by PK and XLEN (milliseconds), and the period chooses the
gauge's freshness, not the
cost: debt is a slow quantity (the drainage of `q:render` ≈1–2 pages/s,
measurement §12), 5 minutes suffice (parity with zone recounting is
over —
T-199 spaced reconciliation out to 30 min); allowed range 30s..1h —
modeled on
`zone_stats_interval`.

`alerts` (T-162) — the missing spec 5.9 thresholds (measurement §12:
all P0s of that
period were found manually) and the delivery of alerts to the operator;
alert
semantics — [`services.md`](services.md); delivery setup —
[`operations.md`](operations.md) §4.3.

- `telegram` — delivering alert transitions (triggering 🚨 / clearing
  ✅,
  one episode = two messages, duplicates are debounced) to Telegram by
  the same
  bot and group as the CI/CD notifications (spec §10.4). Enabled by
  default: the decision is actually made by the host's secrets and
  script —
  with no `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` in the unit's
  EnvironmentFile
  (secrets are not in YAML) or no `telegram_script` script — delivery
  is silently
  off, and alerts remain Warns in the monitor log;
- `telegram_script` — the path to `telegram-notify.sh` on the server;
  the script rides
  in the deploy archive with the binaries (deploy.yml →
  `/opt/crawler/bin`,
  no separate installation needed — [`operations.md`](operations.md)
  §4.3);
- `disk_free_min_bytes` / `disk_free_crit_bytes` — the absolute minimum
  of free space in the content directory (the `disk_free_min` Warn
  alert /
  the `disk_free_crit` escalation): a percent threshold on a 79 GB
  volume fires late
  (10% = 8 GB ≈ 5 h until full at ≈36 GiB/day, measurement §12);
  crit is always less than min (validation);
- `queue_freeze_window` — the window of stillness of the stream
  group's last-delivered-id
  at depth > 0 (the `queue_frozen` alert, the poisoned-task signature
  of T-159);
- `psi_cpu_percent` + `psi_cpu_window` — the PSI CPU some avg300
  threshold and
  the persistence window (the `psi_cpu` alert; a kernel without PSI —
  "not measured").
  The evaluation runs in an alert-loop pass once per `alerts_interval`
  (5 min by default, T-206): the window's discretization error is
  ~5 min
  (on the 30-min loop of T-199 a window ≤ the period degenerated into
  "avg300 above
  the threshold on two adjacent passes" — only starvation of ≳60 min
  was guaranteed to be caught; details — [`services.md`](services.md),
  the `psi_cpu` alert);
- `mem_high_passes` — consecutive alert passes with growth of the
  unit's
  memory.events high counter (the `memory_high` alert; MemoryHigh
  throttling of the
  http-worker is a harbinger of the T-157 fatals; 2 × 5 min = 10 min
  of sustained pressure at the default alert period `alerts_interval`,
  T-206);
- `render_debt_domains` + `render_debt_passes` — the render-debt
  threshold
  (the `render_pending` flags + the `q:render` lag) and the series of
  growing passes
  before the `render_debt` alert fires (T-175): debt above the
  threshold AND
  growing — the SPA inflow outruns the drainage (the `queue_frozen`
  alert does not catch
  accumulation: deliveries keep going); 50K — below the freeze debt of
  24.10
  (443K), above the background of local overflows. A fired alert
  clears only by draining below 90% of the threshold (hysteresis:
  fluctuations of
  the lag at steady debt do not pump the episode); the series passes
  take the period
  of the alert loop (`alerts_interval`, 2 × 5 min = 10 min by default,
  T-206).

### `web` (stage 13, T-121)

| YAML | Variable | Type | Default |
|---|---|---|---|
| `listen` | `CRAWLER_WEB_LISTEN` | [host]:port or an absolute path | `127.0.0.1:8080` |
| `rate_limit` | `CRAWLER_WEB_RATE_LIMIT` | number | `4` |
| `timeout` | `CRAWLER_WEB_TIMEOUT` | duration | `60s` |
| `live_concurrency` | `CRAWLER_WEB_LIVE_CONCURRENCY` | number | `4` |
| `public_host` | `CRAWLER_WEB_PUBLIC_HOST` | string (host[:port]) | `""` |

`listen` — the landing page's public address: the system's public port
for
visitors (the operator dashboard is a separate `dashboard.listen`
listener
for the operator, the dashboard spec).
The form is [host]:port (TCP) or an absolute unix-socket path (a
deployment variant
behind nginx without occupying a TCP port, T-124; the path ≤100 bytes —
the cap of
`sockaddr_un`). The default is loopback: the service is published
outward by a reverse proxy
with TLS (the first sample — `deploy/nginx/`, docs/deploy.md §6), not
by the
web itself. The service's metrics and probes are on `METRICS_ADDR`
(`127.0.0.1:9107`),
they are not exposed to visitors. `rate_limit` — the limit of live
checks of
the manual-check form per IP, requests per minute (T-123;
the recorded decision of stage 13 — like the larasites form's
throttling:
4/min). Behind a reverse proxy the visitor's address reaches the
service via `X-Real-IP`
(the header is trusted only from a TCP loopback peer and a unix-socket
peer —
the nginx sample in `deploy/nginx/` rewrites it with the client's real
address; the fallback `X-Forwarded-For` without `X-Real-IP` takes the
last
position of the list — the one appended by the proxy); without the
header all visitors
of the proxy share one bucket, and the `web_checks` record is skipped
with a Warn
(the address label is not written to inet). `timeout` — the cap of a
live form check: the full pipeline
of one name (DNS → HTTP → SPA render → analysis, T-119/T-123); the
answer
reaches the visitor no later than the cap. `live_concurrency` — the
cap on
concurrent live checks (T-220): a semaphore before the pipeline
starts,
the excess gets a fast 503 `{"error":"busy"}` with `Retry-After`
instead of
waiting for a slot until `timeout` (the 503 is also returned for a
known domain: a 200 with
`found=false` would read as "Laravel not found", and the landing does
not display the stored part). The form's pipeline
runs on the long-lived `checker.Runtime` instance — one resolver
chain
with a small UDP pool (`checker.LiveUDPPoolSize`, 3 chain
clients ≈ 48 fd per service), a shared `Fetcher` with the `http`
section's limits,
a rule cache with background reload and a single render.Manager; the
instance
is assembled by the first live check and closed at service shutdown
(before T-220 every form request built its own resolver chain with
`udp_pool_size` pools — up to 6000 UDP sockets per request; ~10
parallel
checks exhausted the unit's fds together with `/healthz`).
`public_host` —
the canonical host of the landing's public address (T-227, e.g.
`crawler.example.com`, with a non-standard port — with it): the
absolute URLs of `sitemap.xml` are built from it. Behind a reverse
proxy nginx passes `Host`
as the visitor's `$host` — without the canonical host `<loc>` depends
on the header
of an untrusted party (host-header poisoning); an empty field (the
default) —
a fallback to the request's `Host`; a landing published outward must
set
the field. The field does not set the sitemap scheme: a direct
connection is a TLS fact; behind a
proxy — `X-Forwarded-Proto` from a trusted peer (loopback/unix, the
same
principle as the visitor's address); an untrusted value is ignored.

### `dashboard` (the dashboard, dashboard spec §8.1)

| YAML | Variable | Type | Default |
|---|---|---|---|
| `listen` | `CRAWLER_DASHBOARD_LISTEN` | [host]:port | `127.0.0.1:8090` |
| `refresh` | `CRAWLER_DASHBOARD_REFRESH_INTERVAL` | duration | `5s` (allowed range 1s–5m) |
| `status_cache_ttl` | `CRAWLER_DASHBOARD_STATUS_CACHE_TTL` | duration | `1m` (allowed range 1m–5m; T-161) |
| `username` | `CRAWLER_DASHBOARD_USERNAME` | string | `""` |
| `password_hash` | `CRAWLER_DASHBOARD_PASSWORD_HASH` | string of the dashboard spec §7.2 format | `""` |
| `probes` | `CRAWLER_DASHBOARD_PROBES` | map name=url | — |
| `render_metrics_urls` | `CRAWLER_DASHBOARD_RENDER_METRICS_URLS` | list of http(s) URLs | — |

`listen` — the operator dashboard's public address: pages and API
(dashboard spec §4.2). The default is loopback: outward publication is
a reverse proxy with TLS
(dashboard spec §9.1), not the service itself; the dashboard's metrics
and probes are on `METRICS_ADDR`
(`127.0.0.1:9108`). `refresh` — the period of the background build of
state snapshots
(dashboard spec §4.4), allowed range 1s–5m (dashboard spec §8.3):
faster — extra DB load
(the numbers change more slowly); slower — hurts the freshness of the
summary; the load
does not depend on the number of open tabs (the browser receives a
cached
snapshot).

`status_cache_ttl` — the TTL of the summary's heavy-aggregate cache
(T-161, measurement §12
[`load-measurements.md`](load-measurements.md)): zones with the live
aggregate
`check_state`, the rate and the status distribution — full `domains`
passes;
within the TTL window `refresh` passes reuse the last answer
(`status.Cache`
of the shared `internal/status` package); after expiry — one selection
per section;
errors are not cached (a section's degradation stays visible). Light
sections
(the latest finds by index, the ingest log) bypass the cache on every
pass. Allowed range 1m..5m (the T-161 statement): below a minute the
cache does not help
at the default `refresh` of 5 s; above 5 minutes the summary loses its
operativeness.
`crawlerctl status` does not use the cache: one-off diagnostics values
fresh data, and a separate process gets no hits; the `web` landing
does not read the `internal/status` summary (its data — cached zone
counters —
is cheap by design, T-122).

`username`/`password_hash` — the operator's credentials (HTTP Basic,
dashboard spec §7.1).
The shared `config.Validate` does **not** check them: only
`cmd/dashboard` checks their presence
at startup (dashboard spec §7.7) — otherwise the requirement
would hit every service of the project: empty values (or an
unparseable
hash string) are a startup refusal with code 1, before migrations are
applied and
listeners are raised. `password_hash` — a string of the format
dashboard spec §7.2:

```
pbkdf2-sha256$<iterations>$<base64 salt>$<base64 key>
```

PBKDF2-HMAC-SHA256, salt ≥16 bytes, key 32 bytes; the iterations are
read from the
string, generation uses 600,000 (OWASP). Generation — `crawlerctl
dashboard-secret` (the password as an argument or stdin; the help
warns
about shell history); a plaintext password in the configuration or the
environment is
forbidden. Parsing and verification — the `internal/dashboard/auth`
package (also used by
`cmd/dashboard`, T-128).

`probes` — liveness probes of the pipeline services for the health
section
(dashboard spec §4.3, §5.8): name → the service's metrics base URL
(the framework
`internal/service` serves `/livez` on METRICS_ADDR; in the docker
network —
`http://<service-name>:<port>`, in the systemd deployment — the units'
loopback addresses,
see docs/deploy.md). The value is an absolute http(s) address;
names are lowercased; an empty map — the health section shows
"n/a". The variable replaces the YAML map in full, items separated by
commas: `CRAWLER_DASHBOARD_PROBES="dns-worker=http://dns-worker:9103,http-worker=http://http-worker:9104"`.

`render_metrics_urls` — the render workers' `/metrics` addresses for
the engines panel
(T-246, render spec §4.6 item 4): the render rate/success per
engine (the stage counters' `engine` label, T-235 — the attribution
exists
only in the metrics; the DB schema knows nothing about engines) and
the fallback rate —
published/consumed/depth of `q:render-fallback`. The main build is
the stage engine (9105); worker B's fallback build is consumption
(systemd:
9120, deploy.md §2.3); the dashboard reads the depth from Redis
directly.
The values are absolute http(s) addresses; empty — the panel shows
"n/a" (not an
error). The variable replaces the list in full, items separated by
commas
(edge whitespace is trimmed; empty items — extra commas — are removed
by
normalization, like a YAML list).

The dashboard's cookie session (24 h, dashboard spec §7.5) gets the
`Secure` flag based on actual
TLS (T-227): a direct TLS connection or `X-Forwarded-Proto: https`
from a
loopback peer (a TLS-terminating proxy — docs/deploy.md §6; there is
no flag
for this in the configuration). In a plain deployment without TLS the
flag is not set — a cookie with
`Secure` would never be returned over http.

### `log` (the service framework)

| YAML | Variable | Type | Default |
|---|---|---|---|
| `level` | `CRAWLER_LOG_LEVEL` | `debug`\|`info`\|`warn`\|`error` | `info` |

The level of the `log/slog` structured logger of the
`internal/service` framework
(T-012). An extension to the minimal set of spec 5.10: every service
needs the level at once,
not an individual stage. The value's case does not matter.

## Validation

Everything is checked at once; the error lists problems of the form
`section.field: what is wrong`:

- `postgres.dsn` is non-empty; `pool_size` 1..100;
- `redis.addr`, `dns.resolvers`, `dns.fallback_resolvers`,
  `monitor.listen`,
  `dashboard.listen` — valid `host:port` (port 1..65535); at
  least one resolver; `monitor.ingest_metrics_url` (if set) — an
  absolute
  http(s) address; `web.listen` — `host:port` or an absolute
  unix-socket path
  ≤100 bytes (the deployment variant behind nginx, T-124);
  `redis.maxlen` — 0 or 1000..100000000; `redis.max_attempts` 1..100;
  `redis.claim_min_idle` positive; `redis.recover_batch` 1..1000;
  `redis.recover_interval` 1s..1h; `redis.trim_interval` 1s..24h;
  `redis.consumer_idle` 1m..30d;
- all timeouts/intervals are positive; `ingest.batch_size` 1..1000000,
  `ingest.stable_seconds` 1..3600; `ingest.scan_interval` 10ms..1h;
  `scheduler.batch_size` 1000..10000 (spec 5.3);
  `queue_high_watermark` ≥ 1,
  `temp_error_max_retries` 1..100; `scheduler.backlog_interval`
  30s..1h
  (T-161); `monitor.zone_stats_interval` 30s..1h (T-161),
  `monitor.alerts_interval` 30s..1h (T-206);
  `monitor.alerts.disk_free_crit_bytes` < `disk_free_min_bytes`,
  `monitor.alerts.queue_freeze_window`/`psi_cpu_window` 1m..6h,
  `psi_cpu_percent` (0, 100]; `mem_high_passes` 1..10,
  `telegram_script` non-empty at `telegram: true` (T-162);
  `scheduler.zone_weights` — the keys
  (zone punycode names lowercased) syntactically
  valid, weights 1..1000; `dashboard.refresh` 1s..5m;
  `dashboard.status_cache_ttl` 1m..5m (T-161); `dashboard.probes` —
  non-empty names, absolute http(s) URLs (dashboard spec §8.3);
  `dashboard.username`/
  `dashboard.password_hash` are not checked by the shared `Validate` —
  `cmd/dashboard` checks the presence of credentials at startup
  (dashboard spec §7.7);
- `dns.concurrency`/`http.concurrency` 1..100000;
  `dns.max_qps` 1..1000000,
  `http.per_ip_limit` 1..64; `http.per_ip_overrides` — CIDR
  keys
  IPv4/IPv6, limits 1..64, networks without duplicates after masking
  (T-165),
  `http.max_redirects` 0..100; `http.max_rps`
  1..1000000; `max_body_bytes`/`min_body_speed` > 0,
  `http.task_concurrency` 1..10000; `http.task_consume` 1..1000,
  `http.task_per_domain` 1..126 (checks per task ≤ 2 × 63 subdomains,
  spec 5.10); `http.dead_host_cutoff` — a boolean without a range
  (T-164),
  `http.mem_limit` 0 (disabled) or 256 MiB..16 GiB (T-163);
- `dns.timeout`/`dns.tcp_timeout`/`dns.retry_delay` are positive;
  `dns.retries` 1..10 (spec 5.4: retries are mandatory);
  `dns.udp_pool_size`
  1..100000;
- `http.user_agent` non-empty, up to 512 bytes, no line breaks or
  control characters;
- `render.contexts` 1..16; `restart_every_pages` 1..1000000,
  `timeout_attempts` 1..100 and ≤ `redis.max_attempts` (T-148),
  `remote_url` (if set) — a valid URL,
  `engine` — `chromium`/`moli`/`lightpanda`; for `moli`/`lightpanda`
  `remote_url` is required and `restart_memory_bytes` = 0 — another
  process's
  RSS is not accessible (T-233, the stage 35 spec FR-3);
  `render.fallback.worker_rate` 1..1000000; `share_percent` 0 (no
  cap) or (0, 100] (NaN is rejected); `health_gate_streak` 1..100
  — validated at any `fallback.enabled` (T-233),
  `queue_high_watermark` 0 (the shared threshold) or 1..10⁸ (T-174),
  `late_select_timeout` 0 or 1m..24h (T-274),
  `requeue_deprioritized_zones` — zone names in lowercase punycode
  (≤ 253 octets; TLDs — com, in; IDN — xn--p1ai), no duplicates
  after lowercasing (T-184);
- `content.pack_size` 1 MiB..8 GiB; `max_store_bytes` 1 byte..16 MiB,
  `disk_free_percent` in (0, 95]; `retention` 0 or 1m..30d,
  `retention_interval` 1m..24h; `retention_batch` 1000..100000,
  `retention_analyze_watermark` 0 or 1..10⁸ (T-166),
  `retention_select_timeout` 0 or 1m..24h (T-274);
- `analyzer.laravel_min_score` in (0, 1];
- `ingest.max_error_ratio` in (0, 1];
  `ingest.archive_max_uncompressed`
  1 byte..1 TiB; `ingest.archive_max_files` 1..1000000;
  `ingest.allowed_zone_paths` — valid zone names (idn.NormalizeZone)
  in lowercase punycode, at least one dot in each entry,
  duplicates collapse after normalization (spec supplement §6.1);
- `subdomains`: 1..63 entries, lowercase labels without dots, no
  duplicates;
- the paths (`ingest.dir`, `content.dir`, `analyzer.rules_dir`) are
  non-empty;
- `log.level` — one of `debug`/`info`/`warn`/`error`.

## Deprecated Variables (backward compatibility)

| Variable | Effect |
|---|---|
| `APP_PG_DSN` | overrides `postgres.dsn` if `CRAWLER_POSTGRES_DSN` is not set; used by `deploy/docker/compose.yaml` and the Makefile |
| `APP_CONFIG` | the fallback name of the configuration file path variable |
| `METRICS_ADDR` | the address of a particular service's probes (livez/healthz) and /metrics (the fixed ports of the dev framework `:9101`…`:9106`; web — `127.0.0.1:9107` loopback; dashboard — `127.0.0.1:9108` loopback; in compose both are unfolded onto `:91xx` of the docker network for Prometheus); for `monitor` it is practically not applied: the address is set by `monitor.listen` (takes precedence, overridden by `CRAWLER_MONITOR_LISTEN`); `METRICS_ADDR` is a safety net only when `monitor.listen` is empty |

The new names (`CRAWLER_*`) always take precedence over the deprecated
ones. Add new variables only with them; the deprecated ones are
supported while the dev environment references them.

## Dev Environment (Docker)

- [`deploy/docker/config.dev.yaml`](../deploy/docker/config.dev.yaml)
  is mounted into the containers as `/etc/crawler/config.yaml`; the
  path is set by
  the `CRAWLER_CONFIG` variable (the shared `x-go-common-env` block in
  compose).
- The DSN on top of the YAML is provided by `APP_PG_DSN` (assembled in
  compose from `.env`).
- The dev crawler's `User-Agent`: `CRAWLER_USER_AGENT` from `.env` →
  the variable
  `CRAWLER_HTTP_USER_AGENT` in the containers (a mandatory dev mark).
- The docker network has no IPv6: `dns.ipv6: false` in the dev config;
  the fallback resolvers
  are empty — the environment is isolated from the internet (see
  [`dev-environment.md`](dev-environment.md), the internet mode).

## Code Integration

- `config.Load()` — the path from `CRAWLER_CONFIG`/`APP_CONFIG` (or
  auto-discovery of
  `./config.yaml`), merging the environment, validation; called in
  the `main`
  of each service.
- `config.LoadFile(path)` — an explicit file + the process
  environment.
- `config.LoadEnv(getenv)` — the same, with environment injection
  (tests).
- `config.Default()` — the default values from spec 5.10.
- `service.Options.Config` — the loaded config is passed to the
  framework
  (`internal/service`); the DSN for migrations is taken from it (or
  from a direct
  `Options.PGDSN`), and `log.level` — for the framework's logger
  (`service.NewLogger`; `Run` itself also creates a logger if
  `Options.Logger`
  is not set and makes it the default via `slog.SetDefault`).
- `db.NewOptions(cfg.Postgres)` — the `postgres` section → the
  parameters of the
  `internal/db` pool (`db.Open`; see [`db.md`](db.md)).

The "variable → field" binding table — `envSpecs()` in
`internal/config/load.go`; tests: `internal/config/config_test.go`
(defaults, YAML, environment, priorities, validation) and
`test/integration/config_test.go` (the dev environment as a whole).

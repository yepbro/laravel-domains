# DB layer: connection pool and bulk writes

The `internal/db` package (T-007): a `pgx` pool on top of the `postgres` section of
the configuration and bulk write helpers — streaming `COPY` in batches and batched
`UPDATE … FROM unnest(...)`. Principles of the main spec: §6.3 (a pool of ≤ 20–50
connections in total), §3.5 and §6 ("write in batches with COPY, UPDATE … FROM
unnest(...)", memory independent of the data volume).

Table schema — in [`schema.md`](schema.md), migrations — in
[`migrations.md`](migrations.md), parameters of the `postgres` section — in
[`configuration.md`](configuration.md).

## Connection pool

```go
pool, err := db.Open(ctx, db.NewOptions(cfg.Postgres)) // dsn + pool_size
defer pool.Close()
```

- `db.Open` parses the DSN, applies the settings and does a fail-fast `Ping`:
  an unreachable DB is a startup error with a clear message (the password is
  hidden), not a failure on the first query.
- `*pgxpool.Pool` is passed to the package helpers directly (and satisfies the
  `db.DB` interface, by which the helpers accept any matching connection —
  unit tests rely on this).

Settings (`db.Options`; default values are package constants):

| Parameter | Default | Meaning |
|---|---|---|
| `PoolSize` | 20 (`DefaultPoolSize`) | the pool's maximum connections; overrides `pool_max_conns` from the DSN — the configuration is the source of truth |
| `MinPoolSize` | 0 | background connections; 0 — lazy opening, idle time holds no connections |
| `MaxConnLifetime` | 30m | re-establishing a connection |
| `MaxConnIdleTime` | 5m | closing an idle connection |
| `HealthCheckPeriod` | 1m | background connection health check |
| `ConnectTimeout` | 10s | establishing a single connection |

`PoolSize` is validated (1..100): a pool larger than the spec budget makes no sense.

### Session AfterConnect hooks (T-171/T-192/T-201)

`Options.AfterConnect` (`service.Options.DBAfterConnect` in the service
framework) — setup of every pool connection, including re-openings triggered by
`MaxConnLifetime`: session-level `SET`s survive connection rebuilds.
Ready-made hooks of the package:

| Hook | Action | Enabled in |
|---|---|---|
| `db.NoParallelGather` (T-171) | `SET max_parallel_workers_per_gather = 0` — aggregate passes do not consume parallel workers and do not take all cores of the shared CPU away from the pipeline | monitor, dashboard, scheduler (seq passes over backlog/retry temp_error), http-worker (T-201: retention drain — the time-form of T-185 was gathering Gather Merge on a starved CPU of the same service) |
| `db.LockTimeout(db.DefaultLockTimeout)` (T-192) | `SET lock_timeout = '30000ms'` — a cap on waiting for someone else's row locks: the 25.10 convoy (holder ~37 min, victims up to 39 min) is cut off by a transaction rollback and a redelivery (at-least-once), not by mute waiting. 30 s is three orders of magnitude above normal waits and an order of magnitude below incident ones; a constant, not a config | dns-worker, http-worker, scheduler — write pools with row locks on `domains` |
| `db.JoinHooks(...)` | composition of several hooks of one pool (failure of any one is a connection-open error) | scheduler: `JoinHooks(NoParallelGather, LockTimeout(DefaultLockTimeout))`; http-worker — the same two hooks (T-201) |

Readers without row locks (monitor, dashboard, web) and the analyzer do not get
the `LockTimeout` hook (monitor/dashboard get only
`NoParallelGather`): `lock_timeout` also cuts legitimately long waits;
the decision is deliberate per service (sample reasoning — T-171).

Besides the session hooks, heavy background scans of one service are coordinated
by the `db.ScanGate` gate (T-271): a buffered channel of capacity 1, shared by
independent loops (http-worker: retention drain ↔ the late render
re-publisher). The plan pins of the selections themselves
(`SET LOCAL` seq/hash/merge/bitmap off in the tx wrappers, the
T-181/T-191 style) remove the
degradation to seq scan + hash join + Sort and the bitmap path with an explicit
Sort (the bitmap does not preserve the cursor index order); the gate is a guard
against residually heavy scans stacking on top of `shared_buffers` (the incident
of 08–09.11.2026: two scans of ≈1 GiB each — five PostgreSQL OOM kills in
one night). A nil gate is a no-op; point statements are not placed under the gate.

### Connection budget: 20–50 across ALL services

`postgres.pool_size` is set **per service**, while the spec §6.3 recommendation is
a total across all services (in the spec §5.10 example, `pool_size: 20` is the
whole budget). With nine services, plan for 2–5 per service (for example,
`CRAWLER_POSTGRES_POOL_SIZE=4`; the global variable also affects the
dashboard — the unit's EnvironmentFile overrides its Environment,
so a safe global maximum is 4), keeping in mind
`max_connections`
of PostgreSQL (in dev — 130: the service stack + parallel test runs on
isolated schemas, `postgresql.dev.conf`; in production — 50, see the
tuning section below).
Pools are lazy: a service without DB load holds no connections at all,
so the total peak is determined by the actually running workers.

## Integration with the service framework

`service.Options.OnDBPool` (T-007): if set and the DSN is known, the framework
opens a pool from `postgres.dsn`/`pool_size` after applying migrations and calls
`OnDBPool(pool)` with the ready pool. The pool is closed by the framework last
during graceful shutdown — after the worker logic finishes (`Options.OnRun`,
context cancellation on SIGTERM/SIGINT, spec 3.4) and the metrics HTTP server
stops. Stub services without `OnDBPool` keep working without a DB —
the behavior is unchanged.

```go
service.Run(service.Options{
	Name:   "scheduler",
	Config: cfg,
	OnDBPool: func(pool *pgxpool.Pool) {
		// keep the pool for the worker logic
	},
	OnRun: func(ctx context.Context) error {
		// worker logic; termination — on ctx cancellation at
		// SIGTERM/SIGINT, to finish before the framework closes the pool
		<-ctx.Done()
		return nil
	},
})
```

## Bulk write: COPY

`db.CopyFrom` copies a ready batch of rows with a single `COPY` call (a wrapper
over `pgx.CopyFrom` verifying that rows match the column list; an empty batch is
a no-op with no DB access).

`db.CopyFromStream` — the streaming variant (spec §3.5): it reads rows from
a `db.RowIter` iterator (`func() ([]any, error)`, the end — `io.EOF`) in batches
of `batchSize` rows; each batch is a separate `COPY`, that is, a separate
transaction. Therefore:

- memory is bounded by `batchSize` and does not depend on the data volume
  (ingest reads files line by line, spec §5.1);
- progress is committed to the DB in batches — a crash loses at most the
  unfinished batch;
- context cancellation interrupts the copying **before the next batch starts**:
  finished batches stay in the DB, and the error reports the number of committed
  rows; a retry is safe with idempotent writes (spec §3.4).

```go
total, err := db.CopyFromStream(ctx, pool,
	pgx.Identifier{"domains"}, []string{"zone_id", "label"},
	50000, // ingest.batch_size (spec §5.1)
	func() ([]any, error) { return parseNextLine() })
```

COPY into a staging table (the ingest style of spec §5.1) uses the same helpers;
the staging table itself and applying add/delete — task T-017.

## Bulk update: UPDATE … FROM unnest(...)

`db.Update(table)` builds a typed batched UPDATE: keys (`Key`),
per-row values (`Set`), fixed expressions (`SetExpr`), a guard
(`Where`), `Returning` + `Query`. Values are passed **as columns** —
parallel arrays (the order: keys first, then the SET columns).

```go
// Scheduler (spec §5.3): move the selected domains to queued, from pending only.
rows, err := db.Update("domains").
	Key(db.Column{Name: "zone_id", Type: "int2"}).
	Key(db.Column{Name: "id", Type: "int8"}).
	Set(db.Column{Name: "check_state", Type: "int2"}).
	SetExpr("queued_at = now()").
	Where("t.check_state = 0").      // guard against races
	Returning("t.zone_id", "t.id").  // publish to the queue exactly the rows that were moved
	Query(ctx, pool, zones, ids, states)
```

The resulting SQL:

```sql
UPDATE "domains" AS t
SET "check_state" = u."check_state", queued_at = now()
FROM unnest($1::int2[], $2::int8[], $3::int2[]) AS u("zone_id", "id", "check_state")
WHERE t."zone_id" = u."zone_id" AND t."id" = u."id" AND t.check_state = 0
RETURNING t.zone_id, t.id
```

Properties:

- each column's array is **one** query parameter, so the batch size
  does not hit the protocol limit (65535 parameters); the contract is pinned
  by an integration test with 70 000 rows in one query. The upper bound of the
  batch is set by the stage configuration (`scheduler.batch_size` and the like);
- `Where` is a guard: a bulk state transition changes only rows
  in the expected state; a second run returns 0 rows
  (idempotency, spec §3.4);
- table and column names are quoted as identifiers; `Where`,
  `SetExpr` and `Returning` are SQL expressions from code, not from user
  input;
- an empty batch is a no-op with no DB access (`Exec`); `Query` with an empty
  batch returns an error (RETURNING would return no rows);
- a length desync of the column arrays is an error before the query runs.

## Errors and contexts

All helpers accept a `context.Context`: a timeout and cancellation interrupt
the current query; the streaming COPY checks the context between batches. Errors
are wrapped with the table name and the number of affected/written rows;
the root cause is available via `errors.Is`/`errors.As`. Partial writes are
possible only at batch boundaries — retrying the operation is safe with
idempotent writes (spec §3.4).

## Production PostgreSQL tuning (NVMe) — T-086

The production configuration is
[`deploy/postgres/postgresql.conf`](../deploy/postgres/postgresql.conf):
PostgreSQL 16 from OS packages on an 8 GB RAM / NVMe server (spec §10.1, §3.1);
the resource cap — a systemd unit with `MemoryMax` 3 GB (T-085,
`deploy/systemd/`) — a guard above the planned budget of ≈ 2.5 GB
(the rationale for the headroom — the "Memory" section below). The file is
attached to the packaged PG cluster as
`config_file=` (or an include). The file format and the spec's contract values are
guarded by the unit test `deploy/postgres/postgresql_conf_test.go`
(`make test`); the configuration was verified against a live run of
PostgreSQL 16.15 (`SHOW` for all parameters).

The decision on the dev stack: the Docker environment (including the production
`runtime` profile) keeps using
`deploy/docker/postgres/postgresql.dev.conf` —
its planner/WAL/autovacuum semantics match production
(`synchronous_commit=off`, `random_page_cost=1.1`,
`effective_io_concurrency=200`, lowered autovacuum factors); the memory
amounts are dev-scale (`mem_limit` 1536m); `max_connections` = 130 — at 60
the integration tests on isolated schemas hit `53300` (slots
exhausted by parallel runs, the comment in `postgresql.dev.conf`).
`compose.yaml` was not changed: production PostgreSQL is an OS package
(spec §10.1), not a
container; the integration tests run on the same dev server in isolated
schemas and are unrelated to production settings.

### Connections

| Parameter | Value | Rationale |
|---|---|---|
| `listen_addresses` | `localhost` | All services on one server (spec §3.1): TCP locally only + a unix socket; PostgreSQL is not exposed externally (spec §9) |
| `max_connections` | `50` | The upper bound of the spec §6.3 budget (20–50). 8 services × a pool of 5 (7 pipeline ones + web) + the dashboard (2 — its unit sets it; the global `CRAWLER_POSTGRES_POOL_SIZE` would override it) + `crawlerctl` (2) + admin ≈ 44–46; 3 slots are reserved by `superuser_reserved_connections`. Every backend eats memory (§3.2) — no more is possible |

### Memory (the spec §3.2 budget: ≈ 2.5 GB for the whole instance)

| Parameter | Value | Rationale / trade-off |
|---|---|---|
| `shared_buffers` | `2GB` | The §3.2 budget "shared_buffers ≈ 2 GB" — 25% of RAM; the OS caches the rest |
| `effective_cache_size` | `4GB` | A hint to the planner (2 GB of buffers + the page cache); no memory is allocated |
| `work_mem` | `16MB` | The memory of one plan node of one query. Point reads by indexes + lazy pools → only a few heavy nodes are active at a time; worst case ~30 × 16 MB ≈ 0.5 GB — the backends' budget. Bulk work goes in `COPY`/`unnest` batches and spends no `work_mem`. Above 32 MB it leaves the budget |
| `maintenance_work_mem` | `256MB` | `CREATE INDEX`/migrations on partitions of 30–50M rows (§6); one-off short-lived memory |
| `autovacuum_work_mem` | `128MB` | A separate cap for the vacuum workers (the default −1 divides `maintenance_work_mem`): 4 × 128 MB and no competition with migrations. A 128 MB TID array ≈ 20M dead rows — with the 0.02 trigger it is actually an order of magnitude less; on overflow vacuum degrades to a page-by-page pass, the memory does not grow |

The unit's `MemoryMax` is 3 GB, above the planned budget of ≈ 2.5 GB: in a
maintenance window, on top of `shared_buffers` of 2 GB, peak
`work_mem` (~0.5 GB), `maintenance_work_mem` (256 MB — CREATE INDEX/migrations
while workers run) and autovacuum (up to 4 × 128 MB) legitimately add up
to ~3 GB; a cap equal to the budget would kill a backend with a cgroup OOM
instead of the protected mode
(spec 11.4 item 7). As with Redis ([redis.md](redis.md) §7), the cap is a guard
above the budget, not a reservation: normally the instance lives within
≈ 2.5 GB.

### Planner and NVMe (spec §6.3)

| Parameter | Value | Rationale |
|---|---|---|
| `random_page_cost` | `1.1` | Random reads from NVMe are hardly more expensive than sequential ones; the default of 4.0 unfairly penalizes index scan, while the workload is point reads by PK/indexes and bitmap scans of partitions |
| `effective_io_concurrency` | `200` | Spec "≥ 200": the NVMe queue depth digests the prefetch reads of a bitmap heap scan |
| `maintenance_io_concurrency` | `200` | Prefetch for VACUUM and index builds on large partitions — vacuum does not become the bottleneck |
| `default_statistics_target` | `100` | The default; enough for the selection keys (label, check_state via the partial index) |

### WAL and durability (spec §6, §3.4)

`synchronous_commit = off` is prescribed directly by spec §6 ("for crawler data").
The trade-off: on the loss of a server (not a process crash), the commits of the
last ~3 × `wal_writer_delay` ≈ 0.3 s are lost. All writes are idempotent and
recoverable by re-enqueueing/re-checking (§3.4, §6.3: no
backups required) — losing a result costs an extra check, not a loss of
data. In exchange, batched COMMITs (batches of `COPY`, `UPDATE … FROM unnest`)
do not wait for an `fsync` per transaction — critical with billions of updates
per cycle. Migrations: the DDL and the version record go in one commit —
a lost commit rolls back both the marker and the schema changes; the next start
applies the migration
again, no desync.

| Parameter | Value | Rationale / trade-off |
|---|---|---|
| `wal_writer_delay` | `100ms` | The loss window ≤ ~0.3 s (3 intervals — headroom for a missed flush); 10 background fsyncs/s — nothing for NVMe |
| `wal_compression` | `on` | Compresses full-page images: fewer writes at checkpoints — less wear (spec §10.3; a summary TBW estimate — [`disks.md`](disks.md)), a negligible CPU cost |
| `checkpoint_timeout` | `15min` | Less frequent checkpoints — fewer full-page writes and less NVMe wear (§10.3, the estimate — [`disks.md`](disks.md)) |
| `max_wal_size` / `min_wal_size` | `8GB` / `2GB` | "Plenty of free space" (§3.1). The trade-off: crash recovery replays ≤ 8 GB of WAL — minutes on NVMe, acceptable for recoverable data |
| `checkpoint_completion_target` | `0.9` | Spread the flush of dirty pages over the whole interval — without write/latency spikes |

WAL's contribution to NVMe wear is estimated as a summary over the crawler load —
[`disks.md`](disks.md) "TBW estimate": PostgreSQL volume writes ≈ 0.5–1 TB per
the first cycle (3–6 GB/day in steady state) — against the background of
5–15 TB of content writes over half a year; the WAL mode
(`synchronous_commit=off` reduces fsyncs, but not
bytes) and rare checkpoints were chosen partly per §10.3.

### Autovacuum for the `domains`/`subdomain_results` partitions (spec §6.3)

The load: leaf partitions of 30–50M rows each (§6, the hash subpartitioning of
migration 000003), frequent UPDATEs of `check_state`/`last_checked_at` (HOT at
`fillfactor` 85, migrations 000001/000002), the primary ingest of 350M rows
via COPY, bulk done→pending transitions of the daily recheck (T-030);
since T-166 — background HOT cleanups of the `content_hash`/jsonb of non-finds
(retention, batches by `content.retention_batch`, docs/content.md): dead
versions of the same rows in the same rhythm as the rechecks need no separate
tuning — the insert triggers and the `scale_factor` below cover the pace.
A one-off campaign over the historical array (T-167) gives a one-off spike of
dead versions — after it, VACUUM (ANALYZE) the affected partitions
manually and watch the bloat (the procedure —
[`runbooks.md`](runbooks.md) §5.4).

**Decision: settings at the config level only; per-table `ALTER … SET` in
migrations is not used.** Partitions are created dynamically per zone/hash —
per-table values would have to be duplicated into every created partition
(migration 000003, `crawlerctl zones`), while the global config covers all
existing and future partitions at once and carries over between servers.

| Parameter | Value | Rationale / trade-off |
|---|---|---|
| `autovacuum_naptime` | `30s` | Thousands of leaf partitions: at the default 60 s, far-away partitions wait for a worker for cycles on end |
| `autovacuum_max_workers` | `4` | The default of 3 — a work queue across thousands of partitions; the workers' memory is capped by `autovacuum_work_mem` |
| `autovacuum_vacuum_scale_factor` | `0.02` | The trigger: 0.02 × (30–50M) = 0.6–1M dead rows per partition versus 6–10M at the default 0.2. Bloat is held at ~2%, each vacuum is short — 4 workers keep up; together with fillfactor 85, pages quickly return to circulation for HOT versions |
| `autovacuum_vacuum_insert_scale_factor` | `0.02` | The insert trigger (PG13+): COPY creates no dead rows, but without a vacuum the visibility map is never set — the index-only scans of the partial index `domains (check_state = 0)` degrade and freezing does not happen. Vacuum/ANALYZE per ~2% of ingest growth |
| `autovacuum_analyze_scale_factor` | `0.01` | The `check_state` distribution keeps drifting (pending→queued→done); the planner's plan for the partial index depends on it (§5.3); ANALYZE is cheap (a sample of ~30 thousand rows) |
| `autovacuum_vacuum_cost_limit` | `2000` | The default 200 credits (the limit is divided among the workers, a 2 ms tick) throttle vacuum on thousands of partitions — it cannot keep up with the update pace and bloat accumulates. 2000 ≈ 125 thousand pages/s per worker — NVMe digests it |

### Monitoring (spec §5.9)

`shared_preload_libraries = 'pg_stat_statements'` (queries for the monitor
service),
`track_io_timing = on` (I/O — to assess the effect of the NVMe tuning),
`track_functions = pl` (statistics of the `domain_info` SQL function, T-082);
logs from `warning`, UTC time.

`log_min_duration_statement = 3000` (T-177) — the slow query log:
every statement longer than 3 s is written to the server log as a line
`duration: … ms` with the full text and parameters. The rationale for the
threshold:
the pipeline's production queries are point reads by indexes (milliseconds); 3 s is
pathology (a lost index, a lock, a plan degradation), not
noise; the cost is only logging those above the threshold (PostgreSQL
always measures duration), no restart needed (reload). The backstop of the
threshold — the monitor slow query loop
(`internal/monitor/slowqueries.go`,
`monitor.slow_query_min_duration`): it polls `pg_stat_activity` and
sends statements running longer than the threshold to Telegram with the same
delivery as alerts, when `monitor.slow_query_telegram` is on (off by default
— a Warn to the log and the metrics; debounce by pid+query_start; monitor's own
aggregates are excluded by the pool's `application_name`; the service labels
in the message come from
there too; the full post-mortem trace stays in the server log). To read other
sessions' texts in `pg_stat_activity` the DB role needs
`GRANT pg_read_all_stats` (deploy.md §4.1) — without it the message honestly
marks "text unavailable".

`log_lock_waits = on` (T-192) — lock holder observability:
every session waiting for a lock longer than `deadlock_timeout` (1 s) writes to
the log a line `process N still waiting for ShareLock on transaction X …`
— the next convoy is named after it: the holder (pid, transaction).
The 25.10 incident analysis showed: without the parameter the holder's identity
is not reconstructable after the fact — the victims are visible (the slow log),
the lock owner is not. The cost is a few lines only during minutes of real
concurrent waiting; a reload parameter (like `log_min_duration_statement`,
delivered to the server by replacing the config + `systemctl reload postgresql` —
an entry in the operations runbook [operations.md](operations.md) §5).

## Tests

- Unit (`internal/db/*_test.go`, no DB): building `pgxpool.Config`
  (defaults, the priority of `pool_size` over the DSN, validation, fail-fast on
  an unreachable server), the text of the generated SQL, the AfterConnect hooks —
  the `LockTimeout` contract (the 30 s constant, the SET text, the rejection
  of a non-positive duration) and the `JoinHooks` composition (T-192),
  the http-worker pool hook wiring — the composition of `afterConnectHooks`
  (`cmd/http-worker/main_test.go`, T-201), stream splitting into batches,
  source errors, cancellation between batches, argument validation.
- Integration (`test/integration/db_test.go`, the live PostgreSQL of the
  dev environment): a real batched COPY with special characters and unicode,
  cancellation mid-stream (exactly the finished batches are in the DB), a
  batched UPDATE with a guard and different per-row values, RETURNING, a batch
  of 70 000 rows,
  the `LockTimeout` hook — a pool session sees `SHOW lock_timeout`; waiting
  for a row longer than the cap gets SQLSTATE 55P03 (T-192).
  The tests run in their own `db_test_*` schema and do not touch the working
  `crawler` schema.

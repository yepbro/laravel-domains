# PostgreSQL migrations

The DB schema is stored in the repository and applied **automatically at
service startup** — there is no separate deployment step (spec 6.3).

## How it is set up

| Part | Where | Purpose |
|---|---|---|
| SQL migrations | `migrations/` | `NNNNNN_name.up.sql` / `.down.sql` pairs (the golang-migrate format) |
| Embedding | `migrations/embed.go` | `go:embed *.sql` — the set ends up in the service binary |
| Runner | `internal/migrate` | applies the embedded set via golang-migrate as a library |
| Hookup to startup | `internal/service` | the DSN from the configuration (`Options.Config`, spec 5.10) or `Options.PGDSN` → `migrate.Up` before the metrics HTTP server starts |

All long-lived services (ingest, scheduler, dns-worker, http-worker,
render-worker, analyzer, monitor, web, dashboard) apply the migrations at startup: the healthcheck
starts answering only on the current schema. `crawlerctl` does not apply
migrations — it is a CLI: the services guarantee the schema by the time
it starts (the DB commands `status`,
`domain`, `zones`, `subdomains`, `reanalyze`, `recheck` assume an
already-prepared schema).

The services take the DSN from the configuration
([`configuration.md`](configuration.md)): the `CRAWLER_POSTGRES_DSN`
variable or the deprecated `APP_PG_DSN` (set by
`deploy/docker/compose.yaml` and the Makefile) on top of the `postgres.dsn`
YAML key.

## Parallel startup

The services start simultaneously, so the runner:

- takes a session-level `pg_advisory_lock` on a dedicated connection —
  it serializes both the creation of `schema_migrations` and the
  migration applies across all instances (it waits for up to 5 minutes,
  the `lockTimeout` in `internal/migrate`);
- golang-migrate itself holds its own advisory lock for the duration of
  `Up`, so the CLI `migrate` and the runner do not conflict either;
- "the migrations are already applied" (`ErrNoChange`) — not an error:
  restarts and hot reload
  (air rebuilds the service on a code change) are safe.

## Interruption during apply

SIGTERM/SIGINT during a migration apply (T-112) does not kill the
process mid-migration and does not force the migration to run to
completion: the signal handlers are registered before the migrations
(C-04), the waiting phases are cancelled (connecting to the DB,
advisory locks), and the applying migration is interrupted by cancelling
the running SQL statement on the PostgreSQL side —
`pg_cancel_backend` over the runner's connections (they are marked with
a unique `application_name` in `pg_stat_activity`, so the applying
migration of another instance and the services' queries are not
touched).

The atomicity of the interruption is guaranteed by PostgreSQL itself:
the pgx driver executes a migration file as a single simple-protocol
query, and the server executes several statements in one such query
within a single implicit transaction (the migration files contain no
`BEGIN`/`COMMIT` of their own). The cancellation rolls back the file as
a whole — the schema stays exactly at the previous version.

golang-migrate marks the version `dirty` before executing the file and
removes the mark after success, so after a cancellation the runner
itself returns the version to the last applied one (the semantics of
`migrate force <v−1>`, but only
when the interruption is guaranteed to have hit the migration body).
The outcome:

- the service exits normally and quickly (`ErrInterrupted` → graceful
  exit), without bringing up the DB pool or the working logic;
- a repeated start applies the interrupted migration again — no
  dirty state is left behind.

The fallback path: if the statement could not be cancelled (a non-URL
DSN without an `application_name`, the DB unreachable for the
cancellation request) — the applying migration runs to the end (the
C-04 behavior), then the service stops normally.

Verified by the integration test `test/integration/migrate_test.go`
(`TestMigrate_InterruptedMidApply`): a cancellation in the middle of
a long migration → the file and version roll back → a repeated start
completes the apply.

## Creating a new migration

```bash
make migrate-new NAME=results_indexes
# → migrations/000003_results_indexes.{up,down}.sql
```

Edit the SQL files; at the next service restart (or immediately — air
does not always see the edits in `migrations/`, restart `make up`) the
migration will be applied. The file automatically ends up in the
binaries via `go:embed` — nothing needs to be synced by hand.

Requirements for the set (checked by the `internal/migrate` unit test):
versions starting from one with no gaps, both up/down pairs for every
version, no empty files.

## Manual operations

The services apply only up. Rollback and force — via the golang-migrate
CLI in the `tools` container (the `migrations` directory is mounted):

```bash
make migrate-up      # apply all up (usually not needed — the services do it themselves)
make migrate-down    # roll back all migrations (--all)
make psql            # manual SQL queries
```

## Migration 000003: the create_zone_partitions procedure

The `000003_zone_hash_subpartitioning` migration (T-005) adds the stored
procedure `create_zone_partitions(zone_id, hash_parts)` for splitting
large zones out of the DEFAULT partitions. The services apply the
migration as usual but do not call the procedure themselves — it is an
operational command:

```bash
make psql    # CALL create_zone_partitions(<zone_id>, <hash_parts>);
```

When to split out a zone, how to compute `hash_parts` for a target of
≈ 30–50M rows, the partition layout and locking —
[`schema.md`](schema.md#subpartitioning-of-large-zones-t-005).
The down migration removes the procedure but not the zone partitions it
created.

## Migration 000015: the render debt counter

`000015_render_pending_counter` (T-175) creates the
`crawler_counters(name, value)` table and **backfills** the
`render_pending` row with a single `count(*)` over `domains` with the
bit mask `flags & 2`. On the production corpus of 41.8M rows this is
a one-time seq scan — tens of seconds (the same cost class as one T-078
zone recount pass); it applies once at service startup during a deploy,
and from then on the row is maintained by the bit transitions
(`internal/httpworker/renderq.go`: +1/−1 in the transition transaction).
Running the deploy outside a window is not forbidden — the service
startup just takes longer while the backfill runs (the migration runner
holds the advisory lock, parallel instances wait). The rollback (down)
drops the table entirely — a repeated up will recount the current flag
state.

Deploy order (the T-175 review, P3-8): any service applies the migration
at startup, but only http-worker writes the counter's ±1 — apply 000015
together with the http-worker update (the normal CD restarts all units
at once, the desync window is zero; restarting only monitor with an old
http-worker would leave the bit without an increment — a downward drift
until a reconciliation). After the deploy, run a drift reconciliation
once — [`runbooks.md`](runbooks.md) §6. The reverse direction — the same
rule: roll back 000015 only together with the http-worker rollback (or
after draining `render_pending` to zero) — otherwise result records with
the flag will start failing on the counter error (the symptom —
runbooks §6).

More about the table and why an index over the mask was rejected —
[`schema.md`](schema.md) §"crawler_counters".

## Migration 000017: the "render failed" marker

`000017_render_failed_marker` (T-210) adds the nullable column
`subdomain_results.render_failed_at` — the "render failed" stamp:
the render-worker finalizations without a render (the tarpit T-148 /
the cert class T-159 / the task class T-172) mark the record before the
content changes, and the render rework pass selection does not pick up
stamped records. The migration is only an `ADD COLUMN` (metadata, no
rewrite of 40+M rows and no backfill): the existing rows stay NULL and
will be rendered or re-finalized the normal way, receiving the stamp
from their own finalizations.

Deploy order: the migrations are embedded in the binaries
(`migrations/embed.go`) and applied by the service at startup before the
workers start — a service of the new version runs only on the schema
with 000017, the mixed-version window is safe in both directions: old
render-worker/http-worker on the new schema simply do not write and do
not filter the column, and new ones never find themselves on the old
schema (the runner applies the migration first thing). Rolling the
services back to the pre-T-210 versions without rolling back the
migration is also safe: the old code simply ignores the existing stamps
(it does not read and does not filter the column — the behavior as
before T-210); rolling back the migration itself (down) is done only
together with rolling back both services.

More — [`render.md`](render.md) "Queue cycle" and
[`schema.md`](schema.md) §"The subdomain_results columns".

## Recovery from a failure

A cancellation by a termination signal leaves no dirty state (see
"Interruption during apply"). Dirty remains after a failure inside the
migration itself (an SQL error, a power loss): then the
`schema_migrations` table contains an unconfirmed version and the
services will refuse to start with a clear error ("the schema is in
a dirty state"). Investigate the cause, then:

```sql
-- make psql
TRUNCATE schema_migrations;            -- the version is unknown, start clean
INSERT INTO schema_migrations VALUES (1, false);  -- or confirm version N
```

The CLI variant: `docker compose run --rm tools sh -c \
'migrate -path /migrations -database "$APP_PG_DSN" force 1'`.

## Integration tests

`make test-integration` applies the schema via the CLI (the app services
do not participate in the run) and verifies the runner directly:
applying to a clean schema, idempotency, and a simultaneous start of
eight instances (`test/integration/migrate_test.go`). The contract of
the schema itself (partitioning, fillfactor, indexes, sequence, HOT
compatibility) is verified by `test/integration/schema_test.go`, and the
splitting of zones out of the DEFAULT partitions —
`test/integration/zone_partitions_test.go` — see
[`schema.md`](schema.md).

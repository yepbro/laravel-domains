# PostgreSQL Schema

Schema decisions (tasks T-003 — `zones`, `subdomains`, `domains`; T-004 —
`subdomain_results`, `contents`, `laravel_domains`, `laravel_findings`,
`ingest_files`; T-005 — subpartitioning of large zones; T-031 — the
`domains.temp_attempts` column; T-082 — the `domain_info` SQL function; T-104 —
the partial indexes of background passes `domains_lease_idx`/`domains_recheck_idx`;
T-210 — the "render impossible" marker `subdomain_results.render_failed_at`).
The migration mechanism — in [`migrations.md`](migrations.md).

The schema contract is pinned by the integration tests
`test/integration/schema_test.go` and
`test/integration/zone_partitions_test.go` (`make test-integration`): they
apply the migrations into an isolated schema and verify every statement of this
page.

## Table overview

| Table | What it is | Partitioning |
|---|---|---|
| `zones` | the zone directory (TLD + allowlisted test zones), progress counters | none — the size is ≈ thousands of rows |
| `subdomains` | the reference of subdomains to check; `id` = a bit in `dns_mask` (0..62) | none — at most 63 rows (spec 5.10) |
| `domains` | all domains of all zones: status and check state | LIST by `zone_id` |
| `subdomain_results` | check results per (domain, subdomain, variant) — addresses, redirects, TLS, headers, cookies, a link to the content | LIST by `zone_id` |
| `contents` | metadata of the deduplicated content in pack files (`hash`, pack, offset, refcount) | none — no `domain_id`; the `hash` key is global (spec 5.7) |
| `laravel_domains` | the per-domain aggregate of Laravel signs (score, version range, the subdomain mask) | LIST by `zone_id` |
| `laravel_findings` | rule hits per (domain, subdomain, variant) | LIST by `zone_id` |
| `laravel_history` | the appeared/lost detection journal (T-118) | none — transitions only; the volume is orders of magnitude below findings |
| `ingest_files` | the journal of processed input files with sha256 (idempotency) | none — one row per file is appended |
| `web_checks` | the log of manual checks from the landing form (T-120; only cmd/web writes it, T-123) | none — the manual check volume is small |
| `crawler_counters` | incremental transition counters (T-175: `render_pending` — render debt; T-179/T-182: the daily `checks:`/`temp_error:YYYY-MM-DD` — throughput and the error share; T-202: the maturity buckets `retry_mature:`; T-203: the hourly throughput buckets `results:`/`rendered:YYYY-MM-DD"T"HH24`; T-204: the hourly buckets of reanalyze tails `reanalyze:…`; T-256: the `new_done_stale` level — "stale status" domains, the guard of the T-270 compensator) | none — point lookups by PK / sums of a couple of rows |
| `retention_drain_cursor` | the persistent retention drain cursor (T-274, migration 000020): the last visited `(checked_at, PK)` row — a single row (`id = 1`, CHECK), upserted by the `retentionPurge` batch transaction, DELETE on forced catch-up circles; removes the http-worker restart penalty (a full catch-up from zero remains only for the first pass after a standstill) | none — one row |

The spec §6 principle — partitioning by `zone_id` — applies to the tables with
a `domain_id` column (all rows of a zone live in one partition; `zone_id` is
duplicated in every row and is part of the primary key); reference tables and
tables without a `domain_id` are not partitioned.

## LIST partitioning by zone_id

- `PARTITION BY LIST (zone_id)` on `domains`, `subdomain_results`,
  `laravel_domains`, `laravel_findings`; the composite primary key starts
  with `(zone_id, domain_id)` — all indexes start with `zone_id`.
- Initially each table has only a DEFAULT partition — it accepts all
  zones. A large zone is carved out of DEFAULT into its own set of partitions
  by the `create_zone_partitions` procedure (T-005, migration 000003) — see the
  next section.
- A zone partition is created before the zone data is loaded (or with the rows
  moved from DEFAULT), always with `WITH (fillfactor = 85)` for `domains`
  and `subdomain_results` — see below.

## Subpartitioning of large zones (T-005)

Until a zone is carved out, its rows live in the table's DEFAULT partition. When
a large zone grows to ≈ 30–50M rows (spec §6), it is carved out
by the procedure:

```sql
CALL create_zone_partitions(<zone_id>);             -- hash_parts = 4
CALL create_zone_partitions(<zone_id>, <hash_parts>);
```

The structure after the carve-out (`<zone>` — an id from `zones`):

| Table | Zone partition | Second level |
|---|---|---|
| `domains` | `domains_z<zone>` — a plain leaf, `fillfactor = 85` | none |
| `subdomain_results` | `subdomain_results_z<zone>` | HASH by `domain_id`, leaves with `fillfactor = 85` |
| `laravel_domains` | `laravel_domains_z<zone>` | HASH by `domain_id` |
| `laravel_findings` | `laravel_findings_z<zone>` | HASH by `domain_id` |

Hash leaves are named `<table>_z<zone>_h<N>` and cover all remainders
`0..modulus-1` — a DEFAULT inside the hash set is impossible; there are no
"holes" for routing. The hash is by domain id: all rows of one domain
(the results, the Laravel findings) live in one leaf.

### Why domains has no hash

`domains` has two unique keys: PK `(zone_id, id)` and
`UNIQUE (zone_id, label)` (the T-003 contract: ingest idempotency via
`ON CONFLICT`, spec §5.1; search by name, spec §5.11). PostgreSQL 16 requires
every unique key to contain the partition key columns
**of every** level; the only part shared by the two keys is `zone_id`, a constant
inside a zone partition. Therefore subpartitioning `domains` by the hash of the id (or
label) while `UNIQUE (zone_id, label)` exists is impossible in principle — PG
refuses with the error "unique constraint on partitioned table must include all
partitioning columns". This is a refinement of spec §6/§6.1 made during
implementation: a large zone gets a single dedicated
`domains` partition. The table's rows are small
(~100 bytes) and are updated by point updates; the main volume and the rewrites live
in `subdomain_results`, which is subpartitioned in full.

### How to compute hash_parts

The target leaf size is 30–50M rows. `hash_parts` is the expected number
of domains in the zone divided by 40M, rounded up to a **power of two**
(a power of two allows splitting a leaf in half later — `modulus 2M`,
the remainders `N` and `N+M` — without rebuilding the zone's other leaves):

| Domains in the zone | hash_parts |
|---|---|
| up to ≈ 40M | 1 (a dedicated partition without a second level) |
| 40–80M | 2 |
| 80–160M | 4 (the default) |
| 160–320M | 8 |

The current zone size is `zones.domains_total` (incremental counters:
transition deltas + monitor reconciliation; T-178: live domains,
`status <> deleted`) or a direct `count(*)` over the partition (all rows,
including deleted ones — for estimating the physical size).
The upper bound of `hash_parts` is 1024. The adjacent caches `zones.domains_laravel`
(the number of the zone's Laravel domains), `domains_pending` / `domains_queued`
(the pipeline's check states, T-178) keep the same transition deltas and the
monitor service's reconciliation (T-120/T-178) — the stage 13 landing,
`crawlerctl` and the dashboard read them.

### Behavior and locks

The procedure handles all four tables in one transaction; for each:

1. `ALTER TABLE … DETACH PARTITION …_default` — with a DEFAULT attached,
   a zone partition cannot be created while the DEFAULT still holds rows of the zone
   (PG refuses: "updated partition constraint for default partition
   would be violated by some row");
2. `CREATE TABLE … PARTITION OF … FOR VALUES IN (zone)` (for the results
   tables — `PARTITION BY HASH (domain_id)`) and leaves covering all remainders;
   child indexes of all parent indexes are created automatically;
3. the rows are moved by a single command
   `WITH moved AS (DELETE FROM …_default WHERE zone_id = … RETURNING *)
   INSERT INTO …_z<zone> SELECT * FROM moved`;
4. `ALTER TABLE … ATTACH PARTITION …_default DEFAULT` — the DEFAULT no longer
   holds zone rows, the constraint check passes.

`DETACH`/`CREATE`/`ATTACH` hold an `AccessExclusiveLock` on the table:
concurrent writes **wait** for the duration of the procedure (including the row
move) instead of failing. So carve out a zone before loading it (the move is
empty, the procedure is instant) or in a maintenance window. `DETACH … CONCURRENTLY`
inside the procedure is unavailable — it does not work in a transaction.

Properties:

- **idempotency**: tables whose zone partition already exists are
  skipped with a NOTICE — a repeated `CALL` is safe and will finish an interrupted
  carve-out;
- repeated calls for the same zone are serialized by an advisory lock;
- the arguments are validated: the zone must exist in `zones`; `hash_parts` —
  a power of two from 1 to 1024;
- a failure rolls everything back to the original state (one transaction).

The down migration 000003 drops only the procedure; the zone partitions
it created and the data in them remain (this is data, not the procedure
definition).

## fillfactor

A lowered `fillfactor` leaves part of each page free: new
versions of updated rows are placed on the same page (spec §6 — frequent updates,
HOT updates).

- `domains` — `fillfactor = 85` (migration 000001): statuses and masks
  are updated on every check cycle.
- `subdomain_results` — `fillfactor = 85` (migration 000002): check
  history is not kept (spec §3.5); a domain recheck overwrites the result rows
  with the same primary key, and spec §6.3 explicitly lists the table
  as frequently updated (the tuned autovacuum for the `domains`
  and `subdomain_results` partitions).
- `laravel_domains`, `laravel_findings` — no fillfactor: a row is written
  once and is overwritten only by a re-analysis under new rules
  (spec 5.8); the update volume is small; spec §6.3 does not name these tables.
- `contents` — no fillfactor: the table is insert-oriented (hundreds
  of millions of rows of unique content); essentially only
  the `refcount` of the "hot" parking hashes is updated — point updates of small
  rows, for which reserving space across the whole table would cost more.
  The `refcount` updates stay HOT-compatible (no index writes —
  the counter is not part of any index; `contents_pack_off_idx` holds only
  `pack_id, off`; free space on the page is the usual HOT condition). The rare
  `Put` branch (a race of competitors for the same content, the resurrection of
  a dead row)
  rewrites the placement `pack_id, off, len, raw_len` and after T-096
  stops being HOT — the rarity does not justify a fillfactor.

A PostgreSQL 16 quirk: **storage parameters cannot be set on the
partitioned parent** — neither `CREATE TABLE … WITH (fillfactor = …)`,
nor `ALTER TABLE … SET (fillfactor = …)` (the error "cannot specify storage
parameters for a partitioned table"). Therefore the `WITH (fillfactor = 85)` from
the §6.1 DDL sketch is implemented on the partitions:

- each existing partition is created with its own `WITH (fillfactor = 85)`
  (`domains_default` — migration 000001, `subdomain_results_default` —
  migration 000002);
- **the new (T-005) `domains` and `subdomain_results` partitions must include
  `fillfactor = 85`** — reloptions are not inherited from the parent; the tests
  `TestSchema_DomainsPartitioning` and `TestSchema_ResultsTablesPartitioning`
  fail if any leaf partition lacks the parameter.

`ALTER … SET (fillfactor)` affects only future pages: on an already
loaded DB, pages are rewritten by `VACUUM FULL` outside the migration (it cannot
run in a transaction; at the time of migration 000002 there is no data).

## Indexes

The "minimum indexes" principle (spec §6): each table has exactly the set
its queries need; extending the set is a deliberate decision with the contract
updated in `schema_test.go`.

| Table | Indexes | Purpose |
|---|---|---|
| `domains` | `domains_pkey` (`zone_id, id`); `domains_zone_id_label_key` (`zone_id, label`); `domains_pending_idx` (`zone_id, id`) `WHERE check_state = 0`; `domains_lease_idx` (`queued_at, zone_id, id`) `WHERE check_state = 1`; `domains_recheck_idx` (`last_checked_at, zone_id, id`) `WHERE check_state = 2` | search by name (spec 5.11), ingest idempotency (spec 5.1), pending selection by the scheduler (spec 5.3), returning expired leases (T-029/T-104), the daily recheck (T-030/T-104) — see the section "Scheduler background pass indexes" |
| `subdomain_results` | `subdomain_results_pkey` (`zone_id, domain_id, subdomain_id, variant`) | reading a domain's results — by the `(zone_id, domain_id)` prefix of the primary key; the idempotent overwrite on a recheck |
| `contents` | `contents_pkey` (`hash`); `contents_pack_off_idx` (`pack_id, off`) | deduplication and metadata reads by a 16-byte hash (spec 5.7); pack compaction queries — the filter by `pack_id` and the order by `off` (T-096) |
| `laravel_domains` | `laravel_domains_pkey` (`zone_id, domain_id`), `laravel_domains_updated_at_idx` (`updated_at`, T-120) | the per-domain aggregate; updated on a re-analysis; updated_at — the landing's "latest findings" without sorting the whole table |
| `laravel_history` | `laravel_history_pkey` (`id`), `laravel_history_changed_at_idx` (`changed_at`) | an append-only journal of transitions (T-118); changed_at — the landing's "findings in 24 h" (T-120) and the daily dynamics |
| `laravel_findings` | `laravel_findings_pkey` (`zone_id, domain_id, subdomain_id, variant`) | hits per result; reading all of a domain's hits — by the key prefix |
| `ingest_files` | `ingest_files_pkey` (`id`); `ingest_files_zone_id_action_sha256_key` (`zone_id, action, sha256`) UNIQUE; `ingest_files_sha256_idx` (`sha256`) | dedup of exact redelivery — zone + action + content (T-113, migration 000009); `sha256_idx` serves the pipeline's by-content duplicate check (a query per file) |

Indexes for future selections are deliberately not added: for example, searching
`laravel_domains` by `score ≥ threshold` (spec 5.11, stage 8) will get its own
index in its own task, when the real query is known. There are no foreign keys —
the `(zone_id, domain_id)` reference integrity is enforced by the writing code,
while an FK would add indexes and locks to bulk writes.

The keys of `ingest_files` (migration 000009, T-113) are the same kind
of deliberate decision as `contents_pack_off_idx`: the unique triple
`(zone_id, action, sha256)` is the "exact redelivery" dedup contract (a
globally unique `sha256` would let an undo with the same list and a redelivery
into another zone slip through), while `ingest_files_sha256_idx` serves
the pipeline's actual `WHERE sha256 = $1` query on
every file: by the T-113 contract one piece of content may be recorded in
the journal by several rows (zone/action); the pipeline reads them all and
classifies them in code — a unique key does not cover such a prefix
(`sha256` is the third column), and without the index the cost of the check would grow
linearly with the journal. The journal is one row per file; both indexes are
cheap.

The exception is `contents_pack_off_idx` (`pack_id, off`, migration 000007,
T-096): it is not an index "for a future selection" but coverage of the actual
background compaction queries (`internal/content`, CompactLoop — a pass
every hour): `WHERE pack_id = $1 ORDER BY off` (the filter — the key prefix,
the sort — the second column, a plan without Sort) and the control queries
`WHERE pack_id = $1 [AND refcount > 0]` over the same prefix. Without the index
every query is a seq scan of the whole table; the cost grows linearly with the
corpus and competes with the working queries by `hash`
(the stage 12 review, INFRA-05).
`refcount` is not in the key — point updates of the counter stay HOT.

Each partition automatically gets child indexes of all parent indexes
(verified by a test — the number of leaf indexes equals the number of parent
indexes).

## Scheduler background pass indexes (T-104)

Migration 000008 (a deferred finding, S-04): before it only
the pending selection was indexed — returning expired leases (T-029, every lease/2 ≈ 15 min) and
the daily recheck (T-030) did a full seq scan of the table (up to 350M
rows), and the recheck drain re-read the skipped unripe set batch by
batch — quadratic. The "query → index" map:

| Pass | Pass predicate | Index |
|---|---|---|
| pending selection (T-027) | `check_state = 0`, not deleted, ORDER BY `(zone_id, id)` | `domains_pending_idx` (`zone_id, id`) `WHERE check_state = 0` |
| lease return (T-029) | `check_state = 1`, not deleted, `queued_at < now() − lease`, ORDER BY `(queued_at, zone_id, id)` | `domains_lease_idx` (`queued_at, zone_id, id`) `WHERE check_state = 1` |
| queued/NULL queued_at guard (T-255, FR-1.2) | `check_state = 1`, not deleted, `queued_at IS NULL`, ORDER BY `(queued_at, zone_id, id)` | `domains_lease_idx` — the same one: `IS NULL` is a searchable condition on the leading key column (NULL entries in a compact range); the shape of the main lease query does not change |
| recheck (T-030) | `check_state = 2`, not deleted, not invalid, `last_checked_at < now() − interval`, ORDER BY `(last_checked_at, zone_id, id)` | `domains_recheck_idx` (`last_checked_at, zone_id, id`) `WHERE check_state = 2` |
| deleted cleanup (T-020) | `status = deleted`, has `subdomain_results`, ORDER BY `(zone_id, id)` | `domains_deleted_idx` (`zone_id, id`) `WHERE status = 6` (T-170) |
| temp_error retry (T-031) | `status = temp_error AND check_state = 2`, `last_checked_at < now() − retry_after` | **none** — see below |
| backlog collector (T-077) | `status <> deleted` GROUP BY `check_state` | **none** — an aggregate over the whole table; a partial index cannot reduce it |

Properties of the key shape:

- **the age column comes first** — a range scan reads only the needed
  prefix: lease — from the start of the index to the first unexpired lease (normally
  0–hundreds of rows); recheck — from the oldest done rows to the batch limit;
- **the domain key in the tail** (`zone_id, id`) — the pass's ORDER BY matches
  the index key: no sorting, deterministic batches and an early stop by
  LIMIT; the keys are read from the index without heap visits (the heap is
  only the `status` filter: not deleted, and for the recheck also not invalid);
- **a linear batched drain**: the transitioned rows leave the index
  (they no longer satisfy the `check_state` predicate); the next batch
  continues from where the previous stopped — there is no external state (spec 3.4),
  and the skipped set is not re-read;
- **code literals in the SQL**: `check_state`/`status` are baked into the text of the queries
  through `model.*` (the dispatch.go style) — the planner cannot prove a parameter in the predicate of a partial
  index; the generic plan of a prepared statement (pgx caches them) would lose the index. The comparison
  `last_checked_at < $timestamptz-param` works directly:
  the cross-type operator `date < timestamptz` belongs to the datetime
  btree opfamily;
- **size**: `domains_lease_idx` is bounded by the in-flight set
  (selection → lease); `domains_recheck_idx` covers all done rows
  (~26 bytes per domain) — the price of the daily recheck of a corpus of hundreds
  of millions of rows.

**temp_error retries (T-031) — deliberately without an index.**
The pass predicate reduces to `status = temp_error`, and an index with `status`
in the key would break the HOT property of bulk status updates that do not change `check_state`:
applying the delete files (T-019, `status → deleted`) and finalizing the final
status after the attempts are exhausted (T-031, `status → delegated_no_address`).
The only exception is `status` in the predicate (not the key) of the partial
`domains_deleted_idx` (T-170): only deleted rows live in the index,
so only transitions involving deleted become non-HOT (the delete files and
the T-018 resurrection) — rare registry events; bulk updates of the result status of rows not in the index stay HOT.
`domains_recheck_idx` does not help the retry pass: the 24 h window covers almost
all done rows; a scan by age would read them in full for the sake of rare
temp_error. The pass remains a seq scan on an hourly interval; after a drain
the candidate set is empty until new temp_error ripen.

## Monitoring window indexes (T-171)

Migration 000014: the calendar windows of the summary (T-126/TZ_dashboard §4.4) and alerts
(T-080) were served by full table passes on every cycle — on a corpus
of 41.8M domains that is seconds of DB CPU per query and dozens of hours of DB
time over a day or three of the production cycle (the measurement of 23–24.10,
pg_stat_statements). The window indexes make the cost proportional to the
window, not the table:

| Window | Query | Index |
|---|---|---|
| day, `domains` | `last_checked_at >= current_date` — the temp_error share of the DNS alert (T-080); the throughput done_today reads the daily `checks:` counter (T-179, below) — the window left its read path | `domains_last_checked_idx` (`last_checked_at`) |
| hour, `subdomain_results` | `checked_at >= now() − interval '1 hour'` — the throughput results/renders (T-126); since T-203 the reader reads the hourly `results:`/`rendered:` buckets (below); only the reconciliation walks the index window, once per `zone_stats_interval` | `subdomain_results_checked_idx` (`checked_at`) |

The key is the window column itself; the values grow monotonically: the working
window is the right edge of the index; entries are added to the rightmost leaf. `last_checked_at` changes
only together with `check_state` (the HOT argument below); `checked_at` is written
by an INSERT and never changes. The partial `domains_recheck_idx` (T-104) does not
fit the windows: the `check_state = 2` predicate cannot be proven in the window
queries — a window counts checks, not domains, and a row checked today
may no longer be done. Since T-179, the daily throughput window (done_today) is
served by an incremental counter — see the `crawler_counters` section
below for the reason: the index plan remained correct, but
the index-only scan degraded with heap fetches of fresh pages (the measurement
of 24.10: 676 thousand out of 815 thousand window rows, ≈1.1 GB of reads per
call).

## crawler_counters — incremental counters (T-175/T-178/T-179/T-202/T-203/T-204)

A small general-purpose counter table: `name text PRIMARY KEY`,
`value bigint NOT NULL`, `updated_at timestamptz NOT NULL` (migration
000016 — the moment of the row's last write; the age is shown by
`crawlerctl counters list`). The pipeline counters (T-178, the
TZ_incremental spec
`tasks/TZ_incremental_counters.md`):

- `render_pending` (T-175) — the number of domains with the `render_pending` bit set
  (render debt; the T-175 observability — previously visible only through
  manual SQL);
- `status_<name>` per `model.Statuses` (T-178) — the distribution of domains across
  statuses, **all** rows including deleted (the dashboard's full legend);
  written only by reconciliation — transition statuses are not pinned
  by statement predicates (the TZ_incremental spec §13.3); the FILTER columns of the
  reconciliation scan are free;
- `laravel_appeared_total` / `laravel_lost_total` (T-178) — the cumulatives
  of the `laravel_history` transitions (T-118): `+1` incremented by the analyzer in
  the history-write transaction; the authority — reconciliation (the aggregate
  of the journal);
- `new_done_stale` (T-256, TZ-fsm §3 FR-2.2) — "stale status" domains:
  `status = new ∧ check_state = done ∧ last_checked_at` older than a day
  (the DNS stage recorded the "has addresses" branch; the HTTP outcome never arrived — a lost
  `q:http` task). An absolute level, like `status_*`: written ONLY by
  reconciliation (the `f_new_done_stale` FILTER column of the combined scan,
  zero cost; no increments), not part of the drift positions; the readers —
  the point lookup of the `domains_new_done_stale` alert and the gauge
  `crawler_domains_new_done_stale` (the reconciliation cycle). Steady state 0;
  the row's `updated_at` — the snapshot label for the alert series.
- `checks:YYYY-MM-DD` (T-179) — the daily check counter (the pipeline
  throughput): `+1` per recorded HTTP-episode result —
  incremented in the result-write transaction (internal/httpworker
  `record.go`, the RowsAffected of the domain's final UPDATE: not per
  subdomain×variant record; rows cut off by the deleted guard contribute no delta),
  the authority — reconciliation (the `last_checked_at =
  current_date` FILTER column of the combined pass, zero cost). The day name is computed
  by the DB (`current_date`) — the timezone is aligned with the `now()` that writes
  `last_checked_at` (T-058) and with the reconciliation window; Go clocks
  do not participate in the name. The increments and the live window count
  **different classes of events** (the accepted "work throughput" semantics,
  TZ_slow_done_today
  §5.1): an increment — HTTP result-write events; the window — the day's
  domains of all pipeline branches (DNS-terminal completions —
  not_delegated, invalid, etc. — have no HTTP episode; the DNS stage sets the
  `last_checked_at` mark); a repeat episode of the same domain
  gives +1 against a live zero. Between passes the value undercounts
  the window's DNS-only completions (the freshness ≤ `zone_stats_interval`);
  the `daily:checks` drift position — the observable difference between the
  streams (for the T-182 denominator the difference is immaterial: the share is
  stable, the pass fixes the absolutes). Day rotation is free: the first
  increment (or pass) after midnight creates the new day's row; yesterday's
  rows remain the throughput history (365 rows/year, no cleanup needed); the day
  row — a pass snapshot: increments committed within a pass window that
  crossed midnight do not make it into the departed day's history (there are no
  consumers of that history — outside the spec's scope); the reader — the
  `throughputSQL` of the throughput section
  (`status.FetchThroughput`, T-179): a point lookup by PK with an outer
  `coalesce(…, 0)` — an honest zero before the day's first record.
- `temp_error:YYYY-MM-DD` (T-182) — the daily numerator of the temp_error share
  (the `dns_temp_error_share` alert, T-080): `+1` per recorded
  HTTP-episode result with a final temp_error status — the same
  upsert increment in the result-write transaction (internal/
  httpworker `record.go`, after the `checks:` denominator; other outcomes
  leave the day row untouched — the day's first temp_error episode creates it),
  the authority — reconciliation (the FILTER column
  `last_checked_at = current_date AND status = temp_error` of the combined
  pass, zero cost). The day name is computed by the DB, like the denominator's.
  The semantics — record events (TZ_slow_dns_share §5.1): a retry
  that cleared the error stays in the event history; duplicate deliveries grow
  the numerator and the denominator together — the share is stable, the pass
  fixes the absolutes; the live numerator window differs from the increments by
  the same DNS-only completions and repeats — the `daily:temp_error`
  position is streaming, not part of the drift Warns, observed by a gauge. The reader —
  the share alert (observeDNS): two point reads of the day's rows with an outer
  `coalesce(…, 0)` (previously the live aggregate over the day window visited the heap
  for the status of every row — query #6 of the 24.10 slow log);
- `retry_mature:YYYY-MM-DD` (T-202) — the maturity buckets of the temp_error
  retry: temp_error episodes by date (the `last_checked_at` of the result
  record); `+1` — from BOTH pipeline branches: an HTTP outcome of temp_error —
  incremented in the result-write transaction (internal/httpworker
  `record.go`); the terminal SERVFAIL branch of the DNS stage — by a SEPARATE
  autocommit statement AFTER the write statement (internal/dnsworker
  `result.go` `resultMatureIncSQL`, T-267: a mass DNS incident arrives exactly
  through it); `−n` — the retry decisions (both terminate the
  candidate): scheduling a retry — by the tail statement of the same
  transaction (`retryRequeueMatureSQL`, by the dates from the transition's RETURNING),
  finalization — by the statement's own `rm` CTE (it references upd; no zones),
  — by the `last_checked_at` of the transitioned rows. The name — the episode
  date: a bucket ripens at midnight of the next day — the exact moment of
  the cohort's maturity given the date precision of `last_checked_at` ("older than
  24 h" ripens in midnight cohorts); the daily format is for the same reason —
  hourly granularity degenerates (all of a day's episodes ripen together);
  the key is derived identically from `current_date` (the increment) and
  `last_checked_at` (the decrement) — a key desync would leave the decrement
  with no target. The reader — the guard of the heavy retry selection
  (`scheduler/retry.go` `retryMatureSQL`): a zero sum of the past days'
  buckets (a lexicographic range by PK, the `current_date` boundary taken
  from the DB) ⇒ the seq scan over 42+ million rows does not run at all — a refinement
  of the T-191 guard by the `status_temp_error` counter, which could not tell mature
  candidates from unripe ones (543 thousand temp_error with an empty maturity
  window kept a full read-through scan on every hourly tick). The authority —
  reconciliation: the FILTER columns `status = temp_error × check_state =
  done × last_checked_at = / < current_date` of the same scan (zero cost);
  the pass keeps exactly two live buckets — yesterday's (the mature cohort) and
  today's (the day's episodes) — and deletes the stale ones (the tails of the
  decrements for old episodes' dates): the buckets are the live drain schedule,
  not throughput history. Trust boundaries: candidates that leave without
  passing through the retry
  (a temp_error domain deleted by a delete file, the age-based recheck
  of T-030) leave the bucket inflated until the pass — idle passes at most every
  `zone_stats_interval`; duplicate deliveries and repeat episodes of the day —
  +1 against a live zero (event semantics); the
  `retry_mature:past`/`retry_mature:today` positions are streaming, not part of
  the Warns;
  the ±1 non-atomicity window (T-267): the DNS stage's increment — a separate
  autocommit after the transition write; a failure between the statements (a DB
  outage, a worker crash) leaves the bucket deflated until the nearest
  reconciliation (≤ `zone_stats_interval`, 30 min) — safe:
  redelivery of the episode is powerless (the done guard will cut off both
  statements), and the guard reads only the past days' buckets, never seeing
  today's deficit; with `temp_error_retry_after` < 24 h (not
  the default) the guard postpones the drain of intraday candidates until
  midnight — a window over a date column ripens in cohorts anyway; the degradation
  is bounded by a day;
- `results:YYYY-MM-DD"T"HH24` / `rendered:YYYY-MM-DD"T"HH24` (T-203) —
  hourly throughput buckets: the dashboard's "last hour" windows of results and
  renders. The former live `throughputSQL` subqueries over
  `subdomain_results` (`checked_at >= now() − 1 h`) degraded
  with heap fetches of fresh pages — the stage 27 checkpoint (a median
  of 6.0 s > 3 s, 108 events; the same mechanism as the daily window of
  T-179 — the measurement §2 of TZ_slow_done_today). `results:` — `+N` of the
  episode's subdomain_results rows in the result-write transaction
  (internal/httpworker `record.go`; the delta — the RowsAffected of the INSERT batch;
  the hour — `date_trunc('hour', now())` of the same transaction; the rows'
  `checked_at` — the same `now()`; episodes with no records leave the bucket
  untouched);
  `rendered:` — `+1` in the render-worker content swap transaction
  (internal/renderworker `worker.go`: `rendered = true` with
  `RETURNING checked_at` — the hour of the row's EPISODE, not of the render: that is
  how the live window counted — the share of rendered among the window's
  results). A bucket is a calendar hour; the "last hour" sliding window cannot
  be assembled from buckets exactly: the reader (`status.throughputSQL`) sums the TWO
  live buckets — the previous and the current hour — and the honest speed
  divisor `hour_window_sec` — the actual length of the covered window
  `[trunc(now−1h), now]` (1–2 h; dividing by 3600 would overstate the throughput
  sawtooth-style 1–2× toward the hour boundary). The hour names are computed by
  the DB — the timezone is aligned with the `now()` that writes `checked_at`
  (T-058), with the increments and the reconciliation.
  The authority — reconciliation: a SEPARATE aggregate of the two live hours'
  window over `subdomain_results` (prev/cur FILTER positions of both families;
  `rendered` — an asynchronous flag: a late render of a past-hour row is accounted
  to the bucket of its hour) writes prev and cur of each family — prev is
  recomputed, not only cur, because rendered increments keep updating it for
  the whole current hour (the tail of the read window); the stale buckets (older
  than the previous hour) are deleted by the pass — the buckets are the live
  throughput window, not history. The semantics — events (row inserts /
  transitions to rendered)
  versus the window's live rows: duplicate deliveries and episode rewrites (DELETE
  of old rows does not decrement the bucket) give a divergence until the pass —
  the `results:prev`/`results:cur`/`rendered:prev`/`rendered:cur` positions are
  streaming, not part of the drift Warns; `verify` does not compare them; a
  rendered increment more than 1 h late resurrects a stale bucket by upsert —
  the reader does not read it (the IN list of the two live hours); the next pass
  deletes it;
- `reanalyze:YYYY-MM-DD"T"HH24` (T-204) — hourly buckets of the unanalyzed
  reanalyze tails: `subdomain_results` rows with
  `rules_version IS NULL` (never analyzed — a result-record insert of
  T-058 or a render content swap of T-067) by the EPISODE hour
  (`checked_at`). The guard of the reanalyze hourly tick
  (`analyzer/reanalyze.go`, `reanalyzeGuardSQL`) sums the buckets
  older than the current hour: an unripe in-flight stream (record → `q:analyze` →
  the stamp takes seconds) lives in the current hour's bucket and does not open
  the guard — a naive "recorded without analysis" counter could not tell tails
  from the stream (the same disease as the T-191 guard by
  `status_temp_error`, and the same maturity-bucket cure as T-202).
  A zero sum ⇒ the heavy time-form window selection (a heap visit of ~84 thousand
  rows/h for the non-indexable `rules_version` — the HOT invariant) does not
  run at all; the cursor jumps to the current hour boundary — a tick in steady
  state costs one sum over an index range of a couple of rows.
  `+N` — the rows of the INSERT batch in the result-write transaction
  (internal/httpworker `record.go`; the hour — the transaction's `now()` =
  the rows' `checked_at`; silent episodes write no zero delta); `+1` —
  a render content swap (internal/renderworker `worker.go`; the hour — the
  episode's from `RETURNING` — a late render resurrects an old hour's bucket:
  the guard opens in vain, the tail is beyond the time-form cursor; the next
  pass deletes the bucket); `−1` — the analyzer's stamp
  (internal/analyzer `worker.go`) ONLY for stale rows: the version-currency guard in
  the UPDATE stamp makes duplicate deliveries no-ops —
  the decrement does not double; stamps of an old version (rule-change drains)
  decrement without a paired increment — the negative drift until the pass keeps
  the guard closed for the duration of the drain (the tick's own work there
  intersects only with duplicates). The authority — reconciliation:
  the `prev_reanalyze`/`cur_reanalyze` FILTER positions of the two live hours'
  window aggregate (at the same zero cost on the already running T-203 throughput
  scan);
  the pass keeps two live buckets (prev is recomputed — late stamp decrements
  keep updating it for the whole current hour) and deletes the stale ones; the family lock
  order "`retry_mature:` → `reanalyze:` → `rendered:` → `results:`" is shared by all
  writers — no cycles.
  The semantics — transition events versus live NULL rows (an episode rewrite
  deletes rows without a decrement; a render of an unstamped row gives a double
  increment of one tail) — streaming: the
  `reanalyze:prev`/`reanalyze:cur` positions are observed by a gauge, not part
  of the drift Warns; `verify` does not compare them. Trust boundaries: a tail
  not picked up by a tick within the bucket's two-hour life (failed ticks) is
  invisible to the guard — a residual class of the time-form (T-193 §4.2): a
  restart with a 24 h anchor or a full rule-change drain; the cursor-jump race (commits behind
  the hour boundary longer than the seconds-long write transactions + a lost
  publication) — of the same nature and the same rarity; stale NON-NULL
  rows (a stamp of an old version shortly before a rule change + a lost
  drain publication) are not visible to the buckets at all (`IS NULL` versus
  the tick selection's `IS DISTINCT FROM`) — their territory is the next full
  drain/restart.

**The lock order invariant.** All writers of `crawler_counters`
hold the counter rows LAST in their transaction, in the order
"domains ({domains, subdomain_results, contents}) → zones → counters";
among the counters — a single family order
"render_pending → checks: → temp_error: → retry_mature: → reanalyze: →
rendered: → results:" (a convention, not lexicographic by name); within
a family of date/hour buckets — in ascending name order. The caveat for
multi-CTE statements: the "domains → zones → counters" order within
ONE statement (for example, retryFinalizeSQL: upd → rm) —
the textual order of DML CTEs; PostgreSQL does not formalize the execution order
of unreferenced CTEs (an implementation detail, a caveat in zonestats.go, the review
of T-202) — in production on 07.11 the unreferenced zc/rm of one result-write
statement of the dns-worker executed inverted (rm → zc): the statement held
a `retry_mature:` row and waited for a zone — the "counters → zones" edge against
the http-worker `RecordAndPublish` (holds the zones until Commit, waits for
`retry_mature:today`) closed a 40P01 deadlock cycle (~3–4/h; the fix
T-267 — the counters moved out of the zones statement into separate statements:
the DNS stage's increment — an autocommit after the write; the requeue decrement —
the second statement of the decision transaction; the statements of a transaction,
unlike unreferenced CTEs, execute in textual order). New writers do not
mix zones and counters in the unreferenced DML CTEs of one statement:
a referencing dependency (a CTE over upd, as in the finalization) or separate
statements. A counter is never
captured BEFORE the `domains` rows: a transaction already holding
`crawler_counters`/`zones` that then captures `domains` creates the edge
"counters → domains" against the writers going "domains → … → counters"
(the result write of the http-worker/dnsworker, the scheduler passes,
the monitor reconciliation) and closes a 40P01 deadlock cycle — the
retry-wave incident (9 deadlocks in 3 h, `q:http` redelivered as full
HTTP episodes) came from `scheduler.RetryOnce` with two statements of a batch in
one transaction; the fix — separate decision transactions (`retryDecide`,
each half follows the convention on its own). Writers: the result
write of the http-worker (`record.go`, the tail deltas before Commit),
the terminal branches of the DNS stage (`dnsworker/result.go`: the transition with zones —
one statement; the bucket increment — the next autocommit), the passes of the
scheduler (enqueue/lease/recheck/retry and the "stale status" compensator
stalerequeue of T-270 — selection and transition in separate transactions; no
counters involved: "domains → zones"; requeue — the transition and the decrement
by two statements of one transaction), the render-worker, the analyzer,
ingest add/delete (delete since T-269 — two statements of one transaction:
the status transition and the guarded pair over bit strings; the second statement
re-captures only rows already locked by the first — no new
"zones → domains" edges) and the monitor reconciliation (the last one — the
authority: it rewrites the counter rows; the order "zones → scalars →
bucket families"). Incident analysis — [`runbooks.md`](runbooks.md) §2.8.

**A guard on top of the invariant (T-268).** The invariant removes the known
cycles but does not prove the absence of new ones, so the pipeline's hot spots
additionally run under the application-level 40P01 retry (`db.RetryDeadlock`,
`internal/db/deadlock.go`): the result-write transaction of the http-worker
(`RecordAndPublish` → `recordTx`) and the retry decision transaction of the scheduler
(`retryDecide`) are retried as a whole; the autocommit statements of the DNS
episode write (`RecordEpisode`/`resultCount`) — as is, without re-writing a
DNS episode; up to two retries, with a pause of jitter and a cap. A retry is
safe only for idempotent operations (spec 3.4) — every wrapped
spot is idempotent thanks to the guards. The scheduler's temp_error drain survives
a batch error (up to `DefaultRetryDrainFails` consecutive failures). Observability
— the `crawler_db_deadlock_retried_total` counter per service; sustained
growth — a new lock cycle (an order inversion somewhere in the code); analysis —
`runbooks.md` §2.8: the retry is a guard, not a cure.

The zone columns `zones.domains_pending` / `domains_queued` (migration 000016)
— the same mechanics at the zone level: transition deltas (the DNS stage of the
result write — a CTE with `FOR UPDATE` (the main queued/pending →
done transition); the scheduler: a CTE of the same enqueue/lease/recheck/retry
statement; ingest add/delete; the CLI recheck/subdomains recheck; the http-worker —
a `FOR UPDATE` pre-read and a shift; the analyzer — `domains_laravel ±1`) and
the authoritative monitor reconciliation — one
combined pass per `monitor.zone_stats_interval`, the only
full `domains` pass in steady state. The base set of all zone columns —
`status <> deleted` (the unification of the TZ_incremental spec §4.1). `domains_pending`
is also read by the dispatcher's selection filter (`> 0`, T-183): the column's
semantics — the selection's own predicate; drift (manual SQL bypassing the
increments) can hide a zone with work until the nearest reconciliation — observed
by the `crawler_counters_drift` metric. The drift |old − new|
— the `crawler_counters_drift` gauge + a Warn; recovery — the pass itself
or `crawlerctl counters recount` (the analysis —
[`runbooks.md`](runbooks.md)).

The `render_pending` counter is maintained by **bit transitions**: `+1` —
setting the flag in the result-write transaction (internal/httpworker); `−1` —
clearing by the render catch-up pass and by applying a delete file (T-186: a deleted
domain leaves the check cycle §13.1; the pass will not select it again),
always in the same transaction as the
bit transition (§3.4 — a rollback rolls back the pair). Applying a delete — TWO
statements of one transaction (T-269, the 08.11.2026 incident: the decrement based on
the snapshot `had_render` was applied second, on top of a concurrent clearing by the
catch-up pass — −36 in one pass): the status transition without touching flags
(the RETURNING of the update carries the "the bit was set" fact anchored to the
row version the update was based on) and
the guarded pair "bit clearing × −count(fact)" over the bit-string keys —
the delta is tied to the current version's transition fact; the "snapshot →
lock" window is closed (mirroring the D7 guard). The initial value —
the one-off
backfill of migration 000015 (`count(*) WHERE (flags & 2) <> 0`, no
status filter);
the T-178 reconciliation recomputes it with the same predicate — the tail
of the "orphan bits" of domains deleted before T-186 and rare desyncs are absorbed
by aligning the counter with the aggregate (runbooks §6). The reconciliation writes
the ABSOLUTE of the early statement's snapshot (the aggregate) but executes it
late (the upsert): concurrent transitions within the window between the statements
are overwritten by the stale absolute (a false debt/a false zero) — transient,
self-heals on the next pass (≤ `zone_stats_interval`); every rewrite
advances `updated_at` (writer attribution — runbooks §6).
Reading —
a point lookup by PK (microseconds): the gauge
`crawler_render_pending_domains` of the monitor cycle, the "Render debt"
line of the load report (T-173) and the `render_debt` alert (T-175); the readers
of statuses/cumulatives — the dashboard and `crawlerctl` (without `domains`
passes, the TZ_incremental spec §8).

**Why not an index over the bit mask.** The partial index
`(flags & 2) <> 0` (modeled on T-104/T-170) was rejected: it contradicts the
HOT invariant (the section below) — `flags` changes twice per episode
of every domain (the `http_published` bit of T-108 and `dead_host_cutoff`
of T-164), millions of updates per day; an index with `flags` in the predicate
would make every such update non-HOT, not only the rare transitions
`render_pending`. The incremental counter keeps observability without
an index and without a seq scan. Manual operations on the bit must update the
counter
([`runbooks.md`](runbooks.md) §6).

## The domain_id_seq sequence

`domains.id` takes values from the single sequence `domain_id_seq`
(`DEFAULT nextval('domain_id_seq')`) — the identifiers are continuous across all zones
and partitions. The `OWNED BY` binding is not used (as in the §6.1 sketch):
the table and the sequence live equally long; a separate link is not needed.

`ingest_files.id` — a plain `GENERATED ALWAYS AS IDENTITY`: the table —
a journal; continuous numbering across zones is not needed.

## HOT updates

Checking a domain updates `status`, `check_state`, `flags`,
`dns_mask`, `queued_at`, `last_checked_at`, `temp_attempts`. An update stays
HOT (no index writes) as long as the updated column is not part of any
index — neither the key nor the predicate:

- `flags`, `dns_mask`, `temp_attempts` **are not part of any index** —
  bulk updates of the results and the attempt counter stay HOT. `status` is not
  in the keys but forms the predicate of the partial `domains_deleted_idx`
  (T-170): only deleted rows live in the index, so bulk status changes
  outside deleted stay HOT; non-HOT — only applying
  the delete files (T-019, `status → deleted`) and the resurrection by an add file
  (T-018), rare registry events (the T-031 retry pass still
  has no index — its `status = temp_error` predicate is not covered by one).
  It is exactly this invariant that rejected the partial index `(flags & 2) <> 0` for
  render debt observability (T-175): `flags` changes twice per
  domain episode (the `http_published`/`dead_host_cutoff` bits) — an index with
  `flags` in the predicate would make bulk updates non-HOT; instead of an index,
  the transitions of the `render_pending` bit maintain the counter
  `crawler_counters['render_pending']` (migration 000015, the section above);
  reading the debt — a point lookup without a table pass.
- `check_state` is in the predicates of the three partial pass indexes
  (`domains_pending_idx`, `domains_lease_idx`, `domains_recheck_idx`) —
  the `pending → queued → done` state changes are always non-HOT. This
  is an unavoidable trade-off of the spec itself: the selection/return/recheck
  indexes (§5.3) must filter by `check_state`. Such an update places a new
  row version (fillfactor = 85 leaves room) and adds entries to the
  indexes the new version satisfies; the impact is limited by the frequency of state
  changes (two per domain check cycle).
- `queued_at`, `last_checked_at` are in the keys of `domains_lease_idx` and
  `domains_recheck_idx` (T-104); `last_checked_at` is also in the monitoring
  window index `domains_last_checked_idx` (T-171) — their updates are non-HOT,
  **but all writers change these columns only together with `check_state`**:
  enqueueing (pending→queued + `queued_at = now()`), the lease return and
  the recheck (→pending + `queued_at = NULL`), the dns/http worker result
  (→done + `queued_at = NULL, last_checked_at = now()`), the resurrection by an add
  file (T-018, →pending + `queued_at = NULL`). These
  transitions are already non-HOT because of `check_state` in the predicates —
  the new indexes added not a single non-HOT update, only one more entry in the
  same transition (pinned by the `TestSchema_DomainsHOTColumns` test: age
  columns are allowed only in the keys of their own pass/window indexes).
- `subdomain_results` on a result rewrite updates only
  non-indexed columns (the index — the primary key itself, which does not
  change) — the updates are HOT when there is room on the page (fillfactor = 85).
  The window index `subdomain_results_checked_idx` (T-171) does not affect HOT:
  `checked_at` is written only by the INSERT of a new result row
  (a domain rewrite — DELETE + INSERT in one transaction, §3.5);
  the column has no UPDATEs.

## CHECK invariants (T-259)

Invariants that the code always enforced with the SQL guards of the writers are
fixed in the schema as CHECK constraints — manual SQL, future writers
and experiments will not be able to violate them. Adding — by a `NOT
VALID` migration: large tables (tens of millions of rows) cannot be scanned during
deploy; `NOT VALID` checks only new and modified rows,
existing data is not touched (there are no violators among them — the guards
upheld the invariant). `VALIDATE CONSTRAINT` — a separate step per the procedure,
after a soak (the production observation window): on a partitioned
parent, PostgreSQL 16 validates recursively across all partitions with one
statement, and the new partitions of `create_zone_partitions` inherit
the constraint already validated. Before `VALIDATE` — a reconnaissance query for
violators (the same full scan, but the diagnosis comes BEFORE, not after someone
else's UPDATE fails: a latent violator is not visible to `NOT VALID`, and the
first row writer to hit it — the analyzer/retention/collapse — will fail):

```sql
SELECT count(*) FROM subdomain_results
WHERE rendered AND render_failed_at IS NOT NULL;
```

The reconnaissance query for the violators of `contents_refcount_check` (T-265):

```sql
SELECT count(*) FROM contents WHERE refcount < 0;
```

The paired test invariants —
`test/integration` (a violation is rejected; `convalidated = false` before
VALIDATE).

| Constraint | Table | Invariant | Migration | Test |
|---|---|---|---|---|
| `subdomain_results_rendered_check` | `subdomain_results` | `rendered = true ⇒ render_failed_at IS NULL` — "rendered AND impossible" is meaningless (T-259, TZ-fsm §3 FR-5.2; the writer guards — `renderUpdateSQL`/`renderFailMarkerSQL`, `internal/renderworker`) | 000018, NOT VALID | `TestSchema_ResultsRenderedCheck`, `test/integration/render_late_test.go` |
| `contents_refcount_check` | `contents` | `refcount >= 0` — a negative counter breaks the "0 = dead" semantics and masks a double reference write-off (T-265, TZ-fsm §3 FR-11; the writer guards — the `refcount > 0` predicates of the point statements, a `GREATEST` clamp on the bulk ones; the automaton specification — the doc comment of `internal/content/pack.go`) | 000019, NOT VALID | `TestSchema_ContentsRefcountCheck`, `test/integration/content_test.go` |

## Write idempotency

A repeated write of the same result is rejected by the primary keys
(pinned by the `TestSchema_ResultsIdempotency` test):

- `subdomain_results` / `laravel_findings` — the key
  `(zone_id, domain_id, subdomain_id, variant)`; the HTTP stage writes a
  domain's result as a rewrite in one transaction (T-058): `DELETE` of the old
  rows of the domain (including vanished keys — §3.5 "only the last result") with
  `RETURNING content_hash` → −1 of the old content's `refcount` (`SwapRefTx`,
  T-044) → `INSERT` of the new ones in an unnest batch; duplicate delivery is idempotent
  (spec 3.4, 3.5);
- `laravel_domains` — the key `(zone_id, domain_id)`;
- `contents` — the key `hash`: a repeated write of the same content increments
  `refcount` instead of creating a row (spec 5.7);
- `ingest_files` — the unique triple `(zone_id, action, sha256)`
  (migration 000009, T-113): an exact redelivery is not processed
  again (spec 5.1); the same content under a different action (undo) or in
  another zone — a separate journal row.

## rules_version — the rule set version

An `integer` column (migration 000001): `laravel_domains.rules_version` and
`laravel_findings.rules_version` — `NOT NULL` (a row is written only when
rules matched; the set is known); `subdomain_results.rules_version` —
`NULL` until the analyzer has processed the result (a reset to `NULL` on a render
content swap — T-067).

The value — the hash of the `internal/rules` rule set (T-068): FNV-32a
of the canonical representation of the set, cast to a signed `int32`.
Properties: identical sets — an identical hash regardless of the file
splitting and the rule order (in the canonical form the rules are sorted by
`id`); editing `description` and comments does not change the version — it
reflects the detection behavior (`target`/`match`/`regex`/`weight`/version
bounds); a re-analysis after a description edit is not needed. Collisions of 32 bits are
immaterial in practice: the version distinguishes successive revisions of one
set, not all possible sets. On a version change, the background reanalyze task
(spec 5.8, T-074) finds the records with a stale version and
enqueues them into `q:analyze` without re-downloading.

## laravel_domains — the per-domain aggregate of signs (T-072)

A `laravel_domains` row — the aggregate of a domain's confident hits
(`laravel_findings` with `is_laravel`, spec 5.8/§13.1), recomputed
by the analyzer worker in the same transaction as the findings write (details
and decisions — [`analyzer.md`](analyzer.md)):

| Column | Semantics |
|---|---|
| `score` | the maximum across the domain's findings (only the maximum could reach the §13.1 threshold — it equals the maximum across all findings) |
| `ver_min` / `ver_max` | the merged range: `min(ver_min)` / `max(ver_max)` over the numeric version components; a NULL bound of any hit (no bound or a T-070 conflict) propagates to the domain — the union is unbounded on that side |
| `found_on_mask` | the bits of the subdomains with an `is_laravel` hit (variants collapse into a bit); the same bit semantics as `domains.dns_mask` |
| `rules_version` | the version of the set the recompute was made with; during the reanalyze transition period, a domain's findings of mixed versions — the row is overwritten by every delivery |
| `updated_at` | `now()` — the time of the recompute transaction |

A domain without `is_laravel` rows does not enter the table (the §13.1
threshold); an existing row is deleted — the previous signs were not confirmed
in the current episode (§3.5). The recomputes of one domain are serialized by
`pg_advisory_xact_lock(domain_id)` in the worker's transaction (T-072).

## laravel_history — the appeared/lost detection journal (T-118)

`laravel_domains` is rewritten from scratch (§3.5) — the "site arrived at/left
Laravel" trends are kept by the append-only journal of transitions (migration
`000011`, an analog of `laravel_histories` from the old larasites
implementation):

| Column | Semantics |
|---|---|
| `id` | `GENERATED ALWAYS AS IDENTITY` PK — a domain has many transitions over its life |
| `zone_id`, `domain_id` | the domain key (no FK: the partitions of the domain tables hold no references) |
| `appeared` | `true` — the domain's row appeared in `laravel_domains`; `false` — it disappeared |
| `changed_at` | `now()` — the time of the appearance/removal transaction |

The record is written by the same aggregation transaction (`aggregateDomain`,
T-072), but only for a **fresh HTTP check episode** (`queue.Result.Fresh`,
set by the HTTP/render stages when publishing q:analyze): a reanalyze (T-074)
recomputes the same downloaded content under new rules and is not a fact about the
site — a rule change and the flapping of borderline scores around the threshold
(T-116) do not produce false appeared/lost. The semantics are anchored to the
`laravel_domains` row: "left on a reanalyze, returned with a fresh episode"
will leave a single appeared without a paired lost — the price of protection against rule
noise; the trend across a set of domains is correct.

The index — `laravel_history_changed_at_idx (changed_at)`: it covers both
consumer queries — the landing's "findings in 24 h" (T-120) and the daily
dynamics. No partitioning, deliberately: only transitions are written
(not every domain and not every episode); the volume is orders of magnitude
below
`laravel_findings`. The cumulative metrics
`crawler_laravel_appeared_total`/`crawler_laravel_lost_total` are computed
by the monitor as a periodic aggregate (T-118, `internal/monitor/historystats.go`).

## The Go model: internal/model

The `internal/model` package (T-008) mirrors the schema into Go types: the row
structs of all tables (`db` tags = the column names from migration 000001
(+ `domains.temp_attempts` from 000005) in the same
order; NULL — pointers or a nil `[]byte`; `jsonb` — raw JSON `[]byte`),
enumerations and bit types. The package contains no check logic: the
status and state transitions are introduced by the scheduler and the workers (spec 7).

| Type | Column | Values |
|---|---|---|
| `model.Status` | `domains.status` | 0 `new`, 1 `not_delegated`, 2 `delegated_no_address`, 3 `no_http`, 4 `alive`, 5 `temp_error`, 6 `deleted` (spec 6.2), 7 `invalid` (diagnostic: a poison row, the check is impossible — T-109; an extension of §6.2; the recheck does not return such rows); the names and `ParseStatus` for parsing |
| `model.CheckState` | `domains.check_state` | 0 `pending`, 1 `queued`, 2 `done` |
| `model.Flags` | `domains.flags` | bit 0 `is_idn` (spec 5.2 item 9 — the label contains `xn--`), bit 1 `render_pending` (spec 5.6), bit 2 `http_published` — the staging mark "the q:http task of the episode is published, the result is not recorded yet" (T-108: a duplicate delivery of a q:dns batch does not duplicate the publication; cleared by the result write and by the scheduler's enqueueing), bit 3 `dead_host_cutoff` — the last HTTP episode finished on a dead apex+www signature (T-164: the result write sets/clears the bit; the selection for the false-cutoff cross-check — `WHERE flags & 8 <> 0`); the `Set`/`Clear`/`Test` methods |
| `model.DNSMask` | `domains.dns_mask`, `laravel_domains.found_on_mask` | bit № = `subdomains.id`, bits 0..62 (at most 63 subdomains, spec 5.10); `Set`/`Unset`/`Test`/`IDs`/`Count` |
| `int16` | `domains.temp_attempts` | the counter of consecutively assigned `temp_error` rechecks (T-031, migration 000005; spec 5.3: "no more than N times in a row"); the scheduler increments it; the reset — on the final status and on a successful check |
| `model.Variant` | `subdomain_results.variant` | 0 the primary one, 1 the http variant (spec 5.5 item 5) |
| `model.Scheme` | `subdomain_results.scheme` | 1 http, 2 https |
| `model.AliasKind` | `subdomain_results.alias_kind` | 0 none, 1 identical, 2 redirect (spec 5.5 item 6) |
| `model.Action` | `ingest_files.action` | 1 add, 2 delete |

The representations of the binary and jsonb columns of `subdomain_results` are
fixed by the HTTP stage (T-058, `internal/httpworker/record.go`):

- `addrs` — 16 octets per address, IPv4 in the v4-mapped form
  (`::ffff:a.b.c.d`, `netip.Addr.As16`): mixed A/AAAA sets are not
  ambiguous, the length is always a multiple of 16; the address order — the
  `q:http` task's (A first, then AAAA, the DNS stage's answer order);
- `tls`, `redirects` — the JSON of the `httpfetch.TLSInfo` (T-050) and
  `[]httpfetch.RedirectStep` (T-048) structs, snake_case tags;
- `headers` — the canonical header names of the final response
  (`http.Header`: `{"Content-Type":["text/html"], …}`);
- `cookies` — an array of `{name, value, path, domain, expires, max_age,
  secure, http_only, same_site}` (RFC 3339 for `expires`);
- `error` — the check error class (T-053: 1 timeout, 2 redirects, 3 TLS,
  4 body, 5 network; NULL — no error); `rendered = false` until the render
  (T-067); `rules_version` is filled by the analyzer (T-068+);
- `render_failed_at` — the "render impossible" stamp (T-210, migration
  000017): a finalization without a render (a tarpit T-148 / a cert class T-159 /
  a DNS class T-211 / a task class T-172 — the cert class, the DNS class and the
  task class are permanent; for a tarpit the attempt budget is exhausted) marks the
  record until the content changes; the selection of the render catch-up pass
  (`renderPendingRowsSQL`) does not take marked
  records — repeated publications burn deliveries and slot time
  (per the T-207 measurement the tail is ~180–220 thousand records). The value —
  the first finalization (`coalesce`; a repeated stamp does not change the value).
  It is reset by an episode rewrite (the DELETE + INSERT of the result record,
  T-058 — the column is not in the INSERT, DEFAULT NULL: the first attempt
  of a fresh episode is legitimate; a host that recovered will render on the
  next recheck episode of T-030) and by a successful render
  (renderUpdateSQL). NULL — no marker; the migration does not touch existing
  rows. The pair with `rendered` is bound by the CHECK invariant
  `subdomain_results_rendered_check` (T-259, migration 000018 — the
  "CHECK invariants" section); the specification of the row's render-state automaton —
  the doc comment of `internal/renderworker/worker.go` (TZ-fsm §3 FR-5).

The mask bounds are protected in the type itself: bit 63 of a `bigint` is the
sign bit, so `DNSMask.Set`/`Unset` reject numbers outside 0..62 with an error (all
numbers are validated before the change — the mask is never partially changed);
`Test` returns false for an invalid number; `ValidateSubdomainID` — the same
check for `crawlerctl subdomains add`. The zero values of the structs
match the `DEFAULT`s of the columns (`TestDomainZeroValueMatchesDefaults`);
the tags' match to the migration columns is pinned by the test
`TestStructColumnsMatchSchema`.

## The domain_info(text) function — domain search by name (T-082)

The migration `000006_domain_info` (spec 5.11) creates an SQL function for a
direct query from psql — a domain summary as a single `jsonb` value; the
CLI wrapper `crawlerctl domain` (T-081) — over the same tables on the Go side.

```sql
SELECT domain_info('example.com');            -- the summary or NULL
SELECT jsonb_pretty(domain_info('example.com'));
```

A domain or zone not found — `NULL` (not an error): the SQL function reports
the fact; the CLI forms the "friendly message".

### Name handling

- **The SQL minimum of normalization** (spec 5.2 item 1): lowercase, trimming
  edge spaces and dots — `'  Example.COM.  '` finds the domain. The full
  IDN pipeline (inner spaces, punycode, length checks) — the Go side
  (`internal/idn`): names are stored in the DB as punycode; the CLI normalizes
  the unicode input before calling the function.
- **The zone — the last label of the name (the TLD)**: `site.co.uk` → the zone `uk`;
  the composite labels of the domain stay in `label` (`site.co` is normal;
  the TZ_tld_only_zones spec §4, `idn.Normalize`). The split matches
  the ingest pipeline: neither SQL nor Go uses
  external suffix lists. The rule "the longest suffix of the name present
  in `zones`" remains the exact description of the search: the directory is TLDs
  (the canonical source — `crawlerctl zones sync`, an IANA snapshot) plus
  the allowlisted test zones of `ingest.allowed_zone_paths` — such a zone
  (`crawl.test`) is longer than its TLD and captures the names of its scenarios
  (`example.crawl.test` → the zone `crawl.test`).
- The domain is searched by the unique key `(zone_id, label)` through the index
  `domains_zone_id_label_key`, without a full scan (spec 5.11).

### Response composition

| Path | Contents |
|---|---|
| `zone` | `{id, name, name_unicode}` — the domain's zone |
| `domain` | `{id, label, name, status, status_name, check_state, check_state_name, is_idn, render_pending, dns_mask, temp_attempts, queued_at, last_checked_at}` — the `domains` row; `*_name` duplicate the codes with the names from spec 6.2 / `internal/model` (`alive`, `done`, …) so psql can be read without a reference |
| `dns_alive` | `[{id, label}]` — the `dns_mask` decoded via the `subdomains` reference |
| `results` | `[{subdomain_id, subdomain, variant, scheme, scheme_name, http_status, final_url, alias_kind, alias_kind_name, alias, rendered, error, content_hash, checked_at}]` — the results per (subdomain, variant); `alias` — the label of the alias target subdomain; the content is not disclosed; `content_hash` (hex) — the reference to it |
| `laravel` | `{score, ver_min, ver_max, found_on_mask, found_on, rules_version, updated_at, findings}` — the `laravel_domains` aggregate (json `null` when there are no signs); `found_on` — the `found_on_mask` decoded; `findings` — all of the domain's `laravel_findings` hits **by descending score**, including the weak ones below the threshold with the `is_laravel` mark; `signals` are passed as is |

The times (`checked_at`, `updated_at`, `queued_at`) — RFC 3339 strings
with the offset of the session's zone; `last_checked_at` — a date (`date`).

### Security

The function only reads: `STABLE`, `PARALLEL SAFE`, `SECURITY INVOKER`;
it does not modify tables. The body references the tables by unqualified
names, so the `search_path` is **pinned to the function** —
`pg_catalog` + the schema the migration was applied to (an ALTER after CREATE:
the schema name at migration time is unknown — the set is applied both to the working
schema and to the isolated schemas of the integration tests via the `search_path`
of the DSN). The caller cannot swap the tables through their own `search_path`.

The contract is pinned by the integration test `TestSchema_DomainInfoFunction`
(`test/integration/schema_test.go`): the function attributes, the JSON
composition against the test data (including the findings sorting and the mask
decodings), the zone split by the longest suffix, and `NULL` for an unknown
domain/zone. The down migration — `DROP FUNCTION IF EXISTS domain_info(text)`.

The migration `000010_domain_info_invalid_status` (T-109) added to the
`status_name` decoding CASE the branch of code 7 (`invalid` — the diagnostic
status of a poison row) by a full `CREATE OR REPLACE` of the 000006 body: for
codes 0..6 the response is unchanged.

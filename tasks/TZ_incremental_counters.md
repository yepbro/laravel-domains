# Technical Specification: Incremental Counters of the Domain Crawler Pipeline

Version: 1.0 (a supplement to the main spec [`TZ_domain_crawler.md`](./TZ_domain_crawler.md);
prepared based on the analysis of the aggregate queries of 2026-10-24 — the implementation
of the "system alternative" declared by the T-161 statement: *"incremental
zone counters on status transitions (a separate wave)"*)

---

## 1. Purpose and overview

Remove the full passes over the large `domains` table (47M rows, the measurement of §12
of `docs/load-measurements.md`) from the read paths: the pipeline state
counters are maintained by **increments/decrements in the same
transactions as the transitions**, and accuracy is restored by a periodic
**reconciliation** — an authoritative recount by an aggregate. The recount and
verification are managed by the `crawlerctl counters` console command.

### 1.1. Current state (what is being replaced)

| Consumer | Pass | Today |
|---|---|---|
| `internal/status` `zoneStatsSQL` (crawlerctl status, the dashboard §5.1) | `zones LEFT JOIN domains` + `count FILTER ×3 GROUP BY z.id` | a full pass, the dashboard mitigated by the 60 s TTL cache (T-161) |
| `internal/status` `domainStatusSQL` (the dashboard §5.5) | `GROUP BY status` | a full pass, the T-161 cache |
| `internal/status` `laravelTotalsSQL` (the dashboard §5.6) | `count(*)` over `laravel_domains` | a seq scan; the 24 h window — via the T-120 index |
| `internal/scheduler` `backlogSQL` (T-077, the backlog gauge) | `GROUP BY check_state` | 5 min, 17+ s per 47M |
| `internal/monitor` `historyStatsSQL` (T-118) | `count FILTER` over `laravel_history` | 1 h, a seq scan |
| `internal/monitor` `zoneStatsSQL` (T-078/T-120) | `UPDATE zones FROM (aggregate)` | 5 min — remains the basis of the reconciliation |

The §12 measurement: these passes together are the bulk of the postgres CPU (177%).
T-161 already removed the acuteness (intervals, single-flight, the cache); this wave
removes the passes from the read paths altogether.

### 1.2. Target state

1. The readers (the status package, the backlog loop, the dashboard) read only
   the materialized counters: the `zones` reference table (thousands of rows) and
   the `pipeline_counters` table (dozens of rows).
2. The only full `domains` pass in steady state is the reconciliation
   (one combined pass per `monitor.zone_stats_interval`,
   5 min by default).
3. Increment drift (a counter deviating from the aggregate) is an observable
   quantity: a metric + a Warn; the recovery — the reconciliation pass itself.
4. Redis is not used for the counters (the rationale — §13.1).

---

## 2. Terms

| Term | Meaning |
|---|---|
| Spec | The project's main spec (`TZ_domain_crawler.md`), unless stated otherwise |
| This spec | The present specification |
| Increment | `UPDATE … SET v = v ± n` in the transition transaction |
| Reconciliation | an authoritative recount of a counter by an aggregate (`UPDATE … FROM (GROUP BY)`), restores the value wholesale |
| Drift | \|the counter before the reconciliation − the aggregate\| > 0 — a sign of a missed transition or manual SQL bypassing the code |
| Base set | live domains: `status <> deleted` (§13.1 of the main spec) |
| Transition | a change of `domains.check_state` / `domains.status` / `domains.flags` (the render_pending bit) |

---

## 3. Constraints and scope

### 3.1. Scope (what is not included)

1. **The throughput windows remain live queries**: `throughputSQL`
   (`last_checked_at = current_date`, the hourly windows of `subdomain_results`),
   the 24 h `laravelTotalsSQL` FILTER, the landing's `last24hSQL`, the alerts'
   `dnsShareSQL` — the T-171/T-118 window indexes serve them, the cost is proportional
   to the window, not the table. Incrementing the windows would require bucket
   rotation — it does not pay off (§13.7).
2. **`nearStatsSQL` (the gray zone of T-115) is not incremented**: the score
   range is rewritten by reanalyze when the rules change — a finding's membership
   in the range is not preserved across transitions. It remains a periodic
   pass (1 h).
3. The status distribution of `domains.status` is not incremented on all
   transitions (§13.3): it is maintained by the reconciliation within its pass —
   at no extra cost to the existing scan (the same FILTER columns).
4. The point history (Prometheus) does not change: the existing gauges keep
   their names and semantics.

### 3.2. Technical constraints

1. PostgreSQL 16: `RETURNING OLD.*` (introduced in PG 18) is unavailable —
   old row values are obtained by statement predicates and
   `SELECT … FOR UPDATE` (§6).
2. No new dependencies (go.mod is unchanged), no new services.
3. All increments run in the **already existing** transition
   transactions: a transaction failure/rollback rolls back both the transition and the deltas —
   no separate consistency is needed.
4. The migrations are pure SQL, without code calls (the style of 000001–000014).

---

## 4. Counter nomenclature and semantics

### 4.1. The `zones` columns (the live per-zone state distribution)

The existing `domains_total`, `domains_checked`, `domains_laravel`
(T-078/T-120) are supplemented with:

| Column | Semantics (the base set is `status <> deleted`) |
|---|---|
| `domains_pending` | `check_state = pending` — waiting to be selected by the scheduler |
| `domains_queued` | `check_state = queued` — in progress, a lease is active |

`domains_checked` remains the counter of `check_state = done` (no
renaming is introduced — the column is read by the landing, the dashboard, crawlerctl, partitioning).

**Base set unification (§13.4):** all zone columns are counted over
`status <> deleted`, as in the T-078 reconciliation. Today's live
`zoneStatsSQL` counts `pending` without deleted, but `queued`/`done` — without
a status filter (a deleted domain with a hung lease got into `queued`).
After the readers switch to the counters, `queued`/`done` exclude
deleted — unification in favor of the reconciliation semantics: a deleted domain
is neither work nor progress.

### 4.2. The `pipeline_counters` table (global scalars)

| `name` | Semantics |
|---|---|
| `status_<name>` per `model.Statuses` | the number of domains with the given status — **all** rows, including deleted (the legend is complete, as with `FetchDomainStatuses`); written by the reconciliation as the source of truth (§7) |
| `laravel_appeared_total` | appearances in `laravel_history` (cumulative, monotonic) |
| `laravel_lost_total` | disappearances from `laravel_history` (cumulative, monotonic) |
| `render_pending` | domains with the `flags.render_pending` bit (live) — closes the T-175 need for a render-debt counter |

### 4.3. Derived, not stored, quantities

The global pending/queued/done = `sum(zones.domains_pending/…)` over all
zones; `laravel_total` = `sum(zones.domains_laravel)` (this is how the landing
already reads it). No separate `check_state_*` rows are created in
`pipeline_counters` — fewer rows and fewer increments.

---

## 5. Schema (the counters migration)

```sql
ALTER TABLE zones
    ADD COLUMN domains_pending bigint NOT NULL DEFAULT 0,
    ADD COLUMN domains_queued  bigint NOT NULL DEFAULT 0;

CREATE TABLE pipeline_counters (
    name       text PRIMARY KEY,
    value      bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL
);
```

The COMMENT on the columns/table — per the 000012 pattern (the record's owner is
the transition increments + the monitor reconciliation, the readers are read-only).

**Population at migration time** — the 000012 precedent (`domains_laravel`):
`DEFAULT 0` + the first reconciliation pass right at monitor startup
(the loop's startup pass is already built that way). Between the migration and the first
pass (seconds — monitor starts together with the stack) readers see
zeros; the `pipeline_counters` rows (`status_*`, the cumulatives, render_pending)
are created by the first reconciliation with an upsert. No separate backfill SQL is written in the migration
(duplicating the aggregate text between the migration and the code is a source of discrepancies).

The migration `down` — DROP COLUMN ×2 + DROP TABLE.

---

## 6. Increments on transitions

### 6.1. Principles

1. The delta runs in **the same transaction** as the transition — as a second
   statement/CTE. Batch transitions update the zones with a single `UPDATE zones … FROM (VALUES …)` statement / CTE — the style of `db.Update` (T-007),
   `crawlerctl manage` (max(id)+1), `recheck`.
2. **The old state is obtained in one of the ways** (PG 16 without
   `RETURNING OLD`, §3.2):
   - **the statement predicate pins** the old state (`WHERE
     check_state = …`) — the delta is computed from the number of moved rows
     (`RETURNING zone_id` → GROUP BY);
   - **the statement does not change the column** (`apply_delete` does not touch
     `check_state`) — `RETURNING` returns the current (= old) value;
   - **neither** (`httpworker/record.go`) — a point
     `SELECT status, check_state … FOR UPDATE` before the UPDATE (§6.4).
3. The increment runs only when the corresponding statement reports `RowsAffected > 0`
   (duplicate delivery, a concurrently cleared bit — no delta).
4. Clamping to the DB values (`greatest(0, …)`) is not applied:
   a negative value is a visible drift, fixed by the reconciliation, not
   masked.

### 6.2. Scheduler and ingest batch transitions

| Transition | Owner | Zone deltas (per `RETURNING zone_id` GROUP BY) |
|---|---|---|
| pending → queued (a selection batch) | `scheduler/enqueue.go` (T-028) | `pending −n`, `queued +n`; the guard `check_state = pending AND status <> deleted` — a race with the delete file (T-019) does not produce a double `pending −n` (added by T-178) |
| queued → pending (an expired lease) | `scheduler/lease.go` (T-029) | `queued −n`, `pending +n` |
| done → pending (an age-based recheck) | `scheduler/recheck.go` (T-030) | `checked −n`, `pending +n` |
| done → pending (temp_error, retry) | `scheduler/retry.go` (T-031) | `checked −n`, `pending +n` |
| done → pending (forced, CLI) | `cmd/crawlerctl/recheck.go` | a `FOR UPDATE` pre-read: `checked/queued −n`, `pending +n` |
| done → pending (a new subdomain, CLI) | `cmd/crawlerctl/manage.go` (T-083) | `checked −n`, `pending +n` (added by T-178: moving batches of done → pending) |
| queued/pending → done (a DNS result record) | `dnsworker/result.go` (T-039) | a `FOR UPDATE` pre-read in the CTE: `queued −n`/`pending −n`, `checked +n` — the pipeline's main transition (added by T-178: §6.4 covered only the HTTP result record, which lands on top of done already) |
| INSERT of new rows (status=new, pending) | `ingest/apply_add.go` | `total +n`, `pending +n` |
| deleted → new/pending (revive) | `ingest/apply_add.go` | `total +n`, `pending +n` (the domain was outside the base set — no decrements) |
| → deleted (per the delete file) | `ingest/apply_delete.go` | `total −n`; per `RETURNING check_state` (not changed by the statement): `pending −n` / `queued −n` / `checked −n` |

### 6.3. Render debt (`render_pending`, synergy with T-175)

| Transition | Owner | Mechanics |
|---|---|---|
| bit set (the gate closed, T-066/T-174) | `httpworker/renderq.go` `renderSetPendingSQL` | add the guard `AND (flags & 2) = 0` (today the WHERE is by PK only) — `RowsAffected` distinguishes "set" from "already set"; the `render_pending +1` delta only on 1 |
| bit clear (publication) | `renderq.go` `renderClearPendingSQL` | the bit guard is already there — `RowsAffected = 1` → `render_pending −1` |

The guard in `renderSetPendingSQL` changes the observable behavior only for
a concurrent duplicate set (a second instance of the pass) — today the bit
is unconditionally rewritten with the same value, the flag semantics do not change;
the bit invariant is fixed by a test (§12).

### 6.4. `httpworker/record.go` (queued → done + status)

The domain result recording transaction: before `recordDomainSQL` — a point
`SELECT status, check_state FROM domains WHERE zone_id = $1 AND id = $2
FOR UPDATE` (a PK lookup; the row lock takes over the role of "an UPDATE as the
first step locks the row" — the statement comment). Then the UPDATE and
the deltas per the actual old values:

- `check_state`: old ≠ done → `queued/…−1`, `checked +1`; old =
  done (duplicate delivery) → no delta (the first pass already moved
  the domain out of queued);
- status: **not incremented** (§13.3) — the status distribution is
  written by the reconciliation.

The cost: one extra PK read per domain (the completion rate is
tens per second per the §12 measurements) against the delta accuracy — accepted.

### 6.5. Analyzer

| Event | Owner | Deltas |
|---|---|---|
| a `laravel_history` row (appeared/lost) | `analyzer` | `laravel_appeared_total +1` / `laravel_lost_total +1` (an upsert into `pipeline_counters`) |
| a `laravel_domains` row appeared/disappeared | `analyzer` (the §13.1 threshold) | `zones.domains_laravel ±1` |

The analyzer is the only writer of both tables; the increments preserve
the zero staleness of the "latest findings" and the cumulatives (T-118) and remove
the hourly `historyStatsSQL` seq scan (§8.4).

---

## 7. Reconciliation

### 7.1. The pass

`monitor.ZoneStatsOnce` is extended into one consistent pass
(the same transaction, the same snapshot):

1. `zones`: `domains_total`, `domains_pending`, `domains_queued`,
   `domains_checked` — one FILTER aggregate over `domains`
   (`WHERE status <> deleted GROUP BY zone_id`, the partition key —
   partitionwise aggregate as today), `domains_laravel` — an aggregate
   over `laravel_domains` (the existing T-120);
2. `pipeline_counters`: `status_<name>` — FILTER over all `model.Statuses`
   by the same `domains` scan (the extra cost — zero: the status column
   is already read by the pass); `laravel_appeared_total`/`laravel_lost_total` —
   an aggregate over `laravel_history` (the existing T-118 text); `render_pending`
   — a `count FILTER` over the `flags` bit by the same `domains` scan.
3. **Drift**: the old values are read before the overwrite; `|old − new|`
   of each position → the §11 gauge; a Warn on a non-zero drift of any position
   (after the loop's startup pass — the first pass after the migration does not count
   as drift, it fills the zeros of §5).

The reconciliation remains **authoritative**: the value is overwritten wholesale,
the increments provide the freshness between passes. The period is `monitor.
zone_stats_interval` (5 min); after the wave's break-in the interval may be
stretched to 15–30 min (an operator decision based on the drift measurement — drift in
steady state must be zero).

### 7.2. The loop

`ZoneStatsLoop` keeps the contract (the T-161 single-flight, the startup pass,
the duration/skipped-tick metrics); the `crawler_zonestats_pass_seconds` metric is renamed to `crawler_counters_recount_seconds` (the help — the combined
pass) with the buckets kept.

---

## 8. Switching the readers

1. `status.zoneStatsSQL` → `SELECT … domains_total, domains_pending,
   domains_queued, domains_checked, domains_laravel FROM zones ORDER BY
   name` — without the `domains` JOIN. `FetchZoneStats`/`ZoneStats` — without field
   changes (the pending semantics — §4.1).
2. `status.domainStatusSQL` → reads the `pipeline_counters` `status_*`
   (the full `model.Statuses` legend with zeros — as today).
3. `status.laravelTotalsSQL`: `Total` → `sum(zones.domains_laravel)`
   (the landing already reads it this way); the 24 h FILTER remains a live window over the
   T-120 index.
4. `scheduler.BacklogOnce` → `SELECT coalesce(sum(domains_pending), 0),
   coalesce(sum(domains_queued), 0) FROM zones` — one cheap pass over the
   reference table; the full `backlogSQL` pass is removed (the
   `crawler_backlog_domains` gauge, semantics and help — unchanged).
5. `monitor.HistoryStatsLoop` → reads `pipeline_counters` (the deltas are written by
   the analyzer increments + the reconciliation); its own seq scan
   `historyStatsSQL` is removed, the T-118 gauges are kept.
6. The dashboard TTL cache `status.Cache` (T-161) is kept — now it
   caches cheap reads (protection against traffic between refresh cycles),
   not full passes.
7. The landing (`internal/web/data.go`) — unchanged: it has been reading the zone
   counters since T-122.
8. The dispatcher selection (`scheduler.dispatchZonesSQL`, T-183): the filter
   `AND domains_pending > 0` — the column becomes the round-1 criterion
   (zones without pending are excluded before the probe: its semantics is the predicate
   LATERAL selection). The freshness — the transition deltas (enqueue decrements
   in the same statement, `apply_add` opens a new zone immediately); the
   trust boundary — the §4.1 drift (a hidden zone with work until the nearest
   reconciliation, observable via the §11 metric).

---

## 9. The `crawlerctl counters` console command

Implemented per the `cmd/crawlerctl/near.go` pattern (config.Load → pool,
`AppName "crawlerctl"`, `signalCtx`). Subcommands:

```
crawlerctl counters list
    the values of all counters (zones — as totals, pipeline_counters — row by row)
    + the age (zones/counters updated_at); the format — a CLI table.

crawlerctl counters recount [--zone NAME]
    one reconciliation pass (the same monitor Once functions — the SQL in a single
    point of truth, the TZ_dashboard.md §4.5 style); a "before → after" output for the positions with
    a non-zero drift; --zone — only the selected zone (the global
    counters are recounted always).

crawlerctl counters verify [--zone NAME] [--tolerance N]
    a check of the counters against the live aggregates WITHOUT writing: a table of
    discrepancies; exit code 0 — no discrepancies (within N,
    0 by default), 1 — there is drift, 2 — a usage error.
```

Purpose: manual recovery after incidents, a cron recount for
small corpora without a permanent monitor, post-operation verification
(migrations, a restore from backup). The command appears in the top-level
help (`usageText`) and the README.

---

## 10. Configuration

No new fields: the reconciliation period is the existing
`monitor.zone_stats_interval`; the CLI adds no configuration (the DSN from
the shared config). `config.example.yaml` — update the comment of the
monitor section (the reconciliation is combined, §7).

---

## 11. Metrics

| Metric | Type | Meaning |
|---|---|---|
| `crawler_counters_drift{counter}` | gauge | \|old − new\| on the reconciliation pass; the counter label — the counter name (`zone:<id>:pending`, `global:status_alive`, …); steady state — 0 |
| `crawler_counters_recount_seconds` | histogram | the duration of the combined pass (a renaming of `crawler_zonestats_pass_seconds`, the buckets unchanged) |
| `crawler_zonestats_skipped_ticks_total` | counter | unchanged (T-161) |
| `crawler_backlog_domains`, `crawler_laravel_appeared_total`, `crawler_laravel_lost_total`, `crawler_laravel_near_findings` | gauge | names/semantics unchanged — the source is now the counters (§8) |

The drift alert: a Warn to the monitor log on a non-zero drift (after the startup
pass); it needs no Telegram delivery (drift is diagnostics, the cure is the
same pass); a persistent non-zero drift ≥ 3 passes in a row is
a candidate for the T-162 alerts based on operating experience.

---

## 12. Testing and acceptance

1. **Unit tests** (SQL contracts in the project style — exact statement
   texts on a fake `db.DB`):
   - the deltas of each transition (§6.2–6.5): parameters, the batch form
     `UPDATE … FROM (VALUES …)`, zero deltas at `RowsAffected = 0`;
   - the reconciliation: the combined aggregate text, the counters upsert, the drift
     computation, the Warn threshold;
   - the readers: the new texts without `JOIN domains`/`GROUP BY status`;
   - the CLI: help, the verify exit codes (0/1/2), the list/recount output;
   - the `renderSetPendingSQL` bit guard (the §6.3 invariant).
2. **Integration tests** (`test/integration`, skipUnlessIntegration):
   after the existing pipeline episodes (ingest → dispatch → dns →
   http → analyzer) verify the zone counters/counters against the live aggregates
   (the ready-made `verify` logic); a duplicate-delivery record scenario (a repeated
   episode of one domain) — the deltas are not doubled; a scenario of deleting a domain in
   queued — the decrement of the correct column.
3. **Acceptance**: `make test` + `make test-integration` green; in
   steady state (a testbed run ≥ 1 h) `pg_stat_activity` shows no
   full `domains` passes except the reconciliation once per interval;
   `crawlerctl counters verify` — code 0; the dashboard numbers match
   `crawlerctl status`; postgres CPU (top) below the §12 measurement by at least
   the size of the removed passes (backlog 17 s/5 min + the status aggregates);
   after an artificial drift (a manual UPDATE zones) `verify` — code 1,
   `recount` fixes it, the drift metric fires.

---

## 13. Accepted decisions and rejected options

1. **PostgreSQL, not Redis.** Redis in this project is an ephemeral transport:
   the streams are trimmed with MAXLEN, losses on failure are restored by the scheduler
   from the DB (`scheduler/backlog.go`, spec 3.4). A counter in Redis is a second point
   of truth: a flush cycle, at-least-once deduplication, a rebuild after a loss —
   machinery more complex than the drift it cures. All transitions already
   happen in PG transactions — a delta in the same transaction is atomic
   with no new delivery semantics. Redis stays with the operational gauges
   (the stream depths).
2. **Reconciliation, not triggers / materialized views.** There are no triggers in
   the migrations — the project style: explicit SQL with contracts in tests;
   a trigger on the 47M-row table is a hidden cost of every write and cannot be
   beaten by unit tests on fakes. An MV: `REFRESH` is the same full pass plus
   locks; it provides no incrementality.
3. **The status distribution — by reconciliation, not increments.** Batch
   `UPDATE … RETURNING` in PG 16 does not return the old values (no
   `RETURNING OLD`), the old statuses of the apply_delete/revive transitions are not
   pinned by predicates. The extra cost of the status FILTER columns in
   the reconciliation scan is zero; a 5 min freshness for the status
   distribution (the dashboard's gauge section) is enough — today it is already
   cached for a minute by T-161.
4. **Base set unification** (§4.1): the discrepancy between the live
   `zoneStatsSQL` (queued/done without a status filter) and the reconciliation
   (all — `status <> deleted`) is resolved in favor of the reconciliation.
5. **`SELECT … FOR UPDATE` in record.go** (§6.4): one extra
   point PK read per domain buys exact deltas (duplicate delivery,
   a lease returned mid-processing) — cheaper than curing drift.
6. **The `render_pending` counter on the bit transitions** closes the
   "by cost" source from T-175 (the "incremental counter" option): the
   `crawler_render_pending_domains` gauge reads `pipeline_counters`, a partial
   index over the bit is not needed.
7. **Throughput windows are not incremented**: the T-171/T-118 indexes keep the cost
   ∝ the window; bucket rotation for calendar windows is complexity without
   measurable benefit (if needed — a separate wave).
   *Partially revised in T-179 (TZ_slow_done_today, a live measurement of
   24.10)*: the thesis "the cost ∝ the window, the indexes serve it" was not confirmed
   for the daily window — the index-only scan over `last_checked_at =
   current_date` degraded with heap fetches of the fundamentally "dirty"
   fresh pages (676K per 815K rows of the window, ≈1.1 GB of reads,
   a median of 15 s with the dashboard pool's parallel gather disabled):
   done_today was moved to an incremental daily counter
   `checks:YYYY-MM-DD` (a `crawler_counters` row, an increment in
   the result recording transaction, the reconciliation — a FILTER position of that
   pass). The hourly windows (`results_hour`/`rendered_hour`, 0.07 ms)
   remained live — it is the calendar daily window that is expensive.
   *Revised further in T-203 (the stage 27 checkpoint)*: the hourly
   windows `results_hour`/`rendered_hour` (`checked_at ≥ now()−1h` over
   `subdomain_results`) degraded by the same mechanism at the grown
   pipeline rate (a median of 6.0 s > 3 s, 108 events) — they were moved to
   hourly buckets `results:`/`rendered:YYYY-MM-DD"T"HH24` (increments
   in the result recording and render transactions, the reconciliation —
   FILTER positions of the two live hours' window aggregate, the reader sums the
   prev+cur buckets with a window-length divisor).
8. **The `status.Cache` cache is kept** (§8.6): cheap reads are cached too — dashboard traffic between refresh cycles does not hit the DB
   at all.

---

## 14. Staging within the task

One task commit covers the whole spec; the order within (each step —
green tests):

1. The counters migration (§5; the number/file name — the next free one at
   implementation time, decided by the implementer) + extending `ZoneStatsOnce`/Loop into
   the reconciliation (§7) with a drift metric — the readers are unchanged, the behavior is
   compatible (the columns are written, nobody reads them).
2. The transition increments (§6) — scheduler, ingest, httpworker (record,
   renderq), analyzer.
3. Switching the readers (§8) + removing the replaced passes
   (`backlogSQL`, `historyStatsSQL`, the old `status` texts).
4. The CLI `crawlerctl counters` (§9), the config comment (§10).
5. Documentation: `docs/services.md` (the metric list, the backlog source),
   `docs/schema.md` (the zone columns, pipeline_counters), `docs/runbooks.md`
   (recount/verify after incidents), README (the CLI command).

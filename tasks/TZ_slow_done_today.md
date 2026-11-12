# Specification: the done_today daily check counter

Version: 1.1 (based on the analysis of the production slow-query log of
2026-10-24 — the first log entry; the log:
`log_min_duration_statement = 3000`, T-177). A supplement to
[`TZ_incremental_counters.md`](./TZ_incremental_counters.md) (the
incremental counters spec) — a revision of its §13.7 ("throughput
windows are not incremented") based on a live measurement: the premise
"the cost is proportional to the window, the indexes take care of it"
was not confirmed for the daily window.

Version 1.1: storage moved from a separate `domain_checks_daily` table
to the landed `crawler_counters` table (T-175, migration 000015, commit
708aa81 — "future counters of this class do not spawn new tables"); no
separate migration is needed at all.

---

## 1. Current state and source

The `throughputSQL` query (`internal/status/status.go:81`), the
throughput section of the dashboard (dashboard spec §5.2):
`done_today` is a scalar subquery

```sql
(SELECT count(*) FROM domains WHERE last_checked_at = current_date) AS done_today
```

Called by `status.Cache.Throughput` (TTL 60 s, T-161) from the dashboard
snapshot loop (T-129) — in practice ~once per minute — and by
`crawlerctl status`.

Production statistics (the log of Oct 24, 09:39–12:20 UTC): 96 triggers
of the 3 s threshold; min 5.7 s / median 15.2 s / max 51.2 s.

## 2. Diagnosis (EXPLAIN ANALYZE on the production DB)

The plan is **correct** — Index Only Scan
`domains_default_last_checked_at_idx` (migration 0014, T-171),
`Index Cond: last_checked_at = CURRENT_DATE`. The slowness has two
multipliers:

1. **Heap Fetches = 676,230 over the 815k rows of the window (83%)**.
   An index-only scan requires heap-visibility checks for rows whose
   pages lack the all-visible bit. The `current_date` window consists
   of rows updated today — their pages are "dirty" in principle until
   a vacuum that always lags behind them. 148k buffers (≈1.1 GB) per
   execution.
2. **The cost grows toward the evening**: the window accumulates the
   checks of the day (815k by 12:21 UTC; with a live pipeline
   ≈1.5–2 million by the end of the day) — along with the number of
   heap fetches. The median of 15 s is the serial execution of what
   took 2.8 s in EXPLAIN with 2 parallel workers: the dashboard pool
   runs with `max_parallel_workers_per_gather = 0` (the
   `db.NoParallelGather` hook, T-171 — deliberate, so that aggregates
   do not steal cores from the pipeline).

The hourly windows of the same query (`results_hour`/`rendered_hour`
over `subdomain_results`) — 0.07 ms, healthy: the window is small and
the `subdomain_results_checked_idx` index works as designed.

Conclusion: a daily window over live rows is structurally expensive —
row freshness and cheap counting conflict. A counter with increments
and reconciliation breaks the conflict (the same scheme as T-175 for
render_pending and as the incremental counters spec for the pipeline
states).

## 3. Target state

1. `done_today` is read from `crawler_counters` — a point lookup by PK
   (microseconds); the full `domains` pass disappears from the read
   path.
2. The counter is incremented in the same transactions as the check
   result recording (`httpworker/record.go`) — the delta is free.
3. The authority is the reconciliation: a recompute by the live
   aggregate; where possible — a FILTER position of the combined pass
   of the incremental counters spec (T-178: the `domains` rows are
   scanned in full anyway; `last_checked_at = current_date` is added
   to the existing FILTER columns — the extra cost is zero).
4. `elapsed_sec` (the `now()` scalar), `results_hour`, `rendered_hour`
   remain live subqueries — they are cheap (§2).
   *Revised in T-203 (the stage 27 checkpoint)*: at the grown pipeline
   rate the hourly subqueries degraded with the same heap-fetch
   mechanism (median 6.0 s > 3 s, 108 events) — they were moved to the
   hourly buckets `results:`/`rendered:YYYY-MM-DD"T"HH24`
   (increments in the result-recording and render transactions;
   the reconciliation — FILTER positions of the aggregate over the
   window of the two live hours; the reader sums the prev+cur buckets
   with the window length as the divisor).

## 4. Storage: a row in crawler_counters, no migration

The daily counter is a `crawler_counters` row (the T-175 table,
migration 000015, in production after the 708aa81 deploy — as of
Oct 24 prod is on migration 14):

| `name` | `value` |
|---|---|
| `checks:YYYY-MM-DD` (the current day, computed by the DB: `'checks:' \|\| to_char(current_date, 'YYYY-MM-DD')`) | the number of result records since the start of the day |

- The same key pattern as T-175 (`render_pending`), within its design
  decision "a general-purpose counter table — new tables are not
  spawned"; **no new migration** — no DDL required.
- Day rotation creates no work: at 00:00 the increment creates the new
  day's row, yesterday's stays as history (365 rows/year — negligible;
  the potential consumer, daily throughput graphs, is out of scope).
- The day name is produced by the DB (`current_date`) — the timezone
  is consistent with the `now()` of the `last_checked_at` record
  (T-058).
- Until the first record of the day there is no row — the reader
  applies `coalesce(…, 0)` (§7): idle pipeline hours show an honest
  zero.
- Dependency: implementation after the deploys of T-175 (000015) and
  T-178 (000016 — the `crawler_counters.updated_at` column, written by
  the increment and the reconciliation); before them only a backfill
  could create the row — there is none (§10.4).
- A continuation in T-182 (TZ_slow_dns_share): a second daily row
  `temp_error:YYYY-MM-DD` lives alongside — the numerator of the
  temp_error share of the T-080 alert (the denominator — the
  `checks:` row of this spec). The increment — the same
  result-recording transaction, only for temp_error outcomes;
  the reconciliation — a FILTER column `status = temp_error` of the
  same window.

## 5. Increments

The owner is `httpworker/record.go`, the domain result-recording
transaction: after `recordDomainSQL`, as the second statement

```sql
INSERT INTO crawler_counters (name, value)
VALUES ('checks:' || to_char(current_date, 'YYYY-MM-DD'), $1)
ON CONFLICT (name) DO UPDATE
    SET value = crawler_counters.value + EXCLUDED.value
```

where `$1` is the `RowsAffected` of the `recordDomainSQL` batch (not
the number of domains in the delivery batch: a duplicate delivery with
`RowsAffected = 0` yields no delta — incremental counters spec
§6.1.3).

### 5.1. Semantics of the divergence from the live count (accepted)

The live `count(*)` counts domains whose latest check is today:
a re-check of the same domain within the same day (a temp_error retry)
does not grow the number. The increment counts record events — a retry
adds +1. A clarification from the actual scheme (the T-179 review):
q:http duplicate deliveries are record events too (+1 each: there is
no `check_state <> done` guard in `recordDomainSQL` and there must not
be one — a repeat must republish to q:analyze); besides, the
`last_checked_at` window includes DNS-terminal completions
(not_delegated etc.) that have no HTTP episode — the increments do not
count them. The total divergence is the difference between the "HTTP
record events" and the "domains of the day across all branches" flows;
it is restored by the reconciliation (§6); for a "work rate" gauge
with 5 min freshness the difference is insignificant (the share of
DNS-only completions and of repeats within one window is small against
the accumulated daily value). Invariant: the reconciliation converges
to the live aggregate; the drift is an observable metric (§8).

## 6. Reconciliation

A periodic monitor pass: the `value` of the current day's row :=
the live aggregate `count(*) … WHERE last_checked_at = current_date`;
before the overwrite the previous value is read, `|old − new|` is the
drift (§8). If there is no row before the overwrite — insert with the
aggregate value.

Placement:

- with T-178 implemented (the combined `ZoneStatsOnce` pass) — a
  FILTER position of the same `domains` scan (zero cost, the
  `zone_stats_interval` period);
- before T-178 — a temporary hourly monitor pass with the same
  semantics (its own loop following the `renderdebt.go` pattern of
  T-175; removed at the T-178 deploy). An hour of freshness is enough
  for the throughput gauge: between the passes the increments maintain
  the accuracy.

## 7. Readers

`throughputSQL` (`internal/status/status.go`): the `done_today`
subquery is replaced with

```sql
(SELECT coalesce(value, 0) FROM crawler_counters
  WHERE name = 'checks:' || to_char(current_date, 'YYYY-MM-DD')) AS done_today
```

(`coalesce` — NULL safety until the first record of the day, §4).
`status.Cache.Throughput` (TTL 60 s) is unchanged — it caches the
cheap read (incremental counters spec §8.6). `crawlerctl status` gets
the same read with no edits further up the stack. Update the SQL-text
tests (`status_test.go`).

## 8. Metrics and observability

- The dashboard gauge `throughput.done_today` — the name/semantics
  unchanged (now "result-record events over the day" — §5.1; the
  divergence is documented here).
- The daily counter drift — a metric following the T-175 pattern (its
  `crawler_counters` desync alert): Warn on non-zero drift after the
  starting pass; with T-178 implemented — a `crawler_counters_drift`
  position (the `daily:checks` label, incremental counters spec §11).

## 9. Testing and acceptance

1. Unit: the text of the new `throughputSQL` (the exact contract);
   the increment — parameters/`ON CONFLICT`/`RowsAffected = 0` with no
   delta; the reconciliation — the day-row upsert and the drift
   calculation (in the style of incremental counters spec §12.1,
   T-175).
2. Integration (`test/integration`): after a pipeline episode
   `crawler_counters.checks:<day>` equals the number of recorded
   results; a repeat episode of a single domain (a duplicate delivery)
   — the delta does not double; a day change is emulated with a fixed
   date — the new day's row.
3. Acceptance: in the production server log (the 3 s threshold,
   T-177) the `done_today` triggers disappear on days with an active
   pipeline; the `EXPLAIN` of the new `throughputSQL` — point plans
   without a `domains` scan; `crawlerctl status` shows a similar
   throughput before/after.

## 10. Accepted decisions and rejected alternatives

1. **An incremental counter instead of the live window — a revision of
   incremental counters spec §13.7** based on the §2 measurement:
   "it does not pay off" relied on the cost ∝ window; the measurement
   showed a heap-fetch constant (83% of the window, ≈1.1 GB of reads,
   a 15 s median with the pool parallelism disabled) that changes the
   balance.
2. **Daily granularity, not hourly**: the hourly windows remained
   cheap (0.07 ms) — we leave them alone; the calendar day window is
   what is expensive.
3. **Storage — a row in `crawler_counters` (T-175), not a new table**:
   the landed precedent 708aa81 explicitly positions the table as
   general-purpose ("future counters of this class do not spawn new
   tables"); the separate `domain_checks_daily` table of version 1.0
   is dropped — and the migration disappears with it (the next free
   migration number stays with the incremental counters spec/T-178,
   whose §5 does not fix numbers).
4. **Per-day history is preserved** (a row per day): free; the
   potential consumer — daily throughput graphs; no cleanup required
   (365 rows/year).
5. Rejected **"just raise the TTL"**: it reduces the frequency, not
   the cost; the dashboard throughput loses its freshness needlessly.
6. Rejected an **aggressive vacuum for the sake of all-visible**: it
   helps only the tail of the window — today's rows were updated, so
   their pages are "dirty" by definition; a vacuum always lags behind
   fresh rows.
7. Rejected **giving parallel gather back to the dashboard pool**:
   that is a rollback of T-171 — aggregates would steal cores from
   the pipeline again; the counter solves it without CPU.
8. Rejected **backfilling the first row**: until the first check of
   the day there is an honest zero (`coalesce`, §7); the T-175
   precedent with a migration backfill is not needed here —
   retrospective "today" checks cannot appear before the deploy
   (the increment starts with the first record).

## 11. Staging

1. The `record.go` increment (§5) — works after the T-175 deploy
   (000015).
2. The `throughputSQL` reader (§7), the cache unchanged, the text
   tests.
3. Reconciliation: a temporary hourly monitor pass (§6) with the drift
   metric (§8); at the T-178 deploy — moved as a FILTER position into
   its combined pass, the temporary loop is removed.
4. Documentation: `docs/schema.md` (the counter row),
   `docs/services.md` (the source of the throughput gauge), a note to
   incremental counters spec §13.7 about the revision.

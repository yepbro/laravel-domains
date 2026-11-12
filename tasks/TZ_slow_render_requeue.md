# Specification: render debt drain without PK ordering — seq scan batch selection

Version: 1.0 (based on the analysis of the production slow query log of
2026-10-24 — query #2 of the log, the most frequent and the heaviest;
log: `log_min_duration_statement = 3000`, T-177).

---

## 1. Current state and source

The `renderPendingDomainsSQL` query (`internal/httpworker/renderq.go:145`):

```sql
SELECT d.zone_id, d.id, d.label || '.' || z.name
FROM domains AS d JOIN zones AS z ON z.id = d.zone_id
WHERE (d.flags & 2) <> 0 AND d.status <> 6
ORDER BY d.zone_id, d.id LIMIT $1
```

Called by `renderSelectPending` ← `RequeueOnce` ← the
`RequeueRenderPending` loop (`renderq.go:608`; http-worker, 60 s tick,
batch 100): the render debt drain pump of T-174/T-066. `drain()` runs
`RequeueOnce` back to back while the batch is full — with a debt of
344.5K domains it is always full (only the q:render watermark stops
it), i.e. the SELECT runs every 60 s or more often.

Production statistics (log of Oct 24 09:39–12:21 UTC): **137 threshold
hits** at 3 s (the most frequent query of the log), median **44 s**,
max **143 s** (grows over time) — ≈100 minutes of pure scanning over
2.7 hours.

## 2. Diagnosis (EXPLAIN ANALYZE on the production DB, Oct 24)

| Variant | Plan | Time | Rows Removed by Filter | Buffers |
|---|---|---|---|---|
| as in production (ORDER BY) | Index Scan `domains_default_pkey` + Filter | **30,314 ms** | 14,087,147 | 582K (≈4.5 GB) |
| without ORDER BY | the same Index Scan over the PK | 15,992 ms* | 14,088,668 | 582K |
| seq scan (SET LOCAL, diagnostics) | **Seq Scan + Hash Join** | **43 ms** | **35,193** | 1,078 (≈8 MB) |

\* the difference from production is the page cache, **the plan is the
same**.

1. **Removing ORDER BY alone does not speed anything up**: even
   without it the planner picks the Index Scan over the PK. The reason
   — PostgreSQL has no statistics for the `(flags & 2) <> 0` expression
   (the index over `flags` was rejected by T-175 — the HOT invariant),
   the selectivity estimate is `rows=41,665,983` out of 41.6M — "the
   filter cuts out almost nothing", so the index scan with LIMIT looks
   nearly free to it. In reality the first 100 flagged domains sit
   behind 14.1M rows of the head of the PK order.
2. **The "treadmill"**: the query has no cursor — every call starts
   from (0,0) and tramples through the already drained "empty" prefix
   of the PK order; with every drain the prefix gets longer (median
   44 s → max 143 s).
3. **Physical heap order ≠ PK order**: the `.рф` domains (the bulk of
   the render debt) were loaded among the first (ingest ru/rf/su),
   HOT updates keep the versions on their home pages — the flagged
   rows sit at the start of the heap. A seq scan finds 100 of them
   within ~36K row visits (43 ms), the LIMIT early stop lands right
   after the first matches found.

## 3. Target state

1. Batch selection — a seq scan with early stop: **tens of ms** instead
   of tens of seconds; the debt drain stops being the DB bottleneck
   (the pace is set by the q:render gates of T-174, not the SELECT).
2. A steady state without debt (render_pending = 0) — the heavy SELECT
   does not run at all: a spot check of the T-175 counter (guard).
3. The `domains` HOT invariant is untouched (no indexes over `flags`,
   and none will appear); the T-175 render debt counter is the single
   source of truth on whether debt exists.

## 4. Changes

### 4.1. SQL without ORDER BY

```sql
SELECT d.zone_id, d.id, d.label || '.' || z.name
FROM domains AS d JOIN zones AS z ON z.id = d.zone_id
WHERE (d.flags & 2) <> 0 AND d.status <> 6
LIMIT $1
```

The batch order does not matter (the operator's decision of
2026-10-24: "it makes no difference which domains go first"): clearing
the flag after publishing rules out repeats; batch determinism is not
needed for correctness. Update the variable comment (`ORDER BY by
PK — deterministic batches`).

### 4.2. Transaction with SET LOCAL (plan pinning)

`renderSelectPending` runs the selection inside an explicit
transaction (`pool.Begin`, sample `renderClearPendingTx`; the
signature takes `RecordDB` instead of `db.DB`):

```sql
BEGIN;
SET LOCAL enable_indexscan = off;
SET LOCAL enable_bitmapscan = off;
SELECT …;   -- 4.1
COMMIT;
```

- `SET LOCAL` applies only inside this transaction — the pool's other
  queries are unaffected; PostgreSQL has no hints, this is the standard
  way to pin the plan shape of a single statement.
- Disabling the bitmap scan together with the index scan — the plan
  shape is exactly the one verified by the §2 diagnostics (Seq Scan +
  Hash Join; a bitmap over the unindexed `flags` is unavailable anyway
  — pinned in case of future indexes).
- The edit following the T-181 code review (it closes the same goal,
  "all index access paths to domains are closed"): `SET LOCAL
  enable_indexonlyscan = off` added as well — a separate GUC; an
  index-only over the PK would reproduce the prefix pathology.
- The edit is a single spot (`renderSelectPending`); the `RequeueOnce`
  calls are unchanged.

### 4.3. Guard by the render debt counter (T-175)

Before the selection (in `RequeueOnce`, after the `HasCapacity` check)
— a spot read:

```sql
SELECT value FROM crawler_counters WHERE name = 'render_pending'
```

- `value = 0` → an empty batch without the heavy SELECT
  (`Selected = 0` — `drain()` stops correctly); the steady state
  without debt costs one row read per tick.
- `value > 0` → the §4.1 selection.
- A missing row is a pass error (the row is guaranteed by migration
  000015 of T-175; the `renderBumpDebtCounter` philosophy: a silent
  desync would turn the guard into a lie — better to fail loudly).
- Trust boundary: desync of manual SQL setting the flag bypassing the
  counter (symptoms and manual recount — runbooks §6, T-175): with
  counter = 0 and live flags the drain stands still until a recount;
  with counter > 0 and no flags — one "idle" seq scan per tick until
  a recount.

### 4.4. Dependencies

- **T-175 is in production** (migration 000015, the counter maintained
  by bit transitions; as of Oct 24 prod is on migration 14 — the
  T-175 deploy precedes this task). Without the counter the guard is
  impossible; without the guard the no-debt steady state is a full
  seq scan every 60 s.
- No migrations, the schema is unchanged.

## 5. Risks and boundaries

1. **Physical locality is an empirical fact, not a contract**: if the
   heap layout changes (mass rechecks of other zones, table rewrites),
   the seq scan gets more expensive, up to a full pass per tick while
   debt is live. The control is the T-177 log (the query returns to
   the slow log above 3 s); the systematic alternative for that case —
   the keyset cursor (§6.2), deliberately deferred.
2. **Debt → 0**: the per-tick seq scan degrades to a full pass —
   covered by the §4.3 guard (the switch to a spot read).
3. The only pass executor is the http-worker background goroutine
   (a single service instance); a concurrent second instance currently
   races over the flags with the `RowsAffected` guard — the edit does
   not change the concurrency semantics.

## 6. Accepted decisions and rejected alternatives

1. **Seq scan instead of a cursor** — the operator's decision of
   2026-10-24 (domain order does not matter): a minimal diff,
   a sustained 700x win on current data, the guard holds the steady
   state.
2. Deferred: **the keyset cursor** (`ORDER BY … AND (zone_id,id) >
   ($1,$2)` + persistence in `crawler_counters`): robust to the heap
   layout and needs no `SET LOCAL`, but the diff is larger (state,
   wrap, persist); a candidate to revisit if locality degrades
   (§5.1).
3. Rejected: **"just removing ORDER BY"** (without plan pinning): the
   plan does not change — the blind estimation of the `flags & 2`
   expression without statistics keeps the index scan (the §2
   measurement).
4. Rejected: **the partial index `(flags & 2) <> 0`** — T-175: it
   breaks the HOT invariant (`flags` changes twice per episode of
   every domain, millions of non-HOT updates/day).
5. Rejected: **expression statistics** (`CREATE STATISTICS ON
   ((flags & 2) <> 0)`): the semantics on partitioned tables are
   unclear, maintenance is per partition, and §4.2 already delivers
   the evaluable win without new schema objects.
6. The guard **by the T-175 counter**, not "a LIMIT 1 read of the
   flag": the latter is the same full scan when there are no flags,
   i.e. not a guard.

## 7. Testing and acceptance

1. Unit (SQL contracts in the project style): the new text of
   `renderPendingDomainsSQL` (without ORDER BY, LIMIT $1); the guard
   SELECT; the transaction sequence `SET LOCAL ×2 → SELECT` against
   a fake pool; guard=0 → the heavy SELECT is not called; a missing
   counter row is an error.
2. Integration (`test/integration`): flags set (counter > 0) — the
   pass publishes and clears them; all flags cleared (counter = 0) —
   the tick does not run the heavy SELECT (a pool wrapper with a call
   counter); a repeat pass does not select the cleared ones.
3. Acceptance: `renderPendingDomainsSQL` entries disappear from the
   production server log (3 s threshold, T-177); the debt drain pace
   (the "render rework pass" log domains/published, the T-175 debt
   metric) grows from "a batch per 30+ s" to the capacity of the
   q:render gates; with counter = 0 the tick does not show up in
   `pg_stat_activity` at all.

## 8. Stages

1. `renderSelectPending`: transaction + `SET LOCAL` + the new §4.1
   text.
2. The §4.3 counter guard in `RequeueOnce`.
3. The §7 tests, the SQL variable comment, `docs/render.md` (the
   selection plan shape and its pinning, the guard trust boundary).

# Specification: temp_error retries — candidate selection with a seq scan instead of an index marathon

Version: 1.0 (based on the analysis of the slow query log of the window
after the 2026-10-25 12:37 MSK deploy — query #1 of the window by total
DB time; log: `log_min_duration_statement = 3000`, T-177). The disease
class is the same as that of
[`TZ_slow_render_requeue.md`](./TZ_slow_render_requeue.md) (T-181):
blind planner estimation plus an index scan in an unfavorable shape,
which the drain loop runs continuously.

---

## 1. Current state and source

The `retrySelectSQL` query (`internal/scheduler/retry.go:106`):

```sql
SELECT d.zone_id, d.id, d.temp_attempts FROM domains d
WHERE d.status = $1 AND d.check_state = $2 AND d.last_checked_at < $3
ORDER BY d.zone_id, d.id LIMIT $4
```

($1 = 5 `StatusTempError`, $2 = 2 `CheckStateDone`, $3 = now−24 h,
$4 = 1000 — the prod config has no overrides: batch 1000, window 24 h,
interval 1 h.) Called by `retryCandidates` ← `RetryOnce` ← the
`RetryTempErrors` loop (T-031): the drain runs passes back to back
while the batch is full — each pass is one SELECT.

Window statistics for Oct 25 09:38–13:15 UTC (3.6 h after the deploy):
**150 threshold hits** at 3 s — the scan runs continuously every ~90 s,
median **74.8 s**, max 427.4 s, total **12,952 s ≈ 3.6 h of DB time** —
effectively one DB core busy with this statement the whole window.
pg_stat_statements: 175 calls, mean 79.4 s, total 13,893 s.

The cohort multiplier: the DNS incident of Oct 24 (the temp_error share
— alert T-182, 33 hits) left **491,210** domains in the temp_error
status, of which mature retry candidates (done + older than 24 h) are
**157,363** (a snapshot of Oct 25 ~16:20 MSK): the drain keeps batches
full for ~157 passes in a row. The "older than 24 h" retry window over
the date column matures in cohorts at midnight — the storm repeats
after every mass temp_error episode.

## 2. Diagnosis (EXPLAIN ANALYZE on the production DB, Oct 25)

Table `domains_default`: 42.3M rows, heap 4.3 GB (540K pages), indexes
5.5 GB; ever checked 3.56M, the `last_checked_at < now−24 h` window is
**3,356,133 rows** (94% of checked).

| Variant | Plan | Time | Wasted row visits | Buffers |
|---|---|---|---|---|
| as in production | Parallel Index Scan `domains_recheck_idx` (partial, `WHERE check_state = 2`), Index Cond `last_checked_at < …`, Filter `status = 5` | **44,660 ms** | 2,900,000 | 2,113,971 (≈16 GB), reads 420K (3.3 GB, I/O 113 s), dirty 940 |
| parameter cast `::date` | same plan, same estimate (~1K/worker) | — | — | — |
| without ORDER BY + `SET LOCAL` indexscan/bitmapscan/indexonlyscan = off | Parallel Seq Scan + LIMIT early stop | **229 ms** | 141,000 | 1,819 (≈14 MB) |

1. **The planner lands on the partial recheck index** (migration
   000008/T-104): the `check_state = 2` predicate is provable, and the
   range on the leading key `last_checked_at` looks narrow. In reality
   the "older than 24 h" window is almost the entire checked history
   (3.36M rows): the window is not selective, and the index form
   degenerates into a marathon.
2. **The planner estimate cannot be trusted**: rows ≈ 2.5–3K versus
   the actual 3.06M visited. The `last_checked_at` histogram does not
   reflect the check wave (92% of values NULL, the non-null ones packed
   into 4 days of the campaign). Casting the parameter to `date`
   (a same-type comparison) does not change the estimate — verified in
   production; relying on a fresh ANALYZE is not an option (the
   distribution drifts daily) — pin the plan shape.
3. **`status` is not in the index** → a heap visit for every row of the
   window — the same class as `dnsShareSQL` (T-182), but the window is
   ~4x the daily one.
4. The scan dirties ~940 pages per pass (hint bits) — extra WAL and
   pressure on checkpoints.
5. The scheduler pool is serial (`NoParallelGather`, T-171): in a
   production session the scan runs without parallel workers — the
   44.7 s of the superuser measurement matches the 74.8 s median under
   load. Almost all of the time is read I/O (113 s across the three
   workers in the measurement).

## 3. Target state

1. Batch selection — a seq scan with early stop: **fractions of a
   second** (the temp_error density of ≥1/269 rows yields 1000
   candidates within ~140–290K row visits); the drain storm stops
   occupying the DB — the pace is set by the batch UPDATEs, not the
   SELECT.
2. A steady state without temp_error — the heavy SELECT does not run at
   all: a spot check of the `crawler_counters['status_temp_error']`
   counter (T-178; mature candidates are a subset of temp_error,
   counter 0 ⇒ no candidates).
3. The `domains` HOT invariant is untouched: no new indexes, `status`
   stays out of the indexes.

## 4. Changes

### 4.1. SQL without ORDER BY

```sql
SELECT d.zone_id, d.id, d.temp_attempts FROM domains d
WHERE d.status = $1 AND d.check_state = $2 AND d.last_checked_at < $3
LIMIT $4
```

Candidate order does not matter: splitting the batch by
`temp_attempts` is order-independent, the
`retryRequeueSQL`/`retryFinalizeSQL` guards re-check the status and
state at write time (T-031 idempotency), and a repeat pass is safe.
Update the variable comment ("ORDER BY by PK — deterministic
batches"): batch determinism is not needed for correctness.

### 4.2. Transaction with SET LOCAL (plan pinning)

`retryCandidates` runs the selection inside an explicit transaction
(`pool.Begin` — `RetryDB` already supports it; sample —
`renderSelectPending`, T-181):

```sql
BEGIN;
SET LOCAL enable_indexscan = off;
SET LOCAL enable_bitmapscan = off;
SET LOCAL enable_indexonlyscan = off;
SELECT …;   -- 4.1
COMMIT;
```

`SET LOCAL` applies only inside the transaction; the triple disable
(including index-only, following the T-181 review) closes all index
access paths to `domains` — the PK and both partial ones. The edit is
a single spot (`retryCandidates`).

### 4.3. Guard by the temp_error counter (T-178)

Before the selection in `RetryOnce` — a spot read of the
`status_temp_error` row (the name =
`"status_" + model.StatusTempError.String()`, the legend lives in
zonestats.go; transition increments are maintained by the T-178
writers, the authority is the monitor reconciliation):

- `value = 0` → an empty result without the heavy SELECT (`drain()`
  stops correctly, no transaction is opened) — the steady state
  without temp_error costs one row read per hour;
- `value > 0` → the §4.1 selection;
- a missing row is a pass error (the row is guaranteed by the T-178
  backfill; the T-181 guard philosophy: a silent desync would turn the
  guard into a lie — better to fail loudly).

Do not confuse this with the `temp_error:YYYY-MM-DD` row (T-182): that
one counts the day's events, the guard reads the state (the number of
domains currently in the status). The trust boundary is desync of
manual SQL bypassing the increments (symptoms and recount —
`crawlerctl counters recount`, T-178): at 0 with live temp_error the
drain stands still until a recount; at > 0 with none — one "idle" seq
scan per tick.

A residual case, accepted deliberately: temp_error > 0 but no mature
ones (the cohort matures at midnight, the drain has already passed) —
the hourly tick degrades to a full seq scan (~15–20 s) until midnight;
that is ~20 passes/day between storms versus today's 150 in 3.6 h.
The control is the T-177 log; with regular recurrence — a candidate
for the `temp_error_done` counter (§6.5).

### 4.4. Dependencies

- **T-178 is in production** (the `status_*` rows in
  `crawler_counters`) — the Oct 25 12:37 MSK deploy has already
  happened, the rows exist. Without it the guard is impossible.
- No migrations, the schema is unchanged.

## 5. Risks and boundaries

1. **Physical locality is an empirical fact, not a contract**:
   scattered single temp_error rows over a small set stretch the early
   stop up to a full pass (~15–20 s) per tick while the counter is
   live. The control is the T-177 log; the systematic alternative is a
   keyset cursor over the recheck index with persisted state (§6.3).
2. Table growth (a backlog of 38.7M unchecked) makes the full pass
   proportionally more expensive — the guard (§4.3) covers the steady
   state, the early stop covers storms.
3. The only executor is the scheduler background goroutine (a single
   instance); a concurrent second instance races over the candidates
   with the UPDATE guards — the edit does not change the concurrency
   semantics.

## 6. Accepted decisions and rejected alternatives

1. **Seq scan + plan pinning** — the T-181 precedent (the same class:
   blind estimation keeps an index scan in an unfavorable shape):
   measured 195x by time, 1162x by buffers, a minimal diff.
2. Rejected: **the partial index `(zone_id, id) WHERE status = 5`** —
   the `domains` HOT invariant (the T-104/T-175 class): status-5
   transitions are frequent (491K episodes over 4 days of the
   campaign) — non-HOT on each one; the T-170 (deleted) precedent on
   rare transitions does not transfer here.
3. Deferred: **ORDER BY last_checked_at over the recheck index**
   ("oldest first", early stop in key order): with no mature
   candidates it brings back the same marathon across the whole window
   and does not fix the steady state; candidate order is not needed.
   To revisit together with the keyset cursor.
4. Rejected: **the parameter cast and reliance on ANALYZE**: the cast
   was verified in production — the estimate does not change; the
   `last_checked_at` distribution drifts daily with the campaign —
   a plan pin is more reliable than statistics.
5. Deferred: **the two-phase guard** (a `temp_error ∧ done` counter,
   increments through record/retry/recheck/lease): more precise than
   §4.3, but it requires wiring through every state transition — the
   T-178 machine for the sake of the narrow residual case of §4.3;
   a candidate to revisit if the query shows up regularly in the log
   between storms.

## 7. Testing and acceptance

1. Unit (SQL contracts in the project style): the new text of
   `retrySelectSQL` (without ORDER BY, LIMIT $4); the guard SELECT by
   the `status_temp_error` name; the transaction sequence
   `SET LOCAL ×3 → SELECT` against a fake pool; guard=0 → the heavy
   SELECT is not called; a missing counter row is an error;
   `RetryOnce` on fakes — batch splitting, RetryResult.
2. Integration (`test/integration`): a mature temp_error cohort —
   a pass schedules the retry and finalizes by `temp_attempts`;
   `status_temp_error = 0` — the tick does not run the heavy SELECT
   (a pool wrapper with a call counter); drain iterations converge
   (the processed ones are not selected again).
3. Acceptance: `retrySelectSQL` entries disappear from the production
   server log; the post-DNS-incident cohort storm drains within
   minutes (the "temp_error retry drain" log); with
   `status_temp_error = 0` the hourly tick is one row read;
   `make test` + `make test-integration` green.

## 8. Stages

1. `retryCandidates`: transaction + `SET LOCAL` ×3 + the §4.1 text.
2. The §4.3 guard in `RetryOnce` (before candidate selection).
3. The §7 tests, the SQL variable comment, `docs/services.md`
   (scheduler: the selection plan shape and the retry guard).

Dependencies: T-178 in production (the `status_temp_error` counter).
No migrations.

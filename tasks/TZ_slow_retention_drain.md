# Specification: the time-based retention drain — a loop cursor instead of hourly circles

Version: 1.0 (based on the analysis of the production slow-query log
of 2026-10-24 — log entry #13, the heaviest by total time; the log:
`log_min_duration_statement = 3000`, T-177). Synergy — the T-171
window index `subdomain_results_checked_idx` and the T-175 render-debt
counter `crawler_counters['render_pending']`.

---

## 1. Current state and source

The `retentionSelectSQL` query (`internal/content/retention.go:132`):
a batch of content-cleanup candidates for non-finds (T-166) — the age
`checked_at < now() − retention` (6 h), something to clean
(content_hash or jsonb), render safety (`rendered OR (flags & 2) = 0`
via a JOIN on domains), the domain alive and not a find (an anti-join
on laravel_domains); a row-value cursor by PK, `LIMIT 10000`.

Called by `RetentionOnce` ← the hourly `RetentionLoop` (http-worker).
The pass cursor starts from zero **on every pass** (`after :=
retentionKey{}`) and survives only batches within a pass.

Production statistics (the log of Oct 24 09:51–12:15): **50 triggers**
of the 3 s threshold, median **75.9 s**, max 262.5 s — ≈63 minutes of
DB time over 2.4 h, the largest single consumer of the log.

## 2. Diagnosis (EXPLAIN on the production DB, Oct 24)

- The plan: Index Scan `subdomain_results_default_pkey` (a row-value
  cursor, ORDER BY PK); the T-171 age index
  `subdomain_results_checked_idx` is **not used** — the PK order
  forces the planner.
- Each batch walks the PK space from the cursor, visiting the heap of
  every row: rows not yet aged (scattered across the whole PK order —
  the pipeline checks zones round-robin), rows already cleaned
  (jsonb NULL — they drop out of the selection only after a heap
  visit), plus a JOIN on domains (flags/status — heap) and an
  anti-join on laravel_domains for every candidate row.
- The pass starts from zero hourly: the already-traversed and cleaned
  history (millions of rows) is re-read **every hour**; 50 pass
  batches × tens of seconds; the trail grows linearly with the table
  (rechecks write new rows, the inflow ≈3M/day).
- Retries of protected rows are handled precisely by full circles:
  rows under the protection of the queue snapshot
  (retentionpending.go) and of render debt ("they will come back with
  the next pass") have a frozen `checked_at` — a time cursor must get
  an explicit retry mechanism in exchange (§4.3–4.4), otherwise
  cleaning stalls until the recheck (months) and the disk grows (the
  T-166/T-167 goal is defeated).

## 3. Target state

1. The pass cost ∝ the **inflow** (an aging hourly slice ≈ tens of
   thousands of rows), not the table size: the age index serves the
   selection.
2. Every row is visited **once in its life**: a cleaned one stays
   behind the cursor; a rechecked one (DELETE+INSERT, fresh
   checked_at — T-058) enters the drain again once it ages.
3. Retries are explicit and bounded: a list of queue-protection keys
   (bounded, point offers) and a catch-up circle on the zeroing of
   render debt.
4. The q:analyze interlock, the protection snapshot, the
   purge/decref/delete semantics, and the T-166 statistics and
   metrics — unchanged.

## 4. Solution design

### 4.1. Time-based selection (the aging window)

`retentionSelectSQL`: the cursor and the ordering — by time:

```sql
WHERE r.checked_at < now() - $1::interval
  AND (r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant)
      > ($2, $3, $4, $5, $6)
  AND (…something to clean… renders-safe JOIN … anti-join …)
ORDER BY r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant
LIMIT $7
```

- The range from the cursor engages `subdomain_results_checked_idx
  (checked_at)` (T-171): the newly-aged slice + the unprocessed tail
  of previous windows (interlocks/blocks) — everything is ahead of the
  cursor.
- Order determinism within equal `checked_at` values (now() is stable
  within the write transaction — batches of rows share the timestamp):
  **Incremental Sort** by the remaining keys on top of the index order
  (PG16) — verify with EXPLAIN; if the planner does not take it — a
  composite index `(checked_at, zone_id, domain_id, …)` as a separate
  migration (the implementer decides based on the actual plan;
  widening the index ≈ +30 bytes × rows versus the full trail —
  cheap).

### 4.2. The loop cursor (survives passes)

The `RetentionLoop` state: the cursor is initialized to zero at
process start (one catch-up circle at startup — paid by the restart,
not by the hour) and advances in batches; an "incomplete batch" —
drain to the end of the aging window, the next pass continues from
where it stopped.

### 4.3. Queue-protection retries — a bounded key list

Keys deferred by `deferBatch` (a snapshot of q:render/q:analyze) are
no longer "picked up on the next circle": the loop keeps them in
memory (bounded; the limit — the order of the stream depths at the
watermarks; exceeding it — a Warn and a conservative catch-up circle)
and in the next pass offers them **point-wise** (`= ANY(keys)`, a PK
probe), excluding from the offer those whose protection has lifted and
whose cleaning has completed. Losing the list (a restart) — covered by
the startup catch-up circle.

### 4.4. Catch-up on render debt

Rows under the render_pending flag are silently filtered out by the
JOIN — the time cursor walks past them. The moment all protection
lifts is the zeroing of the T-175 counter
`crawler_counters['render_pending']` (a point read at the start of a
pass): when it transitions to 0, the loop schedules one full circle
from the zero cursor (catching up on everything accumulated during
the debt epoch; the frequency — by the actual debt drains, not by the
clock). The absence of the counter row (before the T-175 deploy) — a
fallback: a circle once a day in a quiet hour (do not add a config
field — a constant, with a comment).

### 4.5. What stays unchanged

`retentionPurgeSQL`/`decref`/`delete` (the guard re-checks of age and
hash are already in place), the `retentionBlocked` interlock, the
`retentionPendingSnapshot` snapshots and their updating during a long
drain, the `OnPass` metrics.

## 5. Testing and acceptance

1. Unit: the text of `retentionSelectSQL` (a time-based cursor/ORDER
   BY); the loop cursor across passes; the bounded list: offering
   point keys, eviction; catch-up: the counter-0 trigger, the
   fallback day.
2. Integration: a row is cleaned once (subsequent passes do not visit
   it — an SQL call counter); a row protected by a snapshot is cleaned
   by the next pass after the protection lifts (a point offer); a
   render_pending row — by a catch-up circle after the counter zeroes;
   a rechecked row (DELETE+INSERT) enters the drain again after aging.
3. Acceptance: the `retentionSelectSQL` entries disappear from the
   slow log (T-177); the hourly pass takes seconds (the `OnPass`
   duration log); the T-166/T-167 disk plateau holds (content_size
   monitoring); `make test` + `make test-integration` green.

## 6. Accepted decisions and rejected alternatives

1. **A time cursor, not a PK cursor**: rows age in checked_at order —
   the window is served by the T-171 index; the PK order does not
   localize the aging set (checks go round-robin over the zones).
2. **Explicit retries instead of full circles**: the hourly circle
   served as both the retry and the trail; the replacement — a bounded
   list (queues) + a catch-up on the debt counter (the flag) — both
   mechanisms are bounded and observable.
3. Rejected a **PK cursor surviving passes without retries**: it loses
   protected rows until the recheck — a content leak, a Regression
   against the T-166 goal.
4. Rejected **"smaller batches / rarer passes"** (variant "c" of the
   analysis): it softens the symptom; the trail and the growth with
   the table remain.
5. The composite index — **only based on the actual plan** (§4.1): if
   Incremental Sort holds, the schema is not touched at all.

## 7. Staging

1. The time-based `retentionSelectSQL` + the loop cursor (§4.1–4.2),
   the startup catch-up.
2. The bounded list of queue-protection retries (§4.3).
3. Catch-up on the T-175 counter + the fallback day (§4.4).
4. The §5 tests; documentation: `docs/content.md` (the "once in a
   life" drain model, retries), `docs/render.md` (the interaction with
   render debt).

Dependencies: the T-171 index (already in production, migration 0014);
the T-175 counter — optional (the §4.4 fallback works without it). A
migration — only if Incremental Sort proves insufficient (§4.1).

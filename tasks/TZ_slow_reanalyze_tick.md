# Specification: the reanalysis safety-net pass — a time cursor instead of a full scan

Version: 1.0 (based on the analysis of the slow query log of the window
after the 2026-10-25 12:37 MSK deploy — entry #12 of the log; log:
`log_min_duration_statement = 3000`, T-177). The disease class is the
same as that of the retention drain (T-185): a pass with no index over
the filtered column tramples the table from scratch on every tick,
although the candidate set is a handful of fresh tails.

---

## 1. Current state and source

The `reanalyzeSelectSQL` query (`internal/analyzer/reanalyze.go:89`):

```sql
SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant,
       d.label || '.' || z.name
FROM subdomain_results AS r
JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id
JOIN zones AS z ON z.id = r.zone_id
WHERE r.rules_version IS DISTINCT FROM $1::int4
  AND d.status <> $2::int2
  AND ($3::int2 IS NULL OR r.zone_id = $3)
  AND (r.zone_id, r.domain_id, r.subdomain_id, r.variant)
      > ($4::int2, $5::int8, $6::int2, $7::int2)
ORDER BY r.zone_id, r.domain_id, r.subdomain_id, r.variant
LIMIT $8
```

($1 = the rules version, $8 = 5000 — DefaultReanalyzeBatch.) Called by
the hourly tick of `ReanalyzeLoop` (DefaultReanalyzeInterval) via
`ReanalyzeOnce` — **every pass with a zero cursor** (resultKey{}); the
full drain `ReanalyzeDrain` (a rules change, Subscribe T-068;
`crawlerctl reanalyze`) walks the same SQL in batches with a PK cursor.

Window statistics for Oct 25 09:38–13:15 UTC: 3 threshold hits at 3 s
(10:37:36, 11:37:36, 12:37:36 — exactly the hourly tick), 18.2–24.9 s;
the batches are tiny: selected = 56 and 206 (the analyzer log, the
"reanalysis pass" line). The cost is independent of the result: to find
a handful of stale rows, the pass re-reads the whole table.

## 2. Diagnosis (EXPLAIN ANALYZE on the production DB, Oct 25)

`subdomain_results`: ≈4.8M rows, inflow ≈3M/day (T-185).

| Variant | Plan | Time | Wasted row visits | Buffers |
|---|---|---|---|---|
| as in production (cursor from zero) | Parallel **Seq Scan** `subdomain_results` + the `rules_version` filter | **19,880 ms** | 4,822,000 | 441,782 (≈3.4 GB, reads 423K / 3.3 GB), dirty 1198 |
| time cursor (a 2 h window, a probe) | Bitmap Index Scan `subdomain_results_checked_idx` + Heap + Sort | 3,028 ms | 132,000 (the window) | 87,210 |
| the target mode (a live cursor ~1 h) | the same | **≈300–1000 ms** | an hour's window | ~the window |

1. **The planner is right, the access shape is wrong at the root**:
   `rules_version` is deliberately unindexed (the HOT invariant:
   analysis updates are millions/day; the T-104/T-175 class), a PK
   cursor from zero cuts off nothing — a seq scan over 4.8M rows is
   optimal for the planner and reasonable only for a full drain.
2. **The tick's candidate set is a fresh slice**: rows go stale in two
   ways — new episodes (an INSERT with `rules_version` NULL, T-058;
   a fresh `checked_at`) and rules changes (a full drain on Subscribe).
   The tick's tails (56/206) are fresh records whose analysis lagged
   behind/got lost; everything stale-and-old is cleaned up by the drain
   at the previous rules change.
3. **The cost grows linearly with the table** (inflow 3M/day): today
   20 s/hour, in a month — minutes; dirty pages (hint bits, 1198) add
   WAL.
4. The time cursor lands on the ready-made
   `subdomain_results_checked_idx` (T-171): an hour's window ≈ 15K
   rows, the heap visits for `rules_version` are a fraction of
   a second.

## 3. Target state

1. The hourly tick reads only the fresh slice (from the live cursor):
   fractions of a second, the cost is independent of the table size.
2. The full drain on rules changes and `crawlerctl reanalyze` —
   unchanged (PK order, rare, deliberately full).
3. The HOT invariant is untouched: no index over `rules_version`
   appears.

## 4. Changes

### 4.1. The tick's time cursor

`ReanalyzeLoop` keeps the cursor between passes (in the loop state,
the `retentionCursor` sample of retentiondrain.go T-185):

```sql
WHERE r.rules_version IS DISTINCT FROM $1::int4
  AND d.status <> $2::int2
  AND ($3::int2 IS NULL OR r.zone_id = $3)
  AND (r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant)
      > ($4, $5::int2, $6::int8, $7::int2, $8::int2)
ORDER BY r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant
LIMIT $9
```

- a composite key (time + PK) — deterministic batches and correct
  pagination (a row-value comparison over the same key);
- the plan: the `subdomain_results_checked_idx` index (T-171) cuts the
  window, the batch sort fits in memory (thousands of rows);
- the cursor advances past the last row of a non-empty batch; an empty
  batch — the window is exhausted, the cursor waits for the next tick.

### 4.2. The start anchor — a bounded window, not an epoch

The loop start and the recovery after a failure — the cursor
`now() - DefaultReanalyzeLookback` (24 h, a constant with its
rationale in a comment): the tick's safety net covers fresh losses;
everything older is either already analyzed or the territory of the
full drain at the next rules change. The residual class (a loss older
than 24 h without a rules change) is unreachable in the model
(`rules_version` changes only by an analysis write or a rules change;
a new episode gets a fresh `checked_at`) and is backed by the
q:dead/q:analyze observability (T-162) — document it.

### 4.3. The full drain — unchanged

`ReanalyzeDrain` (Subscribe on rules changes, `crawlerctl reanalyze`)
stays on the PK cursor from zero: rare, and it must pass the entire
set; its cost (one full pass per rules change) is deliberate. Fix the
separation of the two forms in the SQL comment: the PK form — the
drain, the time form — the tick.

### 4.4. Dependencies

T-171 (the `subdomain_results_checked_idx` index). No migrations.

## 5. Risks and boundaries

1. The bitmap+heap of an hour's window under an I/O storm — seconds
   (the §2 measurement; the 2 h probe gave 3 s under the current
   load); the control is the T-177 log; the storm is addressed by
   T-191.
2. The tick and the drain at the same time (a rules change during
   a tick) — both are idempotent at the "selection + publication"
   level (the T-071 rule: once a new rules_version is committed, the
   row is not selected again), batch overlap is safe.
3. The 24 h lookback and the 1 h tick pace: a fresh loss is picked up
   no later than lookback + tick — enough for a safety net (the main
   path is the immediate publication in record.go and Subscribe).

## 6. Accepted decisions and rejected alternatives

1. **The time cursor** — the T-185 precedent (retention): the same
   pair of "an unindexable filter × a fresh slice", the same solution;
   measured 20x–60x on current data, independence from table growth.
2. Rejected: **an index over `rules_version`** (including a partial
   one): the `subdomain_results` HOT invariant — analysis updates of
   millions/day would become non-HOT (the T-104/T-175/T-182 class).
3. Rejected: **an EXISTS probe before the batch** ("cheaply check for
   emptiness"): proving emptiness is the same full scan.
4. Rejected: **widening the tick interval** (an hour → a day): it
   treats the frequency, not the pass cost, and degrades the safety
   net's freshness; unnecessary after §4.1.
5. Rejected: **persisting the cursor in the DB** (crawler_counters):
   the §4.2 lookback anchor makes losing the cursor cheap (one 24 h
   window); the in-process loop state is enough (the T-185 style — the
   cursor in the loop state, a circle from the anchor).

## 7. Testing and acceptance

1. Unit: the SQL contract of the time form (the composite cursor,
   ORDER BY, LIMIT); the drain's PK form unchanged; the loop with
   a fake — the cursor lives between passes, advances by a batch, an
   empty batch does not move it; the start anchor = now−lookback.
2. Integration: a fresh row without analysis is picked up by the tick
   and published; an analyzed one is not selected; a rules change —
   the drain from zero finds the old (outside the lookback); a repeat
   tick does not re-select.
3. Acceptance: the `reanalyzeSelectSQL` tick entries disappear from
   the slow log (the drain on rules changes remains, deliberately);
   the analyzer log, the "reanalysis pass" line — the same numbers as
   before; `make test` + `make test-integration` green.

## 8. Stages

1. The §4.1 time form + the §4.2 cursor state and anchor in
   `ReanalyzeLoop`.
2. The §7 tests, the SQL comments (the separation of forms),
   `docs/analyzer.md` (the safety-net tick — the fresh slice, the
   §4.2 residual class).

Dependencies: T-171. No migrations.

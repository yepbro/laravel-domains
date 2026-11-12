# Specification: dispatcher selection — only zones with work

Version: 1.0 (based on the analysis of the production slow-query log of
2026-10-24 — log entry #9, the critical path of the pipeline; the log:
`log_min_duration_statement = 3000`, T-177). It relies on the
`zones.domains_pending` column of the incremental counters wave
([`TZ_incremental_counters.md`](./TZ_incremental_counters.md), T-178) —
implementation **after** T-178.

---

## 1. Current state and source

The `dispatchZoneSQL` query (`internal/scheduler/dispatch.go:103`):

```sql
SELECT d.zone_id, d.id, d.label
FROM unnest($1::int2[], $2::int8[]) AS z(zone_id, after_id)
JOIN LATERAL (
    SELECT dd.zone_id, dd.id, dd.label FROM domains AS dd
    WHERE dd.zone_id = z.zone_id AND dd.id > z.after_id
      AND dd.check_state = 0 AND dd.status <> 6
    ORDER BY dd.id LIMIT $3
) AS d ON true
```

Called by `DispatchBatch` (T-027) — the main intake of the pipeline:
active zones (`dispatchZonesSQL`) → LATERAL prefixes of the pending
queues via the partial index `domains_pending_idx` → a weighted merge
in Go → a `scheduler.batch_size` batch into q:dns (via the transition
to queued, T-028).

Production statistics (the log of Oct 24): 26 triggers of the 3 s
threshold (the window 09:50–10:09), median **5.2 s**, max **132 s**.

## 2. Diagnosis (EXPLAIN ANALYZE on the production DB, Oct 24)

The plans are correct — Index Scan `domains_default_zone_id_id_idx`
(the partial pending index, T-104), keyset `id > after`. The problem is
**wasted work**, designed before the IANA zone directory was filled in
(T-125):

1. **Round 1 — 2204 LATERAL probes**, one per active zone (the IANA
   directory is `is_active` in full): ~2000 zones are nearly empty in
   terms of pending (the average returned 3 rows) — each probe still
   pays the descent along the large partition index + a heap visit.
   Measurement: 0.6–1.0 s per round, 0.5–0.8 s of which is I/O
   (1.2–3.6k reads).
2. **Round 2, the "top-up"** — zones that returned exactly the quota
   are considered "starved" and are re-read with quota = Limit (the
   dispatch.go comment: "from round 1 on, quota = Limit"). With 2204
   zones the round 1 quota = ceil(L/2204)+1 = 1 → "starved" = any zone
   with at least one pending → round 2 reads up to ~N_zones × 1000
   rows with heap visits (labels) — up to ~200k reads per batch. This
   dominates the median of 5.2 s; the max of 132 s is an
   I/O-contention episode (the T-167 campaign hours).
3. `domains_total > 0` does not filter (verified in production: 2204
   of 2204) — what tells zones apart is **pending**, not the presence
   of domains.

## 3. Target state

1. `dispatchZonesSQL` selects only zones with work:
   `WHERE is_active AND domains_pending > 0` — round 1 = hundreds of
   probes instead of 2204; round 2 — only genuinely starved zones.
2. The round 2 quota is capped by the batch remainder: a read beyond
   the remainder cannot make it into the merge (the external cap is
   Limit), while today `Limit` is read for every starved zone.
3. The selection semantics are unchanged: the same domains, the same
   weighted round-robin order, the same keys/invariants of T-027; only
   the set of offered zones changes (zones empty in terms of pending
   are excluded — their probe returns 0 rows even today).

## 4. Changes

### 4.1. dispatchZonesSQL — a filter by work

```sql
SELECT id, name FROM zones WHERE is_active AND domains_pending > 0
```

- The `zones.domains_pending` column — incremental counters spec §4.1;
  its semantics is exactly the dispatcher predicate (`check_state = 0`
  over the base set `status <> deleted`).
- Freshness: the T-178 transition increments maintain the column in
  the same transactions (the dispatcher enqueue decrements it itself —
  the column is exact by the next batch; a new zone ingest increments
  via `apply_add` — the zone is visible immediately, without waiting
  for the reconciliation); the reconciliation repairs drift.
- Trust boundary: counter drift (manual SQL bypassing the increments)
  can hide a zone with work until the nearest reconciliation
  (`monitor.zone_stats_interval`, 5 min) — observed via the drift
  metric of incremental counters spec §11; the reverse case
  (counter > 0, the queue empty) is a single "idle" probe, no
  degradation.
- Dependency: **T-178** (the column + the enqueue/apply_add
  increments). Before its deploy the task is not implementable without
  duplicating the semantics — the queue order is fixed.

### 4.2. Round 2 quota — the batch remainder

In `DispatchBatch` (the top-up logic): for starved zones of round ≥ 2
the LATERAL quota = `remaining = Limit − collected` (today — Limit).
Correctness: any zone's contribution to the batch does not exceed the
remainder — `remaining` rows of a zone are enough for the merge to fill
the batch completely; a read beyond that cannot make it into the merge
(the external LIMIT is the batch size). The worst case, "a batch from a
single zone" — remaining = Limit, the behavior is unchanged. Update the
test comment of the dispatchMaxRounds mutant.

### 4.3. What is unchanged

`dispatchZoneSQL` (the LATERAL part) — no edits: the descent along the
partial index remains a point read of the zone queue prefix; the
weights, the merge, publication rate, `ResolveZoneWeights` (T-106) —
no edits.

## 5. Testing and acceptance

1. Unit: the text of `dispatchZonesSQL` (the exact contract `AND
   domains_pending > 0`); the round 2 quota = remaining (a table of
   cases: one starved zone, several, remaining < Limit); round 1 does
   not offer a zone with domains_pending = 0 (a fake pool counts the
   probes).
2. Integration: a zone with pending is selected, a zone with
   domains_pending = 0 is not probed; after a zone's pending is
   exhausted (everything transitioned to queued) the zone disappears
   from the selection; ingesting new domains into an empty zone makes
   it visible (the apply_add increment of the T-178 path).
3. Acceptance: the `dispatchZoneSQL`/`dispatchZoneSQL-rounds` entries
   disappear from the slow log (the 3 s threshold, T-177); the q:dns
   publication rate does not drop (measured with the T-092 kit), the
   batch selection latency is a few hundred ms; `make test` +
   `make test-integration` green.

## 6. Accepted decisions and rejected alternatives

1. **A filter by `domains_pending`, not `domains_total`**: the §2.3
   measurement — all 2204 zones have domains; pending is what tells
   them apart. The column semantics matches the dispatcher predicate —
   no probes returning emptiness remain at all.
2. **Synergy with T-178 instead of in-memory heuristics** (skipping
   "empty" zones via scheduler state): state/heuristics/races with new
   ingest versus a single column already maintained by transitions;
   the implementation order — after T-178.
3. **The round 2 quota cap = remaining**: reading `Limit` for every
   starved zone is an artifact of the protection against pathological
   weights (dispatchMaxRounds); the upper bound of a zone's
   contribution is the batch remainder, a read beyond it is garbage.
4. Rejected an **EXISTS pending subquery in dispatchZonesSQL**: the
   same probe cost as round 1 today (a descent into the partition
   index per zone) — it does not filter, it moves the cost.
5. Rejected **"redesigning the merge in SQL"**: the weighted
   round-robin order is a Go algorithm (T-027/T-106); moving it into
   SQL for a single filter is a change of the source of truth with no
   gain.

## 7. Staging

1. `dispatchZonesSQL` + the filter wiring (after the T-178 deploy).
2. The round 2 quota cap (§4.2) — independent, may be done earlier
   than T-178.
3. The §5 tests, the top-up comment in dispatch.go.
4. Documentation: `docs/services.md` (the dispatcher as a reader of
   the zone counters, an addition to incremental counters spec §8),
   a note in the incremental counters spec about the new
   `domains_pending` reader.

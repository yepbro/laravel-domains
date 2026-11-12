# Specification: returning expired leases — a batch of PK probes and a drain cursor instead of a double range scan

Version: 1.0 (based on the analysis of the slow query log of the window
after the 3bffc62 deploy — 2026-10-25 18:16 → 10-26 03:05 UTC, query #3
by total DB time; log: `log_min_duration_statement = 3000`, T-177,
local copy `tmp/pg_slow_after_3bffc62.log`). The disease class is the
same "dead prefix + re-reading the already-scanned" as in
[`TZ_slow_render_requeue.md`](./TZ_slow_render_requeue.md) (T-181) and
the retries ([`TZ_slow_temp_error_retry.md`](./TZ_slow_temp_error_retry.md),
T-191), but here the multiplier is the UPDATE-side plan itself.

---

## 1. Current state and source

The `leaseUpdateSQL` statement (`internal/scheduler/lease.go:103`):
a single CTE statement, "select a batch of expired leases + UPDATE +
zone counter deltas" (T-029/T-178): CTE `b` — up to `LeaseBatch`
(5000) rows with `check_state = 1, queued_at < now() − lease` over the
index `domains_lease_idx (queued_at, zone_id, id) WHERE check_state = 1`
(migration 000008, T-104); `upd` — the move to pending with a guard
re-check of the selection at row-lock time (races with workers);
`zc` — the `zones` deltas. Called by `LeaseOnce` ← the `LeaseWatch`
drain (a lease/2 = 15 min loop; batches repeat while they pass in
full).

Window statistics after 3bffc62 (8.8 h): **75 threshold events** at
3 s (every storm batch), median **8.1 s**, max **111 s**, Σ1288 s —
7% of the slow DB time. The scheduler journal: drains return 1–67K
domains (the midnight storm of the daily recheck + temp_error retries:
the Oct 25 drain at 21:20 UTC moved 104,297 domains); each 5000 batch
is a separate statement, each with its own scan.

## 2. Diagnosis (EXPLAIN on the production DB, Oct 26)

1. **The UPDATE side reads the entire expired range, not the batch.**
   The plan: `Update → Nested Loop`; the outer side is an Index Scan
   of `domains_default_queued_at_zone_id_id_idx` with the condition
   `queued_at < now() − 30m` **without LIMIT**, the inner side is the
   `b` subquery (LIMIT 5000); the join is a Join Filter over
   (zone_id, id). The planner covers the outer side's guard conditions
   (check_state/status/queued_at) with the same lease index and does
   not pick PK probes. Net result: per batch the expired range is
   scanned **twice** (b + upd); the cost of a batch is proportional to
   the range size, not the batch. `WITH b AS MATERIALIZED` does not
   change the plan — verified in production.
2. **The dead index prefix between vacuums.** A measurement on Oct 26
   ~06:40 UTC: the SELECT part with `actual rows=0` (nothing to
   return) — **15,928 buffers, 10,212 reads ≈ 80 MB, I/O 11.4 s**:
   half an hour before the measurement a storm drain moved tens of
   thousands of leases; their index entries are dead until vacuum but
   still get scanned — `status` is not in the index, every entry
   requires a heap visit. The lease index size per partition is
   **273 MB with 996 live entries** (`check_state = 1` at the time of
   the measurement). Every batch of every drain starts its scan **from
   the beginning of the index** — the 14 batches of a storm drain
   re-read the same prefix 14 times.
3. **Autovacuum cleans the prefix rarely**: the
   `autovacuum_vacuum_scale_factor 0.02` threshold × 42.3M rows of
   `domains_default` ≈ 846K dead; the daily recheck wave (~1.5–2M
   enqueue→return/write) gives 2–3 vacuums per day — in between the
   prefix builds up to ~a million entries (tens of seconds of wasted
   scanning per batch).
4. `deleted` domains do not create the trail: `check_state = 1 AND
   status = 6` — 0 rows (measurement); there is no non-find share
   among the expired. The trail is only the dead versions of
   returned/written leases.

## 3. Target state

1. The UPDATE side — spot PK probes over the batch (not a range scan):
   the cost of the move is proportional to the batch, not the size of
   the expired range; the guard re-checks (check_state/status/queued_at
   at lock time) are preserved — the protection against races with
   workers is the same.
2. The dead prefix is read **once per process lifetime**, not by every
   batch: the drain cursor `(queued_at, zone_id, id)` in the
   `LeaseWatch` state (precedents — the time cursors of T-185/T-193).
3. The atomicity of "selection = move" is preserved: both operations
   in one transaction (today a single statement provides it).
4. The HOT invariant is untouched: no new indexes, `status` stays out
   of the indexes.

## 4. Changes

### 4.1. Batch selection with a cursor

```sql
SELECT zone_id, id FROM domains
WHERE check_state = 1 AND status <> 6
  AND queued_at < COALESCE($1::timestamptz, now()) - make_interval(secs => $2)
  AND (queued_at, zone_id, id) > ($3::timestamptz, $4::int2, $5::int8)
ORDER BY queued_at, zone_id, id LIMIT $6
```

- a composite cursor (the full index key) — deterministic batches,
  pagination without skips/repeats; a NULL cursor (start) — as today,
  from the beginning of the index;
- correctness of the movement: everything with a key ≤ the cursor has
  already been returned (expiry depends only on queued_at — rows with
  an equal queued_at share the same fate; the ones not yet expired at
  pass time sit to the right of the threshold, hence to the right of
  the cursor, and will age out ahead of it); re-enqueueing a domain
  writes a fresh `queued_at` — to the right of the threshold and the
  cursor; `deleted` rows to the left of the cursor are skipped forever
  — and they are exactly the ones that must not be returned (the
  lease.go comment, spec 13.1);
- a process restart resets the cursor — the startup full drain is
  preserved (T-012/T-029), pass idempotency is unchanged.

### 4.2. Moving the batch with PK probes

```sql
WITH upd AS (UPDATE domains AS t
  SET check_state = $1, queued_at = NULL
  FROM unnest($2::int2[], $3::int8[]) AS u(zone_id, id)
  WHERE t.zone_id = u.zone_id AND t.id = u.id
    AND t.check_state = 1 AND t.status <> 6
    AND t.queued_at < COALESCE($4::timestamptz, now()) - make_interval(secs => $5)
  RETURNING t.zone_id),
zc AS (UPDATE zones AS z SET domains_queued = z.domains_queued - c.n,
       domains_pending = z.domains_pending + c.n
       FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c
       WHERE z.id = c.zone_id)
SELECT zone_id FROM upd
```

- the shape is the project's batch UPDATE (`db.Update` style, T-028):
  the planner leads from `unnest` and probes `domains` by PK — there
  is no alternative lease scan on the UPDATE side anymore;
- the guard filters (check_state/status/queued_at) are the same,
  applied after the row is locked: the race semantics ("a worker wrote
  a result between the selection and the lock — do not return it") are
  unchanged; the drain counts by `RETURNING` (the rows moved by this
  very pass);
- the check_state/status literals in the text go through `model.*` —
  as today (the partial index predicate must be provable for the
  SELECT side).

### 4.3. Transaction and the drain cursor

- `LeaseOnce` runs the SELECT + UPDATE in one transaction
  (`pool.Begin`, sample `renderSelectPending` of T-181): the atomicity
  is equivalent to today's single statement; `LeaseBatch` does not
  grow.
- `LeaseWatch` keeps the cursor between batches and drains (a loop
  state field, the `retentionCursor` style of T-185): the cursor = the
  composite key of the last row of the moved batch; an empty batch
  does not move it.
- The scheduler pool already runs under `LockTimeout(30s)` (T-192) and
  `NoParallelGather` (T-171) — the new statements inherit both session
  settings.

### 4.4. Dependencies

No migrations, the schema/indexes are unchanged. `db.DB` is enough
(spot statements; `Begin` — via the pool interface, as in T-181).

## 5. Risks and boundaries

1. **The prefix tail beyond the cursor after a restart** is read again
   (the startup drain) — deliberate: losing the cursor is cheaper than
   a full pass (the T-193 §6.5 philosophy); while the process is alive
   the prefix is not re-read.
2. **The cursor and a parallel second scheduler**: impossible today
   (a single unit instance); the guard re-checks make batch overlap
   safe even if one appears.
3. The dead prefix remains a fact of the partition until vacuum — the
   task does not cure the bloat (global settings are deliberately
   untouched) but removes its re-reading from the hot path.

## 6. Accepted decisions and rejected alternatives

1. **Two statements instead of one** — the only way to remove the
   lease scan from the UPDATE side: the guard predicates give the
   planner an index path; `AS MATERIALIZED` and join reordering do not
   change it (the §2.1 measurement). The transaction restores
   atomicity.
2. **The drain cursor** instead of "tolerating the prefix": the
   T-185/T-193 precedent; in a storm the saving is multiples larger
   (14 batches × prefix → 1 × prefix).
3. Rejected: **a partial index with `status`** — the `domains` HOT
   invariant (the T-104/T-175 class).
4. Rejected: **per-table autovacuum settings** for `domains_default` —
   the project rule: settings only via the global config, partitions
   are created dynamically (docs/db.md).
5. Rejected: **raising `autovacuum_vacuum_scale_factor` globally** —
   forcing vacuum on all tables for the sake of one index changes the
   load of the whole cluster (I/O starvation is already a factor in
   every find of the window).

## 7. Testing and acceptance

1. Unit: SQL contracts (the SELECT-with-cursor and the
   UPDATE-with-unnest+guard texts, the model.* literals); `LeaseOnce`
   on a fake — a pair of statements in one transaction, counting by
   RETURNING, an empty batch does not move the cursor, the guard
   filtered out a result written by a worker; `LeaseWatch` — the
   cursor lives across batches/drains, a restart resets it (a zero
   cursor).
2. Integration (`test/integration`): a mature lease is returned,
   a repeat pass — 0; a result written between the selection and the
   move — not returned (a race); the cursor does not skip rows that
   expire later (a small lease, two passes with a pause); the `zones`
   deltas reconcile with the T-178 reconciliation.
3. Acceptance: lease return entries in the slow log (T-177) — single
   occurrences (the first pass after startup over a large prefix);
   a storm drain of tens of thousands of leases — a few minutes
   instead of tens; `make test` + `make test-integration` green.

## 8. Stages

1. The §4.1–4.2 SQL + the §4.3 transaction in `LeaseOnce`.
2. The cursor in `LeaseWatch`, §4.3.
3. The §7 tests, the SQL comments (the cursor, the double scan — why
   two statements), `docs/services.md` (the return pass — a batch of
   PK probes, the drain cursor).

Dependencies: none (the T-104 index is already in production).
No migrations.

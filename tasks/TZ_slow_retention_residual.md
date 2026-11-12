# Specification: residual slowness of the retention drain — a serial pass and the hardware factor

Version: 1.0 (based on the analysis of the slow-query log of the
window after the 3bffc62 deploy — 2026-10-25 18:16 → 10-26 03:05 UTC,
statements #4/#8 by total DB time; the log:
`log_min_duration_statement = 3000`, T-177, local copy
`tmp/pg_slow_after_3bffc62.log`). It extends
[`TZ_slow_retention_drain.md`](./TZ_slow_retention_drain.md) (T-185):
the time cursor proved itself (median 75.9 s → 13.8 s), but the pass
has not left the slow log — an analysis of why and what else can be
done.

---

## 1. Current state and source

`retentionSelectSQL` (`internal/content/retention.go:150`) — the T-185
time form (the cursor `(checked_at, PK)`, the aging window
`checked_at < now() − retention`, `LIMIT content.retention_batch`
(10,000), a JOIN on `domains` (render safety, status) + an anti-join
on `laravel_domains`), the hourly `RetentionLoop` of http-worker; next
to it `retentionClearSQL` — the batch cleaning (UPDATE … FROM unnest,
NULLing the payload columns) of the selected batch.

Statistics of the window after 3bffc62: SELECT — 38 events, median
**13.8 s**, max **222.8 s** (the first pass after the deploy restart —
the anchor window of T-185 §4); the UPDATE cleaning — 37 events,
median 12.1 s. For comparison, before T-185 (the Oct 24 window):
median 75.9 s, max 262.5 s — the ~5.5× gain

is confirmed.

## 2. Diagnosis (EXPLAIN ANALYZE, BUFFERS on the production DB, Oct 26)

A measurement of the pass with an hourly window (cursor = now−7h,
aging 6 h, LIMIT 10,000), a steady-state CPU/IO starvation (PSI some
avg300 ≈ 80%):

| Plan node | Actual | Buffers |
|---|---|---|
| Parallel Index Scan `subdomain_results_default_checked_at_idx` (2 workers, Gather Merge) + Incremental Sort | ~70k window rows examined: the "something to clean" filter (content_hash/jsonb) rejected ~45k (**65%**) — a heap visit for each | 23.7k hit + 39.6k read, **I/O 36 s** |
| Anti Join `laravel_domains` (Index Only, PK) | 22.6k probes, cheap | 45.4k hit |
| Join `domains` (PK probes: status, the render flag) | 22.0k probes | 101k hit + 8.5k read, I/O 16 s |
| Total | a batch of 10,000 in **24.7 s** (superuser, 2 parallel workers) | 170.6k hit + 48.2k read |

1. **The aging window is 84,229 rows/hour** (a count measurement), not
   the "≈15k" of the T-185 §2 estimate: the `subdomain_results` inflow
   ≈ 2–3M/day. The spec's scale constant underestimated the window by
   5.6× — hence the "0.3–1 s" estimate versus 5–25 s in fact.
2. **The form's cost is the necessary minimum**: the payload columns
   (`content_hash`, jsonb) are outside the `checked_at` index and
   cannot be indexed (the HOT invariant of `subdomain_results` —
   cleaning is millions of UPDATEs/day, the T-104/T-175 class);
   therefore every window row requires a heap visit, and 65% of them
   clean nothing. The JOIN on `domains` — point PK probes, as cheap as
   it gets. The T-185 cursor already guarantees a single lifetime
   visit of every window row.
3. **The main multiplier — I/O starvation**: 48k reads per pass under
   competition with the pipeline (PSI ~80%) stretch the form's minimum
   from single seconds to tens; in the midnight storm — up to 86–94 s.
4. **The pass takes CPU away from the pipeline**: the http-worker pool
   lacks `NoParallelGather` (T-181/T-171 installed the hook on
   monitor/dashboard/scheduler) — the drain gathers 2 parallel workers
   (Gather Merge) on the same service where the HTTP slots run.

## 3. Target state

1. The retention drain does not take cores away from the pipeline: the
   http-worker pool is serial (`max_parallel_workers_per_gather = 0`);
   the pass trades wall-clock time for CPU fairness (the wall time of
   a background drain is not critical — the pace is set by
   `retention_batch`/the interval; the T-166 disk drains over hours).
2. SQL/schema unchanged: the form is optimal (§2.2), no migrations.
3. A checkpoint: after the CPU/IO starvation is lifted (a second
   render host, operations Oct 24) — a re-measurement; the goal — the
   SELECT and UPDATE-cleaning entries leaving the slow log (the 3 s
   threshold, T-177) in steady state.

## 4. Changes

### 4.1. NoParallelGather for the http-worker pool

`cmd/http-worker` (wiring `service.Options.DBAfterConnect`): the
`db.NoParallelGather` hook (T-171) on the service pool — extending the
existing solution to the fourth service. The point statements of the
HTTP slots (PK, unnest) do not use parallelism — the pool behavior
changes only for the aggregate/scanning statements of the service's
background loops (retention; the T-066 render rework pass is already
serial by nature).

### 4.2. Documentation and observability

- `docs/db.md` (the T-171 hook table): http-worker in the list; the
  rationale — the retention drain.
- A `retentionSelectSQL` comment: the actual window scale (≈84k rows/h
  versus the 15k of the T-185 estimate) and the form's minimum (§2.2)
  — so that the next analysis does not hunt for a "lost index".

### 4.3. What is deliberately NOT changed

- `retention_batch`, the loop interval: the total work per window does
  not depend on the slicing (one pass over the aging slice); smaller
  batches — a longer drain window with no benefit.
- Indexes on the payload columns and `rendered` — the HOT invariant.
- The pass frequency — the pace is dictated by the T-166/T-167 disk
  goal.

## 5. Risks and boundaries

1. The wall time of a single pass will grow (~×1.5–2 under starvation)
   — a background drain is not on the critical path; the control — the
   pass-duration metric and the depth of the "uncleaned" tail (the
   `content.retention` lag); the T-162 disk alert remains the guard.
2. If after the hardware expansion the pass is still >3 s — back to
   analysis with new measurements (the candidates of that circle:
   narrowing the JOIN with a condition on the `render_pending`
   counter, a two-phase selection) — captured by the §3.3 checkpoint,
   not by this task.

## 6. Accepted decisions and rejected alternatives

1. **Serial over "speedup"** — the only honest local solution: the
   form is already minimal (§2.2), and the drain's parallelism
   competes for the starved CPU with the same service.
2. Rejected a **partial index on payload/`rendered`** — the HOT
   invariant (the T-104/T-175/T-182 class).
3. Rejected **reducing `retention_batch`** — it does not change the
   total work.
4. Rejected **moving retention to a separate service/pool** — a second
   pool in the connection budget (docs/db.md §"Connection budget") for
   a single loop; the hook solves it with the same means.

## 7. Testing and acceptance

1. Unit: a wiring test of `cmd/http-worker` (or a build config test) —
   the pool gets the AfterConnect hook (modeled on the T-171/T-192
   tests); the SQL contracts unchanged.
2. Integration: not required (the SQL behavior is unchanged) — the
   existing retention tests stay green.
3. Acceptance: the `EXPLAIN` of a pass in a service session — no
   Gather/Parallel nodes; retention entries in the slow log do not
   become more frequent; the §3.3 checkpoint after the render
   expansion.

## 8. Staging

1. The §4.1 hook (wiring + test).
2. Documentation §4.2.
3. A checkpoint — a note in the task (a measurement after the hardware
   expansion of render).

Dependencies: none. No migrations.

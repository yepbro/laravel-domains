# Specification: the temp_error share on daily counters — dnsShareSQL without a window scan

Version: 1.0 (based on the analysis of the production slow-query log of
2026-10-24 — log entry #6; the log: `log_min_duration_statement = 3000`,
T-177). A continuation of
[`TZ_slow_done_today.md`](./TZ_slow_done_today.md) (T-179): the same
pair of "calendar day window × fresh rows", the same class of solution —
daily counters in `crawler_counters`.

---

## 1. Current state and source

The `dnsShareSQL` query (`internal/monitor/alerts.go:336`):

```sql
SELECT count(*) FILTER (WHERE status = $1), count(*)
FROM domains WHERE last_checked_at >= current_date
```

($1 = 5, `model.StatusTempError`.) Called by `evalDNS` — the T-080
alert "the temp_error share among the checks of the current day"
(fire/hold hysteresis, the minimum sample count `AlertDNSMinSamples`),
the monitor `AlertsLoop` cycle every 5 min.

Production statistics (the log of Oct 24, 09:43–12:19 UTC): 33 triggers
of the 3 s threshold (each alert evaluation), median **27.7 s**,
min 14.1 s, max 110.7 s.

## 2. Diagnosis (EXPLAIN ANALYZE on the production DB, Oct 24)

- The plan is correct: Parallel Index Scan
  `domains_default_last_checked_at_idx`, `Index Cond:
  last_checked_at >= CURRENT_DATE` — the T-171 index works as intended;
  the window is the same 815k rows as `done_today` (≈1.5–2 million by
  evening).
- But it is an **Index Scan, not Index Only**: the `status` column is
  not in the index → a heap visit is mandatory for every row of the
  window. 308k buffers (≈2.4 GB), of which **170k reads** (1.3 GB of
  I/O, 12.2 s).
- The monitor pool has `max_parallel_workers_per_gather = 0` (the
  `NoParallelGather` hook, T-171): serially ≈14–15 s (matches the log
  min of 14.1 s; the median/max are under competing load).
- The premise of the `dnsShareSQL` comment that "the pages of today's
  checks are the hottest ones, in shared_buffers" did not hold at
  scale: the day window is 1–2 GB of scattered heap pages against
  2 GB of shared_buffers shared with the pipeline; each alert
  evaluation re-reads a gigabyte from NVMe.

## 3. Target state

1. `evalDNS` reads **two point values** from `crawler_counters` — the
   numerator and the denominator of the share; the day-window scan
   disappears from the alert loop.
2. The numerator — a new `temp_error:YYYY-MM-DD` row; the denominator —
   the `checks:YYYY-MM-DD` row (T-179). The increments live in the same
   result-recording transactions (`httpworker/record.go`); the
   authority is the T-179 reconciliation (a monitor pass).
3. Share semantics: **result-record events** over the day instead of
   "domains in the temp_error status right now" (the divergence — §5.1
   of spec T-179: a retry that cleared the error stays in the event
   history) — more honest for a throughput alert; the
   thresholds/hysteresis are unchanged.

## 4. Counter rows

| `name` | `value` | written by |
|---|---|---|
| `checks:YYYY-MM-DD` | the result records of the day | T-179 (already designed) |
| `temp_error:YYYY-MM-DD` | of them, those with the temp_error status | this task |

Day/timezone/rotation/no row until the first record — identical to
spec T-179 §4 (the same `crawler_counters` table, T-175; no
migrations).

## 5. Increments

`httpworker/record.go`, the same transaction and the same statement
pattern as T-179 §5 (`INSERT … ON CONFLICT (name) DO UPDATE SET value =
crawler_counters.value + EXCLUDED.value`); the second row:

```sql
INSERT INTO crawler_counters (name, value)
VALUES ('temp_error:' || to_char(current_date, 'YYYY-MM-DD'), $1)
ON CONFLICT (name) DO UPDATE
    SET value = crawler_counters.value + EXCLUDED.value
```

where `$1` is the number of batch records with a temp_error verdict
(counted in Go from the batch data before writing). Duplicate delivery
adds an extra event to the numerator and the denominator at the same
time — the share is stable; the absolutes are repaired by the
reconciliation (T-179 §5.1).

## 6. Reconciliation

The T-179 pass (before T-178 — a temporary hourly monitor loop; after —
FILTER positions of the combined pass of the incremental counters spec)
recomputes **both** rows with a single aggregate over the rows already
scanned:

```sql
SELECT count(*), count(*) FILTER (WHERE status = 5)
FROM domains WHERE last_checked_at >= current_date
```

(before the overwrite the previous values are read; the drift of both
rows is the T-179 §8 metric). Until the first pass after midnight there
is no row — an honest zero.

## 7. Reader (the alert)

`evalDNS`: instead of `dnsShareSQL` — a point read of two rows
(`SELECT name, value FROM crawler_counters WHERE name IN ($1, $2)`;
the day is computed by the DB):

- no `checks:` row (or 0) and `total < AlertDNSMinSamples` →
  `NotMeasured` (the former minimum-sample semantics);
- `share = temp/total`; the `AlertDNSFireShare`/
  `AlertDNSRecoverShare` hysteresis and the detail format — unchanged.

`dnsShareSQL` is removed (together with the alertShare unit fixture);
the alert loop frequency is unchanged.

## 8. Testing and acceptance

1. Unit: the SQL contracts of the §5 increment and the §7 read; query
   texts without `dnsShareSQL`; evalDNS on fakes: NotMeasured with an
   empty counter, firing/holding per the thresholds, the detail
   semantics preserved.
2. Integration: an episode with temp_error results — the
   numerator/denominator grow; a repeat episode without temp_error —
   the numerator does not grow; the reconciliation converges to the
   live aggregate.
3. Acceptance: the `dnsShareSQL` entries disappear from the production
   server log; a test alert firing (the T-162 threshold) arrives as
   before; `make test` + `make test-integration` green.

## 9. Accepted decisions and rejected alternatives

1. **Daily counters, a continuation of T-179** — the same class as
   `done_today`: a calendar window over fresh rows is structurally
   expensive (heap visits of the window rows — `status` cannot be in
   the index); a counter breaks the conflict; the denominator is
   already designed.
2. Rejected an **index with `status`** (a key or a predicate —
   including a partial `WHERE status = 5`): the status changes on
   every result (tens of millions per day) — getting into the index
   breaks the HOT invariant (the `flags` class/T-175, docs/schema.md).
3. Rejected **"evaluate less often"** (5 → 15–30 min): it softens the
   frequency, but each evaluation remains 15–110 s of I/O; it does not
   solve the problem.
4. Rejected giving **parallel gather back to the monitor pool**: that
   is a rollback of T-171 (aggregates steal cores from the pipeline);
   point reads are cheaper.
5. The "events, not state" semantics is accepted deliberately (§3):
   the share is robust to duplicate deliveries, retries stay in the
   history denominator; the live aggregate is restored by the
   reconciliation.

## 10. Staging

1. The §5 increment (after T-179 — the same transaction, a second
   upsert).
2. The §6 reconciliation (an extension of the T-179 pass).
3. The §7 reader, the removal of `dnsShareSQL`, the §8 tests.
4. Documentation: `docs/services.md` (the source of the DNS alert
   share), a note in spec T-179 (a second daily row).

Dependencies: T-175 (the table), T-179 (the denominator row, the
reconciliation pass). No migrations.

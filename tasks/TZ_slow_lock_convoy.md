# Specification: the row-lock convoy on `domains` — holder observability and a wait cap

Version: 1.0 (based on the analysis of the lock cluster of the window
after the 2026-10-25 12:37 MSK deploy — log entries #2–#4/#6/#11; the
log: `log_min_duration_statement = 3000`, T-177). The class is not a
planned pathology (the victim plans are point scans by PK) but a
deadlock-wait incident: a long transaction holds row locks on
`domains` for tens of minutes while point neighbors wait without a
cap.

---

## 1. Current state and source

The convoy victims are point statements of the pipeline write path
(all by PK, all within the Oct 25 10:22–12:44 UTC window):

| Log entry | Statement | Source | n>3 s | median | max |
|---|---|---|---|---|---|
| #2 | `SELECT status, check_state … FOR UPDATE` | httpworker/record.go (T-178: reading old counter values) | 10 | 15.5 min | 37.1 min |
| #3 | `UPDATE … SET flags = flags \| 4 …` | dnsworker: the http_published bit on q:http publication | 145 | 3.7 s | 37.3 min |
| #4 | `WITH b AS (… queued_at < … )` — lease return | scheduler/LeaseWatch (T-029) | 30 | 4.6 s | 39.4 min |
| #6 | `WITH s AS (… FOR UPDATE), upd …` — DNS result write | dnsworker/result.go | 46 | 3.9 s | 15.4 min |
| #11 | `WITH s AS (… FOR UPDATE)` — HTTP result write | httpworker/record.go | 35 | 3.7 s | 6.1 s |

In total ≈6.1 h of DB time over the 3.6 h window — the second-heaviest
group after retrySelectSQL (T-191).

## 2. Diagnosis (a reconstruction from the log, EXPLAIN on the production DB)

EXPLAIN of the victims (production parameters, the convoy row
`(560, 25398110)`): Index Scan `domains_default_pkey`, cost 2.8,
rows 1 — **the plan is perfect; the time is not the plan but the wait
for a row lock**.

Reconstruction from timestamps (start = the log timestamp − duration):

1. **Wave 1, a mass holder ~10:22–10:38**: nine victims (a lease
   return, a publish, result writes; zones 1, 2, 3, 4, 15, 105, 121,
   290×2 — different rows across the whole table) started at
   10:22:13–10:22:50 and **were released simultaneously at
   10:38:04.212–.226** (waits of 895–950 s) — the signature of a single
   transaction holding scattered rows for ≈16 min: a table traversal
   with UPDATEs along the way.
2. **Wave 2, a single convoy 10:40–11:17**: five sessions of three
   services (dns-worker + 4 http-worker connections) queued on **one
   row `(zone_id=560, id=25398110)`** with waits of 1604–2240 s and
   **were released within one second 11:17:27.899–11:17:28.138** — the
   holder lived ≈37 min; a queue of five is a classic convoy.
3. **The tail 11:17–12:44**: waits of 3–7 s on various rows —
   write-path transactions stretched by the retrySelectSQL I/O storm
   (T-191: 16 GiB of reads every ~90 s) plus short neighbor locks.
4. **The operator is ruled out**: journalctl sshd for 13:00–14:30 MSK —
   not a single Accepted login; the holder is an application
   transaction. Exact identification is impossible post factum:
   **`log_lock_waits = off`** — the log contains no
   "process still waiting for …" lines (0 lines per day); the holder's
   identity (pid, statement, transaction) is unrecoverable. The
   candidate hypothesis (not provable retroactively): a result-write
   transaction stretched by the storm (TOAST deletes/inserts into
   `subdomain_results`, the `contents` refcount) — verifiable only on
   recurrence with log_lock_waits enabled.
5. **The consequence — a pipeline standstill of ≈55 min**: q:dns sat
   at watermark 125 the whole window ("publication suspended" every
   ~30 s, scheduler); dns-worker could not write results (victim #6) →
   no XACK → the depth did not decrease → the dispatcher did not
   publish. The "frozen queue" alert (T-162) does not fire by
   definition (deliveries were happening, albeit slowly); the only
   signal was the stream of T-177 messages.

## 3. Target state

1. On recurrence the DB log **names the holder** (pid, statement, wait
   time) — `log_lock_waits = on`.
2. The convoy is cut off by a cap: the point write path waits on a row
   for no longer than `lock_timeout = 30 s`; beyond that — a
   transaction rollback and the normal delivery retry (at-least-once,
   spec §3.4), instead of 37 minutes of silent waiting that breaks the
   pipeline.
3. A point fix of the holder — a separate task based on the recurrence
   data (this wave yields only the class, not the name).

## 4. Changes

### 4.1. `log_lock_waits = on` on the production DB (operational)

A reload parameter; writes one line "process N still waiting for
ShareLock on transaction X …" when a wait exceeds `deadlock_timeout`
(1 s). The cost — a few log lines only during minutes of real
concurrent waits; the benefit — the holder's name in the next
incident. Pin it in the production config files (the path —
T-086/T-177, where `log_min_duration_statement` already lives) and in
the dev config (`deploy/docker`).

### 4.2. `lock_timeout = 30 s` for the write-path pools

In `db.Options.AfterConnect` (wiring `service.Options.DBAfterConnect`)
next to `NoParallelGather` (T-171) — a new hook `db.LockTimeout(30s)`
(`SET lock_timeout = '30s'`), enabled for dns-worker, http-worker,
scheduler (the pools whose transactions take row locks on `domains`):

- 30 s is three orders of magnitude above the normal waits (ms per
  result write) and one order below the incident ones (15–39 min) —
  it cuts only the convoy;
- a write transaction rollback → a delivery without XACK → a retry
  after the delivery backoff (minutes) outlives the holder's window;
  attempt exhaustion → q:dead — a visible signal (the T-162 alert)
  instead of the silent q:dns standstill at the watermark;
- monitor/dashboard/analyzer untouched (readers without row locks;
  reconciliation — short per-row zone UPDATEs).

The value is a default constant with the rationale in a comment; no
config knob is needed (an incident cap, not tuning).

### 4.3. Dependencies

No migrations. 4.1 — operational, immediately (does not wait for a
deploy); 4.2 — code, the next deploy. Coordination with T-191: both
change the scheduler pool behavior (the AfterConnect hook) — the files
overlap; run in one wave or sequentially without conflict (different
hooks of the same wiring).

## 5. Risks and boundaries

1. **lock_timeout may cut legitimately long waits** of mass passes
   (lease return #4 competes with enqueue for the same rows): the 30 s
   cap covers the observed norms (a maximum of seconds outside the
   incident — 10.6 s for enqueue); the control — the T-177 log + WARNs
   on timeouts in the services (a rollback metric).
2. **The wave 1 holder is not identified** — a table-traversing UPDATE
   for 16 min; no candidates in the code (enqueue/lease/writes are
   key-based batches, seconds) — on recurrence with log_lock_waits,
   analyze by name; the holder fix is outside this task.
3. The 3–5 s tail (#3/#6/#11 outside the monsters) — the T-191 I/O
   storm, goes away with its fix; needs no separate change (monitor
   after T-191).

## 6. Accepted decisions and rejected alternatives

1. **Observability + a cap, not a point fix** — the holder is not
   recoverable retroactively; the class is closed by observation (4.1)
   and damage limitation (4.2); the name — upon recurrence.
2. Rejected **"do nothing, the incident is a one-off"**: the pipeline
   stood silently for 55 min; without log_lock_waits the next
   recurrence will be nameless again.
3. Rejected **`idle_in_transaction_session_timeout`** (protection
   against forgotten transactions): no operator sessions in the
   window, the main path is the application; the parameter also cuts
   legitimate long transactions of the file-ingest apply.
4. Rejected **`statement_timeout` on the write pools**: it cuts the
   long operations themselves (enqueue 10.6 s, writes under the
   storm), turning slow successes into rollbacks; lock_timeout
   targets precisely the wait for someone else's lock.
5. Rejected **waiting for a recurrence without a cap** (4.1 only):
   the recurrence would again cost an hour of pipeline standstill for
   one log line.

## 7. Testing and acceptance

1. Unit: the `db.LockTimeout` hook sets `lock_timeout` in
   AfterConnect (modeled on the NoParallelGather test in
   pool_test.go); the wiring is enabled for
   dns-worker/http-worker/scheduler and not for
   monitor/web/dashboard.
2. Integration: a pool session sees `lock_timeout = 30s`
   (`SHOW lock_timeout`); an operation waiting on a row longer than
   the cap gets SQLSTATE 55P03 and the normal retry path (a testbed
   scenario: the row held by a second session).
3. Acceptance: in production `SHOW log_lock_waits` = on; on convoy
   recurrence the log contains "process … still waiting" with the pid
   and the transaction of the holder; waits in the T-177 log do not
   exceed the cap (30 s) except the holder itself; `make test` +
   `make test-integration` green.

## 8. Staging

1. Operational: `log_lock_waits = on` in production (reload) + pinning
   in the configs (4.1).
2. Code: the `db.LockTimeout` hook + the wiring of the three pools
   (4.2).
3. The §7 tests, a note in docs/operations.md (the Oct 25 incident
   analysis: the convoy signature, where to look for the holder in
   the log).

Dependencies: T-171 (the AfterConnect hook location); coordination
with T-191 (the shared scheduler pool wiring). No migrations.

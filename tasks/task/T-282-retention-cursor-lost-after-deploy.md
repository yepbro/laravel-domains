# T-282: Retention drain cursor lost after the deploy — cursor-less full scans

- **Priority:** P1
- **Status:** [new]

## Context and symptoms

- After the 14.11 00:33 MSK deploy `retention_drain_cursor` has **0 rows**
  (checked 09:56 MSK). Before the deploy (13.11) the row existed and
  advanced: checked_at 02:32:34Z / updated_at 19:05:34Z.
- Since the deploy **no retention pass has completed**:
  `pass_seconds_count=0` for ~9.4 h of http-worker uptime (the previous
  process lifetime had 5 completed passes; T-273 semantics — the counter
  increments at pass end).
- Live symptom at 09:56 MSK: the retention SELECT over subdomain_results
  runs **74–80+ min under the full front** (the >30 min red flag), with
  swap-out bursts up to 11 MB/s. §4.6 norm: minutes in silence, ~60 min
  «впритык» under the front.
- Correlated overnight storm (14.11 02:30–06:11 MSK) — the same
  "long retention SELECT + full front" pattern:
  - 4 PG cgroup OOM kills (02:30, 03:55, 04:52, 06:10) with PG restarts
    (NRestarts 6→10) — a continuation of the T-279 kill series
    (11–13.11);
  - 3 GLOBAL OOM kills (03:40–03:51, CONSTRAINT_NONE — RAM+swap
    exhausted system-wide, headless-shell victims up to 1.06 GB anon);
  - q:dead 38→135 (+97 q:dns batches ×1000 domains, «доставок без
    подтверждения») during the PG downtime windows.
- The §4.6 contract "an http-worker restart no longer resets the
  retention drain (T-274): the cursor is persistent in the DB" is
  currently violated in practice.

## Root cause (to verify)

Candidate hypotheses, in order of likelihood:

1. The deploy/migration cleared (or recreated without restoring) the
   `retention_drain_cursor` row.
2. The new build writes the cursor only at pass completion (T-273
   semantics extended to the cursor) — under the front the pass never
   completes → the cursor is never written → the next pass restarts a
   full scan from scratch: a closed loop.
3. A schema/key rename with the old row left behind in another table.

A data point: during the 14.11 quiet window (opened 10:05 MSK) watch
whether the cursor row appears when a pass completes in silence.

## Requirements

1. Verify the mechanism (migration diff, the cursor write path in
   http-worker: checkpoint vs pass-end).
2. Fix persistence: the cursor row must survive deploys and http-worker
   restarts; if written only at pass end — introduce intra-pass
   checkpointing (e.g. on every N-th batch, as the pre-deploy behavior
   showed ~hourly updates).
3. Recovery from a lost cursor: a documented/automated way to seed the
   cursor (or an explicit operator procedure in docs/operations.md §4.6)
   so that a loss does not turn into cursor-less full scans.
4. A regression test: deploy/restart simulation must keep the cursor
   row and resume the drain from the saved position.

## Acceptance criteria

- After a deploy and http-worker restarts: the cursor row present, the
  drain resumes from the saved position — no full-scan retention SELECT
  >30 min under the full front.
- 24–48 h of watch ticks: no retention long-query flags; PG OOM kills
  of the pattern absent (together with T-279).
- `make test` + `make lint` green; `make test-integration` if the
  migrations/DB/testbed are touched.

## References

- docs/operations.md §4.6 (quiet window), docs/content.md
  (retention/compaction), docs/db.md
- T-274 (the persisted drain cursor), T-279 (PG memory ceiling),
  T-278 (temp_error retry backoff — the repeated-load lever)
- prod-reports/2026-11-13-225911.md (the overnight storm and recovery),
  2026-11-13-090459.md, 2026-11-13-075848.md (the 13.11 watch ticks)

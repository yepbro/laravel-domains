# Task T-273: hotfix — the eternal loop of doomed retention/render-late cursor selects (a T-271 regression)

Version: 1.1 (v1.1 — the subagent's decisions, section 7). Stage 43.
Priority: P0 (a production incident, retention dead since 09.11.2026).
Source: the research subagent investigation of 10.11.2026 —
[`prod-reports/2026-11-10-005208-t271-research.md`](../../prod-reports/2026-11-10-005208-t271-research.md)
(the full evidence base, EXPLAINs, live pg_stat_activity measurements).
Related snapshots: `prod-reports/2026-11-09-204415.md`, `prod-reports/2026-11-09-235559.md`.
The regression was introduced by commit `15a4e41` (T-271).

## 1. Incident

After the T-271 deploy (09.11.2026 14:13 MSK, http-worker PID 3484423) both
heavy http-worker selects in production fail with a timeout on EVERY pass, not
a single success in ~21 hours (the `crawler_content_retention_*` metrics
on :9104 — all zeros since process start; 70+/70+ errors in the journal):

- `WARN msg="content retention pass" err="content: retention: selecting candidates: timeout: context deadline exceeded"`
- `WARN msg="render late republisher" err="httpworker: late republisher: select: timeout: context deadline exceeded"`

Consequences: content retention does not purge rows (~5 GiB/day of content
growth with the pipeline running; currently 19 GiB with 23 GiB free —
~4–5 days of headroom); the render republisher is dead (with render_pending > 0
the render debt never returns to q:render — a hidden backpressure
degradation); the doomed 600 s scans churned the PG buffers — 21 cgroup
OOM kills of postgres overnight 09–10.11 (~once an hour). Note: the
scheduler in production was stopped on 10.11 08:00 MSK (operator) — the bug
reproduces on a fully idle system too.

## 2. Root cause (proven by the investigation)

**The eternal loop of the doomed catch-up: the drain cursor does not move because
the pass does not fit into the timeout, and it never fits because the cursor is
zero.**

1. The drain cursor lives in process memory and moves only after a
   successfully completed batch. Since process start not a single pass has
   completed → the cursor of both loops is zero → every pass scans “from
   the start of the table”.
2. `retentionSelectSQL` with a zero cursor at production scale
   (subdomain_results 11.3M rows, ~677 partitions; domains 43M):
   `checked_at >= '0001-01-01'` → Parallel Append over all partitions +
   a heap visit of every row (the “anything to purge” predicate —
   content_hash/jsonb — is absent from the index), cost ≈ 563M; the first
   candidates sit in checked_at order AFTER millions of already-clean rows
   (retention has not worked since 08.11 — a backlog of ≈ 4–5M rows).
   Consistently > 600 s.
3. Before T-271 the slow catch-up reached the commit and moved the cursor
   (“slow, but with progress”). The client-side `SelectTimeout` deadline
   (600 s) introduced by T-271 kills the query BEFORE the commit → the
   cursor stays zero → the next pass repeats the identical doomed scan.
   **The timeout turned a slow drain into a permanently broken one.**
4. `lateRenderRowsSQL`: the T-271 pins (seq/hash/merge/bitmap off,
   `internal/httpworker/renderlate.go:187-190`) leave the planner only
   Nested Loop, and it unwinds the join from domains (43.4M!), cost ≈ 157M,
   always > the 60 s deadline → the same loop. Before T-271 the query was not
   pinned (deliberately, T-251) and worked in production for weeks.
5. The illusion of “instant” failures: the loop ticker (15 min) does not
   re-anchor after errors (`internal/content/retention.go:961` — the
   `!backoff` condition; `internal/httpworker/renderlate.go:717`), the pause
   is counted from the ticker anchor, not from the end of the pass (the real
   pause is 300 s with a 600 s pass). The phasing of both loops
   (+60.000 s, identical milliseconds) — the shared gate
   `internal/db/scangate.go`: render-late waits forever on a gate occupied
   by the doomed retention pass. The error is a genuine pgx timeout (the
   `timeout:` prefix — pgconn), the queries really executed up to the deadline
   (caught in pg_stat_activity active, wait DataFileRead).

## 3. Requirements (hotfix scope — options A + D from the investigation)

### R1. retention: SelectTimeout 600 → 3600 s
`DefaultRetentionSelectTimeout` (`internal/content/retention.go:136`).
3600 (not 1800): the first catch-up pass in production is estimated at
30–60 min; headroom is mandatory — a second doomed timeout is unacceptable.
After the first successful commit the cursor jumps over the clean prefix and
the regular passes return to seconds.

### R2. render-late: pins and timeout
Remove the aggressive pin set (return to the unpinned T-251 shape that
worked in production) OR replace it with `SET LOCAL join_collapse_limit = 1`
(preserve the FROM order: subdomain_results leads, point probes of
domains/zones). The choice — by EXPLAIN on a testbed with production-scale
data (see R5). The select timeout 60 → 300 s. Do NOT touch the scangate
gate (it works as designed).

### R3. Ticker reset after every error
`internal/content/retention.go:961` (remove `!backoff` from the error
branch) and its counterpart in `internal/httpworker/renderlate.go:717`:
after a failed pass the ticker is set to a pause ≥ the pass duration +
the period — the WARN cadence stops masking the real duration, diagnostics
become honest.

### R4. Loop error metrics
`crawler_content_retention_errors_total` (http-worker :9104) and
`crawler_render_late_errors_total` (:9104/9105 — wherever the loop is
exposed) — currently errors are visible only in journalctl. Pass-error
counters, incremented on every failed pass.

### R5. Tests
- The main T-271 test gap: **a catch-up from the zero cursor must complete
  within SelectTimeout on production-scale data** — a testbed fixture with
  partitions and a volume reproducing the skip-prefix cost (millions of
  clean rows before the first candidates); for render-late — an
  EXPLAIN/measurement of the chosen plan shape.
- A backoff semantics test (R3): after an error the pause ≥ pass + period.
- Update the existing timeout/pin tests to the new constants and the new
  plan shape.

### Constraints (out of scope, follow-ups)
- A partial index + a persistent cursor (option C) — a separate task.
- Timeout config keys in config.yaml (option B) — a separate task;
  currently code constants.
- Do not change the DB schema; no migrations.

## 4. Acceptance criteria

1. `make test`, `make lint`, `make test-race` (content and httpworker are in
   RACE_PKGS — test-race is mandatory) — green.
2. The R5 test “a catch-up from the zero cursor within SelectTimeout at
   production-scale volume” exists and is green; the backoff semantics test
   is green.
3. The R4 metrics are exposed and grow in the test on a failed pass (fake).
4. Documentation updated: `docs/content.md` (retention: timeouts, catch-up
   behavior, the error metric), `docs/render.md` or the relevant one
   (render-late: the plan shape, the timeout).

## 5. Production deploy and verification plan (for the main session/operator)

1. Deploy with the scheduler STOPPED (currently stopped since
   10.11 08:00 MSK) — the first pass drains the backlog with one long scan
   on an empty system.
2. Drain verification: `:9104` — `crawler_content_retention_pass_seconds_count`
   > 0 and growing; `rows/contents/freed_bytes_total` grow in batches of
   ~10000; the 15-min WARN series of both loops stops; in
   `pg_stat_activity` no http-worker queries longer than ~3600 s; INFO
   “content retention pass” with freed_bytes.
3. Only after the drain is confirmed — starting the scheduler (operator);
   then monitoring: zero new PG OOM kills per day, the content disk trend
   downward.

## 7. Decisions made during the task (v1.1, the T-273 subagent)

- **R2, the render-late plan shape — the unpinned T-251 shape chosen.**
  The `SET LOCAL join_collapse_limit = 1` option was rejected by a
  testbed measurement (PG 16.15, a throwaway schema t273_exp: 2M domains /
  550K subdomain_results, the production ratio ~1:3.8): the GUC keeps
  join pairs in FROM order but does NOT forbid unwinding outer/inner
  within a pair — the EXPLAIN under `join_collapse_limit = 1` is identical
  to the unpinned one and still unwinds from domains (Append domains →
  subdomain_results probes by PK). The promised “subdomain_results leads”
  property does not hold. The unpinned shape was measured on the same
  fixture: a zero cursor — Nested Loop from domains with an early Limit
  stop, 19 ms execution (EXPLAIN ANALYZE); mid-table — Merge Join from
  subdomain_results by PK, 42 ms; the domains≫records pressure — an
  r-driven Merge Join, 260 ms. Plus a production fact from the investigation:
  the unpinned T-251 shape worked for weeks, the T-271 pinned one —
  the eternal loop. The wrapper is kept with a single
  `SET LOCAL statement_timeout = '300000ms'`.
- **Integration verification of the shapes** was done by running two new
  tests against the already-up testbed stack (the stack was not recreated):
  TestContent_RetentionCatchUpFromZeroCursor, TestRenderLateSelect-
  PlanUnpinned — green.
- **Review (deepseek-v4.1-flash, one round):** no P0/P1. Accepted:
  P2-2 — the docs/services.md metric registry extended with two counters;
  P3s (the scangate comment, the “cursor does not move” help text under
  partial progress, the rows=1000 boundary in the plan test, the
  work_mem=16 MB argument against the OOM return, clarifying the contract
  nature of the catch-up test) — applied. Rejected: P2-1 “raise
  3600 → 7200” — the task text prescribes 3600 explicitly (“3600 (not
  1800)”); instead of changing the constant, the code/docs wording was
  made honest (3600 = the upper bound of the 30–60 min estimate, no
  headroom above it; if underestimated — the same loop with an honest
  error cadence; the measurement — by the first pass per the §5 deploy
  plan, structural protection — follow-up C). Waiting on the ScanGate gate
  outside SelectTimeout (P3) — deliberately out of scope (“do NOT touch
  the gate”), the behavior documented in the runbooks.

## 8. References

- Investigation (evidence, EXPLAINs, options A–D):
  `prod-reports/2026-11-10-005208-t271-research.md`
- The regression: commit `15a4e41` “fix: pin the plans of the retention/render-late
  cursor selects + timeouts, backoff and the shared http-worker scan gate —
  the PG OOM cycle (T-271)”; the T-271 history — `tasks/tasks_done.md`
- Code: `internal/content/retention.go` (retentionSelect ~592–663, RetentionLoop
  ~944–967, DefaultRetentionSelectTimeout :136), `internal/content/retentiondrain.go`,
  `internal/httpworker/renderlate.go` (pins :187–190, the select :631–656, the
  ticker :717), `internal/db/scangate.go`, wiring `cmd/http-worker/main.go`
  (SelectTimeout/ErrorBackoff passed as constants — no config keys).

# Breakdown of the Specification “Mass Domain Verification and Laravel Site Search Service”

Source: [`TZ_domain_crawler.md`](./TZ_domain_crawler.md) (version 1.2).
Additional stage specs: [`TZ_dashboard.md`](./TZ_dashboard.md) —
the operator web dashboard of the pipeline (stage 15);
[`TZ_render_engines.md`](./TZ_render_engines.md) — render engine selection
(Chromium/Moli/Lightpanda) and the fallback queue (stage 35);
[`TZ-fsm.md`](./TZ-fsm.md) — formalization of the pipeline's implicit
finite-state machines (stage 37, audit 2026-11-06).

## How to work with this file

- Tasks are numbered `T-001…T-282` and ordered by execution (top to bottom).
- A task status is a bracketed label, changed by editing:
  - `[new]` — task not started;
  - `[in progress]` — task underway;
  - `[done]` — task completed; when a wave finishes, its task lines move
    to the archive [`tasks_done.md`](./tasks_done.md) — only unfinished
    tasks remain here.
- Task details (incident context and symptoms, root cause, requirements,
  acceptance criteria, references) live in a separate file
  `task/<T-NNN>-<slug>.md`; the plan holds only a title line with the
  status and the link (AGENTS.md, “New task format”). Detail edits live in
  the task file; pass the task file link to the task subagent.
- Example: `- **T-001** [in progress] — <title> ([details](task/T-001-slug.md))`
- Within a stage, tasks run in parallel where possible; between stages — sequentially.
- Deviation from the spec §12 stage order: the **content store** (stage 4, archive)
  was implemented before the HTTP stage, since `http-worker` saves content through
  `ContentStore` (spec §5.5, §5.7).

## Stage index

Completed and moved to the archive [`tasks_done.md`](tasks_done.md): stages
0–34 (moved 2026-11-03), the code part of stage 35 (T-233…T-246 —
render engines and the fallback queue), stage 36 (T-250…T-254 — extra
review-round fixes), stage 37 (T-255…T-266 — formalization of implicit
finite-state machines, spec [`TZ-fsm.md`](TZ-fsm.md)), stage 38
(T-267…T-268 — zone × counter deadlocks, the 40P01 incidents of 07.11.2026),
stage 39 (T-269 — render_pending counter desync, incident 08.11.2026),
stage 40 (T-270 — the new∧done “stuck status” compensator, incident
22–29.10.2026), stage 41 (T-271 — the PG OOM hotfix: pinning the plans of
the retention/render-late cursor selects, incident 08–09.11.2026), stage 43
(T-273 — the T-271 regression hotfix: the eternal loop of doomed cursor
selects, incident 09–10.11.2026) and stage 44 (T-274 — retention restart
resilience: the select cap config key + a persistent drain cursor);
last move 2026-11-13. Active work covers only the operational tasks of
stage 35: **T-247…T-249** (bench runs of the engines, production rollout,
the canary — sequential). The moli-related [done] tasks are deliberately
kept in the plan as context for the pending T-247 re-test (operator
decision 2026-11-13, not archived): stage 42 **T-272** — the moli v1.1.23
re-test (the upstream fix for Fetch.enable Font — our PR #2470 — shipped in
the release of 09.11.2026), stage 45 **T-275** — block_trackers probes moved
to async-XHR (moli upstream fixes), stage 46 **T-276** — the block_trackers
sync probe on the exact-URL semantics of moli #2729. Stage 47: **T-277** —
TTL for subdomain_results passports of non-findings (10 days): frees
~8–9 GB, steady-state table, covers the future recheck-ghost class.
Stage 48: **T-278–T-282** — production resilience: retries, PG memory,
timeout metrics, crawlerctl.

## Stage 35: render engine selection and the fallback queue (T-233…T-249)

Source: [`TZ_render_engines.md`](./TZ_render_engines.md) (version 0.1).
The “§” and “FR” references in tasks point to the sections and functional
requirements of that spec. Parallelism waves: A (T-233/236/237/241/242 — independent,
started simultaneously in separate worktrees), B (T-234/238/243),
C (T-235/239/240/244), D (T-245/246), then operational T-247…T-249
(sequential). Each task is atomic: one mergeable block,
green `make test`/`make lint` before handing off to review.


*(the code part of T-233…T-246 — waves A–D — moved to the archive [`tasks_done.md`](tasks_done.md) on 2026-11-08; only operational tasks below)*

### Operational (sequential)

- **T-247** [new] — P1, bench comparison of the engines (3 engines × a
  3000-domain sample × 2 runs vs a fresh chromium baseline) against the
  spec §5.1 item 4 thresholds, canary-readiness verdicts per candidate and
  re-test plans for the unready ones; returned by the operator 2026-11-13
  (the moli re-test NOT READY, waiting for the release carrying ①④) —
  [details](task/T-247-engine-bench-comparison.md).

- **T-248** [new] — P1, production rollout of BOTH engine containers
  (moli + lightpanda, pinned versions, egress policy, mem_limit) and
  worker B on q:render-fallback against an empty queue (the wiring check),
  the T-246 alerts, the ≥48 h baseline window, the §6.4 rollback
  rehearsal — [details](task/T-248-production-rollout-engines.md).

- **T-249** [new] — P1, canary windows, §6.3 acceptance and locking in the
  engine switch (an operator run per spec §6.2: the wiring check →
  window 1 → window 2 48 h → acceptance → locking in; starts only with a
  candidate found ready by T-247) —
  [details](task/T-249-canary-windows-acceptance.md).

## Stage 42: the moli v1.1.23 re-test — the upstream fix for Fetch.enable Font (T-272)

*(the context of the upstream release/PR — moved into the task file)*

- **T-272** [done] — P1, operational: bump the moli pin to v1.1.23, the CDP
  slice, a smoke run on the seed hundred, a candidate verdict (input for
  T-247/T-249; the pin infrastructure and smoke procedures — T-241/T-254) —
  [details](task/T-272-moli-retest-fetch-font.md).

## Stage 45: block_trackers — async probes after the moli upstream fixes (T-275)

- **T-275** [done] — P1, switching the `blockLiveTrackerDomains` probes to
  async XHR: after the moli upstream fixes (#2729 — sync XHR under
  interception goes out to the network, sync-XHR blocking is unavailable on
  moli) the synchronous probes give false positives; keep the sync probe as
  an engine-branched case —
  [details](task/T-275-block-trackers-async-probes.md).

## Stage 46: block_trackers — the sync probe on the moli #2729 exact-URL semantics (T-276)

- **T-276** [done] — P1, collapse the sync probe's engine branching after
  #2729 is merged: the maintainer introduced exact URL matching for sync XHR
  (a match → a block, including redirects), the tracker probe is blocked on
  both engines — the “moli: 200 to the network” expectation is outdated —
  [details](task/T-276-block-trackers-sync-exact-url.md).

## Stage 47: TTL for subdomain_results passports of non-findings (T-277)

- **T-277** [new] — P2, background TTL pass (default 10 days, config key):
  delete retention-trimmed non-finding passports with finding protection
  and refcount safety — frees ~8–9 GB now, holds the table at a steady
  state (~1–1.5 GB), and automatically covers the future recheck-ghost
  class — [details](task/T-277-subdomain-results-passport-ttl.md).

## Stage 48: production resilience — retries, PG memory, timeout metrics, crawlerctl (T-278–T-282)

- **T-278** [new] — P1, a progressive temp_error retry backoff (24 h →
  72 h at attempts≥2): the dead corpus gives 63–69% temp_error (the norm
  23–40%) and is driven in a circle up to 4 times — up to ⅔ of the repeated
  q:http/PG load; against the background of the PG OOM kill series of
  11–13.11 — [details](task/T-278-temp-error-retry-backoff.md).
- **T-279** [new] — P1, a PostgreSQL memory cap: memcg OOM kills of the
  backend every 4–5 h under full front (11.11 21:10, 12.11 03:14,
  13.11 02:32); an audit of shared_buffers/work_mem/the cgroup limit
  against the 8 GiB VM budget —
  [details](task/T-279-pg-memory-ceiling.md).
- **T-280** [new] — P2, split the HTTP-stage timeout metric (connect/tls/
  tarpit/per-IP ctx-in-queue): today 92.7% of the errors are one counter,
  the local “load→timeouts→retries” loop is inseparable from the dead
  corpus — [details](task/T-280-stage-errors-timeout-split.md).
- **T-281** [new] — P2, fix crawlerctl domain on production
  (“domain_info: empty response” for any domain, including live ones;
  discovered during the 13.11 investigation) —
  [details](task/T-281-crawlerctl-domain-fix.md).
- **T-282** [new] — P1, the retention drain cursor lost after the 14.11
  deploy (0 rows): cursor-less full scans → retention SELECT 74–80 min
  under the front → the 14.11 02:30–06:11 PG OOM storm (4 kills + 3
  global) and q:dead +97 — restore T-274 persistence,
  [details](task/T-282-retention-cursor-lost-after-deploy.md).

# Breakdown of the Specification “Mass Domain Verification and Laravel Site Search Service”

Source: [`TZ_domain_crawler.md`](./TZ_domain_crawler.md) (version 1.2).
Additional stage specs: [`TZ_dashboard.md`](./TZ_dashboard.md) —
the operator web dashboard of the pipeline (stage 15);
[`TZ_render_engines.md`](./TZ_render_engines.md) — render engine selection
(Chromium/Moli/Lightpanda) and the fallback queue (stage 35);
[`TZ-fsm.md`](./TZ-fsm.md) — formalization of the pipeline's implicit
finite-state machines (stage 37, audit 2026-11-06).

## How to work with this file

- Tasks are numbered `T-001…T-249` and ordered by execution (top to bottom).
- A task status is a bracketed label, changed by editing:
  - `[new]` — task not started;
  - `[in progress]` — task underway;
  - `[done]` — task completed; when a wave finishes, its task lines move
    to the archive [`tasks_done.md`](./tasks_done.md) — only unfinished
    tasks remain here.
- Example: `- **T-001** [in progress] — …`
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
Stage 48: **T-278–T-281** — production resilience: retries, PG memory,
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

- **T-247** [new] — P1, bench comparison of the engines and canary-readiness
  verdicts (spec §5.1; depends on T-234, T-241, T-243,
  T-244; T-254 — engine smoke preliminaries). Not code: runs of the three engines ×
  a 3000-domain sample twice (variability control), baseline — a fresh
  chromium run on the same sample; the T-244 reports against the §5.1
  item 4 thresholds (detection <1%, “engine+fallback” success ≥ baseline minus
  engine-independent losses, throughput no worse, memory <⅓ of the chromium
  budget, 0 crashes); per candidate — a “ready/not ready now” verdict with
  metrics (prod-reports/ or docs). Candidates are NOT cut (operator decision
  2026-11-06, spec v0.3): both engines stay permanent candidates,
  unready ones get a re-test plan with new pinned versions (smoke
  FR-17/T-254), the cadence follows engine releases.
  Criterion: the reports are saved, a verdict per candidate is explicitly
  recorded against the thresholds; for the unready ones — a re-test plan written down.
  Returned to [new] on 2026-11-13 (operator): the moli re-test from
  upstream/main f857b6651 — layer 1 green (after T-276), layer 2 NOT
  READY (success 69% vs 92%, detection divergences 2.94%, RSS 736 MiB
  above ⅓ of the chromium budget; 0 crashes) → the retest is scheduled
  with the first moli release carrying ①④. The 13.11 run: the journal
  docs/operations.md 2026-11-13, the artifacts in var/fixtures-l2/
  (moli-main-f857b6651.snapshot.json, report-moli-main-f857b6651.txt).

- **T-248** [new] — P1, production rollout of BOTH engine containers and
  worker B (spec §4.7 items 2–3 v0.3, §6.2 item 1; depends on T-247,
  T-239, T-240, T-245, T-246). Not code: moli and lightpanda containers with
  pinned versions from T-241 (both — regardless of which candidate goes to
  canary first; deployed for windows and re-tests without
  new deploys), the egress policy of both containers (mandatory — spec
  §4.7 item 2), mem_limit from the T-247 measurements × headroom (initially ≤1 GB
  each); worker B on q:render-fallback (the T-240 profile) runs against an
  empty queue — the wiring check (step 1 of spec §6.2); the T-246 alerts
  live in production; the stage baseline metric window recorded (≥48 h); the
  spec §6.4 rollback rehearsed on the bench.
  Criterion: healthchecks of both engines and worker B green, alerts
  active, the baseline window recorded.

- **T-249** [new] — P1, canary windows, acceptance and locking in the switch
  (spec §5.2, §5.3 v0.3, §6.2–6.4; depends on T-248). The canary is switching
  the main worker to a candidate found ready by T-247; if neither is
  ready now — T-249 does not start, a T-247 re-test is scheduled
  (both engines stay candidates, no cut). Not code: an operator
  run following the steps of spec §6.2: enabling fallback without an engine
  switch (empty queue — the wiring check) → window 1 (one worker on the candidate,
  checkpoints every 4 h: the fallback rate, the share of Laravel verdicts
  against the baseline window, the q:render-fallback depth, engine
  RSS/stability, the PEL of both streams) → window 2 (100% of workers, 48 h) →
  §6.3 acceptance: the fallback balance reconciliation (published = consumed + PEL,
  0 mismatches), the §5.3 thresholds, the T-177 log without new pathologies →
  locking in: the production profile config (engine, caps from the actual
  rate), docs/render.md (profile, rollback runbook, engine version change
  procedure), docs/operations.md (the log), pinned versions. Criterion:
  the §6.2 checklist passed in full, a repeated 48 h confirms
  stability, the decision recorded in the docs; on a threshold failure —
  the §6.4 rollback executed and documented (a valid outcome is “stay on
  chromium”, spec §5.3 item 2).

## Stage 42: the moli v1.1.23 re-test — the upstream fix for Fetch.enable Font (T-272)

Source: the moli release
[v1.1.23](https://github.com/lexmount/moli/releases/tag/v1.1.23)
(2026-11-09, commit `ee0652d`) carrying our PR
[lexmount/moli#2470](https://github.com/lexmount/moli/pull/2470)
“fix(fetch): accept the Font resourceType in Fetch.enable patterns” (fork
`yepbro/moli`, branch `fix/fetch-enable-font-resource-type`, commit
`4729723`; submitted 2026-11-06, the maintainers tagged it for release
1.1.23) and the paired maintainer #2476 “intercept font and media loads
with shared admission”. Reason for the fix: on pinned v1.1.21/1.1.22 the
CDP slice `TestEnginesLiveCDPSlice/moli` is red — `Fetch.enable` with the
patterns of the T-062 base blocking set (Image+Media+Font) answers
InvalidParams → every task fails at startBlock, engine=moli is
unusable (live run 2026-11-06: 0/100 because of the Font pattern;
precedents — the doc comment of `internal/render/engines_live_test.go`,
the log `.opencode/skills/prod-report/references/prod-topology.md` dated
06.11). Procedure — the re-test plan from T-247 (operator decision
2026-11-06, spec v0.3 “watch + smoke per release”), steps —
docs/dev-environment.md, “Changing a pinned engine version” (smoke FR-17/
T-254). An operational task (not code), like T-247…T-249; lightpanda is out
of scope (waiting for upstream #4151 — one browser context per connection).

- **T-272** [done] — P1, operational: bump the moli pin to v1.1.23, the CDP
  slice, a smoke run on the seed hundred, a candidate verdict (input for
  T-247/T-249; the pin infrastructure and smoke procedures — T-241/T-254). Steps:
  1. Bump the pin with the pair `MOLI_VERSION=v1.1.23` + `MOLI_SHA256` (release
     asset `moli-x86_64-unknown-linux-gnu.tar.gz`, sha256
     `df360c04e567ce2e56a8391a03dc677d794465848b5821f944d2de10f3f1f430`)
     in every default location: `deploy/docker/.env.example`,
     `deploy/docker/compose.yaml` (two literals — ARG and the image tag),
     `deploy/docker/Dockerfile.moli` (ARG defaults); on the bench — the pair in
     `.env` (the pin is currently set by the compose defaults; no pair in `.env`).
  2. Rebuild/re-up: `dc --profile render-engines up -d moli` →
     the `crawler/moli:v1.1.23` image (a version tag), the healthcheck
     `/json/version` green, the version in the response matches the pin (the
     slice gate prints it itself).
  3. Layer 1 — the CDP slice: `dc run --use-aliases -e CRAWLER_RENDER_TEST=1
     --rm tools go test ./internal/render
     -run 'TestEnginesLiveCDPSlice/moli' -v -count=1`. Expectation:
     it goes green (InvalidParams on Fetch.enable goes away with
     #2470+#2476; the stage invariants — spec 5.6/FR-5). Additionally run the set with
     `BlockStylesheet` (T-213): if Stylesheet was still not accepted in moli —
     NOT a blocker for the base profile (the option is optional, default false),
     record it as a separate line for the CSS mode. A red slice is a
     stop signal: revert the variable to v1.1.21, analyze the cause; if
     engine-side — an upstream issue.
  4. Layer 2 (with layer 1 green) — a smoke run on the seed hundred per the
     docs/dev-environment.md procedure (steps 3–4 “Real smoke”): `fixtures
     import --force --publish smoke` → the bench render-worker in the
     moli profile (engine: moli + ws://172.28.100.17:9222) → drain
     q:render → `fixtures snapshot --label moli-v1.1.23 --engine moli` →
     `fixtures report` against baseline-chromium. The §5.1 item 4 thresholds
     of the render engines spec (indicative on a hundred; the full 3000×2
     comparison — T-247); the smoke criterion: success rate/throughput without
     degradation against baseline, 0 crashes of the engine core, memory fine;
     between baseline and candidate — restart render-worker and re-run
     `import --force` + `--publish`.
  5. Verdict and recording: “ready/not ready now” against the thresholds — the
     input for T-247 (the full bench comparison) and T-249 (the canary);
     update the precedent doc comment of `internal/render/
     engines_live_test.go` (moli v1.1.23 — green/red), the prod-topology.md
     log line (06.11 “waiting for 1.1.23” → the fact), the
     docs/operations.md log.
  Criterion: the v1.1.23 pin committed in every default location, the slice
  and smoke recorded (snapshots/reports in var/fixtures-render), the verdict and
  doc edits committed; on red — a revert to v1.1.21 and the next
  re-test plan written down (on the next engine release).
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

## Stage 48: production resilience — retries, PG memory, timeout metrics, crawlerctl (T-278–T-281)

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

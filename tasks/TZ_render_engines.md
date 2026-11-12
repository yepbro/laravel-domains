# Technical Specification: Render Engine Selection (Chromium / Moli / Lightpanda) and the Chromium Fallback Queue

Version: 0.3 (v0.2 — fixes of the factual decisions made over the course of
implementation; v0.3 — by the operator's decision of 2026-11-06: both
alternative engines are permanent candidates, the bench comparison is a
periodic retest procedure (the engines are actively developed), candidate
elimination is not provided for; production deploys the containers of both
engines regardless of which one goes into the canary first — §4.7 item 2, §5.3).
The v0.1 primary source — the research of 2026-11-05: a comparison of the CDP
surfaces of `chromedp/headless-shell` 156, Lightpanda 1.0.x and Moli v1.1.x
against the needs of `internal/render`; the research confirmed that both
alternatives cover the entire CDP surface used by the project, including
Fetch interception — T-062/T-213/T-214/T-219/T-230).

Status: draft, no code is written until approval. The document contains no
code or configuration examples — requirements and decisions only.

---

## 1. Functional requirements

### Engine selection

- **FR-1.** The render configuration specifies the engine: `chromium` (default),
  `moli`, `lightpanda`. The default value reproduces today's behavior
  pixel-for-pixel: no changes to the production setup until an explicit
  switch.
- **FR-2.** `engine: chromium` + empty `render.remote_url` — the current
  production mode (an own headless-shell process, the exec mode of `Options`,
  deploy/docker/Dockerfile.render): RSS control (T-065) and recycle
  timers (T-208) work as today.
- **FR-3.** `engine: moli|lightpanda` requires a non-empty `render.remote_url`
  (validated at config build time) and means an external CDP server:
  the engine's container/unit is brought up by the infrastructure, the worker
  only connects. The memory threshold `render.restart_memory_bytes` for these
  engines must be 0 (the RSS of a foreign process is unavailable — as
  already today in the remote mode of the dev stack).
- **FR-4.** Engine specifics are isolated at the "configuration →
  Options" boundary and in the chromedp allocator: the core of the `internal/render`
  package (Session/Pool/extract/block/wait) does not see the engine type;
  branching on the engine inside a session is forbidden. The only allowed
  points of engine differences: allocator parameters (both alternatives need
  a per-engine allocator address mode — v0.2: moli requires the path
  /devtools/browser/<uuid> from /json/version with the authority replaced by
  the configuration address, lightpanda — the address as is; "chromedp.NoModifyURL
  for both" from v0.1 does not work for moli, reproduced on the pinned images),
  the health-check endpoint (all three have
  `/json/version`, unchanged) and documentation.
- **FR-5.** The invariants of spec 5.6 are preserved on all engines: task-scoped
  context isolation (Target.createBrowserContext/disposeBrowserContext),
  capture of the final DOM/cookies/headers (T-064), the SSRF defenses
  (T-219/T-230, Fetch interception), resource blocking (T-062/T-213/T-214),
  a single identifiable crawler UA (spec §9, Emulation.setUserAgentOverride).
  The research of 05.11 confirms support of all the listed CDP surfaces
  by both engines (sources: Moli's `fetch_support.rs`/`target.rs`/`emulation.rs`;
  Lightpanda's `domains/fetch.zig`/`target.zig`/`emulation.zig`;
  chromedp smoke tests in both projects' CI).

### The fallback queue

- **FR-6.** A new queue `q:render-fallback` (Redis Streams, modeled on the
  existing streams of `internal/queue`): tasks that failed rendering on the
  primary engine are collected into it and finished by a separate
  render worker on the Chromium engine.
- **FR-7.** A fallback candidate is a task whose render would be finalized
  without a result for an engine-dependent reason: the task class of T-172
  (isTaskRenderError on the last attempt redis.max_attempts) and the
  tarpit of T-148 (exhaustion of render.timeout_attempts). The engine-independent
  reasons — the DNS class of T-207 (dnsError) and the cert class of T-159 — do not
  get a fallback: Chromium will not fix them.
- **FR-8.** Fallback publication happens once per task, only when the primary
  engine's attempts are exhausted (the redelivery mechanism of q:render
  remains a free retry on the same engine; publishing on the first
  error would multiply copies up to max_attempts).
- **FR-9.** The browser class (ClassBrowser — the primary engine is unavailable) is
  not fallen back per-task. The storm protection is twofold: (a) a health gate —
  fallback publication is active only while the primary engine is healthy
  (by the infra-error streak, modeled on renderFailBackoff); (b) a share budget —
  a cap on the fallback rate via consecutive stage windows of 1000 deliveries
  (v0.2 — instead of a sliding window: enough share statistics
  without startup spikes); when the cap is exceeded the fallback
  is suspended, tasks are finalized as today (the
  render_failed_at marker, T-210), an alert is raised.
- **FR-10.** The fallback result is final. Publication to q:render-fallback and
  the "do not finalize as failed" decision are performed atomically (one
  DB transaction including the publication, modeled on the coupling of finalization
  with the q:analyze publication in renderworker). The render_failed_at marker (T-210) is set
  only after the fallback is exhausted (by worker B), not by the primary worker.
- **FR-11.** The fallback queue consumer is idempotent to repeated deliveries
  (dedup by the record ID — the existing parking/lease
  mechanics of the stage). The task balance is not lost: what is published to the fallback equals
  what is consumed (success + finalization) plus the PEL remainder.
- **FR-12.** The fallback worker is the same renderworker binary in another
  configuration build: the chromium engine, the consumption stream q:render-fallback,
  a separate consumer group. No second worker code is written; the consumption
  stream becomes a build parameter (today the queue plumbing is
  bound to q:render).
- **FR-13.** The fallback worker is rate-limited (records per tick / per window) —
  a batch "collect and finish" mode without scaling up chromium capacity.
- **FR-14.** The fallback is switchable off by configuration without deploying new entities:
  when disabled, the primary worker returns to today's finalization;
  what has accumulated in q:render-fallback is finished by worker B until empty.

### Observability and operations

- **FR-15.** The render stage metrics get the engine label; fallback
  counters are added: publication (by reason — the net::ERR_* code from
  taskFinalizeReason, a closed dictionary), consumption, success/finalization
  by worker B; the depth of q:render-fallback in monitoring — modeled
  on q:render monitoring (XLEN/lag, an alert on growth, an analog of render_debt
  T-175).
- **FR-16.** The dev stack gets compose services for both alternative
  engines (from an image/build — see §4.7) next to the chromium service:
  switching the engine for local development and bench runs is
  a single address edit in the configuration, no code edits.
- **FR-17.** Engine versions are pinned (in the stack's environment variables,
  an analog of CHROMEDP_HEADLESS_IMAGE): changing an engine version is a separate
  procedure with a smoke run (§6.1), not "drifting in with latest".
- **FR-18.** The operational flags of the engines are fixed by documentation and
  reproduced by the stack: Lightpanda — the environment variable that disables
  telemetry (telemetry is enabled by default — unacceptable for a crawler
  over third-party domains), the obey-robots flag is not passed; Moli — without --layout
  (geometry is not needed), resource families are not enabled (images are
  blocked anyway by T-062), optionally --block-private-networks as a second
  layer of the SSRF defenses (not a replacement for T-219/T-230); chromium — unchanged.

## 2. Current state

1. **The render core is engine-agnostic.** `internal/render` speaks CDP
   (chromedp v0.21): Session (an isolated context per task), Pool
   (2–4 contexts), Manager (generations, restarts), result capture
   (extract), blocking (block), network-quiet waiting (wait). Two connection
   modes already exist: external CDP (Options.RemoteURL, the dev stack,
   ws://172.28.100.14:9222) and an own headless-shell (the production render,
   Dockerfile.render). Engine switching in the narrow sense — changing the
   endpoint.
2. **The error classification is ready for the fallback.** render.Class
   (ClassTask/ClassContext/ClassBrowser) + the worker's fine-grained classes: the
   tarpit of T-148, cert T-159, DNS T-207, the task-class finalization of T-172 with
   the render_failed_at marker of T-210 and the reason metric
   crawler_render_task_finalized_total{reason}.
3. **The queue topology is the model for the fallback.** The streams q:dns…q:dead
   (internal/queue), the render-worker consumer group, redelivery
   with redis.max_attempts, PEL/claim, parking dedup, the publication gate of
   q:render (T-174, the QueueHighWatermark watermark), the debt counter of T-175.
4. **Production:** the render worker with an own headless-shell (unit/container,
   the budget ≈2 GB, spec 3.2), the recycle pair of T-208; render scaling —
   a second Chromium on :9223 and a second worker (docs/deploy §2.2).
5. **The economics — the feature's motivation.** The measurements by the alternatives'
   owners (independent ones do not exist): Lightpanda ~123 MiB peak / 100 pages;
   Moli ~92 MiB median per task; versus ~700 MiB–2 GB for Chromium.
   Even adjusted for vendor optimism — an order of magnitude.

## 3. Target state

1. The primary render engine is selected by configuration based on
   measurements (§5); the candidates: moli or lightpanda; the outcome of the choice is not
   predetermined by this spec and may be "stay on chromium". Both engines are
   permanent candidates (v0.3): no elimination is provided for, the engines are actively
   developed — the bench comparison (§5.1) is a periodic
   retest procedure with new pinned versions (the FR-17 smoke test), not a
   one-time selection.
2. Tasks the primary engine could not handle for an engine-dependent reason
   are finished by the chromium worker via q:render-fallback at a
   configurable rate; the pipeline as a whole loses no domains relative to
   today's chromium-only baseline.
3. The production render budget shrinks by the share of tasks the primary engine
   handles on its own; chromium capacity is kept at the fallback-rate
   volume (plus a reserve for primary engine degradation — §4.4 share budget).
4. The `internal/render` core does not branch on the engine; engine specifics live in
   the configuration, the allocator, deployment, documentation.
5. Observability: the stage metrics distinguish engines; the fallback rate is a
   routine health indicator of the primary engine and a signal to update/roll it back.

## 4. Changes

### 4.1 Configuration

1. `internal/config`, the render section: the engine field (an enumeration of
   chromium|moli|lightpanda, default chromium) and the fallback subsection:
   enablement (bool, default false), the worker B rate cap (records per
   window), the fallback share budget (percent per windows of 1000 stage deliveries,
   0 — no cap), the health-gate parameters (the infra-error streak threshold).
2. Validation at config build time: the "engine × remote_url × memory threshold"
   combination (FR-3), the ranges of the new fields — in the style of the existing
   render section checks; the error texts — per the rangeMsg pattern.
3. The worker's consumption stream is a build parameter (default q:render);
   the group/consumer — the existing worker Options fields. The
   q:render-fallback constant goes into internal/queue next to the other streams
   and into allStreams (trim/claim/monitoring pick it up
   automatically, modeled on T-025 MAXLEN: the fallback stream length cap
   is set by the same mechanism — the value 15 000 (a Redis budget
   invariant: the sum of the caps ≤ 480 MiB at the nominal q:dns batch; 150 000
   would break the budget; v0.2 — fixed as a factual implementation decision,
   the coupling with the control rate of §5.3 — in docs/redis.md).
4. config.example.yaml, docs/configuration.md, deploy/docker/config.dev.yaml
   — document the fields and the three engine profiles (no examples here).

### 4.2 The render package

1. Options is extended with the engine attribute (or a derived
   "external alternative engine" flag); it is applied only in the allocator wiring
   (v0.2 — the per-engine address mode: moli resolves the path
   /devtools/browser/<uuid> from /json/version and replaces the authority
   with the configuration address; lightpanda uses the configuration address as
   is; NoModifyURL from v0.1 does not work for moli) and in validation.
   The exec mode remains exclusive to chromium.
2. No changes to Session/Pool/Manager/extract/block/wait.
   The worker health check (CDP availability) does not change: /json/version
   is supported by all three engines.
3. Fixing CDP quirks of a specific engine discovered on the bench is allowed —
   only as allocator/configuration parameters with a justification
   in a comment; the list of expected quirks — §7.

### 4.3 The fallback queue

1. The new q:render-fallback stream; the message format is the same
   queue.Result contract as q:render's (the message is self-contained, no
   conversion needed).
2. A MAXLEN cap modeled on q:render (T-025); inclusion in allStreams
   provides the trim/reliability machinery; depth monitoring — §4.6.
3. The fallback publication gate is not coupled to the q:render watermark
   (T-174): the fallback is published by the worker, not the http-worker; its own
   caps are the share budget and the worker B rate.

### 4.4 Fallback publication in renderworker

1. The injection point — the finalize-without-render paths: the T-148 tarpit and the
   T-172 task class (the last attempt). Instead of setting render_failed_at
   the worker publishes the task to q:render-fallback before the delivery's XACK (v0.2 —
   the "publish → XACK" order with at-least-once instead of the single DB
   transaction of v0.1: observably equivalent — a failure of any step leaves
   the record unmarked and in redelivery) and
   leaves the record unmarked (the intermediate state = "waiting for
   the fallback", clearly distinguishable in logs/metrics; no separate column is
   introduced — the state is expressed by the absence of the marker and the
   queue counter).
2. The reason filter: DNS T-207 and cert T-159 are finalized as today —
   no fallback. The tarpit and the task class — with a fallback (rationale: these are
   properties of the "site × engine" pair — a page that hung on the light engine
   may open in chromium; DNS/cert are host properties).
3. The health gate: with an infra-error streak (ClassBrowser) fallback publication
   is suspended until recovery — tasks wait in the q:render redelivery;
   this rules out overflowing the whole flow onto the expensive engine when
   the light one falls.
4. The share budget: when the share of fallback publications over a window of 1000 deliveries exceeds
   the cap — the fallback is switched off until the end of the window, tasks are finalized
   with the marker as today, an alert. The cap is configurable; the initial
   value comes from the bench run of §5.1.
5. Idempotency: a repeated delivery of the same record (after a publication
   failure) must not produce a second fallback entry — bind to
   the result record ID in the publication decision (a duplicate check
   modeled on the parking dedup); a duplicate in the stream is allowed and
   is filtered out by consumer B (FR-11).

### 4.5 The fallback worker (the Chromium engine)

1. The same binary: the configuration — engine=chromium, the stream
   q:render-fallback, a separate consumer group, the rate per FR-13.
   The local headless-shell (today's render image) unchanged.
2. Success — the normal path: the DOM in storage, rendered=true, publication to
   q:analyze. An engine-dependent failure on attempt exhaustion — finalization
   with the render_failed_at marker and the reason (a recursive fallback of the fallback
   is forbidden: the stream is always one, the engine is always chromium).
3. Production deployment follows the primary engine's readiness for the canary
   (§5.2): before that worker B runs with zero flow (the queue
   empty) and serves as a plumbing check.

### 4.6 Metrics and alerts

1. The engine label on crawler_render_records_total and
   crawler_render_task_finalized_total (cardinality — 3 values).
2. New: fallback publication (by reason — the taskFinalizeReason dictionary),
   consumption/success/finalization by worker B, health-gate and
   share-budget suspensions (an event counter + the reason).
3. The q:render-fallback depth (XLEN/lag) in monitor/dashboards; alerts:
   depth growth (worker B is not keeping up), the share budget being exceeded
   (primary engine degradation), an engine unit failure.
4. The render stage dashboard — an engines panel: the rate, the success rate, memory
   (the engine unit's RSS — already in the system metrics), the fallback rate.

### 4.7 Infrastructure and deployment

1. Dev stack: moli and lightpanda services (port/address per the app-network
   addressing plan; the lightpanda image is the official one, with the telemetry-disabling
   variable; moli — an own minimal image built from the release tarball,
   there is no official image; versions — stack environment variables, FR-17).
   Healthcheck — /json/version modeled on the chromium service.
2. Production: the containers of BOTH engines are deployed — moli and lightpanda
   (pinned versions), next to the render worker (v0.3: regardless of which
   candidate goes into the canary first — both are available for windows and
   retests without new deployments; budgeting: the memory limit is the bench
   measurement × a margin, initially ≤1 GB each);
   the egress policy of the engine containers is mandatory (docs/render.md already
   requires it for Chromium — extend it to the alternatives). A container
   idles outside the canary — the costs are idle memory and version updates only.
3. systemd units: handled per the existing patterns
   (crawler-chromium.service + the recycle pair of T-208 — for the
   chromium worker only; the moli/lightpanda engines do not need recycling —
   restarts are covered by the worker's generation Manager + the fallback health gate).
4. Engine update (a pinned version change): the bench smoke run of §6.1
   (a reduced sample) → the canary window of §5.2 (reduced) →
   expansion. An engine rollback — restoring the version variable.

### 4.8 What does not change

The render core (§4.2), the q:render/q:analyze message contracts, the
T-174 gate, deduplication/parking, the SSRF defenses and blocking (they work on all
engines — FR-5), the content storage and the analyzer rules, the DB schema
(no new columns/tables — the fallback state lives in the queue and the metrics).

## 5. Primary engine selection (the decision procedure)

The decision is made on measurements, not on preference; the "stay on
chromium" outcome is a valid result of the procedure.

### 5.1 Bench comparison (a mandatory stage)

1. The bench: the dev stack (CoreDNS+unbound+Caddy+mockserver are not needed — rendering
   goes to the real internet; perimeter isolation — per the
   dev-environment rules), the fixtures of §6.1, three engine profiles.
2. Each engine runs the same sample twice (a variability control for live pages);
   the baseline is a fresh chromium run on the same sample (not the
   production content of past episodes).
3. Comparison metrics: the share of successful renders; finalization reasons
   (the net::ERR_* dictionary); the rate (pages/s, mean/p95 page time); memory
   (the engine's RSS); the share of Laravel detection discrepancies — the diff of analyzer verdicts
   on the engine's DOM against the baseline chromium per the T-213 protocol; the DOM quality diff — a normalized comparison (detection
   signatures + structural metrics: the DOM length, the node/script counts),
   a byte diff is not applicable (a live DOM is non-deterministic).
4. Canary admission thresholds: detection discrepancies <1% (T-213);
   the success rate of "engine + simulated fallback" ≥ the chromium baseline minus
   engine-independent losses (DNS/cert); the rate no worse than baseline;
   memory steadily <⅓ of the chromium budget; 0 core engine crashes
   on the sample.

### 5.2 Canary windows in production

1. Worker B (chromium, q:render-fallback) is deployed and runs on an
   empty queue; the §4.6 alerts are active; a baseline period of stage metrics is recorded
   (at least 48 h).
2. Window 1 (24–48 h): one render worker (or a share of workers) — on the
   candidate engine, the fallback enabled; the other workers — chromium.
   Monitoring: the fallback rate, the deviation of the Laravel-verdict share against
   the baseline period, the q:render-fallback depth, the memory/stability
   of the engine unit, the stage rate.
3. Window 2 (48 h): the share expands to 100% of render workers on the candidate.
   Worker B keeps finishing the fallback.
4. For the second candidate (if the first passed) — the same windows after
   returning to the baseline configuration and metric stabilization for ≥24 h;
   running two engines in a canary at once is forbidden (it pollutes
   the attribution of the fallback flow).

### 5.3 Selection criteria and thresholds

| Criterion | Acceptance threshold | Rejection threshold |
|---|---|---|
| Laravel detection discrepancies against the baseline window | <1% (T-213) | ≥1% persistently |
| Fallback rate (median over window 2) | ≤15% | >30% |
| End-to-end stage success rate (candidate+fallback) | not below the baseline window | below the baseline |
| Stage rate | not below the baseline window | below the baseline |
| Engine memory (sustained) | <⅓ of the chromium budget | ≥½ of the budget |
| Stability (engine unit crashes) | 0 unplanned | any recurring |

1. If both candidates pass, the better one by the combined score is selected
   (primarily — the fallback rate and memory; on a tie — Moli's permissive
   license and vendor independence against Lightpanda's AGPL restrictions —
   modifying Lightpanda would require opening the sources of the modifications) —
   for the FIRST canary; the other remains a permanent candidate for
   the next window (v0.3: no elimination is provided for).
2. Neither passes now — the primary engine remains chromium; both
   candidates are kept (v0.3), the selection and fallback infrastructure has
   zero cost of ownership with engine=chromium (FR-1); a T-247 retest is scheduled
   with new pinned versions (the engines are actively
   developed, the periodicity — per releases/operator decision).
3. The decision is recorded in docs/render.md and the production config profile;
   the dates and metrics of the windows — in docs/operations.md (the change log).

## 6. Acceptance testing

### 6.1 Local bench: domain fixtures from production

1. **The sample**: 3000 subdomain_results records that are real q:render tasks (the SPA verdict of T-057, http_published), stratified:
   ~2000 random chromium successes (the render passed earlier), ~500
   problematic (tarpits/timeouts/T-172 finalizations of recent weeks —
   potential fallback customers), ~500 SPA-heavy (a large DOM after
   render). Zone stratification — proportional to the production share.
2. **Production export**: an SQL export of the zones/domains/
   subdomain_results rows (the fields the render task needs: identifiers,
   final_url, the episode's cookies/headers, alias_kind); the content storage is not
   exported (the DOM is written by the render itself; the baseline is taken on the bench).
   The export is anonymized as much as debugging allows (domain
   URLs are public by the task's definition).
3. **Load into the test DB**: the dev stack migrations + a fixture dump import
   (the tool is one-off, lives in testdata or scripts; the format is
   agreed with the team; the load is idempotent). The test DB is isolated
   from production (a separate dev stack instance).
4. **Isolated stage run**: tasks are published to q:render directly
   (by a publication script from the fixture; the dns/http stages are not involved),
   the render-worker in the profile of the engine under test; the analyzer is enabled — its
   verdicts are needed for the detection diff. The fallback worker is enabled for
   an end-to-end check of the full chain (light → fallback → chromium).
5. **Bench readiness criteria**: the fixture loads with one command;
   an engine run is reproducible (two runs — the variability control);
   the comparison report (§5.1 item 3) is generated from the metrics and tables
   automatically.
6. The same fixture (reduced, ~300 domains) — the smoke test for engine pinned-version
   changes (FR-17).

### 6.2 Production acceptance test

Participants: the operator (execution), the on-call engineer (alert watching).
Duration: 4–6 days total (the windows of §5.2); the steps:

1. **Preconditions** (a checklist): worker B deployed, the consumer group of
   q:render-fallback active, the queue empty; the candidate engine deployed with a
   pinned version and an egress policy; the §4.6 alerts armed;
   the baseline metrics window recorded; the rollback (§6.4) rehearsed on
   the bench.
2. **Step 1 — enabling the fallback without changing the engine** (2–4 h): the primary
   engine is chromium, fallback publication enabled. Expectation: the
   q:render-fallback queue stays empty (chromium does not fall back to itself —
   publications come only from the candidate engine; a plumbing check without
   flow). The task funnel converges, no alerts.
3. **Step 2 — the engine canary** (window 1 of §5.2): one worker switched to the
   candidate. Checkpoints every 4 h: the fallback rate, the share of
   Laravel verdicts against the baseline window, the q:render-fallback depth,
   the engine's RSS/stability, the PEL of both streams. Immediate rollback —
   on the §6.4 triggers.
4. **Step 3 — expansion** (window 2 of §5.2): 100% of render workers on the
   candidate; worker B finishes the fallback at the capped rate; monitoring of the daily convergence:
   the q:render-fallback depth reaches a plateau, the fallback rate is stable, the detection drift is within tolerance. The window invariant
   (v0.2, from the review extra round): the q:render-fallback depth stays
   far below the MAXLEN cap of 15 000 — a stationary depth near
   the cap with worker B alive means its rate is below the inflow; near the cap
   XADD starts trimming undelivered entries (tasks drop out of the balance;
   recovery is only via the T-030 recheck).
5. **Step 4 — acceptance**: the balance check over the whole period: the tasks
   published to the fallback equal the consumed ones (success + finalization) +
   the PEL remainder, zero discrepancies; the final metrics against the §5.3 thresholds;
   the slow-stages log (T-177) without new pathologies; the memory consumption
   of the render perimeter within the plan.
6. **Step 5 — recording**: the production profile config is updated (the engine, the fallback caps from the actual rate), docs/render.md and operations.md
   are extended (the engine profile, the rollback runbook, the version change procedure),
   the pinned versions are fixed.

### 6.3 Acceptance success criteria

All items of §5.3 (the acceptance threshold) based on the window 2 results; the fallback balance
converges (step 4); zero emergency alerts except the investigated and closed ones;
a repeat run (the next 48 h in normal mode) confirms
metric stability.

### 6.4 Rollback

1. **Partial** (the engine is worse, the fallback mechanics are healthy): the worker profile returns to engine=chromium; the accumulated q:render-fallback
   is finished by worker B until empty; fallback publication dies out on its own
   (chromium does not publish to the fallback — step 2 of §6.2).
2. **Full** (the fallback mechanics are under suspicion): the fallback is disabled by configuration
   (FR-14), the workers — chromium; the q:render-fallback remainder
   is finished by worker B at a manual rate, or the tasks are finalized by the normal recheck procedure
   (the T-030 episode will re-issue the renders).
3. The rollback requires no DB migrations (§4.8) and loses no tasks (the balance check
   of step 4 is mandatory during a rollback too).

## 7. Risks and open questions

1. **The maturity of the alternatives** (the main one): Moli — the repository was created
   2026-08-10, v1.1.x; Lightpanda — 1.0 of 2026-10-02. Both execute
   the hostile JS of arbitrary sites. Mitigations: pinned versions, the canary,
   the health gate + the fallback budget, the stage's defensive architecture
   (classification/restarts) already in the code.
2. **Anti-bot/challenges**: some domains will always go to the fallback — that is the
   normal mode (that is what chromium remains for), but a fallback rate above
   ~30% wipes out the economics (the §5.3 threshold).
3. **CDP quirks of the live flow**: the bench of §6.1 does not cover all the exotic cases;
   discovered quirks are fixed as configuration parameters (§4.2) with an
   issue filed upstream to the engine.
4. **The T-063 timings** (network quiet) may differ on the light engines
   (a different network stack) — monitor the stabilization time on the bench;
   if needed, a per-engine multiplier for the Idle timeout — only through
   the existing parameters, without branching in the code.
5. **The Lightpanda license (AGPL-3.0)**: internal use as
   a separate process is allowed; modifying its sources without
   opening the changes is not; the policy is fixed in docs/render.md.
   Moli (Apache-2.0/MIT) adds no restrictions.
6. **Lightpanda telemetry** — disabled by an environment variable (FR-18);
   the check — at the deployment health stage (no outbound traffic
   except working connections; the container egress policy covers this too).
7. **Open question**: is a record's "intermediate state" (waiting for the fallback) longer than a day
   allowed with worker B stopped — a proposal: an alert on the PEL age of q:render-fallback; the episode recheck deadline of
   T-030 covers the loss.
8. **Open question**: the stream name (q:render-fallback vs
   q:render-chrome) and the addresses/limits of the engine containers in the dev stack —
   to be decided at implementation time, outside this spec.

## 8. Work breakdown

| # | Block | Content | Dependency |
|---|---|---|---|
| 1 | Config+core | engine in the config/validation, NoModifyURL, the engine label in the metrics, the worker's consumption stream | — |
| 2 | Dev infrastructure | moli/lightpanda compose services, pinned versions, healthcheck, the own moli image | 1 |
| 3 | Fixtures | export/load/publication of the 3000-record sample, the comparison report, the 300-record smoke sample | — |
| 4 | Bench A/B | three engines × the sample, the §5.1 metrics, the canary admission decision | 2, 3 |
| 5 | The fallback queue | the stream+allStreams+MAXLEN, publication from the T-148/T-172 paths, filters, the health gate, the budget, transactionality | 1 |
| 6 | Worker B | the chromium/q:render-fallback configuration build, the rate, dedup, deployment | 5 |
| 7 | Observability | the engine and fallback metrics/alerts/dashboard | 5, 6 |
| 8 | Canary+acceptance | the windows of §5.2, the acceptance of §6.2, recording the choice, documentation | 4, 6, 7 |

Stages 1–3 are parallelizable; the canary (8) does not start without the decision of 4 and
the deployed 6–7. Unit/integration tests of each block — by the measure of
the internal conventions (unit tests of the filters/validation; integration —
the queue plumbing and worker B on the dev stack; the acceptance map — update
to the fallback scenario).

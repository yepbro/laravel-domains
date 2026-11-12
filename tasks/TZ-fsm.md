# Technical specification: formalization of the pipeline's implicit finite-state machines

Version: 0.1 (primary source — the codebase audit of 2026-11-06: a full
crawl over the packages, ~40 implicit machines; key findings and
`file:line` references are included in the registry §2 of this document).

Status: draft; no code is written until approval. The document contains
no code examples — requirements and decisions only.

---

## 1. Context and problem

The codebase is built on the approach “state = a derivative of SQL guards,
bits and Redis contents”: there is not a single explicit finite-state machine (FSM).
The approach works thanks to the discipline of idempotency (spec 3.4), but the audit
exposed a systemic problem: **every new feature adds one more
independent variable to the already existing machines** (T-108 added the bit
`http_published`, T-164 — `dead_host_cutoff`, T-175 — the debt counter,
T-202 — maturity buckets, T-204 — the bucket guard), and every new rule
requires a manual cross-check of all the variables. Places were found where one business
concept is encoded by 5–7 independent variables (§2), transitions without a
terminal state (the eternal PEL loop), and silent losses (MAXLEN trimming).
The consolidated principle is fixed in `AGENTS.md` (“Rules”): behavioral logic
is designed as an explicit machine.

The goal of this spec is to formalize the **twelve** highest-priority
machines (audit P0–P2): for each — an explicit specification of states and
transitions, invariant tests, and targeted fixes for the defects confirmed by
the audit. The remaining machines (P3–P4) are out of scope of this spec (§6).

Formalization does **not** mean a rewrite onto an FSM framework: transitions
remain SQL statements with guard predicates (the project's proven
model). Formalization means: (a) the specification “states × events ×
allowed transitions × side effects” lives in the doc comment of the package
next to the code; (b) the machine invariants are verified by tests (unit —
the transition table of the decision logic, integration — the behavior of the
DB/Redis statements); (c) the targeted defects get fixed.

## 2. Registry of spec machines (in descending audit priority)

| # | Machine | State encoding | Key audit defects |
|---|---|---|---|
| 1 | domain `check_state` | `domains.check_state` × `queued_at`; writers — 5 packages (`scheduler/enqueue.go:83-117`, `scheduler/lease.go:129-174`, `scheduler/recheck.go:112-127`, `scheduler/retry.go:158-201`, `cmd/crawlerctl/recheck.go:108-127`, `cmd/crawlerctl/manage.go:255-283`, `dnsworker/result.go:155-246`, `httpworker/record.go:117-128`, `ingest/apply_add.go:119-130`) | queued with NULL `queued_at` — outside the lease pass forever (`lease.go:129-135`); queued with no task in Redis — self-heals in 30 min |
| 2 | domain `status` | `domains.status` × `temp_attempts`; writers `dnsworker/result.go:155-201,228-246`, `httpworker/record.go:117-128`, `ingest/apply_delete.go:88-130`, `scheduler/retry.go:189-201` | the “stuck status” new/done on q:http loss — healed by the T-270 compensator (the early recheck, row R15 of the `check_state` specification); the DNS/HTTP idempotency asymmetry (`record.go:66-74`) absorbs a duplicate task from a revived worker |
| 3 | the `http_published` bit | `domains.flags` bit 2 (`dnsworker/result.go:203-214`) | the “XADD without the bit” window — a duplicate q:http; sticking on done |
| 4 | the `render_pending` bit × counter | `domains.flags` bit 1 × `crawler_counters['render_pending']` (`httpworker/renderq.go:141-161,263-287,725-746`) | double encoding of debt; “orphan bits”; the draft flag/keep |
| 5 | row render state | `subdomain_results.rendered/render_failed_at` + q:render/q:render-fallback messages (`renderworker/worker.go:174-245,2137-2140`) | the “awaiting fallback” state without a column; loss of MAXLEN-truncated tasks — a record indistinguishable from “not tried” |
| 6 | queue delivery | the Redis PEL × RetryCount (`queue/reliab.go:375-450,593-626`, `queue/recover.go:175-211`) | **the eternal PEL loop without task_id** (`reliab.go:593-597`); a burned attempt on an XPENDING error outside Refund (`reliab.go:429-439`) |
| 7 | render outcome policy | error class × PEL delivery number (`renderworker/worker.go:1805-1980`) | one variable (RetryCount) for two policies — mixed series finalize early |
| 8 | worker reader modes | `failed/infraDown/failStreak/infraStreak/rateLim/Alive` (`httpworker/worker.go:1310-1501`, `renderworker/worker.go:1523-1788`) | 6 variables encode 5 modes; different delivery-panic semantics |
| 9 | browser generation | `Manager.b/p/closed` × `Browser.lost/probeFails/ctx` × `Pool.closed` (`render/manager.go:36-50,173-273`) | “generation alive” — 5 variables reduced imperatively |
| 10 | monitor alerts | `Alerts.state[name]` + track memory (`monitor/alerts.go:1060-1223`) | alert state spread over 2–3 variables, consistency maintained manually |
| 11 | content refcount | `contents.refcount` (`content/pack.go:76-104`, `content/retention.go:280-291`) | “dead” in two representations (tombstone/deleted); negativity is excluded only procedurally — no CHECK in the schema |
| 12 | scheme check state | `SchemeCheck` 7 fields × `Record` 5 content fields (`httpworker/worker.go:313-391,1056-1244`, `httpworker/store.go:133-224`) | invariants maintained manually; a cache hit distorts `final_url` |

Out of scope (§6): the DNS/HTTP stage classifiers (ApexStatus, DecideEpisode,
CNAME/AAAA, wildcard, ErrorClass, redirect-chain, variant/alias/SPA, cutoff),
staging/watch ingest, zones/collapse, the render micro-machines (session,
blockPump, waitLoop, remoteAllocator), the zstd dictionary, disk, notify
episodes, the dashboard ready-latch, the rule reloader, the service skeleton.

## 3. Functional requirements

The FR numbering maps one-to-one onto the plan tasks (T-255…T-266,
`tasks/tasks.md`, stage 37): one machine — one task.

### Machine 1: `check_state` (T-255)

- **FR-1.** The `check_state` transition specification (pending → queued →
  done, including the lease return, the daily recheck, the temp_error retry, the
  manual recheck, resurrection) is written as a “state × event × guards ×
  target state × side effects (zone deltas, clearing `http_published`,
  resetting `queued_at`)” table in the doc comment of the `internal/scheduler`
  package (the single source of truth; the worker writers reference it in one
  line).
- **FR-1.2 (fix).** Rows with `check_state = queued` and NULL `queued_at`
  (the broken T-028 invariant — today an eternal hang: NULL is filtered out
  by comparison, `lease.go:129-135`) get a terminal safety net:
  a repair pass of the same `LeaseWatch` loop — a batched select of such rows
  (LIMIT, a separate statement that does not change the shape of the main
  lease query or the usability of `domains_lease_idx`) and a move to pending
  with the same guard statement; the `crawler_scheduler_lease_null_returned_total`
  counter (a non-zero steady state — a signal to the operator, runbooks). The
  behavior of regular leases is unchanged.

### Machine 2: domain `status` (T-256)

- **FR-2.** The `status` transition specification (8 states, spec 6.2):
  a decision table “stage × condition → status” (DNS terminal branches,
  “addresses present → HTTP decides the status”, the HTTP outcome
  alive/temp_error/no_http, invalid, deleted, resurrection, retry finalization) in
  the doc comments of `internal/dnsworker` and `internal/httpworker` (the
  common part — one table in `internal/model/enums.go` next to the codes).
  Deliberate asymmetries are recorded in the specification as decisions and are
  not changed: temp_error→delegated_no_address when attempts are exhausted
  (`retry.go:20-28`), connect-refused→no_http versus
  connect-timeout→temp_error (`record.go:777-787`), the missing
  `check_state <> done` guard on the HTTP stage (`record.go:66-74`).
- **FR-2.2 (observability).** The “stuck status” anomaly — domains
  `status = new AND check_state = done` with a fresh `last_checked_at`
  older than N hours (default 24 h) — is observable: the
  `crawler_domains_new_done_stale` gauge in the `monitor/zonestats.go`
  pass (over counters, no seq scan over domains — the reconciliation window)
  and the `domains_new_done_stale` alert (a streak of 2 passes, stationarily
  zero in the normal case). Self-recovery — the T-270 compensator
  (stage 40): the hourly scheduler pass `scheduler/stalerequeue.go`
  returns new∧done older than 24 h to `pending` (the R15 transition of the
  `check_state` specification; a cheap probe over the `new_done_stale` row —
  domains are not scanned in steady state; a duplicate q:http from a
  revived worker is absorbed by asymmetry A3) — the gauge/alert remain the
  signal about new losses.

### Machine 3: the `http_published` bit (T-257)

- **FR-3.** The bit lifecycle specification (4 transitions: setting after
  XADD to q:http; clearing by recording the result; clearing by a scheduler
  enqueue; skipping publication on redelivery) in the
  doc comment of `internal/dnsworker/result.go` next to
  `resultMarkPublishedSQL`; an integration test covers all four
  transitions with one scenario chain (including the “XADD without the bit”
  window — a duplicate publication is allowed and documented). Behavior is
  unchanged.

### Machine 4: the `render_pending` bit × debt counter (T-258)

- **FR-4.** The debt machine specification (states `no-debt → debt →
  published → cleared` + the special `draft flag`/`keep`/`orphan bits`;
  the invariant “the bit and the counter change only in one transaction”,
  RowsAffected — the indicator of a real transition) in the doc comment of
  `internal/httpworker/renderq.go`. Integration tests: ±1
  pairing on all transitions, including clearing the bit on delete
  (`apply_delete.go:88-185`, two statements in one transaction — T-269)
  and a redelivery with the bit already set (a second +1 is forbidden by the
  guard). Behavior is unchanged.

### Machine 5: row render state (T-259)

- **FR-5.** The row state specification (`new episode → queued →
  rendered | impossible | awaiting fallback | detached from the task`) in
  the doc comment of `internal/renderworker/worker.go`; the columnless
  “awaiting fallback” state is recorded in the specification explicitly, with
  the mechanism for observing it (the published/consumed/buried balance).
- **FR-5.2 (schema invariant).** `rendered = true ⇒ render_failed_at IS NULL`
  — a CHECK constraint on `subdomain_results` (a migration, `NOT VALID` without
  a table scan; VALIDATE — a separate step per the policy after a soak period;
  the invariant is already upheld by the SQL guards). A paired test invariant in
  the integration tests.
- **FR-5.3 (fix, the late republisher).** “Forever young” records —
  `rendered = false AND final_url IS NOT NULL AND render_failed_at IS NULL`
  older than X hours (default 6 h), the domain has no `render_pending` bit
  (those are handled by the debt pass) and there are no unconsumed messages
  in q:render and q:render-fallback (an interlock modeled on
  `content/retentionpending.go`) — are republished into q:render by a
  background pass (location — `internal/httpworker` next to the rework
  pass; a LIMIT batch, a minute-level interval, the metric
  `crawler_render_late_republish_total`). Heals the loss of
  MAXLEN-truncated fallback tasks and lost publications: the state of a
  record stops depending on the survival of a message in the stream.
  Duplicates are bounded by the interval and absorbed by consumer
  idempotency (spec 3.4).

### Machine 6: queue delivery (T-260)

- **FR-6.1 (fix, terminal state).** A PEL entry without `task_id` (outside
  the package formats) gets a terminal branch instead of the eternal loop:
  `bury` (`queue/reliab.go:593-597`) publishes into q:dead an entry with
  the synthetic id `unknown:<stream>:<entry-id>` (no domain fields),
  acknowledges the original entry (XACK) and increments the counter
  `crawler_queue_dead_unidentified_total` — the entry leaves the PEL,
  triage stays manual (q:dead is a quarantine). Existing entries with
  task_id are handled as before.
- **FR-6.2 (fix, Refund).** An XPENDING read error after a claim
  (`reliab.go:429-439`) no longer burns an attempt irrecoverably: entries
  that never got a classification are included in the `Refund`
  return (a RETRYCOUNT rollback) by the same `RecoverLoop` pass — the T-222
  window widens from “ctx canceled” to “any unfinished classification”.

### Machine 7: render outcome policy (T-261)

- **FR-7 (fix).** The tarpit-finalization decision is made by the number of
  tarpit-classified deliveries of an episode, not by the overall PEL delivery
  number: the tarpit counter — a Redis set `render:tarpit:{task_id}`
  with members = delivery ids (SADD is idempotent per delivery) and a TTL
  of 2× the retry horizon (default 48 h); the threshold — the existing
  `render.timeout_attempts` (SCARD). Mixed series (an infra failure between
  tarpits) stop finalizing early (`renderworker/worker.go:1805-1808`); the
  overall `redis.max_attempts` limit keeps working by delivery number. A
  Redis loss degrades to the current behavior (an empty counter — the decision
  by delivery number).

### Machine 8: worker reader modes (T-262)

- **FR-8.** A discrete reader mode (reading | browser pause | infra pause |
  breaker pause | rate pause) — one enum in each worker
  (`internal/httpworker`, `internal/renderworker`): transitions
  are computed by one function over the observations (`Alive`, `Writable`,
  `failed`, allowance), the transition table lives in the doc comment and a
  unit test (observations → mode); the priority order is no longer set by
  the order of ifs. The semantics is preserved: a browser/infra pause does
  not read the stream (attempts are not burned), the breaker reads. Panic
  parity: the http-worker delivery goroutine gets the same recover
  accounting (`failed.Add(1)`, sample — `renderworker/worker.go:1756-1762`)
  instead of crashing the process.

### Machine 9: browser generation (T-263)

- **FR-9.** Generation liveness is reduced to one predicate method with
  an enumeration of states (absent | starting | active | draining | dead |
  closed) instead of five independent checks (`render/manager.go:173-225`):
  `swapReasonLocked` and `Alive` use it, the doc comment holds
  the transition table (replacement reasons, side effects — restart, drain,
  logs). Behavior is unchanged; unit tests on the fixed browsers (cdpfake)
  cover all states.

### Machine 10: monitor alerts (T-264)

- **FR-10.** Each alert is a value of a single “{name, status, track
  memory}” struct with one `transition` transition (the existing
  `monitor/alerts.go:1201-1223`): the track types (threshold hysteresis,
  streak, level latch, ring window) are typed, the eval functions return
  an observation instead of writing state directly. The behavior of all 18
  alerts is unchanged (table tests: a sequence of observations → the expected
  sequence of statuses, one per machine; the existing tests
  migrate to this format without changing expectations).

### Machine 11: content refcount (T-265)

- **FR-11.** A CHECK constraint `contents.refcount >= 0` (a `NOT VALID`
  migration; VALIDATE — per the policy after a soak period): negativity
  stops being excluded only procedurally. The machine specification (absent →
  live(n) → dead(tombstone|deleted) → deleted, resurrection via
  `ON CONFLICT EXCLUDED`, the `GREATEST` clamp) in the doc comment of
  `internal/content/pack.go`; the two representations of “dead” (a
  tombstone row versus a missing row) — as a table in `docs/content.md`.
  Invariant tests: resurrecting a dead row, the clamp on desync, forbidding a
  double Delete.

### Machine 12: scheme check state (T-266)

- **FR-12.** The allowed combinations of the `SchemeCheck` fields (7 fields)
  and the content part of `Record` (5 fields) are centralized in
  struct validator methods (`validate()`), called at stage boundaries
  (before `ContentSaver`, before recording) — the invariants
  `ContentNotStored ⇒ hash = nil` and `AliasNone ⇔ alias_id = nil` stop
  being checked piecemeal; the unit test is a matrix: all legal combinations
  are listed explicitly (including `Available=true ∧ ErrClass=ErrBody`, a
  cache hit, DeadDial), the illegal ones are rejected. The distortion of
  `final_url` on a cache hit (`worker.go:1072` — the final value is replaced
  by the start URL) is recorded in the doc comment as deliberate behavior and
  is not changed.

## 4. General principles

- **Specification format** (for all FRs): a Go doc comment with a
  “states / events / transitions (guard → target) / side effects” table;
  it lives in the machine's owner package; writers from other packages
  reference the specification instead of duplicating it.
- **Invariant tests**: unit — the transition/decision tables of pure
  functions; integration — the behavior of the DB/Redis statements (modeled
  on the existing package tests).
- **Behavior changes** are allowed only in the explicitly marked items
  (FR-1.2, FR-2.2, FR-5.2/5.3, FR-6, FR-7, FR-8 recover parity) and
  must: preserve the spec 3.4 contracts (at-least-once, idempotency),
  be covered by tests before/after, and not change observable metrics except
  the added ones.
- Migrations (`NOT VALID` CHECKs) do not scan large tables at creation
  time; VALIDATE — a separate step per the migration policy.
- Documentation (`docs/`) is updated in each task within its area.

## 5. Stages and dependencies

Stage 37 of the plan (`tasks/tasks.md`), tasks T-255…T-266, one machine —
one task. Parallelism waves (disjoint files):

- **Wave A** (simultaneously): T-255 (scheduler), T-257 (dnsworker),
  T-259 (renderworker + httpworker/renderq), T-260 (queue), T-263
  (render), T-265 (content).
- **Wave B** (after A): T-256 (model/dnsworker/httpworker docs +
  monitor; after T-257/259 — shared doc files), T-258 (httpworker/renderq;
  after T-259), T-261 (renderworker/worker.go; after T-259), T-264
  (monitor/alerts; the files do not overlap with T-256), T-266 (httpworker
  worker/store/record).
- **Wave C**: T-262 (the readers of both workers; after T-258, T-261, T-266).

## 6. Out of scope (audit P3–P4 — documented on first touch)

The DNS/HTTP stage classifiers (ApexStatus, DecideEpisode, the chase of
CNAME/AAAA, wildcard, ErrorClass, the redirect chain, variant/alias/SPA,
dead-host cutoff), the file machine and staging ingest, the watch tracker,
zones (is_active, collapse), the render micro-machines (session, blockPump,
waitLoop, remoteAllocator, distributed task retry), the zstd dictionary, disk
pause, Telegram notification episodes, the dashboard ready-latch, the rule
reloader, the service skeleton, the DNS client (exchange/fallback/adaptive).
For them the audit records: localized, behavior deliberate; decision-table
documentation is added within the regular task cycle on first touch.

// Render outcome policy FSM (T-261, spec tasks/TZ-fsm.md §3 FR-7): the
// single source of truth for the decision of "how a q:render /
// q:render-fallback delivery with a render error ends". The state is
// encoded by the error class (the certError T-159 / dnsError T-211 /
// tarpitError T-148 / isTaskRenderError T-172 / infrastructure
// classifiers) and two budgets: the episode tarpit budget — the
// cardinality of the application-level Redis set render:tarpit:{task_id}
// (queue.TarpitCounter) — and the overall delivery budget — the entry's
// PEL counter (redis.max_attempts, the delivery FSM —
// internal/queue/deliverystate.go). The formalization does not rewrite
// the decision on the FSM framework (TZ-fsm §1): it remains the guard
// predicates of processDelivery; this table is what the single writer
// (processDelivery) and the observability (outcome metrics) check
// against. Related FSMs: the row's render state — worker.go (T-259), the
// domain check state — internal/scheduler/checkstate.go (T-255).
//
// # Delivery outcomes
//
//	retry         an error without XACK — the entry will come back with a claim (Processing,
//	              T-025): the page gets one more delivery
//	fallback      publishing to q:render-fallback (publishFallback, T-238):
//	              engine-branched reasons with
//	              render.fallback.enabled on and a positive
//	              gate/budget decision (T-239); no marker is set — the rework and
//	              finalization are up to worker B (FR-10)
//	finalization  the entry is closed without a render (finalizeNoRender):
//	              the result is the raw HTML of the HTTP phase, the render_failed_at
//	              marker (T-210), publishing to q:analyze, XACK — the pipeline finishes
//	              the entry with analysis, q:dead is not involved
//	q:dead        exhaustion of the overall delivery limit by the infrastructure
//              class — quarantine (T-025), triage is manual
//
// # Transitions: error class × guards × outcome × side effects
//
//	| # | Error class | Finalization guards | Outcome | Side effects |
//	|---|-------------|---------------------|---------|--------------|
//	| R1 | cert (certError, T-159) | always — a host property does not change between attempts | finalization from the first delivery | crawler_render_cert_finalized_total |
//	| R2 | DNS (dnsError, T-211) | always — DNS recovery takes days | finalization from the first delivery | crawler_render_dns_finalized_total |
//	| R3 | tarpit (tarpitError, T-148) | the episode tarpit budget is exhausted: the cardinality of render:tarpit:{task_id} ≥ render.timeout_attempts (tarpitsExhausted, T-261) | fallback (an engine-branched reason) → finalization | SADD render:tarpit:{task_id}; crawler_render_tarpit_finalized_total / crawler_render_fallback_published_total{timeout} |
//	| R4 | task class (isTaskRenderError, T-172) | the PEL delivery number ≥ redis.max_attempts — the overall limit | fallback (an engine-branched reason) → finalization | crawler_render_task_finalized_total{reason} / crawler_render_fallback_published_total{reason} |
//	| R5 | infrastructure (DB, storage, publishing, browser, ctx cancellation) | — | retry; after the overall limit is exhausted — q:dead | crawler_stage_errors_total{class}; reader pauses (T-152) |
//	| R6 | success (or the row slipped out from under the task) | — | renderUpdateSQL / a regular rejection without effects | crawler_render_records_total, q:analyze |
//
// The row order is the check order of processDelivery: cert/DNS before
// the tarpit, the tarpit before the task class (a tarpit on the last
// delivery is the territory of T-148, not T-172,
// TestRenderWorker_TaskFinalizeTarpitPriority).
//
// # The episode tarpit budget (T-261, the mixed-series fix)
//
// The states of a task's counter (the key render:tarpit:{task_id}):
//
//	TS0 "empty"     no tarpit-classified deliveries (or the key
//	                is lost — degradation D3): any tarpit delivery
//	                is retryable
//	TS1 "counting"  cardinality n < render.timeout_attempts: a tarpit delivery
//	                is retryable, n grows only from tarpits
//	TS2 "exhausted" n ≥ render.timeout_attempts: transition R3
//	                (fallback/finalization); further tarpits of the same
//	                episode are exhausted by construction
//
//	Event: a delivery ends with a page timeout (tarpitError) →
//	SADD of the member "{entry-id}#{PEL delivery number}" (tarpitDeliveryID),
//	TTL DefaultTarpitTTL (48 h — 2× the episode's retry horizon: the window
//	of PEL deliveries ≈ max_attempts × claim_min_idle plus the fallback queue,
//	re-publications publish a new task_id with a fresh budget; refreshed
//	by every mark — the key dies 48 h after the task's last tarpit,
//	not the first one), the decision — SCARD after the mark (the threshold
//	delivery counts itself).
//
// # Degradations (the decision — by the PEL delivery number, the behavior before T-261)
//
//	D1  a Redis error in the counter (SADD/EXPIRE/SCARD) — a Warn log, the decision
//	    by attempts ≥ render.timeout_attempts: the same condition as before
//	    T-261 — no earlier than the delivery with the threshold number and no earlier than the overall
//	    limit; in the degradation window mixed series are finalized again
//	    "per attempt" — the price of Redis availability (spec 3.4: losing Redis is not
//	    fatal).
//	D2  the counter is not mounted (Options.Tarpits = nil — isolated
//	    builds/tests without Redis specifics) or a message without a task_id
//	    (formats before T-024) — as D1, without the Warn.
//	D3  the key is lost with Redis alive (an operator cleanup, a restart without
//	    persistence) — the counting starts over (TS0): finalization
//	    is postponed until a new tarpit threshold and does not happen
//	    before the threshold; the guard is the overall limit: a tarpit on delivery #
//	    redis.max_attempts is finalized with the task class (R4, the label
//	    reason "timeout"); tarpits do not reach q:dead.
//
// # Invariants
//
//	I1  Only tarpit-classified deliveries count toward the tarpit budget:
//	    the PEL delivery number grows from every delivery (including infra failures, R5),
//	    the set — only from tarpits; mixed series are not finalized
//	    before the tarpit threshold (the T-261 fix).
//	I2  One delivery — one set member: SADD is idempotent by
//	    "{entry-id}#{number}"; a replay of the number after an attempt refund
//	    (T-222/FR-6.2) and repeated processing of a stuck delivery do not grow
//	    the counter. The counter lives in Redis — it survives worker restarts
//	    and stays consistent with many workers/recoverers.
//	I3  The overall limit redis.max_attempts works by the PEL delivery number
//	    (R4) and does not depend on the tarpit set: max_attempts ≥
//	    timeout_attempts (checked by config.Validate), a tarpit does not bypass
//	    its limit by finalizing with a foreign class.
//	I4  The budget is per task (task_id), not per stream: a fallback publication
//	    (T-238) moves the task_id into q:render-fallback, worker B's tarpits
//	    count into the same set — an episode that exhausted tarpits on
//	    engine A is finalized on the first tarpit of engine B (the second
//	    engine confirmed the page tarpit; the members differ by the entry-id
//	    of the streams, there is no double counting of one delivery). The episode's re-publications
//	    (the rework pass T-066, the late re-publisher T-259) publish
//	    a new task_id — their budget starts from zero.

package renderworker

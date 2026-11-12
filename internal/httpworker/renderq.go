// The render queue's overflow: the render_pending flag and the rework later
// (T-066; spec 5.6 "if the render queue is full, the domains receive
// the render_pending sign and are reworked later; the render must not
// delay the other stages", §7 "a domain is considered done after the
// DNS/HTTP stage completes, and the render and analysis results complement it
// asynchronously").
//
// The graceful path (§5.5 item 8): the result's SPA records (the T-057 verdict is already
// in Record.SPA) are published into q:render — one Result message per
// subdomain×variant record (the T-024 format; the T-067 consumer will replace the content by
// the key). The publication — after the result transaction's commit, as at
// q:analyze (T-058): Result points to the row.
//
// Backpressure without blocking. The T-026 gate (WatermarkPublisher) waits
// for the capacity before the publication — for q:render this is forbidden: OnResult
// runs before the q:http task's XACK (spec 4.2), waiting would delay
// the domain's acknowledgment, i.e. the render would delay the other stages —
// against §5.6. Instead of waiting — a non-blocking check of the stream's depth
// (BackpressureGate.Len, T-026: the consumption group's backlog) BEFORE
// the result transaction:
//
//   - the queue is free (depth < watermark, "strictly below the threshold", spec 4.2)
//     — q:render is published right after the commit, bypassing
//     WatermarkPublisher;
//   - the queue is full or the depth is unavailable — the publication is NOT performed,
//     the domain receives the render_pending flag (bit 1 of domains.flags, T-008) by THE
//     SAME RESULT TRANSACTION (§3.4: the render decision belongs to these
//     rows and is committed atomically with them) and finishes as usual —
//     check_state = done, the XACK without a delay. A depth read error
//     is treated as an overflow (conservatively): the flag saves the render
//     tasks from loss; a real Redis unavailability will still return
//     the delivery at the q:analyze publication (T-058).
//
// The threshold — a separate render.queue_high_watermark (T-174; an extension
// of spec 5.10): the common scheduler.queue_high_watermark (T-026) at the
// T-168 clamp (min(lag, XLEN)+pending) kept the gate open at a lag
// of 98.4k of 100k — the inflow equaled the consumption, the debt
// drainage was zero (the Oct 24 equilibrium, stage 25 of tasks.md). A default of 10k ≈ 2 h
// of consumption buffer (the Oct 24 measurement: ≈5.1k tasks/h) closes
// the gate at the current debt and returns the "draining —
// letting in" hysteresis; 0 — the common threshold. The effective value (the clamp
// min(watermark, MAXLEN/2), as at the T-026 gate — the gate must
// close before the trimming) is computed by the cmd/http-worker wiring
// (renderQueueWatermark): a ready-to-compare number arrives here.
//
// The rework pass (§5.6 "are reworked later"): the background loop
// RequeueRenderPending (the style of the daily recheck T-030 and the cleanup
// T-020) picks up the domains with the flag and reworks them when the queue
// is free. The chunk selection — without a PK order, a seq scan with an early
// LIMIT stop and a pinned plan SET LOCAL + a debt counter guard (T-181:
// the PK index scan trampled the drained "empty" prefix — query #2
// of the Oct 24 slow-query log, a 44 s median; the pinned seq scan — tens of
// ms, a debt-free steady state does not touch domains at all). The zones' priority
// (T-184, the decision (a) of T-176 — the operator of Oct 24: we render everything, we
// lose nothing; the zones with a high DOM contribution — first, the parking .rf —
// the leftovers): a non-empty render.requeue_deprioritized_zones
// divides the selection into two stages WITHOUT sorting — first the domains of the zones NOT
// from the list, an incomplete batch (exhaustion: the LIMIT did not fire, the scan reached
// the end of the table) is topped up by the list's zones; both stages under the same
// plan pinning, the debt queue does not lose records (everything
// is reworked, the list's zones — last), the exhausted
// first stage's empty pass is not repeated by the loop (renderStage: the memory by the debt
// counter). The SPA verdict is taken
// from the record's stored content:
// DetectSPA (T-057) is deterministic over the same bytes the worker
// checked at the write — the verdict matches; the aliases (content_hash NULL,
// alias_subdomain_id NOT NULL) inherit the original subdomain's verdict —
// the T-055 equivalence class is one by definition (one final URL
// or one content), the verdicts inside the class match. The order inside
// the domain — "the q:render publication → the flag removal" (the bit's transitions keep
// the render debt counter in one transaction with the bit, T-175; §3.4, in the spirit of
// T-039:
// a failure after the publication would leave the flag — a pass repeat will publish again,
// the duplicates are bounded and extinguished by the consumer's key idempotency;
// the reverse order would lose the tasks forever — nobody would return
// the removed flag).
//
// The pass's limitations (fixed deliberately):
//   - the records without their own content and without an alias (the content is not stored
//     — a limit/a disk overflow, T-056) are not re-published: there is nowhere
//     to take the verdict from; the regular recheck will rework them (T-030);
//   - the records marked by render_failed_at (T-210 — finalizations
//     without a render: the tarpit T-148, cert T-159, DNS T-211, the task class
//     T-172) are not re-published at all (see renderPendingRowsSQL): repeated editions
//     — a waste of deliveries and slot time; the next
//     recheck episode (T-030) will rework them — the row's rewrite resets the marker,
//     the fresh episode's first attempt is legitimate, a host that fixed itself
//     will render;
//   - a block read failure (a transient store failure, a race with
//     compaction) does NOT remove the domain's flag: the pass will repeat the read
//     at the next tick — the price of a minute's repeat against losing the SPA task
//     before the T-030 recheck (months);
//   - the alias inheritance — by the original subdomain without a variant
//     (alias_subdomain_id references the subdomain, T-055): at an original with
//     two variants of different sites both are inherited — a rare case,
//     the price — an extra render of a static page, not a data loss;
//   - the flag is not erased by the new episode's write: while the pass has not cleared it,
//     a new episode with SPA and a free queue publishes the tasks itself
//     (the duplicates are bounded), without SPA — the pass will clear the flag, not finding
//     SPA records. The race "the pass clears the flag, and a repeated delivery
//     of the episode sets it again" is narrowed by the bit guard in the clear UPDATE
//     (RowsAffected = 0 — a competitor has already decided); the window requires a delivery
//     that did not reach the XACK — the delivery attempts are exhausted within minutes
//     (T-025), and the T-030 recheck self-heals the remainder.
package httpworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/httpfetch"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// The rework pass's defaults.
const (
	// DefaultRenderRequeueInterval — the render rework pass's period.
	// A minute, like DefaultCleanupInterval (T-020): an overflow is an
	// emergency mode, "are reworked later" does not mean hours; with a full queue
	// the pass stands for one depth read and does not touch the DB. There is no separate
	// config option (§5.10, the composition is fixed) — the interval, like the
	// T-030 schedule, lives as a loop option.
	DefaultRenderRequeueInterval = time.Minute

	// DefaultRenderRequeueBatch — the domains in one pass chunk: the pass
	// reads the records' content for the repeated SPA verdict (the bodies up to 2 MiB,
	// spec 5.5) — a moderate batch against the T-030 recheck batches (they have nothing
	// to read); the q:render capacity is re-checked between the domains.
	DefaultRenderRequeueBatch = 100
)

// The pass's and the flag's SQL. The schema codes — as literals (the T-039 style); the
// render_pending bit — 2, the removal mask — 0xFFFD (all the bits except it).
// T-175: the bit's transitions keep the render debt's incremental counter
// crawler_counters['render_pending'] — ±1 in the same transaction as the
// transition (the debt's observability without a seq scan over 41.8M rows;
// a partial index over flags was rejected — it breaks HOT, docs/schema.md
// "HOT updates", the justification — migration 000015).
var (
	// renderSetPendingSQL — the render_pending flag in the result write
	// transaction (T-066): bit 1 via OR — is_idn (bit 0) and the other bits
	// are preserved; the domains.flags column — smallint, the bitwise
	// PostgreSQL operators promote smallint to int — the return via ::int2.
	// The bit guard (T-175): the OR semantics does not change (a repeated
	// bit setting — the same result), but RowsAffected becomes the indicator
	// of a REAL 0→1 transition — by it the debt counter is incremented exactly
	// by the transitions, not by every repeated delivery with an already set
	// flag (mirroring the clear guard below).
	//
	// # The render_pending bit × debt counter FSM (T-258, spec tasks/TZ-fsm.md §3 FR-4)
	//
	// The single source of truth about the render_pending bit's life cycle (bit 1
	// of domains.flags, model.FlagRenderPending — T-066) and its twin —
	// the render debt counter crawler_counters['render_pending'] (T-175,
	// migration 000015). A domain's debt is encoded TWICE — by the row's bit and
	// the +1 contribution into the counter (the debt's observability without a seq scan over the whole
	// domains), therefore the FSM has one core invariant (I1): the
	// "bit × counter" pair changes only by one transaction, the bit operator's
	// RowsAffected — the REAL transition indicator (the bit guard: 0 —
	// there was no transition, the counter is not touched). The transitions remain
	// SQL operators with guard predicates (TZ-fsm spec §1); the writers
	// (the table's last column) are checked against it, not duplicating it.
	// Adjacent FSMs: the row's render state
	// (internal/renderworker/worker.go, T-259 — the T2/T3 transitions own the
	// setting/removal together with this FSM), check_state
	// (internal/scheduler/checkstate.go, T-255), status
	// (internal/model/enums.go, T-256). The late re-publisher
	// (renderlate.go, T-259 T4) does NOT write the bit — the interlock excludes
	// the domains with the bit from its selection. The scenario chain of all the transitions
	// on live PostgreSQL/Redis — test/integration/renderbit_test.go
	// (TZ-fsm spec §4). The formalization does not change the behavior.
	//
	// ## The states (the domain's contribution: the bit × the counter)
	//
	//	no-debt (0/+0)         the domain has no q:render publication and none
	//	                       is promised: the bit is removed, the counter contribution is zero.
	//	                       The pipeline's steady state — the counter 0, the
	//	                       pass guard (T-181) does not touch domains at all.
	//	debt (1/+1)            the SPA records' publication is postponed: q:render
	//	                       was full at the moment of the result write
	//	                       (T-066). The tasks' holder — the bit, not the stream:
	//	                       the rework pass's selection and the
	//	                       debt counter wait for the domain.
	//	published (1/+1)       the rework pass's through point: q:render
	//	                       is published, the bit is STILL set (the order
	//	                       "publication → removal", I3); there is no
	//	                       separate carrier — observable only by a
	//	                       publication duplicate on a failure between the steps.
	//	cleared (0/+0)         the debt is closed: by the rework pass (after
	//	                       the publication or for the lack of need — a "draft
	//	                       flag") or by the delete file (§13.1).
	//
	// ## Transitions: state × event × guards × target × side effects
	//
	//	| # | Source | Event | Guards | Target | Side effects | Writer |
	//	|---|----------|---------|--------|------|-------------------|----------|
	//	| D1 | no-debt | the HTTP episode's result write, q:render is full | there are SPA records (renderSPAResults non-empty); HasCapacity = false — the depth ≥ the watermark or a read error (conservatively); the status <> deleted (the result UPDATE above in the transaction) | debt | the bit OR (the others preserved) and the counter +1 by ONE result transaction (RowsAffected 1 → renderBumpDebtCounter +1); the q:render publication does not run; the episode finishes with the usual done/XACK — the render does not delay the stages (§5.6) | record.go recordTx (renderSetPendingSQL) |
	//	| D2 | debt | a repeated episode delivery, the queue is still full | the bit guard (flags & 2) = 0 → RowsAffected 0 | debt (without change) | the counter is NOT touched — a second +1 is forbidden by the guard (the pairing is not doubled); the rows are rewritten idempotently (DELETE+INSERT, spec 3.4) | record.go recordTx (bumpDebt = false) |
	//	| D3 | debt | the rework pass: the content is read, the repeated verdict SPA | the queue is free (the check before the selection and before the publication), the domain is not deleted (the selection guard), the records are not marked (T-210) | cleared (via "published") | a q:render publication → the bit clear and the counter −1 by one transaction (renderClearPendingTx); a failure between the steps leaves the bit — a pass repeat will publish a duplicate (bounded, extinguished by the consumer's key idempotency) | renderq.go RequeueOnce |
	//	| D4 | debt | the rework pass: no SPA records (a non-SPA content, marked by T-210, without content and alias T-056) | as D3 | cleared | a "draft flag": the removal and −1 WITHOUT a publication — there is nothing to render | renderq.go RequeueOnce (an empty renderDomainKeys) |
	//	| D5 | debt | the rework pass: the content part of the records is unreadable (a transient store failure, a compaction race) | as D3 | debt (without change) | keep: the bit and the counter are preserved — the next tick will repeat the domain (the price of a minute's repeat against losing the SPA task before the T-030 recheck) | renderq.go renderDomainKeys (keep) |
	//	| D6 | debt | the delete file: the domain leaves the check cycle (§13.1) | the status <> deleted guard (a repeated batch application — a no-op) | cleared (the row deleted) | the bit cleared and the counter −count(the actually removed bits) by TWO operators of ONE application transaction (applyDeleteSQL + applyClearRenderSQL, T-186/T-269: the status translation → the guarded pair by the bit rows' keys, the delta — by the fact of the current version's transition, the T-186 snapshot window closed by the 08.11.2026 incident): the deleted would no longer pass the rework selection, a bit without an owner would keep the counter and the guard's empty seq scan forever | ingest/apply_delete.go ApplyDelete |
	//	| D7 | debt | a concurrent clear (a second pass instance, a neighboring instance) | the clear guard (flags & 2) <> 0 → RowsAffected 0 | cleared (without change) | the counter is NOT touched — the transition is already accounted by the competitor (a second −1 is forbidden); the publication duplicates are bounded by one pass window | renderq.go renderClearPendingTx |
	//	| D8 | no-debt/debt | a new episode with a free queue | HasCapacity = true (at empty renderSPAResults the capacity is not checked at all) | without change | the result write does NOT set and does NOT erase the bit: for a debt (not yet removed by the pass) the fresh episode publishes q:render itself — the duplicate is bounded; without SPA — the pass will remove it by D4 | record.go recordTx |
	//
	// ## Special states
	//
	//	draft flag      the bit is set, the domain has no render tasks (D4):
	//	                the page stopped being SPA, the records are marked
	//	                or the content is not stored (T-056). Removed
	//	                by the pass without a publication — not an error.
	//	keep            the content part of the records is unreadable (D5): the bit —
	//	                the task's holder, the removal would lose it before
	//	                the T-030 recheck (months); closed by a read
	//	                (D3/D4) or a deletion (D6).
	//	orphan bits     the counter sees the debt, the selection — not: a remnant of the era
	//	                before T-186 (the deleted with the flag — the selection honestly
	//	                excludes deleted by §13.1) or a manual SQL
	//	                (runbooks §6). The graceful path is closed (D6 is atomic);
	//	                the pass pays an idle seq scan per tick and a Warn —
	//	                the operator must see this. At that the pairing at
	//	                the deleted-orphans is already correct (I2 counts without a status
	//	                filter — the reconciliation only confirms it and
	//	                does not remove the phantom debt): the tail is cleaned ONCE
	//	                by the "bit + counter" pair over status = deleted (runbooks
	//	                §6); a manual SQL desync is leveled by the
	//	                monitor reconciliation.
	//
	// ## The invariants
	//
	//	I1  The "bit × counter" pair changes only by one transaction (±1
	//	    together; a rollback rolls back both). RowsAffected 0 — there
	//	    was no transition, the counter is not touched: a repeated setting (D2) and
	//	    a repeated clear (D7) are idempotent by pairing.
	//	I2  The counter = the number of rows with the bit, WITHOUT a status filter — the same
	//	    predicate at the migration 000015 backfill and the monitor reconciliation
	//	    (VerifyCounters — the pairing authority; the pass guard T-181
	//	    believes the counter, the debt must be honest). A desync is possible
	//	    only by a manual SQL (runbooks §6) and by the "aggregate → rewrite"
	//	    window of the reconciliation pass at competing transitions between its
	//	    operators (a stale ABSOLUTE, not a delta: every row rewrite
	//	    moves updated_at — a "slightly stale" second −n cannot be left
	//	    by the reconciliation, the 08.11.2026 incident disproved the converse;
	//	    it self-heals by the next pass, ≤ zone_stats_interval). The
	//	    apply_delete snapshot window (the decrement by had_render before T-269) is closed:
	//	    the delta — by the fact of the transition, the guard of the row's current version.
	//	I3  The "q:render publication → the bit clear" order (at-least-once,
	//	    in the spirit of T-039): the reverse would lose the tasks forever — nobody
	//	    would return the removed flag; the duplicates are bounded by the pass window and extinguished
	//	    by the consumer's key idempotency (spec 3.4).
	//	I4  The counter's row is mandatory (created and backfilled by migration
	//	    000015): an absence — a transition/guard error, not a silent
	//	    zero (renderBumpDebtCounter/renderDebtCount): a silent
	//	    desync would make the debt gauge a lie.
	renderSetPendingSQL = "UPDATE domains AS t " +
		"SET flags = (t.flags | " + fmt.Sprintf("%d", int16(model.FlagRenderPending)) + ")::int2 " +
		"WHERE t.zone_id = $1::int2 AND t.id = $2::int8 " +
		"AND (t.flags & " + fmt.Sprintf("%d", int16(model.FlagRenderPending)) + ") = 0"

	// renderPendingDomainsSQL — the chunk of domains waiting for the render rework:
	// the render_pending bit is set, the domain is not deleted (§13.1 — outside the
	// check cycle). The name — label || '.' || zone (the T-027 style) for Result.Name
	// and the logs; LIMIT — the chunk. The ORDER BY over the PK is absent deliberately
	// (T-181, the operator's Oct 24 decision — the domains' order does not matter): sorting
	// kept a PK index scan which, with a live debt, tramples
	// the drained "empty" prefix of the PK order (query #2 of the Oct 24
	// slow-query log: 14.1M Rows Removed, ≈4.5 GB of buffers per call,
	// a 44 s median); the non-deterministic chunks are correct — the flag clear
	// after the publication excludes repeats. The plan shape (a seq scan with an early
	// LIMIT stop) is pinned by SET LOCAL in the selection transaction —
	// renderPlanNoIndexScanSQL/renderPlanNoBitmapScanSQL below. This is the stage
	// without a zone filter (an empty render.requeue_deprioritized_zones — T-184);
	// the zones' priority is built by renderPendingDomainsBaseSQL +
	// renderPendingDomainsNotInSQL/…InSQL.
	renderPendingDomainsSQL = renderPendingDomainsBaseSQL + "LIMIT $1"

	// renderPendingDomainsBaseSQL — the stages' common part (T-184):
	// the WHERE/JOIN as at the former renderPendingDomainsSQL (T-181), the stages
	// add the zone filter and the LIMIT.
	renderPendingDomainsBaseSQL = "SELECT d.zone_id, d.id, d.label || '.' || z.name " +
		"FROM domains AS d JOIN zones AS z ON z.id = d.zone_id " +
		"WHERE (d.flags & " + fmt.Sprintf("%d", int16(model.FlagRenderPending)) + ") <> 0 " +
		"AND d.status <> " + fmt.Sprintf("%d", int16(model.StatusDeleted)) + " "

	// renderPendingDomainsNotInSQL / renderPendingDomainsInSQL — the
	// render debt's prioritization stages by zones (T-184, the decision (a) of T-176):
	// a non-empty render.requeue_deprioritized_zones divides the chunk selection into
	// two stages WITHOUT sorting — the first takes the domains of the zones NOT
	// from the list (the priority: the zones with a high DOM contribution), the second — the list's zones'
	// domains (parking classes — the leftovers). The zone name filter
	// in the JOIN (the zones reference table — small, a hash join; the matching is live:
	// the zones are created by ingest with the first file, there is no separate resolution at the start
	//), both stages run in the same plan pinning
	// transactions (T-181): the seq scan over domains with the early LIMIT stop is preserved —
	// the stages' sorting is not introduced deliberately, it would return
	// the prefix trampling/a full sorting pass. The debt queue does not
	// lose records: the first stage's exhaustion (an incomplete chunk — the LIMIT
	// did not fire, the scan reached the end of the table) is topped up by the second.
	renderPendingDomainsNotInSQL = renderPendingDomainsBaseSQL +
		"AND z.name <> ALL($2::text[]) LIMIT $1"
	renderPendingDomainsInSQL = renderPendingDomainsBaseSQL +
		"AND z.name = ANY($2::text[]) LIMIT $1"

	// renderPlanNoIndexScanSQL/renderPlanNoBitmapScanSQL/renderPlanNoIOSQL —
	// pinning the plan shape of the batch selection (T-181): SET LOCAL in the
	// renderSelectPending transaction disables the index scan, the bitmap scan and the index-only
	// scan until the selection operator. The planner has no statistics for the
	// (flags & 2) <> 0 expression (an index over flags is impossible — the HOT invariant, T-175),
	// the filter is estimated almost non-selective (41.6M of 41.6M rows), and without
	// the pinning the planner takes a PK index scan even without the ORDER BY —
	// the same prefix trampling; the pinned seq scan finds the first
	// chunk of flag rows by the early LIMIT stop (the .rf flag debt
	// is physically at the heap's start: the ingest ru/rf/su went first, HOT keeps
	// the versions on the native pages) — 43 ms against 30 s at the Oct 24 measurement
	// (700×). The SET LOCAL action — only this transaction, the pool's other
	// queries are unaffected; the bitmap and index-only scans are disabled
	// following the index scan — all the index access forms to domains
	// are closed (a PK index-only would repeat the pathology, though spec §4.2
	// checked only the index/bitmap pair by diagnostics), there will be
	// no indexes over flags.
	renderPlanNoIndexScanSQL  = "SET LOCAL enable_indexscan = off"
	renderPlanNoBitmapScanSQL = "SET LOCAL enable_bitmapscan = off"
	renderPlanNoIOSQL         = "SET LOCAL enable_indexonlyscan = off"

	// renderDebtCounterSQL — the pass guard by a point read of the
	// render debt counter (T-181; the counter — T-175): value = 0 — the heavy
	// batch selection does not run at all (the debt-free steady state pays one
	// row read per tick instead of a seq scan over 41.8M rows); an absent row —
	// a pass error (the row is guaranteed by migration 000015; a silent
	// desync would make the guard a lie — the renderBumpDebtCounter philosophy,
	// runbooks §6).
	renderDebtCounterSQL = "SELECT value FROM crawler_counters WHERE name = 'render_pending'"

	// renderPendingRowsSQL — the domain's records for the repeated SPA verdict:
	// not yet rendered (rendered = false is set by the result record
	// T-058, true — the render T-067) and with a final URL (the render's target).
	// render_failed_at IS NULL — the "render impossible" marker (T-210):
	// the finalized-without-render records (the tarpit T-148 / cert T-159 /
	// DNS T-211 / the task class T-172 — the cert class, the DNS class and the task
	// class are permanent, at the tarpit the attempts budget is exhausted; the
	// T-172/T-207)
	// are not re-published — the repeated editions (a repeated pass over a domain,
	// whose flag survived the publication: a partial publication failure, a Full
	// exit after the publication of the chunk's neighbors, keeping unreadable content, a restart
	// between the publication and the removal) burn the deliveries and the render's slot time;
	// the marker is reset by a content change (the episode's rewrite T-058 —
	// the fresh episode's first attempt is legitimate), a host that fixed itself
	// will return into the selection the next episode. All the domain's records
	// marked — the flag is removed without a publication (a "draft flag",
	// like at a page that stopped being SPA).
	// content_hash — the body for DetectSPA (T-057/T-056), alias_subdomain_id
	// — the original verdict's inheritance (T-055). The ORDER BY —
	// a deterministic publication order.
	renderPendingRowsSQL = "SELECT subdomain_id, variant, content_hash, alias_subdomain_id " +
		"FROM subdomain_results " +
		"WHERE zone_id = $1::int2 AND domain_id = $2::int8 " +
		"AND rendered = false AND final_url IS NOT NULL AND render_failed_at IS NULL " +
		"ORDER BY subdomain_id, variant"

	// renderClearPendingSQL — the flag clear after the publication. The bit guard:
	// a concurrent clear (a second pass instance, a neighboring service
	// instance) gives RowsAffected = 0 without an error — the publication duplicates
	// are bounded by one pass window. The mask — a signed ~of the bit (−3): the
	// smallint bits are promoted to int4 with a sign extension, an unsigned
	// mask 65533 does not fit into a smallint by value (checked on a live
	// PostgreSQL 16), the signed one clears exactly the render_pending bit and
	// preserves the others, including the senior one.
	renderClearPendingSQL = "UPDATE domains AS t " +
		"SET flags = (t.flags & " + fmt.Sprintf("%d", int16(^int16(model.FlagRenderPending))) + ")::int2 " +
		"WHERE t.zone_id = $1::int2 AND t.id = $2::int8 " +
		"AND (t.flags & " + fmt.Sprintf("%d", int16(model.FlagRenderPending)) + ") <> 0"

	// renderCounterDeltaSQL — the render debt counter's shift (T-175):
	// crawler_counters['render_pending'] ±1 IN THE SAME TRANSACTION as
	// the bit transition (§3.4: the bit and the counter do not diverge — a rollback rolls back
	// both). The delta — a parameter (int8: +1 the flag setting in the result
	// write, −1 the removal by the rework pass), the row's name — a literal
	// (the T-039 style). RowsAffected 0 — the row is absent (possible only by a manual
	// edit of the table, runbooks §6): the caller's error rolls back
	// the whole transaction — a silent counter desync is unacceptable,
	// the debt must be visible honestly (the T-175 setting).
	renderCounterDeltaSQL = "UPDATE crawler_counters AS c " +
		"SET value = c.value + $1::int8 " +
		"WHERE c.name = 'render_pending'"
)

// RenderLen — the stream depth's source: satisfied by
// *queue.BackpressureGate (T-026). A narrow interface — httpworker does not
// depend on the gate's Redis specifics, the unit tests substitute a fake.
type RenderLen interface {
	Len(ctx context.Context, stream string) (int64, error)
}

// RenderContent — reading the stored content for the repeated
// SPA verdict in the rework pass; satisfied by *content.PackStore
// (T-042).
type RenderContent interface {
	Get(ctx context.Context, hash content.Hash) ([]byte, error)
}

// RenderQueue — the q:render publication without waiting for the capacity (§5.6: the render
// does not delay the other stages). The q queue — the publication bypassing
// WatermarkPublisher (T-026): its Publish would wait for the capacity and delay the XACK
// of the q:http task; the capacity is checked separately (HasCapacity) — the
// "publish or flag" decision belongs to the caller. Concurrency-safe:
// the state is immutable after New.
type RenderQueue struct {
	q         queue.Queue
	lenner    RenderLen
	watermark int64
	log       *slog.Logger
}

// NewRenderQueue builds the render queue over a "raw" queue (without
// the backpressure gate) and the streams' depth source; watermark — the
// q:render capacity threshold render.queue_high_watermark (T-174; 0 — the common
// scheduler.queue_high_watermark; the effective value with the
// MAXLEN/2 clamp is computed by the cmd/http-worker wiring — a ready-to-compare
// number arrives here). nil dependencies — an error: a silent loss
// of publications is invisible to the operator.
func NewRenderQueue(q queue.Queue, lenner RenderLen, watermark int64, log *slog.Logger) (*RenderQueue, error) {
	if q == nil {
		return nil, errors.New("httpworker: очередь рендера: очередь не задана")
	}
	if lenner == nil {
		return nil, errors.New("httpworker: очередь рендера: источник длины стрима не задан (BackpressureGate, T-026)")
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &RenderQueue{q: q, lenner: lenner, watermark: watermark, log: log}, nil
}

// HasCapacity — a non-blocking q:render capacity check: true when
// the stream's depth is strictly below the render.queue_high_watermark threshold
// (T-174; depth == watermark — already an overflow, spec 4.2). A Watermark ≤ 0
// disables the check (the T-026 semantics) — the capacity is always there, without
// a Redis call.
func (rq *RenderQueue) HasCapacity(ctx context.Context) (bool, error) {
	if rq.watermark <= 0 {
		return true, nil
	}
	n, err := rq.lenner.Len(ctx, queue.StreamRender)
	if err != nil {
		return false, err
	}
	return n < rq.watermark, nil
}

// Publish publishes the Result messages into q:render without a capacity check:
// the check is always a separate step (HasCapacity) — the write worker decides
// "publish or flag" before the transaction, the pass — "is it the domain's turn".
func (rq *RenderQueue) Publish(ctx context.Context, msgs []queue.Result) error {
	for i := range msgs {
		if _, err := rq.q.Publish(ctx, queue.StreamRender, msgs[i]); err != nil {
			return fmt.Errorf("httpworker: публикация q:render (запись %d/%d): %w", i+1, len(msgs), err)
		}
	}
	return nil
}

// renderSPAResults — the q:render Result messages over the result's SPA records
// (§5.5 item 8): one subdomain×variant key per record with the
// SPA=true verdict (T-057). The alias records carry their source
// check's verdict — the T-055 equivalence class is one, the verdicts inside the class
// match; the T-067 consumer sees the row's alias_kind and decides itself,
// what to do with the alias. A nil verdict (no page) requires no render.
func renderSPAResults(res *DomainResult) []queue.Result {
	var out []queue.Result
	for i := range res.Subdomains {
		sub := &res.Subdomains[i]
		for j := range sub.Records {
			rec := &sub.Records[j]
			if rec.SPA == nil || !rec.SPA.SPA {
				continue
			}
			out = append(out, queue.Result{
				ZoneID:      res.ZoneID,
				DomainID:    res.DomainID,
				SubdomainID: sub.SubdomainID,
				Variant:     rec.Variant,
				Name:        res.Name,
			})
		}
	}
	return out
}

// NewRenderRecorder — the result writer with a render pipeline (T-066):
// NewRecorder (T-058) + the render queue. A separate constructor, not
// a new NewRecorder parameter: the T-058 signature was pinned by its
// tests, the extension (the stage metrics, T-268) went together with them —
// render = nil at NewRecorder preserves the former behavior (no q:render and
// no flag) for the isolated builds.
func NewRenderRecorder(pool RecordDB, q queue.Queue, render *RenderQueue, met *Metrics, log *slog.Logger) (*Recorder, error) {
	r, err := NewRecorder(pool, q, met, log)
	if err != nil {
		return nil, err
	}
	r.render = render
	return r, nil
}

// --- the rework pass ---------------------------------------------------------

// RequeueOptions — the render rework pass's parameters; the zero values
// are replaced by the defaults (withDefaults, the RecheckOptions T-030 style).
type RequeueOptions struct {
	// Interval — the RequeueRenderPending loop's period
	// (DefaultRenderRequeueInterval); does not affect a one-shot RequeueOnce.
	Interval time.Duration
	// BatchSize — the maximum of domains in one chunk
	// (DefaultRenderRequeueBatch).
	BatchSize int
	// DeprioritizedZones — the zones' names (punycode, as in the zones reference table),
	// whose debt the pass serves by the leftovers (T-184, the
	// decision (a) of T-176): the chunk is assembled two-stage — first the domains of the
	// zones NOT from the list, at an incomplete chunk the remainder is topped up by the list's zones.
	// A nil/empty list — the former order (one stage without a filter,
	// T-181). The source — render.requeue_deprioritized_zones; the
	// first stage's exhaustion memory is kept by the RequeueRenderPending loop (a one-shot
	// RequeueOnce executes both stages honestly, without memory between the calls).
	DeprioritizedZones []string
	// Log — the loop's logger and the pass's Warns; nil — the records are suppressed
	// (slog.DiscardHandler).
	Log *slog.Logger
}

func (o RequeueOptions) withDefaults() RequeueOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultRenderRequeueInterval
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultRenderRequeueBatch
	}
	return o
}

// RequeueResult — the outcome of one RequeueOnce pass.
type RequeueResult struct {
	// Selected — the domains selected with the flag (the selection chunk's size).
	Selected int
	// Cleared — the domains whose flag was cleared (after the publication or for the lack of need
	// — no SPA records were found).
	Cleared int
	// Published — the q:render messages published.
	Published int
	// Deprioritized — the chunk's domains from the render.requeue_deprioritized_zones zones
	// (the second selection stage, T-184): 0 — the chunk is entirely from the priority
	// zones; the field's growth in the pass journal — the first stage is exhausted, the
	// drainage reached the postponed zones (an order control without a manual SQL).
	Deprioritized int
	// Full — the pass stopped: q:render is at the threshold; the unprocessed domains
	// kept the flag, the rework will continue with the next pass.
	Full bool
}

// renderPendingDomain — a domain of the pass's chunk.
type renderPendingDomain struct {
	zone int16
	id   int64
	name string
}

// renderPendingRow — a subdomain×variant record of the pass's domain.
type renderPendingRow struct {
	subdomain int16
	variant   model.Variant
	hash      []byte // nil — there is no own content (an alias or not stored, T-056)
	aliasSub  *int16 // not nil — an alias: the verdict is inherited from the original (T-055)
}

// RequeueOnce — one render rework pass (§5.6 "are reworked
// later"): up to opts.BatchSize domains with the render_pending flag. The
// q:render capacity is checked before the selection and before each domain's publication —
// on an overflow the pass stops (Full), the flags are preserved; at
// a zero debt counter (the T-181 guard) the heavy selection does not run.
// For a domain — the SPA records' keys (a repeated verdict over the stored
// content) → the q:render publication → the flag clear with the debt
// counter by one transaction (T-175). An empty key set — the flag is cleared without
// a publication (a draft flag: the page stopped being SPA); unreadable
// content keeps the flag until the next pass (renderDomainKeys).
// A pool with transactions (RecordDB): the bit clear and the render debt
// counter's decrement must commit together (§3.4, T-175).
//
// The zones' priority (T-184): a non-empty opts.DeprioritizedZones divides the selection
// into two stages (the zones NOT from the list, then — at an incomplete chunk — the
// list's zones); a one-shot call executes both stages without memory between the calls
// — only the RequeueRenderPending loop remembers the first stage's exhaustion
// (renderStage, saving an idle full seq scan).
func RequeueOnce(ctx context.Context, pool RecordDB, contents RenderContent, render *RenderQueue, opts RequeueOptions) (RequeueResult, error) {
	if pool == nil {
		return RequeueResult{}, errors.New("httpworker: проход рендера: пул не задан")
	}
	if contents == nil {
		return RequeueResult{}, errors.New("httpworker: проход рендера: хранилище контента не задано")
	}
	if render == nil {
		return RequeueResult{}, errors.New("httpworker: проход рендера: очередь рендера не задана")
	}
	o := opts.withDefaults()
	if o.Log == nil {
		o.Log = slog.New(slog.DiscardHandler)
	}
	return requeuePass(ctx, pool, contents, render, o, &renderStage{})
}

// renderStage — the debt selection stages' switching memory (T-184).
// The first stage's (the zones NOT from the list) exhaustion is proven by an incomplete
// chunk: the LIMIT did not fire — the seq scan reached the end of the table, so
// an idle first-stage pass is a FULL seq scan over domains (the
// pathology of query #2 of the Oct 24 slow-query log, removed by T-181; the debt tail
// of the list's zones would drag for hours, paying a full pass per tick and
// per iteration of the initial drainage). While the debt counter equals the
// expectation (the value at the proof moment minus the flag withdrawals performed
// by this loop after it), there were no new bit settings (each — +1
// to the counter, T-175) and no foreign clears either — the first stage does not run
// at all. Any divergence resumes it: a counter growth — a new
// bit setting (any zone, including the list's — one counter cannot
// distinguish them, the over-caution is cheaper than a lost priority),
// a decrease beyond its own withdrawals — a second pass instance or a manual SQL
// (runbooks §6). The proof is also removed without a counter divergence,
// if at least one first-stage domain of the chunk stayed with the flag (a keep at
// unreadable content, an early Full/error exit — requeuePass, firstLeft):
// the selection saw these rows, but the pass did not rework them, and the second stage
// (the list's zones) would never pick the stuck priority debt. The
// memory is inexact only at an exact compensation "a foreign setting + a foreign clear"
// between the ticks — a single loop instance does not produce this (T-181 §5.3).
type renderStage struct {
	known     bool  // the first stage is provably exhausted
	proofDebt int64 // the render debt counter at the proof moment
	ownClears int64 // the flag withdrawals by this loop after the proof
}

// expectedDebt — the loop's expected debt counter value: a match with the
// read one allows skipping the first stage.
func (s *renderStage) expectedDebt() int64 { return s.proofDebt - s.ownClears }

// requeuePass — the rework pass's body (RequeueOnce without the validations);
// stage — the loop stages' memory (T-184): known/proofDebt are read and
// written only in the two-stage mode, ownClears is shifted in the
// single-stage mode too (a dead state — the memory is not consulted in it).
func requeuePass(ctx context.Context, pool RecordDB, contents RenderContent, render *RenderQueue, o RequeueOptions, stage *renderStage) (RequeueResult, error) {
	out := RequeueResult{}

	// A full queue — the DB is not read (one depth read):
	// the overflow is the very reason of the flags; the "it freed up" signal —
	// the loop's next tick.
	free, err := render.HasCapacity(ctx)
	if err != nil {
		return out, fmt.Errorf("httpworker: проход рендера: ёмкость q:render: %w", err)
	}
	if !free {
		return RequeueResult{Full: true}, nil
	}

	// A guard by the render debt counter (T-181, the counter — T-175): a debt-free
	// steady state does not pay for the chunk selection — a point read of the counter's
	// row instead of a seq scan over the whole domains once per tick; 0 → an empty chunk (Selected
	// = 0 — the drain stops correctly). A negative value is not produced by the code (the bit
	// transitions keep the counter by a ±1 pair, T-175) and
	// is treated the same way — as "no debt", but loudly: a quiet counter bug
	// must not hide behind a silent drainage idle.
	debt, err := renderDebtCount(ctx, pool)
	if err != nil {
		return out, err
	}
	if debt < 0 {
		o.Log.Warn("проход рендера: отрицательный счётчик рендер-долга — рассинхрон (runbooks §6)", "debt", debt)
	}
	if debt <= 0 {
		return out, nil
	}

	// The chunk selection: one stage (T-181) or the zones' priority (T-184).
	//
	// firstLeft — the FIRST stage's domains of the chunk whose flag is not yet cleared
	// (the T-184 review, P1): the exhaustion proof survives the pass
	// only if by the end there are zero of them — a keep (unreadable content) or an early
	// exit (Full/error/cancellation, including the second stage's selection error)
	// leaves the flag alive, and skipping the first stage would clamp the stuck
	// PRIORITY debt forever: the second stage (the list's zones) does not
	// see it, the counter and the expectation do not diverge — a Warn "orphan bits" and
	// a full seq scan every tick instead of the rework (the header's contract:
	// "the pass will repeat the read at the next tick"). The second stage's
	// domains are not counted: the ones stuck in the list's zones will be picked by the second
	// stage of the next pass itself — it runs at every first stage's
	// skip. Removed by a defer at every pass exit.
	var domains []renderPendingDomain
	firstN, firstLeft := 0, 0
	if len(o.DeprioritizedZones) == 0 {
		domains, err = renderSelectPending(ctx, pool, o.BatchSize, nil, false)
		if err != nil {
			return out, err
		}
		out.Selected = len(domains)
		if len(domains) == 0 {
			// The counter sees the debt, the selection — not: a manual SQL
			// desync (runbooks §6) or "orphan" bits — a rare remnant of the era before
			// T-186: the domains deleted with the flag (the selection honestly excludes
			// them by §13.1); the graceful path is closed — the delete application clears
			// the bit and decrements the counter by the same transaction (T-186),
			// new orphans do not accumulate, the tail before T-186 is cleaned once
			// (runbooks §6). The drainage pays a full pass without an early
			// stop per tick — the operator must see this.
			o.Log.Warn("проход рендера: счётчик долга > 0, выборка пуста — биты-«сироты» либо рассинхрон счётчика (runbooks §6)",
				"debt", debt)
			return out, nil
		}
	} else {
		// The first stage is mandatory while there is no exhaustion memory
		// or the counter diverged from the expectation (see renderStage).
		if !stage.known || debt != stage.expectedDebt() {
			first, err := renderSelectPending(ctx, pool, o.BatchSize, o.DeprioritizedZones, false)
			if err != nil {
				return out, err
			}
			if len(first) < o.BatchSize {
				// An incomplete chunk — the SELECTION is exhausted (the scan reached the end);
				// the counter was read by this same pass BEFORE the selection, the own
				// withdrawals after the read have not happened yet. The proof will survive
				// the pass only if every first-stage domain will be
				// cleared — the firstLeft counter below.
				stage.known, stage.proofDebt, stage.ownClears = true, debt, 0
			} else {
				// A full chunk — domains may remain beyond the LIMIT:
				// the former exhaustion proof is invalid.
				stage.known = false
			}
			domains = first
			// The non-clear accounting — right after the first selection: a second
			// stage error below must also remove the proof (the
			// first-stage domains stayed with the flags).
			firstN, firstLeft = len(first), len(first)
			if firstN > 0 {
				defer func() {
					if firstLeft > 0 {
						stage.known = false
					}
				}()
			}
		}
		if len(domains) < o.BatchSize {
			// The first stage's exhaustion (an empty chunk or a remainder top-up):
			// the second stage — the list's zones, the leftovers.
			second, err := renderSelectPending(ctx, pool, o.BatchSize-len(domains), o.DeprioritizedZones, true)
			if err != nil {
				return out, err
			}
			out.Deprioritized = len(second)
			domains = append(domains, second...)
		}
		out.Selected = len(domains)
		if len(domains) == 0 {
			// Both stages are empty — the same diagnostics as in the one-stage mode.
			o.Log.Warn("проход рендера: счётчик долга > 0, выборка пуста — биты-«сироты» либо рассинхрон счётчика (runbooks §6)",
				"debt", debt)
			return out, nil
		}
	}

	for i := range domains {
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("httpworker: проход рендера: %w", err)
		}
		d := &domains[i]
		keys, keep, err := renderDomainKeys(ctx, pool, contents, d, o.Log)
		if err != nil {
			return out, err
		}
		if len(keys) > 0 {
			free, err := render.HasCapacity(ctx)
			if err != nil {
				return out, fmt.Errorf("httpworker: проход рендера: ёмкость q:render: %w", err)
			}
			if !free {
				out.Full = true
				return out, nil
			}
			if err := render.Publish(ctx, keys); err != nil {
				return out, fmt.Errorf("httpworker: проход рендера: домен %s: %w", d.name, err)
			}
			out.Published += len(keys)
		}
		if keep {
			// The content part of the records is not read (a transient failure):
			// the flag is preserved — the next pass will repeat the domain;
			// a clear would remove the SPA task before the T-030 recheck.
			// A first-stage domain (i < firstN) at that "gets stuck" —
			// the defer will remove the proof.
			continue
		}
		// The flag clear — after the publication (§3.4, the order in the file header),
		// with the debt counter by one transaction (T-175).
		cleared, err := renderClearPendingTx(ctx, pool, d.zone, d.id)
		if err != nil {
			return out, fmt.Errorf("httpworker: проход рендера: домен %s: снятие флага: %w", d.name, err)
		}
		if i < firstN {
			// There is no flag anymore (cleared by us or by a competitor — the bit guard):
			// the first-stage domain did not get stuck.
			firstLeft--
		}
		if cleared > 0 {
			out.Cleared++
			// Its own withdrawal shifts the counter's expectation (the stages' memory,
			// T-184): a priority zone or a list one — no difference, any
			// new bit setting on top will still diverge the counter from
			// the expectation and resume the first stage.
			stage.ownClears++
		}
	}
	return out, nil
}

// renderClearPendingTx — the render_pending flag clear and the render debt
// counter's decrement by one transaction (T-175): returns the clear's RowsAffected
// (> 0 — the bit was there and is cleared, the counter −1; 0 — the bit was already gone (the bit
// guard: a concurrent clear by a second pass instance), the counter not touched).
// A failure of any step rolls back the pair entirely — the bit and the counter do
// not diverge; a repeated attempt (the next pass tick / a repeated
// delivery) will execute the pair anew.
func renderClearPendingTx(ctx context.Context, pool RecordDB, zone int16, id int64) (int64, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("httpworker: транзакция снятия: %w", err)
	}
	// The rollback unconditionally: after the Commit — a no-op (the T-044 style).
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, renderClearPendingSQL, zone, id)
	if err != nil {
		return 0, err
	}
	if tag.RowsAffected() > 0 {
		if err := renderBumpDebtCounter(ctx, tx, -1); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("httpworker: фиксация снятия: %w", err)
	}
	return tag.RowsAffected(), nil
}

// renderBumpDebtCounter — the render debt counter's shift in the OPEN
// render_pending bit transition transaction (T-175): delta ±1. Returning with
// an error rolls back the caller's transaction — the bit and the counter change
// only together (§3.4). RowsAffected != 1 — there is no 'render_pending' row
// (created and backfilled by migration 000015; it can vanish only by a manual
// table edit, runbooks §6): a silent desync would make the
// debt gauge a lie — the error is louder.
func renderBumpDebtCounter(ctx context.Context, tx pgx.Tx, delta int64) error {
	tag, err := tx.Exec(ctx, renderCounterDeltaSQL, delta)
	if err != nil {
		return fmt.Errorf("счётчик рендер-долга: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("счётчик рендер-долга: затронуто %d строк (строка 'render_pending' обязательна, миграция 000015)",
			tag.RowsAffected())
	}
	return nil
}

// renderDebtCount — the rework pass's guard (T-181): a point read of the
// render debt counter crawler_counters['render_pending'] (T-175).
// 0 — no debt, the heavy chunk selection (a seq scan over domains) does
// not run at all; an absent row — an error: the row is created by the
// migration 000015 backfill, it can vanish only by a manual table edit
// (runbooks §6) — a silent desync would make the guard a lie, a louder
// error (as at renderBumpDebtCounter).
func renderDebtCount(ctx context.Context, pool RecordDB) (int64, error) {
	rows, err := pool.Query(ctx, renderDebtCounterSQL)
	if err != nil {
		return 0, fmt.Errorf("httpworker: проход рендера: счётчик долга: %w", err)
	}
	defer rows.Close()
	var n int64
	if !rows.Next() {
		// First a read failure: a connection break is not "no row",
		// the absence diagnosis must be honest (runbooks §6
		// leads the operator to do an INSERT).
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("httpworker: проход рендера: счётчик долга: %w", err)
		}
		return 0, errors.New("httpworker: проход рендера: строка счётчика 'render_pending' отсутствует (миграция 000015, runbooks §6)")
	}
	if err := rows.Scan(&n); err != nil {
		return 0, fmt.Errorf("httpworker: проход рендера: счётчик долга: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("httpworker: проход рендера: счётчик долга: %w", err)
	}
	return n, nil
}

// renderSelectPending selects a chunk of domains with the render_pending flag —
// the heavy pass selection's single point. The selection runs in an explicit transaction with
// SET LOCAL enable_indexscan/bitmapscan/indexonlyscan = off (T-181):
// pinning the plan shape (a seq scan over domains + the early LIMIT stop) —
// see the renderPlanNoIndexScanSQL comment; without the pinning the planner,
// without the expression statistics, takes a PK index scan for (flags & 2) <> 0,
// trampling the drained "empty" prefix (a 44 s median on a live
// debt, the Oct 24 measurement). The transaction — only the pinning shell: SELECT
// reads committed data, commit/rollback are equivalent by content.
//
// The T-184 stages: zoneNames empty — the base selection without a zone filter
// (renderPendingDomainsSQL, the T-181 order); otherwise the zone name in the JOIN —
// the first stage takes the zones NOT from the list (renderPendingDomainsNotInSQL),
// the second — the list's zones (renderPendingDomainsInSQL); the filter does not change
// the plan shape (a small zones reference table — a hash join, no sorting),
// the early LIMIT stop is preserved.
func renderSelectPending(ctx context.Context, pool RecordDB, limit int, zoneNames []string, inList bool) ([]renderPendingDomain, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("httpworker: проход рендера: транзакция выборки: %w", err)
	}
	// The rollback unconditionally: after the Commit — a no-op (the T-044 style).
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, renderPlanNoIndexScanSQL); err != nil {
		return nil, fmt.Errorf("httpworker: проход рендера: закрепление плана: %w", err)
	}
	if _, err := tx.Exec(ctx, renderPlanNoBitmapScanSQL); err != nil {
		return nil, fmt.Errorf("httpworker: проход рендера: закрепление плана: %w", err)
	}
	if _, err := tx.Exec(ctx, renderPlanNoIOSQL); err != nil {
		return nil, fmt.Errorf("httpworker: проход рендера: закрепление плана: %w", err)
	}

	sql, args := renderPendingDomainsSQL, []any{limit}
	if len(zoneNames) > 0 {
		sql = renderPendingDomainsNotInSQL
		if inList {
			sql = renderPendingDomainsInSQL
		}
		args = append(args, zoneNames)
	}

	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("httpworker: проход рендера: выборка доменов: %w", err)
	}
	defer rows.Close()

	var out []renderPendingDomain
	for rows.Next() {
		var d renderPendingDomain
		if err := rows.Scan(&d.zone, &d.id, &d.name); err != nil {
			return nil, fmt.Errorf("httpworker: проход рендера: выборка доменов: чтение: %w", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("httpworker: проход рендера: выборка доменов: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("httpworker: проход рендера: фиксация выборки: %w", err)
	}
	return out, nil
}

// renderDomainKeys — the pass's domain's q:render keys: a repeated SPA verdict
// over the stored content. The records with content_hash are read from the store
// (DetectSPA is deterministic — the same verdict as at the write, T-057);
// the aliases inherit the original subdomain's verdict; the records without content and
// without an alias are skipped (the content is not stored, T-056 — the T-030
// recheck will rework them). A block read error is not a pass error, but not
// a reason to clear the flag either: the third return keep=true leaves render_pending
// (a transient store failure or a compaction race must not
// lose the SPA task before the T-030 recheck) — the record is skipped with a Warn,
// the pass continues, the next tick will repeat the domain; a ctx cancellation —
// a pass error.
func renderDomainKeys(ctx context.Context, pool db.DB, contents RenderContent, d *renderPendingDomain, log *slog.Logger) (keys []queue.Result, keep bool, err error) {
	rows, err := pool.Query(ctx, renderPendingRowsSQL, d.zone, d.id)
	if err != nil {
		return nil, false, fmt.Errorf("httpworker: проход рендера: домен %s: записи: %w", d.name, err)
	}
	defer rows.Close()

	var recs []renderPendingRow
	for rows.Next() {
		var (
			r        renderPendingRow
			variant  int16
			hash     []byte
			aliasSub *int16
		)
		if err := rows.Scan(&r.subdomain, &variant, &hash, &aliasSub); err != nil {
			return nil, false, fmt.Errorf("httpworker: проход рендера: домен %s: записи: чтение: %w", d.name, err)
		}
		r.variant, r.hash, r.aliasSub = model.Variant(variant), hash, aliasSub
		recs = append(recs, r)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("httpworker: проход рендера: домен %s: записи: %w", d.name, err)
	}

	// The own verdicts — by the content; the keys' order is deterministic
	// (the query's ORDER BY), the aliases — after their own ones.
	spa := make(map[int16]bool)
	keys = make([]queue.Result, 0, len(recs))
	var aliases []renderPendingRow
	for _, r := range recs {
		if r.aliasSub != nil {
			aliases = append(aliases, r)
			continue
		}
		if len(r.hash) != content.HashLen {
			// No content (T-056) or a hash of an invalid length —
			// a guard for direct calls; there is nowhere to take the verdict from.
			continue
		}
		var h content.Hash
		copy(h[:], r.hash)
		body, err := contents.Get(ctx, h)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, false, fmt.Errorf("httpworker: проход рендера: домен %s: чтение контента: %w", d.name, err)
			}
			// A transient failure: the domain's flag is preserved (keep),
			// the next pass will repeat the read.
			log.Warn("проход рендера: контент записи недоступен — домен останется с флагом",
				"domain", d.name, "subdomain_id", r.subdomain, "err", err)
			keep = true
			continue
		}
		if httpfetch.DetectSPA(body).SPA {
			spa[r.subdomain] = true
			keys = append(keys, queue.Result{
				ZoneID: d.zone, DomainID: d.id,
				SubdomainID: r.subdomain, Variant: r.variant, Name: d.name,
			})
		}
	}
	for _, a := range aliases {
		if spa[*a.aliasSub] {
			keys = append(keys, queue.Result{
				ZoneID: d.zone, DomainID: d.id,
				SubdomainID: a.subdomain, Variant: a.variant, Name: d.name,
			})
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].SubdomainID != keys[j].SubdomainID {
			return keys[i].SubdomainID < keys[j].SubdomainID
		}
		return keys[i].Variant < keys[j].Variant
	})
	return keys, keep, nil
}

// RequeueRenderPending — the render rework passes' loop (§5.6
// "are reworked later"): the initial drainage by chunks until exhaustion or
// until a full q:render (picking up the flags accumulated over the idle time — the style of
// CleanupDeleted T-020 / RecheckDaily T-030), then the Interval ticker.
// The zones' priority (T-184, opts.DeprioritizedZones): the loop keeps the
// first stage's exhaustion memory (renderStage) — without it the list's
// zones' debt tail would pay the first stage's idle full seq scan per
// tick and drainage iteration. A pass error — a Warn, the loop lives (the
// OnRun contract, T-012): the next pass will continue from the same place — what is
// cleared is not cleared twice, the unpublished keeps the flag. A ctx cancellation —
// a clean nil (graceful shutdown, §3.4).
func RequeueRenderPending(ctx context.Context, pool RecordDB, contents RenderContent, render *RenderQueue, opts RequeueOptions) error {
	if pool == nil {
		return errors.New("httpworker: проход рендера: пул не задан")
	}
	if contents == nil {
		return errors.New("httpworker: проход рендера: хранилище контента не задано")
	}
	if render == nil {
		return errors.New("httpworker: проход рендера: очередь рендера не задана")
	}
	o := opts.withDefaults()
	if o.Log == nil {
		// The logger's default — here: drain calls requeuePass directly,
		// bypassing the RequeueOnce default.
		o.Log = slog.New(slog.DiscardHandler)
	}
	log := o.Log
	log.Info("проход доработки рендера запущен",
		"interval", o.Interval.String(), "batch", o.BatchSize,
		"deprioritized_zones", o.DeprioritizedZones)

	// The priority stages' memory (T-184) — per loop: repeated passes do not
	// repeat the exhausted first stage's empty pass.
	stage := &renderStage{}

	drain := func() {
		for {
			res, err := requeuePass(ctx, pool, contents, render, o, stage)
			if err != nil {
				// A cancellation during the pass — part of the stop, not an alarm.
				if ctx.Err() == nil {
					log.Warn("проход доработки рендера", "err", err)
				}
				return
			}
			if res.Published > 0 || res.Cleared > 0 {
				// deprioritized — the chunk's domains from the list's zones (T-184):
				// 0 in the drainage window — the priority works, the publications go
				// by the zones NOT from the list first (an order control without
				// a manual SQL).
				log.Info("проход доработки рендера",
					"domains", res.Cleared, "published", res.Published,
					"deprioritized", res.Deprioritized, "full", res.Full)
			}
			// A full chunk — domains may remain; a full queue —
			// wait for the tick (a publication "right up to the threshold" is not needed).
			// A chunk WITHOUT cleared flags — also wait for the tick: the content is not
			// read (keep), the initial drainage does not spin idle —
			// T-181 removed the natural pacer by the expensive selection (the
			// Oct 24 measurement: 137 log firings over 2.7 h ≈ the drain rate),
			// a bare loop over the failing blocks would hammer the DB without a stop.
			if res.Full || res.Selected < o.BatchSize || res.Cleared == 0 {
				return
			}
		}
	}

	drain()
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			drain()
		}
	}
}

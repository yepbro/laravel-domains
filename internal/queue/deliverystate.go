// The delivery automaton of a stream entry (T-260, spec tasks/TZ-fsm.md
// §3 FR-6): the single source of truth about the states and transitions
// of one entry of a working stream (q:dns/q:http/q:render/
// q:render-fallback/q:analyze) inside a consumer group. The state is
// encoded by the entry's membership in the carriers: the stream body (the
// entry itself), the group's PEL (deliveries without acknowledgement; the
// delivery counter RetryCount lives only there) and q:dead (quarantine).
// The formalization does not rewrite the transitions onto an FSM framework
// (TZ-fsm §1): they remain Redis commands with their inherent atomicity;
// this table is what all the writers (listed below) cross-check against.
// The writers live in this same package (Consume, Processing, Claim/bury,
// Refund/RefundUnclassified, RecoverLoop, TrimLoop) — there are no
// external writers: workers and recoverers use these entry points. The
// adjacent automaton of the row's render state —
// internal/renderworker/worker.go (T-259); the domain state —
// internal/scheduler/checkstate.go (T-255).
//
// # States
//
//	D0 "new"          the entry is in the stream, not read by the
//	                   group: no deliveries, the counter does not exist
//	D1 "in flight(n)" the entry is in the group's PEL behind a
//	                   consumer, n ≥ 1 — the delivery counter (the
//	                   first read = 1, each claim +1); XACK is deferred
//	                   until the result is in the DB (spec 4.2) — the
//	                   entry is somebody's work in flight or stalled
//	                   (the worker died/was cancelled)
//	D2 "quarantine"   terminal: the attempts are exhausted
//	                   (n > max_attempts), Dead is published to q:dead,
//	                   the original entry is acknowledged (XACK) — it
//	                   is no longer in the PEL; triage is manual (q:dead
//	                   has no consumer). D2i "unidentifiable" — a
//	                   sub-state of an entry without task_id: the
//	                   synthetic identifier unknown:<stream>:<entry-id>,
//	                   no subject fields (FR-6.1)
//	D3 "acknowledged" terminal: the processing is error-free, the
//	                   result is in the DB, XACK — the entry is not in
//	                   the PEL; the body stays in the stream until the
//	                   trim (MAXLEN/XTRIM, trim.go)
//	D4 "truncated"    terminal: the entry was cut off by the MAXLEN
//	                   policy — for the queue it no longer exists; an
//	                   unacknowledged one (D1) is lost for deliveries,
//	                   recovery — PostgreSQL is the source of truth
//	                   (spec 3.4: lease/re-check), XAUTOCLAIM cleans it
//	                   out of the PEL
//
// # Transitions: state × event × guards × target × side effects
//
//	| # | From | Event | Guards | Target | Side effects | Writer |
//	|---|------|-------|--------|--------|--------------|--------|
//	| T1 | D0 | first read of the group (XREADGROUP ">") | — | D1(1) | counter 1 (set by Redis) | redisq.go Consume |
//	| T2 | D1(n) | error-free processing (the result is written to the DB) | — | D3 | XACK; the body stays in the stream until the trim | reliab.go Processing/ProcessingAttempt |
//	| T3 | D1(n) | processing error | — | D1(n) | without XACK: the entry will come back with a claim; the counter does not change | Processing/ProcessingAttempt |
//	| T4 | D1(n) | claim on idleness (XAUTOCLAIM, MinIdle), classification succeeded | n+1 ≤ max_attempts | D1(n+1) | counter +1, idle 0, pinning to the recoverer's consumer; return to work by the handler | reliab.go Claim + recover.go RecoverLoop |
//	| T5 | D1(n) | claim, classification succeeded | n+1 > max_attempts, task_id present | D2 | Dead with task_id and subject fields (T-105), publication BEFORE the XACK (an XACK failure — a duplicate in q:dead, not a loss); XACK; the OnBury hook (crawler_queue_dead_buried_total, T-154) | reliab.go bury |
//	| T6 | D1(n) | claim, classification succeeded | n+1 > max_attempts, no task_id (the body is outside the package formats) | D2i | Dead with a synthetic unknown:<stream>:<entry-id> without subject fields; XACK; the OnBuryUnidentified hook (crawler_queue_dead_unidentified_total) — before T-260 an eternal claim loop, now a terminal state (FR-6.1) | reliab.go bury |
//	| T7 | D1(n+1) (after the T4 claim) | the classification failed (an XPENDING error), processing did not start | — | D1(n) | the entry is returned as Claim's second result (unclassified); the attempt refund is XCLAIM JUSTID RETRYCOUNT n (RefundUnclassified, the counter is read at the moment of the refund) — the attempt is not burned (FR-6.2); XCLAIM resets idle — the entry ripens for claim_min_idle again (the price of an honest counter); if the refund did not happen — it stays D1(n+1), until the next claim | reliab.go Claim/RefundUnclassified + recover.go |
//	| T8 | D1(n+1) (after the T4 claim) | RecoverLoop cancelled in the middle of a batch, the entry's processing did not start | the time since the claim < claim_min_idle (the T-153 invariant) | D1(n) | the attempt refund is XCLAIM JUSTID RETRYCOUNT n (Refund, T-222); the idle reset — as in T7; an entry that processing has started spends the attempt | recover.go RecoverLoopAttempt |
//	| T9 | D1(n) | the entry was truncated while its PEL record is alive (XADD MAXLEN ~ publication / a manual XTRIM; TrimLoop does not trim unacknowledged entries) | — | D4 | XAUTOCLAIM cleans it out of the PEL (the values are empty), there is no recovery by deliveries | redisq.go Publish + reliab.go Claim |
//	| T10 | D1(n) | the entry left the PEL between the commands (acknowledged, buried or truncated by someone else) | — | outside the PEL (D2/D3/D4) | the classification observation: skip without actions | reliab.go Claim/RefundUnclassified |
//
// # Invariants
//
//	I1  The delivery counter lives only in the entry's PEL (RetryCount):
//	    the entry body is immutable, there are no separate counter keys —
//	    the counter survives worker restarts and is consistent with many
//	    recoverers.
//	I2  XACK only after error-free processing (the result in the DB, T2)
//	    or after publishing Dead (T5/T6) — at-least-once delivery,
//	    repeated processing is idempotent (spec 3.4); an XACK failure
//	    leaves the entry in the PEL — a repeated pass is safe.
//	I3  Dead is published before the XACK of the original entry: an XACK
//	    failure gives a duplicate in q:dead on the next pass — a
//	    quarantine without a consumer; a duplicate is preferable to a
//	    loss.
//	I4  An attempt is spent only by a delivery (the read T1, the claims
//	    T4–T7); a path where processing did not start returns the
//	    attempt (RETRYCOUNT n): cancelling the batch tail (T8, T-222)
//	    and an unfinished classification (T7, FR-6.2) — the T-222 refund
//	    window is widened from "ctx cancellation" to "any unfinished
//	    classification".
//	I5  A refund extinguishes exactly its own increment: the T-153
//	    invariant (the batch pass < claim_min_idle) — outside it a
//	    neighbouring worker could have re-claimed the entry, and the
//	    refund would extinguish somebody else's increment (the refund
//	    is skipped, Warn).
//	I6  An entry without task_id has no cyclic path into bury: the T6
//	    branch — the only way it gets buried, and it is terminal (before
//	    T-260 — an eternal claim loop). Such an entry leaves the PEL by
//	    the normal paths too — successful processing (T2; the message
//	    validators do not check task_id, the body may be recognized by
//	    a stream format), truncation (T9), external acknowledgement
//	    (T10). The synthetic identifier — only for unidentifiable
//	    entries: live tasks (task_id present) carry their own identifier
//	    and subject fields (T5) — the subject of identifiable tasks is
//	    not lost in q:dead.
//
// Non-transitions (they do not change the delivery state): the entry gate
// of T-152 (Writable — the claim is skipped, attempts are not spent); a
// re-read of its own PEL by a consumer is not performed (Consume reads
// only ">"); the monitor reads depths (XLEN/PEL) without group commands.

package queue

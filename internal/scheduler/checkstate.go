// The domain check_state state machine (T-255, spec tasks/TZ-fsm.md §3 FR-1): the single
// source of truth about the states and transitions of the domains.check_state column
// (smallint: 0 pending, 1 queued, 2 done — model.CheckState, spec 6.1).
// The formalization does not rewrite the transitions onto the FSM framework: they remain
// SQL statements with guard predicates (the project's proven model,
// TZ-fsm §1); this specification is the decision table that all
// writers of the column (listed below) must
// consult. Worker writers
// reference it with a single line next to their statements; the
// status change behavior (spec 6.2) is a separate machine (TZ-fsm §3 FR-2); here only
// the side effects related to check_state transitions.
//
// # States
//
//	pending (0)  awaits dispatcher selection (partial index
//	             domains_pending_idx); queued_at = NULL — no lease
//	             (invariant I2)
//	queued (1)   taken into work, lease in effect: the worker must write
//	             the result within scheduler.lease (spec 5.3); queued_at = the moment
//	             of enqueue — the T-028 invariant "queued ⇒ queued_at NOT NULL"
//	             (I1); a NULL mark is a broken invariant, forever outside the main
//	             lease pass (the NULL-age comparison filters it out) — the terminal guard of FR-1.2 (leasenull.go)
//	done (2)     the check episode is finished (DNS/HTTP, spec 7); queued_at =
//	             NULL (I3); render and analysis supplement the result
//	             asynchronously, without changing the state
//
//	status = deleted is NOT a check_state state: a delete file (T-019)
//	changes only the status, check_state stays as it was — the deleted
//	row drops out of all passes via the guard predicates
//	status <> deleted; returning to the cycle happens only through resurrection (R9).
//
// # Transitions: state × event × guards × target × side effects
//
// All guards are evaluated at the moment the row is LOCKED (FOR UPDATE or
// a guarded UPDATE) — a race between selection and writing does not move a row
// whose fate has changed; RETURNING returns only the rows actually moved
// (idempotency of redeliveries and passes, at-least-once,
// spec 3.4). The zone counter deltas (T-178) are kept by the same transition operators
// based on the row's old state; the authority is monitor reconciliation.
//
//	| # | From state | Event | Guards | Target | Side effects | Writer |
//	|---|----------|---------|--------|------|-------------------|----------|
//	| R1 | pending | scheduler enqueue | check_state = pending, status <> deleted | queued | queued_at = now() (DB server); clearing the http_published bit (a new episode with a clean label); zones pending −n / queued +n; publishing to q:dns by RETURNING (the move happens BEFORE publishing: a publishing failure leaves queued without a task — lease R5 will return it) | enqueue.go EnqueueBatch (T-028) |
//	| R2 | queued | recording the DNS episode result, terminal branch (not_delegated / delegated_no_address / temp_error) | status <> deleted, check_state <> done | done | queued_at = NULL (lease finished); last_checked_at = now(); status outcome; temp_attempts = 0 (except temp_error — it keeps it); clearing http_published; zones queued −n / checked +n; retry_mature bucket +1 on a temp_error outcome | dnsworker/result.go resultUpdateStatusSQL (T-036+) |
//	| R3 | queued | recording the DNS episode result, the "has addresses" branch (the outcome is left to the HTTP stage) | the same | done | queued_at = NULL; last_checked_at = now(); temp_attempts = 0; clearing http_published; zones queued −n / checked +n | dnsworker/result.go resultUpdateKeepStatusSQL |
//	| R4 | pending | recording the DNS/HTTP episode result (lease returned mid-episode, duplicate delivery) | the same (HTTP: status <> deleted WITHOUT the check_state guard — a deliberate asymmetry: a redelivery must re-publish q:analyze, the done filter would drop the publication forever) | done | queued_at = NULL; last_checked_at = now(); clearing http_published in the DNS branch (the HTTP branch does not touch the bit); zones pending −n / checked +n (pre-reading the old state FOR UPDATE, T-178) | dnsworker/result.go, httpworker/record.go recordDomainSQL (T-058) |
//	| R5 | queued | lease expiry (lease return) | check_state = queued, status <> deleted, queued_at < now() − scheduler.lease (NULL is filtered out by the comparison — there is no age) | pending | queued_at = NULL; zones queued −n / pending +n; the dispatcher will re-select it — redelivery is safe (spec 3.4) | lease.go LeaseOnce (T-029) |
//	| R6 | queued | terminal guard of the NULL invariant (I1 broken: queued without queued_at — an eternal hang outside R5) | check_state = queued, status <> deleted, queued_at IS NULL | pending | queued_at = NULL (already NULL); zones queued −n / pending +n; the crawler_scheduler_lease_null_returned_total counter (a non-zero steady state is a signal to the operator, docs/runbooks.md §2.9) | leasenull.go LeaseNullOnce (T-255, FR-1.2) |
//	| R7 | done | daily age-based recheck | check_state = done, status <> deleted, status <> invalid (poison rows are not cycled, T-109), last_checked_at < now() − scheduler.recheck_interval | pending | queued_at = NULL; the status outcome is kept (a new check will rewrite it); zones checked −n / pending +n | recheck.go RecheckOnce (T-030) |
//	| R8 | done | temp_error retry: attempts not exhausted | status = temp_error, check_state = done, last_checked_at older than temp_error_retry_after, temp_attempts < N | pending | temp_attempts + 1; zones checked −n / pending +n; decrement of the retry_mature buckets (T-202/T-267); exhausting the attempts is a STATUS transition (delegated_no_address, temp_attempts → 0), check_state stays done | retry.go RetryOnce (T-031) |
//	| R9 | done | resurrection by an add file | status = deleted | pending | status = new; queued_at = NULL; zones total +n / pending +n (the domain was outside the base counter set); flags/dns_mask/last_checked_at are kept | ingest/apply_add.go reviveAddSQL (T-018) |
//	| R10 | — | inserting a new domain by an add file | ON CONFLICT DO NOTHING (a repeated batch is a no-op) | pending | status = new (schema DEFAULT); zones total +n / pending +n | ingest/apply_add.go insertAddSQL (T-018) |
//	| R11 | done, queued, pending | operator's manual recheck | status <> deleted (found by name; pending → pending is an idempotent no-op; queued → pending is equivalent to a manual lease return) | pending | queued_at = NULL; zones by the old state: done → checked −n / pending +n, queued → queued −n / pending +n, pending → no deltas | cmd/crawlerctl/recheck.go recheckUpdateSQL (T-084) |
//	| R12 | done | bulk subdomain recheck (the whole reference table or a zone) | check_state = done, status <> deleted, (optionally zone_id) — there is NO age threshold (all ready ones must be rechecked) | pending | queued_at = NULL; zones checked −n / pending +n | cmd/crawlerctl/manage.go subdomainRecheck*SQL (T-084) |
//	| R13 | queued, pending | diagnosis of a poison row (empty name — the check cannot be performed deterministically) | status <> deleted, check_state <> done | done (status = invalid) | queued_at = NULL; temp_attempts = 0; clearing http_published; zones by the old state; invalid is not returned by recheck R7 — the only way out is manual triage | dnsworker/result.go resultInvalidSQL (T-109) |
//	| R14 | any | delete file | row is not deleted | check_state NOT changed (status = deleted) | removed from the cycle by the guard predicates of all passes; results are cleaned by T-020; zone counters total −n and the old-state column −n | ingest/apply_delete.go (T-019) |
//	| R15 | done | the "hung status" compensator (early recheck: O6 without O7–O9 — the q:http task is lost after XADD) | check_state = done, status = new, last_checked_at < now() − 24 h (NULL is filtered out by the comparison; the column's date precision makes the threshold equivalent to last_checked_at < current_date — the FR-2.2 gauge window, the cohort ripens at midnight per the T-031/T-202 precedent: the midnight influx of live q:http deliveries drains with a duplicated episode — the price of date granularity, absorbed by A3) | pending | queued_at = NULL (done already has NULL); the status outcome and temp_attempts are kept (new is not temp_error); the http_published bit is NOT touched — the R1 enqueue of a new episode will clear it (I4); zones checked −n / pending +n; the crawler_scheduler_stale_requeued_total counter; republishing the lost message is impossible (HTTPTask carries subdomain addresses, stored nowhere, internal/queue/message.go) — only a return to pending via the dispatcher | stalerequeue.go StaleRequeueOnce (T-270, FR-2.2) |
//
// Non-transitions (observations, check_state is not changed):
//
//   - the freshness filter of the q:dns batch (dnsworker/result.go resultPendingSQL):
//     SELECT check_state <> done ∧ status <> deleted — filters out
//     episodes already recorded by a duplicate delivery;
//   - the http_published mark (dnsworker/result.go resultMarkPublishedSQL,
//     T-108): a bit in flags, not a transition — the bit has its own specification (there,
//     at resultMarkPublishedSQL; TZ-fsm §3 FR-3, T-257); clearing the bit is a
//     side effect of R1–R4, R13 (in R4 — only the DNS branch: the
//     dnsworker operators write done from pending too; the HTTP branch recordDomainSQL does not
//     touch the bit — it preserves the other bits except dead_host_cutoff);
//   - the dispatcher DispatchBatch (dispatch.go, T-027): reads pending,
//     does not change the state (the next step, R1, moves it).
//
// # Invariants
//
//	I1  queued ⇒ queued_at IS NOT NULL (T-028). A violation is a writer bug
//	    or manual SQL: NULL is filtered out by the comparison of lease pass R5,
//	    the row hangs in queued forever. The terminal guard is
//	    the repair pass R6 of the same LeaseWatch cycle (FR-1.2, leasenull.go).
//	I2  pending ⇒ queued_at IS NULL: pending has no lease — every writer
//	    of a move to pending clears the mark (R5–R12, R15).
//	I3  done ⇒ queued_at IS NULL: recording the result ends the lease (R2–R4,
//	    R13); a non-NULL mark remains only on queued rows.
//	I4  the only transition into queued is the scheduler enqueue R1: any
//	    return of a row into work goes through pending (the dispatcher selects
//	    only pending) — a fresh episode always passes the clearing of the
//	    http_published bit and sets a new lease mark.
//	I5  status = deleted is excluded by the guard predicates of all transitions,
//	    except R9 (resurrection) and R14 (the deletion itself).
//	I6  transitions are idempotent: the guards re-check the state at the moment
//	    the row is locked; the counters are kept by RETURNING of the rows actually
//	    moved (redeliveries and passes are safe, spec 3.4).

package scheduler

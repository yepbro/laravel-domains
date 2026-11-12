// Scheduler: temp_error rechecking (T-031; spec 5.3, 6.2).
//
// A domain whose check ended with a temporary error (status = temp_error,
// spec 6.2: SERVFAIL/timeouts, "a retry is required") is rechecked not immediately,
// but after temp_error_retry_after (24 h by default, spec 5.3) and no more than
// N times in a row; after the attempts are exhausted the domain gets the final status
// based on the result of the last check.
//
// The RetryOnce pass with a batch of up to opts.BatchSize selects candidates — domains
// in the temp_error status whose check is finished (check_state = done, spec 7)
// and last_checked_at is older than temp_error_retry_after — and splits them by
// the attempt counter domains.temp_attempts (migration 000005) into two decisions,
// each in its OWN transaction under a 40P01 retry (the lock order —
// retryDecide, T-268; the retry assignment is two statements: the decrement
// of the counter buckets is moved out of the zones statement, T-267):
//
//   - fewer than N attempts — a retry is assigned: check_state → pending
//     (the T-027 dispatcher will re-select the domain and put it into q:dns, T-028),
//     the counter is incremented by one;
//   - N or more attempts — the final status delegated_no_address is recorded,
//     the counter is reset to zero.
//
// The final status is delegated_no_address (code 2, spec 6.2): the last
// check ended with SERVFAIL/a timeout, that is, no addresses were obtained for the HTTP stage
// — this is the terminal "no addresses" branch of the spec 7 algorithm
// ("no addresses → status=delegated_no_address, done"). not_delegated
// was rejected: it encodes exactly an NXDOMAIN of the apex (spec 5.4 item 1, RFC 8020),
// while the spec explicitly distinguishes NXDOMAIN and SERVFAIL/timeouts; the set
// does not add new statuses — the seven statuses were confirmed by the customer (spec 13.1).
// A classification error self-corrects in the regular cycle: the domain
// stays in done and is rechecked by recheck_interval (T-030, 6 months).
//
// The temp_attempts counter cannot be expressed without a schema change: check_state is
// a lifecycle enumeration, flags are boolean bit flags,
// dns_mask is a subdomain mask, timestamps do not count attempts, there is no
// result history (spec 3.5, overwrite). Migration 000005 is minimal:
// one smallint column. Resetting the counter on a successful (non-temp_error)
// check is the DNS stage contract (T-036+): a new temp_error episode
// starts from zero; before it is implemented the counter may
// remain non-zero between episodes — the effect is conservative (fewer retries).
//
// Wiring in cmd/scheduler — a later task (the OnRun of framework T-012).

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/model"
)

const (
	// DefaultRetryAfter — the temp_error retry window (spec 5.3:
	// temp_error_retry_after, 24 hours).
	DefaultRetryAfter = 24 * time.Hour

	// DefaultRetryMaxRetries — the maximum retries in a row (spec 5.3: "no more
	// than N times in a row"). The spec does not set the number; 3 — like the delivery
	// attempt counter of the queue (plan T-025): a single retry policy of the system.
	DefaultRetryMaxRetries = 3

	// DefaultRetryBatch — the batch limit: domains per pass, in the style
	// of the scheduler batches (spec 5.3: 1000–10,000) and the background cleanup T-020.
	DefaultRetryBatch = 1000

	// DefaultRetryInterval — the pass interval of the RetryTempErrors cycle at
	// the default settings. The spec does not set the pass frequency: with a
	// retry window of 24 hours, an hourly pass assigns a retry no later than an
	// hour after the domain becomes ready; if retry_after is configured shorter than an hour,
	// the interval shrinks to it (withDefaults), so the cycle does not "oversleep"
	// its own window. In steady state without mature candidates a guard over the
	// maturity buckets (T-202, retryMatureSQL — the T-191 refinement) holds: an hourly
	// tick costs one cheap sum of counter rows, the heavy batch selection
	// is not executed at all; a cohort storm (a mass temp_error episode)
	// is drained by consecutive passes (drain), the pace is set by the batch UPDATEs,
	// not the selection — a seq scan with an early LIMIT stop, pinned in
	// the selection transaction (retryCandidates, T-191).
	DefaultRetryInterval = time.Hour

	// DefaultRetryDrainFails — the cap on CONSECUTIVELY failed batches of one
	// drain (T-268): a batch error no longer wrecks the hourly pass
	// (the Nov 07 16:04 incident — a WARN of a single batch left the drain of
	// 118k domains unprocessed until the next tick), the drain continues
	// with the following batches; the limit stops it on a persistent failure
	// (the DB unavailable) — until the next tick, without hammering the DB in vain.
	DefaultRetryDrainFails = 3
)

// The pass SQL. unnest batches — the internal/db batch UPDATE style (spec 6:
// "writing in batches … UPDATE … FROM unnest(...)"); the guards in WHERE —
// re-reading the state at the moment of the update (races with T-027/T-030/T-018
// do not break the "one increment per episode" invariant); RETURNING returns
// only the rows actually affected — the pass counters are honest.
const (
	// retrySelectSQL — retry candidates: temp_error after a finished
	// check (check_state = done — the increment point: one retry
	// assignment per one check episode; pending already awaits the dispatcher selection,
	// queued is in progress, spec 5.3), the window — by last_checked_at. NULL
	// last_checked_at is not selected: "not checked yet" is not a reason for
	// a retry. ORDER BY is absent deliberately (T-191, the T-181 precedent):
	// the candidate order does not matter — splitting the batch by temp_attempts does
	// not depend on the order, the guards of retryRequeueSQL/retryFinalizeSQL
	// re-check the status and state at the moment of the write (idempotency
	// of spec 3.4), a repeated pass is safe; sorting would hold index
	// access to the table. The window precision is a day: last_checked_at has the type
	// date (spec 6.1).
	//
	// The pass deliberately has no index (T-104): the predicate reduces to
	// status = temp_error, and an index with status in the key or predicate would break
	// the HOT of mass status updates without a check_state change — applying
	// delete files (apply_delete) and recording the final status
	// (retryFinalizeSQL). But even without status in the index a seq scan is not free:
	// the planner lands on the partial domains_recheck_idx (migration
	// 000008) — the check_state = done predicate is provable, the range over the leading
	// last_checked_at seems narrow, and the blind estimate (the histogram
	// of last_checked_at: 92% NULL, the check wave younger than the statistics)
	// promises the planner thousands of rows; in reality the "older than 24 h" window
	// covers almost all done rows, and the range scan degenerates into
	// a marathon over several million rows with a heap visit of each (query
	// #1 of the slow-query log of Oct 25: median 74.8 s, ≈3.6 h of DB time per
	// window). The plan form is pinned: a seq scan with an early LIMIT stop —
	// fractions of a second, measured 195× (T-191) — SET LOCAL in the selection transaction
	// (retryCandidates, the renderSelectPending T-181 precedent).
	retrySelectSQL = "SELECT d.zone_id, d.id, d.temp_attempts FROM domains d " +
		"WHERE d.status = $1 " +
		"AND d.check_state = $2 " +
		"AND d.last_checked_at < $3 " +
		"LIMIT $4"

	// retryPlanNoIndexScanSQL/retryPlanNoBitmapScanSQL/retryPlanNoIOSQL —
	// pinning the plan form of the candidate selection (T-191, the T-181 precedent):
	// SET LOCAL in the retryCandidates transaction disables index scan, bitmap
	// scan and index-only scan until the selection statement — all index
	// forms of access to domains are closed (the PK and both partials), a seq scan with an early
	// LIMIT stop guarantees the batch in fractions of a second regardless of
	// statistics (the planner's estimate cannot be trusted — see
	// retrySelectSQL). SET LOCAL acts for this transaction only,
	// the other pool queries are unaffected.
	retryPlanNoIndexScanSQL  = "SET LOCAL enable_indexscan = off"
	retryPlanNoBitmapScanSQL = "SET LOCAL enable_bitmapscan = off"
	retryPlanNoIOSQL         = "SET LOCAL enable_indexonlyscan = off"

	// retryRequeueSQL — assigning a retry: check_state → pending (the
	// T-027 dispatcher will re-select the domain), the attempt counter + 1. The guard
	// status = temp_error AND check_state = done: a domain that managed to leave
	// done bypassing the pass (a recheck T-030, a lease return T-029, a resurrection
	// by an add file T-018) is not incremented again. The zone counter
	// deltas (checked −n / pending +n) — the CTE zc of the same statement over
	// the RETURNING zone_id (T-178, incremental-counters spec §6.2: the predicate pins the old
	// done state); the outer SELECT — the keys of the moved rows with the
	// episode date (last_checked_at — the target of the tail decrement of the maturity
	// buckets, retryRequeueMatureSQL). The statement is strictly "domains →
	// zones" (T-267): the bucket decrement is taken out of it as the SECOND statement
	// of the decision transaction — unreferenced DML-CTEs of zones and the counter of one
	// statement execute in an unpredictable order (the PG documentation,
	// the caveat in zonestats.go; the Nov 07 incident — the edge "counters → zones"
	// against zone holders, runbooks.md §2.8), while the statements of a transaction
	// execute in textual order; the zones are held until Commit —
	// the "domains → zones → counters" convention is upheld, the edge
	// "counters → domains" (eliminated by splitting the decision transactions,
	// 15ff3a3) does not return: after the decrement the transaction captures no new
	// domains rows.
	retryRequeueSQL = "WITH upd AS (UPDATE domains d " +
		"SET check_state = $3, temp_attempts = d.temp_attempts + 1 " +
		"FROM unnest($1::int2[], $2::int8[]) AS k(zone_id, domain_id) " +
		"WHERE d.zone_id = k.zone_id AND d.id = k.domain_id " +
		"AND d.status = $4 AND d.check_state = $5 " +
		"RETURNING d.zone_id, d.id, d.last_checked_at), " +
		"zc AS (UPDATE zones AS z " +
		"SET domains_checked = z.domains_checked - c.n, " +
		"domains_pending = z.domains_pending + c.n " +
		"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c " +
		"WHERE z.id = c.zone_id) " +
		"SELECT u.zone_id, u.id, u.last_checked_at FROM upd AS u"

	// retryRequeueMatureSQL — the tail decrement of the maturity buckets
	// of the retry assignment (T-202/T-267): the second statement of the decision
	// transaction AFTER the move (retryRequeueSQL: upd+zc), by the
	// episode dates of the moved rows — the RETURNING of the first statement
	// (the move to pending finishes off the candidate: the maturity moment is derived
	// from last_checked_at, the same key as the increment of the result
	// write — its current_date; a key desync would leave the decrement
	// without a target). The parameter — the batch of dates unnest($1::date[]);
	// ORDER BY determines the capture of bucket rows by ascending date
	// (= the name: old → yesterday) — a common order with the monitor
	// pass (cleaning old → yesterday → today), without an AB-BA cycle on
	// mixed batch dates (review T-202, P2). In the same transaction
	// as the move: a decrement failure rolls back the pair entirely (§3.4),
	// the candidates remain the next pass's candidates — unlike the
	// autocommit-separated statements of the DNS stage result write
	// (dnsworker/result.go, the ±1 window until reconciliation); on an empty batch
	// (the guard blocked everyone) the statement is not executed at all.
	retryRequeueMatureSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"SELECT 'retry_mature:' || to_char(d, 'YYYY-MM-DD'), -count(*), now() " +
		"FROM unnest($1::date[]) AS d " +
		"GROUP BY d ORDER BY d " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()"

	// retryFinalizeSQL — attempts exhausted: the final status per the last
	// check — delegated_no_address (see the comment at the top of the file),
	// the counter to zero: the next temp_error episode will start with a clean
	// counter. The guard is the same as for the retry assignment. No zone counter
	// deltas: check_state stays done, the status distribution
	// is not maintained by increments — monitor reconciliation writes it (incremental-counters
	// spec §13.3, T-178). The CTE rm decrements the maturity bucket (T-202):
	// finalization, like the retry assignment, finishes off the candidate — without the
	// decrement the bucket would keep the guard open after the drain (idle
	// seq scans every tick until the reconciliation pass); ORDER BY —
	// the determination of the bucket row capture (review T-202, P2). The decrement
	// is deliberately KEPT in the statement itself (T-267 moved out the edge
	// "counters → zones" of the hot requeue): rm references upd —
	// the only CTE whose execution is guaranteed after the move; the statement does not
	// touch zones — the lock order "domains → counters"
	// has no edge against zone holders, a separate statement would change
	// nothing.
	retryFinalizeSQL = "WITH upd AS (UPDATE domains d " +
		"SET status = $3, temp_attempts = 0 " +
		"FROM unnest($1::int2[], $2::int8[]) AS k(zone_id, domain_id) " +
		"WHERE d.zone_id = k.zone_id AND d.id = k.domain_id " +
		"AND d.status = $4 AND d.check_state = $5 " +
		"RETURNING d.zone_id, d.id, d.last_checked_at), " +
		"rm AS (INSERT INTO crawler_counters (name, value, updated_at) " +
		"SELECT 'retry_mature:' || to_char(u.last_checked_at, 'YYYY-MM-DD'), -count(*), now() " +
		"FROM upd AS u GROUP BY u.last_checked_at " +
		"ORDER BY u.last_checked_at " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()) " +
		"SELECT u.zone_id, u.id FROM upd AS u"
)

// retryMatureSQL — the pass guard over the sum of the temp_error maturity buckets
// (T-202, the refinement of the T-191 guard: the status_temp_error counter did not distinguish
// mature candidates from immature ones — 543k temp_error with an empty maturity
// window held a seq scan without an early stop on every hourly tick,
// median 51 s / max 239 s after the T-191 fix). The bucket
// crawler_counters['retry_mature:YYYY-MM-DD'] — the temp_error episodes
// of the day (the episode date = the last_checked_at of the result write): +1 is written by
// the result write increments (httpworker/record.go and the terminal
// temp_error branch of the DNS stage dnsworker/result.go — with a separate
// autocommit statement after the write, T-267, — both sources
// of episodes), −n — the retry decisions: the retry assignment with the tail
// statement retryRequeueMatureSQL, finalization — the rm CTE of the
// statement itself (the candidate is exhausted); the authority is monitor reconciliation
// (the FILTER positions of the pass, T-202). The sum of the buckets of elapsed days is
// the number of mature candidates: last_checked_at is a date (migration 000001),
// the "older than 24 h" window over a date column ripens in cohorts at midnight, hence
// the bucket key is the episode date (the equivalent of "the ripening moment now+24 h,
// rounded to the hour": at the column's date precision all episodes of a day
// ripen together at the midnight of the next day — hourly granularity
// degenerates into daily, the format inherits T-179; the same key is produced by both
// the increment (current_date) and the decrement (last_checked_at) —
// a key desync would leave the decrement without a target). The range over
// the PK prefix 'retry_mature:' lexicographically cuts off the bucket of
// the current day (name < …current_date: today's episodes are not
// mature yet) — the DB computes the day name, Go clocks do not participate in the bound
// (the T-179 style).
// coalesce — the bucket rows may be absent entirely (before the first increment
// or reconciliation pass): an honest zero, not an error — unlike
// status_temp_error, nobody guarantees the bucket rows; their absence
// is exactly "no mature ones". A value ≤ 0 — no mature ones: the heavy selection
// (a seq scan over domains) is not executed at all, steady state costs one
// sum over an index range of a couple of rows.
var retryMatureSQL = "SELECT coalesce(sum(value), 0) FROM crawler_counters " +
	"WHERE name >= 'retry_mature:' " +
	"AND name < 'retry_mature:' || to_char(current_date, 'YYYY-MM-DD')"

// RetryDB — the pool for the pass: a db.DB from which a transaction can be started.
// The batch decisions (the retry assignment and the final status) run in SEPARATE
// transactions (retryDecide): each on its own upholds the lock order convention
// "domains → zones → counters, counters last"
// (docs/schema.md, the crawler_counters section); a shared transaction would capture
// the finalization domains rows while already holding zones and counter buckets —
// the edge "counters → domains" would close the 40P01 deadlock cycle with the
// result write transactions of the http-worker (the retry wave incident: batches of 1000,
// dozens of retry_mature buckets until the end of the tx). The retry assignment
// runs in its own transaction with TWO statements (T-267): the move
// (upd+zc), then the bucket decrement (retryRequeueMatureSQL) — the statements
// of a transaction execute in textual order, the zones are held until
// Commit. The batch atomicity is not critical: the guards of both UPDATEs
// are idempotent, an unfinished half is safely picked up by the next
// pass (spec 3.4);
// *pgxpool.Pool satisfies the interface.
type RetryDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// compile-time: the real pool fits the interface.
var _ RetryDB = (*pgxpool.Pool)(nil)

// RetryOptions — pass parameters; zero values are replaced
// with defaults (withDefaults, the ingest.CleanupOptions style).
type RetryOptions struct {
	// RetryAfter — the temp_error retry window: candidates are domains with
	// last_checked_at older (DefaultRetryAfter; config
	// scheduler.temp_error_retry_after, spec 5.3).
	RetryAfter time.Duration
	// MaxRetries — the maximum retries in a row (DefaultRetryMaxRetries;
	// config scheduler.temp_error_max_retries, spec 5.3: "no more than N times
	// in a row").
	MaxRetries int
	// BatchSize — the maximum domains in one pass (DefaultRetryBatch);
	// the batch limit is set by the candidate SELECT.
	BatchSize int
	// Interval — the pass period of the RetryTempErrors cycle; does not affect a one-off
	// RetryOnce. Zero/negative — min(RetryAfter,
	// DefaultRetryInterval), see DefaultRetryInterval.
	Interval time.Duration
	// Metrics — the service's Prometheus registry (T-268): the counter of
	// 40P01 retries of the decision transactions crawler_db_deadlock_retried_total; nil — without
	// metrics (isolated assemblies/tests).
	Metrics *metrics.Registry
	// Log — the RetryTempErrors loop logger; nil — records suppressed
	// (slog.DiscardHandler). RetryOnce does not log — the outcome is returned
	// as RetryResult.
	Log *slog.Logger
}

// withDefaults replaces non-positive values with defaults.
func (o RetryOptions) withDefaults() RetryOptions {
	if o.RetryAfter <= 0 {
		o.RetryAfter = DefaultRetryAfter
	}
	if o.MaxRetries <= 0 {
		o.MaxRetries = DefaultRetryMaxRetries
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultRetryBatch
	}
	if o.Interval <= 0 {
		// No longer than its own retry window: with a short retry_after
		// (tests, experiments) the cycle must wake up more often than the window.
		o.Interval = min(o.RetryAfter, DefaultRetryInterval)
	}
	return o
}

// RetryResult — the outcome of one RetryOnce pass.
type RetryResult struct {
	// Requeued — domains assigned a retry (check_state → pending,
	// temp_attempts + 1); after the guard — only the rows actually moved.
	Requeued int
	// Finalized — domains with the recorded final status
	// (delegated_no_address, temp_attempts → 0).
	Finalized int
}

// retryCandidate — a pass candidate: the domain key and the attempt counter.
type retryCandidate struct {
	zoneID   int16
	id       int64
	attempts int16
}

// RetryOnce performs one recheck pass (spec 5.3, 6.2):
// up to opts.BatchSize domains in the temp_error status with a finished
// check and last_checked_at older than RetryAfter receive either
// an assigned retry (check_state → pending, temp_attempts + 1 — fewer
// than MaxRetries attempts), or the final status delegated_no_address (the attempts
// are exhausted, temp_attempts → 0). The batch decisions run in SEPARATE
// transactions (retryDecide): each upholds the lock order
// "domains → zones → counters" — a shared transaction would capture
// the finalization domains rows while already holding zones and retry_mature buckets
// (the edge "counters → domains", the 40P01 deadlock cycle with the result
// write of the http-worker); the retry assignment is TWO statements of its
// transaction (the move upd+zc, then the bucket decrement by the moved rows'
// dates, T-267 — PG does not guarantee the order of unreferenced DML-CTEs of one
// statement); a failure between the halves is safe — the guards of both UPDATEs
// are idempotent, the unfinished half is picked up by the next pass
// (spec 3.4). Before the selection — the guard over the sum of the maturity buckets
// crawler_counters['retry_mature:…'] of the elapsed days (T-202,
// retryMatureCount): a zero sum returns an empty result without the
// heavy selection. A repeated pass is safe (spec 3.4): domains in
// pending are no longer candidates, the guards in the UPDATE re-check the state
// at the moment of the write.
func RetryOnce(ctx context.Context, pool RetryDB, opts RetryOptions) (RetryResult, error) {
	if pool == nil {
		return RetryResult{}, errors.New("scheduler: повторные проверки temp_error: пул не задан")
	}
	o := opts.withDefaults()

	// The guard over the maturity buckets (T-202): before the heavy selection — the sum of the
	// elapsed days' buckets (retryMatureCount); 0 → an empty result without the selection
	// and a transaction (drain stops correctly). A negative sum —
	// a visible drift of decrements bypassing the increments (candidates that left
	// bypassing the retry: a temp_error domain deleted by a delete file, the age-based
	// recheck T-030): reconciliation will even it out, while the guard must stay silent —
	// a silent counter bug must not hammer the DB (the T-191 legacy).
	mature, err := retryMatureCount(ctx, pool)
	if err != nil {
		return RetryResult{}, err
	}
	if mature <= 0 {
		return RetryResult{}, nil
	}

	candidates, err := retryCandidates(ctx, pool, o.BatchSize, o.RetryAfter)
	if err != nil {
		return RetryResult{}, err
	}
	if len(candidates) == 0 {
		// The buckets see the mature ones, the selection does not (bucket drift: a candidate left
		// bypassing the increments/decrements, OR rare mature ones are scattered and the early
		// LIMIT stop did not catch them): the decision transactions do not open,
		// one idle seq scan per tick until the nearest reconciliation —
		// the residual case of the T-191 spec §4.3, narrowed by T-202 from "all temp_error"
		// to "the bucket sum".
		return RetryResult{}, nil
	}

	var (
		requeueZones []int16
		requeueIDs   []int64
		finalZones   []int16
		finalIDs     []int64
	)
	for _, c := range candidates {
		if int(c.attempts) < o.MaxRetries {
			requeueZones = append(requeueZones, c.zoneID)
			requeueIDs = append(requeueIDs, c.id)
		} else {
			finalZones = append(finalZones, c.zoneID)
			finalIDs = append(finalIDs, c.id)
		}
	}

	var res RetryResult
	met := newRetryMetrics(o.Metrics)
	if len(requeueZones) > 0 {
		n, err := retryDecide(ctx, pool, retryRequeueSQL, retryRequeueMatureSQL, "назначение повтора", requeueZones, requeueIDs,
			int16(model.CheckStatePending), int16(model.StatusTempError), int16(model.CheckStateDone), met)
		if err != nil {
			return RetryResult{}, err
		}
		res.Requeued = n
	}
	if len(finalZones) > 0 {
		n, err := retryDecide(ctx, pool, retryFinalizeSQL, "", "итоговый статус", finalZones, finalIDs,
			int16(model.StatusDelegatedNoAddress), int16(model.StatusTempError), int16(model.CheckStateDone), met)
		if err != nil {
			return RetryResult{}, err
		}
		res.Finalized = n
	}
	return res, nil
}

// retryMetrics — the recheck pass instruments (T-268): the counter of
// 40P01 retries of the decision transactions; nil — no-op (the
// backlogLoopMetrics style).
type retryMetrics struct {
	dbRetried prometheus.Counter
}

// newRetryMetrics — instruments in the reg registry (nil — nil). The counter of
// 40P01 retries — a single name/HELP for all services (the
// internal/metrics constructor, T-268); registration is idempotent — building it on
// every drain pass shares the vector of the first call.
func newRetryMetrics(reg *metrics.Registry) *retryMetrics {
	if reg == nil {
		return nil
	}
	return &retryMetrics{
		dbRetried: metrics.NewDBDeadlockRetried(reg),
	}
}

// dbDeadlockRetried — the transaction was retried after a deadlock (T-268); nil —
// no-op.
func (m *retryMetrics) dbDeadlockRetried(attempt int) {
	if m == nil {
		return
	}
	m.dbRetried.Inc()
}

// retryDecide — one batch decision (a retry assignment or the final
// status) in its own transaction. The transaction split is a lock
// order invariant (docs/schema.md, the crawler_counters section): TWO
// decisions in one transaction capture the second half's domains rows
// while already holding the zones and retry_mature:{dates} buckets of the first — the edge
// "counters → domains" against the result write transactions of the http-worker
// and the reconciliation pass of monitor (both go "domains → … → counters" and
// wait for their domains rows before the counters) closed the 40P01 deadlock cycle:
// 9 cases in 3 h on the retry wave, the q:http task was redelivered with a full
// repeated HTTP episode. Separate transactions return each to
// the convention, the cycles through the http-worker break.
// The retry assignment runs in its transaction with TWO statements
// (T-267, matureSQL = retryRequeueMatureSQL): the move (retryRequeueSQL:
// upd → zc), then the tail decrement of the maturity buckets by the first one's keys
// (the episode dates) — the statements of a transaction execute in
// textual order, unlike the unreferenced DML-CTEs of one statement
// (PG does not guarantee the order, the caveat in zonestats.go; mixing zc/rm
// of one statement created the edge "counters → zones" — the
// Nov 07 incident, runbooks.md §2.8). The zones are held until Commit — the order
// "domains → zones → counters" is the same, the edge "counters → domains"
// does not return: after the decrement the transaction captures no new
// domains rows. matureSQL = "" — a decision without a decrement
// (finalization: rm in the statement itself, references upd). The decrement
// is executed only over a non-empty set of keys (the guard blocked
// everyone — the buckets are not touched).
// The decision transaction runs under a 40P01 retry (T-268, met — the counter
// hook of the retries): the attempt is repeated IN FULL — the guards of both UPDATEs
// are idempotent (rows moved by the first attempt are not moved again,
// a rolled-back attempt leaves no halves), residual lock cycles
// are absorbed in place, without wrecking the drain.
// A failure of any statement rolls back the half entirely (the move and the
// decrement are one transaction, spec 3.4): the interrupted half leaves no
// half-done changes inside itself, the candidates remain the next
// pass's candidates (idempotency); the committed half
// remains (the moved ones are no longer candidates). A half error (after
// the retries are exhausted) returns a pass error — RetryResult
// is discarded; the RetryTempErrors drain will retry the batch on the same tick
// (up to DefaultRetryDrainFails in a row), on a persistent failure — on the
// next tick.
func retryDecide(ctx context.Context, pool RetryDB, sql, matureSQL, kind string,
	zoneIDs []int16, domainIDs []int64, set, guardStatus, guardState int16, met *retryMetrics) (int, error) {
	var n int
	if err := db.RetryDeadlock(ctx, db.DefaultDeadlockRetry, met.dbDeadlockRetried, func() error {
		var err error
		n, err = retryDecideOnce(ctx, pool, sql, matureSQL, kind, zoneIDs, domainIDs, set, guardStatus, guardState)
		return err
	}); err != nil {
		return 0, err
	}
	return n, nil
}

// retryDecideOnce — ONE attempt of the decision transaction (the 40P01
// retry point — retryDecide, T-268).
func retryDecideOnce(ctx context.Context, pool RetryDB, sql, matureSQL, kind string,
	zoneIDs []int16, domainIDs []int64, set, guardStatus, guardState int16) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("scheduler: повторные проверки temp_error: транзакция решения (%s): %w", kind, err)
	}
	// The rollback is unconditional: after Commit — a no-op (the T-044 style). The interrupted
	// half leaves no half-done changes inside itself (spec 3.4).
	defer func() { _ = tx.Rollback(ctx) }()

	keys, err := retryUpdate(ctx, tx, sql, kind, zoneIDs, domainIDs, set, guardStatus, guardState)
	if err != nil {
		return 0, err
	}
	if matureSQL != "" && len(keys) > 0 {
		days := make([]time.Time, len(keys))
		for i, k := range keys {
			days[i] = k.day
		}
		if _, err := tx.Exec(ctx, matureSQL, days); err != nil {
			return 0, fmt.Errorf("scheduler: повторные проверки temp_error: %s: декремент корзин зрелости: %w", kind, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("scheduler: повторные проверки temp_error: фиксация решения (%s): %w", kind, err)
	}
	return len(keys), nil
}

// retryCandidates selects up to limit retry candidates (retrySelectSQL) —
// the single point of the pass's heavy selection: the domain keys and their attempt
// counters for splitting the batch. The selection runs in an explicit transaction with SET LOCAL
// enable_indexscan/bitmapscan/indexonlyscan = off (T-191, the
// renderSelectPending T-181 precedent): pinning the plan form — a seq scan over
// domains with an early LIMIT stop; without pinning the planner lands
// on the partial domains_recheck_idx and degenerates the retry window into a marathon
// (median 74.8 s on the Oct 25 measurement — the retrySelectSQL comment).
// The transaction is only the pinning shell: the SELECT reads
// committed data, commit/rollback are substantively equivalent.
func retryCandidates(ctx context.Context, pool RetryDB, limit int, retryAfter time.Duration) ([]retryCandidate, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("scheduler: повторные проверки temp_error: транзакция выборки: %w", err)
	}
	// The rollback is unconditional: after Commit — a no-op (the T-044 style).
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, retryPlanNoIndexScanSQL); err != nil {
		return nil, fmt.Errorf("scheduler: повторные проверки temp_error: закрепление плана: %w", err)
	}
	if _, err := tx.Exec(ctx, retryPlanNoBitmapScanSQL); err != nil {
		return nil, fmt.Errorf("scheduler: повторные проверки temp_error: закрепление плана: %w", err)
	}
	if _, err := tx.Exec(ctx, retryPlanNoIOSQL); err != nil {
		return nil, fmt.Errorf("scheduler: повторные проверки temp_error: закрепление плана: %w", err)
	}

	cutoff := time.Now().Add(-retryAfter)
	rows, err := tx.Query(ctx, retrySelectSQL,
		int16(model.StatusTempError), int16(model.CheckStateDone), cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("scheduler: повторные проверки temp_error: выборка кандидатов: %w", err)
	}
	defer rows.Close()

	candidates := make([]retryCandidate, 0, limit)
	for rows.Next() {
		var c retryCandidate
		if err := rows.Scan(&c.zoneID, &c.id, &c.attempts); err != nil {
			return nil, fmt.Errorf("scheduler: повторные проверки temp_error: чтение кандидатов: %w", err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scheduler: повторные проверки temp_error: выборка кандидатов: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("scheduler: повторные проверки temp_error: фиксация выборки: %w", err)
	}
	return candidates, nil
}

// retryMatureCount — the guard of the recheck pass (T-202): the sum of
// the maturity buckets crawler_counters['retry_mature:…'] of the elapsed days
// (retryMatureSQL; maintained by the result write increments, the retry
// decrements and monitor reconciliation — the guard reads the mature candidates, not
// the whole temp_error status, as T-191 did). coalesce in the SQL guarantees one
// row even without bucket rows (an honest zero): an empty set is a protocol
// break, not "no buckets", and must be an error.
func retryMatureCount(ctx context.Context, pool RetryDB) (int64, error) {
	rows, err := pool.Query(ctx, retryMatureSQL)
	if err != nil {
		return 0, fmt.Errorf("scheduler: повторные проверки temp_error: корзины зрелости: %w", err)
	}
	defer rows.Close()
	var n int64
	if !rows.Next() {
		// The read failure first: a connection break is not "an empty sum"
		// (the coalesce in the SQL always answers with a row).
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("scheduler: повторные проверки temp_error: корзины зрелости: %w", err)
		}
		return 0, fmt.Errorf("scheduler: повторные проверки temp_error: корзины зрелости: пустой ответ агрегата")
	}
	if err := rows.Scan(&n); err != nil {
		return 0, fmt.Errorf("scheduler: повторные проверки temp_error: корзины зрелости: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("scheduler: повторные проверки temp_error: корзины зрелости: %w", err)
	}
	return n, nil
}

// retryKey — the key of a moved batch row: the zone, the domain and the
// episode date (last_checked_at) — the target of the tail decrement of the
// maturity buckets (T-202/T-267).
type retryKey struct {
	zone int16
	id   int64
	day  time.Time
}

// retryUpdate executes the batch UPDATE … RETURNING and returns the keys of
// the domains actually moved (after the guards — like the cleanup RETURNING,
// T-020: a second concurrent transaction that made it first is not counted).
// The retry assignment returns the keys with the episode date (the third
// RETURNING column — the target of the bucket decrement, T-267); finalization — the pair
// (zone, id). set — the value of the SET column (the check_state of the retry or the
// final status), guardStatus/guardState — the WHERE guard.
func retryUpdate(ctx context.Context, tx pgx.Tx, sql, kind string,
	zoneIDs []int16, domainIDs []int64, set, guardStatus, guardState int16) ([]retryKey, error) {
	rows, err := tx.Query(ctx, sql, zoneIDs, domainIDs, set, guardStatus, guardState)
	if err != nil {
		// The result at err != nil is discarded by the caller (RetryOnce
		// returns RetryResult{}, err) — there are no equivalent mutants.
		return nil, fmt.Errorf("scheduler: повторные проверки temp_error: %s: %w", kind, err)
	}
	defer rows.Close()

	withDay := sql == retryRequeueSQL // the keys with the episode date — the target of the bucket decrement (T-267)
	keys := make([]retryKey, 0, len(zoneIDs))
	for rows.Next() {
		var k retryKey
		var scanErr error
		if withDay {
			scanErr = rows.Scan(&k.zone, &k.id, &k.day)
		} else {
			scanErr = rows.Scan(&k.zone, &k.id)
		}
		if scanErr != nil {
			// The keys at a Scan error are not read by the caller (see above),
			// the return branch is covered by the test TestRetryOnceRowsErrors.
			return nil, fmt.Errorf("scheduler: повторные проверки temp_error: %s: чтение ключей: %w", kind, scanErr)
		}
		keys = append(keys, k)
	}
	if err := rows.Err(); err != nil {
		// The keys at a rows.Err() error are not read by the caller (see above),
		// the return branch is covered by the test TestRetryOnceRowsErrors.
		return nil, fmt.Errorf("scheduler: повторные проверки temp_error: %s: %w", kind, err)
	}
	return keys, nil
}

// RetryTempErrors — the background recheck task (spec 5.3): on
// each ticker tick it drains all ripened candidates in RetryOnce batches
// (the first drain is immediate at start: what accumulated during the downtime is
// processed in full, not one batch per tick), runs until ctx is cancelled and returns nil —
// the OnRun contract of the service framework (T-012: a clean shutdown on
// SIGTERM). A batch error does not interrupt the drain (T-268): it continues
// with the following batches until DefaultRetryDrainFails failed in a row, then —
// the next tick (the loop lives, the pass is idempotent, spec 3.4).
func RetryTempErrors(ctx context.Context, pool RetryDB, opts RetryOptions) error {
	if pool == nil {
		return errors.New("scheduler: повторные проверки temp_error: пул не задан")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("фоновые повторные проверки temp_error",
		"interval", o.Interval.String(), "batch", o.BatchSize,
		"retry_after", o.RetryAfter.String(), "max_retries", o.MaxRetries)

	// The drain until the candidates are exhausted (the LeaseWatch/RecheckDaily style):
	// one tick processes all ripened batches, not one — otherwise
	// the retry queue would dissipate at the rate of BatchSize per interval
	// (1000/hour by default: a mass DNS failure would stay in temp_error
	// for months). Batches repeat while they come back full; rows moved
	// to pending are not selected again — the drain is finite, the moves
	// are picked up by the main enqueue loop (T-028).
	// A batch error does not wreck the drain (T-268: the Nov 07 16:04 incident — a WARN
	// of a single batch left 118k domains unprocessed until
	// the next tick): the batch is retried (a 40P01 of the decision is absorbed by the
	// retryDecide retry even earlier, other errors — a persistent failure — come
	// through here) until DefaultRetryDrainFails consecutive failures accumulate;
	// a successful batch resets the counter — the drain lives as long as there is progress.
	drain := func() {
		var total RetryResult
		fails := 0
		for {
			res, err := RetryOnce(ctx, pool, o)
			switch {
			case err != nil && ctx.Err() != nil:
				// Cancellation during a pass is part of the stop, not an alarm.
				return
			case err != nil:
				fails++
				if fails >= DefaultRetryDrainFails {
					log.Warn("дренаж повторных проверок temp_error: предел упавших пачек — до следующего тика",
						"err", err, "failed_batches", fails,
						"requeued", total.Requeued, "finalized", total.Finalized)
					return
				}
				log.Warn("дренаж повторных проверок temp_error: пачка не прошла — дренаж продолжается",
					"err", err, "failed_batches", fails,
					"requeued", total.Requeued, "finalized", total.Finalized)
				continue
			}
			fails = 0
			total.Requeued += res.Requeued
			total.Finalized += res.Finalized
			if res.Requeued+res.Finalized < o.BatchSize {
				if total.Requeued > 0 || total.Finalized > 0 {
					log.Info("дренаж повторных проверок temp_error",
						"requeued", total.Requeued, "finalized", total.Finalized)
				}
				return // an incomplete batch — the set is exhausted
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

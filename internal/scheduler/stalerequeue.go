// Scheduler: the "hung status" compensator (T-270, spec tasks/TZ-fsm.md §3
// FR-2.2) — an early recheck of domains status = new ∧ check_state = done,
// the R15 move of the check_state machine specification (checkstate.go).
//
// The anomaly (O6 without O7–O9, the common status table of internal/model/enums.go):
// the DNS stage recorded the "has addresses" branch (check_state → done, the status
// stays new — the HTTP stage decides the outcome), and the q:http task was lost already
// AFTER the XADD — there will be no delivery (a Redis restart without persistence,
// docs/redis.md §5; record death in an OOM window,
// docs/load-measurements.md §11.1 — the cohort of the incident week
// Oct 22–29, 2026, 1865 domains, exposed by the T-256 gauge). The row drops out of
// all regular returns: the lease pass T-029 sees only queued,
// the age-based recheck T-030 ripens by recheck_interval (4380 h ≈
// half a year) — the loss class stays open for recurrence.
//
// The compensator returns such rows to pending — the dispatcher
// (T-027) will re-select them, the episode will go through the pipeline again. The lost
// q:http message cannot be republished: HTTPTask carries the subdomain addresses of the DNS episode,
// stored nowhere (internal/queue/message.go) — a return to pending
// through the regular pipeline is the only option; the repeated check
// is idempotent (at-least-once, spec 3.4). A duplicate task after the revival
// of a stalled worker (a late PEL claim) is absorbed by the idempotency
// of the HTTP stage write — the A3 asymmetry: recordDomainSQL writes the outcome WITHOUT
// the done guard, overwrites the episode with DELETE+INSERT and republishes
// q:analyze (httpworker/record.go).
//
// The threshold is the gauge window: 24 h over the date column (spec 6.1) — a cohort ripens at
// midnight (date < now() − 24 h is equivalent to date < current_date,
// the retrySelectSQL T-031 and buckets T-202 precedent, the
// zonestats.go comment) — the compensator and the gauge see the same cohort.
// The price of date granularity is the midnight influx: the minimum age
// of a row in the cohort is MINUTES (an episode at 23:59 is already "older
// than a day" at 00:05), the window intersects with live q:http deliveries (the PEL claim
// claim_min_idle 10 min × max_attempts 3 — the task is legitimately in flight
// for up to ~30 min, internal/queue/reliab.go). A duplicate task of the midnight
// influx is absorbed by the A3 asymmetry: the HTTP outcome write goes without the
// done guard, overwrites the episode with DELETE+INSERT and republishes q:analyze —
// correctness is preserved, the price is a repeated episode. In steady state
// the q:http backlog is minutes: the midnight cohort empties before the first tick
// (the guard is closed by the zero of the nearest reconciliation); an incident backlog
// that survived midnight is drained once with a duplicate — an expected
// counter spike, not losses (runbooks §2.10). Rows of TODAY'S
// episodes are not seen by the compensator at all: today's in-flight tasks
// are not touched.
//
// An empty set — a cheap probe (a guard, the T-202 retryMatureSQL style):
// before the selection the crawler_counters['new_done_stale'] row is read —
// the anomaly level written ONLY by monitor reconciliation (T-256:
// the FILTER column f_new_done_stale of the zonestats.go pass, no increments),
// the filter window is identical to the selection window. In steady state (the cohort
// healed) the zero closes the guard — domains is not scanned at all,
// an hourly tick costs one PK lookup; the freshness of the level is
// monitor.zone_stats_interval (30 min), the total repair delay ≤
// the cycle interval + the reconciliation interval. Guard drift (the counter sees
// the cohort, the selection does not) — one idle seq scan per tick until the nearest
// reconciliation (the residual case of the T-191 spec §4.3).
//
// The batch selection — a seq scan with an early LIMIT stop, pinned by SET
// LOCAL in the selection transaction (T-191, the retryCandidates/T-181 sample):
// there is deliberately no index over the status = new predicate (status in the key or
// predicate of an index would break the HOT of mass status updates,
// docs/schema.md), while the partial domains_recheck_idx (check_state = done,
// T-104) is "provable" for the planner — the blind estimate promises thousands of rows,
// and the range scan degenerates into a marathon over millions of done rows with
// a heap visit of each (query #1 of the slow-query log of Oct 25: median 74.8 s
// — the retrySelectSQL comment). The move is a SEPARATE transaction
// with PK probes (the leaseNullUpdateSQL form, T-200): the plan pinning is not
// inherited by the move, unnest → PK probes are index-bound by construction.
//
// Wiring in cmd/scheduler — the background StaleRequeueLoop cycle (T-270).

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
	// StaleRequeueWindow — the last_checked_at age threshold of the "hung
	// status" anomaly. NOT configurable: the threshold must match the window
	// of the new_done_stale gauge (FR-2.2: last_checked_at < current_date —
	// the project's canonical "24 h" over a date column, ripening in cohorts
	// at midnight, the T-031/T-202 precedent; the midnight influx and its price —
	// the comment in the file header); a larger one would needlessly delay the
	// repair and split the compensator from the gauge.
	StaleRequeueWindow = 24 * time.Hour

	// DefaultStaleRequeueBatch — domains in one move batch (the
	// T-270 task: ≤1000): the batch bounds the move transaction — locks are
	// held for the minimum time (the T-020 style).
	DefaultStaleRequeueBatch = 1000

	// DefaultStaleRequeueInterval — the period of the StaleRequeueLoop cycle (the
	// T-270 task: 1 h): the repair is not urgent (the cohort is already older than a day); in
	// steady state a tick costs one guard PK lookup.
	DefaultStaleRequeueInterval = time.Hour
)

// staleRequeueGuardSQL — a cheap probe of an empty set (the guard style of
// T-202/retryMatureSQL): a point PK lookup of the new_done_stale row —
// the anomaly level written only by monitor reconciliation (T-256),
// the FILTER window is identical to the compensator's selection window (date <
// current_date ≡ date < now() − 24 h). The coalesce subquery guarantees
// exactly one row (the dnsDailyCountersSQL style): before the first
// reconciliation pass there is no row — an honest zero, the guard is closed (the level source
// is monitor; without it the compensator honestly sleeps). A value ≤ 0 — no cohort,
// the heavy selection is not executed at all.
const staleRequeueGuardSQL = "SELECT coalesce((SELECT value FROM crawler_counters WHERE name = 'new_done_stale'), 0)"

// staleRequeueSelectSQL — a batch of anomalous rows: status = new ∧
// check_state = done ∧ last_checked_at older than the threshold (NULL stamps
// are filtered out by the comparison: done writers always set it, NULL on
// new∧done is manual SQL junk, not a lost task — the same
// FILTER semantics as the gauge, zonestats.go). Schema codes — as literals via model.*
// (the leasenull.go style). ORDER BY is absent deliberately (the
// retrySelectSQL precedent, T-191/T-181): the batch order does not matter — the move
// guards re-check the state at the moment of the lock, a repeated pass
// is safe; sorting would hold index access to the table. The plan
// form (a seq scan with an early LIMIT stop) is pinned by the selection
// transaction's SET LOCALs (staleRequeuePlan*SQL): without pinning
// the planner lands on the "provable" partial domains_recheck_idx and
// degenerates the window into a marathon over millions of done rows (the Oct 25 measurement —
// the retrySelectSQL comment).
var staleRequeueSelectSQL = fmt.Sprintf(
	"SELECT d.zone_id, d.id FROM domains AS d "+
		"WHERE d.status = %d AND d.check_state = %d AND d.last_checked_at < $1 "+
		"LIMIT $2",
	int16(model.StatusNew), int16(model.CheckStateDone))

// Pinning the plan form of the selection (T-191, the T-181 precedent): SET LOCAL in
// the selection transaction (staleRequeueBatch) disables the index forms
// of access to domains until the selection statement — the batch in fractions of a second
// regardless of statistics (the planner's estimate cannot be trusted — see
// staleRequeueSelectSQL). SET LOCAL acts for this transaction only;
// the move goes separately, the move's PK probes are unaffected.
const (
	staleRequeuePlanNoIndexScanSQL  = "SET LOCAL enable_indexscan = off"
	staleRequeuePlanNoBitmapScanSQL = "SET LOCAL enable_bitmapscan = off"
	staleRequeuePlanNoIOSQL         = "SET LOCAL enable_indexonlyscan = off"
)

// staleRequeueUpdateSQL — moving the selected batch to pending by PK probes
// (a guard statement of the leaseNullUpdateSQL/leaseUpdateSQL kind, T-200):
// the planner is led away from unnest and probes domains by PK (zone_id, id),
// there is no scan on the UPDATE side. The guard filters are the same as in the selection and
// are evaluated at the moment the row is LOCKED: an HTTP outcome written between
// the selection and the lock (status stops being new — the HTTP stage write
// goes without the done guard by the A3 asymmetry), a delete file (status = deleted),
// a winning competitor (a recheck T-030, a retry T-031, a manual recheck R11)
// do not move the row — the idempotency of repeated passes (I6
// of the specification). The side effects of R15: queued_at = NULL (done already has
// NULL — the unified form of moves to pending, I2); the status outcome and
// temp_attempts are kept (new is not temp_error, a new check will overwrite it); the http_published bit is NOT touched — the R1 enqueue
// of a new episode will clear it (I4: a return into work — only through pending).
// The zone counter deltas (checked −n / pending +n — the predicate pins
// the old done state, T-178) — the CTE zc over the RETURNING zone_id; zc
// references upd — the execution order is guaranteed, zones and counters are not
// mixed in one statement (the lock order invariant of
// docs/schema.md).
var staleRequeueUpdateSQL = fmt.Sprintf(
	"WITH upd AS (UPDATE domains AS t "+
		"SET check_state = $1, queued_at = NULL "+
		"FROM unnest($2::int2[], $3::int8[]) AS u(zone_id, id) "+
		"WHERE t.zone_id = u.zone_id AND t.id = u.id "+
		"AND t.check_state = %d AND t.status = %d AND t.last_checked_at < $4 "+
		"RETURNING t.zone_id), "+
		"zc AS (UPDATE zones AS z "+
		"SET domains_checked = z.domains_checked - c.n, "+
		"domains_pending = z.domains_pending + c.n "+
		"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c "+
		"WHERE z.id = c.zone_id) "+
		"SELECT zone_id FROM upd",
	int16(model.CheckStateDone), int16(model.StatusNew))

// StaleRequeueDB — the pool for the compensator: a db.DB with transactions (the
// RetryDB/LeaseDB style). The selection and the move are SEPARATE transactions: the selection
// pins the plan form (SET LOCAL, a seq scan with an early stop),
// the unnest→PK move must not inherit the pinning; each
// upholds the lock order "domains → zones" (the pass does not touch
// counters — the docs/schema.md invariant); *pgxpool.Pool satisfies
// the interface (the sessions inherit the LockTimeout T-192 and NoParallelGather
// T-171 from the pool's AfterConnect).
type StaleRequeueDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// compile-time: the real pool fits the interface.
var _ StaleRequeueDB = (*pgxpool.Pool)(nil)

// StaleRequeueOptions — compensator parameters; zero values
// are replaced with defaults (withDefaults, the RecheckOptions style). The threshold
// StaleRequeueWindow is a constant, not a field: it must match the
// gauge window (see the constant's comment).
type StaleRequeueOptions struct {
	// BatchSize — the maximum domains in one batch
	// (DefaultStaleRequeueBatch).
	BatchSize int
	// Interval — the period of the StaleRequeueLoop cycle; does not affect a one-off
	// StaleRequeueOnce.
	Interval time.Duration
	// Now — the time source of the threshold now() − StaleRequeueWindow; nil —
	// time.Now (the RecheckOptions style: the threshold precision is a day, seconds
	// of process-vs-DB clock skew are immaterial).
	Now func() time.Time
	// Log — the StaleRequeueLoop loop logger; nil — records suppressed
	// (slog.DiscardHandler). StaleRequeueOnce does not log — the outcome
	// is returned as StaleRequeueResult.
	Log *slog.Logger
	// Metrics — the service's Prometheus registry: the counter
	// crawler_scheduler_stale_requeued_total (T-270); nil — without metrics
	// (isolated assemblies/tests).
	Metrics *metrics.Registry
}

// withDefaults replaces non-positive values with defaults (Now
// is checked separately: functions — nil).
func (o StaleRequeueOptions) withDefaults() StaleRequeueOptions {
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultStaleRequeueBatch
	}
	if o.Interval <= 0 {
		o.Interval = DefaultStaleRequeueInterval
	}
	return o
}

// StaleRequeueResult — the outcome of one StaleRequeueOnce pass.
type StaleRequeueResult struct {
	// Domains — rows moved to pending by THIS pass (by the RETURNING
	// of the move, after the guards — the LeaseNullResult style).
	Domains int64
	// Selected — rows in the selection batch: the LIMIT is filled completely — there are
	// more beyond it, the StaleRequeueLoop drain continues with the next batch.
	Selected int
}

// staleRequeueRow — a batch row: the domain key (states and stamps
// need not be stored — the move guards re-check everything in place).
type staleRequeueRow struct {
	zone int16
	id   int64
}

// staleRequeueMetrics — the compensator counter (T-270); nil — no-op (the
// leaseNullMetrics style).
type staleRequeueMetrics struct {
	requeued prometheus.Counter
}

// newStaleRequeueMetrics — the counter in the reg registry (nil — nil): the name/HELP —
// the single point of truth of the metric documentation (docs/services.md); registration
// is idempotent (T-076) — building it on every pass is safe.
func newStaleRequeueMetrics(reg *metrics.Registry) *staleRequeueMetrics {
	if reg == nil {
		return nil
	}
	return &staleRequeueMetrics{
		requeued: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_scheduler_stale_requeued_total",
			Help: "The \"stuck status\" compensator (FR-2.2 of the TZ-fsm spec, T-270, the R15 transition of the check_state spec): moving check_state done → pending for status=new domains with last_checked_at older than a day — the q:http task was lost after XADD (a Redis restart without persistence, an OOM window); the lease/age-based recheck do not return the row. Each event is one moved row (by RETURNING after the guards); zero in steady state, growth means lost q:http tasks (the level is the crawler_domains_new_done_stale gauge; at the midnight boundary a non-empty q:http gives the regular midnight influx of live deliveries — an episode duplicate is absorbed by the A3 asymmetry, not a loss), analysis in docs/runbooks.md §2.10.",
		}).WithLabelValues(),
	}
}

// add — n moved rows; nil — no-op.
func (m *staleRequeueMetrics) add(n int64) {
	if m == nil {
		return
	}
	m.requeued.Add(float64(n))
}

// StaleRequeueOnce performs one compensator pass (FR-2.2, T-270):
// a cheap guard probe over the new_done_stale row (closed — an empty
// result without touching domains), then up to opts.BatchSize anomalous rows
// are selected as a batch (a seq scan with an early stop, SET LOCAL pinning)
// and by a SEPARATE transaction with PK probes are moved to check_state = pending
// — the zone counter deltas are kept by the move statement's CTE (T-178), the
// crawler_scheduler_stale_requeued_total counter grows by the rows
// actually moved (opts.Metrics; nil — without metrics). A repeated pass
// is safe (spec 3.4): the moved ones stop matching the selection,
// the guards re-check the state at the moment of the lock. An empty batch —
// a no-op without a move (guard drift: no cohort, one idle scan per
// tick — until the nearest reconciliation).
func StaleRequeueOnce(ctx context.Context, pool StaleRequeueDB, opts StaleRequeueOptions) (StaleRequeueResult, error) {
	if pool == nil {
		return StaleRequeueResult{}, errors.New("scheduler: stuck-status compensator: pool not set")
	}
	o := opts.withDefaults()

	// The empty-set guard (the T-202 style): no cohort — domains is
	// not touched. A negative level does not happen (the row is the absolute
	// of reconciliation, there are no increments), but the guard must stay silent on it too.
	stale, err := staleRequeueGuard(ctx, pool)
	if err != nil {
		return StaleRequeueResult{}, err
	}
	if stale <= 0 {
		return StaleRequeueResult{}, nil
	}

	now := time.Now
	if o.Now != nil {
		now = o.Now
	}
	cutoff := now().Add(-StaleRequeueWindow)

	batch, err := staleRequeueBatch(ctx, pool, cutoff, o.BatchSize)
	if err != nil {
		return StaleRequeueResult{}, err
	}
	if len(batch) == 0 {
		return StaleRequeueResult{}, nil
	}

	// One threshold for the selection and the move (the recheck.go symmetry): a race with
	// a fresh result write will not move a row whose age/fate
	// changed between the transactions.
	met := newStaleRequeueMetrics(o.Metrics)
	n, err := staleRequeueTranslate(ctx, pool, batch, cutoff, o.Metrics)
	if err != nil {
		return StaleRequeueResult{}, err
	}
	met.add(n)
	return StaleRequeueResult{Domains: n, Selected: len(batch)}, nil
}

// staleRequeueGuard — a cheap probe of an empty set (staleRequeueGuardSQL):
// the new_done_stale level from crawler_counters; one row is guaranteed
// by the coalesce subquery — an empty set is a protocol break, not a "zero".
func staleRequeueGuard(ctx context.Context, pool StaleRequeueDB) (int64, error) {
	rows, err := pool.Query(ctx, staleRequeueGuardSQL)
	if err != nil {
		return 0, fmt.Errorf("scheduler: stuck-status compensator: new_done_stale level probe: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("scheduler: stuck-status compensator: new_done_stale level probe: %w", err)
		}
		return 0, fmt.Errorf("scheduler: stuck-status compensator: new_done_stale level probe: empty aggregate response")
	}
	var n int64
	if err := rows.Scan(&n); err != nil {
		return 0, fmt.Errorf("scheduler: stuck-status compensator: new_done_stale level probe: read: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("scheduler: stuck-status compensator: new_done_stale level probe: %w", err)
	}
	return n, nil
}

// staleRequeueBatch — a batch of anomalous rows (staleRequeueSelectSQL) in
// its own transaction with plan pinning (SET LOCAL ×3 before the SELECT —
// the retryCandidates T-191 style): a seq scan over domains with an early
// LIMIT stop. The transaction is only the pinning shell: the SELECT reads
// committed data, commit/rollback are substantively equivalent.
func staleRequeueBatch(ctx context.Context, pool StaleRequeueDB, cutoff time.Time, limit int) ([]staleRequeueRow, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("scheduler: stuck-status compensator: selection transaction: %w", err)
	}
	// The rollback is unconditional: after Commit — a no-op (the T-044 style).
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, staleRequeuePlanNoIndexScanSQL); err != nil {
		return nil, fmt.Errorf("scheduler: stuck-status compensator: plan pinning: %w", err)
	}
	if _, err := tx.Exec(ctx, staleRequeuePlanNoBitmapScanSQL); err != nil {
		return nil, fmt.Errorf("scheduler: stuck-status compensator: plan pinning: %w", err)
	}
	if _, err := tx.Exec(ctx, staleRequeuePlanNoIOSQL); err != nil {
		return nil, fmt.Errorf("scheduler: stuck-status compensator: plan pinning: %w", err)
	}

	rows, err := tx.Query(ctx, staleRequeueSelectSQL, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("scheduler: stuck-status compensator: batch selection: %w", err)
	}
	defer rows.Close()

	var out []staleRequeueRow
	for rows.Next() {
		var r staleRequeueRow
		if err := rows.Scan(&r.zone, &r.id); err != nil {
			return nil, fmt.Errorf("scheduler: stuck-status compensator: batch selection: read: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scheduler: stuck-status compensator: batch selection: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("scheduler: stuck-status compensator: selection commit: %w", err)
	}
	return out, nil
}

// staleRequeueTranslate — moving a batch by PK probes (staleRequeueUpdateSQL)
// in its own transaction under a 40P01 retry (T-268, a guard on top of
// the lock order invariant: a repeat is safe — the guards are idempotent);
// reg — the registry for the retry counter crawler_db_deadlock_retried_total
// (the common constructor of the retry pass; nil — no-op); returns the number
// of moved rows by RETURNING (after the guards).
func staleRequeueTranslate(ctx context.Context, pool StaleRequeueDB, batch []staleRequeueRow, cutoff time.Time, reg *metrics.Registry) (int64, error) {
	zones := make([]int16, len(batch))
	ids := make([]int64, len(batch))
	for i, r := range batch {
		zones[i], ids[i] = r.zone, r.id
	}
	dbRetried := newRetryMetrics(reg)
	var n int64
	if err := db.RetryDeadlock(ctx, db.DefaultDeadlockRetry, dbRetried.dbDeadlockRetried, func() error {
		var err error
		n, err = staleRequeueTranslateOnce(ctx, pool, zones, ids, cutoff)
		return err
	}); err != nil {
		return 0, err
	}
	return n, nil
}

// staleRequeueTranslateOnce — ONE attempt of the move transaction (the
// 40P01 retry point — staleRequeueTranslate).
func staleRequeueTranslateOnce(ctx context.Context, pool StaleRequeueDB, zones []int16, ids []int64, cutoff time.Time) (int64, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("scheduler: stuck-status compensator: move transaction: %w", err)
	}
	// The rollback is unconditional: after Commit — a no-op (the T-044 style).
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, staleRequeueUpdateSQL,
		int16(model.CheckStatePending),
		zones, ids,
		cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf("scheduler: stuck-status compensator: batch move: %w", err)
	}
	defer rows.Close()

	var n int64
	for rows.Next() {
		var zoneID int16
		if err := rows.Scan(&zoneID); err != nil {
			return 0, fmt.Errorf("scheduler: stuck-status compensator: batch move: read: %w", err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("scheduler: stuck-status compensator: batch move: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("scheduler: stuck-status compensator: move commit: %w", err)
	}
	return n, nil
}

// StaleRequeueLoop — the hourly cycle of the "hung status" compensator (T-270):
// the drain of everything ripened in StaleRequeueOnce batches, then waiting for a tick.
// The first drain is immediate at start (the pass is idempotent and picks up
// what accumulated during the downtime — the initial drain style of T-020/T-030).
// A pass error interrupts only the current drain: a Warn is logged, the loop
// lives (the OnRun contract, T-012), the next tick will retry — what has been moved
// is not returned twice. ctx cancellation — a clean exit with nil (graceful
// shutdown, spec 3.4).
func StaleRequeueLoop(ctx context.Context, pool StaleRequeueDB, opts StaleRequeueOptions) error {
	if pool == nil {
		return errors.New("scheduler: stuck-status compensator: pool not set")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("stuck-status compensator: early new∧done recheck",
		"interval", o.Interval.String(),
		"batch", o.BatchSize,
		"window", StaleRequeueWindow.String())

	// drain moves the whole cohort: batches repeat while the selection comes back
	// full (the LIMIT is filled — there are more beyond it) AND moves rows;
	// an incomplete batch or a zero move (the guard filtered everything out, a competitor
	// has already moved them) — the set is exhausted, the drain is finite.
	drain := func() {
		var total int64
		for {
			res, err := StaleRequeueOnce(ctx, pool, o)
			if err != nil {
				// Cancellation during a pass is part of the stop, not an alarm;
				// any other error — a Warn, the next drain will retry.
				if ctx.Err() == nil {
					log.Warn("stuck-status compensator drain",
						"err", err, "requeued", total)
				}
				return
			}
			total += res.Domains
			if res.Domains == 0 || res.Selected < o.BatchSize {
				if total > 0 {
					// Each moved row is a lost q:http task:
					// the event is visible to the counter and the gauge; the drain outcome goes to the log.
					log.Info("stuck-status compensator: domains returned to pending",
						"domains", total)
				}
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

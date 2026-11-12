// Scheduler: returning domains with an expired lease (T-029; spec 5.3, 3.4;
// the pass form — a SELECT of the batch + a move by PK probes and a drain cursor,
// T-200, TZ_slow_lease_return.md).
//
// Enqueueing (T-028) moves a domain to check_state = queued and
// stamps queued_at — from that moment the lease is in effect: the worker
// must write the result within scheduler.lease (30 minutes
// by default, spec 5.3). If there is no result — the worker died, Redis was lost
// (the source of truth is PostgreSQL, spec 3.4), a publication failed — the domain
// "hangs" in queued, outside the check cycle. The LeaseOnce pass returns
// such domains to pending: the dispatcher (T-027) will re-select them and enqueue them
// again (T-028) — at-least-once delivery, a repeat is safe.
//
// "Without a result" is built into the selection condition: a worker that wrote a result
// moves the domain to done (the result-writing contracts, spec 5.4/5.5) — the row
// stops matching check_state = queued and is not selected, no matter how
// old it is. For an honest worker that did not make the lease deadline, the return
// is no hindrance: stage timeouts are seconds versus minutes of lease, and
// task redelivery is idempotent (spec 3.4).
//
// Return conditions: check_state = queued AND queued_at older than now() − lease.
// A NULL queued_at is filtered out by the comparison: queued without a timestamp is a
// broken T-028 invariant, moving it by age is pointless;
// such rows are returned by the FR-1.2 terminal guard — the repair
// pass of the same LeaseWatch cycle (leasenull.go, T-255).
// The deleted status is excluded (spec 13.1, as in the recheck T-030):
// a deleted domain drops out of the check cycle entirely — the dispatcher does not
// select it (the T-027 guard), returning it to pending would mean accumulating
// dead rows in the partial index domains_pending_idx. Resurrection
// by an add file (T-018) itself sets new/pending and resets queued_at,
// no matter which state the domain returns from.
//
// The pass is TWO statements in one transaction: batch selection (leaseSelectSQL)
// and the move (leaseUpdateSQL, UPDATE … FROM unnest by PK probes). Before T-200
// the batch and the move were one CTE statement — and the UPDATE side of its plan
// (Update → Nested Loop) scanned the ENTIRE expired range of the lease index
// without a LIMIT: the cost of a batch grew with the size of the range, not the batch, and
// the range was read twice per batch (the subquery + the guarded join;
// WITH AS MATERIALIZED did not change the plan — measurement of the T-200 spec §2.1, query #3
// of the Oct 25–26 window: 75 events, median 8.1 s, max 111 s). Two statements are
// the only way to remove the lease scan from the UPDATE side (spec §6.1):
// the unnest form of the batch UPDATE (the db.Update style, T-028) leads the planner
// away from unnest, and domains is probed by PK; the atomicity of "selection = move"
// is provided by the transaction (spec 3.4) — both operations of one pass go through
// pool.Begin. The guard re-checks of the selection (check_state/status/queued_at) are
// kept in the UPDATE: a race with a worker that wrote a result between the selection and
// the row lock does not return an already finished domain. The moved rows
// stop matching the selection: repeated passes are idempotent (spec 3.4).
// The deleted filter must live in the selection itself (not only in the guard
// of the move): otherwise dead rows could fill the batch limit and shut out
// the living from returning (starvation).
//
// The drain cursor (T-200, spec §3.2/§4.1): between vacuums the lease index keeps
// dead versions of moved leases (measurement §2.2: 273 MB of index with ~1k
// live records; autovacuum with a scale factor of 0.02 × 42.3M rows
// cleans the prefix 2–3 times a day) — without the cursor each batch of EVERY
// drain started the scan from the beginning of the index and reread the same
// dead prefix (the storm drain of Oct 25 — 14 batches × the prefix). The cursor
// (queued_at, zone_id, id) — the index key in full — lives in the state of the
// LeaseWatch loop and survives batches and drains: the dead prefix is read once
// per process lifetime; a restart resets the cursor — the initial full
// drain is preserved (T-012/T-029). The correctness of the movement — spec §4.1: everything
// with a key ≤ the cursor has already been returned (expiry depends only on queued_at);
// the not-yet-expired at the moment of the pass lie to the right of the threshold — to the right of the cursor — and
// will age ahead of it; re-enqueueing writes a fresh queued_at
// to the right of the threshold and the cursor; deleted rows to the left of the cursor are skipped
// forever — they are not supposed to be returned anyway.
//
// Wiring in cmd/scheduler — the service assembly (T-028/later tasks).

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/model"
)

const (
	// DefaultLeaseTimeout — the lease term of a queued domain: after it, the
	// result cannot be waited for, the domain returns to pending (spec 5.3: "30 min
	// by default"). Mirrors the default scheduler.lease (internal/config, T-006);
	// the working loop gets the value from the config.
	DefaultLeaseTimeout = 30 * time.Minute

	// DefaultLeaseBatch — domains in one return batch: in the range of the scheduler
	// batches (spec 5.3: 1000–10,000), matches the defaults of selection
	// (T-027) and recheck (T-030). The batch bounds one transaction
	// of the pass — locks are held for the minimum time (the T-020 style);
	// the size is not changed by the T-200 form (spec §4.3).
	DefaultLeaseBatch = 5000
)

// leaseSelectSQL — the selection of a batch of expired leases (T-200, spec §4.1): up to LIMIT
// rows check_state = queued, not deleted, queued_at older than the threshold — over
// the domains_lease_idx index (T-104): ORDER BY (queued_at, zone_id, id)
// matches the index key, the scan reads from the beginning of the index only the expired
// leases and stops at the first not-yet-expired one; the oldest are returned
// first. The check_state code and the deleted status are literals via model.*
// (a partial index predicate must be provable for the planner,
// the dispatch.go style; a parameter in the predicate would kill domains_lease_idx in
// the generic plan of the prepared statement).
//
// The composite row-value cursor > (queued_at, zone_id, id) — the pagination
// of the drain (the retentionSelectSQL T-185 / reanalyzeTickSQL T-193 style): batches
// are deterministic, without gaps or repeats; the key in full (a unique
// domain key at the tail) — a continuation strictly from the stopping point. The cursor
// survives batches and drains in the LeaseWatch state — the dead prefix
// of the index between vacuums is read once per process lifetime, not by every
// batch (spec §2.2/§3.2); the correctness of the movement — the file comment (spec
// §4.1). A zero cursor (start/restart) — from the beginning of the index, as before T-200.
// queued_at in the column list — the cursor of the next batch (T-185).
//
// The lease threshold is counted by the DB SERVER's clock: queued_at is written by the server
// now() (enqueueUpdate); a process clock skew versus the DB clock
// forward by more than the lease term would return just-enqueued
// domains to pending (duplicate work), backward — would delay the return.
// now() is substituted as a parameter only in tests (the fixed clock
// LeaseOptions.Now); in production the parameter is NULL — COALESCE takes the server's now(),
// which is stable within the pass transaction (the Begin moment). The term is passed in seconds
// via make_interval — the interval at a single point of truth, without client
// timestamp arithmetic.
var leaseSelectSQL = fmt.Sprintf(
	"SELECT zone_id, id, queued_at FROM domains "+
		"WHERE check_state = %d AND status <> %d "+
		"AND queued_at < COALESCE($1::timestamptz, now()) - make_interval(secs => $2) "+
		"AND (queued_at, zone_id, id) > ($3::timestamptz, $4::int2, $5::int8) "+
		"ORDER BY queued_at, zone_id, id LIMIT $6",
	int16(model.CheckStateQueued), int16(model.StatusDeleted))

// leaseUpdateSQL — moving the selected batch to pending by PK probes (T-200,
// spec §4.2): a batch UPDATE … FROM unnest (the db.Update style, T-028) —
// the planner is led away from unnest and probes domains by PK (zone_id, id),
// there is no alternative lease scan on the UPDATE side anymore (the diagnosis of the spec
// §2.1: a single CTE statement held Update → Nested Loop, the outer side
// — an index scan of the entire expired range without a LIMIT, joined with a Join
// Filter; AS MATERIALIZED did not change the plan). The cost of the move ∝ the batch,
// not the size of the expired range. The lease mark is reset — pending has no
// lease (the T-018/T-030 invariant), the other columns are unchanged.
//
// The guard filters (check_state/status/queued_at) are the same as in the selection and
// are evaluated at the moment the row is LOCKED (the recheck T-030 style):
// a worker that wrote a result between the selection and the lock changes
// the row's state — the guard does not let it through to the move, the race semantics
// are unchanged. The transaction's now() — see leaseSelectSQL: the selection and move thresholds
// are the same. Schema codes — as literals via model.*.
//
// The zone counter deltas (queued −n / pending +n) — the CTE zc of the same statement
// over the RETURNING zone_id of the moved rows (T-178, incremental-counters spec §6.2: the predicate
// pins the old queued state); the outer SELECT returns the zone_id
// of the moved rows — LeaseOnce counts the pass outcome by RETURNING, that is,
// the rows moved by this very pass (the guard could have filtered out part of
// the selected batch).
var leaseUpdateSQL = fmt.Sprintf(
	"WITH upd AS (UPDATE domains AS t "+
		"SET check_state = $1, queued_at = NULL "+
		"FROM unnest($2::int2[], $3::int8[]) AS u(zone_id, id) "+
		"WHERE t.zone_id = u.zone_id AND t.id = u.id "+
		"AND t.check_state = %d AND t.status <> %d "+
		"AND t.queued_at < COALESCE($4::timestamptz, now()) - make_interval(secs => $5) "+
		"RETURNING t.zone_id), "+
		"zc AS (UPDATE zones AS z "+
		"SET domains_queued = z.domains_queued - c.n, "+
		"domains_pending = z.domains_pending + c.n "+
		"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c "+
		"WHERE z.id = c.zone_id) "+
		"SELECT zone_id FROM upd",
	int16(model.CheckStateQueued), int16(model.StatusDeleted))

// LeaseDB — the pool for the lease return: a db.DB from which a
// transaction can be started (the RetryDB T-191 / RetentionDB T-185 style). The batch selection and
// the move are one transaction (spec 3.4, T-200 §4.3): the atomicity of the pair of
// statements is equivalent to the former single statement; *pgxpool.Pool
// satisfies the interface (the session inherits the LockTimeout T-192 and
// NoParallelGather T-171 from the pool's AfterConnect).
type LeaseDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// compile-time: the real pool fits the interface.
var _ LeaseDB = (*pgxpool.Pool)(nil)

// LeaseOptions — return parameters; zero values are replaced
// with defaults (withDefaults, the RecheckOptions/ingest.CleanupOptions style).
type LeaseOptions struct {
	// Timeout — the lease term (DefaultLeaseTimeout, spec 5.3): queued_at
	// older than now() − Timeout → a return to pending; the working value is
	// scheduler.lease from the config.
	Timeout time.Duration
	// BatchSize — the maximum domains in one batch (DefaultLeaseBatch).
	BatchSize int
	// Interval — the period of the LeaseWatch cycle; does not affect a one-off LeaseOnce.
	// Zero/negative — Timeout/2 (see the DefaultLeaseInterval comment
	// in LeaseWatch).
	Interval time.Duration
	// Now — a fixed clock for tests: substituted into the SQL as a parameter
	// instead of the server's now() (nil in production — the threshold is counted by the DB
	// clock, see leaseSelectSQL).
	Now func() time.Time
	// Log — the LeaseWatch loop logger; nil — records suppressed
	// (slog.DiscardHandler). LeaseOnce does not log — the outcome is returned
	// as LeaseResult.
	Log *slog.Logger
	// Metrics — the service's Prometheus registry (T-255, FR-1.2): the counter
	// of the repair pass crawler_scheduler_lease_null_returned_total
	// (leasenull.go — the guard of queued/NULL queued_at); nil — without metrics
	// (isolated assemblies/tests). Does not affect the main pass
	// parameters.
	Metrics *metrics.Registry
}

// withDefaults replaces non-positive values with defaults (Now
// is checked separately: functions — nil).
func (o LeaseOptions) withDefaults() LeaseOptions {
	if o.Timeout <= 0 {
		o.Timeout = DefaultLeaseTimeout
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultLeaseBatch
	}
	if o.Interval <= 0 {
		// Half the lease term: see the rationale in LeaseWatch. A degenerate
		// Timeout ≤ 1 ns gives 0 by division — the ticker needs a positive
		// interval, hence the minimum of 1 ns.
		o.Interval = o.Timeout / 2
		// Equivalent mutant (T-M06): `o.Interval <= 1` behaves like
		// `<= 0` — after the division by 2 there are no values from (0, 1 ns)
		// here except exactly 1 ns (a Timeout of 2–3 ns), and the body assigns the same
		// 1 ns (no-op); values ≥ 2 ns are skipped by both boundaries.
		// mutator-disable-next-line numbers/incrementer
		if o.Interval <= 0 {
			o.Interval = time.Nanosecond
		}
	}
	return o
}

// LeaseResult — the outcome of one LeaseOnce pass.
type LeaseResult struct {
	// Domains — domains returned to pending by THIS pass: counted by
	// the move's RETURNING (no more than the batch and no more than the selection — the guard could
	// have filtered out rows whose fate changed between the selection and the lock).
	Domains int64
	// Selected — rows in the selection batch: the LIMIT is filled completely — expired
	// ones remain beyond the cursor, the LeaseWatch drain continues with the next
	// batch. The number of moves is no longer the stop criterion: the guard filtered out
	// results written by workers — the cursor moved past the batch anyway, and
	// returning to it would be an idle read.
	Selected int
}

// LeaseCursor — the drain cursor over time (T-200): the last visited
// row (queued_at, zone_id, id), surviving the batches and drains of the
// LeaseWatch cycle (modeled on retentionCursor T-185 / ReanalyzeCursor T-193).
// Time leads: new leases past the threshold lie ahead; the domain key
// determines the order within identical stamps. The zero value is
// "from the beginning" of the index (the initial full drain, a process restart);
// time.Time{} is less than any real queued_at, zero zone/id — less than any
// real key. The fields are not exported: only LeaseOnce moves the cursor,
// the owner is the LeaseWatch loop (one goroutine, no locks
// needed — the retentionState T-185 style); external calls keep the value
// in full (a zero struct literal is available without fields too).
type LeaseCursor struct {
	queuedAt time.Time
	zone     int16
	id       int64
}

// leaseRow — a batch row: the domain key and the queued_at stamp — the cursor
// of the next batch (T-200).
type leaseRow struct {
	zone     int16
	id       int64
	queuedAt time.Time
}

// LeaseOnce performs one pass (spec 5.3, T-200 §§4.1–4.3): up to
// opts.BatchSize domains with check_state = queued and queued_at older than
// opts.Timeout (not deleted, without a result) are selected by the cursor and
// in the same transaction are moved to check_state = pending by PK probes —
// the lease mark is reset, the other columns are unchanged; the statement's CTE
// keeps the zone counter deltas (T-178). The threshold is counted by the DB server
// (see leaseSelectSQL); a repeated pass is safe (spec 3.4): the moved
// rows are no longer selected by the selection.
//
// cursor — the drain state: after a successful commit it moves past
// its last row (by the selection key); an empty batch does not move it; an error
// of any statement does not move it — a repeated pass takes the batch again.
// Rows filtered out by the guard behind the cursor are not returned: their
// fate changed between the selection and the lock (the worker wrote the result,
// re-enqueueing writes a fresh queued_at to the right of the cursor — spec §4.1),
// they no longer exist under the old key. nil — a one-off call from the zero cursor
// without persisting (the ReanalyzeTick style): the initial full drain
// T-012/T-029 is also preserved by a loop restart.
func LeaseOnce(ctx context.Context, pool LeaseDB, cursor *LeaseCursor, opts LeaseOptions) (LeaseResult, error) {
	if pool == nil {
		return LeaseResult{}, errors.New("scheduler: возврат истёкших lease: пул не задан")
	}
	o := opts.withDefaults()
	if cursor == nil {
		cursor = &LeaseCursor{} // a one-off call: from the beginning of the index, without persisting
	}

	// The process clock — for tests only (a fixed "now"); in production
	// NULL — the only clock is the DB server's now() (stable within the pass
	// transaction: the Begin moment).
	var now *time.Time
	if o.Now != nil {
		t := o.Now()
		now = &t
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return LeaseResult{}, fmt.Errorf("scheduler: возврат истёкших lease: транзакция: %w", err)
	}
	// The rollback is unconditional: after Commit — a no-op (the T-020/T-044 style).
	// An interrupted pass leaves no half-done changes.
	defer func() { _ = tx.Rollback(ctx) }()

	batch, err := leaseSelect(ctx, tx, o, *cursor, now)
	if err != nil {
		return LeaseResult{}, err
	}
	if len(batch) == 0 {
		// An empty batch: nothing to move (the UPDATE is not executed), the cursor
		// does not move; the transaction only read — the rollback is substantively
		// equivalent to the commit (the T-181 style).
		return LeaseResult{}, nil
	}

	n, err := leaseTranslate(ctx, tx, o, batch, now)
	if err != nil {
		return LeaseResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return LeaseResult{}, fmt.Errorf("scheduler: возврат истёкших lease: фиксация: %w", err)
	}
	// The cursor — past the last row of the batch, only after a successful commit
	// (a transaction failure will repeat the whole batch). Batch rows filtered out
	// by the move's guard are skipped: their state changed between the
	// selection and the lock — they no longer exist under the old key (spec §4.1).
	last := batch[len(batch)-1]
	cursor.queuedAt, cursor.zone, cursor.id = last.queuedAt, last.zone, last.id
	return LeaseResult{Domains: n, Selected: len(batch)}, nil
}

// leaseSelect — a batch of expired leases after the after cursor (leaseSelectSQL)
// in the pass transaction: the domain keys and the queued_at stamps — the arguments
// of the move and the cursor of the next batch.
func leaseSelect(ctx context.Context, tx pgx.Tx, o LeaseOptions, after LeaseCursor, now *time.Time) ([]leaseRow, error) {
	rows, err := tx.Query(ctx, leaseSelectSQL,
		now,
		o.Timeout.Seconds(),
		after.queuedAt, after.zone, after.id,
		o.BatchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("scheduler: возврат истёкших lease: выборка пачки: %w", err)
	}
	defer rows.Close()

	var out []leaseRow
	for rows.Next() {
		var r leaseRow
		if err := rows.Scan(&r.zone, &r.id, &r.queuedAt); err != nil {
			return nil, fmt.Errorf("scheduler: возврат истёкших lease: выборка пачки: чтение: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scheduler: возврат истёкших lease: выборка пачки: %w", err)
	}
	return out, nil
}

// leaseTranslate — moving a batch by PK probes (leaseUpdateSQL) in the pass
// transaction; returns the number of moved rows by RETURNING (after the guards —
// like the cleanup RETURNING, T-020: rows whose write the guard saw are not
// counted).
func leaseTranslate(ctx context.Context, tx pgx.Tx, o LeaseOptions, batch []leaseRow, now *time.Time) (int64, error) {
	zones := make([]int16, len(batch))
	ids := make([]int64, len(batch))
	for i, r := range batch {
		zones[i], ids[i] = r.zone, r.id
	}
	rows, err := tx.Query(ctx, leaseUpdateSQL,
		int16(model.CheckStatePending),
		zones, ids,
		now,
		o.Timeout.Seconds(),
	)
	if err != nil {
		return 0, fmt.Errorf("scheduler: возврат истёкших lease: перевод пачки: %w", err)
	}
	defer rows.Close()

	var n int64
	for rows.Next() {
		var zoneID int16
		if err := rows.Scan(&zoneID); err != nil {
			return 0, fmt.Errorf("scheduler: возврат истёкших lease: перевод пачки: чтение: %w", err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("scheduler: возврат истёкших lease: перевод пачки: %w", err)
	}
	return n, nil
}

// LeaseWatch — the background loop of returning domains with an expired lease: the
// drain of everything ripened, in LeaseOnce batches, then waiting for a tick. The first drain is
// immediate at start: the pass is idempotent and picks up what hung during the service
// downtime (a stop/restart leaves no "stuck" domains — spec 3.4,
// 11.4 item 9; the initial drain style of T-020/T-030). A batch
// error interrupts only the current drain: a Warn is logged, the loop lives (the
// OnRun contract, T-012), the next drain will continue — what has been moved is not
// returned twice. ctx cancellation — a clean exit with nil (graceful
// shutdown, spec 3.4). The same tick of the cycle performs the repair pass
// of the queued/NULL queued_at guard (LeaseNullOnce, FR-1.2/T-255 —
// leasenull.go): the drain of the main set, then the guard.
//
// The drain cursor is the loop state surviving batches and drains (T-200,
// the retentionCursor T-185 style): the dead prefix of the lease index is read
// once per process lifetime, not by every batch of every drain (spec
// §2.2/§3.2); a process restart (a new launch of the loop) starts from the zero
// cursor — the initial full drain is preserved. Only the
// loop touches the cursor (one ticker goroutine) — no locks needed.
//
// The check interval is lease/2 (not a fixed value): the balance of "return speed vs
// load". A pass with the domains_lease_idx index (T-104) costs as much
// as there are hung domains in it (normally a few rows), but a fixed interval at
// a lease of 30 min would still give dozens of idle passes per term for the sake
// of a gain of minutes in the return, and with a short configurable lease it would even
// "oversleep" the term itself. Half the term — two checks per lease:
// an expired lease is detected no later than half the term after
// expiry; in total a domain hangs for no longer than one and a half terms.
func LeaseWatch(ctx context.Context, pool LeaseDB, opts LeaseOptions) error {
	if pool == nil {
		return errors.New("scheduler: возврат истёкших lease: пул не задан")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("возврат доменов с истёкшим lease",
		"interval", o.Interval.String(),
		"timeout", o.Timeout.String(),
		"batch", o.BatchSize)

	// The loop's drain cursor (T-200): zero at start — the first drain
	// covers everything ripened from the beginning of the index; further batches continue
	// each other across drains.
	var cursor LeaseCursor

	// drain returns all ripened domains: batches repeat while
	// the selection comes back full (the LIMIT is filled — expired ones remain
	// beyond the cursor); an incomplete batch — the set is exhausted. The criterion is the
	// selection size, not the number of moves: the guard could have filtered out the batch entirely
	// (results written between the selection and the lock), but the cursor moved
	// past it — there is no reason to return to what has been covered.
	drain := func() {
		var total int64
		for {
			res, err := LeaseOnce(ctx, pool, &cursor, o)
			if err != nil {
				// Cancellation during a pass is part of the stop, not an alarm;
				// any other error — a Warn, the next drain will retry.
				if ctx.Err() == nil {
					log.Warn("дренаж возврата истёкших lease",
						"err", err, "returned", total)
				}
				return
			}
			total += res.Domains
			if res.Selected < o.BatchSize {
				if total > 0 {
					log.Info("дренаж возврата истёкших lease", "domains", total)
				}
				return
			}
		}
	}

	// drainNull — the repair pass of the guard (FR-1.2, leasenull.go):
	// LeaseNullOnce batches repeat while the selection comes back full
	// (the LIMIT is filled — there are more beyond it); an incomplete batch — the set
	// is exhausted. An outcome > 0 — a Warn: in steady state the guard is empty (the
	// I1 invariant is upheld by all writers), every event is visible to the operator and to the
	// crawler_scheduler_lease_null_returned_total counter (runbooks §2.9). A batch
	// error interrupts only the pass: the next tick will retry (the
	// OnRun contract, T-012).
	drainNull := func() {
		var total int64
		for {
			res, err := LeaseNullOnce(ctx, pool, o)
			if err != nil {
				if ctx.Err() == nil {
					log.Warn("ремонтный проход queued без queued_at",
						"err", err, "returned", total)
				}
				return
			}
			total += res.Domains
			if res.Selected < o.BatchSize {
				if total > 0 {
					log.Warn("ремонтный проход: строки queued без queued_at возвращены в pending",
						"domains", total)
				}
				return
			}
		}
	}

	drain()
	// The repair pass of the guard (T-255, FR-1.2) — on the same tick of the cycle:
	// rows queued with a NULL queued_at (a broken I1 invariant,
	// checkstate.go) are returned to pending; in steady state the set is empty —
	// the pass costs one selection over the NULL range of the lease index.
	drainNull()
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			drain()
			drainNull()
		}
	}
}

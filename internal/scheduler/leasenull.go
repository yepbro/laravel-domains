// Scheduler: the terminal guard of queued rows with NULL queued_at (T-255,
// spec tasks/TZ-fsm.md §3 FR-1.2) — the repair pass of the LeaseWatch cycle
// (lease.go). The T-028 invariant "queued ⇒ queued_at NOT NULL" (I1
// of the check_state machine specification, checkstate.go) is broken by a writer bug
// or manual SQL — today such a row hangs in queued
// forever: the main lease pass selects by the queued_at age, NULL
// is filtered out by the comparison (lease.go leaseSelectSQL), the dispatcher selects
// only pending, result writes come only on task deliveries,
// which the row has none of. The guard returns such rows to pending —
// the dispatcher (T-027) will re-select them, the repeated check is idempotent
// (at-least-once, spec 3.4): the state stops depending on the survival
// of someone else's timestamp.
//
// The pass is TWO statements in one transaction (the form of the main lease pass
// T-200, without its cursor: the set of NULL rows does not grow via re-enqueues — a fresh
// enqueue writes now(), to the right of any threshold):
// the batch selection (leaseNullSelectSQL) and the move by PK probes
// (leaseNullUpdateSQL — a guard statement of the same kind as leaseUpdateSQL).
// The form of the main lease query and the usability of domains_lease_idx (T-104)
// are unchanged: the guard selection is a separate statement going over the SAME
// partial index (queued_at IS NULL — a search condition on the leading
// key column, NULL records lie in the index as a compact range),
// ORDER BY matches the index key — batches without sorting.
//
// The behavior of regular leases is unchanged: for rows with a non-NULL queued_at
// the move's IS NULL guard does not let them through, and the main pass does not see them anyway.
// The crawler_scheduler_lease_null_returned_total counter — by the RETURNING
// of the move: each event is one returned row; in steady state zero
// (all writers uphold the invariant), a non-zero steady state is a signal
// to the operator (docs/runbooks.md §2.9): someone writes queued without a stamp.

package scheduler

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
	"crawler/internal/model"
)

// leaseNullSelectSQL — the selection of a batch of queued rows with NULL queued_at (not
// deleted): the broken I1 invariant (checkstate.go). It goes over the partial
// index domains_lease_idx (T-104): the predicate check_state = 1 is implied by the
// partial index, queued_at IS NULL — a search condition on the leading
// key column (btree keeps NULL records, IS NULL is an index equality),
// ORDER BY (queued_at, zone_id, id) matches the key — the scan reads
// only the NULL range of each partition and stops by LIMIT; the key
// in full determines the batches (the leaseSelectSQL style). Schema codes —
// as literals via model.* (a partial index predicate must be
// provable for the planner; a parameter would kill the index in the generic plan
// of the prepared statement — the dispatch.go style). deleted is excluded in
// the selection itself (as in leaseSelectSQL): a dead row must not
// take the batch limit and shut out the living from returning.
var leaseNullSelectSQL = fmt.Sprintf(
	"SELECT zone_id, id FROM domains "+
		"WHERE check_state = %d AND status <> %d AND queued_at IS NULL "+
		"ORDER BY queued_at, zone_id, id LIMIT $1",
	int16(model.CheckStateQueued), int16(model.StatusDeleted))

// leaseNullUpdateSQL — moving the selected batch to pending by PK probes
// (a guard statement of the leaseUpdateSQL kind, T-200): an unnest batch UPDATE,
// the planner is led away from unnest and probes domains by PK (zone_id, id) —
// there is no scan on the UPDATE side. The guard filters are the same as in the selection and
// are evaluated at the moment the row is LOCKED: a race with a result write
// (queued → done) or a manual stamping between the selection and the
// lock does not move a row whose fate changed. The reset
// queued_at = NULL — a single form with the other moves to pending
// (I2): the row already has NULL, the assignment is semantically a no-op, but keeps
// the statement self-sufficient (does not create a special branch). The zone counter
// deltas (queued −n / pending +n) — the CTE zc over the RETURNING zone_id
// of the moved rows (T-178, incremental-counters spec §6.2: the predicate pins the old
// queued state); the outer SELECT — LeaseNullOnce counts the outcome by
// RETURNING (the guard could have filtered out part of the batch).
var leaseNullUpdateSQL = fmt.Sprintf(
	"WITH upd AS (UPDATE domains AS t "+
		"SET check_state = $1, queued_at = NULL "+
		"FROM unnest($2::int2[], $3::int8[]) AS u(zone_id, id) "+
		"WHERE t.zone_id = u.zone_id AND t.id = u.id "+
		"AND t.check_state = %d AND t.status <> %d AND t.queued_at IS NULL "+
		"RETURNING t.zone_id), "+
		"zc AS (UPDATE zones AS z "+
		"SET domains_queued = z.domains_queued - c.n, "+
		"domains_pending = z.domains_pending + c.n "+
		"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c "+
		"WHERE z.id = c.zone_id) "+
		"SELECT zone_id FROM upd",
	int16(model.CheckStateQueued), int16(model.StatusDeleted))

// LeaseNullResult — the outcome of one LeaseNullOnce pass.
type LeaseNullResult struct {
	// Domains — rows returned to pending by THIS pass (by the RETURNING
	// of the move, after the guards — the LeaseResult style).
	Domains int64
	// Selected — rows in the selection batch: the LIMIT is filled completely — there are
	// more beyond it, the LeaseWatch drain continues with the next batch.
	Selected int
}

// leaseNullRow — a guard batch row: the domain key (queued_at is NULL
// for all — there is no cursor, nothing to keep).
type leaseNullRow struct {
	zone int16
	id   int64
}

// leaseNullMetrics — the repair pass counter (T-255); nil — no-op
// (the retryMetrics style).
type leaseNullMetrics struct {
	returned prometheus.Counter
}

// newLeaseNullMetrics — the counter in the reg registry (nil — nil): the name/HELP —
// the single point of truth of the metric documentation (docs/services.md); registration
// is idempotent (T-076) — building it on every pass is safe, the vector
// is shared between LeaseWatch and direct LeaseNullOnce calls.
func newLeaseNullMetrics(reg *metrics.Registry) *leaseNullMetrics {
	if reg == nil {
		return nil
	}
	return &leaseNullMetrics{
		returned: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_scheduler_lease_null_returned_total",
			Help: "Return to pending of check_state=queued rows with NULL queued_at by the repair pass of the LeaseWatch cycle (T-255, FR-1.2): a violated T-028 invariant \"queued ⇒ queued_at NOT NULL\" — outside the main lease pass forever (NULL is filtered out by the age comparison). Zero in steady state; a non-zero steady state means manual SQL or a bug in the enqueue writer: analysis in docs/runbooks.md §2.9.",
		}).WithLabelValues(),
	}
}

// add — n returned rows; nil — no-op.
func (m *leaseNullMetrics) add(n int64) {
	if m == nil {
		return
	}
	m.returned.Add(float64(n))
}

// LeaseNullOnce performs one repair pass (FR-1.2): up to
// opts.BatchSize rows check_state = queued with NULL queued_at (not deleted)
// are selected and in the same transaction are moved to check_state = pending
// by PK probes — the zone counter deltas are kept by the move statement's CTE (T-178);
// the crawler_scheduler_lease_null_returned_total counter grows by the number
// of rows actually moved (opts.Metrics; nil — without metrics). A repeated
// pass is safe (spec 3.4): the moved rows stop matching
// the selection. An empty batch is a no-op without a move or commit (the LeaseOnce style).
// The form of the main lease query is not touched (lease.go is unchanged).
func LeaseNullOnce(ctx context.Context, pool LeaseDB, opts LeaseOptions) (LeaseNullResult, error) {
	if pool == nil {
		return LeaseNullResult{}, errors.New("scheduler: queued without queued_at guard: pool not set")
	}
	o := opts.withDefaults()
	met := newLeaseNullMetrics(o.Metrics)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return LeaseNullResult{}, fmt.Errorf("scheduler: queued without queued_at guard: transaction: %w", err)
	}
	// The rollback is unconditional: after Commit — a no-op (the T-020/T-044 style).
	defer func() { _ = tx.Rollback(ctx) }()

	batch, err := leaseNullSelect(ctx, tx, o.BatchSize)
	if err != nil {
		return LeaseNullResult{}, err
	}
	if len(batch) == 0 {
		// Nothing to move: the UPDATE is not executed, the transaction only
		// read (the LeaseOnce style).
		return LeaseNullResult{}, nil
	}

	n, err := leaseNullTranslate(ctx, tx, batch)
	if err != nil {
		return LeaseNullResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return LeaseNullResult{}, fmt.Errorf("scheduler: queued without queued_at guard: commit: %w", err)
	}
	met.add(n)
	return LeaseNullResult{Domains: n, Selected: len(batch)}, nil
}

// leaseNullSelect — a batch of queued/NULL queued_at rows (leaseNullSelectSQL)
// in the pass transaction: the domain keys — the move arguments.
func leaseNullSelect(ctx context.Context, tx pgx.Tx, limit int) ([]leaseNullRow, error) {
	rows, err := tx.Query(ctx, leaseNullSelectSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("scheduler: queued without queued_at guard: batch selection: %w", err)
	}
	defer rows.Close()

	var out []leaseNullRow
	for rows.Next() {
		var r leaseNullRow
		if err := rows.Scan(&r.zone, &r.id); err != nil {
			return nil, fmt.Errorf("scheduler: queued without queued_at guard: batch selection: read: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scheduler: queued without queued_at guard: batch selection: %w", err)
	}
	return out, nil
}

// leaseNullTranslate — moving a batch by PK probes (leaseNullUpdateSQL) in the
// pass transaction; returns the number of moved rows by RETURNING
// (after the guards — the leaseTranslate style).
func leaseNullTranslate(ctx context.Context, tx pgx.Tx, batch []leaseNullRow) (int64, error) {
	zones := make([]int16, len(batch))
	ids := make([]int64, len(batch))
	for i, r := range batch {
		zones[i], ids[i] = r.zone, r.id
	}
	rows, err := tx.Query(ctx, leaseNullUpdateSQL,
		int16(model.CheckStatePending),
		zones, ids,
	)
	if err != nil {
		return 0, fmt.Errorf("scheduler: queued without queued_at guard: batch move: %w", err)
	}
	defer rows.Close()

	var n int64
	for rows.Next() {
		var zoneID int16
		if err := rows.Scan(&zoneID); err != nil {
			return 0, fmt.Errorf("scheduler: queued without queued_at guard: batch move: read: %w", err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("scheduler: queued without queued_at guard: batch move: %w", err)
	}
	return n, nil
}

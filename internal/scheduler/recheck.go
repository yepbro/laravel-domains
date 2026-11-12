// Package scheduler — the domain check scheduler (spec 5.3). The recheck
// file is the daily move of domains ready for rechecking back into
// the queue (T-030; pending selection and lease — T-027/T-028/T-029).
//
// Semantics (spec 5.3: "moves done → pending"). done/pending are
// check states, the domains.check_state column (0 pending, 1 queued,
// 2 done; spec 6.1), NOT the result status domains.status (spec 6.2).
// The result status (alive, no_http, …) is not changed: it keeps the outcome
// of the last check and will be overwritten when the new one finishes; the very fact
// of readiness is expressed by the state — status=alive + check_state=pending
// means "checked earlier, awaiting a recheck". Back into the cycle a domain
// is returned only by check_state=pending: the T-027 dispatcher selects it
// (the partial index domains_pending_idx). queued_at is reset to NULL
// — like a resurrected domain in T-018: pending has no lease (the
// enqueue will set up the lease, T-028).
//
// Readiness conditions (spec 5.3, 3.3, 11.4 item 8): check_state = done AND
// last_checked_at older than scheduler.recheck_interval (4380 h ≈ 6 months
// by default; a repeated check — no more often than this interval).
// A NULL last_checked_at is filtered out by the comparison (never checked —
// there is no age). The deleted status is excluded (spec 13.1): domains deleted
// from the registry are not subject to rechecking; temp_error remains
// a candidate — its frequent retries are handled by T-031 (temp_error_retry_after),
// while the age-based recheck after the interval gives even domains
// stuck in temp_error a chance at a final status.
//
// The pass is one UPDATE … FROM (SELECT … LIMIT) statement: the batch is
// bounded (the T-020 style: short locks, the autocommit of a single
// statement — not a transaction over millions of rows). The moved rows
// stop matching the subquery, so repeated passes and
// restarts are idempotent and do no extra work (spec 3.4). The outer
// WHERE re-checks the selection conditions on the current version of the row —
// a race with a delete file (T-019) or a second pass will not move
// a row that changed its state between the selection and the lock (like
// the status guard in cleanupDeleteSQL, T-020). The subquery goes over
// the partial index domains_recheck_idx starting from the oldest done rows
// (migration 000008, T-104): the drain batches are linear, and the pass keeps no
// position — there is no external state, a failure and a restart lose nothing.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"crawler/internal/db"
	"crawler/internal/model"
)

const (
	// DefaultRecheckInterval — the age of the last check after which
	// the domain is ready for a recheck: 4380 hours ≈ 6 months (spec 3.3). Mirrors
	// the default scheduler.recheck_interval (internal/config); the working loop
	// gets the value from the config (T-012/T-013: the config is the source).
	DefaultRecheckInterval = 4380 * time.Hour

	// DefaultRecheckBatch — domains in one move batch: in the range of the
	// scheduler batches (spec 5.3: 1000–10,000), matches the default
	// scheduler.batch_size. The batch bounds one UPDATE — locks are
	// held for the minimum time (the T-020 style).
	DefaultRecheckBatch = 5000

	// DefaultRecheckSchedule — the period of the RecheckDaily cycle: once a day
	// (spec 5.3). There is no separate config option: the spec 5.10 does not
	// parameterize the schedule, the daily cadence is fixed.
	DefaultRecheckSchedule = 24 * time.Hour
)

// recheckUpdateSQL — one pass: a batch of ripened domains (check_state
// = done, last_checked_at older than the threshold, not deleted) is moved to
// pending without changing the result status. invalid (the diagnosis of a poison
// row, T-109) is excluded from the selection: the only regular way to return
// a row to the pipeline after a terminal diagnosis is this pass; without the
// filter poison rows would cycle forever (DNS-03). dispatch/lease
// do not widen the filter: lease sees only queued (writing the diagnosis
// sets done), while invalid+pending (the race window recheck↔diagnosis write)
// without the dispatch filter is re-published once and the diagnosis is recorded
// again — self-healing; with the dispatch filter such a row would hang
// in pending forever, outside all background passes.
// LIMIT in the subquery — the batch.
// The date < timestamptz comparison is a btree-opfamily cross-type
// datetime operator: the index over last_checked_at applies to the timestamptz
// threshold parameter too; the column granularity is a day (spec 6.1), "older than the interval"
// holds with day precision. The outer WHERE re-checks ALL selection
// conditions on the current version of the row — including the age of last_checked_at
// (symmetry with lease.go: a race with a fresh result write between
// the selection and the lock will not move an already rechecked domain).
//
// The check_state/status codes are baked into the text as literals via model.* — schema
// codes, not data (the dispatch.go style): the planner cannot prove a parameter in a partial
// index predicate; the generic plan of the prepared
// statement (pgx caches them) would lose the usability of domains_recheck_idx
// (migration 000008, T-104); an extra status predicate narrows
// the selection without widening the partial index requirement — the usability
// is preserved (status is not part of the index, HOT updates are unaffected).
//
// ORDER BY (last_checked_at, zone_id, id) matches the key of
// domains_recheck_idx: the scan reads exactly the ripened prefix starting from the
// oldest done rows and stops by LIMIT. The drain batches are
// deterministic (a unique domain key at the tail of the sort),
// the move goes from the longest-ago checked; moved rows leave the
// index, the next batch continues from the stopping point — the drain is linear
// and does not reread the unripened set (there is no external state;
// a failure and a restart lose nothing, spec 3.4).
//
// The zone counter deltas (checked −n / pending +n) — the CTE zc of the same
// statement over the RETURNING zone_id of the moved rows (T-178, incremental-counters spec §6.2:
// the predicate pins the old done state); the outer SELECT returns
// the zone_id of the moved rows — RecheckOnce counts the outcome by it.
var recheckUpdateSQL = fmt.Sprintf(
	"WITH b AS (SELECT zone_id, id FROM domains "+
		"WHERE check_state = %[1]d AND status <> %[2]d AND status <> %[3]d AND last_checked_at < $2 "+
		"ORDER BY last_checked_at, zone_id, id LIMIT $3), "+
		"upd AS (UPDATE domains AS d "+
		"SET check_state = $1, queued_at = NULL "+
		"FROM b WHERE d.zone_id = b.zone_id AND d.id = b.id "+
		"AND d.check_state = %[1]d AND d.status <> %[2]d AND d.status <> %[3]d AND d.last_checked_at < $2 "+
		"RETURNING d.zone_id), "+
		"zc AS (UPDATE zones AS z "+
		"SET domains_checked = z.domains_checked - c.n, "+
		"domains_pending = z.domains_pending + c.n "+
		"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c "+
		"WHERE z.id = c.zone_id) "+
		"SELECT zone_id FROM upd",
	int16(model.CheckStateDone), int16(model.StatusDeleted), int16(model.StatusInvalid))

// RecheckOptions — recheck parameters; zero values are replaced
// with defaults (withDefaults, the ingest.CleanupOptions style).
type RecheckOptions struct {
	// Interval — the age threshold of the last check (DefaultRecheckInterval);
	// the working value is scheduler.recheck_interval from the config.
	Interval time.Duration
	// BatchSize — the maximum domains in one batch (DefaultRecheckBatch).
	BatchSize int
	// Schedule — the period of the RecheckDaily cycle (DefaultRecheckSchedule);
	// does not affect a one-off RecheckOnce.
	Schedule time.Duration
	// Now — the time source for the threshold now() − Interval; nil — time.Now.
	// Tests substitute a fixed clock (the watch style, T-013).
	Now func() time.Time
	// Log — the RecheckDaily loop logger; nil — records suppressed
	// (slog.DiscardHandler). RecheckOnce does not log — the outcome is returned
	// as RecheckResult.
	Log *slog.Logger
}

// withDefaults replaces non-positive values with defaults (Now
// is checked separately: functions — nil).
func (o RecheckOptions) withDefaults() RecheckOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultRecheckInterval
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultRecheckBatch
	}
	if o.Schedule <= 0 {
		o.Schedule = DefaultRecheckSchedule
	}
	return o
}

// RecheckResult — the outcome of one RecheckOnce pass.
type RecheckResult struct {
	// Domains — domains moved to pending (no more than the batch size).
	Domains int64
}

// RecheckOnce performs one pass (spec 5.3): up to opts.BatchSize domains
// with check_state = done and last_checked_at older than opts.Interval (not
// deleted) are moved to check_state = pending by one statement —
// the result status and the other columns are unchanged, queued_at
// is reset; the same statement's CTE keeps the zone counter deltas
// (T-178). A repeated pass is safe (spec 3.4): the moved
// rows are no longer selected by the subquery.
func RecheckOnce(ctx context.Context, pool db.DB, opts RecheckOptions) (RecheckResult, error) {
	if pool == nil {
		return RecheckResult{}, errors.New("scheduler: перепроверка доменов: пул не задан")
	}
	o := opts.withDefaults()
	now := time.Now
	if o.Now != nil {
		now = o.Now
	}
	cutoff := now().Add(-o.Interval)

	rows, err := pool.Query(ctx, recheckUpdateSQL,
		int16(model.CheckStatePending),
		cutoff,
		o.BatchSize,
	)
	if err != nil {
		return RecheckResult{}, fmt.Errorf("scheduler: перепроверка доменов: %w", err)
	}
	defer rows.Close()

	var n int64
	for rows.Next() {
		var zoneID int16
		if err := rows.Scan(&zoneID); err != nil {
			return RecheckResult{}, fmt.Errorf("scheduler: перепроверка доменов: чтение: %w", err)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return RecheckResult{}, fmt.Errorf("scheduler: перепроверка доменов: %w", err)
	}
	return RecheckResult{Domains: n}, nil
}

// RecheckDaily — the daily recheck cycle (spec 5.3: "once a day"):
// the drain of all ripened domains in RecheckOnce batches, then waiting until
// the next daily tick. The first drain is immediate at start: the pass
// is idempotent and picks up what ripened during the service downtime (the style
// of ingest.CleanupDeleted, T-020) instead of waiting for a tick — the start moment
// is nothing special. A batch error interrupts only the current drain:
// a Warn is logged, the loop lives (the OnRun contract, T-012), the next
// drain will continue from the same place — what has been moved is not moved
// twice. ctx cancellation — a clean exit with nil (graceful shutdown, spec 3.4).
func RecheckDaily(ctx context.Context, pool db.DB, opts RecheckOptions) error {
	if pool == nil {
		return errors.New("scheduler: перепроверка доменов: пул не задан")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("суточная перепроверка доменов",
		"schedule", o.Schedule.String(),
		"interval", o.Interval.String(),
		"batch", o.BatchSize)

	// drain moves all ripened domains: batches repeat while they
	// come back full; an incomplete batch — the set is exhausted.
	drain := func() {
		var total int64
		for {
			res, err := RecheckOnce(ctx, pool, o)
			if err != nil {
				// Cancellation during a pass is part of the stop, not an alarm;
				// any other error — a Warn, the next drain will retry.
				if ctx.Err() == nil {
					log.Warn("суточный дренаж перепроверки доменов",
						"err", err, "translated", total)
				}
				return
			}
			total += res.Domains
			if res.Domains < int64(o.BatchSize) {
				if total > 0 {
					log.Info("суточный дренаж перепроверки доменов", "domains", total)
				}
				return
			}
		}
	}

	drain()
	ticker := time.NewTicker(o.Schedule)
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

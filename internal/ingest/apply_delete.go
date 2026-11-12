// Applying delete (T-019, spec 5.1): a staging batch with action=delete
// rows moves the status of the matching domains to deleted. The domain
// stays in the DB (spec 13.1: "a deleted domain stays in the DB with the
// deleted status; the results and content are removed by a background
// task") — the T-020 background task cleans them. Together with the
// status the render_pending bit is cleared (T-186): the deleted domain
// leaves the check cycle, the render rework pass will not select it
// anymore — the bit is cleared in a pair with a decrement of the render
// debt counter by two statements of one application transaction
// (applyDeleteSQL + applyClearRenderSQL, T-269: the delta — by the fact
// of the transition, not by a snapshot).
package ingest

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"crawler/internal/model"
)

// applyDeleteSQL — applying delete: the first statement of the
// ApplyDelete transaction (spec 5.1). In the check_state FSM terms — the
// R14 row of the spec (the single source of truth:
// internal/scheduler/checkstate.go, T-255): a status transition to
// deleted, check_state is NOT changed — the row drops out of all passes
// via the status <> deleted guard predicates. The render_pending bit and
// the render debt counter are NOT touched by this statement (T-269,
// previously a CASE in moved and a CTE rd): the "bit × counter" pair is
// handled by the second statement applyClearRenderSQL of the same
// transaction — see its comment (the 2026-11-08 incident: a decrement by
// the had_render snapshot was applied a SECOND time on top of the
// concurrent clearing by the rework pass, −36 in one pass). The
// statement details:
//
//   - the pre CTE — a snapshot of the transition candidates BEFORE the
//     change (the same statement snapshot as moved's): staging×domains by
//     the (zone_id, label) key with the batch filters and the "not yet
//     deleted" guard (T-178). MATERIALIZED stabilizes the candidate set
//     against inlining (a plan shape, measured by T-178 on a live DB);
//     the cost is a chunk of the file's size; the old value of the
//     render_pending bit is no longer needed here — the bit clearing
//     moved to applyClearRenderSQL with a guard over the current version
//     of the row;
//   - the moved CTE — the status move: UPDATE domains SET status =
//     deleted for the batch's domains — a join with pre over the unique
//     (zone_id, label) key; flags are NOT touched. There was no guard on
//     the current status before T-178: a repeated application moved an
//     already deleted domain to the same deleted (at-least-once
//     idempotency, spec 3.4) — and the report counted the row as
//     "deleted". The d.status <> deleted guard is now needed for the zone
//     counter deltas (T-178, incremental-counters spec §6.2): a repeated
//     application must not decrement the counters for an already deleted
//     domain; the report keeps that — see the outer SELECT. The guard is
//     repeated in moved (not only in pre): when competing for a row (a
//     second delete file over the same domain), re-checking the condition
//     after waiting for the lock cuts off the already moved row from the
//     deltas. RETURNING carries had_render — the render_pending bit of
//     the row version the update works WITH: for rows untouched by
//     competitors it is the statement snapshot; for rows re-acquired
//     after waiting for the lock (EvalPlanQual) — the CURRENT committed
//     version. Columns outside SET are returned by RETURNING at the base
//     version — that is exactly "the bit at the moment of the
//     transition", the fact of the transition the second statement keeps
//     the counter by (PG16 without RETURNING OLD cannot see the old
//     values of the columns FROM SET — hence the bit is not cleared
//     here);
//   - the zc CTE — the zone counter deltas over the RETURNING zone_id,
//     check_state of the moved rows (T-178): total −n and, by the state —
//     pending −n / queued −n / checked −n (the statement does not change
//     check_state — RETURNING yields the old value, incremental-counters
//     spec §6.1.2); the base set of the zone columns is status <>
//     deleted (incremental-counters spec §4.1), the deleted domain leaves
//     all the columns;
//   - the outer SELECT without FROM always returns one row: the deleted
//     counters (the file rows whose domain was moved by this statement —
//     EXISTS over moved — OR was already deleted before it — EXISTS over
//     domains with the old status: the statement snapshot does not see
//     its own moved record; the counter semantics are the same as before
//     — a repeated label in the file counts each of its rows, a repeated
//     application counts its own rows as deleted) and not_found (the
//     rows whose domain is not in domains), plus the bit row keys
//     render_zones/render_labels — parallel arrays of the (zone_id,
//     label) of the moved rows with had_render: the targets of the second
//     statement. All the staging accesses go through StageTable: the
//     single source of the table for COPY and the batch cleanup.
//
// Both sets (staging and domains) are already in the DB, hence one
// set-based statement instead of a db.Update with unnest arrays: nothing
// is carried through the application, memory does not depend on the file
// size (spec 3.5; the bit arrays are only the flag rows of the
// transition, usually small), and the counters are computed by the same
// statement and do not diverge from the application.
var applyDeleteSQL = `WITH pre AS MATERIALIZED (
	SELECT d.zone_id, d.label
	FROM ` + StageTable.Sanitize() + ` AS s
	JOIN domains AS d ON d.zone_id = s.zone_id AND d.label = s.label
	WHERE s.batch_id = $1
	  AND s.action = $3
	  AND d.status <> $2
), moved AS (
	UPDATE domains AS d
	SET status = $2
	FROM pre AS p
	WHERE d.zone_id = p.zone_id
	  AND d.label = p.label
	  AND d.status <> $2
	RETURNING d.zone_id, d.label, d.check_state, (d.flags & ` + fmt.Sprintf("%d", int16(model.FlagRenderPending)) + `) <> 0 AS had_render
), zc AS (
	UPDATE zones AS z
	SET domains_total = z.domains_total - c.n_total,
	    domains_pending = z.domains_pending - c.n_pending,
	    domains_queued = z.domains_queued - c.n_queued,
	    domains_checked = z.domains_checked - c.n_checked
	FROM (SELECT zone_id,
	             count(*) AS n_total,
	             count(*) FILTER (WHERE check_state = ` + fmt.Sprint(int16(model.CheckStatePending)) + `) AS n_pending,
	             count(*) FILTER (WHERE check_state = ` + fmt.Sprint(int16(model.CheckStateQueued)) + `) AS n_queued,
	             count(*) FILTER (WHERE check_state = ` + fmt.Sprint(int16(model.CheckStateDone)) + `) AS n_checked
	      FROM moved GROUP BY zone_id) AS c
	WHERE z.id = c.zone_id
)
SELECT
	(SELECT count(*) FROM ` + StageTable.Sanitize() + ` AS s
	WHERE s.batch_id = $1 AND s.action = $3
	  AND (EXISTS (SELECT 1 FROM moved AS m
	               WHERE m.zone_id = s.zone_id AND m.label = s.label)
	       OR EXISTS (SELECT 1 FROM domains AS d2
	                  WHERE d2.zone_id = s.zone_id AND d2.label = s.label
	                    AND d2.status = $2))) AS deleted,
	(SELECT count(*) FROM ` + StageTable.Sanitize() + ` AS s
	WHERE s.batch_id = $1 AND s.action = $3
	  AND NOT EXISTS (SELECT 1 FROM domains AS d
	                  WHERE d.zone_id = s.zone_id AND d.label = s.label)) AS not_found,
	(SELECT coalesce(array_agg(m.zone_id ORDER BY m.zone_id, m.label), '{}'::int2[]) FROM moved AS m WHERE m.had_render) AS render_zones,
	(SELECT coalesce(array_agg(m.label ORDER BY m.zone_id, m.label), '{}'::text[]) FROM moved AS m WHERE m.had_render) AS render_labels`

// applyClearRenderSQL — the second statement of the ApplyDelete
// transaction: the pair "clearing the render_pending bit × decrementing
// the render debt counter" (T-186 owns the transition, T-269 — the
// shape; the D6 transition of the bit × counter FSM spec —
// internal/httpworker/renderq.go at renderSetPendingSQL, T-258). The
// deleted domain leaves the check cycle (§13.1), the render rework pass
// will not select it anymore (renderq excludes deleted), and an
// ownerless bit would hold the render debt counter forever — the T-181
// guard would not sleep and would run an empty seq scan every tick.
//
// The keys are the parallel arrays of applyDeleteSQL
// (render_zones/render_labels: the moved rows whose bit was set at the
// moment of the transition); the clearing goes by the GUARD of the bit
// of the row's current version `(flags & render_pending) <> 0`, the
// counter delta — by the FACT of the cleared rows (the count from
// RETURNING): exactly the semantics of the runbooks §6 pair and the
// httpworker statements (I1 of the FSM: RowsAffected 0 — no transition,
// the counter is not touched). The guard closes the "snapshot → lock"
// window that broke the single statement of T-186 (the 2026-11-08
// computed by the SNAPSHOT had_render from pre, while the bit-clearing
// CASE read the CURRENT version — the rework pass that cleared the bits
// between the snapshot and the lock had already contributed a −1 each,
// and rd added a second −n by the snapshot: −36 in one pass with a
// single set of clearings of −18, the counter at −18 with a zero bit.
// Now: if a competitor (the rework pass, a second application instance)
// cleared the bit earlier — the guard yields 0 rows, the counter is not
// touched (the transition is already accounted for by the competitor,
// D7); if the bit is set — the clearing and the −1 happen here in one
// application transaction. The idempotency of re-applying the batch
// (spec 3.4) is preserved: no moved rows — the arrays are empty, the
// statement does not run at all (the Go side), no repeated deltas.
//
// The "domains → zones → counter" lock order of the docs/schema.md
// invariant is preserved: cleared re-locks ONLY the rows already locked
// by moved of the same transaction (the arrays are a subset of the moved
// rows), there are no new "zones → domains" edges; the counter row —
// last, like with all the transition writers. The condition
// `(SELECT count(*) FROM cleared) > 0` does not touch the counter row
// for files without flagged domains (the single serialization point of
// the flagging transactions, record.go; the HAVING precedent of T-186).
// RETURNING value — a row check: an empty result = the 'render_pending'
// row is missing (migration 000015; only a manual edit can remove it,
// runbooks §6) — an error is louder than a silent desync (the
// renderBumpDebtCounter philosophy, T-175). The literal style — schema
// codes via model.* (like applyDeleteSQL).
var applyClearRenderSQL = `WITH cleared AS (
	UPDATE domains AS d
	SET flags = (d.flags & ` + fmt.Sprintf("%d", int16(^int16(model.FlagRenderPending))) + `)::int2
	FROM unnest($1::int2[], $2::text[]) AS k(zone_id, label)
	WHERE d.zone_id = k.zone_id
	  AND d.label = k.label
	  AND (d.flags & ` + fmt.Sprintf("%d", int16(model.FlagRenderPending)) + `) <> 0
	RETURNING 1
)
UPDATE crawler_counters
SET value = value - (SELECT count(*) FROM cleared)
WHERE name = 'render_pending' AND (SELECT count(*) FROM cleared) > 0
RETURNING value`

// DeleteResult — the per-file delete application counters for the report
// (spec 5.1: "deleted"; written to ingest_files by the T-021 task). File
// rows are counted: each row of a delete file either moved its domain to
// deleted, or the domain is not in the DB.
type DeleteResult struct {
	// Deleted — the rows whose domains were moved to the deleted status.
	// A domain already in deleted also counts as deleted: the row's
	// "domain deleted" outcome is achieved, there is no separate
	// "already deleted" counter — spec 5.1/13.1 distinguish only the
	// final status, not the path to it.
	Deleted int64
	// NotFound — the rows whose domain is not in domains: nothing to
	// delete.
	NotFound int64
}

// ApplyDelete applies a batch of action=delete from staging (Stage,
// T-017): moves the status of the matching domains to model.StatusDeleted
// and clears the render_pending bit with a decrement of the render debt
// counter — by two statements of ONE transaction (T-186 — the
// transition, T-269 — the pair's shape: the delta by the fact of the
// transition, not by a snapshot; the 2026-11-08 incident — the
// applyClearRenderSQL comment) — and returns the file row counters
// (DeleteResult). The domains stay in the DB (spec 13.1); the results
// and content references are removed by the background task (T-020).
//
// The application is the applyDeleteSQL + applyClearRenderSQL
// transaction: an empty batch — a no-op with zero counters; a failure of
// any step rolls back the move and the pair entirely — a repeated
// delivery of the file converges to the same state (spec 3.4). After the
// application the batch is cleaned from staging (ClearBatch, stage.go) —
// staging does not grow from file to file. The cleanup is a separate
// statement outside the application transaction: a failure between them
// is harmless — re-applying will move the same domains to the same
// deleted (a no-op state-wise, the bit arrays are empty) and will finish
// cleaning the batch (spec 3.4). On a cleanup error the counters are
// returned together with the error: the application has already
// happened, the report on it is valid.
func ApplyDelete(ctx context.Context, pool ApplyDB, b Batch) (DeleteResult, error) {
	if b.ID == (BatchID{}) {
		return DeleteResult{}, errors.New("ingest: batch without an identifier (ingest.NewBatchID)")
	}
	if b.Action != model.ActionDelete {
		return DeleteResult{}, fmt.Errorf("ingest: batch action %s: applying delete expects a delete batch (spec 5.1)", b.Action)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return DeleteResult{}, fmt.Errorf("ingest: applying delete to the batch in %s: transaction: %w", StageTable, err)
	}
	// Rollback unconditionally: after Commit it is a no-op (the T-044
	// style).
	defer func() { _ = tx.Rollback(ctx) }()

	res, zones, labels, err := applyDeleteCounters(ctx, tx, b)
	if err != nil {
		return DeleteResult{}, err
	}
	if len(zones) > 0 {
		if err := applyDeleteClearRender(ctx, tx, zones, labels); err != nil {
			return DeleteResult{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return DeleteResult{}, fmt.Errorf("ingest: applying delete to the batch in %s: commit: %w", StageTable, err)
	}

	// Applied — the batch leaves staging: cleanup after the application,
	// not before (otherwise an application failure would lose the batch
	// rows before the retry).
	if _, err := ClearBatch(ctx, pool, b.ID); err != nil {
		return res, err
	}
	return res, nil
}

// applyDeleteCounters runs applyDeleteSQL and parses the result row (one
// — the query has no FROM): the file row counters and the bit row keys
// (render_zones/render_labels — parallel arrays of the moved rows with
// the render_pending bit) for the second statement.
func applyDeleteCounters(ctx context.Context, tx pgx.Tx, b Batch) (DeleteResult, []int16, []string, error) {
	rows, err := tx.Query(ctx, applyDeleteSQL, b.ID[:], int16(model.StatusDeleted), int16(model.ActionDelete))
	if err != nil {
		return DeleteResult{}, nil, nil, fmt.Errorf("ingest: applying delete to the batch in %s: %w", StageTable, err)
	}
	defer rows.Close()

	var res DeleteResult
	var zones []int16
	var labels []string
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return DeleteResult{}, nil, nil, fmt.Errorf("ingest: applying delete to the batch in %s: %w", StageTable, err)
		}
		return DeleteResult{}, nil, nil, fmt.Errorf("ingest: applying delete to the batch in %s: the counters were not returned", StageTable)
	}
	if err := rows.Scan(&res.Deleted, &res.NotFound, &zones, &labels); err != nil {
		return DeleteResult{}, nil, nil, fmt.Errorf("ingest: applying delete to the batch in %s: %w", StageTable, err)
	}
	if err := rows.Err(); err != nil {
		return DeleteResult{}, nil, nil, fmt.Errorf("ingest: applying delete to the batch in %s: %w", StageTable, err)
	}
	if len(zones) != len(labels) {
		return DeleteResult{}, nil, nil, fmt.Errorf("ingest: applying delete to the batch in %s: the bit arrays diverged (%d zones, %d labels)", StageTable, len(zones), len(labels))
	}
	return res, zones, labels, nil
}

// applyDeleteClearRender runs applyClearRenderSQL — the pair "clearing
// the bit × decrementing the counter" over the keys of the transition's
// bit rows (T-186/T-269; the SQL comment). An empty RETURNING result —
// the 'render_pending' counter row is missing: a silent desync is
// unacceptable, an error is louder (the renderBumpDebtCounter
// philosophy, runbooks §6).
func applyDeleteClearRender(ctx context.Context, tx pgx.Tx, zones []int16, labels []string) error {
	rows, err := tx.Query(ctx, applyClearRenderSQL, zones, labels)
	if err != nil {
		return fmt.Errorf("ingest: applying delete: clearing the render_pending bit: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return fmt.Errorf("ingest: applying delete: clearing the render_pending bit: %w", err)
		}
		return fmt.Errorf("ingest: applying delete: clearing the render_pending bit: the 'render_pending' counter row is missing (migration 000015, runbooks §6)")
	}
	var discarded int64
	if err := rows.Scan(&discarded); err != nil {
		return fmt.Errorf("ingest: applying delete: clearing the render_pending bit: read: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("ingest: applying delete: clearing the render_pending bit: %w", err)
	}
	return nil
}

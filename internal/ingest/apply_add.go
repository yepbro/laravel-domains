// Applying an add batch to domains (T-018, spec 5.1): the batch rows in
// staging are inserted into domains in chunks of ingest.batch_size (T-205)
// via INSERT … SELECT … ON CONFLICT DO NOTHING (the conflict is the unique
// (zone_id, label) of the domains partitions); an existing domain is not
// changed, except for the deleted status — such a domain is returned to
// checking (status → new, check_state → pending, spec 6.2). Chunks are
// walked with a cursor over the staging PK (batch_id, line_no); the whole
// application is a single transaction: atomicity is equivalent to a single
// statement of the T-018 era, and lock hold time and slow-query log entries
// are bounded by a chunk (production measurement T-205: a daily NRD file of
// ~470k domains took 63–200 s as a single statement under evening CPU
// starvation; a 50k chunk takes seconds). After the application the batch
// is removed from staging: apply → clean.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/db"
	"crawler/internal/model"
)

// ApplyDB — a pool for applying add: a db.DB from which a transaction can
// be started. Applying a batch is atomic (T-205): the insert chunks, the
// zone counter deltas, the revive of deleted domains and the staging
// cleanup are one transaction, so db.DB is not enough — Begin is needed;
// *pgxpool.Pool satisfies it (CleanupDB style).
type ApplyDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// compile-time: the real pool satisfies the interface.
var _ ApplyDB = (*pgxpool.Pool)(nil)

// SQL for applying add. Semantic constants (status codes, flags bits)
// are baked into the text via model.*: these are schema codes, not data
// (zones.go style with is_active = true); the test contract ties the
// literals to the constants.
var (
	// addChunkBoundSQL — the bound of the next application chunk (T-205):
	// the line_no of the Nth batch row after cursor $3 in staging PK order
	// (batch_id, line_no, migration 000004) — the subquery cuts the range
	// with LIMIT $4, the outer max gives the line_no of the chunk's last
	// row (ascending order). An empty range — COALESCE → 0: no rows after
	// the cursor, the batch is exhausted. The cursor moves over the
	// staging range, not over the inserted rows: an all-conflict chunk
	// (ON CONFLICT DO NOTHING skipped everything) is still closed by the
	// bound — otherwise applying a batch of all-existing domains would
	// loop forever. The scan is over the primary key only: exactly a
	// chunk of rows, with no reads of the table data.
	addChunkBoundSQL = "SELECT COALESCE(max(t.line_no), 0) FROM (" +
		"SELECT s.line_no FROM ingest_stage AS s " +
		"WHERE s.batch_id = $1 AND s.action = $2 AND s.line_no > $3 " +
		"ORDER BY s.line_no LIMIT $4) AS t"

	// insertAddSQL — the new domains of one batch chunk (spec 5.1; T-205):
	// the staging rows in the cursor range ($3, $4] are inserted by a
	// single INSERT … SELECT; ON CONFLICT DO NOTHING skips the existing
	// ones (including repeated labels within one file, migration 000004).
	// New domains get the schema DEFAULT: status = new, check_state =
	// pending (spec 6.2) — the domain is in the check cycle right away
	// (the R10 insert transition of the check_state FSM spec —
	// internal/scheduler/checkstate.go, T-255).
	// is_idn is computed from the label (spec 5.2 item 9): staging does
	// not store the flag, and the label is its only source. The condition
	// is any name label starting with xn-- (a label can be composite,
	// ING-P1-1): the first one ('xn--%') or one after a dot ('%.xn--%') —
	// equivalent to idn.HasXNLabel; ILIKE — case-insensitive.
	//
	// The RETURNING zone_id of the inserted rows is read as a stream: the
	// Inserted counter and the zone counter deltas (incremental-counters
	// spec §6.2) accumulate in Go and are applied by a single final
	// statement after the chunks finish (zoneDeltaAddSQL) — memory is
	// bounded by the chunk and the batch's zone count, not the file size
	// (spec 3.5).
	insertAddSQL = "INSERT INTO domains (zone_id, label, flags) " +
		"SELECT s.zone_id, s.label, CASE WHEN s.label ILIKE 'xn--%' OR s.label ILIKE '%.xn--%' THEN " + fmt.Sprint(int16(model.FlagIDN)) + " ELSE 0 END " +
		"FROM ingest_stage AS s " +
		"WHERE s.batch_id = $1 AND s.action = $2 AND s.line_no > $3 AND s.line_no <= $4 " +
		"ON CONFLICT DO NOTHING RETURNING zone_id"

	// zoneDeltaAddSQL — the final zone counter deltas of the insert
	// (T-205, incremental-counters spec §6.2: new domains are counted in
	// both total and pending): an unnest batch of zone_id→n pairs, the
	// style of the package's internal statements (internal/db, cleanup.go).
	// One statement per batch instead of a CTE in every INSERT: the zone
	// counters are touched once per application — a point write instead
	// of a storm of small UPDATEs, with atomicity shared with the insert
	// (the ApplyAdd transaction). In SET the columns are not qualified
	// with an alias — a PG requirement (see internal/db/update.go).
	zoneDeltaAddSQL = "UPDATE zones AS z " +
		"SET domains_total = z.domains_total + d.n, " +
		"domains_pending = z.domains_pending + d.n " +
		"FROM unnest($1::int2[], $2::int8[]) AS d(zone_id, n) " +
		"WHERE z.id = d.zone_id"

	// reviveAddSQL — returning the batch's deleted domains to checking
	// (spec 5.1: an existing domain is not changed, "except for the
	// deleted status — it is returned to new/pending"). The check_state
	// transition is the R9 row of the FSM spec (the single source of
	// truth: internal/scheduler/checkstate.go, T-255). The guard
	// d.status = deleted touches only the deleted rows: the batch does
	// not change the other existing domains at all. One SQL instead of a
	// batched db.Update: the keys are already in the DB,
	// selecting the labels into memory and sending them back contradicts
	// streaming (spec 3.5). In SET the columns are not qualified with an
	// alias — a PG requirement (see internal/db/update.go). queued_at is
	// reset: a pending domain has no active lease (spec 5.3). flags,
	// dns_mask, last_checked_at are not touched: the label marks and the
	// history of past checks are preserved — a recheck will rewrite them
	// itself.
	//
	// The zone counter deltas (total +n / pending +n) — the zc CTE over
	// the RETURNING zone_id of the returned rows (T-178,
	// incremental-counters spec §6.2: the domain was outside the base
	// set — the counters did not include it, no decrements).
	//
	// T-205: the revive is left as a single statement for the whole
	// batch, without chunking — revive touches only the batch's deleted
	// domains, and NRD files have almost none (a registration day is
	// younger than any deletions); the scale is small and does not pay
	// off in chunks.
	reviveAddSQL = "WITH rev AS (UPDATE domains AS d " +
		fmt.Sprintf("SET status = %d, check_state = %d, queued_at = NULL ", int16(model.StatusNew), int16(model.CheckStatePending)) +
		"FROM ingest_stage AS s " +
		"WHERE s.batch_id = $1 AND s.action = $2 " +
		"AND d.zone_id = s.zone_id AND d.label = s.label " +
		fmt.Sprintf("AND d.status = %d RETURNING d.zone_id), ", int16(model.StatusDeleted)) +
		"zc AS (UPDATE zones AS z " +
		"SET domains_total = z.domains_total + c.n, " +
		"domains_pending = z.domains_pending + c.n " +
		"FROM (SELECT zone_id, count(*) AS n FROM rev GROUP BY zone_id) AS c " +
		"WHERE z.id = c.zone_id) " +
		"SELECT count(*) FROM rev"

	// countAddSQL — the batch's unique domains (by the zone_id, label
	// pair — duplicate file rows do not inflate the counters) before the
	// cleanup: inserted and revived are subtracted from it — the "already
	// existed" remainder. One query per batch (T-205): DISTINCT over
	// staging is cheaper than the insert and never hit the slow-query
	// window.
	countAddSQL = "SELECT count(*) FROM (" +
		"SELECT DISTINCT s.zone_id, s.label FROM ingest_stage AS s " +
		"WHERE s.batch_id = $1 AND s.action = $2) AS t"
)

// AddResult — the result of applying an add batch: the file report
// counters (spec 5.1 — T-021). Domains are counted by the batch's unique
// (zone_id, label) pairs: duplicate file rows are deduplicated by the
// conflict (migration 000004), each label enters exactly one counter.
type AddResult struct {
	// Inserted — added: new domains inserted (the sum of the chunk
	// inserts, T-205).
	Inserted int64
	// Existing — already existed: existing domains not in the deleted
	// status — the batch did not change them (spec 5.1).
	Existing int64
	// Revived — returned from deleted: status → new, check_state →
	// pending — the domain is back in the check cycle (spec 5.1, 6.2).
	Revived int64
}

// Total — the batch's unique domains: Inserted + Existing + Revived.
func (r AddResult) Total() int64 { return r.Inserted + r.Existing + r.Revived }

// ApplyAdd applies an add batch to domains (spec 5.1) in a SINGLE
// transaction (T-205), step by step in a fixed order pinned by tests:
//
//  1. inserting new domains in chunks of batchSize rows
//     (ingest.batch_size, like COPY into staging): the cursor over the
//     staging PK (batch_id, line_no) — the chunk bound
//     (addChunkBoundSQL), the INSERT of the range (insertAddSQL); the
//     next chunk continues after the last row of the previous one —
//     regardless of whether its rows were inserted (an all-conflict
//     chunk is closed by the same bound). zone_id and label come from
//     the staging rows: the zone was created by EnsureZone (T-014) and
//     is carried by every batch row;
//  2. the zone counter deltas (total +n / pending +n,
//     incremental-counters spec §6.2) — one final UPDATE … FROM unnest
//     over the zone_ids accumulated from the RETURNING of all chunks;
//  3. UPDATE of the batch's existing rows with the deleted status —
//     returning them to checking (status → new, check_state → pending,
//     spec 6.2), one statement per batch;
//  4. counting the batch's unique domains before cleanup:
//     Existing = unique − Inserted − Revived;
//  5. ClearBatch — the batch is removed from staging
//     (apply → clean).
//
// The transaction makes the application an atomic equivalent of a single
// statement of the T-018 era: a failure of any step rolls back the whole
// application, the batch rows stay in staging, a repeated application
// performs the same steps on the same data and converges to the same
// result (idempotency, spec 3.4); a committed application clears staging,
// and re-submitting the same list (a new batch) hits the conflicts — the
// counters of the repeated run reflect the current state, and the file
// report is written by the first successful attempt (T-021). Re-applying
// an already applied (cleaned) batch is a no-op with a zero result.
func ApplyAdd(ctx context.Context, pool ApplyDB, batch Batch, batchSize int) (AddResult, error) {
	var res AddResult
	if batch.ID == (BatchID{}) {
		return res, errors.New("ingest: batch without an identifier (ingest.NewBatchID)")
	}
	if batch.Action != model.ActionAdd {
		return res, fmt.Errorf("ingest: batch action %s: applying add — only for an add batch (spec 5.1)", batch.Action)
	}
	if batchSize < 1 {
		return res, fmt.Errorf("ingest: chunk size %d, minimum 1 (ingest.batch_size)", batchSize)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return res, fmt.Errorf("ingest: applying the add batch: transaction: %w", err)
	}
	// Rollback unconditionally: after Commit it is a no-op. An interrupted
	// application leaves no half-done changes (spec 3.4).
	defer func() { _ = tx.Rollback(ctx) }()

	args := []any{batch.ID[:], int16(model.ActionAdd)}

	inserted, zones, err := applyAddInsert(ctx, tx, batch, batchSize)
	if err != nil {
		return res, err
	}
	res.Inserted = inserted

	if err := applyAddZoneDeltas(ctx, tx, zones); err != nil {
		return res, err
	}

	n, err := applyAddCount(ctx, tx, reviveAddSQL, args, "reviving deleted domains of the add batch")
	if err != nil {
		return res, err
	}
	res.Revived = n

	uniques, err := applyAddCount(ctx, tx, countAddSQL, args, "counting the add batch's domains")
	if err != nil {
		return res, err
	}
	if uniques < res.Inserted+res.Revived {
		return res, fmt.Errorf("ingest: applying the add batch: %d unique domains is less than the inserted and revived %d — the counters diverge",
			uniques, res.Inserted+res.Revived)
	}
	res.Existing = uniques - res.Inserted - res.Revived

	// Applied — the batch leaves staging in the same transaction: cleanup
	// after the application, not before (otherwise an application failure
	// would lose the batch rows before the retry).
	if _, err := ClearBatch(ctx, tx, batch.ID); err != nil {
		return res, err
	}
	if err := tx.Commit(ctx); err != nil {
		return res, fmt.Errorf("ingest: applying the add batch: commit: %w", err)
	}
	return res, nil
}

// applyAddInsert inserts the batch's new domains in chunks of batchSize
// rows (T-205) and returns the inserted counter and the zone counter
// deltas accumulated by zone_id (applied by a single statement after the
// chunks finish). The cursor is the line_no of the last closed row: a
// chunk is the range (cursor, bound], the batch rows are numbered from
// one, the initial cursor 0 covers the whole batch. Memory is bounded by
// the chunk and the batch's zone count, not the file size (spec 3.5):
// the RETURNING of the inserts is read as a stream, only counters
// accumulate in Go.
func applyAddInsert(ctx context.Context, tx pgx.Tx, b Batch, batchSize int) (int64, map[int16]int64, error) {
	zones := make(map[int16]int64)
	var (
		inserted int64
		cursor   int64
	)
	for {
		rows, err := tx.Query(ctx, addChunkBoundSQL, b.ID[:], int16(model.ActionAdd), cursor, batchSize)
		if err != nil {
			return 0, nil, fmt.Errorf("ingest: the add batch's new-domain insert chunk boundary: %w", err)
		}
		bound, err := pgx.CollectOneRow(rows, pgx.RowTo[int64])
		if err != nil {
			return 0, nil, fmt.Errorf("ingest: the add batch's new-domain insert chunk boundary: %w", err)
		}
		if bound == 0 {
			return inserted, zones, nil // the batch is exhausted
		}

		rows, err = tx.Query(ctx, insertAddSQL, b.ID[:], int16(model.ActionAdd), cursor, bound)
		if err != nil {
			return 0, nil, fmt.Errorf("ingest: inserting the add batch's new domains: %w", err)
		}
		for rows.Next() {
			var zone int16
			if err := rows.Scan(&zone); err != nil {
				rows.Close()
				return 0, nil, fmt.Errorf("ingest: inserting the add batch's new domains: %w", err)
			}
			inserted++
			zones[zone]++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return 0, nil, fmt.Errorf("ingest: inserting the add batch's new domains: %w", err)
		}
		rows.Close()

		// The cursor closes the chunk over staging, not over the inserted
		// rows: a fully conflicting chunk also moves the cursor forward.
		cursor = bound
	}
}

// applyAddZoneDeltas applies the accumulated zone counter deltas with a
// single statement (zoneDeltaAddSQL, T-205). Empty deltas (a batch with
// no inserts) are a no-op without a DB call. The zone_id order is
// deterministic (sorted) — reproducible chunks, like cleanupRefcounts.
func applyAddZoneDeltas(ctx context.Context, tx pgx.Tx, zones map[int16]int64) error {
	if len(zones) == 0 {
		return nil
	}
	ids := make([]int16, 0, len(zones))
	for id := range zones {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ns := make([]int64, len(ids))
	for i, id := range ids {
		ns[i] = zones[id]
	}
	if _, err := tx.Exec(ctx, zoneDeltaAddSQL, ids, ns); err != nil {
		return fmt.Errorf("ingest: the add batch's zone counter deltas: %w", err)
	}
	return nil
}

// applyAddCount runs the application's CTE statement (the revive with
// the zone counter deltas in one statement, T-178) or the count, and
// returns the counter of the outer SELECT (the number of batch rows
// affected). Called in the ApplyAdd transaction: pgx.Tx satisfies db.DB.
func applyAddCount(ctx context.Context, pool db.DB, sql string, args []any, kind string) (int64, error) {
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return 0, fmt.Errorf("ingest: %s: %w", kind, err)
	}
	defer rows.Close()
	n, err := pgx.CollectOneRow(rows, pgx.RowTo[int64])
	if err != nil {
		return 0, fmt.Errorf("ingest: %s: %w", kind, err)
	}
	return n, nil
}

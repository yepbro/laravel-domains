// Persistent retention drain cursor (T-274; migration 000020 — the
// retention_drain_cursor table): the drain position "once in a lifetime"
// (T-185) survives not only the cycle passes but also a RESTART of the
// http-worker process — a deploy, an external power-cycle of the VM
// (precedent Nov 11 11:03), a potential OOM kill. Before T-274 a restart
// reset the cursor to zero: the first pass re-traversed the "clean prefix"
// of subdomain_results history (the "anything to purge" predicate is not
// index-backed — a heap visit of every row; measurements 2026-11-10/11:
// ~60 min idle, >60 min under full front load), three passes under front
// load did not fit into the SelectTimeout cap of 3600 s (the T-273
// incident) — the drain stood still until the quiet window.
//
// Atomicity contract: the cursor upsert runs in the batch purge
// transaction (retentionPurge) — the saved position cannot overtake the
// committed batch; a batch failure keeps the previous cursor, and a pass
// retry idempotently re-reads the already-trimmed rows (spec 3.4). A batch
// with rows deferred by the queue guards does NOT move the position in the
// DB (review of T-274, P1): the loss of the retry list on a restart is
// compensated by re-reading the guard window — the keys will be deferred
// again, and the trimmed rows fall out of the selection on their own; the
// in-memory cursor moves as before (the retry probe returns keys within
// the process lifetime). Forced catch-up circles (retentiondrain.go
// planCircle) delete the row with a DELETE at the moment of the plan: a
// restart in the window between the plan and the apply lands in a full
// catch-up from zero rather than a continuation from the old position —
// it is exactly the circle that trims the records behind it.
//
// Missing/invalid state — start from zero (the pre-T-274 behavior: one
// full catch-up pass). No table (DB before migration 000020) — the
// sentinel errRetentionNoCursorTable: one Info per process and operation
// without persistence (the style of errRetentionNoCounterTable, T-185);
// the state's owner is only the RetentionLoop cycle of http-worker; the
// one-off RetentionOnce (runbooks T-167, isolated builds) neither reads
// nor writes it.
//
// Since T-295 the row also carries the debt-era state of the drain
// machine (debt_seen, migration 000021): the era observation is
// persisted at the moment it is made (retentionMarkDebtSeen) and stamped
// by every batch upsert, so a restart restores the "waiting for the
// render-debt lift" state instead of silently disabling the catch-up
// circle (09#P2-2); NULL — the state is unknown (a pre-T-295 row): the
// restart plans a full catch-up circle rather than guessing. The save
// condition itself gained a floor (09#P2-1): while ANY queue-guard
// deferral is unresolved (the retry list is non-empty), the position is
// not saved at all — a clean batch after a deferred one must not move
// the DB position past the deferred rows (the retry list is memory-only;
// its loss on a restart is compensated by re-reading from the floor).
package content

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// SQL of the persistent cursor. There is one row (id = 1, the CHECK of
// migration 000020) and one writer (the http-worker cycle) — no conflicts;
// the texts are pinned by unit tests (retentioncursor_test.go).
var (
	// retentionCursorLoadSQL — reading the cursor at cycle start: a point
	// lookup by the PK of the single row. debt_seen (T-295, migration
	// 000021) — the debt-era state restored together with the position;
	// NULL — unknown (a row of a pre-T-295 binary): the caller plans a
	// catch-up circle instead of guessing.
	retentionCursorLoadSQL = "SELECT checked_at, zone_id, domain_id, subdomain_id, variant, debt_seen " +
		"FROM retention_drain_cursor WHERE id = 1"

	// retentionCursorSaveSQL — the cursor upsert (in the batch
	// transaction): INSERT … ON CONFLICT by the PK of the single row;
	// updated_at = now() — diagnostics of the last movement. Batches with
	// rows deferred by the guards do not write the position (the cursor
	// does not move past unpurged data — review of T-274, P1; while ANY
	// deferred key is unresolved — 09#P2-1, the floor is the retry list).
	// debt_seen is stamped by the same statement (T-295): the row always
	// leaves the transaction with a definite debt state — a NULL (unknown)
	// row heals on the first committed batch.
	retentionCursorSaveSQL = "INSERT INTO retention_drain_cursor " +
		"(id, checked_at, zone_id, domain_id, subdomain_id, variant, debt_seen, updated_at) " +
		"VALUES (1, $1, $2, $3, $4, $5, $6, now()) " +
		"ON CONFLICT (id) DO UPDATE SET " +
		"checked_at = EXCLUDED.checked_at, zone_id = EXCLUDED.zone_id, " +
		"domain_id = EXCLUDED.domain_id, subdomain_id = EXCLUDED.subdomain_id, " +
		"variant = EXCLUDED.variant, debt_seen = EXCLUDED.debt_seen, " +
		"updated_at = EXCLUDED.updated_at"

	// retentionDebtSeenSQL — persisting the debt-era observation at the
	// moment it is made (T-295, 09#P2-2): a separate short transaction,
	// NOT the batch one — the era may start in a pass that commits no
	// batch at all (a standstill), and the marker must reach the DB before
	// a restart can miss the era. On conflict only debt_seen is touched:
	// the position belongs to the batch upserts (retentionCursorSaveSQL);
	// on a fresh insert the position is the zero cursor — the row was
	// absent, i.e. a restart would start from zero anyway, and the load
	// takes the "no state" branch reading debt_seen regardless. The stamp
	// is the Go zero time as a literal (review of T-295, P3-1):
	// '0001-01-01 00:00:00+00' scans into exactly time.Time{}, while the
	// Unix epoch '1970-01-01' would scan into a NON-zero time.Unix(0,0)
	// and load as a position of year 1970 — behaviorally near-equivalent,
	// but silently different from the documented path and from the unit
	// fake.
	retentionDebtSeenSQL = "INSERT INTO retention_drain_cursor " +
		"(id, checked_at, zone_id, domain_id, subdomain_id, variant, debt_seen, updated_at) " +
		"VALUES (1, '0001-01-01 00:00:00+00', 0, 0, 0, 0, true, now()) " +
		"ON CONFLICT (id) DO UPDATE SET debt_seen = true, updated_at = now()"

	// retentionCursorDeleteSQL — resetting the persistent state on a
	// forced catch-up circle (catchUp): the circle must survive a restart —
	// it traverses everything behind the previous cursor; the saved
	// position is in its way. The deletion also drops the debt marker:
	// the circle closes the era it was planned for.
	retentionCursorDeleteSQL = "DELETE FROM retention_drain_cursor WHERE id = 1"
)

// Cursor state sentinels (handled by the callers, not pass errors —
// the style of errRetentionNoCounterTable).
var (
	// errRetentionNoCursorTable — the retention_drain_cursor table does
	// not exist (SQLSTATE 42P01, DB before migration 000020): a normal
	// transient state of an old bench — persistence is disabled for the
	// process.
	errRetentionNoCursorTable = errors.New("content: retention: the persistent cursor table is missing (migration 000020 not applied)")

	// errRetentionNoDebtColumn — the debt_seen column is missing
	// (SQLSTATE 42703, undefined_column — the DB before migration 000021:
	// the table of 000020 without the T-295 extension). The position
	// cannot be read (and the upserts would fail on the column too), so
	// persistence is disabled the same way as for a missing table: a
	// degradation to full catch-ups, not a breakage.
	errRetentionNoDebtColumn = errors.New("content: retention: the debt state column is missing (migration 000021 not applied)")

	// errRetentionCursorInvalid — the saved state is invalid (a stamp in
	// the future beyond the hourly allowance — a desync of manual SQL):
	// start from the zero cursor, a Warn to the journal.
	errRetentionCursorInvalid = errors.New("content: retention: the saved cursor is malformed (the stamp is in the future)")
)

// retentionCursorSkew — the allowed clock skew between the application and
// the DB when validating the loaded cursor: checked_at is written by the
// DB server's now() (T-058) and compared against the process clock.
const retentionCursorSkew = time.Minute

// retentionNoTable — SQLSTATE 42P01 (undefined_table) in a pool error:
// the schema code as a literal, as in retentionDebt (the record.go style).
func retentionNoTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

// retentionNoDebtColumnErr — SQLSTATE 42703 (undefined_column) in a pool
// error: the retention_drain_cursor of migration 000020 without the
// debt_seen column of migration 000021.
func retentionNoDebtColumnErr(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42703"
}

// retentionLoadCursor — reading the persistent cursor at cycle start.
// ok=false: no row (first run, a circle reset the state) — err = nil;
// invalid state — err = errRetentionCursorInvalid. debt — the persisted
// debt-era state read EVEN from a "no state" row (T-295): nil — unknown
// (a NULL column, a row of a pre-T-295 binary), the caller plans a
// catch-up circle; a marker-only row (a zero stamp, debt_seen=true written
// by retentionMarkDebtSeen before any batch) restores the era without a
// position. No table — the sentinel errRetentionNoCursorTable; no column —
// errRetentionNoDebtColumn (caller: an Info and operation without
// persistence). A zero row stamp (an all-zero manual SQL) is treated as
// "no state".
func retentionLoadCursor(ctx context.Context, pool RetentionDB) (cur retentionCursor, debt *bool, ok bool, err error) {
	rows, err := pool.Query(ctx, retentionCursorLoadSQL)
	if err != nil {
		if retentionNoTable(err) {
			return retentionCursor{}, nil, false, errRetentionNoCursorTable
		}
		if retentionNoDebtColumnErr(err) {
			return retentionCursor{}, nil, false, errRetentionNoDebtColumn
		}
		return retentionCursor{}, nil, false, fmt.Errorf("content: retention: reading the persistent cursor: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		// A read failure first: a dropped connection is not "no row"
		// (the retentionDebt style).
		if err := rows.Err(); err != nil {
			return retentionCursor{}, nil, false, fmt.Errorf("content: retention: reading the persistent cursor: %w", err)
		}
		return retentionCursor{}, nil, false, nil
	}
	var variant int16
	var savedDebt *bool
	if err := rows.Scan(&cur.checkedAt, &cur.key.zone, &cur.key.domain, &cur.key.sub, &variant, &savedDebt); err != nil {
		return retentionCursor{}, nil, false, fmt.Errorf("content: retention: reading the persistent cursor: row: %w", err)
	}
	cur.key.variant = variant
	debt = savedDebt
	if err := rows.Err(); err != nil {
		return retentionCursor{}, nil, false, fmt.Errorf("content: retention: reading the persistent cursor: %w", err)
	}
	if cur.checkedAt.IsZero() {
		return retentionCursor{}, debt, false, nil
	}
	if cur.checkedAt.After(time.Now().Add(retentionCursorSkew)) {
		return retentionCursor{}, nil, false, errRetentionCursorInvalid
	}
	return cur, debt, true, nil
}

// retentionSave — what a committed batch writes to the persistent state
// (T-274/T-295): the drain position and the debt-era flag of the machine
// at the moment of the batch (one definite value per statement — a NULL
// debt state heals on the first committed batch).
type retentionSave struct {
	cursor  retentionCursor
	debtEra bool
}

// retentionSaveCursorTx — the cursor upsert in the OPEN batch transaction
// (retentionPurge): the atomicity of "batch committed ⟺ cursor saved".
func retentionSaveCursorTx(ctx context.Context, tx pgx.Tx, save retentionSave) error {
	if _, err := tx.Exec(ctx, retentionCursorSaveSQL,
		save.cursor.checkedAt, save.cursor.key.zone, save.cursor.key.domain,
		save.cursor.key.sub, save.cursor.key.variant, save.debtEra,
	); err != nil {
		if retentionNoTable(err) {
			return errRetentionNoCursorTable
		}
		if retentionNoDebtColumnErr(err) {
			return errRetentionNoDebtColumn
		}
		return fmt.Errorf("content: retention: saving the drain cursor: %w", err)
	}
	return nil
}

// retentionMarkDebtSeen — persisting the debt-era observation at the
// moment it is made (T-295, 09#P2-2): a separate short transaction (the
// era may start in a pass that commits no batch — a standstill — and the
// marker must outrun a restart; see retentionDebtSeenSQL). Best effort:
// a failure keeps the in-memory era working for this process and is only
// a Warn (a restart in the era degrades to the pre-T-295 behavior — the
// fallback day or an overflow circle picks the era up later).
func retentionMarkDebtSeen(ctx context.Context, pool RetentionDB) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("content: retention: debt state transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, retentionDebtSeenSQL); err != nil {
		if retentionNoTable(err) {
			return errRetentionNoCursorTable
		}
		if retentionNoDebtColumnErr(err) {
			return errRetentionNoDebtColumn
		}
		return fmt.Errorf("content: retention: saving the debt state: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("content: retention: committing the debt state: %w", err)
	}
	return nil
}

// retentionResetCursor — deleting the persistent state on a forced
// catch-up circle (planCircle): the circle must survive a restart — it has
// to traverse everything behind the previous cursor; the saved position is
// in its way. A separate short transaction, executed at the moment of the
// plan (before the circle apply).
func retentionResetCursor(ctx context.Context, pool RetentionDB) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("content: retention: cursor reset transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, retentionCursorDeleteSQL); err != nil {
		if retentionNoTable(err) {
			return errRetentionNoCursorTable
		}
		return fmt.Errorf("content: retention: resetting the persistent cursor: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("content: retention: committing the cursor reset: %w", err)
	}
	return nil
}

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
	// lookup by the PK of the single row.
	retentionCursorLoadSQL = "SELECT checked_at, zone_id, domain_id, subdomain_id, variant " +
		"FROM retention_drain_cursor WHERE id = 1"

	// retentionCursorSaveSQL — the cursor upsert (in the batch
	// transaction): INSERT … ON CONFLICT by the PK of the single row;
	// updated_at = now() — diagnostics of the last movement. Batches with
	// rows deferred by the guards do not write the position (the cursor
	// does not move past unpurged data — review of T-274, P1).
	retentionCursorSaveSQL = "INSERT INTO retention_drain_cursor " +
		"(id, checked_at, zone_id, domain_id, subdomain_id, variant, updated_at) " +
		"VALUES (1, $1, $2, $3, $4, $5, now()) " +
		"ON CONFLICT (id) DO UPDATE SET " +
		"checked_at = EXCLUDED.checked_at, zone_id = EXCLUDED.zone_id, " +
		"domain_id = EXCLUDED.domain_id, subdomain_id = EXCLUDED.subdomain_id, " +
		"variant = EXCLUDED.variant, updated_at = EXCLUDED.updated_at"

	// retentionCursorDeleteSQL — resetting the persistent state on a
	// forced catch-up circle (catchUp): the circle must survive a restart —
	// it traverses everything behind the previous cursor; the saved
	// position is in its way.
	retentionCursorDeleteSQL = "DELETE FROM retention_drain_cursor WHERE id = 1"
)

// Cursor state sentinels (handled by the callers, not pass errors —
// the style of errRetentionNoCounterTable).
var (
	// errRetentionNoCursorTable — the retention_drain_cursor table does
	// not exist (SQLSTATE 42P01, DB before migration 000020): a normal
	// transient state of an old bench — persistence is disabled for the
	// process.
	errRetentionNoCursorTable = errors.New("content: retention: таблицы персистентного курсора нет (миграция 000020 не применена)")

	// errRetentionCursorInvalid — the saved state is invalid (a stamp in
	// the future beyond the hourly allowance — a desync of manual SQL):
	// start from the zero cursor, a Warn to the journal.
	errRetentionCursorInvalid = errors.New("content: retention: сохранённый курсор некорректен (отметка в будущем)")
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

// retentionLoadCursor — reading the persistent cursor at cycle start.
// ok=false: no row (first run, a circle reset the state) — err = nil;
// invalid state — err = errRetentionCursorInvalid. No table — the sentinel
// errRetentionNoCursorTable (caller: an Info and operation without
// persistence). A zero row stamp (an all-zero manual SQL) is treated as
// "no state".
func retentionLoadCursor(ctx context.Context, pool RetentionDB) (cur retentionCursor, ok bool, err error) {
	rows, err := pool.Query(ctx, retentionCursorLoadSQL)
	if err != nil {
		if retentionNoTable(err) {
			return retentionCursor{}, false, errRetentionNoCursorTable
		}
		return retentionCursor{}, false, fmt.Errorf("content: retention: чтение персистентного курсора: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		// A read failure first: a dropped connection is not "no row"
		// (the retentionDebt style).
		if err := rows.Err(); err != nil {
			return retentionCursor{}, false, fmt.Errorf("content: retention: чтение персистентного курсора: %w", err)
		}
		return retentionCursor{}, false, nil
	}
	var variant int16
	if err := rows.Scan(&cur.checkedAt, &cur.key.zone, &cur.key.domain, &cur.key.sub, &variant); err != nil {
		return retentionCursor{}, false, fmt.Errorf("content: retention: чтение персистентного курсора: строка: %w", err)
	}
	cur.key.variant = variant
	if err := rows.Err(); err != nil {
		return retentionCursor{}, false, fmt.Errorf("content: retention: чтение персистентного курсора: %w", err)
	}
	if cur.checkedAt.IsZero() {
		return retentionCursor{}, false, nil
	}
	if cur.checkedAt.After(time.Now().Add(retentionCursorSkew)) {
		return retentionCursor{}, false, errRetentionCursorInvalid
	}
	return cur, true, nil
}

// retentionSaveCursorTx — the cursor upsert in the OPEN batch transaction
// (retentionPurge): the atomicity of "batch committed ⟺ cursor saved".
func retentionSaveCursorTx(ctx context.Context, tx pgx.Tx, cur retentionCursor) error {
	if _, err := tx.Exec(ctx, retentionCursorSaveSQL,
		cur.checkedAt, cur.key.zone, cur.key.domain, cur.key.sub, cur.key.variant,
	); err != nil {
		if retentionNoTable(err) {
			return errRetentionNoCursorTable
		}
		return fmt.Errorf("content: retention: сохранение курсора дренажа: %w", err)
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
		return fmt.Errorf("content: retention: транзакция сброса курсора: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, retentionCursorDeleteSQL); err != nil {
		if retentionNoTable(err) {
			return errRetentionNoCursorTable
		}
		return fmt.Errorf("content: retention: сброс персистентного курсора: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("content: retention: фиксация сброса курсора: %w", err)
	}
	return nil
}

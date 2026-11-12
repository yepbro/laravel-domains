// Unit tests of applying delete without a DB (the stage_test.go/
// zones_test.go style): the SQL text and arguments, the counters, the
// "apply → clean" order, an empty batch, the retry idempotency
// (spec 3.4), the pair "clearing the render_pending bit × decrementing
// the debt counter" by two statements of one transaction
// (T-186/T-269), carrying the bit row keys between the statements, the
// step and cleanup errors, the ctx cancellation. The real behavior
// against a live DB — the ingest integration tests (T-023) and renderbit
// (the race of the 2026-11-08 incident, T-269).
package ingest

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/model"
)

// applyDeletePool — an ApplyDB for the delete application tests: the
// Begin → applyDeleteSQL → applyClearRenderSQL → Commit transaction,
// then the batch cleanup through the pool. Remembers the call order
// with the SQL and the arguments; the statement answers are prepared
// rows (a positional Scan, the cleanupFakeRows style). Step errors —
// by named fields.
type applyDeletePool struct {
	ops  []string // the order: "begin", "query:<sql>", "commit", "exec:<sql>"
	args [][]any  // the operation arguments in call order

	applyRows *cleanupFakeRows // the applyDeleteSQL answer: deleted, not_found, render_zones, render_labels
	clearRows *cleanupFakeRows // the applyClearRenderSQL answer: value

	beginErr     error
	applyErr     error
	clearRendErr error
	commitErr    error
	clearErr     error // the batch cleanup (pool)

	begins    int
	commits   int
	rollbacks int
}

var _ ApplyDB = (*applyDeletePool)(nil)

func (p *applyDeletePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom must not be called")
}

// The pool's Query is not needed by the application (the statements are
// in the transaction): fail.
func (p *applyDeletePool) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("applyDeletePool: Query outside a transaction must not be called")
}

func (p *applyDeletePool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.ops = append(p.ops, "exec:"+sql)
	p.args = append(p.args, args)
	if sql != clearStageSQL {
		return pgconn.CommandTag{}, errors.New("applyDeletePool: unexpected Exec: " + sql)
	}
	if p.clearErr != nil {
		return pgconn.CommandTag{}, p.clearErr
	}
	return pgconn.NewCommandTag("DELETE 1"), nil
}

func (p *applyDeletePool) Begin(ctx context.Context) (pgx.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	p.begins++
	p.ops = append(p.ops, "begin")
	return &applyDeleteTx{pool: p}, nil
}

// applyDeleteTx — the fake pool's transaction: the application and
// bit-clearing statements, the commit/rollback.
type applyDeleteTx struct {
	pgx.Tx // the application does not use the rest of the pgx.Tx methods

	pool      *applyDeletePool
	committed bool
}

func (t *applyDeleteTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p := t.pool
	p.ops = append(p.ops, "query:"+sql)
	p.args = append(p.args, args)
	switch sql {
	case applyDeleteSQL:
		if p.applyErr != nil {
			return nil, p.applyErr
		}
		if p.applyRows == nil {
			return nil, errors.New("applyDeletePool: the applyDeleteSQL response is not set")
		}
		return p.applyRows, nil
	case applyClearRenderSQL:
		if p.clearRendErr != nil {
			return nil, p.clearRendErr
		}
		if p.clearRows == nil {
			return nil, errors.New("applyDeletePool: the applyClearRenderSQL response is not set")
		}
		return p.clearRows, nil
	}
	return nil, errors.New("applyDeletePool: unexpected Query: " + sql)
}

func (t *applyDeleteTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("applyDeletePool: Exec inside a transaction must not be called")
}

func (t *applyDeleteTx) Commit(context.Context) error {
	t.pool.ops = append(t.pool.ops, "commit")
	if t.pool.commitErr != nil {
		return t.pool.commitErr
	}
	t.pool.commits++
	t.committed = true
	return nil
}

func (t *applyDeleteTx) Rollback(context.Context) error {
	if t.committed {
		return nil // pgx.ErrTxClosed in reality; a no-op is enough here
	}
	t.pool.rollbacks++
	return nil
}

// validDeleteBatch — a valid delete batch for the application tests.
func validDeleteBatch(t *testing.T) Batch {
	t.Helper()
	id, err := NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	return Batch{ID: id, ZoneID: 7, Action: model.ActionDelete}
}

// wantApplyArgs — the application statement's arguments: the batch
// identifier, the deleted status code and the delete action code (in
// the parameters, not in the SQL text — the exact model values are
// pinned by the test).
func wantApplyArgs(b Batch) []any {
	return []any{b.ID[:], int16(model.StatusDeleted), int16(model.ActionDelete)}
}

// TestApplyDelete — the full application protocol with bit rows: the
// transaction (the status move → the bit clearing with the counter over
// the applyDeleteSQL keys → the commit), then the batch cleanup. The
// bit row keys (render_zones/render_labels) are passed to the second
// statement pair verbatim.
func TestApplyDelete(t *testing.T) {
	b := validDeleteBatch(t)
	zones := []int16{7, 9}
	labels := []string{"a.example", "b.example"}
	pool := &applyDeletePool{
		applyRows: cleanupRows([]any{int64(5), int64(2), zones, labels}),
		clearRows: cleanupRows([]any{int64(3)}),
	}

	res, err := ApplyDelete(context.Background(), pool, b)
	if err != nil {
		t.Fatal(err)
	}
	if res != (DeleteResult{Deleted: 5, NotFound: 2}) {
		t.Errorf("the counters = %+v, want {5 2}", res)
	}

	// The order: begin → the application → the bit clearing → commit →
	// the cleanup.
	want := []string{"begin", "query:" + applyDeleteSQL, "query:" + applyClearRenderSQL, "commit", "exec:" + clearStageSQL}
	if !reflect.DeepEqual(pool.ops, want) {
		t.Fatalf("the call order = %v, want %v", pool.ops, want)
	}
	if !reflect.DeepEqual(pool.args[0], wantApplyArgs(b)) {
		t.Errorf("the apply arguments = %#v, want %#v", pool.args[0], wantApplyArgs(b))
	}
	if !reflect.DeepEqual(pool.args[1], []any{zones, labels}) {
		t.Errorf("the bit-clear arguments = %#v, want [%v %v]", pool.args[1], zones, labels)
	}
	if !reflect.DeepEqual(pool.args[2], []any{b.ID[:]}) {
		t.Errorf("the cleanup arguments = %#v, want the batch identifier", pool.args[2])
	}
	if pool.commits != 1 || pool.rollbacks != 0 {
		t.Errorf("commit=%d rollback=%d, want 1/0", pool.commits, pool.rollbacks)
	}
}

// TestApplyDeleteNoRenderRows — a file without flagged domains: the
// second pair (the bit clearing × the counter) does not run at all —
// the file does not touch the counter row (the single serialization
// point of the flagging transactions, record.go; the HAVING precedent
// of T-186).
func TestApplyDeleteNoRenderRows(t *testing.T) {
	pool := &applyDeletePool{
		applyRows: cleanupRows([]any{int64(4), int64(1), []int16{}, []string{}}),
	}

	res, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t))
	if err != nil {
		t.Fatal(err)
	}
	if res != (DeleteResult{Deleted: 4, NotFound: 1}) {
		t.Errorf("the counters = %+v, want {4 1}", res)
	}
	want := []string{"begin", "query:" + applyDeleteSQL, "commit", "exec:" + clearStageSQL}
	if !reflect.DeepEqual(pool.ops, want) {
		t.Fatalf("the call order = %v, want %v (no applyClearRenderSQL)", pool.ops, want)
	}
}

// TestApplyDeleteSQLContract — the exact application text: the status
// move by the (zone_id, label) key over the own batch's rows with the
// "not yet deleted" guard (T-178: the zone counter deltas decrement
// only the rows actually moved — a repeated batch application is
// idempotent for the counters too, spec 3.4; the report keeps the
// previous semantics: an already deleted domain counts as a "deleted"
// row); the move statement does NOT touch the render_pending bit
// (T-269: the had_render snapshot delta double-decremented the counter
// on a concurrent clearing by the rework pass — the 2026-11-08
// incident); the "the bit was set at the moment of the move" fact is
// carried by the RETURNING column had_render (the update's base
// version) and the render_zones/render_labels arrays; the zone counter
// deltas — by the check_state of the moved rows; the domain stays in
// the DB (no DELETE); the counters — over the staging rows.
func TestApplyDeleteSQLContract(t *testing.T) {
	want := `WITH pre AS MATERIALIZED (
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
	RETURNING d.zone_id, d.label, d.check_state, (d.flags & 2) <> 0 AS had_render
), zc AS (
	UPDATE zones AS z
	SET domains_total = z.domains_total - c.n_total,
	    domains_pending = z.domains_pending - c.n_pending,
	    domains_queued = z.domains_queued - c.n_queued,
	    domains_checked = z.domains_checked - c.n_checked
	FROM (SELECT zone_id,
	             count(*) AS n_total,
	             count(*) FILTER (WHERE check_state = 0) AS n_pending,
	             count(*) FILTER (WHERE check_state = 1) AS n_queued,
	             count(*) FILTER (WHERE check_state = 2) AS n_checked
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
	if applyDeleteSQL != want {
		t.Errorf("applyDeleteSQL = %q, want an exact match", applyDeleteSQL)
	}

	// The staging table — from StageTable, like the COPY and the batch
	// cleanup.
	if !strings.Contains(applyDeleteSQL, `FROM `+StageTable.Sanitize()+` AS s`) {
		t.Errorf("the SQL = %q, want staging addressed via StageTable", applyDeleteSQL)
	}
	// Only the status move: the domain stays in the DB (spec 13.1); the
	// results and content are cleaned by T-020 — they are not here. The
	// move statement does not clear the render_pending bit (T-269) —
	// only status is in SET.
	if !strings.Contains(applyDeleteSQL, "UPDATE domains") || strings.Contains(applyDeleteSQL, "DELETE FROM domains") {
		t.Errorf("the SQL = %q, want only an UPDATE of domains", applyDeleteSQL)
	}
	if strings.Contains(applyDeleteSQL, "SET flags") || strings.Contains(applyDeleteSQL, "CASE WHEN") {
		t.Errorf("the SQL = %q, want a status change WITHOUT touching flags (the bit clear is applyClearRenderSQL, T-269)", applyDeleteSQL)
	}
	// The status guard pins the move for the deltas (T-178): the
	// statement does not change check_state — RETURNING yields the old
	// value (incremental-counters spec §6.1.2).
	if !strings.Contains(applyDeleteSQL, "AND d.status <> $2") {
		t.Errorf("the SQL = %q, want a «not yet deleted» guard for the counter deltas", applyDeleteSQL)
	}
	// The report: a domain already deleted before the statement counts
	// as "deleted"
	// (EXISTS over domains with the old status — the statement snapshot).
	if !strings.Contains(applyDeleteSQL, "OR EXISTS") {
		t.Errorf("the SQL = %q, want an «already deleted» alternative in the deleted counter", applyDeleteSQL)
	}
	// The bit row fact — by the RETURNING column (the base version, not
	// the pre snapshot) and the parallel arrays for the second pair; the
	// ORDER BY in both array_agg is MANDATORY — the key pairing contract
	// (the len check in Go does not catch a permutation, the aggregate
	// order is unspecified without it — the T-269 review, P2-3).
	if !strings.Contains(applyDeleteSQL, "(d.flags & 2) <> 0 AS had_render") ||
		!strings.Contains(applyDeleteSQL, "array_agg(m.zone_id ORDER BY m.zone_id, m.label)") ||
		!strings.Contains(applyDeleteSQL, "array_agg(m.label ORDER BY m.zone_id, m.label)") {
		t.Errorf("the SQL = %q, want had_render by the base version and consistently ordered key arrays", applyDeleteSQL)
	}
}

// TestApplyClearRenderSQLContract — the exact text of the second pair:
// the bit clearing by the GUARD of the current version (the
// "snapshot → lock" window of the 2026-11-08 incident is closed: a
// concurrent clearing yields 0 rows, the counter is not touched —
// mirroring D7); the decrement — by the fact of the cleared rows (the
// count from RETURNING, runbooks §6); the counter row is not touched by
// files without flagged domains (the guard count > 0); the RETURNING —
// the row presence check (migration 000015).
func TestApplyClearRenderSQLContract(t *testing.T) {
	want := `WITH cleared AS (
	UPDATE domains AS d
	SET flags = (d.flags & -3)::int2
	FROM unnest($1::int2[], $2::text[]) AS k(zone_id, label)
	WHERE d.zone_id = k.zone_id
	  AND d.label = k.label
	  AND (d.flags & 2) <> 0
	RETURNING 1
)
UPDATE crawler_counters
SET value = value - (SELECT count(*) FROM cleared)
WHERE name = 'render_pending' AND (SELECT count(*) FROM cleared) > 0
RETURNING value`
	if applyClearRenderSQL != want {
		t.Errorf("applyClearRenderSQL = %q, want an exact match", applyClearRenderSQL)
	}
	// The bit guard — over the row's current version (the
	// renderClearPendingSQL style of httpworker: RowsAffected 0 — no
	// transition, the counter is not touched).
	if !strings.Contains(applyClearRenderSQL, "AND (d.flags & 2) <> 0") {
		t.Errorf("the SQL = %q, want a guard of the current version's bit", applyClearRenderSQL)
	}
	// The delta — only by the rows actually cleared (the count from
	// RETURNING), not by a prepared number.
	if !strings.Contains(applyClearRenderSQL, "value - (SELECT count(*) FROM cleared)") {
		t.Errorf("the SQL = %q, want a decrement by the actually cleared", applyClearRenderSQL)
	}
}

// TestApplyDeleteEmptyBatch — a batch without rows (a file of only
// empty rows/comments): a no-op with zero counters, the cleanup runs.
func TestApplyDeleteEmptyBatch(t *testing.T) {
	pool := &applyDeletePool{
		applyRows: cleanupRows([]any{int64(0), int64(0), []int16{}, []string{}}),
	}

	res, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t))
	if err != nil {
		t.Fatal(err)
	}
	if res != (DeleteResult{}) {
		t.Errorf("the counters = %+v, want zero", res)
	}
	if len(pool.ops) != 4 {
		t.Errorf("calls = %d (%v), want 4 — no second pair", len(pool.ops), pool.ops)
	}
	if pool.commits != 1 {
		t.Errorf("commit = %d, want 1 (the transaction runs even when empty)", pool.commits)
	}
}

// TestApplyDeleteIdempotentRepeat — re-applying after the cleanup is a
// no-op: the batch's staging is empty, the counters are zero, no errors
// (spec 3.4).
func TestApplyDeleteIdempotentRepeat(t *testing.T) {
	b := validDeleteBatch(t)
	pool := &applyDeletePool{
		applyRows: cleanupRows([]any{int64(3), int64(0), []int16{}, []string{}}),
	}
	// The second application — the same answer (the batch is not modeled
	// in the fake's staging: the retry protocol matters, not the state).
	pool2 := &applyDeletePool{
		applyRows: cleanupRows([]any{int64(0), int64(0), []int16{}, []string{}}),
	}

	first, err := ApplyDelete(context.Background(), pool, b)
	if err != nil {
		t.Fatal(err)
	}
	if first != (DeleteResult{Deleted: 3, NotFound: 0}) {
		t.Fatalf("the first apply = %+v, want {3 0}", first)
	}

	second, err := ApplyDelete(context.Background(), pool2, b)
	if err != nil {
		t.Fatalf("the repeated apply: %v", err)
	}
	if second != (DeleteResult{}) {
		t.Errorf("the repeat = %+v, want zero counters (a no-op)", second)
	}
	if len(pool2.ops) != 4 || strings.Join(pool2.ops, ";") != strings.Join([]string{"begin", "query:" + applyDeleteSQL, "commit", "exec:" + clearStageSQL}, ";") {
		t.Errorf("the repeat: the calls = %v, want no second pair", pool2.ops)
	}
}

// TestApplyDeleteValidation — the batch's program errors are fought off
// before the DB: a batch without an identifier and a non-delete batch
// (otherwise the cleanup at the end of ApplyDelete would silently
// destroy an unapplied add batch).
func TestApplyDeleteValidation(t *testing.T) {
	id, err := NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		b    Batch
	}{
		{"a zero identifier", Batch{ZoneID: 1, Action: model.ActionDelete}},
		{"an add batch", Batch{ID: id, ZoneID: 1, Action: model.ActionAdd}},
		{"a zero action code", Batch{ID: id, ZoneID: 1, Action: 0}},
		{"an unknown action code", Batch{ID: id, ZoneID: 1, Action: model.Action(9)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &applyDeletePool{}
			if _, err := ApplyDelete(context.Background(), pool, tc.b); err == nil {
				t.Fatal("a validation error is expected")
			}
			if len(pool.ops) != 0 {
				t.Errorf("an invalid batch reached the DB: %v", pool.ops)
			}
		})
	}
}

// TestApplyDeleteApplyError — an application error (including the ctx
// cancellation) interrupts the transaction before the pair and the
// cleanup: the batch stays in staging for the retry (spec 3.4).
func TestApplyDeleteApplyError(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"a context cancellation", context.Canceled},
		{"a query error", errors.New("connection refused")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &applyDeletePool{
				applyRows: cleanupRows([]any{int64(3), int64(0), []int16{}, []string{}}),
				applyErr:  tc.err,
			}

			_, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t))
			if err == nil || !errors.Is(err, tc.err) {
				t.Fatalf("err = %v, want wrapped %v", err, tc.err)
			}
			if !strings.Contains(err.Error(), "applying delete") {
				t.Errorf("err = %v, want the «applying delete» context", err)
			}
			if pool.commits != 0 || pool.rollbacks != 1 {
				t.Errorf("commit=%d rollback=%d, want 0/1 (a rollback)", pool.commits, pool.rollbacks)
			}
			if len(pool.ops) != 2 { // begin + query:<the application>
				t.Errorf("the calls = %v, want the apply without the pair and the cleanup", pool.ops)
			}
		})
	}
}

// TestApplyDeleteClearRenderError — the second pair's errors: a
// statement failure and a missing counter row roll back the whole
// application (a silent desync "the bit cleared — the counter not
// moved" is unacceptable, the renderBumpDebtCounter philosophy).
func TestApplyDeleteClearRenderError(t *testing.T) {
	t.Run("an operator failure", func(t *testing.T) {
		pool := &applyDeletePool{
			applyRows:    cleanupRows([]any{int64(2), int64(0), []int16{7}, []string{"a"}}),
			clearRendErr: errors.New("deadlock detected"),
		}
		_, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t))
		if err == nil || !strings.Contains(err.Error(), "clearing the render_pending bit") {
			t.Fatalf("err = %v, want the second pair's error", err)
		}
		if pool.commits != 0 || pool.rollbacks != 1 {
			t.Errorf("commit=%d rollback=%d, want 0/1 (a rollback)", pool.commits, pool.rollbacks)
		}
		if len(pool.ops) > 3 {
			t.Errorf("the calls = %v, want no commit/cleanup", pool.ops)
		}
	})
	t.Run("the counter row is missing", func(t *testing.T) {
		pool := &applyDeletePool{
			applyRows: cleanupRows([]any{int64(2), int64(0), []int16{7}, []string{"a"}}),
			clearRows: cleanupRows(), // the RETURNING is empty — the row is missing
		}
		_, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t))
		if err == nil || !strings.Contains(err.Error(), "'render_pending' is missing") {
			t.Fatalf("err = %v, want the missing counter-row error", err)
		}
		if pool.commits != 0 || pool.rollbacks != 1 {
			t.Errorf("commit=%d rollback=%d, want 0/1 (a rollback)", pool.commits, pool.rollbacks)
		}
	})
}

// TestApplyDeleteBadCounters — protection against the query contract
// falling apart: a missing counters row, an extra column, a mismatch of
// the bit arrays — an error, a rollback, no cleanup.
func TestApplyDeleteBadCounters(t *testing.T) {
	cases := []struct {
		name     string
		result   *cleanupFakeRows
		want     string
		rollback bool
	}{
		{"no row", cleanupRows(), "the counters were not returned", true},
		{"an extra column", cleanupRows([]any{int64(1), int64(2), []int16{}, []string{}, int64(9)}), "applying delete", true},
		{"the arrays diverged", cleanupRows([]any{int64(1), int64(0), []int16{7, 8}, []string{"a"}}), "the bit arrays diverged", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &applyDeletePool{applyRows: tc.result}

			_, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want an error with %q", err, tc.want)
			}
			if tc.rollback && pool.rollbacks != 1 {
				t.Errorf("rollback = %d, want 1", pool.rollbacks)
			}
			if pool.commits != 0 {
				t.Errorf("commit = %d, want 0", pool.commits)
			}
		})
	}
}

// TestApplyDeleteTxErrors — transaction errors (begin/commit) interrupt
// the application; the batch cleanup does not run.
func TestApplyDeleteTxErrors(t *testing.T) {
	t.Run("begin", func(t *testing.T) {
		pool := &applyDeletePool{beginErr: errors.New("pool closed")}
		if _, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t)); err == nil || !strings.Contains(err.Error(), "transaction") {
			t.Fatalf("err = %v, want the transaction error", err)
		}
		if len(pool.ops) != 0 {
			t.Errorf("the calls = %v, want empty", pool.ops)
		}
	})
	t.Run("commit", func(t *testing.T) {
		pool := &applyDeletePool{
			applyRows: cleanupRows([]any{int64(1), int64(0), []int16{}, []string{}}),
			commitErr: errors.New("connection reset"),
		}
		if _, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t)); err == nil || !strings.Contains(err.Error(), "commit") {
			t.Fatalf("err = %v, want the commit error", err)
		}
		if len(pool.ops) != 3 { // begin, the application, commit (the failing one)
			t.Errorf("the calls = %v, want no batch cleanup", pool.ops)
		}
	})
}

// TestApplyDeleteClearError — a cleanup error does not undo the applied
// delete: the counters are returned together with the error, a retry
// will finish the cleanup.
func TestApplyDeleteClearError(t *testing.T) {
	pool := &applyDeletePool{
		applyRows: cleanupRows([]any{int64(4), int64(1), []int16{}, []string{}}),
		clearErr:  errors.New("deadlock detected"),
	}

	res, err := ApplyDelete(context.Background(), pool, validDeleteBatch(t))
	if err == nil || !strings.Contains(err.Error(), "deadlock detected") {
		t.Fatalf("err = %v, want the cleanup error", err)
	}
	if !strings.Contains(err.Error(), "ingest: cleaning the batch") {
		t.Errorf("err = %v, want the batch cleanup context", err)
	}
	if res != (DeleteResult{Deleted: 4, NotFound: 1}) {
		t.Errorf("the counters = %+v, want {4 1}: the apply did happen", res)
	}
	if pool.commits != 1 {
		t.Errorf("commit = %d, want 1 (the apply is committed before the cleanup)", pool.commits)
	}
}

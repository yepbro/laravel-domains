// Unit tests of applying an add batch (T-018, spec 5.1; the chunking
// T-205) without a DB (stage_test.go / cleanup_test.go style): exact SQL
// texts and arguments, the cursor movement in chunks over the staging PK
// (an all-conflict chunk is closed too), the accumulation of zone deltas
// and a single final UPDATE, the transaction (Begin/Commit, a rollback
// on a step failure), the counters, the edges (a batch smaller than a
// chunk, an empty batch), repeated application (idempotency, spec 3.4),
// step errors, the counter invariant, the ctx cancellation. Real
// application — the ingest integration tests (T-023).
package ingest

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"crawler/internal/idn"
	"crawler/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// --- fake pool with transactions ------------------------------------------

// addStageRow — a batch row in the fake's staging model.
type addStageRow struct {
	lineNo int64
	zoneID int16
}

// applyAddPool — an ApplyDB for ApplyAdd without a DB: models the batch
// rows in staging (staged) and answers the chunk bound (addChunkBoundSQL)
// and the insert RETURNING (insertAddSQL) over the cursor range — rows
// with line_no >= conflictFrom count as conflicts (0 — no conflicts,
// everything is inserted); revive/count are prepared numbers. Step
// errors are set by named fields; the ops protocol records the
// application order. Like real pgx, it refuses to work with a cancelled
// context.
type applyAddPool struct {
	ops  []string // the order: "begin", "commit", "query:<sql>", "exec:<sql>"
	args [][]any  // the operation arguments in call order

	staged       []addStageRow // the batch rows (the staging model, line_no order)
	conflictFrom int64         // the line_no from which the rows already exist (a conflict)

	revived int64 // the reviveAddSQL answer
	uniques int64 // the countAddSQL answer

	beginErr     error
	boundErr     error
	insertErr    error
	zoneDeltaErr error
	reviveErr    error
	countErr     error
	clearErr     error
	commitErr    error

	begins     int
	commits    int
	rollbacks  int
	txArgs     [][]any // the transaction operation arguments (zoneDelta/clear)
	zoneCall   int     // the index of the final zone UPDATE arguments (the last call)
	zoneCalled bool
}

var _ ApplyDB = (*applyAddPool)(nil)

func (p *applyAddPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("applyAddPool: CopyFrom must not be called")
}

// Exec and Query of the pool are not needed by the application
// (everything is in the transaction): fail if something is called past
// Begin.
func (p *applyAddPool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("applyAddPool: Exec outside a transaction must not be called")
}

func (p *applyAddPool) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("applyAddPool: Query outside a transaction must not be called")
}

// stageRange — the batch rows of the staging model with line_no in
// (after, to]; to >= 1<<62 — no upper bound (the chunk bound).
func (p *applyAddPool) stageRange(after, to int64) []addStageRow {
	var res []addStageRow
	for _, r := range p.staged {
		if r.lineNo > after && r.lineNo <= to {
			res = append(res, r)
		}
	}
	return res
}

func (p *applyAddPool) Begin(ctx context.Context) (pgx.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	p.begins++
	p.ops = append(p.ops, "begin")
	return &applyAddTx{pool: p}, nil
}

// applyAddTx — the fake pool's transaction: all operations go through
// the pool (a shared protocol), errors — by the pool's named fields.
type applyAddTx struct {
	pgx.Tx // the application does not use the rest of the pgx.Tx methods

	pool      *applyAddPool
	committed bool
}

func (t *applyAddTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p := t.pool
	p.ops = append(p.ops, "query:"+sql)
	p.args = append(p.args, args)
	switch sql {
	case addChunkBoundSQL:
		if p.boundErr != nil {
			return nil, p.boundErr
		}
		after, limit := args[2].(int64), args[3].(int)
		var bound int64
		rows := p.stageRange(after, 1<<62)
		if n := len(rows); n > limit {
			rows = rows[:limit]
		}
		if len(rows) > 0 {
			bound = rows[len(rows)-1].lineNo
		}
		return cleanupRows([]any{bound}), nil
	case insertAddSQL:
		if p.insertErr != nil {
			return nil, p.insertErr
		}
		from, to := args[2].(int64), args[3].(int64)
		out := cleanupRows()
		for _, r := range p.stageRange(from, to) {
			if p.conflictFrom > 0 && r.lineNo >= p.conflictFrom {
				continue // an existing domain: ON CONFLICT DO NOTHING
			}
			out.rows = append(out.rows, []any{r.zoneID})
		}
		return out, nil
	case reviveAddSQL:
		if p.reviveErr != nil {
			return nil, p.reviveErr
		}
		return cleanupRows([]any{p.revived}), nil
	case countAddSQL:
		if p.countErr != nil {
			return nil, p.countErr
		}
		return cleanupRows([]any{p.uniques}), nil
	}
	return nil, errors.New("applyAddPool: unexpected query: " + sql)
}

func (t *applyAddTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	p := t.pool
	p.ops = append(p.ops, "exec:"+sql)
	p.txArgs = append(p.txArgs, args)
	switch sql {
	case zoneDeltaAddSQL:
		p.zoneCalled = true
		p.zoneCall = len(p.txArgs) - 1
		if p.zoneDeltaErr != nil {
			return pgconn.CommandTag{}, p.zoneDeltaErr
		}
		return pgconn.NewCommandTag("UPDATE 1"), nil
	case clearStageSQL:
		if p.clearErr != nil {
			return pgconn.CommandTag{}, p.clearErr
		}
		return pgconn.NewCommandTag("DELETE 0"), nil
	}
	return pgconn.CommandTag{}, errors.New("applyAddPool: unexpected Exec: " + sql)
}

func (t *applyAddTx) Commit(context.Context) error {
	p := t.pool
	if p.commitErr != nil {
		return p.commitErr
	}
	p.commits++
	t.committed = true
	p.ops = append(p.ops, "commit")
	return nil
}

func (t *applyAddTx) Rollback(context.Context) error {
	// After Commit the rollback is a no-op (like real pgx's ErrTxClosed).
	if !t.committed {
		t.pool.rollbacks++
	}
	return nil
}

// stageLines — the batch model: rows 1..n in one zone.
func stageLines(n int, zone int16) []addStageRow {
	rows := make([]addStageRow, n)
	for i := range rows {
		rows[i] = addStageRow{lineNo: int64(i + 1), zoneID: zone}
	}
	return rows
}

// applyAddOps — the expected order of the application operations with
// inserts: transaction → chunks (the bound, the insert) until exhausted →
// the zone deltas → the revive → the count → the cleanup → the commit.
func applyAddOps(chunks int) []string {
	ops := []string{"begin"}
	for i := 0; i < chunks; i++ {
		ops = append(ops, "query:"+addChunkBoundSQL, "query:"+insertAddSQL)
	}
	ops = append(ops,
		"query:"+addChunkBoundSQL, // the batch is exhausted
		"exec:"+zoneDeltaAddSQL,
		"query:"+reviveAddSQL,
		"query:"+countAddSQL,
		"exec:"+clearStageSQL,
		"commit",
	)
	return ops
}

// ---------------------------------------------------------------- scenarios

func TestApplyAdd(t *testing.T) {
	b := validBatch(t) // zone 7, add — as after EnsureZone and Stage
	pool := &applyAddPool{
		staged:       stageLines(10, 7), // the batch in one chunk (10 < 50,000)
		conflictFrom: 6,                 // rows 6–10 already exist
		revived:      3,
		uniques:      10,
	}

	res, err := ApplyAdd(context.Background(), pool, b, DefaultBatchSize)
	if err != nil {
		t.Fatal(err)
	}
	if res.Inserted != 5 || res.Revived != 3 || res.Existing != 2 {
		t.Errorf("the result = %+v, want Inserted 5, Revived 3, Existing 2 (10 uniques)", res)
	}
	if res.Total() != 10 {
		t.Errorf("Total() = %d, want 10", res.Total())
	}

	// The order: transaction → the chunk bound/insert → the bound
	// (empty) → the zone deltas → the revive → the count → the cleanup →
	// the commit.
	if got, want := strings.Join(pool.ops, "\n"), strings.Join(applyAddOps(1), "\n"); got != want {
		t.Errorf("the operation order:\n%s\nwant:\n%s", got, want)
	}
	if pool.begins != 1 || pool.commits != 1 || pool.rollbacks != 0 {
		t.Errorf("transactions: begin %d, commit %d, rollback %d, want 1/1/0", pool.begins, pool.commits, pool.rollbacks)
	}

	// The chunk addresses its own batch and the cursor range from zero
	// to the bound.
	wantArgs := []any{[]byte(b.ID[:]), int16(model.ActionAdd), int64(0), int64(10)}
	if !reflect.DeepEqual(pool.args[1], wantArgs) {
		t.Errorf("the insert arguments = %v, want [batch_id, action=%d, line_no > 0, line_no <= 10]", pool.args[1], int16(model.ActionAdd))
	}
	// The zone deltas — one final statement over the accumulated
	// zone_id→n.
	if !pool.zoneCalled {
		t.Fatal("the final zone-delta UPDATE did not run")
	}
	if !reflect.DeepEqual(pool.txArgs[pool.zoneCall], []any{[]int16{7}, []int64{5}}) {
		t.Errorf("the zone-delta arguments = %v, want zone [7] × 5", pool.txArgs[pool.zoneCall])
	}
}

// TestApplyAdd_Chunks — a batch bigger than a chunk (T-205): 12 rows in
// chunks of 5 — the ranges (0,5], (5,10], (10,12]; Inserted — the sum of
// the chunk inserts, the zone deltas — one statement over the
// accumulations of all chunks.
func TestApplyAdd_Chunks(t *testing.T) {
	b := validBatch(t)
	// Two zones: rows 1–6 in zone 7, 7–12 in zone 9.
	staged := stageLines(6, 7)
	staged = append(staged, stageLines(6, 9)...)
	for i := 6; i < len(staged); i++ {
		staged[i].lineNo = int64(i + 1)
	}
	pool := &applyAddPool{staged: staged, uniques: 12}

	res, err := ApplyAdd(context.Background(), pool, b, 5)
	if err != nil {
		t.Fatal(err)
	}
	if res.Inserted != 12 || res.Revived != 0 || res.Existing != 0 {
		t.Errorf("the result = %+v, want Inserted 12 (the chunk sum), Revived/Existing 0", res)
	}
	if got, want := strings.Join(pool.ops, "\n"), strings.Join(applyAddOps(3), "\n"); got != want {
		t.Errorf("the operation order:\n%s\nwant:\n%s", got, want)
	}

	// The chunk ranges continue one another over the staging PK.
	wantRanges := [][2]int64{{0, 5}, {5, 10}, {10, 12}}
	for i, want := range wantRanges {
		args := pool.args[1+2*i] // inserts alternate with bounds
		got := [2]int64{args[2].(int64), args[3].(int64)}
		if got != want {
			t.Errorf("chunk %d: the (cursor, bound] range = %v, want %v", i+1, got, want)
		}
	}

	// The deltas of the two zones are accumulated by all the chunks and
	// applied by one statement: zone 7 × 6, zone 9 × 6, the zone_id order
	// is deterministic.
	if !pool.zoneCalled {
		t.Fatal("the final zone-delta UPDATE did not run")
	}
	if !reflect.DeepEqual(pool.txArgs[pool.zoneCall], []any{[]int16{7, 9}, []int64{6, 6}}) {
		t.Errorf("the zone-delta arguments = %v, want zones [7 9] × [6 6]", pool.txArgs[pool.zoneCall])
	}
}

// TestApplyAdd_ConflictChunkAdvances — a chunk of nothing but conflicts
// (ON CONFLICT DO NOTHING skipped everything) must close the cursor over
// the staging range, not over the inserted rows — otherwise applying a
// batch of all-existing domains would loop forever (T-205).
func TestApplyAdd_ConflictChunkAdvances(t *testing.T) {
	b := validBatch(t)
	pool := &applyAddPool{
		staged:       stageLines(8, 7),
		conflictFrom: 1, // all the batch rows already exist
		uniques:      8,
	}

	res, err := ApplyAdd(context.Background(), pool, b, 3)
	if err != nil {
		t.Fatal(err)
	}
	if res.Inserted != 0 || res.Revived != 0 || res.Existing != 8 {
		t.Errorf("the result = %+v, want a conflicting batch: Inserted/Revived 0, Existing 8", res)
	}
	// Three chunks over the ranges (0,3], (3,6], (6,8] — the cursor
	// reached the end; no inserts — the zone deltas are not applied at
	// all.
	want := append([]string{"begin"},
		"query:"+addChunkBoundSQL, "query:"+insertAddSQL,
		"query:"+addChunkBoundSQL, "query:"+insertAddSQL,
		"query:"+addChunkBoundSQL, "query:"+insertAddSQL,
		"query:"+addChunkBoundSQL, // the batch is exhausted
		"query:"+reviveAddSQL,
		"query:"+countAddSQL,
		"exec:"+clearStageSQL,
		"commit",
	)
	if got := strings.Join(pool.ops, "\n"); got != strings.Join(want, "\n") {
		t.Errorf("the operation order:\n%s\nwant:\n%s", got, strings.Join(want, "\n"))
	}
	if pool.zoneCalled {
		t.Error("the final zone-delta UPDATE ran without inserts, want a no-op")
	}
}

// TestApplyAdd_EmptyBatch — an empty batch (or an already applied one,
// staging empty): the data is not changed, the result is zero; the
// transaction is opened, no insert, the zone deltas are not applied.
func TestApplyAdd_EmptyBatch(t *testing.T) {
	pool := &applyAddPool{}

	res, err := ApplyAdd(context.Background(), pool, validBatch(t), DefaultBatchSize)
	if err != nil {
		t.Fatal(err)
	}
	if res != (AddResult{}) {
		t.Errorf("the result = %+v, want zero", res)
	}
	want := []string{
		"begin",
		"query:" + addChunkBoundSQL, // the batch is exhausted right away
		"query:" + reviveAddSQL,
		"query:" + countAddSQL,
		"exec:" + clearStageSQL,
		"commit",
	}
	if got := strings.Join(pool.ops, "\n"); got != strings.Join(want, "\n") {
		t.Errorf("the operation order:\n%s\nwant:\n%s", got, strings.Join(want, "\n"))
	}
	if pool.zoneCalled {
		t.Error("an empty batch does not touch the zone counters")
	}
}

// TestApplyAdd_Reapply — re-applying the same batch (spec 3.4): after
// the first attempt all the domains exist — re-loading the same rows
// conflicts, no inserts or deltas, the data converges to the same result
// (the counters of the repeated run reflect the current state).
func TestApplyAdd_Reapply(t *testing.T) {
	b := validBatch(t)
	pool := &applyAddPool{staged: stageLines(10, 7), uniques: 10}

	first, err := ApplyAdd(context.Background(), pool, b, 4)
	if err != nil {
		t.Fatal(err)
	}
	if first.Inserted != 10 || first.Existing != 0 {
		t.Fatalf("the first apply = %+v, want Inserted 10", first)
	}

	// The batch is loaded again with the same batch_id (Stage wipes its
	// own batch), the domains are already in the DB — all conflicts.
	pool.staged = stageLines(10, 7)
	pool.conflictFrom = 1
	second, err := ApplyAdd(context.Background(), pool, b, 4)
	if err != nil {
		t.Fatal(err)
	}
	if second.Inserted != 0 || second.Revived != 0 || second.Existing != 10 {
		t.Errorf("the repeated apply = %+v, want a no-op: Inserted/Revived 0, Existing 10", second)
	}
}

func TestApplyAdd_Validation(t *testing.T) {
	id, err := NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name      string
		batch     Batch
		batchSize int
	}{
		{"a zero identifier", Batch{ZoneID: 1, Action: model.ActionAdd}, DefaultBatchSize},
		{"a delete batch", Batch{ID: id, ZoneID: 1, Action: model.ActionDelete}, DefaultBatchSize},
		{"an unknown action", Batch{ID: id, ZoneID: 1, Action: model.Action(3)}, DefaultBatchSize},
		{"a zero chunk size", Batch{ID: id, ZoneID: 1, Action: model.ActionAdd}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &applyAddPool{}
			if _, err := ApplyAdd(context.Background(), pool, tc.batch, tc.batchSize); err == nil {
				t.Fatal("a validation error is expected")
			}
			if len(pool.ops) != 0 || pool.begins != 0 {
				t.Errorf("an invalid batch reached the DB: %v", pool.ops)
			}
		})
	}
}

// TestApplyAdd_StepErrors — a failure of each step returns a wrapped
// error, the transaction is rolled back (the rows stay in staging, the
// repeated application is safe, spec 3.4), the cleanup and the commit do
// not run.
func TestApplyAdd_StepErrors(t *testing.T) {
	boom := errors.New("connection lost")
	cases := []struct {
		name    string
		set     func(*applyAddPool)
		wantOps int    // calls up to and including the failure
		want    string // a fragment of the expected error
	}{
		{"the transaction failure", func(p *applyAddPool) { p.beginErr = boom }, 0, "transaction"},
		{"the chunk boundary failure", func(p *applyAddPool) { p.boundErr = boom }, 2, "chunk boundary"},
		{"the insert failure", func(p *applyAddPool) { p.insertErr = boom }, 3, "inserting the add batch's new domains"},
		{"the zone deltas failure", func(p *applyAddPool) { p.zoneDeltaErr = boom }, 5, "zone counter deltas"},
		{"the revive failure", func(p *applyAddPool) { p.reviveErr = boom }, 6, "reviving deleted"},
		{"the count failure", func(p *applyAddPool) { p.countErr = boom }, 7, "counting the add batch's domains"},
		{"the cleanup failure", func(p *applyAddPool) { p.clearErr = boom }, 8, "cleaning the batch"},
		{"the commit failure", func(p *applyAddPool) { p.commitErr = boom }, 8, "commit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &applyAddPool{staged: stageLines(10, 7), uniques: 10}
			tc.set(pool)

			_, err := ApplyAdd(context.Background(), pool, validBatch(t), DefaultBatchSize)
			if err == nil || !strings.Contains(err.Error(), tc.want) || !errors.Is(err, boom) {
				t.Fatalf("err = %v, want a wrapped error with %q", err, tc.want)
			}
			if len(pool.ops) != tc.wantOps {
				t.Errorf("DB calls %d (%v), want %d — there is no continuation after a failure", len(pool.ops), pool.ops, tc.wantOps)
			}
			if pool.commits != 0 {
				t.Error("the failing step committed the transaction")
			}
			if pool.begins > 0 && pool.rollbacks != 1 {
				t.Errorf("rollbacks %d, want 1 (the open transaction is rolled back)", pool.rollbacks)
			}
			if strings.Contains(strings.Join(pool.ops, "\n"), "commit") {
				t.Error("the apply failure ended with a commit")
			}
		})
	}
}

// TestApplyAdd_CountInvariant — the count is less than the inserted plus
// the revived: the counters diverge, the application is declared failed,
// the transaction is rolled back.
func TestApplyAdd_CountInvariant(t *testing.T) {
	pool := &applyAddPool{
		staged:  stageLines(5, 7),
		revived: 3,
		uniques: 1, // 1 < 5 + 3
	}

	_, err := ApplyAdd(context.Background(), pool, validBatch(t), DefaultBatchSize)
	if err == nil || !strings.Contains(err.Error(), "the counters diverge") {
		t.Fatalf("err = %v, want the counters invariant", err)
	}
	if pool.commits != 0 || pool.rollbacks != 1 {
		t.Errorf("the transaction: commit %d, rollback %d, want 0/1", pool.commits, pool.rollbacks)
	}
}

// TestApplyAdd_ContextCanceled — a cancelled context fails at the
// transaction start (like real pgx), the batch is not applied.
func TestApplyAdd_ContextCanceled(t *testing.T) {
	pool := &applyAddPool{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := ApplyAdd(ctx, pool, validBatch(t), DefaultBatchSize)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if len(pool.ops) != 0 {
		t.Errorf("there must be no DB calls, got %v", pool.ops)
	}
}

// ---------------------------------------------------------------- the SQL contract

const (
	wantAddChunkBoundSQL = "SELECT COALESCE(max(t.line_no), 0) FROM (" +
		"SELECT s.line_no FROM ingest_stage AS s " +
		"WHERE s.batch_id = $1 AND s.action = $2 AND s.line_no > $3 " +
		"ORDER BY s.line_no LIMIT $4) AS t"

	wantAddInsertSQL = "INSERT INTO domains (zone_id, label, flags) " +
		"SELECT s.zone_id, s.label, CASE WHEN s.label ILIKE 'xn--%' OR s.label ILIKE '%.xn--%' THEN 1 ELSE 0 END " +
		"FROM ingest_stage AS s " +
		"WHERE s.batch_id = $1 AND s.action = $2 AND s.line_no > $3 AND s.line_no <= $4 " +
		"ON CONFLICT DO NOTHING RETURNING zone_id"

	wantAddZoneDeltaSQL = "UPDATE zones AS z " +
		"SET domains_total = z.domains_total + d.n, " +
		"domains_pending = z.domains_pending + d.n " +
		"FROM unnest($1::int2[], $2::int8[]) AS d(zone_id, n) " +
		"WHERE z.id = d.zone_id"

	wantAddReviveSQL = "WITH rev AS (UPDATE domains AS d " +
		"SET status = 0, check_state = 0, queued_at = NULL " +
		"FROM ingest_stage AS s " +
		"WHERE s.batch_id = $1 AND s.action = $2 " +
		"AND d.zone_id = s.zone_id AND d.label = s.label " +
		"AND d.status = 6 RETURNING d.zone_id), " +
		"zc AS (UPDATE zones AS z " +
		"SET domains_total = z.domains_total + c.n, " +
		"domains_pending = z.domains_pending + c.n " +
		"FROM (SELECT zone_id, count(*) AS n FROM rev GROUP BY zone_id) AS c " +
		"WHERE z.id = c.zone_id) " +
		"SELECT count(*) FROM rev"

	wantAddCountSQL = "SELECT count(*) FROM (" +
		"SELECT DISTINCT s.zone_id, s.label FROM ingest_stage AS s " +
		"WHERE s.batch_id = $1 AND s.action = $2) AS t"
)

// TestApplyAdd_SQLContract — the exact SQL texts of the application and
// the tie between the literals and the model constants: the chunk cursor
// relies on the staging PK (batch_id, line_no, migration 000004), ON
// CONFLICT — on the unique (zone_id, label) of the domains partitions
// (migration 000001).
func TestApplyAdd_SQLContract(t *testing.T) {
	if addChunkBoundSQL != wantAddChunkBoundSQL {
		t.Errorf("addChunkBoundSQL = %q,\nwant %q", addChunkBoundSQL, wantAddChunkBoundSQL)
	}
	if insertAddSQL != wantAddInsertSQL {
		t.Errorf("insertAddSQL = %q,\nwant %q", insertAddSQL, wantAddInsertSQL)
	}
	if zoneDeltaAddSQL != wantAddZoneDeltaSQL {
		t.Errorf("zoneDeltaAddSQL = %q,\nwant %q", zoneDeltaAddSQL, wantAddZoneDeltaSQL)
	}
	if reviveAddSQL != wantAddReviveSQL {
		t.Errorf("reviveAddSQL = %q,\nwant %q", reviveAddSQL, wantAddReviveSQL)
	}
	if countAddSQL != wantAddCountSQL {
		t.Errorf("countAddSQL = %q,\nwant %q", countAddSQL, wantAddCountSQL)
	}

	// The literals in the texts above must follow the model codes
	// (spec 6.2, 5.2 item 9): when the codes change the texts are rebuilt
	// by themselves — the test catches a desync of the hand-written
	// literal.
	if int16(model.StatusNew) != 0 || int16(model.StatusDeleted) != 6 ||
		int16(model.CheckStatePending) != 0 || int16(model.FlagIDN) != 1 {
		t.Errorf("the model codes changed (new=%d deleted=%d pending=%d is_idn=%d): update the literals in wantAdd*SQL",
			int16(model.StatusNew), int16(model.StatusDeleted), int16(model.CheckStatePending), int16(model.FlagIDN))
	}

	up := readMigration(t, "000001_init.up.sql")
	if !strings.Contains(up, "UNIQUE (zone_id, label)") {
		t.Error("migration 000001 has no UNIQUE (zone_id, label) — ON CONFLICT DO NOTHING has nothing to conflict with (spec 5.1)")
	}
	// The chunk cursor — the staging PK: without it the ORDER BY line_no
	// is not served by an index and the chunk bound would read the whole
	// batch (T-205).
	stage := readMigration(t, "000004_ingest_staging.up.sql")
	if !strings.Contains(stage, "PRIMARY KEY (batch_id, line_no)") {
		t.Error("migration 000004 has no PRIMARY KEY (batch_id, line_no) — the apply-chunk cursor is not served by the key (T-205)")
	}
}

// TestInsertAddIDNConditionEquivalent — the is_idn condition in
// insertAddSQL ('xn--%' OR '%.xn--%') is equivalent to idn.HasXNLabel
// (spec 5.2 item 9) on simple and composite labels (ING-P1-1): "any name
// label starts with xn--".
func TestInsertAddIDNConditionEquivalent(t *testing.T) {
	prefixFold := func(s, p string) bool {
		return len(s) >= len(p) && strings.EqualFold(s[:len(p)], p)
	}
	ilike := func(label string) bool { // the semantics of 'xn--%' OR '%.xn--%'
		return prefixFold(label, "xn--") ||
			strings.Contains(strings.ToLower(label), ".xn--")
	}
	labels := []string{
		"example", "xn--e1afmkfd",
		"shop.xn--p1ai", "xn--p1ai.shop", // diverged before the fix
		"xn--80aairftm.xn--p1ai", "sub.example", "sub.xn--e1afmkfd.example",
		"XN--E1AFMKFD", "sub.XN--e1afmkfd",
	}
	for _, label := range labels {
		if got, want := ilike(label), idn.HasXNLabel(label); got != want {
			t.Errorf("the is_idn(%q) condition = %v, want %v (idn.HasXNLabel)", label, got, want)
		}
	}
}

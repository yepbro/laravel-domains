// Unit tests of the staging loader without a DB (the style of
// internal/db/copy_test.go): batch slicing and the COPY row content, the
// batch cleanup before loading, counters, cancellation between batches,
// source errors and row invariants, the memory cap per batch, the staging
// table contract against the SQL of migration 000004.
// The real loading — the ingest integration tests (T-023).
package ingest

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/migrations"
)

// fakeDB records the order of calls (exec/copy), the rows of each COPY and
// the Exec arguments; when an error is set, it returns it.
type fakeDB struct {
	ops      []string // the order of calls: "exec:<sql>", "copy:<n>"
	execSQL  []string
	execArgs [][]any
	execTag  pgconn.CommandTag // returned by Exec (RowsAffected)
	batches  []int             // rows in each CopyFrom call
	rows     [][]any           // all rows of all batches in order
	copyErr  error
	execErr  error
}

var _ db.DB = (*fakeDB)(nil)

func (f *fakeDB) CopyFrom(_ context.Context, table pgx.Identifier, columns []string, src pgx.CopyFromSource) (int64, error) {
	n := int64(0)
	for src.Next() {
		vals, err := src.Values()
		if err != nil {
			return 0, err
		}
		f.rows = append(f.rows, vals)
		n++
	}
	if err := src.Err(); err != nil {
		return 0, err
	}
	if f.copyErr != nil {
		return 0, f.copyErr
	}
	if len(f.batches) == 0 {
		// the first call: pin down the table and columns contract
		if !slices.Equal(table, StageTable) {
			return 0, fmt.Errorf("COPY into %v, want %v", table, StageTable)
		}
		if !slices.Equal(columns, StageColumns) {
			return 0, fmt.Errorf("the COPY columns %v, want %v", columns, StageColumns)
		}
	}
	f.batches = append(f.batches, int(n))
	f.ops = append(f.ops, fmt.Sprintf("copy:%d", n))
	return n, nil
}

func (f *fakeDB) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if f.execErr != nil {
		return pgconn.CommandTag{}, f.execErr
	}
	f.execSQL = append(f.execSQL, sql)
	f.execArgs = append(f.execArgs, args)
	f.ops = append(f.ops, "exec:"+sql)
	return f.execTag, nil
}

func (f *fakeDB) Query(context.Context, string, ...any) (pgx.Rows, error) { return nil, nil }

// rowsIter — a typed row source with call counting.
type rowsIter struct {
	rows    []Row
	nextIdx int
	onNext  func(i int) // a side effect before yielding row i (cancellation etc.)
}

func (it *rowsIter) next() (Row, error) {
	i := it.nextIdx
	if i >= len(it.rows) {
		return Row{}, io.EOF
	}
	if it.onNext != nil {
		it.onNext(i)
	}
	it.nextIdx++
	return it.rows[i], nil
}

// genRows — n rows example-<i> with line numbers from from.
func genRows(n int) []Row {
	rows := make([]Row, n)
	for i := range rows {
		rows[i] = Row{Label: fmt.Sprintf("example-%d", i), LineNo: int64(i + 1)}
	}
	return rows
}

func validBatch(t *testing.T) Batch {
	t.Helper()
	id, err := NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	return Batch{ID: id, ZoneID: 7, Action: model.ActionAdd}
}

func ints(vs []int) []string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = fmt.Sprint(v)
	}
	return out
}

func TestNewBatchID(t *testing.T) {
	before := time.Now().UnixMicro()
	id, err := NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	if id == (BatchID{}) {
		t.Error("NewBatchID returned a zero identifier")
	}
	other, err := NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	if id == other {
		t.Error("two NewBatchID matched: batch identifiers must be unique")
	}
	// The creation time prefix (T-098): microseconds between the calls.
	got := int64(binary.BigEndian.Uint64(id[:batchTimeLen]))
	after := time.Now().UnixMicro()
	if got < before || got > after {
		t.Errorf("the time in batch_id = %d, want between %d and %d", got, before, after)
	}
}

// TestBatchIDAgeOrdering — the byte order of batch_id matches the creation
// chronology (T-098): a batch older than the purge cutoff falls under it, a
// batch at the cutoff and younger does not. The semantics of the "batch_id <
// cutoff" range against a live DB — the ingest integration tests.
func TestBatchIDAgeOrdering(t *testing.T) {
	base := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	cutoff := batchIDBefore(base.Add(StaleBatchAge))

	old, err := newBatchIDAt(base) // created a day before the cutoff
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(old[:], cutoff[:]) >= 0 {
		t.Error("a batch older than the threshold must fall under the purge: batch_id < the boundary")
	}

	fresh, err := newBatchIDAt(base.Add(StaleBatchAge)) // exactly at the cutoff
	if err != nil {
		t.Fatal(err)
	}
	if fresh == (BatchID{}) || bytes.Compare(fresh[:], cutoff[:]) <= 0 {
		t.Error("a batch exactly at the age boundary need not be deleted: the random tail is greater than zero")
	}

	later, err := newBatchIDAt(base.Add(StaleBatchAge).Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(later[:], cutoff[:]) <= 0 {
		t.Error("a batch younger than the boundary must not be deleted")
	}

	// Uniqueness within one microsecond — by the random tail.
	sameA, err := newBatchIDAt(base)
	if err != nil {
		t.Fatal(err)
	}
	sameB, err := newBatchIDAt(base)
	if err != nil {
		t.Fatal(err)
	}
	if sameA == sameB {
		t.Error("same-microsecond identifiers matched: the tail must be random")
	}
}

func TestStage_Batching(t *testing.T) {
	f := &fakeDB{}
	b := validBatch(t)
	rows := genRows(105)

	staged, err := Stage(context.Background(), f, b, 10, (&rowsIter{rows: rows}).next)
	if err != nil {
		t.Fatal(err)
	}
	if staged != 105 {
		t.Errorf("staged %d, want 105", staged)
	}
	// 10 full batches + a tail of 5 rows (spec 5.1: batches of batch_size).
	if got, want := strings.Join(ints(f.batches), ","), "10,10,10,10,10,10,10,10,10,10,5"; got != want {
		t.Errorf("the chunks = [%s], want [%s]", got, want)
	}
	// Before copying the batch is cleared — the idempotency of a retry (spec 3.4).
	if len(f.ops) == 0 || f.ops[0] != "exec:"+clearStageSQL {
		t.Errorf("the first call = %v, want the batch cleanup %q", f.ops, clearStageSQL)
	}
	if len(f.execSQL) != 1 {
		t.Errorf("cleanups %d, want 1 (only its own batch before the load)", len(f.execSQL))
	}

	// The COPY row content: the batch, the zone, the label, the action, the line number.
	if len(f.rows) != 105 {
		t.Fatalf("rows in COPY %d, want 105", len(f.rows))
	}
	for i, vals := range f.rows {
		want := []any{b.ID[:], b.ZoneID, rows[i].Label, int16(b.Action), rows[i].LineNo}
		if !reflect.DeepEqual(vals, want) {
			t.Fatalf("row %d = %v, want %v", i, vals, want)
		}
	}
}

// TestStage_MemoryBoundedByBatch — CopyFrom receives no more than batchSize
// rows at a time: only one batch is materialized, memory does not depend on
// the file size (spec 3.5).
func TestStage_MemoryBoundedByBatch(t *testing.T) {
	f := &fakeDB{}
	staged, err := Stage(context.Background(), f, validBatch(t), 100, (&rowsIter{rows: genRows(1000)}).next)
	if err != nil || staged != 1000 {
		t.Fatalf("(%d, %v), want (1000, nil)", staged, err)
	}
	for i, n := range f.batches {
		if n > 100 {
			t.Errorf("chunk %d: %d rows > the batchSize 100 — more than a chunk is materialized", i, n)
		}
	}
	if len(f.batches) != 10 {
		t.Errorf("chunks %d, want 10", len(f.batches))
	}
}

func TestStage_EmptyInput(t *testing.T) {
	f := &fakeDB{}
	staged, err := Stage(context.Background(), f, validBatch(t), 10, (&rowsIter{}).next)
	if err != nil || staged != 0 {
		t.Fatalf("(%d, %v), want (0, nil)", staged, err)
	}
	if len(f.batches) != 0 {
		t.Errorf("empty input led to COPY calls: %v", f.batches)
	}
	// The batch cleanup runs on empty input too: a retry of an empty file
	// wipes the batch's previous rows.
	if len(f.execSQL) != 1 {
		t.Errorf("cleanups %d, want 1", len(f.execSQL))
	}
}

func TestStage_Validation(t *testing.T) {
	id, err := NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name      string
		b         Batch
		batchSize int
	}{
		{"a zero identifier", Batch{ZoneID: 1, Action: model.ActionAdd}, 10},
		{"a negative zone", Batch{ID: id, ZoneID: -2, Action: model.ActionDelete}, 10},
		{"action 0", Batch{ID: id, ZoneID: 1, Action: 0}, 10},
		{"an unknown action", Batch{ID: id, ZoneID: 1, Action: model.Action(3)}, 10},
		{"a zero chunk size", Batch{ID: id, ZoneID: 1, Action: model.ActionAdd}, 0},
		{"a negative chunk size", Batch{ID: id, ZoneID: 1, Action: model.ActionAdd}, -5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeDB{}
			_, err := Stage(context.Background(), f, tc.b, tc.batchSize, (&rowsIter{rows: genRows(1)}).next)
			if err == nil {
				t.Fatal("a validation error is expected")
			}
			if len(f.ops) != 0 {
				t.Errorf("an invalid batch reached the DB: %v", f.ops)
			}
		})
	}
}

// TestStage_CancelBetweenBatches — a ctx cancellation between batches
// interrupts the loading, the finished batches stay in staging (a retry
// re-uploads the batch from scratch — spec 3.4/3.5).
func TestStage_CancelBetweenBatches(t *testing.T) {
	f := &fakeDB{}
	ctx, cancel := context.WithCancel(context.Background())

	it := &rowsIter{rows: genRows(100)}
	it.onNext = func(i int) {
		if i == 30 { // rows 0–29: three batches are already committed
			cancel()
		}
	}

	staged, err := Stage(ctx, f, validBatch(t), 10, it.next)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if staged != 30 {
		t.Errorf("staged %d, want 30 (the cancel before the 4th chunk is flushed)", staged)
	}
	if got, want := strings.Join(ints(f.batches), ","), "10,10,10"; got != want {
		t.Errorf("the chunks = [%s], want [%s]", got, want)
	}
}

func TestStage_SourceError(t *testing.T) {
	f := &fakeDB{}
	boom := errors.New("the source broke")
	it := &rowsIter{rows: genRows(30)}
	next := func() (Row, error) {
		if it.nextIdx == 22 {
			return Row{}, boom
		}
		return it.next()
	}

	staged, err := Stage(context.Background(), f, validBatch(t), 10, next)
	if err == nil || !strings.Contains(err.Error(), boom.Error()) {
		t.Fatalf("err = %v, want wrapped %v", err, boom)
	}
	if !strings.Contains(err.Error(), "copied 20") {
		t.Errorf("err = %v, want a mention of 20 committed rows (2 full chunks)", err)
	}
	if staged != 20 {
		t.Errorf("staged %d, want 20 (finished chunks only)", staged)
	}
}

// TestStage_BadRow — the row invariants reject the load before the DB; the
// rows of the batches committed earlier stay (the batch boundaries,
// spec 3.4).
func TestStage_BadRow(t *testing.T) {
	cases := []struct {
		name string
		row  Row
		want string // a fragment of the expected error
	}{
		{"an empty label", Row{Label: "", LineNo: 1}, "an empty domain label"},
		{"a non-ASCII label", Row{Label: "пример", LineNo: 1}, "punycode"},
		{"a label longer than 63", Row{Label: strings.Repeat("a", 64), LineNo: 1}, "at most 63"},
		{"an empty part of a composite label", Row{Label: "sub..example", LineNo: 1}, "non-empty"},
		{"a composite label part longer than 63", Row{Label: "sub." + strings.Repeat("a", 64), LineNo: 1}, "at most 63"},
		{"line number 0", Row{Label: "example", LineNo: 0}, "starts at one"},
		{"a repeated line number", Row{Label: "example", LineNo: 1}, "does not grow"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeDB{}
			// A batch of 10: the bad row at position 10 — the first batch is
			// already committed, the second is rejected entirely.
			rows := append(genRows(10), tc.row)
			staged, err := Stage(context.Background(), f, validBatch(t), 10, (&rowsIter{rows: rows}).next)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want an error with %q", err, tc.want)
			}
			if staged != 10 {
				t.Errorf("staged %d, want 10 (the first chunk is committed before the bad row)", staged)
			}
			if got, want := strings.Join(ints(f.batches), ","), "10"; got != want {
				t.Errorf("the chunks = [%s], want [%s]", got, want)
			}
		})
	}
}

// TestStage_CompositeLabel — composite labels (the internal/idn contract,
// spec 5.2; docs/incoming-files.md: "www2.example.crawl.test — ok") pass
// into staging as is: the T-016 validator yields them as one label of the
// path zone, dots inside are allowed.
func TestStage_CompositeLabel(t *testing.T) {
	f := &fakeDB{}
	b := validBatch(t)
	rows := []Row{
		{Label: "sub.example", LineNo: 1},
		{Label: "xn--80aairftm.xn--p1ai", LineNo: 2},
		{Label: strings.Repeat("a", MaxLabelLen) + "." + strings.Repeat("b", MaxLabelLen), LineNo: 3},
	}

	staged, err := Stage(context.Background(), f, b, 10, (&rowsIter{rows: rows}).next)
	if err != nil || staged != 3 {
		t.Fatalf("(%d, %v), want (3, nil) — composite labels are allowed", staged, err)
	}
	if len(f.rows) != len(rows) {
		t.Fatalf("rows in COPY %d, want %d", len(f.rows), len(rows))
	}
	for i, vals := range f.rows {
		if got := vals[2].(string); got != rows[i].Label {
			t.Errorf("label[%d] = %q, want %q", i, got, rows[i].Label)
		}
	}
}

func TestStage_CopyError(t *testing.T) {
	f := &fakeDB{copyErr: errors.New("relation does not exist")}
	staged, err := Stage(context.Background(), f, validBatch(t), 2, (&rowsIter{rows: genRows(5)}).next)
	if err == nil || !strings.Contains(err.Error(), "relation does not exist") {
		t.Fatalf("err = %v, want the wrapped COPY error", err)
	}
	if !strings.Contains(err.Error(), "ingest_stage") {
		t.Errorf("err = %v, want the table name in the error", err)
	}
	if staged != 0 {
		t.Errorf("staged %d, want 0 (the very first chunk failed)", staged)
	}
}

func TestStage_ClearError(t *testing.T) {
	f := &fakeDB{execErr: errors.New("connection refused")}
	if _, err := Stage(context.Background(), f, validBatch(t), 10, (&rowsIter{rows: genRows(1)}).next); err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("err = %v, want the batch cleanup error", err)
	}
	if len(f.batches) != 0 {
		t.Errorf("after the cleanup error COPY does not run, the chunks = %v", f.batches)
	}
}

func TestClearBatch(t *testing.T) {
	f := &fakeDB{execTag: pgconn.NewCommandTag("DELETE 7")}
	id, err := NewBatchID()
	if err != nil {
		t.Fatal(err)
	}

	n, err := ClearBatch(context.Background(), f, id)
	if err != nil {
		t.Fatal(err)
	}
	if n != 7 {
		t.Errorf("deleted %d, want 7 (RowsAffected)", n)
	}
	if len(f.execSQL) != 1 || f.execSQL[0] != clearStageSQL {
		t.Errorf("the SQL = %v, want a single %q", f.execSQL, clearStageSQL)
	}
	if len(f.execArgs) != 1 || !reflect.DeepEqual(f.execArgs[0][0], id[:]) {
		t.Errorf("the arguments = %v, want the batch identifier", f.execArgs)
	}

	// A zero identifier — an error without a DB call.
	if _, err := ClearBatch(context.Background(), f, BatchID{}); err == nil {
		t.Error("a zero identifier: an error is expected")
	}
	if len(f.execSQL) != 1 {
		t.Errorf("DB calls %d, want 1 (the valid call only)", len(f.execSQL))
	}
}

// ---------------------------------------------------------------- the schema contract

// TestStageTableContract — migration 000004 (the embedded migrations/ set)
// matches the loader: the UNLOGGED table ingest_stage, the columns and types
// in the StageColumns order, all NOT NULL, the PK (batch_id, line_no), no
// extra indexes; down drops the table. The live schema check — the
// integration schema_test.go (T-023).
func TestStageTableContract(t *testing.T) {
	up := readMigration(t, "000004_ingest_staging.up.sql")

	if !strings.Contains(up, "CREATE UNLOGGED TABLE ingest_stage") {
		t.Error("the up migration has no CREATE UNLOGGED TABLE ingest_stage (spec 3.5: staging without WAL)")
	}
	if strings.Contains(up, "CREATE INDEX") || strings.Contains(up, "CREATE UNIQUE INDEX") {
		t.Error("the up migration has indexes beyond the primary key (a minimum of indexes, spec 6)")
	}

	cols := parseCreateTable(t, up)
	wantTypes := map[string]string{
		"batch_id": "bytea", "zone_id": "smallint", "label": "text",
		"action": "smallint", "line_no": "bigint",
	}
	if len(cols) != len(wantTypes) {
		t.Fatalf("columns %d (%v), want %d", len(cols), colNames(cols), len(wantTypes))
	}
	names := colNames(cols)
	if !slices.Equal(names, StageColumns) {
		t.Errorf("the table columns %v do not match StageColumns %v (the COPY contract)", names, StageColumns)
	}
	for _, c := range cols {
		if c.typ != wantTypes[c.name] {
			t.Errorf("%s: the type %q, want %q", c.name, c.typ, wantTypes[c.name])
		}
		if !c.notNull {
			t.Errorf("%s: NOT NULL is expected", c.name)
		}
	}

	down := readMigration(t, "000004_ingest_staging.down.sql")
	if !strings.Contains(down, "DROP TABLE IF EXISTS ingest_stage") {
		t.Error("the down migration does not drop ingest_stage")
	}
}

// TestClearStageSQLContract — the cleanup targets the same table as COPY
// (StageTable), its own batch only.
func TestClearStageSQLContract(t *testing.T) {
	if want := `DELETE FROM "ingest_stage" WHERE batch_id = $1`; clearStageSQL != want {
		t.Errorf("clearStageSQL = %q, want %q", clearStageSQL, want)
	}
}

func readMigration(t *testing.T, name string) string {
	t.Helper()
	raw, err := migrations.FS.ReadFile(name)
	if err != nil {
		t.Fatalf("reading %s from the embedded set: %v", name, err)
	}
	return string(raw)
}

// stageColumn — a column from the DDL: the name, the type, NOT NULL.
type stageColumn struct {
	name    string
	typ     string
	notNull bool
}

// parseCreateTable parses the CREATE TABLE body of the file's first table.
// The DDL lines belong to the project's migration (no nested parens in the
// columns), a light parse is enough for the contract.
func parseCreateTable(t *testing.T, sql string) []stageColumn {
	t.Helper()

	lines := strings.Split(sql, "\n")
	start := -1
	for i, line := range lines {
		if strings.Contains(line, "CREATE") && strings.Contains(line, "TABLE") {
			start = i + 1 // the first line after the CREATE header
			break
		}
	}
	if start < 0 {
		t.Fatal("no CREATE TABLE in the file")
	}

	cols := make([]stageColumn, 0, 6)
	for _, line := range lines[start:] {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, ")") {
			break // the end of the table body
		}
		line = strings.TrimSuffix(line, ",")
		if strings.HasPrefix(line, "PRIMARY KEY") {
			if !strings.Contains(line, "(batch_id, line_no)") {
				t.Errorf("the primary key = %q, want (batch_id, line_no)", line)
			}
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			t.Fatalf("the DDL line %q was not parsed", line)
		}
		cols = append(cols, stageColumn{
			name:    fields[0],
			typ:     fields[1],
			notNull: strings.Contains(line, "NOT NULL"),
		})
	}
	if len(cols) == 0 {
		t.Fatal("the CREATE TABLE body is empty")
	}
	return cols
}

func colNames(cols []stageColumn) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.name
	}
	return names
}

// TestStageRowZone — the COPY row zone: Row.ZoneID (a zone-less file) with
// a fallback to the batch ZoneID (a zone-directory file); with a zone in
// neither the row nor the batch — a rowIter error, the row does not reach
// the DB.
func TestStageRowZone(t *testing.T) {
	id, err := NewBatchID()
	if err != nil {
		t.Fatal(err)
	}

	// A zone-less batch: each row carries its own zone.
	f := &fakeDB{}
	rows := []Row{
		{Label: "example", ZoneID: 7, LineNo: 1},
		{Label: "foo", ZoneID: 8, LineNo: 2},
		{Label: "bar", ZoneID: 7, LineNo: 3},
	}
	if _, err := Stage(context.Background(), f, Batch{ID: id, Action: model.ActionAdd}, 10, (&rowsIter{rows: rows}).next); err != nil {
		t.Fatalf("Stage: %v", err)
	}
	wantZone := []any{int16(7), int16(8), int16(7)}
	if len(f.rows) != len(wantZone) {
		t.Fatalf("rows in staging = %d, want %d", len(f.rows), len(wantZone))
	}
	for i, r := range f.rows {
		if r[1] != wantZone[i] {
			t.Errorf("row %d: zone_id = %v, want %v", i, r[1], wantZone[i])
		}
	}

	// A zone-directory file: the row ZoneID is zero — the batch zone.
	f2 := &fakeDB{}
	rows2 := []Row{{Label: "example", LineNo: 1}, {Label: "foo", LineNo: 2}}
	if _, err := Stage(context.Background(), f2, Batch{ID: id, ZoneID: 5, Action: model.ActionAdd}, 10, (&rowsIter{rows: rows2}).next); err != nil {
		t.Fatalf("Stage: %v", err)
	}
	for i, r := range f2.rows {
		if r[1] != any(int16(5)) {
			t.Errorf("row %d: zone_id = %v, want 5 (from the batch)", i, r[1])
		}
	}

	// The zone is undefined everywhere: a rowIter error — the COPY batch
	// does not reach the DB at all (the batch transaction, spec 3.5).
	f3 := &fakeDB{}
	bad := []Row{{Label: "example", ZoneID: 9, LineNo: 1}, {Label: "foo", LineNo: 2}}
	_, err = Stage(context.Background(), f3, Batch{ID: id, Action: model.ActionAdd}, 10, (&rowsIter{rows: bad}).next)
	if err == nil || !strings.Contains(err.Error(), "the line zone is undefined") {
		t.Fatalf("Stage = %v, want «the line zone is undefined»", err)
	}
	if len(f3.rows) != 0 {
		t.Errorf("rows ended up in staging: %d, want 0 (the chunk was cancelled)", len(f3.rows))
	}
}

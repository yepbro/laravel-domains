// Unit tests of file accounting in ingest_files (T-021, spec 5.1, 6.1): the
// fake FileStore for the pipeline (pipeline_test.go), SQLFileStore — the SQL
// text and parameters against a fake pgx pool without a DB (the style of
// zones_test.go), the log report format of renderReport and the mapping of
// the FileReport.record counters.
// Real behavior against the DB — the ingest integration tests (T-023).
package ingest

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"crawler/internal/db"
	"crawler/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// --- fake FileStore---------------------------------------------------

// fakeFileStore — an in-memory journal of ingest_files with a call protocol.
// Rows with the same sha256 (different zone/action — the T-113 scenarios)
// are returned in insertion order.
type fakeFileStore struct {
	known   map[string][]model.IngestFile
	bySAErr error // the FilesBySHA256 error, if set
	records []FileRecord
	recErr  error // the RecordFile error, if set
}

func newFakeFileStore(known ...model.IngestFile) *fakeFileStore {
	f := &fakeFileStore{known: make(map[string][]model.IngestFile, len(known))}
	for _, k := range known {
		f.known[string(k.SHA256)] = append(f.known[string(k.SHA256)], k)
	}
	return f
}

func (f *fakeFileStore) FilesBySHA256(_ context.Context, sha []byte) ([]model.IngestFile, error) {
	if f.bySAErr != nil {
		return nil, f.bySAErr
	}
	return f.known[string(sha)], nil // nil = the content was not processed
}

func (f *fakeFileStore) RecordFile(_ context.Context, rec FileRecord) error {
	if f.recErr != nil {
		return f.recErr
	}
	f.records = append(f.records, rec)
	return nil
}

// --- fake pgx pool for SQLFileStore------------------------------------

// fileColNames — column names in the ingestFileColumns order.
var fileColNames = strings.Split(ingestFileColumns, ", ")

// fileRows — pgx.Rows with pre-made ingest_files rows: Scan lays out the
// values positionally, FieldDescriptions gives the column names for
// RowToStructByName (like fakeRows in zones_test.go).
type fileRows struct {
	rows   [][]any
	i      int
	closed bool
}

func (r *fileRows) Close()                        { r.closed = true }
func (r *fileRows) Err() error                    { return nil }
func (r *fileRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (r *fileRows) FieldDescriptions() []pgconn.FieldDescription {
	fds := make([]pgconn.FieldDescription, len(fileColNames))
	for i, c := range fileColNames {
		fds[i] = pgconn.FieldDescription{Name: c}
	}
	return fds
}

func (r *fileRows) Next() bool {
	if r.closed || r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *fileRows) Scan(dest ...any) error {
	if r.closed || r.i == 0 || r.i > len(r.rows) {
		return errors.New("fileRows: Scan without Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return errors.New("fileRows: the destination count does not match the column count")
	}
	for i, d := range dest {
		target := reflect.ValueOf(d).Elem()
		if row[i] == nil {
			target.Set(reflect.Zero(target.Type()))
			continue
		}
		val := reflect.ValueOf(row[i])
		if target.Kind() == reflect.Pointer {
			// a NULL model column (pointer): *Action, *string, …
			p := reflect.New(val.Type())
			p.Elem().Set(val)
			target.Set(p)
			continue
		}
		target.Set(val)
	}
	return nil
}

func (r *fileRows) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *fileRows) RawValues() [][]byte    { return nil }

// Conn and TypeMap — the part of pgx.Rows not used by the collector.
func (r *fileRows) Conn() *pgx.Conn      { return nil }
func (r *fileRows) TypeMap() *pgtype.Map { return nil }

// fakeFilePool — a db.DB for SQLFileStore: it remembers the SQL and the
// arguments, Query yields the pre-made rows, Exec — the pre-made tag/error.
type fakeFilePool struct {
	queries  []zoneQuery // reusing the type from zones_test.go
	results  []*fileRows
	queryErr error
	execSQL  []string
	execArgs [][]any
	execTag  pgconn.CommandTag
	execErr  error
}

var _ db.DB = (*fakeFilePool)(nil)

func (p *fakeFilePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("fakeFilePool: CopyFrom must not be called")
}

func (p *fakeFilePool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.execSQL = append(p.execSQL, sql)
	p.execArgs = append(p.execArgs, args)
	if p.execErr != nil {
		return pgconn.CommandTag{}, p.execErr
	}
	return p.execTag, nil
}

func (p *fakeFilePool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.queries = append(p.queries, zoneQuery{sql: sql, args: args})
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	rows := p.results[0]
	p.results = p.results[1:]
	return rows, nil
}

// --- SQLFileStore ---------------------------------------------------------

const wantFileSelect = "SELECT id, zone_id, action, path, sha256, lines_total, added, existed, deleted, rejected, processed_at " +
	"FROM ingest_files WHERE sha256 = $1 ORDER BY id"

const wantFileInsert = "INSERT INTO ingest_files " +
	"(zone_id, action, path, sha256, lines_total, added, existed, deleted, rejected, processed_at) " +
	"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"

// TestSQLFilesBySHA256 — all journal rows with the given content (T-113:
// one content may be delivered in different zone/action — exact redelivery
// is looked up by the pipeline by the triple).
func TestSQLFilesBySHA256(t *testing.T) {
	when := time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC)
	action := model.ActionAdd
	path := "com/add/list.txt"
	sha := []byte{1, 2, 3}
	pool := &fakeFilePool{results: []*fileRows{{rows: [][]any{
		{int64(9), int16(7), action, path, sha,
			int64(100), int64(60), int64(30), nil, int64(10), when},
		{int64(11), int16(7), model.ActionDelete, "com/delete/x.txt", sha,
			int64(100), nil, nil, int64(60), int64(10), when},
	}}}}
	store := NewFileStore(pool)

	fs, err := store.FilesBySHA256(context.Background(), sha)
	if err != nil {
		t.Fatal(err)
	}
	if len(pool.queries) != 1 || pool.queries[0].sql != wantFileSelect {
		t.Errorf("SQL = %v, want SELECT … WHERE sha256 = $1 ORDER BY id", pool.queries)
	}
	if !reflect.DeepEqual(pool.queries[0].args, []any{sha}) {
		t.Errorf("the arguments = %v, want [sha256]", pool.queries[0].args)
	}
	if len(fs) != 2 {
		t.Fatalf("journal rows = %d, want 2 (add and undo-delete of the same content)", len(fs))
	}
	f := fs[0]
	if f.ID != 9 || f.ZoneID == nil || *f.ZoneID != 7 ||
		f.Action == nil || *f.Action != model.ActionAdd ||
		f.Path == nil || *f.Path != path ||
		!reflect.DeepEqual(f.SHA256, sha) ||
		f.LinesTotal == nil || *f.LinesTotal != 100 ||
		f.Added == nil || *f.Added != 60 ||
		f.Existed == nil || *f.Existed != 30 ||
		f.Deleted != nil ||
		f.Rejected == nil || *f.Rejected != 10 ||
		f.ProcessedAt == nil || !f.ProcessedAt.Equal(when) {
		t.Errorf("the journal row = %+v, want as-is from ingest_files", f)
	}
	if d := fs[1]; d.ID != 11 || d.Action == nil || *d.Action != model.ActionDelete {
		t.Errorf("the second journal row = %+v, want the undo-delete of the same sha256", d)
	}
}

// TestSQLFilesBySHA256None — the content was not processed: an empty slice
// without an error (not processed is not a store error).
func TestSQLFilesBySHA256None(t *testing.T) {
	pool := &fakeFilePool{results: []*fileRows{{}}}
	store := NewFileStore(pool)

	fs, err := store.FilesBySHA256(context.Background(), []byte{9})
	if err != nil || len(fs) != 0 {
		t.Errorf("the result = %v, %v; want an empty slice without an error", fs, err)
	}
}

func TestSQLRecordFile(t *testing.T) {
	at := time.Date(2026, 10, 17, 11, 30, 0, 0, time.UTC)
	sha := make([]byte, 32)
	for i := range sha {
		sha[i] = byte(i + 1)
	}
	pool := &fakeFilePool{}
	store := NewFileStore(pool)

	rec := FileRecord{
		ZoneID:      7,
		Action:      model.ActionDelete,
		Path:        "net/delete/x.txt",
		SHA256:      sha,
		LinesTotal:  10,
		Deleted:     8,
		Rejected:    2,
		ProcessedAt: at,
	}
	if err := store.RecordFile(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if len(pool.execSQL) != 1 || pool.execSQL[0] != wantFileInsert {
		t.Errorf("SQL = %v, want INSERT INTO ingest_files …", pool.execSQL)
	}
	want := []any{
		int16(7), int16(model.ActionDelete), "net/delete/x.txt", sha,
		int64(10), int64(0), int64(0), int64(8), int64(2), at,
	}
	if !reflect.DeepEqual(pool.execArgs[0], want) {
		t.Errorf("the arguments = %v, want %v", pool.execArgs[0], want)
	}
}

// TestSQLRecordFileBadSHA — a report without a full sha256 — an error before
// the DB: a broken hash would break idempotency (the unique triple
// (zone_id, action, sha256), T-113).
func TestSQLRecordFileBadSHA(t *testing.T) {
	pool := &fakeFilePool{}
	store := NewFileStore(pool)

	err := store.RecordFile(context.Background(), FileRecord{Path: "x", SHA256: []byte{1}})
	if err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("err = %v, want the sha256 length error", err)
	}
	if len(pool.execSQL) != 0 {
		t.Error("the query must not run on an invalid sha256")
	}
}

// TestSQLFileStoreErrors — store errors are wrapped with the "db:
// ingest_files" context (the SQLZoneStore style).
func TestSQLFileStoreErrors(t *testing.T) {
	boom := errors.New("connection lost")

	pool := &fakeFilePool{queryErr: boom}
	store := NewFileStore(pool)
	_, err := store.FilesBySHA256(context.Background(), []byte{1})
	if err == nil || !strings.Contains(err.Error(), "db: ingest_files: lookup by sha256:") || !errors.Is(err, boom) {
		t.Errorf("the lookup error = %v, want wrapped", err)
	}

	pool = &fakeFilePool{execErr: boom}
	store = NewFileStore(pool)
	err = store.RecordFile(context.Background(), FileRecord{Path: "x", SHA256: make([]byte, 32)})
	if err == nil || !strings.Contains(err.Error(), "db: ingest_files: report x:") || !errors.Is(err, boom) {
		t.Errorf("the write error = %v, want wrapped", err)
	}
}

// --- log report------------------------------------------------------------

// TestRenderReport — the log report format (spec 5.1: next to the file in
// failed/) is fixed: the reason, the line counters by stages, the apply
// counts by action.
func TestRenderReport(t *testing.T) {
	rep := FileReport{
		Zone:       "com",
		ZoneID:     7,
		Action:     model.ActionAdd,
		Path:       "com/add/list.txt",
		SHA256:     []byte{0xab, 0xcd},
		LinesTotal: 6,
		Skipped:    2,
		Rejected:   3,
		Accepted:   1,
		Reasons:    map[Reason]int{ReasonWrongZone: 2, ReasonInvalid: 1},
		Added:      2,
		Revived:    1,
		Existed:    3,
	}
	want := strings.Join([]string{
		"# ingest log report (spec 5.1)",
		"file: com/add/list.txt",
		"zone: com (id 7)",
		"action: add",
		"sha256: abcd",
		"verdict: applied",
		"lines: 6",
		"  skipped (blank and comments): 2",
		"  rejected: 3",
		"    invalid: 1",
		"    wrong_zone: 2",
		"  valid: 1",
		"added: 2 (including revived from deleted: 1)",
		"already existed: 3",
		"",
	}, "\n")
	if got := renderReport(rep, "applied"); got != want {
		t.Errorf("the log report =\n%s\nwant:\n%s", got, want)
	}
}

// TestRenderReportDeleteFailed — the report of a failed delete file: no
// apply, the rejection reasons are attached; the zone is unparsed — a dash.
func TestRenderReportDeleteFailed(t *testing.T) {
	rep := FileReport{
		Zone:       "net",
		Action:     model.ActionDelete,
		Path:       "net/delete/y.txt",
		LinesTotal: 4,
		Rejected:   4,
		Reasons:    map[Reason]int{ReasonWrongZone: 4},
	}
	want := strings.Join([]string{
		"# ingest log report (spec 5.1)",
		"file: net/delete/y.txt",
		"zone: net (id 0)",
		"action: delete",
		"verdict: bad — the share of rejected lines is above the threshold",
		"lines: 4",
		"  skipped (blank and comments): 0",
		"  rejected: 4",
		"    wrong_zone: 4",
		"  valid: 0",
		"deleted: 0",
		"not found in the DB: 0",
		"",
	}, "\n")
	if got := renderReport(rep, "bad — the share of rejected lines is above the threshold"); got != want {
		t.Errorf("the log report =\n%s\nwant:\n%s", got, want)
	}
}

// TestRenderReportNoPath — the path is unparsed (failed files also get a
// log report): the zone and action are dashes, no sha256.
func TestRenderReportNoPath(t *testing.T) {
	want := strings.Join([]string{
		"# ingest log report (spec 5.1)",
		"file: com/list.txt",
		"zone: — (the path was not parsed)",
		"action: —",
		"verdict: bad — the path structure is not <zone>/add|delete/*.txt",
		"lines: 0",
		"  skipped (blank and comments): 0",
		"  rejected: 0",
		"  valid: 0",
		"",
	}, "\n")
	got := renderReport(FileReport{Path: "com/list.txt"},
		"bad — the path structure is not <zone>/add|delete/*.txt")
	if got != want {
		t.Errorf("the log report =\n%s\nwant:\n%s", got, want)
	}
}

// TestFileReportRecord — the mapping of the report into the ingest_files
// columns (spec 6.1): Revived is included in Added, NotFound has no column.
func TestFileReportRecord(t *testing.T) {
	at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	rep := FileReport{
		ZoneID: 7, Action: model.ActionAdd, Path: "com/add/a.txt",
		SHA256: make([]byte, 32), LinesTotal: 10,
		Added: 5, Revived: 2, Existed: 1, NotFound: 3,
	}
	rec := rep.record(at)
	want := FileRecord{
		ZoneID: 7, Action: model.ActionAdd, Path: "com/add/a.txt",
		SHA256: make([]byte, 32), LinesTotal: 10,
		Added: 5, Existed: 1, ProcessedAt: at,
	}
	if !reflect.DeepEqual(rec, want) {
		t.Errorf("record = %+v, want %+v (revived is part of added, not_found has no column)", rec, want)
	}
}

// TestMergeReasons — the parser's and the validator's rejections are summed
// by reason; the names do not overlap (invalid_utf8/line_len vs the idn
// classes and wrong_zone).
func TestMergeReasons(t *testing.T) {
	got := mergeReasons(
		map[Reason]int{ReasonLong: 2, ReasonUTF8: 1},
		map[Reason]int{ReasonWrongZone: 3, ReasonLong: 0},
	)
	want := map[Reason]int{ReasonLong: 2, ReasonUTF8: 1, ReasonWrongZone: 3}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("mergeReasons = %v, want %v", got, want)
	}
}

// compile-time: SQLFileStore — a FileStore on top of internal/db.
var _ FileStore = (*SQLFileStore)(nil)
var _ FileStore = (*fakeFileStore)(nil)

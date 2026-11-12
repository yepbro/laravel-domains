// End-to-end unit tests of the ingest pipeline (T-021, spec 5.1) without a
// DB: fake ZoneStore (zones_test.go), FileStore (report_test.go) and a pgx
// pool with pre-made Stage/ApplyAdd/ApplyDelete results; the files live in
// temporary directories (t.TempDir). Scenarios: a successful add, a
// successful delete, a duplicate — exact redelivery (T-113: the same zone,
// action and sha256 — not applied again), the same content with another
// zone/action — processed (undo), the error threshold →
// failed/ with a log report, a broken path structure → failed/, an
// infrastructure failure → the file stays in incoming (at-least-once,
// spec 3.4), a name collision inside an archive. Real behavior against the
// DB — the integration tests (T-023).
package ingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"crawler/internal/db"
	"crawler/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// --- the fake pool of the pipeline----------------------------------------------

// pipeExec — a pre-made result of one Exec, handed out in turn.
type pipeExec struct {
	tag pgconn.CommandTag
	err error
}

// pipePool — a db.DB for the pipeline: CopyFrom counts the rows of the COPY
// batches (staging), Exec hands out results in turn (in the add apply
// transaction — one common queue), Query answers the known ApplyAdd SQL
// (the batch boundary and the RETURNING of the insert — by the staging
// model: the COPY rows of the batch with line_no in the cursor range;
// inserted — how many rows of the batch are "inserted", the rest count as
// conflicts) and ApplyDelete. Like real pgx, it refuses to work with a
// canceled context.
type pipePool struct {
	ops      []string // the protocol: "exec:<sql>", "query:<sql>", "copy:<n>", "begin", "commit"
	copies   []int64  // the COPY batch sizes in turn
	copyVals [][]any  // the rows of all COPY batches (the column values)
	copyErr  error

	execs []pipeExec // the Exec results in turn (the end — a zero tag)
	execI int

	inserted   int64 // the answer of the insert CTE (T-178: Query)
	revived    int64 // the answer of the revive CTE (T-178: Query)
	uniques    int64 // the answer of countAddSQL (the unique domains of the add batch)
	delDeleted int64 // the answer of applyDeleteSQL
	delMissing int64

	begins  int
	commits int
}

var _ db.DB = (*pipePool)(nil)

func (p *pipePool) CopyFrom(ctx context.Context, _ pgx.Identifier, _ []string, src pgx.CopyFromSource) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var n int64
	for src.Next() {
		vals, err := src.Values()
		if err != nil {
			return n, err
		}
		p.copyVals = append(p.copyVals, vals)
		n++
	}
	if err := src.Err(); err != nil {
		return n, err
	}
	p.copies = append(p.copies, n)
	p.ops = append(p.ops, fmt.Sprintf("copy:%d", n))
	if p.copyErr != nil {
		return n, p.copyErr
	}
	return n, nil
}

func (p *pipePool) Exec(ctx context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	p.ops = append(p.ops, "exec:"+sql)
	i := p.execI
	p.execI++
	if i < len(p.execs) {
		if p.execs[i].err != nil {
			return pgconn.CommandTag{}, p.execs[i].err
		}
		return p.execs[i].tag, nil
	}
	return pgconn.CommandTag{}, nil
}

// oneRowRows — pgx.Rows of one row for a positional Scan (countAddSQL —
// RowTo[int64], applyDeleteSQL — a Scan of two counters).
type oneRowRows struct {
	vals   []any
	next   bool
	closed bool
}

func (r *oneRowRows) Next() bool {
	if r.closed || r.next {
		return false
	}
	r.next = true
	return true
}

func (r *oneRowRows) Scan(dest ...any) error {
	if r.closed || !r.next || len(dest) > len(r.vals) {
		return errors.New("oneRowRows: Scan без Next")
	}
	for i, d := range dest {
		if err := scanAny(d, r.vals[i]); err != nil {
			return err
		}
	}
	return nil
}

// scanAny assigns the value to the destination via reflect (int64 and pointers).
func scanAny(dest any, val any) error {
	target := reflect.ValueOf(dest).Elem()
	v := reflect.ValueOf(val)
	if target.Kind() == reflect.Pointer {
		p := reflect.New(v.Type())
		p.Elem().Set(v)
		target.Set(p)
		return nil
	}
	target.Set(v)
	return nil
}

func (r *oneRowRows) Close()                        { r.closed = true }
func (r *oneRowRows) Err() error                    { return nil }
func (r *oneRowRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (r *oneRowRows) FieldDescriptions() []pgconn.FieldDescription {
	return make([]pgconn.FieldDescription, len(r.vals))
}
func (r *oneRowRows) Values() ([]any, error) { return r.vals, nil }
func (r *oneRowRows) RawValues() [][]byte    { return nil }

// Conn and TypeMap — the part of pgx.Rows not used by the collectors.
func (r *oneRowRows) Conn() *pgx.Conn      { return nil }
func (r *oneRowRows) TypeMap() *pgtype.Map { return nil }

func (p *pipePool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.ops = append(p.ops, "query:"+sql)
	switch sql {
	case addChunkBoundSQL:
		// The batch boundary: the line_no of the limit-th row of the batch
		// after the cursor, by the staging model (the COPY rows of this
		// batch).
		after, limit := args[2].(int64), args[3].(int)
		rows := p.stageRows(args[0].([]byte), args[1].(int16), after, 1<<62)
		if n := len(rows); n > limit {
			rows = rows[:limit]
		}
		var bound int64
		if len(rows) > 0 {
			bound = rows[len(rows)-1].lineNo
		}
		return cleanupRows([]any{bound}), nil
	case insertAddSQL:
		// RETURNING zone_id of the inserted rows of the batch: the first
		// inserted rows of the range (the rest — conflicts).
		from, to := args[2].(int64), args[3].(int64)
		rows := p.stageRows(args[0].([]byte), args[1].(int16), from, to)
		if n := int(p.inserted); n < len(rows) {
			rows = rows[:n]
		}
		out := cleanupRows()
		for _, r := range rows {
			out.rows = append(out.rows, []any{r.zoneID})
		}
		return out, nil
	case reviveAddSQL:
		return &oneRowRows{vals: []any{p.revived}}, nil
	case countAddSQL:
		return &oneRowRows{vals: []any{p.uniques}}, nil
	case applyDeleteSQL:
		// T-269: the row carries the bit arrays too (the pipeline tests have
		// no flagged rows — the second pair is not executed).
		return &oneRowRows{vals: []any{p.delDeleted, p.delMissing, []int16{}, []string{}}}, nil
	}
	return nil, fmt.Errorf("pipePool: неожидаемый запрос: %s", sql)
}

// stagedRow — a batch row in the fake's staging model.
type stagedRow struct {
	lineNo int64
	zoneID int16
}

// stageRows — the batch rows in the staging model (the COPY rows) with
// line_no in (after, to]; to >= 1<<62 — without an upper bound.
func (p *pipePool) stageRows(batch []byte, action int16, after, to int64) []stagedRow {
	var rows []stagedRow
	for _, vals := range p.copyVals {
		if !bytes.Equal(vals[0].([]byte), batch) || vals[3].(int16) != action {
			continue
		}
		if ln := vals[4].(int64); ln > after && ln <= to {
			rows = append(rows, stagedRow{lineNo: ln, zoneID: vals[1].(int16)})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].lineNo < rows[j].lineNo })
	return rows
}

// Begin — the add apply transaction (T-205): the operations go through the
// pool (the common protocol and the Exec queue), Commit/Rollback — flags.
func (p *pipePool) Begin(ctx context.Context) (pgx.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.begins++
	p.ops = append(p.ops, "begin")
	return &pipeTx{pool: p}, nil
}

// pipeTx — a transaction of the fake pipeline pool.
type pipeTx struct {
	pgx.Tx // the pipeline does not use the other pgx.Tx methods

	pool *pipePool
}

func (t *pipeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return t.pool.Exec(ctx, sql, args...)
}

func (t *pipeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.pool.Query(ctx, sql, args...)
}

func (t *pipeTx) Commit(context.Context) error {
	t.pool.commits++
	t.pool.ops = append(t.pool.ops, "commit")
	return nil
}

func (t *pipeTx) Rollback(context.Context) error { return nil }

// --- pipeline assembly------------------------------------------------------

// pipeTest — the pipeline test environment: the incoming directory, the fakes, the pipeline.
type pipeTest struct {
	dir   string // incoming (t.TempDir)
	zones *fakeZoneStore
	files *fakeFileStore
	pool  *pipePool
	pipe  *Pipeline
	now   time.Time
}

func newPipeTest(t *testing.T) *pipeTest {
	t.Helper()
	pt := &pipeTest{
		dir:   t.TempDir(),
		now:   time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC),
		zones: newFakeZoneStore(),
		files: newFakeFileStore(),
		pool:  &pipePool{},
	}
	pipe, err := NewPipeline(pt.zones, pt.files, pt.pool, PipelineOptions{
		Dir:       pt.dir,
		BatchSize: 10,
		Log:       nil, // suppressed: the outcomes are checked via Outcome
		Now:       func() time.Time { return pt.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	pt.pipe = pipe
	return pt
}

// addFile — a file rel (relative to incoming) with the content content;
// returns the absolute path.
func (pt *pipeTest) addFile(t *testing.T, rel, content string) string {
	t.Helper()
	abs := filepath.Join(pt.dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(abs, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return abs
}

// exists — the file/directory exists.
func exists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Lstat(path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	return err == nil
}

// readFile — the file content (for log reports).
func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// --- scenarios--------------------------------------------------------------

// TestPipelineAdd — a successful add: the lines are applied, the report is
// written into ingest_files, the file is moved to processed/ keeping the
// structure.
func TestPipelineAdd(t *testing.T) {
	pt := newPipeTest(t)
	content := "example.com\n# комментарий\n\nfoo.com\nexample.com\n"
	abs := pt.addFile(t, "com/add/list.txt", content)
	sha := sha256.Sum256([]byte(content))

	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")}, // the cleanup before loading (Stage)
		{tag: pgconn.NewCommandTag("DELETE 3")}, // the cleanup after the apply
	}
	pt.pool.inserted = 1 // new: foo (example already exists in deleted)
	pt.pool.revived = 1  // revived from deleted: example
	pt.pool.uniques = 2  // the unique labels of the batch: example, foo (a duplicate line — one label)

	out, err := pt.pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Kind != OutcomeApplied {
		t.Fatalf("итог = %s, want applied", out.Kind)
	}

	rep := out.Report
	if rep.Zone != "com" || rep.ZoneID != 1 || rep.Action != model.ActionAdd {
		t.Errorf("файл отчёта = %+v, want зона com (id 1), действие add", rep)
	}
	// 5 physical lines: 3 significant (a duplicate example is a line too),
	// 1 comment, 1 empty; 3 valid.
	if rep.LinesTotal != 5 || rep.Skipped != 2 || rep.Accepted != 3 || rep.Rejected != 0 {
		t.Errorf("счётчики строк = %+v, want 5/2/3/0", rep)
	}
	if rep.Added != 2 || rep.Revived != 1 || rep.Existed != 0 {
		t.Errorf("счётчики применения = %+v, want added 2 (revived 1), existed 0", rep)
	}
	if !reflect.DeepEqual(rep.SHA256, sha[:]) {
		t.Errorf("sha256 = %x, want хэш содержимого %x", rep.SHA256, sha)
	}

	// The report in the journal: the file counters as one ingest_files row (spec 6.1).
	if len(pt.files.records) != 1 {
		t.Fatalf("записей в журнале = %d, want 1", len(pt.files.records))
	}
	rec := pt.files.records[0]
	wantRec := FileRecord{
		ZoneID: 1, Action: model.ActionAdd, Path: "com/add/list.txt", SHA256: sha[:],
		LinesTotal: 5, Added: 2, ProcessedAt: pt.now,
	}
	if !reflect.DeepEqual(rec, wantRec) {
		t.Errorf("запись журнала = %+v, want %+v", rec, wantRec)
	}

	// DB calls: cleanup → COPY → the apply in one transaction (the batch
	// boundary → the insert → the "empty" boundary → the zone deltas → the
	// revive → the count → the cleanup → the commit), then — nothing (Stage
	// and ApplyAdd, the fixed order of T-017/T-018/T-205).
	want := []string{
		"exec:" + clearStageSQL,
		"copy:3",
		"begin",
		"query:" + addChunkBoundSQL,
		"query:" + insertAddSQL,
		"query:" + addChunkBoundSQL,
		"exec:" + zoneDeltaAddSQL,
		"query:" + reviveAddSQL,
		"query:" + countAddSQL,
		"exec:" + clearStageSQL,
		"commit",
	}
	if !reflect.DeepEqual(pt.pool.ops, want) {
		t.Errorf("порядок обращений =\n%s\nwant:\n%s",
			strings.Join(pt.pool.ops, "\n"), strings.Join(want, "\n"))
	}

	// The file goes to processed/ keeping the structure (spec 5.1).
	if exists(t, abs) {
		t.Error("файл остался в incoming")
	}
	if !exists(t, filepath.Join(pt.dir, DirProcessed, "com/add/list.txt")) {
		t.Error("файл не перемещён в processed/com/add/list.txt")
	}
}

// TestPipelineZonelessAdd — a zone-less file (add/<file>): the zones are
// created lazily by the first lines, the staging rows each carry a zone,
// the report has zone_id NULL (ZoneID 0), the file goes to processed/add/.
func TestPipelineZonelessAdd(t *testing.T) {
	pt := newPipeTest(t)
	content := "example.com\nfoo.ru\nbar.com\n"
	abs := pt.addFile(t, "add/list.txt", content)
	sha := sha256.Sum256([]byte(content))

	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")}, // the cleanup before loading (Stage)
		// the insert/revive — the Query answers (inserted=3, revived=0)
		{tag: pgconn.NewCommandTag("DELETE 3")}, // the cleanup after the apply
	}
	pt.pool.inserted = 3
	pt.pool.revived = 0
	pt.pool.uniques = 3

	out, err := pt.pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Kind != OutcomeApplied {
		t.Fatalf("итог = %s, want applied", out.Kind)
	}

	rep := out.Report
	if rep.Zone != "" || rep.ZoneID != 0 || rep.Action != model.ActionAdd {
		t.Errorf("файл отчёта = %+v, want зона \"\" (id 0), действие add", rep)
	}
	if rep.LinesTotal != 3 || rep.Accepted != 3 || rep.Rejected != 0 {
		t.Errorf("счётчики строк = %+v, want 3/3/0", rep)
	}
	if rep.Added != 3 {
		t.Errorf("добавлено = %d, want 3", rep.Added)
	}
	if !reflect.DeepEqual(rep.SHA256, sha[:]) {
		t.Errorf("sha256 = %x, want хэш содержимого %x", rep.SHA256, sha)
	}

	// The zones are created lazily by the first lines: com (the first line), ru.
	wantCalls := []string{"byName:com", "create:com", "byName:ru", "create:ru"}
	if !reflect.DeepEqual(pt.zones.calls, wantCalls) {
		t.Errorf("обращения к зонам = %v, want %v (кэш: com повторно не ищется)", pt.zones.calls, wantCalls)
	}

	// The COPY rows each carry a zone: example→com(1), foo→ru(2), bar→com(1).
	wantZone := []int16{1, 2, 1}
	if len(pt.pool.copyVals) != len(wantZone) {
		t.Fatalf("строк COPY = %d, want %d", len(pt.pool.copyVals), len(wantZone))
	}
	for i, vals := range pt.pool.copyVals {
		if z, ok := vals[1].(int16); !ok || z != wantZone[i] {
			t.Errorf("строка COPY %d: zone_id = %v, want %d", i, vals[1], wantZone[i])
		}
	}

	// The report in the journal: a zone-less file writes ZoneID 0 (NULL in the DB).
	if len(pt.files.records) != 1 {
		t.Fatalf("записей в журнале = %d, want 1", len(pt.files.records))
	}
	rec := pt.files.records[0]
	if rec.ZoneID != 0 || rec.Path != "add/list.txt" || rec.Added != 3 {
		t.Errorf("запись журнала = %+v, want ZoneID 0, путь add/list.txt, added 3", rec)
	}

	// The file goes to processed/ keeping the structure (no zone directory).
	if exists(t, abs) {
		t.Error("файл остался в incoming")
	}
	if !exists(t, filepath.Join(pt.dir, DirProcessed, "add/list.txt")) {
		t.Error("файл не перемещён в processed/add/list.txt")
	}
}

// TestPipelineZonelessNumericZone — fictitious numeric zones do not arise:
// in a zone-less file the date lines (the T-132 extraction candidates from
// the registrar export fields) are rejected, the zone is neither derived nor
// created (the .SU dump regression: the line "SOCHI.SU <registrar> <date>…"
// was applied as the domain "18.06.1999" with the automatic zone 1999). The
// bare suffix sochi.su is valid as the domain su (junk without the filter,
// the tld-only-zones spec §12.6), a line of dates only — no_domain.
func TestPipelineZonelessNumericZone(t *testing.T) {
	pt := newPipeTest(t)
	content := "0-0.SU\tARDIS-SU\t17.11.2015\t17.11.2026\t20.12.2026\t1\n" +
		"SOCHI.SU\tRUCENTER-SU\t18.06.1999\t01.10.2027\t01.11.2027\t1\n" +
		"RUCENTER-AR\t18.06.1999\t01.10.2027\t01.11.2027\t1\n"
	abs := pt.addFile(t, "add/su_dump.txt", content)

	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")}, // the cleanup before loading (Stage)
		// the insert/revive — the Query answers (inserted=2, revived=0)
		{tag: pgconn.NewCommandTag("DELETE 2")}, // the cleanup after the apply
	}
	pt.pool.inserted = 2
	pt.pool.revived = 0
	pt.pool.uniques = 2

	// A third of the lines are rejected — the default threshold of 10%
	// would be enough to fail the file; what is checked here is the apply,
	// not the threshold.
	pipe, err := NewPipeline(pt.zones, pt.files, pt.pool, PipelineOptions{
		Dir:           pt.dir,
		BatchSize:     10,
		MaxErrorRatio: 0.5, // 1 rejection out of 3 — the file does not fail (strictly above)
		Now:           func() time.Time { return pt.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	out, err := pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Kind != OutcomeApplied {
		t.Fatalf("итог = %s, want applied", out.Kind)
	}
	rep := out.Report
	if rep.LinesTotal != 3 || rep.Accepted != 2 || rep.Rejected != 1 || rep.Added != 2 {
		t.Errorf("счётчики = %+v, want 3 строки, 2 валидных/добавлено, 1 отклонение", rep)
	}
	if rep.Reasons[ReasonNoDomain] != 1 || len(rep.Reasons) != 1 {
		t.Errorf("Reasons = %+v, want ровно no_domain: 1", rep.Reasons)
	}

	// The file's only zone is su: the fictitious "1999" is not created.
	if want := []string{"byName:su", "create:su"}; !reflect.DeepEqual(pt.zones.calls, want) {
		t.Errorf("обращения к зонам = %v, want %v", pt.zones.calls, want)
	}
	// Two su domains went into staging — not dates: an ordinary label and
	// the bare suffix sochi (valid junk, the tld-only-zones spec §12.6).
	if len(pt.pool.copyVals) != 2 {
		t.Fatalf("строк COPY = %d, want 2", len(pt.pool.copyVals))
	}
	for i, want := range []string{"0-0", "sochi"} {
		if vals := pt.pool.copyVals[i]; vals[2] != want {
			t.Errorf("метка COPY[%d] = %v, want %s (не дата)", i, vals[2], want)
		}
	}
	if !exists(t, filepath.Join(pt.dir, DirProcessed, "add/su_dump.txt")) {
		t.Error("файл не перемещён в processed/")
	}
}

// TestPipelineZonelessDuplicate — exact redelivery of a zone-less file: a
// match (NULL zone, action, sha256) — not applied, moved to processed
// without a new report (T-113).
func TestPipelineZonelessDuplicate(t *testing.T) {
	pt := newPipeTest(t)
	content := "example.com\nfoo.ru\n"
	abs := pt.addFile(t, "add/list.txt", content)
	sha := sha256.Sum256([]byte(content))
	processed := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	add := model.ActionAdd
	path := "add/first.txt"
	pt.files = newFakeFileStore(
		model.IngestFile{ // the previous delivery of the same content — zone-less too
			ID: 5, ZoneID: nil, Action: &add, Path: &path,
			SHA256: sha[:], ProcessedAt: &processed,
		},
	)
	pipe, err := NewPipeline(pt.zones, pt.files, pt.pool, PipelineOptions{
		Dir: pt.dir, BatchSize: 10, Now: func() time.Time { return pt.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	pt.pipe = pipe

	out, err := pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Kind != OutcomeDuplicate {
		t.Fatalf("итог = %s, want duplicate", out.Kind)
	}
	if len(pt.files.records) != 0 {
		t.Errorf("записей в журнале = %d, want 0 (дубль не пишет отчёт)", len(pt.files.records))
	}
	if !exists(t, filepath.Join(pt.dir, DirProcessed, "add/list.txt")) {
		t.Error("дубль не перемещён в processed/")
	}
}

// TestPipelineDelete — a successful delete: the domains are moved to
// deleted, the Deleted/NotFound counters in the report.
func TestPipelineDelete(t *testing.T) {
	pt := newPipeTest(t)
	content := "a.net\nb.net\n"
	abs := pt.addFile(t, "net/delete/x.txt", content)
	sha := sha256.Sum256([]byte(content))

	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")}, // the cleanup before loading
		{tag: pgconn.NewCommandTag("DELETE 2")}, // the cleanup after the apply
	}
	pt.pool.delDeleted = 1
	pt.pool.delMissing = 1

	out, err := pt.pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Kind != OutcomeApplied {
		t.Fatalf("итог = %s, want applied", out.Kind)
	}
	rep := out.Report
	if rep.Action != model.ActionDelete || rep.Deleted != 1 || rep.NotFound != 1 {
		t.Errorf("счётчики удаления = %+v, want deleted 1, not_found 1", rep)
	}

	if len(pt.files.records) != 1 {
		t.Fatalf("записей в журнале = %d, want 1", len(pt.files.records))
	}
	rec := pt.files.records[0]
	if rec.Deleted != 1 || rec.Action != model.ActionDelete || !reflect.DeepEqual(rec.SHA256, sha[:]) {
		t.Errorf("запись журнала = %+v, want deleted 1, sha256 содержимого", rec)
	}
	// NotFound — the log report only: there is no column in ingest_files (spec 6.1).
	if rec.Added != 0 || rec.Existed != 0 || rec.Rejected != 0 {
		t.Errorf("запись журнала = %+v, want нулевые add-счётчики", rec)
	}

	want := []string{
		"exec:" + clearStageSQL,
		"copy:2",
		"begin", // the delete apply transaction (T-269): the status flip and
		// the "bit × counter" pair — two statements of one transaction
		"query:" + applyDeleteSQL,
		"commit",
		"exec:" + clearStageSQL,
	}
	if !reflect.DeepEqual(pt.pool.ops, want) {
		t.Errorf("порядок обращений =\n%s\nwant:\n%s",
			strings.Join(pt.pool.ops, "\n"), strings.Join(want, "\n"))
	}
	if !exists(t, filepath.Join(pt.dir, DirProcessed, "net/delete/x.txt")) {
		t.Error("файл не перемещён в processed/net/delete/x.txt")
	}
}

// TestPipelineExtractLines — the mixed lines of a file (T-132): the
// extracted domains are loaded into staging and applied, the lines without
// a domain — no_domain rejections (the rejected log + the Reasons cut of
// the report), the counters stay the same (added/rejected, no new fields).
func TestPipelineExtractLines(t *testing.T) {
	pt := newPipeTest(t)
	content := "xxx.com 2029-11-11 TIMEWEB\n" + // a customer example
		"https://shop.com/page\n" + // a URL with a path
		"plain.com\n" +
		"нет домена тут\n" // no_domain
	abs := pt.addFile(t, "com/add/list.txt", content)
	sha := sha256.Sum256([]byte(content))

	var buf strings.Builder
	pipe, err := NewPipeline(pt.zones, pt.files, pt.pool, PipelineOptions{
		Dir:           pt.dir,
		BatchSize:     10,
		MaxErrorRatio: 0.5, // 1 rejection out of 4 — the file does not fail
		RejectLog:     NewRejectedLogger(&buf, slog.LevelInfo),
		Now:           func() time.Time { return pt.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")}, // the cleanup before loading (Stage)
		// the insert/revive — the Query answers (inserted=3, revived=0)
		{tag: pgconn.NewCommandTag("DELETE 4")}, // the cleanup after the apply
	}
	pt.pool.inserted = 3
	pt.pool.revived = 0
	pt.pool.uniques = 3

	out, err := pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Kind != OutcomeApplied {
		t.Fatalf("итог = %s, want applied", out.Kind)
	}
	rep := out.Report
	if rep.LinesTotal != 4 || rep.Accepted != 3 || rep.Rejected != 1 || rep.Added != 3 {
		t.Errorf("счётчики = %+v, want 4 строки, 3 валидных/добавлено, 1 отклонение", rep)
	}
	if rep.Reasons[ReasonNoDomain] != 1 || len(rep.Reasons) != 1 {
		t.Errorf("Reasons = %+v, want ровно no_domain: 1", rep.Reasons)
	}
	if !reflect.DeepEqual(rep.SHA256, sha[:]) {
		t.Errorf("sha256 = %x, want хэш содержимого", rep.SHA256)
	}
	if len(pt.files.records) != 1 || pt.files.records[0].Rejected != 1 || pt.files.records[0].Added != 3 {
		t.Errorf("журнал = %+v, want rejected 1, added 3", pt.files.records)
	}
	// Three extracted domains went into staging (not the original lines).
	want := []string{
		"exec:" + clearStageSQL,
		"copy:3",
		"begin",
		"query:" + addChunkBoundSQL,
		"query:" + insertAddSQL,
		"query:" + addChunkBoundSQL,
		"exec:" + zoneDeltaAddSQL,
		"query:" + reviveAddSQL,
		"query:" + countAddSQL,
		"exec:" + clearStageSQL,
		"commit",
	}
	if !reflect.DeepEqual(pt.pool.ops, want) {
		t.Errorf("порядок обращений =\n%s\nwant:\n%s",
			strings.Join(pt.pool.ops, "\n"), strings.Join(want, "\n"))
	}
	// The line without a domain — in the rejected log with the original text
	// and the reason (slog quotes text with spaces).
	logged := buf.String()
	for _, wantFrag := range []string{
		"file=com/add/list.txt",
		"line=4",
		`text="нет домена тут"`,
		"reason=no_domain",
	} {
		if !strings.Contains(logged, wantFrag) {
			t.Errorf("rejected-лог = %q, want запись с %q", logged, wantFrag)
		}
	}
	if !exists(t, filepath.Join(pt.dir, DirProcessed, "com/add/list.txt")) {
		t.Error("файл не перемещён в processed/")
	}
}

// TestPipelineDuplicate — exact redelivery (T-113): the same triple (zone,
// action, sha256) is already in the journal (including among other
// deliveries of the same content — the exact match wins): the batch is
// discarded, the apply does not run, the report is not duplicated, the file
// goes to processed/.
func TestPipelineDuplicate(t *testing.T) {
	pt := newPipeTest(t)
	content := "example.com\n"
	abs := pt.addFile(t, "com/add/list.txt", content)
	sha := sha256.Sum256([]byte(content))
	processed := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	zone, otherZone := int16(1), int16(9)
	add, del := model.ActionAdd, model.ActionDelete
	path, otherPath, undoPath := "com/add/list.txt", "net/add/list.txt", "com/delete/x.txt"
	pt.files = newFakeFileStore(
		model.IngestFile{ // another action, the same zone (undo)
			ID: 6, ZoneID: &zone, Action: &del, Path: &undoPath,
			SHA256: sha[:], ProcessedAt: &processed,
		},
		model.IngestFile{ // exact redelivery
			ID: 5, ZoneID: &zone, Action: &add, Path: &path,
			SHA256: sha[:], ProcessedAt: &processed,
		},
		model.IngestFile{ // the same action, another zone
			ID: 7, ZoneID: &otherZone, Action: &add, Path: &otherPath,
			SHA256: sha[:], ProcessedAt: &processed,
		},
	)
	pipe, err := NewPipeline(pt.zones, pt.files, pt.pool, PipelineOptions{
		Dir: pt.dir, BatchSize: 10, Now: func() time.Time { return pt.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	pt.pipe = pipe

	out, err := pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Kind != OutcomeDuplicate {
		t.Fatalf("итог = %s, want duplicate", out.Kind)
	}
	if len(pt.files.records) != 0 {
		t.Errorf("записей в журнале = %d, want 0 (дубль не перезаписывает отчёт)", len(pt.files.records))
	}

	// The loading happened (the hash is known only after reading), the apply
	// did not: cleanup → COPY → the batch discard.
	want := []string{
		"exec:" + clearStageSQL,
		"copy:1",
		"exec:" + clearStageSQL,
	}
	if !reflect.DeepEqual(pt.pool.ops, want) {
		t.Errorf("порядок обращений =\n%s\nwant:\n%s",
			strings.Join(pt.pool.ops, "\n"), strings.Join(want, "\n"))
	}
	if !exists(t, filepath.Join(pt.dir, DirProcessed, "com/add/list.txt")) {
		t.Error("дубль не перемещён в processed/")
	}
}

// TestPipelineDuplicateOtherZoneWarn — a sha256 duplicate in another zone
// with the same action (T-113): no exact redelivery, the file is applied
// (the first delivery did not touch this zone's domains) — but a Warn is
// highlighted to the operator (ING-P2-5, a non-contract part).
func TestPipelineDuplicateOtherZoneWarn(t *testing.T) {
	pt := newPipeTest(t)
	content := "example.com\n"
	abs := pt.addFile(t, "com/add/list.txt", content)
	sha := sha256.Sum256([]byte(content))
	zone := int16(9)
	action := model.ActionAdd
	path := "net/add/list.txt"
	processed := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	pt.files = newFakeFileStore(model.IngestFile{
		ID: 7, ZoneID: &zone, Action: &action, Path: &path,
		SHA256: sha[:], ProcessedAt: &processed,
	})

	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")}, // the cleanup before loading (Stage)
		// the insert/revive — the Query answers (inserted=1, revived=0)
		{tag: pgconn.NewCommandTag("DELETE 1")}, // the cleanup after the apply
	}
	pt.pool.inserted = 1
	pt.pool.revived = 0
	pt.pool.uniques = 1

	var buf bytes.Buffer
	pipe, err := NewPipeline(pt.zones, pt.files, pt.pool, PipelineOptions{
		Dir:       pt.dir,
		BatchSize: 10,
		Log:       slog.New(slog.NewTextHandler(&buf, nil)),
		Now:       func() time.Time { return pt.now },
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Kind != OutcomeApplied {
		t.Fatalf("итог = %s, want applied (другая зона — не дубль, T-113)", out.Kind)
	}
	if len(pt.files.records) != 1 {
		t.Error("повторная обработка в другой зоне пишет свой отчёт")
	}
	log := buf.String()
	for _, want := range []string{
		"дубль по sha256 в другой зоне при том же действии",
		"zone_id=1",
		"first_zone_id=9",
		"first_path=net/add/list.txt",
	} {
		if !strings.Contains(log, want) {
			t.Errorf("лог = %q, want запись с %q", log, want)
		}
	}
}

// TestPipelineUndoSameContentApplied — an undo by the same list (T-113):
// the same content was delivered to delete/ after add/ — a different
// action, no exact redelivery, the file is processed: the domains are
// deleted, the report in the journal. It overrides the T-097 pinning
// (TestIngest_SameContentCrossActionSkipped, the dedup contract was
// replaced by T-113).
func TestPipelineUndoSameContentApplied(t *testing.T) {
	pt := newPipeTest(t)
	content := "example.com\n"
	abs := pt.addFile(t, "com/delete/x.txt", content)
	sha := sha256.Sum256([]byte(content))
	zone := int16(1)
	action := model.ActionAdd
	path := "com/add/list.txt"
	processed := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	pt.files = newFakeFileStore(model.IngestFile{
		ID: 7, ZoneID: &zone, Action: &action, Path: &path,
		SHA256: sha[:], ProcessedAt: &processed,
	})

	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")}, // the cleanup before loading (Stage)
		{tag: pgconn.NewCommandTag("DELETE 1")}, // the cleanup after the apply
	}
	pt.pool.delDeleted = 1

	var buf bytes.Buffer
	pipe, err := NewPipeline(pt.zones, pt.files, pt.pool, PipelineOptions{
		Dir:       pt.dir,
		BatchSize: 10,
		Log:       slog.New(slog.NewTextHandler(&buf, nil)),
		Now:       func() time.Time { return pt.now },
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Kind != OutcomeApplied {
		t.Fatalf("итог = %s, want applied (другое действие — не дубль, undo)", out.Kind)
	}
	if out.Report.Deleted != 1 {
		t.Errorf("undo: deleted = %d, want 1", out.Report.Deleted)
	}
	if len(pt.files.records) != 1 || pt.files.records[0].Action != model.ActionDelete {
		t.Errorf("журнал = %+v, want строка delete для undo-файла", pt.files.records)
	}
	if !strings.Contains(buf.String(), "уже доставлялось другим действием") {
		t.Errorf("лог = %q, want Info об undo (ранее — Warn о пропуске, ING-P2-5)", buf.String())
	}
	if !exists(t, filepath.Join(pt.dir, DirProcessed, "com/delete/x.txt")) {
		t.Error("undo-файл не перемещён в processed/")
	}
}

// TestPipelineErrorRatio — the rejected-line ratio above the threshold
// (T-016): the file is failed as a whole — the apply does not run, the file
// → failed/ with a log report next to it, the report is not written into
// ingest_files (the hash of a failed file does not occupy the journal — the
// fixed file can be loaded again).
func TestPipelineErrorRatio(t *testing.T) {
	pt := newPipeTest(t)
	content := "a.org\nb.org\nc.com\n" // 2/3 rejections (wrong_zone) > 0.1
	abs := pt.addFile(t, "com/add/list.txt", content)
	sha := sha256.Sum256([]byte(content))

	out, err := pt.pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v (ошибочный файл — не ошибка сервиса)", err)
	}
	if out.Kind != OutcomeFailed {
		t.Fatalf("итог = %s, want failed", out.Kind)
	}
	if out.Err == nil || !strings.Contains(out.Err.Error(), "ingest.max_error_ratio") {
		t.Errorf("причина = %v, want доля отклонённых строк выше порога", out.Err)
	}

	rep := out.Report
	if rep.Rejected != 2 || rep.Accepted != 1 || rep.Added != 0 {
		t.Errorf("счётчики = %+v, want rejected 2, accepted 1, применения нет", rep)
	}
	if !reflect.DeepEqual(rep.SHA256, sha[:]) {
		t.Errorf("sha256 = %x, want хэш содержимого", rep.SHA256)
	}
	if len(pt.files.records) != 0 {
		t.Errorf("записей в журнале = %d, want 0", len(pt.files.records))
	}

	// The loading happened, the apply did not; the batch was discarded.
	want := []string{
		"exec:" + clearStageSQL,
		"copy:1", // c.com
		"exec:" + clearStageSQL,
	}
	if !reflect.DeepEqual(pt.pool.ops, want) {
		t.Errorf("порядок обращений =\n%s\nwant:\n%s",
			strings.Join(pt.pool.ops, "\n"), strings.Join(want, "\n"))
	}

	// The file — in failed/ with a log report next to it (spec 5.1).
	failed := filepath.Join(pt.dir, DirFailed, "com/add/list.txt")
	if exists(t, abs) || !exists(t, failed) {
		t.Fatalf("файл должен лежать в %s, исходный удалён", failed)
	}
	report := readFile(t, failed+".report")
	for _, frag := range []string{
		"файл: com/add/list.txt",
		"итог: ошибочен — доля отклонённых строк 2 из 3 выше ingest.max_error_ratio (0.1)",
		"    wrong_zone: 2",
		"  валидных: 1",
		fmt.Sprintf("sha256: %x", sha[:]),
	} {
		if !strings.Contains(report, frag) {
			t.Errorf("лог-отчёт не содержит %q:\n%s", frag, report)
		}
	}
}

// TestPipelineParserRejectsFailFile — the parser's per-line rejections
// (invalid_utf8, line_len) are counted in the error ratio threshold
// (T-016): a file whose junk did not reach the validator is still failed as
// a whole (a broken dump is exactly non-UTF-8, docs/incoming-files.md §4),
// not a "successfully applied" empty batch.
func TestPipelineParserRejectsFailFile(t *testing.T) {
	// 1 non-UTF-8 line + 8 valid ones: the validator alone does not fail
	// 0/8, the combined ratio 1/9 > 0.1 — the file is failed.
	t.Run("мусор плюс валидные строки", func(t *testing.T) {
		pt := newPipeTest(t)
		var b strings.Builder
		b.WriteString("\xff\xfe\n")
		for i := 0; i < 8; i++ {
			fmt.Fprintf(&b, "good%d.com\n", i)
		}
		abs := pt.addFile(t, "com/add/list.txt", b.String())

		out, err := pt.pipe.Process(context.Background(), abs)
		if err != nil {
			t.Fatalf("Process: %v (ошибочный файл — не ошибка сервиса)", err)
		}
		if out.Kind != OutcomeFailed {
			t.Fatalf("итог = %s, want failed", out.Kind)
		}
		if out.Err == nil || !strings.Contains(out.Err.Error(), "доля отклонённых строк 1 из 9 выше ingest.max_error_ratio (0.1)") {
			t.Errorf("причина = %v, want объединённая доля 1 из 9", out.Err)
		}
		rep := out.Report
		if rep.Rejected != 1 || rep.Accepted != 8 || rep.Reasons[ReasonUTF8] != 1 {
			t.Errorf("счётчики = %+v, want rejected 1 (invalid_utf8), accepted 8", rep)
		}
		if len(pt.files.records) != 0 {
			t.Error("отчёт ошибочного файла не пишется в журнал")
		}
		if !exists(t, filepath.Join(pt.dir, DirFailed, "com/add/list.txt")) {
			t.Error("файл не перемещён в failed/")
		}
	})

	// A file entirely of non-UTF-8: the parser rejected every line, the
	// validator saw none (total = 0) — without counting the parser's
	// rejections the ratio would be computed as 0/0 and the file would go to
	// processed/.
	t.Run("файл целиком из не-UTF-8", func(t *testing.T) {
		pt := newPipeTest(t)
		abs := pt.addFile(t, "com/add/dump.txt", "\xff\xfe\xff\n\xff\xfe\n")

		out, err := pt.pipe.Process(context.Background(), abs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if out.Kind != OutcomeFailed {
			t.Fatalf("итог = %s, want failed", out.Kind)
		}
		if out.Err == nil || !strings.Contains(out.Err.Error(), "доля отклонённых строк 2 из 2 выше ingest.max_error_ratio (0.1)") {
			t.Errorf("причина = %v, want объединённая доля 2 из 2", out.Err)
		}
		if rep := out.Report; rep.Rejected != 2 || rep.Accepted != 0 {
			t.Errorf("счётчики = %+v, want rejected 2, accepted 0", rep)
		}
		// The zero-row batch was discarded, the apply did not run.
		want := []string{"exec:" + clearStageSQL, "exec:" + clearStageSQL}
		if !reflect.DeepEqual(pt.pool.ops, want) {
			t.Errorf("обращения = %v, want очистка → отбрасывание партии без COPY", pt.pool.ops)
		}
		if len(pt.files.records) != 0 {
			t.Error("отчёт ошибочного файла не пишется в журнал")
		}
	})
}

// TestPipelineBadPath — the path structure is not <zone>/add|delete/*.txt
// (T-014): the file is failed, nothing reaches the DB, failed/ with a log
// report.
func TestPipelineBadPath(t *testing.T) {
	pt := newPipeTest(t)
	abs := pt.addFile(t, "com/list.txt", "example.com\n")

	out, err := pt.pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Kind != OutcomeFailed {
		t.Fatalf("итог = %s, want failed", out.Kind)
	}
	if kind, ok := PathKindOf(out.Err); !ok || kind != ErrPathStructure {
		t.Errorf("причина = %v, want ErrPathStructure", out.Err)
	}
	if len(pt.pool.ops) != 0 || len(pt.files.records) != 0 || len(pt.zones.calls) != 0 {
		t.Error("при ошибке пути обращений к БД быть не должно")
	}
	failed := filepath.Join(pt.dir, DirFailed, "com/list.txt")
	if !exists(t, failed) {
		t.Fatalf("файл не перемещён в %s", failed)
	}
	report := readFile(t, failed+".report")
	if !strings.Contains(report, "итог: ошибочен — ingest: структура пути не <зона>/add|delete/<файл>") {
		t.Errorf("лог-отчёт без причины:\n%s", report)
	}
}

// TestPipelineBadZoneDir — the zone directory name does not normalize
// (T-014): the file is failed (failed/), the zone is not created.
func TestPipelineBadZoneDir(t *testing.T) {
	pt := newPipeTest(t)
	abs := pt.addFile(t, "не%зона/add/list.txt", "example.com\n")

	out, err := pt.pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Kind != OutcomeFailed {
		t.Fatalf("итог = %s, want failed", out.Kind)
	}
	if kind, ok := PathKindOf(out.Err); !ok || kind != ErrPathZone {
		t.Errorf("причина = %v, want ErrPathZone", out.Err)
	}
	if len(pt.zones.calls) != 0 {
		t.Errorf("зона не должна создаваться, calls = %v", pt.zones.calls)
	}
	if !exists(t, filepath.Join(pt.dir, DirFailed, "не%зона/add/list.txt")) {
		t.Error("файл не перемещён в failed/")
	}
}

// TestPipelineZonePathAllowlist — a multi-level zone directory
// (incoming/<zone with a dot>/) outside the ingest.allowed_zone_paths
// allowlist — a file-level error (the tld-only-zones spec §5.1): the file in
// failed/ with the reason, the lines are not parsed, the zone is not
// created; a zone from the allowlist (crawl.test in the testbed) is
// processed normally by branch 3 of domainLabel (the tld-only-zones
// spec §5.2).
func TestPipelineZonePathAllowlist(t *testing.T) {
	t.Run("вне allowlist — failed", func(t *testing.T) {
		pt := newPipeTest(t)
		abs := pt.addFile(t, "crawl.test/add/list.txt", "example.crawl.test\n")

		out, err := pt.pipe.Process(context.Background(), abs)
		if err != nil {
			t.Fatalf("Process: %v (ошибочный файл — не ошибка сервиса)", err)
		}
		if out.Kind != OutcomeFailed {
			t.Fatalf("итог = %s, want failed", out.Kind)
		}
		if !errors.Is(out.Err, errZoneNotAllowed) {
			t.Errorf("причина = %v, want errZoneNotAllowed", out.Err)
		}
		// The lines are not parsed, the zone is not created, the batch is not loaded.
		if len(pt.pool.ops) != 0 || len(pt.files.records) != 0 || len(pt.zones.calls) != 0 {
			t.Errorf("при запрете зоны обращений к БД быть не должно: ops=%v records=%d zones=%v",
				pt.pool.ops, len(pt.files.records), pt.zones.calls)
		}
		failed := filepath.Join(pt.dir, DirFailed, "crawl.test/add/list.txt")
		if !exists(t, failed) {
			t.Fatalf("файл не перемещён в %s", failed)
		}
		if report := readFile(t, failed+".report"); !strings.Contains(report, "ingest.allowed_zone_paths") {
			t.Errorf("лог-отчёт без причины allowlist:\n%s", report)
		}
	})
	t.Run("allowlist — applied", func(t *testing.T) {
		pt := newPipeTest(t)
		pipe, err := NewPipeline(pt.zones, pt.files, pt.pool, PipelineOptions{
			Dir:              pt.dir,
			BatchSize:        10,
			AllowedZonePaths: []string{"crawl.test"},
			Now:              func() time.Time { return pt.now },
		})
		if err != nil {
			t.Fatal(err)
		}
		abs := pt.addFile(t, "crawl.test/add/list.txt", "example.crawl.test\n")

		pt.pool.execs = []pipeExec{
			{tag: pgconn.NewCommandTag("DELETE 0")}, // the cleanup before loading (Stage)
			{tag: pgconn.NewCommandTag("DELETE 1")}, // the cleanup after the apply
		}
		pt.pool.inserted = 1
		pt.pool.uniques = 1

		out, err := pipe.Process(context.Background(), abs)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if out.Kind != OutcomeApplied {
			t.Fatalf("итог = %s, want applied", out.Kind)
		}
		// Branch 3 of domainLabel (the tld-only-zones spec §5.2):
		// example.crawl.test in the crawl.test directory — the label example,
		// the path zone crawl.test. The line label is checked against the
		// staging batch (COPY: batch, zone, label, …).
		if rep := out.Report; rep.Zone != "crawl.test" || rep.Added != 1 {
			t.Errorf("отчёт = %+v, want зона crawl.test, added 1", rep)
		}
		if len(pt.pool.copyVals) != 1 {
			t.Fatalf("строк в staging = %d, want 1", len(pt.pool.copyVals))
		}
		if label := pt.pool.copyVals[0][2].(string); label != "example" {
			t.Errorf("метка строки = %q, want example (ветка 3 domainLabel)", label)
		}
		if !exists(t, filepath.Join(pt.dir, DirProcessed, "crawl.test/add/list.txt")) {
			t.Error("файл не перемещён в processed/crawl.test/")
		}
	})
}

// TestPipelineFailFileOutsideIncoming — Process is a public API: a path
// outside incoming (a rel with ".." after filepath.Rel, ParsePath rejects
// it) must not move the archive outside failed/ (ING-P3-1): the destination
// is sanitized to the file's base name.
func TestPipelineFailFileOutsideIncoming(t *testing.T) {
	pt := newPipeTest(t)
	// A sibling file of the incoming directory: rel = "../escape/com/list.txt".
	abs := filepath.Join(filepath.Dir(pt.dir), "escape", "com", "list.txt")
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(abs, []byte("example.com\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	out, err := pt.pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v (ошибочный файл — не ошибка сервиса)", err)
	}
	if out.Kind != OutcomeFailed {
		t.Fatalf("итог = %s, want failed", out.Kind)
	}
	failed := filepath.Join(pt.dir, DirFailed, "list.txt")
	if exists(t, abs) || !exists(t, failed) {
		t.Fatalf("файл должен лежать в %s, исходный удалён", failed)
	}
	if !exists(t, failed+".report") {
		t.Error("лог-отчёт не записан рядом с файлом")
	}
	// The archive did not leave failed/ (and did not "fall through" into ../).
	if exists(t, filepath.Join(pt.dir, "escape", "com", "list.txt")) {
		t.Error("файл оказался снаружи failed/ — путь назначения не санирован")
	}
}

// TestPipelineZoneDBError — a DB failure at the zone stage: the file stays
// in incoming (at-least-once, spec 3.4), the outcome is left, the error goes
// to the caller.
func TestPipelineZoneDBError(t *testing.T) {
	pt := newPipeTest(t)
	abs := pt.addFile(t, "com/add/list.txt", "example.com\n")
	pt.zones.byNameErr = errors.New("бд недоступна")

	out, err := pt.pipe.Process(context.Background(), abs)
	if err == nil || !errors.Is(err, pt.zones.byNameErr) {
		t.Fatalf("err = %v, want ошибка БД вызывающему", err)
	}
	if out.Kind != OutcomeLeft {
		t.Errorf("итог = %s, want left", out.Kind)
	}
	if !exists(t, abs) {
		t.Error("файл должен остаться в incoming для повтора (ТЗ 3.4)")
	}
	if len(pt.pool.ops) != 0 || len(pt.files.records) != 0 {
		t.Error("после сбоя зоны обращений к staging/журналу быть не должно")
	}
}

// TestPipelineStageError — a COPY failure: the file stays in incoming, the
// batch is discarded (staging does not grow), a retry will take a new batch.
func TestPipelineStageError(t *testing.T) {
	pt := newPipeTest(t)
	abs := pt.addFile(t, "com/add/list.txt", "example.com\n")
	pt.pool.copyErr = errors.New("COPY отвергнут")

	out, err := pt.pipe.Process(context.Background(), abs)
	if err == nil || !errors.Is(err, pt.pool.copyErr) {
		t.Fatalf("err = %v, want ошибка COPY вызывающему", err)
	}
	if out.Kind != OutcomeLeft {
		t.Errorf("итог = %s, want left", out.Kind)
	}
	if !exists(t, abs) {
		t.Error("файл должен остаться в incoming")
	}
	// cleanup (Stage) → COPY (failure) → the batch discard (best effort)
	if len(pt.pool.ops) != 3 || pt.pool.ops[0] != "exec:"+clearStageSQL ||
		!strings.HasPrefix(pt.pool.ops[1], "copy:") || pt.pool.ops[2] != "exec:"+clearStageSQL {
		t.Errorf("обращения = %v, want очистка, COPY, отбрасывание партии", pt.pool.ops)
	}
	if len(pt.files.records) != 0 {
		t.Error("журнал не пишется при сбое загрузки")
	}
}

// TestPipelineApplyError — an add apply failure: the file stays, the report
// is not written (the apply did not happen as a whole), the batch is
// discarded.
func TestPipelineApplyError(t *testing.T) {
	pt := newPipeTest(t)
	abs := pt.addFile(t, "com/add/list.txt", "example.com\n")
	boom := errors.New("сервер закрыл соединение")
	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")}, // Stage
		{err: boom},                             // the insert
	}

	out, err := pt.pipe.Process(context.Background(), abs)
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("err = %v, want ошибка применения", err)
	}
	if out.Kind != OutcomeLeft || !exists(t, abs) {
		t.Fatalf("итог = %s, файл на месте = %v, want left и файл в incoming", out.Kind, exists(t, abs))
	}
	if len(pt.files.records) != 0 {
		t.Error("отчёт не пишется при сбое применения")
	}
	// the insert without the ApplyAdd cleanup (failure) → the pipeline discard
	if !strings.Contains(strings.Join(pt.pool.ops, "\n"), "query:"+insertAddSQL) {
		t.Errorf("обращения = %v, want попытка вставки", pt.pool.ops)
	}
	if pt.pool.ops[len(pt.pool.ops)-1] != "exec:"+clearStageSQL {
		t.Errorf("последнее обращение = %v, want отбрасывание партии", pt.pool.ops[len(pt.pool.ops)-1])
	}
}

// TestPipelineRecordError — a report write failure: the apply happened, but
// the file stays in incoming — a retry will find the sha256 duplicate and
// only move the file (self-recovery, spec 3.4/5.1).
func TestPipelineRecordError(t *testing.T) {
	pt := newPipeTest(t)
	abs := pt.addFile(t, "com/add/list.txt", "example.com\n")
	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")},
		// the insert/revive — the Query answers (inserted=1, revived=0)
		{tag: pgconn.NewCommandTag("DELETE 1")},
	}
	pt.pool.inserted = 1
	pt.pool.revived = 0
	pt.pool.uniques = 1
	pt.files.recErr = errors.New("журнал недоступен")

	out, err := pt.pipe.Process(context.Background(), abs)
	if err == nil || !errors.Is(err, pt.files.recErr) {
		t.Fatalf("err = %v, want ошибка журнала", err)
	}
	if out.Kind != OutcomeLeft || !exists(t, abs) {
		t.Fatalf("итог = %s, want left и файл в incoming", out.Kind)
	}
	// The apply happened fully (the transaction: the batch boundaries/insert,
	// the zone deltas, the revive, the count, the cleanup, the commit) — on
	// a retry the sha256 duplicate will only move the file.
	if len(pt.pool.ops) != 11 {
		t.Errorf("обращений к БД = %d (%v), want 11 — применение завершено", len(pt.pool.ops), pt.pool.ops)
	}
	if pt.pool.commits != 1 {
		t.Errorf("фиксаций = %d, want 1", pt.pool.commits)
	}
}

// TestPipelineDuplicateCheckError — a duplicate check failure: the file
// stays, the apply does not run.
func TestPipelineDuplicateCheckError(t *testing.T) {
	pt := newPipeTest(t)
	abs := pt.addFile(t, "com/add/list.txt", "example.com\n")
	pt.files.bySAErr = errors.New("журнал недоступен")

	out, err := pt.pipe.Process(context.Background(), abs)
	if err == nil || !errors.Is(err, pt.files.bySAErr) {
		t.Fatalf("err = %v, want ошибка журнала", err)
	}
	if out.Kind != OutcomeLeft || !exists(t, abs) {
		t.Fatalf("итог = %s, want left и файл в incoming", out.Kind)
	}
	for _, op := range pt.pool.ops {
		if strings.Contains(op, insertAddSQL) || strings.Contains(op, applyDeleteSQL) {
			t.Errorf("применение выполнено несмотря на сбой проверки дубля: %v", pt.pool.ops)
		}
	}
}

// TestPipelineContextCanceled — a canceled context: like real pgx, the fake
// refuses the first call; the file stays (graceful shutdown finishes it or
// returns it, spec 3.4).
func TestPipelineContextCanceled(t *testing.T) {
	pt := newPipeTest(t)
	abs := pt.addFile(t, "com/add/list.txt", "example.com\n")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	out, err := pt.pipe.Process(ctx, abs)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if out.Kind != OutcomeLeft {
		t.Errorf("итог = %s, want left", out.Kind)
	}
	if !exists(t, abs) {
		t.Error("файл должен остаться в incoming")
	}
	if len(pt.files.records) != 0 {
		t.Error("журнал не пишется при отмене")
	}
	// The batch is cleared after the ctx cancellation too: discard cleans
	// staging with its own context (ING-P2-3) — the rows do not pile up in
	// staging from restart to restart. (The cleanup inside Stage with a
	// canceled ctx did not reach the DB — the fake, like pgx, refuses before
	// recording the call.)
	if len(pt.pool.ops) != 1 || pt.pool.ops[0] != "exec:"+clearStageSQL {
		t.Errorf("обращения = %v, want одна очистка отброшенной партии", pt.pool.ops)
	}
}

// TestPipelineMoveCollision — the archive already has a file with the same
// name (another instance): the new one gets a numeric suffix, the existing
// one is not overwritten.
func TestPipelineMoveCollision(t *testing.T) {
	pt := newPipeTest(t)
	content := "example.com\n"
	abs := pt.addFile(t, "com/add/list.txt", content)
	// Another instance with the same name was processed earlier.
	older := filepath.Join(pt.dir, DirProcessed, "com/add/list.txt")
	if err := os.MkdirAll(filepath.Dir(older), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(older, []byte("older\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")},
		// the insert/revive — the Query answers (inserted=1, revived=0)
		{tag: pgconn.NewCommandTag("DELETE 1")},
	}
	pt.pool.inserted = 1
	pt.pool.revived = 0
	pt.pool.uniques = 1

	out, err := pt.pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Kind != OutcomeApplied {
		t.Fatalf("итог = %s, want applied", out.Kind)
	}
	if !exists(t, older) || readFile(t, older) != "older\n" {
		t.Error("существующий файл архива затёрт")
	}
	if !exists(t, filepath.Join(pt.dir, DirProcessed, "com/add/list-2.txt")) {
		t.Error("новый файл не получил суффикс коллизии list-2.txt")
	}
}

// TestPipelineEmptyFile — a file without domains (empty/comments only): not
// failed (T-016: a file without meaningful lines is not considered failed),
// applied as an empty batch, the report is written.
func TestPipelineEmptyFile(t *testing.T) {
	pt := newPipeTest(t)
	abs := pt.addFile(t, "com/add/empty.txt", "# только комментарий\n\n")

	out, err := pt.pipe.Process(context.Background(), abs)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Kind != OutcomeApplied {
		t.Fatalf("итог = %s, want applied", out.Kind)
	}
	rep := out.Report
	if rep.LinesTotal != 2 || rep.Skipped != 2 || rep.Accepted != 0 || rep.Added != 0 {
		t.Errorf("счётчики = %+v, want 2 строки, 2 пропущено, применения нет", rep)
	}
	if len(pt.files.records) != 1 {
		t.Error("отчёт пустого файла должен записаться в журнал")
	}
	// A COPY with an empty batch does not run (db.CopyFrom: an empty batch is
	// a no-op); the apply of an empty batch — the boundaries and the count
	// without the insert and the deltas.
	if strings.Join(pt.pool.ops, "\n") != strings.Join([]string{
		"exec:" + clearStageSQL,
		"begin",
		"query:" + addChunkBoundSQL,
		"query:" + reviveAddSQL,
		"query:" + countAddSQL,
		"exec:" + clearStageSQL,
		"commit",
	}, "\n") {
		t.Errorf("обращения = %v, want очистка → пустое применение → очистка", pt.pool.ops)
	}
}

// TestPipelineRejectLogPassed — the rejected log (T-016) is wired into the
// pipeline and receives the file's rejected lines with their reasons.
func TestPipelineRejectLogPassed(t *testing.T) {
	pt := newPipeTest(t)
	abs := pt.addFile(t, "com/add/list.txt", "a.org\n") // wrong_zone
	pt.pool.execs = []pipeExec{{tag: pgconn.NewCommandTag("DELETE 0")}}

	var buf strings.Builder
	pipe, err := NewPipeline(pt.zones, pt.files, pt.pool, PipelineOptions{
		Dir:       pt.dir,
		BatchSize: 10,
		RejectLog: NewRejectedLogger(&buf, slog.LevelInfo),
		Now:       func() time.Time { return pt.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pipe.Process(context.Background(), abs); err != nil {
		t.Fatal(err)
	}
	logged := buf.String()
	if !strings.Contains(logged, "service=rejected") ||
		!strings.Contains(logged, "file=com/add/list.txt") ||
		!strings.Contains(logged, "reason=wrong_zone") {
		t.Errorf("rejected-лог = %q, want запись об отклонённой строке", logged)
	}
}

// TestPipelineParserRejectLog — the parser's per-line rejections are written
// into the rejected log in the same format as the validator's rejections
// (spec 5.1, docs/incoming-files.md §3, §7: invalid_utf8/line_len — the log
// reasons, the truncated line — in the entry).
func TestPipelineParserRejectLog(t *testing.T) {
	pt := newPipeTest(t)
	content := "example.com\n\xff\xfe\n" + strings.Repeat("a", MaxLineLen+76) + "\n"
	abs := pt.addFile(t, "com/add/list.txt", content)

	var buf strings.Builder
	pipe, err := NewPipeline(pt.zones, pt.files, pt.pool, PipelineOptions{
		Dir:       pt.dir,
		BatchSize: 10,
		RejectLog: NewRejectedLogger(&buf, slog.LevelInfo),
		Now:       func() time.Time { return pt.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pipe.Process(context.Background(), abs); err != nil {
		t.Fatal(err)
	}
	logged := buf.String()
	for _, want := range []string{
		"service=rejected",
		"file=com/add/list.txt",
		"line=2",
		"reason=invalid_utf8",
		"line=3",
		"reason=line_len",
		"text=" + strings.Repeat("a", maxRejectText), // the parser's truncation, bytes
	} {
		if !strings.Contains(logged, want) {
			t.Errorf("rejected-лог = %q, want запись с %q", logged, want)
		}
	}
}

// TestNewPipelineValidation — the mandatory dependencies and the directory.
func TestNewPipelineValidation(t *testing.T) {
	opts := PipelineOptions{Dir: "/tmp"}
	if _, err := NewPipeline(nil, newFakeFileStore(), &pipePool{}, opts); err == nil {
		t.Error("хранилище зон обязательно")
	}
	if _, err := NewPipeline(newFakeZoneStore(), nil, &pipePool{}, opts); err == nil {
		t.Error("журнал файлов обязателен")
	}
	if _, err := NewPipeline(newFakeZoneStore(), newFakeFileStore(), nil, opts); err == nil {
		t.Error("пул обязателен")
	}
	if _, err := NewPipeline(newFakeZoneStore(), newFakeFileStore(), &pipePool{}, PipelineOptions{}); err == nil {
		t.Error("каталог incoming обязателен")
	}

	// The defaults: the batch size and the error ratio threshold (the T-016 style).
	p, err := NewPipeline(newFakeZoneStore(), newFakeFileStore(), &pipePool{},
		PipelineOptions{Dir: "/tmp"})
	if err != nil {
		t.Fatal(err)
	}
	if p.opts.BatchSize != DefaultBatchSize || p.opts.MaxErrorRatio != DefaultMaxErrorRatio {
		t.Errorf("дефолты = %d/%v, want %d/%v",
			p.opts.BatchSize, p.opts.MaxErrorRatio, DefaultBatchSize, DefaultMaxErrorRatio)
	}
}

// Юнит-тесты учёта файлов в ingest_files (T-021, ТЗ 5.1, 6.1): фейковый
// FileStore для конвейера (pipeline_test.go), SQLFileStore — текст SQL и
// параметры на фейковом pgx-пуле без БД (стиль zones_test.go), формат
// лог-отчёта renderReport и отображение счётчиков FileReport.record.
// Реальное поведение против БД — интеграционные тесты ingest (T-023).
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

// --- фейковый FileStore ---------------------------------------------------

// fakeFileStore — память-журнал ingest_files с протоколом вызовов.
// Строки с одним sha256 (разные зона/действие — сценарии T-113)
// возвращаются в порядке добавления.
type fakeFileStore struct {
	known   map[string][]model.IngestFile
	bySAErr error // ошибка FilesBySHA256, если задана
	records []FileRecord
	recErr  error // ошибка RecordFile, если задана
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
	return f.known[string(sha)], nil // nil = содержимое не обрабатывалось
}

func (f *fakeFileStore) RecordFile(_ context.Context, rec FileRecord) error {
	if f.recErr != nil {
		return f.recErr
	}
	f.records = append(f.records, rec)
	return nil
}

// --- фейковый pgx-пул для SQLFileStore ------------------------------------

// fileColNames — имена колонок в порядке ingestFileColumns.
var fileColNames = strings.Split(ingestFileColumns, ", ")

// fileRows — pgx.Rows с заготовленными строками ingest_files: Scan
// раскладывает значения позиционно, FieldDescriptions — имена колонок
// для RowToStructByName (как fakeRows в zones_test.go).
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
		return errors.New("fileRows: Scan без Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return errors.New("fileRows: число приёмников не равно числу колонок")
	}
	for i, d := range dest {
		target := reflect.ValueOf(d).Elem()
		if row[i] == nil {
			target.Set(reflect.Zero(target.Type()))
			continue
		}
		val := reflect.ValueOf(row[i])
		if target.Kind() == reflect.Pointer {
			// NULL-колонка модели (указатель): *Action, *string, …
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

// Conn и TypeMap — часть pgx.Rows, не используемая коллектором.
func (r *fileRows) Conn() *pgx.Conn      { return nil }
func (r *fileRows) TypeMap() *pgtype.Map { return nil }

// fakeFilePool — db.DB для SQLFileStore: запоминает SQL и аргументы,
// Query выдаёт заготовленные строки, Exec — заготовленный тег/ошибку.
type fakeFilePool struct {
	queries  []zoneQuery // переиспользование типа из zones_test.go
	results  []*fileRows
	queryErr error
	execSQL  []string
	execArgs [][]any
	execTag  pgconn.CommandTag
	execErr  error
}

var _ db.DB = (*fakeFilePool)(nil)

func (p *fakeFilePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("fakeFilePool: CopyFrom не должен вызываться")
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

// TestSQLFilesBySHA256 — все строки журнала с данным содержимым
// (T-113: одно содержимое может доставляться в разных зоне/действии —
// точная повторная доставка ищется конвейером по тройке).
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
		t.Errorf("аргументы = %v, want [sha256]", pool.queries[0].args)
	}
	if len(fs) != 2 {
		t.Fatalf("строк журнала = %d, want 2 (add и undo-delete одного содержимого)", len(fs))
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
		t.Errorf("строка журнала = %+v, want как есть из ingest_files", f)
	}
	if d := fs[1]; d.ID != 11 || d.Action == nil || *d.Action != model.ActionDelete {
		t.Errorf("вторая строка журнала = %+v, want undo-delete того же sha256", d)
	}
}

// TestSQLFilesBySHA256None — содержимое не обрабатывалось: пустой срез
// без ошибки (не обрабатывалось — не ошибка хранилища).
func TestSQLFilesBySHA256None(t *testing.T) {
	pool := &fakeFilePool{results: []*fileRows{{}}}
	store := NewFileStore(pool)

	fs, err := store.FilesBySHA256(context.Background(), []byte{9})
	if err != nil || len(fs) != 0 {
		t.Errorf("результат = %v, %v; want пустой срез без ошибки", fs, err)
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
		t.Errorf("аргументы = %v, want %v", pool.execArgs[0], want)
	}
}

// TestSQLRecordFileBadSHA — отчёт без полного sha256 — ошибка до БД:
// битый хэш сломал бы идемпотентность (уникальная тройка
// (zone_id, action, sha256), T-113).
func TestSQLRecordFileBadSHA(t *testing.T) {
	pool := &fakeFilePool{}
	store := NewFileStore(pool)

	err := store.RecordFile(context.Background(), FileRecord{Path: "x", SHA256: []byte{1}})
	if err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("err = %v, want ошибка длины sha256", err)
	}
	if len(pool.execSQL) != 0 {
		t.Error("запрос не должен выполняться при невалидном sha256")
	}
}

// TestSQLFileStoreErrors — ошибки хранилища оборачиваются контекстом
// «db: ingest_files» (стиль SQLZoneStore).
func TestSQLFileStoreErrors(t *testing.T) {
	boom := errors.New("соединение потеряно")

	pool := &fakeFilePool{queryErr: boom}
	store := NewFileStore(pool)
	_, err := store.FilesBySHA256(context.Background(), []byte{1})
	if err == nil || !strings.Contains(err.Error(), "db: ingest_files: поиск по sha256:") || !errors.Is(err, boom) {
		t.Errorf("ошибка поиска = %v, want обёрнутая", err)
	}

	pool = &fakeFilePool{execErr: boom}
	store = NewFileStore(pool)
	err = store.RecordFile(context.Background(), FileRecord{Path: "x", SHA256: make([]byte, 32)})
	if err == nil || !strings.Contains(err.Error(), "db: ingest_files: отчёт x:") || !errors.Is(err, boom) {
		t.Errorf("ошибка записи = %v, want обёрнутая", err)
	}
}

// --- лог-отчёт ------------------------------------------------------------

// TestRenderReport — формат лог-отчёта (ТЗ 5.1: рядом с файлом в failed/)
// зафиксирован: причина, счётчики строк по этапам, применение по действию.
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
		"# лог-отчёт ingest (ТЗ 5.1)",
		"файл: com/add/list.txt",
		"зона: com (id 7)",
		"действие: add",
		"sha256: abcd",
		"итог: применён",
		"строк: 6",
		"  пропущено (пустые и комментарии): 2",
		"  отклонено: 3",
		"    invalid: 1",
		"    wrong_zone: 2",
		"  валидных: 1",
		"добавлено: 2 (в т.ч. возвращено из deleted: 1)",
		"уже было: 3",
		"",
	}, "\n")
	if got := renderReport(rep, "применён"); got != want {
		t.Errorf("лог-отчёт =\n%s\nwant:\n%s", got, want)
	}
}

// TestRenderReportDeleteFailed — отчёт ошибочного delete-файла: применения
// нет, причины отклонений прилагаются; зона не разобрана — прочерк.
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
		"# лог-отчёт ingest (ТЗ 5.1)",
		"файл: net/delete/y.txt",
		"зона: net (id 0)",
		"действие: delete",
		"итог: ошибочен — доля отклонённых строк выше порога",
		"строк: 4",
		"  пропущено (пустые и комментарии): 0",
		"  отклонено: 4",
		"    wrong_zone: 4",
		"  валидных: 0",
		"удалено: 0",
		"не найдено в БД: 0",
		"",
	}, "\n")
	if got := renderReport(rep, "ошибочен — доля отклонённых строк выше порога"); got != want {
		t.Errorf("лог-отчёт =\n%s\nwant:\n%s", got, want)
	}
}

// TestRenderReportNoPath — путь не разобран (ошибочные файлы тоже
// получают лог-отчёт): зона и действие — прочерки, sha256 нет.
func TestRenderReportNoPath(t *testing.T) {
	want := strings.Join([]string{
		"# лог-отчёт ingest (ТЗ 5.1)",
		"файл: com/list.txt",
		"зона: — (путь не разобран)",
		"действие: —",
		"итог: ошибочен — структура пути не <зона>/add|delete/*.txt",
		"строк: 0",
		"  пропущено (пустые и комментарии): 0",
		"  отклонено: 0",
		"  валидных: 0",
		"",
	}, "\n")
	got := renderReport(FileReport{Path: "com/list.txt"},
		"ошибочен — структура пути не <зона>/add|delete/*.txt")
	if got != want {
		t.Errorf("лог-отчёт =\n%s\nwant:\n%s", got, want)
	}
}

// TestFileReportRecord — отображение итога в колонки ingest_files (ТЗ 6.1):
// Revived входит в Added, NotFound колонки не имеет.
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
		t.Errorf("record = %+v, want %+v (revived — в added, not_found — без колонки)", rec, want)
	}
}

// TestMergeReasons — отклонения парсера и валидатора суммируются по
// причинам; имена не пересекаются (invalid_utf8/line_len против классов
// idn и wrong_zone).
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

// compile-time: SQLFileStore — FileStore поверх internal/db.
var _ FileStore = (*SQLFileStore)(nil)
var _ FileStore = (*fakeFileStore)(nil)

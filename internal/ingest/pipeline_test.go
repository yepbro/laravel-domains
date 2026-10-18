// Сквозные юнит-тесты конвейера ingest (T-021, ТЗ 5.1) без БД: фейковые
// ZoneStore (zones_test.go), FileStore (report_test.go) и pgx-пул с
// заготовленными результатами Stage/ApplyAdd/ApplyDelete; файлы — во
// временных каталогах (t.TempDir). Сценарии: удачный add, удачный
// delete, дубль — точная повторная доставка (T-113: та же зона, действие
// и sha256 — не применяется повторно), то же содержимое другой зоной/
// действием — обработка (undo), порог ошибок →
// failed/ с лог-отчётом, битая структура пути → failed/, инфраструктурный
// сбой → файл остаётся в incoming (at-least-once, ТЗ 3.4), коллизия имён
// в архиве. Реальное поведение против БД — интеграционные тесты (T-023).
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
	"strings"
	"testing"
	"time"

	"crawler/internal/db"
	"crawler/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// --- фейковый пул конвейера ----------------------------------------------

// pipeExec — заготовленный результат одного Exec по очереди.
type pipeExec struct {
	tag pgconn.CommandTag
	err error
}

// pipePool — db.DB для конвейера: CopyFrom считает строки пачек COPY
// (staging), Exec выдаёт результаты по очереди, Query отвечает на
// известные SQL ApplyAdd/ApplyDelete. Как реальный pgx, отказывается
// работать с отменённым контекстом.
type pipePool struct {
	ops     []string // протокол: "exec:<sql>", "query:<sql>", "copy:<n>"
	copies  []int64  // размеры пачек COPY по очереди
	copyErr error

	execs []pipeExec // результаты Exec по очереди (конец — нулевой тег)
	execI int

	uniques    int64 // ответ countAddSQL (уникальных доменов партии add)
	delDeleted int64 // ответ applyDeleteSQL
	delMissing int64
}

var _ db.DB = (*pipePool)(nil)

func (p *pipePool) CopyFrom(ctx context.Context, _ pgx.Identifier, _ []string, src pgx.CopyFromSource) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var n int64
	for src.Next() {
		if _, err := src.Values(); err != nil {
			return n, err
		}
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

// oneRowRows — pgx.Rows из одной строки для позиционного Scan
// (countAddSQL — RowTo[int64], applyDeleteSQL — Scan двух счётчиков).
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

// scanAny присваивает значение приёмнику через reflect (int64 и указатели).
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

// Conn и TypeMap — часть pgx.Rows, не используемая коллекторами.
func (r *oneRowRows) Conn() *pgx.Conn      { return nil }
func (r *oneRowRows) TypeMap() *pgtype.Map { return nil }

func (p *pipePool) Query(ctx context.Context, sql string, _ ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.ops = append(p.ops, "query:"+sql)
	switch sql {
	case countAddSQL:
		return &oneRowRows{vals: []any{p.uniques}}, nil
	case applyDeleteSQL:
		return &oneRowRows{vals: []any{p.delDeleted, p.delMissing}}, nil
	}
	return nil, fmt.Errorf("pipePool: неожидаемый запрос: %s", sql)
}

// --- сборка конвейера ------------------------------------------------------

// pipeTest — окружение теста конвейера: каталог incoming, фейки, конвейер.
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
		Log:       nil, // подавлен: итоги проверяются по Outcome
		Now:       func() time.Time { return pt.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	pt.pipe = pipe
	return pt
}

// addFile — файл rel (относительно incoming) с содержимым content;
// возвращает абсолютный путь.
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

// exists — файл/каталог существует.
func exists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Lstat(path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	return err == nil
}

// readFile — содержимое файла (для лог-отчётов).
func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// --- сценарии --------------------------------------------------------------

// TestPipelineAdd — удачный add: строка применена, отчёт записан в
// ingest_files, файл перемещён в processed/ с сохранением структуры.
func TestPipelineAdd(t *testing.T) {
	pt := newPipeTest(t)
	content := "example.com\n# комментарий\n\nfoo.com\nexample.com\n"
	abs := pt.addFile(t, "com/add/list.txt", content)
	sha := sha256.Sum256([]byte(content))

	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")}, // очистка перед загрузкой (Stage)
		{tag: pgconn.NewCommandTag("INSERT 1")}, // новый: foo (example существует в deleted)
		{tag: pgconn.NewCommandTag("UPDATE 1")}, // возвращён из deleted: example
		{tag: pgconn.NewCommandTag("DELETE 3")}, // очистка после применения
	}
	pt.pool.uniques = 2 // уникальных меток партии: example, foo (дубль строки — одна метка)

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
	// 5 физических строк: 3 значимых (дубль example — тоже строка),
	// 1 комментарий, 1 пустая; валидных 3.
	if rep.LinesTotal != 5 || rep.Skipped != 2 || rep.Accepted != 3 || rep.Rejected != 0 {
		t.Errorf("счётчики строк = %+v, want 5/2/3/0", rep)
	}
	if rep.Added != 2 || rep.Revived != 1 || rep.Existed != 0 {
		t.Errorf("счётчики применения = %+v, want added 2 (revived 1), existed 0", rep)
	}
	if !reflect.DeepEqual(rep.SHA256, sha[:]) {
		t.Errorf("sha256 = %x, want хэш содержимого %x", rep.SHA256, sha)
	}

	// Отчёт в журнале: счётчики файла одной строкой ingest_files (ТЗ 6.1).
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

	// Обращения к БД: очистка → COPY → вставка → возврат → подсчёт →
	// очистка (Stage и ApplyAdd, фиксированный порядок T-017/T-018).
	want := []string{
		"exec:" + clearStageSQL,
		"copy:3",
		"exec:" + insertAddSQL,
		"exec:" + reviveAddSQL,
		"query:" + countAddSQL,
		"exec:" + clearStageSQL,
	}
	if !reflect.DeepEqual(pt.pool.ops, want) {
		t.Errorf("порядок обращений =\n%s\nwant:\n%s",
			strings.Join(pt.pool.ops, "\n"), strings.Join(want, "\n"))
	}

	// Файл уходит в processed/ с сохранением структуры (ТЗ 5.1).
	if exists(t, abs) {
		t.Error("файл остался в incoming")
	}
	if !exists(t, filepath.Join(pt.dir, DirProcessed, "com/add/list.txt")) {
		t.Error("файл не перемещён в processed/com/add/list.txt")
	}
}

// TestPipelineDelete — удачный delete: домены переведены в deleted,
// счётчики Deleted/NotFound в отчёте.
func TestPipelineDelete(t *testing.T) {
	pt := newPipeTest(t)
	content := "a.net\nb.net\n"
	abs := pt.addFile(t, "net/delete/x.txt", content)
	sha := sha256.Sum256([]byte(content))

	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")}, // очистка перед загрузкой
		{tag: pgconn.NewCommandTag("DELETE 2")}, // очистка после применения
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
	// NotFound — только лог-отчёт: колонки в ingest_files нет (ТЗ 6.1).
	if rec.Added != 0 || rec.Existed != 0 || rec.Rejected != 0 {
		t.Errorf("запись журнала = %+v, want нулевые add-счётчики", rec)
	}

	want := []string{
		"exec:" + clearStageSQL,
		"copy:2",
		"query:" + applyDeleteSQL,
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

// TestPipelineDuplicate — точная повторная доставка (T-113): та же
// тройка (зона, действие, sha256) уже в журнале (в том числе среди
// других доставок того же содержимого — точное совпадение побеждает):
// партия отброшена, применение не выполняется, отчёт не дублируется,
// файл уходит в processed/.
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
		model.IngestFile{ // другое действие, та же зона (undo)
			ID: 6, ZoneID: &zone, Action: &del, Path: &undoPath,
			SHA256: sha[:], ProcessedAt: &processed,
		},
		model.IngestFile{ // точная повторная доставка
			ID: 5, ZoneID: &zone, Action: &add, Path: &path,
			SHA256: sha[:], ProcessedAt: &processed,
		},
		model.IngestFile{ // то же действие, другая зона
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

	// Загрузка была (хэш известен только после чтения), применение — нет:
	// очистка → COPY → отбрасывание партии.
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

// TestPipelineDuplicateOtherZoneWarn — дубль по sha256 в другой зоне при
// том же действии (T-113): точной повторной доставки нет, файл
// применяется (домены этой зоны первая доставка не трогала) — но
// оператору подсвечивается Warn (ING-P2-5, неконтрактная часть).
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
		{tag: pgconn.NewCommandTag("DELETE 0")}, // очистка перед загрузкой (Stage)
		{tag: pgconn.NewCommandTag("INSERT 1")}, // example
		{tag: pgconn.NewCommandTag("UPDATE 0")}, // возвращать некого
		{tag: pgconn.NewCommandTag("DELETE 1")}, // очистка после применения
	}
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

// TestPipelineUndoSameContentApplied — undo тем же списком (T-113):
// то же содержимое доставлено в delete/ после add/ — действие другое,
// точной повторной доставки нет, файл обрабатывается: домены
// удаляются, отчёт в журнале. Переопределяет пиннинг T-097
// (TestIngest_SameContentCrossActionSkipped, контракт дедупа сменила
// T-113).
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
		{tag: pgconn.NewCommandTag("DELETE 0")}, // очистка перед загрузкой (Stage)
		{tag: pgconn.NewCommandTag("DELETE 1")}, // очистка после применения
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

// TestPipelineErrorRatio — доля отклонённых строк выше порога (T-016):
// файл целиком ошибочен — применение не выполняется, файл → failed/
// с лог-отчётом рядом, отчёт в ingest_files не пишется (хэш ошибочного
// файла не занимает журнал — исправленный файл можно загрузить снова).
func TestPipelineErrorRatio(t *testing.T) {
	pt := newPipeTest(t)
	content := "a.org\nb.org\nc.com\n" // 2/3 отклонений (wrong_zone) > 0.1
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

	// Загрузка была, применение — нет; партия отброшена.
	want := []string{
		"exec:" + clearStageSQL,
		"copy:1", // c.com
		"exec:" + clearStageSQL,
	}
	if !reflect.DeepEqual(pt.pool.ops, want) {
		t.Errorf("порядок обращений =\n%s\nwant:\n%s",
			strings.Join(pt.pool.ops, "\n"), strings.Join(want, "\n"))
	}

	// Файл — в failed/ с лог-отчётом рядом (ТЗ 5.1).
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

// TestPipelineParserRejectsFailFile — построчные отказы парсера
// (invalid_utf8, line_len) учитываются в пороге доли ошибок (T-016):
// файл, чей мусор не дошёл до валидатора, всё равно ошибочен целиком
// (битый дамп — именно не-UTF-8, docs/incoming-files.md §4), а не
// «успешно применённая» пустая партия.
func TestPipelineParserRejectsFailFile(t *testing.T) {
	// 1 строка не-UTF-8 + 8 валидных: валидатор один 0/8 не роняет,
	// объединённая доля 1/9 > 0.1 — файл ошибочен.
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

	// Файл целиком из не-UTF-8: парсер отверг каждую строку, валидатор
	// не видел ни одной (total = 0) — без учёта отказов парсера доля
	// считалась бы 0/0 и файл уезжал бы в processed/.
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
		// Партия из нуля строк отброшена, применение не выполнялось.
		want := []string{"exec:" + clearStageSQL, "exec:" + clearStageSQL}
		if !reflect.DeepEqual(pt.pool.ops, want) {
			t.Errorf("обращения = %v, want очистка → отбрасывание партии без COPY", pt.pool.ops)
		}
		if len(pt.files.records) != 0 {
			t.Error("отчёт ошибочного файла не пишется в журнал")
		}
	})
}

// TestPipelineBadPath — структура пути не <зона>/add|delete/*.txt
// (T-014): файл ошибочен, до БД не доходит, failed/ с лог-отчётом.
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
	if !strings.Contains(report, "итог: ошибочен — ingest: структура пути не <зона>/add|delete/*.txt") {
		t.Errorf("лог-отчёт без причины:\n%s", report)
	}
}

// TestPipelineBadZoneDir — имя каталога зоны не нормализуется (T-014):
// файл ошибочен (failed/), зона не создаётся.
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

// TestPipelineFailFileOutsideIncoming — Process — публичный API: путь
// вне incoming (rel с «..» после filepath.Rel, ParsePath его отвергает)
// не должен выводить архив за пределы failed/ (ING-P3-1): назначение
// санируется до базового имени файла.
func TestPipelineFailFileOutsideIncoming(t *testing.T) {
	pt := newPipeTest(t)
	// Файл-сиблинг каталога incoming: rel = "../escape/com/list.txt".
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
	// Архив не вышел за пределы failed/ (и не «провалился» в ../).
	if exists(t, filepath.Join(pt.dir, "escape", "com", "list.txt")) {
		t.Error("файл оказался снаружи failed/ — путь назначения не санирован")
	}
}

// TestPipelineZoneDBError — сбой БД на этапе зоны: файл остаётся в
// incoming (at-least-once, ТЗ 3.4), итог left, ошибка — вызывающему.
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

// TestPipelineStageError — сбой COPY: файл остаётся в incoming, партия
// отброшена (staging не растёт), повтор возьмёт новую партию.
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
	// очистка (Stage) → COPY (сбой) → отбрасывание партии (best effort)
	if len(pt.pool.ops) != 3 || pt.pool.ops[0] != "exec:"+clearStageSQL ||
		!strings.HasPrefix(pt.pool.ops[1], "copy:") || pt.pool.ops[2] != "exec:"+clearStageSQL {
		t.Errorf("обращения = %v, want очистка, COPY, отбрасывание партии", pt.pool.ops)
	}
	if len(pt.files.records) != 0 {
		t.Error("журнал не пишется при сбое загрузки")
	}
}

// TestPipelineApplyError — сбой применения add: файл остаётся, отчёт не
// пишется (применение не состоялось целиком), партия отброшена.
func TestPipelineApplyError(t *testing.T) {
	pt := newPipeTest(t)
	abs := pt.addFile(t, "com/add/list.txt", "example.com\n")
	boom := errors.New("сервер закрыл соединение")
	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")}, // Stage
		{err: boom},                             // вставка
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
	// вставка без очистки ApplyAdd (сбой) → отбрасывание конвейером
	if !strings.Contains(strings.Join(pt.pool.ops, "\n"), "exec:"+insertAddSQL) {
		t.Errorf("обращения = %v, want попытка вставки", pt.pool.ops)
	}
	if pt.pool.ops[len(pt.pool.ops)-1] != "exec:"+clearStageSQL {
		t.Errorf("последнее обращение = %v, want отбрасывание партии", pt.pool.ops[len(pt.pool.ops)-1])
	}
}

// TestPipelineRecordError — сбой записи отчёта: применение состоялось,
// но файл остаётся в incoming — повтор найдёт дубль по sha256 и только
// перенесёт файл (самовосстановление, ТЗ 3.4/5.1).
func TestPipelineRecordError(t *testing.T) {
	pt := newPipeTest(t)
	abs := pt.addFile(t, "com/add/list.txt", "example.com\n")
	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")},
		{tag: pgconn.NewCommandTag("INSERT 1")},
		{tag: pgconn.NewCommandTag("UPDATE 0")},
		{tag: pgconn.NewCommandTag("DELETE 1")},
	}
	pt.pool.uniques = 1
	pt.files.recErr = errors.New("журнал недоступен")

	out, err := pt.pipe.Process(context.Background(), abs)
	if err == nil || !errors.Is(err, pt.files.recErr) {
		t.Fatalf("err = %v, want ошибка журнала", err)
	}
	if out.Kind != OutcomeLeft || !exists(t, abs) {
		t.Fatalf("итог = %s, want left и файл в incoming", out.Kind)
	}
	// Применение состоялось полностью (вставка → возврат → подсчёт →
	// очистка) — на повторе дубль по sha256 только перенесёт файл.
	if len(pt.pool.ops) != 6 {
		t.Errorf("обращений к БД = %d (%v), want 6 — применение завершено", len(pt.pool.ops), pt.pool.ops)
	}
}

// TestPipelineDuplicateCheckError — сбой проверки дубля: файл остаётся,
// применение не выполняется.
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

// TestPipelineContextCanceled — отменённый контекст: как реальный pgx,
// фейк отказывает на первом обращении; файл остаётся (graceful shutdown
// дорабатывает или возвращает, ТЗ 3.4).
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
	// Партия очищена и после отмены ctx: discard чистит staging собственным
	// контекстом (ING-P2-3) — строки не копятся в staging от рестарта к
	// рестарту. (Очистка внутри Stage с отменённым ctx не дошла до БД —
	// фейк, как pgx, отказывает до записи обращения.)
	if len(pt.pool.ops) != 1 || pt.pool.ops[0] != "exec:"+clearStageSQL {
		t.Errorf("обращения = %v, want одна очистка отброшенной партии", pt.pool.ops)
	}
}

// TestPipelineMoveCollision — в архиве уже есть файл с тем же именем
// (другой экземпляр): новый получает числовой суффикс, существующий
// не затирается.
func TestPipelineMoveCollision(t *testing.T) {
	pt := newPipeTest(t)
	content := "example.com\n"
	abs := pt.addFile(t, "com/add/list.txt", content)
	// Другой экземпляр с тем же именем уже обработан ранее.
	older := filepath.Join(pt.dir, DirProcessed, "com/add/list.txt")
	if err := os.MkdirAll(filepath.Dir(older), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(older, []byte("older\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	pt.pool.execs = []pipeExec{
		{tag: pgconn.NewCommandTag("DELETE 0")},
		{tag: pgconn.NewCommandTag("INSERT 1")},
		{tag: pgconn.NewCommandTag("UPDATE 0")},
		{tag: pgconn.NewCommandTag("DELETE 1")},
	}
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

// TestPipelineEmptyFile — файл без доменов (пустой/одни комментарии):
// не ошибочен (T-016: файл без содержательных строк ошибочным не
// считается), применяется как пустая партия, отчёт пишется.
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
	// COPY с пустой пачкой не выполняется (db.CopyFrom: пустая пачка — no-op).
	if strings.Join(pt.pool.ops, "\n") != strings.Join([]string{
		"exec:" + clearStageSQL,
		"exec:" + insertAddSQL,
		"exec:" + reviveAddSQL,
		"query:" + countAddSQL,
		"exec:" + clearStageSQL,
	}, "\n") {
		t.Errorf("обращения = %v, want очистка → пустое применение → очистка", pt.pool.ops)
	}
}

// TestPipelineRejectLogPassed — rejected-лог (T-016) подключается к
// конвейеру и получает отклонённые строки файла с их причинами.
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

// TestPipelineParserRejectLog — построчные отказы парсера пишутся в
// rejected-лог тем же форматом, что и отказы валидатора (ТЗ 5.1,
// docs/incoming-files.md §3, §7: invalid_utf8/line_len — причины лога,
// обрезанная строка — в записи).
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
		"text=" + strings.Repeat("a", maxRejectText), // обрезка парсера, байты
	} {
		if !strings.Contains(logged, want) {
			t.Errorf("rejected-лог = %q, want запись с %q", logged, want)
		}
	}
}

// TestNewPipelineValidation — обязательные зависимости и каталог.
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

	// Дефолты: размер пачки и порог доли ошибок (стиль T-016).
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

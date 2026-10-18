// Юнит-тесты загрузчика staging без БД (стиль internal/db/copy_test.go):
// нарезка пачек и содержимое строк COPY, очистка партии перед загрузкой,
// счётчики, отмена между пачками, ошибки источника и инварианты строк,
// лимит памяти пачкой, контракт staging-таблицы против SQL миграции 000004.
// Реальная загрузка — интеграционные тесты ingest (T-023).
package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/migrations"
)

// fakeDB записывает порядок обращений (exec/copy), строки каждого COPY и
// аргументы Exec; при заданной ошибке возвращает её.
type fakeDB struct {
	ops      []string // порядок обращений: "exec:<sql>", "copy:<n>"
	execSQL  []string
	execArgs [][]any
	execTag  pgconn.CommandTag // возвращается Exec (RowsAffected)
	batches  []int             // строк в каждом вызове CopyFrom
	rows     [][]any           // все строки всех пачек по порядку
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
		// первый вызов: свернуть контракт таблицы и колонок
		if !slices.Equal(table, StageTable) {
			return 0, fmt.Errorf("COPY в %v, want %v", table, StageTable)
		}
		if !slices.Equal(columns, StageColumns) {
			return 0, fmt.Errorf("колонки COPY %v, want %v", columns, StageColumns)
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

// rowsIter — типизированный источник строк с подсчётом вызовов.
type rowsIter struct {
	rows    []Row
	nextIdx int
	onNext  func(i int) // побочный эффект перед выдачей строки i (отмена и т.п.)
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

// genRows — n строк example-<i> с номерами строк от from.
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
	id, err := NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	if id == (BatchID{}) {
		t.Error("NewBatchID вернул нулевой идентификатор")
	}
	other, err := NewBatchID()
	if err != nil {
		t.Fatal(err)
	}
	if id == other {
		t.Error("два NewBatchID совпали: идентификаторы партий должны быть уникальными")
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
		t.Errorf("загружено %d, want 105", staged)
	}
	// 10 полных пачек + хвост из 5 строк (ТЗ 5.1: пачки по batch_size).
	if got, want := strings.Join(ints(f.batches), ","), "10,10,10,10,10,10,10,10,10,10,5"; got != want {
		t.Errorf("пачки = [%s], want [%s]", got, want)
	}
	// Перед копированием партия очищается — идемпотентность повтора (ТЗ 3.4).
	if len(f.ops) == 0 || f.ops[0] != "exec:"+clearStageSQL {
		t.Errorf("первое обращение = %v, want очистка партии %q", f.ops, clearStageSQL)
	}
	if len(f.execSQL) != 1 {
		t.Errorf("очисток %d, want 1 (только своя партия перед загрузкой)", len(f.execSQL))
	}

	// Содержимое строк COPY: партия, зона, метка, операция, номер строки.
	if len(f.rows) != 105 {
		t.Fatalf("строк в COPY %d, want 105", len(f.rows))
	}
	for i, vals := range f.rows {
		want := []any{b.ID[:], b.ZoneID, rows[i].Label, int16(b.Action), rows[i].LineNo}
		if !reflect.DeepEqual(vals, want) {
			t.Fatalf("строка %d = %v, want %v", i, vals, want)
		}
	}
}

// TestStage_MemoryBoundedByBatch — CopyFrom получает не больше batchSize
// строк за раз: материализуется только одна пачка, память не зависит от
// размера файла (ТЗ 3.5).
func TestStage_MemoryBoundedByBatch(t *testing.T) {
	f := &fakeDB{}
	staged, err := Stage(context.Background(), f, validBatch(t), 100, (&rowsIter{rows: genRows(1000)}).next)
	if err != nil || staged != 1000 {
		t.Fatalf("(%d, %v), want (1000, nil)", staged, err)
	}
	for i, n := range f.batches {
		if n > 100 {
			t.Errorf("пачка %d: %d строк > batchSize 100 — материализуется больше пачки", i, n)
		}
	}
	if len(f.batches) != 10 {
		t.Errorf("пачек %d, want 10", len(f.batches))
	}
}

func TestStage_EmptyInput(t *testing.T) {
	f := &fakeDB{}
	staged, err := Stage(context.Background(), f, validBatch(t), 10, (&rowsIter{}).next)
	if err != nil || staged != 0 {
		t.Fatalf("(%d, %v), want (0, nil)", staged, err)
	}
	if len(f.batches) != 0 {
		t.Errorf("пустой ввод привёл к вызовам COPY: %v", f.batches)
	}
	// Очистка партии выполняется и на пустом вводе: повтор пустого файла
	// затирает прежние строки партии.
	if len(f.execSQL) != 1 {
		t.Errorf("очисток %d, want 1", len(f.execSQL))
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
		{"нулевой идентификатор", Batch{ZoneID: 1, Action: model.ActionAdd}, 10},
		{"зона 0", Batch{ID: id, ZoneID: 0, Action: model.ActionAdd}, 10},
		{"отрицательная зона", Batch{ID: id, ZoneID: -2, Action: model.ActionDelete}, 10},
		{"операция 0", Batch{ID: id, ZoneID: 1, Action: 0}, 10},
		{"неизвестная операция", Batch{ID: id, ZoneID: 1, Action: model.Action(3)}, 10},
		{"нулевой размер пачки", Batch{ID: id, ZoneID: 1, Action: model.ActionAdd}, 0},
		{"отрицательный размер пачки", Batch{ID: id, ZoneID: 1, Action: model.ActionAdd}, -5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeDB{}
			_, err := Stage(context.Background(), f, tc.b, tc.batchSize, (&rowsIter{rows: genRows(1)}).next)
			if err == nil {
				t.Fatal("ожидалась ошибка валидации")
			}
			if len(f.ops) != 0 {
				t.Errorf("невалидная партия дошла до БД: %v", f.ops)
			}
		})
	}
}

// TestStage_CancelBetweenBatches — отмена ctx между пачками прерывает
// загрузку, законченные пачки остаются в staging (повтор перезаливает партию
// начисто — ТЗ 3.4/3.5).
func TestStage_CancelBetweenBatches(t *testing.T) {
	f := &fakeDB{}
	ctx, cancel := context.WithCancel(context.Background())

	it := &rowsIter{rows: genRows(100)}
	it.onNext = func(i int) {
		if i == 30 { // строки 0–29: три пачки уже закоммичены
			cancel()
		}
	}

	staged, err := Stage(ctx, f, validBatch(t), 10, it.next)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if staged != 30 {
		t.Errorf("загружено %d, want 30 (отмена до сброса 4-й пачки)", staged)
	}
	if got, want := strings.Join(ints(f.batches), ","), "10,10,10"; got != want {
		t.Errorf("пачки = [%s], want [%s]", got, want)
	}
}

func TestStage_SourceError(t *testing.T) {
	f := &fakeDB{}
	boom := errors.New("источник сломался")
	it := &rowsIter{rows: genRows(30)}
	next := func() (Row, error) {
		if it.nextIdx == 22 {
			return Row{}, boom
		}
		return it.next()
	}

	staged, err := Stage(context.Background(), f, validBatch(t), 10, next)
	if err == nil || !strings.Contains(err.Error(), boom.Error()) {
		t.Fatalf("err = %v, want обёрнутая %v", err, boom)
	}
	if !strings.Contains(err.Error(), "скопировано 20") {
		t.Errorf("err = %v, want упоминание 20 закоммиченных строк (2 полные пачки)", err)
	}
	if staged != 20 {
		t.Errorf("загружено %d, want 20 (только законченные пачки)", staged)
	}
}

// TestStage_BadRow — инварианты строки отбивают загрузку до БД; строки
// пачек, закоммиченных раньше, остаются (границы пачек, ТЗ 3.4).
func TestStage_BadRow(t *testing.T) {
	cases := []struct {
		name string
		row  Row
		want string // фрагмент ожидаемой ошибки
	}{
		{"пустая метка", Row{Label: "", LineNo: 1}, "пустая метка"},
		{"не-ASCII метка", Row{Label: "пример", LineNo: 1}, "punycode"},
		{"метка длиннее 63", Row{Label: strings.Repeat("a", 64), LineNo: 1}, "не длиннее 63"},
		{"пустая часть составной метки", Row{Label: "sub..example", LineNo: 1}, "непустая"},
		{"часть составной метки длиннее 63", Row{Label: "sub." + strings.Repeat("a", 64), LineNo: 1}, "не длиннее 63"},
		{"номер строки 0", Row{Label: "example", LineNo: 0}, "с единицы"},
		{"номер строки повторяется", Row{Label: "example", LineNo: 1}, "не растёт"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeDB{}
			// Пачка 10: плохая строка на позиции 10 — первая пачка уже
			// закоммичена, вторая отбита целиком.
			rows := append(genRows(10), tc.row)
			staged, err := Stage(context.Background(), f, validBatch(t), 10, (&rowsIter{rows: rows}).next)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want ошибку с %q", err, tc.want)
			}
			if staged != 10 {
				t.Errorf("загружено %d, want 10 (первая пачка закоммичена до плохой строки)", staged)
			}
			if got, want := strings.Join(ints(f.batches), ","), "10"; got != want {
				t.Errorf("пачки = [%s], want [%s]", got, want)
			}
		})
	}
}

// TestStage_CompositeLabel — составные метки (контракт internal/idn,
// ТЗ 5.2; docs/incoming-files.md: «www2.example.crawl.test — ок»)
// проходят в staging как есть: валидатор T-016 отдаёт их одной меткой
// зоны пути, точки внутри допустимы.
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
		t.Fatalf("(%d, %v), want (3, nil) — составные метки допустимы", staged, err)
	}
	if len(f.rows) != len(rows) {
		t.Fatalf("строк в COPY %d, want %d", len(f.rows), len(rows))
	}
	for i, vals := range f.rows {
		if got := vals[2].(string); got != rows[i].Label {
			t.Errorf("метка[%d] = %q, want %q", i, got, rows[i].Label)
		}
	}
}

func TestStage_CopyError(t *testing.T) {
	f := &fakeDB{copyErr: errors.New("relation does not exist")}
	staged, err := Stage(context.Background(), f, validBatch(t), 2, (&rowsIter{rows: genRows(5)}).next)
	if err == nil || !strings.Contains(err.Error(), "relation does not exist") {
		t.Fatalf("err = %v, want обёрнутая ошибка COPY", err)
	}
	if !strings.Contains(err.Error(), "ingest_stage") {
		t.Errorf("err = %v, want имя таблицы в ошибке", err)
	}
	if staged != 0 {
		t.Errorf("загружено %d, want 0 (первая же пачка упала)", staged)
	}
}

func TestStage_ClearError(t *testing.T) {
	f := &fakeDB{execErr: errors.New("connection refused")}
	if _, err := Stage(context.Background(), f, validBatch(t), 10, (&rowsIter{rows: genRows(1)}).next); err == nil || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("err = %v, want ошибка очистки партии", err)
	}
	if len(f.batches) != 0 {
		t.Errorf("после ошибки очистки COPY не выполняется, пачки = %v", f.batches)
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
		t.Errorf("удалено %d, want 7 (RowsAffected)", n)
	}
	if len(f.execSQL) != 1 || f.execSQL[0] != clearStageSQL {
		t.Errorf("SQL = %v, want один %q", f.execSQL, clearStageSQL)
	}
	if len(f.execArgs) != 1 || !reflect.DeepEqual(f.execArgs[0][0], id[:]) {
		t.Errorf("аргументы = %v, want идентификатор партии", f.execArgs)
	}

	// Нулевой идентификатор — ошибка без обращения к БД.
	if _, err := ClearBatch(context.Background(), f, BatchID{}); err == nil {
		t.Error("нулевой идентификатор: ожидалась ошибка")
	}
	if len(f.execSQL) != 1 {
		t.Errorf("обращений к БД %d, want 1 (только валидный вызов)", len(f.execSQL))
	}
}

// ---------------------------------------------------------------- контракт схемы

// TestStageTableContract — соответствие миграции 000004 (встроенный набор
// migrations/) загрузчику: UNLOGGED-таблица ingest_stage, колонки и типы
// в порядке StageColumns, все NOT NULL, PK (batch_id, line_no), без
// дополнительных индексов; down удаляет таблицу. Живая проверка схемы —
// интеграционный schema_test.go (T-023).
func TestStageTableContract(t *testing.T) {
	up := readMigration(t, "000004_ingest_staging.up.sql")

	if !strings.Contains(up, "CREATE UNLOGGED TABLE ingest_stage") {
		t.Error("в up-миграции нет CREATE UNLOGGED TABLE ingest_stage (ТЗ 3.5: staging без WAL)")
	}
	if strings.Contains(up, "CREATE INDEX") || strings.Contains(up, "CREATE UNIQUE INDEX") {
		t.Error("в up-миграции есть индексы сверх первичного ключа (минимум индексов, ТЗ 6)")
	}

	cols := parseCreateTable(t, up)
	wantTypes := map[string]string{
		"batch_id": "bytea", "zone_id": "smallint", "label": "text",
		"action": "smallint", "line_no": "bigint",
	}
	if len(cols) != len(wantTypes) {
		t.Fatalf("колонок %d (%v), want %d", len(cols), colNames(cols), len(wantTypes))
	}
	names := colNames(cols)
	if !slices.Equal(names, StageColumns) {
		t.Errorf("колонки таблицы %v не совпадают с StageColumns %v (контракт COPY)", names, StageColumns)
	}
	for _, c := range cols {
		if c.typ != wantTypes[c.name] {
			t.Errorf("%s: тип %q, want %q", c.name, c.typ, wantTypes[c.name])
		}
		if !c.notNull {
			t.Errorf("%s: ожидается NOT NULL", c.name)
		}
	}

	down := readMigration(t, "000004_ingest_staging.down.sql")
	if !strings.Contains(down, "DROP TABLE IF EXISTS ingest_stage") {
		t.Error("down-миграция не удаляет ingest_stage")
	}
}

// TestClearStageSQLContract — очистка адресует ту же таблицу, что и COPY
// (StageTable), только свою партию.
func TestClearStageSQLContract(t *testing.T) {
	if want := `DELETE FROM "ingest_stage" WHERE batch_id = $1`; clearStageSQL != want {
		t.Errorf("clearStageSQL = %q, want %q", clearStageSQL, want)
	}
}

func readMigration(t *testing.T, name string) string {
	t.Helper()
	raw, err := migrations.FS.ReadFile(name)
	if err != nil {
		t.Fatalf("чтение %s из встроенного набора: %v", name, err)
	}
	return string(raw)
}

// stageColumn — колонка из DDL: имя, тип, NOT NULL.
type stageColumn struct {
	name    string
	typ     string
	notNull bool
}

// parseCreateTable разбирает тело CREATE TABLE первой таблицы файла.
// Строки DDL принадлежат миграции проекта (без вложенных скобок в колонках),
// лёгкий разбор достаточен для контракта.
func parseCreateTable(t *testing.T, sql string) []stageColumn {
	t.Helper()

	lines := strings.Split(sql, "\n")
	start := -1
	for i, line := range lines {
		if strings.Contains(line, "CREATE") && strings.Contains(line, "TABLE") {
			start = i + 1 // первая строка после заголовка CREATE
			break
		}
	}
	if start < 0 {
		t.Fatal("в файле нет CREATE TABLE")
	}

	cols := make([]stageColumn, 0, 6)
	for _, line := range lines[start:] {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, ")") {
			break // конец тела таблицы
		}
		line = strings.TrimSuffix(line, ",")
		if strings.HasPrefix(line, "PRIMARY KEY") {
			if !strings.Contains(line, "(batch_id, line_no)") {
				t.Errorf("первичный ключ = %q, want (batch_id, line_no)", line)
			}
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			t.Fatalf("строка DDL %q не разобрана", line)
		}
		cols = append(cols, stageColumn{
			name:    fields[0],
			typ:     fields[1],
			notNull: strings.Contains(line, "NOT NULL"),
		})
	}
	if len(cols) == 0 {
		t.Fatal("тело CREATE TABLE пустое")
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

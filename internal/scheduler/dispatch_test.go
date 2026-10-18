// Юнит-тесты выборки пачки планировщиком (T-027, ТЗ 5.3) без БД (стиль
// internal/ingest/apply_add_test.go / zones_test.go): точные SQL-тексты
// двух запросов (активные зоны, LATERAL-префиксы очередей) и их
// параметры (массивы зон/keyset-курсоров, квота), круговое слияние с
// весами, дозаполнение голодных зон, пустой результат, валидация
// параметров, ошибки запроса/чтения, отмена ctx. Реальное поведение
// против живой БД — интеграционные тесты планировщика.
package scheduler

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/migrations"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// --- фейковый pgx-пул -----------------------------------------------

// dispatchPool — db.DB для DispatchBatch: запоминает SQL и аргументы
// Query, выдаёт заготовленные строки (или ошибку) по очереди вызовов.
// Как и реальный pgx, отказывается работать с отменённым контекстом.
type dispatchPool struct {
	queries []dispatchPoolQuery
	results []*dispatchRows
	err     error // ошибка самого Query, если задана
}

type dispatchPoolQuery struct {
	sql  string
	args []any
}

var _ db.DB = (*dispatchPool)(nil)

func (p *dispatchPool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("dispatchPool: CopyFrom не должен вызываться")
}

func (p *dispatchPool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("dispatchPool: Exec не должен вызываться")
}

func (p *dispatchPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.queries = append(p.queries, dispatchPoolQuery{sql: sql, args: args})
	if p.err != nil {
		return nil, p.err
	}
	if len(p.results) == 0 {
		return nil, errors.New("dispatchPool: результат Query не заготовлен")
	}
	rows := p.results[0]
	p.results = p.results[1:]
	return rows, nil
}

// dispatchRows — pgx.Rows ответа: имена колонок и строки; Scan
// раскладывает значения позиционно по типу приёмника (int16/int64/
// string — как pgx по OID колонок обоих запросов выборки).
type dispatchRows struct {
	colNames []string
	rows     [][]any
	i        int // индекс текущей строки (0 — до первого Next)
	closed   bool
	err      error // Err() после исчерпания, если задан
	scanErr  error // ошибка Scan, если задана
}

func (r *dispatchRows) Close() { r.closed = true }

func (r *dispatchRows) Err() error { return r.err }

func (r *dispatchRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *dispatchRows) FieldDescriptions() []pgconn.FieldDescription {
	fds := make([]pgconn.FieldDescription, len(r.colNames))
	for i, c := range r.colNames {
		fds[i] = pgconn.FieldDescription{Name: c}
	}
	return fds
}

func (r *dispatchRows) Next() bool {
	if r.closed || r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *dispatchRows) Scan(dest ...any) error {
	if r.closed || r.i == 0 || r.i > len(r.rows) {
		return errors.New("dispatchRows: Scan без Next")
	}
	if r.scanErr != nil {
		return r.scanErr
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return errors.New("dispatchRows: число приёмников не равно числу колонок")
	}
	for i := range row {
		switch d := dest[i].(type) {
		case *int16:
			*d = row[i].(int16)
		case *int64:
			*d = row[i].(int64)
		case *string:
			*d = row[i].(string)
		default:
			return errors.New("dispatchRows: неизвестный приёмник")
		}
	}
	return nil
}

func (r *dispatchRows) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *dispatchRows) RawValues() [][]byte    { return nil }

// Conn и TypeMap — часть pgx.Rows, не используемая чтением вручную.
func (r *dispatchRows) Conn() *pgx.Conn      { return nil }
func (r *dispatchRows) TypeMap() *pgtype.Map { return nil }

// --- заготовки ответов --------------------------------------------------------

// Колонки запросов выборки в порядке SELECT.
var (
	dispatchZonesCols  = []string{"id", "name"}
	dispatchDomainCols = []string{"zone_id", "id", "label"}
)

// dispatchZonesResult — ответ запроса активных зон: строки (id, name).
func dispatchZonesResult(rows ...[]any) *dispatchRows {
	return &dispatchRows{colNames: dispatchZonesCols, rows: rows}
}

// dispatchDomainsResult — ответ запроса очередей: строки
// (zone_id, id, label) в любом порядке — слияние сортирует само.
func dispatchDomainsResult(rows ...[]any) *dispatchRows {
	return &dispatchRows{colNames: dispatchDomainCols, rows: rows}
}

// zrow — строка зон: (id, name).
func zrow(id int16, name string) []any { return []any{id, name} }

// drow — строка очереди: (zone_id, id, label).
func drow(zone int16, id int64, label string) []any { return []any{zone, id, label} }

// --- выборка ------------------------------------------------------------

func TestDispatchBatch(t *testing.T) {
	// Строки зон перемешаны и внутри зоны приходят не по id — слияние
	// обязано отсортировать очередь зоны и собрать круговой порядок.
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")),
		dispatchDomainsResult(
			drow(2, 201, "b"),
			drow(1, 102, "c"),
			drow(1, 101, "a"),
		),
	}}
	q := DispatchQuery{Limit: 3, ZoneWeights: map[int16]int{2: 3, 1: 1}}

	batch, err := DispatchBatch(context.Background(), pool, q)
	if err != nil {
		t.Fatal(err)
	}
	// Вес зоны 2 = 3: её первая строка идёт первой (ключ 0.5/3 < 0.5/1),
	// дальше — чередование по ключу (rn − 0.5)/вес.
	want := []DispatchRow{
		{DomainID: 201, ZoneID: 2, Name: "b.example.net"},
		{DomainID: 101, ZoneID: 1, Name: "a.example.com"},
		{DomainID: 102, ZoneID: 1, Name: "c.example.com"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("пачка = %+v, want %+v (круговой порядок с весами)", batch, want)
	}

	if len(pool.queries) != 2 {
		t.Fatalf("запросов = %d, want 2 (зоны + очереди)", len(pool.queries))
	}
	if pool.queries[0].sql != dispatchZonesSQL || len(pool.queries[0].args) != 0 {
		t.Errorf("запрос зон: %q args=%v, want dispatchZonesSQL без параметров", pool.queries[0].sql, pool.queries[0].args)
	}
	if pool.queries[1].sql != dispatchZoneSQL {
		t.Errorf("запрос очередей: %q, want dispatchZoneSQL", pool.queries[1].sql)
	}
	// Аргументы: зоны, keyset-курсоры (0 — с головы) и квота —
	// ceil(L × maxW / ΣW) + 1 = ceil(3 × 3 / 4) + 1 = 4.
	wantArgs := []any{[]any{int16(1), int16(2)}, []any{int64(0), int64(0)}, 4}
	if !reflect.DeepEqual(pool.queries[1].args, wantArgs) {
		t.Errorf("аргументы = %v, want [zone_id[], after[], quota] = %v", pool.queries[1].args, wantArgs)
	}
}

// TestDispatchBatch_Refill — исчерпанные зоны отдают доли соседям:
// прочитанного префикса не хватило, «голодная» зона (вернула ровно
// квоту) перезапрашивается keyset-курсором от последнего id.
func TestDispatchBatch_Refill(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")),
		// Квота ceil(4 × 1 / 2) + 1 = 3: зона 1 обрезана (голодная),
		// зона 2 исчерпана (меньше квоты).
		dispatchDomainsResult(
			drow(1, 11, "a"), drow(1, 12, "b"), drow(1, 13, "c"),
		),
		// Дозаполнение: только зона 1, после id 13, квота — остаток пачки.
		dispatchDomainsResult(drow(1, 14, "d")),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 4})
	if err != nil {
		t.Fatal(err)
	}
	want := []DispatchRow{
		{DomainID: 11, ZoneID: 1, Name: "a.example.com"},
		{DomainID: 12, ZoneID: 1, Name: "b.example.com"},
		{DomainID: 13, ZoneID: 1, Name: "c.example.com"},
		{DomainID: 14, ZoneID: 1, Name: "d.example.com"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("пачка = %+v, want %+v", batch, want)
	}

	if len(pool.queries) != 3 {
		t.Fatalf("запросов = %d, want 3 (зоны + очереди + дозаполнение)", len(pool.queries))
	}
	// Дозаполнение: зоны только голодные, курсор от последнего id,
	// квота — остаток пачки (4).
	wantArgs := []any{[]any{int16(1)}, []any{int64(13)}, 4}
	if !reflect.DeepEqual(pool.queries[2].args, wantArgs) {
		t.Errorf("аргументы дозаполнения = %v, want %v", pool.queries[2].args, wantArgs)
	}
}

// TestDispatchBatch_Empty — активных зон нет: достаточно запроса зон,
// пачка пуста без ошибки.
func TestDispatchBatch_Empty(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{dispatchZonesResult()}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 1000})
	if err != nil {
		t.Fatalf("пустая выборка: %v", err)
	}
	if len(batch) != 0 {
		t.Errorf("пачка = %+v, want пустая", batch)
	}
	if len(pool.queries) != 1 {
		t.Errorf("запросов = %d, want 1 (очереди не читаются без зон)", len(pool.queries))
	}
}

// TestDispatchBatch_Exhausted — все зоны отдали меньше квоты: пачка
// короче Limit (всё pending прочитано), без дозаполнения.
func TestDispatchBatch_Exhausted(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")),
		dispatchDomainsResult(drow(1, 11, "a"), drow(2, 21, "b")),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 {
		t.Fatalf("пачка = %+v, want 2 строки (pending исчерпано)", batch)
	}
	if len(pool.queries) != 2 {
		t.Errorf("запросов = %d, want 2 (дозаполнение не нужно: зон без хвоста нет)", len(pool.queries))
	}
}

func TestDispatchBatch_Validation(t *testing.T) {
	cases := []struct {
		name string
		q    DispatchQuery
		want string
	}{
		{"лимит 0", DispatchQuery{}, "Limit = 0"},
		{"лимит отрицательный", DispatchQuery{Limit: -5}, "Limit = -5"},
		{"вес нулевой", DispatchQuery{Limit: 1000, ZoneWeights: map[int16]int{7: 0}}, "вес зоны 7 = 0"},
		{"вес отрицательный", DispatchQuery{Limit: 1000, ZoneWeights: map[int16]int{7: -2}}, "вес зоны 7 = -2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &dispatchPool{}
			_, err := DispatchBatch(context.Background(), pool, tc.q)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want описание с %q", err, tc.want)
			}
			if !strings.Contains(err.Error(), "scheduler:") {
				t.Errorf("сообщение = %q, want контекст «scheduler:»", err)
			}
			if len(pool.queries) != 0 {
				t.Errorf("невалидный запрос дошёл до БД: %v", pool.queries)
			}
		})
	}
}

func TestDispatchBatch_QueryError(t *testing.T) {
	boom := errors.New("соединение потеряно")

	// Ошибка запроса зон.
	pool := &dispatchPool{err: boom}
	if _, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 1000}); err == nil ||
		!errors.Is(err, boom) || !strings.Contains(err.Error(), "активные зоны") {
		t.Fatalf("err = %v, want обёрнутая ошибка запроса зон", err)
	}

	// Ошибка запроса очередей (зоны прочитаны).
	pool = &dispatchPool{results: []*dispatchRows{dispatchZonesResult(zrow(1, "example.com"))}}
	pool.err = boom
	if _, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 1000}); err == nil ||
		!errors.Is(err, boom) || !strings.Contains(err.Error(), "scheduler: выборка пачки доменов") {
		t.Fatalf("err = %v, want обёрнутая «соединение потеряно»", err)
	}
}

// TestDispatchBatch_ReadErrors — ошибка чтения строки и ошибка итерации
// прерывают выборку обёрнутой ошибкой (оба запроса).
func TestDispatchBatch_ReadErrors(t *testing.T) {
	boom := errors.New("декодирование сорвалось")

	pool := &dispatchPool{results: []*dispatchRows{
		{colNames: dispatchZonesCols, rows: [][]any{zrow(1, "example.com")}, scanErr: boom},
	}}
	_, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 10})
	if err == nil || !errors.Is(err, boom) || !strings.Contains(err.Error(), "активные зоны") {
		t.Fatalf("err = %v, want обёрнутая ошибка чтения зон", err)
	}

	pool = &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com")),
		{colNames: dispatchDomainCols, rows: [][]any{drow(1, 1, "a"), drow(1, 2, "b")}, scanErr: boom},
	}}
	_, err = DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 10})
	if err == nil || !errors.Is(err, boom) || !strings.Contains(err.Error(), "чтение строки") {
		t.Fatalf("err = %v, want обёрнутая ошибка чтения строки", err)
	}

	pool = &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com")),
		{colNames: dispatchDomainCols, rows: [][]any{drow(1, 1, "a")}, err: boom},
	}}
	if _, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 10}); err == nil || !errors.Is(err, boom) {
		t.Fatalf("err = %v, want ошибка итерации (rows.Err)", err)
	}
}

// TestDispatchBatch_ContextCanceled — отменённый контекст отказывает на
// первом же обращении к БД (как реальный pgx).
func TestDispatchBatch_ContextCanceled(t *testing.T) {
	pool := &dispatchPool{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := DispatchBatch(ctx, pool, DispatchQuery{Limit: 1000})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if len(pool.queries) != 0 {
		t.Errorf("обращений к БД быть не должно, got %v", pool.queries)
	}
}

// ---------------------------------------------------------------- контракт SQL

const wantDispatchZonesSQL = "SELECT id, name FROM zones WHERE is_active"

const wantDispatchZoneSQL = "SELECT d.zone_id, d.id, d.label " +
	"FROM unnest($1::int2[], $2::int8[]) AS z(zone_id, after_id) " +
	"JOIN LATERAL (" +
	"SELECT dd.zone_id, dd.id, dd.label FROM domains AS dd " +
	"WHERE dd.zone_id = z.zone_id AND dd.id > z.after_id " +
	"AND dd.check_state = 0 AND dd.status <> 6 " +
	"ORDER BY dd.id LIMIT $3" +
	") AS d ON true"

// TestDispatch_SQLContract — точные тексты обоих запросов и опора на
// схему (миграция 000001): чтение очереди зоны по частичному индексу
// domains_pending_idx (фильтр зоны + ORDER BY id + LIMIT — без окон и
// сортировки всего pending), литералы кодов — из модели, охрана от
// deleted, зоны — только активные.
func TestDispatch_SQLContract(t *testing.T) {
	if dispatchZonesSQL != wantDispatchZonesSQL {
		t.Errorf("dispatchZonesSQL = %q,\nwant %q", dispatchZonesSQL, wantDispatchZonesSQL)
	}
	if dispatchZoneSQL != wantDispatchZoneSQL {
		t.Errorf("dispatchZoneSQL = %q,\nwant %q", dispatchZoneSQL, wantDispatchZoneSQL)
	}

	// Литералы в тексте выше обязаны следовать за кодами модели (ТЗ 6.1,
	// 6.2): при изменении кодов текст пересобирается сам — тест ловит
	// рассинхронизацию рукописного литерала.
	if int16(model.CheckStatePending) != 0 || int16(model.StatusDeleted) != 6 {
		t.Errorf("коды модели изменились (pending=%d deleted=%d): обновите литералы в wantDispatchZoneSQL",
			int16(model.CheckStatePending), int16(model.StatusDeleted))
	}

	up := dispatchReadMigration(t, "000001_init.up.sql")

	// Очередь зоны — по готовому частичному индексу планировщика:
	// LATERAL читает (zone_id, id) WHERE check_state = 0 с LIMIT.
	if !strings.Contains(up, "CREATE INDEX domains_pending_idx") ||
		!strings.Contains(up, "ON domains (zone_id, id) WHERE check_state = 0") {
		t.Error("в миграции 000001 нет domains_pending_idx (zone_id, id) WHERE check_state = 0 — LATERAL-выборка очереди зоны опирается на него (ТЗ 6.1)")
	}
	// Фильтр активных зон — колонка схемы, а не выдумка.
	if !strings.Contains(up, "is_active") {
		t.Error("в миграции 000001 нет zones.is_active — фильтр активных зон ссылается на схему")
	}
	// Весов в схеме нет — источник весов только DispatchQuery.
	if strings.Contains(up, "zones.weight") {
		t.Error("в схеме появилась колонка zones.weight — веса можно брать из БД, а не параметрами")
	}
}

// dispatchReadMigration — SQL миграции из встроенного набора (своя копия
// хелпера internal/ingest: файл самодостаточен, общих тест-хелперов
// параллельные задачи T-030/T-031 не делят).
func dispatchReadMigration(t *testing.T, name string) string {
	t.Helper()
	raw, err := migrations.FS.ReadFile(name)
	if err != nil {
		t.Fatalf("чтение %s из встроенного набора: %v", name, err)
	}
	return string(raw)
}

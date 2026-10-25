// Юнит-тесты выборки пачки планировщиком (T-027, ТЗ 5.3) без БД (стиль
// internal/ingest/apply_add_test.go / zones_test.go): точные SQL-тексты
// двух запросов (активные зоны с работой — domains_pending > 0, T-183;
// LATERAL-префиксы очередей) и их параметры (массивы зон/keyset-курсоров,
// квота — в дозаполнении остаток пачки, T-183), круговое слияние с
// весами, дозаполнение голодных зон, пустой результат, валидация
// параметров, ошибки запроса/чтения, отмена ctx. Реальное поведение
// против живой БД — интеграционные тесты планировщика.
package scheduler

import (
	"context"
	"errors"
	"fmt"
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
	errs    []error // ошибка на каждый вызов Query по одному (nil — успех): инъекция сбоя в середину диалога
	err     error   // ошибка самого Query, если задана
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
	if len(p.errs) > 0 {
		err := p.errs[0]
		p.errs = p.errs[1:]
		if err != nil {
			return nil, err
		}
	} else if p.err != nil {
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

// drowID — drow с меткой "d<id>" для многорядных пачек тестов слияния:
// полное имя домена в ожиданиях — "d<id>.<зона>".
func drowID(zone int16, id int64) []any { return drow(zone, id, fmt.Sprintf("d%d", id)) }

// drowIDs — строки одной зоны по списку id (id по возрастанию — очередь
// зоны; перемешивание проверяет TestDispatchBatch отдельно).
func drowIDs(zone int16, ids ...int64) []any {
	rows := make([]any, len(ids))
	for i, id := range ids {
		rows[i] = drowID(zone, id)
	}
	return rows
}

// drows — заготовка ответа запроса очередей из строк, собранных
// append'ом (элементы []any динамически []any — как у drow/drowIDs).
func drows(rows []any) *dispatchRows {
	out := make([][]any, len(rows))
	for i, r := range rows {
		out[i] = r.([]any)
	}
	return dispatchDomainsResult(out...)
}

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
	// Дозаполнение: только зона 1, после id 13, квота — остаток пачки
	// (4 − 3 = 1): вклад зоны в пачку не превосходит остатка (T-183).
	wantArgs := []any{[]any{int16(1)}, []any{int64(13)}, 1}
	if !reflect.DeepEqual(pool.queries[2].args, wantArgs) {
		t.Errorf("аргументы дозаполнения = %v, want %v", pool.queries[2].args, wantArgs)
	}
}

// TestDispatchBatch_RefillQuotaRemaining — квота раунда дозаполнения —
// остаток пачки (Limit − набрано), не Limit (T-183): несколько голодных
// зон, remaining > 1 < Limit — аргумент LATERAL-запроса дозаполнения
// равен остатку; больше остатка одной зоне недоступно, пачка заполняется.
func TestDispatchBatch_RefillQuotaRemaining(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(
			zrow(1, "example.com"), zrow(2, "example.net"), zrow(3, "example.org"),
			zrow(4, "example.dev"), zrow(5, "example.io"), zrow(6, "example.app"),
		),
		// Квота ceil(10 × 1 / 6) + 1 = 3: зоны 1 и 2 обрезаны (голодные),
		// зоны 3–6 отдали всё (пусты).
		drows(append(drowIDs(1, 11, 12, 13), drowIDs(2, 21, 22, 23)...)),
		// Дозаполнение: зона 1 исчерпана (2 строки хвоста), зона 2
		// отдаёт остаток целиком (4 = квота раунда).
		drows(append(drowIDs(1, 14, 15), drowIDs(2, 24, 25, 26, 27)...)),
		// Пачка полна (5 + 7 ≥ 10) — третьего раунда нет.
		dispatchDomainsResult(),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	// Круговой порядок весов 1: чередование зон 1 и 2, равные ключи —
	// меньший zone_id; пачка обрезана по Limit = 10.
	wantIDs := []int64{11, 21, 12, 22, 13, 23, 14, 24, 15, 25}
	if len(batch) != len(wantIDs) {
		t.Fatalf("пачка = %d строк, want %d", len(batch), len(wantIDs))
	}
	for i, id := range wantIDs {
		if batch[i].DomainID != id {
			t.Fatalf("пачка[%d] = домен %d, want %d (круговой порядок)", i, batch[i].DomainID, id)
		}
	}

	if len(pool.queries) != 3 {
		t.Fatalf("запросов = %d, want 3 (зоны + очереди + дозаполнение)", len(pool.queries))
	}
	// Квота дозаполнения — остаток пачки: 10 − 6 набранных = 4 (не
	// Limit = 10): читать сверх остатка бессмысленно — в слияние сверх
	// не попадает.
	wantArgs := []any{[]any{int16(1), int16(2)}, []any{int64(13), int64(23)}, 4}
	if !reflect.DeepEqual(pool.queries[2].args, wantArgs) {
		t.Errorf("аргументы дозаполнения = %v, want [голодные зоны, курсоры, quota=остаток 4] = %v",
			pool.queries[2].args, wantArgs)
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

// TestDispatchBatch_QuotaMissingZoneWeight — зона вне карты весов
// участвует в квоте раунда с весом 1 (не 0): квота ceil(L × maxW / ΣW) + 1
// считается по нормализованным весам всех активных зон.
func TestDispatchBatch_QuotaMissingZoneWeight(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")),
		dispatchDomainsResult(drow(1, 11, "a")),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{
		Limit:       7,
		ZoneWeights: map[int16]int{1: 5}, // зона 2 вне карты → вес 1
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 1 || batch[0].DomainID != 11 {
		t.Fatalf("пачка = %+v, want единственная строка зоны 1", batch)
	}

	// Квота ceil(7 × 5 / (5 + 1)) + 1 = 7 — вес отсутствующей зоны равен 1.
	wantArgs := []any{[]any{int16(1), int16(2)}, []any{int64(0), int64(0)}, 7}
	if !reflect.DeepEqual(pool.queries[1].args, wantArgs) {
		t.Errorf("аргументы = %v, want [зоны, курсоры, quota=7] = %v (вес зоны без карты — 1)",
			pool.queries[1].args, wantArgs)
	}
}

// TestDispatchBatch_FullBatchStopsRefill — пачка заполнилась в раунде
// дозаполнения: новых запросов нет (break «пачка полная»), обращений —
// зоны + очереди + одно дозаполнение.
func TestDispatchBatch_FullBatchStopsRefill(t *testing.T) {
	r0 := append(drowIDs(1, 11, 12, 13, 14), drow(2, 31, "z"))
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")),
		// Квота ceil(6 × 1 / 2) + 1 = 4: зона 1 обрезана (голодная),
		// зона 2 отдала всё (1 строка).
		drows(r0),
		// Дозаполнение зоны 1 (после id 14): квота — остаток пачки
		// (6 − 5 = 1, T-183), LATERAL возвращает не больше остатка.
		drows(drowIDs(1, 21)),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 6})
	if err != nil {
		t.Fatal(err)
	}
	// Ключи зоны 1 (вес 1): 0.5, 1.5, …; зона 2 — одна строка с ключом 0.5;
	// равные ключи — меньший zone_id.
	want := []DispatchRow{
		{DomainID: 11, ZoneID: 1, Name: "d11.example.com"},
		{DomainID: 31, ZoneID: 2, Name: "z.example.net"},
		{DomainID: 12, ZoneID: 1, Name: "d12.example.com"},
		{DomainID: 13, ZoneID: 1, Name: "d13.example.com"},
		{DomainID: 14, ZoneID: 1, Name: "d14.example.com"},
		{DomainID: 21, ZoneID: 1, Name: "d21.example.com"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("пачка = %+v, want %+v", batch, want)
	}
	if len(pool.queries) != 3 {
		t.Fatalf("запросов = %d, want 3: полная пачка останавливает дозаполнение", len(pool.queries))
	}
}

// TestDispatchBatch_RefillStopsAtExhaustedZone — пустой ответ голодной
// зоны в раунде дозаполнения не останавливает обработку остальных зон:
// continue (не break) — хвост соседней зоны входит в пачку.
func TestDispatchBatch_RefillStopsAtExhaustedZone(t *testing.T) {
	r0 := append(drowIDs(1, 11, 12, 13, 14, 15, 16, 17, 18),
		drowIDs(2, 21, 22, 23, 24, 25, 26, 27, 28)...)
	r0 = append(r0, drow(3, 31, "z1"), drow(3, 32, "z2"), drow(3, 33, "z3"))
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net"), zrow(3, "example.org")),
		// Квота ceil(20 × 1 / 3) + 1 = 8: зоны 1 и 2 обрезаны (голодные),
		// зона 3 отдала всё (3 строки).
		drows(r0),
		// Дозаполнение (квота — остаток 20 − 19 = 1): зона 1 исчерпана
		// (пусто), зона 2 отдаёт хвост.
		drows(drowIDs(2, 29)),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	// Круговой порядок: ключи (rn − 0.5)/1, равные — меньший zone_id.
	wantIDs := []int64{11, 21, 31, 12, 22, 32, 13, 23, 33, 14, 24, 15, 25, 16, 26, 17, 27, 18, 28, 29}
	if len(batch) != len(wantIDs) {
		t.Fatalf("пачка = %d строк, want %d (хвост зоны 2 из дозаполнения вошёл)", len(batch), len(wantIDs))
	}
	for i, id := range wantIDs {
		if batch[i].DomainID != id {
			t.Fatalf("пачка[%d] = домен %d, want %d (круговой порядок с весами 1)", i, batch[i].DomainID, id)
		}
	}
	if len(pool.queries) != 3 {
		t.Fatalf("запросов = %d, want 3 (зоны + очереди + дозаполнение)", len(pool.queries))
	}
}

// TestDispatchBatch_MergeSkipsEmptyZones — зона без строк пропускается
// при построении голов слияния (continue, не break): строки следующих
// зон попадают в пачку.
func TestDispatchBatch_MergeSkipsEmptyZones(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")),
		dispatchDomainsResult(drow(2, 21, "b"), drow(2, 22, "c")), // зона 1 пуста
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	want := []DispatchRow{
		{DomainID: 21, ZoneID: 2, Name: "b.example.net"},
		{DomainID: 22, ZoneID: 2, Name: "c.example.net"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("пачка = %+v, want %+v (пустая зона не глушит слияние)", batch, want)
	}
	if len(pool.queries) != 2 {
		t.Fatalf("запросов = %d, want 2 (дозаполнения нет: зоны не голодные)", len(pool.queries))
	}
}

// TestDispatchBatch_MergeNormalizesMissingWeight — вес зоны вне карты
// нормализуется в 1 и в слиянии: ключ первой строки (1 − 0.5)/1 = 0.5 (не
// +Inf нулевого веса и не 0.25 веса 2) — точный порядок чередования
// с зоной веса 2.
func TestDispatchBatch_MergeNormalizesMissingWeight(t *testing.T) {
	rows := append(drowIDs(1, 11, 12), drowIDs(3, 31, 32)...)
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net"), zrow(3, "example.org")),
		drows(rows), // зона 2 пуста
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{
		Limit:       4,
		ZoneWeights: map[int16]int{3: 2}, // зоны 1 и 2 вне карты → вес 1
	})
	if err != nil {
		t.Fatal(err)
	}
	// Зона 1 (вес 1): ключи 0.5, 1.5; зона 3 (вес 2): 0.25, 0.75.
	want := []DispatchRow{
		{DomainID: 31, ZoneID: 3, Name: "d31.example.org"},
		{DomainID: 11, ZoneID: 1, Name: "d11.example.com"},
		{DomainID: 32, ZoneID: 3, Name: "d32.example.org"},
		{DomainID: 12, ZoneID: 1, Name: "d12.example.com"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("пачка = %+v, want %+v (вес зоны без карты — 1)", batch, want)
	}
}

// TestDispatchBatch_MergeAdvanceKey — ключ продвижения головы зоны
// (rn − 0.5)/вес пересчитывается с тем же нормализованным весом: вторая
// строка зоны без карты идёт по ключу 1.5/1 (не +Inf нулевого веса и не
// 0.75 веса 2), чередуясь с хвостами зон весов 1 и 2.
func TestDispatchBatch_MergeAdvanceKey(t *testing.T) {
	r0 := append(drowIDs(1, 11, 12), drowIDs(3, 31, 32, 33)...)
	r0 = append(r0, drowIDs(4, 41, 42, 43)...)
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(3, "example.org"), zrow(4, "example.dev")),
		drows(r0),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{
		Limit:       8,
		ZoneWeights: map[int16]int{3: 1, 4: 2}, // зона 1 вне карты → вес 1
	})
	if err != nil {
		t.Fatal(err)
	}
	// Ключи: зона 1 — 0.5, 1.5; зона 3 (вес 1) — 0.5, 1.5, 2.5;
	// зона 4 (вес 2) — 0.25, 0.75, 1.25; равные — меньший zone_id.
	want := []DispatchRow{
		{DomainID: 41, ZoneID: 4, Name: "d41.example.dev"},
		{DomainID: 11, ZoneID: 1, Name: "d11.example.com"},
		{DomainID: 31, ZoneID: 3, Name: "d31.example.org"},
		{DomainID: 42, ZoneID: 4, Name: "d42.example.dev"},
		{DomainID: 43, ZoneID: 4, Name: "d43.example.dev"},
		{DomainID: 12, ZoneID: 1, Name: "d12.example.com"},
		{DomainID: 32, ZoneID: 3, Name: "d32.example.org"},
		{DomainID: 33, ZoneID: 3, Name: "d33.example.org"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("пачка = %+v, want %+v (ключ продвижения — с нормализованным весом)", batch, want)
	}
}

// TestDispatchBatch_MergeKeyFormula — ключ головы (rn − 0.5)/вес: первая
// строка зоны веса 2 опережает строку зоны веса 1, вторая (1.5/2 = 0.75)
// обгоняет её вторую (1.5/1) — точный паттерн чередования весов 1 и 2.
func TestDispatchBatch_MergeKeyFormula(t *testing.T) {
	rows := append(drowIDs(1, 11, 12), drowIDs(2, 21, 22)...)
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")),
		drows(rows),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{
		Limit:       4,
		ZoneWeights: map[int16]int{1: 1, 2: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Ключи зоны 1: 0.5, 1.5; зоны 2 (вес 2): 0.25, 0.75.
	want := []DispatchRow{
		{DomainID: 21, ZoneID: 2, Name: "d21.example.net"},
		{DomainID: 11, ZoneID: 1, Name: "d11.example.com"},
		{DomainID: 22, ZoneID: 2, Name: "d22.example.net"},
		{DomainID: 12, ZoneID: 1, Name: "d12.example.com"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("пачка = %+v, want %+v ((rn − 0.5)/вес)", batch, want)
	}
}

// TestDispatchBatch_MergeAdvanceShift — вычитание именно 0.5 в ключе
// продвижения (rn − 0.5)/вес: сдвиг на +1/вес (мутант +0.5) меняет
// чередование — вторая строка зоны веса 4 (ключ 1.5/4 = 0.375) идёт до
// первой строки зоны веса 1 (0.5), а со сдвигом (2.5/4 = 0.625) — после;
// вес 4 против 1 даёт существенную разницу шагов.
func TestDispatchBatch_MergeAdvanceShift(t *testing.T) {
	rows := append(drowIDs(1, 11, 12), drowIDs(2, 21, 22, 23, 24, 25, 26)...)
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")),
		drows(rows),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{
		Limit:       8,
		ZoneWeights: map[int16]int{1: 1, 2: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Ключи зоны 1: 0.5, 1.5; зоны 2 (вес 4): 0.125, 0.375, 0.625, 0.875,
	// 1.125, 1.375 — зона 2 забирает 6 из 8 мест пачки.
	want := []DispatchRow{
		{DomainID: 21, ZoneID: 2, Name: "d21.example.net"},
		{DomainID: 22, ZoneID: 2, Name: "d22.example.net"},
		{DomainID: 11, ZoneID: 1, Name: "d11.example.com"},
		{DomainID: 23, ZoneID: 2, Name: "d23.example.net"},
		{DomainID: 24, ZoneID: 2, Name: "d24.example.net"},
		{DomainID: 25, ZoneID: 2, Name: "d25.example.net"},
		{DomainID: 26, ZoneID: 2, Name: "d26.example.net"},
		{DomainID: 12, ZoneID: 1, Name: "d12.example.com"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("пачка = %+v, want %+v (сдвиг ключей продвижения — 0.5)", batch, want)
	}
}

// TestDispatchBatch_MergeTieSmallerZoneFirst — равные ключи (равные rn и
// веса) разрешаются меньшим zone_id, даже когда зона с большим id идёт
// в справочнике первой.
func TestDispatchBatch_MergeTieSmallerZoneFirst(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		// Порядок справочника: зона 2 раньше зоны 1.
		dispatchZonesResult(zrow(2, "example.net"), zrow(1, "example.com")),
		dispatchDomainsResult(drow(1, 11, "a"), drow(2, 21, "b")),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	want := []DispatchRow{
		{DomainID: 11, ZoneID: 1, Name: "a.example.com"},
		{DomainID: 21, ZoneID: 2, Name: "b.example.net"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("пачка = %+v, want %+v (тай-брейк — меньший zone_id)", batch, want)
	}
}

// TestDispatchBatch_MergeKeyBeatsZoneID — ключ главнее zone_id: голова
// с меньшим ключом берётся раньше, даже если её зона больше по id и
// позже в heads (тай-брейк — только при равенстве ключей).
func TestDispatchBatch_MergeKeyBeatsZoneID(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		// Порядок справочника: зона 2 (вес 3, ключ 1/6) раньше зоны 1 (0.5).
		dispatchZonesResult(zrow(2, "example.net"), zrow(1, "example.com")),
		dispatchDomainsResult(drow(1, 11, "a"), drow(2, 21, "b")),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{
		Limit:       2,
		ZoneWeights: map[int16]int{2: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []DispatchRow{
		{DomainID: 21, ZoneID: 2, Name: "b.example.net"},
		{DomainID: 11, ZoneID: 1, Name: "a.example.com"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("пачка = %+v, want %+v (меньший ключ — раньше)", batch, want)
	}
}

// TestDispatchBatch_MergeLimitCap — слияние останавливается ровно на
// Limit: «лишние» прочитанные строки (запас квоты) в пачку не попадают.
func TestDispatchBatch_MergeLimitCap(t *testing.T) {
	rows := append(drowIDs(1, 11, 12), drowIDs(2, 21, 22)...)
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")),
		// Квота ceil(2 × 1 / 2) + 1 = 2: обе зоны «голодные», прочитано
		// вдвое больше Limit.
		drows(rows),
		dispatchDomainsResult(), // дозаполнение (не выполняется на оригинале)
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	want := []DispatchRow{
		{DomainID: 11, ZoneID: 1, Name: "d11.example.com"},
		{DomainID: 21, ZoneID: 2, Name: "d21.example.net"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("пачка = %+v, want %+v (Limit = 2)", batch, want)
	}
	if len(pool.queries) != 2 {
		t.Fatalf("запросов = %d, want 2: полная пачка — без дозаполнения", len(pool.queries))
	}
}

// TestDispatchBatch_LimitOne — Limit = 1 валиден (нижняя граница):
// пачка из одной строки.
func TestDispatchBatch_LimitOne(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com")),
		dispatchDomainsResult(drow(1, 11, "a")),
	}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	want := []DispatchRow{{DomainID: 11, ZoneID: 1, Name: "a.example.com"}}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("пачка = %+v, want %+v", batch, want)
	}
}

// TestDispatchBatch_ZonesIterError — ошибка итерации справочника зон
// (rows.Err после строк) прерывает выборку обёрнутой ошибкой «активные
// зоны».
func TestDispatchBatch_ZonesIterError(t *testing.T) {
	boom := errors.New("итерация сорвалась")
	pool := &dispatchPool{results: []*dispatchRows{
		{colNames: dispatchZonesCols, rows: [][]any{zrow(1, "example.com")}, err: boom},
	}}

	_, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 10})
	if err == nil || !errors.Is(err, boom) || !strings.Contains(err.Error(), "активные зоны") {
		t.Fatalf("err = %v, want обёрнутая ошибка итерации справочника зон", err)
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

	// Ошибка запроса очередей (зоны прочитаны): сбой внедрён вторым
	// вызовом, сообщение — без контекста зон (отличие от ошибки выше).
	pool = &dispatchPool{
		results: []*dispatchRows{dispatchZonesResult(zrow(1, "example.com"))},
		errs:    []error{nil, boom},
	}
	if _, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 1000}); err == nil ||
		!errors.Is(err, boom) || !strings.Contains(err.Error(), "scheduler: выборка пачки доменов") ||
		strings.Contains(err.Error(), "активные зоны") {
		t.Fatalf("err = %v, want обёрнутая «соединение потеряно» запроса очередей", err)
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

// --- веса зон из конфигурации (T-106) ---------------------------------

// TestResolveZoneWeights — имена зон (ключи scheduler.zone_weights)
// разрешаются в zone_id по справочнику одним запросом: все зоны, не
// только активные (вес неактивной вступит в силу при её включении);
// лишние строки справочника — мимо карты; имена без строки — unknown
// по возрастанию (Warn вызывающего); карта вызывающего не изменяется.
func TestResolveZoneWeights(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(
			zrow(2, "example.net"), // активная или нет — справочник целиком
			zrow(1, "example.com"),
			zrow(3, "example.org"),
		),
	}}
	byName := map[string]int{"example.com": 1, "example.net": 3, "example.org": 2,
		"zzz.missing": 7, "aaa.missing": 5}

	weights, unknown, err := ResolveZoneWeights(context.Background(), pool, byName)
	if err != nil {
		t.Fatal(err)
	}
	want := map[int16]int{1: 1, 2: 3, 3: 2}
	if !reflect.DeepEqual(weights, want) {
		t.Errorf("веса = %v, want %v (имена → zone_id по справочнику)", weights, want)
	}
	// Неизвестные имена — по возрастанию: детерминированный Warn.
	if want := []string{"aaa.missing", "zzz.missing"}; !reflect.DeepEqual(unknown, want) {
		t.Errorf("unknown = %v, want %v", unknown, want)
	}
	// Карта вызывающего не изменяется (конфиг живёт своей жизнью).
	if len(byName) != 5 || byName["example.net"] != 3 {
		t.Errorf("карта вызывающего изменена: %v", byName)
	}
	if len(pool.queries) != 1 || pool.queries[0].sql != resolveZoneWeightsSQL {
		t.Errorf("запросы = %v, want один resolveZoneWeightsSQL", pool.queries)
	}

	// Пустая карта и nil — nil без обращения к БД: равные веса,
	// поведение по умолчанию (ТЗ 5.3: «опционально»).
	for _, m := range []map[string]int{nil, {}} {
		pool := &dispatchPool{}
		weights, unknown, err := ResolveZoneWeights(context.Background(), pool, m)
		if err != nil || unknown != nil || weights != nil {
			t.Fatalf("карта %v: weights=%v unknown=%v err=%v, want nil, nil, nil", m, weights, unknown, err)
		}
		if len(pool.queries) != 0 {
			t.Errorf("карта %v: обращений к БД быть не должно, got %v", m, pool.queries)
		}
	}
}

// TestResolveZoneWeights_UnknownSorted — неизвестные имена возвращаются
// по возрастанию (детерминированный Warn вызывающего): sort.Strings
// обязателен — порядок итерации карты рандомизирован. Один вызов ловит
// удаление сортировки лишь с шансом (итерация может совпасть с
// отсортированным порядком), поэтому несколько проходов по карте из
// пяти имён (шанс совпадения одного прохода ≤ 1/5!).
func TestResolveZoneWeights_UnknownSorted(t *testing.T) {
	byName := map[string]int{
		"m.example": 1, "c.example": 2, "a.example": 3, "x.example": 4, "k.example": 5,
	}
	want := []string{"a.example", "c.example", "k.example", "m.example", "x.example"}
	for i := 0; i < 12; i++ {
		pool := &dispatchPool{results: []*dispatchRows{
			dispatchZonesResult(zrow(1, "example.com")), // все имена unknown
		}}
		_, unknown, err := ResolveZoneWeights(context.Background(), pool, byName)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(unknown, want) {
			t.Fatalf("проход %d: unknown = %v, want %v (по возрастанию)", i, unknown, want)
		}
	}
}

// TestResolveZoneWeights_Errors — ошибки запроса/чтения/итерации
// прерывают разрешение обёрнутой ошибкой; отменённый ctx не доходит до
// БД (как реальный pgx).
func TestResolveZoneWeights_Errors(t *testing.T) {
	boom := errors.New("справочник недоступен")
	weights := map[string]int{"example.com": 2}

	pool := &dispatchPool{err: boom}
	if _, _, err := ResolveZoneWeights(context.Background(), pool, weights); err == nil ||
		!errors.Is(err, boom) || !strings.Contains(err.Error(), "справочник зон") {
		t.Fatalf("err = %v, want обёрнутая ошибка запроса справочника", err)
	}

	pool = &dispatchPool{results: []*dispatchRows{
		{colNames: dispatchZonesCols, rows: [][]any{zrow(1, "example.com")}, scanErr: boom},
	}}
	if _, _, err := ResolveZoneWeights(context.Background(), pool, weights); err == nil ||
		!errors.Is(err, boom) || !strings.Contains(err.Error(), "чтение") {
		t.Fatalf("err = %v, want обёрнутая ошибка чтения справочника", err)
	}

	pool = &dispatchPool{results: []*dispatchRows{
		{colNames: dispatchZonesCols, rows: [][]any{zrow(1, "example.com")}, err: boom},
	}}
	if _, _, err := ResolveZoneWeights(context.Background(), pool, weights); err == nil || !errors.Is(err, boom) {
		t.Fatalf("err = %v, want ошибка итерации (rows.Err)", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pool = &dispatchPool{}
	if _, _, err := ResolveZoneWeights(ctx, pool, weights); err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if len(pool.queries) != 0 {
		t.Errorf("обращений к БД быть не должно, got %v", pool.queries)
	}
}

// TestResolveZoneWeights_Dispatch — веса из конфигурации доходят до
// слияния пачки: имена разрешаются в zone_id, выборка с полученной
// картой чередует зоны пропорционально весам (по образцу TestDispatchBatch
// с ZoneWeights — то же ожидание при весе зоны 2 = 3).
func TestResolveZoneWeights_Dispatch(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")), // разрешение имён
		dispatchZonesResult(zrow(1, "example.com"), zrow(2, "example.net")), // выборка
		dispatchDomainsResult(
			drow(2, 201, "b"),
			drow(1, 102, "c"),
			drow(1, 101, "a"),
		),
	}}

	weights, unknown, err := ResolveZoneWeights(context.Background(), pool, map[string]int{"example.net": 3})
	if err != nil || len(unknown) != 0 {
		t.Fatalf("разрешение весов: unknown=%v err=%v", unknown, err)
	}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 3, ZoneWeights: weights})
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
		t.Errorf("пачка = %+v, want %+v (веса из конфигурации влияют на слияние)", batch, want)
	}
}

// ---------------------------------------------------------------- контракт SQL

const wantDispatchZonesSQL = "SELECT id, name FROM zones WHERE is_active AND domains_pending > 0"

const wantDispatchZoneSQL = "SELECT d.zone_id, d.id, d.label " +
	"FROM unnest($1::int2[], $2::int8[]) AS z(zone_id, after_id) " +
	"JOIN LATERAL (" +
	"SELECT dd.zone_id, dd.id, dd.label FROM domains AS dd " +
	"WHERE dd.zone_id = z.zone_id AND dd.id > z.after_id " +
	"AND dd.check_state = 0 AND dd.status <> 6 " +
	"ORDER BY dd.id LIMIT $3" +
	") AS d ON true"

// TestDispatch_SQLContract — точные тексты обоих запросов и опора на
// схему (миграции 000001/000016): чтение очереди зоны по частичному
// индексу domains_pending_idx (фильтр зоны + ORDER BY id + LIMIT — без
// окон и сортировки всего pending), литералы кодов — из модели, охрана от
// deleted, зоны — только активные и только с работой (domains_pending >
// 0, T-183: колонка T-178 — её семантика и есть предикат диспетчера,
// проба зоны без pending вернула бы 0 строк — круг 1 их больше не
// делает).
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
	// Фильтр работы — колонка счётчиков T-178 (миграция 000016):
	// фильтр диспетчера опирается на живой инкрементальный счётчик.
	counters := dispatchReadMigration(t, "000016_zone_pipeline_counters.up.sql")
	if !strings.Contains(counters, "ADD COLUMN domains_pending bigint NOT NULL DEFAULT 0") {
		t.Error("в миграции 000016 нет zones.domains_pending — фильтр «только зоны с работой» (T-183) ссылается на счётчик T-178")
	}
	// Весов в схеме нет — источник весов только DispatchQuery.
	if strings.Contains(up, "zones.weight") {
		t.Error("в схеме появилась колонка zones.weight — веса можно брать из БД, а не параметрами")
	}
}

// wantResolveZoneWeightsSQL — разрешение имён весов из конфигурации:
// справочник целиком (активные и неактивные зоны — вес неактивной
// вступит в силу при включении зоны без рестарта планировщика).
const wantResolveZoneWeightsSQL = "SELECT id, name FROM zones"

// TestResolveZoneWeights_SQLContract — точный текст запроса и опора на
// схему: уникальность zones.name (миграция 000001) делает разрешение
// «имя → zone_id» однозначным.
func TestResolveZoneWeights_SQLContract(t *testing.T) {
	if resolveZoneWeightsSQL != wantResolveZoneWeightsSQL {
		t.Errorf("resolveZoneWeightsSQL = %q,\nwant %q", resolveZoneWeightsSQL, wantResolveZoneWeightsSQL)
	}
	up := dispatchReadMigration(t, "000001_init.up.sql")
	if !strings.Contains(up, "name            text     NOT NULL UNIQUE") {
		t.Error("в миграции 000001 нет UNIQUE на zones.name — разрешение имён весов опирается на уникальность")
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

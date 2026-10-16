// Юнит-тесты выборки пачки планировщиком (T-027, ТЗ 5.3) без БД (стиль
// internal/ingest/apply_add_test.go / zones_test.go): точный SQL-текст и
// параметры (лимит, смещение, массивы весов), чтение строк в порядке
// выдачи, пустой результат, валидация параметров, ошибки запроса/чтения,
// отмена ctx. Чередование зон и веса живут в SQL — их контракт закреплён
// точным текстом и проверкой индекса/колонок миграции 000001; реальное
// поведение против живой БД — интеграционные тесты планировщика.
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

// dispatchColNames — имена колонок выборки в порядке SELECT.
var dispatchColNames = []string{"domain_id", "zone_id", "name"}

// dispatchRows — pgx.Rows ответа выборки: строки (domain_id int8,
// zone_id int2, name text); Scan раскладывает их позиционно, как это
// делает pgx для DispatchRow.
type dispatchRows struct {
	rows    [][]any
	i       int // индекс текущей строки (0 — до первого Next)
	closed  bool
	err     error // Err() после исчерпания, если задана
	scanErr error // ошибка Scan, если задана
}

func dispatchResult(rows ...[]any) *dispatchRows { return &dispatchRows{rows: rows} }

func (r *dispatchRows) Close() { r.closed = true }

func (r *dispatchRows) Err() error { return r.err }

func (r *dispatchRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *dispatchRows) FieldDescriptions() []pgconn.FieldDescription {
	fds := make([]pgconn.FieldDescription, len(dispatchColNames))
	for i, c := range dispatchColNames {
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
	domainID, ok := dest[0].(*int64)
	if !ok {
		return errors.New("dispatchRows: приёмник 0 — *int64 (domain_id)")
	}
	zoneID, ok := dest[1].(*int16)
	if !ok {
		return errors.New("dispatchRows: приёмник 1 — *int16 (zone_id)")
	}
	name, ok := dest[2].(*string)
	if !ok {
		return errors.New("dispatchRows: приёмник 2 — *string (name)")
	}
	*domainID = row[0].(int64)
	*zoneID = row[1].(int16)
	*name = row[2].(string)
	return nil
}

func (r *dispatchRows) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *dispatchRows) RawValues() [][]byte    { return nil }

// Conn и TypeMap — часть pgx.Rows, не используемая чтением вручную.
func (r *dispatchRows) Conn() *pgx.Conn      { return nil }
func (r *dispatchRows) TypeMap() *pgtype.Map { return nil }

// --- выборка ------------------------------------------------------------

// dispatchRow3 — строка выборки для заготовок: (domain_id, zone_id, name).
func dispatchRow3(domainID int64, zoneID int16, name string) []any {
	return []any{domainID, zoneID, name}
}

func TestDispatchBatch(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{dispatchResult(
		dispatchRow3(101, 1, "a.example.com"),
		dispatchRow3(201, 2, "b.example.net"),
		dispatchRow3(102, 1, "c.example.com"),
	)}}
	q := DispatchQuery{Limit: 3, ZoneWeights: map[int16]int{2: 3, 1: 1}}

	batch, err := DispatchBatch(context.Background(), pool, q)
	if err != nil {
		t.Fatal(err)
	}
	want := []DispatchRow{
		{DomainID: 101, ZoneID: 1, Name: "a.example.com"},
		{DomainID: 201, ZoneID: 2, Name: "b.example.net"},
		{DomainID: 102, ZoneID: 1, Name: "c.example.com"},
	}
	if !reflect.DeepEqual(batch, want) {
		t.Errorf("пачка = %+v, want %+v (порядок выдачи сохраняется — вход T-028)", batch, want)
	}

	if len(pool.queries) != 1 || pool.queries[0].sql != dispatchSelectSQL {
		t.Fatalf("SQL = %v, want dispatchSelectSQL единственным запросом", pool.queries)
	}
	// Веса — параллельные массивы, отсортированные по zone_id.
	wantArgs := []any{[]int16{1, 2}, []int32{1, 3}, 3, 0}
	if !reflect.DeepEqual(pool.queries[0].args, wantArgs) {
		t.Errorf("аргументы = %v, want [zone_id[], weight[], limit, offset] = %v", pool.queries[0].args, wantArgs)
	}
}

// TestDispatchBatch_Paging — повторная выборка: смещение уходит в SQL
// параметром (следующая пачка — не те же строки, пока T-028 не перевёл
// предыдущую в queued).
func TestDispatchBatch_Paging(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{
		dispatchResult(dispatchRow3(1, 1, "a.example.com")),
		dispatchResult(dispatchRow3(9, 9, "z.example.org")),
	}}

	first, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	second, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 1, Offset: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].Name != "a.example.com" {
		t.Errorf("первая пачка = %+v, want a.example.com", first)
	}
	if len(second) != 1 || second[0].Name != "z.example.org" {
		t.Errorf("вторая пачка = %+v, want z.example.org — Offset адресует следующие строки", second)
	}
	wantArgs := []any{[]int16{}, []int32{}, 1, 0}
	if !reflect.DeepEqual(pool.queries[0].args, wantArgs) {
		t.Errorf("аргументы первой = %v, want %v (веса не заданы — пустые массивы, вес 1)", pool.queries[0].args, wantArgs)
	}
	wantArgs[3] = 1
	if !reflect.DeepEqual(pool.queries[1].args, wantArgs) {
		t.Errorf("аргументы второй = %v, want %v (Offset = 1)", pool.queries[1].args, wantArgs)
	}
}

// TestDispatchBatch_Empty — pending-доменов нет: пустой срез без ошибки
// (не ErrNoRows — выборка множественная).
func TestDispatchBatch_Empty(t *testing.T) {
	pool := &dispatchPool{results: []*dispatchRows{dispatchResult()}}

	batch, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 1000})
	if err != nil {
		t.Fatalf("пустая выборка: %v", err)
	}
	if len(batch) != 0 {
		t.Errorf("пачка = %+v, want пустая", batch)
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
		{"смещение отрицательное", DispatchQuery{Limit: 1000, Offset: -1}, "Offset = -1"},
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
	pool := &dispatchPool{err: boom}

	_, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 1000})
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("err = %v, want обёрнутая «соединение потеряно»", err)
	}
	if !strings.Contains(err.Error(), "scheduler: выборка пачки доменов") {
		t.Errorf("сообщение = %q, want контекст «scheduler: выборка пачки доменов»", err)
	}
}

// TestDispatchBatch_ReadErrors — ошибка чтения строки и ошибка итерации
// прерывают выборку обёрнутой ошибкой.
func TestDispatchBatch_ReadErrors(t *testing.T) {
	boom := errors.New("декодирование сорвалось")

	pool := &dispatchPool{results: []*dispatchRows{
		{rows: [][]any{dispatchRow3(1, 1, "a.example.com"), dispatchRow3(2, 1, "b.example.com")}, scanErr: boom},
	}}
	_, err := DispatchBatch(context.Background(), pool, DispatchQuery{Limit: 10})
	if err == nil || !errors.Is(err, boom) || !strings.Contains(err.Error(), "чтение строки") {
		t.Fatalf("err = %v, want обёрнутая ошибка чтения строки", err)
	}

	pool = &dispatchPool{results: []*dispatchRows{
		{rows: [][]any{dispatchRow3(1, 1, "a.example.com")}, err: boom},
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

const wantDispatchSelectSQL = "SELECT t.domain_id, t.zone_id, t.label || '.' || t.zone_name AS name " +
	"FROM (SELECT d.id AS domain_id, d.zone_id, d.label, z.name AS zone_name, " +
	"row_number() OVER (PARTITION BY d.zone_id ORDER BY d.id) AS rn " +
	"FROM domains AS d " +
	"JOIN zones AS z ON z.id = d.zone_id AND z.is_active " +
	"WHERE d.check_state = 0 AND d.status <> 6) AS t " +
	"LEFT JOIN unnest($1::int2[], $2::int4[]) AS w(zone_id, weight) ON w.zone_id = t.zone_id " +
	"ORDER BY (t.rn - 0.5) / COALESCE(w.weight, 1), t.zone_id, t.rn " +
	"LIMIT $3 OFFSET $4"

// TestDispatch_SQLContract — точный текст выборки и опора на схему
// (миграция 000001): чередование зон (row_number PARTITION BY zone_id,
// сортировка по позиции очереди, а не по зоне), веса через unnest с
// дефолтом 1, нумерация внутри зоны по индексу планировщика
// domains_pending_idx, oхрана от deleted и неактивных зон.
func TestDispatch_SQLContract(t *testing.T) {
	if dispatchSelectSQL != wantDispatchSelectSQL {
		t.Errorf("dispatchSelectSQL = %q,\nwant %q", dispatchSelectSQL, wantDispatchSelectSQL)
	}

	// Литералы в тексте выше обязаны следовать за кодами модели (ТЗ 6.1,
	// 6.2): при изменении кодов текст пересобирается сам — тест ловит
	// рассинхронизацию рукописного литерала.
	if int16(model.CheckStatePending) != 0 || int16(model.StatusDeleted) != 6 {
		t.Errorf("коды модели изменились (pending=%d deleted=%d): обновите литералы в wantDispatchSelectSQL",
			int16(model.CheckStatePending), int16(model.StatusDeleted))
	}

	up := dispatchReadMigration(t, "000001_init.up.sql")

	// Нумерация очередей зон — по готовому частичному индексу планировщика.
	if !strings.Contains(up, "CREATE INDEX domains_pending_idx") ||
		!strings.Contains(up, "ON domains (zone_id, id) WHERE check_state = 0") {
		t.Error("в миграции 000001 нет domains_pending_idx (zone_id, id) WHERE check_state = 0 — ORDER BY id внутри зоны опирается на него (ТЗ 6.1)")
	}
	// Фильтр неактивных зон — колонка схемы, а не выдумка.
	if !strings.Contains(up, "is_active") {
		t.Error("в миграции 000001 нет zones.is_active — фильтр неактивных зон ссылается на схему")
	}
	// Весов в схеме нет — массив unnest единственный их источник.
	if strings.Contains(up, "weight") && strings.Contains(up, "zones") && strings.Contains(dispatchSelectSQL, "zones.weight") {
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

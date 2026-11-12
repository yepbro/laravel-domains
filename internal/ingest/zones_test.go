// Unit tests of the reference table of ingest zones (T-014, spec 5.1,
// 5.2 items 6-7): EnsureZone — against a fake ZoneStore (lookup, automatic
// creation with is_active = true, name_unicode, one zone for
// unicode/punycode), SQLZoneStore — the SQL text and parameters against a
// fake pgx pool without a DB (real behavior — the integration tests, T-023).
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

// --- fake ZoneStore-------------------------------------------------

// fakeZoneStore — an in-memory zone store with a call protocol.
type fakeZoneStore struct {
	zones      map[string]model.Zone
	nextID     int16
	byNameErr  error    // the ZoneByName error, if set
	createErr  error    // the CreateZone error, if set
	calls      []string // the protocol: "byName:<name>", "create:<name>"
	createdUni *string  // name_unicode of the last CreateZone
}

func newFakeZoneStore(zones ...model.Zone) *fakeZoneStore {
	f := &fakeZoneStore{zones: make(map[string]model.Zone, len(zones))}
	for _, z := range zones {
		f.zones[z.Name] = z
		if z.ID >= f.nextID {
			f.nextID = z.ID
		}
	}
	return f
}

func (f *fakeZoneStore) ZoneByName(_ context.Context, name string) (model.Zone, error) {
	f.calls = append(f.calls, "byName:"+name)
	if f.byNameErr != nil {
		return model.Zone{}, f.byNameErr
	}
	z, ok := f.zones[name]
	if !ok {
		return model.Zone{}, ErrZoneNotFound
	}
	return z, nil
}

func (f *fakeZoneStore) CreateZone(_ context.Context, name string, nameUnicode *string) (model.Zone, error) {
	f.calls = append(f.calls, "create:"+name)
	f.createdUni = nameUnicode
	if f.createErr != nil {
		return model.Zone{}, f.createErr
	}
	// The ZoneStore contract: an already taken name is not an error, return
	// the existing one.
	if z, ok := f.zones[name]; ok {
		return z, nil
	}
	f.nextID++
	z := model.Zone{ID: f.nextID, Name: name, NameUnicode: nameUnicode, IsActive: true}
	f.zones[name] = z
	return z, nil
}

// --- EnsureZone ---------------------------------------------------------

func TestEnsureZoneExisting(t *testing.T) {
	existing := model.Zone{ID: 7, Name: "com", IsActive: true, DomainsTotal: 10}
	store := newFakeZoneStore(existing)

	z, err := EnsureZone(context.Background(), store, "com")
	if err != nil {
		t.Fatalf("EnsureZone: %v", err)
	}
	if z != existing {
		t.Errorf("зона = %+v, want существующая %+v", z, existing)
	}
	if want := []string{"byName:com"}; !reflect.DeepEqual(store.calls, want) {
		t.Errorf("вызовы = %v, want только поиск %v", store.calls, want)
	}
}

func TestEnsureZoneCreates(t *testing.T) {
	store := newFakeZoneStore()

	z, err := EnsureZone(context.Background(), store, "com")
	if err != nil {
		t.Fatalf("EnsureZone: %v", err)
	}
	if z.Name != "com" || !z.IsActive {
		t.Errorf("зона = %+v, want name=com, is_active=true (ТЗ 5.1)", z)
	}
	if z.NameUnicode != nil {
		t.Errorf("name_unicode = %v, want nil: ASCII-зона совпадает с name (ТЗ 5.2 п. 7)", *z.NameUnicode)
	}
	if want := []string{"byName:com", "create:com"}; !reflect.DeepEqual(store.calls, want) {
		t.Errorf("вызовы = %v, want %v", store.calls, want)
	}
}

// TestEnsureZoneUnicodePunycode — spec 11.4 item 1: both spellings of the
// directory are one zone; the second name finds the created row and does
// not create a new one.
func TestEnsureZoneUnicodePunycode(t *testing.T) {
	store := newFakeZoneStore()

	fromUni, err := EnsureZone(context.Background(), store, "москва")
	if err != nil {
		t.Fatalf("unicode-имя: %v", err)
	}
	fromPuny, err := EnsureZone(context.Background(), store, "xn--80adxhks")
	if err != nil {
		t.Fatalf("punycode-имя: %v", err)
	}
	if fromUni.ID != fromPuny.ID || fromUni.Name != "xn--80adxhks" {
		t.Errorf("зоны %d/%s и %d/…: want одна зона xn--80adxhks", fromUni.ID, fromUni.Name, fromPuny.ID)
	}
	if fromPuny.NameUnicode == nil || *fromPuny.NameUnicode != "москва" {
		t.Errorf("name_unicode = %v, want «москва» (ТЗ 5.2 п. 7)", fromPuny.NameUnicode)
	}
	if want := []string{
		"byName:xn--80adxhks", "create:xn--80adxhks", // the first name — created
		"byName:xn--80adxhks", // the second — found, without creation
	}; !reflect.DeepEqual(store.calls, want) {
		t.Errorf("вызовы = %v, want %v", store.calls, want)
	}
}

// TestEnsureZoneNormalizesInput — the input is normalized by the same
// pipeline (spec 5.2 item 6): case, spaces, the trailing dot.
func TestEnsureZoneNormalizesInput(t *testing.T) {
	store := newFakeZoneStore()
	z, err := EnsureZone(context.Background(), store, " МосквА. ")
	if err != nil {
		t.Fatalf("EnsureZone: %v", err)
	}
	if z.Name != "xn--80adxhks" {
		t.Errorf("Name = %q, want xn--80adxhks", z.Name)
	}
}

// TestEnsureZoneCreateResolvesRace — ZoneStore.CreateZone returns the
// existing row if the zone was created by a concurrent writer (the
// interface contract).
func TestEnsureZoneCreateResolvesRace(t *testing.T) {
	// zones.name is always punycode (spec 5.2): "рф" is stored as xn--p1ai.
	uni := "рф"
	winner := model.Zone{ID: 3, Name: "xn--p1ai", NameUnicode: &uni, IsActive: true}
	store := newFakeZoneStore(winner)
	store.byNameErr = ErrZoneNotFound // the lookup "did not notice" the zone — a race

	z, err := EnsureZone(context.Background(), store, "рф")
	if err != nil {
		t.Fatalf("EnsureZone: %v", err)
	}
	if z.ID != winner.ID {
		t.Errorf("зона = %+v, want строка конкурента %+v", z, winner)
	}
	if len(store.zones) != 1 {
		t.Errorf("зон в хранилище = %d, want 1 (без дублей)", len(store.zones))
	}
}

func TestEnsureZoneStoreErrors(t *testing.T) {
	ctx := context.Background()

	store := newFakeZoneStore()
	store.byNameErr = errors.New("бд недоступна")
	if _, err := EnsureZone(ctx, store, "com"); err == nil || !errors.Is(err, store.byNameErr) {
		t.Errorf("ошибка поиска: %v, want обёрнутая «бд недоступна»", err)
	}
	if want := []string{"byName:com"}; !reflect.DeepEqual(store.calls, want) {
		t.Errorf("вызовы = %v, want только поиск: при ошибке хранилища зона не создаётся", store.calls)
	}

	store = newFakeZoneStore()
	store.createErr = errors.New("бд недоступна")
	_, err := EnsureZone(ctx, store, "com")
	if err == nil || !errors.Is(err, store.createErr) {
		t.Fatalf("ошибка создания: %v, want обёрнутая «бд недоступна»", err)
	}
	if !strings.Contains(err.Error(), "ingest: создание зоны com:") {
		t.Errorf("сообщение = %q, want контекст «ingest: создание зоны com:»", err)
	}
}

func TestEnsureZoneInvalidName(t *testing.T) {
	store := newFakeZoneStore()
	_, err := EnsureZone(context.Background(), store, "не%зона")
	if err == nil {
		t.Fatal("ожидалась ошибка нормализации")
	}
	if !strings.Contains(err.Error(), "ingest: зона") {
		t.Errorf("сообщение = %q, want контекст «ingest: зона …»", err)
	}
	if len(store.calls) != 0 {
		t.Errorf("обращений к хранилищу быть не должно, got %v", store.calls)
	}
}

// --- fake pgx pool for SQLZoneStore-----------------------------------

// zoneColNames — column names in the zoneColumns order.
var zoneColNames = []string{"id", "name", "name_unicode", "is_active", "domains_total", "domains_checked", "updated_at"}

// fakeRows — pgx.Rows with pre-made rows: FieldDescriptions gives the zones
// column names, Scan lays the values out over the destinations the same way
// pgx does it for model.Zone by the db tags (RowToStructByName).
type fakeRows struct {
	rows   [][]any
	i      int // the index of the current row (0 — before the first Next)
	closed bool
}

func zoneRows(rows ...[]any) fakeRows { return fakeRows{rows: rows} }

func (r *fakeRows) Close() { r.closed = true }

func (r *fakeRows) Err() error { return nil }

func (r *fakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription {
	fds := make([]pgconn.FieldDescription, len(zoneColNames))
	for i, c := range zoneColNames {
		fds[i] = pgconn.FieldDescription{Name: c}
	}
	return fds
}

func (r *fakeRows) Next() bool {
	if r.closed || r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.closed || r.i == 0 || r.i > len(r.rows) {
		return errors.New("fakeRows: Scan без Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return errors.New("fakeRows: число приёмников не равно числу колонок")
	}
	for i, d := range dest {
		target := reflect.ValueOf(d).Elem()
		if row[i] == nil {
			target.Set(reflect.Zero(target.Type()))
			continue
		}
		val := reflect.ValueOf(row[i])
		if target.Kind() == reflect.Pointer {
			// a NULL model column (pointer): *string, *time.Time.
			p := reflect.New(val.Type())
			p.Elem().Set(val)
			target.Set(p)
			continue
		}
		target.Set(val)
	}
	return nil
}

func (r *fakeRows) Values() ([]any, error) { return r.rows[r.i-1], nil }

func (r *fakeRows) RawValues() [][]byte { return nil }

// Conn and TypeMap — the part of pgx.Rows not used by CollectOneRow.
func (r *fakeRows) Conn() *pgx.Conn      { return nil }
func (r *fakeRows) TypeMap() *pgtype.Map { return nil }

// fakeZonePool — a db.DB that remembers the SQL and the arguments and hands
// out the pre-made results one by one.
type fakeZonePool struct {
	queries []zoneQuery
	results []fakeRows
	err     error // the error of Query itself, if set
}

type zoneQuery struct {
	sql  string
	args []any
}

func (p *fakeZonePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

func (p *fakeZonePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("Exec не должен вызываться")
}

func (p *fakeZonePool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.queries = append(p.queries, zoneQuery{sql: sql, args: args})
	if p.err != nil {
		return nil, p.err
	}
	rows := p.results[0]
	p.results = p.results[1:]
	return &rows, nil
}

// --- SQLZoneStore: the SQL text and parameters---------------------------------

const wantZoneSelect = "SELECT id, name, name_unicode, is_active, domains_total, domains_checked, updated_at " +
	"FROM zones WHERE name = $1"

const wantZoneInsert = "INSERT INTO zones (name, name_unicode, is_active) " +
	"VALUES ($1, $2, true) ON CONFLICT (name) DO NOTHING " +
	"RETURNING id, name, name_unicode, is_active, domains_total, domains_checked, updated_at"

func TestSQLZoneByName(t *testing.T) {
	updated := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	pool := &fakeZonePool{results: []fakeRows{
		zoneRows([]any{int16(3), "com", nil, true, int64(10), int64(4), updated}),
	}}
	store := NewZoneStore(pool)

	z, err := store.ZoneByName(context.Background(), "com")
	if err != nil {
		t.Fatalf("ZoneByName: %v", err)
	}
	if len(pool.queries) != 1 || pool.queries[0].sql != wantZoneSelect {
		t.Errorf("SQL = %v, want SELECT … WHERE name = $1", pool.queries[0].sql)
	}
	if !reflect.DeepEqual(pool.queries[0].args, []any{"com"}) {
		t.Errorf("аргументы = %v, want [com]", pool.queries[0].args)
	}
	if z.ID != 3 || z.Name != "com" || z.NameUnicode != nil || !z.IsActive ||
		z.DomainsTotal != 10 || z.DomainsChecked != 4 ||
		z.UpdatedAt == nil || !z.UpdatedAt.Equal(updated) {
		t.Errorf("зона = %+v, want строка zones как есть", z)
	}
}

func TestSQLZoneByNameIDN(t *testing.T) {
	pool := &fakeZonePool{results: []fakeRows{
		zoneRows([]any{int16(5), "xn--80adxhks", "москва", true, int64(0), int64(0), nil}),
	}}
	store := NewZoneStore(pool)

	z, err := store.ZoneByName(context.Background(), "xn--80adxhks")
	if err != nil {
		t.Fatalf("ZoneByName: %v", err)
	}
	if z.NameUnicode == nil || *z.NameUnicode != "москва" {
		t.Errorf("name_unicode = %v, want «москва»", z.NameUnicode)
	}
}

func TestSQLZoneByNameNotFound(t *testing.T) {
	pool := &fakeZonePool{results: []fakeRows{zoneRows()}} // zero rows
	store := NewZoneStore(pool)

	if _, err := store.ZoneByName(context.Background(), "net"); !errors.Is(err, ErrZoneNotFound) {
		t.Errorf("ошибка = %v, want ErrZoneNotFound", err)
	}
}

func TestSQLCreateZone(t *testing.T) {
	uni := "москва"
	pool := &fakeZonePool{results: []fakeRows{
		zoneRows([]any{int16(5), "xn--80adxhks", uni, true, int64(0), int64(0), nil}),
	}}
	store := NewZoneStore(pool)

	z, err := store.CreateZone(context.Background(), "xn--80adxhks", &uni)
	if err != nil {
		t.Fatalf("CreateZone: %v", err)
	}
	if len(pool.queries) != 1 || pool.queries[0].sql != wantZoneInsert {
		t.Errorf("SQL = %v, want INSERT … ON CONFLICT DO NOTHING RETURNING …", pool.queries[0].sql)
	}
	if !reflect.DeepEqual(pool.queries[0].args, []any{"xn--80adxhks", &uni}) {
		t.Errorf("аргументы = %v, want [xn--80adxhks, &москва]", pool.queries[0].args)
	}
	if z.ID != 5 || !z.IsActive || z.NameUnicode == nil || *z.NameUnicode != uni {
		t.Errorf("зона = %+v, want созданная строка", z)
	}
}

// TestSQLCreateZoneConflict — an INSERT without a RETURNING row (ON
// CONFLICT): the zone was created by a concurrent writer, read back by a
// repeated lookup.
func TestSQLCreateZoneConflict(t *testing.T) {
	pool := &fakeZonePool{results: []fakeRows{
		zoneRows(), // INSERT … DO NOTHING — no rows
		zoneRows([]any{int16(5), "xn--p1ai", "рф", true, int64(0), int64(0), nil}),
	}}
	store := NewZoneStore(pool)

	z, err := store.CreateZone(context.Background(), "xn--p1ai", nil)
	if err != nil {
		t.Fatalf("CreateZone: %v", err)
	}
	if z.ID != 5 || z.Name != "xn--p1ai" {
		t.Errorf("зона = %+v, want строка конкурента", z)
	}
	if len(pool.queries) != 2 {
		t.Fatalf("запросов = %d, want 2 (INSERT + повторный SELECT)", len(pool.queries))
	}
	if pool.queries[0].sql != wantZoneInsert {
		t.Errorf("первый SQL = %s, want INSERT", pool.queries[0].sql)
	}
	if pool.queries[1].sql != wantZoneSelect {
		t.Errorf("второй SQL = %s, want повторный SELECT", pool.queries[1].sql)
	}
	if !reflect.DeepEqual(pool.queries[1].args, []any{"xn--p1ai"}) {
		t.Errorf("аргументы SELECT = %v, want [xn--p1ai]", pool.queries[1].args)
	}
}

func TestSQLZoneQueryError(t *testing.T) {
	pool := &fakeZonePool{err: errors.New("соединение закрыто")}
	store := NewZoneStore(pool)

	if _, err := store.ZoneByName(context.Background(), "com"); err == nil ||
		!strings.Contains(err.Error(), "db: zones: поиск зоны com:") || !strings.Contains(err.Error(), "соединение закрыто") {
		t.Errorf("ошибка = %v, want обёрнутая «db: zones: поиск зоны com: … соединение закрыто»", err)
	}
	if _, err := store.CreateZone(context.Background(), "com", nil); err == nil ||
		!strings.Contains(err.Error(), "db: zones: создание зоны com:") {
		t.Errorf("ошибка = %v, want обёрнутая «db: zones: создание зоны com: …»", err)
	}
}

// compile-time: SQLZoneStore — a ZoneStore implementation on top of internal/db.
var _ ZoneStore = (*SQLZoneStore)(nil)
var _ db.DB = (*fakeZonePool)(nil)

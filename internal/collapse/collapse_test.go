// Юнит-тесты переноса 2LD → TLD (T-196, ТЗТ §7) на фейковом соединении
// (Conn) без БД: точные SQL-тексты и параметры (контракт, стиль
// zonessync_test.go), порядок шагов §7.2 по протоколу вызовов, батчинг
// §7.3 (окна id, транзакция на батч, 4 UPDATE диапазона вместе),
// идемпотентность/возобновление (сбой после N батчей — повтор
// продолжается), ошибки батча (откат, остановка с именем зоны),
// страховка остатка перед DROP, дедуп ingest_files, инвентаризация с
// allowlist, предусловие check_state = 1 и пост-проверки §7.5.
// Поведение против живого PostgreSQL — test/integration.
package collapse

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// --- SQL-контракты -------------------------------------------------------------

// want-тексты дублируют константы: изменение SQL без обновления ожиданий
// (или наоборот) валит тест громко.
func TestSQLContract(t *testing.T) {
	cases := map[string]string{
		InventorySQL: "SELECT z.id, z.name, z.domains_total, " +
			"(SELECT count(*) FROM domains d WHERE d.zone_id = z.id) AS domains_now, " +
			"(SELECT count(*) FROM subdomain_results r WHERE r.zone_id = z.id), " +
			"(SELECT count(*) FROM laravel_findings f WHERE f.zone_id = z.id), " +
			"(SELECT count(*) FROM laravel_history h WHERE h.zone_id = z.id), " +
			"(SELECT count(*) FROM ingest_files i WHERE i.zone_id = z.id), " +
			"EXISTS (SELECT 1 FROM pg_inherits i " +
			"JOIN pg_class c ON c.oid = i.inhrelid " +
			"JOIN pg_class p ON p.oid = i.inhparent " +
			"JOIN pg_namespace n ON n.oid = p.relnamespace " +
			"WHERE n.nspname = current_schema() " +
			"AND p.relname IN ('domains','subdomain_results','laravel_domains','laravel_findings') " +
			"AND pg_get_expr(c.relpartbound, c.oid) IN (" +
			"format('FOR VALUES IN (%s)', z.id), format('FOR VALUES IN (%L)', z.id::text))) " +
			"FROM zones z WHERE z.name LIKE '%.%' AND z.name <> ALL($1) ORDER BY z.name",
		QueuedSQL: "SELECT zone_id, count(*) FROM domains " +
			"WHERE zone_id = ANY($1::int2[]) AND check_state = $2 GROUP BY zone_id",
		ZoneIDByNameSQL: "SELECT id FROM zones WHERE name = $1",
		ZoneInsertSQL: "INSERT INTO zones (name, name_unicode, is_active) " +
			"VALUES ($1, nullif($2, ''), true) RETURNING id",
		CallPartitionsSQL: "CALL create_zone_partitions($1)",
		movingDropSQL:     "DROP TABLE IF EXISTS moving_ids",
		movingCreateSQL:   "CREATE TEMP TABLE moving_ids AS SELECT id FROM domains WHERE zone_id = $1",
		movingIndexSQL:    "CREATE INDEX ON moving_ids (id)",
		movingRangeSQL:    "SELECT coalesce(min(id), 0), coalesce(max(id), 0) FROM moving_ids",
		spotLabelSQL:      "SELECT label FROM domains WHERE zone_id = $1 ORDER BY id LIMIT 1",
		moveDomainsSQL: "UPDATE domains " +
			"SET zone_id = $1, label = label || '.' || $2 " +
			"WHERE zone_id = $3 AND id >= $4 AND id < $5",
		moveResultsSQL:  "UPDATE subdomain_results SET zone_id = $1 WHERE zone_id = $2 AND domain_id >= $3 AND domain_id < $4",
		moveLaravelSQL:  "UPDATE laravel_domains SET zone_id = $1 WHERE zone_id = $2 AND domain_id >= $3 AND domain_id < $4",
		moveFindingsSQL: "UPDATE laravel_findings SET zone_id = $1 WHERE zone_id = $2 AND domain_id >= $3 AND domain_id < $4",
		moveHistorySQL:  "UPDATE laravel_history SET zone_id = $1 WHERE zone_id = $2",
		filesDedupeSQL: "DELETE FROM ingest_files a USING ingest_files b " +
			"WHERE a.zone_id = $1 AND b.zone_id = $2 " +
			"AND a.action IS NOT DISTINCT FROM b.action AND a.sha256 = b.sha256",
		filesMoveSQL: "UPDATE ingest_files SET zone_id = $1 WHERE zone_id = $2",
		ZonePartsSQL: "SELECT p.relname AS parent, c.relname AS part " +
			"FROM pg_inherits i " +
			"JOIN pg_class c ON c.oid = i.inhrelid " +
			"JOIN pg_class p ON p.oid = i.inhparent " +
			"JOIN pg_namespace n ON n.oid = p.relnamespace " +
			"WHERE n.nspname = current_schema() " +
			"AND p.relname IN ('domains','subdomain_results','laravel_domains','laravel_findings') " +
			"AND pg_get_expr(c.relpartbound, c.oid) IN (" +
			"format('FOR VALUES IN (%s)', $1::smallint), format('FOR VALUES IN (%L)', $1::text))",
		ZoneDeleteSQL:     "DELETE FROM zones WHERE id = $1",
		CheckZonesLeftSQL: "SELECT count(*) FROM zones WHERE name LIKE '%.%' AND name <> ALL($1)",
		CheckOrphansSQL: "SELECT " +
			"(SELECT count(*) FROM domains t WHERE NOT EXISTS (SELECT 1 FROM zones z WHERE z.id = t.zone_id)), " +
			"(SELECT count(*) FROM subdomain_results t WHERE NOT EXISTS (SELECT 1 FROM zones z WHERE z.id = t.zone_id)), " +
			"(SELECT count(*) FROM laravel_domains t WHERE NOT EXISTS (SELECT 1 FROM zones z WHERE z.id = t.zone_id)), " +
			"(SELECT count(*) FROM laravel_findings t WHERE NOT EXISTS (SELECT 1 FROM zones z WHERE z.id = t.zone_id)), " +
			"(SELECT count(*) FROM laravel_history t WHERE NOT EXISTS (SELECT 1 FROM zones z WHERE z.id = t.zone_id))",
		SpotCheckSQL: "SELECT di->'zone'->>'name', di->'domain'->>'label' " +
			"FROM domain_info($1) di WHERE di IS NOT NULL",
		ZoneLeftSQL: "SELECT " +
			"(SELECT count(*) FROM domains WHERE zone_id = $1), " +
			"(SELECT count(*) FROM subdomain_results WHERE zone_id = $1), " +
			"(SELECT count(*) FROM laravel_domains WHERE zone_id = $1), " +
			"(SELECT count(*) FROM laravel_findings WHERE zone_id = $1), " +
			"(SELECT count(*) FROM laravel_history WHERE zone_id = $1), " +
			"(SELECT count(*) FROM ingest_files WHERE zone_id = $1)",
	}
	for sql, want := range cases {
		if sql != want {
			t.Errorf("SQL-контракт:\nполучен  %s\nожидался %s", sql, want)
		}
	}
	if DefaultBatch != 50000 {
		t.Errorf("DefaultBatch = %d, хочу 50000 (§7.3)", DefaultBatch)
	}
}

// --- SpecOf ----------------------------------------------------------------------

func TestSpecOf(t *testing.T) {
	for _, tc := range []struct{ name, tld, prefix string }{
		{"5g.in", "in", "5g"},
		{"city.kawasaki.jp", "jp", "city.kawasaki"},
		{"xn--80adjurfhd.test", "test", "xn--80adjurfhd"},
	} {
		spec, err := SpecOf(ZoneRow{ID: 7, Name: tc.name})
		if err != nil {
			t.Fatalf("SpecOf(%q): %v", tc.name, err)
		}
		if spec.TLD != tc.tld || spec.Prefix != tc.prefix || spec.ID != 7 || spec.Name != tc.name {
			t.Errorf("SpecOf(%q) = %+v, хочу TLD %q, префикс %q", tc.name, spec, tc.tld, tc.prefix)
		}
	}
	for _, bad := range []string{"in", ".in", "5g.", "in."} {
		if _, err := SpecOf(ZoneRow{ID: 1, Name: bad}); err == nil {
			t.Errorf("SpecOf(%q): ожидалась ошибка пустой метки", bad)
		}
	}
}

// --- фейковое соединение ----------------------------------------------------------

// fakeRows — pgx.Rows с заготовленными строками (позиционный Scan,
// стиль statusRows в тестах cmd/crawlerctl).
type fakeRows struct {
	rows [][]any
	i    int
}

func fakeRowsOf(rows ...[]any) *fakeRows { return &fakeRows{rows: rows} }

func (r *fakeRows) Close()                                       {}
func (r *fakeRows) Err() error                                   { return nil }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) Values() ([]any, error)                       { return r.rows[r.i-1], nil }
func (r *fakeRows) RawValues() [][]byte                          { return nil }
func (r *fakeRows) Conn() *pgx.Conn                              { return nil }
func (r *fakeRows) TypeMap() *pgtype.Map                         { return nil }

func (r *fakeRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) {
		return errors.New("fakeRows: Scan без Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("fakeRows: приёмников %d, колонок %d", len(dest), len(row))
	}
	for i, d := range dest {
		v := row[i]
		switch dst := d.(type) {
		case *int16:
			*dst = v.(int16)
		case *int64:
			*dst = v.(int64)
		case *string:
			*dst = v.(string)
		case *bool:
			*dst = v.(bool)
		default:
			return fmt.Errorf("fakeRows: приёмник %T не поддерживается", d)
		}
	}
	return nil
}

// Модель данных фейка: справочник зон, домены, дети четырёх таблиц,
// дедуп-журнал и партиции; moving — temp-таблица шага 3.
type (
	fZone struct {
		id    int16
		total int64
		name  string
	}
	fDomain struct {
		zone   int16
		id     int64
		label  string
		queued bool
	}
	fChild struct {
		zone   int16
		domain int64
	}
	fFile struct {
		zone   int16
		action int16
		sha256 string
	}
)

// call — обращение к соединению/транзакции для контрактных проверок.
type call struct {
	sql  string
	args []any
}

func (c call) String() string { return c.sql + " " + fmt.Sprint(c.args...) }

// fakeConn — Conn без БД: живая семантика SQL переноса — ensure зоны
// (SELECT/INSERT RETURNING), CALL партиций, temp-таблица moving_ids,
// транзакции батчей (4 UPDATE окна: зона+доклейка label у доменов,
// переброска детей по domain_id), history/files одним оператором,
// выборка и DROP партиций, удаление строки зоны. Ошибки — инъекциями
// errE (Exec), queryErr (Query), batchErrFrom (UPDATE доменов батча с
// lo >= значения — имитация конфликта UNIQUE), beginErrAfter (Begin
// после N успешных — имитация сбоя на середине).
type fakeConn struct {
	zones    []fZone
	nextZone int16
	domains  []fDomain
	// дети по таблицам: subdomain_results / laravel_domains /
	// laravel_findings / laravel_history (ключ — константа UPDATE).
	children map[string][]fChild
	files    []fFile
	parts    map[int16][]string
	moving   []int64

	mu      sync.Mutex
	ops     []string
	execs   []call
	queries []call
	begins  int

	errE, queryErr error
	batchErrFrom   int64
	beginErrAfter  int
}

var _ Conn = (*fakeConn)(nil)

func newFakeConn() *fakeConn {
	return &fakeConn{
		zones:    []fZone{{id: 1, name: "in"}, {id: 2, name: "uk"}, {id: 3, name: "crawl.test"}},
		nextZone: 4,
		children: map[string][]fChild{
			moveResultsSQL:  {},
			moveLaravelSQL:  {},
			moveFindingsSQL: {},
			moveHistorySQL:  {},
		},
		parts: map[int16][]string{},
	}
}

// seed2LD — зона 5g.in с доменами ids и по одному ребёнку каждой таблицы
// на домен; партиции по флагу. Возвращает id зоны.
func (f *fakeConn) seed2LD(t *testing.T, ids []int64, withParts bool) int16 {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	zid := f.nextZone
	f.nextZone++
	f.zones = append(f.zones, fZone{id: zid, name: "5g.in"})
	for _, id := range ids {
		f.domains = append(f.domains, fDomain{zone: zid, id: id, label: "example" + strconv.FormatInt(id, 10)})
		f.children[moveResultsSQL] = append(f.children[moveResultsSQL], fChild{zid, id})
		f.children[moveLaravelSQL] = append(f.children[moveLaravelSQL], fChild{zid, id})
		f.children[moveFindingsSQL] = append(f.children[moveFindingsSQL], fChild{zid, id})
		f.children[moveHistorySQL] = append(f.children[moveHistorySQL], fChild{zid, id})
	}
	if withParts {
		f.parts[zid] = zonePartNames(zid)
	}
	return zid
}

// zonePartNames — имена партиций зоны во всех четырёх таблицах (формат
// миграции 000003).
func zonePartNames(zid int16) []string {
	s := strconv.Itoa(int(zid))
	return []string{"domains_z" + s, "subdomain_results_z" + s, "laravel_domains_z" + s, "laravel_findings_z" + s}
}

func (f *fakeConn) zoneID(name string) (int16, bool) {
	for _, z := range f.zones {
		if z.name == name {
			return z.id, true
		}
	}
	return 0, false
}

// zoneByID — существует ли строка зоны (сироты по zone_id).
func (f *fakeConn) zoneByID(id int16) bool {
	for _, z := range f.zones {
		if z.id == id {
			return true
		}
	}
	return false
}

func (f *fakeConn) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("fakeConn: CopyFrom не должен вызываться")
}

// op — протокол шагов (под мьютексом).
func (f *fakeConn) op(op string) { f.ops = append(f.ops, op) }

func (f *fakeConn) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execs = append(f.execs, call{sql, append([]any(nil), args...)})
	if f.errE != nil {
		return pgconn.CommandTag{}, f.errE
	}
	switch sql {
	case movingDropSQL:
		f.op("moving-drop")
		f.moving = nil
		return pgconn.NewCommandTag("DROP TABLE"), nil
	case movingCreateSQL:
		f.op("moving-create")
		f.moving = f.moving[:0]
		zid := args[0].(int16)
		for _, d := range f.domains {
			if d.zone == zid {
				f.moving = append(f.moving, d.id)
			}
		}
		return pgconn.NewCommandTag(fmt.Sprintf("SELECT %d", len(f.moving))), nil
	case movingIndexSQL:
		f.op("moving-index")
		return pgconn.NewCommandTag("CREATE INDEX"), nil
	case CallPartitionsSQL:
		f.op("call-partitions:" + fmt.Sprint(args[0]))
		zid := args[0].(int16)
		if len(f.parts[zid]) == 0 { // идемпотентность процедуры (NOTICE)
			f.parts[zid] = zonePartNames(zid)
		}
		return pgconn.NewCommandTag("CALL"), nil
	case moveHistorySQL:
		f.op("history")
		return f.moveChildren(moveHistorySQL, args[0].(int16), args[1].(int16), 0, 1<<62)
	case filesDedupeSQL:
		f.op("files-dedupe")
		zid, tid := args[0].(int16), args[1].(int16)
		kept := f.files[:0]
		n := int64(0)
		for _, a := range f.files {
			dup := false
			if a.zone == zid {
				for _, b := range f.files {
					if b.zone == tid && b.action == a.action && b.sha256 == a.sha256 {
						dup = true
						break
					}
				}
			}
			if dup {
				n++
				continue
			}
			kept = append(kept, a)
		}
		f.files = kept
		return pgconn.NewCommandTag(fmt.Sprintf("DELETE %d", n)), nil
	case filesMoveSQL:
		f.op("files-move")
		tid, zid := args[0].(int16), args[1].(int16)
		n := int64(0)
		for i := range f.files {
			if f.files[i].zone == zid {
				f.files[i].zone = tid
				n++
			}
		}
		return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", n)), nil
	case ZoneDeleteSQL:
		f.op("zone-delete")
		zid := args[0].(int16)
		for i := range f.zones {
			if f.zones[i].id == zid {
				f.zones = append(f.zones[:i], f.zones[i+1:]...)
				return pgconn.NewCommandTag("DELETE 1"), nil
			}
		}
		return pgconn.NewCommandTag("DELETE 0"), nil
	default:
		if !strings.HasPrefix(sql, "DROP TABLE ") {
			return pgconn.CommandTag{}, errors.New("fakeConn: неожидаемый Exec: " + sql)
		}
		name := strings.Trim(sql[len("DROP TABLE "):], `"`)
		f.op("drop:" + name)
		for zid, ps := range f.parts {
			for i, p := range ps {
				if p == name {
					f.parts[zid] = append(ps[:i], ps[i+1:]...)
					return pgconn.NewCommandTag("DROP TABLE"), nil
				}
			}
		}
		return pgconn.CommandTag{}, fmt.Errorf("fakeConn: партиции %s нет", name)
	}
}

// moveChildren — семантика UPDATE детей окна (под мьютексом).
func (f *fakeConn) moveChildren(table string, tid, zid int16, lo, hi int64) (pgconn.CommandTag, error) {
	n := int64(0)
	for i := range f.children[table] {
		c := &f.children[table][i]
		if c.zone == zid && c.domain >= lo && c.domain < hi {
			c.zone = tid
			n++
		}
	}
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", n)), nil
}

func (f *fakeConn) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, call{sql, append([]any(nil), args...)})
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	switch sql {
	case InventorySQL:
		allow := map[string]bool{}
		for _, a := range args[0].([]string) {
			allow[a] = true
		}
		var rows [][]any
		for _, z := range f.zones {
			if !strings.Contains(z.name, ".") || allow[z.name] {
				continue
			}
			var d, r, lf, h, files int64
			for _, dom := range f.domains {
				if dom.zone == z.id {
					d++
				}
			}
			for _, c := range f.children[moveResultsSQL] {
				if c.zone == z.id {
					r++
				}
			}
			for _, c := range f.children[moveFindingsSQL] {
				if c.zone == z.id {
					lf++
				}
			}
			for _, c := range f.children[moveHistorySQL] {
				if c.zone == z.id {
					h++
				}
			}
			for _, fl := range f.files {
				if fl.zone == z.id {
					files++
				}
			}
			rows = append(rows, []any{z.id, z.name, z.total, d, r, lf, h, files, len(f.parts[z.id]) > 0})
		}
		// ORDER BY z.name — фейк зеркалит SQL.
		sort.Slice(rows, func(i, j int) bool { return rows[i][1].(string) < rows[j][1].(string) })
		return fakeRowsOf(rows...), nil
	case QueuedSQL:
		ids := map[int16]bool{}
		for _, id := range args[0].([]int16) {
			ids[id] = true
		}
		counts := map[int16]int64{}
		for _, d := range f.domains {
			if ids[d.zone] && d.queued {
				counts[d.zone]++
			}
		}
		var rows [][]any
		for id, n := range counts {
			rows = append(rows, []any{id, n})
		}
		return fakeRowsOf(rows...), nil
	case ZoneIDByNameSQL:
		if id, ok := f.zoneID(args[0].(string)); ok {
			return fakeRowsOf([]any{id}), nil
		}
		return fakeRowsOf(), nil
	case ZoneInsertSQL:
		zid := f.nextZone
		f.nextZone++
		f.zones = append(f.zones, fZone{id: zid, name: args[0].(string)})
		return fakeRowsOf([]any{zid}), nil
	case movingRangeSQL:
		if len(f.moving) == 0 {
			return fakeRowsOf([]any{int64(0), int64(0)}), nil
		}
		lo, hi := f.moving[0], f.moving[0]
		for _, id := range f.moving {
			if id < lo {
				lo = id
			}
			if id > hi {
				hi = id
			}
		}
		return fakeRowsOf([]any{lo, hi}), nil
	case spotLabelSQL:
		zid := args[0].(int16)
		var first *fDomain
		for i := range f.domains {
			d := &f.domains[i]
			if d.zone == zid && (first == nil || d.id < first.id) {
				first = d
			}
		}
		if first == nil {
			return fakeRowsOf(), nil
		}
		return fakeRowsOf([]any{first.label}), nil
	case ZoneLeftSQL:
		zid := args[0].(int16)
		var d, r, ld, lf, h, files int64
		for _, dom := range f.domains {
			if dom.zone == zid {
				d++
			}
		}
		for _, c := range f.children[moveResultsSQL] {
			if c.zone == zid {
				r++
			}
		}
		for _, c := range f.children[moveLaravelSQL] {
			if c.zone == zid {
				ld++
			}
		}
		for _, c := range f.children[moveFindingsSQL] {
			if c.zone == zid {
				lf++
			}
		}
		for _, c := range f.children[moveHistorySQL] {
			if c.zone == zid {
				h++
			}
		}
		for _, fl := range f.files {
			if fl.zone == zid {
				files++
			}
		}
		return fakeRowsOf([]any{d, r, ld, lf, h, files}), nil
	case ZonePartsSQL:
		zid := args[0].(int16)
		var rows [][]any
		for _, p := range f.parts[zid] {
			parent := p
			if i := strings.Index(p, "_z"); i > 0 {
				parent = p[:i]
			}
			rows = append(rows, []any{parent, p})
		}
		return fakeRowsOf(rows...), nil
	case CheckZonesLeftSQL:
		allow := map[string]bool{}
		for _, a := range args[0].([]string) {
			allow[a] = true
		}
		var n int64
		for _, z := range f.zones {
			if strings.Contains(z.name, ".") && !allow[z.name] {
				n++
			}
		}
		return fakeRowsOf([]any{n}), nil
	case CheckOrphansSQL:
		var d, r, ld, lf, h int64
		for _, dom := range f.domains {
			if !f.zoneByID(dom.zone) {
				d++
			}
		}
		for table, dst := range map[string]*int64{
			moveResultsSQL: &r, moveLaravelSQL: &ld, moveFindingsSQL: &lf, moveHistorySQL: &h,
		} {
			for _, c := range f.children[table] {
				if !f.zoneByID(c.zone) {
					*dst++
				}
			}
		}
		return fakeRowsOf([]any{d, r, ld, lf, h}), nil
	case SpotCheckSQL:
		// Эмуляция domain_info: зона — самый длинный суффикс имени из
		// справочника, label — оставшиеся метки; домен по (zone, label).
		labels := strings.Split(args[0].(string), ".")
		for k := 2; k <= len(labels); k++ {
			zoneName := strings.Join(labels[k-1:], ".")
			zid, ok := f.zoneID(zoneName)
			if !ok {
				continue
			}
			label := strings.Join(labels[:k-1], ".")
			for _, d := range f.domains {
				if d.zone == zid && d.label == label {
					return fakeRowsOf([]any{zoneName, label}), nil
				}
			}
			return fakeRowsOf(), nil // зона есть, домена нет — NULL
		}
		return fakeRowsOf(), nil
	default:
		return nil, errors.New("fakeConn: неожидаемый Query: " + sql)
	}
}

func (f *fakeConn) Begin(ctx context.Context) (pgx.Tx, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.begins++
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.beginErrAfter > 0 && f.begins > f.beginErrAfter {
		return nil, errors.New("имитация сбоя: соединение потеряно")
	}
	f.op("begin")
	return &fakeTx{conn: f}, nil
}

// fakeTx — транзакция батча: 4 UPDATE окна, фиксация/откат по флагу.
type fakeTx struct {
	pgx.Tx // остальные методы pgx.Tx перенос не использует
	conn   *fakeConn
	done   bool
}

func (tx *fakeTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f := tx.conn
	f.mu.Lock()
	defer f.mu.Unlock()
	f.execs = append(f.execs, call{sql, append([]any(nil), args...)})
	switch sql {
	case moveDomainsSQL:
		lo, hi := args[3].(int64), args[4].(int64)
		f.op(fmt.Sprintf("batch:[%d,%d)", lo, hi))
		if f.batchErrFrom > 0 && lo >= f.batchErrFrom {
			return pgconn.CommandTag{}, errors.New(`duplicate key value violates unique constraint "domains_zone_id_label_key"`)
		}
		n := int64(0)
		for i := range f.domains {
			d := &f.domains[i]
			if d.zone == args[2].(int16) && d.id >= lo && d.id < hi {
				d.zone = args[0].(int16)
				d.label = d.label + "." + args[1].(string)
				n++
			}
		}
		return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", n)), nil
	case moveResultsSQL, moveLaravelSQL, moveFindingsSQL:
		return f.moveChildren(sql, args[0].(int16), args[1].(int16), args[2].(int64), args[3].(int64))
	default:
		return pgconn.CommandTag{}, errors.New("fakeTx: неожидаемый Exec: " + sql)
	}
}

func (tx *fakeTx) Commit(context.Context) error {
	tx.conn.mu.Lock()
	defer tx.conn.mu.Unlock()
	tx.conn.op("commit")
	tx.done = true
	return nil
}

func (tx *fakeTx) Rollback(context.Context) error {
	tx.conn.mu.Lock()
	defer tx.conn.mu.Unlock()
	if !tx.done {
		tx.conn.op("rollback")
	}
	return nil
}

// spec5g — Spec зоны 5g.in по id.
func spec5g(zid int16) Spec {
	return Spec{ID: zid, Name: "5g.in", TLD: "in", Prefix: "5g"}
}

// --- Zone: шаги, батчи, идемпотентность -------------------------------------------

// TestZoneFullTransfer — полный перенос зоны с партициями: порядок шагов
// §7.2, доклейка префикса, переброска всех детей, дедуп-журнал, дроп
// партиций, удаление строки зоны, счётчики результата и спот-чек.
func TestZoneFullTransfer(t *testing.T) {
	f := newFakeConn()
	zid := f.seed2LD(t, []int64{10, 11, 12, 13, 14, 15, 16}, true)
	in, _ := f.zoneID("in")
	// Файлы: обычный + дубль содержимого, уже доставлявшегося в T.
	f.files = []fFile{
		{zone: zid, action: 1, sha256: "aa"},
		{zone: zid, action: 1, sha256: "bb"},
		{zone: in, action: 1, sha256: "aa"}, // конфликт ключа (zone_id, action, sha256)
		{zone: 3, action: 2, sha256: "cc"},  // чужая зона — не трогается
	}

	res, err := Zone(context.Background(), f, spec5g(zid), 3) // batch = 3 → 3 батча
	if err != nil {
		t.Fatalf("Zone: %v", err)
	}

	// Шаги §7.2 по протоколу вызовов.
	wantOps := []string{
		"call-partitions:" + fmt.Sprint(in),
		"moving-drop", "moving-create", "moving-index",
		// батчи по 3 id: 10..16 → окна [10,13), [13,16), [16,19)
		"begin", "batch:[10,13)", "commit",
		"begin", "batch:[13,16)", "commit",
		"begin", "batch:[16,19)", "commit",
		"history", "files-dedupe", "files-move",
		"drop:domains_z" + strconv.Itoa(int(zid)),
		"drop:subdomain_results_z" + strconv.Itoa(int(zid)),
		"drop:laravel_domains_z" + strconv.Itoa(int(zid)),
		"drop:laravel_findings_z" + strconv.Itoa(int(zid)),
		"zone-delete",
	}
	if got := strings.Join(f.ops, ","); got != strings.Join(wantOps, ",") {
		t.Errorf("протокол шагов:\nполучен  %s\nожидался %s", got, strings.Join(wantOps, ","))
	}

	// Домены: зона in, label с доклейкой префикса.
	for _, d := range f.domains {
		if d.zone != in {
			t.Fatalf("домен %d остался в зоне %d, хочу %d (in)", d.id, d.zone, in)
		}
		if want := "example" + strconv.FormatInt(d.id, 10) + ".5g"; d.label != want {
			t.Errorf("домен %d: label %q, хочу %q (доклейка префикса)", d.id, d.label, want)
		}
	}
	// Дети всех таблиц и история переехали.
	for table, rows := range f.children {
		for _, c := range rows {
			if c.zone != in {
				t.Errorf("%s: домен %d остался в зоне %d", table, c.domain, c.zone)
			}
		}
	}
	// Файлы: дубль удалён, остальные перепривязаны; чужой не тронут.
	if len(f.files) != 3 {
		t.Fatalf("ingest_files: строк %d, хочу 3 (дубль удалён)", len(f.files))
	}
	for _, fl := range f.files {
		switch fl.sha256 {
		case "aa", "bb":
			if fl.zone != in {
				t.Errorf("файл %s в зоне %d, хочу in (дубль 5g.in удалён, владелец и bb в in)", fl.sha256, fl.zone)
			}
		case "cc":
			if fl.zone != 3 {
				t.Errorf("чужой файл cc в зоне %d, хочу 3 (не трогается)", fl.zone)
			}
		}
	}
	// Партиции 2LD дропнуты (партиции TLD созданы шагом 2).
	if len(f.parts[zid]) != 0 {
		t.Errorf("партиции 2LD остались: %v", f.parts[zid])
	}
	if len(f.parts[in]) != 4 {
		t.Errorf("партиции зоны in не созданы шагом 2: %v", f.parts[in])
	}
	if _, ok := f.zoneID("5g.in"); ok {
		t.Error("строка зоны 5g.in не удалена")
	}

	// Счётчики результата (все строки — этим запуском).
	if res.Domains != 7 || res.Results != 7 || res.Laravel != 7 || res.Findings != 7 || res.History != 7 {
		t.Errorf("счётчики результата: %+v, хочу по 7 строк каждой таблицы", res)
	}
	if res.Files != 1 || res.FilesDupes != 1 {
		t.Errorf("ingest_files: перенесено %d, дублей удалено %d; хочу 1 и 1", res.Files, res.FilesDupes)
	}
	if res.Batches != 3 || res.TLDZoneNew || res.TLDZoneID != in {
		t.Errorf("итог зоны: батчей %d, зона TLD id %d (новая %v), хочу 3 / %d / существующая",
			res.Batches, res.TLDZoneID, res.TLDZoneNew, in)
	}
	if res.SpotName != "example10.5g.in" || res.SpotLabel != "example10.5g" {
		t.Errorf("спот-чек: имя %q, label %q; хочу example10.5g.in / example10.5g", res.SpotName, res.SpotLabel)
	}
}

// TestZoneBatches — окна батчей §7.3 по возрастанию id: [min, min+batch)
// и далее; каждый батч — 4 UPDATE одного диапазона в одной транзакции.
func TestZoneBatches(t *testing.T) {
	f := newFakeConn()
	zid := f.seed2LD(t, []int64{5, 100, 9000}, false) // разреженные id

	if _, err := Zone(context.Background(), f, spec5g(zid), 50000); err != nil {
		t.Fatalf("Zone: %v", err)
	}
	// Одно окно [5, 50005): все id внутри одной ширины батча.
	if f.begins != 1 {
		t.Errorf("транзакций %d, хочу 1 (все id в одном окне)", f.begins)
	}
	var dom []call
	var child []call
	for _, c := range f.execs {
		switch c.sql {
		case moveDomainsSQL:
			dom = append(dom, c)
		case moveResultsSQL, moveLaravelSQL, moveFindingsSQL:
			child = append(child, c)
		}
	}
	if len(dom) != 1 {
		t.Fatalf("батчей UPDATE доменов %d, хочу 1", len(dom))
	}
	in, _ := f.zoneID("in")
	// Параметры UPDATE доменов: tid, префикс, zid, lo, hi (контракт §7.3).
	if got := dom[0].args; len(got) != 5 || got[0] != in || got[1] != "5g" ||
		got[2] != zid || got[3] != int64(5) || got[4] != int64(50005) {
		t.Errorf("аргументы UPDATE доменов = %v, хочу [%d 5g %d 5 50005]", got, in, zid)
	}
	// Дети — тем же диапазоном, тем же батчем (4 UPDATE вместе).
	if len(child) != 3 {
		t.Fatalf("UPDATE детей в батче: %d таблиц, хочу 3", len(child))
	}
	for _, c := range child {
		if got := c.args; len(got) != 4 || got[0] != in || got[1] != zid ||
			got[2] != int64(5) || got[3] != int64(50005) {
			t.Errorf("аргументы UPDATE детей %s = %v", c.sql[:30], got)
		}
	}
}

// TestZoneEmpty — зона без доменов: батчей нет, остальные шаги идут
// (свернуть пустую 2LD-зону — легальный случай, спот-чека нет).
func TestZoneEmpty(t *testing.T) {
	f := newFakeConn()
	zid := f.seed2LD(t, nil, false)
	res, err := Zone(context.Background(), f, spec5g(zid), 50000)
	if err != nil {
		t.Fatalf("Zone: %v", err)
	}
	if res.Batches != 0 || res.Domains != 0 || res.SpotName != "" {
		t.Errorf("пустая зона: %+v, хочу нули без спот-чека", res)
	}
	if _, ok := f.zoneID("5g.in"); ok {
		t.Error("строка пустой зоны не удалена")
	}
	if !strings.Contains(strings.Join(f.ops, ","), "zone-delete") {
		t.Error("шаг удаления строки не выполнялся")
	}
}

// TestZoneResume — возобновление: сбой после первого батча (имитация
// прерывания команды) оставляет полуперенос; повтор Zone догоняет без
// потерь и дублей; следующий повтор — пустой no-op.
func TestZoneResume(t *testing.T) {
	f := newFakeConn()
	zid := f.seed2LD(t, []int64{1, 2, 3, 4, 5}, false)
	in, _ := f.zoneID("in")

	f.beginErrAfter = 1 // первый батч commit, второй Begin — сбой
	_, err := Zone(context.Background(), f, spec5g(zid), 2)
	if err == nil || !strings.Contains(err.Error(), "зона 5g.in") {
		t.Fatalf("ошибка прерывания = %v, хочу контекст зоны 5g.in", err)
	}
	// Частичный перенос: окно [1,3) уехало, файлы/партиции/зона на месте.
	moved, left := 0, 0
	for _, d := range f.domains {
		switch d.zone {
		case in:
			moved++
		case zid:
			left++
		}
	}
	if moved != 2 || left != 3 {
		t.Fatalf("после сбоя: перенесено %d, осталось %d; хочу 2/3", moved, left)
	}
	if _, ok := f.zoneID("5g.in"); !ok {
		t.Fatal("строка зоны удалена до завершения переноса")
	}

	// Повтор: moving_ids пересобран по остатку, окна адресуют только
	// оставшиеся строки.
	f.beginErrAfter = 0
	res, err := Zone(context.Background(), f, spec5g(zid), 2)
	if err != nil {
		t.Fatalf("повтор Zone: %v", err)
	}
	if res.Domains != 3 {
		t.Errorf("повтор перенёс %d доменов, хочу 3 (остаток; уже перенесённые не трогаются)", res.Domains)
	}
	for _, d := range f.domains {
		if d.zone != in {
			t.Fatalf("домен %d в зоне %d после повтора, хочу in", d.id, d.zone)
		}
		if want := "example" + strconv.FormatInt(d.id, 10) + ".5g"; d.label != want {
			t.Errorf("домен %d: label %q, хочу %q", d.id, d.label, want)
		}
	}
	if _, ok := f.zoneID("5g.in"); ok {
		t.Error("строка зоны не удалена повтором")
	}

	// Повтор по завершённой зоне: перенос не находит ни одной строки
	// (moving_ids пуст, дети/файлы зоны отсутствуют) — все дельты нулевые,
	// шаги идемпотентны (строка зоны восстановлена с новым id, как если бы
	// инвентаризация увидела её повторно).
	f2 := &fakeConn{zones: f.zones, nextZone: f.nextZone, domains: f.domains,
		children: f.children, files: f.files, parts: f.parts}
	zid2 := f2.nextZone
	f2.nextZone++
	f2.zones = append(f2.zones, fZone{id: zid2, name: "5g.in"})
	res, err = Zone(context.Background(), f2, Spec{ID: zid2, Name: "5g.in", TLD: "in", Prefix: "5g"}, 2)
	if err != nil {
		t.Fatalf("повтор по пустой зоне: %v", err)
	}
	if res.Domains != 0 || res.History != 0 || res.Files != 0 || res.Batches != 0 {
		t.Errorf("повтор по пустой зоне перенёс строки: %+v", res)
	}
}

// TestZoneBatchConflict — аномальный конфликт UNIQUE в батче: транзакция
// батча откатывается, команда останавливается с именем зоны и диапазоном;
// последующие шаги (файлы, дроп, удаление зоны) не выполняются.
func TestZoneBatchConflict(t *testing.T) {
	f := newFakeConn()
	zid := f.seed2LD(t, []int64{1, 2, 3, 4}, false)
	f.batchErrFrom = 3 // второй батч [3,5)

	_, err := Zone(context.Background(), f, spec5g(zid), 2)
	if err == nil {
		t.Fatal("конфликт UNIQUE не остановил команду")
	}
	for _, want := range []string{"зона 5g.in", "[3, 5)", "domains_zone_id_label_key"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ошибка %q не содержит %q", err, want)
		}
	}
	ops := strings.Join(f.ops, ",")
	if !strings.Contains(ops, "rollback") {
		t.Error("транзакция батча не откатилась")
	}
	for _, late := range []string{"files-move", "zone-delete", "drop:"} {
		if strings.Contains(ops, late) {
			t.Errorf("после сбоя батча выполнился шаг %q", late)
		}
	}
	// Первый батч остался зафиксированным (возобновляемость).
	in, _ := f.zoneID("in")
	n := 0
	for _, d := range f.domains {
		if d.zone == in {
			n++
		}
	}
	if n != 2 {
		t.Errorf("зафиксированный батч перенёс %d доменов, хочу 2", n)
	}
}

// TestZoneLeftGuard — остаток строк зоны перед DROP (аномалия данных)
// останавливает команду: партиции не дропаются, зона не удаляется.
func TestZoneLeftGuard(t *testing.T) {
	f := newFakeConn()
	zid := f.seed2LD(t, []int64{1, 2}, true)
	// Сирота: ребёнок вне окон батчей (domain_id за пределами
	// [min, min+batch) — единственное окно при batch = 50000).
	f.children[moveResultsSQL] = append(f.children[moveResultsSQL], fChild{zid, 60000})

	_, err := Zone(context.Background(), f, spec5g(zid), 50000)
	if err == nil || !strings.Contains(err.Error(), "перенос неполон") {
		t.Fatalf("ошибка = %v, хочу отказ страховки остатка", err)
	}
	if !strings.Contains(err.Error(), "subdomain_results") {
		t.Errorf("ошибка %v не называет таблицу остатка", err)
	}
	if len(f.parts[zid]) != 4 {
		t.Errorf("партиции дропнуты при остатке строк: %v", f.parts[zid])
	}
	if _, ok := f.zoneID("5g.in"); !ok {
		t.Error("строка зоны удалена при остатке строк")
	}
}

// TestZoneNewTLDZone — целевой зоны нет: создаётся включённой, партиции
// выделяются ей, домены едут в новую зону; вставка — ASCII-зона с
// unicode-пустышкой (nullif → NULL).
func TestZoneNewTLDZone(t *testing.T) {
	f := newFakeConn()
	f.zones = nil // только 2LD-зона; TLD uk создаст перенос
	f.nextZone = 1
	zid := f.nextZone
	f.nextZone++
	f.zones = append(f.zones, fZone{id: zid, name: "co.uk"})
	f.domains = append(f.domains, fDomain{zone: zid, id: 1, label: "shop"})
	f.children[moveResultsSQL] = append(f.children[moveResultsSQL], fChild{zid, 1})

	spec := Spec{ID: zid, Name: "co.uk", TLD: "uk", Prefix: "co"}
	res, err := Zone(context.Background(), f, spec, 50000)
	if err != nil {
		t.Fatalf("Zone: %v", err)
	}
	uk, ok := f.zoneID("uk")
	if !ok {
		t.Fatal("зона uk не создана")
	}
	if res.TLDZoneID != uk || !res.TLDZoneNew {
		t.Errorf("итог зоны: TLD id %d (новая %v)", res.TLDZoneID, res.TLDZoneNew)
	}
	if len(f.parts[uk]) != 4 {
		t.Errorf("партиции новой зоны uk не созданы: %v", f.parts[uk])
	}
	if d := f.domains[0]; d.zone != uk || d.label != "shop.co" {
		t.Errorf("домен: зона %d, label %q; хочу uk / shop.co", d.zone, d.label)
	}
	// Вставка зоны TLD — аргументы контракта шага 1.
	var insert *call
	for i := range f.queries {
		if f.queries[i].sql == ZoneInsertSQL {
			insert = &f.queries[i]
		}
	}
	if insert == nil || insert.args[0] != "uk" || insert.args[1] != "" {
		t.Errorf("вставка зоны TLD = %+v, хочу (uk, \"\" — nullif → NULL)", insert)
	}
}

// TestZoneExecError — отказ БД на подготовительных шагах: контекст зоны
// в ошибке, ничего не переносится.
func TestZoneExecError(t *testing.T) {
	f := newFakeConn()
	zid := f.seed2LD(t, []int64{1}, false)
	f.errE = errors.New("отказ БД")

	if _, err := Zone(context.Background(), f, spec5g(zid), 50000); err == nil ||
		!strings.Contains(err.Error(), "зона 5g.in") {
		t.Errorf("ошибка подготовки = %v, хочу контекст зоны", err)
	}
	if d := f.domains[0]; d.zone != zid {
		t.Errorf("домен перенесён при отказе БД: %+v", d)
	}
}

// TestZoneNilConn — отказ до обращения к БД.
func TestZoneNilConn(t *testing.T) {
	if _, err := Zone(context.Background(), nil, spec5g(1), 0); err == nil ||
		!strings.Contains(err.Error(), "соединение не задано") {
		t.Errorf("Zone(nil) = %v, хочу отказ", err)
	}
}

// --- инвентаризация / предусловие / пост-проверки --------------------------------

func TestInventory(t *testing.T) {
	f := newFakeConn()
	zid := f.seed2LD(t, []int64{1, 2}, true)
	f.files = append(f.files, fFile{zone: zid, action: 1, sha256: "aa"})

	// nil-allowlist нормализуется в пустой массив (ALL(NULL) — NULL);
	// crawl.test — тоже составное имя: вне allowlist оно сворачивается
	// наравне с 5g.in (защита allowlist — единственная, §7.1).
	zones, err := Inventory(context.Background(), f, nil)
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	if len(zones) != 2 {
		t.Fatalf("2LD-зон %d, хочу 2 (5g.in и crawl.test; TLD не считаются)", len(zones))
	}
	z := zones[0]
	if z.Name != "5g.in" || z.ID != zid || z.Domains != 2 || z.Results != 2 ||
		z.Findings != 2 || z.History != 2 || z.Files != 1 || !z.Parts {
		t.Errorf("строка инвентаризации = %+v", z)
	}

	// Allowlist исключает свои зоны из сворачивания (§7.1).
	zones, err = Inventory(context.Background(), f, []string{"5g.in", "crawl.test"})
	if err != nil {
		t.Fatalf("Inventory с allowlist: %v", err)
	}
	if len(zones) != 0 {
		t.Errorf("allowlist-зоны попали в инвентаризацию: %+v", zones)
	}
}

func TestQueuedCounts(t *testing.T) {
	f := newFakeConn()
	zid := f.seed2LD(t, []int64{1, 2, 3}, false)
	f.domains[1].queued = true
	f.domains[2].queued = true

	queued, err := QueuedCounts(context.Background(), f, []int16{zid})
	if err != nil {
		t.Fatalf("QueuedCounts: %v", err)
	}
	if n := queued[zid]; n != 2 {
		t.Errorf("доменов в очереди %d, хочу 2", n)
	}
	// Аргументы контракта: массив зон и код check_state = 1 (queued).
	last := f.queries[len(f.queries)-1]
	if ids := last.args[0].([]int16); len(ids) != 1 || ids[0] != zid {
		t.Errorf("аргумент зон = %v", last.args[0])
	}
	if last.args[1] != int16(1) {
		t.Errorf("check_state = %v, хочу 1 (queued)", last.args[1])
	}

	if queued, err = QueuedCounts(context.Background(), f, nil); err != nil || len(queued) != 0 {
		t.Errorf("пустой список зон: %v, %v", queued, err)
	}
}

func TestPostChecksAndSpot(t *testing.T) {
	f := newFakeConn()
	zid := f.seed2LD(t, []int64{1}, false)
	// crawl.test — allowlist-зона (как в живом конфиге): пост-проверка
	// «2LD-зон вне allowlist не осталось» её не считает.
	allow := []string{"crawl.test"}
	in, _ := f.zoneID("in")

	// До переноса: 2LD-зона есть — пост-проверка её видит.
	left, _, err := PostChecks(context.Background(), f, allow)
	if err != nil {
		t.Fatalf("PostChecks: %v", err)
	}
	if left != 1 {
		t.Errorf("2LD-зон %d, хочу 1 (переноса ещё не было)", left)
	}

	// Сироты: ребёнок в зоне без строки.
	f.children[moveResultsSQL] = append(f.children[moveResultsSQL], fChild{9999, 1})
	_, orphans, err := PostChecks(context.Background(), f, allow)
	if err != nil {
		t.Fatalf("PostChecks с сиротами: %v", err)
	}
	if orphans.Results != 1 || !orphans.Any() {
		t.Errorf("сироты = %+v, хочу 1 в subdomain_results", orphans)
	}

	// Чистое состояние: перенос руками фейка — зона удалена, домен
	// и его дети в TLD с доклеенным label, сирота убрана.
	for i := range f.zones {
		if f.zones[i].id == zid {
			f.zones = append(f.zones[:i], f.zones[i+1:]...)
			break
		}
	}
	f.children[moveResultsSQL] = f.children[moveResultsSQL][:len(f.children[moveResultsSQL])-1]
	f.domains[0].zone = in
	f.domains[0].label = "example1.5g"
	for table := range f.children {
		rows := f.children[table]
		for i := range rows {
			if rows[i].zone == zid {
				rows[i].zone = in
			}
		}
	}
	left, orphans, err = PostChecks(context.Background(), f, allow)
	if err != nil {
		t.Fatalf("PostChecks чистый: %v", err)
	}
	if left != 0 || orphans.Any() {
		t.Errorf("чистое состояние: left %d, сироты %+v", left, orphans)
	}

	// Спот-чек: эмуляция domain_info отвечает зону T и label с префиксом.
	if err := SpotCheck(context.Background(), f, "example1.5g.in", "in", "example1.5g"); err != nil {
		t.Fatalf("SpotCheck: %v", err)
	}
	if err := SpotCheck(context.Background(), f, "example1.5g.in", "in", "не-такой"); err == nil ||
		!strings.Contains(err.Error(), "label") {
		t.Errorf("SpotCheck с чужим label = %v, хочу расхождение label", err)
	}
	if err := SpotCheck(context.Background(), f, "no-such.in", "in", "no-such"); err == nil ||
		!strings.Contains(err.Error(), "не нашёл") {
		t.Errorf("SpotCheck отсутствующего = %v, хочу «не нашёл»", err)
	}
}

// TestAllowlistArg — nil → пустой массив: ALL(NULL) вышибает предикат.
func TestAllowlistArg(t *testing.T) {
	if got := allowlistArg(nil); got == nil || len(got) != 0 {
		t.Errorf("allowlistArg(nil) = %v, хочу пустой не-nil массив", got)
	}
	if got := allowlistArg([]string{"crawl.test"}); len(got) != 1 || got[0] != "crawl.test" {
		t.Errorf("allowlistArg = %v, хочу вход без изменений", got)
	}
}

// TestPrintPlanFormat — таблица плана §7.4: заголовок, колонки зон,
// выравнивание чисел вправо и итоги по TLD.
func TestPrintPlanFormat(t *testing.T) {
	zones := []ZoneRow{
		{ID: 5, Name: "5g.in", Domains: 12, Results: 34, Findings: 5, History: 6, Files: 2, Parts: true},
		{ID: 6, Name: "co.in", Domains: 3456789, Results: 1},
		{ID: 7, Name: "co.uk", Domains: 7, Files: 1},
	}
	var b strings.Builder
	printPlan(&b, zones)
	got := b.String()
	for _, want := range []string{
		"зона", "TLD", "доменов", "результатов", "находок", "история", "файлов", "партиции",
		"5g.in", " in ", "12", "есть",
		"co.in", "default",
		"итого: 2LD-зон 2, доменов 3456801 → зоны in",
		"итого: 2LD-зон 1, доменов 7 → зона uk",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("план не содержит %q:\n%s", want, got)
		}
	}
	// Числа вправо: колонка доменов выровнена по ширине максимального
	// значения (3456789) — узкое 12 дополнено пробелами слева.
	for _, line := range strings.Split(got, "\n") {
		switch {
		case strings.HasPrefix(line, "5g.in"):
			if !strings.Contains(line, "      12 ") {
				t.Errorf("колонка доменов 5g.in не вправо:\n%s", got)
			}
		case strings.HasPrefix(line, "co.in"):
			if !strings.Contains(line, " 3456789 ") {
				t.Errorf("колонка доменов co.in не вправо:\n%s", got)
			}
		}
	}
}

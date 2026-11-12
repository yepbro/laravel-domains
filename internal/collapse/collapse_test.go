// Юнит-тесты переноса 2LD → TLD (T-196, ТЗТ §7) на фейковом соединении
// (Conn) без БД: точные SQL-тексты и параметры (контракт, стиль
// zonessync_test.go), порядок шагов §7.2 по протоколу вызовов, батчинг
// §7.3 (пачки id, транзакция на батч, 4 UPDATE пачки вместе),
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
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

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
		movingChunkSQL:    "SELECT id FROM moving_ids WHERE id > $1 ORDER BY id LIMIT $2",
		spotLabelSQL:      "SELECT label FROM domains WHERE zone_id = $1 ORDER BY id LIMIT 1",
		moveDomainsSQL: "UPDATE domains " +
			"SET zone_id = $1, label = label || '.' || $2 " +
			"WHERE zone_id = $3 AND id >= $4 AND id <= $5",
		moveResultsSQL:  "UPDATE subdomain_results SET zone_id = $1 WHERE zone_id = $2 AND domain_id >= $3 AND domain_id <= $4",
		moveLaravelSQL:  "UPDATE laravel_domains SET zone_id = $1 WHERE zone_id = $2 AND domain_id >= $3 AND domain_id <= $4",
		moveFindingsSQL: "UPDATE laravel_findings SET zone_id = $1 WHERE zone_id = $2 AND domain_id >= $3 AND domain_id <= $4",
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
			t.Errorf("the SQL contract:\n got  %s\n want %s", sql, want)
		}
	}
	if DefaultBatch != 50000 {
		t.Errorf("DefaultBatch = %d, want 50000 (§7.3)", DefaultBatch)
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
			t.Errorf("SpecOf(%q) = %+v, want TLD %q, prefix %q", tc.name, spec, tc.tld, tc.prefix)
		}
	}
	for _, bad := range []string{"in", ".in", "5g.", "in."} {
		if _, err := SpecOf(ZoneRow{ID: 1, Name: bad}); err == nil {
			t.Errorf("SpecOf(%q): an empty-label error expected", bad)
		}
	}
}

// --- фейковое соединение ----------------------------------------------------------

// fakeRows — pgx.Rows с заготовленными строками (позиционный Scan,
// стиль statusRows в тестах cmd/crawlerctl). onClose — наблюдение
// закрытия set'а пулом фейка (контракт T-217: set пост-проверки
// закрывается до следующего запроса).
type fakeRows struct {
	rows    [][]any
	i       int
	onClose func()
}

func fakeRowsOf(rows ...[]any) *fakeRows { return &fakeRows{rows: rows} }

func (r *fakeRows) Close() {
	if r.onClose != nil {
		r.onClose()
		r.onClose = nil
	}
}
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
		return errors.New("fakeRows: Scan without Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("fakeRows: %d destinations, %d columns", len(dest), len(row))
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
			return fmt.Errorf("fakeRows: the destination %T is not supported", d)
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
	// openSets — выданные Query и не закрытые set'ы: модель занятости
	// соединений пула (T-217) — guard CheckOrphansSQL.
	openSets int

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
	return 0, errors.New("fakeConn: CopyFrom must not be called")
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
			return pgconn.CommandTag{}, errors.New("fakeConn: an unexpected Exec: " + sql)
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
		return pgconn.CommandTag{}, fmt.Errorf("fakeConn: the partition %s does not exist", name)
	}
}

// moveChildren — семантика UPDATE детей окна (под мьютексом).
func (f *fakeConn) moveChildren(table string, tid, zid int16, lo, hi int64) (pgconn.CommandTag, error) {
	n := int64(0)
	for i := range f.children[table] {
		c := &f.children[table][i]
		if c.zone == zid && c.domain >= lo && c.domain <= hi {
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
	rows, err := f.query(sql, args)
	if err != nil {
		return nil, err
	}
	// Выдача set'а: не закрытый set держит соединение пула (T-217).
	f.openSets++
	rows.onClose = func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.openSets--
	}
	return rows, nil
}

// query — семантика SQL переноса; mu фейка уже удерживается.
func (f *fakeConn) query(sql string, args []any) (*fakeRows, error) {
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
	case movingChunkSQL:
		// keyset-пачка: id > after, по возрастанию, не более limit.
		after := args[0].(int64)
		limit := args[1].(int)
		ids := append([]int64(nil), f.moving...)
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		out := ids[:0]
		for _, id := range ids {
			if id > after && len(out) < limit {
				out = append(out, id)
			}
		}
		rowsVals := make([][]any, len(out))
		for i, id := range out {
			rowsVals[i] = []any{id}
		}
		return fakeRowsOf(rowsVals...), nil
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
		// Симметричный guard (ревью T-217 P3-1): первый запрос
		// пост-проверок тоже обязан приходить без открытых set'ов —
		// перестановка хелперов местами на реальном пуле тот же дедлок.
		if f.openSets > 0 {
			return nil, fmt.Errorf("fakeConn: CheckZonesLeftSQL with an open set (%d) — post-checks must not issue queries while a set is open (the pool deadlock, T-217)", f.openSets)
		}
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
		// Модель пула CLI (T-217): запрос сирот пост-проверки обязан
		// приходить после Close первого set'а — открытый set держит
		// соединение, и при pool_size = 2 (соединение переноса + один
		// set) Acquire второго запроса ждал бы вечно (дедлок
		// пост-проверок инцидента 29.10). Стиль statusPool.
		if f.openSets > 0 {
			return nil, fmt.Errorf("fakeConn: CheckOrphansSQL with an open set (%d) — the first post-check set closes before the second query (the pool deadlock, T-217)", f.openSets)
		}
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
		return nil, errors.New("fakeConn: an unexpected Query: " + sql)
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
		return nil, errors.New("a failure simulation: the connection is lost")
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
		f.op(fmt.Sprintf("batch:[%d,%d]", lo, hi))
		if f.batchErrFrom > 0 && lo >= f.batchErrFrom {
			return pgconn.CommandTag{}, errors.New(`duplicate key value violates unique constraint "domains_zone_id_label_key"`)
		}
		n := int64(0)
		for i := range f.domains {
			d := &f.domains[i]
			if d.zone == args[2].(int16) && d.id >= lo && d.id <= hi {
				d.zone = args[0].(int16)
				d.label = d.label + "." + args[1].(string)
				n++
			}
		}
		return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", n)), nil
	case moveResultsSQL, moveLaravelSQL, moveFindingsSQL:
		return f.moveChildren(sql, args[0].(int16), args[1].(int16), args[2].(int64), args[3].(int64))
	default:
		return pgconn.CommandTag{}, errors.New("fakeTx: an unexpected Exec: " + sql)
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
		{zone: in, action: 1, sha256: "aa"}, // a key conflict (zone_id, action, sha256)
		{zone: 3, action: 2, sha256: "cc"},  // a foreign zone — not touched
	}

	res, err := Zone(context.Background(), f, spec5g(zid), 3) // batch = 3 → 3 батча
	if err != nil {
		t.Fatalf("Zone: %v", err)
	}

	// Шаги §7.2 по протоколу вызовов.
	wantOps := []string{
		"call-partitions:" + fmt.Sprint(in),
		"moving-drop", "moving-create", "moving-index",
		// батчи по 3 актуальных id: 10..16 → пачки [10,12], [13,15], [16,16]
		"begin", "batch:[10,12]", "commit",
		"begin", "batch:[13,15]", "commit",
		"begin", "batch:[16,16]", "commit",
		"history", "files-dedupe", "files-move",
		"drop:domains_z" + strconv.Itoa(int(zid)),
		"drop:subdomain_results_z" + strconv.Itoa(int(zid)),
		"drop:laravel_domains_z" + strconv.Itoa(int(zid)),
		"drop:laravel_findings_z" + strconv.Itoa(int(zid)),
		"zone-delete",
	}
	if got := strings.Join(f.ops, ","); got != strings.Join(wantOps, ",") {
		t.Errorf("the step protocol:\n got  %s\n want %s", got, strings.Join(wantOps, ","))
	}

	// Домены: зона in, label с доклейкой префикса.
	for _, d := range f.domains {
		if d.zone != in {
			t.Fatalf("domain %d stayed in zone %d, want %d (in)", d.id, d.zone, in)
		}
		if want := "example" + strconv.FormatInt(d.id, 10) + ".5g"; d.label != want {
			t.Errorf("domain %d: label %q, want %q (the prefix appended)", d.id, d.label, want)
		}
	}
	// Дети всех таблиц и история переехали.
	for table, rows := range f.children {
		for _, c := range rows {
			if c.zone != in {
				t.Errorf("%s: domain %d stayed in zone %d", table, c.domain, c.zone)
			}
		}
	}
	// Файлы: дубль удалён, остальные перепривязаны; чужой не тронут.
	if len(f.files) != 3 {
		t.Fatalf("ingest_files: %d rows, want 3 (the dupe removed)", len(f.files))
	}
	for _, fl := range f.files {
		switch fl.sha256 {
		case "aa", "bb":
			if fl.zone != in {
				t.Errorf("the file %s in zone %d, want in (the 5g.in dupe removed, the owner and bb in in)", fl.sha256, fl.zone)
			}
		case "cc":
			if fl.zone != 3 {
				t.Errorf("the foreign file cc in zone %d, want 3 (not touched)", fl.zone)
			}
		}
	}
	// Партиции 2LD дропнуты (партиции TLD созданы шагом 2).
	if len(f.parts[zid]) != 0 {
		t.Errorf("the 2LD partitions remain: %v", f.parts[zid])
	}
	if len(f.parts[in]) != 4 {
		t.Errorf("the partitions of zone in not created by step 2: %v", f.parts[in])
	}
	if _, ok := f.zoneID("5g.in"); ok {
		t.Error("the row of zone 5g.in not deleted")
	}

	// Счётчики результата (все строки — этим запуском).
	if res.Domains != 7 || res.Results != 7 || res.Laravel != 7 || res.Findings != 7 || res.History != 7 {
		t.Errorf("the result counters: %+v, want 7 rows in each table", res)
	}
	if res.Files != 1 || res.FilesDupes != 1 {
		t.Errorf("ingest_files: %d moved, %d dupes removed; want 1 and 1", res.Files, res.FilesDupes)
	}
	if res.Batches != 3 || res.TLDZoneNew || res.TLDZoneID != in {
		t.Errorf("the zone result: %d batches, the TLD zone id %d (new %v), want 3 / %d / existing",
			res.Batches, res.TLDZoneID, res.TLDZoneNew, in)
	}
	if res.SpotName != "example10.5g.in" || res.SpotLabel != "example10.5g" {
		t.Errorf("the spot check: name %q, label %q; want example10.5g.in / example10.5g", res.SpotName, res.SpotLabel)
	}
}

// TestZoneBatches — пачки батчей §7.3 по возрастанию фактических id из
// moving_ids: разреженные id зоны — одна пачка [min, max] (а не окно
// глобального id-пространства), каждый батч — 4 UPDATE одной пачки
// в одной транзакции.
func TestZoneBatches(t *testing.T) {
	f := newFakeConn()
	zid := f.seed2LD(t, []int64{5, 100, 9000}, false) // разреженные id

	if _, err := Zone(context.Background(), f, spec5g(zid), 50000); err != nil {
		t.Fatalf("Zone: %v", err)
	}
	// Одна пачка [5, 9000]: все 3 id — в одном батче (пачка строк,
	// ширина id-пространства между ними не множит батчи).
	if f.begins != 1 {
		t.Errorf("transactions %d, want 1 (all ids in one chunk)", f.begins)
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
		t.Fatalf("%d domain UPDATE batches, want 1", len(dom))
	}
	in, _ := f.zoneID("in")
	// Параметры UPDATE доменов: tid, префикс, zid, lo, hi (контракт §7.3).
	if got := dom[0].args; len(got) != 5 || got[0] != in || got[1] != "5g" ||
		got[2] != zid || got[3] != int64(5) || got[4] != int64(9000) {
		t.Errorf("the domain UPDATE args = %v, want [%d 5g %d 5 9000]", got, in, zid)
	}
	// Дети — тем же диапазоном, тем же батчем (4 UPDATE вместе).
	if len(child) != 3 {
		t.Fatalf("child UPDATEs in the batch: %d tables, want 3", len(child))
	}
	for _, c := range child {
		if got := c.args; len(got) != 4 || got[0] != in || got[1] != zid ||
			got[2] != int64(5) || got[3] != int64(9000) {
			t.Errorf("the child UPDATE args %s = %v", c.sql[:30], got)
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
		t.Errorf("an empty zone: %+v, want zeros without a spot check", res)
	}
	if _, ok := f.zoneID("5g.in"); ok {
		t.Error("the empty zone's row not deleted")
	}
	if !strings.Contains(strings.Join(f.ops, ","), "zone-delete") {
		t.Error("the row deletion step did not run")
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
	if err == nil || !strings.Contains(err.Error(), "zone 5g.in") {
		t.Fatalf("the interrupt error = %v, want the zone 5g.in context", err)
	}
	// Частичный перенос: пачка [1,2] уехала, файлы/партиции/зона на месте.
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
		t.Fatalf("after the failure: %d moved, %d left; want 2/3", moved, left)
	}
	if _, ok := f.zoneID("5g.in"); !ok {
		t.Fatal("the zone's row deleted before the move finished")
	}

	// Повтор: moving_ids пересобран по остатку, пачки адресуют только
	// оставшиеся строки.
	f.beginErrAfter = 0
	res, err := Zone(context.Background(), f, spec5g(zid), 2)
	if err != nil {
		t.Fatalf("the repeated Zone: %v", err)
	}
	if res.Domains != 3 {
		t.Errorf("the rerun moved %d domains, want 3 (the remainder; the already moved ones untouched)", res.Domains)
	}
	for _, d := range f.domains {
		if d.zone != in {
			t.Fatalf("domain %d in zone %d after the rerun, want in", d.id, d.zone)
		}
		if want := "example" + strconv.FormatInt(d.id, 10) + ".5g"; d.label != want {
			t.Errorf("domain %d: label %q, want %q", d.id, d.label, want)
		}
	}
	if _, ok := f.zoneID("5g.in"); ok {
		t.Error("the zone's row not deleted by the rerun")
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
		t.Fatalf("the rerun on an empty zone: %v", err)
	}
	if res.Domains != 0 || res.History != 0 || res.Files != 0 || res.Batches != 0 {
		t.Errorf("the rerun on an empty zone moved rows: %+v", res)
	}
}

// TestZoneBatchConflict — аномальный конфликт UNIQUE в батче: транзакция
// батча откатывается, команда останавливается с именем зоны и диапазоном;
// последующие шаги (файлы, дроп, удаление зоны) не выполняются.
func TestZoneBatchConflict(t *testing.T) {
	f := newFakeConn()
	zid := f.seed2LD(t, []int64{1, 2, 3, 4}, false)
	f.batchErrFrom = 3 // второй батч [3,4]

	_, err := Zone(context.Background(), f, spec5g(zid), 2)
	if err == nil {
		t.Fatal("a UNIQUE conflict did not stop the command")
	}
	for _, want := range []string{"zone 5g.in", "[3, 4]", "domains_zone_id_label_key"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error %q does not contain %q", err, want)
		}
	}
	ops := strings.Join(f.ops, ",")
	if !strings.Contains(ops, "rollback") {
		t.Error("the batch transaction did not roll back")
	}
	for _, late := range []string{"files-move", "zone-delete", "drop:"} {
		if strings.Contains(ops, late) {
			t.Errorf("after the batch failure step %q ran", late)
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
		t.Errorf("the committed batch moved %d domains, want 2", n)
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
	if err == nil || !strings.Contains(err.Error(), "the move is incomplete") {
		t.Fatalf("the error = %v, want the leftover guard rejection", err)
	}
	if !strings.Contains(err.Error(), "subdomain_results") {
		t.Errorf("the error %v does not name the leftover table", err)
	}
	if len(f.parts[zid]) != 4 {
		t.Errorf("partitions dropped with rows left: %v", f.parts[zid])
	}
	if _, ok := f.zoneID("5g.in"); !ok {
		t.Error("the zone's row deleted with rows left")
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
		t.Fatal("the zone uk not created")
	}
	if res.TLDZoneID != uk || !res.TLDZoneNew {
		t.Errorf("the zone result: the TLD id %d (new %v)", res.TLDZoneID, res.TLDZoneNew)
	}
	if len(f.parts[uk]) != 4 {
		t.Errorf("the partitions of the new zone uk not created: %v", f.parts[uk])
	}
	if d := f.domains[0]; d.zone != uk || d.label != "shop.co" {
		t.Errorf("the domain: zone %d, label %q; want uk / shop.co", d.zone, d.label)
	}
	// Вставка зоны TLD — аргументы контракта шага 1.
	var insert *call
	for i := range f.queries {
		if f.queries[i].sql == ZoneInsertSQL {
			insert = &f.queries[i]
		}
	}
	if insert == nil || insert.args[0] != "uk" || insert.args[1] != "" {
		t.Errorf("the TLD zone insert = %+v, want (uk, \"\" — nullif → NULL)", insert)
	}
}

// TestZoneExecError — отказ БД на подготовительных шагах: контекст зоны
// в ошибке, ничего не переносится.
func TestZoneExecError(t *testing.T) {
	f := newFakeConn()
	zid := f.seed2LD(t, []int64{1}, false)
	f.errE = errors.New("DB failure")

	if _, err := Zone(context.Background(), f, spec5g(zid), 50000); err == nil ||
		!strings.Contains(err.Error(), "zone 5g.in") {
		t.Errorf("the prepare error = %v, want the zone context", err)
	}
	if d := f.domains[0]; d.zone != zid {
		t.Errorf("a domain moved on a DB failure: %+v", d)
	}
}

// TestZoneNilConn — отказ до обращения к БД.
func TestZoneNilConn(t *testing.T) {
	if _, err := Zone(context.Background(), nil, spec5g(1), 0); err == nil ||
		!strings.Contains(err.Error(), "connection not set") {
		t.Errorf("Zone(nil) = %v, want a rejection", err)
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
		t.Fatalf("%d 2LD zones, want 2 (5g.in and crawl.test; TLDs do not count)", len(zones))
	}
	z := zones[0]
	if z.Name != "5g.in" || z.ID != zid || z.Domains != 2 || z.Results != 2 ||
		z.Findings != 2 || z.History != 2 || z.Files != 1 || !z.Parts {
		t.Errorf("the inventory row = %+v", z)
	}

	// Allowlist исключает свои зоны из сворачивания (§7.1).
	zones, err = Inventory(context.Background(), f, []string{"5g.in", "crawl.test"})
	if err != nil {
		t.Fatalf("Inventory with an allowlist: %v", err)
	}
	if len(zones) != 0 {
		t.Errorf("allowlist zones made it into the inventory: %+v", zones)
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
		t.Errorf("queued domains %d, want 2", n)
	}
	// Аргументы контракта: массив зон и код check_state = 1 (queued).
	last := f.queries[len(f.queries)-1]
	if ids := last.args[0].([]int16); len(ids) != 1 || ids[0] != zid {
		t.Errorf("the zones argument = %v", last.args[0])
	}
	if last.args[1] != int16(1) {
		t.Errorf("check_state = %v, want 1 (queued)", last.args[1])
	}

	if queued, err = QueuedCounts(context.Background(), f, nil); err != nil || len(queued) != 0 {
		t.Errorf("an empty zone list: %v, %v", queued, err)
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
		t.Errorf("%d 2LD zones, want 1 (no move yet)", left)
	}

	// Сироты: ребёнок в зоне без строки.
	f.children[moveResultsSQL] = append(f.children[moveResultsSQL], fChild{9999, 1})
	_, orphans, err := PostChecks(context.Background(), f, allow)
	if err != nil {
		t.Fatalf("PostChecks with orphans: %v", err)
	}
	if orphans.Results != 1 || !orphans.Any() {
		t.Errorf("orphans = %+v, want 1 in subdomain_results", orphans)
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
		t.Fatalf("PostChecks clean: %v", err)
	}
	if left != 0 || orphans.Any() {
		t.Errorf("the clean state: left %d, orphans %+v", left, orphans)
	}

	// Спот-чек: эмуляция domain_info отвечает зону T и label с префиксом.
	if err := SpotCheck(context.Background(), f, "example1.5g.in", "in", "example1.5g"); err != nil {
		t.Fatalf("SpotCheck: %v", err)
	}
	if err := SpotCheck(context.Background(), f, "example1.5g.in", "in", "no-such"); err == nil ||
		!strings.Contains(err.Error(), "label") {
		t.Errorf("SpotCheck with a foreign label = %v, want a label mismatch", err)
	}
	if err := SpotCheck(context.Background(), f, "no-such.in", "in", "no-such"); err == nil ||
		!strings.Contains(err.Error(), "found no domain") {
		t.Errorf("SpotCheck of a missing domain = %v, want «found no domain»", err)
	}
}

// TestPostChecksClosesSetBeforeOrphans — регрессия T-217: set'ы
// пост-проверок закрываются до следующего запроса. Guard'ы фейка
// (CheckZonesLeftSQL/CheckOrphansSQL при открытом set'е) моделируют пул
// CLI из 2 соединений: соединение переноса + один открытый set — второй
// запрос ждал бы третье соединение вечно (дедлок инцидента 29.10; живой
// контур — интеграционный TestZonesCollapse_SmallPoolDeadlock).
// Итоговое openSets == 0 закрывает и утечку второго set'а, и
// перестановку хелперов (ревью T-217 P3-1).
func TestPostChecksClosesSetBeforeOrphans(t *testing.T) {
	f := newFakeConn()
	f.seed2LD(t, []int64{1}, false)
	if _, _, err := PostChecks(context.Background(), f, []string{"crawl.test"}); err != nil {
		t.Fatalf("PostChecks: %v", err)
	}
	f.mu.Lock()
	open := f.openSets
	f.mu.Unlock()
	if open != 0 {
		t.Errorf("after PostChecks %d open sets, want 0 (both sets close)", open)
	}
}

// TestAllowlistArg — nil → пустой массив: ALL(NULL) вышибает предикат.
func TestAllowlistArg(t *testing.T) {
	if got := allowlistArg(nil); got == nil || len(got) != 0 {
		t.Errorf("allowlistArg(nil) = %v, want an empty non-nil slice", got)
	}
	if got := allowlistArg([]string{"crawl.test"}); len(got) != 1 || got[0] != "crawl.test" {
		t.Errorf("allowlistArg = %v, want the input unchanged", got)
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
		"zone", "TLD", "domains", "results", "findings", "history", "files", "partitions",
		"5g.in", " in ", "12", "yes",
		"co.in", "default",
		"total: 2 2LD zones, 3456801 domains → zones in",
		"total: 1 2LD zones, 7 domains → zone uk",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the plan does not contain %q:\n%s", want, got)
		}
	}
	// Числа вправо: колонка доменов выровнена по ширине максимального
	// значения (3456789) — узкое 12 дополнено пробелами слева.
	for _, line := range strings.Split(got, "\n") {
		switch {
		case strings.HasPrefix(line, "5g.in"):
			if !strings.Contains(line, "      12 ") {
				t.Errorf("the domains column of 5g.in not right-aligned:\n%s", got)
			}
		case strings.HasPrefix(line, "co.in"):
			if !strings.Contains(line, " 3456789 ") {
				t.Errorf("the domains column of co.in not right-aligned:\n%s", got)
			}
		}
	}
}

// hangDropConn — Conn для теста бюджета уборки moving_ids (T-229): Exec
// DROP висит до отмены контекста (зависший PostgreSQL — команда принята,
// ответа нет); отмену фиксирует закрытие канала. Прочие методы не
// вызываются и падают при вызове.
type hangDropConn struct {
	canceled chan struct{}
	once     sync.Once
}

var _ Conn = (*hangDropConn)(nil)

func (h *hangDropConn) Exec(ctx context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	if sql != movingDropSQL {
		return pgconn.CommandTag{}, nil
	}
	<-ctx.Done()
	h.once.Do(func() { close(h.canceled) })
	return pgconn.CommandTag{}, ctx.Err()
}

func (h *hangDropConn) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("hangDropConn: CopyFrom must not be called")
}

func (h *hangDropConn) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("hangDropConn: Query must not be called")
}

func (h *hangDropConn) Begin(context.Context) (pgx.Tx, error) {
	return nil, errors.New("hangDropConn: Begin must not be called")
}

// TestDropMovingTimeout — уборка moving_ids обязана отменяться
// собственным бюджетом времени (T-229): зависший DROP не удерживает
// соединение и выход CLI неограниченно; ошибка логируется Warn, не
// возвращаясь (затенять результат команды ей нечем).
func TestDropMovingTimeout(t *testing.T) {
	if movingDropTimeout != 5*time.Second {
		t.Errorf("movingDropTimeout = %s, want 5s (modeled on discardTimeout, ingest)", movingDropTimeout)
	}
	conn := &hangDropConn{canceled: make(chan struct{})}
	var logBuf strings.Builder
	log := slog.New(slog.NewTextHandler(&logBuf, nil))

	// Бюджет теста короткий: проверяется факт отмены контекста, а не
	// величина продуктового бюджета (она — контрактом выше).
	const budget = 50 * time.Millisecond
	started := time.Now()
	dropMoving(conn, budget, log)
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Errorf("the cleanup hung for %s — the %s budget did not cancel the hung DROP", elapsed, budget)
	}
	select {
	case <-conn.canceled:
	default:
		t.Error("Exec did not see the context cancel (a context without a deadline would hang forever)")
	}
	if out := logBuf.String(); !strings.Contains(out, "moving_ids") {
		t.Errorf("the cleanup error did not reach the log (Warn): %q", out)
	}
}

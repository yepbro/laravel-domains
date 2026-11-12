// Unit tests of the 2LD → TLD move (T-196, spec §7) on a fake connection
// (Conn) without a DB: the exact SQL texts and parameters (a contract, the
// style of zonessync_test.go), the order of the steps §7.2 by the call
// protocol, batching §7.3 (chunks of ids, a transaction per batch, the
// 4 UPDATEs of a chunk together), idempotency/resumption (a failure after
// N batches — the rerun continues), batch errors (a rollback, a stop with
// the zone name), the remainder guard before the DROP, the ingest_files
// dedup, the inventory with an allowlist, the check_state = 1
// precondition and the post-checks §7.5. The behavior against a live
// PostgreSQL — test/integration.
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

// --- SQL contracts -------------------------------------------------------------

// The want texts duplicate the constants: changing the SQL without
// updating the expectations (or vice versa) fails the test loudly.
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

// --- fake connection ----------------------------------------------------------

// fakeRows — pgx.Rows with pre-prepared rows (a positional Scan, the style
// of statusRows in the tests of cmd/crawlerctl). onClose — the observation
// of the set closing by the pool of the fake (the T-217 contract: the
// post-check set is closed before the next query).
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

// The data model of the fake: the zone reference table, the domains, the
// children of the four tables, the dedup journal and the partitions;
// moving is the temp table of step 3.
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

// call — a use of the connection/transaction for contract checks.
type call struct {
	sql  string
	args []any
}

func (c call) String() string { return c.sql + " " + fmt.Sprint(c.args...) }

// fakeConn — a Conn without a DB: the live semantics of the move SQL — the
// zone ensure (SELECT/INSERT RETURNING), the partition CALL, the moving_ids
// temp table, the batch transactions (4 UPDATEs of a window: the zone + the
// label append at the domains, the children re-zoning by domain_id),
// history/files by one statement, the partition selection and DROP, the
// zone row deletion. Errors — via the injections errE (Exec), queryErr
// (Query), batchErrFrom (a domains UPDATE of a batch with lo >= the value —
// a UNIQUE conflict simulation), beginErrAfter (Begin after N successful
// ones — a mid-way failure simulation).
type fakeConn struct {
	zones    []fZone
	nextZone int16
	domains  []fDomain
	// The children by tables: subdomain_results / laravel_domains /
	// laravel_findings / laravel_history (the key is the UPDATE constant).
	children map[string][]fChild
	files    []fFile
	parts    map[int16][]string
	moving   []int64

	mu      sync.Mutex
	ops     []string
	execs   []call
	queries []call
	begins  int
	// openSets — the sets issued by Query and not closed: a model of the
	// pool connection occupancy (T-217) — the CheckOrphansSQL guard.
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

// seed2LD — the zone 5g.in with the domains ids and one child of each
// table per domain; partitions per the flag. Returns the zone id.
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

// zonePartNames — the names of the zone partitions in all four tables
// (the format of migration 000003).
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

// zoneByID — does the zone row exist (orphans by zone_id)?
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

// op — the protocol of the steps (under the mutex).
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
		if len(f.parts[zid]) == 0 { // the idempotency of the procedure (NOTICE)
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

// moveChildren — the semantics of the children UPDATE of a window (under
// the mutex).
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
	// Set delivery: an unclosed set holds the pool connection (T-217).
	f.openSets++
	rows.onClose = func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.openSets--
	}
	return rows, nil
}

// query — the semantics of the move SQL; the fake's mu is already held.
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
		// ORDER BY z.name — the fake mirrors the SQL.
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
		// A keyset chunk: id > after, ascending, at most limit.
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
		// A symmetric guard (the T-217 review P3-1): the first post-check
		// query must also arrive with no open sets — swapping the helpers
		// places the same deadlock on a real pool.
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
		// A model of the CLI pool (T-217): the orphans query of the
		// post-check must arrive after the Close of the first set — an
		// open set holds the connection, and with pool_size = 2 (the move
		// connection + one set) the Acquire of the second query would wait
		// forever (the post-check deadlock of the 29.10 incident). The
		// style of statusPool.
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
		// A domain_info emulation: the zone is the longest suffix of the
		// name from the reference table, the label is the remaining
		// labels; the domain by (zone, label).
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
			return fakeRowsOf(), nil // the zone exists, the domain does not — NULL
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

// fakeTx — the batch transaction: 4 UPDATEs of a window, commit/rollback
// per the flag.
type fakeTx struct {
	pgx.Tx // the move does not use the other methods of pgx.Tx
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

// spec5g — the Spec of the zone 5g.in by id.
func spec5g(zid int16) Spec {
	return Spec{ID: zid, Name: "5g.in", TLD: "in", Prefix: "5g"}
}

// --- Zone: steps, batches, idempotency -------------------------------------------

// TestZoneFullTransfer — a full move of a zone with partitions: the order
// of the steps §7.2, the prefix append, the re-zoning of all the children,
// the dedup journal, the partition drop, the zone row deletion, the result
// counters and the spot check.
func TestZoneFullTransfer(t *testing.T) {
	f := newFakeConn()
	zid := f.seed2LD(t, []int64{10, 11, 12, 13, 14, 15, 16}, true)
	in, _ := f.zoneID("in")
	// Files: a regular one + a duplicate of content already delivered to T.
	f.files = []fFile{
		{zone: zid, action: 1, sha256: "aa"},
		{zone: zid, action: 1, sha256: "bb"},
		{zone: in, action: 1, sha256: "aa"}, // a key conflict (zone_id, action, sha256)
		{zone: 3, action: 2, sha256: "cc"},  // a foreign zone — not touched
	}

	res, err := Zone(context.Background(), f, spec5g(zid), 3) // batch = 3 → 3 batches
	if err != nil {
		t.Fatalf("Zone: %v", err)
	}

	// The steps §7.2 by the call protocol.
	wantOps := []string{
		"call-partitions:" + fmt.Sprint(in),
		"moving-drop", "moving-create", "moving-index",
		// batches of 3 live ids: 10..16 → chunks [10,12], [13,15], [16,16]
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

	// Domains: the zone in, the label with the appended prefix.
	for _, d := range f.domains {
		if d.zone != in {
			t.Fatalf("domain %d stayed in zone %d, want %d (in)", d.id, d.zone, in)
		}
		if want := "example" + strconv.FormatInt(d.id, 10) + ".5g"; d.label != want {
			t.Errorf("domain %d: label %q, want %q (the prefix appended)", d.id, d.label, want)
		}
	}
	// The children of all the tables and the history moved.
	for table, rows := range f.children {
		for _, c := range rows {
			if c.zone != in {
				t.Errorf("%s: domain %d stayed in zone %d", table, c.domain, c.zone)
			}
		}
	}
	// Files: the duplicate is deleted, the rest are rebound; the foreign
	// one is untouched.
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
	// The 2LD partitions are dropped (the TLD partitions were created by
	// step 2).
	if len(f.parts[zid]) != 0 {
		t.Errorf("the 2LD partitions remain: %v", f.parts[zid])
	}
	if len(f.parts[in]) != 4 {
		t.Errorf("the partitions of zone in not created by step 2: %v", f.parts[in])
	}
	if _, ok := f.zoneID("5g.in"); ok {
		t.Error("the row of zone 5g.in not deleted")
	}

	// The result counters (all the rows — by this run).
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

// TestZoneBatches — the batch chunks §7.3 by the ascending actual ids from
// moving_ids: the sparse ids of the zone are one chunk [min, max] (not a
// window of the global id space), every batch is 4 UPDATEs of one chunk
// in one transaction.
func TestZoneBatches(t *testing.T) {
	f := newFakeConn()
	zid := f.seed2LD(t, []int64{5, 100, 9000}, false) // sparse ids

	if _, err := Zone(context.Background(), f, spec5g(zid), 50000); err != nil {
		t.Fatalf("Zone: %v", err)
	}
	// One chunk [5, 9000]: all 3 ids are in one batch (a chunk of rows,
	// the width of the id space between them does not multiply the
	// batches).
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
	// The UPDATE parameters of the domains: tid, prefix, zid, lo, hi
	// (the contract §7.3).
	if got := dom[0].args; len(got) != 5 || got[0] != in || got[1] != "5g" ||
		got[2] != zid || got[3] != int64(5) || got[4] != int64(9000) {
		t.Errorf("the domain UPDATE args = %v, want [%d 5g %d 5 9000]", got, in, zid)
	}
	// The children — with the same range, in the same batch (4 UPDATEs
	// together).
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

// TestZoneEmpty — a zone without domains: no batches, the other steps run
// (collapsing an empty 2LD zone is a legal case, no spot check).
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

// TestZoneResume — resumption: a failure after the first batch (an
// interruption of the command simulation) leaves a half-move; a rerun of
// Zone catches up without losses or duplicates; the next rerun is an empty
// no-op.
func TestZoneResume(t *testing.T) {
	f := newFakeConn()
	zid := f.seed2LD(t, []int64{1, 2, 3, 4, 5}, false)
	in, _ := f.zoneID("in")

	f.beginErrAfter = 1 // the first batch commits, the second Begin fails
	_, err := Zone(context.Background(), f, spec5g(zid), 2)
	if err == nil || !strings.Contains(err.Error(), "zone 5g.in") {
		t.Fatalf("the interrupt error = %v, want the zone 5g.in context", err)
	}
	// A partial move: the chunk [1,2] left, the files/partitions/zone in
	// place.
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

	// The rerun: moving_ids rebuilt from the remainder, the chunks address
	// only the remaining rows.
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

	// A rerun over a finished zone: the move finds no rows (moving_ids
	// empty, the zone children/files absent) — all deltas are zero, the
	// steps are idempotent (the zone row is restored with a new id, as if
	// the inventory had seen it again).
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

// TestZoneBatchConflict — an anomalous UNIQUE conflict in a batch: the
// batch transaction rolls back, the command stops with the zone name and
// the range; the following steps (files, drop, zone deletion) do not run.
func TestZoneBatchConflict(t *testing.T) {
	f := newFakeConn()
	zid := f.seed2LD(t, []int64{1, 2, 3, 4}, false)
	f.batchErrFrom = 3 // the second batch [3,4]

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
	// The first batch stayed committed (resumability).
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

// TestZoneLeftGuard — a remainder of the zone rows before the DROP (a data
// anomaly) stops the command: the partitions are not dropped, the zone is
// not deleted.
func TestZoneLeftGuard(t *testing.T) {
	f := newFakeConn()
	zid := f.seed2LD(t, []int64{1, 2}, true)
	// An orphan: a child outside the batch windows (a domain_id beyond
	// [min, min+batch) — the only window at batch = 50000).
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

// TestZoneNewTLDZone — the target zone is missing: it is created active,
// the partitions are allocated to it, the domains move into the new zone;
// the insert is an ASCII zone with an empty unicode (nullif → NULL).
func TestZoneNewTLDZone(t *testing.T) {
	f := newFakeConn()
	f.zones = nil // only the 2LD zone; the TLD uk will be created by the move
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
	// The TLD zone insert — the contract arguments of step 1.
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

// TestZoneExecError — a DB failure at the preparatory steps: the zone
// context in the error, nothing is moved.
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

// TestZoneNilConn — a failure before any DB use.
func TestZoneNilConn(t *testing.T) {
	if _, err := Zone(context.Background(), nil, spec5g(1), 0); err == nil ||
		!strings.Contains(err.Error(), "connection not set") {
		t.Errorf("Zone(nil) = %v, want a rejection", err)
	}
}

// --- inventory / precondition / post-checks --------------------------------------

func TestInventory(t *testing.T) {
	f := newFakeConn()
	zid := f.seed2LD(t, []int64{1, 2}, true)
	f.files = append(f.files, fFile{zone: zid, action: 1, sha256: "aa"})

	// A nil allowlist is normalized to an empty array (ALL(NULL) is NULL);
	// crawl.test is a compound name too: outside the allowlist it is
	// collapsed on a par with 5g.in (the allowlist protection is the only
	// one, §7.1).
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

	// The allowlist excludes its zones from collapsing (§7.1).
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
	// The contract arguments: the array of zones and the check_state = 1
	// code (queued).
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
	// crawl.test is an allowlist zone (as in the live config): the
	// post-check "no 2LD zones outside the allowlist remain" does not
	// count it.
	allow := []string{"crawl.test"}
	in, _ := f.zoneID("in")

	// Before the move: the 2LD zone exists — the post-check sees it.
	left, _, err := PostChecks(context.Background(), f, allow)
	if err != nil {
		t.Fatalf("PostChecks: %v", err)
	}
	if left != 1 {
		t.Errorf("%d 2LD zones, want 1 (no move yet)", left)
	}

	// Orphans: a child in a zone without a row.
	f.children[moveResultsSQL] = append(f.children[moveResultsSQL], fChild{9999, 1})
	_, orphans, err := PostChecks(context.Background(), f, allow)
	if err != nil {
		t.Fatalf("PostChecks with orphans: %v", err)
	}
	if orphans.Results != 1 || !orphans.Any() {
		t.Errorf("orphans = %+v, want 1 in subdomain_results", orphans)
	}

	// A clean state: the move by the fake's hands — the zone is deleted,
	// the domain and its children in the TLD with the appended label, the
	// orphan removed.
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

	// The spot check: the domain_info emulation answers the zone T and the
	// label with the prefix.
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

// TestPostChecksClosesSetBeforeOrphans — the T-217 regression: the
// post-check sets are closed before the next query. The fake guards
// (CheckZonesLeftSQL/CheckOrphansSQL with an open set) model the CLI pool
// of 2 connections: the move connection + one open set — the second query
// would wait forever for a third connection (the deadlock of the 29.10
// incident; the live circuit — the integration
// TestZonesCollapse_SmallPoolDeadlock). The final openSets == 0 covers
// both the leak of the second set and the helper swap (the T-217 review
// P3-1).
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

// TestAllowlistArg — nil → an empty array: ALL(NULL) kills the predicate.
func TestAllowlistArg(t *testing.T) {
	if got := allowlistArg(nil); got == nil || len(got) != 0 {
		t.Errorf("allowlistArg(nil) = %v, want an empty non-nil slice", got)
	}
	if got := allowlistArg([]string{"crawl.test"}); len(got) != 1 || got[0] != "crawl.test" {
		t.Errorf("allowlistArg = %v, want the input unchanged", got)
	}
}

// TestPrintPlanFormat — the plan table §7.4: the header, the zone columns,
// the right alignment of the numbers and the totals per TLD.
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
	// Numbers right-aligned: the domains column is aligned by the width of
	// the maximum value (3456789) — the narrow 12 is padded with spaces on
	// the left.
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

// hangDropConn — a Conn for the test of the moving_ids cleanup budget
// (T-229): the DROP Exec hangs until the context cancellation (a stuck
// PostgreSQL — the command accepted, no answer); the cancellation is
// observed by the channel closing. The other methods are not called and
// fail when called.
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

// TestDropMovingTimeout — the moving_ids cleanup must be cancellable by
// its own time budget (T-229): a stuck DROP does not hold the connection
// and the CLI exit indefinitely; the error is logged as a Warn, not
// returned (it has nothing to shadow the command result with).
func TestDropMovingTimeout(t *testing.T) {
	if movingDropTimeout != 5*time.Second {
		t.Errorf("movingDropTimeout = %s, want 5s (modeled on discardTimeout, ingest)", movingDropTimeout)
	}
	conn := &hangDropConn{canceled: make(chan struct{})}
	var logBuf strings.Builder
	log := slog.New(slog.NewTextHandler(&logBuf, nil))

	// The test budget is short: the fact of the context cancellation is
	// checked, not the size of the production budget (that one is covered
	// by the contract above).
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

// Integration contracts of the schema (spec 6, 6.1):
//
//   - T-003, the zones/subdomains/domains tables: LIST partitioning by
//     zone_id, fillfactor = 85 on every leaf partition, the partial
//     scheduler pass indexes over check_state — the pending selection
//     (domains_pending_idx), the lease return (domains_lease_idx) and
//     the recheck (domains_recheck_idx, T-104), the partial index of
//     deleted domains for the ingest cleanup (domains_deleted_idx,
//     T-170), the monitoring window by last_checked_at
//     (domains_last_checked_idx, T-171), the domain_id_seq sequence,
//     HOT compatibility — the mass-updated columns are not in any
//     index except the lease/recheck age and the monitoring window in
//     the keys (they change only together with check_state) and status
//     in the deleted-index predicate (the rows outside deleted do not
//     get into the index);
//   - T-004, the subdomain_results/contents/laravel_domains/
//     laravel_findings/ingest_files tables: partitioning of the tables
//     with domain_id, fillfactor = 85 of the subdomain_results leaves
//     (frequent record rewrites), the minimal index set, the write
//     idempotency by the primary keys.
//
// The schema is applied by the embedded runner (internal/migrate) into
// an isolated schema via search_path — the same as at the first
// services start on an empty DB; the tests do not depend on the state
// of the working schema.
package integration

import (
	"context"
	"encoding/json"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/migrate"
)

// migratedSchema applies the embedded migration set into an isolated
// schema (withTestSchema from migrate_test.go) and returns a pool with
// search_path on it.
func migratedSchema(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn, cleanup := withTestSchema(t)
	t.Cleanup(cleanup)

	if err := migrate.Up(context.Background(), dsn); err != nil {
		t.Fatalf("применение миграций к изолированной схеме: %v", err)
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("подключение к тестовой схеме: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// containsWord — a whole-word match (by the word boundaries), so that,
// for example, a "label" search does not fire on "name_unicode_label".
func containsWord(s, word string) bool {
	return regexp.MustCompile(`\b` + regexp.QuoteMeta(word) + `\b`).MatchString(s)
}

// tablePartition — a table partition: a leaf (relkind r) or an
// intermediate parent of subpartitions (relkind p, T-005).
type tablePartition struct {
	name       string
	relkind    string
	reloptions string // array_to_string(reloptions), '' when NULL
	bound      string // pg_get_expr(relpartbound), '' at the root
	indexCount int
}

// tablePartitions returns all partitions of a table recursively: the
// direct children and (once the T-005 subpartitions appear) the nested
// levels together with the index count of each.
func tablePartitions(t *testing.T, pool *pgxpool.Pool, table string) []tablePartition {
	t.Helper()

	// A recursive walk of the inheritance from the table (not from the
	// indexes: partitioned indexes have their own inheritance, it does
	// not interfere here).
	const query = `
		WITH RECURSIVE parts AS (
			SELECT c.oid, c.relname, c.relkind, c.reloptions, c.relpartbound
			FROM pg_inherits i
			JOIN pg_class c ON c.oid = i.inhrelid
			JOIN pg_class parent ON parent.oid = i.inhparent
			JOIN pg_namespace n ON n.oid = parent.relnamespace
			WHERE parent.relname = $1 AND n.nspname = current_schema()
			UNION ALL
			SELECT c.oid, c.relname, c.relkind, c.reloptions, c.relpartbound
			FROM parts p
			JOIN pg_inherits i ON i.inhparent = p.oid
			JOIN pg_class c ON c.oid = i.inhrelid
		)
		SELECT relname, relkind,
		       COALESCE(array_to_string(reloptions, ','), ''),
		       COALESCE(pg_get_expr(relpartbound, parts.oid), ''),
		       (SELECT count(*) FROM pg_index x WHERE x.indrelid = parts.oid)
		FROM parts`

	rows, err := pool.Query(context.Background(), query, table)
	if err != nil {
		t.Fatalf("каталог партиций %s: %v", table, err)
	}
	defer rows.Close()

	parts := make([]tablePartition, 0, 2)
	for rows.Next() {
		var p tablePartition
		if err := rows.Scan(&p.name, &p.relkind, &p.reloptions, &p.bound, &p.indexCount); err != nil {
			t.Fatalf("чтение партиции: %v", err)
		}
		parts = append(parts, p)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("итерация партиций: %v", err)
	}
	return parts
}

// tableIndex — an index on a table (for the partitioned ones — on the parent).
type tableIndex struct {
	name string
	def  string // the whole pg_get_indexdef
	pred string // the partial index predicate, '' for a plain one
}

// tableIndexes returns the indexes of a table (not its partitions).
func tableIndexes(t *testing.T, pool *pgxpool.Pool, table string) []tableIndex {
	t.Helper()

	const query = `
		SELECT i.relname, pg_get_indexdef(i.oid),
		       COALESCE(pg_get_expr(x.indpred, x.indrelid), '')
		FROM pg_index x
		JOIN pg_class i ON i.oid = x.indexrelid
		JOIN pg_class tbl ON tbl.oid = x.indrelid
		JOIN pg_namespace n ON n.oid = tbl.relnamespace
		WHERE n.nspname = current_schema() AND tbl.relname = $1
		ORDER BY i.relname`

	rows, err := pool.Query(context.Background(), query, table)
	if err != nil {
		t.Fatalf("каталог индексов %s: %v", table, err)
	}
	defer rows.Close()

	indexes := make([]tableIndex, 0, 3)
	for rows.Next() {
		var ix tableIndex
		if err := rows.Scan(&ix.name, &ix.def, &ix.pred); err != nil {
			t.Fatalf("чтение индекса: %v", err)
		}
		indexes = append(indexes, ix)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("итерация индексов: %v", err)
	}
	return indexes
}

// assertListPartitionedByZoneID — the table is LIST partitioned by
// zone_id (the spec 6 principle): relkind p, strategy l, the key
// "LIST (zone_id)".
func assertListPartitionedByZoneID(t *testing.T, pool *pgxpool.Pool, table string) {
	t.Helper()

	var relkind, partkey, strat string
	err := pool.QueryRow(context.Background(), `
		SELECT c.relkind, COALESCE(p.partstrat::text, ''),
		       COALESCE(pg_get_partkeydef(c.oid), '')
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_partitioned_table p ON p.partrelid = c.oid
		WHERE n.nspname = current_schema() AND c.relname = $1`, table,
	).Scan(&relkind, &strat, &partkey)
	if err != nil {
		t.Fatalf("pg_class для %s: %v", table, err)
	}
	if relkind != "p" {
		t.Errorf("%s: relkind = %s, want p (партиционированная таблица)", table, relkind)
	}
	if strat != "l" {
		t.Errorf("%s: стратегия партиционирования = %q, want \"l\" (LIST)", table, strat)
	}
	if partkey != "LIST (zone_id)" {
		t.Errorf("%s: ключ партиционирования = %q, want \"LIST (zone_id)\"", table, partkey)
	}
}

// leafPartitions returns the leaf partitions of a table and checks that
// they exist and include DEFAULT: without it the new zones could not
// write until a partition is extracted (T-005).
func leafPartitions(t *testing.T, pool *pgxpool.Pool, table string) []tablePartition {
	t.Helper()

	parts := tablePartitions(t, pool, table)
	leaves := make([]tablePartition, 0, len(parts))
	defaultSeen := false
	for _, p := range parts {
		if p.relkind != "r" {
			continue // the intermediate parent of subpartitions (T-005)
		}
		leaves = append(leaves, p)
		if strings.Contains(p.bound, "DEFAULT") {
			defaultSeen = true
		}
	}
	if len(leaves) == 0 {
		t.Errorf("нет ни одной листовой партиции %s", table)
	}
	if !defaultSeen {
		t.Errorf("нет DEFAULT-партиции %s: новые зоны не смогут записаться до выделения партиции", table)
	}
	return leaves
}

// TestSchema_DomainsPartitioning — domains is LIST partitioned by
// zone_id with a DEFAULT partition (writes of new zones until their
// own partitions are extracted); every leaf partition has fillfactor
// = 85 (spec 6: page room for the row versions under frequent
// updates). Storage parameters on a partitioned parent are not allowed
// in PostgreSQL 16, and reloptions are not inherited — hence the
// parameter is checked per partition.
func TestSchema_DomainsPartitioning(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)

	assertListPartitionedByZoneID(t, pool, "domains")

	for _, p := range leafPartitions(t, pool, "domains") {
		if !strings.Contains(p.reloptions, "fillfactor=85") {
			t.Errorf("партиция %s: reloptions = %q, want fillfactor=85 (reloptions не наследуются от родителя)", p.name, p.reloptions)
		}
	}
}

// TestSchema_DomainsIndexes — exactly seven indexes on domains (the
// "minimum of indexes", spec 6): the primary key, the unique
// (zone_id, label) for the name lookup (spec 5.11), three partial
// check_state ones for the scheduler passes (spec 5.3): the pending
// selection (domains_pending_idx, 000001), the expired lease return
// (domains_lease_idx, T-104) and the daily recheck
// (domains_recheck_idx, T-104), the partial deleted-domains index of
// the ingest cleanup (domains_deleted_idx, T-170, 0013) and the
// monitoring window by last_checked_at (domains_last_checked_idx,
// T-171, 0014). The keys of the background indexes start with the age
// column (queued_at / last_checked_at) and end with the domain key —
// the ORDER BY of the passes matches the key, the scan reads only the
// needed prefix. Every leaf partition has the attached child indexes
// of all the parent indexes.
func TestSchema_DomainsIndexes(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)

	indexes := tableIndexes(t, pool, "domains")
	want := map[string]bool{
		"domains_pkey":              true,
		"domains_zone_id_label_key": true,
		"domains_pending_idx":       true,
		"domains_lease_idx":         true,
		"domains_recheck_idx":       true,
		"domains_deleted_idx":       true,
		"domains_last_checked_idx":  true,
	}
	got := make(map[string]bool, len(indexes))
	for _, ix := range indexes {
		got[ix.name] = true
	}
	for name := range want {
		if !got[name] {
			t.Errorf("индекс %s отсутствует на domains", name)
		}
	}
	for name := range got {
		if !want[name] {
			t.Errorf("лишний индекс %s на domains: состав индексов зафиксирован (минимум индексов, ТЗ 6)", name)
		}
	}

	// The partial pass indexes: the predicate — check_state only, the
	// key — the age column + the domain key (T-104, migration 000008;
	// pending — 000001).
	wantPartial := map[string]struct{ key, pred string }{
		"domains_pending_idx": {"(zone_id, id)", "check_state = 0"},
		"domains_lease_idx":   {"(queued_at, zone_id, id)", "check_state = 1"},
		"domains_recheck_idx": {"(last_checked_at, zone_id, id)", "check_state = 2"},
		"domains_deleted_idx": {"(zone_id, id)", "status = 6"},
	}
	for name, w := range wantPartial {
		var ix *tableIndex
		for i := range indexes {
			if indexes[i].name == name {
				ix = &indexes[i]
			}
		}
		if ix == nil {
			t.Fatalf("%s не найден", name)
		}
		if key, _, _ := strings.Cut(ix.def, " WHERE "); !strings.Contains(key, w.key) {
			t.Errorf("%s: ключ = %q, want %s", name, key, w.key)
		}
		if !strings.Contains(ix.pred, w.pred) {
			t.Errorf("%s: предикат = %q, want %q", name, ix.pred, w.pred)
		}
	}

	for _, p := range leafPartitions(t, pool, "domains") {
		if p.indexCount != len(indexes) {
			t.Errorf("партиция %s: индексов %d, want %d (дочерние всех индексов родителя)", p.name, p.indexCount, len(indexes))
		}
	}
}

// TestSchema_DomainIDSequence — the domain ids are issued by the
// domain_id_seq sequence, shared by all zones and partitions (spec
// 6.1); a (zone_id, label) duplicate is rejected by the unique index
// (the ingest idempotency, spec 5.1).
func TestSchema_DomainIDSequence(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	var seqExists bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE c.relkind = 'S' AND c.relname = 'domain_id_seq'
			  AND n.nspname = current_schema())`,
	).Scan(&seqExists); err != nil || !seqExists {
		t.Fatalf("sequence domain_id_seq не создана (err=%v)", err)
	}

	var idDefault string
	if err := pool.QueryRow(ctx, `
		SELECT pg_get_expr(ad.adbin, ad.adrelid)
		FROM pg_attrdef ad
		JOIN pg_class tbl ON tbl.oid = ad.adrelid
		JOIN pg_namespace n ON n.oid = tbl.relnamespace
		JOIN pg_attribute a ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
		WHERE n.nspname = current_schema() AND tbl.relname = 'domains'
		  AND a.attname = 'id'`,
	).Scan(&idDefault); err != nil {
		t.Fatalf("default колонки domains.id: %v", err)
	}
	if !strings.Contains(idDefault, "nextval(") || !strings.Contains(idDefault, "domain_id_seq") {
		t.Errorf("domains.id: default = %q, want nextval('domain_id_seq')", idDefault)
	}

	// The id is filled by the sequence automatically (no foreign keys
	// in the schema — the minimum of indexes, a zone is not needed).
	var id int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO domains (zone_id, label) VALUES (1, 'example') RETURNING id`,
	).Scan(&id); err != nil {
		t.Fatalf("insert без id: %v", err)
	}
	if id <= 0 {
		t.Errorf("id = %d, want > 0 (выдан sequence)", id)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO domains (zone_id, label) VALUES (1, 'example')`,
	); err == nil || !strings.Contains(err.Error(), "duplicate key") {
		t.Errorf("повторный insert (zone_id, label): err = %v, want duplicate key", err)
	}
}

// TestSchema_DomainsHOTColumns — the columns updated during a check
// are not in any index, except the pre-accepted compromises (spec 6):
// the mass updates stay HOT, and the fillfactor = 85 leaves room for
// the new row versions on the same page. Three compromises:
//
//   - check_state — in the predicates of the three partial pass
//     indexes (pending/lease/recheck): the state changes are always
//     non-HOT — unavoidable with the spec §5.3-required partial
//     indexes;
//   - queued_at / last_checked_at — in the keys of the lease/recheck
//     indexes (T-104) and last_checked_at also in the key of the
//     domain monitoring window domains_last_checked_idx (T-171): their
//     updates are non-HOT, but all the writers change these columns
//     ONLY together with check_state (the enqueue, the lease return,
//     the recheck, the dns/http worker result, the revival by an
//     add-file) — the transitions are already non-HOT, the indexes do
//     not create new non-HOT updates;
//   - status — in the predicate (not the key!) of the partial
//     deleted-domains index domains_deleted_idx (T-170): only the
//     status = deleted rows live in the index, only the transitions
//     involving deleted become non-HOT (the T-019 delete-file apply,
//     the T-018 revival) — rare registry events; the mass updates of
//     the result status and the finalization of the temp_error final
//     status (T-031) have no rows in the index and stay HOT.
func TestSchema_DomainsHOTColumns(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)

	hotColumns := []string{"flags", "dns_mask", "temp_attempts"}
	// The age columns — only in the keys of their own pass/window indexes.
	ageColumns := map[string][]string{
		"queued_at":       {"domains_lease_idx"},
		"last_checked_at": {"domains_recheck_idx", "domains_last_checked_idx"},
	}
	for _, ix := range tableIndexes(t, pool, "domains") {
		for _, col := range hotColumns {
			if containsWord(ix.def, col) {
				t.Errorf("индекс %s ссылается на часто обновляемую колонку %s: её апдейты перестают быть HOT", ix.name, col)
			}
		}
		// status — only the predicate (not the key) of domains_deleted_idx
		// (T-170): the rows outside deleted do not get into the index,
		// the mass status changes stay HOT.
		if containsWord(ix.def, "status") {
			if ix.name != "domains_deleted_idx" {
				t.Errorf("индекс %s ссылается на колонку status: допустима только в предикате domains_deleted_idx (T-170)", ix.name)
			} else {
				key, _, _ := strings.Cut(ix.def, " WHERE ")
				if containsWord(key, "status") {
					t.Errorf("domains_deleted_idx: status в ключе (%s): индексация смен статуса всегда non-HOT", key)
				}
			}
		}
		for col, allowed := range ageColumns {
			if !slices.Contains(allowed, ix.name) && containsWord(ix.def, col) {
				t.Errorf("индекс %s ссылается на колонку %s: допустима только в ключах %v", ix.name, col, allowed)
			}
		}
		if containsWord(ix.def, "check_state") &&
			ix.name != "domains_pending_idx" && ix.name != "domains_lease_idx" && ix.name != "domains_recheck_idx" {
			t.Errorf("индекс %s ссылается на check_state: допустимо только в предикатах индексов проходов", ix.name)
		}
	}

	// In the partial pass indexes check_state is the predicate only,
	// not the key: indexing the state changes would make them non-HOT
	// even where the state filter is enough.
	for _, ix := range tableIndexes(t, pool, "domains") {
		switch ix.name {
		case "domains_pending_idx", "domains_lease_idx", "domains_recheck_idx":
			key, _, _ := strings.Cut(ix.def, " WHERE ")
			if containsWord(key, "check_state") {
				t.Errorf("%s: check_state в ключе (%s): индексируемые смены состояния всегда non-HOT", ix.name, key)
			}
		}
	}
}

// TestSchema_TableColumns — the zones and the subdomains are small
// reference tables without partitioning (the spec 6 principle — the
// tables with domain_id are partitioned); the composition and the
// column types of all three tables match the DDL sketch 6.1 (the
// minimal field sizes).
func TestSchema_TableColumns(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	// table → relkind (r — plain, p — partitioned) and column → type.
	want := map[string]struct {
		relkind string
		columns map[string]string
	}{
		"zones": {relkind: "r", columns: map[string]string{
			"id": "smallint", "name": "text", "name_unicode": "text",
			"is_active": "boolean", "domains_total": "bigint",
			"domains_checked": "bigint", "updated_at": "timestamp with time zone",
			"domains_laravel": "bigint", // the zone Laravel-domains cache (T-120, the landing page)
		}},
		"subdomains": {relkind: "r", columns: map[string]string{
			"id": "smallint", "label": "text", "enabled": "boolean",
		}},
		"domains": {relkind: "p", columns: map[string]string{
			"zone_id": "smallint", "id": "bigint", "label": "text",
			"status": "smallint", "check_state": "smallint", "flags": "smallint",
			"dns_mask": "bigint", "queued_at": "timestamp with time zone",
			"last_checked_at": "date",
			"temp_attempts":   "smallint", // T-031: the temp_error retry counter (migration 000005)
		}},
		"web_checks": {relkind: "r", columns: map[string]string{ // T-120: the landing form log
			"id": "bigint", "domain": "text", "punycode_domain": "text",
			"ip_address": "inet", "found": "boolean", "score": "real",
			"duration_ms": "integer", "created_at": "timestamp with time zone",
		}},
		"crawler_counters": {relkind: "r", columns: map[string]string{ // T-175: the transition counters
			"name": "text", "value": "bigint",
		}},
	}
	// The unique keys of the reference tables: the zones are deduped by
	// the punycode name (spec 5.2), the subdomains — by the label.
	wantUnique := map[string]string{"zones": "name", "subdomains": "label"}

	for table, w := range want {
		var relkind string
		if err := pool.QueryRow(ctx, `
			SELECT c.relkind FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = current_schema() AND c.relname = $1`, table,
		).Scan(&relkind); err != nil {
			t.Fatalf("таблица %s не найдена: %v", table, err)
		}
		if relkind != w.relkind {
			t.Errorf("%s: relkind = %s, want %s", table, relkind, w.relkind)
		}
		assertColumnTypes(t, pool, table, w.columns)

		if col, ok := wantUnique[table]; ok {
			assertUniqueIndex(t, pool, table, col)
		}
	}
}

// assertColumnTypes checks the presence of the columns and their types
// (format_type).
func assertColumnTypes(t *testing.T, pool *pgxpool.Pool, table string, want map[string]string) {
	t.Helper()

	rows, err := pool.Query(context.Background(), `
		SELECT a.attname, format_type(a.atttypid, a.atttypmod)
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = current_schema() AND c.relname = $1
		  AND a.attnum > 0 AND NOT a.attisdropped`, table)
	if err != nil {
		t.Fatalf("колонки %s: %v", table, err)
	}
	defer rows.Close()

	got := make(map[string]string, len(want))
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			t.Fatalf("чтение колонки %s: %v", table, err)
		}
		got[name] = typ
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("итерация колонок %s: %v", table, err)
	}

	for col, typ := range want {
		if got[col] != typ {
			t.Errorf("%s.%s: тип = %q, want %q", table, col, got[col], typ)
		}
	}
}

// assertUniqueIndex checks that the table has a unique index with the
// given key (a column or a comma-separated list).
func assertUniqueIndex(t *testing.T, pool *pgxpool.Pool, table, key string) {
	t.Helper()

	rows, err := pool.Query(context.Background(), `
		SELECT pg_get_indexdef(x.indexrelid)
		FROM pg_index x
		JOIN pg_class tbl ON tbl.oid = x.indrelid
		JOIN pg_namespace n ON n.oid = tbl.relnamespace
		WHERE n.nspname = current_schema() AND tbl.relname = $1
		  AND x.indisunique`, table)
	if err != nil {
		t.Fatalf("уникальные индексы %s: %v", table, err)
	}
	defer rows.Close()

	found := false
	for rows.Next() {
		var def string
		if err := rows.Scan(&def); err != nil {
			t.Fatalf("чтение индекса %s: %v", table, err)
		}
		if head, _, _ := strings.Cut(def, " WHERE "); strings.Contains(head, "("+key+")") {
			found = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("итерация индексов %s: %v", table, err)
	}
	if !found {
		t.Errorf("%s: нет уникального индекса по ключу %s", table, key)
	}
}

// assertNotNull checks that the table columns are NOT NULL (the
// mandatory fields of the sketch 6.1).
func assertNotNull(t *testing.T, pool *pgxpool.Pool, table string, columns []string) {
	t.Helper()

	for _, col := range columns {
		var notNull bool
		err := pool.QueryRow(context.Background(), `
			SELECT a.attnotnull
			FROM pg_attribute a
			JOIN pg_class c ON c.oid = a.attrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = current_schema() AND c.relname = $1
			  AND a.attname = $2 AND a.attnum > 0 AND NOT a.attisdropped`,
			table, col,
		).Scan(&notNull)
		if err != nil {
			t.Fatalf("колонка %s.%s не найдена: %v", table, col, err)
		}
		if !notNull {
			t.Errorf("%s.%s: ожидается NOT NULL (эскиз 6.1)", table, col)
		}
	}
}

// ---------------------------------------------------------------- T-004: the result tables

// TestSchema_ResultsTablesPartitioning — the result tables with
// domain_id (subdomain_results, laravel_domains, laravel_findings) are
// LIST partitioned by zone_id with a DEFAULT partition (the sketch
// 6.1). Every leaf partition of subdomain_results has fillfactor = 85:
// the check history is not kept (spec 3.5) — a recheck rewrites the
// rows with the same PK, and the spec 6.3 classifies the table as
// frequently updated; the laravel_* ones are written once and
// rewritten only by the re-analysis — no fillfactor is set for them.
func TestSchema_ResultsTablesPartitioning(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)

	assertListPartitionedByZoneID(t, pool, "subdomain_results")
	for _, p := range leafPartitions(t, pool, "subdomain_results") {
		if !strings.Contains(p.reloptions, "fillfactor=85") {
			t.Errorf("партиция %s: reloptions = %q, want fillfactor=85 (частые перезаписи результатов, ТЗ 6.3)", p.name, p.reloptions)
		}
	}

	for _, table := range []string{"laravel_domains", "laravel_findings"} {
		assertListPartitionedByZoneID(t, pool, table)
		leafPartitions(t, pool, table)
	}
}

// TestSchema_ResultsTablesColumns — the composition and the column
// types of the five T-004 tables match the DDL sketch 6.1 (the minimal
// field sizes: smallint, int4, bytea); contents and ingest_files
// without domain_id — plain tables (the spec 6 principle: the tables
// with domain_id are partitioned).
func TestSchema_ResultsTablesColumns(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)

	want := map[string]struct {
		relkind string
		columns map[string]string
		notNull []string
	}{
		"subdomain_results": {relkind: "p", columns: map[string]string{
			"zone_id": "smallint", "domain_id": "bigint", "subdomain_id": "smallint",
			"variant": "smallint", "scheme": "smallint", "http_status": "smallint",
			"final_url": "text", "alias_kind": "smallint", "alias_subdomain_id": "smallint",
			"addrs": "bytea", "cname": "text", "tls": "jsonb",
			"redirects": "jsonb", "headers": "jsonb", "cookies": "jsonb",
			"rendered": "boolean", "content_hash": "bytea", "error": "smallint",
			"response_ms": "integer", "checked_at": "timestamp with time zone",
			"rules_version": "integer",
			// the "render is impossible" marker (T-210, migration 000017)
			"render_failed_at": "timestamp with time zone",
		}, notNull: []string{
			"zone_id", "domain_id", "subdomain_id", "variant", "scheme",
			"alias_kind", "rendered", "checked_at",
		}},
		"contents": {relkind: "r", columns: map[string]string{
			"hash": "bytea", "pack_id": "integer", "off": "bigint",
			"len": "integer", "raw_len": "integer", "refcount": "integer",
		}, notNull: []string{
			"hash", "pack_id", "off", "len", "raw_len", "refcount",
		}},
		"laravel_domains": {relkind: "p", columns: map[string]string{
			"zone_id": "smallint", "domain_id": "bigint", "score": "real",
			"ver_min": "text", "ver_max": "text", "found_on_mask": "bigint",
			"rules_version": "integer", "updated_at": "timestamp with time zone",
		}, notNull: []string{
			"zone_id", "domain_id", "score", "found_on_mask",
			"rules_version", "updated_at",
		}},
		"laravel_findings": {relkind: "p", columns: map[string]string{
			"zone_id": "smallint", "domain_id": "bigint", "subdomain_id": "smallint",
			"variant": "smallint", "score": "real", "ver_min": "text",
			"ver_max": "text", "signals": "jsonb", "is_laravel": "boolean",
			"rules_version": "integer",
		}, notNull: []string{
			"zone_id", "domain_id", "subdomain_id", "variant",
			"score", "signals", "is_laravel", "rules_version",
		}},
		"ingest_files": {relkind: "r", columns: map[string]string{
			"id": "bigint", "zone_id": "smallint", "action": "smallint",
			"path": "text", "sha256": "bytea", "lines_total": "bigint",
			"added": "bigint", "existed": "bigint", "deleted": "bigint",
			"rejected": "bigint", "processed_at": "timestamp with time zone",
		}, notNull: []string{"id"}},
	}

	for table, w := range want {
		var relkind string
		if err := pool.QueryRow(context.Background(), `
			SELECT c.relkind FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = current_schema() AND c.relname = $1`, table,
		).Scan(&relkind); err != nil {
			t.Fatalf("таблица %s не найдена: %v", table, err)
		}
		if relkind != w.relkind {
			t.Errorf("%s: relkind = %s, want %s", table, relkind, w.relkind)
		}
		assertColumnTypes(t, pool, table, w.columns)
		assertNotNull(t, pool, table, w.notNull)
	}

	// The ingest idempotency: the dedup — an exact repeated delivery
	// (zone_id, action, sha256) — the unique key of T-113 (migration
	// 000009).
	assertUniqueIndex(t, pool, "ingest_files", "zone_id, action, sha256")
}

// TestSchema_ResultsTablesIndexes — the "minimum of indexes" (spec 6):
// on the T-004 tables only the primary keys — the per-domain selections
// go by the (zone_id, domain_id) prefix of the composite keys — plus
// the unique ingest_files (zone_id, action, sha256) with its serving
// ingest_files_sha256_idx (T-113: the dedup of the exact repeated
// delivery and the pipeline query by content), the contents
// compaction index contents_pack_off_idx (pack_id, off) for the
// background CompactLoop queries over a pack sorted by the offset
// (T-096, INFRA-05; refcount is not in the key — the counter updates
// stay HOT) and the monitoring window subdomain_results_checked_idx
// (checked_at) — the hourly pace counters of the summary (T-171;
// checked_at is written only by an INSERT, the HOT picture does not
// change). The indexes for the future selections (for example,
// laravel_domains by score — stage 8, spec 5.11) are deliberately not
// added: the composition is pinned by this contract.
func TestSchema_ResultsTablesIndexes(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)

	// table → index name → expected key.
	want := map[string]map[string]string{
		"subdomain_results": {
			"subdomain_results_pkey":        "(zone_id, domain_id, subdomain_id, variant)",
			"subdomain_results_checked_idx": "(checked_at)",
		},
		"contents": {
			"contents_pkey":         "(hash)",
			"contents_pack_off_idx": "(pack_id, off)",
		},
		"laravel_domains": {
			"laravel_domains_pkey":           "(zone_id, domain_id)",
			"laravel_domains_updated_at_idx": "(updated_at)", // the landing page "latest findings" (T-120)
		},
		"laravel_findings": {"laravel_findings_pkey": "(zone_id, domain_id, subdomain_id, variant)"},
		"ingest_files": {
			"ingest_files_pkey":                      "(id)",
			"ingest_files_zone_id_action_sha256_key": "(zone_id, action, sha256)",
			"ingest_files_sha256_idx":                "(sha256)",
		},
		"web_checks": { // the landing form log (T-120)
			"web_checks_pkey":           "(id)",
			"web_checks_ip_created_idx": "(ip_address, created_at)",
			"web_checks_created_idx":    "(created_at)",
		},
	}

	for table, wantIndexes := range want {
		got := make(map[string]string, len(wantIndexes))
		for _, ix := range tableIndexes(t, pool, table) {
			got[ix.name] = ix.def
		}
		for name, key := range wantIndexes {
			def, ok := got[name]
			if !ok {
				t.Errorf("%s: индекс %s отсутствует", table, name)
				continue
			}
			if !strings.Contains(def, " USING btree "+key) {
				t.Errorf("%s.%s: определение = %s, want ключ %s", table, name, def, key)
			}
		}
		for name := range got {
			if _, ok := wantIndexes[name]; !ok {
				t.Errorf("%s: лишний индекс %s: состав индексов зафиксирован (минимум индексов, ТЗ 6)", table, name)
			}
		}

		if table == "ingest_files" || table == "contents" || table == "web_checks" {
			continue // plain tables, no child indexes
		}
		for _, p := range leafPartitions(t, pool, table) {
			if p.indexCount != len(wantIndexes) {
				t.Errorf("партиция %s: индексов %d, want %d (дочерние всех индексов родителя)", p.name, p.indexCount, len(wantIndexes))
			}
		}
	}
}

// TestSchema_ResultsIdempotency — the record writing is idempotent
// (spec 3.4): a repeated insert of a row with the same key is rejected
// by the primary key, for ingest_files — by the unique triple
// (zone_id, action, sha256) even with a different file path (T-113,
// spec 5.1); the same content with a different action (undo) or in a
// different zone is written as a separate row; the sketch defaults are
// applied (variant/alias_kind/rendered, refcount). There are no
// foreign keys to domains — the minimum of indexes (spec 6), the
// integrity of the (zone_id, domain_id) references is provided by the
// writing code.
func TestSchema_ResultsIdempotency(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	// dupInsert — the second insert of the same row must fail on a duplicate.
	dupInsert := func(table, sql string) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql); err == nil || !strings.Contains(err.Error(), "duplicate key") {
			t.Errorf("%s: повторная вставка: err = %v, want duplicate key", table, err)
		}
	}

	// subdomain_results: a result row with the defaults.
	var variant, aliasKind int
	var rendered bool
	err := pool.QueryRow(ctx, `
		INSERT INTO subdomain_results (zone_id, domain_id, subdomain_id, scheme, http_status, checked_at)
		VALUES (1, 10, 0, 2, 200, now()) RETURNING variant, alias_kind, rendered`,
	).Scan(&variant, &aliasKind, &rendered)
	if err != nil {
		t.Fatalf("insert subdomain_results: %v", err)
	}
	if variant != 0 || aliasKind != 0 || rendered {
		t.Errorf("subdomain_results defaults: variant=%d alias_kind=%d rendered=%v, want 0/0/false", variant, aliasKind, rendered)
	}
	dupInsert("subdomain_results", `
		INSERT INTO subdomain_results (zone_id, domain_id, subdomain_id, scheme, checked_at)
		VALUES (1, 10, 0, 2, now())`)

	// contents: the content metadata, refcount defaults to 1.
	var refcount int
	if err := pool.QueryRow(ctx, `
		INSERT INTO contents (hash, pack_id, off, len, raw_len)
		VALUES ('\x00000000000000000000000000000001', 1, 0, 100, 200) RETURNING refcount`,
	).Scan(&refcount); err != nil {
		t.Fatalf("insert contents: %v", err)
	}
	if refcount != 1 {
		t.Errorf("contents.refcount = %d, want 1 (default)", refcount)
	}
	dupInsert("contents", `
		INSERT INTO contents (hash, pack_id, off, len, raw_len)
		VALUES ('\x00000000000000000000000000000001', 2, 0, 1, 1)`)

	// laravel_domains: the per-domain aggregate.
	if _, err := pool.Exec(ctx, `
		INSERT INTO laravel_domains (zone_id, domain_id, score, ver_min, ver_max, found_on_mask, rules_version, updated_at)
		VALUES (1, 10, 0.75, '9.19', '11', 3, 1, now())`,
	); err != nil {
		t.Fatalf("insert laravel_domains: %v", err)
	}
	dupInsert("laravel_domains", `
		INSERT INTO laravel_domains (zone_id, domain_id, score, found_on_mask, rules_version, updated_at)
		VALUES (1, 10, 1, 1, 1, now())`)

	// laravel_findings: the rule hits per (domain, subdomain, variant).
	if _, err := pool.Exec(ctx, `
		INSERT INTO laravel_findings (zone_id, domain_id, subdomain_id, variant, score, signals, is_laravel, rules_version)
		VALUES (1, 10, 0, 0, 0.75, '[{"rule":"cookie_xsrf_token","weight":0.25}]', true, 1)`,
	); err != nil {
		t.Fatalf("insert laravel_findings: %v", err)
	}
	dupInsert("laravel_findings", `
		INSERT INTO laravel_findings (zone_id, domain_id, subdomain_id, variant, score, signals, is_laravel, rules_version)
		VALUES (1, 10, 0, 0, 1, '[]', true, 1)`)

	// ingest_files: the report of a processed file; an exact repeated
	// delivery — the same zone, action and sha256 (a different path
	// does not help) — is rejected by the T-113 unique key; the same
	// content with a different action (undo) or in a different zone —
	// separate legitimate rows.
	if _, err := pool.Exec(ctx, `
		INSERT INTO ingest_files (zone_id, action, path, sha256, lines_total, added, existed, deleted, rejected)
		VALUES (1, 1, 'incoming/com/add/a.txt', '\x0102', 100, 90, 5, 0, 5)`,
	); err != nil {
		t.Fatalf("insert ingest_files: %v", err)
	}
	dupInsert("ingest_files", `
		INSERT INTO ingest_files (zone_id, action, path, sha256)
		VALUES (1, 1, 'processed/com/add/a.txt', '\x0102')`)
	for _, d := range []struct{ zone, action int16 }{
		{1, 2}, // an undo with the same list: the same content from delete/
		{2, 1}, // a repeated delivery in another zone
	} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO ingest_files (zone_id, action, path, sha256)
			VALUES ($1, $2, 'com/add/b.txt', '\x0102')`, d.zone, d.action); err != nil {
			t.Errorf("ingest_files: то же содержимое (zone=%d, action=%d) обязано записываться отдельно: %v",
				d.zone, d.action, err)
		}
	}
}

// TestSchema_RenderDebtCounter — the render debt counter (T-175,
// migration 000015): the render_pending row is created by the backfill
// (an empty schema — zero), the PK name rejects duplicates, the
// render_pending bit transitions (the SQL — literals from
// internal/httpworker/renderq.go, the T-039 style; the shape pinned by
// TestRenderSQLContract) work on live PostgreSQL by contract: the bit
// guard makes RowsAffected the transition indicator (SET 0→1, CLEAR
// 1→0, a repeat — 0), the other bits are preserved, the counter shift
// requires the row by name.
func TestSchema_RenderDebtCounter(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	// The migration backfill created the row; an empty schema — zero.
	var n int64
	if err := pool.QueryRow(ctx,
		"SELECT value FROM crawler_counters WHERE name = 'render_pending'",
	).Scan(&n); err != nil {
		t.Fatalf("строка счётчика: %v (бэкфилл миграции 000015)", err)
	}
	if n != 0 {
		t.Errorf("счётчик пустой схемы = %d, want 0", n)
	}
	// The PK by name — duplicates are rejected.
	if _, err := pool.Exec(ctx,
		"INSERT INTO crawler_counters (name, value) VALUES ('render_pending', 1)"); err == nil ||
		!strings.Contains(err.Error(), "duplicate key") {
		t.Errorf("повторная строка render_pending: err = %v, want duplicate key", err)
	}

	// A domain with the is_idn bit (the render_pending flag is set on
	// top, clearing it preserves the rest).
	var id int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO domains (zone_id, label, flags) VALUES (1, 'render-debt', 1) RETURNING id`,
	).Scan(&id); err != nil {
		t.Fatalf("insert domains: %v", err)
	}

	// SET with the bit guard (renderSetPendingSQL): the 0→1 transition
	// — one row, a repeated set — zero (the transition indicator,
	// T-175).
	setSQL := "UPDATE domains AS t SET flags = (t.flags | 2)::int2 " +
		"WHERE t.zone_id = $1::int2 AND t.id = $2::int8 AND (t.flags & 2) = 0"
	tag, err := pool.Exec(ctx, setSQL, int16(1), id)
	if err != nil {
		t.Fatalf("set render_pending: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Errorf("переход 0→1: RowsAffected = %d, want 1", tag.RowsAffected())
	}
	tag, err = pool.Exec(ctx, setSQL, int16(1), id)
	if err != nil {
		t.Fatalf("повторный set render_pending: %v", err)
	}
	if tag.RowsAffected() != 0 {
		t.Errorf("повторная установка: RowsAffected = %d, want 0 (охрана бита)", tag.RowsAffected())
	}

	// The counter shift (renderCounterDeltaSQL): the row exists —
	// UPDATE 1, the value changes; a foreign name — zero rows.
	bumpSQL := "UPDATE crawler_counters AS c SET value = c.value + $1::int8 " +
		"WHERE c.name = 'render_pending'"
	tag, err = pool.Exec(ctx, bumpSQL, int64(1))
	if err != nil {
		t.Fatalf("сдвиг счётчика: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Errorf("сдвиг счётчика: RowsAffected = %d, want 1", tag.RowsAffected())
	}
	if err := pool.QueryRow(ctx,
		"SELECT value FROM crawler_counters WHERE name = 'render_pending'",
	).Scan(&n); err != nil || n != 1 {
		t.Errorf("счётчик после +1 = %d (err=%v), want 1", n, err)
	}
	tag, err = pool.Exec(ctx,
		"UPDATE crawler_counters AS c SET value = c.value + $1::int8 WHERE c.name = 'nope'", int64(1))
	if err != nil {
		t.Fatalf("сдвиг несуществующей строки: %v", err)
	}
	if tag.RowsAffected() != 0 {
		t.Errorf("сдвиг несуществующей строки: RowsAffected = %d, want 0", tag.RowsAffected())
	}

	// CLEAR with the bit guard (renderClearPendingSQL): 1→0 — one row,
	// a repeat — zero; is_idn is preserved.
	clearSQL := "UPDATE domains AS t SET flags = (t.flags & -3)::int2 " +
		"WHERE t.zone_id = $1::int2 AND t.id = $2::int8 AND (t.flags & 2) <> 0"
	tag, err = pool.Exec(ctx, clearSQL, int16(1), id)
	if err != nil {
		t.Fatalf("clear render_pending: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Errorf("переход 1→0: RowsAffected = %d, want 1", tag.RowsAffected())
	}
	tag, err = pool.Exec(ctx, clearSQL, int16(1), id)
	if err != nil {
		t.Fatalf("повторный clear render_pending: %v", err)
	}
	if tag.RowsAffected() != 0 {
		t.Errorf("повторное снятие: RowsAffected = %d, want 0 (охрана бита)", tag.RowsAffected())
	}
	var flags int16
	if err := pool.QueryRow(ctx, "SELECT flags FROM domains WHERE id = $1", id).Scan(&flags); err != nil {
		t.Fatal(err)
	}
	if flags != 1 {
		t.Errorf("flags после снятия = %d, want 1 (is_idn сохранён)", flags)
	}

	// The decrement returns the counter to zero.
	if tag, err = pool.Exec(ctx, bumpSQL, int64(-1)); err != nil || tag.RowsAffected() != 1 {
		t.Errorf("декремент: tag=%v err=%v, want UPDATE 1", tag.RowsAffected(), err)
	}
	if err := pool.QueryRow(ctx,
		"SELECT value FROM crawler_counters WHERE name = 'render_pending'",
	).Scan(&n); err != nil || n != 0 {
		t.Errorf("счётчик после −1 = %d (err=%v), want 0", n, err)
	}
}

// ---------------------------------------------------------------- T-082: domain_info

// dinfoDecode parses the domain_info response into a map
// (encoding/json: the numbers are float64, the jsonb key order is not
// preserved — the comparison is by maps, not by strings).
func dinfoDecode(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	if raw == nil {
		t.Fatal("domain_info: ответ NULL, ожидался jsonb")
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("domain_info: разбор jsonb: %v", err)
	}
	return m
}

// dinfoMap, dinfoSlice, dinfoFloat, dinfoStr — access to the response
// elements with a type check: the field names and kinds are part of
// the contract (the deterministic paths for psql and the tests).
func dinfoMap(t *testing.T, v any, what string) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("domain_info: %s — не объект: %T", what, v)
	}
	return m
}

func dinfoSlice(t *testing.T, v any, what string) []any {
	t.Helper()
	s, ok := v.([]any)
	if !ok {
		t.Fatalf("domain_info: %s — не массив: %T", what, v)
	}
	return s
}

func dinfoFloat(t *testing.T, v any, what string) float64 {
	t.Helper()
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("domain_info: %s — не число: %T", what, v)
	}
	return f
}

func dinfoStr(t *testing.T, v any, what string) string {
	t.Helper()
	s, ok := v.(string)
	if !ok {
		t.Fatalf("domain_info: %s — не строка: %T", what, v)
	}
	return s
}

// dinfoTime parses the timestamp of the response: a timestamptz in
// jsonb is an RFC 3339 string with the session zone offset, so the
// moments are compared, not the strings.
func dinfoTime(t *testing.T, v any, what string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, dinfoStr(t, v, what))
	if err != nil {
		t.Fatalf("domain_info: %s: %v", what, err)
	}
	return tm
}

// TestSchema_DomainInfoFunction — the contract of the SQL function
// domain_info(text) (migration 000006, spec 5.11): a domain summary in
// one jsonb — the zone, the domain row with the status names, the
// dns_mask decode, the results per subdomain/variant and the Laravel
// signs (the laravel_domains aggregate + all the findings by the score
// descending, the weak ones flagged is_laravel). The input is
// normalized by the SQL minimum (the case, the edge spaces and the
// dot), the zone — the longest suffix from the zones reference table;
// the domain lookup — by the unique key (zone_id, label). The
// domain/zone not found — NULL, not an error. The function only reads:
// STABLE, PARALLEL SAFE, SECURITY INVOKER, the search_path is fixed
// (pg_catalog + the migration schema).
func TestSchema_DomainInfoFunction(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	// The function is in place and only reads: provolatile/proparallel
	// 's' (STABLE, PARALLEL SAFE), prokind 'f', a jsonb return, the
	// search_path configuration points to pg_catalog and the migration
	// schema.
	var retType, kind, volatile, parallel, config string
	err := pool.QueryRow(ctx, `
		SELECT p.prorettype::regtype::text, p.prokind::text,
		       p.provolatile::text, p.proparallel::text,
		       COALESCE(array_to_string(p.proconfig, ','), '')
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = current_schema() AND p.proname = 'domain_info'`,
	).Scan(&retType, &kind, &volatile, &parallel, &config)
	if err != nil {
		t.Fatalf("pg_proc domain_info: %v", err)
	}
	if retType != "jsonb" || kind != "f" {
		t.Errorf("domain_info: возврат %s/%s, want jsonb/f", retType, kind)
	}
	if volatile != "s" {
		t.Errorf("domain_info: provolatile = %s, want s (STABLE — только чтение)", volatile)
	}
	if parallel != "s" {
		t.Errorf("domain_info: proparallel = %s, want s (PARALLEL SAFE)", parallel)
	}
	if !strings.Contains(config, "search_path") || !strings.Contains(config, "pg_catalog") {
		t.Errorf("domain_info: search_path не зафиксирован: %q", config)
	}

	mustExec := func(q string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, q, args...); err != nil {
			t.Fatalf("подготовка данных: %v", err)
		}
	}
	zoneID := func(name string) int {
		t.Helper()
		var id int
		if err := pool.QueryRow(ctx,
			`SELECT id FROM zones WHERE name = $1`, name,
		).Scan(&id); err != nil {
			t.Fatalf("зона %s: %v", name, err)
		}
		return id
	}

	// The data: the zones test + uk/co.uk (to separate the zone by the
	// longest suffix), the domain example with results and Laravel
	// signs, the domain site in co.uk without results.
	mustExec(`INSERT INTO zones (name) VALUES ('test'), ('uk'), ('co.uk')`)
	var domID int64
	err = pool.QueryRow(ctx, `
		INSERT INTO domains (zone_id, label, status, check_state, flags, dns_mask,
		                     last_checked_at, temp_attempts)
		VALUES ($1, 'example', 4, 2, 3, 5, DATE '2026-10-01', 2)
		RETURNING id`, zoneID("test"),
	).Scan(&domID)
	if err != nil {
		t.Fatalf("insert domains: %v", err)
	}
	mustExec(`
		INSERT INTO subdomain_results
		    (zone_id, domain_id, subdomain_id, variant, scheme, http_status,
		     final_url, alias_kind, alias_subdomain_id, rendered, content_hash,
		     error, checked_at, rules_version)
		VALUES
		    ($1, $2, 0, 0, 2, 200, 'https://example.test/', 0, NULL, true,
		     '\x0102030405060708090a0b0c0d0e0f10', NULL,
		     TIMESTAMPTZ '2026-10-01 12:00:00+00', 1),
		    ($1, $2, 2, 1, 1, NULL, NULL, 0, NULL, false, NULL, 1,
		     TIMESTAMPTZ '2026-10-01 12:00:01+00', NULL)`,
		zoneID("test"), domID)
	mustExec(`
		INSERT INTO laravel_findings
		    (zone_id, domain_id, subdomain_id, variant, score, ver_min, ver_max,
		     signals, is_laravel, rules_version)
		VALUES
		    ($1, $2, 0, 0, 0.8, '9.19', '11.0',
		     '[{"rule":"cookie_xsrftoken","weight":0.3,"evidence":"XSRF-TOKEN"},
	               {"rule":"header_laravel","weight":0.5,"evidence":"Set-Cookie: laravel_session"}]',
		     true, 42),
		    ($1, $2, 2, 1, 0.2, NULL, NULL,
		     '[{"rule":"generator","weight":0.2,"evidence":"meta generator"}]',
		     false, 42)`,
		zoneID("test"), domID)
	mustExec(`
		INSERT INTO laravel_domains (zone_id, domain_id, score, ver_min, ver_max,
		                             found_on_mask, rules_version, updated_at)
		VALUES ($1, $2, 0.8, '9.19', '11.0', 1, 42, TIMESTAMPTZ '2026-10-01 12:05:00+00')`,
		zoneID("test"), domID)
	mustExec(`INSERT INTO domains (zone_id, label)
		SELECT id, 'site' FROM zones WHERE name = 'co.uk'`)

	// The found domain: an input with the case, the edge spaces and a
	// dot — the SQL minimum of normalization (spec 5.2 item 1); the
	// fields — spec 5.11.
	var raw []byte
	if err := pool.QueryRow(ctx,
		`SELECT domain_info('  Example.TEST.  ')`,
	).Scan(&raw); err != nil {
		t.Fatalf("domain_info: %v", err)
	}
	info := dinfoDecode(t, raw)

	wantZone := map[string]any{
		"id":           float64(zoneID("test")),
		"name":         "test",
		"name_unicode": nil,
	}
	if got := dinfoMap(t, info["zone"], "zone"); !reflect.DeepEqual(got, wantZone) {
		t.Errorf("zone = %v, want %v", got, wantZone)
	}

	wantDomain := map[string]any{
		"id":               float64(domID),
		"label":            "example",
		"name":             "example.test",
		"status":           4.0,
		"status_name":      "alive",
		"check_state":      2.0,
		"check_state_name": "done",
		"is_idn":           true,
		"render_pending":   true,
		"dns_mask":         5.0,
		"temp_attempts":    2.0,
		"queued_at":        nil,
		"last_checked_at":  "2026-10-01",
	}
	if got := dinfoMap(t, info["domain"], "domain"); !reflect.DeepEqual(got, wantDomain) {
		t.Errorf("domain = %v, want %v", got, wantDomain)
	}

	// The dns_mask = 5 decode (the bits 0 and 2): the apex and admin.
	wantAlive := []any{
		map[string]any{"id": 0.0, "label": ""},
		map[string]any{"id": 2.0, "label": "admin"},
	}
	if got := dinfoSlice(t, info["dns_alive"], "dns_alive"); !reflect.DeepEqual(got, wantAlive) {
		t.Errorf("dns_alive = %v, want %v", got, wantAlive)
	}

	// The results per (subdomain, variant) in the subdomain/variant
	// order; checked_at is compared as a moment (the offset depends on
	// the session zone).
	wantChecked := []time.Time{
		time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 10, 1, 12, 0, 1, 0, time.UTC),
	}
	wantResults := []map[string]any{
		{
			"subdomain_id": 0.0, "subdomain": "", "variant": 0.0,
			"scheme": 2.0, "scheme_name": "https",
			"http_status": 200.0, "final_url": "https://example.test/",
			"alias_kind": 0.0, "alias_kind_name": "none", "alias": nil,
			"rendered": true, "error": nil,
			"content_hash": "0102030405060708090a0b0c0d0e0f10",
		},
		{
			"subdomain_id": 2.0, "subdomain": "admin", "variant": 1.0,
			"scheme": 1.0, "scheme_name": "http",
			"http_status": nil, "final_url": nil,
			"alias_kind": 0.0, "alias_kind_name": "none", "alias": nil,
			"rendered": false, "error": 1.0,
			"content_hash": nil,
		},
	}
	results := dinfoSlice(t, info["results"], "results")
	if len(results) != len(wantResults) {
		t.Fatalf("results: %d записей, want %d", len(results), len(wantResults))
	}
	for i, w := range wantResults {
		got := dinfoMap(t, results[i], "results[i]")
		if tm := dinfoTime(t, got["checked_at"], "results[i].checked_at"); !tm.UTC().Equal(wantChecked[i]) {
			t.Errorf("results[%d].checked_at = %v, want %v", i, tm.UTC(), wantChecked[i])
		}
		delete(got, "checked_at")
		if !reflect.DeepEqual(got, w) {
			t.Errorf("results[%d] = %v, want %v", i, got, w)
		}
	}

	// The Laravel signs: the laravel_domains aggregate + the findings
	// by the score descending — the confident hit first, the weak one
	// (is_laravel = false) is returned too (spec 5.11).
	lar := dinfoMap(t, info["laravel"], "laravel")
	wantFoundOn := []any{map[string]any{"id": 0.0, "label": ""}}
	if got := dinfoSlice(t, lar["found_on"], "laravel.found_on"); !reflect.DeepEqual(got, wantFoundOn) {
		t.Errorf("laravel.found_on = %v, want %v", got, wantFoundOn)
	}
	for field, want := range map[string]float64{
		"score": 0.8, "found_on_mask": 1.0, "rules_version": 42.0,
	} {
		if got := dinfoFloat(t, lar[field], "laravel."+field); got != want {
			t.Errorf("laravel.%s = %v, want %v", field, got, want)
		}
	}
	for field, want := range map[string]string{"ver_min": "9.19", "ver_max": "11.0"} {
		if got := dinfoStr(t, lar[field], "laravel."+field); got != want {
			t.Errorf("laravel.%s = %q, want %q", field, got, want)
		}
	}
	if tm := dinfoTime(t, lar["updated_at"], "laravel.updated_at"); !tm.UTC().Equal(time.Date(2026, 10, 1, 12, 5, 0, 0, time.UTC)) {
		t.Errorf("laravel.updated_at = %v, want 2026-10-01 12:05 UTC", tm.UTC())
	}
	wantFindings := []map[string]any{
		{
			"subdomain_id": 0.0, "subdomain": "", "variant": 0.0,
			"score": 0.8, "ver_min": "9.19", "ver_max": "11.0",
			"is_laravel": true,
			"signals": []any{
				map[string]any{"rule": "cookie_xsrftoken", "weight": 0.3, "evidence": "XSRF-TOKEN"},
				map[string]any{"rule": "header_laravel", "weight": 0.5, "evidence": "Set-Cookie: laravel_session"},
			},
		},
		{
			"subdomain_id": 2.0, "subdomain": "admin", "variant": 1.0,
			"score": 0.2, "ver_min": nil, "ver_max": nil,
			"is_laravel": false,
			"signals": []any{
				map[string]any{"rule": "generator", "weight": 0.2, "evidence": "meta generator"},
			},
		},
	}
	findings := dinfoSlice(t, lar["findings"], "laravel.findings")
	if len(findings) != len(wantFindings) {
		t.Fatalf("laravel.findings: %d записей, want %d", len(findings), len(wantFindings))
	}
	for i, w := range wantFindings {
		if got := dinfoMap(t, findings[i], "findings[i]"); !reflect.DeepEqual(got, w) {
			t.Errorf("laravel.findings[%d] = %v, want %v", i, got, w)
		}
	}

	// The zone — the longest name suffix from zones: site.co.uk in the
	// co.uk zone, although the uk zone also exists (like the PSL in
	// idn.Normalize).
	var zoneName, label string
	if err := pool.QueryRow(ctx, `
		SELECT domain_info('site.co.uk') -> 'zone' ->> 'name',
		       domain_info('site.co.uk') -> 'domain' ->> 'label'`,
	).Scan(&zoneName, &label); err != nil {
		t.Fatalf("domain_info site.co.uk: %v", err)
	}
	if zoneName != "co.uk" || label != "site" {
		t.Errorf("суффикс зоны: zone=%q label=%q, want co.uk/site", zoneName, label)
	}

	// Not found — NULL, not an error: an unknown domain in a known
	// zone, an unknown zone, a name of a single label (that is a zone,
	// not a domain), an empty input and NULL.
	var noDomain, noZone, singleLabel, empty, nullIn bool
	err = pool.QueryRow(ctx, `
		SELECT domain_info('unknown.test') IS NULL,
		       domain_info('example.unknownzone') IS NULL,
		       domain_info('test') IS NULL,
		       domain_info('') IS NULL,
		       domain_info(NULL) IS NULL`,
	).Scan(&noDomain, &noZone, &singleLabel, &empty, &nullIn)
	if err != nil {
		t.Fatalf("domain_info не найдено: %v", err)
	}
	for name, got := range map[string]bool{
		"неизвестный домен": noDomain, "неизвестная зона": noZone,
		"одна метка": singleLabel, "пустой вход": empty, "NULL": nullIn,
	} {
		if !got {
			t.Errorf("domain_info(%s): ожидался NULL", name)
		}
	}

	// A domain without results and signs: the empty arrays, laravel —
	// a jsonb null (not a SQL NULL: jsonb_build_object puts a JSON
	// null); the fresh domain is displayed with the default status
	// names.
	var noResults, noAlive, noLaravel, newStatus bool
	if err := pool.QueryRow(ctx, `
		SELECT domain_info('site.co.uk') -> 'results' = '[]'::jsonb,
		       domain_info('site.co.uk') -> 'dns_alive' = '[]'::jsonb,
		       domain_info('site.co.uk') -> 'laravel' = 'null'::jsonb,
		       domain_info('site.co.uk') -> 'domain' ->> 'status_name' = 'new'`,
	).Scan(&noResults, &noAlive, &noLaravel, &newStatus); err != nil {
		t.Fatalf("domain_info site (пустой): %v", err)
	}
	if !noResults || !noAlive || !noLaravel || !newStatus {
		t.Errorf("домен без результатов: results-пусто=%v dns_alive-пусто=%v laravel-null=%v status=%v, want все true",
			noResults, noAlive, noLaravel, newStatus)
	}
}

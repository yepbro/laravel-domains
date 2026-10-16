// Интеграционные контракты схемы (ТЗ 6, 6.1):
//
//   - T-003, таблицы zones/subdomains/domains: партиционирование LIST по
//     zone_id, fillfactor = 85 у каждой листовой партиции, частичный индекс
//     планировщика domains_pending_idx, sequence domain_id_seq,
//     HOT-совместимость — часто обновляемые колонки не входят ни в один
//     индекс (минимум индексов, last_checked_at без индекса);
//   - T-004, таблицы subdomain_results/contents/laravel_domains/
//     laravel_findings/ingest_files: партиционирование таблиц с domain_id,
//     fillfactor = 85 листьев subdomain_results (частые перезаписи
//     результатов), минимальный состав индексов, идемпотентность записи
//     по первичным ключам.
//
// Схема накатывается встроенным раннером (internal/migrate) в изолированную
// схему через search_path — так же, как при первом старте сервисов на пустой
// БД; тесты не зависят от состояния рабочей схемы.
package integration

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/migrate"
)

// migratedSchema накатывает встроенный набор миграций в изолированную схему
// (withTestSchema из migrate_test.go) и возвращает пул с search_path на неё.
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

// containsWord — совпадение слова целиком (по границам слова), чтобы,
// например, поиск «label» не срабатывал на «name_unicode_label».
func containsWord(s, word string) bool {
	return regexp.MustCompile(`\b` + regexp.QuoteMeta(word) + `\b`).MatchString(s)
}

// tablePartition — партиция таблицы: листовая (relkind r) либо
// промежуточный родитель субпартиций (relkind p, T-005).
type tablePartition struct {
	name       string
	relkind    string
	reloptions string // array_to_string(reloptions), '' если NULL
	bound      string // pg_get_expr(relpartbound), '' у корня
	indexCount int
}

// tablePartitions возвращает все партиции таблицы рекурсивно: прямых детей
// и (когда появятся субпартиции T-005) вложенные уровни вместе с числом
// индексов каждой.
func tablePartitions(t *testing.T, pool *pgxpool.Pool, table string) []tablePartition {
	t.Helper()

	// Рекурсивный обход наследования от таблицы (не от индексов:
	// у партиционированных индексов своё наследование, здесь оно не мешает).
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

// tableIndex — индекс на таблице (для партиционированных — на родителе).
type tableIndex struct {
	name string
	def  string // pg_get_indexdef целиком
	pred string // предикат частичного индекса, '' у обычного
}

// tableIndexes возвращает индексы таблицы (не её партиций).
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

// assertListPartitionedByZoneID — таблица партиционирована LIST по zone_id
// (принцип ТЗ 6): relkind p, стратегия l, ключ «LIST (zone_id)».
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

// leafPartitions возвращает листовые партиции таблицы и проверяет, что
// они есть и среди них DEFAULT: без неё новые зоны не смогут записаться
// до выделения партиции (T-005).
func leafPartitions(t *testing.T, pool *pgxpool.Pool, table string) []tablePartition {
	t.Helper()

	parts := tablePartitions(t, pool, table)
	leaves := make([]tablePartition, 0, len(parts))
	defaultSeen := false
	for _, p := range parts {
		if p.relkind != "r" {
			continue // промежуточный родитель субпартиций (T-005)
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

// TestSchema_DomainsPartitioning — domains партиционирована LIST по zone_id
// с DEFAULT-партицией (запись новых зон до выделения отдельных партиций);
// у каждой листовой партиции fillfactor = 85 (ТЗ 6: место на странице под
// версии строк при частых апдейтах). Storage parameters на партиционированном
// родителе в PostgreSQL 16 недопустимы, а reloptions не наследуются — поэтому
// параметр проверяется у каждой партиции отдельно.
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

// TestSchema_DomainsIndexes — на domains ровно три индекса («минимум
// индексов», ТЗ 6): первичный ключ, уникальный (zone_id, label) для поиска
// по имени (ТЗ 5.11) и частичный domains_pending_idx для выборки pending
// планировщиком (ТЗ 5.3). У каждой листовой партиции — прикреплённые
// дочерние индексы всех индексов родителя.
func TestSchema_DomainsIndexes(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)

	indexes := tableIndexes(t, pool, "domains")
	want := map[string]bool{
		"domains_pkey":              true,
		"domains_zone_id_label_key": true,
		"domains_pending_idx":       true,
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

	var pending *tableIndex
	for i := range indexes {
		if indexes[i].name == "domains_pending_idx" {
			pending = &indexes[i]
		}
	}
	if pending == nil {
		t.Fatal("domains_pending_idx не найден")
	}
	if !strings.Contains(pending.pred, "check_state = 0") {
		t.Errorf("domains_pending_idx: предикат = %q, want \"check_state = 0\"", pending.pred)
	}
	if key, _, _ := strings.Cut(pending.def, " WHERE "); !strings.Contains(key, "(zone_id, id)") {
		t.Errorf("domains_pending_idx: ключ = %q, want (zone_id, id)", key)
	}

	for _, p := range leafPartitions(t, pool, "domains") {
		if p.indexCount != len(indexes) {
			t.Errorf("партиция %s: индексов %d, want %d (дочерние всех индексов родителя)", p.name, p.indexCount, len(indexes))
		}
	}
}

// TestSchema_DomainIDSequence — id доменам выдаёт sequence domain_id_seq,
// единый для всех зон и партиций (ТЗ 6.1); дубль (zone_id, label) отбивается
// уникальным индексом (идемпотентность ingest, ТЗ 5.1).
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

	// id заполняется sequence автоматически (внешних ключей в схеме нет —
	// минимум индексов, зона не нужна).
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

// TestSchema_DomainsHOTColumns — обновляемые при проверке колонки (status,
// flags, dns_mask, queued_at, last_checked_at) не входят ни в один индекс:
// массовые апдейты результатов остаются HOT, а fillfactor = 85 оставляет
// место под новые версии строк на той же странице (ТЗ 6). Исключение из
// HOT — смены check_state: колонка входит в предикат domains_pending_idx,
// что неустранимо при требуемом ТЗ частичном индексе (§ 5.3).
func TestSchema_DomainsHOTColumns(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)

	hotColumns := []string{"status", "flags", "dns_mask", "queued_at", "last_checked_at", "temp_attempts"}
	for _, ix := range tableIndexes(t, pool, "domains") {
		for _, col := range hotColumns {
			if containsWord(ix.def, col) {
				t.Errorf("индекс %s ссылается на часто обновляемую колонку %s: её апдейты перестают быть HOT", ix.name, col)
			}
		}
		if ix.name != "domains_pending_idx" && containsWord(ix.def, "check_state") {
			t.Errorf("индекс %s ссылается на check_state: допустимо только в предикате domains_pending_idx", ix.name)
		}
	}

	// В самом domains_pending_idx check_state — только предикат, не ключ.
	for _, ix := range tableIndexes(t, pool, "domains") {
		if ix.name != "domains_pending_idx" {
			continue
		}
		key, _, _ := strings.Cut(ix.def, " WHERE ")
		if containsWord(key, "check_state") {
			t.Errorf("domains_pending_idx: check_state в ключе (%s): индексируемые смены состояния всегда non-HOT", key)
		}
	}
}

// TestSchema_TableColumns — зоны и субдомены — маленькие справочники без
// партиционирования (принцип ТЗ 6 — партиционируются таблицы с domain_id);
// состав и типы колонок всех трёх таблиц соответствуют эскизу DDL 6.1
// (минимальные размеры полей).
func TestSchema_TableColumns(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	// таблица → relkind (r — обычная, p — партиционированная) и колонка → тип.
	want := map[string]struct {
		relkind string
		columns map[string]string
	}{
		"zones": {relkind: "r", columns: map[string]string{
			"id": "smallint", "name": "text", "name_unicode": "text",
			"is_active": "boolean", "domains_total": "bigint",
			"domains_checked": "bigint", "updated_at": "timestamp with time zone",
		}},
		"subdomains": {relkind: "r", columns: map[string]string{
			"id": "smallint", "label": "text", "enabled": "boolean",
		}},
		"domains": {relkind: "p", columns: map[string]string{
			"zone_id": "smallint", "id": "bigint", "label": "text",
			"status": "smallint", "check_state": "smallint", "flags": "smallint",
			"dns_mask": "bigint", "queued_at": "timestamp with time zone",
			"last_checked_at": "date",
			"temp_attempts":   "smallint", // T-031: счётчик повторов temp_error (миграция 000005)
		}},
	}
	// Уникальные ключи справочников: зоны дедуплицируются по punycode-имени
	// (ТЗ 5.2), субдомены — по метке.
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

// assertColumnTypes проверяет присутствие колонок и их типы (format_type).
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

// assertUniqueIndex проверяет, что на таблице есть уникальный индекс по колонке.
func assertUniqueIndex(t *testing.T, pool *pgxpool.Pool, table, column string) {
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
		if key, _, _ := strings.Cut(def, " WHERE "); strings.Contains(key, "("+column+")") {
			found = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("итерация индексов %s: %v", table, err)
	}
	if !found {
		t.Errorf("%s: нет уникального индекса по колонке %s", table, column)
	}
}

// assertNotNull проверяет, что колонки таблицы NOT NULL (обязательные поля
// эскиза 6.1).
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

// ---------------------------------------------------------------- T-004: таблицы результатов

// TestSchema_ResultsTablesPartitioning — таблицы результатов с domain_id
// (subdomain_results, laravel_domains, laravel_findings) партиционированы
// LIST по zone_id с DEFAULT-партицией (эскиз 6.1). У каждой листовой
// партиции subdomain_results fillfactor = 85: история проверок не хранится
// (ТЗ 3.5) — перепроверка перезаписывает строки с тем же PK, а ТЗ 6.3
// относит таблицу к часто обновляемым; laravel_* пишутся один раз
// и перезаписываются только переанализом — fillfactor им не задаётся.
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

// TestSchema_ResultsTablesColumns — состав и типы колонок пяти таблиц T-004
// соответствуют эскизу DDL 6.1 (минимальные размеры полей: smallint, int4,
// bytea); contents и ingest_files без domain_id — обычные таблицы (принцип
// ТЗ 6: партиционируются таблицы с domain_id).
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
		}, notNull: []string{
			"zone_id", "domain_id", "subdomain_id", "variant",
			"score", "signals", "is_laravel",
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

	// Идемпотентность ingest: файл опознаётся по sha256 (ТЗ 5.1).
	assertUniqueIndex(t, pool, "ingest_files", "sha256")
}

// TestSchema_ResultsTablesIndexes — «минимум индексов» (ТЗ 6): на таблицах
// T-004 только первичные ключи — выборки по домену идут по префиксу
// (zone_id, domain_id) составных ключей, — плюс уникальный ingest_files.sha256.
// Индексы под будущие выборки (например, laravel_domains по score — этап 8,
// ТЗ 5.11) сознательно не добавляются: состав зафиксирован этим контрактом.
func TestSchema_ResultsTablesIndexes(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)

	// таблица → имя индекса → ожидаемый ключ.
	want := map[string]map[string]string{
		"subdomain_results": {"subdomain_results_pkey": "(zone_id, domain_id, subdomain_id, variant)"},
		"contents":          {"contents_pkey": "(hash)"},
		"laravel_domains":   {"laravel_domains_pkey": "(zone_id, domain_id)"},
		"laravel_findings":  {"laravel_findings_pkey": "(zone_id, domain_id, subdomain_id, variant)"},
		"ingest_files": {
			"ingest_files_pkey":       "(id)",
			"ingest_files_sha256_key": "(sha256)",
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

		if table == "ingest_files" || table == "contents" {
			continue // обычные таблицы, дочерних индексов нет
		}
		for _, p := range leafPartitions(t, pool, table) {
			if p.indexCount != len(wantIndexes) {
				t.Errorf("партиция %s: индексов %d, want %d (дочерние всех индексов родителя)", p.name, p.indexCount, len(wantIndexes))
			}
		}
	}
}

// TestSchema_ResultsIdempotency — запись результатов идемпотентна (ТЗ 3.4):
// повторная вставка строки с тем же ключом отбивается первичным ключом,
// для ingest_files — уникальным sha256 даже при другом пути файла (ТЗ 5.1);
// применяются defaults эскиза (variant/alias_kind/rendered, refcount).
// Внешних ключей на domains нет — минимум индексов (ТЗ 6), целостность
// ссылок (zone_id, domain_id) обеспечивает код записи.
func TestSchema_ResultsIdempotency(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	// dupInsert — вторая вставка той же строки должна упасть на дубликат.
	dupInsert := func(table, sql string) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql); err == nil || !strings.Contains(err.Error(), "duplicate key") {
			t.Errorf("%s: повторная вставка: err = %v, want duplicate key", table, err)
		}
	}

	// subdomain_results: строка результата с defaults.
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

	// contents: метаданные контента, refcount по умолчанию 1.
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

	// laravel_domains: агрегат по домену.
	if _, err := pool.Exec(ctx, `
		INSERT INTO laravel_domains (zone_id, domain_id, score, ver_min, ver_max, found_on_mask, rules_version, updated_at)
		VALUES (1, 10, 0.75, '9.19', '11', 3, 1, now())`,
	); err != nil {
		t.Fatalf("insert laravel_domains: %v", err)
	}
	dupInsert("laravel_domains", `
		INSERT INTO laravel_domains (zone_id, domain_id, score, found_on_mask, rules_version, updated_at)
		VALUES (1, 10, 1, 1, 1, now())`)

	// laravel_findings: срабатывания правил по (домен, субдомен, вариант).
	if _, err := pool.Exec(ctx, `
		INSERT INTO laravel_findings (zone_id, domain_id, subdomain_id, variant, score, signals, is_laravel)
		VALUES (1, 10, 0, 0, 0.75, '[{"rule":"cookie_xsrf_token","weight":0.25}]', true)`,
	); err != nil {
		t.Fatalf("insert laravel_findings: %v", err)
	}
	dupInsert("laravel_findings", `
		INSERT INTO laravel_findings (zone_id, domain_id, subdomain_id, variant, score, signals, is_laravel)
		VALUES (1, 10, 0, 0, 1, '[]', true)`)

	// ingest_files: отчёт по обработанному файлу; дубль sha256 — другой
	// путь, тот же контент файла — отбивается уникальным индексом.
	if _, err := pool.Exec(ctx, `
		INSERT INTO ingest_files (zone_id, action, path, sha256, lines_total, added, existed, deleted, rejected)
		VALUES (1, 1, 'incoming/com/add/a.txt', '\x0102', 100, 90, 5, 0, 5)`,
	); err != nil {
		t.Fatalf("insert ingest_files: %v", err)
	}
	dupInsert("ingest_files", `
		INSERT INTO ingest_files (zone_id, action, path, sha256)
		VALUES (1, 1, 'processed/com/add/a.txt', '\x0102')`)
}

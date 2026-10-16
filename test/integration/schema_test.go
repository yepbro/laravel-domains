// Интеграционные тесты схемы таблиц zones/subdomains/domains (T-003,
// ТЗ 6, 6.1): партиционирование LIST по zone_id, fillfactor = 85 у каждой
// листовой партиции, частичный индекс планировщика domains_pending_idx,
// sequence domain_id_seq и HOT-совместимость — часто обновляемые колонки
// не входят ни в один индекс (ТЗ 6: минимум индексов, last_checked_at
// без индекса).
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

// domainPartition — партиция таблицы domains: листовая (relkind r)
// либо промежуточный родитель субпартиций (relkind p, T-005).
type domainPartition struct {
	name       string
	relkind    string
	reloptions string // array_to_string(reloptions), '' если NULL
	bound      string // pg_get_expr(relpartbound), '' у корня
	indexCount int
}

// domainPartitions возвращает все партиции domains рекурсивно: прямых детей
// и (когда появятся субпартиции T-005) вложенные уровни вместе с числом
// индексов каждой.
func domainPartitions(t *testing.T, pool *pgxpool.Pool) []domainPartition {
	t.Helper()

	// Рекурсивный обход наследования от таблицы domains (не от индексов:
	// у партиционированных индексов своё наследование, здесь оно не мешает).
	const query = `
		WITH RECURSIVE parts AS (
			SELECT c.oid, c.relname, c.relkind, c.reloptions, c.relpartbound
			FROM pg_inherits i
			JOIN pg_class c ON c.oid = i.inhrelid
			JOIN pg_class parent ON parent.oid = i.inhparent
			JOIN pg_namespace n ON n.oid = parent.relnamespace
			WHERE parent.relname = 'domains' AND n.nspname = current_schema()
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

	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("каталог партиций domains: %v", err)
	}
	defer rows.Close()

	parts := make([]domainPartition, 0, 2)
	for rows.Next() {
		var p domainPartition
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

// domainIndex — индекс на партиционированном родителе domains.
type domainIndex struct {
	name string
	def  string // pg_get_indexdef целиком
	pred string // предикат частичного индекса, '' у обычного
}

// domainIndexes возвращает индексы родителя domains (не партиций).
func domainIndexes(t *testing.T, pool *pgxpool.Pool) []domainIndex {
	t.Helper()

	const query = `
		SELECT i.relname, pg_get_indexdef(i.oid),
		       COALESCE(pg_get_expr(x.indpred, x.indrelid), '')
		FROM pg_index x
		JOIN pg_class i ON i.oid = x.indexrelid
		JOIN pg_class tbl ON tbl.oid = x.indrelid
		JOIN pg_namespace n ON n.oid = tbl.relnamespace
		WHERE n.nspname = current_schema() AND tbl.relname = 'domains'
		ORDER BY i.relname`

	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("каталог индексов domains: %v", err)
	}
	defer rows.Close()

	indexes := make([]domainIndex, 0, 3)
	for rows.Next() {
		var ix domainIndex
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

// TestSchema_DomainsPartitioning — domains партиционирована LIST по zone_id
// с DEFAULT-партицией (запись новых зон до выделения отдельных партиций);
// у каждой листовой партиции fillfactor = 85 (ТЗ 6: место на странице под
// версии строк при частых апдейтах). Storage parameters на партиционированном
// родителе в PostgreSQL 16 недопустимы, а reloptions не наследуются — поэтому
// параметр проверяется у каждой партиции отдельно.
func TestSchema_DomainsPartitioning(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)

	var relkind, partkey, strat string
	err := pool.QueryRow(context.Background(), `
		SELECT c.relkind, COALESCE(p.partstrat::text, ''),
		       COALESCE(pg_get_partkeydef(c.oid), '')
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_partitioned_table p ON p.partrelid = c.oid
		WHERE n.nspname = current_schema() AND c.relname = 'domains'`,
	).Scan(&relkind, &strat, &partkey)
	if err != nil {
		t.Fatalf("pg_class для domains: %v", err)
	}
	if relkind != "p" {
		t.Errorf("domains: relkind = %s, want p (партиционированная таблица)", relkind)
	}
	if strat != "l" {
		t.Errorf("domains: стратегия партиционирования = %q, want \"l\" (LIST)", strat)
	}
	if partkey != "LIST (zone_id)" {
		t.Errorf("domains: ключ партиционирования = %q, want \"LIST (zone_id)\"", partkey)
	}

	parts := domainPartitions(t, pool)
	leaves, defaultSeen := 0, false
	for _, p := range parts {
		if p.relkind != "r" {
			continue // промежуточный родитель субпартиций (T-005)
		}
		leaves++
		if strings.Contains(p.bound, "DEFAULT") {
			defaultSeen = true
		}
		if !strings.Contains(p.reloptions, "fillfactor=85") {
			t.Errorf("партиция %s: reloptions = %q, want fillfactor=85 (reloptions не наследуются от родителя)", p.name, p.reloptions)
		}
	}
	if leaves == 0 {
		t.Error("нет ни одной листовой партиции domains")
	}
	if !defaultSeen {
		t.Error("нет DEFAULT-партиции: новые зоны не смогут записаться до выделения партиции")
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

	indexes := domainIndexes(t, pool)
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

	var pending *domainIndex
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

	for _, p := range domainPartitions(t, pool) {
		if p.relkind == "r" && p.indexCount != len(indexes) {
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

	hotColumns := []string{"status", "flags", "dns_mask", "queued_at", "last_checked_at"}
	for _, ix := range domainIndexes(t, pool) {
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
	for _, ix := range domainIndexes(t, pool) {
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

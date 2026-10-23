// Интеграционные контракты схемы (ТЗ 6, 6.1):
//
//   - T-003, таблицы zones/subdomains/domains: партиционирование LIST по
//     zone_id, fillfactor = 85 у каждой листовой партиции, частичные
//     индексы проходов планировщика по check_state — выборка pending
//     (domains_pending_idx), возврат lease (domains_lease_idx) и
//     перепроверка (domains_recheck_idx, T-104), частичный индекс
//     deleted-доменов очистки ingest (domains_deleted_idx, T-170), окно
//     мониторинга по last_checked_at (domains_last_checked_idx, T-171),
//     sequence domain_id_seq, HOT-совместимость — массово обновляемые
//     колонки не входят ни в один индекс, кроме возраста lease/recheck
//     и окна мониторинга в ключах (меняется только вместе с check_state)
//     и status в предикате deleted-индекса (строки вне deleted в индекс
//     не попадают);
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

// TestSchema_DomainsIndexes — на domains ровно семь индексов («минимум
// индексов», ТЗ 6): первичный ключ, уникальный (zone_id, label) для поиска
// по имени (ТЗ 5.11), три частичных по check_state для проходов
// планировщика (ТЗ 5.3): выборка pending (domains_pending_idx, 000001),
// возврат истёкших lease (domains_lease_idx, T-104) и суточная
// перепроверка (domains_recheck_idx, T-104), частичный deleted-доменов
// очистки ingest (domains_deleted_idx, T-170, 0013) и окно мониторинга
// по last_checked_at (domains_last_checked_idx, T-171, 0014). Ключи
// фоновых индексов начинаются с колонки возраста (queued_at /
// last_checked_at) и завершаются доменным ключом — ORDER BY проходов
// совпадает с ключом, скан читает только нужный префикс. У каждой
// листовой партиции — прикреплённые дочерние индексы всех индексов
// родителя.
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

	// Частичные индексы проходов: предикат — только check_state,
	// ключ — колонка возраста + доменный ключ (T-104, миграция 000008;
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

// TestSchema_DomainsHOTColumns — обновляемые при проверке колонки не
// входят ни в один индекс, кроме заранее принятых компромиссов (ТЗ 6):
// massовые апдейты остаются HOT, а fillfactor = 85 оставляет место под
// новые версии строк на той же странице. Три компромисса:
//
//   - check_state — в предикатах трёх частичных индексов проходов
//     (pending/lease/recheck): смены состояний всегда non-HOT —
//     неустранимо при требуемых ТЗ §5.3 частичных индексах;
//   - queued_at / last_checked_at — в ключах lease/recheck-индексов
//     (T-104) и last_checked_at ещё в ключе доменного окна мониторинга
//     domains_last_checked_idx (T-171): их апдейты non-HOT, однако все
//     писатели меняют эти колонки ТОЛЬКО вместе с check_state
//     (enqueue, возврат lease, перепроверка, результат воркера dns/http,
//     воскрешение add-файлом) — переходы уже non-HOT, новых non-HOT
//     апдейтов индексы не создают;
//   - status — в предикате (не ключе!) частичного индекса deleted-
//     доменов domains_deleted_idx (T-170): в индексе живут только
//     строки status = deleted, non-HOT становятся лишь переходы с
//     участием deleted (применение delete-файлов T-019, воскрешение
//     T-018) — редкие события реестра; массовые апдейты
//     статуса-результата и фиксация итогового статуса temp_error
//     (T-031) строк в индексе не имеют и остаются HOT.
func TestSchema_DomainsHOTColumns(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)

	hotColumns := []string{"flags", "dns_mask", "temp_attempts"}
	// Колонки возраста — только в ключах своих индексов проходов/окон.
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
		// status — только предикат (не ключ) domains_deleted_idx (T-170):
		// строки вне deleted в индекс не попадают, массовые смены
		// статуса остаются HOT.
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

	// В частичных индексах проходов check_state — только предикат, не ключ:
	// индексация смен состояний делала бы их non-HOT даже там, где фильтра
	// по состоянию достаточно.
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
			"domains_laravel": "bigint", // кэш Laravel-доменов зоны (T-120, лендинг)
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
		"web_checks": {relkind: "r", columns: map[string]string{ // T-120: лог формы лендинга
			"id": "bigint", "domain": "text", "punycode_domain": "text",
			"ip_address": "inet", "found": "boolean", "score": "real",
			"duration_ms": "integer", "created_at": "timestamp with time zone",
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

// assertUniqueIndex проверяет, что на таблице есть уникальный индекс
// с данным ключом (колонка или список через запятую).
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

	// Идемпотентность ingest: дедуп — точная повторная доставка
	// (zone_id, action, sha256) — уникальный ключ T-113 (миграция 000009).
	assertUniqueIndex(t, pool, "ingest_files", "zone_id, action, sha256")
}

// TestSchema_ResultsTablesIndexes — «минимум индексов» (ТЗ 6): на таблицах
// T-004 только первичные ключи — выборки по домену идут по префиксу
// (zone_id, domain_id) составных ключей, — плюс уникальный
// ingest_files (zone_id, action, sha256) с обслуживающим
// ingest_files_sha256_idx (T-113: дедуп точной повторной доставки и
// запрос конвейера по содержимому), индекс компактификации
// contents_pack_off_idx (pack_id, off) для фоновых
// запросов CompactLoop по паку с сортировкой по смещению (T-096, INFRA-05;
// refcount в ключ не входит — апдейты счётчика остаются HOT) и окно
// мониторинга subdomain_results_checked_idx (checked_at) — часовые
// счётчики темпа сводки (T-171; checked_at пишется только INSERT'ом,
// HOT-картина не меняется).
// Индексы под будущие выборки (например, laravel_domains по score — этап 8,
// ТЗ 5.11) сознательно не добавляются: состав зафиксирован этим контрактом.
func TestSchema_ResultsTablesIndexes(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)

	// таблица → имя индекса → ожидаемый ключ.
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
			"laravel_domains_updated_at_idx": "(updated_at)", // «последние находки» лендинга (T-120)
		},
		"laravel_findings": {"laravel_findings_pkey": "(zone_id, domain_id, subdomain_id, variant)"},
		"ingest_files": {
			"ingest_files_pkey":                      "(id)",
			"ingest_files_zone_id_action_sha256_key": "(zone_id, action, sha256)",
			"ingest_files_sha256_idx":                "(sha256)",
		},
		"web_checks": { // лог формы лендинга (T-120)
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
// для ingest_files — уникальной тройкой (zone_id, action, sha256) даже
// при другом пути файла (T-113, ТЗ 5.1); то же содержимое другим
// действием (undo) или в другой зоне записывается отдельной строкой;
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
		INSERT INTO laravel_findings (zone_id, domain_id, subdomain_id, variant, score, signals, is_laravel, rules_version)
		VALUES (1, 10, 0, 0, 0.75, '[{"rule":"cookie_xsrf_token","weight":0.25}]', true, 1)`,
	); err != nil {
		t.Fatalf("insert laravel_findings: %v", err)
	}
	dupInsert("laravel_findings", `
		INSERT INTO laravel_findings (zone_id, domain_id, subdomain_id, variant, score, signals, is_laravel, rules_version)
		VALUES (1, 10, 0, 0, 1, '[]', true, 1)`)

	// ingest_files: отчёт по обработанному файлу; точная повторная
	// доставка — та же зона, действие и sha256 (другой путь не помогает)
	// — отбивается уникальным ключом T-113; то же содержимое другим
	// действием (undo) или в другой зоне — отдельные легитимные строки.
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
		{1, 2}, // undo тем же списком: то же содержимое из delete/
		{2, 1}, // повторная доставка в другой зоне
	} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO ingest_files (zone_id, action, path, sha256)
			VALUES ($1, $2, 'com/add/b.txt', '\x0102')`, d.zone, d.action); err != nil {
			t.Errorf("ingest_files: то же содержимое (zone=%d, action=%d) обязано записываться отдельно: %v",
				d.zone, d.action, err)
		}
	}
}

// ---------------------------------------------------------------- T-082: domain_info

// dinfoDecode разбирает ответ domain_info в карту (encoding/json:
// числа — float64, порядок ключей jsonb не сохраняет — сравнение
// по картам, а не по строкам).
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

// dinfoMap, dinfoSlice, dinfoFloat, dinfoStr — доступ к элементам
// ответа с проверкой типа: имена и виды полей — часть контракта
// (детерминированные пути для psql и тестов).
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

// dinfoTime разбирает метку времени ответа: timestamptz в jsonb —
// строка RFC 3339 со смещением зоны сессии, поэтому сравниваются
// моменты, а не строки.
func dinfoTime(t *testing.T, v any, what string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, dinfoStr(t, v, what))
	if err != nil {
		t.Fatalf("domain_info: %s: %v", what, err)
	}
	return tm
}

// TestSchema_DomainInfoFunction — контракт SQL-функции domain_info(text)
// (миграция 000006, ТЗ 5.11): сводка домена одним jsonb — зона, строка
// домена с именами статусов, расшифровка dns_mask, результаты по
// субдоменам/вариантам и признаки Laravel (агрегат laravel_domains +
// все findings по убыванию score, слабые с пометкой is_laravel).
// Вход нормализуется SQL-минимумом (регистр, краевые пробелы и точка),
// зона — самый длинный суффикс из справочника zones; поиск домена —
// по уникальному ключу (zone_id, label). Домен/зона не найдены — NULL,
// не ошибка. Функция только читает: STABLE, PARALLEL SAFE, SECURITY
// INVOKER, search_path зафиксирован (pg_catalog + схема миграции).
func TestSchema_DomainInfoFunction(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	// Функция на месте и только читает: provolatile/proparallel 's'
	// (STABLE, PARALLEL SAFE), prokind 'f', возврат jsonb, конфигурация
	// search_path указывает на pg_catalog и схему миграции.
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

	// Данные: зоны test + uk/co.uk (для отделения зоны по самому длинному
	// суффиксу), домен example с результатами и признаками Laravel,
	// домен site в co.uk без результатов.
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

	// Найденный домен: вход с регистром, краевыми пробелами и точкой —
	// SQL-минимум нормализации (ТЗ 5.2 п. 1); поля — ТЗ 5.11.
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

	// Расшифровка dns_mask = 5 (биты 0 и 2): апекс и admin.
	wantAlive := []any{
		map[string]any{"id": 0.0, "label": ""},
		map[string]any{"id": 2.0, "label": "admin"},
	}
	if got := dinfoSlice(t, info["dns_alive"], "dns_alive"); !reflect.DeepEqual(got, wantAlive) {
		t.Errorf("dns_alive = %v, want %v", got, wantAlive)
	}

	// Результаты по (субдомен, вариант) в порядке субдомен/вариант;
	// checked_at сравнивается моментом (смещение зависит от зоны сессии).
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

	// Признаки Laravel: агрегат laravel_domains + findings по убыванию
	// score — уверенное срабатывание первым, слабое (is_laravel = false)
	// тоже возвращается (ТЗ 5.11).
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

	// Зона — самый длинный суффикс имени из zones: site.co.uk в зоне
	// co.uk, хотя зона uk тоже существует (как PSL в idn.Normalize).
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

	// Не найдено — NULL, не ошибка: неизвестный домен в известной зоне,
	// неизвестная зона, имя из одной метки (это зона, не домен), пустой
	// вход и NULL.
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

	// Домен без результатов и признаков: пустые массивы, laravel —
	// jsonb null (не SQL NULL: jsonb_build_object кладёт JSON null);
	// свежий домен отображается именами статусов по умолчанию.
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

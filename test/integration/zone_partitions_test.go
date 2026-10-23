// Интеграционные тесты субпартиционирования крупных зон (T-005, ТЗ §6:
// «крупные зоны дополнительно по хэшу id, чтобы партиция не превышала
// ≈ 30–50 млн строк»).
//
// Процедура create_zone_partitions (миграция 000003) выделяет зону из
// DEFAULT-партиции во всех четырёх таблицах с zone_id:
//   - domains — в обычную LIST-партицию: субпартиционирование по хэшу
//     невозможно при живом UNIQUE (zone_id, label) — PG16 требует включать
//     колонки ключа партиционирования каждого уровня во все уникальные
//     ключи, а общая часть PK (zone_id, id) и (zone_id, label) — только
//     zone_id, константа внутри партиции зоны (решение T-005, docs/schema.md);
//   - subdomain_results / laravel_domains / laravel_findings — в LIST-партицию,
//     субпартиционированную HASH по domain_id (PK содержат domain_id,
//     других уникальных ключей нет).
//
// Проверяются: перенос данных из DEFAULT, структура листьев (bound, fillfactor,
// дочерние индексы), состояние DEFAULT после цикла detach/attach, маршрутизация
// новых строк, идемпотентность повторного вызова, валидация аргументов
// и откат миграции.
package integration

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"testing"

	golangmigrate "github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/migrate"
	"crawler/migrations"

	// database/sql-драйвер для golang-migrate в тесте отката
	// (тот же, что использует раннер internal/migrate).
	_ "github.com/jackc/pgx/v5/stdlib"
)

// insertZone добавляет зону и возвращает её id (identity последовательный,
// но тест не зависит от конкретных значений).
func insertZone(t *testing.T, pool *pgxpool.Pool, name string) int16 {
	t.Helper()

	var id int16
	if err := pool.QueryRow(context.Background(),
		`INSERT INTO zones (name) VALUES ($1) RETURNING id`, name,
	).Scan(&id); err != nil {
		t.Fatalf("insert zone %s: %v", name, err)
	}
	return id
}

// callCreateZonePartitions вызывает процедуру создания партиций зоны.
func callCreateZonePartitions(t *testing.T, pool *pgxpool.Pool, zoneID int16, hashParts int) {
	t.Helper()

	if _, err := pool.Exec(context.Background(),
		`CALL create_zone_partitions($1, $2)`, zoneID, hashParts,
	); err != nil {
		t.Fatalf("CALL create_zone_partitions(%d, %d): %v", zoneID, hashParts, err)
	}
}

// zonePartition возвращает партицию зоны <table>_z<zone> (лист для domains,
// промежуточный родитель hash-листьев для таблиц результатов).
func zonePartition(t *testing.T, pool *pgxpool.Pool, table string, zoneID int16) tablePartition {
	t.Helper()

	name := fmt.Sprintf("%s_z%d", table, zoneID)
	for _, p := range tablePartitions(t, pool, table) {
		if p.name == name {
			return p
		}
	}
	t.Fatalf("партиция %s не найдена", name)
	return tablePartition{}
}

// zoneLeafs возвращает hash-листья партиции зоны — части <table>_z<zone>_h<N>.
func zoneLeafs(t *testing.T, pool *pgxpool.Pool, table string, zoneID int16) []tablePartition {
	t.Helper()

	prefix := fmt.Sprintf("%s_z%d_h", table, zoneID)
	leafs := make([]tablePartition, 0, 4)
	for _, p := range tablePartitions(t, pool, table) {
		if strings.HasPrefix(p.name, prefix) {
			leafs = append(leafs, p)
		}
	}
	return leafs
}

// countZoneRows считает строки зоны в таблице.
func countZoneRows(t *testing.T, pool *pgxpool.Pool, table string, zoneID int16) int {
	t.Helper()

	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM `+table+` WHERE zone_id = $1`, zoneID,
	).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// TestZonePartitions_ExtractZoneWithData — полный сценарий: данные зоны
// лежат в DEFAULT, процедура выделяет зону, данные переносятся в новые
// партиции, запросы работают, новые строки маршрутизируются, прочие зоны
// остаются в DEFAULT.
func TestZonePartitions_ExtractZoneWithData(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	// Зоны: big — выделяется с данными; small — остаётся в DEFAULT;
	// empty — выделяется без данных (путь «до загрузки зоны»).
	big := insertZone(t, pool, "big")
	small := insertZone(t, pool, "small")
	empty := insertZone(t, pool, "empty")

	// Данные зоны big до выделения пишутся в DEFAULT-партиции.
	seed := func(table string, zoneID int16, n int) {
		t.Helper()
		queries := map[string]string{
			"domains": `INSERT INTO domains (zone_id, label)
				SELECT $1, 'site' || g FROM generate_series(1, $2) g`,
			"subdomain_results": `INSERT INTO subdomain_results (zone_id, domain_id, subdomain_id, scheme, checked_at)
				SELECT $1, g, 0, 2, now() FROM generate_series(1, $2) g`,
			"laravel_domains": `INSERT INTO laravel_domains (zone_id, domain_id, score, ver_min, ver_max, found_on_mask, rules_version, updated_at)
				SELECT $1, g, 0.75, '10', '11', 3, 1, now() FROM generate_series(1, $2) g`,
			"laravel_findings": `INSERT INTO laravel_findings (zone_id, domain_id, subdomain_id, variant, score, signals, is_laravel, rules_version)
				SELECT $1, g, 0, 0, 0.8, '[{"rule":"x","weight":0.5}]', true, 1 FROM generate_series(1, $2) g`,
		}
		if _, err := pool.Exec(ctx, queries[table], zoneID, n); err != nil {
			t.Fatalf("seed %s: %v", table, err)
		}
	}
	tables := []string{"domains", "subdomain_results", "laravel_domains", "laravel_findings"}
	seed("domains", big, 120)
	seed("subdomain_results", big, 40)
	seed("laravel_domains", big, 6)
	seed("laravel_findings", big, 8)
	seed("domains", small, 5)
	seed("subdomain_results", small, 3)

	wantRows := make(map[string]int, len(tables))
	for _, table := range tables {
		wantRows[table] = countZoneRows(t, pool, table, big)
	}

	callCreateZonePartitions(t, pool, big, 4)
	callCreateZonePartitions(t, pool, empty, 2)

	t.Run("данные перенесены и читаются", func(t *testing.T) {
		for _, table := range tables {
			if got := countZoneRows(t, pool, table, big); got != wantRows[table] {
				t.Errorf("%s: строк зоны стало %d, было %d (потери при переносе)", table, got, wantRows[table])
			}
		}

		// Точечный запрос по уникальному ключу (ТЗ 5.11) и апдейт статуса —
		// строка доступна и изменяема через корневую таблицу.
		var status int
		if err := pool.QueryRow(ctx,
			`SELECT status FROM domains WHERE zone_id = $1 AND label = 'site42'`, big,
		).Scan(&status); err != nil {
			t.Fatalf("точечный запрос по (zone_id, label): %v", err)
		}
		if status != 0 {
			t.Errorf("status = %d, want 0 (default)", status)
		}
		if _, err := pool.Exec(ctx,
			`UPDATE domains SET status = 4, check_state = 2 WHERE zone_id = $1 AND label = 'site1'`, big,
		); err != nil {
			t.Fatalf("update строки зоны: %v", err)
		}
		if err := pool.QueryRow(ctx,
			`SELECT status FROM domains WHERE zone_id = $1 AND label = 'site1'`, big,
		).Scan(&status); err != nil {
			t.Fatalf("перечитывание после update: %v", err)
		}
		if status != 4 {
			t.Errorf("status после update = %d, want 4", status)
		}
	})

	t.Run("структура партиций зоны", func(t *testing.T) {
		// domains — обычный лист с fillfactor и всеми тремя индексами
		// родителя (pkey, zone_id+label, pending).
		dom := zonePartition(t, pool, "domains", big)
		if dom.relkind != "r" {
			t.Errorf("domains_z%d: relkind = %s, want r (лист: субпартиционирование domains невозможно)", big, dom.relkind)
		}
		if dom.bound != fmt.Sprintf("FOR VALUES IN ('%d')", big) {
			t.Errorf("domains_z%d: bound = %q", big, dom.bound)
		}
		if !strings.Contains(dom.reloptions, "fillfactor=85") {
			t.Errorf("domains_z%d: reloptions = %q, want fillfactor=85", big, dom.reloptions)
		}
		if n := len(tableIndexes(t, pool, "domains")); dom.indexCount != n {
			t.Errorf("domains_z%d: индексов %d, want %d (дочерние всех индексов родителя)", big, dom.indexCount, n)
		}

		// Пустая зона выделена по тому же пути, что и нагруженная.
		if e := zonePartition(t, pool, "domains", empty); !strings.Contains(e.reloptions, "fillfactor=85") {
			t.Errorf("domains_z%d (пустая зона): reloptions = %q, want fillfactor=85", empty, e.reloptions)
		}

		// Таблицы результатов: hash-листья с fillfactor у subdomain_results,
		// без fillfactor у laravel_*, у каждого — дочерний индекс PK.
		// Каталог не гарантирует порядок частей — лист ищется по имени _h<i>.
		for _, table := range []string{"subdomain_results", "laravel_domains", "laravel_findings"} {
			parentIdx := len(tableIndexes(t, pool, table))
			byName := make(map[string]tablePartition)
			for _, leaf := range zoneLeafs(t, pool, table, big) {
				byName[leaf.name] = leaf
			}
			if len(byName) != 4 {
				t.Fatalf("%s: hash-листьев %d, want 4", table, len(byName))
			}
			for i := range 4 {
				leaf, ok := byName[fmt.Sprintf("%s_z%d_h%d", table, big, i)]
				if !ok {
					t.Errorf("%s: лист h%d не найден", table, i)
					continue
				}
				if leaf.relkind != "r" {
					t.Errorf("%s: relkind = %s, want r", leaf.name, leaf.relkind)
				}
				if want := fmt.Sprintf("FOR VALUES WITH (modulus 4, remainder %d)", i); leaf.bound != want {
					t.Errorf("%s: bound = %q, want %q", leaf.name, leaf.bound, want)
				}
				if leaf.indexCount != parentIdx {
					t.Errorf("%s: индексов %d, want %d (дочерние всех индексов родителя)", leaf.name, leaf.indexCount, parentIdx)
				}
				if table == "subdomain_results" && !strings.Contains(leaf.reloptions, "fillfactor=85") {
					t.Errorf("%s: reloptions = %q, want fillfactor=85 (частые перезаписи, ТЗ 6.3)", leaf.name, leaf.reloptions)
				}
				if table != "subdomain_results" && strings.Contains(leaf.reloptions, "fillfactor") {
					t.Errorf("%s: reloptions = %q, laravel_* без fillfactor", leaf.name, leaf.reloptions)
				}
			}
		}

		// Пустая зона выделена с другим modulus — число частей задаётся вызовом.
		if leafs := zoneLeafs(t, pool, "subdomain_results", empty); len(leafs) != 2 {
			t.Errorf("subdomain_results_z%d: hash-листьев %d, want 2", empty, len(leafs))
		}
	})

	t.Run("хэш распределяет строки по листьям", func(t *testing.T) {
		rows, err := pool.Query(ctx, `
			SELECT tableoid::regclass::text, count(*)
			FROM subdomain_results WHERE zone_id = $1 GROUP BY 1`, big)
		if err != nil {
			t.Fatalf("группировка по листьям: %v", err)
		}
		defer rows.Close()

		pattern := regexp.MustCompile(fmt.Sprintf(`^subdomain_results_z%d_h\d+$`, big))
		leafs := 0
		for rows.Next() {
			var name string
			var n int
			if err := rows.Scan(&name, &n); err != nil {
				t.Fatalf("чтение группы листа: %v", err)
			}
			if !pattern.MatchString(name) {
				t.Errorf("строки зоны big в неожиданной партиции %s", name)
			}
			if n <= 0 {
				t.Errorf("лист %s пуст в группировке", name)
			}
			leafs++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("итерация групп листьев: %v", err)
		}
		if leafs != 4 {
			t.Errorf("непустых hash-листьев %d, want 4 (40 строк распределены по всем частям)", leafs)
		}
	})

	t.Run("DEFAULT вернулась и принимает прочие зоны", func(t *testing.T) {
		// Строки невыделенной зоны остались в DEFAULT.
		for table, want := range map[string]int{"domains": 5, "subdomain_results": 3} {
			var got int
			err := pool.QueryRow(ctx,
				`SELECT count(*) FROM `+table+` WHERE zone_id = $1 AND tableoid = $2::regclass`,
				small, table+"_default",
			).Scan(&got)
			if err != nil {
				t.Fatalf("count %s в DEFAULT: %v", table, err)
			}
			if got != want {
				t.Errorf("%s: строк зоны small в DEFAULT = %d, want %d", table, got, want)
			}
		}

		// Новые строки: зоны big — в партицию зоны, прочих — в DEFAULT.
		var leaf string
		if err := pool.QueryRow(ctx,
			`INSERT INTO domains (zone_id, label) VALUES ($1, 'post-extract') RETURNING tableoid::regclass::text`, big,
		).Scan(&leaf); err != nil {
			t.Fatalf("insert после выделения (big): %v", err)
		}
		if want := fmt.Sprintf("domains_z%d", big); leaf != want {
			t.Errorf("новый домен big попал в %s, want %s", leaf, want)
		}
		if err := pool.QueryRow(ctx,
			`INSERT INTO subdomain_results (zone_id, domain_id, subdomain_id, scheme, checked_at)
			 VALUES ($1, 999, 0, 2, now()) RETURNING tableoid::regclass::text`, big,
		).Scan(&leaf); err != nil {
			t.Fatalf("insert результата после выделения: %v", err)
		}
		if !regexp.MustCompile(fmt.Sprintf(`^subdomain_results_z%d_h\d+$`, big)).MatchString(leaf) {
			t.Errorf("новый результат big попал в %s, want hash-лист", leaf)
		}
		if err := pool.QueryRow(ctx,
			`INSERT INTO domains (zone_id, label) VALUES ($1, 'new-small') RETURNING tableoid::regclass::text`, small,
		).Scan(&leaf); err != nil {
			t.Fatalf("insert после выделения (small): %v", err)
		}
		if leaf != "domains_default" {
			t.Errorf("новый домен small попал в %s, want domains_default", leaf)
		}

		// Цикл detach/attach не дублирует индексы на DEFAULT.
		// domains: PK + unique(zone_id,label) + domains_pending_idx +
		// 2 частичных миграции 000008 (T-104: lease/recheck) +
		// domains_deleted_idx (T-170) + domains_last_checked_idx (T-171) —
		// партиция-наследник родительских индексов.
		for table, want := range map[string]int{"domains": 7, "subdomain_results": 2} {
			for _, p := range tablePartitions(t, pool, table) {
				if p.name != table+"_default" {
					continue
				}
				if p.indexCount != want {
					t.Errorf("%s_default: индексов %d, want %d (detach/attach без дублей)", table, p.indexCount, want)
				}
			}
		}
	})

	t.Run("повторный вызов идемпотентен", func(t *testing.T) {
		// Базовые количества — после предыдущих вставок маршрутизации.
		before := make(map[string]int, len(tables))
		for _, table := range tables {
			before[table] = countZoneRows(t, pool, table, big)
		}

		callCreateZonePartitions(t, pool, big, 4)  // те же параметры
		callCreateZonePartitions(t, pool, big, 16) // и с другим числом частей: существующая партиция выигрывает

		if leafs := zoneLeafs(t, pool, "subdomain_results", big); len(leafs) != 4 {
			t.Errorf("после повторных вызовов hash-листьев %d, want 4 (партиция не пересоздаётся)", len(leafs))
		}
		for _, table := range tables {
			if got := countZoneRows(t, pool, table, big); got != before[table] {
				t.Errorf("%s: строк зоны после повторного вызова %d, want %d", table, got, before[table])
			}
		}
	})
}

// TestZonePartitions_Validation — процедура отклоняет неизвестную зону
// и не-степень двойки, не оставляя после себя партиций; вызов с одним
// аргументом использует hash_parts по умолчанию (4).
func TestZonePartitions_Validation(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	ok := insertZone(t, pool, "ok")

	expectError := func(name string, zoneID int16, hashParts int, wantSubstr string) {
		t.Helper()
		_, err := pool.Exec(ctx, `CALL create_zone_partitions($1, $2)`, zoneID, hashParts)
		if err == nil {
			t.Errorf("%s: ожидалась ошибка, CALL прошёл", name)
			return
		}
		if !strings.Contains(err.Error(), wantSubstr) {
			t.Errorf("%s: err = %v, want упоминание %q", name, err, wantSubstr)
		}
	}
	expectError("неизвестная зона", 999, 4, "нет в таблице zones")
	expectError("hash_parts = 3", ok, 3, "степень двойки")
	expectError("hash_parts = 0", ok, 0, "степень двойки")
	expectError("hash_parts = -4", ok, -4, "степень двойки")

	// Ошибочные вызовы не создали партиций зоны.
	if parts := zoneLeafs(t, pool, "subdomain_results", ok); len(parts) != 0 {
		t.Errorf("после ошибочных вызовов создано hash-листьев: %d, want 0", len(parts))
	}

	// Вызов с одним аргументом — hash_parts по умолчанию (4).
	if _, err := pool.Exec(ctx, `CALL create_zone_partitions($1)`, ok); err != nil {
		t.Fatalf("CALL с hash_parts по умолчанию: %v", err)
	}
	if leafs := zoneLeafs(t, pool, "subdomain_results", ok); len(leafs) != 4 {
		t.Errorf("hash-листьев по умолчанию %d, want 4", len(leafs))
	}
}

// TestZonePartitions_DownMigration — откат миграции 000003 (down к версии 2
// тем же golang-migrate, что применяет раннер) удаляет процедуру, не трогая
// таблицы и данные. Версия указана явно: 000003 — не последняя миграция
// схемы (после неё идут миграции других задач, например 000004 ingest_stage),
// Steps(-1) откатывал бы чужую миграцию.
const zoneSubpartitioningVersion = 3

func TestZonePartitions_DownMigration(t *testing.T) {
	skipUnlessIntegration(t)

	dsn, cleanup := withTestSchema(t)
	t.Cleanup(cleanup)

	if err := migrate.Up(context.Background(), dsn); err != nil {
		t.Fatalf("применение миграций: %v", err)
	}

	// Откат до версии ниже 000003 — та же операция, которую выполнял бы
	// CLI migrate (internal/migrate сознательно применяет только up).
	db, err := sql.Open("pgx/v5", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close() //nolint:errcheck // тестовое соединение

	driver, err := pgxmigrate.WithInstance(db, &pgxmigrate.Config{})
	if err != nil {
		t.Fatalf("драйвер миграций: %v", err)
	}
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		t.Fatalf("источник миграций: %v", err)
	}
	m, err := golangmigrate.NewWithInstance("iofs", src, "postgres", driver)
	if err != nil {
		t.Fatalf("инициализация golang-migrate: %v", err)
	}
	defer m.Close() //nolint:errcheck // тестовое соединение — как db выше
	if err := m.Migrate(zoneSubpartitioningVersion - 1); err != nil {
		t.Fatalf("откат к версии %d: %v", zoneSubpartitioningVersion-1, err)
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if version, ok := schemaVersion(t, dsn); !ok || version != zoneSubpartitioningVersion-1 {
		t.Errorf("версия схемы после отката = %d (ok=%v), want %d", version, ok, zoneSubpartitioningVersion-1)
	}

	var procExists bool
	if err := pool.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT 1 FROM pg_proc p
			JOIN pg_namespace n ON n.oid = p.pronamespace
			WHERE n.nspname = current_schema() AND p.proname = 'create_zone_partitions')`,
	).Scan(&procExists); err != nil || procExists {
		t.Errorf("процедура create_zone_partitions после отката: exists=%v (err=%v), want false", procExists, err)
	}

	var domainsExists bool
	if err := pool.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = current_schema() AND table_name = 'domains')`,
	).Scan(&domainsExists); err != nil || !domainsExists {
		t.Errorf("таблица domains после отката миграции 000003: exists=%v (err=%v), want true", domainsExists, err)
	}
}

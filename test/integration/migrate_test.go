// Интеграционные тесты миграционного механизма (T-002): встроенный раннер
// internal/migrate — тот же код, что сервисы выполняют при старте, —
// применяется к чистой схеме, идемпотентен и безопасен при одновременном
// старте нескольких экземпляров (advisory-блокировка).
//
// Чистая схема изолируется через search_path: DSN получает параметр
// search_path=<временная схема>, и вся DDL выполняется в ней, не задевая
// рабочую схему crawler (аналог применения на пустой БД).
package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/migrate"
	"crawler/migrations"
)

// pgDSN возвращает DSN PostgreSQL из окружения (как у сервисов).
func pgDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("APP_PG_DSN")
	if dsn == "" {
		dsn = "postgres://crawler:dev-only-crawler@172.28.100.2:5432/crawler?sslmode=disable"
	}
	return dsn
}

// embeddedNames — отсортированный список файлов встроенного набора.
func embeddedNames(t *testing.T) []string {
	t.Helper()
	entries, err := migrations.FS.ReadDir(".")
	if err != nil {
		t.Fatalf("чтение встроенного набора: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

// latestEmbeddedVersion — максимальный номер версии во встроенном наборе.
func latestEmbeddedVersion(t *testing.T) uint {
	t.Helper()
	var max uint
	for _, name := range embeddedNames(t) {
		var v uint
		if _, err := fmt.Sscanf(name, "%d_", &v); err != nil {
			t.Fatalf("имя миграции %s: %v", name, err)
		}
		if v > max {
			max = v
		}
	}
	return max
}

// withTestSchema создаёт изолированную схему и возвращает DSN с search_path
// на неё плюс функцию очистки.
func withTestSchema(t *testing.T) (string, func()) {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, pgDSN(t))
	if err != nil {
		t.Fatalf("подключение к PostgreSQL: %v", err)
	}
	defer pool.Close()

	name := fmt.Sprintf("mig_test_%d", os.Getpid())
	if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+name+" CASCADE"); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+name); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	dsn := pgDSN(t) + "&search_path=" + name
	return dsn, func() {
		p, err := pgxpool.New(ctx, pgDSN(t))
		if err != nil {
			t.Errorf("подключение для очистки: %v", err)
			return
		}
		defer p.Close()
		if _, err := p.Exec(ctx, "DROP SCHEMA IF EXISTS "+name+" CASCADE"); err != nil {
			t.Errorf("drop schema после теста: %v", err)
		}
	}
}

// schemaVersion читает версию применённых миграций в схеме из DSN.
func schemaVersion(t *testing.T, dsn string) (uint, bool) {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("подключение: %v", err)
	}
	defer pool.Close()
	var v int
	err = pool.QueryRow(ctx, "SELECT version FROM schema_migrations").Scan(&v)
	if err != nil {
		return 0, false
	}
	return uint(v), true
}

// TestMigrate_FreshSchema применяет встроенный набор к чистой схеме —
// ровно то, что делает первый старт сервисов на пустой БД.
func TestMigrate_FreshSchema(t *testing.T) {
	skipUnlessIntegration(t)
	latest := latestEmbeddedVersion(t)
	if latest == 0 {
		t.Fatal("встроенный набор миграций пуст")
	}

	dsn, cleanup := withTestSchema(t)
	defer cleanup()

	if err := migrate.Up(context.Background(), dsn); err != nil {
		t.Fatalf("применение миграций к чистой схеме: %v", err)
	}

	version, ok := schemaVersion(t, dsn)
	if !ok {
		t.Fatal("schema_migrations не создана или пуста")
	}
	if version != latest {
		t.Errorf("версия схемы = %d, want %d (последняя встроенная)", version, latest)
	}

	// Ключевые таблицы эскиза DDL (ТЗ 6.1) появились в новой схеме.
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, table := range []string{"zones", "subdomains", "domains", "subdomain_results", "contents", "laravel_domains", "laravel_findings", "ingest_files"} {
		var ok bool
		if err := pool.QueryRow(context.Background(),
			"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = $1)",
			table,
		).Scan(&ok); err != nil || !ok {
			t.Errorf("таблица %s не создана (err=%v)", table, err)
		}
	}

	var subdomains int
	if err := pool.QueryRow(context.Background(), "SELECT count(*) FROM subdomains WHERE enabled").Scan(&subdomains); err != nil {
		t.Fatalf("subdomains: %v", err)
	}
	if subdomains != 6 {
		t.Errorf("включённых субдоменов = %d, want 6 (миграция 000001)", subdomains)
	}
}

// TestMigrate_AlreadyApplied — повторное применение на актуальной схеме
// («миграции уже применены») — не ошибка, версия не меняется.
func TestMigrate_AlreadyApplied(t *testing.T) {
	skipUnlessIntegration(t)

	dsn, cleanup := withTestSchema(t)
	defer cleanup()

	ctx := context.Background()
	if err := migrate.Up(ctx, dsn); err != nil {
		t.Fatalf("первое применение: %v", err)
	}
	before, _ := schemaVersion(t, dsn)

	if err := migrate.Up(ctx, dsn); err != nil {
		t.Fatalf("повторное применение: %v (ожидался успех как «нет изменений»)", err)
	}
	if after, _ := schemaVersion(t, dsn); after != before {
		t.Errorf("версия изменилась при повторном применении: %d → %d", before, after)
	}
}

// TestMigrate_ConcurrentStart — одновременный старт восьми экземпляров
// раннера (как сервисы в compose): advisory-блокировка сериализует
// применение, все экземпляры завершаются успешно.
func TestMigrate_ConcurrentStart(t *testing.T) {
	skipUnlessIntegration(t)

	dsn, cleanup := withTestSchema(t)
	defer cleanup()

	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = migrate.Up(context.Background(), dsn)
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("экземпляр %d: %v", i, err)
		}
	}

	if version, ok := schemaVersion(t, dsn); !ok || version != latestEmbeddedVersion(t) {
		t.Errorf("версия после гонки старта = %d (ok=%v), want %d", version, ok, latestEmbeddedVersion(t))
	}
}

// TestMigrate_EmbeddedSetMatchesRepo — встроенный в бинарник набор
// совпадает с актуальными файлами migrations/ (сборка не устарела).
func TestMigrate_EmbeddedSetMatchesRepo(t *testing.T) {
	skipUnlessIntegration(t)

	// Тесты выполняются из каталога пакета test/integration.
	onDisk, err := filepath.Glob("../../migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(onDisk))
	for _, p := range onDisk {
		names = append(names, filepath.Base(p))
	}
	sort.Strings(names)

	embedded := embeddedNames(t)
	if strings.Join(embedded, ",") != strings.Join(names, ",") {
		t.Errorf("встроенный набор отличается от migrations/ на диске:\n  embedded: %v\n  disk:     %v", embedded, names)
	}
}

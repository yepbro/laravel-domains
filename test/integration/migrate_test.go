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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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

// schemaSeq — счётчик схем внутри одного тест-бинаря: имя схемы
// уникально на каждый вызов withTestSchema, иначе второй env внутри
// одного теста (TestContent_Compaction) делал DROP SCHEMA первого, а
// t.Parallel был бы небезопасен.
var schemaSeq atomic.Uint64

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

	name := fmt.Sprintf("mig_test_%d_%d", os.Getpid(), schemaSeq.Add(1))
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

// TestMigrate_InterruptedMidApply — отмена контекста (сигнал завершения
// сервиса) во время применения длительной миграции (T-112): выполнявшийся
// SQL снимается на стороне PostgreSQL по метке раннера, недоведённая
// миграция откатывается целиком (файл — одна неявная транзакция простого
// протокола), версия возвращается к предыдущей — и повторный старт
// доводит применение, не оставляя dirty.
//
// Длительная миграция моделируется детерминированно: применению 000008
// (CREATE INDEX, снимается блокировкой записи) препятствует транзакция,
// держащая ROW EXCLUSIVE на domains, — CREATE INDEX ждёт её освобождения.
func TestMigrate_InterruptedMidApply(t *testing.T) {
	skipUnlessIntegration(t)

	dsn, cleanup := withTestSchema(t)
	defer cleanup()

	ctx := context.Background()
	if err := migrate.Up(ctx, dsn); err != nil {
		t.Fatalf("подготовка — полное применение: %v", err)
	}

	// Состояние «применено 7, предстоит 000008»: индексы 000008 снимаются
	// её down-миграцией, версия возвращается к 7 (эквивалент force 7).
	// Псевдо-откат обязан покрывать и миграции ПОСЛЕ 000008 (новые —
	// дополнять сюда их down-эффектами), иначе повторный Up споткнётся
	// в них: 000009 возвращает глобальный ключ ingest_files.sha256,
	// 000011 снимает таблицу laravel_history (000010 — CREATE OR REPLACE
	// функции, повторное применение безопасно без подготовки), 0013/0014
	// снимают частичный индекс deleted-доменов и индексы окон
	// мониторинга (T-170/T-171), 0015 — таблицу счётчика crawler_counters
	// вместе с колонкой updated_at из 0016 (T-178), 0016 — колонки
	// счётчиков зон.
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	for _, stmt := range []string{
		"DROP INDEX IF EXISTS subdomain_results_checked_idx",
		"DROP INDEX IF EXISTS domains_last_checked_idx",
		"DROP INDEX IF EXISTS domains_deleted_idx",
		"DROP INDEX IF EXISTS ingest_files_sha256_idx",
		"DROP INDEX IF EXISTS ingest_files_zone_id_action_sha256_key",
		"ALTER TABLE ingest_files ADD CONSTRAINT ingest_files_sha256_key UNIQUE (sha256)",
		"DROP INDEX IF EXISTS domains_recheck_idx",
		"DROP INDEX IF EXISTS domains_lease_idx",
		"DROP TABLE IF EXISTS laravel_history",
		"DROP TABLE IF EXISTS web_checks",
		"DROP TABLE IF EXISTS crawler_counters",
		"DROP INDEX IF EXISTS laravel_domains_updated_at_idx",
		"ALTER TABLE zones DROP COLUMN IF EXISTS domains_laravel",
		"ALTER TABLE zones DROP COLUMN IF EXISTS domains_pending",
		"ALTER TABLE zones DROP COLUMN IF EXISTS domains_queued",
		"TRUNCATE schema_migrations",
		"INSERT INTO schema_migrations (version, dirty) VALUES (7, false)",
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("подготовка версии 7 (%s): %v", stmt, err)
		}
	}

	// Блокировщик: держит ROW EXCLUSIVE на domains — CREATE INDEX из 000008
	// (нужен SHARE) висит до освобождения. Отдельное соединение без метки
	// раннера: проверяет заодно, что отмена не трогает чужие запросы.
	blocker, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("соединение блокировщика: %v", err)
	}
	defer func() { _ = blocker.Close(ctx) }()
	if _, err := blocker.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("BEGIN блокировщика: %v", err)
	}
	defer func() { _, _ = blocker.Exec(ctx, "ROLLBACK") }()
	if _, err := blocker.Exec(ctx, "LOCK TABLE domains IN ROW EXCLUSIVE MODE"); err != nil {
		t.Fatalf("LOCK TABLE блокировщика: %v", err)
	}

	upCtx, cancelUp := context.WithCancel(ctx)
	defer cancelUp()
	upDone := make(chan error, 1)
	go func() { upDone <- migrate.Up(upCtx, dsn) }()

	// Миграция 000008 начала применяться: версия отмечена 8 dirty
	// (golang-migrate ставит dirty до выполнения файла).
	waitVersionState(t, dsn, 8, true)

	cancelUp() // «сигнал завершения» во время применения
	select {
	case err := <-upDone:
		if !errors.Is(err, migrate.ErrInterrupted) {
			t.Fatalf("Up = %v, хочу migrate.ErrInterrupted", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Up не завершился после отмены — применяющаяся миграция не прервана")
	}

	// Недоведённая миграция откатана: версия 7 чистая, dirty нет,
	// индексы 000008 не созданы (неявная транзакция откатила CREATE INDEX).
	version, dirty := schemaVersionState(t, dsn)
	if version != 7 || dirty {
		t.Errorf("после прерывания: версия = %d dirty = %v, want 7 false (сброс к предыдущей)", version, dirty)
	}
	for _, idx := range []string{"domains_lease_idx", "domains_recheck_idx"} {
		if indexExists(t, pool, idx) {
			t.Errorf("индекс %s создан прерванной миграцией — файл не откатился целиком", idx)
		}
	}

	// Повторный старт доводит применение до конца: 000008 и всё, что за
	// ней в наборе (последняя версия набора, а не конкретный номер —
	// набор растёт с новыми миграциями).
	if _, err := blocker.Exec(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("освобождение блокировщика: %v", err)
	}
	if err := migrate.Up(ctx, dsn); err != nil {
		t.Fatalf("повторный старт: %v", err)
	}
	latest := int(latestEmbeddedVersion(t))
	if version, dirty := schemaVersionState(t, dsn); version != latest || dirty {
		t.Errorf("после повторного старта: версия = %d dirty = %v, want %d false", version, dirty, latest)
	}
	for _, idx := range []string{"domains_lease_idx", "domains_recheck_idx"} {
		if !indexExists(t, pool, idx) {
			t.Errorf("индекс %s не создан повторным применением", idx)
		}
	}
}

// waitVersionState ждёт, пока schema_migrations примет указанное состояние
// (применение миграции стартовало: версия помечена dirty), опрашивая
// отдельным пулом — соединения раннера заняты применением.
func waitVersionState(t *testing.T, dsn string, wantVersion int, wantDirty bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("подключение для опроса версии: %v", err)
	}
	defer pool.Close()
	for {
		var version int
		var dirty bool
		err := pool.QueryRow(ctx, "SELECT version, dirty FROM schema_migrations").Scan(&version, &dirty)
		if err == nil && version == wantVersion && dirty == wantDirty {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("версия не достигла %d dirty=%v: последняя ошибка опроса %v", wantVersion, wantDirty, err)
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// schemaVersionState — текущая запись schema_migrations (версия и dirty).
func schemaVersionState(t *testing.T, dsn string) (int, bool) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("подключение: %v", err)
	}
	defer pool.Close()
	var version int
	var dirty bool
	if err := pool.QueryRow(context.Background(),
		"SELECT version, dirty FROM schema_migrations").Scan(&version, &dirty); err != nil {
		t.Fatalf("чтение schema_migrations: %v", err)
	}
	return version, dirty
}

// indexExists — индекс существует в текущей схеме DSN.
func indexExists(t *testing.T, pool *pgxpool.Pool, name string) bool {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(context.Background(),
		"SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND indexname = $1)",
		name,
	).Scan(&exists); err != nil {
		t.Fatalf("чтение pg_indexes (%s): %v", name, err)
	}
	return exists
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

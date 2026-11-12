// Integration tests of the migration mechanism (T-002): the embedded
// runner internal/migrate — the same code the services run at startup —
// is applied to a clean schema, is idempotent and safe when several
// instances start at once (advisory lock).
//
// The clean schema is isolated via search_path: the DSN gets the
// search_path=<temporary schema> parameter, and all DDL runs there
// without touching the working crawler schema (an equivalent of
// applying on an empty DB).
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

// pgDSN returns the PostgreSQL DSN from the environment (as the services do).
func pgDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("APP_PG_DSN")
	if dsn == "" {
		dsn = "postgres://crawler:dev-only-crawler@172.28.100.2:5432/crawler?sslmode=disable"
	}
	return dsn
}

// embeddedNames — the sorted list of the embedded set files.
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

// latestEmbeddedVersion — the maximum version number in the embedded set.
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

// schemaSeq — a schema counter within one test binary: the schema name
// is unique per withTestSchema call, otherwise a second env inside one
// test (TestContent_Compaction) dropped the first schema, and
// t.Parallel would be unsafe.
var schemaSeq atomic.Uint64

// withTestSchema creates an isolated schema and returns a DSN with
// search_path pointing to it plus a cleanup function.
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

// schemaVersion reads the version of applied migrations in the schema from the DSN.
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

// TestMigrate_FreshSchema applies the embedded set to a clean schema —
// exactly what the first services start does on an empty DB.
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

	// Key tables of the DDL sketch (spec 6.1) appeared in the new schema.
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

// TestMigrate_AlreadyApplied — re-applying to an up-to-date schema
// ("migrations are already applied") is not an error, the version does
// not change.
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

// TestMigrate_ConcurrentStart — eight runner instances start at once
// (as the services in compose do): the advisory lock serializes the
// apply, all instances finish successfully.
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

// TestMigrate_InterruptedMidApply — a context cancellation (the service
// termination signal) during the apply of a long migration (T-112): the
// running SQL is cancelled on the PostgreSQL side by the runner tag,
// the incomplete migration is rolled back as a whole (a file is one
// implicit transaction of the simple protocol), the version returns to
// the previous one — and a repeated start finishes the apply without
// leaving dirty.
//
// The long migration is modeled deterministically: the apply of 000008
// (CREATE INDEX, cancelled by a record lock) is obstructed by a
// transaction holding ROW EXCLUSIVE on domains — CREATE INDEX waits
// for its release.
func TestMigrate_InterruptedMidApply(t *testing.T) {
	skipUnlessIntegration(t)

	dsn, cleanup := withTestSchema(t)
	defer cleanup()

	ctx := context.Background()
	if err := migrate.Up(ctx, dsn); err != nil {
		t.Fatalf("подготовка — полное применение: %v", err)
	}

	// The "7 applied, 000008 pending" state: the 000008 indexes are
	// dropped by its down migration, the version returns to 7 (an
	// equivalent of force 7). The pseudo-rollback must also cover the
	// migrations AFTER 000008 (new ones — append their down effects
	// here), otherwise a repeated Up would stumble on them: 000009
	// returns the global ingest_files.sha256 key, 000011 drops the
	// laravel_history table (000010 — CREATE OR REPLACE of functions,
	// re-applying is safe without preparation), 0013/0014 drop the
	// partial index of deleted domains and the monitoring window
	// indexes (T-170/T-171), 0015 — the crawler_counters counter table
	// together with the updated_at column from 0016 (T-178), 0016 —
	// the zone counter columns.
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
		// 000017 (T-210): the render_failed_at marker — the down effect,
		// so that a repeated Up applies the migration onto a clean column.
		"ALTER TABLE subdomain_results DROP COLUMN IF EXISTS render_failed_at",
		// 000018 (T-259) needs no separate entry: its CHECK
		// subdomain_results_rendered_check dies together with the
		// render_failed_at column (the DROP COLUMN cascade) — the effect
		// is already removed by the line above. 000019 (T-265): the
		// refcount CHECK — the down effect, so that a repeated Up applies
		// the constraint onto a clean table (otherwise ADD CONSTRAINT
		// fails with 42710 duplicate_object).
		"ALTER TABLE contents DROP CONSTRAINT IF EXISTS contents_refcount_check",
		// 000020 (T-274): the persistent drain cursor table — the down
		// effect, otherwise a repeated Up fails with 42P07 already exists.
		"DROP TABLE IF EXISTS retention_drain_cursor",
		"TRUNCATE schema_migrations",
		"INSERT INTO schema_migrations (version, dirty) VALUES (7, false)",
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("подготовка версии 7 (%s): %v", stmt, err)
		}
	}

	// The blocker: holds ROW EXCLUSIVE on domains — the 000008
	// CREATE INDEX (needs SHARE) hangs until release. A separate
	// connection without the runner tag: also checks that the
	// cancellation does not touch foreign queries.
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

	// Migration 000008 started applying: the version is marked 8 dirty
	// (golang-migrate sets dirty before running the file).
	waitVersionState(t, dsn, 8, true)

	cancelUp() // the "termination signal" during the apply
	select {
	case err := <-upDone:
		if !errors.Is(err, migrate.ErrInterrupted) {
			t.Fatalf("Up = %v, хочу migrate.ErrInterrupted", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Up не завершился после отмены — применяющаяся миграция не прервана")
	}

	// The incomplete migration is rolled back: version 7 is clean, no
	// dirty, the 000008 indexes are not created (the implicit
	// transaction rolled back the CREATE INDEX).
	version, dirty := schemaVersionState(t, dsn)
	if version != 7 || dirty {
		t.Errorf("после прерывания: версия = %d dirty = %v, want 7 false (сброс к предыдущей)", version, dirty)
	}
	for _, idx := range []string{"domains_lease_idx", "domains_recheck_idx"} {
		if indexExists(t, pool, idx) {
			t.Errorf("индекс %s создан прерванной миграцией — файл не откатился целиком", idx)
		}
	}

	// A repeated start carries the apply to the end: 000008 and
	// everything after it in the set (the latest version of the set,
	// not a specific number — the set grows with new migrations).
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

// waitVersionState waits until schema_migrations reaches the given state
// (the migration apply has started: the version is marked dirty),
// polling with a separate pool — the runner connections are busy
// applying.
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

// schemaVersionState — the current schema_migrations record (version and dirty).
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

// indexExists — the index exists in the current schema of the DSN.
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

// TestMigrate_EmbeddedSetMatchesRepo — the set embedded in the binary
// matches the current migrations/ files (the build is not stale).
func TestMigrate_EmbeddedSetMatchesRepo(t *testing.T) {
	skipUnlessIntegration(t)

	// Tests run from the test/integration package directory.
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

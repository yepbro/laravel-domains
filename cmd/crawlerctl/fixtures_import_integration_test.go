// Интеграционный тест наката фикстуры (T-243) против живого PostgreSQL
// дев-окружения: критерий задачи — повторный накат идемпотентен — на
// реальном SQL (юнит-тесты проверяют контракты текстом на фейковом пуле,
// ревью T-243 P2-3). Изоляция — одноразовая схема с миграциями в БД
// дев-стека (образец withTestSchema интеграционных тестов; сам тест живёт
// в пакете crawlerctl — вызвает внутренние функции команды напрямую).
// Запуск: make test-integration (CRAWLER_INTEGRATION=1, БД — APP_PG_DSN
// tools-контейнера); без переменной тест пропускается (make test его
// не замечает).
package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/migrate"
)

// t243Schema — изолированная схема с миграциями в БД дев-стека.
func t243Schema(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	base := os.Getenv("APP_PG_DSN")
	if base == "" {
		base = os.Getenv("CRAWLER_POSTGRES_DSN")
	}
	if base == "" {
		t.Fatal("нет DSN PostgreSQL (APP_PG_DSN/CRAWLER_POSTGRES_DSN) — тест требует дев-стек")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, base)
	if err != nil {
		t.Fatalf("подключение: %v", err)
	}
	name := fmt.Sprintf("t243_%d", os.Getpid())
	if _, err := admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+name+" CASCADE"); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+name); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	admin.Close()
	cleanup := func() {
		c, err := pgxpool.New(ctx, base)
		if err != nil {
			t.Errorf("подключение очистки: %v", err)
			return
		}
		_, _ = c.Exec(ctx, "DROP SCHEMA IF EXISTS "+name+" CASCADE")
		c.Close()
	}
	dsn := base + dsnSep(base) + "search_path=" + name
	if err := migrate.Up(ctx, dsn); err != nil {
		cleanup()
		t.Fatalf("миграции в изолированной схеме: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		cleanup()
		t.Fatalf("пул тестовой схемы: %v", err)
	}
	return pool, func() { pool.Close(); cleanup() }
}

// dsnSep — разделитель для дописывания параметра в DSN ("?" нет — "?",
// уже есть — "&").
func dsnSep(dsn string) string {
	for i := 0; i < len(dsn); i++ {
		if dsn[i] == '?' {
			return "&"
		}
	}
	return "?"
}

// TestFixturesImportIdempotentLive — накат дампа v1 на живую схему:
// повторный накат не размножает строки (дубли ключей нет), состояние
// дампа восстанавливается (content_hash сбрасывается после имитации
// прогона воркера), последовательности выше максимума дампа, осиротевший
// контент прогона выметается повторным накатом.
func TestFixturesImportIdempotentLive(t *testing.T) {
	if os.Getenv("CRAWLER_INTEGRATION") != "1" {
		t.Skip("интеграционный тест наката: запустите make test-integration")
	}
	ctx := context.Background()
	pool, cleanup := t243Schema(t)
	defer cleanup()

	// Дамп: строка с контентной ссылкой (ok) и без (heavy NULL-колонки).
	ts := time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)
	url := "https://fixture.example/"
	dump := &fixturesDump{
		Manifest: fixturesManifest{FormatVersion: fixturesFormatVersion},
		Zones:    []fixturesZone{{ID: 9, Name: "fixture.test", IsActive: true, DomainsTotal: 2, DomainsChecked: 1}},
		Domains:  []fixturesDomain{{ZoneID: 9, ID: 77, Label: "spa", Status: 2, CheckState: 2, DNSMask: 1}},
		Results: []fixturesResult{
			{ZoneID: 9, DomainID: 77, SubdomainID: 0, Variant: 0, Scheme: 2, FinalURL: &url,
				Rendered: true, ContentHash: []byte{1, 2, 3}, CheckedAt: ts, Stratum: "ok"},
			{ZoneID: 9, DomainID: 77, SubdomainID: 1, Variant: 1, Scheme: 1, FinalURL: &url,
				CheckedAt: ts, Stratum: "heavy"},
		},
		Smoke: []fixturesSmokeDomain{{ZoneID: 9, DomainID: 77, Name: "spa.fixture.test"}},
	}

	count := func(sql string) int64 {
		t.Helper()
		var n int64
		if err := pool.QueryRow(ctx, sql).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}

	// Накат №1: строки на месте, контентные ссылки сброшены.
	if _, err := fixturesImportRun(ctx, pool, dump, os.Stdout); err != nil {
		t.Fatalf("накат 1: %v", err)
	}
	if n := count("SELECT count(*) FROM subdomain_results"); n != 2 {
		t.Fatalf("строк после наката 1: %d, хочу 2", n)
	}
	if n := count("SELECT count(*) FROM subdomain_results WHERE content_hash IS NOT NULL"); n != 0 {
		t.Fatalf("контентные ссылки не сброшены: %d", n)
	}

	// Имитация прогона воркера: DOM записан (contents + refcount), строка
	// переведена в rendered со ссылкой (worker.go: Put → renderUpdateSQL).
	someHash := []byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}
	if _, err := pool.Exec(ctx,
		"INSERT INTO contents (hash, pack_id, off, len, raw_len, refcount) VALUES ($1, 0, 0, 1, 1, 1)", someHash); err != nil {
		t.Fatalf("сид контента: %v", err)
	}
	if _, err := pool.Exec(ctx,
		"UPDATE subdomain_results SET rendered = true, content_hash = $1 WHERE subdomain_id = 0", someHash); err != nil {
		t.Fatalf("имитация рендера: %v", err)
	}

	// Накат №2 (сброс к состоянию дампа): дубликатов нет, ссылка сброшена,
	// осиротевший контент прогона выметан (ревью P2-1).
	var out bytes.Buffer
	if _, err := fixturesImportRun(ctx, pool, dump, &out); err != nil {
		t.Fatalf("накат 2: %v", err)
	}
	if n := count("SELECT count(*) FROM subdomain_results"); n != 2 {
		t.Errorf("строк после наката 2: %d, хочу 2 (идемпотентность)", n)
	}
	dups := count("SELECT count(*) FROM (SELECT 1 FROM subdomain_results " +
		"GROUP BY zone_id, domain_id, subdomain_id, variant HAVING count(*) > 1) d")
	if dups != 0 {
		t.Errorf("дубли ключей после повторного наката: %d", dups)
	}
	if n := count("SELECT count(*) FROM subdomain_results WHERE content_hash IS NOT NULL"); n != 0 {
		t.Errorf("повторный накат не сбросил ссылку рендера: %d", n)
	}
	if n := count("SELECT count(*) FROM contents"); n != 0 {
		t.Errorf("осиротевший контент прогона не убран повторным накатом: %d", n)
	}

	// Последовательности: выше максимума дампа — новые строки не
	// сталкиваются с фикстурой (контракт T-242).
	var seq int64
	if err := pool.QueryRow(ctx, "SELECT last_value FROM domain_id_seq").Scan(&seq); err != nil || seq < 77 {
		t.Errorf("domain_id_seq last_value = %d (err %v), хочу ≥ 77", seq, err)
	}
	var probe int64
	if err := pool.QueryRow(ctx, "INSERT INTO domains (zone_id, label) VALUES (9, 'probe') RETURNING id").Scan(&probe); err != nil {
		t.Fatalf("вставка после setval: %v", err)
	}
	if probe <= 77 {
		t.Errorf("новый домен id %d ≤ максимума фикстуры 77", probe)
	}
}

// Integration test of the fixture import (T-243) against the live
// PostgreSQL of the dev environment: the task's criterion — a repeated
// import is idempotent — on real SQL (unit tests check the contracts by
// text on a fake pool, review T-243 P2-3). Isolation — a one-off schema
// with migrations in the dev-stack DB (the withTestSchema model of the
// integration tests; the test itself lives in the crawlerctl package —
// it calls the command's internal functions directly). Run: make
// test-integration (CRAWLER_INTEGRATION=1, the DB — the tools
// container's APP_PG_DSN); without the variable the test is skipped
// (make test does not notice it).
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

// t243Schema — an isolated schema with migrations in the dev-stack DB.
func t243Schema(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	base := os.Getenv("APP_PG_DSN")
	if base == "" {
		base = os.Getenv("CRAWLER_POSTGRES_DSN")
	}
	if base == "" {
		t.Fatal("no PostgreSQL DSN (APP_PG_DSN/CRAWLER_POSTGRES_DSN) — the test needs the dev stack")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, base)
	if err != nil {
		t.Fatalf("connecting: %v", err)
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
			t.Errorf("the cleanup connection: %v", err)
			return
		}
		_, _ = c.Exec(ctx, "DROP SCHEMA IF EXISTS "+name+" CASCADE")
		c.Close()
	}
	dsn := base + dsnSep(base) + "search_path=" + name
	if err := migrate.Up(ctx, dsn); err != nil {
		cleanup()
		t.Fatalf("migrations in an isolated schema: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		cleanup()
		t.Fatalf("the test schema pool: %v", err)
	}
	return pool, func() { pool.Close(); cleanup() }
}

// dsnSep — the separator for appending a parameter to the DSN (no "?" —
// "?", already present — "&").
func dsnSep(dsn string) string {
	for i := 0; i < len(dsn); i++ {
		if dsn[i] == '?' {
			return "&"
		}
	}
	return "?"
}

// TestFixturesImportIdempotentLive — importing a v1 dump into a live
// schema: a repeated import does not multiply rows (no duplicate keys),
// the dump state is restored (content_hash is reset after a worker run
// simulation), the sequences are above the dump's maximum, the run's
// orphaned content is swept by the repeated import.
func TestFixturesImportIdempotentLive(t *testing.T) {
	if os.Getenv("CRAWLER_INTEGRATION") != "1" {
		t.Skip("the import integration test: run make test-integration")
	}
	ctx := context.Background()
	pool, cleanup := t243Schema(t)
	defer cleanup()

	// Dump: a row with a content reference (ok) and one without (heavy
	// NULL columns).
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

	// Import #1: the rows are in place, the content references are reset.
	if _, err := fixturesImportRun(ctx, pool, dump, os.Stdout); err != nil {
		t.Fatalf("import 1: %v", err)
	}
	if n := count("SELECT count(*) FROM subdomain_results"); n != 2 {
		t.Fatalf("rows after import 1: %d, want 2", n)
	}
	if n := count("SELECT count(*) FROM subdomain_results WHERE content_hash IS NOT NULL"); n != 0 {
		t.Fatalf("the content links were not reset: %d", n)
	}

	// Worker run simulation: a DOM is written (contents + refcount), the
	// row is set to rendered with a reference (worker.go: Put →
	// renderUpdateSQL).
	someHash := []byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9}
	if _, err := pool.Exec(ctx,
		"INSERT INTO contents (hash, pack_id, off, len, raw_len, refcount) VALUES ($1, 0, 0, 1, 1, 1)", someHash); err != nil {
		t.Fatalf("the content seed: %v", err)
	}
	if _, err := pool.Exec(ctx,
		"UPDATE subdomain_results SET rendered = true, content_hash = $1 WHERE subdomain_id = 0", someHash); err != nil {
		t.Fatalf("the render simulation: %v", err)
	}

	// Import #2 (a reset to the dump state): no duplicates, the reference
	// is reset, the run's orphaned content is swept (review P2-1).
	var out bytes.Buffer
	if _, err := fixturesImportRun(ctx, pool, dump, &out); err != nil {
		t.Fatalf("import 2: %v", err)
	}
	if n := count("SELECT count(*) FROM subdomain_results"); n != 2 {
		t.Errorf("rows after import 2: %d, want 2 (idempotency)", n)
	}
	dups := count("SELECT count(*) FROM (SELECT 1 FROM subdomain_results " +
		"GROUP BY zone_id, domain_id, subdomain_id, variant HAVING count(*) > 1) d")
	if dups != 0 {
		t.Errorf("duplicate keys after the repeated import: %d", dups)
	}
	if n := count("SELECT count(*) FROM subdomain_results WHERE content_hash IS NOT NULL"); n != 0 {
		t.Errorf("the repeated import did not reset the render reference: %d", n)
	}
	if n := count("SELECT count(*) FROM contents"); n != 0 {
		t.Errorf("the run's orphaned content was not swept by the repeated import: %d", n)
	}

	// Sequences: above the dump's maximum — new rows do not collide with
	// the fixture (the T-242 contract).
	var seq int64
	if err := pool.QueryRow(ctx, "SELECT last_value FROM domain_id_seq").Scan(&seq); err != nil || seq < 77 {
		t.Errorf("domain_id_seq last_value = %d (err %v), want ≥ 77", seq, err)
	}
	var probe int64
	if err := pool.QueryRow(ctx, "INSERT INTO domains (zone_id, label) VALUES (9, 'probe') RETURNING id").Scan(&probe); err != nil {
		t.Fatalf("an insert after setval: %v", err)
	}
	if probe <= 77 {
		t.Errorf("the new domain id %d ≤ the fixture maximum 77", probe)
	}
}

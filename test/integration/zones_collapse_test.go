// Integration tests of collapsing 2LD zones into the TLD (T-196, the
// tld-only-zones spec §9): on a seeded schema with migrations — the
// dry-run changes nothing and prints the plan, the check_state = 1
// precondition refuses (and --force bypasses it), --apply moves all
// six tables (domains with the label re-attachment, subdomain_results,
// laravel_domains, laravel_findings, laravel_history, ingest_files
// with the duplicate conflict), drops the 2LD partitions (a zone with
// partitions and a zone in DEFAULT), removes the zone rows, the
// reconciliation converges the counters, the §7.5 post-checks are
// green; a repeated run is a no-op; an interrupted transfer (a failure
// after N batches) resumes. The allowlist zone is not touched. Run —
// make test-integration.
package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/collapse"
	"crawler/internal/migrate"
)

// collapseSeedZone — a reference-data zone; returns the id.
func collapseSeedZone(t *testing.T, pool *pgxpool.Pool, name string) int16 {
	t.Helper()
	var id int16
	if err := pool.QueryRow(context.Background(),
		`INSERT INTO zones (name) VALUES ($1) RETURNING id`, name,
	).Scan(&id); err != nil {
		t.Fatalf("зона %s: %v", name, err)
	}
	return id
}

// collapseSeedDomains — the domain rows of a zone with prefix<i>
// labels; returns the ids in insertion order (the ids come from the
// domain_id_seq sequence). The first queued domains — check_state = 1
// (the precondition material).
func collapseSeedDomains(t *testing.T, pool *pgxpool.Pool, zone int16, prefix string, n, queued int) []int64 {
	t.Helper()
	ids := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		state := 0
		if i < queued {
			state = 1
		}
		var id int64
		if err := pool.QueryRow(context.Background(),
			`INSERT INTO domains (zone_id, label, check_state) VALUES ($1, $2, $3) RETURNING id`,
			zone, fmt.Sprintf("%s%d", prefix, i), state,
		).Scan(&id); err != nil {
			t.Fatalf("домен %s%d: %v", prefix, i, err)
		}
		ids = append(ids, id)
	}
	return ids
}

// collapseSeedChildren — a results/laravel-domain/finding row and a
// history row per domain: the move of all tables is checked by the
// actual zone_id re-pointing.
func collapseSeedChildren(t *testing.T, pool *pgxpool.Pool, zone int16, ids []int64) {
	t.Helper()
	ctx := context.Background()
	for _, id := range ids {
		if _, err := pool.Exec(ctx,
			`INSERT INTO subdomain_results (zone_id, domain_id, subdomain_id, scheme, checked_at)
			 VALUES ($1, $2, 0, 2, now())`, zone, id); err != nil {
			t.Fatalf("subdomain_results %d: %v", id, err)
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO laravel_domains (zone_id, domain_id, score, found_on_mask, rules_version, updated_at)
			 VALUES ($1, $2, 0.75, 1, 1, now())`, zone, id); err != nil {
			t.Fatalf("laravel_domains %d: %v", id, err)
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO laravel_findings (zone_id, domain_id, subdomain_id, score, signals, is_laravel, rules_version)
			 VALUES ($1, $2, 0, 0.8, '[{"rule":"x","weight":0.5}]', true, 1)`, zone, id); err != nil {
			t.Fatalf("laravel_findings %d: %v", id, err)
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO laravel_history (zone_id, domain_id, appeared) VALUES ($1, $2, true)`, zone, id); err != nil {
			t.Fatalf("laravel_history %d: %v", id, err)
		}
	}
}

// collapseCount — a scalar counter for the checks.
func collapseCount(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("счётчик %q %v: %v", query, args, err)
	}
	return n
}

// collapseZoneID — the id of a zone by name; found = false — no zone.
func collapseZoneID(t *testing.T, pool *pgxpool.Pool, name string) (int16, bool) {
	t.Helper()
	var id int16
	err := pool.QueryRow(context.Background(), `SELECT id FROM zones WHERE name = $1`, name).Scan(&id)
	if err == pgx.ErrNoRows {
		return 0, false
	}
	if err != nil {
		t.Fatalf("зона %s: %v", name, err)
	}
	return id, true
}

// collapseHasZonePartition — whether the table has an extracted zone
// partition (the "FOR VALUES IN (zid)" bound, two pg_get_expr
// spellings).
func collapseHasZonePartition(t *testing.T, pool *pgxpool.Pool, table string, zone int16) bool {
	t.Helper()
	for _, p := range tablePartitions(t, pool, table) {
		if p.bound == fmt.Sprintf("FOR VALUES IN (%d)", zone) ||
			p.bound == fmt.Sprintf("FOR VALUES IN ('%d')", zone) {
			return true
		}
	}
	return false
}

// collapseSnapshot — a data imprint for the "before/after" comparison:
// the domains, the zone reference data, the partition names, the
// history.
type collapseSnapshot struct {
	domains string
	zones   string
	parts   string
	history string
}

func collapseTakeSnapshot(t *testing.T, pool *pgxpool.Pool) collapseSnapshot {
	t.Helper()
	ctx := context.Background()
	var s collapseSnapshot

	rows, err := pool.Query(ctx, `SELECT zone_id, id, label, check_state FROM domains ORDER BY zone_id, id`)
	if err != nil {
		t.Fatalf("слепок доменов: %v", err)
	}
	for rows.Next() {
		var z, state int16
		var id int64
		var label string
		if err := rows.Scan(&z, &id, &label, &state); err != nil {
			rows.Close()
			t.Fatalf("слепок доменов: чтение: %v", err)
		}
		s.domains += fmt.Sprintf("[%d %d %s %d]", z, id, label, state)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("слепок доменов: %v", err)
	}

	zrows, err := pool.Query(ctx, `SELECT id, name FROM zones ORDER BY name`)
	if err != nil {
		t.Fatalf("слепок зон: %v", err)
	}
	for zrows.Next() {
		var id int16
		var name string
		if err := zrows.Scan(&id, &name); err != nil {
			zrows.Close()
			t.Fatalf("слепок зон: чтение: %v", err)
		}
		s.zones += fmt.Sprintf("[%d %s]", id, name)
	}
	zrows.Close()
	if err := zrows.Err(); err != nil {
		t.Fatalf("слепок зон: %v", err)
	}

	for _, table := range []string{"domains", "subdomain_results", "laravel_domains", "laravel_findings"} {
		names := make([]string, 0, 4)
		for _, p := range tablePartitions(t, pool, table) {
			names = append(names, p.name)
		}
		s.parts += table + ":" + strings.Join(names, ",") + ";"
	}

	s.history = fmt.Sprintf("%d", collapseCount(t, pool, `SELECT count(*) FROM laravel_history`))
	return s
}

// collapseSeed — the scenario's typical seed: a TLD in (with a native
// domain), the 2LD 5g.in (5 domains, extracted partitions, files with
// a duplicate conflict), the 2LD co.uk without partitions (rows in
// DEFAULT), the allowlist zone crawl.test.
func collapseSeed(t *testing.T, pool *pgxpool.Pool) (in, twoLD, coUK, allow int16) {
	t.Helper()
	ctx := context.Background()

	in = collapseSeedZone(t, pool, "in")
	collapseSeedDomains(t, pool, in, "native", 1, 0)

	twoLD = collapseSeedZone(t, pool, "5g.in")
	five := collapseSeedDomains(t, pool, twoLD, "site", 5, 0)
	collapseSeedChildren(t, pool, twoLD, five)

	coUK = collapseSeedZone(t, pool, "co.uk")
	uk := collapseSeedDomains(t, pool, coUK, "shop", 2, 0)
	collapseSeedChildren(t, pool, coUK, uk)

	allow = collapseSeedZone(t, pool, "crawl.test")
	allowIDs := collapseSeedDomains(t, pool, allow, "keep", 1, 0)
	collapseSeedChildren(t, pool, allow, allowIDs)

	// The delivery journal: a 5g.in file, a duplicate conflict (the
	// same content was already delivered to in — the owner row stays)
	// and a co.uk file.
	dup := []byte{0xAA, 0xBB}
	for i, f := range []struct {
		zone   int16
		action int16
		sha    []byte
	}{
		{twoLD, 1, []byte{0x01}},
		{twoLD, 1, dup},
		{in, 1, dup},
		{coUK, 2, []byte{0x02}},
	} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO ingest_files (zone_id, action, path, sha256, lines_total, processed_at)
			 VALUES ($1, $2, $3, $4, 10, now())`, f.zone, f.action, fmt.Sprintf("incoming/f%d", i), f.sha,
		); err != nil {
			t.Fatalf("ingest_files %d: %v", i, err)
		}
	}

	// The extracted partitions of the 5g.in zone (migration 000003):
	// the rows move out of DEFAULT — the transfer must drop the
	// partitions at the end.
	callCreateZonePartitions(t, pool, twoLD, 2)
	return in, twoLD, coUK, allow
}

// collapseAllowlist — the allowlist of the test seed.
var collapseAllowlist = []string{"crawl.test"}

// TestZonesCollapse — the full §9 scenario: a dry-run, the
// precondition, the apply (the move of the six tables, the partitions,
// the dedup, the counters, the §7.5 post-checks), a repeat — a no-op.
func TestZonesCollapse(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()
	in, twoLD, coUK, allow := collapseSeed(t, pool)

	t.Run("dry-run: план без изменений", func(t *testing.T) {
		before := collapseTakeSnapshot(t, pool)
		var out strings.Builder
		if err := collapse.Run(ctx, pool, collapse.Options{Allowlist: collapseAllowlist}, &out); err != nil {
			t.Fatalf("dry-run: %v", err)
		}
		got := out.String()
		for _, want := range []string{
			"зона", "TLD", "доменов", "результатов", "находок", "история", "файлов", "партиции",
			"5g.in", "co.uk", " in ", " uk ", "есть", "default",
			"итого: 2LD-зон 1, доменов 5 → зона in",
			"итого: 2LD-зон 1, доменов 2 → зона uk",
			"dry-run: данные не изменены",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("план dry-run не содержит %q:\n%s", want, got)
			}
		}
		if strings.Contains(got, "crawl.test") {
			t.Errorf("allowlist-зона попала в план:\n%s", got)
		}
		if after := collapseTakeSnapshot(t, pool); after != before {
			t.Error("dry-run изменил данные")
		}
	})

	t.Run("предусловие check_state = 1 отказывает", func(t *testing.T) {
		if _, err := pool.Exec(ctx,
			`UPDATE domains SET check_state = 1 WHERE zone_id = $1 AND label = 'site0'`, twoLD); err != nil {
			t.Fatalf("queued: %v", err)
		}
		before := collapseTakeSnapshot(t, pool)
		var out strings.Builder
		err := collapse.Run(ctx, pool, collapse.Options{Allowlist: collapseAllowlist, Apply: true}, &out)
		if err == nil || !strings.Contains(err.Error(), "check_state = 1") {
			t.Fatalf("ошибка = %v, хочу отказ предусловия", err)
		}
		if !strings.Contains(err.Error(), "5g.in") {
			t.Errorf("ошибка %v не перечисляет зону", err)
		}
		if !strings.Contains(out.String(), "5g.in — 1") {
			t.Errorf("отчёт не перечисляет домены в очереди:\n%s", out.String())
		}
		if after := collapseTakeSnapshot(t, pool); after != before {
			t.Error("отказ предусловия изменил данные")
		}

		// The operator triage: the queue is drained — a normal apply must
		// go through without --force.
		if _, err := pool.Exec(ctx,
			`UPDATE domains SET check_state = 0 WHERE zone_id = $1 AND label = 'site0'`, twoLD); err != nil {
			t.Fatalf("сброс очереди: %v", err)
		}
	})

	t.Run("apply переносит всё и пост-проверки зелёные", func(t *testing.T) {
		var out strings.Builder
		// Batch = 2: the five 5g.in domains go in several batches — the
		// window path (§7.3) is checked along the way on a live DB.
		if err := collapse.Run(ctx, pool, collapse.Options{Allowlist: collapseAllowlist, Apply: true, Batch: 2}, &out); err != nil {
			t.Fatalf("apply: %v", err)
		}
		got := out.String()

		// Zones: the 2LD ones removed, the TLD created, the allowlist
		// alive.
		if _, ok := collapseZoneID(t, pool, "5g.in"); ok {
			t.Error("строка зоны 5g.in не удалена")
		}
		if _, ok := collapseZoneID(t, pool, "co.uk"); ok {
			t.Error("строка зоны co.uk не удалена")
		}
		uk, hasUK := collapseZoneID(t, pool, "uk")
		if !hasUK {
			t.Error("зона uk не создана переносом")
		}
		if id, ok := collapseZoneID(t, pool, "crawl.test"); !ok || id != allow {
			t.Error("allowlist-зона crawl.test затронута")
		}

		// Domains: the TLD zone, the label with the prefix re-attached;
		// the in native and the allowlist domain are untouched.
		var n int64
		if n = collapseCount(t, pool,
			`SELECT count(*) FROM domains WHERE zone_id = $1 AND label LIKE 'site%.5g'`, in); n != 5 {
			t.Errorf("доменов site<N>.5g в зоне in: %d, хочу 5", n)
		}
		if n = collapseCount(t, pool,
			`SELECT count(*) FROM domains WHERE zone_id = $1 AND label LIKE 'shop%.co'`, uk); n != 2 {
			t.Errorf("доменов shop<N>.co в зоне uk: %d, хочу 2", n)
		}
		if n = collapseCount(t, pool, `SELECT count(*) FROM domains WHERE zone_id = $1 AND label = 'native0'`, in); n != 1 {
			t.Errorf("домен-абориген native0: %d, хочу 1 (не тронут)", n)
		}
		if n = collapseCount(t, pool, `SELECT count(*) FROM domains WHERE zone_id = $1 AND label = 'keep0'`, allow); n != 1 {
			t.Errorf("allowlist-домен keep0: %d, хочу 1 (не тронут)", n)
		}

		// The children and the history: re-pointed by zone_id (a join by
		// domain_id).
		for table, want := range map[string]int64{
			`SELECT count(*) FROM subdomain_results r JOIN domains d ON d.id = r.domain_id AND d.zone_id = r.zone_id
			 WHERE r.zone_id = $1`: 5,
			`SELECT count(*) FROM laravel_domains r JOIN domains d ON d.id = r.domain_id AND d.zone_id = r.zone_id
			 WHERE r.zone_id = $1`: 5,
			`SELECT count(*) FROM laravel_findings r JOIN domains d ON d.id = r.domain_id AND d.zone_id = r.zone_id
			 WHERE r.zone_id = $1`: 5,
		} {
			if n = collapseCount(t, pool, table, in); n != want {
				t.Errorf("дети зоны in (%s…): %d, хочу %d", table[:40], n, want)
			}
		}
		if n = collapseCount(t, pool,
			`SELECT count(*) FROM laravel_history WHERE zone_id = $1`, in); n != 5 {
			t.Errorf("laravel_history зоны in: %d, хочу 5", n)
		}

		// ingest_files: the 5g.in duplicate is removed (the owner in in
		// stayed), the others are re-bound to the TLD.
		if n = collapseCount(t, pool,
			`SELECT count(*) FROM ingest_files WHERE sha256 = $1`, []byte{0xAA, 0xBB}); n != 1 {
			t.Errorf("строк журнала с дублем содержимого: %d, хочу 1 (владелец в in)", n)
		}
		if n = collapseCount(t, pool,
			`SELECT count(*) FROM ingest_files WHERE zone_id IN ($1, $2)`, in, allow); n != 2 {
			t.Errorf("файлов в in/crawl.test: %d, хочу 2 (перепривязка без потерь)", n)
		}
		if n = collapseCount(t, pool,
			`SELECT count(*) FROM ingest_files WHERE zone_id = ANY($1)`, []int16{twoLD, coUK}); n != 0 {
			t.Errorf("файлов осталось в 2LD-зонах: %d, хочу 0", n)
		}

		// Partitions: the 2LD ones dropped (including the hash leaves),
		// the TLD extracted.
		for _, table := range []string{"domains", "subdomain_results", "laravel_domains", "laravel_findings"} {
			if collapseHasZonePartition(t, pool, table, twoLD) {
				t.Errorf("%s: партиция 5g.in не дропнута", table)
			}
			if collapseHasZonePartition(t, pool, table, coUK) {
				t.Errorf("%s: партиция co.uk не дропнута", table)
			}
			if !collapseHasZonePartition(t, pool, table, in) {
				t.Errorf("%s: партиция зоны in не создана (шаг 2)", table)
			}
		}
		for _, leaf := range zoneLeafs(t, pool, "subdomain_results", twoLD) {
			t.Errorf("hash-лист 2LD не дропнут: %s", leaf.name)
		}

		// The §7.5 post-checks — directly and from the command report.
		if n = collapseCount(t, pool,
			`SELECT count(*) FROM zones WHERE name LIKE '%.%' AND name <> ALL($1)`, collapseAllowlist); n != 0 {
			t.Errorf("2LD-зон вне allowlist: %d, хочу 0", n)
		}
		for _, table := range []string{"domains", "subdomain_results", "laravel_domains", "laravel_findings", "laravel_history"} {
			if n = collapseCount(t, pool,
				`SELECT count(*) FROM `+table+` x WHERE NOT EXISTS (SELECT 1 FROM zones z WHERE z.id = x.zone_id)`); n != 0 {
				t.Errorf("%s: сиротских zone_id %d, хочу 0", table, n)
			}
		}
		for _, want := range []string{
			"пост-проверки: 2LD-зон вне allowlist: 0",
			"сиротских zone_id: domains 0, subdomain_results 0, laravel_domains 0, laravel_findings 0, laravel_history 0",
			"счётчики зон = агрегат: расхождений нет",
			"реконсиляция счётчиков: перезаписано зон",
			"спот-чек site0.5g.in: зона in, label site0.5g — ок",
			"перенос завершён: 2LD-зон 2",
		} {
			if !strings.Contains(got, want) {
				t.Errorf("отчёт apply не содержит %q:\n%s", want, got)
			}
		}

		// The zone counters = the aggregate (the step 8 reconciliation):
		// in holds the native and the five transferred ones.
		var total int64
		if err := pool.QueryRow(ctx,
			`SELECT domains_total FROM zones WHERE id = $1`, in).Scan(&total); err != nil {
			t.Fatalf("zones.domains_total: %v", err)
		}
		if total != 6 {
			t.Errorf("zones.domains_total зоны in = %d, хочу 6 (реконсиляция)", total)
		}

		// The §7.5 spot check — by the SQL function itself: the T zone,
		// the label with the prefix.
		var zoneName, label string
		if err := pool.QueryRow(ctx,
			`SELECT di->'zone'->>'name', di->'domain'->>'label' FROM domain_info('site1.5g.in') di`,
		).Scan(&zoneName, &label); err != nil {
			t.Fatalf("domain_info(site1.5g.in): %v", err)
		}
		if zoneName != "in" || label != "site1.5g" {
			t.Errorf("domain_info: зона %q, label %q; хочу in / site1.5g", zoneName, label)
		}
	})

	t.Run("повторный apply — no-op", func(t *testing.T) {
		before := collapseTakeSnapshot(t, pool)
		var out strings.Builder
		if err := collapse.Run(ctx, pool, collapse.Options{Allowlist: collapseAllowlist, Apply: true, Batch: 2}, &out); err != nil {
			t.Fatalf("повторный apply: %v", err)
		}
		if got := out.String(); !strings.Contains(got, "2LD-зон вне allowlist нет — сворачивать нечего") {
			t.Errorf("повторный apply не no-op:\n%s", got)
		}
		if after := collapseTakeSnapshot(t, pool); after != before {
			t.Error("повторный apply изменил данные")
		}
	})
}

// TestZonesCollapse_Resume — resuming after a mid-way failure (§7.1):
// the zone transfer fails after the first committed batch (a connection
// drop simulation), a repeated Zone over the same state catches up
// without losses and duplicates; the outcome is identical to the
// single-pass transfer.
func TestZonesCollapse_Resume(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()
	twoLD := collapseSeedZone(t, pool, "5g.in")
	ids := collapseSeedDomains(t, pool, twoLD, "site", 5, 0)
	collapseSeedChildren(t, pool, twoLD, ids)
	// The extracted 2LD zone partitions: after the failure they must
	// stay in place (the transfer is not finished), the repeat drops
	// them.
	callCreateZonePartitions(t, pool, twoLD, 2)

	spec, err := collapse.SpecOf(collapse.ZoneRow{ID: twoLD, Name: "5g.in"})
	if err != nil {
		t.Fatal(err)
	}

	// A connection whose BEGIN fails after the first batch: the first
	// batch commits, the second breaks — a half-done transfer. Release
	// immediately via defer: an unclosed connection hangs the
	// pool.Close() of the test cleanup on any failure below.
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer conn.Release()
	flaky := &collapseFlakyConn{Conn: conn, allowBegin: 1}
	if _, err := collapse.Zone(ctx, flaky, spec, 2); err == nil {
		t.Fatal("имитация сбоя не сработала")
	}
	var in int16
	if err := pool.QueryRow(ctx, `SELECT id FROM zones WHERE name = 'in'`).Scan(&in); err != nil {
		t.Fatalf("зона in после сбоя: %v", err)
	}
	moved := collapseCount(t, pool, `SELECT count(*) FROM domains WHERE zone_id = $1`, in)
	left := collapseCount(t, pool, `SELECT count(*) FROM domains WHERE zone_id = $1`, twoLD)
	if moved != 2 || left != 3 {
		t.Fatalf("после сбоя: перенесено %d, осталось %d; хочу 2/3 (первый батч зафиксирован)", moved, left)
	}
	// The zone row and the partitions are in place — the transfer is
	// not finished.
	if _, ok := collapseZoneID(t, pool, "5g.in"); !ok {
		t.Fatal("строка зоны удалена до завершения переноса")
	}
	if !collapseHasZonePartition(t, pool, "domains", twoLD) {
		t.Fatal("партиции 2LD дропнуты до завершения переноса")
	}

	// A repeat by the same code (Run): the inventory sees the zone,
	// moving_ids is rebuilt over the remainder, the windows walk the
	// remaining ids.
	var out strings.Builder
	if err := collapse.Run(ctx, pool, collapse.Options{Apply: true, Batch: 2}, &out); err != nil {
		t.Fatalf("возобновление: %v", err)
	}

	if _, ok := collapseZoneID(t, pool, "5g.in"); ok {
		t.Error("строка зоны не удалена повтором")
	}
	if n := collapseCount(t, pool,
		`SELECT count(*) FROM domains WHERE zone_id = $1 AND label LIKE 'site%.5g'`, in); n != 5 {
		t.Errorf("доменов site<N>.5g в зоне in: %d, хочу 5 (без потерь и дублей)", n)
	}
	for _, table := range []string{"domains", "subdomain_results", "laravel_domains", "laravel_findings", "laravel_history"} {
		if n := collapseCount(t, pool,
			`SELECT count(*) FROM `+table+` WHERE zone_id = $1`, twoLD); n != 0 {
			t.Errorf("%s: строк 2LD-зоны осталось %d, хочу 0", table, n)
		}
	}
	if collapseHasZonePartition(t, pool, "domains", twoLD) {
		t.Error("партиции 2LD не дропнуты повтором")
	}
	if n := collapseCount(t, pool,
		`SELECT count(*) FROM domains d WHERE NOT EXISTS (SELECT 1 FROM zones z WHERE z.id = d.zone_id)`); n != 0 {
		t.Errorf("сиротских zone_id после возобновления: %d", n)
	}
}

// TestZonesCollapse_Force — the §7.1 precondition semantics in full: a
// dry-run warns about the queued domains but does not refuse; an apply
// without --force refuses before the transfer; an apply --force
// performs the transfer.
func TestZonesCollapse_Force(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()
	twoLD := collapseSeedZone(t, pool, "5g.in")
	ids := collapseSeedDomains(t, pool, twoLD, "site", 2, 2) // both queued
	collapseSeedChildren(t, pool, twoLD, ids)

	t.Run("dry-run предупреждает, но не отказывает", func(t *testing.T) {
		var out strings.Builder
		if err := collapse.Run(ctx, pool, collapse.Options{}, &out); err != nil {
			t.Fatalf("dry-run с очередью: %v", err)
		}
		got := out.String()
		for _, want := range []string{"5g.in — 2", "dry-run: данные не изменены"} {
			if !strings.Contains(got, want) {
				t.Errorf("dry-run не содержит %q:\n%s", want, got)
			}
		}
	})

	t.Run("apply без --force отказывает", func(t *testing.T) {
		before := collapseTakeSnapshot(t, pool)
		var out strings.Builder
		if err := collapse.Run(ctx, pool, collapse.Options{Apply: true}, &out); err == nil ||
			!strings.Contains(err.Error(), "check_state = 1") {
			t.Fatalf("ошибка = %v, хочу отказ предусловия", err)
		}
		if after := collapseTakeSnapshot(t, pool); after != before {
			t.Error("отказ предусловия изменил данные")
		}
	})

	t.Run("apply --force переносит", func(t *testing.T) {
		var out strings.Builder
		if err := collapse.Run(ctx, pool, collapse.Options{Apply: true, Force: true}, &out); err != nil {
			t.Fatalf("apply --force: %v", err)
		}
		if !strings.Contains(out.String(), "продолжение по --force") {
			t.Errorf("отчёт не отмечает обход предусловия:\n%s", out.String())
		}
		if _, ok := collapseZoneID(t, pool, "5g.in"); ok {
			t.Error("зона 5g.in не свернулась")
		}
		var in int16
		if err := pool.QueryRow(ctx, `SELECT id FROM zones WHERE name = 'in'`).Scan(&in); err != nil {
			t.Fatalf("зона in: %v", err)
		}
		if n := collapseCount(t, pool,
			`SELECT count(*) FROM domains WHERE zone_id = $1 AND label LIKE 'site%.5g'`, in); n != 2 {
			t.Errorf("доменов site<N>.5g в in: %d, хочу 2 (включая бывший queued)", n)
		}
	})
}

// collapseFlakyConn — a transfer connection with a BEGIN count limit:
// after allowBegin successful transactions BEGIN fails (a simulation
// of a mid-way command drop, §7.1 "resumable").
type collapseFlakyConn struct {
	*pgxpool.Conn
	allowBegin int
	begins     int
}

func (c *collapseFlakyConn) Begin(ctx context.Context) (pgx.Tx, error) {
	c.begins++
	if c.begins > c.allowBegin {
		return nil, fmt.Errorf("имитация сбоя: соединение потеряно после %d батчей", c.allowBegin)
	}
	return c.Conn.Begin(ctx)
}

// TestZonesCollapse_SmallPoolDeadlock — the §7.5 post-check deadlock
// regression (T-217, a P2 finding of the T-198 window, the 2026-10-29
// incident): crawlerctl zones collapse opens a pool of TWO connections
// (the spec 6.3 budget), the transfer connection is held until the end
// of Run (the moving_ids temp table, §7.3), and the post-checks read
// the first aggregate (CheckZonesLeftSQL) as a set closed only in
// defer PostChecks — the second query (CheckOrphansSQL) waited for a
// third connection forever: the server had executed CheckZonesLeftSQL
// (the session idle in ClientRead, the result unconsumed by the
// client), the transfer connection hung on DELETE FROM zones, the
// process stood waiting for the pool (futex). The test repeats the CLI
// contour (MaxConns = 2) and turns the eternal wait into a FAIL by
// deadline: Run must finish on its own within the allotted time
// (seconds after the fix), a hang — a failure.
func TestZonesCollapse_SmallPoolDeadlock(t *testing.T) {
	skipUnlessIntegration(t)
	dsn, cleanup := withTestSchema(t)
	t.Cleanup(cleanup)
	if err := migrate.Up(context.Background(), dsn); err != nil {
		t.Fatalf("миграции: %v", err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("разбор DSN: %v", err)
	}
	cfg.MaxConns = 2 // the CLI contour: crawlerctl zones collapse (PoolSize = 2)
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("пул: %v", err)
	}
	t.Cleanup(pool.Close)

	twoLD := collapseSeedZone(t, pool, "5g.in")
	ids := collapseSeedDomains(t, pool, twoLD, "site", 2, 0)
	collapseSeedChildren(t, pool, twoLD, ids)

	// Run in a goroutine: a hung run is detected by the deadline, not
	// the go test timeout; cancel unties the Acquire wait (a ctx
	// cancellation interrupts it) and returns the connections to the
	// cleanup pool.Close.
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	var out strings.Builder
	go func() {
		done <- collapse.Run(runCtx, pool, collapse.Options{Apply: true}, &out)
	}()

	const deadline = 30 * time.Second
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("collapse.Run (пул 2): %v", err)
		}
		if !strings.Contains(out.String(), "перенос завершён: 2LD-зон 1") {
			t.Errorf("отчёт apply неполон:\n%s", out.String())
		}
	case <-time.After(deadline):
		// Untie the run (Acquire feels the ctx cancellation) and WAIT for
		// the connections to come back before the cleanup pool.Close:
		// the Run defer runs movingDropSQL on the held connection with
		// the movingDropTimeout budget of 5 s (T-229) — if the hang ever
		// turns out to be not in Acquire, we wait not forever but a
		// grace (10 s covers the cleanup) and fail with an honest FAIL
		// (review T-217 P3-2).
		cancel()
		const grace = 10 * time.Second
		select {
		case <-done:
		case <-time.After(grace):
		}
		t.Fatalf("collapse.Run не завершился за %v (+%v на развязку отменой) — подвисание прохода, подозрение: дедлок пост-проверок §7.5 на пуле из 2 соединений (T-217)", deadline, grace)
	}
}

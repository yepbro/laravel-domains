// Интеграционные тесты сворачивания 2LD-зон в TLD (T-196, ТЗТ §9):
// на сидированной схеме с миграциями — dry-run ничего не меняет и
// печатает план, предусловие check_state = 1 отказывает (и --force
// обходит), --apply переносит все шесть таблиц (домены с доклейкой
// label, subdomain_results, laravel_domains, laravel_findings,
// laravel_history, ingest_files с конфликтом дубля), дропает партиции
// 2LD (зона с партициями и зона в DEFAULT), удаляет строки зон,
// реконсиляция сводит счётчики, пост-проверки §7.5 зелёные; повторный
// запуск — no-op; прерванный перенос (сбой после N батчей)
// продолжается. Зона allowlist не трогается. Прогон — make
// test-integration.
package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/collapse"
)

// collapseSeedZone — зона справочника; возвращает id.
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

// collapseSeedDomains — домены зоны с метками prefix<i>; возвращает id
// по порядку вставки (id выдаёт последовательность domain_id_seq).
// Первые queued доменов — check_state = 1 (материал предусловия).
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

// collapseSeedChildren — по строке результатов/laravel-домена/находки и
// истории на каждый домен: перенос всех таблиц проверяется по
// фактическому перебросу zone_id.
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

// collapseCount — скалярный счётчик для проверок.
func collapseCount(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("счётчик %q %v: %v", query, args, err)
	}
	return n
}

// collapseZoneID — id зоны по имени; found = false — зоны нет.
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

// collapseHasZonePartition — есть ли у таблицы выделенная партиция зоны
// (граница «FOR VALUES IN (zid)», два варианта записи pg_get_expr).
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

// collapseSnapshot — слепок данных для сравнения «до/после»:
// домены, справочник зон, имена партиций, история.
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

// collapseSeed — типовой сид сценария: TLD in (с доменом-аборигеном),
// 2LD 5g.in (5 доменов, выделенные партиции, файлы с конфликтом дубля),
// 2LD co.uk без партиций (строки в DEFAULT), allowlist-зона crawl.test.
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

	// Журнал доставки: файл 5g.in, конфликт дубля (то же содержимое уже
	// доставлялось в in — остаётся строка-владелец) и файл co.uk.
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

	// Выделенные партиции зоны 5g.in (миграция 000003): строки уезжают
	// из DEFAULT — перенос обязан дропнуть партиции по завершении.
	callCreateZonePartitions(t, pool, twoLD, 2)
	return in, twoLD, coUK, allow
}

// collapseAllowlist — allowlist тестового сида.
var collapseAllowlist = []string{"crawl.test"}

// TestZonesCollapse — полный сценарий §9: dry-run, предусловие, apply
// (перенос шести таблиц, партиции, дедуп, счётчики, пост-проверки §7.5),
// повтор — no-op.
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

		// Разбор оператором: очередь дренирована — обычный apply обязан
		// пройти без --force.
		if _, err := pool.Exec(ctx,
			`UPDATE domains SET check_state = 0 WHERE zone_id = $1 AND label = 'site0'`, twoLD); err != nil {
			t.Fatalf("сброс очереди: %v", err)
		}
	})

	t.Run("apply переносит всё и пост-проверки зелёные", func(t *testing.T) {
		var out strings.Builder
		// Batch = 2: пять доменов 5g.in идут несколькими батчами —
		// попутно проверяется путь окон (§7.3) на живой БД.
		if err := collapse.Run(ctx, pool, collapse.Options{Allowlist: collapseAllowlist, Apply: true, Batch: 2}, &out); err != nil {
			t.Fatalf("apply: %v", err)
		}
		got := out.String()

		// Зоны: 2LD удалены, TLD созданы, allowlist жив.
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

		// Домены: зона TLD, label с доклейкой префикса; абориген in
		// и allowlist-домен не тронуты.
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

		// Дети и история: переброшены по zone_id (join по domain_id).
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

		// ingest_files: дубль 5g.in удалён (владелец в in остался),
		// прочие перепривязаны к TLD.
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

		// Партиции: 2LD дропнуты (включая hash-листья), TLD выделены.
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

		// Пост-проверки §7.5 — напрямую и из отчёта команды.
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

		// Счётчики зон = агрегат (реконсиляция шага 8): in содержит
		// аборигена и пять перенесённых.
		var total int64
		if err := pool.QueryRow(ctx,
			`SELECT domains_total FROM zones WHERE id = $1`, in).Scan(&total); err != nil {
			t.Fatalf("zones.domains_total: %v", err)
		}
		if total != 6 {
			t.Errorf("zones.domains_total зоны in = %d, хочу 6 (реконсиляция)", total)
		}

		// Спот-чек §7.5 — самой SQL-функцией: зона T, label с префиксом.
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

// TestZonesCollapse_Resume — возобновление после сбоя на середине
// (§7.1): перенос зоны падает после первого зафиксированного батча
// (имитация обрыва соединения), повтор Zone по тому же состоянию
// догоняет без потерь и дублей; итог идентичен однопроходному переносу.
func TestZonesCollapse_Resume(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()
	twoLD := collapseSeedZone(t, pool, "5g.in")
	ids := collapseSeedDomains(t, pool, twoLD, "site", 5, 0)
	collapseSeedChildren(t, pool, twoLD, ids)
	// Выделенные партиции 2LD-зоны: после сбоя они обязаны оставаться
	// на месте (перос не завершён), повтор их дропнет.
	callCreateZonePartitions(t, pool, twoLD, 2)

	spec, err := collapse.SpecOf(collapse.ZoneRow{ID: twoLD, Name: "5g.in"})
	if err != nil {
		t.Fatal(err)
	}

	// Соединение, BEGIN которого падает после первого батча: первый
	// батч фиксируется, второй обрывается — половинчатый перенос.
	// Release сразу через defer: незакрытое соединение вешает
	// pool.Close() очистки теста при любом отказе ниже.
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
	// Строка зоны и партиции на месте — перенос не завершён.
	if _, ok := collapseZoneID(t, pool, "5g.in"); !ok {
		t.Fatal("строка зоны удалена до завершения переноса")
	}
	if !collapseHasZonePartition(t, pool, "domains", twoLD) {
		t.Fatal("партиции 2LD дропнуты до завершения переноса")
	}

	// Повтор тем же кодом (Run): инвентаризация видит зону, moving_ids
	// пересобирается по остатку, окна идут по оставшимся id.
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

// TestZonesCollapse_Force — семантика предусловия §7.1 целиком: dry-run
// предупреждает о доменах в очереди, но не отказывает; apply без --force
// отказывает до переноса; apply --force выполняет перенос.
func TestZonesCollapse_Force(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()
	twoLD := collapseSeedZone(t, pool, "5g.in")
	ids := collapseSeedDomains(t, pool, twoLD, "site", 2, 2) // оба в очереди
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

// collapseFlakyConn — соединение переноса с ограничением числа BEGIN:
// после allowBegin успешных транзакций BEGIN падает (имитация обрыва
// команды на середине, §7.1 «возобновляема»).
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

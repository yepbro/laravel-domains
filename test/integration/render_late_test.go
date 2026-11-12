// Integration tests of T-259 (the TZ-fsm spec §3 FR-5): the render
// late re-publisher (internal/httpworker/renderlate.go, FR-5.3)
// against live PostgreSQL/Redis — the selection predicate, the
// interlock with unconsumed q:render/q:render-fallback messages, the
// SPA verdict by the saved content and alias inheritance, the cursor
// closure; and the CHECK invariant of the render state
// subdomain_results_rendered_check (migration 000018, FR-5.2): a
// violation is rejected, the legal combinations and the existing data
// are untouched (NOT VALID), convalidated = false until the scheduled
// VALIDATE. The call level — library-level, without starting services
// (the render_requeue_test.go style); isolation — withTestSchema +
// Redis DB 15.
package integration

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"crawler/internal/content"
	"crawler/internal/httpworker"
	"crawler/internal/metrics"
	"crawler/internal/migrate"
	"crawler/internal/queue"
)

// lateBody — an SPA shell (T-057, the same as the rqBody of the
// neighboring file).
var lateBody = []byte(`<html><head><title>x</title></head><body><div id="root"></div><noscript>please enable javascript</noscript></body></html>`)

// latePendingRedis — an adapter of the unconsumed key source for the
// test interlocks: the semantics of the cmd/http-worker
// retentionPending (the undelivered-to-the-group tail from the
// last-delivered-id; no groups — the whole stream), a minimal
// implementation without pagination (the test tails are short).
type latePendingRedis struct {
	rdb *redis.Client
}

func (p latePendingRedis) PendingKeys(ctx context.Context, stream string) (map[content.RetentionPendingKey]struct{}, error) {
	keys := make(map[content.RetentionPendingKey]struct{})
	msgs, err := p.rdb.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		if err == redis.Nil || strings.Contains(err.Error(), "no such key") {
			return keys, nil
		}
		return nil, err
	}
	for _, m := range msgs {
		raw, ok := m.Values["data"].(string)
		if !ok {
			continue
		}
		var r queue.Result
		if json.Unmarshal([]byte(raw), &r) != nil || r.ZoneID <= 0 || r.DomainID <= 0 {
			continue
		}
		keys[content.RetentionPendingKey{
			Zone: r.ZoneID, Domain: r.DomainID, Sub: r.SubdomainID, Variant: int16(r.Variant),
		}] = struct{}{}
	}
	return keys, nil
}

// TestRenderLateRepublisher — the FR-5.3 acceptance criteria: a lost
// record (older than 6 h, without the bit, without messages) is
// re-published to q:render; a record in a stream (q:render or
// q:render-fallback) or under the render_pending bit is NOT touched;
// a fresh one, a marked one, rendered and non-SPA — not either.
func TestRenderLateRepublisher(t *testing.T) {
	skipUnlessIntegration(t)
	ctx := context.Background()

	dsn, cleanup := withTestSchema(t)
	t.Cleanup(cleanup)
	if err := migrate.Up(ctx, dsn); err != nil {
		t.Fatalf("применение миграций к изолированной схеме: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("подключение к тестовой схеме: %v", err)
	}
	t.Cleanup(pool.Close)

	rdb := resilienceRedisEnv(t)
	t.Cleanup(func() {
		if err := rdb.Del(ctx, queue.StreamRender, queue.StreamRenderFallback).Err(); err != nil {
			t.Fatalf("очистка стримов: %v", err)
		}
	})
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})
	store, err := content.OpenPackStore(pool, content.PackOptions{
		Dir:           t.TempDir(),
		PackSize:      8 << 10,
		MaxStoreBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("OpenPackStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	// watermark 0 — there is always capacity: the predicates and the
	// interlock are checked, not the gate (the render_requeue_test.go
	// style).
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{})
	renderQ, err := httpworker.NewRenderQueue(q, gate, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	reg := metrics.New("http-worker-test")
	met := httpworker.NewMetrics(reg)

	// Live messages for the interlock: published BEFORE the pass,
	// nobody consumes — an undelivered tail (the streams have no
	// groups).
	liveMsg := func(stream string, res queue.Result) {
		t.Helper()
		if _, err := q.Publish(ctx, stream, res); err != nil {
			t.Fatalf("публикация живой задачи %s: %v", stream, err)
		}
	}

	var zone int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name) VALUES ('late.test') RETURNING id",
	).Scan(&zone); err != nil {
		t.Fatalf("insert zones: %v", err)
	}

	// seed — a domain + records; age controls checked_at (the FR-5.3
	// window — 6 h).
	spaHash := func() []byte {
		put, err := store.Put(ctx, lateBody)
		if err != nil {
			t.Fatalf("сохранение SPA-контента: %v", err)
		}
		return put.Hash[:]
	}()
	rawPut, err := store.Put(ctx, []byte(`<html><body>plain static page with plenty of text</body></html>`))
	if err != nil {
		t.Fatalf("сохранение статического контента: %v", err)
	}

	var domainID int64
	seedDomain := func(label string, flags int16) int64 {
		t.Helper()
		if err := pool.QueryRow(ctx,
			"INSERT INTO domains (zone_id, label, flags) VALUES ($1, $2, $3) RETURNING id",
			zone, label, flags,
		).Scan(&domainID); err != nil {
			t.Fatalf("insert domains %s: %v", label, err)
		}
		return domainID
	}
	seedRow := func(domain int64, sub int16, hash []byte, age time.Duration, alias *int16, marked, rendered bool) {
		t.Helper()
		if _, err := pool.Exec(ctx,
			`INSERT INTO subdomain_results
			 (zone_id, domain_id, subdomain_id, scheme, final_url, content_hash, alias_subdomain_id,
			  render_failed_at, rendered, checked_at)
			 VALUES ($1, $2, $3, 2, 'https://late.test/', $4, $5, $6, $7, now() - $8::interval)`,
			zone, domain, sub, hash, alias,
			markerAge(marked), rendered, age.String()); err != nil {
			t.Fatalf("insert subdomain_results (домен %d): %v", domain, err)
		}
	}

	old, fresh := 7*time.Hour, time.Hour
	// The lost SPA record: older than the window, without the bit,
	// without messages.
	lost := seedDomain("lost", 0)
	// The original + the alias: both older than the window, the alias
	// without content — the verdict is inherited from the original
	// (T-055).
	aliasDom := seedDomain("aliasdom", 0)
	origSub := int16(3)
	// A fresh record — younger than the window: publishing would breed
	// duplicates of live tasks.
	freshDom := seedDomain("fresh", 0)
	// Debt: the render_pending bit — the ownership of the finish-up
	// pass, not of the re-publisher.
	debtDom := seedDomain("debt", 2)
	// The live messages of the streams: the interlock must exclude.
	liveDom := seedDomain("live", 0)
	fbDom := seedDomain("fb", 0)
	// The marked one (E3), rendered (E2) and non-SPA — outside the
	// re-publishing.
	markedDom := seedDomain("marked", 0)
	renderedDom := seedDomain("rendered", 0)
	staticDom := seedDomain("static", 0)
	// A rendered original + a lost alias: the fact of the original's
	// render proves the SPA verdict of the class — the alias is
	// re-published (review P2-1).
	doneOrigDom := seedDomain("doneorig", 0)
	doneOrigSub := int16(4)

	seedRow(lost, 0, spaHash, old, nil, false, false)
	seedRow(aliasDom, origSub, spaHash, old, nil, false, false)
	seedRow(aliasDom, 7, nil, old, &origSub, false, false)
	seedRow(freshDom, 0, spaHash, fresh, nil, false, false)
	seedRow(debtDom, 0, spaHash, old, nil, false, false)
	seedRow(liveDom, 0, spaHash, old, nil, false, false)
	seedRow(fbDom, 0, spaHash, old, nil, false, false)
	seedRow(markedDom, 0, spaHash, old, nil, true, false)
	seedRow(renderedDom, 0, spaHash, old, nil, false, true)
	seedRow(staticDom, 0, rawPut.Hash[:], old, nil, false, false)
	seedRow(doneOrigDom, doneOrigSub, spaHash, old, nil, false, true)
	seedRow(doneOrigDom, 9, nil, old, &doneOrigSub, false, false)

	liveMsg(queue.StreamRender, queue.Result{ZoneID: zone, DomainID: liveDom, SubdomainID: 0})
	liveMsg(queue.StreamRenderFallback, queue.Result{ZoneID: zone, DomainID: fbDom, SubdomainID: 0})

	// Leveling the debt counter for the seed domain's bit (the 000015
	// backfill invariant): the re-publisher pass does not touch the
	// counter, but the schema stays consistent for the neighboring
	// checks.
	if _, err := pool.Exec(ctx,
		"UPDATE crawler_counters SET value = 1 WHERE name = 'render_pending'"); err != nil {
		t.Fatalf("выравнивание счётчика долга: %v", err)
	}

	cur := &httpworker.RenderLateCursor{}
	res, err := httpworker.LateOnce(ctx, pool, store, renderQ, latePendingRedis{rdb: rdb}, met,
		httpworker.LateOptions{BatchSize: 100}, cur)
	if err != nil {
		t.Fatalf("LateOnce: %v", err)
	}
	// The batch: the lost one, the original+alias pair, the alias of
	// the rendered original, the two live ones (the interlock), the
	// non-SPA — 7 selected in total, 4 published, 3 skipped; the
	// fresh/debt/marked/rendered are cut off by the SQL.
	if res.Selected != 7 || res.Published != 4 || res.Skipped != 3 || res.Full {
		t.Fatalf("итог %+v, хочу selected=7 published=4 skipped=3", res)
	}
	if !res.Wrapped {
		t.Fatalf("проход не замкнул курсор (пачка короче лимита): %+v", res)
	}

	// The publication composition: the lost record, the
	// original+alias pair and the alias of the rendered original.
	msgs, err := rdb.XRange(ctx, queue.StreamRender, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRANGE %s: %v", queue.StreamRender, err)
	}
	got := map[int64]int{}
	for _, m := range msgs {
		var r queue.Result
		if err := json.Unmarshal([]byte(m.Values["data"].(string)), &r); err != nil {
			t.Fatalf("разбор сообщения %s: %v", m.ID, err)
		}
		got[r.DomainID]++
	}
	if len(got) != 4 || got[lost] != 1 || got[aliasDom] != 2 || got[doneOrigDom] != 1 {
		t.Fatalf("публикации по доменам %v, хочу {потерянная %d:1, оригинал+алиас %d:2, алиас rendered-оригинала %d:1}",
			got, lost, aliasDom, doneOrigDom)
	}
	if n := streamLen(t, rdb, queue.StreamRender); n != 5 {
		t.Fatalf("сообщений q:render = %d, хочу 5 (4 переиздания + 1 живая)", n)
	}
	if v := lateMetricValue(t, reg, "crawler_render_late_republish_total"); v != 4 {
		t.Fatalf("crawler_render_late_republish_total = %v, хочу 4", v)
	}

	// A repeated pass: the pass's own publications are now an
	// unconsumed tail of q:render, the interlock excludes the same
	// records; no duplicates.
	res, err = httpworker.LateOnce(ctx, pool, store, renderQ, latePendingRedis{rdb: rdb}, met,
		httpworker.LateOptions{BatchSize: 100}, cur)
	if err != nil {
		t.Fatalf("повторный LateOnce: %v", err)
	}
	if res.Published != 0 {
		t.Fatalf("повторный проход: published=%d, хочу 0 (интерлок собственных сообщений)", res.Published)
	}
	if n := streamLen(t, rdb, queue.StreamRender); n != 5 {
		t.Fatalf("сообщений q:render после повтора = %d, хочу 5 (без дублей)", n)
	}
}

// label — the domain name for the final_url seed (not used by the messages).

// markerAge — the marker stamp for the seed: NULL or an hour ago.
func markerAge(marked bool) any {
	if !marked {
		return nil
	}
	return time.Now().Add(-time.Hour)
}

// lateMetricValue — the registry counter value by name (without
// labels); the series is not found — −1 (distinguishable from an
// honest zero). The family types are inferred (the httpMetricValue
// style, internal/httpworker/metrics_test.go).
func lateMetricValue(t *testing.T, g *metrics.Registry, name string) float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
		}
	}
	return -1
}

// TestSchema_ResultsRenderedCheck — the paired invariant test of the
// CHECK subdomain_results_rendered_check (T-259, FR-5.2, migration
// 000018): "rendered = true ⇒ render_failed_at IS NULL" — a violation
// is rejected by the schema (23514), the legal combinations pass, the
// constraint was added NOT VALID (convalidated = false on the parent
// and the sections — VALIDATE per the migration procedure as a
// separate step, docs/schema.md) and does not touch the existing
// data.
func TestSchema_ResultsRenderedCheck(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	var zone int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name) VALUES ('chk.test') RETURNING id").Scan(&zone); err != nil {
		t.Fatalf("insert zones: %v", err)
	}
	var id int64
	if err := pool.QueryRow(ctx,
		"INSERT INTO domains (zone_id, label) VALUES ($1, 'chk') RETURNING id", zone,
	).Scan(&id); err != nil {
		t.Fatalf("insert domains: %v", err)
	}
	insertRow := func(sub int16, rendered bool, marker any) error {
		_, err := pool.Exec(ctx,
			`INSERT INTO subdomain_results
			 (zone_id, domain_id, subdomain_id, scheme, final_url, rendered, render_failed_at, checked_at)
			 VALUES ($1, $2, $3, 2, 'https://chk.test/', $4, $5, now())`,
			zone, id, sub, rendered, marker)
		return err
	}

	// The legal combinations: rendered without a marker; non-render
	// with a marker (E3); non-render without a marker (E0/E1) — unique
	// PKs (review P1-1).
	if err := insertRow(0, true, nil); err != nil {
		t.Fatalf("rendered без маркера: %v", err)
	}
	if err := insertRow(1, false, time.Now()); err != nil {
		t.Fatalf("не-рендер с маркером: %v", err)
	}
	if err := insertRow(2, false, nil); err != nil {
		t.Fatalf("не-рендер без маркера: %v", err)
	}

	// The invariant violation: rendered = true + a marker — 23514 by
	// the schema, the code does not have to keep the column pair by
	// hand (FR-5.2).
	if err := insertRow(3, true, time.Now()); err == nil ||
		!strings.Contains(err.Error(), "subdomain_results_rendered_check") {
		t.Errorf("rendered с маркером: err = %v, want violation subdomain_results_rendered_check", err)
	}
	// An UPDATE of an existing row into violation is rejected too (NOT
	// VALID checks new writes, including changes); the target — the
	// specific rendered row (sub=0).
	if _, err := pool.Exec(ctx,
		"UPDATE subdomain_results SET render_failed_at = now() "+
			"WHERE zone_id = $1 AND domain_id = $2 AND subdomain_id = 0",
		zone, id); err == nil ||
		!strings.Contains(err.Error(), "subdomain_results_rendered_check") {
		t.Errorf("UPDATE маркера на rendered-строке: err = %v, want violation", err)
	}

	// NOT VALID: the constraint in the TEST SCHEMA (the current_schema
	// filter — review P3-1: the DB holds the schemas of neighboring
	// runs) is not yet validated on the parent and the sections —
	// VALIDATE as a separate step per the procedure (docs/schema.md);
	// new sections (create_zone_partitions, T-005) inherit it
	// validated. total ≥ 2 — the recursion into the sections happened
	// (the parent + the DEFAULT partition).
	rows, err := pool.Query(ctx,
		`SELECT count(*) FILTER (WHERE convalidated) AS validated, count(*) AS total
		 FROM pg_constraint
		 WHERE conname = 'subdomain_results_rendered_check'
		   AND connamespace = current_schema()::regnamespace`)
	if err != nil {
		t.Fatalf("pg_constraint: %v", err)
	}
	defer rows.Close()
	var validated, total int
	if !rows.Next() || rows.Scan(&validated, &total) != nil {
		t.Fatalf("чтение pg_constraint: %v", rows.Err())
	}
	if total < 2 {
		t.Fatalf("ограничение в схеме теста на %d объектах, хочу ≥2 (родитель + секция; миграция 000018)", total)
	}
	if validated != 0 {
		t.Errorf("convalidated = true у %d из %d строк ограничения, хочу 0 (NOT VALID, VALIDATE — по регламенту)",
			validated, total)
	}
}

// latePinSelectSQL — the full lateRenderRowsSQL text with literal
// parameters (T-271; the anchor — internal/httpworker/renderlate.go,
// the duplication — the rqBatchSQL style; the schema codes as
// literals: FlagRenderPending = 2, StatusDeleted = 6).
const latePinSelectSQL = "SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, " +
	"r.content_hash, r.alias_subdomain_id, d.label || '.' || z.name " +
	"FROM subdomain_results AS r " +
	"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id " +
	"JOIN zones AS z ON z.id = r.zone_id " +
	"WHERE r.rendered = false AND r.final_url IS NOT NULL AND r.render_failed_at IS NULL " +
	"AND r.checked_at < now() - interval '6 hours' " +
	"AND (d.flags & 2) = 0 " +
	"AND d.status <> 6 " +
	"AND (r.zone_id, r.domain_id, r.subdomain_id, r.variant) > (0::int2, 0::int8, 0::int2, 0::int2) " +
	"ORDER BY r.zone_id, r.domain_id, r.subdomain_id, r.variant " +
	"LIMIT 1000"

// TestRenderLateSelectPlanUnpinned — the plan shape of the late
// re-publisher batch selection (T-273 removed the T-271 pins): at
// production-proportion volume (domains ≈ 4× the records — production
// 43M at 11.3M) the UNpinned planner must assemble a limited batch
// with an early stop — access to subdomain_results via an index +
// Limit, without a full Sort of the records. The T-271 pinning
// (SET LOCAL seq/hash/merge/bitmap off) at such volume flipped the
// join to lead from domains (cost ≈157M) — an infinite loop over the
// 60 s deadline (the 2026-11-09–10 incident); the unpinned T-251 form
// ("the planner decides itself") worked in production for weeks. The
// specific path is not pinned (Seq/Hash/Merge are in the planner's
// hands — hence the production-proportion fixture, not three rows: on
// an empty table the planner legitimately takes a seq scan + Sort).
// The T-273 measurement (the testbed PG 16.15, 2M domains / 550k
// records): the zero cursor — a Nested Loop from domains with PK
// probes of the records, 19 ms; the middle of the table — a Merge
// Join from subdomain_results, 42 ms.
func TestRenderLateSelectPlanUnpinned(t *testing.T) {
	skipUnlessIntegration(t)
	pool := migratedSchema(t)
	ctx := context.Background()

	var zone int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name) VALUES ('late-unpin.test') RETURNING id").Scan(&zone); err != nil {
		t.Fatalf("insert zones: %v", err)
	}

	// The production proportion in miniature: 480k domains, 125k
	// records (1:3.8 ≈ the production 43:11.3M) — most domains without
	// records, the selection from the zero cursor is maximally
	// non-selective, as in catch-up.
	const (
		lateDomains = 480000
		lateRows    = 125000
	)
	if _, err := pool.Exec(ctx, `
		INSERT INTO domains (zone_id, id, label)
		SELECT $1, g, 'lu' || g FROM generate_series(1, $2) g`, zone, lateDomains); err != nil {
		t.Fatalf("посев domains: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO subdomain_results
		    (zone_id, domain_id, subdomain_id, scheme, final_url, checked_at)
		SELECT $1, g, (g % 63)::smallint, 2, 'https://lu.test/' || g,
		       now() - interval '7 hours' - (g * interval '1 second')
		FROM generate_series(1, $2) g`, zone, lateRows); err != nil {
		t.Fatalf("посев subdomain_results: %v", err)
	}
	if _, err := pool.Exec(ctx, "ANALYZE domains"); err != nil {
		t.Fatalf("ANALYZE domains: %v", err)
	}
	if _, err := pool.Exec(ctx, "ANALYZE subdomain_results"); err != nil {
		t.Fatalf("ANALYZE subdomain_results: %v", err)
	}

	// Without pins — as in the production tx wrapper of T-273 (the only
	// SET LOCAL there is statement_timeout, which does not affect the
	// plan shape).
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// EXPLAIN ANALYZE — the shape and the fact of execution: the batch
	// must be assembled (rows=1000) in finite time — exactly what the
	// pinned form failed to do at production volume.
	plan := explainAnalyze(t, tx, ctx, latePinSelectSQL)
	if !strings.Contains(plan, "Limit") {
		t.Errorf("план без Limit (ранний останов пачки):\n%s", plan)
	}
	if !planIdxScanRe.MatchString(plan) {
		t.Errorf("план без Index Scan (записи читаются мимо индексов):\n%s", plan)
	}
	// A full Sort of the records — the "scan everything and sort"
	// shape, its cost was attributed to the T-271 incident; an
	// Incremental Sort over the index order is allowed (the node is
	// named "Incremental Sort", the badSortRe anchor does not match
	// it).
	if badSortRe.MatchString(plan) {
		t.Errorf("план содержит полный Sort записей (порядок курсора должен закрываться индексом):\n%s", plan)
	}
	if !strings.Contains(plan, "rows=1000 loops=1") {
		t.Errorf("исполнение не собрало пачку 1000 (rows=1000 loops=1 в Limit-узле):\n%s", plan)
	}
}

// badSortRe — a full Sort node (not "Incremental Sort" and not the
// "Sort Key:"/"Sort Method:" lines) in the T-273 EXPLAIN tests: the
// anchor at the beginning of the node line and the requirement of a
// bracket/line end after the name — Go regexp without lookahead, and
// "Sort Key" starts with the same word.
var badSortRe = regexp.MustCompile(`(?m)^\s*(?:->\s*)?Sort(\s*\(|$)`)

// explainAnalyze — the EXPLAIN ANALYZE (COSTS OFF) plan text of a
// query inside a transaction (the explainPlan style; T-273: ANALYZE —
// a measurement of the actual execution of the limited batch).
func explainAnalyze(t *testing.T, tx pgx.Tx, ctx context.Context, sql string) string {
	t.Helper()
	rows, err := tx.Query(ctx, "EXPLAIN (ANALYZE, COSTS OFF) "+sql)
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE: %v", err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("EXPLAIN ANALYZE: чтение: %v", err)
		}
		plan.WriteString(line)
		plan.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("EXPLAIN ANALYZE: %v", err)
	}
	return plan.String()
}

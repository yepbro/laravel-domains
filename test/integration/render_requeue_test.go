// Integration tests of the render rework pass RequeueOnce (T-066;
// the batch selection without a PK order, the plan pinning SET LOCAL
// and the guard by the debt counter — T-181; the zone priority by the
// two-stage selection — T-184) against live PostgreSQL/Redis: the
// renderq_test.go unit tests check the SQL contracts and the sequence
// on fakes; here — the real behavior on a migrated schema: the pass
// publishes to q:render and clears the flag with a counter decrement;
// the steady state without debt (counter 0) does not run the heavy
// selection at all (a pool wrapper with a call counter — the T-181
// §7.2 spec prescription); a repeated pass does not select the
// cleared ones; a missing counter row — a pass error. The pinned plan
// shape is checked by EXPLAIN in the same transaction with SET LOCAL
// (a seq scan over domains, the index forms disabled). The call level
// — like the neighbors: library calls without starting services;
// isolation — withTestSchema + Redis DB 15.
package integration

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/content"
	"crawler/internal/httpworker"
	"crawler/internal/migrate"
	"crawler/internal/queue"
)

// rqSeqScanRe — a seq scan over domains (the d alias) in EXPLAIN:
// PG16 prints the leaf partition ("Seq Scan on domains_default d"),
// PG17+ — the parent ("Seq Scan on d").
var rqSeqScanRe = regexp.MustCompile(`Seq Scan on (\w+ )?d\b`)

// rqBody — an SPA shell (T-057: little visible text, a root container,
// a noscript demanding JS; the same body as in the renderq_test.go
// unit fixtures): the repeated verdict of the pass must see the SPA
// and publish to q:render.
var rqBody = []byte(`<html><head><title>x</title></head><body><div id="root"></div><noscript>please enable javascript</noscript></body></html>`)

// rqBatchPrefix — the beginning of renderPendingDomainsSQL (a literal
// from internal/httpworker/renderq.go, the duplication — the
// schema_test.go style): the pool wrapper counts the heavy batch
// selection calls — the T-181 guard must keep it behind a zero debt
// counter. The selection runs INSIDE the plan pinning transaction, so
// the counter lives in the transaction wrapper.
const rqBatchPrefix = "SELECT d.zone_id, d.id, d.label || '.' || z.name"

// rqBatchSQL — the full selection text for EXPLAIN (the LIMIT — as a
// literal: a parameter in EXPLAIN requires a prepared statement, the
// plan shape check does not care).
const rqBatchSQL = rqBatchPrefix + " FROM domains AS d JOIN zones AS z ON z.id = d.zone_id " +
	"WHERE (d.flags & 2) <> 0 AND d.status <> 6 LIMIT 100"

// rqTracePool — a RecordDB pool wrapper: counts the heavy batch
// selection calls (a pool wrapper with a call counter — the T-181
// §7.2 spec prescription).
type rqTracePool struct {
	*pgxpool.Pool

	mu    sync.Mutex
	batch int
}

func (p *rqTracePool) countBatch() {
	p.mu.Lock()
	p.batch++
	p.mu.Unlock()
}

func (p *rqTracePool) batchCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.batch
}

// Begin wraps the transaction with the selection counter.
func (p *rqTracePool) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &rqTraceTx{Tx: tx, pool: p}, nil
}

// rqTraceTx — a transaction wrapper: the heavy selection runs inside
// the plan pinning transaction (renderSelectPending, T-181).
type rqTraceTx struct {
	pgx.Tx
	pool *rqTracePool
}

func (t *rqTraceTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if strings.HasPrefix(sql, rqBatchPrefix) {
		t.pool.countBatch()
	}
	return t.Tx.Query(ctx, sql, args...)
}

// TestRenderRequeuePass — the rework pass on live PostgreSQL/Redis:
// there is debt — publishes and clears; the counter 0 — the heavy
// selection is not run; the cleared ones are not selected again; a
// missing counter row — an error; the plan shape is pinned (a seq
// scan).
func TestRenderRequeuePass(t *testing.T) {
	skipUnlessIntegration(t)
	ctx := context.Background()

	dsn, cleanup := withTestSchema(t)
	t.Cleanup(cleanup)
	if err := migrate.Up(ctx, dsn); err != nil {
		t.Fatalf("applying migrations to the isolated schema: %v", err)
	}
	raw, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting to the test schema: %v", err)
	}
	t.Cleanup(raw.Close)
	pool := &rqTracePool{Pool: raw}

	rdb := resilienceRedisEnv(t)
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
	// watermark 0 — there is always capacity (HasCapacity without
	// Redis): the pass does not hit the gate, the selection and the
	// guard are checked (T-181).
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{})
	renderQ, err := httpworker.NewRenderQueue(q, gate, 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	// The plan shape is pinned (§4.2 of the T-181 spec): inside the
	// SET LOCAL transaction (all index forms disabled) EXPLAIN shows a
	// seq scan over domains, no index nodes. PG16 prints the leaf
	// partition ("Seq Scan on domains_default d"), PG17+ — the parent
	// ("Seq Scan on d"): the regexp covers both spellings.
	t.Run("the selection plan pinned by SET LOCAL", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		for _, pin := range []string{
			"SET LOCAL enable_indexscan = off",
			"SET LOCAL enable_bitmapscan = off",
			"SET LOCAL enable_indexonlyscan = off",
		} {
			if _, err := tx.Exec(ctx, pin); err != nil {
				t.Fatalf("%s: %v", pin, err)
			}
		}
		rows, err := tx.Query(ctx, "EXPLAIN (COSTS OFF) "+rqBatchSQL)
		if err != nil {
			t.Fatalf("EXPLAIN: %v", err)
		}
		defer rows.Close()
		var plan strings.Builder
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatalf("EXPLAIN: reading: %v", err)
			}
			plan.WriteString(line)
			plan.WriteByte('\n')
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("EXPLAIN: %v", err)
		}
		if !rqSeqScanRe.MatchString(plan.String()) {
			t.Errorf("the pinned plan has no seq scan over domains (the d alias):\n%s", plan.String())
		}
		if strings.Contains(plan.String(), "Index") || strings.Contains(plan.String(), "Bitmap") {
			t.Errorf("the pinned plan has an index form (SET LOCAL did not take effect):\n%s", plan.String())
		}
	})

	// Debt: a domain with the flag and an SPA record; the counter is
	// aligned by hand (the seed is a direct INSERT, the code keeps the
	// bit+counter pair; the backfill invariant of migration 000015:
	// the counter = the number of rows with the bit).
	var zone int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name) VALUES ('requeue.test') RETURNING id",
	).Scan(&zone); err != nil {
		t.Fatalf("insert zones: %v", err)
	}
	var id int64
	if err := pool.QueryRow(ctx,
		"INSERT INTO domains (zone_id, label, flags) VALUES ($1, 'spa', 2) RETURNING id", zone,
	).Scan(&id); err != nil {
		t.Fatalf("insert domains: %v", err)
	}
	put, err := store.Put(ctx, rqBody)
	if err != nil {
		t.Fatalf("storing the content: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO subdomain_results
		 (zone_id, domain_id, subdomain_id, scheme, final_url, content_hash, checked_at)
		 VALUES ($1, $2, 0, 2, 'https://spa.requeue.test/', $3, now())`,
		zone, id, put.Hash[:]); err != nil {
		t.Fatalf("insert subdomain_results: %v", err)
	}
	if _, err := pool.Exec(ctx,
		"UPDATE crawler_counters SET value = 1 WHERE name = 'render_pending'"); err != nil {
		t.Fatalf("aligning the debt counter: %v", err)
	}

	t.Run("there is debt — the pass publishes and clears", func(t *testing.T) {
		res, err := httpworker.RequeueOnce(ctx, pool, store, renderQ, httpworker.RequeueOptions{})
		if err != nil {
			t.Fatalf("RequeueOnce: %v", err)
		}
		if res.Selected != 1 || res.Cleared != 1 || res.Published != 1 {
			t.Fatalf("outcome %+v, expected selected=1 cleared=1 published=1", res)
		}
		if n := streamLen(t, rdb, queue.StreamRender); n != 1 {
			t.Fatalf("q:render messages = %d, want 1", n)
		}
		var flags int16
		if err := pool.QueryRow(ctx,
			"SELECT flags FROM domains WHERE zone_id = $1 AND id = $2", zone, id,
		).Scan(&flags); err != nil {
			t.Fatal(err)
		}
		if flags&2 != 0 {
			t.Errorf("the render_pending bit not cleared (flags=%d)", flags)
		}
		var counter int64
		if err := pool.QueryRow(ctx,
			"SELECT value FROM crawler_counters WHERE name = 'render_pending'",
		).Scan(&counter); err != nil || counter != 0 {
			t.Errorf("the debt counter after the clear = %d (err=%v), want 0", counter, err)
		}
	})

	t.Run("steady state: counter 0 — the heavy selection does not run", func(t *testing.T) {
		before := pool.batchCalls()
		res, err := httpworker.RequeueOnce(ctx, pool, store, renderQ, httpworker.RequeueOptions{})
		if err != nil {
			t.Fatalf("RequeueOnce: %v", err)
		}
		if res.Selected != 0 || res.Published != 0 || res.Full {
			t.Fatalf("outcome %+v, expected empty (the guard: no debt)", res)
		}
		if n := pool.batchCalls(); n != before {
			t.Fatalf("the heavy selection ran %d times with a zero counter (was %d)", n-before, before)
		}
		if n := streamLen(t, rdb, queue.StreamRender); n != 1 {
			t.Fatalf("q:render publications became %d, want 1 (no new ones)", n)
		}
	})

	t.Run("a repeated pass does not select the cleared ones", func(t *testing.T) {
		// The counter desync > 0 with the flags cleared (the guard trust
		// boundary, the T-181 §4.3 spec): one idle seq scan per pass — the
		// selection is empty, the cleared ones are not re-published.
		if _, err := pool.Exec(ctx,
			"UPDATE crawler_counters SET value = 1 WHERE name = 'render_pending'"); err != nil {
			t.Fatal(err)
		}
		before := pool.batchCalls()
		res, err := httpworker.RequeueOnce(ctx, pool, store, renderQ, httpworker.RequeueOptions{})
		if err != nil {
			t.Fatalf("RequeueOnce: %v", err)
		}
		if res.Selected != 0 || res.Published != 0 {
			t.Fatalf("outcome %+v, expected selected=0 published=0 (the cleared ones are not selected)", res)
		}
		if n := pool.batchCalls(); n != before+1 {
			t.Fatalf("%d heavy selections (was %d), want +1 — an idle pass driven by the counter", n, before)
		}
		if n := streamLen(t, rdb, queue.StreamRender); n != 1 {
			t.Fatalf("q:render publications became %d, want 1 (no duplicates)", n)
		}
	})

	t.Run("the counter row is missing — a pass error", func(t *testing.T) {
		if _, err := pool.Exec(ctx, "DELETE FROM crawler_counters WHERE name = 'render_pending'"); err != nil {
			t.Fatal(err)
		}
		if _, err := httpworker.RequeueOnce(ctx, pool, store, renderQ, httpworker.RequeueOptions{}); err == nil {
			t.Fatal("the missing counter row not returned as an error (the guard must fail loudly)")
		}
	})
}

// TestRenderRequeueFailedMarker — the "render is impossible" marker
// subdomain_results.render_failed_at (T-210, migration 000017) on live
// PostgreSQL/Redis: the finalization stamp (the SQL — a literal from
// internal/renderworker/worker.go, the duplication — the
// schema_test.go style; the content_hash/rendered guards,
// idempotency) excludes the record from the pass selection (no
// publications, the flag is cleared without a publication — a "draft"),
// an episode rewrite (a DELETE + INSERT of the result record, T-058)
// resets the marker — the next pass re-publishes the record.
func TestRenderRequeueFailedMarker(t *testing.T) {
	skipUnlessIntegration(t)
	ctx := context.Background()

	dsn, cleanup := withTestSchema(t)
	t.Cleanup(cleanup)
	if err := migrate.Up(ctx, dsn); err != nil {
		t.Fatalf("applying migrations to the isolated schema: %v", err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting to the test schema: %v", err)
	}
	t.Cleanup(pool.Close)

	rdb := resilienceRedisEnv(t)
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
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{})
	renderQ, err := httpworker.NewRenderQueue(q, gate, 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	// renderFailMarkerSQL — a literal from internal/renderworker/worker.go
	// (renderFailMarkerSQL, T-210): a code-vs-test divergence is visible
	// from the behavior of the guards below.
	const markerSQL = "UPDATE subdomain_results AS r " +
		"SET render_failed_at = coalesce(r.render_failed_at, now()) " +
		"WHERE r.zone_id = $1::int2 AND r.domain_id = $2::int8 " +
		"AND r.subdomain_id = $3::int2 AND r.variant = $4::int2 " +
		"AND r.rendered = false " +
		"AND r.content_hash IS NOT DISTINCT FROM $5::bytea"

	// Seed: a domain with the debt flag and an SPA record.
	var zone int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name) VALUES ('failed.test') RETURNING id",
	).Scan(&zone); err != nil {
		t.Fatalf("insert zones: %v", err)
	}
	var id int64
	if err := pool.QueryRow(ctx,
		"INSERT INTO domains (zone_id, label, flags) VALUES ($1, 'spa', 2) RETURNING id", zone,
	).Scan(&id); err != nil {
		t.Fatalf("insert domains: %v", err)
	}
	put, err := store.Put(ctx, rqBody)
	if err != nil {
		t.Fatalf("storing the content: %v", err)
	}
	insertRow := func(url string, hash []byte) {
		t.Helper()
		if _, err := pool.Exec(ctx,
			`INSERT INTO subdomain_results
			 (zone_id, domain_id, subdomain_id, scheme, final_url, content_hash, checked_at)
			 VALUES ($1, $2, 0, 2, $3, $4, now())`,
			zone, id, url, hash); err != nil {
			t.Fatalf("insert subdomain_results: %v", err)
		}
	}
	insertRow("https://spa.failed.test/", put.Hash[:])
	alignDebt := func() {
		t.Helper()
		if _, err := pool.Exec(ctx,
			"UPDATE domains SET flags = flags | 2 WHERE zone_id = $1 AND id = $2", zone, id); err != nil {
			t.Fatalf("setting the flag: %v", err)
		}
		if _, err := pool.Exec(ctx,
			"UPDATE crawler_counters SET value = 1 WHERE name = 'render_pending'"); err != nil {
			t.Fatalf("aligning the debt counter: %v", err)
		}
	}
	alignDebt()

	marker := func() (*time.Time, error) {
		var m *time.Time
		err := pool.QueryRow(ctx,
			"SELECT render_failed_at FROM subdomain_results WHERE zone_id = $1 AND domain_id = $2", zone, id,
		).Scan(&m)
		return m, err
	}

	t.Run("the finalization stamp is idempotent, the guards work", func(t *testing.T) {
		tag, err := pool.Exec(ctx, markerSQL, zone, id, int16(0), int16(0), put.Hash[:])
		if err != nil {
			t.Fatalf("the marker: %v", err)
		}
		if tag.RowsAffected() != 1 {
			t.Fatalf("RowsAffected = %d, want 1 (the row of that content, rendered = false)", tag.RowsAffected())
		}
		m, err := marker()
		if err != nil || m == nil {
			t.Fatalf("render_failed_at after the stamp: %v err=%v", m, err)
		}

		// A repeated finalization (a publish failure returned the
		// delivery) — RowsAffected 1, the stamp of the FIRST finalization
		// is kept (coalesce).
		if tag, _ := pool.Exec(ctx, markerSQL, zone, id, int16(0), int16(0), put.Hash[:]); tag.RowsAffected() != 1 {
			t.Fatalf("a repeated stamp: RowsAffected = %d, want 1 (idempotency)", tag.RowsAffected())
		}
		if m2, err := marker(); err != nil || m2 == nil || !m2.Equal(*m) {
			t.Fatalf("the stamp moved by the repeat: %v (err=%v), want the first finalization %v", m2, err, m)
		}

		// The content guard: a new episode rewrote content_hash — the
		// old delivery's stamp is not set and not touched.
		fresh := put.Hash
		fresh[0] ^= 0xff
		if _, err := pool.Exec(ctx,
			"UPDATE subdomain_results SET content_hash = $3 WHERE zone_id = $1 AND domain_id = $2",
			zone, id, fresh[:]); err != nil {
			t.Fatalf("changing the content: %v", err)
		}
		if tag, _ := pool.Exec(ctx, markerSQL, zone, id, int16(0), int16(0), put.Hash[:]); tag.RowsAffected() != 0 {
			t.Fatalf("a stamp on foreign content: RowsAffected = %d, want 0 (the content_hash guard)", tag.RowsAffected())
		}
		if m, err := marker(); err != nil || m == nil {
			t.Fatalf("the stamp knocked out by the guard: %v err=%v", m, err)
		}

		// The rendered guard: a successful concurrent render closed the
		// question — the stamp is not set and is cleared by the
		// transition to rendered.
		if _, err := pool.Exec(ctx,
			"UPDATE subdomain_results SET rendered = true, render_failed_at = NULL "+
				"WHERE zone_id = $1 AND domain_id = $2", zone, id); err != nil {
			t.Fatalf("moving to rendered: %v", err)
		}
		if tag, _ := pool.Exec(ctx, markerSQL, zone, id, int16(0), int16(0), fresh[:]); tag.RowsAffected() != 0 {
			t.Fatalf("a stamp on a rendered row: RowsAffected = %d, want 0 (the rendered guard)", tag.RowsAffected())
		}
		if m, err := marker(); err != nil || m != nil {
			t.Fatalf("render_failed_at after a successful render: %v err=%v (it is removed)", m, err)
		}
	})

	t.Run("a marked record is not re-published by the pass", func(t *testing.T) {
		if _, err := pool.Exec(ctx, "DELETE FROM subdomain_results WHERE zone_id = $1 AND domain_id = $2", zone, id); err != nil {
			t.Fatalf("clearing the rows: %v", err)
		}
		insertRow("https://spa.failed.test/", put.Hash[:])
		alignDebt()
		if tag, _ := pool.Exec(ctx, markerSQL, zone, id, int16(0), int16(0), put.Hash[:]); tag.RowsAffected() != 1 {
			t.Fatalf("the stamp: RowsAffected = %d, want 1", tag.RowsAffected())
		}
		res, err := httpworker.RequeueOnce(ctx, pool, store, renderQ, httpworker.RequeueOptions{})
		if err != nil {
			t.Fatalf("RequeueOnce: %v", err)
		}
		if res.Selected != 1 || res.Cleared != 1 || res.Published != 0 {
			t.Fatalf("outcome %+v, expected selected=1 cleared=1 published=0 (a marked record is not re-published)", res)
		}
		if n := streamLen(t, rdb, queue.StreamRender); n != 0 {
			t.Fatalf("q:render messages = %d, want 0 (T-210)", n)
		}
	})

	t.Run("a new episode resets the marker — the pass re-publishes", func(t *testing.T) {
		// The episode rewrite (T-058): a DELETE of the old rows + an
		// INSERT of the new ones; render_failed_at is not in the INSERT —
		// DEFAULT NULL.
		if _, err := pool.Exec(ctx, "DELETE FROM subdomain_results WHERE zone_id = $1 AND domain_id = $2", zone, id); err != nil {
			t.Fatalf("the episode DELETE: %v", err)
		}
		insertRow("https://spa.failed.test/", put.Hash[:])
		if m, err := marker(); err != nil || m != nil {
			t.Fatalf("render_failed_at after the episode rewrite: %v err=%v (reset to NULL)", m, err)
		}
		alignDebt()

		res, err := httpworker.RequeueOnce(ctx, pool, store, renderQ, httpworker.RequeueOptions{})
		if err != nil {
			t.Fatalf("RequeueOnce: %v", err)
		}
		if res.Selected != 1 || res.Cleared != 1 || res.Published != 1 {
			t.Fatalf("outcome %+v, expected selected=1 cleared=1 published=1 (the reset marker returned the record to the selection)", res)
		}
		if n := streamLen(t, rdb, queue.StreamRender); n != 1 {
			t.Fatalf("q:render messages = %d, want 1", n)
		}
	})
}

// TestRenderRequeueZonePriority — the zone priority of the finish-up
// pass (T-184, the T-176 decision (a)): the first-stage batch takes
// the domains of the zones NOT in the
// render.requeue_deprioritized_zones list (even those lying further in
// the "heap" — inserting .рф first imitates the production layout "the
// .рф flag debt at the head of the heap", T-181), the debt of the list
// zones is finished up on the leftovers principle; the plan shape of
// the stage with the zone filter — the same pinned seq scan.
func TestRenderRequeueZonePriority(t *testing.T) {
	skipUnlessIntegration(t)
	ctx := context.Background()

	dsn, cleanup := withTestSchema(t)
	t.Cleanup(cleanup)
	if err := migrate.Up(ctx, dsn); err != nil {
		t.Fatalf("applying migrations to the isolated schema: %v", err)
	}
	raw, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting to the test schema: %v", err)
	}
	t.Cleanup(raw.Close)

	// The first-stage plan shape (the zone filter in the JOIN): a seq
	// scan over domains, no index forms — the priority does not pull
	// the T-181 pathology.
	t.Run("the stage plan pinned by SET LOCAL", func(t *testing.T) {
		tx, err := raw.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		for _, pin := range []string{
			"SET LOCAL enable_indexscan = off",
			"SET LOCAL enable_bitmapscan = off",
			"SET LOCAL enable_indexonlyscan = off",
		} {
			if _, err := tx.Exec(ctx, pin); err != nil {
				t.Fatalf("%s: %v", pin, err)
			}
		}
		rows, err := tx.Query(ctx, "EXPLAIN (COSTS OFF) "+rqBatchPrefix+
			" FROM domains AS d JOIN zones AS z ON z.id = d.zone_id "+
			"WHERE (d.flags & 2) <> 0 AND d.status <> 6 "+
			"AND z.name <> ALL('{xn--p1ai}'::text[]) LIMIT 1")
		if err != nil {
			t.Fatalf("EXPLAIN: %v", err)
		}
		defer rows.Close()
		var plan strings.Builder
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatalf("EXPLAIN: reading: %v", err)
			}
			plan.WriteString(line)
			plan.WriteByte('\n')
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("EXPLAIN: %v", err)
		}
		if !rqSeqScanRe.MatchString(plan.String()) {
			t.Errorf("the pinned stage plan has no seq scan over domains (the d alias):\n%s", plan.String())
		}
		if strings.Contains(plan.String(), "Index") || strings.Contains(plan.String(), "Bitmap") {
			t.Errorf("the pinned stage plan has an index form:\n%s", plan.String())
		}
	})

	pool := &rqTracePool{Pool: raw}
	rdb := resilienceRedisEnv(t)
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
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{})
	renderQ, err := httpworker.NewRenderQueue(q, gate, 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Debt: the .рф domain is inserted FIRST (the production heap
	// layout), the priority zone — after it; both records are SPA (the
	// pass must publish both).
	var zoneRF, zonePrio int16
	if err := pool.QueryRow(ctx, "INSERT INTO zones (name) VALUES ('xn--p1ai') RETURNING id").Scan(&zoneRF); err != nil {
		t.Fatalf("insert zones: %v", err)
	}
	if err := pool.QueryRow(ctx, "INSERT INTO zones (name) VALUES ('prio.test') RETURNING id").Scan(&zonePrio); err != nil {
		t.Fatalf("insert zones: %v", err)
	}
	var rfID, prioID int64
	if err := pool.QueryRow(ctx,
		"INSERT INTO domains (zone_id, label, flags) VALUES ($1, 'rf', 2) RETURNING id", zoneRF,
	).Scan(&rfID); err != nil {
		t.Fatalf("insert domains: %v", err)
	}
	if err := pool.QueryRow(ctx,
		"INSERT INTO domains (zone_id, label, flags) VALUES ($1, 'prio', 2) RETURNING id", zonePrio,
	).Scan(&prioID); err != nil {
		t.Fatalf("insert domains: %v", err)
	}
	put, err := store.Put(ctx, rqBody)
	if err != nil {
		t.Fatalf("storing the content: %v", err)
	}
	for _, d := range []struct {
		zone int16
		id   int64
		url  string
	}{
		{zoneRF, rfID, "https://rf.xn--p1ai/"},
		{zonePrio, prioID, "https://prio.prio.test/"},
	} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO subdomain_results
			 (zone_id, domain_id, subdomain_id, scheme, final_url, content_hash, checked_at)
			 VALUES ($1, $2, 0, 2, $3, $4, now())`,
			d.zone, d.id, d.url, put.Hash[:]); err != nil {
			t.Fatalf("insert subdomain_results: %v", err)
		}
	}
	if _, err := pool.Exec(ctx,
		"UPDATE crawler_counters SET value = 2 WHERE name = 'render_pending'"); err != nil {
		t.Fatalf("aligning the debt counter: %v", err)
	}

	opts := httpworker.RequeueOptions{BatchSize: 1, DeprioritizedZones: []string{"xn--p1ai"}}

	// Pass 1 (batch 1): the first stage takes the priority zone,
	// skipping .рф at the head of the heap.
	res, err := httpworker.RequeueOnce(ctx, pool, store, renderQ, opts)
	if err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	if res.Selected != 1 || res.Deprioritized != 0 || res.Cleared != 1 || res.Published != 1 {
		t.Fatalf("pass 1: outcome %+v, expected selected=1 deprioritized=0 cleared=1 published=1", res)
	}
	var rfFlags int16
	if err := pool.QueryRow(ctx,
		"SELECT flags FROM domains WHERE zone_id = $1 AND id = $2", zoneRF, rfID,
	).Scan(&rfFlags); err != nil {
		t.Fatal(err)
	}
	if rfFlags&2 == 0 {
		t.Fatal("the .рф domain finished up before the priority one — the zone priority does not work")
	}

	// Pass 2: the first stage is exhausted — the second finishes up .рф.
	res, err = httpworker.RequeueOnce(ctx, pool, store, renderQ, opts)
	if err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if res.Selected != 1 || res.Deprioritized != 1 || res.Cleared != 1 || res.Published != 1 {
		t.Fatalf("pass 2: outcome %+v, expected selected=1 deprioritized=1 (the list-zone debt)", res)
	}
	var counter int64
	if err := pool.QueryRow(ctx,
		"SELECT value FROM crawler_counters WHERE name = 'render_pending'",
	).Scan(&counter); err != nil || counter != 0 {
		t.Fatalf("the debt counter after the drain = %d (err=%v), want 0", counter, err)
	}

	// The q:render publication order: the priority zone — first, .рф —
	// after it (even the reverse of the heap insertion order).
	msgs, err := rdb.XRange(ctx, queue.StreamRender, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRANGE %s: %v", queue.StreamRender, err)
	}
	if len(msgs) != 2 {
		t.Fatalf("q:render messages = %d, want 2", len(msgs))
	}
	got := make([]int64, 0, 2)
	for _, m := range msgs {
		var r queue.Result
		if err := json.Unmarshal([]byte(m.Values["data"].(string)), &r); err != nil {
			t.Fatalf("parsing message %s: %v", m.ID, err)
		}
		got = append(got, r.DomainID)
	}
	if got[0] != prioID || got[1] != rfID {
		t.Fatalf("the publication order %v, expected [the priority one %d, .рф %d]", got, prioID, rfID)
	}
}

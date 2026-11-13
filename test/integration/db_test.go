// Integration tests of the DB layer (T-007): the internal/db pool and bulk
// write helpers — streaming COPY in batches and batched UPDATE … FROM
// unnest(...) — against the live PostgreSQL of the dev environment.
//
// Isolation: the tests run in their own db_test_* schema with an items
// table (zone_id+id keys, like the tables of the spec 6.1 schema); the
// working crawler schema is untouched (as in the migration tests but
// without applying migrations — the DB layer does not need the project
// schema).
package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
	"crawler/internal/db"
)

// dbTestEnv creates a schema with an items table and returns a pool with
// search_path set to it; the schema is dropped when the test finishes.
func dbTestEnv(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	base, err := pgxpool.New(ctx, pgDSN(t))
	if err != nil {
		t.Fatalf("PostgreSQL connection: %v", err)
	}

	schema := fmt.Sprintf("db_test_%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	if _, err := base.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	if _, err := base.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	const table = `CREATE TABLE %s.items (
		zone_id   smallint NOT NULL,
		id        bigint   NOT NULL,
		status    smallint NOT NULL DEFAULT 0,
		data      text,
		queued_at timestamptz,
		PRIMARY KEY (zone_id, id))`
	if _, err := base.Exec(ctx, fmt.Sprintf(table, schema)); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// The pool is opened by the package itself from the postgres section — as in the services.
	pool, err := db.Open(ctx, db.NewOptions(config.Postgres{
		DSN:      pgDSN(t) + "&search_path=" + schema,
		PoolSize: 4,
	}))
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		if _, err := base.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			t.Errorf("schema %s cleanup: %v", schema, err)
		}
		base.Close()
	})
	return pool
}

// itemData — row data: COPY special characters (tab, newline, quotes)
// and unicode, to check escaping without distortion.
func itemData(i int64) string {
	return fmt.Sprintf("row-%06d \tданные\nюникод «сайт-проверка» \"в кавычках\"", i)
}

// itemsIter — iterator over n rows (zone_id=1, id=0..n-1); onCancel is
// called before yielding row rowIdx (context cancellation, etc.).
func itemsIter(n int64, onCancel func(rowIdx int64)) db.RowIter {
	i := int64(0)
	return func() ([]any, error) {
		if i >= n {
			return nil, io.EOF
		}
		if onCancel != nil {
			onCancel(i)
		}
		row := []any{int16(1), i, itemData(i)}
		i++
		return row, nil
	}
}

var itemsCopyColumns = []string{"zone_id", "id", "data"}

// itemsCopy copies n rows into items in batches of batchSize (test data).
func itemsCopy(t *testing.T, ctx context.Context, pool *pgxpool.Pool, n int64, batchSize int) {
	t.Helper()
	total, err := db.CopyFromStream(ctx, pool, pgx.Identifier{"items"}, itemsCopyColumns, batchSize, itemsIter(n, nil))
	if err != nil {
		t.Fatalf("COPY %d rows: %v", n, err)
	}
	if total != n {
		t.Fatalf("COPY returned %d, want %d", total, n)
	}
}

func countItems(t *testing.T, pool *pgxpool.Pool, where string) int {
	t.Helper()
	var n int
	query := "SELECT count(*) FROM items"
	if where != "" {
		query += " WHERE " + where
	}
	if err := pool.QueryRow(context.Background(), query).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// TestDB_PoolOpen — the pool from the config's postgres section opens
// (the fail-fast Ping has already passed), settings are applied, queries work.
func TestDB_PoolOpen(t *testing.T) {
	skipUnlessIntegration(t)
	pool := dbTestEnv(t)

	if max := pool.Config().MaxConns; max != 4 {
		t.Errorf("MaxConns = %d, want 4 (pool_size from NewOptions)", max)
	}
	var one int
	if err := pool.QueryRow(context.Background(), "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Errorf("SELECT 1: %d, err=%v", one, err)
	}
	if total := pool.Stat().TotalConns(); total < 1 {
		t.Errorf("TotalConns = %d, want >= 1 after a query", total)
	}
}

// TestDB_CopyFromStream — a real COPY: batches of 100 rows (1005 rows =
// 11 COPY calls), data with special characters is not distorted, the
// DEFAULT of columns outside the COPY list is applied.
func TestDB_CopyFromStream(t *testing.T) {
	skipUnlessIntegration(t)
	pool := dbTestEnv(t)
	ctx := context.Background()

	const n = 1005
	itemsCopy(t, ctx, pool, n, 100)

	if got := countItems(t, pool, ""); got != n {
		t.Errorf("rows in items = %d, want %d", got, n)
	}

	var data string
	if err := pool.QueryRow(ctx, "SELECT data FROM items WHERE id = $1", int64(500)).Scan(&data); err != nil {
		t.Fatal(err)
	}
	if want := itemData(500); data != want {
		t.Errorf("row 500 data = %q, want %q (did COPY distort the special characters?)", data, want)
	}

	var status int
	if err := pool.QueryRow(ctx, "SELECT status FROM items WHERE id = 0").Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != 0 {
		t.Errorf("status = %d, want 0 (a DEFAULT column outside the COPY list)", status)
	}
}

// TestDB_CopyFromStream_Cancel — context cancellation in the middle of
// the stream: completed batches stay in the DB, the incomplete one is not
// written, and an error with the context and the number of committed rows
// is returned.
func TestDB_CopyFromStream_Cancel(t *testing.T) {
	skipUnlessIntegration(t)
	pool := dbTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cancel before row 30 (the 31st): rows 0–29 = 3 full batches of 10.
	it := itemsIter(100, func(rowIdx int64) {
		if rowIdx == 30 {
			cancel()
		}
	})
	total, err := db.CopyFromStream(ctx, pool, pgx.Identifier{"items"}, itemsCopyColumns, 10, it)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if total != 30 {
		t.Errorf("copied %d, want 30 (the batches before the cancel)", total)
	}
	if got := countItems(t, pool, ""); got != 30 {
		t.Errorf("rows in items = %d, want 30 (an unfinished chunk not written)", got)
	}
}

// TestDB_UpdateUnnest — batched UPDATE via unnest: bulk status change,
// the Where guard (a repeat — 0 rows), different values per row.
func TestDB_UpdateUnnest(t *testing.T) {
	skipUnlessIntegration(t)
	pool := dbTestEnv(t)
	ctx := context.Background()

	const n = 60
	itemsCopy(t, ctx, pool, n, 50)

	zones := make([]any, n)
	ids := make([]any, n)
	for i := range n {
		zones[i] = int16(1)
		ids[i] = int64(i)
	}
	five := make([]any, n)
	for i := range five {
		five[i] = int16(5)
	}

	// Move all to status 5 with a timestamp; only from status 0.
	got, err := db.Update("items").
		Key(db.Column{Name: "zone_id", Type: "int2"}, db.Column{Name: "id", Type: "int8"}).
		Set(db.Column{Name: "status", Type: "int2"}).
		SetExpr("queued_at = now()").
		Where("t.status = 0").
		Exec(ctx, pool, zones, ids, five)
	if err != nil {
		t.Fatal(err)
	}
	if got != n {
		t.Errorf("updated %d, want %d", got, n)
	}
	if c := countItems(t, pool, "status = 5 AND queued_at IS NOT NULL"); c != n {
		t.Errorf("status=5 with the mark: %d, want %d", c, n)
	}

	// A repeat with the same guard — zero rows (the Where guard protects against races).
	if got, err = db.Update("items").
		Key(db.Column{Name: "zone_id", Type: "int2"}, db.Column{Name: "id", Type: "int8"}).
		SetExpr("status = 6").
		Where("t.status = 0").
		Exec(ctx, pool, zones, ids); err != nil || got != 0 {
		t.Errorf("the repeat move: (%d, %v), want (0, nil)", got, err)
	}

	// Different values per batch row: even → 7, odd → 9.
	mixed := make([]any, n)
	for i := range mixed {
		mixed[i] = int16(7)
		if i%2 == 1 {
			mixed[i] = int16(9)
		}
	}
	got, err = db.Update("items").
		Key(db.Column{Name: "zone_id", Type: "int2"}, db.Column{Name: "id", Type: "int8"}).
		Set(db.Column{Name: "status", Type: "int2"}).
		Where("t.status = 5").
		Exec(ctx, pool, zones, ids, mixed)
	if err != nil || got != n {
		t.Fatalf("the move of 5: (%d, %v), want (%d, nil)", got, err, n)
	}
	if c := countItems(t, pool, "status = 7"); c != n/2 {
		t.Errorf("status=7: %d, want %d", c, n/2)
	}
	if c := countItems(t, pool, "status = 9"); c != n/2 {
		t.Errorf("status=9: %d, want %d", c, n/2)
	}
}

// TestDB_UpdateUnnestReturning — Query with RETURNING returns the keys of
// the rows actually changed under the guard (the scheduler pattern of
// spec 5.3: enqueueing exactly the changed domains).
func TestDB_UpdateUnnestReturning(t *testing.T) {
	skipUnlessIntegration(t)
	pool := dbTestEnv(t)
	ctx := context.Background()

	const n = 20
	itemsCopy(t, ctx, pool, n, 100)

	zones := make([]any, n)
	evenIDs := make([]any, 0, n/2)
	for i := range n {
		zones[i] = int16(1)
		if i%2 == 0 {
			evenIDs = append(evenIDs, int64(i))
		}
	}
	evenZones := make([]any, len(evenIDs))
	for i := range evenZones {
		evenZones[i] = int16(1)
	}

	rows, err := db.Update("items").
		Key(db.Column{Name: "zone_id", Type: "int2"}, db.Column{Name: "id", Type: "int8"}).
		SetExpr("status = 3").
		Where("t.status = 0 AND t.id % 2 = 0").
		Returning("t.id").
		Query(ctx, pool, evenZones, evenIDs)
	if err != nil {
		t.Fatal(err)
	}
	got := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		got[id] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("RETURNING iteration: %v", err)
	}
	rows.Close()

	if len(got) != n/2 {
		t.Fatalf("RETURNING returned %d rows, want %d", len(got), n/2)
	}
	for i := 0; i < n; i += 2 {
		if !got[int64(i)] {
			t.Errorf("id %d missing from RETURNING", i)
		}
	}
	if c := countItems(t, pool, "status = 3"); c != n/2 {
		t.Errorf("status=3: %d, want %d", c, n/2)
	}
}

// TestDB_UpdateUnnest_BeyondParamLimit — a batch larger than the protocol
// parameter limit (65535): each column's array is a single unnest
// argument, so 70,000 rows are updated by one query. Regression contract:
// the builder must not fall apart into a parameter per row.
func TestDB_UpdateUnnest_BeyondParamLimit(t *testing.T) {
	skipUnlessIntegration(t)
	pool := dbTestEnv(t)
	ctx := context.Background()

	const n = 70_000
	itemsCopy(t, ctx, pool, n, 10_000)

	zones := make([]any, n)
	for i := range zones {
		zones[i] = int16(1)
	}

	got, err := db.Update("items").
		Key(db.Column{Name: "zone_id", Type: "int2"}, db.Column{Name: "id", Type: "int8"}).
		SetExpr("status = 1").
		Exec(ctx, pool, zones, idRange(n))
	if err != nil {
		t.Fatalf("updating %d rows in one query: %v", n, err)
	}
	if got != n {
		t.Errorf("updated %d, want %d", got, n)
	}
	if c := countItems(t, pool, "status = 1"); c != n {
		t.Errorf("status=1: %d, want %d", c, n)
	}
}

// idRange — array of ids 0..n-1 (the batch keys).
func idRange(n int64) []any {
	ids := make([]any, n)
	for i := range n {
		ids[i] = i
	}
	return ids
}

// TestDB_AfterConnectLockTimeout — the LockTimeout hook (T-192): a pool
// session sees the lock_timeout cap (SHOW), and an operation waiting for
// a row longer than the cap gets SQLSTATE 55P03 (lock_not_available) — a
// server error, not a context timeout; after the row is released the same
// statement passes normally. The test hook's cap is 1 s (DefaultLockTimeout
// is a unit test): the scenario is quick, the semantics are the same.
func TestDB_AfterConnectLockTimeout(t *testing.T) {
	skipUnlessIntegration(t)
	// The context is wider than the cap (1 s): 55P03 must come from the
	// server, not from context cancellation.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	schema := fmt.Sprintf("db_locktimeout_%d_%d", os.Getpid(), time.Now().UnixNano()%1_000_000)
	// base — the row holder and DDL: search_path goes into the DSN right
	// away (the schema is created below by its full name — the GUC is not
	// validated on connect), otherwise the seed and the holder would work
	// with public.
	base, err := pgxpool.New(ctx, pgDSN(t)+"&search_path="+schema)
	if err != nil {
		t.Fatalf("PostgreSQL connection: %v", err)
	}
	if _, err := base.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	if _, err := base.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	const table = `CREATE TABLE %s.items (
		zone_id smallint NOT NULL,
		id      bigint  NOT NULL,
		PRIMARY KEY (zone_id, id))`
	if _, err := base.Exec(ctx, fmt.Sprintf(table, schema)); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// The row holder and the victim pool; cleanup closes everything alive.
	var pool *pgxpool.Pool
	var holder *pgxpool.Conn
	t.Cleanup(func() {
		if pool != nil {
			pool.Close()
		}
		if holder != nil {
			_, _ = holder.Exec(context.Background(), "ROLLBACK")
			holder.Release()
		}
		if _, err := base.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			t.Errorf("schema %s cleanup: %v", schema, err)
		}
		base.Close()
	})

	// A pool with the hook — as in the write services (T-192): db.Open
	// runs AfterConnect already on the verifying Ping.
	pool, err = db.Open(ctx, db.Options{
		DSN:          pgDSN(t) + "&search_path=" + schema,
		PoolSize:     1,
		AfterConnect: db.LockTimeout(time.Second),
	})
	if err != nil {
		t.Fatalf("db.Open with the AfterConnect LockTimeout: %v", err)
	}

	// Seed: a committed row — the conflict is over it (the lock on the
	// committed row held by the holder; the victim simply does not see the
	// uncommitted INSERT under READ COMMITTED — no conflict).
	if _, err := base.Exec(ctx, "INSERT INTO items (zone_id, id) VALUES (1, 1)"); err != nil {
		t.Fatalf("the seed: inserting a row: %v", err)
	}

	// The row holder — a second session with an open transaction
	// (BEGIN + SELECT FOR UPDATE of the committed row): the victim waits
	// on ShareLock on transaction — the same wait class as in the lock
	// convoy of Oct 25 (entries #2/#6 of the slow-query log).
	holder, err = base.Acquire(ctx)
	if err != nil {
		t.Fatalf("the holder's connection: %v", err)
	}
	if _, err := holder.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("the holder: BEGIN: %v", err)
	}
	if _, err := holder.Exec(ctx, "SELECT zone_id FROM items WHERE zone_id = 1 AND id = 1 FOR UPDATE"); err != nil {
		t.Fatalf("the holder: grabbing the row: %v", err)
	}

	// The pool session sees the hook's cap (1000ms → "1s").
	var lt string
	if err := pool.QueryRow(ctx, "SHOW lock_timeout").Scan(&lt); err != nil {
		t.Fatalf("SHOW lock_timeout: %v", err)
	}
	if lt != "1s" {
		t.Errorf("the pool session's lock_timeout = %q, want 1s (the AfterConnect hook)", lt)
	}

	// The victim: SELECT FOR UPDATE waits on the holder's row longer than the cap.
	_, err = pool.Exec(ctx, "SELECT zone_id FROM items WHERE zone_id = 1 AND id = 1 FOR UPDATE")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("the row wait error = %v, want SQLSTATE 55P03 (lock_not_available)", err)
	}
	if ctx.Err() != nil {
		t.Fatal("the context expired before the lock_timeout cap: the error is not from the server")
	}

	// Row release (the holder's rollback) — the same statement passes
	// without an error: the cap cuts only the wait, the normal path is intact.
	if _, err := holder.Exec(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("the holder: ROLLBACK: %v", err)
	}
	if _, err := pool.Exec(ctx, "SELECT zone_id FROM items WHERE zone_id = 1 AND id = 1 FOR UPDATE"); err != nil {
		t.Fatalf("after the row release: %v", err)
	}
}

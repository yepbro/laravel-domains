// Unit tests of the background cleanup of deleted domains (T-020, spec
// 5.1, 13.1, 8) on a fake pgx pool with transactions: exact SQL texts and
// arguments (DELETE subdomain_results / UPDATE contents refcount),
// chunking, hash aggregation, repeated passes, the empty set, errors and
// ctx cancellation, the CleanupDeleted loop. Behavior against a live DB —
// the ingest integration tests (T-023).
package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/model"
)

// --- fake pool with transactions -----------------------------------------

// cleanupCall — a recorded SQL call with its arguments.
type cleanupCall struct {
	sql  string
	args []any
}

// cleanupFakeRows — pgx.Rows with prepared rows; Scan is positional, in
// the SQL column order.
type cleanupFakeRows struct {
	rows   [][]any
	i      int
	closed bool
	err    error // Err() after the rows are exhausted
}

func cleanupRows(rows ...[]any) *cleanupFakeRows {
	return &cleanupFakeRows{rows: rows}
}

func (r *cleanupFakeRows) Close() { r.closed = true }

func (r *cleanupFakeRows) Err() error { return r.err }

func (r *cleanupFakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *cleanupFakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *cleanupFakeRows) Next() bool {
	if r.closed || r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *cleanupFakeRows) Scan(dest ...any) error {
	if r.closed || r.i == 0 || r.i > len(r.rows) {
		return errors.New("cleanupFakeRows: Scan without Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("cleanupFakeRows: %d destinations, %d columns", len(dest), len(row))
	}
	for i, d := range dest {
		target := reflect.ValueOf(d).Elem()
		if row[i] == nil {
			target.Set(reflect.Zero(target.Type()))
			continue
		}
		target.Set(reflect.ValueOf(row[i]))
	}
	return nil
}

func (r *cleanupFakeRows) Values() ([]any, error) { return r.rows[r.i-1], nil }

func (r *cleanupFakeRows) RawValues() [][]byte { return nil }

// Conn and TypeMap — the part of pgx.Rows not used by the positional
// Scan.
func (r *cleanupFakeRows) Conn() *pgx.Conn      { return nil }
func (r *cleanupFakeRows) TypeMap() *pgtype.Map { return nil }

// cleanupFakePool — a CleanupDB without a DB: queues of prepared SELECT
// candidate results and DELETE … RETURNING results, errors by the pass
// steps, a call protocol. Everything is under a lock: the CleanupDeleted
// loop runs in a goroutine, the test waits for progress by the counters.
type cleanupFakePool struct {
	mu         sync.Mutex
	selects    []*cleanupFakeRows // the queue of SELECT results (exhausted — an empty set)
	deletes    []*cleanupFakeRows // the queue of DELETE … RETURNING results
	selectErrs []error            // the queue of SELECT errors (nil — a pass without an error)
	deleteErr  error
	updateErr  error
	beginErr   error
	commitErr  error

	execs     []pipeExec // Exec results in order — the cleanup of orphaned staging batches (T-098)
	execI     int
	execCalls []cleanupCall // Execs through the pool

	queries []cleanupCall // SELECTs through the pool
	begins  int
	txs     []*cleanupFakeTx
}

func (p *cleanupFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom must not be called")
}

// Exec serves the cleanup of orphaned batches (CleanupStaleBatches,
// T-098): the result is taken from the queue, the end of the queue is
// DELETE 0 (nothing to clean). The call is recorded before the ctx check
// — like Query: after cancel() the loop may start one more pass (the
// select branch choice is random), and an unrecorded Exec would loosen
// the "loop passes = cleanups" correspondence in the tests. CleanupOnce
// itself does not Exec through the pool — the pass tests verify this
// (execCalls is empty).
func (p *cleanupFakePool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.execCalls = append(p.execCalls, cleanupCall{sql: sql, args: args})
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	i := p.execI
	p.execI++
	if i < len(p.execs) {
		if p.execs[i].err != nil {
			return pgconn.CommandTag{}, p.execs[i].err
		}
		return p.execs[i].tag, nil
	}
	return pgconn.NewCommandTag("DELETE 0"), nil
}

func (p *cleanupFakePool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.queries = append(p.queries, cleanupCall{sql: sql, args: args})
	if len(p.selectErrs) > 0 {
		err := p.selectErrs[0]
		p.selectErrs = p.selectErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	if len(p.selects) > 0 {
		rows := p.selects[0]
		p.selects = p.selects[1:]
		return rows, nil
	}
	return cleanupRows(), nil
}

func (p *cleanupFakePool) Begin(context.Context) (pgx.Tx, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.begins++
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	tx := &cleanupFakeTx{pool: p}
	p.txs = append(p.txs, tx)
	return tx, nil
}

// stats — a snapshot of the counters under the lock (waiting for
// progress in the loop tests).
type cleanupStats struct {
	selects int
	begins  int
	commits int
}

func (p *cleanupFakePool) stats() cleanupStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := cleanupStats{selects: len(p.queries), begins: p.begins}
	for _, tx := range p.txs {
		if tx.committed {
			s.commits++
		}
	}
	return s
}

// cleanupFakeTx — the fake pool's transaction: remembers the calls and
// the outcome.
type cleanupFakeTx struct {
	pgx.Tx // the pass does not use the rest of the pgx.Tx methods

	pool       *cleanupFakePool
	calls      []cleanupCall
	committed  bool
	rolledBack bool
}

func (t *cleanupFakeTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.calls = append(t.calls, cleanupCall{sql: sql, args: args})
	if t.pool.updateErr != nil {
		return pgconn.CommandTag{}, t.pool.updateErr
	}
	// UPDATE with unnest arrays: the number of rows affected = the number
	// of hashes.
	hashes := args[0].([][]byte)
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", len(hashes))), nil
}

func (t *cleanupFakeTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.calls = append(t.calls, cleanupCall{sql: sql, args: args})
	if t.pool.deleteErr != nil {
		return nil, t.pool.deleteErr
	}
	if len(t.pool.deletes) > 0 {
		rows := t.pool.deletes[0]
		t.pool.deletes = t.pool.deletes[1:]
		return rows, nil
	}
	return cleanupRows(), nil
}

func (t *cleanupFakeTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.pool.commitErr != nil {
		return t.pool.commitErr
	}
	t.committed = true
	return nil
}

func (t *cleanupFakeTx) Rollback(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if !t.committed {
		t.rolledBack = true
	}
	return nil
}

// compile-time: the fake pool is a CleanupDB for tests without a DB.
var _ CleanupDB = (*cleanupFakePool)(nil)

// --- expected SQL texts (the contract) -----------------------------------
//
// The deleted status code is baked into the text as a literal (T-170:
// the planner proves the partial index predicate of domains_deleted_idx
// only by a literal), the arguments are only the chunk's data.

var wantCleanupSelect = fmt.Sprintf(
	"SELECT d.zone_id, d.id FROM domains d "+
		"WHERE d.status = %d "+
		"AND EXISTS (SELECT 1 FROM subdomain_results r "+
		"WHERE r.zone_id = d.zone_id AND r.domain_id = d.id) "+
		"ORDER BY d.zone_id, d.id LIMIT $1",
	int16(model.StatusDeleted))

var wantCleanupDelete = fmt.Sprintf(
	"DELETE FROM subdomain_results r "+
		"USING unnest($1::int2[], $2::int8[]) AS k(zone_id, domain_id) "+
		"WHERE r.zone_id = k.zone_id AND r.domain_id = k.domain_id "+
		"AND EXISTS (SELECT 1 FROM domains d "+
		"WHERE d.zone_id = k.zone_id AND d.id = k.domain_id AND d.status = %d) "+
		"RETURNING r.zone_id, r.domain_id, r.content_hash",
	int16(model.StatusDeleted))

const wantCleanupUpdate = "UPDATE contents SET refcount = GREATEST(refcount - h.n, 0) " +
	"FROM unnest($1::bytea[], $2::int4[]) AS h(hash, n) " +
	"WHERE contents.hash = h.hash"

// wantStaleStageSQL — the contract of the orphaned batch cleanup
// (T-098): the range over the batch_id prefix (StageTable.Sanitize —
// "ingest_stage").
const wantStaleStageSQL = "DELETE FROM \"ingest_stage\" WHERE batch_id < $1"

// cleanupHash16 — a 16-octet content_hash with the given first byte.
func cleanupHash16(b byte) []byte {
	h := make([]byte, contentHashLen)
	h[0] = b
	return h
}

// --- CleanupOnce ---------------------------------------------------------

func TestCleanupOnceEmpty(t *testing.T) {
	pool := &cleanupFakePool{} // the SELECT queue is empty — no candidates

	res, err := CleanupOnce(context.Background(), pool, CleanupOptions{})
	if err != nil {
		t.Fatalf("CleanupOnce: %v", err)
	}
	if res != (CleanupResult{}) {
		t.Errorf("the outcome = %+v, want zero: nothing to clean", res)
	}
	if len(pool.queries) != 1 || pool.queries[0].sql != wantCleanupSelect {
		t.Errorf("the SELECT = %v, want a single candidate query", pool.queries)
	}
	if pool.begins != 0 {
		t.Errorf("transactions = %d, want 0: an empty set opens no transaction", pool.begins)
	}
}

func TestCleanupOnceSQLAndFlow(t *testing.T) {
	hashA := cleanupHash16(0xaa)
	hashB := cleanupHash16(0xbb)
	pool := &cleanupFakePool{
		selects: []*cleanupFakeRows{cleanupRows(
			[]any{int16(1), int64(10)},
			[]any{int16(1), int64(11)},
			[]any{int16(2), int64(20)},
		)},
		deletes: []*cleanupFakeRows{cleanupRows(
			[]any{int16(1), int64(10), hashA},
			[]any{int16(1), int64(10), nil}, // a result without content
			[]any{int16(1), int64(11), hashA},
			[]any{int16(2), int64(20), hashB},
		)},
	}

	res, err := CleanupOnce(context.Background(), pool, CleanupOptions{BatchSize: 7})
	if err != nil {
		t.Fatalf("CleanupOnce: %v", err)
	}

	// Candidate selection: the deleted status and the chunk limit.
	if len(pool.queries) != 1 {
		t.Fatalf("SELECTs = %d, want 1", len(pool.queries))
	}
	if pool.queries[0].sql != wantCleanupSelect {
		t.Errorf("the select SQL:\n%s\nwant:\n%s", pool.queries[0].sql, wantCleanupSelect)
	}
	if want := []any{7}; !reflect.DeepEqual(pool.queries[0].args, want) {
		t.Errorf("the select arguments = %v, want %v (the chunk limit; the deleted status is a literal in SQL, T-170)", pool.queries[0].args, want)
	}

	// Transaction: deleting the results + decreasing the refcount.
	if pool.begins != 1 || len(pool.txs) != 1 {
		t.Fatalf("transactions = %d, want 1", pool.begins)
	}
	tx := pool.txs[0]
	if len(tx.calls) != 2 {
		t.Fatalf("calls in the transaction = %d, want 2 (DELETE + UPDATE)", len(tx.calls))
	}
	if tx.calls[0].sql != wantCleanupDelete {
		t.Errorf("the delete SQL:\n%s\nwant:\n%s", tx.calls[0].sql, wantCleanupDelete)
	}
	if want := []any{[]int16{1, 1, 2}, []int64{10, 11, 20}}; !reflect.DeepEqual(tx.calls[0].args, want) {
		t.Errorf("the delete arguments = %v, want %v (the deleted status is a literal in SQL, T-170)", tx.calls[0].args, want)
	}
	if tx.calls[1].sql != wantCleanupUpdate {
		t.Errorf("SQL refcount:\n%s\nwant:\n%s", tx.calls[1].sql, wantCleanupUpdate)
	}
	// The hashes are aggregated by the number of deleted references, the
	// NULL is skipped, the order is deterministic.
	if want := []any{[][]byte{hashA, hashB}, []int32{2, 1}}; !reflect.DeepEqual(tx.calls[1].args, want) {
		t.Errorf("the refcount arguments = %v, want %v", tx.calls[1].args, want)
	}

	want := CleanupResult{Domains: 3, Results: 4, Contents: 2}
	if res != want {
		t.Errorf("the outcome = %+v, want %+v", res, want)
	}
	if !tx.committed || tx.rolledBack {
		t.Errorf("the transaction: committed=%v rolledBack=%v, want a commit without a rollback", tx.committed, tx.rolledBack)
	}
	// The domain cleanup pass does not write to the pool directly: Exec
	// is only the channel of the orphaned batch cleanup (T-098).
	if len(pool.execCalls) != 0 {
		t.Errorf("Execs through the pool = %v, want none: CleanupOnce works in a transaction", pool.execCalls)
	}
}

// TestCleanupOnceDefaultBatch — a zero chunk size is replaced with the
// default.
func TestCleanupOnceDefaultBatch(t *testing.T) {
	pool := &cleanupFakePool{}

	if _, err := CleanupOnce(context.Background(), pool, CleanupOptions{}); err != nil {
		t.Fatalf("CleanupOnce: %v", err)
	}
	got := pool.queries[0].args[0]
	if got != DefaultCleanupBatch {
		t.Errorf("the chunk limit = %v, want the default %d", got, DefaultCleanupBatch)
	}
}

// TestCleanupOnceRepeatPasses — a cleaned domain is not selected: the
// repeated pass sees an empty set and does not open a transaction.
func TestCleanupOnceRepeatPasses(t *testing.T) {
	pool := &cleanupFakePool{
		selects: []*cleanupFakeRows{cleanupRows([]any{int16(3), int64(30)})},
		deletes: []*cleanupFakeRows{cleanupRows([]any{int16(3), int64(30), nil})},
	}

	first, err := CleanupOnce(context.Background(), pool, CleanupOptions{})
	if err != nil {
		t.Fatalf("the first pass: %v", err)
	}
	if first.Domains != 1 || first.Results != 1 {
		t.Fatalf("the first pass = %+v, want 1 domain and 1 result", first)
	}

	second, err := CleanupOnce(context.Background(), pool, CleanupOptions{})
	if err != nil {
		t.Fatalf("the second pass: %v", err)
	}
	if second != (CleanupResult{}) {
		t.Errorf("the second pass = %+v, want zero", second)
	}
	if len(pool.queries) != 2 {
		t.Errorf("SELECTs = %d, want 2 (one per pass)", len(pool.queries))
	}
	if pool.begins != 1 {
		t.Errorf("transactions = %d, want 1: a repeated pass opens no new one", pool.begins)
	}
}

// TestCleanupOnceBadHash — a content_hash of not 16 octets (the contents
// contract, spec 6.1) does not crash the pass (poison pill, ING-P2-4):
// the row is already deleted, its refcount is not changed, the
// transaction is committed, and the anomaly is visible in
// CleanupResult.BadHashes.
func TestCleanupOnceBadHash(t *testing.T) {
	t.Run("a junk hash next to a correct one", func(t *testing.T) {
		pool := &cleanupFakePool{
			selects: []*cleanupFakeRows{cleanupRows([]any{int16(1), int64(10)})},
			deletes: []*cleanupFakeRows{cleanupRows(
				[]any{int16(1), int64(10), make([]byte, 15)},
				[]any{int16(1), int64(10), cleanupHash16(0xaa)},
			)},
		}

		res, err := CleanupOnce(context.Background(), pool, CleanupOptions{})
		if err != nil {
			t.Fatalf("CleanupOnce: %v, want a pass without an error", err)
		}
		if res.BadHashes != 1 || res.Domains != 1 || res.Results != 2 || res.Contents != 1 {
			t.Errorf("the outcome = %+v, want bad_hashes 1, domains 1, results 2, contents 1", res)
		}
		tx := pool.txs[0]
		if len(tx.calls) != 2 {
			t.Fatalf("calls in the transaction %d, want 2 (DELETE + UPDATE for the correct hash)", len(tx.calls))
		}
		if want := []any{[][]byte{cleanupHash16(0xaa)}, []int32{1}}; !reflect.DeepEqual(tx.calls[1].args, want) {
			t.Errorf("the refcount arguments = %v, want the correct hash only", tx.calls[1].args)
		}
		if !tx.committed || tx.rolledBack {
			t.Errorf("the transaction: committed=%v rolledBack=%v, want a commit without a rollback", tx.committed, tx.rolledBack)
		}
	})
	t.Run("all hashes are malformed — the pass continues", func(t *testing.T) {
		pool := &cleanupFakePool{
			selects: []*cleanupFakeRows{cleanupRows(
				[]any{int16(1), int64(10)},
				[]any{int16(2), int64(20)},
			)},
			deletes: []*cleanupFakeRows{cleanupRows(
				[]any{int16(1), int64(10), make([]byte, 3)},
				[]any{int16(2), int64(20), nil},
			)},
		}

		res, err := CleanupOnce(context.Background(), pool, CleanupOptions{})
		if err != nil {
			t.Fatalf("CleanupOnce: %v, want a pass without an error", err)
		}
		if res.BadHashes != 1 || res.Domains != 2 || res.Contents != 0 {
			t.Errorf("the outcome = %+v, want bad_hashes 1, domains 2, contents 0", res)
		}
		// No references for the refcount — the UPDATE does not run, but
		// the pass is committed: the domains are cleaned, the next passes
		// will not select them.
		tx := pool.txs[0]
		if len(tx.calls) != 1 {
			t.Fatalf("calls in the transaction %d, want 1 (DELETE only)", len(tx.calls))
		}
		if !tx.committed {
			t.Error("the transaction was not committed — the candidates will be selected again")
		}
	})
}

func TestCleanupOnceErrors(t *testing.T) {
	newPool := func() *cleanupFakePool {
		return &cleanupFakePool{
			selects: []*cleanupFakeRows{cleanupRows([]any{int16(1), int64(10)})},
			deletes: []*cleanupFakeRows{cleanupRows([]any{int16(1), int64(10), cleanupHash16(1)})},
		}
	}

	tests := []struct {
		name    string
		pool    *cleanupFakePool
		context string // the expected fragment of the message
	}{
		{"a SELECT error", &cleanupFakePool{selectErrs: []error{errors.New("db unavailable")}}, "the candidate selection"},
		{"a Begin error", func() *cleanupFakePool {
			p := newPool()
			p.beginErr = errors.New("db unavailable")
			return p
		}(), "transaction"},
		{"a DELETE error", func() *cleanupFakePool {
			p := newPool()
			p.deleteErr = errors.New("db unavailable")
			return p
		}(), "deleting the results"},
		{"an UPDATE error", func() *cleanupFakePool {
			p := newPool()
			p.updateErr = errors.New("db unavailable")
			return p
		}(), "refcount"},
		{"a Commit error", func() *cleanupFakePool {
			p := newPool()
			p.commitErr = errors.New("db unavailable")
			return p
		}(), "commit"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CleanupOnce(context.Background(), tt.pool, CleanupOptions{})
			if err == nil {
				t.Fatal("an error is expected")
			}
			if !strings.Contains(err.Error(), "ingest: deleted-domains cleanup") ||
				!strings.Contains(err.Error(), tt.context) ||
				!strings.Contains(err.Error(), "db unavailable") {
				t.Errorf("the message = %q, want the context %q and the original error", err, tt.context)
			}
			for _, tx := range tt.pool.txs {
				if tx.committed {
					t.Errorf("the transaction committed on the %q error", tt.name)
				}
			}
		})
	}
}

// TestCleanupOnceCtxCanceled — the context cancellation goes through both
// steps of the pass as an error (errors.Is), the transaction is not
// committed.
func TestCleanupOnceCtxCanceled(t *testing.T) {
	selectPool := &cleanupFakePool{selectErrs: []error{context.Canceled}}
	if _, err := CleanupOnce(context.Background(), selectPool, CleanupOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("the select error = %v, want context.Canceled", err)
	}

	deletePool := &cleanupFakePool{
		selects: []*cleanupFakeRows{cleanupRows([]any{int16(1), int64(10)})},
		deletes: []*cleanupFakeRows{cleanupRows()},
	}
	deletePool.deleteErr = context.Canceled
	if _, err := CleanupOnce(context.Background(), deletePool, CleanupOptions{}); !errors.Is(err, context.Canceled) {
		t.Errorf("the delete error = %v, want context.Canceled", err)
	}
	if deletePool.txs[0].committed {
		t.Error("the transaction committed on a context cancellation")
	}
}

func TestCleanupOnceNilPool(t *testing.T) {
	if _, err := CleanupOnce(context.Background(), nil, CleanupOptions{}); err == nil ||
		!strings.Contains(err.Error(), "the pool is not set") {
		t.Errorf("the error = %v, want «the pool is not set»", err)
	}
}

// --- CleanupStaleBatches (T-098) -----------------------------------------

// TestCleanupStaleBatchesSQLAndArg — the cleanup contract: the SQL range
// over the batch_id prefix and the exact "now − StaleBatchAge" bound (a
// fixed now makes the argument deterministic). The "old batch is deleted,
// a fresh one lives" semantics of the range against a live DB — the
// ingest integration tests; here — the bound encoding
// (TestBatchIDAgeOrdering).
func TestCleanupStaleBatchesSQLAndArg(t *testing.T) {
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	pool := &cleanupFakePool{execs: []pipeExec{{tag: pgconn.NewCommandTag("DELETE 7")}}}

	rows, err := CleanupStaleBatches(context.Background(), pool, now)
	if err != nil {
		t.Fatalf("CleanupStaleBatches: %v", err)
	}
	if rows != 7 {
		t.Errorf("deleted rows = %d, want 7 (from CommandTag)", rows)
	}
	if len(pool.execCalls) != 1 {
		t.Fatalf("Execs = %d, want 1", len(pool.execCalls))
	}
	call := pool.execCalls[0]
	if call.sql != wantStaleStageSQL {
		t.Errorf("the sweep SQL:\n%s\nwant:\n%s", call.sql, wantStaleStageSQL)
	}
	wantArg := batchIDBefore(now.Add(-StaleBatchAge))
	if !reflect.DeepEqual(call.args, []any{wantArg[:]}) {
		t.Errorf("the sweep boundary = %x, want %x (batch_idBefore(now − %s))",
			call.args[0], wantArg[:], StaleBatchAge)
	}
}

// TestCleanupStaleBatchesNoop — an empty staging: DELETE 0 without an
// error, no extra calls.
func TestCleanupStaleBatchesNoop(t *testing.T) {
	pool := &cleanupFakePool{} // the Exec queue is empty — DELETE 0

	rows, err := CleanupStaleBatches(context.Background(), pool, time.Now())
	if err != nil {
		t.Fatalf("CleanupStaleBatches: %v", err)
	}
	if rows != 0 {
		t.Errorf("deleted rows = %d, want 0: nothing to clean", rows)
	}
	if len(pool.execCalls) != 1 || pool.begins != 0 {
		t.Errorf("calls = %v (transactions %d), want a single Exec without a transaction",
			pool.execCalls, pool.begins)
	}
}

func TestCleanupStaleBatchesErrors(t *testing.T) {
	if _, err := CleanupStaleBatches(context.Background(), nil, time.Now()); err == nil ||
		!strings.Contains(err.Error(), "the pool is not set") {
		t.Errorf("the error = %v, want «the pool is not set»", err)
	}

	boom := &cleanupFakePool{execs: []pipeExec{{err: errors.New("db unavailable")}}}
	if _, err := CleanupStaleBatches(context.Background(), boom, time.Now()); err == nil ||
		!strings.Contains(err.Error(), "ingest: purging orphaned") ||
		!strings.Contains(err.Error(), "db unavailable") {
		t.Errorf("the error = %v, want the sweep context and the original error", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	canceled := &cleanupFakePool{}
	if _, err := CleanupStaleBatches(ctx, canceled, time.Now()); !errors.Is(err, context.Canceled) {
		t.Errorf("the error = %v, want context.Canceled", err)
	}

	// An age bound before 1970 — a failure before the DB call: the uint64
	// cast of the bound would make the cleanup cut away the whole staging
	// (a P1 of round 3: it is exactly now−StaleBatchAge that is checked,
	// not now — a clock exactly on the Unix epoch gives a negative bound
	// with a positive now).
	preEpoch := &cleanupFakePool{}
	if _, err := CleanupStaleBatches(context.Background(), preEpoch, time.Unix(0, 0)); err == nil ||
		!strings.Contains(err.Error(), "before 1970") {
		t.Errorf("the error = %v, want the «the age bound is before 1970» rejection for the Unix epoch", err)
	}
	if _, err := CleanupStaleBatches(context.Background(), preEpoch, time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC)); err == nil ||
		!strings.Contains(err.Error(), "before 1970") {
		t.Errorf("the error = %v, want the «the age bound is before 1970» rejection", err)
	}
	if len(preEpoch.execCalls) != 0 {
		t.Errorf("Execs = %v, want none: the rejection happens before any DB call", preEpoch.execCalls)
	}

	// A day after the epoch the bound is positive — the cleanup works
	// even with "shifted" clocks (the contract does not require 2026).
	epochPlus := &cleanupFakePool{execs: []pipeExec{{tag: pgconn.NewCommandTag("DELETE 2")}}}
	now := time.Unix(2*86400+3600, 0) // 1970-01-03 01:00 UTC
	rows, err := CleanupStaleBatches(context.Background(), epochPlus, now)
	if err != nil {
		t.Fatalf("CleanupStaleBatches(epoch+2 days): %v", err)
	}
	if rows != 2 {
		t.Errorf("deleted rows = %d, want 2", rows)
	}
	wantArg := batchIDBefore(now.Add(-StaleBatchAge))
	if want := []any{wantArg[:]}; !reflect.DeepEqual(epochPlus.execCalls[0].args, want) {
		t.Errorf("the boundary = %x, want %x", epochPlus.execCalls[0].args[0], want[0])
	}
}

// --- CleanupOptions ------------------------------------------------------

func TestCleanupOptionsDefaults(t *testing.T) {
	got := CleanupOptions{}.withDefaults()
	want := CleanupOptions{BatchSize: DefaultCleanupBatch, Interval: DefaultCleanupInterval}
	// OnPass (a function) is not comparable with == — the whole struct is
	// compared via reflect.DeepEqual (nil == nil).
	if !reflect.DeepEqual(got, want) {
		t.Errorf("withDefaults = %+v, want %+v", got, want)
	}

	set := CleanupOptions{BatchSize: 5, Interval: time.Second}.withDefaults()
	if set.BatchSize != 5 || set.Interval != time.Second {
		t.Errorf("withDefaults overwrites the given values: %+v", set)
	}

	neg := CleanupOptions{BatchSize: -1, Interval: -time.Second}.withDefaults()
	if !reflect.DeepEqual(neg, want) {
		t.Errorf("withDefaults(negative) = %+v, want the defaults %+v", neg, want)
	}
}

// --- CleanupDeleted ------------------------------------------------------

// TestCleanupDeletedLoop — the first pass immediately, pass errors do
// not stop the loop, the ctx cancellation returns nil (the OnRun
// contract, T-012). The same passes run the orphaned staging batch
// cleanup (T-098): the first pass deletes rows (a Warn with the count),
// the second — a DB error (a Warn, the loop is alive), then — DELETE 0
// with no records.
func TestCleanupDeletedLoop(t *testing.T) {
	var buf bytes.Buffer
	logger := newCleanupTestLogger(&buf)

	pool := &cleanupFakePool{
		// Pass 1 (right after the start) — work; pass 2 — a DB error;
		// then — empty sets.
		selects: []*cleanupFakeRows{
			cleanupRows([]any{int16(1), int64(10)}),
			cleanupRows(),
		},
		selectErrs: []error{nil, errors.New("db unavailable")},
		deletes: []*cleanupFakeRows{cleanupRows(
			[]any{int16(1), int64(10), cleanupHash16(0xaa)},
			[]any{int16(1), int64(10), make([]byte, 9)}, // poison pill: a Warn, the pass is alive
		)},
		// Staging cleanup: pass 1 — 5 orphaned rows, pass 2 — a DB error,
		// then — DELETE 0 (the queue is exhausted).
		execs: []pipeExec{
			{tag: pgconn.NewCommandTag("DELETE 5")},
			{err: errors.New("db unavailable")},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- CleanupDeleted(ctx, pool, CleanupOptions{
			Interval: 2 * time.Millisecond,
			Log:      logger,
		})
	}()

	// Wait for three passes (immediately + at least two ticks) and stop.
	deadline := time.Now().Add(2 * time.Second)
	for pool.stats().selects < 3 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("the loop did not reach the third pass: %+v", pool.stats())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("CleanupDeleted: %v, want nil on the ctx cancellation", err)
	}

	st := pool.stats()
	if st.begins != 1 || st.commits != 1 {
		t.Errorf("transactions %d, commits %d, want 1 and 1: the failed and empty passes open none", st.begins, st.commits)
	}
	// Each pass is one staging cleanup; its errors do not interrupt the
	// passes.
	pool.mu.Lock()
	sweeps := len(pool.execCalls)
	pool.mu.Unlock()
	if sweeps != st.selects {
		t.Errorf("staging sweeps = %d, want = the loop's passes (%d)", sweeps, st.selects)
	}

	log := buf.String()
	for _, want := range []string{
		"the deleted-domains background cleanup", // start
		"the deleted-domains cleanup pass",       // the first pass's result
		"domains=1",                              // pass counters
		"malformed content_hash",                 // poison pill — a Warn (ING-P2-4)
		"db unavailable",                         // the second pass's error
		"orphaned staging rows were deleted",     // the cleanup: a Warn with the row count (T-098)
		"rows=5",                                 // the cleanup counter
		"older_than=" + StaleBatchAge.String(),   // the age threshold
		"purging orphaned staging batches",       // the cleanup error — a Warn, the loop is alive
	} {
		if !strings.Contains(log, want) {
			t.Errorf("the log does not contain %q:\n%s", want, log)
		}
	}
}

func TestCleanupDeletedNilPool(t *testing.T) {
	err := CleanupDeleted(context.Background(), nil, CleanupOptions{})
	if err == nil || !strings.Contains(err.Error(), "the pool is not set") {
		t.Errorf("the error = %v, want «the pool is not set»", err)
	}
}

// newCleanupTestLogger — a text logger into a buffer (checking the loop
// records).
func newCleanupTestLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, nil))
}

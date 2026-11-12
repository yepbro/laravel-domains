// Unit tests of content group commit (T-102): one fsync and one batched
// INSERT per batch, the C-1 invariants preserved (fsync before the rows are
// committed; the commit under flock — compaction cannot remove the pack of
// an uncommitted row), dedup and batch duplicates, per-item
// ErrTooLarge/ErrStoreFull, a batch failure and the idempotency of the retry.
// The style is pack_test.go/compact_test.go (a fake pool, real packs).
package content

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5"
)

// --- counting fsyncs of the active pack --------------------------------------

// syncCountFile — a packWriter counting Syncs: the group semantics "a batch
// of several Puts — one fsync" is observable without a real disk.
type syncCountFile struct {
	packWriter
	syncs *atomic.Int32
}

func (f *syncCountFile) Sync() error {
	f.syncs.Add(1)
	return f.packWriter.Sync()
}

// wrapActiveSyncs wraps the active pack's file with an fsync counter
// (after the warm-up Put that created the pack; without concurrent
// writers).
func wrapActiveSyncs(t *testing.T, st *PackStore) *atomic.Int32 {
	t.Helper()
	var syncs atomic.Int32
	st.mu.Lock()
	if st.active != nil {
		st.active.f = &syncCountFile{packWriter: st.active.f, syncs: &syncs}
	}
	st.mu.Unlock()
	return &syncs
}

// countSQL — the number of SQL calls with this text in the pool's record.
func countSQL(calls []packCall, sql string) int {
	n := 0
	for _, c := range calls {
		if c.sql == sql {
			n++
		}
	}
	return n
}

// --- SQL contract --------------------------------------------------------------

// The exact texts of the batched statements (against schema 000001, the
// TestPackSQLContract style).
const (
	wantSQLAddRefBatch = "UPDATE contents SET refcount = refcount + v.c " +
		"FROM unnest($1::bytea[], $2::int4[]) AS v(h, c) " +
		"WHERE hash = v.h AND refcount > 0 RETURNING hash"
	wantSQLPutBatch = "INSERT INTO contents (hash, pack_id, off, len, raw_len, refcount) " +
		"SELECT hash, pack_id, off, len, raw_len, n " +
		"FROM unnest($1::bytea[], $2::int4[], $3::int8[], $4::int4[], $5::int4[], $6::int4[]) " +
		"AS v(hash, pack_id, off, len, raw_len, n) " +
		"ON CONFLICT (hash) DO UPDATE SET refcount = contents.refcount + EXCLUDED.refcount, " +
		"pack_id = EXCLUDED.pack_id, off = EXCLUDED.off, " +
		"len = EXCLUDED.len, raw_len = EXCLUDED.raw_len " +
		"RETURNING hash, (xmax = 0) AS created"
)

// TestPutBatchSQLContract — the texts and arguments of the batched
// statements: hashes []byte[16] with int32 copy counters (the dedup batch),
// pack ids int32, offsets int64, lengths/counters int32; one dedup command
// and one commit per batch.
func TestPutBatchSQLContract(t *testing.T) {
	if sqlMetaAddRefBatch != wantSQLAddRefBatch || sqlMetaPutBatch != wantSQLPutBatch {
		t.Fatal("the group commit SQL changed — update the contract and schema_test")
	}

	st, pool := newPackTestStore(t, nil)
	ctx := context.Background()
	d1 := []byte("<html>first content of the SQL-contract batch</html>")
	d2 := []byte("<html>second content of the SQL-contract batch</html>")

	if _, _, err := st.PutBatch(ctx, [][]byte{d1, d2}); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	calls := pool.snapshotCalls()
	if len(calls) != 2 {
		t.Fatalf("%d DB calls, want 2 (addRefBatch, putBatch)", len(calls))
	}
	if calls[0].sql != sqlMetaAddRefBatch || calls[1].sql != sqlMetaPutBatch {
		t.Fatalf("SQL order: %q, %q — want addRefBatch, putBatch", calls[0].sql, calls[1].sql)
	}
	if len(calls[0].args) != 2 {
		t.Fatalf("addRefBatch: %d arguments, want 2 (hashes, copy counters)", len(calls[0].args))
	}
	if hashes, ok := calls[0].args[0].([][]byte); !ok || len(hashes) != 2 {
		t.Errorf("addRefBatch: argument %T(%d), want [][]byte(2)", calls[0].args[0], len(hashes))
	}
	if counts, ok := calls[0].args[1].([]int32); !ok || len(counts) != 2 {
		t.Errorf("addRefBatch: counters %T(%d), want []int32(2)", calls[0].args[1], len(counts))
	} else {
		for i, c := range counts {
			if c != 1 {
				t.Errorf("addRefBatch: counter %d = %d, want 1 (a batch without duplicates)", i, c)
			}
		}
	}
	args := calls[1].args
	if len(args) != 6 {
		t.Fatalf("putBatch: %d arguments, want 6", len(args))
	}
	hashes, ok := args[0].([][]byte)
	if !ok || len(hashes) != 2 {
		t.Fatalf("putBatch: hashes %T, want [][]byte(2)", args[0])
	}
	for _, hb := range hashes {
		if len(hb) != HashLen {
			t.Errorf("putBatch: hash %x: %d octets, want %d", hb, len(hb), HashLen)
		}
	}
	if packs, ok := args[1].([]int32); !ok || len(packs) != 2 {
		t.Errorf("putBatch: packs %T(%d), want []int32(2)", args[1], len(packs))
	}
	if offs, ok := args[2].([]int64); !ok || len(offs) != 2 {
		t.Errorf("putBatch: offsets %T(%d), want []int64(2)", args[2], len(offs))
	}
	for _, sl := range []any{args[3], args[4], args[5]} {
		if v, ok := sl.([]int32); !ok || len(v) != 2 {
			t.Errorf("putBatch: array %T, want []int32(2)", sl)
		}
	}
}

// --- group semantics --------------------------------------------------------

// TestPutBatchGroupCommit — a batch of several contents: all rows committed,
// one batched statement per batch and exactly one fsync (the active pack's
// counter); an empty item is valid content (the T-041 contract).
func TestPutBatchGroupCommit(t *testing.T) {
	st, pool := newPackTestStore(t, func(o *PackOptions) { o.PackSize = 1 << 20 })
	ctx := context.Background()

	// Warm-up: creates the pack and the active file for the counter wrapper.
	if _, err := st.Put(ctx, []byte("warmup")); err != nil {
		t.Fatal(err)
	}
	syncs := wrapActiveSyncs(t, st)

	items := [][]byte{
		nil, // empty content is valid (T-041)
		[]byte("<html>page 1 of the group commit</html>"),
		[]byte("<html>page 2 of the group commit</html>"),
		[]byte("<html>page 3 of the group commit</html>"),
		[]byte("<html>page 4 of the group commit</html>"),
	}
	results, errs, err := st.PutBatch(ctx, items)
	if err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	if len(results) != len(items) || len(errs) != len(items) {
		t.Fatalf("result sizes: %d/%d, want %d", len(results), len(errs), len(items))
	}
	offs := make(map[int64]bool, len(items))
	for i, d := range items {
		if errs[i] != nil {
			t.Fatalf("item %d: %v", i, errs[i])
		}
		want := HashOf(d)
		if results[i].Hash != want {
			t.Errorf("item %d: hash %s, want HashOf(data) = %s (contract T-041)", i, results[i].Hash, want)
		}
		if !results[i].Created {
			t.Errorf("item %d: Created = false, want true (new bytes)", i)
		}
		got, err := st.Get(ctx, want)
		if err != nil || string(got) != string(d) {
			t.Errorf("Get(%d): %v (match = %v)", i, err, string(got) == string(d))
		}
		row, ok := pool.row(want)
		if !ok {
			t.Fatalf("item %d: no metadata row", i)
		}
		if int(row.rawLen) != len(d) {
			t.Errorf("item %d: raw_len = %d, want %d", i, row.rawLen, len(d))
		}
		if offs[row.off] {
			t.Errorf("item %d: offset %d repeats — the batch blocks overlap", i, row.off)
		}
		offs[row.off] = true
	}

	if got := syncs.Load(); got != 1 {
		t.Errorf("fsyncs for a batch of %d items: %d, want 1", len(items), got)
	}
	if n := countSQL(pool.snapshotCalls(), sqlMetaAddRefBatch); n != 1 {
		t.Errorf("dedup commands per batch: %d, want 1", n)
	}
	if n := countSQL(pool.snapshotCalls(), sqlMetaPutBatch); n != 1 {
		t.Errorf("batch INSERTs per batch: %d, want 1", n)
	}
}

// TestPutBatchDuplicatesAndDedup — a mixed batch: content already live in
// the DB is a dedup (Created=false, refcount+1, no bytes); a hash repeat
// within the batch — the bytes are written once, each copy adds a reference;
// fresh contents — Created=true.
func TestPutBatchDuplicatesAndDedup(t *testing.T) {
	st, pool := newPackTestStore(t, func(o *PackOptions) { o.PackSize = 1 << 20 })
	ctx := context.Background()

	a := []byte("<html>content already live in the store</html>")
	if _, err := st.Put(ctx, a); err != nil {
		t.Fatal(err)
	}
	sizeBefore := compactPackSize(t, st.opts.Dir, 1)

	b := []byte("<html>new content, repeated twice in the batch</html>")
	c := []byte("<html>just new batch content</html>")
	results, errs, err := st.PutBatch(ctx, [][]byte{a, b, b, c})
	if err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	for i, e := range errs {
		if e != nil {
			t.Fatalf("item %d: %v", i, e)
		}
	}
	if results[0].Created {
		t.Error("live content: Created = true, want false (dedup)")
	}
	if !results[1].Created || results[2].Created {
		t.Errorf("in-batch repeat: Created = %v/%v, want true/false (bytes once)", results[1].Created, results[2].Created)
	}
	if !results[3].Created {
		t.Error("new content: Created = false")
	}

	for h, want := range map[Hash]int32{HashOf(a): 2, HashOf(b): 2, HashOf(c): 1} {
		row, ok := pool.row(h)
		if !ok {
			t.Fatalf("%s: no row", h)
		}
		if row.refc != want {
			t.Errorf("%s: refcount = %d, want %d", h, row.refc, want)
		}
	}
	// Bytes: only b and c (a is a dedup); the file growth is two records.
	rowB, _ := pool.row(HashOf(b))
	rowC, _ := pool.row(HashOf(c))
	wantGrow := int64(4+rowB.clen) + int64(4+rowC.clen)
	if got := compactPackSize(t, st.opts.Dir, 1) - sizeBefore; got != wantGrow {
		t.Errorf("pack growth: %d bytes, want %d (only the new blocks b and c)", got, wantGrow)
	}
	for _, d := range [][]byte{a, b, c} {
		if got, err := st.Get(ctx, HashOf(d)); err != nil || string(got) != string(d) {
			t.Errorf("Get: %v (match = %v)", err, string(got) == string(d))
		}
	}
}

// TestPutBatchDuplicateRefsBothPaths — duplicates of one hash in a batch on
// both commit paths, table-style (T-228, a latent defect: the dedup UPDATE
// incremented exactly +1 regardless of the number of copies — references
// were undercounted, and with a batched writer appearing, compaction would
// have removed a still-needed block):
//
//	path                       | refcount
//	----------------------------+------------------------------------
//	live hash, 1 ref before batch| 1 + n copies (dedup, sqlMetaAddRefBatch)
//	new hash                    | n copies      (INSERT, counts → sqlMetaPutBatch)
//
// The references are real: exhausting Delete × refcount kills the content.
func TestPutBatchDuplicateRefsBothPaths(t *testing.T) {
	st, pool := newPackTestStore(t, func(o *PackOptions) { o.PackSize = 1 << 20 })
	ctx := context.Background()

	live := []byte("<html>live before the batch, three times in the batch</html>")
	if _, err := st.Put(ctx, live); err != nil {
		t.Fatal(err)
	}
	fresh := []byte("<html>new content, three times in the batch</html>")

	results, errs, err := st.PutBatch(ctx, [][]byte{live, live, live, fresh, fresh, fresh})
	if err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	for i, e := range errs {
		if e != nil {
			t.Fatalf("item %d: %v", i, e)
		}
	}
	// Created: live is a dedup (no bytes), the first fresh item writes the
	// bytes, the hash repeats do not.
	for i, want := range []bool{false, false, false, true, false, false} {
		if results[i].Created != want {
			t.Errorf("item %d: Created = %v, want %v", i, results[i].Created, want)
		}
	}
	for h, want := range map[Hash]int32{HashOf(live): 4, HashOf(fresh): 3} {
		row, ok := pool.row(h)
		if !ok {
			t.Fatalf("%s: no row", h)
		}
		if row.refc != want {
			t.Errorf("%s: refcount = %d, want %d (every copy in the batch is a reference)", h, row.refc, want)
		}
	}

	// The references are real, not a number: exhausting Delete × refcount.
	for i := 0; i < 3; i++ {
		if err := st.Delete(ctx, HashOf(live)); err != nil {
			t.Fatalf("Delete of the live one, %d of 4: %v", i+1, err)
		}
		if _, err := st.Get(ctx, HashOf(live)); err != nil {
			t.Fatalf("Get of the live one after Delete %d of 4: %v (the content is still needed)", i+1, err)
		}
	}
	if err := st.Delete(ctx, HashOf(live)); err != nil {
		t.Fatal("Delete of the live one's fourth reference")
	}
	if _, err := st.Get(ctx, HashOf(live)); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get of the live one after 4 Deletes: %v, want ErrNotFound", err)
	}
	for i := 0; i < 2; i++ {
		if err := st.Delete(ctx, HashOf(fresh)); err != nil {
			t.Fatalf("Delete of the fresh one, %d of 3: %v", i+1, err)
		}
	}
	if err := st.Delete(ctx, HashOf(fresh)); err != nil {
		t.Fatal("Delete of the fresh one's third reference")
	}
	if _, err := st.Get(ctx, HashOf(fresh)); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get of the fresh one after 3 Deletes: %v, want ErrNotFound", err)
	}
}

// TestPutBatchResurrectDeadRow — a dead row (refcount = 0) is not
// resurrected by dedup: the batch writes a fresh block, the upsert moves the
// placement and sets refcount = n copies (the sqlMetaPut semantics).
func TestPutBatchResurrectDeadRow(t *testing.T) {
	st, pool := newPackTestStore(t, func(o *PackOptions) { o.PackSize = 1 << 20 })
	ctx := context.Background()

	x := []byte("<html>content dying before the batch</html>")
	if _, err := st.Put(ctx, x); err != nil {
		t.Fatal(err)
	}
	oldRow, _ := pool.row(HashOf(x))
	if err := st.Delete(ctx, HashOf(x)); err != nil {
		t.Fatal(err)
	}

	results, errs, err := st.PutBatch(ctx, [][]byte{x, x})
	if err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	for i, e := range errs {
		if e != nil {
			t.Fatalf("item %d: %v", i, e)
		}
	}
	if results[0].Created || results[1].Created {
		t.Error("resurrection: Created = true, want false (the conflict goes through the DO UPDATE branch)")
	}
	row, _ := pool.row(HashOf(x))
	if row.refc != 2 {
		t.Errorf("the refcount after the resurrection by two copies: %d, want 2", row.refc)
	}
	if row.off == oldRow.off {
		t.Error("the placement was not moved to a fresh block (resurrection writes new bytes)")
	}
	if got, err := st.Get(ctx, HashOf(x)); err != nil || string(got) != string(x) {
		t.Errorf("Get of the resurrected: %v (match = %v)", err, string(got) == string(x))
	}
}

// TestPutBatchEmptyAndTooLarge — an empty batch does not touch the DB; an
// item over max_store_bytes is rejected per item (ErrTooLarge), the others
// are stored.
func TestPutBatchEmptyAndTooLarge(t *testing.T) {
	st, pool := newPackTestStore(t, func(o *PackOptions) { o.MaxStoreBytes = 64 })
	ctx := context.Background()

	callsBefore := len(pool.snapshotCalls())
	if res, errs, err := st.PutBatch(ctx, nil); err != nil || len(res) != 0 || len(errs) != 0 {
		t.Errorf("empty batch: %v / %d / %d, want nil/0/0", err, len(res), len(errs))
	}
	if got := len(pool.snapshotCalls()) - callsBefore; got != 0 {
		t.Errorf("the empty batch made %d DB calls, want 0", got)
	}

	small := []byte("small content")
	big := []byte("a batch item deliberately exceeding the 64-byte max_store_bytes limit")
	alsoSmall := []byte("also small")
	_, errs, err := st.PutBatch(ctx, [][]byte{small, big, alsoSmall})
	if err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	if !errors.Is(errs[1], ErrTooLarge) {
		t.Fatalf("the over-limit item: %v, want ErrTooLarge", errs[1])
	}
	if errs[0] != nil || errs[2] != nil {
		t.Fatalf("the regular items rejected: %v, %v", errs[0], errs[2])
	}
	for _, d := range [][]byte{small, alsoSmall} {
		if got, err := st.Get(ctx, HashOf(d)); err != nil || string(got) != string(d) {
			t.Errorf("Get: %v (match = %v)", err, string(got) == string(d))
		}
	}
	if _, err := st.Get(ctx, HashOf(big)); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get of the over-limit: %v, want ErrNotFound", err)
	}
}

// TestPutBatchStoreFullPause — the content write pause (spec 8): the new
// bytes of the batch are not written (a per-item ErrStoreFull), the dedup
// reference of live content passes, the alert event fires.
func TestPutBatchStoreFullPause(t *testing.T) {
	var sink *fullSink
	st, pool := newPackTestStore(t, func(o *PackOptions) {
		o.DiskFreePercent = 5
		sink = &fullSink{}
		o.OnStoreFull = sink.hook
	})
	ctx := context.Background()
	fake := &fakeSpace{}
	fake.set(50, 100)
	st.stat = fake

	live := []byte("<html>live before the overflow</html>")
	if _, err := st.Put(ctx, live); err != nil {
		t.Fatal(err)
	}
	sizeBefore := compactPackSize(t, st.opts.Dir, 1)

	fake.set(4, 100) // 4% < the 5% threshold
	fresh := []byte("<html>new content into a full disk</html>")
	results, errs, err := st.PutBatch(ctx, [][]byte{live, fresh})
	if err != nil {
		t.Fatalf("PutBatch on a full disk: %v (per-item failures are not a batch failure)", err)
	}
	if !errors.Is(errs[1], ErrStoreFull) {
		t.Fatalf("the new item: %v, want ErrStoreFull", errs[1])
	}
	if errs[0] != nil {
		t.Fatalf("the live content dedup rejected: %v", errs[0])
	}
	if results[0].Created {
		t.Error("dedup on a full disk: Created = true")
	}
	if row, _ := pool.row(HashOf(live)); row.refc != 2 {
		t.Errorf("the live one's refcount: %d, want 2 (the dedup reference went through)", row.refc)
	}
	if got := compactPackSize(t, st.opts.Dir, 1) - sizeBefore; got != 0 {
		t.Errorf("%d bytes written on a full disk, want 0", got)
	}
	if n := countSQL(pool.snapshotCalls(), sqlMetaPutBatch); n != 0 {
		t.Errorf("batch INSERTs on a full disk: %d, want 0", n)
	}
	events := sink.snapshot()
	if len(events) != 1 || events[0].Source != StoreFullPut {
		t.Errorf("alert events: %+v, want one Source=put", events)
	}
}

// --- failures and idempotency ---------------------------------------------------

// TestPutBatchCommitFailureIdempotent — a batched INSERT failure: no rows,
// the blocks are garbage without rows (invisible, compaction will purge
// them); the batch retry is idempotent by hash. A dedup command failure is
// before the locks are taken: the file is not touched at all.
func TestPutBatchCommitFailureIdempotent(t *testing.T) {
	st, pool := newCompactTestStore(t, nil)
	ctx := context.Background()
	a := []byte("<html>content of failed commit A</html>")
	b := []byte("<html>content of failed commit B</html>")

	pool.failSQL, pool.failN = sqlMetaPutBatch, 1
	if _, _, err := st.PutBatch(ctx, [][]byte{a, b}); err == nil {
		t.Fatal("the batch INSERT failure is not returned as an error")
	}
	for _, d := range [][]byte{a, b} {
		if _, ok := pool.row(HashOf(d)); ok {
			t.Errorf("row %s committed despite the INSERT failure", HashOf(d))
		}
	}
	info, err := st.CompactScan(ctx, 1)
	if err != nil {
		t.Fatalf("CompactScan: %v", err)
	}
	if info.GarbageBlocks != 2 || info.LiveBlocks != 0 {
		t.Errorf("blocks of the failed batch: %d garbage, %d live — want 2/0", info.GarbageBlocks, info.LiveBlocks)
	}

	// The retry (the failure lifted) is idempotent: the rows are in place,
	// the bytes are readable.
	results, errs, err := st.PutBatch(ctx, [][]byte{a, b})
	if err != nil {
		t.Fatalf("PutBatch retry: %v", err)
	}
	for i, e := range errs {
		if e != nil {
			t.Fatalf("retry item %d: %v", i, e)
		}
	}
	if !results[0].Created || !results[1].Created {
		t.Error("retry after the failure: Created = false, want true (there were no rows)")
	}
	for _, d := range [][]byte{a, b} {
		if got, err := st.Get(ctx, HashOf(d)); err != nil || string(got) != string(d) {
			t.Errorf("Get after the retry: %v (match = %v)", err, string(got) == string(d))
		}
	}

	// A dedup command failure is before appending: the file size does not
	// change.
	pool.failSQL, pool.failN = sqlMetaAddRefBatch, 1
	sizeBefore := compactPackSize(t, st.opts.Dir, 1)
	c := []byte("<html>content of the failed dedup</html>")
	if _, _, err := st.PutBatch(ctx, [][]byte{c}); err == nil {
		t.Fatal("the dedup command failure is not returned as an error")
	}
	if got := compactPackSize(t, st.opts.Dir, 1) - sizeBefore; got != 0 {
		t.Errorf("%d bytes written on the dedup failure, want 0 (dedup before the locks)", got)
	}
}

// TestPutBatchCtxCanceled — a cancelled ctx: a context error, the batch not
// committed.
func TestPutBatchCtxCanceled(t *testing.T) {
	st, pool := newPackTestStore(t, nil)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	d := []byte("content of the canceled batch")
	if _, _, err := st.PutBatch(canceled, [][]byte{d}); !errors.Is(err, context.Canceled) {
		t.Fatalf("PutBatch with a canceled ctx: %v, want context.Canceled", err)
	}
	if _, ok := pool.row(HashOf(d)); ok {
		t.Error("a row committed by the canceled batch")
	}
}

// --- invariants against compaction ---------------------------------------------

// batchProbe — the probe result at the moment of the batched INSERT: the
// batch's fsync has already happened, the store locks (the mutex, the
// directory flock — the path by which compaction removes packs) are held by
// the row commit.
type batchProbe struct {
	fired     bool
	syncs     int
	muHeld    bool
	flockHeld bool
}

// probeBatchPool — a compaction pool with a probe on sqlMetaPutBatch (the
// getRacePool/killOnRowsPool style — modeling a race at the point of
// interest).
type probeBatchPool struct {
	*compactFakePool
	st    *PackStore
	syncs *atomic.Int32
	probe *batchProbe
}

func (p *probeBatchPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if sql == sqlMetaPutBatch && !p.probe.fired {
		p.probe.fired = true
		p.probe.syncs = int(p.syncs.Load())
		p.probe.muHeld = !p.st.mu.TryLock()
		// The flock is probed with a separate descriptor of the same file:
		// LOCK_EX|LOCK_NB must run into PutBatch's lock.
		lf, err := os.OpenFile(filepath.Join(p.st.opts.Dir, packLockFile), os.O_RDWR, 0o644)
		if err != nil {
			return nil, fmt.Errorf("directory lock probe: %w", err)
		}
		err = syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		p.probe.flockHeld = errors.Is(err, syscall.EWOULDBLOCK)
		_ = lf.Close()
	}
	return p.compactFakePool.Query(ctx, sql, args...)
}

// TestPutBatchInvariantFsyncAndFlock — the T-102 race point: at the moment
// the batch's rows are committed (the batched INSERT) the bytes are already
// synced (fsync before INSERT — C-1 "bytes before rows"), and the mutex and
// the directory flock are held — compaction can neither take the locks nor
// remove the pack of an uncommitted row (C-1 "a pack is not removed before
// its rows are committed"). After the locks are released, the compaction pass
// over the old pack is correct.
func TestPutBatchInvariantFsyncAndFlock(t *testing.T) {
	probe := &batchProbe{}
	probePool := &probeBatchPool{
		compactFakePool: &compactFakePool{packFakePool: newPackFakePool()},
		probe:           probe,
	}
	st, err := OpenPackStore(probePool, PackOptions{Dir: t.TempDir(), PackSize: 1 << 20, MaxStoreBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close() //nolint:errcheck // the test closes the store in cleanup
	probePool.st = st
	ctx := context.Background()

	// Pack 1: the content dies (live share 0 — compactable).
	x := []byte("<html>content of the dying pack</html>")
	if _, err := st.Put(ctx, x); err != nil {
		t.Fatal(err)
	}
	if err := st.Delete(ctx, HashOf(x)); err != nil {
		t.Fatal(err)
	}
	// Pack 2 — the newest: the warm-up creates its active file for the
	// counter.
	compactCraftPack(t, st.opts.Dir, 2, nil)
	if _, err := st.Put(ctx, []byte("pack 2 warmup")); err != nil {
		t.Fatal(err)
	}
	probePool.syncs = wrapActiveSyncs(t, st)

	y := []byte("<html>batch page A</html>")
	z := []byte("<html>batch page B</html>")
	if _, _, err := st.PutBatch(ctx, [][]byte{y, z}); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	if !probe.fired {
		t.Fatal("the probe did not fire — the batch INSERT never ran")
	}
	if probe.syncs < 1 {
		t.Errorf("fsyncs by the INSERT time: %d, want >= 1 (bytes before rows, C-1)", probe.syncs)
	}
	if !probe.muHeld {
		t.Error("the store mutex is not held while the batch rows commit")
	}
	if !probe.flockHeld {
		t.Error("the packs directory flock is not held while the batch rows commit — compaction could have removed the pack")
	}

	// After the commit: the compaction pass over the old pack succeeds, the
	// batch's content is alive.
	stats, err := CompactOnce(ctx, st, CompactOptions{})
	if err != nil || stats.Compacted != 1 {
		t.Fatalf("CompactOnce of the old pack: %+v, %v", stats, err)
	}
	if got := compactPackSize(t, st.opts.Dir, 1); got != 0 {
		t.Errorf("the old pack was not removed: %d bytes", got)
	}
	for _, d := range [][]byte{y, z} {
		if got, err := st.Get(ctx, HashOf(d)); err != nil || string(got) != string(d) {
			t.Errorf("Get after compaction: %v (match = %v)", err, string(got) == string(d))
		}
	}
}

// TestPutBatchWithConcurrentCompaction — an end-to-end race: compaction of
// the old pack runs concurrently with a group commit into the newest
// (the locks serialize the appends) — all contents are alive, the old pack
// is removed, the live block has moved.
func TestPutBatchWithConcurrentCompaction(t *testing.T) {
	st, pool := newCompactTestStore(t, nil)
	ctx := context.Background()

	live := []byte("<html>live content of the pack being compacted</html>")
	lres, err := st.Put(ctx, live)
	if err != nil {
		t.Fatal(err)
	}
	compactCraftPack(t, st.opts.Dir, 2, nil) // pack 2 is the newest

	m := []byte("<html>batch page racing compaction M</html>")
	n := []byte("<html>batch page racing compaction N</html>")

	var wg sync.WaitGroup
	compactErr := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := st.CompactPack(ctx, 1)
		compactErr <- err
	}()
	bres, berrs, err := st.PutBatch(ctx, [][]byte{m, n})
	wg.Wait()
	if err := <-compactErr; err != nil {
		t.Fatalf("CompactPack: %v", err)
	}
	if err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	for i, e := range berrs {
		if e != nil {
			t.Fatalf("item %d: %v", i, e)
		}
	}

	if got := compactPackSize(t, st.opts.Dir, 1); got != 0 {
		t.Errorf("the pack being compacted was not removed: %d bytes", got)
	}
	if row, _ := pool.row(lres.Hash); row.packID != 2 {
		t.Errorf("the old pack's live block did not move: pack %d, want 2", row.packID)
	}
	for i, d := range [][]byte{m, n} {
		if !bres[i].Created {
			t.Errorf("item %d: Created = false", i)
		}
		if got, err := st.Get(ctx, HashOf(d)); err != nil || string(got) != string(d) {
			t.Errorf("Get(%d): %v (match = %v)", i, err, string(got) == string(d))
		}
	}
	if got, err := st.Get(ctx, lres.Hash); err != nil || string(got) != string(live) {
		t.Errorf("Get of the live block after the move: %v (match = %v)", err, string(got) == string(live))
	}
}

// TestPutBatchRotation — a batch that does not fit into one pack: rotation
// inside the batch does not break the commit (the bytes of all blocks are on
// disk before the INSERT: packs closed by rotation are synced at rotation),
// all contents are readable.
func TestPutBatchRotation(t *testing.T) {
	st, pool := newCompactTestStore(t, func(o *PackOptions) { o.PackSize = 256 })
	ctx := context.Background()
	var items [][]byte
	for i := 0; i < 5; i++ {
		items = append(items, []byte(fmt.Sprintf("<html>page %d of in-batch rotation — pad-pad-pad</html>", i)))
	}
	if _, errs, err := st.PutBatch(ctx, items); err != nil {
		t.Fatalf("PutBatch: %v", err)
	} else {
		for i, e := range errs {
			if e != nil {
				t.Fatalf("item %d: %v", i, e)
			}
		}
	}
	packs := make(map[int32]int)
	for _, d := range items {
		h := HashOf(d)
		if got, err := st.Get(ctx, h); err != nil || string(got) != string(d) {
			t.Errorf("Get: %v (match = %v)", err, string(got) == string(d))
		}
		row, _ := pool.row(h)
		packs[row.packID]++
	}
	if len(packs) < 2 {
		t.Errorf("the batch blocks landed in one pack (%v) at the rotation threshold of 256 bytes", packs)
	}
}

// --- a synthetic benchmark of the commit cost (T-102, step 3) ------------------

// benchPage — a synthetic page of ~30 KiB (a typical parking page):
// compressible HTML with a unique number — deduplication must not eat the
// benchmark.
func benchPage(i int) []byte {
	buf := make([]byte, 0, 32<<10)
	buf = append(buf, "<html><head><title>parking "...)
	buf = strconv.AppendInt(buf, int64(i), 10)
	buf = append(buf, "</title></head><body>"...)
	for j := 0; j < 200; j++ {
		buf = append(buf, fmt.Sprintf(
			"<div class=\"row-%d\"><a href=\"/p/%d\">item #%d — a typical page for the content store benchmark</a></div>", j, i, j)...)
	}
	buf = append(buf, "</body></html>"...)
	return buf
}

// benchStore — a store on the fake pool (the DB adds no latency — what is
// measured is the FS cost: write + fsync).
func benchStore(b *testing.B) *PackStore {
	b.Helper()
	st, err := OpenPackStore(newPackFakePool(), PackOptions{
		Dir: b.TempDir(), PackSize: 512 << 20, MaxStoreBytes: 1 << 20,
	})
	if err != nil {
		b.Fatalf("OpenPackStore: %v", err)
	}
	b.Cleanup(func() { _ = st.Close() })
	return st
}

// BenchmarkPutOneByOne — per-row Put: an fsync and an INSERT per block
// (the pre-T-102 path).
func BenchmarkPutOneByOne(b *testing.B) {
	st := benchStore(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := st.Put(ctx, benchPage(i)); err != nil {
			b.Fatal(err)
		}
	}
}

// benchPutBatch — group commit in batches of k: one fsync and one batched
// INSERT per batch (T-102).
func benchPutBatch(b *testing.B, k int) {
	st := benchStore(b)
	ctx := context.Background()
	items := make([][]byte, k)
	b.ResetTimer()
	for n := 0; n+k <= b.N; n += k {
		for j := 0; j < k; j++ {
			items[j] = benchPage(n + j)
		}
		if _, _, err := st.PutBatch(ctx, items); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutBatch8(b *testing.B)  { benchPutBatch(b, 8) }
func BenchmarkPutBatch32(b *testing.B) { benchPutBatch(b, 32) }

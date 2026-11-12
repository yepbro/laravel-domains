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
		t.Fatal("SQL групповой фиксации изменился — обновите контракт и schema_test")
	}

	st, pool := newPackTestStore(t, nil)
	ctx := context.Background()
	d1 := []byte("<html>первый контент пачки SQL-контракта</html>")
	d2 := []byte("<html>второй контент пачки SQL-контракта</html>")

	if _, _, err := st.PutBatch(ctx, [][]byte{d1, d2}); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	calls := pool.snapshotCalls()
	if len(calls) != 2 {
		t.Fatalf("вызовов БД %d, want 2 (addRefBatch, putBatch)", len(calls))
	}
	if calls[0].sql != sqlMetaAddRefBatch || calls[1].sql != sqlMetaPutBatch {
		t.Fatalf("порядок SQL: %q, %q — want addRefBatch, putBatch", calls[0].sql, calls[1].sql)
	}
	if len(calls[0].args) != 2 {
		t.Fatalf("addRefBatch: аргументов %d, want 2 (хэши, счётчики копий)", len(calls[0].args))
	}
	if hashes, ok := calls[0].args[0].([][]byte); !ok || len(hashes) != 2 {
		t.Errorf("addRefBatch: аргумент %T(%d), want [][]byte(2)", calls[0].args[0], len(hashes))
	}
	if counts, ok := calls[0].args[1].([]int32); !ok || len(counts) != 2 {
		t.Errorf("addRefBatch: счётчики %T(%d), want []int32(2)", calls[0].args[1], len(counts))
	} else {
		for i, c := range counts {
			if c != 1 {
				t.Errorf("addRefBatch: счётчик %d = %d, want 1 (в пачке без дублей)", i, c)
			}
		}
	}
	args := calls[1].args
	if len(args) != 6 {
		t.Fatalf("putBatch: аргументов %d, want 6", len(args))
	}
	hashes, ok := args[0].([][]byte)
	if !ok || len(hashes) != 2 {
		t.Fatalf("putBatch: хэши %T, want [][]byte(2)", args[0])
	}
	for _, hb := range hashes {
		if len(hb) != HashLen {
			t.Errorf("putBatch: хэш %x: %d октетов, want %d", hb, len(hb), HashLen)
		}
	}
	if packs, ok := args[1].([]int32); !ok || len(packs) != 2 {
		t.Errorf("putBatch: паки %T(%d), want []int32(2)", args[1], len(packs))
	}
	if offs, ok := args[2].([]int64); !ok || len(offs) != 2 {
		t.Errorf("putBatch: смещения %T(%d), want []int64(2)", args[2], len(offs))
	}
	for _, sl := range []any{args[3], args[4], args[5]} {
		if v, ok := sl.([]int32); !ok || len(v) != 2 {
			t.Errorf("putBatch: массив %T, want []int32(2)", sl)
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
	if _, err := st.Put(ctx, []byte("прогрев")); err != nil {
		t.Fatal(err)
	}
	syncs := wrapActiveSyncs(t, st)

	items := [][]byte{
		nil, // empty content is valid (T-041)
		[]byte("<html>страница 1 групповой фиксации</html>"),
		[]byte("<html>страница 2 групповой фиксации</html>"),
		[]byte("<html>страница 3 групповой фиксации</html>"),
		[]byte("<html>страница 4 групповой фиксации</html>"),
	}
	results, errs, err := st.PutBatch(ctx, items)
	if err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	if len(results) != len(items) || len(errs) != len(items) {
		t.Fatalf("размеры итогов: %d/%d, want %d", len(results), len(errs), len(items))
	}
	offs := make(map[int64]bool, len(items))
	for i, d := range items {
		if errs[i] != nil {
			t.Fatalf("элемент %d: %v", i, errs[i])
		}
		want := HashOf(d)
		if results[i].Hash != want {
			t.Errorf("элемент %d: хэш %s, want HashOf(data) = %s (контракт T-041)", i, results[i].Hash, want)
		}
		if !results[i].Created {
			t.Errorf("элемент %d: Created = false, want true (новые байты)", i)
		}
		got, err := st.Get(ctx, want)
		if err != nil || string(got) != string(d) {
			t.Errorf("Get(%d): %v (совпадение = %v)", i, err, string(got) == string(d))
		}
		row, ok := pool.row(want)
		if !ok {
			t.Fatalf("элемент %d: нет строки метаданных", i)
		}
		if int(row.rawLen) != len(d) {
			t.Errorf("элемент %d: raw_len = %d, want %d", i, row.rawLen, len(d))
		}
		if offs[row.off] {
			t.Errorf("элемент %d: смещение %d повторяется — блоки пачки перекрываются", i, row.off)
		}
		offs[row.off] = true
	}

	if got := syncs.Load(); got != 1 {
		t.Errorf("fsync на пачку из %d элементов: %d, want 1", len(items), got)
	}
	if n := countSQL(pool.snapshotCalls(), sqlMetaAddRefBatch); n != 1 {
		t.Errorf("дедуп-команд на пачку: %d, want 1", n)
	}
	if n := countSQL(pool.snapshotCalls(), sqlMetaPutBatch); n != 1 {
		t.Errorf("пакетных INSERT на пачку: %d, want 1", n)
	}
}

// TestPutBatchDuplicatesAndDedup — a mixed batch: content already live in
// the DB is a dedup (Created=false, refcount+1, no bytes); a hash repeat
// within the batch — the bytes are written once, each copy adds a reference;
// fresh contents — Created=true.
func TestPutBatchDuplicatesAndDedup(t *testing.T) {
	st, pool := newPackTestStore(t, func(o *PackOptions) { o.PackSize = 1 << 20 })
	ctx := context.Background()

	a := []byte("<html>уже живой в хранилище контент</html>")
	if _, err := st.Put(ctx, a); err != nil {
		t.Fatal(err)
	}
	sizeBefore := compactPackSize(t, st.opts.Dir, 1)

	b := []byte("<html>новый контент, повторённый в пачке дважды</html>")
	c := []byte("<html>просто новый контент пачки</html>")
	results, errs, err := st.PutBatch(ctx, [][]byte{a, b, b, c})
	if err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	for i, e := range errs {
		if e != nil {
			t.Fatalf("элемент %d: %v", i, e)
		}
	}
	if results[0].Created {
		t.Error("живой контент: Created = true, want false (дедуп)")
	}
	if !results[1].Created || results[2].Created {
		t.Errorf("повтор в пачке: Created = %v/%v, want true/false (байты один раз)", results[1].Created, results[2].Created)
	}
	if !results[3].Created {
		t.Error("новый контент: Created = false")
	}

	for h, want := range map[Hash]int32{HashOf(a): 2, HashOf(b): 2, HashOf(c): 1} {
		row, ok := pool.row(h)
		if !ok {
			t.Fatalf("%s: нет строки", h)
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
		t.Errorf("рост пака: %d байт, want %d (только новые блоки b и c)", got, wantGrow)
	}
	for _, d := range [][]byte{a, b, c} {
		if got, err := st.Get(ctx, HashOf(d)); err != nil || string(got) != string(d) {
			t.Errorf("Get: %v (совпадение = %v)", err, string(got) == string(d))
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

	live := []byte("<html>живой до пачки, в пачке трижды</html>")
	if _, err := st.Put(ctx, live); err != nil {
		t.Fatal(err)
	}
	fresh := []byte("<html>новый контент, в пачке трижды</html>")

	results, errs, err := st.PutBatch(ctx, [][]byte{live, live, live, fresh, fresh, fresh})
	if err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	for i, e := range errs {
		if e != nil {
			t.Fatalf("элемент %d: %v", i, e)
		}
	}
	// Created: live is a dedup (no bytes), the first fresh item writes the
	// bytes, the hash repeats do not.
	for i, want := range []bool{false, false, false, true, false, false} {
		if results[i].Created != want {
			t.Errorf("элемент %d: Created = %v, want %v", i, results[i].Created, want)
		}
	}
	for h, want := range map[Hash]int32{HashOf(live): 4, HashOf(fresh): 3} {
		row, ok := pool.row(h)
		if !ok {
			t.Fatalf("%s: нет строки", h)
		}
		if row.refc != want {
			t.Errorf("%s: refcount = %d, want %d (каждая копия в пачке — ссылка)", h, row.refc, want)
		}
	}

	// The references are real, not a number: exhausting Delete × refcount.
	for i := 0; i < 3; i++ {
		if err := st.Delete(ctx, HashOf(live)); err != nil {
			t.Fatalf("Delete живого %d из 4: %v", i+1, err)
		}
		if _, err := st.Get(ctx, HashOf(live)); err != nil {
			t.Fatalf("Get живого после Delete %d из 4: %v (контент ещё нужен)", i+1, err)
		}
	}
	if err := st.Delete(ctx, HashOf(live)); err != nil {
		t.Fatal("Delete четвёртой ссылки живого")
	}
	if _, err := st.Get(ctx, HashOf(live)); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get живого после 4 Delete: %v, want ErrNotFound", err)
	}
	for i := 0; i < 2; i++ {
		if err := st.Delete(ctx, HashOf(fresh)); err != nil {
			t.Fatalf("Delete свежего %d из 3: %v", i+1, err)
		}
	}
	if err := st.Delete(ctx, HashOf(fresh)); err != nil {
		t.Fatal("Delete третьей ссылки свежего")
	}
	if _, err := st.Get(ctx, HashOf(fresh)); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get свежего после 3 Delete: %v, want ErrNotFound", err)
	}
}

// TestPutBatchResurrectDeadRow — a dead row (refcount = 0) is not
// resurrected by dedup: the batch writes a fresh block, the upsert moves the
// placement and sets refcount = n copies (the sqlMetaPut semantics).
func TestPutBatchResurrectDeadRow(t *testing.T) {
	st, pool := newPackTestStore(t, func(o *PackOptions) { o.PackSize = 1 << 20 })
	ctx := context.Background()

	x := []byte("<html>контент, умирающий до пачки</html>")
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
			t.Fatalf("элемент %d: %v", i, e)
		}
	}
	if results[0].Created || results[1].Created {
		t.Error("воскрешение: Created = true, want false (конфликт идёт веткой DO UPDATE)")
	}
	row, _ := pool.row(HashOf(x))
	if row.refc != 2 {
		t.Errorf("refcount после воскрешения двумя копиями: %d, want 2", row.refc)
	}
	if row.off == oldRow.off {
		t.Error("размещение не переведено на свежий блок (воскрешение пишет новые байты)")
	}
	if got, err := st.Get(ctx, HashOf(x)); err != nil || string(got) != string(x) {
		t.Errorf("Get воскрешённого: %v (совпадение = %v)", err, string(got) == string(x))
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
		t.Errorf("пустая пачка: %v / %d / %d, want nil/0/0", err, len(res), len(errs))
	}
	if got := len(pool.snapshotCalls()) - callsBefore; got != 0 {
		t.Errorf("пустая пачка сделала %d обращений к БД, want 0", got)
	}

	small := []byte("маленький контент")
	big := []byte("элемент пачки, заведомо превышающий лимит max_store_bytes в 64 байта")
	alsoSmall := []byte("тоже маленький")
	_, errs, err := st.PutBatch(ctx, [][]byte{small, big, alsoSmall})
	if err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	if !errors.Is(errs[1], ErrTooLarge) {
		t.Fatalf("сверхлимитный элемент: %v, want ErrTooLarge", errs[1])
	}
	if errs[0] != nil || errs[2] != nil {
		t.Fatalf("обычные элементы отвергнуты: %v, %v", errs[0], errs[2])
	}
	for _, d := range [][]byte{small, alsoSmall} {
		if got, err := st.Get(ctx, HashOf(d)); err != nil || string(got) != string(d) {
			t.Errorf("Get: %v (совпадение = %v)", err, string(got) == string(d))
		}
	}
	if _, err := st.Get(ctx, HashOf(big)); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get сверхлимитного: %v, want ErrNotFound", err)
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

	live := []byte("<html>живой до переполнения</html>")
	if _, err := st.Put(ctx, live); err != nil {
		t.Fatal(err)
	}
	sizeBefore := compactPackSize(t, st.opts.Dir, 1)

	fake.set(4, 100) // 4% < the 5% threshold
	fresh := []byte("<html>новый контент в полный диск</html>")
	results, errs, err := st.PutBatch(ctx, [][]byte{live, fresh})
	if err != nil {
		t.Fatalf("PutBatch на полном диске: %v (по-элементные отказы — не сбой пачки)", err)
	}
	if !errors.Is(errs[1], ErrStoreFull) {
		t.Fatalf("новый элемент: %v, want ErrStoreFull", errs[1])
	}
	if errs[0] != nil {
		t.Fatalf("дедуп живого контента отвергнут: %v", errs[0])
	}
	if results[0].Created {
		t.Error("дедуп на полном диске: Created = true")
	}
	if row, _ := pool.row(HashOf(live)); row.refc != 2 {
		t.Errorf("refcount живого: %d, want 2 (дедуп-ссылка прошла)", row.refc)
	}
	if got := compactPackSize(t, st.opts.Dir, 1) - sizeBefore; got != 0 {
		t.Errorf("на полном диске записано %d байт, want 0", got)
	}
	if n := countSQL(pool.snapshotCalls(), sqlMetaPutBatch); n != 0 {
		t.Errorf("пакетных INSERT на полном диске: %d, want 0", n)
	}
	events := sink.snapshot()
	if len(events) != 1 || events[0].Source != StoreFullPut {
		t.Errorf("события алерта: %+v, want один Source=put", events)
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
	a := []byte("<html>контент сбойной фиксации A</html>")
	b := []byte("<html>контент сбойной фиксации B</html>")

	pool.failSQL, pool.failN = sqlMetaPutBatch, 1
	if _, _, err := st.PutBatch(ctx, [][]byte{a, b}); err == nil {
		t.Fatal("сбой пакетного INSERT не возвращён ошибкой")
	}
	for _, d := range [][]byte{a, b} {
		if _, ok := pool.row(HashOf(d)); ok {
			t.Errorf("строка %s зафиксирована несмотря на сбой INSERT", HashOf(d))
		}
	}
	info, err := st.CompactScan(ctx, 1)
	if err != nil {
		t.Fatalf("CompactScan: %v", err)
	}
	if info.GarbageBlocks != 2 || info.LiveBlocks != 0 {
		t.Errorf("блоки сбойной пачки: мусора %d, живых %d — want 2/0", info.GarbageBlocks, info.LiveBlocks)
	}

	// The retry (the failure lifted) is idempotent: the rows are in place,
	// the bytes are readable.
	results, errs, err := st.PutBatch(ctx, [][]byte{a, b})
	if err != nil {
		t.Fatalf("повтор PutBatch: %v", err)
	}
	for i, e := range errs {
		if e != nil {
			t.Fatalf("элемент %d повтора: %v", i, e)
		}
	}
	if !results[0].Created || !results[1].Created {
		t.Error("повтор после сбоя: Created = false, want true (строк не было)")
	}
	for _, d := range [][]byte{a, b} {
		if got, err := st.Get(ctx, HashOf(d)); err != nil || string(got) != string(d) {
			t.Errorf("Get после повтора: %v (совпадение = %v)", err, string(got) == string(d))
		}
	}

	// A dedup command failure is before appending: the file size does not
	// change.
	pool.failSQL, pool.failN = sqlMetaAddRefBatch, 1
	sizeBefore := compactPackSize(t, st.opts.Dir, 1)
	c := []byte("<html>контент сбойного дедупа</html>")
	if _, _, err := st.PutBatch(ctx, [][]byte{c}); err == nil {
		t.Fatal("сбой дедуп-команды не возвращён ошибкой")
	}
	if got := compactPackSize(t, st.opts.Dir, 1) - sizeBefore; got != 0 {
		t.Errorf("при сбое дедупа записано %d байт, want 0 (дедуп раньше блокировок)", got)
	}
}

// TestPutBatchCtxCanceled — a cancelled ctx: a context error, the batch not
// committed.
func TestPutBatchCtxCanceled(t *testing.T) {
	st, pool := newPackTestStore(t, nil)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	d := []byte("контент отменённой пачки")
	if _, _, err := st.PutBatch(canceled, [][]byte{d}); !errors.Is(err, context.Canceled) {
		t.Fatalf("PutBatch с отменённым ctx: %v, want context.Canceled", err)
	}
	if _, ok := pool.row(HashOf(d)); ok {
		t.Error("строка зафиксирована отменённой пачкой")
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
			return nil, fmt.Errorf("зонд блокировки каталога: %w", err)
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
	x := []byte("<html>контент умирающего пака</html>")
	if _, err := st.Put(ctx, x); err != nil {
		t.Fatal(err)
	}
	if err := st.Delete(ctx, HashOf(x)); err != nil {
		t.Fatal(err)
	}
	// Pack 2 — the newest: the warm-up creates its active file for the
	// counter.
	compactCraftPack(t, st.opts.Dir, 2, nil)
	if _, err := st.Put(ctx, []byte("прогрев пака 2")); err != nil {
		t.Fatal(err)
	}
	probePool.syncs = wrapActiveSyncs(t, st)

	y := []byte("<html>страница пачки A</html>")
	z := []byte("<html>страница пачки B</html>")
	if _, _, err := st.PutBatch(ctx, [][]byte{y, z}); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}
	if !probe.fired {
		t.Fatal("зонд не сработал — пакетный INSERT не выполнялся")
	}
	if probe.syncs < 1 {
		t.Errorf("fsync к моменту INSERT: %d, want >= 1 (байты раньше строк, C-1)", probe.syncs)
	}
	if !probe.muHeld {
		t.Error("мьютекс хранилища не удержан в момент фиксации строк пачки")
	}
	if !probe.flockHeld {
		t.Error("flock каталога паков не удержан в момент фиксации строк пачки — компактификация могла бы удалить пак")
	}

	// After the commit: the compaction pass over the old pack succeeds, the
	// batch's content is alive.
	stats, err := CompactOnce(ctx, st, CompactOptions{})
	if err != nil || stats.Compacted != 1 {
		t.Fatalf("CompactOnce старого пака: %+v, %v", stats, err)
	}
	if got := compactPackSize(t, st.opts.Dir, 1); got != 0 {
		t.Errorf("старый пак не удалён: %d байт", got)
	}
	for _, d := range [][]byte{y, z} {
		if got, err := st.Get(ctx, HashOf(d)); err != nil || string(got) != string(d) {
			t.Errorf("Get после компактификации: %v (совпадение = %v)", err, string(got) == string(d))
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

	live := []byte("<html>живой контент компактифицируемого пака</html>")
	lres, err := st.Put(ctx, live)
	if err != nil {
		t.Fatal(err)
	}
	compactCraftPack(t, st.opts.Dir, 2, nil) // pack 2 is the newest

	m := []byte("<html>страница пачки против компактификации M</html>")
	n := []byte("<html>страница пачки против компактификации N</html>")

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
			t.Fatalf("элемент %d: %v", i, e)
		}
	}

	if got := compactPackSize(t, st.opts.Dir, 1); got != 0 {
		t.Errorf("компактифицируемый пак не удалён: %d байт", got)
	}
	if row, _ := pool.row(lres.Hash); row.packID != 2 {
		t.Errorf("живой блок старого пака не переехал: пак %d, want 2", row.packID)
	}
	for i, d := range [][]byte{m, n} {
		if !bres[i].Created {
			t.Errorf("элемент %d: Created = false", i)
		}
		if got, err := st.Get(ctx, HashOf(d)); err != nil || string(got) != string(d) {
			t.Errorf("Get(%d): %v (совпадение = %v)", i, err, string(got) == string(d))
		}
	}
	if got, err := st.Get(ctx, lres.Hash); err != nil || string(got) != string(live) {
		t.Errorf("Get живого блока после переезда: %v (совпадение = %v)", err, string(got) == string(live))
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
		items = append(items, []byte(fmt.Sprintf("<html>страница %d ротации внутри пачки — pad-pad-pad</html>", i)))
	}
	if _, errs, err := st.PutBatch(ctx, items); err != nil {
		t.Fatalf("PutBatch: %v", err)
	} else {
		for i, e := range errs {
			if e != nil {
				t.Fatalf("элемент %d: %v", i, e)
			}
		}
	}
	packs := make(map[int32]int)
	for _, d := range items {
		h := HashOf(d)
		if got, err := st.Get(ctx, h); err != nil || string(got) != string(d) {
			t.Errorf("Get: %v (совпадение = %v)", err, string(got) == string(d))
		}
		row, _ := pool.row(h)
		packs[row.packID]++
	}
	if len(packs) < 2 {
		t.Errorf("блоки пачки легли в один пак (%v) при пороге ротации 256 байт", packs)
	}
}

// --- a synthetic benchmark of the commit cost (T-102, step 3) ------------------

// benchPage — a synthetic page of ~30 KiB (a typical parking page):
// compressible HTML with a unique number — deduplication must not eat the
// benchmark.
func benchPage(i int) []byte {
	buf := make([]byte, 0, 32<<10)
	buf = append(buf, "<html><head><title>парковка "...)
	buf = strconv.AppendInt(buf, int64(i), 10)
	buf = append(buf, "</title></head><body>"...)
	for j := 0; j < 200; j++ {
		buf = append(buf, fmt.Sprintf(
			"<div class=\"row-%d\"><a href=\"/p/%d\">пункт №%d — типовая страница для бенчмарка хранилища контента</a></div>", j, i, j)...)
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

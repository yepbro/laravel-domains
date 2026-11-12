// Unit tests of pack compaction (T-045; spec 5.7, 13.2) on a fake
// pgx pool with transactions (the contents semantics + placement moves) and
// real pack files in t.TempDir — the pack_test.go style. Behavior against
// live DB — end-to-end store tests (T-046).
package content

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// --- fake pool with compaction transactions ---------------------------------

// compactFakePool — a packFakePool (the Put/Get/Delete semantics) plus the
// compaction queries and transactions: the pack scan, the live-row check,
// the placement moves and the dead-row deletions. Step failures — crash
// windows.
type compactFakePool struct {
	*packFakePool

	beginErr  error
	commitErr error
	txExecErr error
	begins    int
	commits   int
}

// compile-time: the fake pool is a CompactDB for tests without a DB.
var _ CompactDB = (*compactFakePool)(nil)

// Query — the compaction SQL is served from the contents map,
// the rest (Put/Get/Delete) goes to the embedded packFakePool.
func (p *compactFakePool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	switch sql {
	case sqlCompactRows, sqlCompactLive, sqlCompactAll:
		p.mu.Lock()
		defer p.mu.Unlock()
		p.calls = append(p.calls, packCall{sql: sql, args: args})
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if p.queryErr != nil {
			return nil, p.queryErr
		}
		packID := args[0].(int32)
		if sql == sqlCompactLive || sql == sqlCompactAll {
			var n int64
			for _, r := range p.contents {
				if r.packID == packID && (sql == sqlCompactAll || r.refc > 0) {
					n++
				}
			}
			return &packRows{rows: [][]any{{n}}}, nil
		}
		type crow struct {
			h    [HashLen]byte
			off  int64
			clen int32
			refc int32
		}
		list := make([]crow, 0, len(p.contents))
		for h, r := range p.contents {
			if r.packID == packID {
				list = append(list, crow{h: h, off: r.off, clen: r.clen, refc: r.refc})
			}
		}
		sort.Slice(list, func(i, j int) bool { return list[i].off < list[j].off })
		rows := make([][]any, len(list))
		for i, r := range list {
			rows[i] = []any{r.h[:], r.off, r.clen, r.refc}
		}
		return &packRows{rows: rows}, nil
	}
	return p.packFakePool.Query(ctx, sql, args...)
}

func (p *compactFakePool) Begin(context.Context) (pgx.Tx, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.begins++
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	return &compactFakeTx{pool: p}, nil
}

// Exec — the final sweep of the pack's dead rows (sqlCompactDeadPack);
// the Put/Get/Delete statements go to the embedded packFakePool.
func (p *compactFakePool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if sql != sqlCompactDeadPack {
		return p.packFakePool.Exec(ctx, sql, args...)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, packCall{sql: sql, args: args})
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	if p.queryErr != nil {
		return pgconn.CommandTag{}, p.queryErr
	}
	packID := args[0].(int32)
	var n int64
	for h, r := range p.contents {
		if r.packID == packID && r.refc == 0 {
			delete(p.contents, h)
			n++
		}
	}
	return pgconn.NewCommandTag(fmt.Sprintf("DELETE %d", n)), nil
}

// compactFakeMove — a deferred row placement change.
type compactFakeMove struct {
	h      [HashLen]byte
	packID int32
	off    int64
}

// compactFakeTx — a transaction of the fake pack: changes accumulate and
// are applied only in Commit (a rollback honestly undoes them).
type compactFakeTx struct {
	pgx.Tx // the pass does not use the other pgx.Tx methods

	pool     *compactFakePool
	moves    []compactFakeMove
	deads    [][HashLen]byte
	corrupts [][HashLen]byte
}

func (t *compactFakeTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.pool.txExecErr != nil {
		return pgconn.CommandTag{}, t.pool.txExecErr
	}
	switch sql {
	case sqlCompactMove:
		hashes := args[0].([][]byte)
		packs := args[1].([]int32)
		offs := args[2].([]int64)
		from := args[3].(int32)
		var n int64
		for i, hb := range hashes {
			var h [HashLen]byte
			copy(h[:], hb)
			if r := t.pool.contents[h]; r != nil && r.packID == from && r.refc > 0 {
				t.moves = append(t.moves, compactFakeMove{h: h, packID: packs[i], off: offs[i]})
				n++
			}
		}
		return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", n)), nil
	case sqlCompactDead:
		hashes := args[0].([][]byte)
		from := args[1].(int32)
		var n int64
		for _, hb := range hashes {
			var h [HashLen]byte
			copy(h[:], hb)
			if r := t.pool.contents[h]; r != nil && r.packID == from {
				// The refcount = 0 guard is evaluated at statement execution
				// time (as in PostgreSQL): the dead markings from the same
				// transaction are already applied — sqlCompactCorrupt runs
				// in CompactPack before sqlCompactDead.
				if r.refc == 0 || t.corruptPending(h) {
					t.deads = append(t.deads, h)
					n++
				}
			}
		}
		return pgconn.NewCommandTag(fmt.Sprintf("DELETE %d", n)), nil
	case sqlCompactCorrupt:
		hb := args[0].([]byte)
		from := args[1].(int32)
		var h [HashLen]byte
		copy(h[:], hb)
		var n int64
		if r := t.pool.contents[h]; r != nil && r.packID == from && r.refc > 0 {
			t.corrupts = append(t.corrupts, h)
			n = 1
		}
		return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", n)), nil
	default:
		return pgconn.CommandTag{}, fmt.Errorf("compactFakeTx: неожидаемый SQL: %s", sql)
	}
}

func (t *compactFakeTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.pool.commitErr != nil {
		return t.pool.commitErr
	}
	for _, h := range t.corrupts { // the dead markings go before the deads deletions
		if r := t.pool.contents[h]; r != nil {
			r.refc = 0
		}
	}
	for _, m := range t.moves {
		if r := t.pool.contents[m.h]; r != nil {
			r.packID, r.off = m.packID, m.off
		}
	}
	for _, h := range t.deads {
		if r := t.pool.contents[h]; r != nil && r.refc == 0 {
			delete(t.pool.contents, h)
		}
	}
	t.pool.commits++
	return nil
}

func (t *compactFakeTx) Rollback(context.Context) error { return nil }

// corruptPending — the row is marked dead by this transaction's
// sqlCompactCorrupt statement (applied in Commit before the deads).
func (t *compactFakeTx) corruptPending(h [HashLen]byte) bool {
	for _, c := range t.corrupts {
		if c == h {
			return true
		}
	}
	return false
}

// --- the shared constructor and utilities -----------------------------------

// newCompactTestStore — a PackStore in a temporary directory with a fake pool
// that supports compaction transactions.
func newCompactTestStore(t *testing.T, mutate func(*PackOptions)) (*PackStore, *compactFakePool) {
	t.Helper()
	opts := PackOptions{Dir: t.TempDir(), PackSize: 1 << 20, MaxStoreBytes: 1 << 20}
	if mutate != nil {
		mutate(&opts)
	}
	pool := &compactFakePool{packFakePool: newPackFakePool()}
	st, err := OpenPackStore(pool, opts)
	if err != nil {
		t.Fatalf("OpenPackStore: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, pool
}

// compactCraftPack — manual pack assembly: a v1 header and records
// [len u32 LE][block]; returns the block offsets. For precise byte control
// (the live share threshold) and for creating the newest pack without
// writing into it.
func compactCraftPack(t *testing.T, dir string, id int32, blocks [][]byte) []int64 {
	t.Helper()
	var buf bytes.Buffer
	hdr := packHeader{id: id, maxSize: 1 << 20}
	buf.Write(hdr.bytes())
	offs := make([]int64, len(blocks))
	for i, b := range blocks {
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(b)))
		offs[i] = int64(buf.Len()) + 4
		buf.Write(lenBuf[:])
		buf.Write(b)
	}
	if err := os.WriteFile(filepath.Join(dir, packFileName(id)), buf.Bytes(), 0o644); err != nil {
		t.Fatalf("сборка пака %d: %v", id, err)
	}
	return offs
}

// compactCraftRow — a contents row on top of a manually assembled pack.
func compactCraftRow(t *testing.T, pool *compactFakePool, key string, packID int32, off int64, clen int, refc int32) Hash {
	t.Helper()
	h := HashOf([]byte(key))
	pool.contents[h] = &packFakeRow{packID: packID, off: off, clen: int32(clen), rawLen: 0, refc: refc}
	return h
}

// compactPackSize — the size of the pack's file (0 — no file).
func compactPackSize(t *testing.T, dir string, id int32) int64 {
	t.Helper()
	st, err := os.Stat(filepath.Join(dir, packFileName(id)))
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}

// compactWaitFor — polling a condition until the deadline (the cycle runs in
// a goroutine).
func compactWaitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("не дождались: %s", what)
}

// --- SQL contract ------------------------------------------------------------

const (
	wantCompactRows = "SELECT hash, off, len, refcount FROM contents " +
		"WHERE pack_id = $1 ORDER BY off"
	wantCompactMove = "UPDATE contents SET pack_id = v.pack_id, off = v.off " +
		"FROM unnest($1::bytea[], $2::int4[], $3::int8[]) AS v(hash, pack_id, off) " +
		"WHERE contents.hash = v.hash AND contents.pack_id = $4 " +
		"AND contents.refcount > 0"
	wantCompactDead = "DELETE FROM contents " +
		"USING unnest($1::bytea[]) AS h(hash) " +
		"WHERE contents.hash = h.hash AND contents.pack_id = $2 " +
		"AND contents.refcount = 0"
	wantCompactLive = "SELECT count(*) FROM contents " +
		"WHERE pack_id = $1 AND refcount > 0"
	wantCompactAll      = "SELECT count(*) FROM contents WHERE pack_id = $1"
	wantCompactDeadPack = "DELETE FROM contents " +
		"WHERE pack_id = $1 AND refcount = 0"
)

// TestCompactSQLContract — the exact SQL texts against the contents schema
// (migration 000001: hash, pack_id, off, len, raw_len, refcount).
func TestCompactSQLContract(t *testing.T) {
	if sqlCompactRows != wantCompactRows || sqlCompactMove != wantCompactMove ||
		sqlCompactDead != wantCompactDead || sqlCompactLive != wantCompactLive ||
		sqlCompactAll != wantCompactAll || sqlCompactDeadPack != wantCompactDeadPack {
		t.Fatal("SQL компактификации изменился — обновите контракт и schema_test")
	}
}

// --- scan --------------------------------------------------------------------

// TestPackShortHeaderRecovered — a pack with a torn header (1..22 bytes):
// the writer rebuilds the header from scratch and writes (the C-1/C-7 crash
// window must not jam the store).
func TestPackShortHeaderRecovered(t *testing.T) {
	dir := t.TempDir()
	pool := newPackFakePool()
	// A "torn header write": a partial file without a header.
	if err := os.WriteFile(filepath.Join(dir, packFileName(1)), []byte("CDPK\x01"), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := OpenPackStore(pool, PackOptions{Dir: dir, PackSize: 1 << 20, MaxStoreBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close() //nolint:errcheck // the test closes the store in cleanup
	ctx := context.Background()
	d := []byte("контент в пак с обрывом заголовка")
	if _, err := st.Put(ctx, d); err != nil {
		t.Fatalf("Put в пак с обрывом заголовка: %v", err)
	}
	if got, err := st.Get(ctx, HashOf(d)); err != nil || string(got) != string(d) {
		t.Errorf("Get после восстановления заголовка: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, packFileName(1)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parsePackHeader(raw); err != nil {
		t.Errorf("заголовок не восстановлен: %v", err)
	}
}

// TestCompactShortPackSwept — a non-newest pack shorter than the header with
// no contents rows: the scan sees an "empty" pack (live share 0), the pass
// removes the file instead of an eternal error; rows on such a pack mean
// corruption (an error).
func TestCompactShortPackSwept(t *testing.T) {
	dir := t.TempDir()
	pool := &compactFakePool{packFakePool: newPackFakePool()}
	// Pack 1 — a torn header (6 bytes); pack 2 is the newest.
	if err := os.WriteFile(filepath.Join(dir, packFileName(1)), []byte("CDPK\x01\x02"), 0o644); err != nil {
		t.Fatal(err)
	}
	compactCraftPack(t, dir, 2, nil)

	st, err := OpenPackStore(pool, PackOptions{Dir: dir, PackSize: 1 << 20, MaxStoreBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close() //nolint:errcheck // the test closes the store in cleanup
	ctx := context.Background()

	info, err := st.CompactScan(ctx, 1)
	if err != nil {
		t.Fatalf("скан пака без заголовка: %v", err)
	}
	if info.Ratio != 0 || info.LiveBytes != 0 {
		t.Errorf("скан «пустого» пака: %+v, want нулевые живые байты", info)
	}
	stats, err := CompactOnce(ctx, st, CompactOptions{})
	if err != nil {
		t.Fatalf("CompactOnce: %v", err)
	}
	if stats.Compacted != 1 {
		t.Errorf("итог прохода %+v, want 1 удалённый пак-пустышка", stats)
	}
	if got := compactPackSize(t, dir, 1); got != 0 {
		t.Errorf("пак-пустышка не удалён: %d байт", got)
	}

	// Rows on a headerless pack mean corruption: the scan refuses.
	if err := os.WriteFile(filepath.Join(dir, packFileName(3)), []byte("CDPK\x01"), 0o644); err != nil {
		t.Fatal(err)
	}
	compactCraftRow(t, pool, "orphan", 3, 0, 8, 1)
	if _, err := st.CompactScan(ctx, 3); err == nil {
		t.Error("скан пака без заголовка со строками: ожидалась ошибка")
	}
}

// TestGetCorrupted — block byte corruption with a live row: Get answers
// ErrCorrupted (distinguishable from ErrNotFound and from a DB error).
func TestGetCorrupted(t *testing.T) {
	st, pool := newCompactTestStore(t, nil)
	ctx := context.Background()
	d := []byte("контент, чьи байты сейчас испортим")
	res, err := st.Put(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	row, _ := pool.row(res.Hash)
	path := filepath.Join(st.opts.Dir, packFileName(row.packID))
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("\xFF\xFF\xFF\xFF"), row.off); err != nil { // smearing the zstd frame
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get(ctx, res.Hash); !errors.Is(err, ErrCorrupted) {
		t.Errorf("Get испорченного блока: %v, want ErrCorrupted", err)
	}
	if _, err := st.Get(ctx, HashOf([]byte("никогда не писали"))); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get отсутствующего: %v, want ErrNotFound", err)
	}
}

// TestCompactCorruptBlock — a block unreadable while copying (a torn file
// tail) of a live row: the row is marked dead, the pack is removed, the live
// neighbors move, the integrity discrepancy is in the pass error.
func TestCompactCorruptBlock(t *testing.T) {
	st, pool := newCompactTestStore(t, nil)
	ctx := context.Background()
	var hashes []Hash
	for i := 0; i < 3; i++ {
		d := []byte(strings.Repeat(fmt.Sprintf("<p>сосед %d для порчи</p>", i), 10))
		res, err := st.Put(ctx, d)
		if err != nil {
			t.Fatal(err)
		}
		hashes = append(hashes, res.Hash)
	}
	compactCraftPack(t, st.opts.Dir, 2, nil) // the newest

	// Tear off the pack's tail: the last row's block extends past the end of
	// the file (a write crash/lost tail), reading it is an I/O error, not a
	// decoder error.
	row, _ := pool.row(hashes[2])
	if err := os.Truncate(filepath.Join(st.opts.Dir, packFileName(row.packID)), row.off-2); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get(ctx, hashes[2]); !errors.Is(err, ErrCorrupted) {
		t.Errorf("Get блока за концом файла: %v, want ErrCorrupted", err)
	}

	res, err := st.CompactPack(ctx, 1)
	if err == nil {
		t.Fatal("компактификация с нечитаемым блоком: ожидалась ошибка целостности")
	}
	if !strings.Contains(err.Error(), "блок нечитаем") {
		t.Errorf("ошибка не называет нечитаемый блок: %v", err)
	}
	if !res.Removed || res.Corrupted != 1 || res.CopiedBlocks != 2 || res.Tombstones != 1 {
		t.Errorf("итог %+v, want удалённый пак, 1 порченая и 2 переписанные строки", res)
	}
	// The corrupted row is dead (removed by compaction), the neighbors moved.
	if _, ok := pool.row(hashes[2]); ok {
		t.Error("строка нечитаемого блока не помечена мёртвой")
	}
	for _, h := range []Hash{hashes[0], hashes[1]} {
		if r, _ := pool.row(h); r.packID != 2 {
			t.Errorf("живой сосед %s не переехал: %+v", h, r)
		}
	}
}

// TestGetRetryAfterCompaction — a Get ↔ compaction race: the pack file is
// removed between reading the metadata and opening it (the row has already
// moved) — Get re-reads the metadata and answers with the content, not
// ENOENT.
func TestGetRetryAfterCompaction(t *testing.T) {
	st, pool := newCompactTestStore(t, nil)
	ctx := context.Background()
	d := []byte("контент, чей пак уходит под компактификацией")
	res, err := st.Put(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	row, _ := pool.row(res.Hash)

	// Compaction moves the record into pack 2 and removes the old file.
	raw1, err := os.ReadFile(filepath.Join(st.opts.Dir, packFileName(row.packID)))
	if err != nil {
		t.Fatal(err)
	}
	rec := raw1[row.off-4 : row.off+int64(row.clen)]
	hdr := packHeader{id: 2, maxSize: 1 << 20}
	if err := os.WriteFile(filepath.Join(st.opts.Dir, packFileName(2)),
		append(hdr.bytes(), rec...), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(st.opts.Dir, packFileName(row.packID))); err != nil {
		t.Fatal(err)
	}

	// The race pool: the first sqlMetaGet returns the old placement and only
	// then "moves" the row (as an UPDATE under compaction).
	rp := &getRacePool{compactFakePool: pool, hash: res.Hash,
		newRow: packFakeRow{packID: 2, off: packHeaderLen + 4, clen: row.clen, rawLen: row.rawLen, refc: row.refc}}
	st2, err := OpenPackStore(rp, PackOptions{Dir: st.opts.Dir, PackSize: 1 << 20, MaxStoreBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close() //nolint:errcheck // the test closes the store in cleanup

	got, err := st2.Get(ctx, res.Hash)
	if err != nil {
		t.Fatalf("Get в гонке с компактификацией: %v", err)
	}
	if string(got) != string(d) {
		t.Error("Get после переезда вернул чужие байты")
	}
	if !rp.moved {
		t.Error("повторного чтения метаданных не было — гонка не смоделирована")
	}
}

// getRacePool — a compactFakePool that on the first sqlMetaGet returns the
// row's old placement and simultaneously moves it to the new one (the
// "SELECT → file open" race window under compaction).
type getRacePool struct {
	*compactFakePool
	hash   Hash
	newRow packFakeRow
	moved  bool
}

func (p *getRacePool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows, err := p.packFakePool.Query(ctx, sql, args...)
	if err == nil && sql == sqlMetaGet && !p.moved {
		p.moved = true
		p.mu.Lock()
		nr := p.newRow
		p.contents[p.hash] = &nr
		p.mu.Unlock()
	}
	return rows, err
}

// TestCompactOrphanDeadRowsSwept — a row that died between reading the
// pack's rows and the repair transaction (C-13): the move skips it (the
// refcount > 0 guard), the batch deletion does not see it (it was read as
// live). The final sweep of the pack's dead rows must remove it before the
// file's unlink — otherwise a dead pointer to a nonexistent pack stays in
// contents forever.
func TestCompactOrphanDeadRowsSwept(t *testing.T) {
	st, pool := newCompactTestStore(t, nil)
	ctx := context.Background()
	d := []byte("контент, чья ссылка умрёт во время прохода")
	res, err := st.Put(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	// Pack 2 is the newest (closes pack 1 to writes and compaction).
	compactCraftPack(t, st.opts.Dir, 2, nil)

	// Kill the row at the moment the pack's rows are served: the SELECT has
	// already read it as live, the repair transaction will see refcount = 0
	// (as a Delete between the steps).
	dead := &killOnRowsPool{compactFakePool: pool, hash: res.Hash}
	st2, err := OpenPackStore(dead, PackOptions{Dir: st.opts.Dir, PackSize: 1 << 20, MaxStoreBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close() //nolint:errcheck // the test closes the store in cleanup

	if _, err := st2.CompactPack(ctx, 1); err != nil {
		t.Fatalf("CompactPack: %v", err)
	}
	if compactPackSize(t, st.opts.Dir, 1) != 0 {
		t.Error("файл пака 1 не удалён")
	}
	if _, ok := pool.row(res.Hash); ok {
		t.Error("осиротевшая мёртвая строка осталась в contents (утечка строк C-13)")
	}
}

// killOnRowsPool — a compaction pool that kills a row at the moment the
// pack's rows are served (simulating a Delete between the SELECT and the
// repair transaction, C-13).
type killOnRowsPool struct {
	*compactFakePool
	hash Hash
}

func (p *killOnRowsPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows, err := p.compactFakePool.Query(ctx, sql, args...)
	if err == nil && sql == sqlCompactRows {
		p.mu.Lock()
		if r := p.contents[p.hash]; r != nil {
			r.refc = 0
		}
		p.mu.Unlock()
	}
	return rows, err
}

// TestCompactScanMixed — the scan distinguishes live, dead, and garbage
// blocks: a live and a dead row, a Put crash-window block (without a row),
// and an unterminated tail.
func TestCompactScanMixed(t *testing.T) {
	st, pool := newCompactTestStore(t, nil)
	ctx := context.Background()
	live := []byte("<html>живой контент пака</html>")
	dead := []byte("<html>мёртвый контент пака</html>")
	orphan := []byte("<html>контент, чьи метаданные не закоммитятся</html>")

	for _, d := range [][]byte{live, dead} {
		if _, err := st.Put(ctx, d); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := st.Delete(ctx, HashOf(dead)); err != nil {
		t.Fatal(err)
	}
	// The third Put: addRef (Exec) passes, the append passes,
	// the metadata commit (Query 3 — after put live and put dead) fails —
	// a block without a row.
	pool.failOnQuery = 3
	if _, err := st.Put(ctx, orphan); err == nil {
		t.Fatal("Put с упавшей фиксацией метаданных должен вернуть ошибку")
	}
	// An unterminated tail: a length prefix of 255 and one data byte.
	f, err := os.OpenFile(filepath.Join(st.opts.Dir, packFileName(1)), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xFF, 0, 0, 0, 0x01}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	info, err := st.CompactScan(ctx, 1)
	if err != nil {
		t.Fatalf("CompactScan: %v", err)
	}
	lr, ok := pool.row(HashOf(live))
	if !ok {
		t.Fatal("нет строки живого контента")
	}
	dr, ok := pool.row(HashOf(dead))
	if !ok {
		t.Fatal("нет мёртвой строки")
	}
	wantLive := int64(4 + lr.clen)
	wantDead := int64(4 + dr.clen)
	size := compactPackSize(t, st.opts.Dir, 1)

	if info.Size != size {
		t.Errorf("Size = %d, want %d", info.Size, size)
	}
	if info.DataBytes != size-packHeaderLen {
		t.Errorf("DataBytes = %d, want %d", info.DataBytes, size-packHeaderLen)
	}
	if info.LiveBytes != wantLive || info.LiveBlocks != 1 {
		t.Errorf("живые: %d байт / %d блоков, want %d / 1", info.LiveBytes, info.LiveBlocks, wantLive)
	}
	if info.DeadBytes != wantDead || info.DeadBlocks != 1 {
		t.Errorf("мёртвые: %d байт / %d блоков, want %d / 1", info.DeadBytes, info.DeadBlocks, wantDead)
	}
	// Garbage: the crash-window block (4+len) and the 5-byte tail — one whole
	// block.
	orphanRow, ok := pool.row(HashOf(orphan))
	if ok {
		t.Fatalf("строка orphan не должна была зафиксироваться: %+v", orphanRow)
	}
	if want := size - packHeaderLen - wantLive - wantDead; info.GarbageBytes != want {
		t.Errorf("GarbageBytes = %d, want %d", info.GarbageBytes, want)
	}
	if info.GarbageBlocks != 1 {
		t.Errorf("GarbageBlocks = %d, want 1 (хвост — не блок)", info.GarbageBlocks)
	}
	if !info.Truncated {
		t.Error("Truncated = false, want true (недописанный хвост)")
	}
	if want := float64(wantLive) / float64(size-packHeaderLen); info.Ratio != want {
		t.Errorf("Ratio = %v, want %v", info.Ratio, want)
	}
}

// TestCompactScanValidation — a row pointing outside a whole file record
// means corrupted metadata: the scan refuses, compaction of such a pack is
// not started.
func TestCompactScanValidation(t *testing.T) {
	st, pool := newCompactTestStore(t, nil)
	ctx := context.Background()
	d := []byte("<html>контент с битой строкой</html>")
	if _, err := st.Put(ctx, d); err != nil {
		t.Fatal(err)
	}
	h := HashOf(d)
	row, _ := pool.row(h)

	for name, mutate := range map[string]func(){
		"блок за концом файла": func() { pool.contents[h].off = row.off + 1<<20 },
		"смещение в заголовке": func() { pool.contents[h].off = 4 },
		"чужой префикс длины":  func() { pool.contents[h].clen = row.clen + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			pool.contents[h].off, pool.contents[h].clen = row.off, row.clen
			mutate()
			if _, err := st.CompactScan(ctx, 1); err == nil {
				t.Error("скан битой строки: ожидалась ошибка")
			}
		})
	}
}

// --- threshold ----------------------------------------------------------------

// TestCompactThreshold49 — the live share below the threshold: the pack is
// rewritten and removed, the row moved into the newest pack, the dead row
// deleted.
func TestCompactThreshold49(t *testing.T) {
	dir := t.TempDir()
	pool := &compactFakePool{packFakePool: newPackFakePool()}
	liveBlock := bytes.Repeat([]byte("L"), 49)
	deadBlock := bytes.Repeat([]byte("D"), 51)
	offs := compactCraftPack(t, dir, 1, [][]byte{liveBlock, deadBlock})
	compactCraftPack(t, dir, 2, nil) // the newest, header only
	hLive := compactCraftRow(t, pool, "live", 1, offs[0], len(liveBlock), 2)
	hDead := compactCraftRow(t, pool, "dead", 1, offs[1], len(deadBlock), 0)
	oldSize := compactPackSize(t, dir, 1)

	st, err := OpenPackStore(pool, PackOptions{Dir: dir, PackSize: 1 << 20, MaxStoreBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close() //nolint:errcheck // the test closes the store in cleanup

	info, err := st.CompactScan(context.Background(), 1)
	if err != nil {
		t.Fatalf("CompactScan: %v", err)
	}
	if info.Ratio >= 0.5 {
		t.Fatalf("доля живых %v — тест рассчитан на строго меньше 0.5", info.Ratio)
	}

	stats, err := CompactOnce(context.Background(), st, CompactOptions{})
	if err != nil {
		t.Fatalf("CompactOnce: %v", err)
	}
	if stats.Compacted != 1 || stats.FreedBytes != oldSize {
		t.Fatalf("итог прохода %+v, want 1 пак / %d байт", stats, oldSize)
	}
	if got := compactPackSize(t, dir, 1); got != 0 {
		t.Errorf("старый пак существует (%d байт), want удалён", got)
	}
	row, ok := pool.row(hLive)
	if !ok {
		t.Fatal("живая строка исчезла")
	}
	if row.packID != 2 || row.off != packHeaderLen+4 {
		t.Errorf("живая строка: пак %d смещение %d, want пак 2 смещение %d",
			row.packID, row.off, packHeaderLen+4)
	}
	if row.refc != 2 || row.clen != int32(len(liveBlock)) {
		t.Errorf("живая строка изменила refcount/len: %+v", row)
	}
	if _, ok := pool.row(hDead); ok {
		t.Error("мёртвая строка не удалена")
	}
	if got := compactPackSize(t, dir, 2); got != packHeaderLen+4+int64(len(liveBlock)) {
		t.Errorf("новейший пак %d байт, want %d (заголовок + живой блок)",
			got, packHeaderLen+4+int64(len(liveBlock)))
	}
}

// TestCompactThreshold51 — the live share not below the threshold: the pass
// touches nothing.
func TestCompactThreshold51(t *testing.T) {
	dir := t.TempDir()
	pool := &compactFakePool{packFakePool: newPackFakePool()}
	liveBlock := bytes.Repeat([]byte("L"), 51)
	deadBlock := bytes.Repeat([]byte("D"), 49)
	offs := compactCraftPack(t, dir, 1, [][]byte{liveBlock, deadBlock})
	compactCraftPack(t, dir, 2, nil)
	hLive := compactCraftRow(t, pool, "live", 1, offs[0], len(liveBlock), 1)
	hDead := compactCraftRow(t, pool, "dead", 1, offs[1], len(deadBlock), 0)
	oldSize := compactPackSize(t, dir, 1)

	st, err := OpenPackStore(pool, PackOptions{Dir: dir, PackSize: 1 << 20, MaxStoreBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close() //nolint:errcheck // the test closes the store in cleanup

	stats, err := CompactOnce(context.Background(), st, CompactOptions{})
	if err != nil {
		t.Fatalf("CompactOnce: %v", err)
	}
	if stats != (CompactStats{Packs: 1}) {
		t.Errorf("итог прохода %+v, want только просмотр одного пака", stats)
	}
	if got := compactPackSize(t, dir, 1); got != oldSize {
		t.Errorf("пак изменён: %d байт, want %d", got, oldSize)
	}
	if row, _ := pool.row(hLive); row.packID != 1 || row.off != offs[0] {
		t.Errorf("живая строка переехала: %+v", row)
	}
	if _, ok := pool.row(hDead); !ok {
		t.Error("мёртвая строка удалена преждевременно")
	}
	if got := compactPackSize(t, dir, 2); got != packHeaderLen {
		t.Errorf("новейший пак дописан: %d байт, want только заголовок", got)
	}
}

// --- rewriting ----------------------------------------------------------------

// TestCompactPackRewrites — repairing a real pack: the bytes and offsets in
// the metadata are correct, Get works, the dead rows are removed, writing
// after compaction keeps going into the newest pack.
func TestCompactPackRewrites(t *testing.T) {
	st, pool := newCompactTestStore(t, nil)
	ctx := context.Background()
	pages := make(map[Hash][]byte)
	var hashes []Hash
	for i := 0; i < 5; i++ {
		d := []byte(strings.Repeat(fmt.Sprintf("<p>страница %d прогона компактификации</p>", i), 8))
		res, err := st.Put(ctx, d)
		if err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
		pages[res.Hash] = d
		hashes = append(hashes, res.Hash)
	}
	compactCraftPack(t, st.opts.Dir, 2, nil) // the newest pack
	for _, h := range hashes[2:] {           // three of five are dead
		if err := st.Delete(ctx, h); err != nil {
			t.Fatal(err)
		}
	}
	oldSize := compactPackSize(t, st.opts.Dir, 1)

	res, err := st.CompactPack(ctx, 1)
	if err != nil {
		t.Fatalf("CompactPack: %v", err)
	}
	if !res.Removed || res.FreedBytes != oldSize {
		t.Errorf("итог %+v, want Removed и FreedBytes = %d", res, oldSize)
	}
	if res.CopiedBlocks != 2 || res.Tombstones != 3 {
		t.Errorf("итог %+v, want 2 переписанных блока и 3 мёртвые строки", res)
	}
	if got := compactPackSize(t, st.opts.Dir, 1); got != 0 {
		t.Errorf("старый пак существует (%d байт)", got)
	}

	var copiedBytes int64
	pack2, err := os.ReadFile(filepath.Join(st.opts.Dir, packFileName(2)))
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range hashes[:2] {
		row, ok := pool.row(h)
		if !ok {
			t.Fatalf("живая строка %s исчезла", h)
		}
		if row.packID != 2 {
			t.Fatalf("%s: pack_id = %d, want 2", h, row.packID)
		}
		if prefix := binary.LittleEndian.Uint32(pack2[row.off-4 : row.off]); int32(prefix) != row.clen {
			t.Errorf("%s: префикс длины %d не сходится с len %d", h, prefix, row.clen)
		}
		got, err := st.Get(ctx, h)
		if err != nil || string(got) != string(pages[h]) {
			t.Errorf("Get(%s) после компактификации: %v, совпадение = %v", h, err, string(got) == string(pages[h]))
		}
		copiedBytes += 4 + int64(row.clen)
	}
	if res.CopiedBytes != copiedBytes {
		t.Errorf("CopiedBytes = %d, want %d", res.CopiedBytes, copiedBytes)
	}
	for _, h := range hashes[2:] {
		if _, ok := pool.row(h); ok {
			t.Errorf("мёртвая строка %s не удалена", h)
		}
	}

	// Writing after compaction goes into the newest pack, on top of the
	// rewritten one.
	after := []byte("<html>контент после компактификации</html>")
	ares, err := st.Put(ctx, after)
	if err != nil {
		t.Fatalf("Put после компактификации: %v", err)
	}
	row, _ := pool.row(ares.Hash)
	if row.packID != 2 {
		t.Errorf("запись после компактификации пошла в пак %d, want 2", row.packID)
	}
	if got, err := st.Get(ctx, ares.Hash); err != nil || string(got) != string(after) {
		t.Errorf("Get нового контента: %v", err)
	}
}

// --- crash windows ------------------------------------------------------------

// TestCompactCrashBeforeCommit — a transaction failure (after the blocks
// were rewritten): the rows are untouched, the old pack is intact, reading
// works, the copied blocks are garbage in the newest pack; a repeated pass
// finishes the compaction.
func TestCompactCrashBeforeCommit(t *testing.T) {
	for name, fail := range map[string]func(*compactFakePool){
		"Begin":  func(p *compactFakePool) { p.beginErr = errors.New("сбой Begin") },
		"Exec":   func(p *compactFakePool) { p.txExecErr = errors.New("сбой Exec") },
		"Commit": func(p *compactFakePool) { p.commitErr = errors.New("сбой Commit") },
	} {
		t.Run(name, func(t *testing.T) {
			st, pool := newCompactTestStore(t, nil)
			ctx := context.Background()
			var hashes []Hash
			pages := make(map[Hash][]byte)
			for i := 0; i < 4; i++ {
				d := []byte(strings.Repeat(fmt.Sprintf("<p>краш-окно %d</p>", i), 12))
				res, err := st.Put(ctx, d)
				if err != nil {
					t.Fatal(err)
				}
				hashes = append(hashes, res.Hash)
				pages[res.Hash] = d
			}
			compactCraftPack(t, st.opts.Dir, 2, nil)
			if err := st.Delete(ctx, hashes[0]); err != nil { // 3 live out of 4
				t.Fatal(err)
			}
			sizeBefore := compactPackSize(t, st.opts.Dir, 2)

			fail(pool)
			if _, err := st.CompactPack(ctx, 1); err == nil {
				t.Fatal("компактификация с упавшей транзакцией должна вернуть ошибку")
			}

			// The state did not change: the old pack is intact, the rows are in
			// place.
			if got := compactPackSize(t, st.opts.Dir, 1); got == 0 {
				t.Fatal("старый пак удалён при упавшей транзакции")
			}
			for _, h := range hashes[1:] {
				if row, _ := pool.row(h); row.packID != 1 {
					t.Fatalf("строка %s переехала без коммита", h)
				}
				if got, err := st.Get(ctx, h); err != nil || string(got) != string(pages[h]) {
					t.Fatalf("Get(%s) в краш-окне: %v", h, err)
				}
			}
			if got := compactPackSize(t, st.opts.Dir, 2); got <= sizeBefore {
				t.Errorf("новейший пак не вырос (%d ≤ %d) — блоки не переписывались", got, sizeBefore)
			}

			// A repeated pass finishes the compaction.
			pool.beginErr, pool.txExecErr, pool.commitErr = nil, nil, nil
			res, err := st.CompactPack(ctx, 1)
			if err != nil || !res.Removed {
				t.Fatalf("повторная компактификация: %v, %+v", err, res)
			}
			for _, h := range hashes[1:] {
				got, err := st.Get(ctx, h)
				if err != nil || string(got) != string(pages[h]) {
					t.Errorf("Get(%s) после повтора: %v", h, err)
				}
			}
			if got := compactPackSize(t, st.opts.Dir, 1); got != 0 {
				t.Errorf("старый пак не удалён: %d байт", got)
			}
		})
	}
}

// TestCompactCrashAfterCommitBeforeDelete — the state "the transactions are
// committed, the file is still in place" (a crash between the steps): reads
// go through the new placements, the next pass removes the garbage file.
func TestCompactCrashAfterCommitBeforeDelete(t *testing.T) {
	st, pool := newCompactTestStore(t, nil)
	ctx := context.Background()
	var hashes []Hash
	pages := make(map[Hash][]byte)
	for i := 0; i < 3; i++ {
		d := []byte(strings.Repeat(fmt.Sprintf("<p>хвост коммита %d</p>", i), 15))
		res, err := st.Put(ctx, d)
		if err != nil {
			t.Fatal(err)
		}
		hashes = append(hashes, res.Hash)
		pages[res.Hash] = d
	}
	compactCraftPack(t, st.opts.Dir, 2, nil)
	oldBytes, err := os.ReadFile(filepath.Join(st.opts.Dir, packFileName(1)))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := st.CompactPack(ctx, 1); err != nil {
		t.Fatalf("CompactPack: %v", err)
	}
	// Put the file back — the equivalent of a crash after COMMIT before the
	// unlink.
	if err := os.WriteFile(filepath.Join(st.opts.Dir, packFileName(1)), oldBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	for _, h := range hashes {
		got, err := st.Get(ctx, h)
		if err != nil || string(got) != string(pages[h]) {
			t.Fatalf("Get(%s) в состоянии «после коммита»: %v", h, err)
		}
		if row, _ := pool.row(h); row.packID != 2 {
			t.Fatalf("строка %s не на новых местах", h)
		}
	}

	stats, err := CompactOnce(ctx, st, CompactOptions{})
	if err != nil {
		t.Fatalf("CompactOnce: %v", err)
	}
	if stats.Compacted != 1 {
		t.Fatalf("итог прохода %+v, want 1 пак (файл-мусор удалён)", stats)
	}
	if got := compactPackSize(t, st.opts.Dir, 1); got != 0 {
		t.Errorf("файл-мусор не удалён: %d байт", got)
	}
	for _, h := range hashes {
		if got, err := st.Get(ctx, h); err != nil || string(got) != string(pages[h]) {
			t.Errorf("Get(%s) после повтора: %v", h, err)
		}
	}
}

// --- concurrency ---------------------------------------------------------------

// TestCompactConcurrentWriter — a concurrent writer into the newest pack
// does not interfere with compaction: the blocks interleave in the new pack,
// all contents are readable (the -race run is in the manual check).
func TestCompactConcurrentWriter(t *testing.T) {
	st, _ := newCompactTestStore(t, nil)
	ctx := context.Background()
	var hashes []Hash
	for i := 0; i < 10; i++ {
		d := []byte(strings.Repeat(fmt.Sprintf("<p>старый контент %02d</p>", i), 10))
		res, err := st.Put(ctx, d)
		if err != nil {
			t.Fatal(err)
		}
		hashes = append(hashes, res.Hash)
	}
	compactCraftPack(t, st.opts.Dir, 2, nil) // the newest pack
	for _, h := range hashes[:8] {           // 8 of 10 are dead
		if err := st.Delete(ctx, h); err != nil {
			t.Fatal(err)
		}
	}

	wctx, cancel := context.WithCancel(ctx)
	type put struct {
		h   Hash
		d   []byte
		err error
	}
	puts := make(chan put, 4096)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-wctx.Done():
				return
			default:
			}
			d := []byte(strings.Repeat(fmt.Sprintf("<p>горячая запись %05d</p>", i), 6))
			res, err := st.Put(wctx, d)
			// Sending with an exit on cancellation: the main test reads the
			// channel only after the writer stops.
			select {
			case puts <- put{h: res.Hash, d: d, err: err}:
			case <-wctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	res, err := st.CompactPack(ctx, 1)
	cancel()
	wg.Wait()
	close(puts)
	if err != nil {
		t.Fatalf("CompactPack с конкурентным писателем: %v", err)
	}
	if !res.Removed || res.CopiedBlocks != 2 {
		t.Fatalf("итог %+v, want 2 переписанных блока и удалённый пак", res)
	}

	for p := range puts {
		if p.err != nil {
			// A cancellation in flight is not a loss: the worker retries
			// (spec 3.4).
			if errors.Is(p.err, context.Canceled) {
				continue
			}
			t.Fatalf("конкурентный Put: %v", p.err)
		}
		got, err := st.Get(ctx, p.h)
		if err != nil || string(got) != string(p.d) {
			t.Fatalf("Get конкурентного контента %s: %v", p.h, err)
		}
	}
	// Old contents: the dead ones are invisible, the live ones are readable
	// at their new places.
	for i, h := range hashes {
		_, err := st.Get(ctx, h)
		if i < 8 {
			if !errors.Is(err, ErrNotFound) {
				t.Errorf("Get мёртвого %s: %v, want ErrNotFound", h, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("Get живого %s: %v", h, err)
		}
	}
}

// TestCompactWriterSyncsToNewest — a "lagging" writer always synchronizes
// with the newest pack (the T-042/T-045 invariant): writing after a new pack
// appears goes into it, and after the old pack is removed — into the newest,
// with no losses.
func TestCompactWriterSyncsToNewest(t *testing.T) {
	dir := t.TempDir()
	pool := &compactFakePool{packFakePool: newPackFakePool()}
	a, err := OpenPackStore(pool, PackOptions{Dir: dir, PackSize: 1 << 20, MaxStoreBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close() //nolint:errcheck // the test closes the store in cleanup
	b, err := OpenPackStore(pool, PackOptions{Dir: dir, PackSize: 1 << 20, MaxStoreBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close() //nolint:errcheck // the test closes the store in cleanup
	ctx := context.Background()

	// A writes first: pack 1; the content dies right away (compaction will
	// take the whole pack).
	x := []byte("<html>первый контент писателя A</html>")
	if _, err := a.Put(ctx, x); err != nil {
		t.Fatal(err)
	}
	if err := a.Delete(ctx, HashOf(x)); err != nil {
		t.Fatal(err)
	}
	// The newest pack 2 appears (another "process" rotated).
	compactCraftPack(t, dir, 2, nil)

	// The lagging A must switch to pack 2.
	z := []byte("<html>запись отстающего писателя</html>")
	zres, err := a.Put(ctx, z)
	if err != nil {
		t.Fatal(err)
	}
	if row, _ := pool.row(zres.Hash); row.packID != 2 {
		t.Fatalf("отстающий писатель дописал в пак %d, want 2 (всегда новейший)", row.packID)
	}

	if _, err := b.Put(ctx, []byte("<html>контент писателя B</html>")); err != nil {
		t.Fatal(err)
	}

	// Compacting pack 1 (live share 0) and writing after it.
	res, err := a.CompactPack(ctx, 1)
	if err != nil || !res.Removed {
		t.Fatalf("CompactPack(1): %v, %+v", err, res)
	}
	w := []byte("<html>запись после удаления старого пака</html>")
	wres, err := a.Put(ctx, w)
	if err != nil {
		t.Fatal(err)
	}
	if row, _ := pool.row(wres.Hash); row.packID != 2 {
		t.Errorf("запись после удаления пошла в пак %d, want 2", row.packID)
	}
	for _, d := range [][]byte{z, w} {
		if got, err := a.Get(ctx, HashOf(d)); err != nil || string(got) != string(d) {
			t.Errorf("Get: %v", err)
		}
	}
	if _, err := a.Get(ctx, HashOf(x)); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get мёртвого контента: %v, want ErrNotFound", err)
	}
}

// --- pass and cycle -------------------------------------------------------------

// TestCompactNewestNotCompacted — the newest pack (the only one) is not
// compacted: a direct refusal and an empty pass.
func TestCompactNewestNotCompacted(t *testing.T) {
	st, _ := newCompactTestStore(t, nil)
	ctx := context.Background()
	if _, err := st.Put(ctx, []byte("единственный пак")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CompactPack(ctx, 1); !errors.Is(err, errCompactNewest) {
		t.Errorf("CompactPack новейшего: %v, want errCompactNewest", err)
	}
	stats, err := CompactOnce(ctx, st, CompactOptions{})
	if err != nil || stats != (CompactStats{}) {
		t.Errorf("CompactOnce одного пака: %+v, %v, want пустой итог", stats, err)
	}
	if got := compactPackSize(t, st.opts.Dir, 1); got == 0 {
		t.Error("единственный пак удалён проходом")
	}
}

// TestCompactOnceErrors — the scan error of one pack is collected into the
// pass result, the other packs are processed.
func TestCompactOnceErrors(t *testing.T) {
	dir := t.TempDir()
	pool := &compactFakePool{packFakePool: newPackFakePool()}
	// Pack 1: only a dead row — compactable (live share 0).
	// Pack 2: a live row with a broken len — the scan will refuse. Pack 3 is
	// the newest.
	offs1 := compactCraftPack(t, dir, 1, [][]byte{bytes.Repeat([]byte("x"), 40)})
	offs2 := compactCraftPack(t, dir, 2, [][]byte{bytes.Repeat([]byte("y"), 40)})
	compactCraftPack(t, dir, 3, nil)
	hDead := compactCraftRow(t, pool, "dead", 1, offs1[0], 40, 0)
	hLive := compactCraftRow(t, pool, "live", 2, offs2[0], 40+7, 1) // len does not match

	st, err := OpenPackStore(pool, PackOptions{Dir: dir, PackSize: 1 << 20, MaxStoreBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close() //nolint:errcheck // the test closes the store in cleanup

	stats, err := CompactOnce(context.Background(), st, CompactOptions{})
	if err == nil {
		t.Fatal("CompactOnce с битым паком: ожидалась ошибка")
	}
	if stats.Compacted != 1 || stats.Packs != 2 {
		t.Fatalf("итог прохода %+v, want Packs 2 / Compacted 1 (битой пропущен)", stats)
	}
	if got := compactPackSize(t, dir, 1); got != 0 {
		t.Errorf("пак 1 не удалён: %d", got)
	}
	if _, ok := pool.row(hDead); ok {
		t.Error("мёртвая строка пака 1 не удалена")
	}
	if got := compactPackSize(t, dir, 2); got == 0 {
		t.Error("битый пак 2 удалён несмотря на ошибку скана")
	}
	if row, ok := pool.row(hLive); !ok || row.packID != 2 {
		t.Errorf("живая строка битого пака изменена: %+v", row)
	}
}

// TestCompactOnceFullDiskSkipsLivePacks — the "old + new" space peak
// (C-17): on a volume below the write-pause threshold the pass does not
// start copying live blocks (it would run into ENOSPC halfway), a pack with
// live data is postponed with a pass error; a pack without live blocks is
// processed even on a full disk — removing garbage frees space without
// copying.
func TestCompactOnceFullDiskSkipsLivePacks(t *testing.T) {
	newStore := func(t *testing.T) (*PackStore, *compactFakePool, string) {
		t.Helper()
		st, pool := newCompactTestStore(t, func(o *PackOptions) { o.DiskFreePercent = 10 })
		return st, pool, st.opts.Dir
	}
	fullDisk := func(st *PackStore) *fakeSpace {
		fake := &fakeSpace{}
		fake.set(1<<20, 100<<20) // 1% free — below the 10% threshold
		st.stat = fake
		return fake
	}

	// A pack with live blocks (the live share below the compaction threshold)
	// is postponed on a full disk, the file remains.
	st, pool, dir := newStore(t)
	ctx := context.Background()
	offs := compactCraftPack(t, dir, 1, [][]byte{
		bytes.Repeat([]byte("l"), 40),  // a live block
		bytes.Repeat([]byte("d"), 400), // a dead block
	})
	compactCraftRow(t, pool, "живой контент полного диска", 1, offs[0], 40, 1)
	compactCraftRow(t, pool, "мёртвый наполнитель", 1, offs[1], 400, 0)
	compactCraftPack(t, dir, 2, nil) // pack 2 is the newest, pack 1 is closed
	fullDisk(st)

	stats, err := CompactOnce(ctx, st, CompactOptions{})
	if err == nil {
		t.Fatal("CompactOnce ниже порога места: ожидалась ошибка отложенного пака")
	}
	if !strings.Contains(err.Error(), "отложена") {
		t.Errorf("ошибка прохода не про отложенный пак: %v", err)
	}
	if stats.Compacted != 0 {
		t.Errorf("Compacted = %d, want 0", stats.Compacted)
	}
	if got := compactPackSize(t, dir, 1); got == 0 {
		t.Error("пак с живыми блоками тронут на полном диске")
	}

	// A pack of pure garbage is processed even on a full disk.
	st2, pool2, dir2 := newStore(t)
	fullDisk(st2)
	h := HashOf([]byte("мёртвый контент полного диска"))
	offs2 := compactCraftPack(t, dir2, 1, [][]byte{bytes.Repeat([]byte("d"), 30)})
	compactCraftRow(t, pool2, "мёртвый контент полного диска", 1, offs2[0], 30, 0)
	compactCraftPack(t, dir2, 2, nil)

	stats2, err2 := CompactOnce(ctx, st2, CompactOptions{})
	if err2 != nil {
		t.Fatalf("CompactOnce пака без живых блоков на полном диске: %v", err2)
	}
	if stats2.Compacted != 1 {
		t.Errorf("Compacted = %d, want 1 (мусор удаляется без копирования)", stats2.Compacted)
	}
	if got := compactPackSize(t, dir2, 1); got != 0 {
		t.Error("пак без живых блоков не удалён на полном диске")
	}
	if _, ok := pool2.row(h); ok {
		t.Error("мёртвая строка не удалена")
	}
}

// TestCompactLoop — the cycle: the first pass fires immediately, a pass
// error does not stop it (a Warn and a retry on the tick), ctx cancellation
// is a clean nil.
func TestCompactLoop(t *testing.T) {
	st, pool := newCompactTestStore(t, nil)
	ctx := context.Background()
	var hashes []Hash
	for i := 0; i < 4; i++ {
		d := []byte(strings.Repeat(fmt.Sprintf("<p>цикл компактификации %d</p>", i), 9))
		res, err := st.Put(ctx, d)
		if err != nil {
			t.Fatal(err)
		}
		hashes = append(hashes, res.Hash)
	}
	compactCraftPack(t, st.opts.Dir, 2, nil)
	for _, h := range hashes[:3] {
		if err := st.Delete(ctx, h); err != nil {
			t.Fatal(err)
		}
	}

	lctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	// The first pass fails (a DB scan failure); after 30 ms the failure is
	// lifted — the cycle's tick retries and succeeds: a pass error does not
	// stop the cycle.
	pool.queryErr = errors.New("сбой БД")
	time.AfterFunc(30*time.Millisecond, func() {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		pool.queryErr = nil
	})
	go func() {
		done <- CompactLoop(lctx, st, CompactOptions{Interval: 10 * time.Millisecond})
	}()
	compactWaitFor(t, func() bool { return compactPackSize(t, st.opts.Dir, 1) == 0 },
		"компактификация в цикле")
	if _, ok := pool.row(hashes[3]); !ok {
		t.Fatal("живая строка потеряна циклом")
	}
	if got, err := st.Get(ctx, hashes[3]); err != nil || len(got) == 0 {
		t.Errorf("Get живого контента после цикла: %v", err)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("CompactLoop после отмены: %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CompactLoop не завершился после отмены ctx")
	}
}

// TestCompactOptionsDefaults — zeros and absurd values are replaced with
// defaults.
func TestCompactOptionsDefaults(t *testing.T) {
	o := CompactOptions{}.withDefaults()
	if o.Interval != DefaultCompactInterval || o.Threshold != DefaultCompactThreshold {
		t.Errorf("дефолты: %+v", o)
	}
	o = CompactOptions{Interval: -time.Second, Threshold: 2}.withDefaults()
	if o.Interval != DefaultCompactInterval || o.Threshold != DefaultCompactThreshold {
		t.Errorf("некорректные значения не заменены: %+v", o)
	}
	o = CompactOptions{Interval: time.Minute, Threshold: 0.3}.withDefaults()
	if o.Interval != time.Minute || o.Threshold != 0.3 {
		t.Errorf("явные значения искажены: %+v", o)
	}
}

// TestCompactEmptyDir — an empty directory: a pass with no work and no
// errors.
func TestCompactEmptyDir(t *testing.T) {
	st, _ := newCompactTestStore(t, nil)
	stats, err := CompactOnce(context.Background(), st, CompactOptions{})
	if err != nil || stats != (CompactStats{}) {
		t.Errorf("CompactOnce пустого каталога: %+v, %v", stats, err)
	}
}

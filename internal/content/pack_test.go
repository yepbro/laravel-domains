// Unit tests of PackStore (T-042): the ContentStore contract on real pack
// files in a temporary directory and contents metadata on a fake pgx pool
// (the ingest-test style: a pool with exact SQL and live refcount
// semantics). Behavior against live PostgreSQL — end-to-end store tests
// (T-046).
package content

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// --- fake pool: contents semantics without a DB -------------------------------

// packCall — a recorded SQL call with arguments.
type packCall struct {
	sql  string
	args []any
}

// packFakeRow — a contents row.
type packFakeRow struct {
	packID int32
	off    int64
	clen   int32
	rawLen int32
	refc   int32
}

// packFakePool — a db.DB with real semantics of the four metadata SQL
// statements (a map hash → row, refcount by the store's rules): the external
// StoreContract run (pack_contract_test.go, T-228) and the rotation tests
// need live state, not canned responses.
// Unknown SQL is an error: the pool pins the exact query texts.
type packFakePool struct {
	mu       sync.Mutex
	contents map[[HashLen]byte]*packFakeRow
	calls    []packCall

	execSeq    int   // the Exec counter for failOnExec
	failOnExec int   // fail the Exec with this ordinal number (1-based)
	execErr    error // what to fail it with (a generic DB error by default)

	querySeq    int // the Query counter for failOnQuery
	failOnQuery int // fail the Query with this ordinal number (1-based)
	queryErr    error

	failSQL string // the SQL that must fail failSQLCount times (batch crash windows, T-102)
	failN   int
}

func newPackFakePool() *packFakePool {
	return &packFakePool{contents: make(map[[HashLen]byte]*packFakeRow)}
}

func (p *packFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

func (p *packFakePool) record(ctx context.Context, sql string, args []any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, packCall{sql: sql, args: args})
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (p *packFakePool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := p.record(ctx, sql, args); err != nil {
		return pgconn.CommandTag{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.execSeq++
	if p.failOnExec == p.execSeq {
		err := p.execErr
		if err == nil {
			err = errors.New("packFakePool: сбой БД")
		}
		return pgconn.CommandTag{}, err
	}
	var h [HashLen]byte
	copy(h[:], args[0].([]byte))
	switch sql {
	case sqlMetaAddRef:
		if r := p.contents[h]; r != nil && r.refc > 0 {
			r.refc++
			return pgconn.NewCommandTag("UPDATE 1"), nil
		}
		return pgconn.NewCommandTag("UPDATE 0"), nil
	case sqlMetaDelRef:
		if r := p.contents[h]; r != nil && r.refc > 0 {
			r.refc--
			return pgconn.NewCommandTag("UPDATE 1"), nil
		}
		return pgconn.NewCommandTag("UPDATE 0"), nil
	default:
		return pgconn.CommandTag{}, fmt.Errorf("packFakePool: неожидаемый SQL: %s", sql)
	}
}

func (p *packFakePool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := p.record(ctx, sql, args); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.querySeq++
	if p.failOnQuery == p.querySeq {
		err := p.queryErr
		if err == nil {
			err = errors.New("packFakePool: сбой БД")
		}
		return nil, err
	}
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	if p.failSQL != "" && sql == p.failSQL && p.failN > 0 {
		p.failN--
		return nil, errors.New("packFakePool: сбой пачки")
	}
	switch sql {
	case sqlMetaPut:
		var h [HashLen]byte
		copy(h[:], args[0].([]byte))
		// RETURNING (xmax = 0): an insert is true, a conflict
		// (ON CONFLICT DO UPDATE) is false, as in PostgreSQL.
		if r := p.contents[h]; r != nil {
			r.refc++ // ON CONFLICT: the reference of a concurrent writer/a dead row
			r.packID, r.off = args[1].(int32), args[2].(int64)
			r.clen, r.rawLen = args[3].(int32), args[4].(int32)
			return &packRows{rows: [][]any{{false}}}, nil
		}
		p.contents[h] = &packFakeRow{
			packID: args[1].(int32), off: args[2].(int64),
			clen: args[3].(int32), rawLen: args[4].(int32), refc: 1,
		}
		return &packRows{rows: [][]any{{true}}}, nil
	case sqlMetaAddRefBatch:
		// The dedup batch (T-102; copy counters — T-228): +n to every
		// live hash (n copies in the batch), RETURNING for it.
		// The PutBatch invariant — hashes are unique (the step-0 byHash
		// dedup): live PostgreSQL with two join rows targeting one row would
		// update it once with an arbitrary pair, not sum the pairs —
		// the fake fails on a duplicate rather than silently diverging from
		// the server (review T-228, P3-2).
		hashes := args[0].([][]byte)
		counts := args[1].([]int32)
		seen := make(map[[HashLen]byte]bool, len(hashes))
		var rows [][]any
		for i, hb := range hashes {
			var h [HashLen]byte
			copy(h[:], hb)
			if seen[h] {
				return nil, fmt.Errorf("packFakePool: дубликат хэша %x в дедуп-пачке — нарушен инвариант PutBatch (byHash)", h)
			}
			seen[h] = true
			if r := p.contents[h]; r != nil && r.refc > 0 {
				r.refc += counts[i]
				rows = append(rows, []any{h[:]})
			}
		}
		return &packRows{rows: rows}, nil
	case sqlMetaPutBatch:
		// The batched upsert (T-102): rows by unnest arrays; a conflict —
		// refcount += n and a placement move; RETURNING hash, (xmax = 0).
		hashes := args[0].([][]byte)
		packs := args[1].([]int32)
		offs := args[2].([]int64)
		clens := args[3].([]int32)
		rawLens := args[4].([]int32)
		counts := args[5].([]int32)
		rows := make([][]any, 0, len(hashes))
		for i, hb := range hashes {
			var h [HashLen]byte
			copy(h[:], hb)
			if r := p.contents[h]; r != nil {
				r.refc += counts[i]
				r.packID, r.off = packs[i], offs[i]
				r.clen, r.rawLen = clens[i], rawLens[i]
				rows = append(rows, []any{h[:], false})
				continue
			}
			p.contents[h] = &packFakeRow{
				packID: packs[i], off: offs[i], clen: clens[i],
				rawLen: rawLens[i], refc: counts[i],
			}
			rows = append(rows, []any{h[:], true})
		}
		return &packRows{rows: rows}, nil
	case sqlMetaGet:
		var h [HashLen]byte
		copy(h[:], args[0].([]byte))
		if r := p.contents[h]; r != nil && r.refc > 0 {
			return &packRows{rows: [][]any{{r.packID, r.off, r.clen, r.rawLen}}}, nil
		}
		return &packRows{}, nil // pgx: zero rows → ErrNoRows at the caller
	default:
		return nil, fmt.Errorf("packFakePool: неожидаемый SQL: %s", sql)
	}
}

// row — a snapshot of a contents row for assertions (refcount, offsets).
func (p *packFakePool) row(h Hash) (packFakeRow, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	r, ok := p.contents[h]
	if !ok {
		return packFakeRow{}, false
	}
	return packFakeRow(*r), true
}

// dropRow — remove a contents row from the fake (the retention/
// compaction path: the row goes away entirely, not via the counter).
func (p *packFakePool) dropRow(h Hash) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.contents, h)
}

func (p *packFakePool) snapshotCalls() []packCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]packCall(nil), p.calls...)
}

// packRows — pgx.Rows with canned rows; Scan is positional.
type packRows struct {
	rows   [][]any
	i      int
	closed bool
}

func (r *packRows) Close() { r.closed = true }

func (r *packRows) Err() error { return nil }

func (r *packRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *packRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *packRows) Next() bool {
	if r.closed || r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *packRows) Scan(dest ...any) error {
	if r.closed || r.i == 0 || r.i > len(r.rows) {
		return errors.New("packRows: Scan без Next")
	}
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("packRows: приёмников %d, колонок %d", len(dest), len(row))
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

func (r *packRows) Values() ([]any, error) { return r.rows[r.i-1], nil }

func (r *packRows) RawValues() [][]byte { return nil }

func (r *packRows) Conn() *pgx.Conn      { return nil }
func (r *packRows) TypeMap() *pgtype.Map { return nil }

// --- the shared store constructor for tests --------------------------------

// newPackTestStore — a PackStore in a temporary directory with a fake pool.
// A small PackSize by default: some tests rely on rotation,
// the contract probes it too.
func newPackTestStore(t *testing.T, mutate func(*PackOptions)) (*PackStore, *packFakePool) {
	t.Helper()
	opts := PackOptions{Dir: t.TempDir(), PackSize: 64 << 10, MaxStoreBytes: 1 << 20}
	if mutate != nil {
		mutate(&opts)
	}
	pool := newPackFakePool()
	st, err := OpenPackStore(pool, opts)
	if err != nil {
		t.Fatalf("OpenPackStore: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, pool
}

// TestStoreContractOnPack moved to the external test package
// (pack_contract_test.go, T-228): the StoreContract suite lives in
// contenttest, which imports content — the package's internal _test files
// cannot import contenttest (an import cycle in the test), while the
// external content_test can (go rebinds its dependency on content to the
// test variant of the package); the store factory is export_test.go.

// --- expected SQL (the contract against schema 000001) -----------------------

const (
	wantSQLAddRef = "UPDATE contents SET refcount = refcount + 1 " +
		"WHERE hash = $1 AND refcount > 0"
	wantSQLPut = "INSERT INTO contents (hash, pack_id, off, len, raw_len, refcount) " +
		"VALUES ($1, $2, $3, $4, $5, 1) " +
		"ON CONFLICT (hash) DO UPDATE SET refcount = contents.refcount + 1, " +
		"pack_id = EXCLUDED.pack_id, off = EXCLUDED.off, " +
		"len = EXCLUDED.len, raw_len = EXCLUDED.raw_len " +
		"RETURNING (xmax = 0) AS created"
	wantSQLGet = "SELECT pack_id, off, len, raw_len FROM contents " +
		"WHERE hash = $1 AND refcount > 0"
	wantSQLDelRef = "UPDATE contents SET refcount = refcount - 1 " +
		"WHERE hash = $1 AND refcount > 0"
)

// TestPackSQLContract — the metadata SQL and parameters: the exact texts,
// the Put/Get/Delete call order, the argument types. The contents schema is
// migration 000001 (hash, pack_id, off, len, raw_len, refcount).
func TestPackSQLContract(t *testing.T) {
	if sqlMetaAddRef != wantSQLAddRef || sqlMetaPut != wantSQLPut ||
		sqlMetaGet != wantSQLGet || sqlMetaDelRef != wantSQLDelRef {
		t.Fatal("SQL метаданных изменился — обновите контракт и schema_test")
	}

	st, pool := newPackTestStore(t, nil)
	ctx := context.Background()
	data := []byte("<html>контент для SQL-контракта</html>")
	h := HashOf(data)

	res, err := st.Put(ctx, data)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !res.Created {
		t.Fatal("первый Put: Created = false")
	}
	if _, err := st.Get(ctx, h); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if err := st.Delete(ctx, h); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	calls := pool.snapshotCalls()
	if len(calls) != 4 {
		t.Fatalf("вызовов БД %d, want 4 (addRef, put, get, delRef)", len(calls))
	}
	wantSQL := []string{sqlMetaAddRef, sqlMetaPut, sqlMetaGet, sqlMetaDelRef}
	for i, want := range wantSQL {
		if calls[i].sql != want {
			t.Errorf("вызов %d: SQL %q, want %q", i, calls[i].sql, want)
		}
	}
	if got := calls[0].args[0].([]byte); string(got) != string(h[:]) {
		t.Errorf("addRef: хэш-аргумент %x, want %x", got, h)
	}
	putArgs := calls[1].args
	if len(putArgs) != 5 {
		t.Fatalf("sqlMetaPut: аргументов %d, want 5", len(putArgs))
	}
	if got := putArgs[0].([]byte); string(got) != string(h[:]) {
		t.Errorf("sqlMetaPut: хэш %x, want %x", got, h)
	}
	if pid, ok := putArgs[1].(int32); !ok || pid != 1 {
		t.Errorf("sqlMetaPut: pack_id = %v (%T), want int32(1)", putArgs[1], putArgs[1])
	}
	off, ok := putArgs[2].(int64)
	if !ok {
		t.Fatalf("sqlMetaPut: off = %v (%T), want int64", putArgs[2], putArgs[2])
	}
	if off != int64(packHeaderLen+4) {
		t.Errorf("sqlMetaPut: off = %d, want %d (заголовок + префикс длины)", off, packHeaderLen+4)
	}
	if clen, ok := putArgs[3].(int32); !ok || clen <= 0 {
		t.Errorf("sqlMetaPut: len = %v (%T), want положительный int32", putArgs[3], putArgs[3])
	}
	if rl, ok := putArgs[4].(int32); !ok || int(rl) != len(data) {
		t.Errorf("sqlMetaPut: raw_len = %v (%T), want int32(%d)", putArgs[4], putArgs[4], len(data))
	}
}

// --- pack format --------------------------------------------------------------

// TestPackHeaderAndOffsets — the file format: the v1 header (magic, version,
// length, id, rotation threshold, dictionary ID) and the records
// [len u32 LE][zstd block]; the off/len metadata point at the compressed
// block data.
func TestPackHeaderAndOffsets(t *testing.T) {
	st, pool := newPackTestStore(t, func(o *PackOptions) { o.PackSize = 1 << 20 })
	ctx := context.Background()
	byHash := make(map[Hash][]byte)
	for i := 0; i < 3; i++ {
		d := []byte(strings.Repeat(fmt.Sprintf("<p>страница %d</p>", i), 40))
		if _, err := st.Put(ctx, d); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
		byHash[HashOf(d)] = d
	}

	raw, err := os.ReadFile(filepath.Join(st.opts.Dir, packFileName(1)))
	if err != nil {
		t.Fatalf("чтение пака: %v", err)
	}
	hdr, err := parsePackHeader(raw)
	if err != nil {
		t.Fatalf("заголовок: %v", err)
	}
	if hdr.id != 1 {
		t.Errorf("заголовок: id = %d, want 1", hdr.id)
	}
	if hdr.maxSize != 1<<20 {
		t.Errorf("заголовок: maxSize = %d, want %d", hdr.maxSize, 1<<20)
	}
	if hdr.dictID != 0 {
		t.Errorf("заголовок: dictID = %d, want 0 (словарь не установлен)", hdr.dictID)
	}
	if string(raw[:4]) != packMagic || raw[4] != packVersion {
		t.Errorf("заголовок: magic/версия = %q/%d", raw[:4], raw[4])
	}
	if ln := binary.LittleEndian.Uint16(raw[5:7]); int(ln) != packHeaderLen {
		t.Errorf("заголовок: hdrLen = %d, want %d", ln, packHeaderLen)
	}

	// Each block: the offset from the metadata, with the length prefix in
	// front of the block.
	seen := 0
	for h, d := range byHash {
		row, ok := pool.row(h)
		if !ok {
			t.Fatalf("нет метаданных для %s", h)
		}
		if row.packID != 1 {
			t.Errorf("%s: pack_id = %d, want 1", h, row.packID)
		}
		prefix := binary.LittleEndian.Uint32(raw[row.off-4 : row.off])
		if int32(prefix) != row.clen {
			t.Errorf("%s: префикс длины %d не сходится с len метаданных %d", h, prefix, row.clen)
		}
		got, err := DecompressWithDict(raw[row.off : row.off+int64(row.clen)])
		if err != nil {
			t.Fatalf("%s: распаковка блока: %v", h, err)
		}
		if string(got) != string(d) {
			t.Errorf("%s: блок не совпал с записанным контентом", h)
		}
		if int32(len(d)) != row.rawLen {
			t.Errorf("%s: raw_len = %d, want %d", h, row.rawLen, len(d))
		}
		seen++
	}
	if seen != 3 {
		t.Errorf("проверено блоков %d, want 3", seen)
	}
}

// TestPackRotation — rotation by size: several small packs, all contents
// readable, pack_ids monotonic and gap-free, the pack size does not exceed
// the threshold (except the first block — the exception drives rotation
// forward).
func TestPackRotation(t *testing.T) {
	const packSize = 400
	st, pool := newPackTestStore(t, func(o *PackOptions) { o.PackSize = packSize })
	ctx := context.Background()

	var maxClen int32
	hashes := make([]Hash, 0, 10)
	for i := 0; i < 10; i++ {
		d := []byte(strings.Repeat(fmt.Sprintf("байт контента %02d;", i), 30))
		res, err := st.Put(ctx, d)
		if err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
		hashes = append(hashes, res.Hash)
		if row, ok := pool.row(res.Hash); ok && row.clen > maxClen {
			maxClen = row.clen
		}
	}

	ents, err := os.ReadDir(st.opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int
	for _, e := range ents {
		if id, ok := packIDFromName(e.Name()); ok {
			ids = append(ids, int(id))
			fi, err := e.Info()
			if err != nil {
				t.Fatal(err)
			}
			if bound := int64(packSize) + 4 + int64(maxClen); fi.Size() > bound {
				t.Errorf("пак %d: размер %d > порог+первый блок %d", id, fi.Size(), bound)
			}
			if fi.Size() <= packHeaderLen {
				t.Errorf("пак %d: размер %d — только заголовок, ротация создала пустой пак", id, fi.Size())
			}
			raw, err := os.ReadFile(filepath.Join(st.opts.Dir, e.Name()))
			if err != nil {
				t.Fatal(err)
			}
			hdr, err := parsePackHeader(raw)
			if err != nil {
				t.Errorf("пак %d: %v", id, err)
				continue
			}
			if hdr.id != id {
				t.Errorf("пак %d: заголовок называет %d", id, hdr.id)
			}
		}
	}
	if len(ids) < 2 {
		t.Fatalf("ротация не сработала: паков %d при пороге %d байт", len(ids), packSize)
	}
	for i, id := range ids {
		if id != i+1 {
			t.Fatalf("pack_id с дырами: %v", ids)
		}
	}

	// All contents are readable after rotation.
	for i, h := range hashes {
		got, err := st.Get(ctx, h)
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		want := []byte(strings.Repeat(fmt.Sprintf("байт контента %02d;", i), 30))
		if string(got) != string(want) {
			t.Errorf("Get(%d): контент не совпал", i)
		}
	}
}

// TestPackDedupNoBytes — a repeated Put of the same content: a reference
// only (UPDATE), no bytes appended to the pack, Created = false.
func TestPackDedupNoBytes(t *testing.T) {
	st, pool := newPackTestStore(t, func(o *PackOptions) { o.PackSize = 1 << 20 })
	ctx := context.Background()
	d := []byte("парковочная страница для дедупликации")

	first, err := st.Put(ctx, d)
	if err != nil {
		t.Fatalf("первый Put: %v", err)
	}
	path := filepath.Join(st.opts.Dir, packFileName(1))
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	callsBefore := len(pool.snapshotCalls())

	second, err := st.Put(ctx, append([]byte(nil), d...))
	if err != nil {
		t.Fatalf("второй Put: %v", err)
	}
	if second.Created {
		t.Error("второй Put: Created = true, want false")
	}
	if second.Hash != first.Hash {
		t.Error("второй Put: другой хэш")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Errorf("повторный Put дописал байты: было %d, стало %d", len(before), len(after))
	}
	calls := pool.snapshotCalls()
	if len(calls) != callsBefore+1 || calls[len(calls)-1].sql != sqlMetaAddRef {
		t.Errorf("повторный Put: ожидался только %s, вызовы %v", sqlMetaAddRef, calls[callsBefore:])
	}
	if row, _ := pool.row(first.Hash); row.refc != 2 {
		t.Errorf("refcount = %d, want 2", row.refc)
	}
}

// TestPackCrashWindow — a crash between the block spool and the metadata:
// the block is in the pack but invisible (Get — ErrNotFound); writing again
// works.
func TestPackCrashWindow(t *testing.T) {
	st, pool := newPackTestStore(t, func(o *PackOptions) { o.PackSize = 1 << 20 })
	ctx := context.Background()
	d := []byte("контент, чьи метаданные не закоммитятся")
	pool.failOnQuery = 1 // the first Exec (addRef) passes, the metadata commit (Query 1) fails

	if _, err := st.Put(ctx, d); err == nil {
		t.Fatal("Put с упавшей БД метаданных должен вернуть ошибку")
	}
	h := HashOf(d)
	if _, err := st.Get(ctx, h); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get после краш-окна: %v, want ErrNotFound (блок без метаданных невидим)", err)
	}
	raw, err := os.ReadFile(filepath.Join(st.opts.Dir, packFileName(1)))
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(raw)) <= packHeaderLen {
		t.Fatal("мусорный блок не дописан в пак")
	}
	if row, ok := pool.row(h); ok {
		t.Fatalf("метаданные всё же появились: %+v", row)
	}

	// Writing the same content again fixes the situation.
	res, err := st.Put(ctx, d)
	if err != nil {
		t.Fatalf("повторный Put: %v", err)
	}
	if !res.Created {
		t.Error("повторный Put после краш-окна: Created = false, want true")
	}
	got, err := st.Get(ctx, h)
	if err != nil || string(got) != string(d) {
		t.Errorf("Get после повторной записи: %v, совпадение = %v", err, string(got) == string(d))
	}
}

// TestPackDeadRowInvisible — refcount = 0: the row remains ("dead"),
// Get does not see it, Delete refuses; a new Put writes fresh bytes
// (Created = true, a new offset) instead of resurrecting the old block.
func TestPackDeadRowInvisible(t *testing.T) {
	st, pool := newPackTestStore(t, func(o *PackOptions) { o.PackSize = 1 << 20 })
	ctx := context.Background()
	d := []byte("контент с умирающей ссылкой")

	res, err := st.Put(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	oldRow, _ := pool.row(res.Hash)
	if err := st.Delete(ctx, res.Hash); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get(ctx, res.Hash); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get мёртвой строки: %v, want ErrNotFound", err)
	}
	if err := st.Delete(ctx, res.Hash); !errors.Is(err, ErrNotFound) {
		t.Errorf("Delete мёртвой строки: %v, want ErrNotFound", err)
	}
	if row, ok := pool.row(res.Hash); !ok {
		t.Fatal("мёртвая строка должна остаться в contents до компактификации (T-045)")
	} else if row.refc != 0 {
		t.Fatalf("refcount мёртвой строки = %d, want 0", row.refc)
	}

	again, err := st.Put(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	// A conflict with a dead row — ON CONFLICT DO UPDATE, not an insert:
	// Created = false (RETURNING (xmax = 0)) although a fresh block is
	// written and the placement is moved onto it.
	if again.Created {
		t.Error("Put после исчерпания ссылок: Created = true, want false (конфликт мёртвой строки)")
	}
	newRow, _ := pool.row(res.Hash)
	if newRow.off == oldRow.off && newRow.packID == oldRow.packID {
		t.Error("новый Put оставил размещение мёртвой строки — ожидались свежие байты")
	}
	if got, err := st.Get(ctx, res.Hash); err != nil || string(got) != string(d) {
		t.Errorf("Get воскрешённого контента: %v", err)
	}
}

// TestPackRefcountFSM — an invariant pass over the refcount automaton
// (T-265, TZ-fsm §3 FR-11; the specification is the pack.go doc comment):
// the chain T1→T2→T4→T5 (absent → live → tombstone), the ban on double
// removal (I4), the dead state invisible to Get and dedup (I2), the T6
// resurrection with the placement moved onto a fresh block (I3), and the T7
// "no row" reading.
// The adjacent behavioral tests (TestPackDeadRowInvisible, the StoreContract
// suite) cover the same statements by scenario — here
// the states are pinned explicitly, by the transition table. Live PostgreSQL —
// test/integration/content_test.go (TestContent_RefcountFSM).
func TestPackRefcountFSM(t *testing.T) {
	st, pool := newPackTestStore(t, func(o *PackOptions) { o.PackSize = 1 << 20 })
	ctx := context.Background()
	d := []byte("автомат refcount: absent → live(n) → dead → deleted")

	// T1: S0 absent → S1(1) by insert, Created = true.
	res, err := st.Put(ctx, d)
	if err != nil {
		t.Fatalf("Put нового контента: %v", err)
	}
	if !res.Created {
		t.Error("первый Put: Created = false, want true (П1 — вставка)")
	}
	live1, ok := pool.row(res.Hash)
	if !ok || live1.refc != 1 {
		t.Fatalf("после П1: строка %+v (found=%v), want refcount 1", live1, ok)
	}

	// T2: S1(1) → S1(2) by dedup, no bytes written.
	dup, err := st.Put(ctx, append([]byte(nil), d...))
	if err != nil {
		t.Fatalf("Put-дедуп: %v", err)
	}
	if dup.Created {
		t.Error("Put-дедуп: Created = true, want false (П2)")
	}
	if row, _ := pool.row(res.Hash); row.refc != 2 {
		t.Errorf("после П2: refcount = %d, want 2", row.refc)
	}

	// T4: S1(2) → S1(1) — one reference removed.
	if err := st.Delete(ctx, res.Hash); err != nil {
		t.Fatalf("Delete при двух ссылках: %v", err)
	}
	if row, _ := pool.row(res.Hash); row.refc != 1 {
		t.Errorf("после П4: refcount = %d, want 1", row.refc)
	}

	// T5: S1(1) → S2 dead·tombstone — the row remains with refcount = 0.
	if err := st.Delete(ctx, res.Hash); err != nil {
		t.Fatalf("Delete последней ссылки: %v", err)
	}
	row, ok := pool.row(res.Hash)
	if !ok {
		t.Fatal("П5: мёртвая строка обязана остаться в contents до компактификации (tombstone)")
	}
	if row.refc != 0 {
		t.Fatalf("П5: refcount = %d, want 0", row.refc)
	}

	// I2: the dead row is invisible to Get; dedup does not see it (the next
	// Put goes into the upsert, not AddRef); I4: a double removal —
	// ErrNotFound.
	if _, err := st.Get(ctx, res.Hash); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get мёртвой (И2): %v, want ErrNotFound", err)
	}
	if err := st.Delete(ctx, res.Hash); !errors.Is(err, ErrNotFound) {
		t.Errorf("повторный Delete (И4): %v, want ErrNotFound", err)
	}

	// T6: S2 → S1(1) resurrection — DO UPDATE from zero, the placement
	// is moved onto the fresh block (EXCLUDED), Created = false.
	res2, err := st.Put(ctx, d)
	if err != nil {
		t.Fatalf("Put воскрешения: %v", err)
	}
	if res2.Created {
		t.Error("воскрешение tombstone: Created = true, want false (П6 — DO UPDATE)")
	}
	rised, ok := pool.row(res.Hash)
	if !ok || rised.refc != 1 {
		t.Fatalf("после П6: строка %+v (found=%v), want refcount 1", rised, ok)
	}
	if rised.packID == live1.packID && rised.off == live1.off {
		t.Error("П6: размещение не переведено на свежий блок (EXCLUDED)")
	}
	if got, err := st.Get(ctx, res.Hash); err != nil || string(got) != string(d) {
		t.Errorf("Get воскрешённого: %v", err)
	}

	// T7 (the "no row" reading): deleting the row by hand — the retention/
	// compaction path (retentionDeleteSQL/sqlCompactDeadPack) —
	// and resurrecting via the insert branch: Created = true,
	// indistinguishable from new content.
	pool.dropRow(res.Hash)
	if _, err := st.Get(ctx, res.Hash); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get отсутствующей строки: %v, want ErrNotFound", err)
	}
	if err := st.Delete(ctx, res.Hash); !errors.Is(err, ErrNotFound) {
		t.Errorf("Delete отсутствующей строки (И4): %v, want ErrNotFound", err)
	}
	res3, err := st.Put(ctx, d)
	if err != nil {
		t.Fatalf("Put после удаления строки: %v", err)
	}
	if !res3.Created {
		t.Error("воскрешение отсутствующей строки: Created = false, want true (П7 — INSERT)")
	}
	if row, ok := pool.row(res.Hash); !ok || row.refc != 1 {
		t.Errorf("после П7: строка %+v (found=%v), want refcount 1", row, ok)
	}
}

// TestPackTooLarge — the max_store_bytes limit: the content is rejected
// entirely (ErrTooLarge), not a byte into the pack, no metadata row; empty
// content is allowed (the T-041 contract).
func TestPackTooLarge(t *testing.T) {
	st, pool := newPackTestStore(t, func(o *PackOptions) {
		o.PackSize = 1 << 20
		o.MaxStoreBytes = 32
	})
	ctx := context.Background()

	if _, err := st.Put(ctx, make([]byte, 33)); !errors.Is(err, ErrTooLarge) {
		t.Errorf("Put сверх лимита: %v, want ErrTooLarge", err)
	}
	ents, err := os.ReadDir(st.opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if _, ok := packIDFromName(e.Name()); ok {
			t.Errorf("после отвергнутого Put существует пак %s", e.Name())
		}
	}
	if calls := pool.snapshotCalls(); len(calls) != 0 {
		t.Errorf("после отвергнутого Put были вызовы БД: %v", calls)
	}
	if _, err := st.Get(ctx, HashOf(make([]byte, 33))); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get отвергнутого контента: %v, want ErrNotFound", err)
	}

	if res, err := st.Put(ctx, nil); err != nil || !res.Created {
		t.Errorf("Put пустого контента: %v, Created = %v", err, res.Created)
	}
	if got, err := st.Get(ctx, HashOf(nil)); err != nil || len(got) != 0 {
		t.Errorf("Get пустого контента: %v, длина %d", err, len(got))
	}
}

// TestPackReopen — reopening the directory: the newest pack is picked up,
// the previous run's content is readable, writing continues into the same
// pack.
func TestPackReopen(t *testing.T) {
	dir := t.TempDir()
	pool := newPackFakePool()
	ctx := context.Background()

	st1, err := OpenPackStore(pool, PackOptions{Dir: dir, PackSize: 1 << 20, MaxStoreBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	d := []byte("контент первого запуска")
	h := HashOf(d)
	if _, err := st1.Put(ctx, d); err != nil {
		t.Fatal(err)
	}
	if err := st1.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := OpenPackStore(pool, PackOptions{Dir: dir, PackSize: 1 << 20, MaxStoreBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close() //nolint:errcheck // the test closes the store in cleanup
	if got, err := st2.Get(ctx, h); err != nil || string(got) != string(d) {
		t.Fatalf("Get после переоткрытия: %v", err)
	}
	if _, err := st2.Put(ctx, []byte("контент второго запуска")); err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var packs []string
	for _, e := range ents {
		if _, ok := packIDFromName(e.Name()); ok {
			packs = append(packs, e.Name())
		}
	}
	if len(packs) != 1 || packs[0] != packFileName(1) {
		t.Errorf("после переоткрытия паки %v, want [%s] (запись продолжает тот же пак)", packs, packFileName(1))
	}
}

// TestPackDictOnOff — the T-043 dictionary: a pack created with a dictionary
// remembers its ID in the header; without a dictionary — dictID 0. The
// readability of records by their dictionary is the T-043 limitation (see
// docs/configuration.md).
func TestPackDictOnOff(t *testing.T) {
	corpus := dictCorpus(24, 2048)
	dict, err := TrainDict(corpus, TrainOptions{DictSize: 16 << 10})
	if err != nil {
		t.Fatalf("TrainDict: %v", err)
	}
	SetDict(dict)
	defer SetDict(nil)

	st, _ := newPackTestStore(t, func(o *PackOptions) { o.PackSize = 1 << 20 })
	ctx := context.Background()
	d := corpus[0]
	res, err := st.Put(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	got, err := st.Get(ctx, res.Hash)
	if err != nil || string(got) != string(d) {
		t.Fatalf("Get со словарём: %v, совпадение = %v", err, string(got) == string(d))
	}
	raw, err := os.ReadFile(filepath.Join(st.opts.Dir, packFileName(1)))
	if err != nil {
		t.Fatal(err)
	}
	hdr, err := parsePackHeader(raw)
	if err != nil {
		t.Fatal(err)
	}
	if hdr.dictID != dict.ID() {
		t.Errorf("заголовок пака: dictID = %d, want %d", hdr.dictID, dict.ID())
	}

	// The dictionary is off: a new directory, a dictionary-less roundtrip,
	// dictID 0.
	SetDict(nil)
	st2, _ := newPackTestStore(t, func(o *PackOptions) { o.PackSize = 1 << 20 })
	d2 := append([]byte(nil), d...)
	d2 = append(d2, "без словаря"...)
	res2, err := st2.Put(ctx, d2)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := st2.Get(ctx, res2.Hash); err != nil || string(got) != string(d2) {
		t.Fatalf("Get без словаря: %v", err)
	}
	raw2, err := os.ReadFile(filepath.Join(st2.opts.Dir, packFileName(1)))
	if err != nil {
		t.Fatal(err)
	}
	hdr2, err := parsePackHeader(raw2)
	if err != nil {
		t.Fatal(err)
	}
	if hdr2.dictID != 0 {
		t.Errorf("заголовок пака без словаря: dictID = %d, want 0", hdr2.dictID)
	}
}

// TestPackConcurrent — concurrent Puts (the write-position/rotation mutex)
// and parallel Gets: unique and identical contents, the reference counters
// converge. The -race run is in the manual check (make test does not include
// it).
func TestPackConcurrent(t *testing.T) {
	st, pool := newPackTestStore(t, func(o *PackOptions) { o.PackSize = 8 << 10 })
	ctx := context.Background()
	const workers = 16
	same := []byte("общий контент всех горутин")

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := st.Put(ctx, same); err != nil {
				t.Errorf("Put общего: %v", err)
				return
			}
			uniq := []byte(strings.Repeat(fmt.Sprintf("уникальный контент горутины %02d;", i), 50))
			if _, err := st.Put(ctx, uniq); err != nil {
				t.Errorf("Put уникального %d: %v", i, err)
			}
			if _, err := st.Get(ctx, HashOf(same)); err != nil {
				t.Errorf("Get общего из горутины %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	if row, ok := pool.row(HashOf(same)); !ok || row.refc != workers {
		t.Errorf("refcount общего контента = %v, want %d", row.refc, workers)
	}
	for i := 0; i < workers; i++ {
		uniq := []byte(strings.Repeat(fmt.Sprintf("уникальный контент горутины %02d;", i), 50))
		got, err := st.Get(ctx, HashOf(uniq))
		if err != nil || string(got) != string(uniq) {
			t.Fatalf("Get уникального %d: %v", i, err)
		}
	}
}

// TestPackClosed — writing after Close is forbidden, reading works
// (packs are opened for reading independently).
func TestPackClosed(t *testing.T) {
	st, _ := newPackTestStore(t, nil)
	ctx := context.Background()
	res, err := st.Put(ctx, []byte("до закрытия"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Put(ctx, []byte("после закрытия")); !errors.Is(err, errPackClosed) {
		t.Errorf("Put после Close: %v, want errPackClosed", err)
	}
	if got, err := st.Get(ctx, res.Hash); err != nil || string(got) != "до закрытия" {
		t.Errorf("Get после Close: %v", err)
	}
}

// TestPackStaleCacheSyncsToNewest — the FS mtime granularity: a pack created
// by another process within the same mtime unit must not go unnoticed — the
// registry cache must re-check, the append goes to the newest pack (the
// T-042/T-045 invariant).
func TestPackStaleCacheSyncsToNewest(t *testing.T) {
	st, pool := newPackTestStore(t, func(o *PackOptions) { o.PackSize = 1 << 20 })
	ctx := context.Background()
	if _, err := st.Put(ctx, []byte("контент до появления нового пака")); err != nil {
		t.Fatal(err)
	}
	// The second Put re-reads the directory (the pack creation changed the
	// mtime) — the cache records maxID 1.
	if _, err := st.Put(ctx, []byte("контент для прогрева кэша")); err != nil {
		t.Fatal(err)
	}
	if !st.dirCache.valid || st.dirCache.maxID != 1 {
		t.Fatalf("кэш реестра после Put: %+v", st.dirCache)
	}

	// "Another process" created pack 2 within the mtime granularity:
	// the directory restores its previous time — the cache looks fresh.
	mt := st.dirCache.modTime
	compactCraftPack(t, st.opts.Dir, 2, nil)
	if err := os.Chtimes(st.opts.Dir, mt, mt); err != nil {
		t.Fatal(err)
	}

	res, err := st.Put(ctx, []byte("контент отстающего писателя"))
	if err != nil {
		t.Fatal(err)
	}
	if row, _ := pool.row(res.Hash); row.packID != 2 {
		t.Errorf("допись при протухшем кэше ушла в пак %d, want 2 (новейший)", row.packID)
	}
	if !st.dirCache.valid || st.dirCache.maxID != 2 {
		t.Errorf("кэш реестра не пересобран: %+v", st.dirCache)
	}
}

// TestPackCreatePackClosesOnSize — incompatible contents when creating a
// pack: an error, and the open file does not leak (the fd is closed —
// repeated rotation over a stale cache must not eat descriptors up to
// EMFILE).
func TestPackCreatePackClosesOnSize(t *testing.T) {
	st, _ := newPackTestStore(t, func(o *PackOptions) { o.PackSize = 1 << 20 })
	// Pack 2 already exists with contents beyond the header —
	// createPackLocked must refuse and close the open file.
	compactCraftPack(t, st.opts.Dir, 2, [][]byte{bytes.Repeat([]byte("x"), 32)})

	openPacks := func() int {
		t.Helper()
		ents, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Skipf("/proc/self/fd недоступен: %v", err)
		}
		n := 0
		for _, e := range ents {
			if link, err := os.Readlink("/proc/self/fd/" + e.Name()); err == nil &&
				strings.Contains(link, packFileName(2)) {
				n++
			}
		}
		return n
	}
	if before := openPacks(); before != 0 {
		t.Fatalf("пак 2 уже открыт до вызова: %d fd", before)
	}
	for i := 0; i < 3; i++ {
		if _, err := st.createPackLocked(2); err == nil {
			t.Fatal("createPackLocked поверх непустого пака: ожидалась ошибка")
		}
		if n := openPacks(); n != 0 {
			t.Fatalf("после ошибки createPackLocked открытых fd пака 2: %d, want 0 (утечка)", n)
		}
	}
}

// TestPackOpenValidation — constructor parameters.
func TestPackOpenValidation(t *testing.T) {
	pool := newPackFakePool()
	if _, err := OpenPackStore(nil, PackOptions{Dir: t.TempDir()}); err == nil {
		t.Error("nil пул: ожидалась ошибка")
	}
	if _, err := OpenPackStore(pool, PackOptions{}); err == nil {
		t.Error("пустой каталог: ожидалась ошибка")
	}
	dir := t.TempDir()
	if _, err := OpenPackStore(pool, PackOptions{Dir: dir, PackSize: -1}); err == nil {
		t.Error("отрицательный pack_size: ожидалась ошибка")
	}
	if _, err := OpenPackStore(pool, PackOptions{Dir: dir, MaxStoreBytes: -1}); err == nil {
		t.Error("отрицательный max_store_bytes: ожидалась ошибка")
	}
	// A zero pack_size means DefaultPackSize; the directory and the lock are
	// created.
	st, err := OpenPackStore(pool, PackOptions{Dir: dir})
	if err != nil {
		t.Fatalf("нули в опциях: %v", err)
	}
	defer st.Close() //nolint:errcheck // the test closes the store in cleanup
	if st.opts.PackSize != DefaultPackSize {
		t.Errorf("pack_size по умолчанию = %d, want %d", st.opts.PackSize, DefaultPackSize)
	}
	if _, err := os.Stat(filepath.Join(dir, packLockFile)); err != nil {
		t.Errorf("файл блокировки не создан: %v", err)
	}
}

// TestPackReadOnly — a reader without writes (crawlerctl domain --content):
// opening creates neither a directory nor a lock file, a missing directory is
// an error; Put/Delete/beginAppend refuse, Get reads the existing
// content, Close works without the lock.
func TestPackReadOnly(t *testing.T) {
	pool := newPackFakePool()
	ctx := context.Background()

	// No directory — an error, and it is not created (a typo in content.dir
	// must not silently look like an empty store).
	missing := filepath.Join(t.TempDir(), "missing")
	if _, err := OpenPackStore(pool, PackOptions{Dir: missing, ReadOnly: true}); err == nil {
		t.Fatal("ReadOnly на отсутствующем каталоге: ожидалась ошибка")
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("ReadOnly не должен создавать каталог: %v", err)
	}

	// An empty existing directory — the reader's open and close leave no
	// traces (neither a lock nor packs).
	clean := t.TempDir()
	roClean, err := OpenPackStore(pool, PackOptions{Dir: clean, ReadOnly: true})
	if err != nil {
		t.Fatalf("OpenPackStore ReadOnly на пустом каталоге: %v", err)
	}
	if err := roClean.Close(); err != nil {
		t.Errorf("Close читателем: %v", err)
	}
	if _, err := os.Stat(filepath.Join(clean, packLockFile)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("читатель не создаёт файл блокировки: %v", err)
	}

	// Filling via a regular store (the lock and the pack in place) — reading
	// by the reader without writes.
	dir := t.TempDir()
	wr, err := OpenPackStore(pool, PackOptions{Dir: dir, PackSize: 1 << 20, MaxStoreBytes: 1 << 20})
	if err != nil {
		t.Fatalf("OpenPackStore: %v", err)
	}
	data := []byte("<html>контент для читателя</html>")
	res, err := wr.Put(ctx, data)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := wr.Close(); err != nil {
		t.Fatalf("Close писателя: %v", err)
	}

	ro, err := OpenPackStore(pool, PackOptions{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("OpenPackStore ReadOnly: %v", err)
	}
	got, err := ro.Get(ctx, res.Hash)
	if err != nil {
		t.Fatalf("Get читателем: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("Get читателем = %q, want исходный контент", got)
	}
	if _, err := ro.Put(ctx, []byte("x")); !errors.Is(err, errReadOnly) {
		t.Errorf("Put читателем: err = %v, want errReadOnly", err)
	}
	if err := ro.Delete(ctx, res.Hash); !errors.Is(err, errReadOnly) {
		t.Errorf("Delete читателем: err = %v, want errReadOnly", err)
	}
	if _, err := ro.beginAppend(); !errors.Is(err, errReadOnly) {
		t.Errorf("beginAppend читателем: err = %v, want errReadOnly", err)
	}
	if err := ro.Close(); err != nil { // Close without the lock does not fail
		t.Errorf("Close читателем: %v", err)
	}
}

// TestParsePackHeaderHdrLen — a header of a future version (hdrLen larger
// than the v1 fields) is read: v1 takes the first 23 bytes and does not
// reject the extension (documented forward compatibility); a shortened
// hdrLen is corruption.
func TestParsePackHeaderHdrLen(t *testing.T) {
	future := packHeader{id: 7, maxSize: 1 << 20, dictID: 0xABCD}
	b := future.bytes()
	// hdrLen = 40: the v1 fields are read, the extension is ignored.
	binary.LittleEndian.PutUint16(b[5:7], 40)
	hdr, err := parsePackHeader(b)
	if err != nil {
		t.Fatalf("заголовок будущей версии отвергнут: %v", err)
	}
	if hdr.id != 7 || hdr.maxSize != 1<<20 || hdr.dictID != 0xABCD {
		t.Errorf("поля v1 искажены: %+v", hdr)
	}
	// An hdrLen smaller than the mandatory fields — unreadable.
	binary.LittleEndian.PutUint16(b[5:7], packHeaderLen-4)
	if _, err := parsePackHeader(b); err == nil {
		t.Error("укороченный hdrLen принят")
	}
}

// TestPackIDFromNameCanonical — the pack registry accepts only the canonical
// names pack-NNNNNN.pack (C-16): a name without padding is a foreign
// file, not a second file of the same pack_id; long ids (≥ 7 digits)
// are canonical — packFileName does not truncate digits.
func TestPackIDFromNameCanonical(t *testing.T) {
	for _, ok := range []struct {
		name string
		id   int32
	}{
		{"pack-000001.pack", 1},
		{"pack-123456.pack", 123456},
		{"pack-1000000.pack", 1000000}, // 7 digits: %06d does not truncate
	} {
		if id, valid := packIDFromName(ok.name); !valid || id != ok.id {
			t.Errorf("packIDFromName(%q) = %d, %v; want %d, true", ok.name, id, valid, ok.id)
		}
	}
	for _, bad := range []string{
		"pack-1.pack",       // no padding
		"pack-0001.pack",    // short padding
		"pack-0000001.pack", // long padding
		"pack-000001.pack.bak",
		"dict.zst",
		".packs.lock",
	} {
		if _, valid := packIDFromName(bad); valid {
			t.Errorf("packIDFromName(%q) принял чужое имя", bad)
		}
	}
}

// TestPackReadBlockBounds — readPackBlock checks the placement against the
// file before the allocation (C-14): a corrupted row (a len of up to 2 GiB or
// negative, an offset past the file) is an error, not a giant buffer and not
// a panic.
func TestPackReadBlockBounds(t *testing.T) {
	dir := t.TempDir()
	compactCraftPack(t, dir, 1, [][]byte{bytes.Repeat([]byte("block"), 8)})
	const size = packHeaderLen + 4 + 40

	for _, tc := range []struct {
		name string
		off  int64
		clen int32
	}{
		{"len из повреждённой строки", packHeaderLen + 4, 1 << 30},
		{"отрицательный len", packHeaderLen + 4, -1},
		{"смещение мимо файла", size, 4},
		{"блок длиннее файла", packHeaderLen + 4, 41},
	} {
		if _, err := readPackBlock(dir, 1, tc.off, tc.clen); err == nil {
			t.Errorf("%s: readPackBlock принял (%d, %d)", tc.name, tc.off, tc.clen)
		}
	}
	// A live placement is read as before.
	block, err := readPackBlock(dir, 1, packHeaderLen+4, 40)
	if err != nil || len(block) != 40 {
		t.Errorf("чтение целого блока: %v, длина %d", err, len(block))
	}
}

// TestPackWriteErrENOSPC — out of disk space is ErrStoreFull
// (spec 8); the original error stays available to errors.Is.
func TestPackWriteErrENOSPC(t *testing.T) {
	diskFull := &os.PathError{Op: "write", Path: "pack-000001.pack", Err: syscall.ENOSPC}
	err := wrapPackWriteErr(diskFull, 1)
	if !errors.Is(err, ErrStoreFull) {
		t.Errorf("ENOSPC не отображён в ErrStoreFull: %v", err)
	}
	if !errors.Is(err, syscall.ENOSPC) {
		t.Errorf("исходный ENOSPC потерян: %v", err)
	}
	other := &os.PathError{Op: "write", Path: "pack-000001.pack", Err: syscall.EIO}
	if errors.Is(wrapPackWriteErr(other, 1), ErrStoreFull) {
		t.Error("прочая ошибка записи попала в ErrStoreFull")
	}
}

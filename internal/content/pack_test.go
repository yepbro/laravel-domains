// Юнит-тесты PackStore (T-042): контракт ContentStore на реальных
// пак-файлах во временном каталоге и метаданных contents на фейковом
// pgx-пуле (стиль ingest-тестов: пул с точными SQL и живой семантикой
// refcount). Поведение против живой PostgreSQL — сквозные тесты
// хранилища (T-046).
package content

import (
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

// --- фейковый пул: семантика contents без БД -------------------------------

// packCall — записанный SQL-вызов с аргументами.
type packCall struct {
	sql  string
	args []any
}

// packFakeRow — строка contents.
type packFakeRow struct {
	packID int32
	off    int64
	clen   int32
	rawLen int32
	refc   int32
}

// packFakePool — db.DB с настоящей семантикой четырёх SQL метаданных
// (карта hash → строка, refcount по правилам хранилища): StoreContract
// и тесты ротации требуют живое состояние, а не заготовленные ответы.
// Неизвестный SQL — ошибка: пул закрепляет точные тексты запросов.
type packFakePool struct {
	mu       sync.Mutex
	contents map[[HashLen]byte]*packFakeRow
	calls    []packCall

	execSeq    int   // счётчик Exec для failOnExec
	failOnExec int   // уронить Exec с этим порядковым номером (1-based)
	execErr    error // чем уронить (по умолчанию общая ошибка БД)
	queryErr   error
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
	case sqlMetaPut:
		if r := p.contents[h]; r != nil {
			r.refc++ // ON CONFLICT: ссылка конкурента/мёртвой строки
			r.packID, r.off = args[1].(int32), args[2].(int64)
			r.clen, r.rawLen = args[3].(int32), args[4].(int32)
			return pgconn.NewCommandTag("UPDATE 1"), nil
		}
		p.contents[h] = &packFakeRow{
			packID: args[1].(int32), off: args[2].(int64),
			clen: args[3].(int32), rawLen: args[4].(int32), refc: 1,
		}
		return pgconn.NewCommandTag("INSERT 0 1"), nil
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
	if p.queryErr != nil {
		return nil, p.queryErr
	}
	if sql != sqlMetaGet {
		return nil, fmt.Errorf("packFakePool: неожидаемый SQL: %s", sql)
	}
	var h [HashLen]byte
	copy(h[:], args[0].([]byte))
	if r := p.contents[h]; r != nil && r.refc > 0 {
		return &packRows{rows: [][]any{{r.packID, r.off, r.clen, r.rawLen}}}, nil
	}
	return &packRows{}, nil // pgx: ноль строк → ErrNoRows у вызывающего
}

// row — снимок строки contents для проверок (refcount, смещения).
func (p *packFakePool) row(h Hash) (packFakeRow, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	r, ok := p.contents[h]
	if !ok {
		return packFakeRow{}, false
	}
	return packFakeRow(*r), true
}

func (p *packFakePool) snapshotCalls() []packCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]packCall(nil), p.calls...)
}

// packRows — pgx.Rows с заготовленными строками; Scan позиционный.
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

// --- общий конструктор хранилища для тестов --------------------------------

// newPackTestStore — PackStore во временном каталоге с фейковым пулом.
// Маленький PackSize по умолчанию: часть тестов полагается на ротацию,
// контракт тоже её простреливает.
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

// TestStoreContractOnPack — контракт ContentStore (T-041) на реализации
// пак-файлов: главная проверка задачи.
func TestStoreContractOnPack(t *testing.T) {
	StoreContract(t, func(t *testing.T) ContentStore {
		st, _ := newPackTestStore(t, nil)
		return st
	})
}

// --- ожидаемые SQL (контракт против схемы 000001) ---------------------------

const (
	wantSQLAddRef = "UPDATE contents SET refcount = refcount + 1 " +
		"WHERE hash = $1 AND refcount > 0"
	wantSQLPut = "INSERT INTO contents (hash, pack_id, off, len, raw_len, refcount) " +
		"VALUES ($1, $2, $3, $4, $5, 1) " +
		"ON CONFLICT (hash) DO UPDATE SET refcount = contents.refcount + 1, " +
		"pack_id = EXCLUDED.pack_id, off = EXCLUDED.off, " +
		"len = EXCLUDED.len, raw_len = EXCLUDED.raw_len"
	wantSQLGet = "SELECT pack_id, off, len, raw_len FROM contents " +
		"WHERE hash = $1 AND refcount > 0"
	wantSQLDelRef = "UPDATE contents SET refcount = refcount - 1 " +
		"WHERE hash = $1 AND refcount > 0"
)

// TestPackSQLContract — SQL метаданных и параметры: точные тексты,
// порядок вызовов Put/Get/Delete, типы аргументов. Схема contents —
// миграция 000001 (hash, pack_id, off, len, raw_len, refcount).
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

// --- формат пака --------------------------------------------------------------

// TestPackHeaderAndOffsets — формат файла: заголовок v1 (magic, версия,
// длина, id, порог ротации, ID словаря) и записи [len u32 LE][zstd-блок];
// метаданные off/len указывают на сжатые данные блока.
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

	// Каждый блок: смещение из метаданных, перед блоком — префикс длины.
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

// TestPackRotation — ротация по размеру: несколько маленьких паков, все
// контенты читаются, pack_id монотонны и без дыр, размер пака не выходит
// за порог (кроме первого блока — исключение продвигает ротацию).
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

	// Все контенты читаются после ротации.
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

// TestPackDedupNoBytes — повторный Put того же контента: только ссылка
// (UPDATE), байты в пак не пишутся, Created = false.
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

// TestPackCrashWindow — краш между спулом блока и метаданными: блок в
// паке есть, но невидим (Get — ErrNotFound); повторная запись работает.
func TestPackCrashWindow(t *testing.T) {
	st, pool := newPackTestStore(t, func(o *PackOptions) { o.PackSize = 1 << 20 })
	ctx := context.Background()
	d := []byte("контент, чьи метаданные не закоммитятся")
	pool.failOnExec = 2 // первый Exec (addRef) проходит, второй (метаданные) падает

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

	// Повторная запись того же контента исправляет ситуацию.
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

// TestPackDeadRowInvisible — refcount = 0: строка остаётся («мёртвая»),
// Get её не видит, Delete отказывает; новый Put пишет свежие байты
// (Created = true, новое смещение), а не воскрешает старый блок.
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
	// Конфликт с мёртвой строкой tag-ом неотличим от живой гонки —
	// Created = false (зафиксировано в pack.go), но блок пишется свежий
	// и размещение переводится на него.
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

// TestPackTooLarge — лимит max_store_bytes: контент отвергается целиком
// (ErrTooLarge), ни байта в пак, ни строки метаданных; пустой контент —
// допустим (контракт T-041).
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

// TestPackReopen — повторное открытие каталога: новейший пак подхватывается,
// контент прошлого запуска читается, запись продолжает тот же пак.
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
	defer st2.Close() //nolint:errcheck // тест закрывает при очистке
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

// TestPackDictOnOff — словарь T-043: пак, созданный со словарём, помнит
// его ID в заголовке; без словаря — dictID 0. Читаемость записей их
// словарём — ограничение T-043 (см. docs/configuration.md).
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

	// Словарь выключен: новый каталог, roundtrip без словаря, dictID 0.
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

// TestPackConcurrent — конкурентные Put (мьютекс позиции записи/ротации)
// и параллельные Get: уникальные и одинаковые контенты, счётчики ссылок
// сходятся. Прогон с -race — в ручной проверке (make test не включает).
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

// TestPackClosed — запись после Close запрещена, чтение работает
// (паки открываются на чтение независимо).
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

// TestPackOpenValidation — параметры конструктора.
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
	// Нулевой pack_size — DefaultPackSize, каталог и замок создаются.
	st, err := OpenPackStore(pool, PackOptions{Dir: dir})
	if err != nil {
		t.Fatalf("нули в опциях: %v", err)
	}
	defer st.Close() //nolint:errcheck // тест закрывает при очистке
	if st.opts.PackSize != DefaultPackSize {
		t.Errorf("pack_size по умолчанию = %d, want %d", st.opts.PackSize, DefaultPackSize)
	}
	if _, err := os.Stat(filepath.Join(dir, packLockFile)); err != nil {
		t.Errorf("файл блокировки не создан: %v", err)
	}
}

// TestPackWriteErrENOSPC — кончившееся место на диске — ErrStoreFull
// (ТЗ 8); исходная ошибка остаётся доступной errors.Is.
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

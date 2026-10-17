// Юнит-тесты компактификации паков (T-045; ТЗ 5.7, 13.2) на фейковом
// pgx-пуле с транзакциями (семантика contents + перенос размещений) и
// реальных пак-файлах в t.TempDir — стиль pack_test.go. Поведение против
// живой БД — сквозные тесты хранилища (T-046).
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

// --- фейковый пул с транзакциями компактификации ----------------------------

// compactFakePool — packFakePool (семантика Put/Get/Delete) плюс запросы
// и транзакции компактификации: скан пака, контроль живых строк, перенос
// размещений и удаление мёртвых строк. Отказы по шагам — краш-окна.
type compactFakePool struct {
	*packFakePool

	beginErr  error
	commitErr error
	txExecErr error
	begins    int
	commits   int
}

// compile-time: фейковый пул — CompactDB для тестов без БД.
var _ CompactDB = (*compactFakePool)(nil)

// Query — SQL компактификации обслуживается по карте contents,
// остальное (Put/Get/Delete) — во встроенный packFakePool.
func (p *compactFakePool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	switch sql {
	case sqlCompactRows, sqlCompactLive:
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
		if sql == sqlCompactLive {
			var n int64
			for _, r := range p.contents {
				if r.packID == packID && r.refc > 0 {
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

// compactFakeMove — отложенное изменение размещения строки.
type compactFakeMove struct {
	h      [HashLen]byte
	packID int32
	off    int64
}

// compactFakeTx — транзакция фейкового пака: изменения копятся и
// применяются только в Commit (откат честно отменяет их).
type compactFakeTx struct {
	pgx.Tx // остальные методы pgx.Tx проход не использует

	pool  *compactFakePool
	moves []compactFakeMove
	deads [][HashLen]byte
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
			if r := t.pool.contents[h]; r != nil && r.packID == from && r.refc == 0 {
				t.deads = append(t.deads, h)
				n++
			}
		}
		return pgconn.NewCommandTag(fmt.Sprintf("DELETE %d", n)), nil
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

// --- общий конструктор и утилиты --------------------------------------------

// newCompactTestStore — PackStore во временном каталоге с фейковым пулом,
// поддерживающим транзакции компактификации.
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

// compactCraftPack — ручная сборка пака: заголовок v1 и записи
// [len u32 LE][блок]; возвращает смещения блоков. Для точного управления
// байтами (порог доли живых) и создания новейшего пака без записи в него.
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

// compactCraftRow — строка contents поверх вручную собранного пака.
func compactCraftRow(t *testing.T, pool *compactFakePool, key string, packID int32, off int64, clen int, refc int32) Hash {
	t.Helper()
	h := HashOf([]byte(key))
	pool.contents[h] = &packFakeRow{packID: packID, off: off, clen: int32(clen), rawLen: 0, refc: refc}
	return h
}

// compactPackSize — размер файла пака (0 — файла нет).
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

// compactWaitFor — опрос условия до дедлайна (цикл работает в горутине).
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

// --- SQL-контракт ------------------------------------------------------------

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
)

// TestCompactSQLContract — точные тексты SQL против схемы contents
// (миграция 000001: hash, pack_id, off, len, raw_len, refcount).
func TestCompactSQLContract(t *testing.T) {
	if sqlCompactRows != wantCompactRows || sqlCompactMove != wantCompactMove ||
		sqlCompactDead != wantCompactDead || sqlCompactLive != wantCompactLive {
		t.Fatal("SQL компактификации изменился — обновите контракт и schema_test")
	}
}

// --- скан --------------------------------------------------------------------

// TestCompactScanMixed — скан различает живые, мёртвые и мусорные блоки:
// живая и мёртвая строки, блок краш-окна Put (без строки) и недописанный
// хвост.
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
	// Третий Put: addRef (Exec 6) проходит, дописывание проходит,
	// фиксация метаданных (Exec 7) падает — блок без строки.
	pool.failOnExec = 7
	if _, err := st.Put(ctx, orphan); err == nil {
		t.Fatal("Put с упавшей фиксацией метаданных должен вернуть ошибку")
	}
	// Недописанный хвост: префикс длины 255 и один байт данных.
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
	// Мусор: блок краш-окна (4+len) и хвост 5 байт — один целый блок.
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

// TestCompactScanValidation — строка, указывающая мимо целой записи
// файла, — повреждение метаданных: скан отказывается, компактификация
// такого пака не запускается.
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

// --- порог -------------------------------------------------------------------

// TestCompactThreshold49 — доля живых ниже порога: пак переписан и удалён,
// строка переехала в новейший пак, мёртвая строка удалена.
func TestCompactThreshold49(t *testing.T) {
	dir := t.TempDir()
	pool := &compactFakePool{packFakePool: newPackFakePool()}
	liveBlock := bytes.Repeat([]byte("L"), 49)
	deadBlock := bytes.Repeat([]byte("D"), 51)
	offs := compactCraftPack(t, dir, 1, [][]byte{liveBlock, deadBlock})
	compactCraftPack(t, dir, 2, nil) // новейший, только заголовок
	hLive := compactCraftRow(t, pool, "live", 1, offs[0], len(liveBlock), 2)
	hDead := compactCraftRow(t, pool, "dead", 1, offs[1], len(deadBlock), 0)
	oldSize := compactPackSize(t, dir, 1)

	st, err := OpenPackStore(pool, PackOptions{Dir: dir, PackSize: 1 << 20, MaxStoreBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close() //nolint:errcheck // тест закрывает при очистке

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

// TestCompactThreshold51 — доля живых не ниже порога: проход ничего
// не трогает.
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
	defer st.Close() //nolint:errcheck // тест закрывает при очистке

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

// --- переписывание ------------------------------------------------------------

// TestCompactPackRewrites — правка реального пака: байты и смещения в
// метаданных верны, Get работает, мёртвые строки удалены, запись после
// компактификации продолжает идти в новейший пак.
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
	compactCraftPack(t, st.opts.Dir, 2, nil) // новейший пак
	for _, h := range hashes[2:] {           // три из пяти — мёртвые
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

	// Запись после компактификации — в новейший пак, поверх переписанного.
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

// --- краш-окна ----------------------------------------------------------------

// TestCompactCrashBeforeCommit — сбой транзакции (после переписывания
// блоков): строки не тронуты, старый пак цел, чтение работает,
// скопированные блоки — мусор в новейшем паке; повторный проход
// завершает компактификацию.
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
			if err := st.Delete(ctx, hashes[0]); err != nil { // 3 живых из 4
				t.Fatal(err)
			}
			sizeBefore := compactPackSize(t, st.opts.Dir, 2)

			fail(pool)
			if _, err := st.CompactPack(ctx, 1); err == nil {
				t.Fatal("компактификация с упавшей транзакцией должна вернуть ошибку")
			}

			// Состояние не изменилось: старый пак цел, строки на месте.
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

			// Повторный проход завершает компактификацию.
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

// TestCompactCrashAfterCommitBeforeDelete — состояние «транзакции
// зафиксированы, файл ещё на месте» (крах между шагами): чтение идёт по
// новым размещениям, следующий проход удаляет файл-мусор.
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
	// Возвращаем файл на место — эквивалент краха после COMMIT до unlink.
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

// --- конкурентность -----------------------------------------------------------

// TestCompactConcurrentWriter — конкурентный писатель в новейший пак не
// мешает компактификации: блоки перемешиваются в новом паке, все
// контенты читаются (прогон с -race — в ручной проверке).
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
	compactCraftPack(t, st.opts.Dir, 2, nil) // новейший пак
	for _, h := range hashes[:8] {           // 8 из 10 мёртвые
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
			// Отправка с выходом по отмене: главный тест читает канал
			// только после остановки писателя.
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
			// Отмена в полёте — не потеря: воркер повторит (ТЗ 3.4).
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
	// Старые контенты: мёртвые невидимы, живые читаются по новым местам.
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

// TestCompactWriterSyncsToNewest — «отстающий» писатель всегда
// синхронизируется с новейшим паком (инвариант T-042/T-045): запись
// после появления нового пака идёт в него, а после удаления старого пака
// — в новейший, без потерь.
func TestCompactWriterSyncsToNewest(t *testing.T) {
	dir := t.TempDir()
	pool := &compactFakePool{packFakePool: newPackFakePool()}
	a, err := OpenPackStore(pool, PackOptions{Dir: dir, PackSize: 1 << 20, MaxStoreBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close() //nolint:errcheck // тест закрывает при очистке
	b, err := OpenPackStore(pool, PackOptions{Dir: dir, PackSize: 1 << 20, MaxStoreBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close() //nolint:errcheck // тест закрывает при очистке
	ctx := context.Background()

	// A пишет первым: пак 1; контент тут же умирает (компактификация
	// заберёт весь пак).
	x := []byte("<html>первый контент писателя A</html>")
	if _, err := a.Put(ctx, x); err != nil {
		t.Fatal(err)
	}
	if err := a.Delete(ctx, HashOf(x)); err != nil {
		t.Fatal(err)
	}
	// Появляется новейший пак 2 (другой «процесс» ротировал).
	compactCraftPack(t, dir, 2, nil)

	// Отстающий A обязан переключиться на пак 2.
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

	// Компактификация пака 1 (доля живых 0) и запись после неё.
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

// --- проход и цикл ------------------------------------------------------------

// TestCompactNewestNotCompacted — новейший пак (единственный) не
// компактифицируется: прямой отказ и пустой проход.
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

// TestCompactOnceErrors — ошибка скана одного пака собирается в итог
// прохода, остальные паки обрабатываются.
func TestCompactOnceErrors(t *testing.T) {
	dir := t.TempDir()
	pool := &compactFakePool{packFakePool: newPackFakePool()}
	// Пак 1: только мёртвая строка — компактифицируется (доля живых 0).
	// Пак 2: живая строка с битым len — скан откажет. Пак 3 — новейший.
	offs1 := compactCraftPack(t, dir, 1, [][]byte{bytes.Repeat([]byte("x"), 40)})
	offs2 := compactCraftPack(t, dir, 2, [][]byte{bytes.Repeat([]byte("y"), 40)})
	compactCraftPack(t, dir, 3, nil)
	hDead := compactCraftRow(t, pool, "dead", 1, offs1[0], 40, 0)
	hLive := compactCraftRow(t, pool, "live", 2, offs2[0], 40+7, 1) // len не сходится

	st, err := OpenPackStore(pool, PackOptions{Dir: dir, PackSize: 1 << 20, MaxStoreBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close() //nolint:errcheck // тест закрывает при очистке

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

// TestCompactLoop — цикл: первый проход срабатывает сразу, ошибка
// прохода не останавливает (Warn и retry тиком), отмена ctx — чистый nil.
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
	// Первый проход падает (сбой БД скана); через 30 мс сбой снимается —
	// тик цикла повторяет и преуспевает: ошибка прохода не останавливает.
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

// TestCompactOptionsDefaults — нули и нелепости заменяются дефолтами.
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

// TestCompactEmptyDir — пустой каталог: проход без работы и без ошибок.
func TestCompactEmptyDir(t *testing.T) {
	st, _ := newCompactTestStore(t, nil)
	stats, err := CompactOnce(context.Background(), st, CompactOptions{})
	if err != nil || stats != (CompactStats{}) {
		t.Errorf("CompactOnce пустого каталога: %+v, %v", stats, err)
	}
}

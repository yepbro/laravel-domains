// Юнит-тесты свопа ссылок contents при перезаписи результата (T-044,
// ТЗ 5.7, 8): фейковый pgx-пул с транзакциями (стиль pack_test.go и
// ingest/cleanup_test.go) — точные SQL-тексты и параметры, порядок
// операторов, единственная транзакция на своп, сценарии нулевых и равных
// хэшей, живая семантика refcount с откатом, ошибки целостности, сбои
// шагов и отмена ctx. Поведение против живой PostgreSQL — сквозные
// тесты хранилища (T-046).
package content

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// --- фейковый пул: семантика refcount с транзакциями ------------------------

// refcountCall — записанный SQL-вызов с аргументами.
type refcountCall struct {
	sql  string
	args []any
}

// refcountFakePool — RefDB без БД: живая семантика refcount таблицы
// contents (карта hash → refcount; 0 или отсутствие строки — мёртвая,
// UPDATE с охраной её не трогает). Незакоммиченные изменения живут
// в overlay своей транзакции: Commit применяет их к пулу, Rollback
// отбрасывает — тесты проверяют атомарность отката. Exec/Query вне
// транзакции — ошибка: операторы свопа обязаны идти одной транзакцией.
type refcountFakePool struct {
	mu   sync.Mutex
	refc map[[HashLen]byte]int32

	begins    int
	txs       []*refcountFakeTx
	beginErr  error
	poolExecs int // Exec мимо транзакции (должно быть 0)

	execErr     error // чем уронить оператор transOnStep
	transOnStep int   // номер оператора в транзакции (1-based; 0 — не ронять)
	commitErr   error
}

func newRefcountFakePool(seed map[Hash]int32) *refcountFakePool {
	p := &refcountFakePool{refc: make(map[[HashLen]byte]int32)}
	for h, n := range seed {
		p.refc[h] = n
	}
	return p
}

func (p *refcountFakePool) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

func (p *refcountFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.poolExecs++
	return pgconn.CommandTag{}, errors.New("refcountFakePool: Exec вне транзакции")
}

func (p *refcountFakePool) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("refcountFakePool: Query не должен вызываться")
}

func (p *refcountFakePool) Begin(context.Context) (pgx.Tx, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.begins++
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	tx := &refcountFakeTx{pool: p, overlay: make(map[[HashLen]byte]int32)}
	p.txs = append(p.txs, tx)
	return tx, nil
}

// count — снимок refcount хэша под замком.
func (p *refcountFakePool) count(h Hash) int32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.refc[h]
}

// refcountFakeTx — транзакция над пулом: операторы читают базу через
// overlay (незакоммиченное видит только своя транзакция), Commit сливает
// изменения в пул, Rollback отбрасывает.
type refcountFakeTx struct {
	pgx.Tx // остальные методы pgx.Tx своп не использует

	pool       *refcountFakePool
	overlay    map[[HashLen]byte]int32
	calls      []refcountCall
	committed  bool
	rolledBack bool
}

// visible — refcount хэша глазами транзакции (pool.mu уже удержан).
func (t *refcountFakeTx) visible(h [HashLen]byte) int32 {
	if v, ok := t.overlay[h]; ok {
		return v
	}
	return t.pool.refc[h]
}

func (t *refcountFakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := ctx.Err(); err != nil {
		return pgconn.CommandTag{}, err
	}
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	t.calls = append(t.calls, refcountCall{sql: sql, args: args})
	if t.pool.execErr != nil && t.pool.transOnStep == len(t.calls) {
		return pgconn.CommandTag{}, t.pool.execErr
	}
	var h [HashLen]byte
	copy(h[:], args[0].([]byte))
	cur := t.visible(h)
	switch sql {
	case sqlMetaAddRef:
		if cur <= 0 {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		t.overlay[h] = cur + 1
		return pgconn.NewCommandTag("UPDATE 1"), nil
	case sqlMetaDelRef:
		if cur <= 0 {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		t.overlay[h] = cur - 1
		return pgconn.NewCommandTag("UPDATE 1"), nil
	default:
		return pgconn.CommandTag{}, fmt.Errorf("refcountFakePool: неожидаемый SQL: %s", sql)
	}
}

func (t *refcountFakeTx) Commit(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if t.pool.commitErr != nil {
		return t.pool.commitErr
	}
	for h, v := range t.overlay {
		t.pool.refc[h] = v
	}
	t.committed = true
	return nil
}

func (t *refcountFakeTx) Rollback(context.Context) error {
	t.pool.mu.Lock()
	defer t.pool.mu.Unlock()
	if !t.committed {
		t.rolledBack = true
	}
	return nil
}

// compile-time: фейковый пул — RefDB для тестов без БД.
var _ RefDB = (*refcountFakePool)(nil)

// refcountHash — различимые тестовые хэши: refcountHash(1), (2), …
func refcountHash(n byte) Hash {
	var h Hash
	h[0] = n
	return h
}

// --- своп: порядок, параметры, транзакция -----------------------------------

func TestSwapRefOrderAndTransaction(t *testing.T) {
	oldH, newH := refcountHash(1), refcountHash(2)
	pool := newRefcountFakePool(map[Hash]int32{oldH: 1, newH: 1})

	if err := SwapRef(context.Background(), pool, oldH, newH); err != nil {
		t.Fatalf("SwapRef: %v", err)
	}
	if pool.begins != 1 {
		t.Fatalf("транзакций открыто %d, ожидалась 1 — своп атомарен", pool.begins)
	}
	if pool.poolExecs != 0 {
		t.Fatalf("Exec вне транзакции: %d — операторы обязаны идти через транзакцию", pool.poolExecs)
	}
	tx := pool.txs[0]
	want := []refcountCall{
		{sql: wantSQLAddRef, args: []any{newH[:]}}, // сначала +1 новому…
		{sql: wantSQLDelRef, args: []any{oldH[:]}}, // …потом −1 старому
	}
	if !reflect.DeepEqual(tx.calls, want) {
		t.Fatalf("операторы свопа:\nполучено  %v\nожидалось %v", tx.calls, want)
	}
	if !tx.committed || tx.rolledBack {
		t.Fatalf("транзакция: committed=%v rolledBack=%v, ожидался чистый commit", tx.committed, tx.rolledBack)
	}
	if got := pool.count(oldH); got != 0 {
		t.Fatalf("refcount старого = %d, ожидался 0 (строка мёртвая, остаётся — контракт T-042)", got)
	}
	if got := pool.count(newH); got != 2 {
		t.Fatalf("refcount нового = %d, ожидался 2", got)
	}
}

func TestSwapRefScenarios(t *testing.T) {
	zero := Hash{}
	a, b := refcountHash(1), refcountHash(2)
	cases := []struct {
		name       string
		oldH, newH Hash
		seed       map[Hash]int32
		wantOld    int32 // refcount старого после свопа (ненулевого)
		wantNew    int32 // refcount нового после свопа (ненулевого)
		wantCalls  int   // операторов в транзакции
		wantBegins int   // открытых транзакций
	}{
		{"контент не сменился (old == new)", b, b, map[Hash]int32{b: 3}, 3, 3, 0, 0},
		{"обе ссылки нулевые", zero, zero, nil, 0, 0, 0, 0},
		{"первый контент (старый нулевой)", zero, b, map[Hash]int32{b: 2}, 0, 3, 1, 1},
		{"очистка результата (новый нулевой)", a, zero, map[Hash]int32{a: 2}, 1, 0, 1, 1},
		{"очистка до мёртвой строки (refcount 0)", a, zero, map[Hash]int32{a: 1}, 0, 0, 1, 1},
		{"своп со сменой контента", a, b, map[Hash]int32{a: 5, b: 7}, 4, 8, 2, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := newRefcountFakePool(tc.seed)
			if err := SwapRef(context.Background(), pool, tc.oldH, tc.newH); err != nil {
				t.Fatalf("SwapRef: %v", err)
			}
			if pool.begins != tc.wantBegins {
				t.Fatalf("транзакций %d, ожидалось %d", pool.begins, tc.wantBegins)
			}
			calls := 0
			if len(pool.txs) == 1 {
				calls = len(pool.txs[0].calls)
			}
			if calls != tc.wantCalls {
				t.Fatalf("операторов %d, ожидалось %d", calls, tc.wantCalls)
			}
			if tc.oldH != zero && pool.count(tc.oldH) != tc.wantOld {
				t.Fatalf("refcount старого = %d, ожидался %d", pool.count(tc.oldH), tc.wantOld)
			}
			if tc.newH != zero && pool.count(tc.newH) != tc.wantNew {
				t.Fatalf("refcount нового = %d, ожидался %d", pool.count(tc.newH), tc.wantNew)
			}
		})
	}
}

func TestSwapRefIntegrity(t *testing.T) {
	a, b := refcountHash(1), refcountHash(2)

	t.Run("новый контент не записан Put-ом", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 1})
		err := SwapRef(context.Background(), pool, a, b)
		if !errors.Is(err, ErrRefIntegrity) {
			t.Fatalf("ошибка %v, ожидалась ErrRefIntegrity", err)
		}
		if got := pool.count(a); got != 1 {
			t.Fatalf("refcount старого = %d, ожидался 1 — транзакция откатилась целиком", got)
		}
		if !pool.txs[0].rolledBack {
			t.Fatal("транзакция не откачена")
		}
	})
	t.Run("новый мёртв (refcount = 0)", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 1, b: 0})
		if err := SwapRef(context.Background(), pool, a, b); !errors.Is(err, ErrRefIntegrity) {
			t.Fatalf("ошибка %v, ожидалась ErrRefIntegrity", err)
		}
		if !pool.txs[0].rolledBack {
			t.Fatal("транзакция не откачена")
		}
	})
	t.Run("у старого нет живой ссылки", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{b: 1})
		err := SwapRef(context.Background(), pool, a, b)
		if !errors.Is(err, ErrRefIntegrity) {
			t.Fatalf("ошибка %v, ожидалась ErrRefIntegrity", err)
		}
		// Атомарность отката: успевший +1 новому не применился.
		if got := pool.count(b); got != 1 {
			t.Fatalf("refcount нового = %d, ожидался 1 — добавление откачено вместе со снятием", got)
		}
		if !pool.txs[0].rolledBack {
			t.Fatal("транзакция не откачена")
		}
	})
	t.Run("старый мёртв (refcount = 0)", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 0, b: 1})
		if err := SwapRef(context.Background(), pool, a, b); !errors.Is(err, ErrRefIntegrity) {
			t.Fatalf("ошибка %v, ожидалась ErrRefIntegrity", err)
		}
		if got := pool.count(b); got != 1 {
			t.Fatalf("refcount нового = %d, ожидался 1", got)
		}
	})
	t.Run("повторный своп той же пары — двойное снятие отвергнуто", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 1, b: 1})
		if err := SwapRef(context.Background(), pool, a, b); err != nil {
			t.Fatalf("первый своп: %v", err)
		}
		// Повтор идемпотентной записи результата даёт old == new (no-op);
		// прямой повторный своп — двойное снятие старого: у A ссылка
		// уже снята, охрана refcount > 0 отказывает, минуса нет.
		err := SwapRef(context.Background(), pool, a, b)
		if !errors.Is(err, ErrRefIntegrity) {
			t.Fatalf("ошибка %v, ожидалась ErrRefIntegrity", err)
		}
		if got := pool.count(b); got != 2 {
			t.Fatalf("refcount нового = %d, ожидался 2 — второй +1 откачен", got)
		}
	})
}

func TestSwapRefFailures(t *testing.T) {
	a, b := refcountHash(1), refcountHash(2)
	boom := errors.New("сбой БД")

	t.Run("пул не задан", func(t *testing.T) {
		if err := SwapRef(context.Background(), nil, a, b); err == nil {
			t.Fatal("ожидалась ошибка")
		}
	})
	t.Run("отмена ctx до свопа", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		pool := newRefcountFakePool(map[Hash]int32{a: 1, b: 1})
		if err := SwapRef(ctx, pool, a, b); !errors.Is(err, context.Canceled) {
			t.Fatalf("ошибка %v, ожидалась отмена контекста", err)
		}
		if pool.begins != 0 {
			t.Fatalf("транзакций %d, ожидалось 0 — операция несостоявшаяся", pool.begins)
		}
	})
	t.Run("ошибка Begin", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 1, b: 1})
		pool.beginErr = boom
		if err := SwapRef(context.Background(), pool, a, b); !errors.Is(err, boom) {
			t.Fatalf("ошибка %v, ожидалась оборачивающая boom", err)
		}
	})
	t.Run("ошибка оператора +1 новому", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 1, b: 1})
		pool.execErr, pool.transOnStep = boom, 1
		if err := SwapRef(context.Background(), pool, a, b); !errors.Is(err, boom) {
			t.Fatalf("ошибка %v, ожидалась оборачивающая boom", err)
		}
		if !pool.txs[0].rolledBack {
			t.Fatal("транзакция не откачена")
		}
		if got := pool.count(a); got != 1 {
			t.Fatalf("refcount старого = %d, ожидался 1", got)
		}
	})
	t.Run("ошибка оператора −1 старому", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 1, b: 1})
		pool.execErr, pool.transOnStep = boom, 2
		if err := SwapRef(context.Background(), pool, a, b); !errors.Is(err, boom) {
			t.Fatalf("ошибка %v, ожидалась оборачивающая boom", err)
		}
		if !pool.txs[0].rolledBack {
			t.Fatal("транзакция не откачена")
		}
		// Успевший +1 новому откачен вместе с транзакцией.
		if got := pool.count(b); got != 1 {
			t.Fatalf("refcount нового = %d, ожидался 1 — добавление откачено", got)
		}
		if got := pool.count(a); got != 1 {
			t.Fatalf("refcount старого = %d, ожидался 1", got)
		}
	})
	t.Run("ошибка Commit", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 1, b: 1})
		pool.commitErr = boom
		if err := SwapRef(context.Background(), pool, a, b); !errors.Is(err, boom) {
			t.Fatalf("ошибка %v, ожидалась оборачивающая boom", err)
		}
		// Незафиксированное не применилось: счётчики не тронуты.
		if got := pool.count(a); got != 1 {
			t.Fatalf("refcount старого = %d, ожидался 1", got)
		}
		if got := pool.count(b); got != 1 {
			t.Fatalf("refcount нового = %d, ожидался 1", got)
		}
	})
}

// --- примитив для транзакции вызывающего ------------------------------------

func TestSwapRefTx(t *testing.T) {
	a, b := refcountHash(1), refcountHash(2)

	t.Run("в транзакции вызывающего", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 1, b: 1})
		tx, err := pool.Begin(context.Background())
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		if err := SwapRefTx(context.Background(), tx, a, b); err != nil {
			t.Fatalf("SwapRefTx: %v", err)
		}
		if err := tx.Commit(context.Background()); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		// SwapRefTx не открывает собственных транзакций.
		if pool.begins != 1 {
			t.Fatalf("транзакций %d, ожидалась 1 (только вызывающего)", pool.begins)
		}
		if got := pool.count(a); got != 0 {
			t.Fatalf("refcount старого = %d, ожидался 0", got)
		}
		if got := pool.count(b); got != 2 {
			t.Fatalf("refcount нового = %d, ожидался 2", got)
		}
	})
	t.Run("no-op без операторов", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{b: 3})
		tx, _ := pool.Begin(context.Background())
		ftx := tx.(*refcountFakeTx)
		if err := SwapRefTx(context.Background(), ftx, b, b); err != nil {
			t.Fatalf("SwapRefTx: %v", err)
		}
		if len(ftx.calls) != 0 {
			t.Fatalf("операторов %d, ожидалось 0", len(ftx.calls))
		}
	})
	t.Run("транзакция не задана", func(t *testing.T) {
		if err := SwapRefTx(context.Background(), nil, a, b); err == nil {
			t.Fatal("ожидалась ошибка")
		}
	})
	t.Run("отмена ctx", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 1, b: 1})
		tx, _ := pool.Begin(context.Background())
		ftx := tx.(*refcountFakeTx)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := SwapRefTx(ctx, tx, a, b); !errors.Is(err, context.Canceled) {
			t.Fatalf("ошибка %v, ожидалась отмена контекста", err)
		}
		if len(ftx.calls) != 0 {
			t.Fatalf("операторов %d, ожидалось 0 — операция несостоявшаяся", len(ftx.calls))
		}
	})
}

// --- метод PackStore ---------------------------------------------------------

func TestPackStoreSwapRef(t *testing.T) {
	a, b := refcountHash(1), refcountHash(2)
	dir := t.TempDir()

	t.Run("через пул хранилища", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 1, b: 1})
		st, err := OpenPackStore(pool, PackOptions{Dir: dir})
		if err != nil {
			t.Fatalf("OpenPackStore: %v", err)
		}
		defer func() { _ = st.Close() }()

		// no-op через метод: транзакция не открывается.
		if err := st.SwapRef(context.Background(), b, b); err != nil {
			t.Fatalf("no-op своп: %v", err)
		}
		if pool.begins != 0 {
			t.Fatalf("транзакций %d, ожидалось 0", pool.begins)
		}
		if err := st.SwapRef(context.Background(), a, b); err != nil {
			t.Fatalf("своп: %v", err)
		}
		if got := pool.count(a); got != 0 {
			t.Fatalf("refcount старого = %d, ожидался 0", got)
		}
		if got := pool.count(b); got != 2 {
			t.Fatalf("refcount нового = %d, ожидался 2", got)
		}
	})
	t.Run("пул без Begin отвергается", func(t *testing.T) {
		// packFakePool (pack_test.go) — только db.DB, без Begin.
		st, err := OpenPackStore(newPackFakePool(), PackOptions{Dir: dir})
		if err != nil {
			t.Fatalf("OpenPackStore: %v", err)
		}
		defer func() { _ = st.Close() }()

		err = st.SwapRef(context.Background(), a, b)
		if err == nil || !strings.Contains(err.Error(), "RefDB") {
			t.Fatalf("ошибка %v, ожидался отказ с указанием RefDB", err)
		}
	})
}

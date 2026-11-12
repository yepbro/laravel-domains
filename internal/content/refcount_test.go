// Unit tests of the contents reference swap on result rewrite (T-044,
// spec 5.7, 8): a fake pgx pool with transactions (the pack_test.go and
// ingest/cleanup_test.go style) — exact SQL texts and parameters, statement
// order, a single transaction per swap, zero and equal hash scenarios,
// live refcount semantics with rollback, integrity errors, step
// failures, and ctx cancellation. Behavior against live PostgreSQL is covered by
// end-to-end store tests (T-046).
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

// --- fake pool: refcount semantics with transactions ------------------------

// refcountCall — a recorded SQL call with arguments.
type refcountCall struct {
	sql  string
	args []any
}

// refcountFakePool — a RefDB without a DB: live refcount semantics of the
// contents table (a map hash → refcount; 0 or a missing row means dead,
// a guarded UPDATE does not touch it). Uncommitted changes live
// in their transaction's overlay: Commit applies them to the pool, Rollback
// discards them — the tests check rollback atomicity. Exec/Query outside
// a transaction is an error: the swap statements must run in one transaction.
type refcountFakePool struct {
	mu   sync.Mutex
	refc map[[HashLen]byte]int32

	begins    int
	txs       []*refcountFakeTx
	beginErr  error
	poolExecs int // Exec outside a transaction (must be 0)

	execErr     error // what to fail the transOnStep statement with
	transOnStep int   // the statement number in the transaction (1-based; 0 — do not fail)
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
	return 0, errors.New("CopyFrom must not be called")
}

func (p *refcountFakePool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.poolExecs++
	return pgconn.CommandTag{}, errors.New("refcountFakePool: Exec outside a transaction")
}

func (p *refcountFakePool) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("refcountFakePool: Query must not be called")
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

// count — a snapshot of a hash's refcount under the lock.
func (p *refcountFakePool) count(h Hash) int32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.refc[h]
}

// refcountFakeTx — a transaction over the pool: statements read the base through
// the overlay (uncommitted state is visible only to their own transaction), Commit
// merges the changes into the pool, Rollback discards them.
type refcountFakeTx struct {
	pgx.Tx // the swap does not use the other pgx.Tx methods

	pool       *refcountFakePool
	overlay    map[[HashLen]byte]int32
	calls      []refcountCall
	committed  bool
	rolledBack bool
}

// visible — a hash's refcount as seen by the transaction (pool.mu is already held).
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
		return pgconn.CommandTag{}, fmt.Errorf("refcountFakePool: unexpected SQL: %s", sql)
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

// compile-time: the fake pool is a RefDB for tests without a DB.
var _ RefDB = (*refcountFakePool)(nil)

// refcountHash — distinguishable test hashes: refcountHash(1), (2), …
func refcountHash(n byte) Hash {
	var h Hash
	h[0] = n
	return h
}

// --- swap: order, parameters, transaction -----------------------------------

func TestSwapRefOrderAndTransaction(t *testing.T) {
	oldH, newH := refcountHash(1), refcountHash(2)
	pool := newRefcountFakePool(map[Hash]int32{oldH: 1, newH: 1})

	if err := SwapRef(context.Background(), pool, oldH, newH); err != nil {
		t.Fatalf("SwapRef: %v", err)
	}
	if pool.begins != 1 {
		t.Fatalf("%d transactions opened, expected 1 — the swap is atomic", pool.begins)
	}
	if pool.poolExecs != 0 {
		t.Fatalf("Exec outside a transaction: %d — the statements must go through a transaction", pool.poolExecs)
	}
	tx := pool.txs[0]
	want := []refcountCall{
		{sql: wantSQLAddRef, args: []any{newH[:]}}, // first +1 to the new one…
		{sql: wantSQLDelRef, args: []any{oldH[:]}}, // …then −1 to the old one
	}
	if !reflect.DeepEqual(tx.calls, want) {
		t.Fatalf("swap statements:\ngot      %v\nexpected %v", tx.calls, want)
	}
	if !tx.committed || tx.rolledBack {
		t.Fatalf("transaction: committed=%v rolledBack=%v, want a clean commit", tx.committed, tx.rolledBack)
	}
	if got := pool.count(oldH); got != 0 {
		t.Fatalf("the old one's refcount = %d, expected 0 (the row is dead and stays — contract T-042)", got)
	}
	if got := pool.count(newH); got != 2 {
		t.Fatalf("the new one's refcount = %d, expected 2", got)
	}
}

func TestSwapRefScenarios(t *testing.T) {
	zero := Hash{}
	a, b := refcountHash(1), refcountHash(2)
	cases := []struct {
		name       string
		oldH, newH Hash
		seed       map[Hash]int32
		wantOld    int32 // the old one's refcount after the swap (when non-zero)
		wantNew    int32 // the new one's refcount after the swap (when non-zero)
		wantCalls  int   // statements in the transaction
		wantBegins int   // transactions opened
	}{
		{"the content did not change (old == new)", b, b, map[Hash]int32{b: 3}, 3, 3, 0, 0},
		{"both references are zero", zero, zero, nil, 0, 0, 0, 0},
		{"the first content (a zero old one)", zero, b, map[Hash]int32{b: 2}, 0, 3, 1, 1},
		{"result cleanup (a zero new one)", a, zero, map[Hash]int32{a: 2}, 1, 0, 1, 1},
		{"the cleanup of a dead row (refcount 0)", a, zero, map[Hash]int32{a: 1}, 0, 0, 1, 1},
		{"a swap with a content change", a, b, map[Hash]int32{a: 5, b: 7}, 4, 8, 2, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := newRefcountFakePool(tc.seed)
			if err := SwapRef(context.Background(), pool, tc.oldH, tc.newH); err != nil {
				t.Fatalf("SwapRef: %v", err)
			}
			if pool.begins != tc.wantBegins {
				t.Fatalf("%d transactions, expected %d", pool.begins, tc.wantBegins)
			}
			calls := 0
			if len(pool.txs) == 1 {
				calls = len(pool.txs[0].calls)
			}
			if calls != tc.wantCalls {
				t.Fatalf("%d statements, expected %d", calls, tc.wantCalls)
			}
			if tc.oldH != zero && pool.count(tc.oldH) != tc.wantOld {
				t.Fatalf("the old one's refcount = %d, expected %d", pool.count(tc.oldH), tc.wantOld)
			}
			if tc.newH != zero && pool.count(tc.newH) != tc.wantNew {
				t.Fatalf("the new one's refcount = %d, expected %d", pool.count(tc.newH), tc.wantNew)
			}
		})
	}
}

func TestSwapRefIntegrity(t *testing.T) {
	a, b := refcountHash(1), refcountHash(2)

	t.Run("the new content was not written by Put", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 1})
		err := SwapRef(context.Background(), pool, a, b)
		if !errors.Is(err, ErrRefIntegrity) {
			t.Fatalf("error %v, expected ErrRefIntegrity", err)
		}
		if got := pool.count(a); got != 1 {
			t.Fatalf("the old one's refcount = %d, expected 1 — the transaction rolled back entirely", got)
		}
		if !pool.txs[0].rolledBack {
			t.Fatal("the transaction not rolled back")
		}
	})
	t.Run("the new one dead (refcount = 0)", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 1, b: 0})
		if err := SwapRef(context.Background(), pool, a, b); !errors.Is(err, ErrRefIntegrity) {
			t.Fatalf("error %v, expected ErrRefIntegrity", err)
		}
		if !pool.txs[0].rolledBack {
			t.Fatal("the transaction not rolled back")
		}
	})
	t.Run("the old one has no live reference", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{b: 1})
		err := SwapRef(context.Background(), pool, a, b)
		if !errors.Is(err, ErrRefIntegrity) {
			t.Fatalf("error %v, expected ErrRefIntegrity", err)
		}
		// Rollback atomicity: the +1 to the new one that did happen was not applied.
		if got := pool.count(b); got != 1 {
			t.Fatalf("the new one's refcount = %d, expected 1 — the addition rolled back along with the removal", got)
		}
		if !pool.txs[0].rolledBack {
			t.Fatal("the transaction not rolled back")
		}
	})
	t.Run("the old one dead (refcount = 0)", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 0, b: 1})
		if err := SwapRef(context.Background(), pool, a, b); !errors.Is(err, ErrRefIntegrity) {
			t.Fatalf("error %v, expected ErrRefIntegrity", err)
		}
		if got := pool.count(b); got != 1 {
			t.Fatalf("the new one's refcount = %d, expected 1", got)
		}
	})
	t.Run("a repeated swap of the same pair — a double removal rejected", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 1, b: 1})
		if err := SwapRef(context.Background(), pool, a, b); err != nil {
			t.Fatalf("the first swap: %v", err)
		}
		// A repeat of the idempotent result write yields old == new (a no-op);
		// a direct repeated swap is a double removal of the old one: A's reference
		// is already removed, the refcount > 0 guard refuses, there is no minus.
		err := SwapRef(context.Background(), pool, a, b)
		if !errors.Is(err, ErrRefIntegrity) {
			t.Fatalf("error %v, expected ErrRefIntegrity", err)
		}
		if got := pool.count(b); got != 2 {
			t.Fatalf("the new one's refcount = %d, expected 2 — the second +1 rolled back", got)
		}
	})
}

func TestSwapRefFailures(t *testing.T) {
	a, b := refcountHash(1), refcountHash(2)
	boom := errors.New("DB failure")

	t.Run("the pool is not set", func(t *testing.T) {
		if err := SwapRef(context.Background(), nil, a, b); err == nil {
			t.Fatal("an error expected")
		}
	})
	t.Run("ctx cancel before the swap", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		pool := newRefcountFakePool(map[Hash]int32{a: 1, b: 1})
		if err := SwapRef(ctx, pool, a, b); !errors.Is(err, context.Canceled) {
			t.Fatalf("error %v, expected a context cancellation", err)
		}
		if pool.begins != 0 {
			t.Fatalf("%d transactions, expected 0 — the operation did not happen", pool.begins)
		}
	})
	t.Run("Begin error", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 1, b: 1})
		pool.beginErr = boom
		if err := SwapRef(context.Background(), pool, a, b); !errors.Is(err, boom) {
			t.Fatalf("error %v, expected the wrapping boom", err)
		}
	})
	t.Run("the +1-to-new statement error", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 1, b: 1})
		pool.execErr, pool.transOnStep = boom, 1
		if err := SwapRef(context.Background(), pool, a, b); !errors.Is(err, boom) {
			t.Fatalf("error %v, expected the wrapping boom", err)
		}
		if !pool.txs[0].rolledBack {
			t.Fatal("the transaction not rolled back")
		}
		if got := pool.count(a); got != 1 {
			t.Fatalf("the old one's refcount = %d, expected 1", got)
		}
	})
	t.Run("the −1-from-old statement error", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 1, b: 1})
		pool.execErr, pool.transOnStep = boom, 2
		if err := SwapRef(context.Background(), pool, a, b); !errors.Is(err, boom) {
			t.Fatalf("error %v, expected the wrapping boom", err)
		}
		if !pool.txs[0].rolledBack {
			t.Fatal("the transaction not rolled back")
		}
		// The +1 to the new one that did happen is rolled back with the transaction.
		if got := pool.count(b); got != 1 {
			t.Fatalf("the new one's refcount = %d, expected 1 — the addition rolled back", got)
		}
		if got := pool.count(a); got != 1 {
			t.Fatalf("the old one's refcount = %d, expected 1", got)
		}
	})
	t.Run("Commit error", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 1, b: 1})
		pool.commitErr = boom
		if err := SwapRef(context.Background(), pool, a, b); !errors.Is(err, boom) {
			t.Fatalf("error %v, expected the wrapping boom", err)
		}
		// The uncommitted state was not applied: the counters are untouched.
		if got := pool.count(a); got != 1 {
			t.Fatalf("the old one's refcount = %d, expected 1", got)
		}
		if got := pool.count(b); got != 1 {
			t.Fatalf("the new one's refcount = %d, expected 1", got)
		}
	})
}

// --- the primitive for the caller's transaction ------------------------------

func TestSwapRefTx(t *testing.T) {
	a, b := refcountHash(1), refcountHash(2)

	t.Run("in the caller's transaction", func(t *testing.T) {
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
		// SwapRefTx does not open transactions of its own.
		if pool.begins != 1 {
			t.Fatalf("%d transactions, expected 1 (the caller's only)", pool.begins)
		}
		if got := pool.count(a); got != 0 {
			t.Fatalf("the old one's refcount = %d, expected 0", got)
		}
		if got := pool.count(b); got != 2 {
			t.Fatalf("the new one's refcount = %d, expected 2", got)
		}
	})
	t.Run("a no-op without statements", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{b: 3})
		tx, _ := pool.Begin(context.Background())
		ftx := tx.(*refcountFakeTx)
		if err := SwapRefTx(context.Background(), ftx, b, b); err != nil {
			t.Fatalf("SwapRefTx: %v", err)
		}
		if len(ftx.calls) != 0 {
			t.Fatalf("%d statements, expected 0", len(ftx.calls))
		}
	})
	t.Run("the transaction is not set", func(t *testing.T) {
		if err := SwapRefTx(context.Background(), nil, a, b); err == nil {
			t.Fatal("an error expected")
		}
	})
	t.Run("ctx cancel", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 1, b: 1})
		tx, _ := pool.Begin(context.Background())
		ftx := tx.(*refcountFakeTx)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := SwapRefTx(ctx, tx, a, b); !errors.Is(err, context.Canceled) {
			t.Fatalf("error %v, expected a context cancellation", err)
		}
		if len(ftx.calls) != 0 {
			t.Fatalf("%d statements, expected 0 — the operation did not happen", len(ftx.calls))
		}
	})
}

// --- the PackStore method ----------------------------------------------------

func TestPackStoreSwapRef(t *testing.T) {
	a, b := refcountHash(1), refcountHash(2)
	dir := t.TempDir()

	t.Run("via the store pool", func(t *testing.T) {
		pool := newRefcountFakePool(map[Hash]int32{a: 1, b: 1})
		st, err := OpenPackStore(pool, PackOptions{Dir: dir})
		if err != nil {
			t.Fatalf("OpenPackStore: %v", err)
		}
		defer func() { _ = st.Close() }()

		// A no-op through the method: no transaction is opened.
		if err := st.SwapRef(context.Background(), b, b); err != nil {
			t.Fatalf("the no-op swap: %v", err)
		}
		if pool.begins != 0 {
			t.Fatalf("%d transactions, expected 0", pool.begins)
		}
		if err := st.SwapRef(context.Background(), a, b); err != nil {
			t.Fatalf("the swap: %v", err)
		}
		if got := pool.count(a); got != 0 {
			t.Fatalf("the old one's refcount = %d, expected 0", got)
		}
		if got := pool.count(b); got != 2 {
			t.Fatalf("the new one's refcount = %d, expected 2", got)
		}
	})
	t.Run("a pool without Begin is rejected", func(t *testing.T) {
		// packFakePool (pack_test.go) is only a db.DB, without Begin.
		st, err := OpenPackStore(newPackFakePool(), PackOptions{Dir: dir})
		if err != nil {
			t.Fatalf("OpenPackStore: %v", err)
		}
		defer func() { _ = st.Close() }()

		err = st.SwapRef(context.Background(), a, b)
		if err == nil || !strings.Contains(err.Error(), "RefDB") {
			t.Fatalf("error %v, expected a rejection naming RefDB", err)
		}
	})
}

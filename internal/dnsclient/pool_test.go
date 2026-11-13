// Tests of the UDP socket pool. The sockets are real, on the 127.0.0.1
// loopback (they pile up in the receiver buffer, no external stack); no
// network exchange: only issue/return/parallelism are checked.

package dnsclient

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// loopUDPAddr opens a free UDP address on the loopback: the pool's
// connected sockets need a valid peer so that sends do not generate
// ICMP and errors — the pool is tested without exchanges.
func loopUDPAddr(t *testing.T) string {
	t.Helper()
	c, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c.LocalAddr().String()
}

// assertConnClosed checks that the socket is closed (reading returns the
// "use of closed connection" error, not a timeout).
func assertConnClosed(t *testing.T, conn *net.UDPConn) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := conn.Read(make([]byte, 16)); err == nil {
		t.Fatal("want a read error on a closed socket, got an answer")
	}
}

func TestUDPPoolReuseSocket(t *testing.T) {
	addr := loopUDPAddr(t)
	p, err := NewUDPPool(context.Background(), addr, 1)
	if err != nil {
		t.Fatalf("NewUDPPool: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	// A one-socket pool: the returned socket must be issued again, no new
	// Dials appear (the free channel is FIFO; reuse is checked at size 1,
	// where the socket is the only one).
	c1, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	p.Put(c1)
	c2, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if c1 != c2 {
		t.Fatal("the returned socket must be reused, got a new one")
	}
	p.Put(c2)
}

func TestUDPPoolGetWaitsForFreeSocket(t *testing.T) {
	addr := loopUDPAddr(t)
	p, err := NewUDPPool(context.Background(), addr, 1)
	if err != nil {
		t.Fatalf("NewUDPPool: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	c1, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	// The pool is exhausted: a waiting Get fits into its own wait timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if c, err := p.Get(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v, %v", c, err)
	}

	// A release wakes the waiter.
	got := make(chan *net.UDPConn, 1)
	errCh := make(chan error, 1)
	go func() {
		c, err := p.Get(context.Background())
		if err != nil {
			errCh <- err
			return
		}
		got <- c
	}()
	// Give the waiter time to enter the select, then return the socket.
	time.Sleep(30 * time.Millisecond)
	p.Put(c1)
	select {
	case c := <-got:
		if c != c1 {
			t.Fatal("the waiter must get the released socket")
		}
		p.Put(c)
	case err := <-errCh:
		t.Fatalf("get after the release: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("Get did not wait for the socket release")
	}
}

func TestUDPPoolGetCancelledWhileWaiting(t *testing.T) {
	addr := loopUDPAddr(t)
	p, err := NewUDPPool(context.Background(), addr, 1)
	if err != nil {
		t.Fatalf("NewUDPPool: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	c1, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer p.Put(c1)

	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan error, 1)
	go func() {
		_, err := p.Get(ctx)
		res <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-res:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Get did not react to the context cancellation")
	}
}

// TestUDPPoolParallelInvariants — concurrent work (the -race run catches
// races): a socket is never issued to two owners at once, and the total
// number of sockets never exceeds the pool size.
func TestUDPPoolParallelInvariants(t *testing.T) {
	addr := loopUDPAddr(t)
	const size = 4
	p, err := NewUDPPool(context.Background(), addr, size)
	if err != nil {
		t.Fatalf("NewUDPPool: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	var (
		mu      sync.Mutex
		inUse   = make(map[*net.UDPConn]bool)
		seenAll = make(map[*net.UDPConn]bool)
	)
	const workers = 16
	const iters = 40
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for i := 0; i < iters; i++ {
				conn, err := p.Get(ctx)
				if err != nil {
					t.Errorf("get: %v", err)
					return
				}
				mu.Lock()
				if inUse[conn] {
					mu.Unlock()
					t.Errorf("the socket %v was handed out again while its owner is alive", conn)
					p.Put(conn)
					return
				}
				inUse[conn] = true
				seenAll[conn] = true
				mu.Unlock()

				time.Sleep(time.Millisecond) // "work" with the socket

				mu.Lock()
				delete(inUse, conn)
				mu.Unlock()
				p.Put(conn)
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(inUse) != 0 {
		t.Fatalf("after all goroutines finish the sockets are still counted as in use: %d", len(inUse))
	}
	if len(seenAll) == 0 || len(seenAll) > size {
		t.Fatalf("the number of distinct pool sockets %d is outside 1..%d", len(seenAll), size)
	}
}

func TestUDPPoolDiscardReplacedLazily(t *testing.T) {
	addr := loopUDPAddr(t)
	p, err := NewUDPPool(context.Background(), addr, 2)
	if err != nil {
		t.Fatalf("NewUDPPool: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	// Count dials after the initial eager dial (DNS-08): Get does not open
	// a new socket while a free one is around.
	var dials atomic.Int32
	old := p.dial
	p.dial = func(ctx context.Context) (*net.UDPConn, error) {
		dials.Add(1)
		return old(ctx)
	}

	c1, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	p.Discard(c1)
	assertConnClosed(t, c1)

	// Room freed up, but a free eager socket sits in the channel — Get
	// hands it out instead of dialing a replacement.
	c2, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("get after discard: %v", err)
	}
	if c2 == c1 {
		t.Fatal("a retired socket is not handed out again")
	}
	if n := dials.Load(); n != 0 {
		t.Fatalf("dial sets %d next to a free socket, want 0", n)
	}

	// The channel is empty and there is room — now Get lazily dials a replacement.
	c3, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if c3 == c1 || c3 == c2 {
		t.Fatal("want a replacement (new) socket")
	}
	if n := dials.Load(); n != 1 {
		t.Fatalf("dial sets %d, want 1 (a lazy replacement)", n)
	}
	p.Put(c2)
	p.Put(c3)
}

func TestUDPPoolClose(t *testing.T) {
	addr := loopUDPAddr(t)
	p, err := NewUDPPool(context.Background(), addr, 2)
	if err != nil {
		t.Fatalf("NewUDPPool: %v", err)
	}

	// One socket is issued, the other is free.
	held, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	assertConnClosed(t, held) // the issued socket is closed on return…
	p.Put(held)               // …and does not go back into the closed pool
	if _, err := p.Get(context.Background()); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("get after close: want ErrPoolClosed, got %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("a second close must be a no-op: %v", err)
	}
}

// TestUDPPoolCloseWakesWaiters — Close wakes waiting Get calls (the done
// channel): without a deadline of its own ctx a waiter gets ErrPoolClosed,
// not hangs until the end of the process (DNS-09).
func TestUDPPoolCloseWakesWaiters(t *testing.T) {
	addr := loopUDPAddr(t)
	p, err := NewUDPPool(context.Background(), addr, 1)
	if err != nil {
		t.Fatalf("NewUDPPool: %v", err)
	}

	held, err := p.Get(context.Background()) // the only socket is taken
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	res := make(chan error, 1)
	go func() {
		_, err := p.Get(context.Background()) // a ctx with no deadline
		res <- err
	}()
	time.Sleep(30 * time.Millisecond) // the waiter has entered the select
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-res:
		if !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("a waiting Get after Close: %v, want ErrPoolClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wake the waiting Get")
	}
	p.Put(held)
}

func TestUDPPoolBadArguments(t *testing.T) {
	if _, err := NewUDPPool(context.Background(), "127.0.0.1:53", 0); err == nil {
		t.Fatal("size 0: want an error")
	}
	if _, err := NewUDPPool(context.Background(), "nohostport", 1); err == nil {
		t.Fatal("a malformed address: want an error")
	}
	// A port out of range: SplitHostPort will parse it, DialContext will
	// not — the eager dial must fail, not hand out a dead pool.
	if _, err := NewUDPPool(context.Background(), "127.0.0.1:99999", 1); err == nil {
		t.Fatal("an unreachable port: want an eager dial set error")
	}
}

// TestUDPPoolDialFailureAtGet — a lazy dial failing after Discard does not
// break the counter: the next attempt dials the socket again.
func TestUDPPoolDialFailureAtGet(t *testing.T) {
	addr := loopUDPAddr(t)
	p, err := NewUDPPool(context.Background(), addr, 1)
	if err != nil {
		t.Fatalf("NewUDPPool: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	c1, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	p.Discard(c1)

	var attempts atomic.Int32
	old := p.dial
	p.dial = func(ctx context.Context) (*net.UDPConn, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("dial boom")
		}
		return old(ctx)
	}
	if _, err := p.Get(context.Background()); err == nil {
		t.Fatal("the first (failing) dial set: want an error")
	}
	// The room counter is freed: a repeated Get dials the socket again.
	c2, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("get after a failed dial set: %v", err)
	}
	p.Put(c2)
}

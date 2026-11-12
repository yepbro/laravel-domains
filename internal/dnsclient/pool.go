// A pool of connected UDP sockets to a single resolver (spec 5.4: "its own
// pool of UDP sockets").
//
// Why an own pool instead of dns.Client from miekg/dns: dns.Client Exchange
// opens a socket per request — at the planned concurrency of 2000 (spec
// 5.10 dns.concurrency) that is 2000 socket(2)+connect(2) operations per
// second per every thousand of QPS, plus the same flow of closes; sockets
// connected in advance (connect(2), no traffic — a UDP connect only fixes
// the peer address) remove both. Connectedness also gives kernel filtering
// of datagrams from foreign sources and an ECONNREFUSED error from an
// unreachable resolver (ICMP port unreachable) instead of silence.
//
// One socket — one request in flight: Exchange holds the socket from Get
// to Put, so the pool size also caps the number of concurrent UDP exchanges
// (waiters queue up in Get; this is also the natural boundary under the
// T-035 limiter). Waiters face no risk of stealing foreign responses: the
// next socket owner works with its own Id (see Client.attempt); late
// responses of previous exchanges are dropped.

package dnsclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
)

// ErrPoolClosed is returned by Get after Close (Put closes the socket
// silently: its exchange has finished, nobody and nothing waits for it).
var ErrPoolClosed = errors.New("dnsclient: UDP socket pool closed")

// UDPPool is a pool of connected UDP sockets to one address. Safe for
// concurrent use from goroutines.
type UDPPool struct {
	addr string
	size int
	free chan *net.UDPConn
	// done is closed in Close: waiting Get calls wake up with ErrPoolClosed,
	// without waiting for the deadline of their own ctx (DNS-09).
	done chan struct{}

	mu     sync.Mutex
	live   int // open sockets: free + issued + being dialed
	closed bool

	// dial opens one socket; replaced in tests (transport seam).
	dial func(ctx context.Context) (*net.UDPConn, error)
}

// NewUDPPool creates the pool and opens all size sockets in advance: a
// population error (bad address/port, no route) is a pool creation error
// at startup, not at the first request. connect does not check resolver
// availability (a UDP connect sends no packets): a dead server makes
// itself known in the first exchange — ECONNREFUSED from the connected
// socket (ICMP port unreachable) or a timeout; see NewClient about the
// startup probe (T-107). The context bounds the opening.
func NewUDPPool(ctx context.Context, addr string, size int) (*UDPPool, error) {
	if size < 1 {
		return nil, fmt.Errorf("dnsclient: UDP socket pool size %d: expected ≥ 1", size)
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return nil, fmt.Errorf("dnsclient: resolver address %q: expected host:port: %w", addr, err)
	}
	p := &UDPPool{
		addr: addr,
		size: size,
		free: make(chan *net.UDPConn, size),
		done: make(chan struct{}),
	}
	p.dial = p.dialUDP
	for i := 0; i < size; i++ {
		conn, err := p.dial(ctx)
		if err != nil {
			_ = p.Close()
			return nil, fmt.Errorf("dnsclient: opening UDP socket pool to %s (socket %d of %d): %w", addr, i+1, size, err)
		}
		p.free <- conn
	}
	p.mu.Lock()
	p.live = size
	p.mu.Unlock()
	return p, nil
}

// dialUDP connects one socket to the pool address.
func (p *UDPPool) dialUDP(ctx context.Context) (*net.UDPConn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "udp", p.addr)
	if err != nil {
		return nil, err
	}
	udp, ok := conn.(*net.UDPConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("dnsclient: %s: connection is not UDP (%T)", p.addr, conn)
	}
	return udp, nil
}

// Get returns a free socket; if none is available it opens a new one (up
// to the pool size) or waits for a release or ctx cancellation. The
// socket is given back via Put or retired via Discard; for the time of
// ownership a Get/Put pair per exchange is mandatory.
func (p *UDPPool) Get(ctx context.Context) (*net.UDPConn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	if p.live < p.size {
		// There is room (after a Discard or a failed dial), but in these states
		// the live counter need not reflect the actual number of live sockets
		// (the invariant live == free + issued is not restored) — take a free
		// socket from the channel first, and dial a new one only when the
		// channel is empty: otherwise Get would open an extra socket next to
		// an idle free one (DNS-08).
		select {
		case conn := <-p.free:
			p.mu.Unlock()
			return conn, nil
		default:
		}
		// Dialing a new socket: the counter reserves the slot in advance so
		// that concurrent Get calls do not overshoot the pool size.
		p.live++
		p.mu.Unlock()
		conn, err := p.dial(ctx)
		if err != nil {
			p.mu.Lock()
			p.live--
			p.mu.Unlock()
			return nil, err
		}
		return conn, nil
	}
	p.mu.Unlock()

	select {
	case conn := <-p.free:
		return conn, nil
	case <-p.done:
		return nil, ErrPoolClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Put returns the socket to the pool. The channel send happens under the
// mutex together with Close: otherwise, between the closed check and the
// send, Close could drain the channel and the socket would hang in it
// forever (a descriptor leak). A closed pool accepts the socket by closing
// it: its exchange has finished, nobody waits for it anymore.
func (p *UDPPool) Put(conn *net.UDPConn) {
	if conn == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		_ = conn.Close()
		return
	}
	select {
	case p.free <- conn:
	default:
		// Must not happen (no more than size sockets are issued) — a guard
		// against a descriptor leak.
		_ = conn.Close()
	}
}

// Discard retires a socket (closes it and removes it from the pool): for
// cases when the socket is known to be broken. The replacement is opened
// lazily by the nearest Get — a dial failure does not block the pool forever.
func (p *UDPPool) Discard(conn *net.UDPConn) {
	if conn == nil {
		return
	}
	_ = conn.Close()
	p.mu.Lock()
	p.live--
	p.mu.Unlock()
}

// Close closes free sockets and wakes waiting Get calls (done →
// ErrPoolClosed — without it a waiter was released only by the deadline of
// its own ctx, and with context.Background it would hang forever); issued
// sockets are closed on their return (Put into a closed pool closes the
// socket). A repeated Close is a no-op.
func (p *UDPPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()
	close(p.done) // wakes waiting Get calls — a closed pool has no sockets left
	for {
		select {
		case conn := <-p.free:
			_ = conn.Close()
			p.mu.Lock()
			p.live--
			p.mu.Unlock()
		default:
			return nil
		}
	}
}

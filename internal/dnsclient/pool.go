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
//
// The pool is fully populated at construction and sockets are never
// retired (T-300, the bulk review 04#P3-2: the Discard of "known broken"
// sockets had no production caller — the only reachable lazy-dial path was
// a failed dial of a replacement): the invariant is |free| ≤ size, Get
// waits for a Put or fails, no live counter is needed. A socket that
// misbehaves at the transport level is the client retry loop's business
// (and the chain's — FallbackResolver), not the pool's.

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
	free chan *net.UDPConn
	// done is closed in Close: waiting Get calls wake up with ErrPoolClosed,
	// without waiting for the deadline of their own ctx (DNS-09).
	done   chan struct{}
	closed bool
	mu     sync.Mutex
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
		free: make(chan *net.UDPConn, size),
		done: make(chan struct{}),
	}
	for i := 0; i < size; i++ {
		conn, err := p.dialUDP(ctx)
		if err != nil {
			_ = p.Close()
			return nil, fmt.Errorf("dnsclient: opening UDP socket pool to %s (socket %d of %d): %w", addr, i+1, size, err)
		}
		p.free <- conn
	}
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

// Get returns a free socket, waiting for a release or ctx cancellation
// (all size sockets are opened at construction and never retired — a
// waiter is released by a Put; Close wakes it with ErrPoolClosed). The
// socket is given back via Put; for the time of ownership a Get/Put pair
// per exchange is mandatory.
func (p *UDPPool) Get(ctx context.Context) (*net.UDPConn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
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
		default:
			return nil
		}
	}
}

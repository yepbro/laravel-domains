// Package dnsclient is the DNS client of the domain check stage (spec 5.4):
// exchanges with a local Unbound on top of github.com/miekg/dns, an own
// pool of UDP sockets, TCP fallback on truncated responses (TC=1,
// RFC 1035 §4.2.1), and retries on timeout/network error.
//
// The result contract matters to the consumer (T-036…T-039): an RCODE is
// not an error. NXDOMAIN, SERVFAIL, REFUSED and other codes are legal
// server responses returned in Result.RCode without an error; an error is
// only the network (timeout, disconnect, transport refusal) after the
// attempts are exhausted. Spec 5.4 separates the branches explicitly:
// NXDOMAIN → not_delegated (RFC 8020), SERVFAIL/timeout after retries → temp_error.
//
// Records are returned raw (the []dns.RR of the Answer/Ns sections):
// unwinding CNAME chains and picking A/AAAA/CNAME/NS is the consumer's
// job (T-037, spec 5.4 item 2); RecordsOf filters by type without interpretation.
//
// The T-035 extension point (concurrency/QPS, adaptive load shedding,
// AAAA refusal) is the Resolver interface: the limiter wraps Exchange
// with a decorator without changing the contract; the socket pool by
// itself bounds the number of in-flight UDP requests with UDPPoolSize.
package dnsclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"

	"crawler/internal/config"
)

// Default values. The timeouts are computed for a local Unbound
// (spec 5.4: recursive, 512–768 MB cache): hot answers take microseconds,
// cold ones bottom out at the upstream resolver; 2s per UDP attempt is
// enough with margin; the TCP fallback is rarer and costlier (connection setup) — 5s.
const (
	// DefaultTimeout is the timeout of one UDP attempt (the question plus
	// waiting for the answer, including waiting for a socket from the pool).
	DefaultTimeout = 2 * time.Second
	// DefaultTCPTimeout is the TCP fallback timeout: connection setup,
	// the exchange, reading the truncated response in full.
	DefaultTCPTimeout = 5 * time.Second
	// DefaultRetryDelay is the pause between attempts: it gives the resolver
	// time to work off its queue/recover instead of diagnosing the error at once.
	DefaultRetryDelay = 200 * time.Millisecond

	// DefaultEDNS0UDPSize is the advertised UDP buffer size in the OPT RR
	// (RFC 6891, EDNS0): 1232 bytes — the DNS flag day 2020 safe size (it fits
	// into the transit MTU of 1280 without fragmentation). Without an OPT
	// the resolver must fit the historical 512 bytes and truncates anything
	// larger to TC=1 — such a response would go to the costly TCP fallback in
	// every attempt (DNS-07).
	DefaultEDNS0UDPSize = 1232
)

// DefaultRetries is the retries beyond the first attempt (up to 3 exchanges total).
// The spec sets no number; 3 attempts match the system retry policy (queue
// delivery T-025, temp_error rechecks T-031). The per-request attempt cap
// and repeated check episodes (T-031) are different levels: here we absorb
// second-scale failures, there — persistent resolver problems.
const DefaultRetries = 2

// MaxRetries is the upper bound of retries: 10 attempts × 2s = 20s on a
// dead resolver; more than that is a job for the scheduler (T-031), not
// for a single request.
const MaxRetries = 10

// DefaultUDPPoolSize is the default UDP socket pool size. It equals
// dns.concurrency from spec 5.10 (2000): the pool must hold the planned
// concurrency, otherwise it becomes a limiter below the configured one
// (waiting Get calls eat into the attempt timeout). 2000 file descriptors
// per worker is a normal value for Linux (no_file in Docker ≥ 1M).
const DefaultUDPPoolSize = 2000

// Question is one DNS question (spec 5.4 items 1–3: A, AAAA, CNAME, NS).
type Question struct {
	// Name is the full domain name (apex or subdomain) in punycode.
	// Name normalization is the consumer's responsibility (internal/idn,
	// spec 5.2); here the name only gets the trailing dot appended.
	Name string
	// Type is the record type (dns.TypeA, dns.TypeAAAA, dns.TypeCNAME,
	// dns.TypeNS); one question — one type,
	// DNS does not use mixed queries.
	Type uint16
}

// validate checks the question before any network attempts: a packing
// error is deterministic, retrying it is pointless.
func (q Question) validate() error {
	if strings.TrimSpace(q.Name) == "" {
		return errors.New("dnsclient: имя вопроса пусто")
	}
	// The sign of a working type is an RR implementation in miekg/dns:
	// pseudo-types from TypeToString (None, Reserved) have no records, and
	// neither do service ones (AXFR/IXFR); this also guarantees the response parses.
	if dns.TypeToRR[q.Type] == nil {
		return fmt.Errorf("dnsclient: неизвестный тип записи %d (ожидаются dns.TypeA/AAAA/CNAME/NS/…)", q.Type)
	}
	return nil
}

// RCode is a server response code (RFC 1035 §4.1.1; extended codes are
// a number). Part of Result, not an error: see the package comment.
type RCode int

// Standard codes (the low 4 bits of the header). The values match the
// dns.Rcode* constants of miekg/dns.
const (
	RCodeNOERROR  RCode = dns.RcodeSuccess
	RCodeFORMERR  RCode = dns.RcodeFormatError
	RCodeSERVFAIL RCode = dns.RcodeServerFailure
	RCodeNXDOMAIN RCode = dns.RcodeNameError
	RCodeNOTIMP   RCode = dns.RcodeNotImplemented
	RCodeREFUSED  RCode = dns.RcodeRefused
)

// String returns the code mnemonic (NOERROR, NXDOMAIN, …) or RCODEn.
func (r RCode) String() string {
	if s, ok := dns.RcodeToString[int(r)]; ok {
		return s
	}
	return fmt.Sprintf("RCODE%d", int(r))
}

// Result is the outcome of one Exchange: the response code, raw records
// and exchange metrics. The Answer/Ns fields are carried over from the
// response as is (ownership passes to the caller).
type Result struct {
	// RCode is the server response code (NOERROR with an empty answer is
	// NODATA, the legal outcome of "the name exists, no records of this type").
	RCode RCode
	// Answer is the answer section: A/AAAA/CNAME/NS mixed with everything
	// else (for example, a CNAME chain comes before the target A). Raw
	// records — interpretation (CNAME chains, wildcard matching T-038) is
	// up to the consumer.
	Answer []dns.RR
	// Ns is the authority section (the SOA on NXDOMAIN, delegation NS on
	// referrals) — needed by the T-036 consumer ("NS when necessary").
	Ns []dns.RR
	// Elapsed is the whole Exchange time: all attempts and the pauses between them.
	Elapsed time.Duration
	// Attempts is the number of network exchanges (each UDP attempt and
	// each TCP fallback counts separately).
	Attempts int
	// TCP — the final answer arrived over TCP (the TC=1 fallback fired).
	TCP bool
}

// RecordsOf returns the records of the answer section of the given type
// (dns.TypeA, dns.TypeAAAA, …) — a filter without interpretation.
func (r *Result) RecordsOf(t uint16) []dns.RR {
	var out []dns.RR
	for _, rr := range r.Answer {
		if rr.Header().Rrtype == t {
			out = append(out, rr)
		}
	}
	return out
}

// Resolver is the integration point of the DNS stage: the consumer
// (dns-worker, T-036…T-039) depends on the interface, not the
// implementation. T-035 wraps it in a decorator with concurrency/QPS
// limits and an AAAA filter (dns.ipv6), without changing the contract.
type Resolver interface {
	// Exchange performs the question against the resolver. An error means
	// only the network (after retries are exhausted) or ctx cancellation;
	// the RCODE is in Result.
	Exchange(ctx context.Context, q Question) (*Result, error)
}

// compile-time check: *Client — Resolver.
var _ Resolver = (*Client)(nil)

// Options holds the client parameters. Zero values are replaced with the
// defaults (withDefaults); Addr is required.
type Options struct {
	// Addr is the resolver address host:port (the first of dns.resolvers —
	// the local Unbound, spec 5.4). One client — one resolver: the public
	// fallback resolvers (dns.fallback_resolvers) are the FallbackResolver
	// chain above the transport (fallback.go, T-107).
	Addr string
	// Timeout is the timeout of one UDP attempt. Defaults to 2s.
	Timeout time.Duration
	// TCPTimeout is the TCP fallback timeout (connection + exchange).
	// Defaults to 5s.
	TCPTimeout time.Duration
	// Retries are the retries beyond the first attempt on timeout/network error.
	// Defaults to 2 (up to 3 exchanges total); 0/negative means the default
	// too: there is no "no retries" mode (spec 5.4 requires retries).
	Retries int
	// RetryDelay is the pause between attempts. Defaults to 200ms.
	RetryDelay time.Duration
	// UDPPoolSize is the UDP socket pool size (see DefaultUDPPoolSize).
	UDPPoolSize int
}

// NewOptions assembles the parameters from the dns section of the config (T-006):
// the address is the first resolver (the client works with one server — Unbound);
// timeouts/retries/pool are the section's own fields.
func NewOptions(cfg config.DNS) Options {
	addr := ""
	if len(cfg.Resolvers) > 0 {
		addr = cfg.Resolvers[0]
	}
	return Options{
		Addr:        addr,
		Timeout:     cfg.Timeout.D(),
		TCPTimeout:  cfg.TCPTimeout.D(),
		Retries:     cfg.Retries,
		RetryDelay:  cfg.RetryDelay.D(),
		UDPPoolSize: cfg.UDPPoolSize,
	}
}

// withDefaults replaces zero and negative values with the defaults
// (the db.Options repo style: "0 means the default value"). There is no
// separate "no retries" mode: spec 5.4 requires retries, the minimum is
// one retry; a "no pause" mode is not needed either.
func (o Options) withDefaults() Options {
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	if o.TCPTimeout <= 0 {
		o.TCPTimeout = DefaultTCPTimeout
	}
	if o.Retries <= 0 {
		o.Retries = DefaultRetries
	}
	if o.RetryDelay <= 0 {
		o.RetryDelay = DefaultRetryDelay
	}
	if o.UDPPoolSize <= 0 {
		o.UDPPoolSize = DefaultUDPPoolSize
	}
	return o
}

// validate checks the values after the defaults are applied. The
// timeout/pool checks are redundant (withDefaults has already normalized)
// and are not repeated here; only what normalization cannot sensibly fix is caught.
func (o Options) validate() error {
	if _, _, err := net.SplitHostPort(o.Addr); err != nil {
		return fmt.Errorf("dnsclient: адрес резолвера: %w", err)
	}
	if o.Retries > MaxRetries {
		return fmt.Errorf("dnsclient: retries %d: максимум %d", o.Retries, MaxRetries)
	}
	// Guard against direct construction bypassing NewClient.
	if o.UDPPoolSize < 1 {
		return fmt.Errorf("dnsclient: udp_pool_size %d: ожидается ≥ 1", o.UDPPoolSize)
	}
	return nil
}

// exchangeFunc is the transport of one network exchange: send the message,
// read the response (tcp — the TC=1 TCP fallback). A separate type is the
// seam for swapping the transport in unit tests without a network; the
// production implementation is exchangeNet: the UDP socket pool and a TCP connection per request.
type exchangeFunc func(ctx context.Context, m *dns.Msg, tcp bool) (*dns.Msg, error)

// Client is a DNS client over one resolver. Safe for concurrent use from
// goroutines (the socket pool is the only shared state); the cap on
// concurrent Exchange calls is the pool size: waiters line up in the Get
// queue.
type Client struct {
	opt      Options
	pool     *UDPPool
	bufs     sync.Pool // *[]byte: UDP read buffers (64 KiB) carried between attempts
	exchange exchangeFunc
}

// NewClient creates the client and opens the UDP socket pool in advance.
// The startup fail-fast is limited to the address itself: a dial error (a
// bad address, port, route) is a pool creation error at startup; resolver
// availability is not checked by the UDP connect (it sends no packets) — a
// dead server shows up in the very first exchange (ECONNREFUSED/timeout →
// temp_error) and is visible in the limiter metrics
// (crawler_dns_limit_timeout_ratio, DNS-06), not as a startup error. The
// startup synthetic probe of the resolver and the switch to
// dns.fallback_resolvers are decorators above the client (probe.go,
// fallback.go, T-107). The context bounds the socket opening.
func NewClient(ctx context.Context, opts Options) (*Client, error) {
	opts = opts.withDefaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}
	pool, err := NewUDPPool(ctx, opts.Addr, opts.UDPPoolSize)
	if err != nil {
		return nil, err
	}
	c := &Client{opt: opts, pool: pool}
	c.bufs.New = func() any {
		b := make([]byte, dns.MaxMsgSize)
		return &b
	}
	c.exchange = c.exchangeNet
	return c, nil
}

// Close closes the socket pool; the client is unusable afterwards.
func (c *Client) Close() error { return c.pool.Close() }

// Exchange performs the question against the resolver with retries (spec 5.4).
//
// An attempt = a UDP exchange; a TC=1 response is retried over TCP (its
// own timeout) with the same question — a TCP error fails the whole
// attempt and the cycle starts over with UDP. Network errors are retried
// up to Retries times with a RetryDelay pause; ctx cancellation is checked
// before an attempt, after each failure and during the pause (between attempts Exchange aborts at once).
func (c *Client) Exchange(ctx context.Context, q Question) (*Result, error) {
	if err := q.validate(); err != nil {
		return nil, err
	}
	msg := new(dns.Msg)
	msg.SetQuestion(dns.Fqdn(q.Name), q.Type) // SetQuestion sets Id and RD
	// EDNS0 (RFC 6891): an OPT RR with the advertised DefaultEDNS0UDPSize —
	// the resolver answers up to 1232 bytes over UDP, so long CNAME chains
	// and large RRsets do not go to TC=1/TCP fallback on every attempt.
	// DO is not set: DNSSEC is not requested (spec 5.4 does not require it);
	// the OPT comes in the response Additional and does not affect Answer/Ns parsing.
	msg.SetEdns0(DefaultEDNS0UDPSize, false)
	if _, err := msg.Pack(); err != nil {
		// The packing error is deterministic (name length/characters) —
		// retries will not change the outcome.
		return nil, fmt.Errorf("dnsclient: некорректное имя вопроса %q: %w", q.Name, err)
	}

	start := time.Now()
	attempts := 0
	var lastErr error
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		resp, n, viaTCP, err := c.attempt(ctx, msg)
		attempts += n
		if err == nil {
			return &Result{
				RCode:    RCode(resp.Rcode),
				Answer:   resp.Answer,
				Ns:       resp.Ns,
				Elapsed:  time.Since(start),
				Attempts: attempts,
				TCP:      viaTCP,
			}, nil
		}
		lastErr = err
		if ctxErr := ctx.Err(); ctxErr != nil {
			// A cancellation/deadline of the call itself is not a network failure,
			// no retries are due.
			return nil, ctxErr
		}
		if attempt >= c.opt.Retries {
			break
		}
		if c.opt.RetryDelay > 0 {
			timer := time.NewTimer(c.opt.RetryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return nil, fmt.Errorf("dnsclient: %s %s: исчерпаны попытки (%d сетевых обменов): %w",
		qTypeName(q.Type), q.Name, attempts, lastErr)
}

// qTypeName returns the record type mnemonic (A, AAAA, …).
func qTypeName(t uint16) string {
	if s, ok := dns.TypeToString[t]; ok {
		return s
	}
	return fmt.Sprintf("TYPE%d", t)
}

// attempt is one attempt of the retry loop: a UDP exchange and, if the
// response is truncated (TC=1), a retry of the same question over TCP.
// Returns the response, the number of network exchanges (1 or 2), the
// "final over TCP" flag and the error.
func (c *Client) attempt(ctx context.Context, msg *dns.Msg) (*dns.Msg, int, bool, error) {
	actx, cancel := context.WithTimeout(ctx, c.opt.Timeout)
	defer cancel()

	// A copy with a fresh Id per exchange: a socket has one request in
	// flight, but sockets return to the pool and a late response of the
	// previous exchange may arrive at the start of the next one — the fresh
	// Id drops it in the udpExchange check.
	m := *msg
	m.Id = dns.Id()
	resp, err := c.exchange(actx, &m, false)
	if err != nil {
		return nil, 1, false, fmt.Errorf("udp: %w", err)
	}
	if !resp.Truncated {
		return resp, 1, false, nil
	}

	// TC=1: the server truncated the response (the UDP datagram is too
	// small) — the same question over TCP, where there is no datagram size
	// limit (RFC 1035 §4.2.2). Its own timeout: the TCP fallback is costlier
	// (connection setup, transferring the full response).
	tctx, tcancel := context.WithTimeout(ctx, c.opt.TCPTimeout)
	defer tcancel()
	m.Id = dns.Id()
	tresp, err := c.exchange(tctx, &m, true)
	if err != nil {
		return nil, 2, false, fmt.Errorf("tcp-fallback: %w", err)
	}
	return tresp, 2, true, nil
}

// exchangeNet is the network transport: UDP via the socket pool, TCP —
// a connection per request.
func (c *Client) exchangeNet(ctx context.Context, m *dns.Msg, tcp bool) (*dns.Msg, error) {
	if tcp {
		return c.tcpExchange(ctx, m)
	}
	return c.udpExchange(ctx, m)
}

// udpExchange performs the exchange via the pool of connected UDP sockets.
// The attempt deadline is taken from ctx (the timeout is already accounted for in attempt).
//
// Responses with a foreign Id and unparseable datagrams are skipped: the
// socket is reused and after a timeout a late response of the previous
// request may sit on it; reading continues until the own deadline.
func (c *Client) udpExchange(ctx context.Context, m *dns.Msg) (*dns.Msg, error) {
	conn, err := c.pool.Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("сокет из пула: %w", err)
	}
	defer c.pool.Put(conn)

	wire, err := m.Pack()
	if err != nil {
		return nil, fmt.Errorf("упаковка запроса: %w", err)
	}
	dl, ok := ctx.Deadline()
	if !ok {
		dl = time.Now().Add(c.opt.Timeout) // guard: the transport must be time-bounded
	}
	_ = conn.SetDeadline(dl)
	if _, err := conn.Write(wire); err != nil {
		return nil, err
	}

	bufp := c.bufs.Get().(*[]byte)
	defer c.bufs.Put(bufp)
	buf := *bufp
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return nil, err
		}
		resp := new(dns.Msg)
		if err := resp.Unpack(buf[:n]); err != nil {
			continue // a broken datagram — keep waiting for the own response until the deadline
		}
		if resp.Id != m.Id {
			continue // a late response of the previous exchange on this socket
		}
		return resp, nil
	}
}

// tcpExchange is the TCP fallback: a connection per request, no pool.
//
// Rationale: TC=1 is a rare event (regular A/AAAA/CNAME/NS answers fit
// into a 512-byte datagram), so keeping a permanent pool of TCP
// connections for a rare branch is a constant cost (keepalive,
// re-establishing after the resolver idle timeout) with no gain.
// The cost of establishing a connection to the local Unbound is
// fractions of a millisecond; a fresh connection also carries no state
// left over from foreign requests.
func (c *Client) tcpExchange(ctx context.Context, m *dns.Msg) (*dns.Msg, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", c.opt.Addr)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	dconn := &dns.Conn{Conn: conn}
	if err := dconn.WriteMsg(m); err != nil {
		return nil, err
	}
	resp, err := dconn.ReadMsg()
	if err != nil {
		return nil, err
	}
	if resp.Id != m.Id {
		return nil, fmt.Errorf("идентификатор ответа %d не совпадает с запросом %d", resp.Id, m.Id)
	}
	return resp, nil
}

// IsTimeout reports whether an Exchange error was caused by a timeout (an
// attempt deadline, waiting for a socket from the pool) rather than a
// network/protocol refusal. The distinction is needed by the T-035
// adaptive load shedding (spec 5.4: the timeout share) and consumer
// diagnostics (T-036: SERVFAIL/timeout).
func IsTimeout(err error) bool {
	if err == nil {
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return ne.Timeout()
	}
	return errors.Is(err, context.DeadlineExceeded)
}

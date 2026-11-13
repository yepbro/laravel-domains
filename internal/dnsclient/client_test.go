// Client tests. Two levels, as the task requires:
//
//   - the exchangeFunc transport seam — retries, TC→TCP fallback, RCODE
//     mapping, ctx cancellation, attempt counters (no network at all);
//   - a local miekg/dns DNS server on 127.0.0.1:0 (loopback, not an
//     external stack) — real UDP and TCP wiring: packing, reading,
//     skipping garbage and late answers, deadlines.

package dnsclient

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/miekg/dns"

	"crawler/internal/config"
)

// --- helper constructors ---------------------------------------------------

// fakeCall is a recorded transport call.
type fakeCall struct {
	m   *dns.Msg
	tcp bool
}

// newFakeClient is a client with the transport swapped out: no network,
// no pool. modify layers parameters on top of zero values (then withDefaults).
func newFakeClient(t *testing.T, modify func(*Options), fn exchangeFunc) (*Client, *[]fakeCall) {
	t.Helper()
	var calls []fakeCall
	var opt Options
	if modify != nil {
		modify(&opt)
	}
	c := &Client{opt: opt.withDefaults()}
	c.bufs.New = func() any {
		b := make([]byte, dns.MaxMsgSize)
		return &b
	}
	c.exchange = func(ctx context.Context, m *dns.Msg, tcp bool) (*dns.Msg, error) {
		calls = append(calls, fakeCall{m: m, tcp: tcp})
		return fn(ctx, m, tcp)
	}
	return c, &calls
}

// fakeNetErr is a network error with a controllable timeout flag.
type fakeNetErr struct{ timeout bool }

func (fakeNetErr) Error() string   { return "fake network error" }
func (e fakeNetErr) Timeout() bool { return e.timeout }
func (fakeNetErr) Temporary() bool { return false }

func aRR(name, ip string) dns.RR {
	return &dns.A{
		Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60},
		A:   net.ParseIP(ip).To4(),
	}
}

func aaaaRR(name, ip string) dns.RR {
	return &dns.AAAA{
		Hdr:  dns.RR_Header{Name: name, Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: 60},
		AAAA: net.ParseIP(ip),
	}
}

func cnameRR(from, to string) dns.RR {
	return &dns.CNAME{
		Hdr:    dns.RR_Header{Name: from, Rrtype: dns.TypeCNAME, Class: dns.ClassINET, Ttl: 60},
		Target: to,
	}
}

func nsRR(name, host string) dns.RR {
	return &dns.NS{
		Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeNS, Class: dns.ClassINET, Ttl: 60},
		Ns:  host,
	}
}

// makeReply is a reply to the request q with the code and an answer section.
func makeReply(q *dns.Msg, rcode int, an ...dns.RR) *dns.Msg {
	m := new(dns.Msg)
	m.SetReply(q)
	m.Rcode = rcode
	m.Answer = an
	return m
}

// --- transport seam: the Exchange contract ---------------------------------

func TestExchangeReturnsAnswerRecords(t *testing.T) {
	c, calls := newFakeClient(t, nil, func(_ context.Context, m *dns.Msg, tcp bool) (*dns.Msg, error) {
		return makeReply(m, dns.RcodeSuccess,
			aRR("example.com.", "192.0.2.1"),
			cnameRR("www.example.com.", "example.com.")), nil
	})

	res, err := c.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if res.RCode != RCodeNOERROR || res.RCode.String() != "NOERROR" {
		t.Fatalf("rcode: got %v", res.RCode)
	}
	if got := res.RecordsOf(dns.TypeA); len(got) != 1 {
		t.Fatalf("A records: %d, want 1", len(got))
	} else if got[0].(*dns.A).A.String() != "192.0.2.1" {
		t.Fatalf("the A address: %s", got[0].(*dns.A).A)
	}
	if got := res.RecordsOf(dns.TypeCNAME); len(got) != 1 {
		t.Fatalf("CNAME records: %d, want 1", len(got))
	}
	if got := res.RecordsOf(dns.TypeAAAA); len(got) != 0 {
		t.Fatalf("there must be no AAAA records, got %d", len(got))
	}
	if res.TCP || res.Attempts != 1 {
		t.Fatalf("attempts/TCP: attempts=%d tcp=%v", res.Attempts, res.TCP)
	}

	// The question reached the transport canonical: FQDN, type, class, recursion.
	if len(*calls) != 1 {
		t.Fatalf("transport calls: %d", len(*calls))
	}
	call := (*calls)[0]
	if call.tcp {
		t.Fatal("the first exchange must be UDP")
	}
	q := call.m.Question[0]
	if q.Name != "example.com." || q.Qtype != dns.TypeA || q.Qclass != dns.ClassINET {
		t.Fatalf("the question on the transport: %+v", q)
	}
	if !call.m.RecursionDesired {
		t.Fatal("RecursionDesired must be set (the resolver is recursive)")
	}
}

func TestExchangeRCodeIsResultNotError(t *testing.T) {
	cases := []struct {
		rcode int
		want  RCode
	}{
		{dns.RcodeSuccess, RCodeNOERROR},
		{dns.RcodeFormatError, RCodeFORMERR},
		{dns.RcodeServerFailure, RCodeSERVFAIL},
		{dns.RcodeNameError, RCodeNXDOMAIN},
		{dns.RcodeNotImplemented, RCodeNOTIMP},
		{dns.RcodeRefused, RCodeREFUSED},
	}
	for _, tc := range cases {
		t.Run(RCode(tc.rcode).String(), func(t *testing.T) {
			c, _ := newFakeClient(t, nil, func(_ context.Context, m *dns.Msg, _ bool) (*dns.Msg, error) {
				return makeReply(m, tc.rcode), nil
			})
			res, err := c.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
			if err != nil {
				t.Fatalf("an RCODE is not an error, got: %v", err)
			}
			if res.RCode != tc.want {
				t.Fatalf("rcode: %v, want %v", res.RCode, tc.want)
			}
			if res.RCode.String() != dns.RcodeToString[tc.rcode] {
				t.Fatalf("String(): %q, want %q", res.RCode, dns.RcodeToString[tc.rcode])
			}
		})
	}
	// An extended code is a number with a speaking mnemonic.
	if got := RCode(42).String(); got != "RCODE42" {
		t.Fatalf("unknown code: %q", got)
	}
}

func TestExchangeTCFallbackToTCP(t *testing.T) {
	full := []dns.RR{cnameRR("www.example.com.", "example.com."), aRR("example.com.", "192.0.2.7")}
	c, calls := newFakeClient(t, nil, func(_ context.Context, m *dns.Msg, tcp bool) (*dns.Msg, error) {
		if !tcp {
			r := makeReply(m, dns.RcodeSuccess)
			r.Truncated = true // TC=1
			return r, nil
		}
		return makeReply(m, dns.RcodeSuccess, full...), nil
	})

	res, err := c.Exchange(context.Background(), Question{Name: "www.example.com", Type: dns.TypeA})
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if !res.TCP {
		t.Fatal("the final answer must arrive over TCP")
	}
	if res.Attempts != 2 {
		t.Fatalf("exchanges: %d, want 2 (UDP + TCP)", res.Attempts)
	}
	if len(res.Answer) != len(full) {
		t.Fatalf("records in the answer: %d, want %d", len(res.Answer), len(full))
	}
	if len(*calls) != 2 || (*calls)[0].tcp || !(*calls)[1].tcp {
		t.Fatalf("the exchange order: UDP first, then TCP; got %+v", *calls)
	}
	// TCP repeats the same question (name and type), nothing else.
	q1 := (*calls)[0].m.Question[0]
	q2 := (*calls)[1].m.Question[0]
	if q1 != q2 {
		t.Fatalf("the question changed between UDP and TCP: %+v → %+v", q1, q2)
	}
}

func TestExchangeTCPFailureFailsAttempt(t *testing.T) {
	sentinel := errors.New("tcp refused")
	c, calls := newFakeClient(t, func(o *Options) {
		o.Retries = 1
		o.RetryDelay = time.Millisecond
	}, func(_ context.Context, m *dns.Msg, tcp bool) (*dns.Msg, error) {
		if !tcp {
			r := makeReply(m, dns.RcodeSuccess)
			r.Truncated = true
			return r, nil
		}
		return nil, sentinel
	})

	_, err := c.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeNS})
	if err == nil {
		t.Fatal("the TCP-fallback error must fail the query")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("the error does not wrap the cause: %v", err)
	}
	if !strings.Contains(err.Error(), "tcp-fallback") {
		t.Fatalf("the message does not mention the tcp-fallback branch: %v", err)
	}
	// Two attempts × (UDP + TCP) = 4 exchanges: a TCP error fails the
	// attempt entirely, the cycle starts over with UDP.
	if n := len(*calls); n != 4 {
		t.Fatalf("exchanges: %d, want 4", n)
	}
}

func TestExchangeRetriesThenSucceeds(t *testing.T) {
	attempt := 0
	c, _ := newFakeClient(t, func(o *Options) {
		o.Retries = 2
		o.RetryDelay = 25 * time.Millisecond
	}, func(_ context.Context, m *dns.Msg, _ bool) (*dns.Msg, error) {
		attempt++
		if attempt < 3 {
			return nil, fakeNetErr{timeout: true}
		}
		return makeReply(m, dns.RcodeSuccess, aRR("example.com.", "192.0.2.1")), nil
	})

	start := time.Now()
	res, err := c.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
	if err != nil {
		t.Fatalf("Exchange after the retries: %v", err)
	}
	if res.Attempts != 3 {
		t.Fatalf("exchanges: %d, want 3", res.Attempts)
	}
	// The two 25 ms pauses between the three attempts must show up in Elapsed.
	if min := 50 * time.Millisecond; res.Elapsed < min {
		t.Fatalf("Elapsed %s is less than the sum of the pauses %s", res.Elapsed, min)
	}
	if since := time.Since(start); since < 50*time.Millisecond {
		t.Fatalf("the real exchange time %s is less than the sum of the pauses", since)
	}
}

func TestExchangeRetriesExhausted(t *testing.T) {
	sentinel := errors.New("wire cut")
	c, calls := newFakeClient(t, func(o *Options) {
		o.Retries = 2
		o.RetryDelay = time.Millisecond
	}, func(_ context.Context, _ *dns.Msg, _ bool) (*dns.Msg, error) {
		return nil, sentinel
	})

	_, err := c.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
	if err == nil {
		t.Fatal("attempt exhaustion — an error")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("the error does not wrap the last cause: %v", err)
	}
	if !strings.Contains(err.Error(), "attempts exhausted") || !strings.Contains(err.Error(), "example.com") {
		t.Fatalf("the message without context: %v", err)
	}
	if n := len(*calls); n != 3 { // the first + 2 retries
		t.Fatalf("exchanges: %d, want 3", n)
	}
}

func TestExchangeCancelledDuringRetryDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c, calls := newFakeClient(t, func(o *Options) {
		o.Retries = 2
		o.RetryDelay = 10 * time.Second // a pause long enough for the cancellation to arrive
	}, func(_ context.Context, _ *dns.Msg, _ bool) (*dns.Msg, error) {
		// The first attempt fails and cancels the outer ctx at the same time:
		// Exchange must exit during the pause without making the second attempt.
		time.AfterFunc(10*time.Millisecond, cancel)
		return nil, fakeNetErr{timeout: true}
	})

	start := time.Now()
	_, err := c.Exchange(ctx, Question{Name: "example.com", Type: dns.TypeA})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if n := len(*calls); n != 1 {
		t.Fatalf("no new attempts after cancellation, exchanges %d", n)
	}
	if since := time.Since(start); since > time.Second {
		t.Fatalf("cancellation during a pause does not interrupt Exchange immediately: %s", since)
	}
}

func TestExchangeCancelledBeforeStart(t *testing.T) {
	c, calls := newFakeClient(t, nil, func(_ context.Context, _ *dns.Msg, _ bool) (*dns.Msg, error) {
		t.Error("the transport must not be called with a cancelled ctx")
		return nil, errors.New("unreachable")
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Exchange(ctx, Question{Name: "example.com", Type: dns.TypeA}); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if len(*calls) != 0 {
		t.Fatal("the transport was called with a cancelled ctx")
	}
}

func TestExchangeQuestionValidation(t *testing.T) {
	cases := []struct {
		name string
		q    Question
		want string
	}{
		{"an empty name", Question{Name: "", Type: dns.TypeA}, "empty question name"},
		{"a blank name", Question{Name: "  ", Type: dns.TypeA}, "empty question name"},
		{"a zero type", Question{Name: "example.com", Type: 0}, "unknown record type"},
		{"a non-existent type", Question{Name: "example.com", Type: 0xFFFF}, "unknown record type"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, calls := newFakeClient(t, nil, func(_ context.Context, _ *dns.Msg, _ bool) (*dns.Msg, error) {
				t.Error("an invalid question must not reach the transport")
				return nil, errors.New("unreachable")
			})
			_, err := c.Exchange(context.Background(), tc.q)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want the error %q, got %v", tc.want, err)
			}
			if len(*calls) != 0 {
				t.Fatal("the transport was called for an invalid question")
			}
		})
	}
}

func TestExchangeBadNameFailsPack(t *testing.T) {
	// A label longer than 63 octets does not pack into the wire format — the
	// error is deterministic, the network is never reached.
	long := strings.Repeat("a", 64) + ".example.com"
	c, calls := newFakeClient(t, nil, func(_ context.Context, _ *dns.Msg, _ bool) (*dns.Msg, error) {
		t.Error("the transport must not be called for an unpackable name")
		return nil, errors.New("unreachable")
	})
	if _, err := c.Exchange(context.Background(), Question{Name: long, Type: dns.TypeA}); err == nil {
		t.Fatal("want a packing error")
	} else if !strings.Contains(err.Error(), "invalid question name") {
		t.Fatalf("the message without the packing context: %v", err)
	}
	if len(*calls) != 0 {
		t.Fatal("the transport was called for an unpackable name")
	}
}

func TestExchangeQuestionFqdnPassthrough(t *testing.T) {
	c, calls := newFakeClient(t, nil, func(_ context.Context, m *dns.Msg, _ bool) (*dns.Msg, error) {
		return makeReply(m, dns.RcodeSuccess, nsRR("example.com.", "ns1.example.com.")), nil
	})
	// A name without the trailing dot gets it appended, with the dot it is unchanged.
	for _, name := range []string{"example.com", "example.com."} {
		if _, err := c.Exchange(context.Background(), Question{Name: name, Type: dns.TypeNS}); err != nil {
			t.Fatalf("Exchange(%q): %v", name, err)
		}
	}
	for i, want := range []string{"example.com.", "example.com."} {
		if got := (*calls)[i].m.Question[0].Name; got != want {
			t.Fatalf("the question[%d] on the transport %q, want %q", i, got, want)
		}
	}
}

// TestExchangeEDNS0 — the request carries an OPT RR (RFC 6891) with the
// advertised DefaultEDNS0UDPSize and without the DO bit: large answers
// arrive in full over UDP, not with TC=1 and a TCP fallback on every attempt (DNS-07).
func TestExchangeEDNS0(t *testing.T) {
	c, calls := newFakeClient(t, nil, func(_ context.Context, m *dns.Msg, _ bool) (*dns.Msg, error) {
		return makeReply(m, dns.RcodeSuccess, aRR("example.com.", "192.0.2.1")), nil
	})
	if _, err := c.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA}); err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("transport calls: %d, want 1", len(*calls))
	}
	var opt *dns.OPT
	for _, rr := range (*calls)[0].m.Extra {
		if o, ok := rr.(*dns.OPT); ok {
			opt = o
			break
		}
	}
	if opt == nil {
		t.Fatal("the query has no OPT RR — EDNS0 is not requested")
	}
	if opt.UDPSize() != DefaultEDNS0UDPSize {
		t.Fatalf("OPT UDPSize = %d, want %d", opt.UDPSize(), DefaultEDNS0UDPSize)
	}
	if opt.Do() {
		t.Fatal("the DO bit is not set: DNSSEC is not requested")
	}
}

// --- configuration ---------------------------------------------------------

func TestOptionsDefaultsAndValidation(t *testing.T) {
	def := Options{Addr: "127.0.0.1:53"}.withDefaults()
	if def.Timeout != DefaultTimeout || def.TCPTimeout != DefaultTCPTimeout {
		t.Fatalf("the default timeouts: %+v", def)
	}
	if def.Retries != DefaultRetries || def.RetryDelay != DefaultRetryDelay {
		t.Fatalf("the default retries: %+v", def)
	}
	if def.UDPPoolSize != DefaultUDPPoolSize {
		t.Fatalf("the default pool: %d", def.UDPPoolSize)
	}
	if err := def.validate(); err != nil {
		t.Fatalf("the defaults must pass validation: %v", err)
	}

	// What normalization cannot sensibly fix: a bad address and too many
	// retries stay errors (a negative pool size and timeouts are replaced
	// with defaults by withDefaults — like db.Options).
	bad := []Options{
		{},                  // an empty address
		{Addr: "127.0.0.1"}, // no port
		{Addr: "127.0.0.1:53", Retries: MaxRetries + 1},
	}
	for i, o := range bad {
		if err := o.withDefaults().validate(); err == nil {
			t.Fatalf("option %d (%+v) must fail validation", i, o)
		}
	}
}

func TestExchangeAAAARecords(t *testing.T) {
	c, _ := newFakeClient(t, nil, func(_ context.Context, m *dns.Msg, _ bool) (*dns.Msg, error) {
		return makeReply(m, dns.RcodeSuccess,
			aaaaRR("v6.example.com.", "2001:db8::1"),
			aRR("v6.example.com.", "192.0.2.9")), nil
	})
	res, err := c.Exchange(context.Background(), Question{Name: "v6.example.com", Type: dns.TypeAAAA})
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	got := res.RecordsOf(dns.TypeAAAA)
	if len(got) != 1 || got[0].(*dns.AAAA).AAAA.String() != "2001:db8::1" {
		t.Fatalf("the AAAA records: %+v", got)
	}
}

func TestNewOptionsFromConfig(t *testing.T) {
	cfg := config.DNS{
		Resolvers:   []string{"10.0.0.2:5353", "10.0.0.3:53"},
		Timeout:     config.Duration(3 * time.Second),
		TCPTimeout:  config.Duration(7 * time.Second),
		Retries:     4,
		RetryDelay:  config.Duration(250 * time.Millisecond),
		UDPPoolSize: 64,
	}
	got := NewOptions(cfg)
	want := Options{
		Addr:        "10.0.0.2:5353", // the first resolver (unbound)
		Timeout:     3 * time.Second,
		TCPTimeout:  7 * time.Second,
		Retries:     4,
		RetryDelay:  250 * time.Millisecond,
		UDPPoolSize: 64,
	}
	if got != want {
		t.Fatalf("NewOptions: %+v, want %+v", got, want)
	}
	// An empty resolver list is an empty address: NewClient will catch the
	// validation (the config guarantees a non-empty list, spec 5.10).
	if got := (NewOptions(config.DNS{})).Addr; got != "" {
		t.Fatalf("without resolvers Addr must be empty, got %q", got)
	}
}

func TestNewClientValidation(t *testing.T) {
	if _, err := NewClient(context.Background(), Options{Addr: "nohostport", UDPPoolSize: 1}); err == nil {
		t.Fatal("a malformed address: want an error")
	}
	if _, err := NewClient(context.Background(), Options{Addr: "127.0.0.1:99999", UDPPoolSize: 1}); err == nil {
		t.Fatal("an unreachable port: want a pool opening error (fail-fast)")
	}
}

// --- local DNS server (loopback) -------------------------------------------

// isTCPWriter determines the request protocol from the client address:
// for UDP it is *net.UDPAddr, for TCP — *net.TCPAddr (there is no other
// way to learn the protocol from a Handler in miekg/dns).
func isTCPWriter(w dns.ResponseWriter) bool {
	_, ok := w.RemoteAddr().(*net.TCPAddr)
	return ok
}

// spinLocalDNS brings up a DNS server on 127.0.0.1:0, UDP and TCP on the
// same port (like a real resolver), and returns the address.
func spinLocalDNS(t *testing.T, h dns.Handler) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	lc, err := net.Listen("tcp", pc.LocalAddr().String())
	if err != nil {
		_ = pc.Close()
		t.Fatalf("listen tcp on a udp port: %v", err)
	}
	udpSrv := &dns.Server{PacketConn: pc, Handler: h}
	tcpSrv := &dns.Server{Listener: lc, Handler: h}
	go func() { _ = udpSrv.ActivateAndServe() }()
	go func() { _ = tcpSrv.ActivateAndServe() }()
	t.Cleanup(func() {
		_ = udpSrv.Shutdown()
		_ = tcpSrv.Shutdown()
	})
	return pc.LocalAddr().String()
}

// newLocalClient is a real client (a socket pool) to the local server.
func newLocalClient(t *testing.T, addr string, modify func(*Options)) *Client {
	t.Helper()
	opts := Options{
		Addr:        addr,
		Timeout:     time.Second,
		TCPTimeout:  time.Second,
		Retries:     1, // no retries by default: deterministic test timing
		RetryDelay:  5 * time.Millisecond,
		UDPPoolSize: 4,
	}
	if modify != nil {
		modify(&opts)
	}
	c, err := NewClient(context.Background(), opts)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestLocalExchangeAOverUDP(t *testing.T) {
	addr := spinLocalDNS(t, dns.HandlerFunc(func(w dns.ResponseWriter, r *dns.Msg) {
		if isTCPWriter(w) {
			t.Error("the query is not required to go over TCP")
		}
		m := makeReply(r, dns.RcodeSuccess, aRR(r.Question[0].Name, "192.0.2.10"))
		_ = w.WriteMsg(m)
	}))
	c := newLocalClient(t, addr, nil)

	res, err := c.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if res.RCode != RCodeNOERROR || res.TCP || res.Attempts != 1 {
		t.Fatalf("the result: %+v", res)
	}
	got := res.RecordsOf(dns.TypeA)
	if len(got) != 1 || got[0].(*dns.A).A.String() != "192.0.2.10" {
		t.Fatalf("the A records: %+v", got)
	}
}

func TestLocalTCFallback(t *testing.T) {
	full := []dns.RR{
		cnameRR("www.example.com.", "example.com."),
		aRR("example.com.", "192.0.2.7"),
		nsRR("example.com.", "ns1.example.com."),
	}
	addr := spinLocalDNS(t, dns.HandlerFunc(func(w dns.ResponseWriter, r *dns.Msg) {
		if !isTCPWriter(w) {
			// UDP: a short response with the truncation flag (TC=1).
			m := makeReply(r, dns.RcodeSuccess)
			m.Truncated = true
			_ = w.WriteMsg(m)
			return
		}
		_ = w.WriteMsg(makeReply(r, dns.RcodeSuccess, full...))
	}))
	c := newLocalClient(t, addr, nil)

	res, err := c.Exchange(context.Background(), Question{Name: "www.example.com", Type: dns.TypeA})
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if !res.TCP || res.Attempts != 2 {
		t.Fatalf("want the fallback to TCP: attempts=%d tcp=%v", res.Attempts, res.TCP)
	}
	if len(res.Answer) != len(full) {
		t.Fatalf("records: %d, want %d", len(res.Answer), len(full))
	}
	if got := res.RecordsOf(dns.TypeNS); len(got) != 1 {
		t.Fatalf("the NS record did not arrive over TCP: %+v", got)
	}
}

func TestLocalRCodes(t *testing.T) {
	addr := spinLocalDNS(t, dns.HandlerFunc(func(w dns.ResponseWriter, r *dns.Msg) {
		name := r.Question[0].Name
		switch name {
		case "nx.example.com.":
			m := makeReply(r, dns.RcodeNameError)
			m.Ns = []dns.RR{nsRR("example.com.", "ns1.example.com.")}
			_ = w.WriteMsg(m)
		case "servfail.example.com.":
			_ = w.WriteMsg(makeReply(r, dns.RcodeServerFailure))
		case "refused.example.com.":
			_ = w.WriteMsg(makeReply(r, dns.RcodeRefused))
		default:
			_ = w.WriteMsg(makeReply(r, dns.RcodeRefused))
		}
	}))
	c := newLocalClient(t, addr, nil)

	for name, want := range map[string]RCode{
		"nx.example.com":       RCodeNXDOMAIN,
		"servfail.example.com": RCodeSERVFAIL,
		"refused.example.com":  RCodeREFUSED,
	} {
		res, err := c.Exchange(context.Background(), Question{Name: name, Type: dns.TypeA})
		if err != nil {
			t.Fatalf("%s: an RCODE is not an error: %v", name, err)
		}
		if res.RCode != want {
			t.Fatalf("%s: rcode %v, want %v", name, res.RCode, want)
		}
		if name == "nx.example.com" && len(res.Ns) != 1 {
			t.Fatalf("the authority section (SOA/NS) was not carried over: %+v", res.Ns)
		}
	}
}

// TestLocalTimeout — a silent server: the attempt times out, the retries
// run their course, the outcome is an error with the timeout flag.
func TestLocalTimeout(t *testing.T) {
	addr := loopUDPAddr(t) // the receiver reads and stays silent
	c := newLocalClient(t, addr, func(o *Options) {
		o.Timeout = 80 * time.Millisecond
		o.Retries = 1
		o.RetryDelay = 5 * time.Millisecond
	})

	start := time.Now()
	_, err := c.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
	if err == nil {
		t.Fatal("a silent server: want an error")
	}
	if !IsTimeout(err) {
		t.Fatalf("the error is not recognized as a timeout: %v", err)
	}
	if since := time.Since(start); since < 160*time.Millisecond {
		t.Fatalf("two attempts of 80ms must fit in real time: %s", since)
	}
}

// TestLocalSkipsGarbageAndStaleAnswers — broken datagrams and responses
// with a foreign Id neither fail the exchange nor pass for the answer:
// reading continues until the own deadline.
func TestLocalSkipsGarbageAndStaleAnswers(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })

	go func() {
		buf := make([]byte, dns.MaxMsgSize)
		for {
			_, peer, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			// Per request: garbage, then a valid message with a foreign Id (a
			// late response of the previous exchange), then silence.
			if _, err := pc.WriteTo([]byte("this is not a dns message"), peer); err != nil {
				return
			}
			reqID := binary.BigEndian.Uint16(buf[:2])
			stale := new(dns.Msg)
			stale.SetQuestion("stale.example.com.", dns.TypeA)
			stale.Id = reqID ^ 0xFFFF // guaranteed foreign
			wire, err := stale.Pack()
			if err != nil {
				return
			}
			if _, err := pc.WriteTo(wire, peer); err != nil {
				return
			}
		}
	}()

	c := newLocalClient(t, pc.LocalAddr().String(), func(o *Options) {
		o.Timeout = 150 * time.Millisecond
		o.Retries = 1 // two attempts — both get garbage instead of an answer
	})

	_, err = c.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
	if err == nil {
		t.Fatal("there was no valid answer — want an error")
	}
	if !IsTimeout(err) {
		t.Fatalf("want a timeout after skipping junk, got: %v", err)
	}
}

// TestLocalConcurrentExchange — several Exchange calls at once on one
// client (the pool hands out sockets; the -race run catches races).
func TestLocalConcurrentExchange(t *testing.T) {
	addr := spinLocalDNS(t, dns.HandlerFunc(func(w dns.ResponseWriter, r *dns.Msg) {
		// The answer depends on the name: each goroutine checks its own address.
		ip := fmt.Sprintf("192.0.2.%d", 1+len(r.Question[0].Name)%98)
		_ = w.WriteMsg(makeReply(r, dns.RcodeSuccess, aRR(r.Question[0].Name, ip)))
	}))
	c := newLocalClient(t, addr, nil)

	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("host%d.example.com", i)
			res, err := c.Exchange(context.Background(), Question{Name: name, Type: dns.TypeA})
			if err != nil {
				t.Errorf("Exchange(%s): %v", name, err)
				return
			}
			if res.RCode != RCodeNOERROR || len(res.RecordsOf(dns.TypeA)) != 1 {
				t.Errorf("Exchange(%s): an unexpected result %+v", name, res)
			}
		}(i)
	}
	wg.Wait()
}

// --- IsTimeout --------------------------------------------------------------

func TestIsTimeout(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"deadline", fmt.Errorf("udp: %w", context.DeadlineExceeded), true},
		{"net timeout", fmt.Errorf("udp: %w", fakeNetErr{timeout: true}), true},
		{"net refusal", fmt.Errorf("udp: %w", fakeNetErr{timeout: false}), false},
		{"other", errors.New("boom"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsTimeout(tc.err); got != tc.want {
				t.Fatalf("IsTimeout(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// Tests of the apex check (T-036): all classification branches on a
// scenario fake of dnsclient.Resolver — no network needed. The fake
// answers the calls with the scenario records in order and records the
// questions (the FQDN format, the A/NS type), the order and the number
// of calls.

package dnsworker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"testing"

	"github.com/miekg/dns"

	"crawler/internal/dnsclient"
	"crawler/internal/model"
)

// --- the scenario fake of Resolver -------------------------------------------

// apexStep — a single scenario answer: either a Result with a code and
// an answer section, or an error (a network failure simulation after the
// dnsclient retries are exhausted).
type apexStep struct {
	rcode  dnsclient.RCode
	answer []dns.RR
	err    error
}

// apexReply — an answer step with a code and answer records.
func apexReply(rcode dnsclient.RCode, an ...dns.RR) apexStep {
	return apexStep{rcode: rcode, answer: an}
}

// apexFail — an Exchange error step (the T-034 contract: the network,
// the attempts are exhausted).
func apexFail(err error) apexStep {
	return apexStep{err: err}
}

// apexTimeoutErr — a network error with the timeout sign (the dnsclient
// style).
type apexTimeoutErr struct{}

func (apexTimeoutErr) Error() string   { return "fake i/o timeout" }
func (apexTimeoutErr) Timeout() bool   { return true }
func (apexTimeoutErr) Temporary() bool { return true }

// apexFakeResolver — a scenario dnsclient.Resolver without a network:
// every call takes the next scenario step and records the question.
// onCall (if set) is called after the recording — the test pulls the ctx
// cancellation between the queries.
type apexFakeResolver struct {
	steps  []apexStep
	calls  []dnsclient.Question
	onCall func(n int)
}

func (f *apexFakeResolver) Exchange(ctx context.Context, q dnsclient.Question) (*dnsclient.Result, error) {
	f.calls = append(f.calls, q)
	if f.onCall != nil {
		f.onCall(len(f.calls))
	}
	if len(f.steps) == 0 {
		return nil, fmt.Errorf("apexFakeResolver: the script exhausted (call #%d: %s)", len(f.calls), q.Name)
	}
	st := f.steps[0]
	f.steps = f.steps[1:]
	if st.err != nil {
		return nil, st.err
	}
	return &dnsclient.Result{RCode: st.rcode, Answer: st.answer}, nil
}

// --- record constructors ------------------------------------------------------

func apexARR(name, ip string) dns.RR {
	return &dns.A{
		Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60},
		A:   net.ParseIP(ip).To4(),
	}
}

func apexAAAARR(name, ip string) dns.RR {
	return &dns.AAAA{
		Hdr:  dns.RR_Header{Name: name, Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: 60},
		AAAA: net.ParseIP(ip),
	}
}

func apexCNAME(from, to string) dns.RR {
	return &dns.CNAME{
		Hdr:    dns.RR_Header{Name: from, Rrtype: dns.TypeCNAME, Class: dns.ClassINET, Ttl: 60},
		Target: to,
	}
}

func apexNSRR(name, host string) dns.RR {
	return &dns.NS{
		Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeNS, Class: dns.ClassINET, Ttl: 60},
		Ns:  host,
	}
}

// apexAddrs — []netip.Addr for comparing addresses in the tests.
func apexAddrs(ips ...string) []netip.Addr {
	out := make([]netip.Addr, len(ips))
	for i, ip := range ips {
		out[i] = netip.MustParseAddr(ip)
	}
	return out
}

// --- classification: all branches ----------------------------------------------

func TestCheckApexClassification(t *testing.T) {
	cases := []struct {
		name       string
		steps      []apexStep
		wantStatus ApexStatus
		wantStop   bool
		wantAddrs  []netip.Addr
		wantCalls  int
		wantNS     bool
	}{
		{
			name:       "NXDOMAIN → not_delegated, subdomains are not checked (RFC 8020)",
			steps:      []apexStep{apexReply(dnsclient.RCodeNXDOMAIN)},
			wantStatus: ApexNotDelegated,
			wantStop:   true,
			wantCalls:  1,
		},
		{
			name: "NOERROR with A — delegated, the addresses returned",
			steps: []apexStep{apexReply(dnsclient.RCodeNOERROR,
				apexARR("example.com.", "192.0.2.10"),
				apexARR("example.com.", "192.0.2.11"))},
			wantStatus: ApexDelegated,
			wantStop:   false,
			wantAddrs:  apexAddrs("192.0.2.10", "192.0.2.11"),
			wantCalls:  1,
		},
		{
			name: "NOERROR with CNAME and A (the recursor expanded the chain) — addresses present",
			steps: []apexStep{apexReply(dnsclient.RCodeNOERROR,
				apexCNAME("example.com.", "edge.example.net."),
				apexARR("edge.example.net.", "192.0.2.20"))},
			wantStatus: ApexDelegated,
			wantStop:   false,
			wantAddrs:  apexAddrs("192.0.2.20"),
			wantCalls:  1,
		},
		{
			name: "NOERROR with CNAME only, no A — no addresses, the NS confirms the zone",
			steps: []apexStep{
				apexReply(dnsclient.RCodeNOERROR, apexCNAME("example.com.", "edge.example.net.")),
				apexReply(dnsclient.RCodeNOERROR, apexNSRR("example.com.", "ns1.example.com."))},
			wantStatus: ApexDelegatedNoAddress,
			wantStop:   false,
			wantCalls:  2,
			wantNS:     true,
		},
		{
			name: "NODATA + NS with records — delegated without addresses",
			steps: []apexStep{
				apexReply(dnsclient.RCodeNOERROR),
				apexReply(dnsclient.RCodeNOERROR,
					apexNSRR("example.com.", "ns1.example.com."),
					apexNSRR("example.com.", "ns2.example.com."))},
			wantStatus: ApexDelegatedNoAddress,
			wantStop:   false,
			wantCalls:  2,
			wantNS:     true,
		},
		{
			name: "NODATA + NS NODATA — the name exists (NOERROR), not_delegated is for NXDOMAIN only",
			steps: []apexStep{
				apexReply(dnsclient.RCodeNOERROR),
				apexReply(dnsclient.RCodeNOERROR)},
			wantStatus: ApexDelegatedNoAddress,
			wantStop:   false,
			wantCalls:  2,
			wantNS:     true,
		},
		{
			name: "NODATA + NS NXDOMAIN — a fresh denial, the domain does not exist",
			steps: []apexStep{
				apexReply(dnsclient.RCodeNOERROR),
				apexReply(dnsclient.RCodeNXDOMAIN)},
			wantStatus: ApexNotDelegated,
			wantStop:   true,
			wantCalls:  2,
			wantNS:     true,
		},
		{
			name: "NODATA + NS SERVFAIL — temp_error",
			steps: []apexStep{
				apexReply(dnsclient.RCodeNOERROR),
				apexReply(dnsclient.RCodeSERVFAIL)},
			wantStatus: ApexTempError,
			wantStop:   true,
			wantCalls:  2,
			wantNS:     true,
		},
		{
			name: "NODATA + NS network — temp_error",
			steps: []apexStep{
				apexReply(dnsclient.RCodeNOERROR),
				apexFail(apexTimeoutErr{})},
			wantStatus: ApexTempError,
			wantStop:   true,
			wantCalls:  2,
			wantNS:     true,
		},
		{
			name:       "SERVFAIL on A — temp_error, NS is not queried",
			steps:      []apexStep{apexReply(dnsclient.RCodeSERVFAIL)},
			wantStatus: ApexTempError,
			wantStop:   true,
			wantCalls:  1,
		},
		{
			name:       "REFUSED on A — the resolver talking about itself, temp_error",
			steps:      []apexStep{apexReply(dnsclient.RCodeREFUSED)},
			wantStatus: ApexTempError,
			wantStop:   true,
			wantCalls:  1,
		},
		{
			name:       "FORMERR on A — temp_error",
			steps:      []apexStep{apexReply(dnsclient.RCodeFORMERR)},
			wantStatus: ApexTempError,
			wantStop:   true,
			wantCalls:  1,
		},
		{
			name:       "NOTIMP on A — temp_error",
			steps:      []apexStep{apexReply(dnsclient.RCodeNOTIMP)},
			wantStatus: ApexTempError,
			wantStop:   true,
			wantCalls:  1,
		},
		{
			name:       "unknown RCODE 11 on A — temp_error",
			steps:      []apexStep{apexReply(dnsclient.RCode(11))},
			wantStatus: ApexTempError,
			wantStop:   true,
			wantCalls:  1,
		},
		{
			name:       "a timeout on A after the dnsclient retries — temp_error",
			steps:      []apexStep{apexFail(apexTimeoutErr{})},
			wantStatus: ApexTempError,
			wantStop:   true,
			wantCalls:  1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &apexFakeResolver{steps: tc.steps}
			res, err := CheckApex(context.Background(), fake, "example.com")
			if err != nil {
				t.Fatalf("CheckApex: an unexpected error: %v", err)
			}
			if res.Status != tc.wantStatus {
				t.Errorf("Status = %v, expected %v", res.Status, tc.wantStatus)
			}
			if got := res.Status.Stop(); got != tc.wantStop {
				t.Errorf("Stop = %v, expected %v", got, tc.wantStop)
			}
			if res.NSQueried != tc.wantNS {
				t.Errorf("NSQueried = %v, expected %v", res.NSQueried, tc.wantNS)
			}
			if len(fake.calls) != tc.wantCalls {
				t.Fatalf("%d resolver calls, expected %d (questions: %v)",
					len(fake.calls), tc.wantCalls, fake.calls)
			}
			want := tc.wantAddrs
			if len(res.Addresses) != len(want) {
				t.Fatalf("Addresses = %v, expected %v", res.Addresses, want)
			}
			for i := range want {
				if res.Addresses[i] != want[i] {
					t.Errorf("Addresses[%d] = %v, expected %v", i, res.Addresses[i], want[i])
				}
			}
		})
	}
}

// --- question format: FQDN and types -------------------------------------------

func TestCheckApexQuestionFormat(t *testing.T) {
	t.Run("a name with and without a dot — the same A FQDN question", func(t *testing.T) {
		for _, name := range []string{"example.com", "example.com."} {
			fake := &apexFakeResolver{steps: []apexStep{
				apexReply(dnsclient.RCodeNOERROR, apexARR("example.com.", "192.0.2.1"))}}
			if _, err := CheckApex(context.Background(), fake, name); err != nil {
				t.Fatalf("CheckApex(%q): %v", name, err)
			}
			if len(fake.calls) != 1 {
				t.Fatalf("CheckApex(%q): %d calls, expected 1", name, len(fake.calls))
			}
			q := fake.calls[0]
			if q.Name != "example.com." {
				t.Errorf("CheckApex(%q): the question name %q, expected the FQDN \"example.com.\"", name, q.Name)
			}
			if q.Type != dns.TypeA {
				t.Errorf("CheckApex(%q): the question type %v, expected A", name, q.Type)
			}
		}
	})
	t.Run("NODATA — a second NS question with the same FQDN", func(t *testing.T) {
		fake := &apexFakeResolver{steps: []apexStep{
			apexReply(dnsclient.RCodeNOERROR),
			apexReply(dnsclient.RCodeNOERROR, apexNSRR("example.com.", "ns1.example.com."))}}
		if _, err := CheckApex(context.Background(), fake, "example.com"); err != nil {
			t.Fatalf("CheckApex: %v", err)
		}
		if len(fake.calls) != 2 {
			t.Fatalf("%d calls, expected 2", len(fake.calls))
		}
		q := fake.calls[1]
		if q.Name != "example.com." {
			t.Errorf("the NS question name %q, expected the FQDN \"example.com.\"", q.Name)
		}
		if q.Type != dns.TypeNS {
			t.Errorf("the question type %v, expected NS", q.Type)
		}
	})
	t.Run("an IDN name in punycode — no re-encoding", func(t *testing.T) {
		fake := &apexFakeResolver{steps: []apexStep{
			apexReply(dnsclient.RCodeNOERROR, apexARR("xn--80adxhks.", "192.0.2.1"))}}
		if _, err := CheckApex(context.Background(), fake, "xn--80adxhks"); err != nil {
			t.Fatalf("CheckApex: %v", err)
		}
		if got := fake.calls[0].Name; got != "xn--80adxhks." {
			t.Errorf("the question name %q, expected a punycode FQDN", got)
		}
	})
}

// --- addresses: composition and order ------------------------------------------

func TestCheckApexAddresses(t *testing.T) {
	t.Run("only valid IPv4 from A, the answer order preserved", func(t *testing.T) {
		bad := &dns.A{Hdr: dns.RR_Header{Name: "example.com.", Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60}}
		fake := &apexFakeResolver{steps: []apexStep{
			apexReply(dnsclient.RCodeNOERROR,
				apexARR("example.com.", "192.0.2.2"),
				bad, // an A without an IP — junk, skipped
				apexAAAARR("example.com.", "2001:db8::1"), // not the record type we need
				apexARR("example.com.", "192.0.2.1"))}}
		res, err := CheckApex(context.Background(), fake, "example.com")
		if err != nil {
			t.Fatalf("CheckApex: %v", err)
		}
		want := apexAddrs("192.0.2.2", "192.0.2.1")
		if len(res.Addresses) != len(want) {
			t.Fatalf("Addresses = %v, expected %v", res.Addresses, want)
		}
		for i := range want {
			if res.Addresses[i] != want[i] {
				t.Errorf("Addresses[%d] = %v, expected %v", i, res.Addresses[i], want[i])
			}
		}
		if !res.Addresses[0].Is4() {
			t.Errorf("the apex address must be IPv4 (an A record), got %v", res.Addresses[0])
		}
	})
	t.Run("the no-address branches return an empty list", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			steps []apexStep
		}{
			{"NXDOMAIN", []apexStep{apexReply(dnsclient.RCodeNXDOMAIN)}},
			{"SERVFAIL", []apexStep{apexReply(dnsclient.RCodeSERVFAIL)}},
			{"NODATA + NS", []apexStep{apexReply(dnsclient.RCodeNOERROR), apexReply(dnsclient.RCodeNOERROR)}},
		} {
			fake := &apexFakeResolver{steps: tc.steps}
			res, err := CheckApex(context.Background(), fake, "example.com")
			if err != nil {
				t.Fatalf("%s: CheckApex: %v", tc.name, err)
			}
			if len(res.Addresses) != 0 {
				t.Errorf("%s: Addresses = %v, expected empty", tc.name, res.Addresses)
			}
		}
	})
}

// --- the ApexStatus contract: Stop and DomainStatus ----------------------------

func TestApexStatusStopAndDomainStatus(t *testing.T) {
	cases := []struct {
		status    ApexStatus
		wantStop  bool
		wantDom   model.Status
		wantDomOK bool
		wantStr   string
	}{
		{ApexDelegated, false, model.StatusNew, false, "delegated"},
		{ApexDelegatedNoAddress, false, model.StatusNew, false, "delegated_no_address"},
		{ApexNotDelegated, true, model.StatusNotDelegated, true, "not_delegated"},
		{ApexTempError, true, model.StatusTempError, true, "temp_error"},
	}
	for _, tc := range cases {
		if got := tc.status.Stop(); got != tc.wantStop {
			t.Errorf("%v.Stop() = %v, expected %v", tc.status, got, tc.wantStop)
		}
		dom, ok := tc.status.DomainStatus()
		if ok != tc.wantDomOK || dom != tc.wantDom {
			t.Errorf("%v.DomainStatus() = (%v, %v), expected (%v, %v)",
				tc.status, dom, ok, tc.wantDom, tc.wantDomOK)
		}
		if got := tc.status.String(); got != tc.wantStr {
			t.Errorf("%v.String() = %q, expected %q", tc.status, got, tc.wantStr)
		}
	}
	if got := ApexStatus(7).String(); got != "ApexStatus(7)" {
		t.Errorf("String() of an unknown code = %q", got)
	}
}

// --- context cancellation -------------------------------------------------------

func TestCheckApexContext(t *testing.T) {
	t.Run("a cancel before the query — an error, the resolver not called", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		fake := &apexFakeResolver{}
		res, err := CheckApex(ctx, fake, "example.com")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, expected context.Canceled", err)
		}
		if res != nil {
			t.Errorf("the result = %v, expected nil", res)
		}
		if len(fake.calls) != 0 {
			t.Errorf("%d resolver calls, expected 0", len(fake.calls))
		}
	})
	t.Run("a passed deadline — DeadlineExceeded", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 0)
		defer cancel()
		fake := &apexFakeResolver{}
		res, err := CheckApex(ctx, fake, "example.com")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, expected context.DeadlineExceeded", err)
		}
		if res != nil {
			t.Errorf("the result = %v, expected nil", res)
		}
	})
	t.Run("a cancel on the NS query — an error, not temp_error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fake := &apexFakeResolver{steps: []apexStep{
			apexReply(dnsclient.RCodeNOERROR), // A: NODATA
			apexFail(context.Canceled)}}       // NS: a cancellation during the query
		fake.onCall = func(n int) {
			if n == 2 {
				cancel()
			}
		}
		res, err := CheckApex(ctx, fake, "example.com")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, expected context.Canceled", err)
		}
		if res != nil {
			t.Errorf("the result = %v, expected nil", res)
		}
	})
	t.Run("an Exchange error without a ctx cancel — temp_error, not an error", func(t *testing.T) {
		// The resolver returned context.Canceled, but the caller's own
		// ctx is alive: the classification goes by the state of the
		// caller's context.
		fake := &apexFakeResolver{steps: []apexStep{apexFail(context.Canceled)}}
		res, err := CheckApex(context.Background(), fake, "example.com")
		if err != nil {
			t.Fatalf("CheckApex: %v", err)
		}
		if res.Status != ApexTempError {
			t.Errorf("Status = %v, expected ApexTempError", res.Status)
		}
	})
}

// --- an invalid name -------------------------------------------------------------

func TestCheckApexEmptyDomain(t *testing.T) {
	for _, name := range []string{"", "   ", "."} {
		fake := &apexFakeResolver{}
		res, err := CheckApex(context.Background(), fake, name)
		if err == nil {
			t.Errorf("CheckApex(%q): an error expected, got the result %v", name, res)
		}
		if res != nil {
			t.Errorf("CheckApex(%q): the result = %v, expected nil", name, res)
		}
		if len(fake.calls) != 0 {
			t.Errorf("CheckApex(%q): %d resolver calls, expected 0", name, len(fake.calls))
		}
	}
}

// Tests of the wildcard detection (T-038). Without a network: the
// resolver is a fake implementing dnsclient.Resolver; the RR constructors
// are in the style of the internal/dnsclient tests.

package dnsworker

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/miekg/dns"

	"crawler/internal/dnsclient"
	"crawler/internal/model"
)

// --- the fake resolver --------------------------------------------------------

// wildFakeResolver records the questions and answers with a function.
// The tests are single-threaded (DetectWildcard is synchronous) — no
// locks are needed.
type wildFakeResolver struct {
	calls  []dnsclient.Question
	answer func(q dnsclient.Question) (*dnsclient.Result, error)
}

// compile-time check: wildFakeResolver is a dnsclient.Resolver.
var _ dnsclient.Resolver = (*wildFakeResolver)(nil)

func (f *wildFakeResolver) Exchange(ctx context.Context, q dnsclient.Question) (*dnsclient.Result, error) {
	f.calls = append(f.calls, q)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return f.answer(q)
}

func wildA(name, ip string) dns.RR {
	return &dns.A{
		Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60},
		A:   net.ParseIP(ip).To4(),
	}
}

func wildAAAA(name, ip string) dns.RR {
	return &dns.AAAA{
		Hdr:  dns.RR_Header{Name: name, Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: 60},
		AAAA: net.ParseIP(ip),
	}
}

func wildCNAME(from, to string) dns.RR {
	return &dns.CNAME{
		Hdr:    dns.RR_Header{Name: from, Rrtype: dns.TypeCNAME, Class: dns.ClassINET, Ttl: 60},
		Target: to,
	}
}

// wildMakeResult — an Exchange outcome with a code and an answer section.
func wildMakeResult(rcode dnsclient.RCode, an ...dns.RR) *dnsclient.Result {
	return &dnsclient.Result{RCode: rcode, Answer: an}
}

// --- random label generation ---------------------------------------------------

func TestWildRandomLabel(t *testing.T) {
	seen := make(map[string]bool, 1000)
	for i := 0; i < 1000; i++ {
		label, err := wildRandomLabel()
		if err != nil {
			t.Fatalf("wildRandomLabel: %v", err)
		}
		if len(label) != wildLabelLength {
			t.Fatalf("the label length %d, expected %d (%q)", len(label), wildLabelLength, label)
		}
		for _, c := range label {
			if !strings.ContainsRune(wildLabelAlphabet, c) {
				t.Fatalf("the char %q outside the a-z0-9 alphabet: %q", c, label)
			}
		}
		// The punycode prefix is impossible (there is no hyphen in the
		// alphabet), but the invariant is cheap — it is checked
		// explicitly.
		if strings.HasPrefix(label, "xn--") {
			t.Fatalf("a label with a punycode prefix: %q", label)
		}
		if seen[label] {
			t.Fatalf("a repeated label %q", label)
		}
		seen[label] = true
	}
}

// --- DetectWildcard -------------------------------------------------------------

// TestDetectWildcardZoneWithWildcard — the random name receives A
// records: the zone is under a wildcard, the addresses are carried into
// the outcome, the label is stable within the call (the question name =
// the label + the domain).
func TestDetectWildcardZoneWithWildcard(t *testing.T) {
	f := &wildFakeResolver{answer: func(q dnsclient.Question) (*dnsclient.Result, error) {
		return wildMakeResult(dnsclient.RCodeNOERROR,
			wildA(q.Name, "192.0.2.10"),
			wildA(q.Name, "192.0.2.11")), nil
	}}

	info, err := DetectWildcard(context.Background(), f, "example.com")
	if err != nil {
		t.Fatalf("DetectWildcard: %v", err)
	}
	if !info.Detected {
		t.Fatal("the random name resolves — Detected must be true")
	}
	if len(info.Addrs) != 2 ||
		!info.Addrs[0].Equal(net.ParseIP("192.0.2.10")) ||
		!info.Addrs[1].Equal(net.ParseIP("192.0.2.11")) {
		t.Fatalf("the wildcard answer addresses: %v", info.Addrs)
	}
	if len(f.calls) != 1 {
		t.Fatalf("detect queries: %d, expected 1", len(f.calls))
	}
	q := f.calls[0]
	if q.Type != dns.TypeA {
		t.Fatalf("the detect question type %v, expected A (spec 5.4 item 3)", dns.TypeToString[q.Type])
	}
	// Fqdn (the trailing dot) is the dnsclient transport's business; the
	// fake sees the name as DetectWildcard passed it.
	if want := info.Label + ".example.com"; q.Name != want {
		t.Fatalf("the question %q does not match the declared label (expected %q)", q.Name, want)
	}
}

// TestDetectWildcardZoneWithoutWildcard — NXDOMAIN for the random name:
// no wildcard in the zone, all subdomains genuine (the detection
// negation).
func TestDetectWildcardZoneWithoutWildcard(t *testing.T) {
	f := &wildFakeResolver{answer: func(q dnsclient.Question) (*dnsclient.Result, error) {
		return wildMakeResult(dnsclient.RCodeNXDOMAIN), nil
	}}

	info, err := DetectWildcard(context.Background(), f, "example.com")
	if err != nil {
		t.Fatalf("NXDOMAIN — an authoritative denial, not an error: %v", err)
	}
	if info.Detected {
		t.Fatal("NXDOMAIN on a random name: Detected must be false")
	}
	if info.Addrs != nil {
		t.Fatalf("a denial must carry no addresses: %v", info.Addrs)
	}
	if info.Label == "" {
		t.Fatal("the label is recorded for a denial too (diagnostics)")
	}
}

// TestDetectWildcardNODATA — NOERROR without A records: a wildcard
// without addresses (or NODATA) — nothing to compare, the outcome as
// without a wildcard.
func TestDetectWildcardNODATA(t *testing.T) {
	f := &wildFakeResolver{answer: func(q dnsclient.Question) (*dnsclient.Result, error) {
		return wildMakeResult(dnsclient.RCodeNOERROR), nil
	}}

	info, err := DetectWildcard(context.Background(), f, "example.com")
	if err != nil {
		t.Fatalf("NOERROR without A — not an error: %v", err)
	}
	if info.Detected || info.Addrs != nil {
		t.Fatalf("NODATA: Detected=%v Addrs=%v, expected false/nil", info.Detected, info.Addrs)
	}
}

// TestDetectWildcardCNAMEChain — a wildcard wrapped in a CNAME: the
// terminal addresses are taken (the target's A); the CNAME record itself
// does not enter the set.
func TestDetectWildcardCNAMEChain(t *testing.T) {
	f := &wildFakeResolver{answer: func(q dnsclient.Question) (*dnsclient.Result, error) {
		return wildMakeResult(dnsclient.RCodeNOERROR,
			wildCNAME(q.Name, "parking.hosting.example."),
			wildA("parking.hosting.example.", "203.0.113.5")), nil
	}}

	info, err := DetectWildcard(context.Background(), f, "example.com")
	if err != nil {
		t.Fatalf("DetectWildcard: %v", err)
	}
	if !info.Detected {
		t.Fatal("a CNAME → A resolves — the zone is under a wildcard")
	}
	if len(info.Addrs) != 1 || !info.Addrs[0].Equal(net.ParseIP("203.0.113.5")) {
		t.Fatalf("only the chain's final address expected, got %v", info.Addrs)
	}
}

// TestDetectWildcardCNAMEWithoutA — a CNAME to a name without A records:
// the wildcard gives no addresses, the detection is negative for
// comparison purposes (without an error).
func TestDetectWildcardCNAMEWithoutA(t *testing.T) {
	f := &wildFakeResolver{answer: func(q dnsclient.Question) (*dnsclient.Result, error) {
		return wildMakeResult(dnsclient.RCodeNOERROR,
			wildCNAME(q.Name, "addressless.example.")), nil
	}}

	info, err := DetectWildcard(context.Background(), f, "example.com")
	if err != nil {
		t.Fatalf("a CNAME without A — not an error: %v", err)
	}
	if info.Detected || info.Addrs != nil {
		t.Fatalf("a CNAME without A: Detected=%v Addrs=%v, expected false/nil", info.Detected, info.Addrs)
	}
}

// TestDetectWildcardServFail — a non-authoritative answer: the detection
// failed (an error, not a "successful negation"); it does not drop the
// domain check — the consumer's "continue without filtering" policy is
// documented.
func TestDetectWildcardServFail(t *testing.T) {
	f := &wildFakeResolver{answer: func(q dnsclient.Question) (*dnsclient.Result, error) {
		return wildMakeResult(dnsclient.RCodeSERVFAIL), nil
	}}

	info, err := DetectWildcard(context.Background(), f, "example.com")
	if err == nil {
		t.Fatal("SERVFAIL — a non-authoritative answer, a detect error expected")
	}
	if !strings.Contains(err.Error(), "wildcard detect") || !strings.Contains(err.Error(), "SERVFAIL") {
		t.Fatalf("a message without context: %v", err)
	}
	if info.Detected {
		t.Fatal("a failed detect must not mark the zone wildcard")
	}
}

// TestDetectWildcardNetworkError — a network/transport error is
// propagated with the cause preserved; "the detection failed", not a
// panic.
func TestDetectWildcardNetworkError(t *testing.T) {
	sentinel := errors.New("the resolver unavailable")
	f := &wildFakeResolver{answer: func(q dnsclient.Question) (*dnsclient.Result, error) {
		return nil, sentinel
	}}

	info, err := DetectWildcard(context.Background(), f, "example.com")
	if !errors.Is(err, sentinel) {
		t.Fatalf("a wrapping of the cause expected, got: %v", err)
	}
	if info.Detected {
		t.Fatal("an exchange error — Detected must be false")
	}
}

// TestDetectWildcardCtxCancelled — a context cancellation: the
// cancellation error is propagated from the resolver without a
// substitution (the ctx check before the transport is done by
// dnsclient.Client — the detection does not duplicate it).
func TestDetectWildcardCtxCancelled(t *testing.T) {
	f := &wildFakeResolver{answer: func(q dnsclient.Question) (*dnsclient.Result, error) {
		t.Error("the answer must not be queried with a canceled ctx")
		return nil, errors.New("unreachable")
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	info, err := DetectWildcard(ctx, f, "example.com")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("context.Canceled expected, got %v", err)
	}
	if info.Detected {
		t.Fatal("a cancellation is not \"a wildcard-free zone\", Detected must be false")
	}
}

// TestDetectWildcardEmptyDomain — an empty domain name: an error before
// the label generation and before the exchange.
func TestDetectWildcardEmptyDomain(t *testing.T) {
	f := &wildFakeResolver{answer: func(q dnsclient.Question) (*dnsclient.Result, error) {
		t.Error("no exchange must happen for an empty domain")
		return nil, errors.New("unreachable")
	}}

	for _, domain := range []string{"", "  "} {
		if _, err := DetectWildcard(context.Background(), f, domain); err == nil ||
			!strings.Contains(err.Error(), "empty domain name") {
			t.Fatalf("domain %q: an \"empty domain name\" error expected, got %v", domain, err)
		}
	}
	if len(f.calls) != 0 {
		t.Fatalf("exchanges for the empty domain: %d", len(f.calls))
	}
}

// TestDetectWildcardLabelUniqueAndStablePerCall — two detections give
// different labels (uniqueness), and each label is really the one that
// went into the question (stability within a call).
func TestDetectWildcardLabelUniqueAndStablePerCall(t *testing.T) {
	f := &wildFakeResolver{answer: func(q dnsclient.Question) (*dnsclient.Result, error) {
		return wildMakeResult(dnsclient.RCodeNOERROR, wildA(q.Name, "192.0.2.10")), nil
	}}

	labels := make(map[string]bool, 2)
	for i := 0; i < 2; i++ {
		info, err := DetectWildcard(context.Background(), f, "example.com")
		if err != nil {
			t.Fatalf("detect %d: %v", i, err)
		}
		if !info.Detected {
			t.Fatalf("detect %d: a wildcard expected", i)
		}
		if labels[info.Label] {
			t.Fatalf("the label repeated between calls: %q", info.Label)
		}
		labels[info.Label] = true
		if want := info.Label + ".example.com"; f.calls[i].Name != want {
			t.Fatalf("call %d: the question %q does not match the label (expected %q)", i, f.calls[i].Name, want)
		}
	}
	if len(f.calls) != 2 {
		t.Fatalf("exchanges: %d, expected 2", len(f.calls))
	}
}

// --- MarkWildcardSubdomains -------------------------------------------------------

// TestMarkWildcardSubdomains — the marking table: a subset/equality of
// the addresses → wildcard; the apex and www — never; an address outside
// the set or empty addresses — a genuine subdomain.
func TestMarkWildcardSubdomains(t *testing.T) {
	wild := []net.IP{net.ParseIP("192.0.2.10"), net.ParseIP("192.0.2.11")}
	subs := []WildSubdomain{
		{ID: 0, Label: "", Addrs: wild}, // the apex — the spec 5.4 item 3 exception
		{ID: 1, Label: "www", Addrs: wild},
		{ID: 2, Label: "admin", Addrs: []net.IP{net.ParseIP("192.0.2.10")}},                            // a subset
		{ID: 3, Label: "crm", Addrs: []net.IP{net.ParseIP("192.0.2.11"), net.ParseIP("192.0.2.10")}},   // an equality
		{ID: 4, Label: "api", Addrs: []net.IP{net.ParseIP("192.0.2.10"), net.ParseIP("198.51.100.7")}}, // a partial match
		{ID: 5, Label: "app", Addrs: []net.IP{net.ParseIP("198.51.100.7")}},                            // everything outside the set
		{ID: 6, Label: "shop", Addrs: nil},                                                             // no addresses
	}

	res, err := MarkWildcardSubdomains(wild, subs)
	if err != nil {
		t.Fatalf("MarkWildcardSubdomains: %v", err)
	}
	want := map[int]bool{2: true, 3: true}
	for _, sub := range subs {
		if got := res.IsWildcard(sub.ID); got != want[sub.ID] {
			t.Errorf("subdomain %d (%q): IsWildcard=%v, expected %v",
				sub.ID, sub.Label, got, want[sub.ID])
		}
	}
	if ids := res.IDs(); len(ids) != 2 || ids[0] != 2 || ids[1] != 3 {
		t.Fatalf("IDs(): %v, expected [2 3]", ids)
	}
	if res.Mask != model.DNSMask(0b1100) {
		t.Fatalf("the mask: %b, expected 1100", res.Mask)
	}
}

// TestMarkWildcardSubdomainsNoWildcard — an empty set of wildcard
// addresses (the detection is negative/failed): nobody is marked.
func TestMarkWildcardSubdomainsNoWildcard(t *testing.T) {
	subs := []WildSubdomain{
		{ID: 2, Label: "admin", Addrs: []net.IP{net.ParseIP("192.0.2.10")}},
		{ID: 3, Label: "crm", Addrs: []net.IP{net.ParseIP("192.0.2.10")}},
	}
	res, err := MarkWildcardSubdomains(nil, subs)
	if err != nil {
		t.Fatalf("MarkWildcardSubdomains: %v", err)
	}
	if !res.Mask.IsEmpty() || len(res.IDs()) != 0 {
		t.Fatalf("without wildcard addresses the mask must be empty: %v", res.Mask.IDs())
	}

	// An empty subdomain input — also without an error.
	res, err = MarkWildcardSubdomains([]net.IP{net.ParseIP("192.0.2.10")}, nil)
	if err != nil || !res.Mask.IsEmpty() {
		t.Fatalf("empty subdomain input: res=%v err=%v", res, err)
	}
}

// TestMarkWildcardSubdomainsIPForms — one point in the 4- and 16-octet
// forms is one address (net.IP.Equal); an IPv6 address outside the
// wildcard set (of A records) leaves the subdomain genuine.
func TestMarkWildcardSubdomainsIPForms(t *testing.T) {
	wild := []net.IP{net.ParseIP("192.0.2.10")} // an A record — the 4-octet form

	mapped, err := MarkWildcardSubdomains(wild, []WildSubdomain{
		{ID: 2, Label: "admin", Addrs: []net.IP{net.ParseIP("192.0.2.10").To16()}},
	})
	if err != nil {
		t.Fatalf("MarkWildcardSubdomains: %v", err)
	}
	if !mapped.IsWildcard(2) {
		t.Fatal("the 16-octet form of the same address must match the 4-octet one")
	}

	// An IPv6 address is not inside the wildcard set (the detection is an
	// A query, spec 5.4 item 3): a dual stack is not marked and goes to
	// HTTP — the safe direction (an extra check instead of a missed
	// site).
	dual, err := MarkWildcardSubdomains(wild, []WildSubdomain{
		{ID: 3, Label: "crm", Addrs: []net.IP{net.ParseIP("192.0.2.10"), net.ParseIP("2001:db8::1")}},
	})
	if err != nil {
		t.Fatalf("MarkWildcardSubdomains: %v", err)
	}
	if dual.IsWildcard(3) {
		t.Fatal("an address outside the wildcard set — the subdomain is honest")
	}
}

// TestMarkWildcardSubdomainsInvalidID — a number outside 0..62 (the mask
// model): an error, a partial mask is not returned.
func TestMarkWildcardSubdomainsInvalidID(t *testing.T) {
	wild := []net.IP{net.ParseIP("192.0.2.10")}
	for _, id := range []int{-1, 63, 100} {
		res, err := MarkWildcardSubdomains(wild, []WildSubdomain{
			{ID: 2, Label: "admin", Addrs: wild}, // the valid one goes first
			{ID: id, Label: "broken", Addrs: wild},
		})
		if err == nil {
			t.Fatalf("id %d: a mask validation error expected", id)
		}
		if !res.Mask.IsEmpty() || len(res.IDs()) != 0 {
			t.Fatalf("id %d: a partial mask %v instead of empty", id, res.Mask.IDs())
		}
		if !strings.Contains(err.Error(), "wildcard marking") {
			t.Fatalf("id %d: a message without context: %v", id, err)
		}
	}
}

// TestWildScenarioEndToEnd — detection + marking together on one
// scenario (a zone under a wildcard, a CNAME wrap): half of the
// subdomains are the stub, the apex/www and the subdomain with its own
// address go to HTTP.
func TestWildScenarioEndToEnd(t *testing.T) {
	f := &wildFakeResolver{answer: func(q dnsclient.Question) (*dnsclient.Result, error) {
		return wildMakeResult(dnsclient.RCodeNOERROR,
			wildCNAME(q.Name, "parking.hosting.example."),
			wildA("parking.hosting.example.", "203.0.113.5"),
			wildAAAA("parking.hosting.example.", "2001:db8::53")), nil
	}}

	info, err := DetectWildcard(context.Background(), f, "example.com")
	if err != nil {
		t.Fatalf("DetectWildcard: %v", err)
	}
	parking := net.ParseIP("203.0.113.5")
	res, err := MarkWildcardSubdomains(info.Addrs, []WildSubdomain{
		{ID: 0, Label: "", Addrs: []net.IP{parking}},
		{ID: 1, Label: "www", Addrs: []net.IP{parking}},
		{ID: 4, Label: "api", Addrs: []net.IP{parking}},
		{ID: 5, Label: "app", Addrs: []net.IP{parking}},
		{ID: 2, Label: "admin", Addrs: []net.IP{net.ParseIP("198.51.100.7")}},
	})
	if err != nil {
		t.Fatalf("MarkWildcardSubdomains: %v", err)
	}
	for id, want := range map[int]bool{0: false, 1: false, 4: true, 5: true, 2: false} {
		if res.IsWildcard(id) != want {
			t.Errorf("subdomain %d: wildcard=%v, expected %v", id, res.IsWildcard(id), want)
		}
	}
	// Neither the AAAA addresses of the chain nor the CNAME enter the
	// mask — only the A records.
	if ids := res.IDs(); len(ids) != 2 || ids[0] != 4 || ids[1] != 5 {
		t.Fatalf("IDs(): %v, expected [4 5]", ids)
	}
}

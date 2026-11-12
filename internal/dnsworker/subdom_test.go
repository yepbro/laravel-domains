// Unit tests of the subdomain check (T-037, spec 5.4 item 2) on a fake
// dnsclient.Resolver without a network (the style of
// internal/dnsclient/client_test.go): A records, CNAME chains in one
// answer and across exchanges, loops and the depth limit,
// CNAME→NXDOMAIN, the AAAA flag, the DNSMask aggregation, the isolation
// of subdomain errors, input validation and a ctx cancellation. The
// identifiers carry the subdom prefix against the parallel apex*/wild*
// files (T-036/T-038).

package dnsworker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"testing"

	"github.com/miekg/dns"

	"crawler/internal/dnsclient"
	"crawler/internal/model"
)

// --- the fake Resolver --------------------------------------------------------

// subdomFakeResolver — a dnsclient.Resolver without a network: the
// handler answers the questions, all calls are recorded (the contract on
// the number and order of the exchanges is part of the checks: there
// must be no extra CNAME questions and no repeated chain traversals).
type subdomFakeResolver struct {
	calls   []dnsclient.Question
	handler func(q dnsclient.Question) (*dnsclient.Result, error)
}

// compile-time check: the fake is a Resolver.
var _ dnsclient.Resolver = (*subdomFakeResolver)(nil)

func (f *subdomFakeResolver) Exchange(ctx context.Context, q dnsclient.Question) (*dnsclient.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.calls = append(f.calls, q)
	return f.handler(q)
}

// subdomZone — a declarative zone for the fake: the answer section, the
// RCODE and transport errors by the "fqdn|type" key (subdomKey). A
// missing key — NOERROR with an empty answer (NODATA).
type subdomZone struct {
	answers map[string][]dns.RR
	rcodes  map[string]dnsclient.RCode
	errs    map[string]error
}

// subdomKey — the zone key: the canonical name and the record type.
func subdomKey(name string, t uint16) string {
	return subdomCanon(name) + "|" + dns.TypeToString[t]
}

// subdomResolver assembles a Resolver from the zone.
func (z subdomZone) resolver() *subdomFakeResolver {
	return &subdomFakeResolver{handler: func(q dnsclient.Question) (*dnsclient.Result, error) {
		key := subdomKey(q.Name, q.Type)
		if err, ok := z.errs[key]; ok {
			return nil, err
		}
		rcode := dnsclient.RCodeNOERROR
		if r, ok := z.rcodes[key]; ok {
			rcode = r
		}
		return &dnsclient.Result{RCode: rcode, Answer: z.answers[key]}, nil
	}}
}

// --- record constructors and helper checks ------------------------------------

func subdomA(name, ip string) dns.RR {
	return &dns.A{
		Hdr: dns.RR_Header{Name: name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60},
		A:   net.ParseIP(ip).To4(),
	}
}

func subdomAAAA(name, ip string) dns.RR {
	return &dns.AAAA{
		Hdr:  dns.RR_Header{Name: name, Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: 60},
		AAAA: net.ParseIP(ip),
	}
}

func subdomCNAME(from, to string) dns.RR {
	return &dns.CNAME{
		Hdr:    dns.RR_Header{Name: from, Rrtype: dns.TypeCNAME, Class: dns.ClassINET, Ttl: 60},
		Target: to,
	}
}

// subdomIPs — the string representation of the addresses for error
// messages.
func subdomIPs(addrs []netip.Addr) []string {
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, a.String())
	}
	return out
}

// subdomEqualIPs compares the addresses as strings in order.
func subdomEqualIPs(t *testing.T, got []netip.Addr, want ...string) {
	t.Helper()
	if gotS, wantS := subdomIPs(got), want; len(gotS) != len(wantS) {
		t.Fatalf("адреса: получено %v, ожидалось %v", gotS, wantS)
	} else {
		for i := range gotS {
			if gotS[i] != wantS[i] {
				t.Fatalf("адрес №%d: получено %s, ожидалось %s", i+1, gotS[i], wantS[i])
			}
		}
	}
}

// subdomCallCount counts the exchanges with the name and type.
func subdomCallCount(f *subdomFakeResolver, name string, t uint16) int {
	n := 0
	for _, q := range f.calls {
		if subdomCanon(q.Name) == subdomCanon(name) && q.Type == t {
			n++
		}
	}
	return n
}

// subdomAssertNoCNAMEQuestions — the project contract: separate
// CNAME-type questions are not issued, CNAME records arrive in the
// A/AAAA answers (RFC 1035 §3.3.1; the subdomCheckOne comment).
func subdomAssertNoCNAMEQuestions(t *testing.T, f *subdomFakeResolver) {
	t.Helper()
	for _, q := range f.calls {
		if q.Type == dns.TypeCNAME {
			t.Fatalf("выдан отдельный вопрос CNAME %q — записи CNAME приходят в ответах A/AAAA", q.Name)
		}
	}
}

// subdomSubs — an input of label rows with sequential ids.
func subdomSubs(labels ...string) []model.Subdomain {
	subs := make([]model.Subdomain, 0, len(labels))
	for i, l := range labels {
		subs = append(subs, model.Subdomain{ID: int16(i), Label: l, Enabled: true})
	}
	return subs
}

// --- A records directly ---------------------------------------------------------

func TestSubdomDirectA(t *testing.T) {
	z := subdomZone{answers: map[string][]dns.RR{
		subdomKey("example.com", dns.TypeA): {
			subdomA("example.com.", "192.0.2.10"),
			subdomA("example.com.", "192.0.2.10"), // a duplicate record — one address
			subdomA("example.com.", "192.0.2.11"),
		},
		subdomKey("www.example.com", dns.TypeA): {subdomA("www.example.com.", "192.0.2.20")},
	}}
	f := z.resolver()

	rep, err := CheckSubdomains(context.Background(), f, "example.com", subdomSubs("", "www"), true)
	if err != nil {
		t.Fatalf("CheckSubdomains: %v", err)
	}
	if len(rep.Results) != 2 {
		t.Fatalf("результатов %d, ожидалось 2 (апекс + www)", len(rep.Results))
	}

	apex := rep.Results[0]
	if apex.Label != "" || apex.ID != 0 {
		t.Fatalf("апекс: label=%q id=%d, ожидалось \"\" и 0", apex.Label, apex.ID)
	}
	subdomEqualIPs(t, apex.A, "192.0.2.10", "192.0.2.11")
	subdomEqualIPs(t, apex.AAAA) // the AAAA question was asked, the answer is empty (NODATA)
	if apex.HasAddrs() != true {
		t.Fatal("апекс: HasAddrs = false при найденных A")
	}
	if apex.Chain != nil || apex.CNAME() != "" || apex.Target() != "" {
		t.Fatalf("апекс: цепочка %#v у имени без CNAME", apex.Chain)
	}
	if apex.RCode != dnsclient.RCodeNOERROR || apex.Err != nil {
		t.Fatalf("апекс: RCode=%v Err=%v", apex.RCode, apex.Err)
	}

	www := rep.Results[1]
	subdomEqualIPs(t, www.A, "192.0.2.20")

	// The apex is queried by the domain name itself (not ".example.com"),
	// the AAAA questions are asked of both, and there are no CNAME
	// questions.
	if q := f.calls[0]; subdomCanon(q.Name) != "example.com." || q.Type != dns.TypeA {
		t.Fatalf("первый вопрос %q %v, ожидался A example.com.", q.Name, dns.TypeToString[q.Type])
	}
	if n := subdomCallCount(f, "example.com", dns.TypeAAAA); n != 1 {
		t.Fatalf("вопросов AAAA апексу: %d, ожидался 1", n)
	}
	if n := subdomCallCount(f, "www.example.com", dns.TypeAAAA); n != 1 {
		t.Fatalf("вопросов AAAA www: %d, ожидался 1", n)
	}
	subdomAssertNoCNAMEQuestions(t, f)

	// The mask: both subdomains have addresses (bits 0 and 1).
	if want := model.DNSMask(0b11); rep.Mask != want {
		t.Fatalf("маска %v (%05b), ожидалось %v", rep.Mask, int64(rep.Mask), want)
	}
}

// --- CNAME chains ---------------------------------------------------------------

func TestSubdomCNAMEOneHopInAnswer(t *testing.T) {
	// The recursing resolver attaches the chain to the answer in full:
	// the CNAME and the target's A are in one answer, a second A
	// exchange is not needed.
	z := subdomZone{answers: map[string][]dns.RR{
		subdomKey("www.example.com", dns.TypeA): {
			subdomCNAME("www.example.com.", "example.com."),
			subdomA("example.com.", "192.0.2.1"),
		},
		subdomKey("example.com", dns.TypeAAAA): {subdomAAAA("example.com.", "2001:db8::1")},
	}}
	f := z.resolver()

	rep, err := CheckSubdomains(context.Background(), f, "example.com", subdomSubs("www"), true)
	if err != nil {
		t.Fatalf("CheckSubdomains: %v", err)
	}
	r := rep.Results[0]
	if len(r.Chain) != 1 || r.Chain[0] != "example.com." {
		t.Fatalf("цепочка %v, ожидалась [example.com.]", r.Chain)
	}
	if r.CNAME() != "example.com." || r.Target() != "example.com." {
		t.Fatalf("CNAME=%q Target=%q, ожидалась цель example.com.", r.CNAME(), r.Target())
	}
	subdomEqualIPs(t, r.A, "192.0.2.1")
	subdomEqualIPs(t, r.AAAA, "2001:db8::1") // AAAA — by the terminal name
	if !r.HasAddrs() || !rep.Mask.Test(0) {
		t.Fatal("www с адресами: бит маски не поставлен")
	}
	if n := subdomCallCount(f, "www.example.com", dns.TypeA); n != 1 {
		t.Fatalf("A-обменов с www: %d, ожидался 1 (цепочка в одном ответе)", n)
	}
	if n := subdomCallCount(f, "example.com", dns.TypeA); n != 0 {
		t.Fatalf("повторный A-обмен с целью: %d, ожидался 0", n)
	}
	if n := subdomCallCount(f, "example.com", dns.TypeAAAA); n != 1 {
		t.Fatalf("AAAA-обменов с терминалом: %d, ожидался 1", n)
	}
	subdomAssertNoCNAMEQuestions(t, f)
}

func TestSubdomCNAMEFollowUpExchanges(t *testing.T) {
	// The answer of the first exchange contains only the CNAME (the
	// target is outside the answer's coverage) — the terminal is chased
	// by its own exchange.
	z := subdomZone{answers: map[string][]dns.RR{
		subdomKey("www.example.com", dns.TypeA): {
			subdomCNAME("www.example.com.", "cdn.example.net."),
		},
		subdomKey("cdn.example.net", dns.TypeA): {subdomA("cdn.example.net.", "203.0.113.5")},
	}}
	f := z.resolver()

	rep, err := CheckSubdomains(context.Background(), f, "example.com", subdomSubs("www"), false)
	if err != nil {
		t.Fatalf("CheckSubdomains: %v", err)
	}
	r := rep.Results[0]
	if len(r.Chain) != 1 || r.Chain[0] != "cdn.example.net." {
		t.Fatalf("цепочка %v, ожидалась [cdn.example.net.]", r.Chain)
	}
	subdomEqualIPs(t, r.A, "203.0.113.5")
	if r.RCode != dnsclient.RCodeNOERROR || r.Err != nil {
		t.Fatalf("RCode=%v Err=%v", r.RCode, r.Err)
	}
	if n := subdomCallCount(f, "cdn.example.net", dns.TypeA); n != 1 {
		t.Fatalf("A-обменов с терминалом: %d, ожидался 1", n)
	}
	if !rep.Mask.Test(0) {
		t.Fatal("бит маски не поставлен")
	}
}

func TestSubdomLongChainInOneAnswer(t *testing.T) {
	// The chain www→c1→…→c5 with an A at c5 entirely in one answer: it
	// unwinds without a single extra exchange.
	chain := []string{"www.example.com.", "c1.example.net.", "c2.example.net.",
		"c3.example.net.", "c4.example.net.", "c5.example.net."}
	answer := []dns.RR{subdomCNAME(chain[0], chain[1])}
	for i := 1; i < len(chain)-1; i++ {
		answer = append(answer, subdomCNAME(chain[i], chain[i+1]))
	}
	answer = append(answer, subdomA(chain[len(chain)-1], "203.0.113.9"))
	z := subdomZone{answers: map[string][]dns.RR{
		subdomKey("www.example.com", dns.TypeA): answer,
	}}
	f := z.resolver()

	rep, err := CheckSubdomains(context.Background(), f, "example.com", subdomSubs("www"), false)
	if err != nil {
		t.Fatalf("CheckSubdomains: %v", err)
	}
	r := rep.Results[0]
	if len(r.Chain) != 5 {
		t.Fatalf("длина цепочки %d, ожидалась 5 (звенья c1..c5): %v", len(r.Chain), r.Chain)
	}
	if r.CNAME() != "c1.example.net." || r.Target() != "c5.example.net." {
		t.Fatalf("CNAME=%q Target=%q", r.CNAME(), r.Target())
	}
	subdomEqualIPs(t, r.A, "203.0.113.9")
	if len(f.calls) != 1 {
		t.Fatalf("обменов %d, ожидался 1 (цепочка в одном ответе)", len(f.calls))
	}
}

func TestSubdomCNAMELoop(t *testing.T) {
	// A→B→A: the loop is visible via the set of visited names earlier
	// than the depth limit; no addresses, the bit is not set, AAAA is
	// not asked.
	z := subdomZone{answers: map[string][]dns.RR{
		subdomKey("www.example.com", dns.TypeA): {subdomCNAME("www.example.com.", "a.example.com.")},
		subdomKey("a.example.com", dns.TypeA):   {subdomCNAME("a.example.com.", "www.example.com.")},
	}}
	f := z.resolver()

	rep, err := CheckSubdomains(context.Background(), f, "example.com", subdomSubs("www"), true)
	if err != nil {
		t.Fatalf("CheckSubdomains: %v", err)
	}
	r := rep.Results[0]
	if !errors.Is(r.Err, ErrSubdomCNAMELoop) {
		t.Fatalf("Err %v, ожидался ErrSubdomCNAMELoop", r.Err)
	}
	if r.HasAddrs() || len(r.A) != 0 {
		t.Fatal("цикл: адресов быть не должно")
	}
	if len(r.Chain) != 1 || r.Chain[0] != "a.example.com." {
		t.Fatalf("цепочка %v, ожидалось единственное звено a.example.com.", r.Chain)
	}
	if rep.Mask.Test(0) {
		t.Fatal("цикл: бит маски поставлен")
	}
	if len(f.calls) != 2 {
		t.Fatalf("обменов %d, ожидалось 2 (www и a), AAAA не спрашивается", len(f.calls))
	}
}

func TestSubdomCNAMEDepthExceeded(t *testing.T) {
	// A ladder of unique names longer than the limit: it breaks off with
	// ErrSubdomCNAMEDepth after subdomMaxChain hops.
	const rungs = subdomMaxChain + 5
	z := subdomZone{answers: map[string][]dns.RR{}}
	for i := 0; i < rungs; i++ {
		from := fmt.Sprintf("h%d.example.com.", i)
		to := fmt.Sprintf("h%d.example.com.", i+1)
		z.answers[subdomKey(from, dns.TypeA)] = []dns.RR{subdomCNAME(from, to)}
	}
	f := z.resolver()

	rep, err := CheckSubdomains(context.Background(), f, "example.com",
		[]model.Subdomain{{ID: 0, Label: "h0", Enabled: true}}, false)
	if err != nil {
		t.Fatalf("CheckSubdomains: %v", err)
	}
	r := rep.Results[0]
	if !errors.Is(r.Err, ErrSubdomCNAMEDepth) {
		t.Fatalf("Err %v, ожидался ErrSubdomCNAMEDepth", r.Err)
	}
	if len(r.Chain) != subdomMaxChain {
		t.Fatalf("длина цепочки %d, ожидался предел %d", len(r.Chain), subdomMaxChain)
	}
	if r.HasAddrs() || rep.Mask.Test(0) {
		t.Fatal("превышение глубины: адресов и бита быть не должно")
	}
}

func TestSubdomCNAMETargetNXDOMAIN(t *testing.T) {
	// A CNAME to an external name that does not exist (RFC 8020): the
	// alias subdomain exists, there are no addresses — no Err, the
	// NXDOMAIN sign is in RCode, the bit is not set, AAAA is not asked
	// (NXDOMAIN is unconditional for all types). For the domain status —
	// a contribution to "no addresses" (spec 5.4 item 5), not temp_error.
	z := subdomZone{
		answers: map[string][]dns.RR{
			subdomKey("www.example.com", dns.TypeA): {
				subdomCNAME("www.example.com.", "gone.example.net."),
			},
		},
		rcodes: map[string]dnsclient.RCode{
			subdomKey("gone.example.net", dns.TypeA): dnsclient.RCodeNXDOMAIN,
		},
	}
	f := z.resolver()

	rep, err := CheckSubdomains(context.Background(), f, "example.com", subdomSubs("www"), true)
	if err != nil {
		t.Fatalf("CheckSubdomains: %v", err)
	}
	r := rep.Results[0]
	if r.Err != nil {
		t.Fatalf("CNAME→NXDOMAIN — не ошибка субдомена, получен %v", r.Err)
	}
	if r.RCode != dnsclient.RCodeNXDOMAIN {
		t.Fatalf("RCode %v, ожидался NXDOMAIN", r.RCode)
	}
	if r.HasAddrs() || len(r.A) != 0 {
		t.Fatal("цели нет — адресов быть не должно")
	}
	if len(r.Chain) != 1 || r.Chain[0] != "gone.example.net." {
		t.Fatalf("цепочка %v, ожидалась [gone.example.net.]", r.Chain)
	}
	if rep.Mask.Test(0) {
		t.Fatal("бит маски поставлен у субдомена без адресов")
	}
	if n := subdomCallCount(f, "gone.example.net", dns.TypeAAAA); n != 0 {
		t.Fatalf("AAAA после NXDOMAIN: %d вопросов, ожидалось 0", n)
	}
}

// --- the AAAA flag ---------------------------------------------------------------

func TestSubdomAAAAFlag(t *testing.T) {
	// Off — no AAAA questions at all, the addresses are A only.
	z := subdomZone{answers: map[string][]dns.RR{
		subdomKey("example.com", dns.TypeA):    {subdomA("example.com.", "192.0.2.1")},
		subdomKey("example.com", dns.TypeAAAA): {subdomAAAA("example.com.", "2001:db8::1")},
	}}
	f := z.resolver()
	rep, err := CheckSubdomains(context.Background(), f, "example.com", subdomSubs(""), false)
	if err != nil {
		t.Fatalf("CheckSubdomains: %v", err)
	}
	if n := subdomCallCount(f, "example.com", dns.TypeAAAA); n != 0 {
		t.Fatalf("askAAAA=false: вопросов AAAA %d, ожидалось 0", n)
	}
	subdomEqualIPs(t, rep.Results[0].AAAA)
	if !rep.Mask.Test(0) {
		t.Fatal("бит по A-адресам не поставлен")
	}

	// On, and the name only lives over IPv6: A — NODATA, AAAA gives the
	// addresses, the subdomain counts as resolving.
	z2 := subdomZone{answers: map[string][]dns.RR{
		subdomKey("v6.example.com", dns.TypeAAAA): {subdomAAAA("v6.example.com.", "2001:db8::3")},
	}}
	f2 := z2.resolver()
	rep2, err := CheckSubdomains(context.Background(), f2, "example.com", subdomSubs("v6"), true)
	if err != nil {
		t.Fatalf("CheckSubdomains (v6-only): %v", err)
	}
	r := rep2.Results[0]
	subdomEqualIPs(t, r.A)
	subdomEqualIPs(t, r.AAAA, "2001:db8::3")
	if !r.HasAddrs() || !rep2.Mask.Test(0) {
		t.Fatal("v6-only субдомен: бит не поставлен")
	}
}

func TestSubdomAAAATransportError(t *testing.T) {
	// A transport failure of AAAA with A records found: the bit is set
	// by the available addresses (the subdomain resolves), the failure —
	// in Err, the RCode of the last successful exchange is preserved
	// (NOERROR).
	z := subdomZone{
		answers: map[string][]dns.RR{
			subdomKey("www.example.com", dns.TypeA): {subdomA("www.example.com.", "192.0.2.7")},
		},
		errs: map[string]error{
			subdomKey("www.example.com", dns.TypeAAAA): errors.New("сеть недоступна"),
		},
	}
	f := z.resolver()

	rep, err := CheckSubdomains(context.Background(), f, "example.com", subdomSubs("www"), true)
	if err != nil {
		t.Fatalf("CheckSubdomains: %v", err)
	}
	r := rep.Results[0]
	if r.Err == nil || !strings.Contains(r.Err.Error(), "запрос AAAA") {
		t.Fatalf("Err %v, ожидалась ошибка запроса AAAA", r.Err)
	}
	subdomEqualIPs(t, r.A, "192.0.2.7")
	if !r.HasAddrs() || !rep.Mask.Test(0) {
		t.Fatal("A найдены — бит обязан стоять")
	}
	if r.RCode != dnsclient.RCodeNOERROR {
		t.Fatalf("RCode %v, ожидался NOERROR последнего успешного обмена", r.RCode)
	}
}

// --- DNSMask aggregation and error isolation -------------------------------------

func TestSubdomMaskAggregatesAndErrorsIsolated(t *testing.T) {
	// A batch of subdomains with different outcomes: SERVFAIL, NXDOMAIN
	// and a transport failure of individual names do not interrupt the
	// others — an outcome per each, the mask only by the addresses.
	z := subdomZone{
		answers: map[string][]dns.RR{
			subdomKey("example.com", dns.TypeA):     {subdomA("example.com.", "192.0.2.1")},
			subdomKey("crm.example.com", dns.TypeA): {subdomA("crm.example.com.", "192.0.2.9")},
		},
		rcodes: map[string]dnsclient.RCode{
			subdomKey("www.example.com", dns.TypeA):   dnsclient.RCodeSERVFAIL,
			subdomKey("admin.example.com", dns.TypeA): dnsclient.RCodeNXDOMAIN,
		},
		errs: map[string]error{
			subdomKey("api.example.com", dns.TypeA): errors.New("истекли попытки"),
		},
	}
	f := z.resolver()

	subs := []model.Subdomain{
		{ID: 0, Label: "", Enabled: true},
		{ID: 1, Label: "www", Enabled: true},
		{ID: 2, Label: "admin", Enabled: true},
		{ID: 3, Label: "api", Enabled: true},
		{ID: 4, Label: "crm", Enabled: true},
	}
	rep, err := CheckSubdomains(context.Background(), f, "example.com", subs, false)
	if err != nil {
		t.Fatalf("CheckSubdomains: %v", err)
	}
	if len(rep.Results) != 5 {
		t.Fatalf("результатов %d, ожидалось 5 — ошибки субдоменов не роняют пачку", len(rep.Results))
	}

	www := rep.Results[1]
	if www.RCode != dnsclient.RCodeSERVFAIL || www.Err != nil || www.HasAddrs() {
		t.Fatalf("www: RCode=%v Err=%v HasAddrs=%v — ожидался голый SERVFAIL", www.RCode, www.Err, www.HasAddrs())
	}
	admin := rep.Results[2]
	if admin.RCode != dnsclient.RCodeNXDOMAIN || admin.Err != nil {
		t.Fatalf("admin: RCode=%v Err=%v — NXDOMAIN без Err", admin.RCode, admin.Err)
	}
	api := rep.Results[3]
	if api.Err == nil || api.RCode != dnsclient.RCodeNOERROR {
		t.Fatalf("api: Err=%v RCode=%v — транспортный сбой в Err", api.Err, api.RCode)
	}

	// Bits 0 and 4 — only the subdomains with addresses.
	want, errW := model.NewDNSMask(0, 4)
	if errW != nil {
		t.Fatalf("NewDNSMask: %v", errW)
	}
	if rep.Mask != want {
		t.Fatalf("маска %v (%b), ожидалась %v (биты 0 и 4)", int64(rep.Mask), int64(rep.Mask), int64(want))
	}
	if rep.Mask.Test(1) || rep.Mask.Test(2) || rep.Mask.Test(3) {
		t.Fatal("биты субдоменов без адресов поставлены")
	}
}

func TestSubdomDisabledSkipped(t *testing.T) {
	// Disabled rows of the reference table are not queried and do not
	// enter the report: the input may be the full reference table (the
	// "enabled" filter lives here).
	z := subdomZone{answers: map[string][]dns.RR{
		subdomKey("www.example.com", dns.TypeA):   {subdomA("www.example.com.", "192.0.2.2")},
		subdomKey("admin.example.com", dns.TypeA): {subdomA("admin.example.com.", "192.0.2.3")},
	}}
	f := z.resolver()

	subs := []model.Subdomain{
		{ID: 0, Label: "www", Enabled: true},
		{ID: 1, Label: "admin", Enabled: false},
	}
	rep, err := CheckSubdomains(context.Background(), f, "example.com", subs, false)
	if err != nil {
		t.Fatalf("CheckSubdomains: %v", err)
	}
	if len(rep.Results) != 1 || rep.Results[0].Label != "www" {
		t.Fatalf("результаты %v, ожидался только www", rep.Results)
	}
	if n := subdomCallCount(f, "admin.example.com", dns.TypeA); n != 0 {
		t.Fatalf("отключённый субдомен опрошен: %d обменов", n)
	}
}

// --- input validation -------------------------------------------------------------

func TestSubdomValidationErrors(t *testing.T) {
	long := strings.Repeat("a", 64)
	cases := []struct {
		name   string
		domain string
		subs   []model.Subdomain
	}{
		{"пустой домен", "", subdomSubs("")},
		{"домен из пробелов", "  ", subdomSubs("")},
		{"id вне диапазона 0..62", "example.com", []model.Subdomain{{ID: 63, Label: "www", Enabled: true}}},
		{"отрицательный id", "example.com", []model.Subdomain{{ID: -1, Label: "www", Enabled: true}}},
		{"дубль id", "example.com", []model.Subdomain{
			{ID: 1, Label: "www", Enabled: true}, {ID: 1, Label: "api", Enabled: true}}},
		{"дубль метки", "example.com", []model.Subdomain{
			{ID: 0, Label: "www", Enabled: true}, {ID: 1, Label: "www", Enabled: true}}},
		{"метка с точкой", "example.com", subdomSubs("a.b")},
		{"метка с пробелом", "example.com", subdomSubs("a b")},
		{"метка длиннее 63 октетов", "example.com", subdomSubs(long)},
		{"нет включённых", "example.com", []model.Subdomain{{ID: 0, Label: "", Enabled: false}}},
		{"пустой список", "example.com", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := subdomZone{}.resolver()
			rep, err := CheckSubdomains(context.Background(), f, tc.domain, tc.subs, true)
			if err == nil {
				t.Fatalf("ожидалась ошибка валидации, получен отчёт %+v", rep)
			}
			if rep != nil {
				t.Fatal("при ошибке входа отчёт не выдаётся")
			}
			if len(f.calls) != 0 {
				t.Fatalf("валидация обязана отсечь обмены, было %d", len(f.calls))
			}
		})
	}
}

// --- context cancellation -----------------------------------------------------------

func TestSubdomContextCanceled(t *testing.T) {
	// A cancellation before the call: not a single exchange, a ctx
	// error, no report.
	f := subdomZone{answers: map[string][]dns.RR{
		subdomKey("example.com", dns.TypeA): {subdomA("example.com.", "192.0.2.1")},
	}}.resolver()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	rep, err := CheckSubdomains(ctx, f, "example.com", subdomSubs(""), false)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v, ожидалась context.Canceled", err)
	}
	if rep != nil {
		t.Fatal("отчёт при отмене не выдаётся")
	}

	// A cancellation in the middle of the batch: the first subdomain is
	// checked, the second's exchange is interrupted — the whole call
	// ends with a ctx error without a partial report (the task will come
	// back via at-least-once delivery, spec 3.4).
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	f2 := &subdomFakeResolver{handler: func(q dnsclient.Question) (*dnsclient.Result, error) {
		if subdomCanon(q.Name) == subdomCanon("www.example.com") {
			cancel2() // the worker "died" in the middle of the second subdomain
			return nil, ctx2.Err()
		}
		return &dnsclient.Result{RCode: dnsclient.RCodeNOERROR,
			Answer: []dns.RR{subdomA("example.com.", "192.0.2.1")}}, nil
	}}
	rep2, err2 := CheckSubdomains(ctx2, f2, "example.com", subdomSubs("", "www"), false)
	if !errors.Is(err2, context.Canceled) {
		t.Fatalf("err %v, ожидалась context.Canceled", err2)
	}
	if rep2 != nil {
		t.Fatal("частичный отчёт при отмене не выдаётся")
	}
}

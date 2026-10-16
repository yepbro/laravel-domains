// Юнит-тесты проверки субдоменов (T-037, ТЗ 5.4 п. 2) на фейковом
// dnsclient.Resolver без сети (стиль internal/dnsclient/client_test.go):
// A-записи, цепочки CNAME в одном ответе и обменами, циклы и предел
// глубины, CNAME→NXDOMAIN, флаг AAAA, агрегация DNSMask, изоляция ошибок
// субдоменов, валидация входа и отмена ctx. Идентификаторы — с префиксом
// subdom против параллельных файлов apex*/wild* (T-036/T-038).

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

// --- фейковый Resolver -------------------------------------------------------

// subdomFakeResolver — dnsclient.Resolver без сети: handler отвечает на
// вопросы, все вызовы записываются (контракт числа и порядка обменов —
// часть проверок: лишних CNAME-вопросов и повторных обходов цепочки
// быть не должно).
type subdomFakeResolver struct {
	calls   []dnsclient.Question
	handler func(q dnsclient.Question) (*dnsclient.Result, error)
}

// compile-time check: фейк — Resolver.
var _ dnsclient.Resolver = (*subdomFakeResolver)(nil)

func (f *subdomFakeResolver) Exchange(ctx context.Context, q dnsclient.Question) (*dnsclient.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.calls = append(f.calls, q)
	return f.handler(q)
}

// subdomZone — декларативная зона для фейка: секция answer, RCODE и
// транспортные ошибки по ключу «fqdn|тип» (subdomKey). Отсутствующий
// ключ — NOERROR с пустым ответом (NODATA).
type subdomZone struct {
	answers map[string][]dns.RR
	rcodes  map[string]dnsclient.RCode
	errs    map[string]error
}

// subdomKey — ключ зоны: каноническое имя и тип записи.
func subdomKey(name string, t uint16) string {
	return subdomCanon(name) + "|" + dns.TypeToString[t]
}

// subdomResolver собирает Resolver из зоны.
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

// --- конструкторы записей и вспомогательные проверки -----------------------

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

// subdomIPs — строковое представление адресов для сообщений об ошибке.
func subdomIPs(addrs []netip.Addr) []string {
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, a.String())
	}
	return out
}

// subdomEqualIPs сравнивает адреса как строки в порядке следования.
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

// subdomCallCount считает обмены с именем и типом.
func subdomCallCount(f *subdomFakeResolver, name string, t uint16) int {
	n := 0
	for _, q := range f.calls {
		if subdomCanon(q.Name) == subdomCanon(name) && q.Type == t {
			n++
		}
	}
	return n
}

// subdomAssertNoCNAMEQuestions — контракт проекта: отдельные вопросы типа
// CNAME не выдаются, записи CNAME приходят в ответах A/AAAA (RFC 1035
// §3.3.1; комментарий subdomCheckOne).
func subdomAssertNoCNAMEQuestions(t *testing.T, f *subdomFakeResolver) {
	t.Helper()
	for _, q := range f.calls {
		if q.Type == dns.TypeCNAME {
			t.Fatalf("выдан отдельный вопрос CNAME %q — записи CNAME приходят в ответах A/AAAA", q.Name)
		}
	}
}

// subdomSubs — вход из строк-меток с последовательными id.
func subdomSubs(labels ...string) []model.Subdomain {
	subs := make([]model.Subdomain, 0, len(labels))
	for i, l := range labels {
		subs = append(subs, model.Subdomain{ID: int16(i), Label: l, Enabled: true})
	}
	return subs
}

// --- A-записи напрямую -------------------------------------------------------

func TestSubdomDirectA(t *testing.T) {
	z := subdomZone{answers: map[string][]dns.RR{
		subdomKey("example.com", dns.TypeA): {
			subdomA("example.com.", "192.0.2.10"),
			subdomA("example.com.", "192.0.2.10"), // дубль записи — один адрес
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
	subdomEqualIPs(t, apex.AAAA) // AAAA-вопрос задан, ответ пуст (NODATA)
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

	// Апекс спрашивается именем самого домена (не «.example.com»),
	// вопросы AAAA заданы обоим, CNAME-вопросов нет.
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

	// Маска: оба субдомена с адресами (биты 0 и 1).
	if want := model.DNSMask(0b11); rep.Mask != want {
		t.Fatalf("маска %v (%05b), ожидалось %v", rep.Mask, int64(rep.Mask), want)
	}
}

// --- цепочки CNAME -----------------------------------------------------------

func TestSubdomCNAMEOneHopInAnswer(t *testing.T) {
	// Рекурсор прикладывает цепочку к ответу целиком: CNAME и A цели —
	// в одном ответе, второй A-обмен не нужен.
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
	subdomEqualIPs(t, r.AAAA, "2001:db8::1") // AAAA — по терминальному имени
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
	// Ответ первого обмена содержит только CNAME (цель вне зоны охвата
	// ответа) — терминал доискивается собственным обменом.
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
	// Цепочка www→c1→…→c5 с A у c5 целиком в одном ответе: разворачивается
	// без единого дополнительного обмена.
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
	// A→B→A: цикл виден по множеству посещённых имён раньше предела
	// глубины; адресов нет, бит не ставится, AAAA не спрашивается.
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
	// Лестница уникальных имён длиннее предела: обрыв ErrSubdomCNAMEDepth
	// после subdomMaxChain переходов.
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
	// CNAME на внешнее имя, которого не существует (RFC 8020): субдомен-
	// псевдоним существует, адресов нет — Err нет, признак NXDOMAIN в
	// RCode, бит не ставится, AAAA не спрашивается (NXDOMAIN безусловен
	// для всех типов). Для статуса домена — вклад в «нет адресов»
	// (ТЗ 5.4 п. 5), не temp_error.
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

// --- флаг AAAA ---------------------------------------------------------------

func TestSubdomAAAAFlag(t *testing.T) {
	// Выключен — вопросов AAAA нет вовсе, адреса только A.
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

	// Включён и имя живёт только по IPv6: A — NODATA, AAAA даёт адреса,
	// субдомен считается резолвящимся.
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
	// Транспортный сбой AAAA при найденных A: бит ставится по имеющимся
	// адресам (субдомен резолвится), сбой — в Err, RCode последнего
	// успешного обмена сохранён (NOERROR).
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

// --- агрегация DNSMask и изоляция ошибок ------------------------------------

func TestSubdomMaskAggregatesAndErrorsIsolated(t *testing.T) {
	// Пачка субдоменов с разными исходами: SERVFAIL, NXDOMAIN и
	// транспортный сбой отдельных имён не прерывают остальные — итог по
	// каждому, маска только по адресам.
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

	// Биты 0 и 4 — только субдомены с адресами.
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
	// Отключённые строки справочника не спрашиваются и в отчёт не входят:
	// входом может быть полный справочник (фильтр «включённые» — здесь).
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

// --- валидация входа ---------------------------------------------------------

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

// --- отмена контекста --------------------------------------------------------

func TestSubdomContextCanceled(t *testing.T) {
	// Отмена до вызова: ни одного обмена, ошибка ctx, отчёта нет.
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

	// Отмена в середине пачки: первый субдомен проверен, обмен второго
	// прерывается — весь вызов завершается ошибкой ctx без частичного
	// отчёта (задача вернётся доставкой at-least-once, ТЗ 3.4).
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	f2 := &subdomFakeResolver{handler: func(q dnsclient.Question) (*dnsclient.Result, error) {
		if subdomCanon(q.Name) == subdomCanon("www.example.com") {
			cancel2() // воркер «погиб» посреди второго субдомена
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

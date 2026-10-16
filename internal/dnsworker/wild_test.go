// Тесты wildcard-детекта (T-038). Без сети: резолвер — фейк,
// реализующий dnsclient.Resolver; RR-конструкторы — в стиле тестов
// internal/dnsclient.

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

// --- фейковый резолвер -------------------------------------------------------

// wildFakeResolver фиксирует вопросы и отвечает функцией. Тесты
// однопоточны (DetectWildcard синхронный) — блокировки не нужны.
type wildFakeResolver struct {
	calls  []dnsclient.Question
	answer func(q dnsclient.Question) (*dnsclient.Result, error)
}

// compile-time check: wildFakeResolver — dnsclient.Resolver.
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

// wildMakeResult — итог Exchange с кодом и секцией answer.
func wildMakeResult(rcode dnsclient.RCode, an ...dns.RR) *dnsclient.Result {
	return &dnsclient.Result{RCode: rcode, Answer: an}
}

// --- генерация случайной метки ----------------------------------------------

func TestWildRandomLabel(t *testing.T) {
	seen := make(map[string]bool, 1000)
	for i := 0; i < 1000; i++ {
		label, err := wildRandomLabel()
		if err != nil {
			t.Fatalf("wildRandomLabel: %v", err)
		}
		if len(label) != wildLabelLength {
			t.Fatalf("длина метки %d, ожидалась %d (%q)", len(label), wildLabelLength, label)
		}
		for _, c := range label {
			if !strings.ContainsRune(wildLabelAlphabet, c) {
				t.Fatalf("символ %q вне алфавита a-z0-9: %q", c, label)
			}
		}
		// Префикс punycode невозможен (дефиса в алфавите нет), но
		// инвариант дешёвый — проверяется явно.
		if strings.HasPrefix(label, "xn--") {
			t.Fatalf("метка с punycode-префиксом: %q", label)
		}
		if seen[label] {
			t.Fatalf("повтор метки %q", label)
		}
		seen[label] = true
	}
}

// --- DetectWildcard ---------------------------------------------------------

// TestDetectWildcardZoneWithWildcard — случайное имя получает A-записи:
// зона под wildcard, адресы перенесены в итог, метка стабильна в вызове
// (имя вопроса = метка + домен).
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
		t.Fatal("случайное имя резолвится — Detected обязан быть true")
	}
	if len(info.Addrs) != 2 ||
		!info.Addrs[0].Equal(net.ParseIP("192.0.2.10")) ||
		!info.Addrs[1].Equal(net.ParseIP("192.0.2.11")) {
		t.Fatalf("адреса wildcard-ответа: %v", info.Addrs)
	}
	if len(f.calls) != 1 {
		t.Fatalf("детект-запросов: %d, ожидался 1", len(f.calls))
	}
	q := f.calls[0]
	if q.Type != dns.TypeA {
		t.Fatalf("тип детект-вопроса %v, ожидался A (ТЗ 5.4 п. 3)", dns.TypeToString[q.Type])
	}
	// Fqdn (завершающая точка) — дело транспорта dnsclient; фейк видит
	// имя как передал DetectWildcard.
	if want := info.Label + ".example.com"; q.Name != want {
		t.Fatalf("вопрос %q не совпадает с заявленной меткой (ожидался %q)", q.Name, want)
	}
}

// TestDetectWildcardZoneWithoutWildcard — NXDOMAIN на случайное имя:
// зоны без wildcard, все субдомены честные (детект-отрицание).
func TestDetectWildcardZoneWithoutWildcard(t *testing.T) {
	f := &wildFakeResolver{answer: func(q dnsclient.Question) (*dnsclient.Result, error) {
		return wildMakeResult(dnsclient.RCodeNXDOMAIN), nil
	}}

	info, err := DetectWildcard(context.Background(), f, "example.com")
	if err != nil {
		t.Fatalf("NXDOMAIN — авторитетное отрицание, не ошибка: %v", err)
	}
	if info.Detected {
		t.Fatal("NXDOMAIN на случайное имя: Detected обязан быть false")
	}
	if info.Addrs != nil {
		t.Fatalf("адресов у отрицания быть не может: %v", info.Addrs)
	}
	if info.Label == "" {
		t.Fatal("метка фиксируется и для отрицания (диагностика)")
	}
}

// TestDetectWildcardNODATA — NOERROR без A-записей: wildcard без адресов
// (или NODATA) — сравнивать нечего, итог как без wildcard.
func TestDetectWildcardNODATA(t *testing.T) {
	f := &wildFakeResolver{answer: func(q dnsclient.Question) (*dnsclient.Result, error) {
		return wildMakeResult(dnsclient.RCodeNOERROR), nil
	}}

	info, err := DetectWildcard(context.Background(), f, "example.com")
	if err != nil {
		t.Fatalf("NOERROR без A — не ошибка: %v", err)
	}
	if info.Detected || info.Addrs != nil {
		t.Fatalf("NODATA: Detected=%v Addrs=%v, ожидалось false/nil", info.Detected, info.Addrs)
	}
}

// TestDetectWildcardCNAMEChain — wildcard-обёртка CNAME: адреса берутся
// конечные (A цели), сама CNAME-запись в набор не попадает.
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
		t.Fatal("CNAME → A резолвится — зона под wildcard")
	}
	if len(info.Addrs) != 1 || !info.Addrs[0].Equal(net.ParseIP("203.0.113.5")) {
		t.Fatalf("ожидался только конечный адрес цепочки, получено %v", info.Addrs)
	}
}

// TestDetectWildcardCNAMEWithoutA — CNAME на имя без A-записей: wildcard
// адресов не даёт, детект для целей сравнения отрицателен (без ошибки).
func TestDetectWildcardCNAMEWithoutA(t *testing.T) {
	f := &wildFakeResolver{answer: func(q dnsclient.Question) (*dnsclient.Result, error) {
		return wildMakeResult(dnsclient.RCodeNOERROR,
			wildCNAME(q.Name, "addressless.example.")), nil
	}}

	info, err := DetectWildcard(context.Background(), f, "example.com")
	if err != nil {
		t.Fatalf("CNAME без A — не ошибка: %v", err)
	}
	if info.Detected || info.Addrs != nil {
		t.Fatalf("CNAME без A: Detected=%v Addrs=%v, ожидалось false/nil", info.Detected, info.Addrs)
	}
}

// TestDetectWildcardServFail — неавторитетный ответ: детект не удался
// (ошибка, не «успешное отрицание»); проверку домена не роняет —
// политика потребителя «продолжить без фильтрации» документирована.
func TestDetectWildcardServFail(t *testing.T) {
	f := &wildFakeResolver{answer: func(q dnsclient.Question) (*dnsclient.Result, error) {
		return wildMakeResult(dnsclient.RCodeSERVFAIL), nil
	}}

	info, err := DetectWildcard(context.Background(), f, "example.com")
	if err == nil {
		t.Fatal("SERVFAIL — не авторитетный ответ, ожидается ошибка детекта")
	}
	if !strings.Contains(err.Error(), "wildcard-детект") || !strings.Contains(err.Error(), "SERVFAIL") {
		t.Fatalf("сообщение без контекста: %v", err)
	}
	if info.Detected {
		t.Fatal("неудавшийся детект не может помечать зону wildcard")
	}
}

// TestDetectWildcardNetworkError — ошибка сети/транспорта прокидывается
// с сохранением причины; «детект не удался», не паника.
func TestDetectWildcardNetworkError(t *testing.T) {
	sentinel := errors.New("резолвер недоступен")
	f := &wildFakeResolver{answer: func(q dnsclient.Question) (*dnsclient.Result, error) {
		return nil, sentinel
	}}

	info, err := DetectWildcard(context.Background(), f, "example.com")
	if !errors.Is(err, sentinel) {
		t.Fatalf("ожидается оборачивание причины, получено: %v", err)
	}
	if info.Detected {
		t.Fatal("ошибка обмена — Detected обязан быть false")
	}
}

// TestDetectWildcardCtxCancelled — отмена контекста: ошибка отмены
// прокидывается из резолвера без подмены (проверку ctx до транспорта
// делает dnsclient.Client — детект её не дублирует).
func TestDetectWildcardCtxCancelled(t *testing.T) {
	f := &wildFakeResolver{answer: func(q dnsclient.Question) (*dnsclient.Result, error) {
		t.Error("ответ не должен запрашиваться при отменённом ctx")
		return nil, errors.New("unreachable")
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	info, err := DetectWildcard(ctx, f, "example.com")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ожидается context.Canceled, получено %v", err)
	}
	if info.Detected {
		t.Fatal("отмена — не «зона без wildcard», Detected обязан быть false")
	}
}

// TestDetectWildcardEmptyDomain — пустое имя домена: ошибка до генерации
// метки и до обмена.
func TestDetectWildcardEmptyDomain(t *testing.T) {
	f := &wildFakeResolver{answer: func(q dnsclient.Question) (*dnsclient.Result, error) {
		t.Error("обмен не должен происходить для пустого домена")
		return nil, errors.New("unreachable")
	}}

	for _, domain := range []string{"", "  "} {
		if _, err := DetectWildcard(context.Background(), f, domain); err == nil ||
			!strings.Contains(err.Error(), "пустое имя домена") {
			t.Fatalf("домен %q: ожидается ошибка «пустое имя домена», получено %v", domain, err)
		}
	}
	if len(f.calls) != 0 {
		t.Fatalf("обменов для пустого домена: %d", len(f.calls))
	}
}

// TestDetectWildcardLabelUniqueAndStablePerCall — два детекта дают
// разные метки (уникальность), и каждая метка действительно та, что
// ушла в вопрос (стабильность внутри вызова).
func TestDetectWildcardLabelUniqueAndStablePerCall(t *testing.T) {
	f := &wildFakeResolver{answer: func(q dnsclient.Question) (*dnsclient.Result, error) {
		return wildMakeResult(dnsclient.RCodeNOERROR, wildA(q.Name, "192.0.2.10")), nil
	}}

	labels := make(map[string]bool, 2)
	for i := 0; i < 2; i++ {
		info, err := DetectWildcard(context.Background(), f, "example.com")
		if err != nil {
			t.Fatalf("детект %d: %v", i, err)
		}
		if !info.Detected {
			t.Fatalf("детект %d: ожидался wildcard", i)
		}
		if labels[info.Label] {
			t.Fatalf("метка повторилась между вызовами: %q", info.Label)
		}
		labels[info.Label] = true
		if want := info.Label + ".example.com"; f.calls[i].Name != want {
			t.Fatalf("вызов %d: вопрос %q не совпадает с меткой (ожидался %q)", i, f.calls[i].Name, want)
		}
	}
	if len(f.calls) != 2 {
		t.Fatalf("обменов: %d, ожидалось 2", len(f.calls))
	}
}

// --- MarkWildcardSubdomains -------------------------------------------------

// TestMarkWildcardSubdomains — таблица пометки: подмножество/равенство
// адресов → wildcard; апекс и www — никогда; адрес вне набора или пустые
// адреса — честный субдомен.
func TestMarkWildcardSubdomains(t *testing.T) {
	wild := []net.IP{net.ParseIP("192.0.2.10"), net.ParseIP("192.0.2.11")}
	subs := []WildSubdomain{
		{ID: 0, Label: "", Addrs: wild}, // апекс — исключение ТЗ 5.4 п. 3
		{ID: 1, Label: "www", Addrs: wild},
		{ID: 2, Label: "admin", Addrs: []net.IP{net.ParseIP("192.0.2.10")}},                            // подмножество
		{ID: 3, Label: "crm", Addrs: []net.IP{net.ParseIP("192.0.2.11"), net.ParseIP("192.0.2.10")}},   // равенство
		{ID: 4, Label: "api", Addrs: []net.IP{net.ParseIP("192.0.2.10"), net.ParseIP("198.51.100.7")}}, // частичное совпадение
		{ID: 5, Label: "app", Addrs: []net.IP{net.ParseIP("198.51.100.7")}},                            // всё вне набора
		{ID: 6, Label: "shop", Addrs: nil},                                                             // без адресов
	}

	res, err := MarkWildcardSubdomains(wild, subs)
	if err != nil {
		t.Fatalf("MarkWildcardSubdomains: %v", err)
	}
	want := map[int]bool{2: true, 3: true}
	for _, sub := range subs {
		if got := res.IsWildcard(sub.ID); got != want[sub.ID] {
			t.Errorf("субдомен %d (%q): IsWildcard=%v, ожидалось %v",
				sub.ID, sub.Label, got, want[sub.ID])
		}
	}
	if ids := res.IDs(); len(ids) != 2 || ids[0] != 2 || ids[1] != 3 {
		t.Fatalf("IDs(): %v, ожидалось [2 3]", ids)
	}
	if res.Mask != model.DNSMask(0b1100) {
		t.Fatalf("маска: %b, ожидалась 1100", res.Mask)
	}
}

// TestMarkWildcardSubdomainsNoWildcard — пустой набор адресов wildcard
// (детект отрицателен/не удался): не помечается никто.
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
		t.Fatalf("без wildcard-адресов маска обязана быть пустой: %v", res.Mask.IDs())
	}

	// Пустой вход субдоменов — тоже без ошибки.
	res, err = MarkWildcardSubdomains([]net.IP{net.ParseIP("192.0.2.10")}, nil)
	if err != nil || !res.Mask.IsEmpty() {
		t.Fatalf("пустой вход субдоменов: res=%v err=%v", res, err)
	}
}

// TestMarkWildcardSubdomainsIPForms — одна точка в 4- и 16-октетной
// форме — один адрес (net.IP.Equal); IPv6-адрес вне wildcard-набора (A)
// оставляет субдомен честным.
func TestMarkWildcardSubdomainsIPForms(t *testing.T) {
	wild := []net.IP{net.ParseIP("192.0.2.10")} // запись A — 4-октетная форма

	mapped, err := MarkWildcardSubdomains(wild, []WildSubdomain{
		{ID: 2, Label: "admin", Addrs: []net.IP{net.ParseIP("192.0.2.10").To16()}},
	})
	if err != nil {
		t.Fatalf("MarkWildcardSubdomains: %v", err)
	}
	if !mapped.IsWildcard(2) {
		t.Fatal("16-октетная форма того же адреса обязана совпадать с 4-октетной")
	}

	// IPv6-адрес не входит в wildcard-набор (детект — A-запрос, ТЗ 5.4
	// п. 3): двойной стек не помечается и уходит на HTTP — безопасное
	// направление (лишняя проверка вместо пропущенного сайта).
	dual, err := MarkWildcardSubdomains(wild, []WildSubdomain{
		{ID: 3, Label: "crm", Addrs: []net.IP{net.ParseIP("192.0.2.10"), net.ParseIP("2001:db8::1")}},
	})
	if err != nil {
		t.Fatalf("MarkWildcardSubdomains: %v", err)
	}
	if dual.IsWildcard(3) {
		t.Fatal("адрес вне wildcard-набора — субдомен честный")
	}
}

// TestMarkWildcardSubdomainsInvalidID — номер вне 0..62 (модель маски):
// ошибка, частичная маска не возвращается.
func TestMarkWildcardSubdomainsInvalidID(t *testing.T) {
	wild := []net.IP{net.ParseIP("192.0.2.10")}
	for _, id := range []int{-1, 63, 100} {
		res, err := MarkWildcardSubdomains(wild, []WildSubdomain{
			{ID: 2, Label: "admin", Addrs: wild}, // валидный идёт первым
			{ID: id, Label: "broken", Addrs: wild},
		})
		if err == nil {
			t.Fatalf("id %d: ожидалась ошибка валидации маски", id)
		}
		if !res.Mask.IsEmpty() || len(res.IDs()) != 0 {
			t.Fatalf("id %d: частичная маска %v вместо пустой", id, res.Mask.IDs())
		}
		if !strings.Contains(err.Error(), "wildcard-пометка") {
			t.Fatalf("id %d: сообщение без контекста: %v", id, err)
		}
	}
}

// TestWildScenarioEndToEnd — связка детект + пометка на одном сценарии
// (зона под wildcard, CNAME-обёртка): половина субдоменов — заглушка,
// апекс/www и субдомен с собственным адресом идут на HTTP.
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
			t.Errorf("субдомен %d: wildcard=%v, ожидалось %v", id, res.IsWildcard(id), want)
		}
	}
	// В маску не попадают и AAAA-адреса цепочки, и CNAME — только A.
	if ids := res.IDs(); len(ids) != 2 || ids[0] != 4 || ids[1] != 5 {
		t.Fatalf("IDs(): %v, ожидалось [4 5]", ids)
	}
}

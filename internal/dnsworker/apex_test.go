// Тесты проверки апекса (T-036): все ветки классификации на сценарном
// фейке dnsclient.Resolver — сеть не нужна. Фейк отвечает на вызовы по
// порядку записями сценария и фиксирует вопросы (формат FQDN, тип A/NS),
// порядок и число обращений.

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

// --- сценарный фейк Resolver -------------------------------------------------

// apexStep — один ответ сценария: либо Result с кодом и секцией answer,
// либо ошибка (имитация сети после исчерпания ретраев dnsclient).
type apexStep struct {
	rcode  dnsclient.RCode
	answer []dns.RR
	err    error
}

// apexReply — шаг-ответ с кодом и записями answer.
func apexReply(rcode dnsclient.RCode, an ...dns.RR) apexStep {
	return apexStep{rcode: rcode, answer: an}
}

// apexFail — шаг-ошибка Exchange (контракт T-034: сеть, попытки исчерпаны).
func apexFail(err error) apexStep {
	return apexStep{err: err}
}

// apexTimeoutErr — сетевая ошибка с признаком таймаута (стиль dnsclient).
type apexTimeoutErr struct{}

func (apexTimeoutErr) Error() string   { return "fake i/o timeout" }
func (apexTimeoutErr) Timeout() bool   { return true }
func (apexTimeoutErr) Temporary() bool { return true }

// apexFakeResolver — сценарный dnsclient.Resolver без сети: каждый вызов
// снимает очередной шаг сценария и фиксирует вопрос. onCall (если задан)
// вызывается после фиксации — тест дергает отмену ctx между запросами.
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
		return nil, fmt.Errorf("apexFakeResolver: сценарий исчерпан (вызов №%d: %s)", len(f.calls), q.Name)
	}
	st := f.steps[0]
	f.steps = f.steps[1:]
	if st.err != nil {
		return nil, st.err
	}
	return &dnsclient.Result{RCode: st.rcode, Answer: st.answer}, nil
}

// --- конструкторы записей ----------------------------------------------------

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

// apexAddrs — []netip.Addr для сравнения адресов в тестах.
func apexAddrs(ips ...string) []netip.Addr {
	out := make([]netip.Addr, len(ips))
	for i, ip := range ips {
		out[i] = netip.MustParseAddr(ip)
	}
	return out
}

// --- классификация: все ветки --------------------------------------------------

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
			name:       "NXDOMAIN → not_delegated, субдомены не проверяются (RFC 8020)",
			steps:      []apexStep{apexReply(dnsclient.RCodeNXDOMAIN)},
			wantStatus: ApexNotDelegated,
			wantStop:   true,
			wantCalls:  1,
		},
		{
			name: "NOERROR с A — делегирован, адреса возвращены",
			steps: []apexStep{apexReply(dnsclient.RCodeNOERROR,
				apexARR("example.com.", "192.0.2.10"),
				apexARR("example.com.", "192.0.2.11"))},
			wantStatus: ApexDelegated,
			wantStop:   false,
			wantAddrs:  apexAddrs("192.0.2.10", "192.0.2.11"),
			wantCalls:  1,
		},
		{
			name: "NOERROR с CNAME и A (цепочку развернул рекурсор) — адреса есть",
			steps: []apexStep{apexReply(dnsclient.RCodeNOERROR,
				apexCNAME("example.com.", "edge.example.net."),
				apexARR("edge.example.net.", "192.0.2.20"))},
			wantStatus: ApexDelegated,
			wantStop:   false,
			wantAddrs:  apexAddrs("192.0.2.20"),
			wantCalls:  1,
		},
		{
			name: "NOERROR только CNAME без A — адресов нет, NS подтверждает зону",
			steps: []apexStep{
				apexReply(dnsclient.RCodeNOERROR, apexCNAME("example.com.", "edge.example.net.")),
				apexReply(dnsclient.RCodeNOERROR, apexNSRR("example.com.", "ns1.example.com."))},
			wantStatus: ApexDelegatedNoAddress,
			wantStop:   false,
			wantCalls:  2,
			wantNS:     true,
		},
		{
			name: "NODATA + NS с записями — делегирован без адресов",
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
			name: "NODATA + NS NODATA — имя существует (NOERROR), not_delegated только для NXDOMAIN",
			steps: []apexStep{
				apexReply(dnsclient.RCodeNOERROR),
				apexReply(dnsclient.RCodeNOERROR)},
			wantStatus: ApexDelegatedNoAddress,
			wantStop:   false,
			wantCalls:  2,
			wantNS:     true,
		},
		{
			name: "NODATA + NS NXDOMAIN — свежий отказ, домена нет",
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
			name: "NODATA + NS сеть — temp_error",
			steps: []apexStep{
				apexReply(dnsclient.RCodeNOERROR),
				apexFail(apexTimeoutErr{})},
			wantStatus: ApexTempError,
			wantStop:   true,
			wantCalls:  2,
			wantNS:     true,
		},
		{
			name:       "SERVFAIL на A — temp_error, NS не запрашивается",
			steps:      []apexStep{apexReply(dnsclient.RCodeSERVFAIL)},
			wantStatus: ApexTempError,
			wantStop:   true,
			wantCalls:  1,
		},
		{
			name:       "REFUSED на A — заявление резолвера о себе, temp_error",
			steps:      []apexStep{apexReply(dnsclient.RCodeREFUSED)},
			wantStatus: ApexTempError,
			wantStop:   true,
			wantCalls:  1,
		},
		{
			name:       "FORMERR на A — temp_error",
			steps:      []apexStep{apexReply(dnsclient.RCodeFORMERR)},
			wantStatus: ApexTempError,
			wantStop:   true,
			wantCalls:  1,
		},
		{
			name:       "NOTIMP на A — temp_error",
			steps:      []apexStep{apexReply(dnsclient.RCodeNOTIMP)},
			wantStatus: ApexTempError,
			wantStop:   true,
			wantCalls:  1,
		},
		{
			name:       "неизвестный RCODE 11 на A — temp_error",
			steps:      []apexStep{apexReply(dnsclient.RCode(11))},
			wantStatus: ApexTempError,
			wantStop:   true,
			wantCalls:  1,
		},
		{
			name:       "таймаут на A после ретраев dnsclient — temp_error",
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
				t.Fatalf("CheckApex: неожиданная ошибка: %v", err)
			}
			if res.Status != tc.wantStatus {
				t.Errorf("Status = %v, ожидалось %v", res.Status, tc.wantStatus)
			}
			if got := res.Status.Stop(); got != tc.wantStop {
				t.Errorf("Stop = %v, ожидалось %v", got, tc.wantStop)
			}
			if res.NSQueried != tc.wantNS {
				t.Errorf("NSQueried = %v, ожидалось %v", res.NSQueried, tc.wantNS)
			}
			if len(fake.calls) != tc.wantCalls {
				t.Fatalf("обращений к резолверу %d, ожидалось %d (вопросы: %v)",
					len(fake.calls), tc.wantCalls, fake.calls)
			}
			want := tc.wantAddrs
			if len(res.Addresses) != len(want) {
				t.Fatalf("Addresses = %v, ожидалось %v", res.Addresses, want)
			}
			for i := range want {
				if res.Addresses[i] != want[i] {
					t.Errorf("Addresses[%d] = %v, ожидалось %v", i, res.Addresses[i], want[i])
				}
			}
		})
	}
}

// --- формат вопросов: FQDN и типы ----------------------------------------------

func TestCheckApexQuestionFormat(t *testing.T) {
	t.Run("имя с точкой и без — один и тот же FQDN-вопрос A", func(t *testing.T) {
		for _, name := range []string{"example.com", "example.com."} {
			fake := &apexFakeResolver{steps: []apexStep{
				apexReply(dnsclient.RCodeNOERROR, apexARR("example.com.", "192.0.2.1"))}}
			if _, err := CheckApex(context.Background(), fake, name); err != nil {
				t.Fatalf("CheckApex(%q): %v", name, err)
			}
			if len(fake.calls) != 1 {
				t.Fatalf("CheckApex(%q): обращений %d, ожидалось 1", name, len(fake.calls))
			}
			q := fake.calls[0]
			if q.Name != "example.com." {
				t.Errorf("CheckApex(%q): имя вопроса %q, ожидался FQDN \"example.com.\"", name, q.Name)
			}
			if q.Type != dns.TypeA {
				t.Errorf("CheckApex(%q): тип вопроса %v, ожидался A", name, q.Type)
			}
		}
	})
	t.Run("NODATA — второй вопрос NS с тем же FQDN", func(t *testing.T) {
		fake := &apexFakeResolver{steps: []apexStep{
			apexReply(dnsclient.RCodeNOERROR),
			apexReply(dnsclient.RCodeNOERROR, apexNSRR("example.com.", "ns1.example.com."))}}
		if _, err := CheckApex(context.Background(), fake, "example.com"); err != nil {
			t.Fatalf("CheckApex: %v", err)
		}
		if len(fake.calls) != 2 {
			t.Fatalf("обращений %d, ожидалось 2", len(fake.calls))
		}
		q := fake.calls[1]
		if q.Name != "example.com." {
			t.Errorf("имя NS-вопроса %q, ожидался FQDN \"example.com.\"", q.Name)
		}
		if q.Type != dns.TypeNS {
			t.Errorf("тип вопроса %v, ожидался NS", q.Type)
		}
	})
	t.Run("IDN-имя в punycode — без перекодирования", func(t *testing.T) {
		fake := &apexFakeResolver{steps: []apexStep{
			apexReply(dnsclient.RCodeNOERROR, apexARR("xn--80adxhks.", "192.0.2.1"))}}
		if _, err := CheckApex(context.Background(), fake, "xn--80adxhks"); err != nil {
			t.Fatalf("CheckApex: %v", err)
		}
		if got := fake.calls[0].Name; got != "xn--80adxhks." {
			t.Errorf("имя вопроса %q, ожидался punycode FQDN", got)
		}
	})
}

// --- адреса: состав и порядок ---------------------------------------------------

func TestCheckApexAddresses(t *testing.T) {
	t.Run("только валидные IPv4 из A, порядок ответа сохранён", func(t *testing.T) {
		bad := &dns.A{Hdr: dns.RR_Header{Name: "example.com.", Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60}}
		fake := &apexFakeResolver{steps: []apexStep{
			apexReply(dnsclient.RCodeNOERROR,
				apexARR("example.com.", "192.0.2.2"),
				bad, // A без IP — мусор, пропускается
				apexAAAARR("example.com.", "2001:db8::1"), // не тот тип записи
				apexARR("example.com.", "192.0.2.1"))}}
		res, err := CheckApex(context.Background(), fake, "example.com")
		if err != nil {
			t.Fatalf("CheckApex: %v", err)
		}
		want := apexAddrs("192.0.2.2", "192.0.2.1")
		if len(res.Addresses) != len(want) {
			t.Fatalf("Addresses = %v, ожидалось %v", res.Addresses, want)
		}
		for i := range want {
			if res.Addresses[i] != want[i] {
				t.Errorf("Addresses[%d] = %v, ожидалось %v", i, res.Addresses[i], want[i])
			}
		}
		if !res.Addresses[0].Is4() {
			t.Errorf("адрес апекса обязан быть IPv4 (A-запись), получен %v", res.Addresses[0])
		}
	})
	t.Run("ветки без адресов возвращают пустой список", func(t *testing.T) {
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
				t.Errorf("%s: Addresses = %v, ожидалось пусто", tc.name, res.Addresses)
			}
		}
	})
}

// --- контракт ApexStatus: Stop и DomainStatus ------------------------------------

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
			t.Errorf("%v.Stop() = %v, ожидалось %v", tc.status, got, tc.wantStop)
		}
		dom, ok := tc.status.DomainStatus()
		if ok != tc.wantDomOK || dom != tc.wantDom {
			t.Errorf("%v.DomainStatus() = (%v, %v), ожидалось (%v, %v)",
				tc.status, dom, ok, tc.wantDom, tc.wantDomOK)
		}
		if got := tc.status.String(); got != tc.wantStr {
			t.Errorf("%v.String() = %q, ожидалось %q", tc.status, got, tc.wantStr)
		}
	}
	if got := ApexStatus(7).String(); got != "ApexStatus(7)" {
		t.Errorf("String() неизвестного кода = %q", got)
	}
}

// --- отмена контекста --------------------------------------------------------

func TestCheckApexContext(t *testing.T) {
	t.Run("отмена до запроса — ошибка, резолвер не зовётся", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		fake := &apexFakeResolver{}
		res, err := CheckApex(ctx, fake, "example.com")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ошибка = %v, ожидалась context.Canceled", err)
		}
		if res != nil {
			t.Errorf("результат = %v, ожидался nil", res)
		}
		if len(fake.calls) != 0 {
			t.Errorf("обращений к резолверу %d, ожидалось 0", len(fake.calls))
		}
	})
	t.Run("прошедший дедлайн — DeadlineExceeded", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 0)
		defer cancel()
		fake := &apexFakeResolver{}
		res, err := CheckApex(ctx, fake, "example.com")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("ошибка = %v, ожидалась context.DeadlineExceeded", err)
		}
		if res != nil {
			t.Errorf("результат = %v, ожидался nil", res)
		}
	})
	t.Run("отмена на NS-запросе — ошибка, не temp_error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fake := &apexFakeResolver{steps: []apexStep{
			apexReply(dnsclient.RCodeNOERROR), // A: NODATA
			apexFail(context.Canceled)}}       // NS: отмена во время запроса
		fake.onCall = func(n int) {
			if n == 2 {
				cancel()
			}
		}
		res, err := CheckApex(ctx, fake, "example.com")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ошибка = %v, ожидалась context.Canceled", err)
		}
		if res != nil {
			t.Errorf("результат = %v, ожидался nil", res)
		}
	})
	t.Run("ошибка Exchange без отмены ctx — temp_error, не ошибка", func(t *testing.T) {
		// Резолвер вернул context.Canceled, но собственный ctx жив:
		// классификация по состоянию вызывающего контекста.
		fake := &apexFakeResolver{steps: []apexStep{apexFail(context.Canceled)}}
		res, err := CheckApex(context.Background(), fake, "example.com")
		if err != nil {
			t.Fatalf("CheckApex: %v", err)
		}
		if res.Status != ApexTempError {
			t.Errorf("Status = %v, ожидался ApexTempError", res.Status)
		}
	})
}

// --- недопустимое имя --------------------------------------------------------

func TestCheckApexEmptyDomain(t *testing.T) {
	for _, name := range []string{"", "   ", "."} {
		fake := &apexFakeResolver{}
		res, err := CheckApex(context.Background(), fake, name)
		if err == nil {
			t.Errorf("CheckApex(%q): ожидалась ошибка, получен результат %v", name, res)
		}
		if res != nil {
			t.Errorf("CheckApex(%q): результат = %v, ожидался nil", name, res)
		}
		if len(fake.calls) != 0 {
			t.Errorf("CheckApex(%q): обращений к резолверу %d, ожидалось 0", name, len(fake.calls))
		}
	}
}

// Тесты клиента. Два уровня, как требует задача:
//
//   - транспортный шов exchangeFunc — ретраи, TC→TCP-fallback, маппинг
//     RCODE, отмена ctx, счётчики попыток (сети нет вообще);
//   - локальный DNS-сервер miekg/dns на 127.0.0.1:0 (петля, не внешний
//     стек) — реальный провод UDP и TCP: упаковка, чтение, пропуск
//     мусора и поздних ответов, дедлайны.

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

// --- вспомогательные конструкторы ------------------------------------------

// fakeCall — зафиксированный вызов транспорта.
type fakeCall struct {
	m   *dns.Msg
	tcp bool
}

// newFakeClient — клиент с подменённым транспортом: без сети и пула.
// modify накладывает параметры поверх нулевых (потом withDefaults).
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

// fakeNetErr — сетевая ошибка с управляемым признаком таймаута.
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

// makeReply — ответ на запрос q с кодом и секцией answer.
func makeReply(q *dns.Msg, rcode int, an ...dns.RR) *dns.Msg {
	m := new(dns.Msg)
	m.SetReply(q)
	m.Rcode = rcode
	m.Answer = an
	return m
}

// --- транспортный шов: контракт Exchange -----------------------------------

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
		t.Fatalf("rcode: получен %v", res.RCode)
	}
	if got := res.RecordsOf(dns.TypeA); len(got) != 1 {
		t.Fatalf("записей A: %d, ожидалась 1", len(got))
	} else if got[0].(*dns.A).A.String() != "192.0.2.1" {
		t.Fatalf("адрес A: %s", got[0].(*dns.A).A)
	}
	if got := res.RecordsOf(dns.TypeCNAME); len(got) != 1 {
		t.Fatalf("записей CNAME: %d, ожидалась 1", len(got))
	}
	if got := res.RecordsOf(dns.TypeAAAA); len(got) != 0 {
		t.Fatalf("записей AAAA не должно быть, есть %d", len(got))
	}
	if res.TCP || res.Attempts != 1 {
		t.Fatalf("попытки/TCP: attempts=%d tcp=%v", res.Attempts, res.TCP)
	}

	// Вопрос ушёл на транспорт каноничным: FQDN, тип, класс, рекурсия.
	if len(*calls) != 1 {
		t.Fatalf("вызовов транспорта: %d", len(*calls))
	}
	call := (*calls)[0]
	if call.tcp {
		t.Fatal("первый обмен обязан быть UDP")
	}
	q := call.m.Question[0]
	if q.Name != "example.com." || q.Qtype != dns.TypeA || q.Qclass != dns.ClassINET {
		t.Fatalf("вопрос на транспорте: %+v", q)
	}
	if !call.m.RecursionDesired {
		t.Fatal("RecursionDesired должен быть выставлен (резолвер рекурсивный)")
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
				t.Fatalf("RCODE — не ошибка, получено: %v", err)
			}
			if res.RCode != tc.want {
				t.Fatalf("rcode: %v, ожидался %v", res.RCode, tc.want)
			}
			if res.RCode.String() != dns.RcodeToString[tc.rcode] {
				t.Fatalf("String(): %q, ожидалось %q", res.RCode, dns.RcodeToString[tc.rcode])
			}
		})
	}
	// Расширенный код — числом с говорящей мнемоникой.
	if got := RCode(42).String(); got != "RCODE42" {
		t.Fatalf("неизвестный код: %q", got)
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
		t.Fatal("итоговый ответ обязан прийти по TCP")
	}
	if res.Attempts != 2 {
		t.Fatalf("обменов: %d, ожидалось 2 (UDP + TCP)", res.Attempts)
	}
	if len(res.Answer) != len(full) {
		t.Fatalf("записей в ответе: %d, ожидалось %d", len(res.Answer), len(full))
	}
	if len(*calls) != 2 || (*calls)[0].tcp || !(*calls)[1].tcp {
		t.Fatalf("порядок обменов: сначала UDP, затем TCP; получено %+v", *calls)
	}
	// TCP повторяет тот же вопрос (имя и тип), а не что-то иное.
	q1 := (*calls)[0].m.Question[0]
	q2 := (*calls)[1].m.Question[0]
	if q1 != q2 {
		t.Fatalf("вопрос изменился между UDP и TCP: %+v → %+v", q1, q2)
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
		t.Fatal("ошибка TCP-fallback обязана провалить запрос")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("ошибка не оборачивает причину: %v", err)
	}
	if !strings.Contains(err.Error(), "tcp-fallback") {
		t.Fatalf("в сообщении нет ветки tcp-fallback: %v", err)
	}
	// Две попытки × (UDP + TCP) = 4 обмена: TCP-ошибка роняет попытку
	// целиком, цикл начинается заново с UDP.
	if n := len(*calls); n != 4 {
		t.Fatalf("обменов: %d, ожидалось 4", n)
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
		t.Fatalf("Exchange после ретраев: %v", err)
	}
	if res.Attempts != 3 {
		t.Fatalf("обменов: %d, ожидалось 3", res.Attempts)
	}
	// Две паузы по 25 мс между тремя попытками обязаны быть в Elapsed.
	if min := 50 * time.Millisecond; res.Elapsed < min {
		t.Fatalf("Elapsed %s меньше суммы пауз %s", res.Elapsed, min)
	}
	if since := time.Since(start); since < 50*time.Millisecond {
		t.Fatalf("реальное время обмена %s меньше суммы пауз", since)
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
		t.Fatal("исчерпание попыток — ошибка")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("ошибка не оборачивает последнюю причину: %v", err)
	}
	if !strings.Contains(err.Error(), "исчерпаны попытки") || !strings.Contains(err.Error(), "example.com") {
		t.Fatalf("сообщение без контекста: %v", err)
	}
	if n := len(*calls); n != 3 { // первая + 2 повтора
		t.Fatalf("обменов: %d, ожидалось 3", n)
	}
}

func TestExchangeCancelledDuringRetryDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c, calls := newFakeClient(t, func(o *Options) {
		o.Retries = 2
		o.RetryDelay = 10 * time.Second // пауза, в которой придёт отмена
	}, func(_ context.Context, _ *dns.Msg, _ bool) (*dns.Msg, error) {
		// Первая попытка падает и одновременно отменяет внешний ctx:
		// Exchange обязан выйти в паузе, не делая вторую попытку.
		time.AfterFunc(10*time.Millisecond, cancel)
		return nil, fakeNetErr{timeout: true}
	})

	start := time.Now()
	_, err := c.Exchange(ctx, Question{Name: "example.com", Type: dns.TypeA})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ожидается context.Canceled, получено %v", err)
	}
	if n := len(*calls); n != 1 {
		t.Fatalf("после отмены не должно быть новых попыток, обменов %d", n)
	}
	if since := time.Since(start); since > time.Second {
		t.Fatalf("отмена в паузе не прерывает Exchange сразу: %s", since)
	}
}

func TestExchangeCancelledBeforeStart(t *testing.T) {
	c, calls := newFakeClient(t, nil, func(_ context.Context, _ *dns.Msg, _ bool) (*dns.Msg, error) {
		t.Error("транспорт не должен вызываться при отменённом ctx")
		return nil, errors.New("unreachable")
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Exchange(ctx, Question{Name: "example.com", Type: dns.TypeA}); !errors.Is(err, context.Canceled) {
		t.Fatalf("ожидается context.Canceled, получено %v", err)
	}
	if len(*calls) != 0 {
		t.Fatal("транспорт вызван при отменённом ctx")
	}
}

func TestExchangeQuestionValidation(t *testing.T) {
	cases := []struct {
		name string
		q    Question
		want string
	}{
		{"пустое имя", Question{Name: "", Type: dns.TypeA}, "имя вопроса пусто"},
		{"пробельное имя", Question{Name: "  ", Type: dns.TypeA}, "имя вопроса пусто"},
		{"нулевой тип", Question{Name: "example.com", Type: 0}, "неизвестный тип записи"},
		{"несуществующий тип", Question{Name: "example.com", Type: 0xFFFF}, "неизвестный тип записи"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, calls := newFakeClient(t, nil, func(_ context.Context, _ *dns.Msg, _ bool) (*dns.Msg, error) {
				t.Error("некорректный вопрос не должен попадать на транспорт")
				return nil, errors.New("unreachable")
			})
			_, err := c.Exchange(context.Background(), tc.q)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ожидается ошибка %q, получено %v", tc.want, err)
			}
			if len(*calls) != 0 {
				t.Fatal("транспорт вызван для некорректного вопроса")
			}
		})
	}
}

func TestExchangeBadNameFailsPack(t *testing.T) {
	// Метка длиннее 63 октетов не упаковывается в wire-формат — ошибка
	// детерминирована, до сети дело не доходит.
	long := strings.Repeat("a", 64) + ".example.com"
	c, calls := newFakeClient(t, nil, func(_ context.Context, _ *dns.Msg, _ bool) (*dns.Msg, error) {
		t.Error("транспорт не должен вызываться для неупаковываемого имени")
		return nil, errors.New("unreachable")
	})
	if _, err := c.Exchange(context.Background(), Question{Name: long, Type: dns.TypeA}); err == nil {
		t.Fatal("ожидается ошибка упаковки")
	} else if !strings.Contains(err.Error(), "некорректное имя вопроса") {
		t.Fatalf("сообщение без контекста упаковки: %v", err)
	}
	if len(*calls) != 0 {
		t.Fatal("транспорт вызван для неупаковываемого имени")
	}
}

func TestExchangeQuestionFqdnPassthrough(t *testing.T) {
	c, calls := newFakeClient(t, nil, func(_ context.Context, m *dns.Msg, _ bool) (*dns.Msg, error) {
		return makeReply(m, dns.RcodeSuccess, nsRR("example.com.", "ns1.example.com.")), nil
	})
	// Имя без завершающей точки дополняется, с точкой — не меняется.
	for _, name := range []string{"example.com", "example.com."} {
		if _, err := c.Exchange(context.Background(), Question{Name: name, Type: dns.TypeNS}); err != nil {
			t.Fatalf("Exchange(%q): %v", name, err)
		}
	}
	for i, want := range []string{"example.com.", "example.com."} {
		if got := (*calls)[i].m.Question[0].Name; got != want {
			t.Fatalf("вопрос[%d] на транспорте %q, ожидался %q", i, got, want)
		}
	}
}

// TestExchangeEDNS0 — запрос несёт OPT RR (RFC 6891) с advertised
// размером DefaultEDNS0UDPSize и без DO-бита: большие ответы приходят
// целиком по UDP, а не с TC=1 и TCP-fallback в каждой попытке (DNS-07).
func TestExchangeEDNS0(t *testing.T) {
	c, calls := newFakeClient(t, nil, func(_ context.Context, m *dns.Msg, _ bool) (*dns.Msg, error) {
		return makeReply(m, dns.RcodeSuccess, aRR("example.com.", "192.0.2.1")), nil
	})
	if _, err := c.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA}); err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if len(*calls) != 1 {
		t.Fatalf("вызовов транспорта: %d, want 1", len(*calls))
	}
	var opt *dns.OPT
	for _, rr := range (*calls)[0].m.Extra {
		if o, ok := rr.(*dns.OPT); ok {
			opt = o
			break
		}
	}
	if opt == nil {
		t.Fatal("запрос без OPT RR — EDNS0 не запрашивается")
	}
	if opt.UDPSize() != DefaultEDNS0UDPSize {
		t.Fatalf("OPT UDPSize = %d, want %d", opt.UDPSize(), DefaultEDNS0UDPSize)
	}
	if opt.Do() {
		t.Fatal("DO-бит не выставляется: DNSSEC не запрашивается")
	}
}

// --- конфигурация -----------------------------------------------------------

func TestOptionsDefaultsAndValidation(t *testing.T) {
	def := Options{Addr: "127.0.0.1:53"}.withDefaults()
	if def.Timeout != DefaultTimeout || def.TCPTimeout != DefaultTCPTimeout {
		t.Fatalf("таймауты по умолчанию: %+v", def)
	}
	if def.Retries != DefaultRetries || def.RetryDelay != DefaultRetryDelay {
		t.Fatalf("ретраи по умолчанию: %+v", def)
	}
	if def.UDPPoolSize != DefaultUDPPoolSize {
		t.Fatalf("пул по умолчанию: %d", def.UDPPoolSize)
	}
	if err := def.validate(); err != nil {
		t.Fatalf("дефолты обязаны проходить валидацию: %v", err)
	}

	// Нормализация не исправляет осмысленно: кривой адрес и перебор
	// ретраев остаются ошибками (отрицательный размер пула и таймауты
	// withDefaults заменяет дефолтами — как db.Options).
	bad := []Options{
		{},                  // пустой адрес
		{Addr: "127.0.0.1"}, // без порта
		{Addr: "127.0.0.1:53", Retries: MaxRetries + 1},
	}
	for i, o := range bad {
		if err := o.withDefaults().validate(); err == nil {
			t.Fatalf("вариант %d (%+v) обязан падать валидацией", i, o)
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
		t.Fatalf("записи AAAA: %+v", got)
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
		Addr:        "10.0.0.2:5353", // первый резолвер (unbound)
		Timeout:     3 * time.Second,
		TCPTimeout:  7 * time.Second,
		Retries:     4,
		RetryDelay:  250 * time.Millisecond,
		UDPPoolSize: 64,
	}
	if got != want {
		t.Fatalf("NewOptions: %+v, ожидалось %+v", got, want)
	}
	// Пустой список резолверов — пустой адрес: валидацию поймает
	// NewClient (конфиг гарантирует непустоту, ТЗ 5.10).
	if got := (NewOptions(config.DNS{})).Addr; got != "" {
		t.Fatalf("без резолверов Addr обязан быть пустым, получен %q", got)
	}
}

func TestNewClientValidation(t *testing.T) {
	if _, err := NewClient(context.Background(), Options{Addr: "nohostport", UDPPoolSize: 1}); err == nil {
		t.Fatal("кривой адрес: ожидается ошибка")
	}
	if _, err := NewClient(context.Background(), Options{Addr: "127.0.0.1:99999", UDPPoolSize: 1}); err == nil {
		t.Fatal("недоступный порт: ожидается ошибка открытия пула (fail-fast)")
	}
}

// --- локальный DNS-сервер (петля) ------------------------------------------

// isTCPWriter определяет протокол запроса по адресу клиента: у UDP это
// *net.UDPAddr, у TCP — *net.TCPAddr (другого способа узнать протокол
// из Handler в miekg/dns нет).
func isTCPWriter(w dns.ResponseWriter) bool {
	_, ok := w.RemoteAddr().(*net.TCPAddr)
	return ok
}

// spinLocalDNS поднимает DNS-сервер на 127.0.0.1:0, UDP и TCP на одном
// порту (как настоящий резолвер), и возвращает адрес.
func spinLocalDNS(t *testing.T, h dns.Handler) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	lc, err := net.Listen("tcp", pc.LocalAddr().String())
	if err != nil {
		_ = pc.Close()
		t.Fatalf("listen tcp на порту udp: %v", err)
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

// newLocalClient — настоящий клиент (пул сокетов) к локальному серверу.
func newLocalClient(t *testing.T, addr string, modify func(*Options)) *Client {
	t.Helper()
	opts := Options{
		Addr:        addr,
		Timeout:     time.Second,
		TCPTimeout:  time.Second,
		Retries:     1, // без ретраев по умолчанию: детерминированное время теста
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
			t.Error("запрос не обязан уходить на TCP")
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
		t.Fatalf("итог: %+v", res)
	}
	got := res.RecordsOf(dns.TypeA)
	if len(got) != 1 || got[0].(*dns.A).A.String() != "192.0.2.10" {
		t.Fatalf("записи A: %+v", got)
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
			// UDP: короткий ответ с флагом усечения (TC=1).
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
		t.Fatalf("ожидался fallback на TCP: attempts=%d tcp=%v", res.Attempts, res.TCP)
	}
	if len(res.Answer) != len(full) {
		t.Fatalf("записей: %d, ожидалось %d", len(res.Answer), len(full))
	}
	if got := res.RecordsOf(dns.TypeNS); len(got) != 1 {
		t.Fatalf("NS-запись не дошла по TCP: %+v", got)
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
			t.Fatalf("%s: RCODE — не ошибка: %v", name, err)
		}
		if res.RCode != want {
			t.Fatalf("%s: rcode %v, ожидался %v", name, res.RCode, want)
		}
		if name == "nx.example.com" && len(res.Ns) != 1 {
			t.Fatalf("секция authority (SOA/NS) не перенесена: %+v", res.Ns)
		}
	}
}

// TestLocalTimeout — молчащий сервер: попытка истекает по таймауту,
// ретраи отрабатывают, итог — ошибка с признаком таймаута.
func TestLocalTimeout(t *testing.T) {
	addr := loopUDPAddr(t) // приёмник читает и молчит
	c := newLocalClient(t, addr, func(o *Options) {
		o.Timeout = 80 * time.Millisecond
		o.Retries = 1
		o.RetryDelay = 5 * time.Millisecond
	})

	start := time.Now()
	_, err := c.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
	if err == nil {
		t.Fatal("молчащий сервер: ожидается ошибка")
	}
	if !IsTimeout(err) {
		t.Fatalf("ошибка не распознана как таймаут: %v", err)
	}
	if since := time.Since(start); since < 160*time.Millisecond {
		t.Fatalf("две попытки по 80 мс обязаны уложиться в реальное время: %s", since)
	}
}

// TestLocalSkipsGarbageAndStaleAnswers — кривые датаграммы и ответы с
// чужим Id не роняют обмен и не выдаются за ответ: чтение продолжается
// до собственного дедлайна.
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
			// На каждый запрос: мусор, затем валидное сообщение с чужим
			// Id (поздний ответ прежнего обмена), затем тишина.
			if _, err := pc.WriteTo([]byte("this is not a dns message"), peer); err != nil {
				return
			}
			reqID := binary.BigEndian.Uint16(buf[:2])
			stale := new(dns.Msg)
			stale.SetQuestion("stale.example.com.", dns.TypeA)
			stale.Id = reqID ^ 0xFFFF // гарантированно чужой
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
		o.Retries = 1 // две попытки — обе с мусором вместо ответа
	})

	_, err = c.Exchange(context.Background(), Question{Name: "example.com", Type: dns.TypeA})
	if err == nil {
		t.Fatal("валидного ответа не было — ожидается ошибка")
	}
	if !IsTimeout(err) {
		t.Fatalf("ожидается таймаут после пропуска мусора, получено: %v", err)
	}
}

// TestLocalConcurrentExchange — несколько Exchange одновременно на
// одном клиенте (пул раздаёт сокеты; гонки ловит -race).
func TestLocalConcurrentExchange(t *testing.T) {
	addr := spinLocalDNS(t, dns.HandlerFunc(func(w dns.ResponseWriter, r *dns.Msg) {
		// Ответ зависит от имени: каждая горутина проверяет свой адрес.
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
				t.Errorf("Exchange(%s): неожиданный итог %+v", name, res)
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
		{"прочая", errors.New("boom"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsTimeout(tc.err); got != tc.want {
				t.Fatalf("IsTimeout(%v) = %v, ожидалось %v", tc.err, got, tc.want)
			}
		})
	}
}

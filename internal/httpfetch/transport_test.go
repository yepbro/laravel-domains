// Тесты транспорта. Три уровня:
//
//   - шов dialFunc — гонка адресов (happy-eyeballs-подобная), фолбэк,
//     connect-таймаут, без сети вообще;
//   - httptest.Server на 127.0.0.1 (петля, не внешний стек) — базовый
//     GET, подключение по IP с чужим Host/SNI (TLS), резолв-хук
//     редиректов, таймауты слоёв (тарпиты на заголовках и теле),
//     отмена ctx, отказ соединения;
//   - сырой net.Listener для TLS-тарпита (рукопожатие без ответа).

package httpfetch

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"crawler/internal/config"
)

// --- вспомогательные --------------------------------------------------------

// loopbackAddr — адрес предрезолвленного хоста в тестах: httptest и
// сырые слушатели живут на петле.
func loopbackAddr() []netip.Addr {
	return []netip.Addr{netip.MustParseAddr("127.0.0.1")}
}

// hostURL заменяет хост в базовом URL тестового сервера на имя,
// которого нет ни в одном резолвере: подключение обязано идти по
// переданным адресам, а не по DNS. Порт сохраняется.
func hostURL(base, host string) *url.URL {
	u, err := url.Parse(base)
	if err != nil {
		panic(fmt.Sprintf("тестовый URL %q: %v", base, err))
	}
	_, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		panic(fmt.Sprintf("тестовый URL %q без порта: %v", base, err))
	}
	u.Host = net.JoinHostPort(host, port)
	return u
}

// newTestFetcher — транспорт с подменённым диалом (шов тестов) и
// записью попыток (потокобезопасной: попытки стартуют горутинами).
func newTestFetcher(t *testing.T, opts Options, dial dialFunc) (*Fetcher, func() []netip.Addr) {
	t.Helper()
	f := NewFetcher(opts)
	var mu sync.Mutex
	calls := []netip.Addr{}
	f.dial = func(ctx context.Context, addr netip.Addr, port string) (net.Conn, error) {
		mu.Lock()
		calls = append(calls, addr)
		mu.Unlock()
		return dial(ctx, addr, port)
	}
	return f, func() []netip.Addr {
		mu.Lock()
		defer mu.Unlock()
		return append([]netip.Addr(nil), calls...)
	}
}

// --- шов диала: гонка адресов ------------------------------------------------

// TestDialAddrsFallback: первый адрес молчит (SYN в пустоту), второй
// жив — подключение проходит после паузы FallbackDelay, висящая
// попытка отменяется.
func TestDialAddrsFallback(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("слушатель: %v", err)
	}
	defer func() { _ = ln.Close() }()

	released := make(chan struct{})
	var once sync.Once
	f, calls := newTestFetcher(t, Options{
		ConnectTimeout: 2 * time.Second,
		FallbackDelay:  40 * time.Millisecond,
	}, func(ctx context.Context, addr netip.Addr, port string) (net.Conn, error) {
		if addr == netip.MustParseAddr("203.0.113.1") { // TEST-NET-3: чёрная дыра
			<-ctx.Done() // молчим до отмены попытки
			once.Do(func() { close(released) })
			return nil, ctx.Err()
		}
		var d net.Dialer
		return d.DialContext(ctx, "tcp", ln.Addr().String())
	})

	start := time.Now()
	conn, err := f.dialAddrs(context.Background(),
		[]netip.Addr{netip.MustParseAddr("203.0.113.1"), netip.MustParseAddr("192.0.2.10")}, "80")
	elapsed := time.Since(start)
	if err != nil || conn == nil {
		t.Fatalf("ожидалось соединение через второй адрес: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if elapsed < 40*time.Millisecond {
		t.Errorf("фолбэк раньше паузы FallbackDelay: %s", elapsed)
	}
	got := calls()
	if len(got) != 2 {
		t.Errorf("попытки подключения: ожидалось 2 адреса, получено %v", got)
	}
	// Висящая попытка обязана отмениться возвратом победителя.
	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Errorf("проигравшая попытка не отменена (утечка диала)")
	}
}

// TestDialAddrsFirstWins: успешный первый адрес — второй не стартует.
func TestDialAddrsFirstWins(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("слушатель: %v", err)
	}
	defer func() { _ = ln.Close() }()

	f, calls := newTestFetcher(t, Options{
		ConnectTimeout: 2 * time.Second,
		FallbackDelay:  250 * time.Millisecond, // успели бы запустить второй
	}, func(ctx context.Context, addr netip.Addr, port string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", ln.Addr().String())
	})

	conn, err := f.dialAddrs(context.Background(),
		[]netip.Addr{netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("192.0.2.2")}, "80")
	if err != nil || conn == nil {
		t.Fatalf("ожидалось соединение: %v", err)
	}
	_ = conn.Close()
	if got := calls(); len(got) != 1 {
		t.Errorf("после успеха первого адреса запущено попыток: %d (ожидалась 1)", len(got))
	}
}

// TestDialAddrsAllFailImmediate: падение всех запущенных попыток
// стартует следующую без паузы — последовательный фолбэк без латентности.
func TestDialAddrsAllFailImmediate(t *testing.T) {
	boom := errors.New("сеть недоступна")
	f, calls := newTestFetcher(t, Options{
		ConnectTimeout: 2 * time.Second,
		FallbackDelay:  500 * time.Millisecond, // пауза не должна сработать
	}, func(ctx context.Context, addr netip.Addr, port string) (net.Conn, error) {
		return nil, boom
	})

	start := time.Now()
	_, err := f.dialAddrs(context.Background(),
		[]netip.Addr{netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("192.0.2.2"), netip.MustParseAddr("192.0.2.3")}, "80")
	elapsed := time.Since(start)
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("ожидалась ошибка последней попытки: %v", err)
	}
	if got := calls(); len(got) != 3 {
		t.Errorf("попыток: %d, ожидалось 3 (все адреса)", len(got))
	}
	if elapsed >= 500*time.Millisecond {
		t.Errorf("фолбэк ждал паузу при мгновенных отказах: %s", elapsed)
	}
}

// TestDialAddrsConnectTimeout: слой connect-таймаута — попытка,
// не уложившаяся в ConnectTimeout, прерывается (детерминированно через
// диал, уважающий ctx).
func TestDialAddrsConnectTimeout(t *testing.T) {
	f, calls := newTestFetcher(t, Options{
		ConnectTimeout: 80 * time.Millisecond,
		FallbackDelay:  10 * time.Millisecond,
	}, func(ctx context.Context, addr netip.Addr, port string) (net.Conn, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Second):
			return nil, errors.New("диал не был прерван")
		}
	})

	start := time.Now()
	_, err := f.dialAddrs(context.Background(),
		[]netip.Addr{netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("192.0.2.2")}, "80")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("ожидалась ошибка connect-таймаута")
	}
	if !IsTimeout(err) {
		t.Errorf("connect-таймаут не распознан IsTimeout: %v", err)
	}
	if got := calls(); len(got) != 2 {
		t.Errorf("попыток: %d, ожидалось 2", len(got))
	}
	if elapsed >= 2*time.Second {
		t.Errorf("connect-таймаут сработал слишком поздно: %s", elapsed)
	}
}

// TestDialAddrsCancelAfterWinner: отмена ctx в момент победы попытки —
// select может взять ветку отмены уже после того, как попытка положила
// соединение в winner. Проигравший выход обязан закрыть это соединение
// (вместе со слотами лимитов limitConn), а не утекать: слоты глобального
// семафора GC не собирает, накопившись, они останавливают транспорт.
func TestDialAddrsCancelAfterWinner(t *testing.T) {
	var mu sync.Mutex
	created, closed := 0, 0
	newConn := func() net.Conn {
		c1, _ := net.Pipe()
		mu.Lock()
		created++
		mu.Unlock()
		return &closeCountingConn{Conn: c1, onClose: func() {
			mu.Lock()
			closed++
			mu.Unlock()
		}}
	}
	f, _ := newTestFetcher(t, Options{
		ConnectTimeout: 2 * time.Second,
		FallbackDelay:  time.Hour, // второй адрес не стартует — в гонке одна попытка
	}, func(ctx context.Context, addr netip.Addr, port string) (net.Conn, error) {
		<-ctx.Done() // «победа» прибывает вместе с отменой — окно утечки
		return newConn(), nil
	})

	addrs := []netip.Addr{netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("192.0.2.2")}
	cancelled := 0
	for i := 0; i < 64; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(time.Millisecond, cancel)
		conn, err := f.dialAddrs(ctx, addrs, "80")
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				cancel()
				t.Fatalf("ожидалась ошибка отмены, получено: %v", err)
			}
			cancelled++
			continue
		}
		_ = conn.Close() // победитель отдан вызывающему — закрывает он
		cancel()
	}
	if cancelled == 0 {
		t.Fatal("гонка не выполнилась: ни одного выхода по отмене")
	}
	// Закрытие проигравших/победителя отмены асинхронно: attempt-горутина
	// последней итерации может закрывать соединение уже после возврата
	// dialAddrs. ждём дренажа, а не фиксируем мгновенный срез.
	deadline := time.Now().Add(2 * time.Second)
	mu.Lock()
	for closed != created && time.Now().Before(deadline) {
		mu.Unlock()
		time.Sleep(time.Millisecond)
		mu.Lock()
	}
	c, d := closed, created
	mu.Unlock()
	if c != d {
		t.Errorf("закрыто %d из %d соединений — утечка победителя по отмене ctx", c, d)
	}
}

// closeCountingConn — соединение с подсчётом Close (утечки по отмене).
type closeCountingConn struct {
	net.Conn
	onClose func()
}

func (c *closeCountingConn) Close() error {
	c.onClose()
	return c.Conn.Close()
}

// TestDialAddrsEmptyAndSingle — границы списка адресов.
func TestDialAddrsEmptyAndSingle(t *testing.T) {
	f := NewFetcher(Options{})
	if _, err := f.dialAddrs(context.Background(), nil, "80"); err == nil {
		t.Error("пустой список адресов должен давать ошибку")
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("слушатель: %v", err)
	}
	defer func() { _ = ln.Close() }()
	conn, err := f.dialAddrs(context.Background(), loopbackAddr(),
		strconv.Itoa(ln.Addr().(*net.TCPAddr).Port))
	if err != nil {
		t.Fatalf("единственный адрес: %v", err)
	}
	_ = conn.Close()
}

// --- базовый GET -------------------------------------------------------------

func TestFetchBasic(t *testing.T) {
	var gotHost, gotUA atomicString
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost.Store(r.Host)
		gotUA.Store(r.Header.Get("User-Agent"))
		w.Header().Set("X-Stage", "http")
		_, _ = fmt.Fprint(w, "hello")
	}))
	defer srv.Close()

	f := NewFetcher(Options{UserAgent: "TestCrawler/1.0 (contact: test@example.invalid)"})
	u := hostURL(srv.URL, "www.example-test.net")
	res, err := f.Fetch(context.Background(), FetchRequest{URL: u, Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != http.StatusOK || res.Status != "200 OK" {
		t.Errorf("статус: %d %q", res.StatusCode, res.Status)
	}
	if res.Header.Get("X-Stage") != "http" {
		t.Errorf("заголовок ответа не перенесён: %v", res.Header)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("чтение тела: %v", err)
	}
	if string(body) != "hello" {
		t.Errorf("тело: %q", body)
	}
	if res.FinalURL.String() != u.String() {
		t.Errorf("итоговый URL: %s, ожидался %s", res.FinalURL, u)
	}
	if res.TLS != nil {
		t.Error("http-ответ не должен содержать TLS-состояние")
	}
	if res.Elapsed <= 0 || res.Elapsed > 2*time.Second {
		t.Errorf("время ответа вне разумных границ: %s", res.Elapsed)
	}
	// Хост запроса — из URL, подключение — по адресу DNS-этапа.
	if host, _, _ := net.SplitHostPort(gotHost.Load()); host != "www.example-test.net" {
		t.Errorf("Host-заголовок: %q, ожидался www.example-test.net", gotHost.Load())
	}
	if gotUA.Load() != "TestCrawler/1.0 (contact: test@example.invalid)" {
		t.Errorf("User-Agent: %q", gotUA.Load())
	}
}

// TestFetchUsesProvidedAddrs: адреса DNS-этапа доходят до диала,
// резолвер не вызывается вовсе.
func TestFetchUsesProvidedAddrs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	resolverCalls := 0
	f, calls := newTestFetcher(t, Options{UserAgent: "t"}, func(ctx context.Context, addr netip.Addr, port string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", port))
	})
	f.res = ResolverFunc(func(ctx context.Context, host string) ([]netip.Addr, error) {
		resolverCalls++
		return nil, errors.New("резолвер не должен вызываться")
	})

	res, err := f.Fetch(context.Background(), FetchRequest{URL: hostURL(srv.URL, "main.example.net"), Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	_ = res.Body.Close()
	if got := calls(); len(got) != 1 || got[0] != netip.MustParseAddr("127.0.0.1") {
		t.Errorf("диал по адресам DNS-этапа: %v", got)
	}
	if resolverCalls != 0 {
		t.Errorf("резолвер вызван %d раз при наличии адресов", resolverCalls)
	}
}

// --- Host/SNI поверх предрезолвленного IP (TLS) ------------------------------

func TestFetchHTTPS_HostAndSNIOverResolvedIP(t *testing.T) {
	sniCh := make(chan string, 1)
	var gotHost atomicString
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost.Store(r.Host)
		_, _ = fmt.Fprint(w, "secure")
	}))
	srv.TLS = &tls.Config{
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			select {
			case sniCh <- hello.ServerName:
			default:
			}
			return nil, nil // конфиг сервера не меняем
		},
	}
	srv.StartTLS()
	defer srv.Close()

	f := NewFetcher(Options{
		// Проверка сертификата не предмет теста: SNI до него не доходит,
		// а InsecureSkipVerify не отключает отправку имени (T-050).
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	})
	u := hostURL(srv.URL, "sni-probe.example.net")
	res, err := f.Fetch(context.Background(), FetchRequest{URL: u, Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("Fetch https: %v", err)
	}
	defer func() { _ = res.Body.Close() }()

	body, _ := io.ReadAll(res.Body)
	if string(body) != "secure" {
		t.Errorf("тело: %q", body)
	}
	if res.TLS == nil {
		t.Error("TLS-соединение не установлено")
	}
	select {
	case sni := <-sniCh:
		if sni != "sni-probe.example.net" {
			t.Errorf("SNI: %q, ожидался sni-probe.example.net", sni)
		}
	default:
		t.Error("ServerName не отправлен (SNI отсутствует)")
	}
	if host, _, _ := net.SplitHostPort(gotHost.Load()); host != "sni-probe.example.net" {
		t.Errorf("Host: %q, ожидался sni-probe.example.net", gotHost.Load())
	}
}

// --- резолв-хук редирект-хостов ----------------------------------------------

// TestRoundTripResolvesHostsWithoutStageAddrs: запрос без адресов
// DNS-этапа в ctx (редирект на другой хост — T-048, или http.Client
// поверх транспорта) подключается через Resolver; кэш сворачивает
// повторные запросы к одному хосту в один резолв.
func TestRoundTripResolvesHostsWithoutStageAddrs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	newReq := func() *http.Request {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
			hostURL(srv.URL, "redirect-target.example.net").String(), nil)
		if err != nil {
			t.Fatalf("запрос: %v", err)
		}
		return req
	}

	calls := 0
	plain := ResolverFunc(func(ctx context.Context, host string) ([]netip.Addr, error) {
		calls++
		if host != "redirect-target.example.net" {
			t.Errorf("резолв неожидаемого хоста: %q", host)
		}
		return loopbackAddr(), nil
	})

	f := NewFetcher(Options{Resolver: plain})
	for i := 0; i < 2; i++ {
		resp, err := f.RoundTrip(newReq())
		if err != nil {
			t.Fatalf("RoundTrip #%d: %v", i+1, err)
		}
		_ = resp.Body.Close()
	}
	if calls != 2 {
		t.Errorf("без кэша: резолвов %d, ожидалось 2", calls)
	}

	cached := 0
	f2 := NewFetcher(Options{Resolver: NewCachingResolver(
		ResolverFunc(func(ctx context.Context, host string) ([]netip.Addr, error) {
			cached++
			return loopbackAddr(), nil
		}), CacheOptions{TTL: time.Minute})})
	for i := 0; i < 2; i++ {
		resp, err := f2.RoundTrip(newReq())
		if err != nil {
			t.Fatalf("RoundTrip с кэшем #%d: %v", i+1, err)
		}
		_ = resp.Body.Close()
	}
	if cached != 1 {
		t.Errorf("с кэшем: резолвов %d, ожидался 1", cached)
	}
}

// TestDialContextRedirectIPLiteral: Location с IP-литералом не трогает
// резолвер.
func TestDialContextRedirectIPLiteral(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	_, port, _ := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	resolverCalls := 0
	f := NewFetcher(Options{Resolver: ResolverFunc(func(ctx context.Context, host string) ([]netip.Addr, error) {
		resolverCalls++
		return nil, errors.New("резолвер не нужен для IP-литерала")
	})})

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		"http://127.0.0.1:"+port+"/", nil)
	if err != nil {
		t.Fatalf("запрос: %v", err)
	}
	resp, err := f.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip к IP-литералу: %v", err)
	}
	_ = resp.Body.Close()
	if resolverCalls != 0 {
		t.Errorf("IP-литерал вызвал резолвер (%d раз)", resolverCalls)
	}
}

// --- SSRF-защита пути редиректов ---------------------------------------------

// TestLookupBlocksInternalAddrs: Options.BlockInternalAddrs отбраковывает
// в воронке lookup внутренние адреса — литералы из Location (SSRF:
// loopback, RFC1918, cloud metadata) и адреса резолва; внешний адрес
// смешанного ответа остаётся.
func TestLookupBlocksInternalAddrs(t *testing.T) {
	f := NewFetcher(Options{
		BlockInternalAddrs: true,
		Resolver: ResolverFunc(func(ctx context.Context, host string) ([]netip.Addr, error) {
			return []netip.Addr{
				netip.MustParseAddr("192.0.2.10"),      // TEST-NET-1 — не внутренний
				netip.MustParseAddr("10.0.0.5"),        // RFC1918
				netip.MustParseAddr("169.254.169.254"), // cloud metadata
			}, nil
		}),
	})

	addrs, err := f.lookup(context.Background(), "mixed.example.net")
	if err != nil {
		t.Fatalf("резолв со внешним адресом: %v", err)
	}
	if len(addrs) != 1 || addrs[0] != netip.MustParseAddr("192.0.2.10") {
		t.Errorf("ожидался только внешний адрес, получено %v", addrs)
	}

	for _, host := range []string{"127.0.0.1", "10.0.0.5", "169.254.169.254", "::1", "fd00::1", "192.168.1.1"} {
		if _, err := f.lookup(context.Background(), host); err == nil {
			t.Errorf("внутренний литерал %q не заблокирован", host)
		}
	}
	f2 := NewFetcher(Options{
		BlockInternalAddrs: true,
		Resolver: ResolverFunc(func(ctx context.Context, host string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr("172.16.0.1")}, nil
		}),
	})
	if _, err := f2.lookup(context.Background(), "internal-only.example.net"); err == nil {
		t.Error("ответ только с внутренними адресами не заблокирован")
	}

	// Флаг выключен — прежнее поведение: адреса проходят как есть.
	f3 := NewFetcher(Options{
		Resolver: ResolverFunc(func(ctx context.Context, host string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr("10.0.0.5")}, nil
		}),
	})
	if addrs, err := f3.lookup(context.Background(), "legacy.example.net"); err != nil || len(addrs) != 1 {
		t.Errorf("без флага внутренние адреса должны проходить: %v %v", addrs, err)
	}
}

// TestFetchChainBlocksInternalRedirect: Location на внутренний адрес —
// переход не выполняется (ошибка резолва, цепочка — выполненная часть);
// адреса DNS-этапа стартового хоста фильтр не затрагивает.
func TestFetchChainBlocksInternalRedirect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "http://169.254.169.254/latest/meta-data/")
		w.WriteHeader(http.StatusFound)
	}))
	defer srv.Close()

	// Стартовый запрос идёт по адресам DNS-этапа (петля!) — не фильтруется.
	f := NewFetcher(Options{BlockInternalAddrs: true, Resolver: redirectResolverFail})
	got, err := FetchChain(context.Background(), f, FetchRequest{
		URL: redirectReqURL(srv.URL, "ssrf.example.net", "/start"), Addrs: loopbackAddr()})
	if err == nil || !strings.Contains(err.Error(), "внутренние") {
		t.Fatalf("ожидалась блокировка внутреннего адреса, получено: %v", err)
	}
	if len(got.Chain) != 1 {
		t.Errorf("цепочка: %+v, ожидался 1 шаг (перехода нет)", got.Chain)
	}
}

// --- таймауты слоёв ------------------------------------------------------------

// stallServer — «тарпит»: обработчик отвечает только после закрытия
// канала stall (или страховочного таймаута) — тест не ждёт спящего
// обработчика при закрытии сервера.
func stallServer(t *testing.T, handler func(w http.ResponseWriter, stall <-chan struct{})) (*httptest.Server, func()) {
	t.Helper()
	stall := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler(w, stall)
	}))
	return srv, func() {
		select {
		case <-stall: // уже закрыт вторым вызовом — не страшно
		default:
			close(stall)
		}
		srv.Close()
	}
}

// TestFetchHeaderTimeout: сервер молчит до заголовков — срабатывает
// ожидание заголовков (10 с из ТЗ; здесь укорочено), не общий таймаут.
func TestFetchHeaderTimeout(t *testing.T) {
	srv, done := stallServer(t, func(w http.ResponseWriter, stall <-chan struct{}) {
		select {
		case <-stall:
		case <-time.After(5 * time.Second):
		}
		w.WriteHeader(http.StatusOK)
	})
	defer done()

	f := NewFetcher(Options{HeaderTimeout: 150 * time.Millisecond, TotalTimeout: 10 * time.Second})
	start := time.Now()
	_, err := f.Fetch(context.Background(), FetchRequest{URL: hostURL(srv.URL, "tarpit.example.net"), Addrs: loopbackAddr()})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("ожидался таймаут заголовков")
	}
	if !IsTimeout(err) {
		t.Errorf("таймаут заголовков не распознан IsTimeout: %v", err)
	}
	if elapsed >= 2*time.Second {
		t.Errorf("таймаут заголовков сработал поздно: %s", elapsed)
	}
}

// TestFetchTotalTimeoutBodyTarpit: заголовки пришли, тело капает
// медленнее общего таймаута — чтение тела прерывается.
func TestFetchTotalTimeoutBodyTarpit(t *testing.T) {
	srv, done := stallServer(t, func(w http.ResponseWriter, stall <-chan struct{}) {
		w.Header().Set("Content-Length", "1000000")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, "part")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-stall:
		case <-time.After(5 * time.Second):
		}
	})
	defer done()

	f := NewFetcher(Options{TotalTimeout: 250 * time.Millisecond})
	res, err := f.Fetch(context.Background(), FetchRequest{URL: hostURL(srv.URL, "tarpit.example.net"), Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("заголовки должны успеть: %v", err)
	}
	start := time.Now()
	_, readErr := io.ReadAll(res.Body)
	elapsed := time.Since(start)
	_ = res.Body.Close()
	if readErr == nil {
		t.Fatal("чтение тела должно прерваться общим таймаутом")
	}
	if elapsed >= 2*time.Second {
		t.Errorf("общий таймаут прервал чтение поздно: %s", elapsed)
	}
}

// TestFetchTLSHandshakeTimeout: TCP установлен, сервер не отвечает на
// ClientHello — срабатывает таймаут рукопожатия TLS.
func TestFetchTLSHandshakeTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("слушатель: %v", err)
	}
	conns := make(chan net.Conn, 16)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			conns <- c // приняли и молчим
		}
	}()
	defer func() {
		_ = ln.Close()
		for {
			select {
			case c := <-conns:
				_ = c.Close()
			default:
				return
			}
		}
	}()

	f := NewFetcher(Options{
		TLSHandshakeTimeout: 200 * time.Millisecond,
		TotalTimeout:        10 * time.Second,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
	})
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	u := &url.URL{Scheme: "https", Host: net.JoinHostPort("slow-tls.example.net", port)}
	start := time.Now()
	_, err = f.Fetch(context.Background(), FetchRequest{URL: u, Addrs: loopbackAddr()})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("ожидался таймаут TLS-рукопожатия")
	}
	if !IsTimeout(err) {
		t.Errorf("таймаут TLS не распознан IsTimeout: %v", err)
	}
	if elapsed >= 2*time.Second {
		t.Errorf("таймаут TLS сработал поздно: %s", elapsed)
	}
}

// --- отмена и отказ соединения ---------------------------------------------------

func TestFetchContextCancel(t *testing.T) {
	srv, done := stallServer(t, func(w http.ResponseWriter, stall <-chan struct{}) {
		select {
		case <-stall:
		case <-time.After(5 * time.Second):
		}
		w.WriteHeader(http.StatusOK)
	})
	defer done()

	// Слои ниже отмены не срабатывают: заголовки 10 с, общий 20 с —
	// прервать запрос должен именно ctx вызывающего.
	f := NewFetcher(Options{HeaderTimeout: 10 * time.Second, TotalTimeout: 20 * time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(120*time.Millisecond, cancel)
	start := time.Now()
	_, err := f.Fetch(ctx, FetchRequest{URL: hostURL(srv.URL, "cancel.example.net"), Addrs: loopbackAddr()})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("ожидалась отмена запроса")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка отмены не пробрасывает context.Canceled: %v", err)
	}
	if elapsed >= 2*time.Second {
		t.Errorf("отмена сработала поздно: %s", elapsed)
	}
}

func TestFetchConnectionRefused(t *testing.T) {
	// Порт, на котором никто не слушает: слушатель закрывается сразу,
	// порт остаётся в адресе запроса.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("слушатель: %v", err)
	}
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	_ = ln.Close()

	f := NewFetcher(Options{})
	u := &url.URL{Scheme: "http", Host: net.JoinHostPort("gone.example.net", port)}
	res, err := f.Fetch(context.Background(), FetchRequest{URL: u, Addrs: loopbackAddr()})
	if err == nil {
		_ = res.Body.Close()
		t.Fatal("ожидался отказ соединения")
	}
	if res != nil {
		t.Error("при ошибке результат не возвращается")
	}
	if IsTimeout(err) {
		t.Errorf("отказ соединения (RST) не таймаут: %v", err)
	}
	if !IsConnectFailure(err) {
		t.Errorf("отказ соединения (RST) не распознан как отказ connect-фазы (T-164): %v", err)
	}
	if !strings.Contains(err.Error(), "gone.example.net") {
		t.Errorf("ошибка без контекста запроса: %v", err)
	}
}

// --- валидация запроса и опции из конфигурации ---------------------------------

func TestFetchValidation(t *testing.T) {
	f := NewFetcher(Options{})
	u := &url.URL{Scheme: "http", Host: "x.example.net"}

	if _, err := f.Fetch(context.Background(), FetchRequest{Addrs: loopbackAddr()}); err == nil {
		t.Error("запрос без URL должен отклоняться")
	}
	bad := &url.URL{Scheme: "ftp", Host: "x.example.net"}
	if _, err := f.Fetch(context.Background(), FetchRequest{URL: bad, Addrs: loopbackAddr()}); err == nil {
		t.Error("не-http(s) схема должна отклоняться")
	}
	noHost := &url.URL{Scheme: "http"}
	if _, err := f.Fetch(context.Background(), FetchRequest{URL: noHost, Addrs: loopbackAddr()}); err == nil {
		t.Error("URL без хоста должен отклоняться")
	}
	if _, err := f.Fetch(context.Background(), FetchRequest{URL: u}); err == nil {
		t.Error("запрос без адресов DNS-этапа должен отклоняться")
	}
}

func TestOptionsFromConfig(t *testing.T) {
	cfg := config.HTTP{
		ConnectTimeout:      config.Duration(3 * time.Second),
		TLSHandshakeTimeout: config.Duration(4 * time.Second),
		HeaderTimeout:       config.Duration(7 * time.Second),
		TotalTimeout:        config.Duration(15 * time.Second),
		MaxBodyBytes:        config.ByteSize(1 << 20),
		UserAgent:           "CfgCrawler/1.0",
	}
	o := NewOptions(cfg)
	if o.ConnectTimeout != 3*time.Second || o.TLSHandshakeTimeout != 4*time.Second ||
		o.HeaderTimeout != 7*time.Second || o.TotalTimeout != 15*time.Second {
		t.Errorf("таймауты из конфигурации: %+v", o)
	}
	if o.MaxBodyBytes != 1<<20 || o.UserAgent != "CfgCrawler/1.0" {
		t.Errorf("лимит тела и UA из конфигурации: %+v", o)
	}

	// Нулевые Options — дефолты таблицы лимитов ТЗ 5.5.
	d := Options{}.withDefaults()
	if d.ConnectTimeout != DefaultConnectTimeout || d.TLSHandshakeTimeout != DefaultTLSHandshakeTimeout ||
		d.HeaderTimeout != DefaultHeaderTimeout || d.TotalTimeout != DefaultTotalTimeout ||
		d.FallbackDelay != DefaultFallbackDelay {
		t.Errorf("дефолты таблицы лимитов: %+v", d)
	}
}

// TestIsTimeout — распознавание таймаутов по категориям ошибок.
func TestIsTimeout(t *testing.T) {
	if !IsTimeout(context.DeadlineExceeded) {
		t.Error("DeadlineExceeded — таймаут")
	}
	if !IsTimeout(fmt.Errorf("обёртка: %w", context.DeadlineExceeded)) {
		t.Error("обёрнутый DeadlineExceeded — таймаут")
	}
	if IsTimeout(context.Canceled) {
		t.Error("отмена — не таймаут")
	}
	if IsTimeout(errors.New("просто ошибка")) {
		t.Error("обычная ошибка — не таймаут")
	}
	if IsTimeout(nil) {
		t.Error("nil — не таймаут")
	}
}

// dialRefusedErr — ошибка connect-фазы реального диала: RST на SYN.
func dialRefusedErr() error {
	return &net.OpError{
		Op: "dial", Net: "tcp",
		Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1).To4(), Port: 80},
		Err:  &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED},
	}
}

// dialTimeoutErr — connect-таймаут реального диала: дедлайн попытки.
func dialTimeoutErr() error {
	return &net.OpError{
		Op: "dial", Net: "tcp",
		Addr: &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1).To4(), Port: 443},
		Err:  os.ErrDeadlineExceeded,
	}
}

// TestIsConnectFailure — распознавание отказов connect-фазы (dead-host
// cutoff, T-164): только dial-OpError с ECONNREFUSED или таймаутом;
// прочие слои и обёртки цепочки — нет.
func TestIsConnectFailure(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"refused", dialRefusedErr(), true},
		{"connect-таймаут (дедлайн диала)", dialTimeoutErr(), true},
		{"connect-таймаут (ETIMEDOUT)", &net.OpError{
			Op: "dial", Net: "tcp",
			Addr: &net.TCPAddr{IP: net.IPv4(192, 0, 2, 2).To4(), Port: 80},
			Err:  &os.SyscallError{Syscall: "connect", Err: syscall.ETIMEDOUT},
		}, true},
		{"обёртка Fetch", fmt.Errorf("httpfetch: GET https://x.example/: %w", dialRefusedErr()), true},
		{"обёртка гонки адресов", fmt.Errorf("httpfetch: недоступны все 2 адресов: %w", dialTimeoutErr()), true},
		{"отмена в фазе диала", &net.OpError{
			Op: "dial", Net: "tcp",
			Addr: &net.TCPAddr{IP: net.IPv4(192, 0, 2, 3).To4(), Port: 80},
			Err:  context.Canceled,
		}, false},
		{"refused на чтении (соединение было)", &net.OpError{
			Op: "read", Net: "tcp",
			Addr: &net.TCPAddr{IP: net.IPv4(192, 0, 2, 4).To4(), Port: 80},
			Err:  &os.SyscallError{Syscall: "read", Err: syscall.ECONNREFUSED},
		}, false},
		{"таймаут без диала (заголовки/общий)", context.DeadlineExceeded, false},
		{"обычная ошибка", errors.New("connection refused (текстом)"), false},
	}
	for _, tc := range cases {
		if got := IsConnectFailure(tc.err); got != tc.want {
			t.Errorf("%s: IsConnectFailure = %v, want %v (%v)", tc.name, got, tc.want, tc.err)
		}
	}
}

// atomicString — строка, записываемая горутиной обработчика.
type atomicString struct {
	mu sync.Mutex
	s  string
}

func (a *atomicString) Store(s string) {
	a.mu.Lock()
	a.s = s
	a.mu.Unlock()
}

func (a *atomicString) Load() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.s
}

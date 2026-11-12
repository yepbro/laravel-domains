// Transport tests. Three levels:
//
//   - the dialFunc seam — the address race (happy-eyeballs-like), the
//     fallback, the connect timeout, with no network at all;
//   - httptest.Server on 127.0.0.1 (the loopback, not the external
//     stack) — a basic GET, connecting by IP with a foreign Host/SNI
//     (TLS), the redirect resolve hook, the layer timeouts (tarpits on
//     headers and body), a ctx cancellation, a connection refusal;
//   - a raw net.Listener for the TLS tarpit (a handshake with no
//     answer).

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

// --- helpers -----------------------------------------------------------------

// loopbackAddr is the pre-resolved host address in tests: httptest and
// raw listeners live on the loopback.
func loopbackAddr() []netip.Addr {
	return []netip.Addr{netip.MustParseAddr("127.0.0.1")}
}

// hostURL replaces the host in the test server's base URL with a name no
// resolver knows: the connection must go by the passed addresses, not by
// DNS. The port is preserved.
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

// newTestFetcher is a transport with a swapped dial (the tests' seam) and
// a record of the attempts (thread-safe: attempts are started by
// goroutines).
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

// --- the dial seam: the address race -----------------------------------------

// TestDialAddrsFallback: the first address is silent (a SYN into the
// void), the second is alive — the connection goes through after the
// FallbackDelay pause, and the hanging attempt is canceled.
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
		if addr == netip.MustParseAddr("203.0.113.1") { // TEST-NET-3: a black hole
			<-ctx.Done() // stay silent until the attempt is canceled
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
	// The hanging attempt must be canceled by the winner's return.
	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Errorf("проигравшая попытка не отменена (утечка диала)")
	}
}

// TestDialAddrsFirstWins: a successful first address — the second never
// starts.
func TestDialAddrsFirstWins(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("слушатель: %v", err)
	}
	defer func() { _ = ln.Close() }()

	f, calls := newTestFetcher(t, Options{
		ConnectTimeout: 2 * time.Second,
		FallbackDelay:  250 * time.Millisecond, // the second would have been launched
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

// TestDialAddrsAllFailImmediate: the failure of all launched attempts
// starts the next one without a pause — a sequential fallback with no
// latency.
func TestDialAddrsAllFailImmediate(t *testing.T) {
	boom := errors.New("сеть недоступна")
	f, calls := newTestFetcher(t, Options{
		ConnectTimeout: 2 * time.Second,
		FallbackDelay:  500 * time.Millisecond, // the pause must not fire
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

// TestDialAddrsConnectTimeout: the connect-timeout layer — an attempt that
// misses ConnectTimeout is interrupted (deterministically via a ctx-aware
// dial).
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

// TestDialAddrsCancelAfterWinner: a ctx cancellation at the moment an
// attempt wins — the select may take the cancellation branch after the
// attempt has already put the connection into winner. The losing exit
// must close that connection (together with the limitConn limit slots),
// not leak: the global semaphore's slots are not collected by the GC and,
// accumulating, they stall the transport.
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
		FallbackDelay:  time.Hour, // the second address never starts — one attempt in the race
	}, func(ctx context.Context, addr netip.Addr, port string) (net.Conn, error) {
		<-ctx.Done() // the "win" arrives together with the cancellation — the leak window
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
		_ = conn.Close() // the winner was handed to the caller — they close it
		cancel()
	}
	if cancelled == 0 {
		t.Fatal("гонка не выполнилась: ни одного выхода по отмене")
	}
	// Closing the losers/the cancellation winner is asynchronous: the
	// attempt goroutine of the last iteration may be closing the
	// connection after dialAddrs has already returned. Wait for the drain
	// instead of pinning an instant snapshot.
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

// closeCountingConn is a connection that counts Close calls (cancellation
// leaks).
type closeCountingConn struct {
	net.Conn
	onClose func()
}

func (c *closeCountingConn) Close() error {
	c.onClose()
	return c.Conn.Close()
}

// TestDialAddrsEmptyAndSingle — the boundaries of the address list.
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

// --- a basic GET -------------------------------------------------------------

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
	// The request host comes from the URL, the connection — by the DNS
	// stage address.
	if host, _, _ := net.SplitHostPort(gotHost.Load()); host != "www.example-test.net" {
		t.Errorf("Host-заголовок: %q, ожидался www.example-test.net", gotHost.Load())
	}
	if gotUA.Load() != "TestCrawler/1.0 (contact: test@example.invalid)" {
		t.Errorf("User-Agent: %q", gotUA.Load())
	}
}

// TestFetchUsesProvidedAddrs: the DNS stage addresses reach the dial, the
// resolver is never called.
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

// --- Host/SNI over a pre-resolved IP (TLS) -----------------------------------

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
			return nil, nil // the server's config is left unchanged
		},
	}
	srv.StartTLS()
	defer srv.Close()

	f := NewFetcher(Options{
		// The certificate check is not the test's subject: SNI never gets
		// that far, and InsecureSkipVerify does not disable sending the
		// name (T-050).
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

// --- the redirect-host resolve hook ------------------------------------------

// TestRoundTripResolvesHostsWithoutStageAddrs: a request without DNS
// stage addresses in the ctx (a redirect to another host — T-048, or an
// http.Client over the transport) connects via the Resolver; the cache
// folds repeated requests to one host into a single resolve.
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

// TestDialContextRedirectIPLiteral: a Location with an IP literal never
// touches the resolver.
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

// --- the SSRF circuit (T-219) ---------------------------------------------------

// ssrfDialSeam is the dial seam for the SSRF tests: connecting to any
// address leads to the local test server (the attempt's port is
// preserved), and the attempt addresses are recorded — the tests assert
// that internal addresses never reached the dial (the filterInternalAddrs
// funnel sits before the seam). The funnel fires before the dial, so the
// seam does not interfere with blocking but makes "it would pass without
// the filter" an observable request success (without the seam a literal
// like ::ffff:169.254.169.254 would simply fail to connect — the test
// could not tell a block from a timeout). The recording is thread-safe:
// dialAddrs attempts are started by goroutines (the newTestFetcher
// style).
func ssrfDialSeam() (dialFunc, func() []netip.Addr) {
	var mu sync.Mutex
	calls := []netip.Addr{}
	return func(ctx context.Context, addr netip.Addr, port string) (net.Conn, error) {
			mu.Lock()
			calls = append(calls, addr)
			mu.Unlock()
			var d net.Dialer
			return d.DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", port))
		}, func() []netip.Addr {
			mu.Lock()
			defer mu.Unlock()
			return append([]netip.Addr(nil), calls...)
		}
}

// TestAddrIsInternal is the predicate of the single funnel (T-219): the
// internal ranges, including IPv4-in-IPv6 (a measurement: only Unmap
// catches ::ffff:0.0.0.0 — IsUnspecified does not unwrap 4-in-6; the other
// predicates unwrap themselves, Unmap gives one semantics to any literal);
// external addresses and documentation networks are not.
func TestAddrIsInternal(t *testing.T) {
	internal := []string{
		"127.0.0.1", "10.0.0.5", "192.168.1.1", "172.16.0.1",
		"169.254.169.254", "0.0.0.0", "224.0.0.1",
		"::1", "fd00::1", "fe80::1", "ff02::1", "::",
		"::ffff:127.0.0.1", "::ffff:10.0.0.5", "::ffff:169.254.169.254",
		"::ffff:0.0.0.0",
	}
	for _, s := range internal {
		if !AddrIsInternal(netip.MustParseAddr(s)) {
			t.Errorf("AddrIsInternal(%s) = false, хочу true", s)
		}
	}
	external := []string{
		"192.0.2.10", "198.51.100.7", "203.0.113.10", "1.1.1.1",
		"2001:db8::1", "2606:4700::1111", "::ffff:203.0.113.10",
	}
	for _, s := range external {
		if AddrIsInternal(netip.MustParseAddr(s)) {
			t.Errorf("AddrIsInternal(%s) = true, хочу false", s)
		}
	}
}

// TestLookupBlocksInternalAddrs: Options.BlockInternalAddrs rejects
// internal addresses in the lookup funnel — literals from Location (SSRF:
// loopback, RFC1918, cloud metadata, including IPv4-in-IPv6 — Unmap in
// AddrIsInternal) and resolve addresses; the external address of a mixed
// answer stays.
func TestLookupBlocksInternalAddrs(t *testing.T) {
	f := NewFetcher(Options{
		BlockInternalAddrs: true,
		Resolver: ResolverFunc(func(ctx context.Context, host string) ([]netip.Addr, error) {
			return []netip.Addr{
				netip.MustParseAddr("192.0.2.10"),      // TEST-NET-1 — not internal
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

	for _, host := range []string{
		"127.0.0.1", "10.0.0.5", "169.254.169.254", "::1", "fd00::1", "192.168.1.1",
		// IPv4-in-IPv6: some netip predicates do not unwrap 4-in-6
		// (IsUnspecified — ::ffff:0.0.0.0); a single semantics — Unmap in
		// AddrIsInternal (T-219).
		"::ffff:127.0.0.1", "::ffff:169.254.169.254", "::ffff:10.0.0.5", "::ffff:0.0.0.0",
	} {
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

	// The flag off — the previous behavior: the addresses pass as is.
	f3 := NewFetcher(Options{
		Resolver: ResolverFunc(func(ctx context.Context, host string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr("10.0.0.5")}, nil
		}),
	})
	if addrs, err := f3.lookup(context.Background(), "legacy.example.net"); err != nil || len(addrs) != 1 {
		t.Errorf("без флага внутренние адреса должны проходить: %v %v", addrs, err)
	}
}

// TestFetchBlocksInternalDNSAddrs: with the circuit enabled, the DNS stage
// addresses (FetchRequest.Addrs) pass the same funnel as redirects — a
// starting request to an A record from an internal network (SSRF: a
// planted DNS or a local resolver) never happens. Before T-219 this was
// red: the Fetch connected by internal addresses without checking (the
// dial went to the local server — success instead of a refusal).
func TestFetchBlocksInternalDNSAddrs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	f := NewFetcher(Options{BlockInternalAddrs: true, Resolver: redirectResolverFail})
	dial, dialed := ssrfDialSeam()
	f.dial = dial
	for _, addr := range []string{"127.0.0.1", "10.0.0.5", "::ffff:127.0.0.1"} {
		_, err := f.Fetch(context.Background(), FetchRequest{
			URL:   redirectReqURL(srv.URL, "ssrf.example.net", "/start"),
			Addrs: []netip.Addr{netip.MustParseAddr(addr)},
		})
		if err == nil || !strings.Contains(err.Error(), "внутренние") {
			t.Errorf("%s: ожидалась блокировка внутренних адресов DNS-этапа, получено: %v", addr, err)
		}
	}
	// A mixed list leaves the external ones — the funnel's semantics; the
	// internal address never reaches the dial at all (a pin against the
	// "filter only when all are internal" regression).
	res, err := f.Fetch(context.Background(), FetchRequest{
		URL: redirectReqURL(srv.URL, "ssrf.example.net", "/start"),
		Addrs: []netip.Addr{
			netip.MustParseAddr("10.0.0.5"),     // internal — rejected
			netip.MustParseAddr("203.0.113.10"), // TEST-NET-3 — external
		},
	})
	if err != nil {
		t.Fatalf("смешанный список адресов: %v", err)
	}
	_ = res.Body.Close()
	attempts := dialed()
	if len(attempts) != 1 || attempts[0] != netip.MustParseAddr("203.0.113.10") {
		t.Errorf("до диала дошли адреса %v, хочу только 203.0.113.10", attempts)
	}

	// The flag off — the DNS stage addresses pass as is (dev/testbed).
	f2 := NewFetcher(Options{Resolver: redirectResolverFail})
	f2.dial, _ = ssrfDialSeam()
	res2, err := f2.Fetch(context.Background(), FetchRequest{
		URL: redirectReqURL(srv.URL, "dev.example.net", "/start"), Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("без флага внутренние адреса DNS-этапа должны проходить: %v", err)
	}
	_ = res2.Body.Close()
}

// TestFetchChainBlocksInternalRedirect: a Location to an internal address
// — no hop happens (a funnel error, the chain holds the completed part).
// The starting host is an external address (the dial seam leads to the
// local server): since T-219 the starting dial is filtered too — the
// loopback in FetchRequest.Addrs would have blocked the request before
// the 302 response.
func TestFetchChainBlocksInternalRedirect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "http://169.254.169.254/latest/meta-data/")
		w.WriteHeader(http.StatusFound)
	}))
	defer srv.Close()

	f := NewFetcher(Options{BlockInternalAddrs: true, Resolver: redirectResolverFail})
	f.dial, _ = ssrfDialSeam()
	got, err := FetchChain(context.Background(), f, FetchRequest{
		URL:   redirectReqURL(srv.URL, "ssrf.example.net", "/start"),
		Addrs: []netip.Addr{netip.MustParseAddr("203.0.113.10")}})
	if err == nil || !strings.Contains(err.Error(), "внутренние") {
		t.Fatalf("ожидалась блокировка внутреннего адреса, получено: %v", err)
	}
	if len(got.Chain) != 1 {
		t.Errorf("цепочка: %+v, ожидался 1 шаг (перехода нет)", got.Chain)
	}
}

// TestFetchChainBlocksInternalRedirect4in6: a Location with an IPv4-in-
// IPv6 literal of an internal range — the same refusal. ::ffff:0.0.0.0 is
// the only form caught by Unmap specifically (the T-219 measurement: the
// IsLoopback/IsPrivate/IsLinkLocalUnicast predicates unwrap 4-in-6
// themselves, IsUnspecified does not): without Unmap the dial seam would
// have carried the request to success. ::ffff:169.254.169.254 pins the
// cloud-metadata form.
func TestFetchChainBlocksInternalRedirect4in6(t *testing.T) {
	for _, location := range []string{
		"http://[::ffff:169.254.169.254]/latest/meta-data/",
		"http://[::ffff:0.0.0.0]/latest/meta-data/",
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Location", location)
			w.WriteHeader(http.StatusFound)
		}))

		f := NewFetcher(Options{BlockInternalAddrs: true, Resolver: redirectResolverFail})
		f.dial, _ = ssrfDialSeam()
		got, err := FetchChain(context.Background(), f, FetchRequest{
			URL:   redirectReqURL(srv.URL, "ssrf.example.net", "/start"),
			Addrs: []netip.Addr{netip.MustParseAddr("203.0.113.10")}})
		if err == nil || !strings.Contains(err.Error(), "внутренние") {
			t.Errorf("Location %s: ожидалась блокировка IPv4-в-IPv6 внутреннего литерала, получено: %v",
				location, err)
			srv.Close()
			continue
		}
		if len(got.Chain) != 1 {
			t.Errorf("Location %s: цепочка %+v, ожидался 1 шаг (перехода нет)", location, got.Chain)
		}
		srv.Close()
	}
}

// --- the layer timeouts ----------------------------------------------------------

// stallServer is a "tarpit": the handler answers only after the stall
// channel is closed (or the guard timeout fires) — the test does not wait
// for a sleeping handler when closing the server.
func stallServer(t *testing.T, handler func(w http.ResponseWriter, stall <-chan struct{})) (*httptest.Server, func()) {
	t.Helper()
	stall := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler(w, stall)
	}))
	return srv, func() {
		select {
		case <-stall: // already closed by a second call — no harm
		default:
			close(stall)
		}
		srv.Close()
	}
}

// TestFetchHeaderTimeout: the server stays silent until the headers — the
// header wait fires (10 s per the spec; shortened here), not the total
// timeout.
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

// TestFetchTotalTimeoutBodyTarpit: the headers arrived, the body trickles
// slower than the total timeout — reading the body is interrupted.
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

// TestFetchTLSHandshakeTimeout: the TCP connection is up, the server does
// not answer the ClientHello — the TLS handshake timeout fires.
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
			conns <- c // accepted and staying silent
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

// --- cancellation and connection refusal -----------------------------------------

func TestFetchContextCancel(t *testing.T) {
	srv, done := stallServer(t, func(w http.ResponseWriter, stall <-chan struct{}) {
		select {
		case <-stall:
		case <-time.After(5 * time.Second):
		}
		w.WriteHeader(http.StatusOK)
	})
	defer done()

	// The layers below the cancellation do not fire: headers 10 s, total
	// 20 s — it is the caller's ctx that must interrupt the request.
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
	// A port nobody listens on: the listener is closed at once, the port
	// stays in the request address.
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

// --- request validation and the options from the configuration -----------------

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

	// Zero Options — the defaults of the spec 5.5 limits table.
	d := Options{}.withDefaults()
	if d.ConnectTimeout != DefaultConnectTimeout || d.TLSHandshakeTimeout != DefaultTLSHandshakeTimeout ||
		d.HeaderTimeout != DefaultHeaderTimeout || d.TotalTimeout != DefaultTotalTimeout ||
		d.FallbackDelay != DefaultFallbackDelay {
		t.Errorf("дефолты таблицы лимитов: %+v", d)
	}
}

// TestIsTimeout — recognizing timeouts by error categories.
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

// dialRefusedErr is a connect-phase error of a real dial: an RST on SYN.
func dialRefusedErr() error {
	return &net.OpError{
		Op: "dial", Net: "tcp",
		Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1).To4(), Port: 80},
		Err:  &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED},
	}
}

// dialTimeoutErr is a connect timeout of a real dial: the attempt's
// deadline.
func dialTimeoutErr() error {
	return &net.OpError{
		Op: "dial", Net: "tcp",
		Addr: &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1).To4(), Port: 443},
		Err:  os.ErrDeadlineExceeded,
	}
}

// TestIsConnectFailure — recognizing connect-phase failures (the dead-host
// cutoff, T-164): only a dial OpError with ECONNREFUSED or a timeout; the
// other layers and chain wrappers are not.
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

// atomicString is a string written by the handler goroutine.
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

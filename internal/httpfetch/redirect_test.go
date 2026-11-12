// Redirect chain pass tests (T-048). httptest servers on the loopback
// (127.0.0.1); the URL's starting host is swapped for a name that no
// resolver knows (hostURL from transport_test.go): the connection goes by
// the DNS stage addresses or the stub resolver, while the hosts of real
// requests are checked via the Host header and the resolver log.

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
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crawler/internal/idn"
)

// --- helpers -----------------------------------------------------------------

// redirectResolverStub is a stub resolver: the loopback address + a host
// log.
type redirectResolverStub struct {
	mu    sync.Mutex
	hosts []string
}

func (r *redirectResolverStub) LookupNetIP(ctx context.Context, host string) ([]netip.Addr, error) {
	r.mu.Lock()
	r.hosts = append(r.hosts, host)
	r.mu.Unlock()
	return loopbackAddr(), nil
}

func (r *redirectResolverStub) calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.hosts...)
}

// redirectResolverFail is a resolver whose invocation means a test error:
// paths without resolving (the DNS stage addresses) must not call it.
var redirectResolverFail = ResolverFunc(func(ctx context.Context, host string) ([]netip.Addr, error) {
	return nil, fmt.Errorf("неожидаемый резолв %q", host)
})

// redirectReqURL is the test server URL with a swapped host and a path.
func redirectReqURL(base, host, path string) *url.URL {
	u := hostURL(base, host)
	u.Path = path
	return u
}

// redirectTestURL is the string form of redirectReqURL (the chain
// expectations).
func redirectTestURL(base, host, path string) string {
	return redirectReqURL(base, host, path).String()
}

// redirectReadBody reads and closes the final response body.
func redirectReadBody(t *testing.T, res *ChainResult) string {
	t.Helper()
	b, err := io.ReadAll(res.Result.Body)
	_ = res.Result.Body.Close()
	if err != nil {
		t.Fatalf("чтение итогового тела: %v", err)
	}
	return string(b)
}

// --- HTTP redirects ----------------------------------------------------------

// TestFetchChainThreeSteps: a chain of three responses (301 → 302 → 200);
// the final page is the end of the chain: the body, status and URL of the
// last response (spec 5.5 item 4).
func TestFetchChainThreeSteps(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			w.Header().Set("Location", "/one")
			w.WriteHeader(http.StatusMovedPermanently)
		case "/one":
			w.Header().Set("Location", "/two")
			w.WriteHeader(http.StatusFound)
		case "/two":
			_, _ = fmt.Fprint(w, "FINAL")
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	f := NewFetcher(Options{Resolver: redirectResolverFail})
	got, err := FetchChain(context.Background(), f, FetchRequest{
		URL: redirectReqURL(srv.URL, "chain.example.net", "/"), Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("FetchChain: %v", err)
	}
	if got.Result.StatusCode != http.StatusOK {
		t.Errorf("итоговый статус: %d, ожидался 200", got.Result.StatusCode)
	}
	if body := redirectReadBody(t, got); body != "FINAL" {
		t.Errorf("итоговое тело: %q", body)
	}
	final := redirectTestURL(srv.URL, "chain.example.net", "/two")
	if got.Result.FinalURL.String() != final {
		t.Errorf("итоговый URL: %s, ожидался %s", got.Result.FinalURL, final)
	}
	want := []RedirectStep{
		{URL: redirectTestURL(srv.URL, "chain.example.net", "/"), Status: 301},
		{URL: redirectTestURL(srv.URL, "chain.example.net", "/one"), Status: 302},
		{URL: final, Status: 200},
	}
	if !reflect.DeepEqual(got.Chain, want) {
		t.Errorf("цепочка: %+v, ожидалась %+v", got.Chain, want)
	}
}

// TestFetchChainRedirectCodes: we follow 301/302/303/307/308, the next
// request is GET (303 requires switching the method to GET — all chain
// requests are GET anyway); 300 without a hop status is not followed.
func TestFetchChainRedirectCodes(t *testing.T) {
	for _, code := range []int{301, 302, 303, 307, 308} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			var method atomicString
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/start" {
					w.Header().Set("Location", "/land")
					w.WriteHeader(code)
					return
				}
				method.Store(r.Method)
				_, _ = fmt.Fprint(w, "landed")
			}))
			defer srv.Close()

			f := NewFetcher(Options{Resolver: redirectResolverFail})
			got, err := FetchChain(context.Background(), f, FetchRequest{
				URL: redirectReqURL(srv.URL, "codes.example.net", "/start"), Addrs: loopbackAddr()})
			if err != nil {
				t.Fatalf("FetchChain: %v", err)
			}
			if got.Result.StatusCode != http.StatusOK {
				t.Errorf("итоговый статус: %d, ожидался 200", got.Result.StatusCode)
			}
			if method.Load() != http.MethodGet {
				t.Errorf("метод запроса после %d: %q, ожидался GET", code, method.Load())
			}
			if len(got.Chain) != 2 {
				t.Errorf("цепочка: %+v, ожидались 2 шага", got.Chain)
			}
		})
	}

	t.Run("300-не-переход", func(t *testing.T) {
		var lands atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/start" {
				w.Header().Set("Location", "/land")
				w.WriteHeader(http.StatusMultipleChoices)
				return
			}
			lands.Add(1)
			_, _ = fmt.Fprint(w, "landed")
		}))
		defer srv.Close()

		f := NewFetcher(Options{Resolver: redirectResolverFail})
		got, err := FetchChain(context.Background(), f, FetchRequest{
			URL: redirectReqURL(srv.URL, "codes.example.net", "/start"), Addrs: loopbackAddr()})
		if err != nil {
			t.Fatalf("FetchChain: %v", err)
		}
		if got.Result.StatusCode != http.StatusMultipleChoices {
			t.Errorf("итоговый статус: %d, ожидался 300", got.Result.StatusCode)
		}
		if n := lands.Load(); n != 0 {
			t.Errorf("за 300 прослежено %d переходов", n)
		}
		if len(got.Chain) != 1 {
			t.Errorf("цепочка: %+v, ожидался 1 шаг", got.Chain)
		}
	})
}

// TestFetchChainRelativeLocation: a relative Location is resolved against
// the current response as the base — an absolute path, a path relative to
// the directory, and the protocol-relative form.
func TestFetchChainRelativeLocation(t *testing.T) {
	cases := []struct {
		name     string
		basePath string
		location string // {port} — the test server's port
		wantPath string
	}{
		{"абсолютный-путь", "/dir/page", "/land", "/land"},
		{"относительный-путь", "/dir/page", "sub/land", "/dir/sub/land"},
		// Protocol-relative inherits only the scheme — the port must be in
		// the Location itself, otherwise the hop would go to the standard
		// :80.
		{"protocol-relative", "/dir/page", "//rel.example.net:{port}/land", "/land"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var srv *httptest.Server
			srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == c.basePath {
					_, p, _ := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
					w.Header().Set("Location", strings.ReplaceAll(c.location, "{port}", p))
					w.WriteHeader(http.StatusFound)
					return
				}
				_, _ = fmt.Fprint(w, "OK ", r.URL.Path)
			}))
			defer srv.Close()

			f := NewFetcher(Options{Resolver: redirectResolverFail})
			got, err := FetchChain(context.Background(), f, FetchRequest{
				URL: redirectReqURL(srv.URL, "rel.example.net", c.basePath), Addrs: loopbackAddr()})
			if err != nil {
				t.Fatalf("FetchChain: %v", err)
			}
			want := redirectReqURL(srv.URL, "rel.example.net", c.wantPath)
			if got.Result.FinalURL.String() != want.String() {
				t.Errorf("итоговый URL: %s, ожидался %s", got.Result.FinalURL, want)
			}
			if body := redirectReadBody(t, got); body != "OK "+c.wantPath {
				t.Errorf("итоговое тело: %q", body)
			}
		})
	}
}

// TestFetchChainLoop: A → B → A — a URL reappearing, ErrTooManyRedirects.
func TestFetchChainLoop(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/a":
			w.Header().Set("Location", "/b")
			w.WriteHeader(http.StatusFound)
		case "/b":
			w.Header().Set("Location", "/a")
			w.WriteHeader(http.StatusFound)
		}
	}))
	defer srv.Close()

	f := NewFetcher(Options{Resolver: redirectResolverFail})
	got, err := FetchChain(context.Background(), f, FetchRequest{
		URL: redirectReqURL(srv.URL, "loop.example.net", "/a"), Addrs: loopbackAddr()})
	if !errors.Is(err, ErrTooManyRedirects) {
		t.Fatalf("ожидался ErrTooManyRedirects, получено: %v", err)
	}
	if got.Result != nil {
		t.Error("при цикле итогового ответа нет")
	}
	if len(got.Chain) != 2 {
		t.Errorf("цепочка: %+v, ожидались 2 шага", got.Chain)
	}
}

// TestFetchChainSelfRedirect: a redirect to itself — a cycle on the first
// step.
func TestFetchChainSelfRedirect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/self")
		w.WriteHeader(http.StatusFound)
	}))
	defer srv.Close()

	f := NewFetcher(Options{Resolver: redirectResolverFail})
	got, err := FetchChain(context.Background(), f, FetchRequest{
		URL: redirectReqURL(srv.URL, "self.example.net", "/self"), Addrs: loopbackAddr()})
	if !errors.Is(err, ErrTooManyRedirects) {
		t.Fatalf("ожидался ErrTooManyRedirects, получено: %v", err)
	}
	if len(got.Chain) != 1 {
		t.Errorf("цепочка: %+v, ожидался 1 шаг", got.Chain)
	}
}

// TestFetchChainQueryInCycleKey: the query is part of the cycle key — an
// exact repetition of a URL with the query is a cycle, unique queries are
// not (counter redirects are legitimate).
func TestFetchChainQueryInCycleKey(t *testing.T) {
	t.Run("повтор-query-цикл", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Query().Get("i") {
			case "1", "2":
				w.Header().Set("Location", "/q?i=2")
				w.WriteHeader(http.StatusFound)
			}
		}))
		defer srv.Close()

		f := NewFetcher(Options{Resolver: redirectResolverFail})
		start := redirectReqURL(srv.URL, "q.example.net", "/q")
		start.RawQuery = "i=1"
		got, err := FetchChain(context.Background(), f, FetchRequest{URL: start, Addrs: loopbackAddr()})
		if !errors.Is(err, ErrTooManyRedirects) {
			t.Fatalf("ожидался ErrTooManyRedirects, получено: %v", err)
		}
		if len(got.Chain) != 2 {
			t.Errorf("цепочка: %+v, ожидались 2 шага", got.Chain)
		}
	})

	t.Run("уникальные-query-не-цикл", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if i := r.URL.Query().Get("i"); i == "1" || i == "2" {
				w.Header().Set("Location", "/u?i=3")
				w.WriteHeader(http.StatusFound)
				return
			}
			_, _ = fmt.Fprint(w, "done")
		}))
		defer srv.Close()

		f := NewFetcher(Options{Resolver: redirectResolverFail})
		start := redirectReqURL(srv.URL, "q.example.net", "/u")
		start.RawQuery = "i=1"
		got, err := FetchChain(context.Background(), f, FetchRequest{URL: start, Addrs: loopbackAddr()})
		if err != nil {
			t.Fatalf("FetchChain: %v", err)
		}
		if got.Result.StatusCode != http.StatusOK {
			t.Errorf("итоговый статус: %d, ожидался 200", got.Result.StatusCode)
		}
		if len(got.Chain) != 2 {
			t.Errorf("цепочка: %+v, ожидались 2 шага", got.Chain)
		}
	})
}

// TestFetchChainLimitExceeded: an explicit MaxRedirects — no more hops
// than the limit, one more request than that; no outcome, the error is
// classified.
func TestFetchChainLimitExceeded(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		w.Header().Set("Location", fmt.Sprintf("/inf?n=%d", n))
		w.WriteHeader(http.StatusFound)
	}))
	defer srv.Close()

	f := NewFetcher(Options{Resolver: redirectResolverFail})
	got, err := FetchChainOpts(context.Background(), f, FetchRequest{
		URL: redirectReqURL(srv.URL, "limit.example.net", "/inf"), Addrs: loopbackAddr(),
	}, ChainOptions{MaxRedirects: 3})
	if !errors.Is(err, ErrTooManyRedirects) {
		t.Fatalf("ожидался ErrTooManyRedirects, получено: %v", err)
	}
	if got.Result != nil {
		t.Error("при превышении лимита итогового ответа нет")
	}
	if len(got.Chain) != 4 {
		t.Errorf("цепочка: %+v, ожидались 4 шага (1 стартовый + 3 перехода)", got.Chain)
	}
	if n := hits.Load(); n != 4 {
		t.Errorf("запросов к серверу: %d, ожидалось 4", n)
	}
}

// TestFetchChainDefaultLimit: FetchChain without options — the default of
// the spec 5.5 limits table: 8 hops.
func TestFetchChainDefaultLimit(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		w.Header().Set("Location", fmt.Sprintf("/inf?n=%d", n))
		w.WriteHeader(http.StatusFound)
	}))
	defer srv.Close()

	f := NewFetcher(Options{Resolver: redirectResolverFail})
	got, err := FetchChain(context.Background(), f, FetchRequest{
		URL: redirectReqURL(srv.URL, "def.example.net", "/inf"), Addrs: loopbackAddr()})
	if !errors.Is(err, ErrTooManyRedirects) {
		t.Fatalf("ожидался ErrTooManyRedirects, получено: %v", err)
	}
	if n := hits.Load(); n != 1+DefaultMaxRedirects {
		t.Errorf("запросов к серверу: %d, ожидалось %d (1 стартовый + %d переходов)", n, 1+DefaultMaxRedirects, DefaultMaxRedirects)
	}
	if len(got.Chain) != 1+DefaultMaxRedirects {
		t.Errorf("цепочка: %d шагов, ожидалось %d", len(got.Chain), 1+DefaultMaxRedirects)
	}
}

// TestFetchChainZeroLimit: MaxRedirects 0 and negative — the "do not
// follow" mode (the semantics of http.max_redirects, T-006): the outcome
// is the first response.
func TestFetchChainZeroLimit(t *testing.T) {
	var lands atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			w.Header().Set("Location", "/land")
			w.WriteHeader(http.StatusFound)
			return
		}
		lands.Add(1)
		_, _ = fmt.Fprint(w, "landed")
	}))
	defer srv.Close()

	for _, max := range []int{0, -5} {
		f := NewFetcher(Options{Resolver: redirectResolverFail})
		got, err := FetchChainOpts(context.Background(), f, FetchRequest{
			URL: redirectReqURL(srv.URL, "zero.example.net", "/start"), Addrs: loopbackAddr(),
		}, ChainOptions{MaxRedirects: max})
		if err != nil {
			t.Fatalf("MaxRedirects %d: %v", max, err)
		}
		if got.Result.StatusCode != http.StatusFound {
			t.Errorf("MaxRedirects %d: итоговый статус %d, ожидался 302", max, got.Result.StatusCode)
		}
		if len(got.Chain) != 1 {
			t.Errorf("MaxRedirects %d: цепочка %+v, ожидался 1 шаг", max, got.Chain)
		}
		if n := lands.Load(); n != 0 {
			t.Errorf("MaxRedirects %d: выполнено переходов %d", max, n)
		}
	}
}

// TestFetchChainZeroLimitNoMetaScan: at max_redirects=0 the body meta scan
// is not performed at all (the body is not wrapped in redirectBody):
// without hops, reading up to 256 KB for meta refresh is needless I/O.
func TestFetchChainZeroLimitNoMetaScan(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = fmt.Fprint(w, `<html><head><meta http-equiv="refresh" content="0; url=/after"></head><body>STAY</body></html>`)
	}))
	defer srv.Close()

	f := NewFetcher(Options{Resolver: redirectResolverFail})
	got, err := FetchChainOpts(context.Background(), f, FetchRequest{
		URL: redirectReqURL(srv.URL, "zero-noscan.example.net", "/page"), Addrs: loopbackAddr(),
	}, ChainOptions{MaxRedirects: 0})
	if err != nil {
		t.Fatalf("FetchChainOpts: %v", err)
	}
	defer func() { _ = got.Result.Body.Close() }()
	if _, wrapped := got.Result.Body.(*redirectBody); wrapped {
		t.Error("при max_redirects=0 тело не должно сканироваться на meta refresh")
	}
}

// --- meta refresh -------------------------------------------------------------

// TestFetchChainMetaRefresh: parsing the meta tag content — ordinary,
// zero delay, case/spaces/quotes, attribute order, self-closing tag; no
// url (a self-refresh) and invalid forms are ignored (spec 5.5 item 4).
func TestFetchChainMetaRefresh(t *testing.T) {
	cases := []struct {
		name   string
		meta   string
		follow bool
	}{
		{"обычный", `<meta http-equiv="refresh" content="3; url=/after">`, true},
		{"нулевая-задержка", `<meta http-equiv="refresh" content="0;url=/after">`, true},
		{"регистр-и-пробелы", `<META HTTP-EQUIV="Refresh" CONTENT=" 7 ;  URL  =  /after  ">`, true},
		{"кавычки-вокруг-url", `<meta http-equiv="refresh" content="0; url='/after'">`, true},
		{"одинарные-атрибуты", `<meta http-equiv='refresh' content='0; url=/after'>`, true},
		{"самозакрытый", `<meta http-equiv="refresh" content="0; url=/after" />`, true},
		{"content-первым", `<meta content="0; url=/after" http-equiv="refresh">`, true},
		{"без-url", `<meta http-equiv="refresh" content="10">`, false},
		{"пустой-url", `<meta http-equiv="refresh" content="0; url=">`, false},
		{"битое-число", `<meta http-equiv="refresh" content="soon; url=/after">`, false},
		{"без-числа", `<meta http-equiv="refresh" content="url=/after">`, false},
		{"не-url-параметр", `<meta http-equiv="refresh" content="5; href=/after">`, false},
		{"отрицательное-число", `<meta http-equiv="refresh" content="-1; url=/after">`, false},
		{"не-refresh", `<meta http-equiv="content-type" content="0; url=/after">`, false},
		{"metadata-не-мета", `<metadata http-equiv="refresh" content="0; url=/after">`, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/after" {
					_, _ = fmt.Fprint(w, "AFTER")
					return
				}
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				_, _ = fmt.Fprintf(w, "<html><head>%s</head><body>STAY</body></html>", c.meta)
			}))
			defer srv.Close()

			f := NewFetcher(Options{Resolver: redirectResolverFail})
			got, err := FetchChain(context.Background(), f, FetchRequest{
				URL: redirectReqURL(srv.URL, "meta.example.net", "/page"), Addrs: loopbackAddr()})
			if err != nil {
				t.Fatalf("FetchChain: %v", err)
			}
			if c.follow {
				body := redirectReadBody(t, got)
				if got.Result.StatusCode != http.StatusOK || body != "AFTER" {
					t.Errorf("ожидался переход на /after, итог: %d %q", got.Result.StatusCode, body)
				}
				if len(got.Chain) != 2 {
					t.Errorf("цепочка: %+v, ожидались 2 шага", got.Chain)
				}
				return
			}
			if got.Result.StatusCode != http.StatusOK || !strings.Contains(redirectReadBody(t, got), "STAY") {
				t.Errorf("переход не должен выполняться, итог: %d", got.Result.StatusCode)
			}
			if len(got.Chain) != 1 {
				t.Errorf("цепочка: %+v, ожидался 1 шаг", got.Chain)
			}
		})
	}
}

// TestFetchChainMetaRefreshContentTypes: the meta scan applies only to
// HTML documents (a Content-Type with html); other types — a no-hop
// outcome, the body untouched. httptest never serves a response without a
// Content-Type at all (the Go server sets it by content autodetection) —
// that branch is covered by the unit check of redirectNext.
func TestFetchChainMetaRefreshContentTypes(t *testing.T) {
	cases := []struct {
		ct     string
		follow bool
	}{
		{"text/html; charset=utf-8", true},
		{"application/xhtml+xml", true},
		{"TEXT/HTML", true},
		{"text/plain", false},
		{"image/png", false},
	}
	for _, c := range cases {
		t.Run("ct="+c.ct, func(t *testing.T) {
			const meta = `<meta http-equiv="refresh" content="0; url=/after">`
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/after" {
					_, _ = fmt.Fprint(w, "AFTER")
					return
				}
				if c.ct != "" {
					w.Header().Set("Content-Type", c.ct)
				}
				_, _ = fmt.Fprintf(w, "<html><head>%s</head><body>RAW</body></html>", meta)
			}))
			defer srv.Close()

			f := NewFetcher(Options{Resolver: redirectResolverFail})
			got, err := FetchChain(context.Background(), f, FetchRequest{
				URL: redirectReqURL(srv.URL, "ct.example.net", "/page"), Addrs: loopbackAddr()})
			if err != nil {
				t.Fatalf("FetchChain: %v", err)
			}
			if c.follow {
				if body := redirectReadBody(t, got); body != "AFTER" {
					t.Errorf("Content-Type %q: ожидался переход, итог %q", c.ct, body)
				}
				return
			}
			body := redirectReadBody(t, got)
			if !strings.Contains(body, meta) {
				t.Errorf("Content-Type %q: итоговое тело изменено: %q", c.ct, body)
			}
			if len(got.Chain) != 1 {
				t.Errorf("Content-Type %q: цепочка %+v, ожидался 1 шаг", c.ct, got.Chain)
			}
		})
	}
}

// TestRedirectNextNoContentType: a response without Content-Type is not
// scanned for meta refresh (the header decides; httptest cannot serve such
// a response — the Go server substitutes the autodetect, so the branch is
// checked directly).
func TestRedirectNextNoContentType(t *testing.T) {
	html := `<html><head><meta http-equiv="refresh" content="0; url=/after"></head><body>RAW</body></html>`
	base, _ := url.Parse("http://noct.example.net/page")
	res := &FetchResult{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(html)),
		FinalURL:   base,
	}
	if _, follow := redirectNext(res); follow {
		t.Error("ответ без Content-Type не должен уходить в мета-скан")
	}
	got, err := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if err != nil || string(got) != html {
		t.Errorf("тело изменено при пропуске скана: %d байт, err %v", len(got), err)
	}
}

// TestFetchChainMetaRefreshStatuses: the meta scan applies only to 200 —
// a 404 with a meta tag does not hop; for 3xx Location has priority.
func TestFetchChainMetaRefreshStatuses(t *testing.T) {
	const meta = `<meta http-equiv="refresh" content="0; url=/after">`
	t.Run("404-не-сканируется", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusNotFound)
			_, _ = fmt.Fprintf(w, "<html><head>%s</head><body>NOT FOUND</body></html>", meta)
		}))
		defer srv.Close()

		f := NewFetcher(Options{Resolver: redirectResolverFail})
		got, err := FetchChain(context.Background(), f, FetchRequest{
			URL: redirectReqURL(srv.URL, "st.example.net", "/page"), Addrs: loopbackAddr()})
		if err != nil {
			t.Fatalf("FetchChain: %v", err)
		}
		if got.Result.StatusCode != http.StatusNotFound {
			t.Errorf("итоговый статус: %d, ожидался 404", got.Result.StatusCode)
		}
		if len(got.Chain) != 1 {
			t.Errorf("цепочка: %+v, ожидался 1 шаг", got.Chain)
		}
	})

	t.Run("у-3xx-приоритет-location", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/page" {
				w.Header().Set("Location", "/landed")
				w.WriteHeader(http.StatusFound)
				return
			}
			_, _ = fmt.Fprint(w, "BY-LOCATION")
		}))
		defer srv.Close()

		f := NewFetcher(Options{Resolver: redirectResolverFail})
		got, err := FetchChain(context.Background(), f, FetchRequest{
			URL: redirectReqURL(srv.URL, "st.example.net", "/page"), Addrs: loopbackAddr()})
		if err != nil {
			t.Fatalf("FetchChain: %v", err)
		}
		if body := redirectReadBody(t, got); body != "BY-LOCATION" {
			t.Errorf("итоговое тело: %q, ожидался переход по Location", body)
		}
	})
}

// TestFetchChainMetaRefreshBigBody: the scan chunk boundaries — a meta tag
// beyond a single read; without a meta tag the final body is returned in
// full (the scan prefix + the rest of the stream).
func TestFetchChainMetaRefreshBigBody(t *testing.T) {
	pad := strings.Repeat("<!-- padding-padding-padding -->", 1200) // ~35 KiB
	t.Run("мета-за-границей-чанка", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/after" {
				_, _ = fmt.Fprint(w, "AFTER")
				return
			}
			w.Header().Set("Content-Type", "text/html")
			_, _ = fmt.Fprintf(w, "<html><head>%s<meta http-equiv=\"refresh\" content=\"0; url=/after\"></head><body></body></html>", pad)
		}))
		defer srv.Close()

		f := NewFetcher(Options{Resolver: redirectResolverFail})
		got, err := FetchChain(context.Background(), f, FetchRequest{
			URL: redirectReqURL(srv.URL, "big.example.net", "/page"), Addrs: loopbackAddr()})
		if err != nil {
			t.Fatalf("FetchChain: %v", err)
		}
		if body := redirectReadBody(t, got); body != "AFTER" {
			t.Errorf("переход за границей чанков не выполнен, итог %q", body)
		}
	})

	t.Run("тело-возвращается-целиком", func(t *testing.T) {
		want := "<html><head>" + pad + "</head><body>BIG-TAIL</body></html>"
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			_, _ = fmt.Fprint(w, want)
		}))
		defer srv.Close()

		f := NewFetcher(Options{Resolver: redirectResolverFail})
		got, err := FetchChain(context.Background(), f, FetchRequest{
			URL: redirectReqURL(srv.URL, "big.example.net", "/page"), Addrs: loopbackAddr()})
		if err != nil {
			t.Fatalf("FetchChain: %v", err)
		}
		if body := redirectReadBody(t, got); body != want {
			t.Errorf("итоговое тело: %d байт, ожидалось %d (roundtrip нарушен)", len(body), len(want))
		}
		if len(got.Chain) != 1 {
			t.Errorf("цепочка: %+v, ожидался 1 шаг", got.Chain)
		}
	})
}

// TestFetchChainMetaRefreshSelfURL: a meta tag with a url pointing to the
// page itself — a cycle (an exact URL repetition).
func TestFetchChainMetaRefreshSelfURL(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = fmt.Fprintf(w,
			`<html><head><meta http-equiv="refresh" content="0; url=%s"></head><body></body></html>`,
			redirectTestURL(srv.URL, "selfmeta.example.net", "/page"))
	}))
	defer srv.Close()

	f := NewFetcher(Options{Resolver: redirectResolverFail})
	got, err := FetchChain(context.Background(), f, FetchRequest{
		URL: redirectReqURL(srv.URL, "selfmeta.example.net", "/page"), Addrs: loopbackAddr()})
	if !errors.Is(err, ErrTooManyRedirects) {
		t.Fatalf("ожидался ErrTooManyRedirects (цикл), получено: %v", err)
	}
	if len(got.Chain) != 1 {
		t.Errorf("цепочка: %+v, ожидался 1 шаг", got.Chain)
	}
}

// TestFetchChainMetaRefreshUnfollowableURLKeepsBody: the meta scan found a
// url that cannot be followed (a foreign scheme — redirectResolve gives
// false) — the response stays final, and the prefix read by the scan must
// reach the consumer: content/hash must not see a "hole" or emptiness in
// place of the page's first kilobytes.
func TestFetchChainMetaRefreshUnfollowableURLKeepsBody(t *testing.T) {
	want := fmt.Sprintf(
		`<html><head><meta http-equiv="refresh" content="0; url=ftp://elsewhere.example/"></head><body>%s</body></html>`,
		strings.Repeat("page-body ", 8<<10)) // ~80 KiB: well beyond the scan chunks
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = fmt.Fprint(w, want)
	}))
	defer srv.Close()

	f := NewFetcher(Options{Resolver: redirectResolverFail})
	got, err := FetchChain(context.Background(), f, FetchRequest{
		URL: redirectReqURL(srv.URL, "meta-unfollow.example.net", "/page"), Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("FetchChain: %v", err)
	}
	if body := redirectReadBody(t, got); body != want {
		t.Errorf("итоговое тело: %d байт, ожидалось %d (префикс мета-скана потерян)", len(body), len(want))
	}
	if len(got.Chain) != 1 {
		t.Errorf("цепочка: %+v, ожидался 1 шаг (перехода нет)", got.Chain)
	}
}

// TestFetchChainMetaRefreshZeroLimitKeepsBody: a valid meta refresh at
// MaxRedirects 0 — "do not follow" (T-006): the outcome is the first
// response, and its body comes back in full even though the scan has
// already read its start.
func TestFetchChainMetaRefreshZeroLimitKeepsBody(t *testing.T) {
	want := `<html><head><meta http-equiv="refresh" content="0; url=/after"></head><body>ZERO-STAY</body></html>`
	var lands atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/after" {
			lands.Add(1)
			_, _ = fmt.Fprint(w, "AFTER")
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = fmt.Fprint(w, want)
	}))
	defer srv.Close()

	f := NewFetcher(Options{Resolver: redirectResolverFail})
	got, err := FetchChainOpts(context.Background(), f, FetchRequest{
		URL: redirectReqURL(srv.URL, "meta-zero.example.net", "/page"), Addrs: loopbackAddr(),
	}, ChainOptions{MaxRedirects: 0})
	if err != nil {
		t.Fatalf("FetchChainOpts: %v", err)
	}
	if body := redirectReadBody(t, got); body != want {
		t.Errorf("итоговое тело: %q, ожидалось исходное (%d байт)", body, len(want))
	}
	if n := lands.Load(); n != 0 {
		t.Errorf("MaxRedirects 0: выполнено переходов %d", n)
	}
}

// --- hop hosts ----------------------------------------------------------------

// TestFetchChainUnicodeHostInLocation: a unicode host in Location is
// converted to punycode before resolving and the request (spec 5.2
// item 8): the resolver and the Host header see the ASCII form.
func TestFetchChainUnicodeHostInLocation(t *testing.T) {
	const unicodeHost = "пример.испытание"
	asciiHost, err := idn.HostToASCII(unicodeHost)
	if err != nil {
		t.Fatalf("HostToASCII: %v", err)
	}

	var gotHost atomicString
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			_, p, _ := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
			w.Header().Set("Location", fmt.Sprintf("http://%s:%s/land", unicodeHost, p))
			w.WriteHeader(http.StatusFound)
			return
		}
		gotHost.Store(r.Host)
		_, _ = fmt.Fprint(w, "IDN OK")
	}))
	defer srv.Close()

	rs := &redirectResolverStub{}
	f := NewFetcher(Options{Resolver: rs})
	got, err := FetchChain(context.Background(), f, FetchRequest{
		URL: redirectReqURL(srv.URL, "start.example.net", "/start"), Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("FetchChain: %v", err)
	}
	if body := redirectReadBody(t, got); body != "IDN OK" {
		t.Errorf("итоговое тело: %q", body)
	}
	_, srvPort, _ := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	if calls := rs.calls(); len(calls) != 1 || calls[0] != asciiHost {
		t.Errorf("резолверу передан %v, ожидался punycode %q", calls, asciiHost)
	}
	if want := net.JoinHostPort(asciiHost, srvPort); gotHost.Load() != want {
		t.Errorf("Host итогового запроса: %q, ожидался %q", gotHost.Load(), want)
	}
	if host := got.Result.FinalURL.Hostname(); host != asciiHost {
		t.Errorf("итоговый URL-хост: %q, ожидался %q", host, asciiHost)
	}
}

// TestFetchChainSchemeSwitchHTTPToHTTPS: http → https on the same host is
// an ordinary step; the DNS stage addresses apply to it too (the resolver
// is not called).
func TestFetchChainSchemeSwitchHTTPToHTTPS(t *testing.T) {
	tlsSrv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "secure page")
	}))
	tlsSrv.StartTLS()
	defer tlsSrv.Close()
	_, tlsPort, _ := net.SplitHostPort(strings.TrimPrefix(tlsSrv.URL, "https://"))

	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "https://same.example.net:"+tlsPort+"/")
		w.WriteHeader(http.StatusMovedPermanently)
	}))
	defer httpSrv.Close()

	f := NewFetcher(Options{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		Resolver:        redirectResolverFail,
	})
	got, err := FetchChain(context.Background(), f, FetchRequest{
		URL: redirectReqURL(httpSrv.URL, "same.example.net", "/"), Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("FetchChain: %v", err)
	}
	if body := redirectReadBody(t, got); body != "secure page" {
		t.Errorf("итоговое тело: %q", body)
	}
	if got.Result.TLS == nil {
		t.Error("итоговый ответ не по TLS")
	}
	if s := got.Result.FinalURL.Scheme; s != "https" {
		t.Errorf("итоговая схема: %q, ожидался https", s)
	}
	if got.Result.FinalURL.Host != "same.example.net:"+tlsPort {
		t.Errorf("итоговый хост: %q", got.Result.FinalURL.Host)
	}
}

// TestFetchChainOtherHostResolves: a hop to another host — resolving via
// the transport's resolver (the DNS stage addresses do not apply to it).
func TestFetchChainOtherHostResolves(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			_, p, _ := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
			w.Header().Set("Location", "http://other.example.net:"+p+"/x")
			w.WriteHeader(http.StatusFound)
			return
		}
		_, _ = fmt.Fprint(w, "OTHER ", r.Host)
	}))
	defer srv.Close()

	rs := &redirectResolverStub{}
	f := NewFetcher(Options{Resolver: rs})
	got, err := FetchChain(context.Background(), f, FetchRequest{
		URL: redirectReqURL(srv.URL, "main.example.net", "/start"), Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("FetchChain: %v", err)
	}
	if body := redirectReadBody(t, got); !strings.HasPrefix(body, "OTHER ") {
		t.Errorf("итоговое тело: %q", body)
	}
	if calls := rs.calls(); len(calls) != 1 || calls[0] != "other.example.net" {
		t.Errorf("резолвер: %v, ожидался один вызов other.example.net", calls)
	}
	if host := got.Result.FinalURL.Hostname(); host != "other.example.net" {
		t.Errorf("итоговый хост: %q", host)
	}
}

// TestFetchChainInvalidLocation: a broken URL, a foreign scheme and an
// invalid host in Location — no hop possible, the outcome is the response
// itself (like a browser without a hop).
func TestFetchChainInvalidLocation(t *testing.T) {
	cases := []struct {
		name     string
		location string
	}{
		{"битый-url", "http://[::1"},
		{"чужая-схема", "ftp://example.invalid/x"},
		{"битый-punycode", "http://xn--.example.net/"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", c.location)
				w.WriteHeader(http.StatusFound)
			}))
			defer srv.Close()

			f := NewFetcher(Options{Resolver: redirectResolverFail})
			got, err := FetchChain(context.Background(), f, FetchRequest{
				URL: redirectReqURL(srv.URL, "inv.example.net", "/start"), Addrs: loopbackAddr()})
			if err != nil {
				t.Fatalf("FetchChain: %v", err)
			}
			if got.Result.StatusCode != http.StatusFound {
				t.Errorf("итоговый статус: %d, ожидался 302", got.Result.StatusCode)
			}
			if len(got.Chain) != 1 {
				t.Errorf("цепочка: %+v, ожидался 1 шаг", got.Chain)
			}
		})
	}
}

// --- context cancellation -----------------------------------------------------

// TestFetchChainContextCancel: a ctx cancellation in the middle of the
// chain (the second step is silent) — the cancellation error propagates.
func TestFetchChainContextCancel(t *testing.T) {
	stall := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/first" {
			w.Header().Set("Location", "/second")
			w.WriteHeader(http.StatusFound)
			return
		}
		select {
		case <-stall:
		case <-time.After(5 * time.Second):
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer func() {
		select {
		case <-stall:
		default:
			close(stall)
		}
		srv.Close()
	}()

	f := NewFetcher(Options{HeaderTimeout: 10 * time.Second, TotalTimeout: 20 * time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(120*time.Millisecond, cancel)
	start := time.Now()
	got, err := FetchChain(ctx, f, FetchRequest{
		URL: redirectReqURL(srv.URL, "cancel.example.net", "/first"), Addrs: loopbackAddr()})
	elapsed := time.Since(start)
	if err == nil {
		_ = got.Result.Body.Close()
		t.Fatal("ожидалась отмена цепочки")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка не пробрасывает context.Canceled: %v", err)
	}
	if elapsed >= 2*time.Second {
		t.Errorf("отмена сработала поздно: %s", elapsed)
	}
	if len(got.Chain) != 1 {
		t.Errorf("цепочка: %+v, ожидался 1 шаг до отмены", got.Chain)
	}
}

// --- the chain budget -----------------------------------------------------

// TestFetchChainBudget: the total chain budget (ChainOptions.TotalTimeout)
// bounds all hops together, not one by one: a silent hop and a chain of
// slow hops are interrupted with a timeout-classified error (temp_error at
// the consumer, spec 6.2) without holding a worker slot for minutes.
func TestFetchChainBudget(t *testing.T) {
	t.Run("молчаливый хоп прерывается бюджетом", func(t *testing.T) {
		stall := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/start" {
				w.Header().Set("Location", "/final")
				w.WriteHeader(http.StatusFound)
				return
			}
			select {
			case <-stall:
			case <-time.After(5 * time.Second):
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer func() {
			select {
			case <-stall:
			default:
				close(stall)
			}
			srv.Close()
		}()

		// The hop layers do not fire (5 s); the chain budget itself is the
		// one that must interrupt the request.
		f := NewFetcher(Options{HeaderTimeout: 5 * time.Second, TotalTimeout: 5 * time.Second})
		start := time.Now()
		got, err := FetchChainOpts(context.Background(), f, FetchRequest{
			URL: redirectReqURL(srv.URL, "budget.example.net", "/start"), Addrs: loopbackAddr(),
		}, ChainOptions{MaxRedirects: 3, TotalTimeout: 250 * time.Millisecond})
		elapsed := time.Since(start)
		if err == nil || !IsTimeout(err) {
			t.Fatalf("ожидался таймаут бюджета цепочки, получено: %v", err)
		}
		if elapsed >= 2*time.Second {
			t.Errorf("бюджет сработал поздно: %s", elapsed)
		}
		if len(got.Chain) != 1 {
			t.Errorf("цепочка: %+v, ожидался 1 шаг до таймаута", got.Chain)
		}
	})

	t.Run("бюджет копится по хопам", func(t *testing.T) {
		// Each hop fits into its own timeouts (400 ms < 5 s), but three
		// hops do not fit into the 1 s budget: the third is interrupted
		// mid-way. The margins are 200 ms in both directions — robust to
		// jitter under the load of the run.
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(400 * time.Millisecond)
			w.Header().Set("Location", r.URL.Path+"x")
			w.WriteHeader(http.StatusFound)
		}))
		defer srv.Close()

		f := NewFetcher(Options{HeaderTimeout: 5 * time.Second, TotalTimeout: 5 * time.Second})
		got, err := FetchChainOpts(context.Background(), f, FetchRequest{
			URL: redirectReqURL(srv.URL, "slowchain.example.net", "/a"), Addrs: loopbackAddr(),
		}, ChainOptions{MaxRedirects: 3, TotalTimeout: 1 * time.Second})
		if err == nil || !IsTimeout(err) {
			t.Fatalf("ожидался таймаут бюджета цепочки, получено: %v", err)
		}
		if len(got.Chain) != 2 {
			t.Errorf("цепочка: %d шагов, ожидались 2 (третий хоп за бюджетом)", len(got.Chain))
		}
	})
}

// --- IP literals in hops --------------------------------------------------

// TestRedirectResolveIPLiteral: IP literals in Location pass as is —
// without punycode (Hostname() strips the brackets, the former "[" prefix
// check never fired, and HostToASCII fails on IPv6 — the hop never
// happened), with and without a port, IPv4 and IPv6; a foreign scheme is
// still false.
func TestRedirectResolveIPLiteral(t *testing.T) {
	base := &url.URL{Scheme: "http", Host: "start.example-test.net", Path: "/page"}
	cases := []struct {
		raw  string
		want string
		ok   bool
	}{
		{"http://[::1]:8080/next", "http://[::1]:8080/next", true},
		{"http://[2001:db8::1]/", "http://[2001:db8::1]/", true},
		{"http://192.0.2.10/p", "http://192.0.2.10/p", true},
		{"ftp://[::1]/", "", false},
	}
	for _, c := range cases {
		u, ok := redirectResolve(base, c.raw)
		if ok != c.ok {
			t.Errorf("redirectResolve(%q): ok=%v, ожидалось %v", c.raw, ok, c.ok)
			continue
		}
		if ok && u.String() != c.want {
			t.Errorf("redirectResolve(%q) = %q, ожидалось %q", c.raw, u, c.want)
		}
	}
}

// TestFetchChainIPv6LiteralLocation: a Location with an IPv6 literal is an
// ordinary chain hop: the [::1] host is taken literally (lookup does not
// touch the resolver), the outcome is the page over IPv6.
func TestFetchChainIPv6LiteralLocation(t *testing.T) {
	ln, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6-петля недоступна: %v", err)
	}
	srv6 := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprint(w, "V6-LAND")
	})}
	go func() { _ = srv6.Serve(ln) }()
	t.Cleanup(func() { _ = srv6.Close() })
	_, v6port, _ := net.SplitHostPort(ln.Addr().String())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "http://[::1]:"+v6port+"/land")
		w.WriteHeader(http.StatusFound)
	}))
	defer srv.Close()

	f := NewFetcher(Options{Resolver: redirectResolverFail})
	got, err := FetchChain(context.Background(), f, FetchRequest{
		URL: redirectReqURL(srv.URL, "v6start.example.net", "/start"), Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("FetchChain: %v", err)
	}
	if body := redirectReadBody(t, got); body != "V6-LAND" {
		t.Errorf("итоговое тело: %q, ожидался переход на [::1]", body)
	}
	if host := got.Result.FinalURL.Hostname(); host != "::1" {
		t.Errorf("хост итогового URL: %q, ожидался ::1", host)
	}
	if len(got.Chain) != 2 {
		t.Errorf("цепочка: %+v, ожидались 2 шага", got.Chain)
	}
}

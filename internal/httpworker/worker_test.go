// Unit tests of the http-worker (T-053): the domain task processing (a chunk of subdomains,
// both protocol checks), the final URLs' cache: one URL — one
// download), the variants/aliases end-to-end, the SPA flag, subdomain errors do not drop
// the domain, the ctx cancellation, the Run loop's smoke, the DNS resolver adapter. The load
// is substituted by a scenario (the FetchFunc seam), the queue — by a queue.Queue fake;
// no network and Redis — with a single exception: a real
// *httpfetch.BodyLimiter is built only by the real transport, so
// TestWorkerBodyLimiterCeiling works with an httptest loopback server.
// The identifiers with the worker* prefix against collisions with the neighboring
// packages.

package httpworker

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/redis/go-redis/v9"

	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/dnsclient"
	"crawler/internal/httpfetch"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// --- the load fake (the FetchFunc seam) ------------------------------------------

// workerTimeoutErr — a timeout error (net.Error) for the tarpit/overall
// timeout scenario: classified by httpfetch.IsTimeout → ErrTimeout.
type workerTimeoutErr struct{}

func (workerTimeoutErr) Error() string   { return "i/o timeout" }
func (workerTimeoutErr) Timeout() bool   { return true }
func (workerTimeoutErr) Temporary() bool { return true }

// workerPageScript — one scenario page. One fake call depicts
// the whole redirect chain pass: steps — the prehistory, the final step
// is appended by the fake itself.
type workerPageScript struct {
	status    int
	header    http.Header
	body      string
	final     string // the final URL; "" — as requested
	steps     []httpfetch.RedirectStep
	err       error         // a transport error instead of an answer
	tooMany   bool          // ErrTooManyRedirects (Result nil, Chain — steps)
	blockCtx  bool          // wait for the ctx cancellation (the cancellation test)
	setCookie []string      // the Set-Cookie values, if header is not set
	elapsed   time.Duration // the final Fetch's Elapsed (the check's ResponseMS)
}

// workerFetchLog — the fake's call journal: which URLs actually went "to the network".
type workerFetchLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *workerFetchLog) add(u string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, u)
}

func (l *workerFetchLog) countHost(host string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, c := range l.calls {
		if strings.Contains(c, "//"+host+"/") {
			n++
		}
	}
	return n
}

// workerFakeFetch — the "key → page" scenario. The exact key
// "scheme://host/" has the priority, the host key — the fallback of both schemes.
func workerFakeFetch(script map[string]*workerPageScript) (FetchFunc, *workerFetchLog) {
	log := &workerFetchLog{}
	lookup := func(scheme, host string) *workerPageScript {
		if p, ok := script[scheme+"://"+host+"/"]; ok {
			return p
		}
		return script[host]
	}
	return func(ctx context.Context, req httpfetch.FetchRequest, _ httpfetch.ChainOptions) (*httpfetch.ChainResult, error) {
		host := req.URL.Host
		log.add(req.URL.Scheme + "://" + host + "/")
		p := lookup(req.URL.Scheme, host)
		if p == nil {
			return nil, fmt.Errorf("httpfetch: GET %s: нет сценария", req.URL)
		}
		if p.blockCtx {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if p.err != nil {
			return &httpfetch.ChainResult{Chain: p.steps}, p.err
		}
		if p.tooMany {
			return &httpfetch.ChainResult{Chain: p.steps},
				fmt.Errorf("%w: лимит превышен", httpfetch.ErrTooManyRedirects)
		}
		final := p.final
		if final == "" {
			final = req.URL.Scheme + "://" + host + "/"
		}
		fu, _ := url.Parse(final)
		h := p.header
		if h == nil {
			h = http.Header{}
			for _, c := range p.setCookie {
				h.Add("Set-Cookie", c)
			}
		}
		res := &httpfetch.FetchResult{
			StatusCode: p.status,
			Status:     fmt.Sprintf("%d status", p.status),
			Header:     h,
			Body:       io.NopCloser(strings.NewReader(p.body)),
			FinalURL:   fu,
			Elapsed:    p.elapsed,
		}
		chain := append([]httpfetch.RedirectStep(nil), p.steps...)
		chain = append(chain, httpfetch.RedirectStep{URL: final, Status: p.status})
		return &httpfetch.ChainResult{Result: res, Chain: chain}, nil
	}, log
}

// workerWorker — the worker assembly over a scenario. The store by default —
// a fake (the early body storing T-147 is mandatory; the concrete fake —
// newStoreFake from store_test.go, the same package).
func workerWorker(t *testing.T, fetch FetchFunc, mutate func(*Options)) *Worker {
	t.Helper()
	opt := Options{}
	if mutate != nil {
		mutate(&opt)
	}
	if opt.Store == nil {
		opt.Store = newStoreFake()
	}
	w, err := New(fetch, opt)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return w
}

func workerSubMsg(id int16, label string) queue.HTTPSubdomain {
	return queue.HTTPSubdomain{SubdomainID: id, Label: label, Addrs: []string{"192.0.2.1"}}
}

// --- end-to-end processing: variants, aliases, the cache -------------------------------

// TestWorkerProcessDomain — a task of three subdomains:
//
//   - the bare apex: https → a redirect to https://www.example.com/ (the home body);
//     http leads there too — the variants fold into one variant 0 record
//     (https);
//   - www: the https check starts at a URL already downloaded by the bare
//     check (its chain's outcome) — the final URLs' cache, WITHOUT the network (§5.5 item 6);
//     http leads to another site (http://old.example.com/) → the second
//     variant 1;
//   - api: https does not answer (a timeout), http works → the only
//     variant 0 record with scheme http.
//
// The aliases (T-055): the apex's record and the www variant 0 converge on one final
// URL → an equivalence class, the original — the www "home" record, the apex
// is marked by a redirect to the www subdomain.
func TestWorkerProcessDomain(t *testing.T) {
	home := "<html><body>" + strings.Repeat("достаточно видимого текста главной страницы сайта ", 12) + "</body></html>"
	old := "<html><body>совсем другой сайт на старом адресе, много текста</body></html>"
	fetch, log := workerFakeFetch(map[string]*workerPageScript{
		"example.com": { // both bare checks: a redirect to www
			status: 301, body: home, final: "https://www.example.com/",
			steps: []httpfetch.RedirectStep{{URL: "https://example.com/", Status: 301}},
		},
		"http://www.example.com/": {
			status: 200, body: old, final: "http://old.example.com/",
			steps: []httpfetch.RedirectStep{{URL: "http://www.example.com/", Status: 301}},
		},
		"https://api.example.com/": {err: workerTimeoutErr{}},
		"api.example.com":          {status: 200, body: "<html>api gateway page</html>"},
	})
	// PerDomain=1: the checks run strictly in the launch order —
	// the www https cache hit (bare's outcome) is deterministic, without the race
	// "www started earlier than bare finished the chain".
	w := workerWorker(t, fetch, func(o *Options) { o.PerDomain = 1 })

	res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 1, DomainID: 7, Name: "example.com",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, ""), workerSubMsg(1, "www"), workerSubMsg(2, "api")},
	})
	if err != nil {
		t.Fatalf("ProcessDomain: %v", err)
	}

	// The network: bare both checks (2) + www http (1) + api both (2); the www https
	// is taken from the final URLs' cache — there are no https www downloads.
	for host, want := range map[string]int{
		"example.com": 2, "www.example.com": 1, "api.example.com": 2,
	} {
		if got := log.countHost(host); got != want {
			t.Errorf("загрузок %s: %d, ожидалось %d", host, got, want)
		}
	}

	if !res.HasRecords() || res.RecordCount() != 4 {
		t.Fatalf("RecordCount: %d (HasRecords %v), ожидалось 4 (bare 1 + www 2 + api 1)",
			res.RecordCount(), res.HasRecords())
	}
	bare, www, api := &res.Subdomains[0], &res.Subdomains[1], &res.Subdomains[2]

	// The cache hit is marked; the final URL and the synthetic chain are in place.
	if !www.HTTPS.Cached {
		t.Error("https-проверка www не взята из кэша итоговых URL")
	}
	if bare.HTTP.Cached {
		t.Error("http-проверка bare стартует на своём URL — кэш неприменим")
	}
	if bare.HTTPS.FinalURL == nil || bare.HTTPS.FinalURL.String() != "https://www.example.com/" {
		t.Errorf("итоговый URL bare https: %v", bare.HTTPS.FinalURL)
	}
	if len(bare.HTTPS.Redirects) != 2 { // start + outcome
		t.Errorf("цепочка bare https: %d шагов, ожидалось 2", len(bare.HTTPS.Redirects))
	}
	if len(www.HTTPS.Redirects) != 1 { // a synthetic single step
		t.Errorf("цепочка кэш-итога www https: %d шагов, ожидался 1", len(www.HTTPS.Redirects))
	}

	// bare: both protocols — one site (the same final host+path) → 1 record.
	if len(bare.Records) != 1 || bare.Records[0].Variant != model.VariantMain ||
		bare.Records[0].Scheme != model.SchemeHTTPS {
		t.Fatalf("записи bare: %+v", bare.Records)
	}
	// www: two variants — 0 (https, the cache outcome) and 1 (http, another site).
	if len(www.Records) != 2 {
		t.Fatalf("записи www: %d, ожидалось 2", len(www.Records))
	}
	if www.Records[0].Variant != model.VariantMain || www.Records[0].Scheme != model.SchemeHTTPS {
		t.Errorf("вариант 0 www: variant=%v scheme=%v", www.Records[0].Variant, www.Records[0].Scheme)
	}
	if www.Records[1].Variant != model.VariantHTTP || www.Records[1].Scheme != model.SchemeHTTP {
		t.Errorf("вариант 1 www: variant=%v scheme=%v", www.Records[1].Variant, www.Records[1].Scheme)
	}
	if www.Records[1].Host != "old.example.com" || www.Records[1].FinalURL != "http://old.example.com/" {
		t.Errorf("итог варианта 1 www: host=%q url=%q", www.Records[1].Host, www.Records[1].FinalURL)
	}
	// api: only http → one variant 0 scheme http record.
	if len(api.Records) != 1 || api.Records[0].Scheme != model.SchemeHTTP ||
		api.Records[0].Variant != model.VariantMain {
		t.Fatalf("записи api: %+v", api.Records)
	}
	if api.HTTPS.ErrClass != ErrTimeout || !api.HTTPS.ErrClass.IsTemp() {
		t.Errorf("класс ошибки api https: %v", api.HTTPS.ErrClass)
	}

	// The aliases: bare v0 and www v0 — one final URL; the original — www
	// ("at home"), bare — a redirect to the www subdomain; the original stores the content.
	bareRec, wwwMain := &bare.Records[0], &www.Records[0]
	if bareRec.AliasKind != model.AliasRedirect || bareRec.AliasSubdomainID == nil || *bareRec.AliasSubdomainID != 1 {
		t.Errorf("алиас bare: kind=%v id=%v", bareRec.AliasKind, bareRec.AliasSubdomainID)
	}
	if bareRec.StoreContent {
		t.Error("контент алиаса не хранится (StoreContent у не-оригинала)")
	}
	if wwwMain.AliasKind != model.AliasNone || wwwMain.AliasSubdomainID != nil {
		t.Errorf("алиас www v0: kind=%v id=%v", wwwMain.AliasKind, wwwMain.AliasSubdomainID)
	}
	if !wwwMain.StoreContent || wwwMain.ContentHash == nil {
		t.Error("оригинал класса обязан хранить контент с ненулевым хэшем")
	}
	// The cache outcome's hash matches the primary source's hash (the same body).
	if bareRec.ContentHash == nil || *bareRec.ContentHash != *wwwMain.ContentHash {
		t.Error("хэши одной страницы (первоисточник и кэш) расходятся")
	}
	// The www variant 1 and api are independent.
	if www.Records[1].AliasKind != model.AliasNone || api.Records[0].AliasKind != model.AliasNone {
		t.Error("чужие записи помечены алиасами")
	}
	// The DNS stage's data carried into the result (spec 5.5 item 7).
	if len(api.Addrs) != 1 || api.Addrs[0].String() != "192.0.2.1" {
		t.Errorf("адреса api: %v", api.Addrs)
	}
}

// TestWorkerCacheKey — the cache key's normalization: scheme+host+path; the query,
// the fragment and the schemes' default ports (80/443) are not included, a non-standard port
// distinguishes the keys, the host case does not matter (the T-054 criterion).
func TestWorkerCacheKey(t *testing.T) {
	base, err := url.Parse("https://www.example.com/some/path")
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		"https://www.example.com/some/path",
		"https://WWW.EXAMPLE.COM/some/path",
		"https://www.example.com:443/some/path",
		"https://www.example.com/some/path?i=12",
		"https://www.example.com/some/path#frag",
	} {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if workerCacheKey(u) != workerCacheKey(base) {
			t.Errorf("ключ %q ≠ ключу базы", raw)
		}
	}
	httpBase, err := url.Parse("http://www.example.com/some/path")
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse("http://www.example.com:80/some/path")
	if err != nil {
		t.Fatal(err)
	}
	if workerCacheKey(u) != workerCacheKey(httpBase) {
		t.Error("ключ с явным дефолтным портом :80 ≠ ключу без порта")
	}
	for _, raw := range []string{
		"http://www.example.com/some/path",          // another scheme
		"https://old.example.com/some/path",         // another host
		"https://www.example.com/other",             // another path
		"https://www.example.com",                   // an empty path — the root "/"
		"https://www.example.com:8443/some/path",    // a non-standard port — another service
		"https://www.example.com:444/some/path?q=1", // the port is not muffled by a query
	} {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if workerCacheKey(u) == workerCacheKey(base) {
			t.Errorf("ключ %q совпал с ключом базы", raw)
		}
	}
	if workerCacheKey(nil) != "" {
		t.Error("ключ nil-URL не пуст")
	}
}

// TestWorkerCacheQueryAndTaskScope — a final URL with a query is put into the cache
// under a key without the query (agreed with the T-054 variants criterion): the
// park check's start without a query hits the cache. The cache lives one task: a second
// task of the same domain downloads anew.
func TestWorkerCacheQueryAndTaskScope(t *testing.T) {
	body := "<html>" + strings.Repeat("текст парковочной страницы для проверки кэша ", 40) + "</html>"
	fetch, log := workerFakeFetch(map[string]*workerPageScript{
		"bare.test":              {status: 301, body: body, final: "https://park.bare.test/?i=1"},
		"park.bare.test":         {status: 200, body: body, final: "https://park.bare.test/"},
		"http://bare.test/":      {status: 301, body: body, final: "https://park.bare.test/?i=2"},
		"http://park.bare.test/": {status: 200, body: body, final: "https://park.bare.test/"},
	})
	// PerDomain=1 — a deterministic check order (see TestWorkerProcessDomain).
	w := workerWorker(t, fetch, func(o *Options) { o.PerDomain = 1 })
	subs := []queue.HTTPSubdomain{workerSubMsg(0, ""), workerSubMsg(3, "park")}

	res1, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 1, DomainID: 7, Name: "bare.test", Subdomains: subs,
	})
	if err != nil {
		t.Fatalf("ProcessDomain(1): %v", err)
	}
	// bare: both checks lead to https://park.bare.test/?i=N — the key
	// https://park.bare.test/ (the query is not included); the park https check
	// starts exactly at it → the cache, without the network. Only the http
	// request went to the network over park.
	if !res1.Subdomains[1].HTTPS.Cached {
		t.Error("итог с query не найден в кэше по ключу без query")
	}
	if got := log.countHost("park.bare.test"); got != 1 {
		t.Errorf("обращений к park в первой задаче: %d, ожидался 1 (только http)", got)
	}

	// A per-task cache: the second task WITHOUT bare — its cache is empty at the start,
	// the https park is downloaded for real (if the cache survived the task,
	// the park checks would again land in the first task's records).
	res2, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 1, DomainID: 8, Name: "bare.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(3, "park")},
	})
	if err != nil {
		t.Fatalf("ProcessDomain(2): %v", err)
	}
	if res2.Subdomains[0].HTTPS.Cached {
		t.Error("кэш пережил задачу домена")
	}
	if got := log.countHost("park.bare.test"); got != 3 { // 1 (the first's http) + https + http of the second
		t.Errorf("обращений к park после второй задачи: %d, ожидалось 3", got)
	}
}

// TestWorkerCacheHitKeepsErrorClass — a truncated page in the cache
// gives its error class to the cache hit (ErrBody): the same
// content must not be classified differently depending on
// the cache hit (the source record — error=body, the cache record — NULL).
func TestWorkerCacheHitKeepsErrorClass(t *testing.T) {
	huge := strings.Repeat("a", int(workerBodyHardLimit)+128)
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		// Both apex checks converge on the truncated www page
		// (https — a download, http — a network failure).
		"trunc.test":         {status: 200, body: huge, final: "https://www.trunc.test/"},
		"http://trunc.test/": {err: errors.New("connection refused")},
	})
	// PerDomain=1 — a deterministic order: the apex's https fills the
	// cache before the https www starts (see TestWorkerProcessDomain).
	w := workerWorker(t, fetch, func(o *Options) { o.PerDomain = 1 })
	res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 9, DomainID: 21, Name: "trunc.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, ""), workerSubMsg(1, "www")},
	})
	if err != nil {
		t.Fatalf("ProcessDomain: %v", err)
	}
	www := &res.Subdomains[1]
	if !www.HTTPS.Cached || !www.HTTPS.Truncated {
		t.Fatalf("https www — не кэш-итог обрезанной страницы: cached=%v truncated=%v",
			www.HTTPS.Cached, www.HTTPS.Truncated)
	}
	if www.HTTPS.ErrClass != ErrBody || www.HTTPS.ErrText == "" {
		t.Errorf("кэш-попадание потеряло класс ошибки обрезанной страницы: %v текст %q",
			www.HTTPS.ErrClass, www.HTTPS.ErrText)
	}
	// Cached=true — the network was not touched at all (the www http check at
	// the error scenario honestly carries ErrNetwork, not substituted by the cache).
	if www.HTTP.ErrClass != ErrNetwork {
		t.Errorf("http www: класс %v (ожидался ErrNetwork)", www.HTTP.ErrClass)
	}
	// The class reaches the cache source's record too.
	if len(www.Records) == 0 {
		t.Fatal("у www нет записей, хотя https-итог доступен")
	}
	if www.Records[0].ErrClass != ErrBody {
		t.Errorf("запись кэш-источника: класс %v (ожидался ErrBody)", www.Records[0].ErrClass)
	}
}

// TestWorkerCacheHitResponseMS — the cache hit carries the response
// time measured by the primary source: the cache outcome often becomes the alias
// class's original record (bare→www); its response_ms must not be
// zero with a live measurement. The cache hit metrics still do not
// observe it (Cached).
func TestWorkerCacheHitResponseMS(t *testing.T) {
	body := "<html>" + strings.Repeat("страница для теста времени ответа кэша ", 30) + "</html>"
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"rt.test": {status: 200, body: body, final: "https://www.rt.test/", elapsed: 150 * time.Millisecond},
		// the apex's http check — another outcome (not the cache), its own time.
		"http://rt.test/": {status: 200, body: body, final: "http://old.rt.test/", elapsed: 40 * time.Millisecond},
	})
	w := workerWorker(t, fetch, func(o *Options) { o.PerDomain = 1 })
	res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 12, DomainID: 31, Name: "rt.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, ""), workerSubMsg(1, "www")},
	})
	if err != nil {
		t.Fatalf("ProcessDomain: %v", err)
	}
	bare, www := &res.Subdomains[0], &res.Subdomains[1]
	if !www.HTTPS.Cached {
		t.Fatal("https www — не кэш-итог (порядок проверок сломан)")
	}
	if got := www.HTTPS.ResponseMS; got != 150 {
		t.Errorf("ResponseMS кэш-попадания: %d, ожидалось измерение первоисточника 150", got)
	}
	if got := bare.HTTPS.ResponseMS; got != 150 {
		t.Errorf("ResponseMS первоисточника: %d, ожидалось 150", got)
	}
	if got := bare.HTTP.ResponseMS; got != 40 {
		t.Errorf("ResponseMS собственной (некэшовой) проверки: %d, ожидалось 40", got)
	}
	// The cache source's record carries the response time into subdomain_results.
	if len(www.Records) == 0 {
		t.Fatal("у www нет записей")
	}
	if got := www.Records[0].ResponseMS; got != 150 {
		t.Errorf("ResponseMS записи кэш-источника: %d, ожидалось 150", got)
	}
}

// --- errors and special cases ----------------------------------------------------

// TestWorkerErrorsClassified — the subdomains' errors (a redirect loop, a network
// refusal, a truncated body) are classified into the result and do not drop the domain
// (§8): another subdomain gives records.
func TestWorkerErrorsClassified(t *testing.T) {
	huge := strings.Repeat("a", int(httpfetch.DefaultLimitBodyBytes)+128)
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"test": {tooMany: true, steps: []httpfetch.RedirectStep{
			{URL: "https://test/1", Status: 301}, {URL: "https://test/2", Status: 301},
		}},
		"http://test/": {err: errors.New("connection refused")},
		"big.test":     {status: 200, body: huge},
		"http://big.test/": {status: 200, body: "<html>зеркало на старом адресе с обычным текстом</html>",
			final: "http://old.big.test/"},
	})
	w := workerWorker(t, fetch, nil)
	res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 2, DomainID: 9, Name: "test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, ""), workerSubMsg(4, "big")},
	})
	if err != nil {
		t.Fatalf("ProcessDomain: %v", err)
	}

	apex := &res.Subdomains[0]
	if apex.HTTPS.ErrClass != ErrRedirects || apex.HTTPS.Available {
		t.Errorf("https апекса: класс %v available %v (ожидался redirects/недоступен)",
			apex.HTTPS.ErrClass, apex.HTTPS.Available)
	}
	if len(apex.HTTPS.Redirects) != 2 {
		t.Errorf("выполненная часть цепочки потеряна: %d шагов", len(apex.HTTPS.Redirects))
	}
	if apex.HTTP.ErrClass != ErrNetwork || apex.HTTP.ErrText == "" {
		t.Errorf("http апекса: класс %v текст %q", apex.HTTP.ErrClass, apex.HTTP.ErrText)
	}
	if len(apex.Records) != 0 {
		t.Errorf("записей у молчащего апекса: %d", len(apex.Records))
	}

	big := &res.Subdomains[1]
	if !big.HTTPS.Available || !big.HTTPS.Truncated || big.HTTPS.ErrClass != ErrBody {
		t.Errorf("обрезанное тело: available=%v truncated=%v класс=%v",
			big.HTTPS.Available, big.HTTPS.Truncated, big.HTTPS.ErrClass)
	}
	if !big.HTTP.Available || big.HTTP.Truncated {
		t.Errorf("http big: available=%v truncated=%v", big.HTTP.Available, big.HTTP.Truncated)
	}
	// big: https — a truncated body, http — another site → 2 records; the
	// source check's error class reaches the record.
	if len(big.Records) != 2 {
		t.Fatalf("записей big: %d, ожидалось 2", len(big.Records))
	}
	if big.Records[0].ErrClass != ErrBody || !big.Records[0].Truncated {
		t.Errorf("класс ошибки не дошёл до записи: %v truncated=%v",
			big.Records[0].ErrClass, big.Records[0].Truncated)
	}
	if !res.HasRecords() {
		t.Error("домен без записей, хотя big ответил")
	}
	if res.AnyTimeout() {
		t.Error("таймаутов в задаче не было")
	}
}

// TestWorkerBodyLimiterCeiling — the body cap is set by the BodyLimiter T-049
// (http.max_body_bytes), not the worker's guard constant: a page
// of 3 MiB at a 4 MiB cap is read entirely (the 2 MiB guard would cut
// it with a false ErrBody); a page of 5 MiB at a 4 MiB cap is cut
// by the limiter. Only the real transport: a real BodyLimiter cannot be
// built by a FetchFunc seam scenario — an httptest loopback server.
func TestWorkerBodyLimiterCeiling(t *testing.T) {
	caseRun := func(t *testing.T, serve, ceiling int64) (*SchemeCheck, []byte) {
		t.Helper()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, strings.Repeat("a", int(serve)))
		}))
		defer srv.Close()
		u, err := url.Parse(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		f := httpfetch.NewFetcher(httpfetch.Options{
			Limits: &httpfetch.LimitOptions{MaxBodyBytes: ceiling},
		})
		w, err := New(ChainFetch(f), Options{Store: newStoreFake()})
		if err != nil {
			t.Fatal(err)
		}
		res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
			ZoneID: 1, DomainID: 1, Name: u.Host,
			Subdomains: []queue.HTTPSubdomain{{SubdomainID: 0, Addrs: []string{"127.0.0.1"}}},
		})
		if err != nil {
			t.Fatalf("ProcessDomain: %v", err)
		}
		// ProcessDomain holds no bodies (the early Put T-147) —
		// the expected bytes are compared by hash.
		wantLen := serve
		if wantLen > ceiling {
			wantLen = ceiling
		}
		return &res.Subdomains[0].HTTP, []byte(strings.Repeat("a", int(wantLen))) // the httptest server is http
	}
	t.Run("3 МиБ при потолке 4 МиБ — целая", func(t *testing.T) {
		check, want := caseRun(t, 3<<20, 4<<20)
		if !check.Available || check.Truncated || check.ErrClass != ErrNone || check.ErrText != "" {
			t.Fatalf("available=%v truncated=%v класс=%v текст=%q — страница ниже потолка читается целиком",
				check.Available, check.Truncated, check.ErrClass, check.ErrText)
		}
		if check.ContentHash == nil || *check.ContentHash != content.HashOf(want) {
			t.Error("хэш тела не совпадает с хэшем целой страницы (обрезка страховкой 2 МиБ?)")
		}
	})
	t.Run("5 МиБ при потолке 4 МиБ — обрезка лимитёром", func(t *testing.T) {
		check, want := caseRun(t, 5<<20, 4<<20)
		if !check.Available || !check.Truncated || check.ErrClass != ErrBody {
			t.Fatalf("available=%v truncated=%v класс=%v — обрезка настроенным max_body_bytes",
				check.Available, check.Truncated, check.ErrClass)
		}
		if check.ContentHash == nil || *check.ContentHash != content.HashOf(want) {
			t.Errorf("хэш тела не совпадает с хэшем обрезанной до потолка страницы (%d байт)", len(want))
		}
	})
}

// TestWorkerReadBody — the preallocation of the body read buffer by Content-Length
// (T-147): the bytes are read entirely by one buffer, a Content-Length without
// Content-Encoding sets the capacity (a server's lie in both directions does not
// break the read), a compressed answer and an absent header — the ordinary path,
// the guard cap without a BodyLimiter cuts the read (workerBodyHardLimit).
func TestWorkerReadBody(t *testing.T) {
	mkRes := func(cl string, ce string, body string, limiter *httpfetch.BodyLimiter) *httpfetch.FetchResult {
		h := http.Header{}
		if cl != "" {
			h.Set("Content-Length", cl)
		}
		if ce != "" {
			h.Set("Content-Encoding", ce)
		}
		return &httpfetch.FetchResult{
			Header:      h,
			Body:        io.NopCloser(strings.NewReader(body)),
			BodyLimiter: limiter,
		}
	}
	cases := []struct {
		name   string
		cl     string
		ce     string
		body   string
		want   string
		maxCap int // the buffer capacity's cap (0 — do not check): one
		// allocation by Content-Length plus the ReadFrom's service slack
		// (minRead 512), without the io.ReadAll doublings
	}{
		{"Content-Length честен", "9", "", "123456789", "123456789", 9 + 512},
		{"сервер недослал", "100", "", "коротко", "коротко", 100 + 512},
		{"тело 64 КиБ одним буфером", "65536", "", strings.Repeat("z", 65536), strings.Repeat("z", 65536), 65536 + 512},
		{"сервер переслал", "2", "", "длиннее заявленного", "длиннее заявленного", 0},
		{"без Content-Length", "", "", "чанкованный ответ", "чанкованный ответ", 0},
		{"сжатый ответ", "4", "gzip", "распакованные байты", "распакованные байты", 0},
		{"кривой Content-Length", "много", "", "тело", "тело", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := workerReadBody(mkRes(c.cl, c.ce, c.body, nil))
			if err != nil {
				t.Fatalf("workerReadBody: %v", err)
			}
			if string(got) != c.want {
				t.Errorf("тело %q, ожидалось %q", got, c.want)
			}
			if c.maxCap > 0 && cap(got) > c.maxCap {
				t.Errorf("ёмкость буфера %d — выше выделения по Content-Length+%d (удвоения ReadAll?)", cap(got), 512)
			}
		})
	}
	// A giant Content-Length does not reserve a gigabyte — the
	// workerPreallocMax preallocation cap, the read continues by the buffer growth.
	res := mkRes("1073741824", "", strings.Repeat("x", 128), nil)
	got, err := workerReadBody(res)
	if err != nil || len(got) != 128 {
		t.Fatalf("гигантский Content-Length: байтов %d, ошибка %v", len(got), err)
	}
	// Without a BodyLimiter the read is bounded by the worker's guard cap.
	big := strings.Repeat("y", int(workerBodyHardLimit)+64)
	got, err = workerReadBody(mkRes("", "", big, nil))
	if err != nil {
		t.Fatalf("workerReadBody без лимитёра: %v", err)
	}
	if int64(len(got)) != workerBodyHardLimit+1 {
		t.Errorf("прочитано %d байт, ожидалось %d (LimitReader потолка+1)", len(got), workerBodyHardLimit+1)
	}
}

// TestWorkerAllFailedNoHTTP — all the subdomains are erroneous → no records ("no
// one answered" by §7 — T-058 will assign the status); the timeout class is visible
// via AnyTimeout (the material for temp_error, spec 6.2).
func TestWorkerAllFailedNoHTTP(t *testing.T) {
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"dead.test":              {err: workerTimeoutErr{}},
		"http://dead.test/":      {err: workerTimeoutErr{}},
		"gone.dead.test":         {tooMany: true},
		"http://gone.dead.test/": {err: workerTimeoutErr{}},
	})
	w := workerWorker(t, fetch, nil)
	res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 3, DomainID: 11, Name: "dead.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, ""), workerSubMsg(2, "gone")},
	})
	if err != nil {
		t.Fatalf("ProcessDomain: %v", err)
	}
	if res.HasRecords() || res.RecordCount() != 0 {
		t.Fatalf("все проверки упали, а записи есть: %d", res.RecordCount())
	}
	if !res.AnyTimeout() {
		t.Error("таймаут-класс не виден в агрегате")
	}
}

// --- dead-host cutoff (T-164) --------------------------------------------------

// workerDialRefusedErr — a connect-phase error in the form the real
// net.Dialer dial returns it (IsConnectFailure distinguishes by
// *net.OpError{Op:"dial"}): an RST on a SYN — the port is closed.
func workerDialRefusedErr() error {
	return &net.OpError{
		Op: "dial", Net: "tcp",
		Addr: &net.TCPAddr{IP: net.IPv4(192, 0, 2, 10).To4(), Port: 80},
		Err:  &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED},
	}
}

// workerDialTimeoutErr — a real dial's connect timeout: the attempt's
// deadline (poll.ErrDeadlineExceeded, a net.Error with Timeout()).
func workerDialTimeoutErr() error {
	return &net.OpError{
		Op: "dial", Net: "tcp",
		Addr: &net.TCPAddr{IP: net.IPv4(192, 0, 2, 11).To4(), Port: 443},
		Err:  os.ErrDeadlineExceeded,
	}
}

// workerDeadDialScript — the "dead head" scenario: apex and www — a
// connect-phase refusal over both protocols; alive — a live page for the subdomain
// the cutoff must leave unchecked.
func workerDeadDialScript(deadErr func() error, alive string) map[string]*workerPageScript {
	if deadErr == nil {
		deadErr = workerDialRefusedErr
	}
	script := map[string]*workerPageScript{}
	for _, host := range []string{"example.net", "www.example.net"} {
		script[host] = &workerPageScript{err: deadErr()}
	}
	if alive != "" {
		script[alive] = &workerPageScript{status: 200, body: "<html>живой субдомен с обычным текстом страницы</html>"}
	}
	return script
}

// TestWorkerDeadHostCutoff — the dead host's signature (both head names
// fell at the connect phase over both protocols) finishes the task without checking
// the other subdomains: their network is not touched, the checks — the synthetic
// ErrCutoff (observability), no records (§8), the domain — no_http; the variant with
// connect timeouts keeps the temp_error semantics (AnyTimeout).
func TestWorkerDeadHostCutoff(t *testing.T) {
	t.Run("refused — no_http", func(t *testing.T) {
		fetch, log := workerFakeFetch(workerDeadDialScript(nil, "blog.example.net"))
		w := workerWorker(t, fetch, func(o *Options) { o.DeadHostCutoff = true })
		res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
			ZoneID: 4, DomainID: 21, Name: "example.net",
			Subdomains: []queue.HTTPSubdomain{
				workerSubMsg(0, ""), workerSubMsg(1, "www"),
				workerSubMsg(3, "blog"), workerSubMsg(4, "shop"),
			},
		})
		if err != nil {
			t.Fatalf("ProcessDomain: %v", err)
		}
		if !res.Cutoff {
			t.Fatal("cutoff не сработал: оба головных хоста TCP-мёртвы")
		}
		// The network: only the head (4 checks), the tail was not checked.
		for host, want := range map[string]int{
			"example.net": 2, "www.example.net": 2,
			"blog.example.net": 0, "shop.example.net": 0,
		} {
			if got := log.countHost(host); got != want {
				t.Errorf("загрузок %s: %d, ожидалось %d", host, got, want)
			}
		}
		// The head: the real error class (refused — network) + the dead
		// dial sign.
		for i := 0; i < 2; i++ {
			sub := &res.Subdomains[i]
			for _, c := range []SchemeCheck{sub.HTTPS, sub.HTTP} {
				if c.Available || c.DeadDial != true || c.ErrClass != ErrNetwork {
					t.Errorf("голова %q %v: available=%v deadDial=%v класс=%v, ожидалось false/true/network",
						sub.Hostname, c.Scheme, c.Available, c.DeadDial, c.ErrClass)
				}
			}
		}
		// The tail: the synthetic ErrCutoff, the identity and the DNS stage's addresses
		// preserved, no records (§8).
		for i := 2; i < len(res.Subdomains); i++ {
			sub := &res.Subdomains[i]
			if sub.SubdomainID == 0 || sub.Hostname == "" || len(sub.Addrs) == 0 {
				t.Fatalf("хвост #%d: идентичность потеряна: %+v", i, sub)
			}
			for _, c := range []SchemeCheck{sub.HTTPS, sub.HTTP} {
				if c.ErrClass != ErrCutoff || c.ErrText == "" || c.Available || c.DeadDial {
					t.Errorf("хвост %q %v: класс=%v available=%v deadDial=%v, ожидался cutoff/false/false",
						sub.Hostname, c.Scheme, c.ErrClass, c.Available, c.DeadDial)
				}
			}
			if len(sub.Records) != 0 {
				t.Errorf("хвост %q: записи пропущенного субдомена: %d", sub.Hostname, len(sub.Records))
			}
		}
		if res.HasRecords() {
			t.Error("записи у целиком мёртвого домена")
		}
		if res.AnyTimeout() {
			t.Error("refused — не таймаут, temp_error быть не должно")
		}
	})

	t.Run("connect-таймауты — temp_error-семантика сохранена", func(t *testing.T) {
		fetch, _ := workerFakeFetch(workerDeadDialScript(workerDialTimeoutErr, "blog.example.net"))
		w := workerWorker(t, fetch, func(o *Options) { o.DeadHostCutoff = true })
		res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
			ZoneID: 4, DomainID: 22, Name: "example.net",
			Subdomains: []queue.HTTPSubdomain{
				workerSubMsg(0, ""), workerSubMsg(1, "www"), workerSubMsg(3, "blog"),
			},
		})
		if err != nil {
			t.Fatalf("ProcessDomain: %v", err)
		}
		if !res.Cutoff || res.HasRecords() {
			t.Fatalf("cutoff=%v записей=%d, ожидалось true/0", res.Cutoff, res.RecordCount())
		}
		apex := &res.Subdomains[0]
		if apex.HTTPS.ErrClass != ErrTimeout || !apex.HTTPS.DeadDial {
			t.Errorf("apex https: класс=%v deadDial=%v, ожидался timeout/true",
				apex.HTTPS.ErrClass, apex.HTTPS.DeadDial)
		}
		// The performed checks' timeouts decide the episode's repeat (T-031),
		// the tail's ErrCutoff gives no temp_error.
		if !res.AnyTimeout() {
			t.Error("connect-таймауты головы не видны в AnyTimeout (temp_error)")
		}
	})
}

// TestWorkerDeadHostCutoffStrict — the signature is strict: any head outcome
// other than a dead dial (a live check, a TLS failure after connect, a headers
// timeout, the redirect host's fall) or the www's absence in the task —
// the domain is checked in full, the tail sees the network.
func TestWorkerDeadHostCutoffStrict(t *testing.T) {
	alive := "<html>страница живого апекса с достаточным текстом</html>"
	cases := []struct {
		name   string
		script map[string]*workerPageScript
		// heads: if true — it is also checked that the DeadDial sign is not
		// set by foreign classes (see the redirect branch).
		checkApexDeadDialFalse bool
	}{
		{
			name: "apex https жив — www мёртв не сигнатура",
			script: func() map[string]*workerPageScript {
				s := workerDeadDialScript(nil, "blog.example.net")
				s["example.net"] = &workerPageScript{status: 200, body: alive}
				return s
			}(),
		},
		{
			name: "TLS-сбой после connect — соединение было",
			script: func() map[string]*workerPageScript {
				s := workerDeadDialScript(nil, "blog.example.net")
				s["https://example.net/"] = &workerPageScript{err: x509.UnknownAuthorityError{}}
				return s
			}(),
			checkApexDeadDialFalse: true,
		},
		{
			name: "таймаут заголовков — connect прошёл",
			script: func() map[string]*workerPageScript {
				s := workerDeadDialScript(nil, "blog.example.net")
				s["example.net"] = &workerPageScript{err: workerTimeoutErr{}}
				return s
			}(),
			checkApexDeadDialFalse: true,
		},
		{
			name: "падение хоста редиректа — выполненная часть цепочки не пуста",
			script: func() map[string]*workerPageScript {
				s := workerDeadDialScript(nil, "blog.example.net")
				// the apex https answered 301 and led to a dead host: the dial
				// failure — the hop's target, not the starting host.
				s["https://example.net/"] = &workerPageScript{
					err:   workerDialRefusedErr(),
					steps: []httpfetch.RedirectStep{{URL: "https://example.net/", Status: 301}},
				}
				return s
			}(),
			checkApexDeadDialFalse: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fetch, log := workerFakeFetch(tc.script)
			w := workerWorker(t, fetch, func(o *Options) { o.DeadHostCutoff = true })
			res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
				ZoneID: 4, DomainID: 23, Name: "example.net",
				Subdomains: []queue.HTTPSubdomain{
					workerSubMsg(0, ""), workerSubMsg(1, "www"), workerSubMsg(3, "blog"),
				},
			})
			if err != nil {
				t.Fatalf("ProcessDomain: %v", err)
			}
			if res.Cutoff {
				t.Error("cutoff сработал без полной сигнатуры")
			}
			if got := log.countHost("blog.example.net"); got != 2 {
				t.Errorf("загрузок blog: %d, ожидалось 2 — хвост обязан проверяться", got)
			}
			if tc.checkApexDeadDialFalse && res.Subdomains[0].HTTPS.DeadDial {
				t.Error("DeadDial у проверки, чей сбой не connect-фаза стартового хоста")
			}
		})
	}

	t.Run("www нет в задаче — сигнатура не собрана", func(t *testing.T) {
		script := workerDeadDialScript(nil, "blog.example.net")
		delete(script, "www.example.net")
		fetch, log := workerFakeFetch(script)
		w := workerWorker(t, fetch, func(o *Options) { o.DeadHostCutoff = true })
		res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
			ZoneID: 4, DomainID: 24, Name: "example.net",
			Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, ""), workerSubMsg(3, "blog")},
		})
		if err != nil {
			t.Fatalf("ProcessDomain: %v", err)
		}
		if res.Cutoff {
			t.Error("cutoff без www в задаче")
		}
		if got := log.countHost("blog.example.net"); got != 2 {
			t.Errorf("загрузок blog: %d, ожидалось 2", got)
		}
	})
}

// TestWorkerDeadHostCutoffDisabled — the flag off: the former behavior,
// dead apex+www do not hinder the other subdomains' check.
func TestWorkerDeadHostCutoffDisabled(t *testing.T) {
	fetch, log := workerFakeFetch(workerDeadDialScript(nil, "blog.example.net"))
	w := workerWorker(t, fetch, nil) // DeadHostCutoff not set — off
	res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 4, DomainID: 25, Name: "example.net",
		Subdomains: []queue.HTTPSubdomain{
			workerSubMsg(0, ""), workerSubMsg(1, "www"), workerSubMsg(3, "blog"),
		},
	})
	if err != nil {
		t.Fatalf("ProcessDomain: %v", err)
	}
	if res.Cutoff {
		t.Error("cutoff при выключенном флаге")
	}
	if got := log.countHost("blog.example.net"); got != 2 {
		t.Errorf("загрузок blog: %d, ожидалось 2 (проверка как раньше)", got)
	}
	blog := &res.Subdomains[2]
	if !blog.HTTPS.Available || blog.HTTPS.ErrClass != ErrNone || len(blog.Records) != 1 {
		t.Errorf("blog: available=%v класс=%v записей=%d — живой субдомен обязан проверяться",
			blog.HTTPS.Available, blog.HTTPS.ErrClass, len(blog.Records))
	}
}

// TestWorkerDeadHostCutoffEmptyTail — a task of apex+www only: nothing
// to check, the cutoff does not fire (no saving — the sign is honestly not set).
func TestWorkerDeadHostCutoffEmptyTail(t *testing.T) {
	fetch, log := workerFakeFetch(workerDeadDialScript(nil, ""))
	w := workerWorker(t, fetch, func(o *Options) { o.DeadHostCutoff = true })
	res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 4, DomainID: 26, Name: "example.net",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, ""), workerSubMsg(1, "www")},
	})
	if err != nil {
		t.Fatalf("ProcessDomain: %v", err)
	}
	if res.Cutoff {
		t.Error("cutoff без хвоста — экономить нечего")
	}
	if got := log.countHost("example.net") + log.countHost("www.example.net"); got != 4 {
		t.Errorf("загрузок головы: %d, ожидалось 4", got)
	}
}

// TestWorkerSPADetect — the SPA flag over the final page's body (T-057):
// a shell with a root container and a JS requirement gets SPA=true.
func TestWorkerSPADetect(t *testing.T) {
	spa := `<!DOCTYPE html><html><head><title>x</title></head>` +
		`<body><noscript>Для работы сайта включите JavaScript</noscript><div id="root"></div></body></html>`
	plain := "<html><body>" + strings.Repeat("Обычный сайт с большим количеством видимого текста. ", 20) + "</body></html>"
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"spa.test":              {status: 200, body: spa},
		"http://spa.test/":      {status: 200, body: spa},
		"flat.spa.test":         {status: 200, body: plain},
		"http://flat.spa.test/": {status: 200, body: plain},
	})
	w := workerWorker(t, fetch, nil)
	res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 4, DomainID: 12, Name: "spa.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, ""), workerSubMsg(6, "flat")},
	})
	if err != nil {
		t.Fatalf("ProcessDomain: %v", err)
	}
	// Both apex protocols — one body at one host+path → 1 record.
	if len(res.Subdomains[0].Records) != 1 {
		t.Fatalf("записей spa: %d", len(res.Subdomains[0].Records))
	}
	rec := &res.Subdomains[0].Records[0]
	if rec.SPA == nil || !rec.SPA.SPA {
		t.Fatalf("SPA-флаг оболочки не поднят: %+v", rec.SPA)
	}
	found := false
	for _, r := range rec.SPA.Reasons {
		if r == httpfetch.SPAReasonRootContainer {
			found = true
		}
	}
	if !found {
		t.Errorf("признак root_container не зафиксирован: %v", rec.SPA.Reasons)
	}
	flat := &res.Subdomains[1].Records[0]
	if flat.SPA == nil || flat.SPA.SPA {
		t.Errorf("обычная страница помечена SPA: %+v", flat.SPA)
	}
}

// TestWorkerCookies — the final answer's cookies are parsed into the result
// (spec 5.5 item 7 "cookies"), the final page's headers are carried by the record.
func TestWorkerCookies(t *testing.T) {
	page := "<html>магазин с сессией и достаточным текстом страницы</html>"
	h := http.Header{}
	h.Add("Content-Type", "text/html; charset=utf-8")
	h.Add("Set-Cookie", "XSRF-TOKEN=abc; Path=/")
	h.Add("Set-Cookie", "laravel_session=eyJpdiI6; Path=/")
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"shop.test":         {status: 200, body: page, header: h},
		"http://shop.test/": {status: 200, body: page, header: h},
	})
	w := workerWorker(t, fetch, nil)
	res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 7, DomainID: 13, Name: "shop.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})
	if err != nil {
		t.Fatalf("ProcessDomain: %v", err)
	}
	rec := &res.Subdomains[0].Records[0]
	if len(rec.Cookies) != 2 ||
		rec.Cookies[0].Name != "XSRF-TOKEN" || rec.Cookies[1].Name != "laravel_session" {
		t.Errorf("cookies записи: %+v", rec.Cookies)
	}
	if got := rec.Header.Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Errorf("Content-Type записи: %q", got)
	}
	// A scenario without a TLS connection — the record's TLSInfo is nil (not an error, §8).
	if rec.TLS != nil {
		t.Errorf("TLS без TLS-соединения: %+v", rec.TLS)
	}
	if rec.Charset != "utf-8" {
		t.Errorf("кодировка: %q", rec.Charset)
	}
}

// TestWorkerContextCancel — a ctx cancellation in flight: ProcessDomain returns
// an error (the episode is not finished), no result is issued — no false "no HTTP"
// will happen; the delivery will return with a repeated delivery (spec 3.4).
func TestWorkerContextCancel(t *testing.T) {
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"slow.test": {blockCtx: true},
	})
	w := workerWorker(t, fetch, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	res, err := w.ProcessDomain(ctx, queue.HTTPTask{
		ZoneID: 8, DomainID: 14, Name: "slow.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})
	if err == nil {
		t.Fatal("ожидалась ошибка отменённого эпизода")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка не про отмену: %v", err)
	}
	if res != nil {
		t.Error("отменённый эпизод вернул результат")
	}
}

// TestWorkerTaskValidation — unfit tasks are rejected before the network.
func TestWorkerTaskValidation(t *testing.T) {
	fetch, _ := workerFakeFetch(nil)
	w := workerWorker(t, fetch, nil)
	cases := []struct {
		name string
		task queue.HTTPTask
	}{
		{"без имени", queue.HTTPTask{Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")}}},
		{"без субдоменов", queue.HTTPTask{Name: "a.test"}},
		{"дубликат id", queue.HTTPTask{Name: "a.test", Subdomains: []queue.HTTPSubdomain{
			workerSubMsg(1, "www"), workerSubMsg(1, "www"),
		}}},
		{"id вне диапазона", queue.HTTPTask{Name: "a.test", Subdomains: []queue.HTTPSubdomain{
			{SubdomainID: 63, Label: "x", Addrs: []string{"192.0.2.1"}},
		}}},
		{"метка с точкой", queue.HTTPTask{Name: "a.test", Subdomains: []queue.HTTPSubdomain{
			{SubdomainID: 2, Label: "a.b", Addrs: []string{"192.0.2.1"}},
		}}},
		{"кривой адрес", queue.HTTPTask{Name: "a.test", Subdomains: []queue.HTTPSubdomain{
			{SubdomainID: 2, Label: "x", Addrs: []string{"не-адрес"}},
		}}},
		{"без адресов", queue.HTTPTask{Name: "a.test", Subdomains: []queue.HTTPSubdomain{
			{SubdomainID: 2, Label: "x"},
		}}},
	}
	for _, c := range cases {
		if _, err := w.ProcessDomain(context.Background(), c.task); err == nil {
			t.Errorf("%s: задача принята", c.name)
		}
	}
	if _, err := New(nil, Options{}); err == nil {
		t.Error("New без шва загрузки прошёл")
	}
	if _, err := New(workerFakeFetchNoop(), Options{}); err == nil {
		t.Error("New без хранилища контента прошёл (ранний Put T-147 обязателен)")
	}
}

// workerFakeFetchNoop — a minimal load seam for the constructor's checks.
func workerFakeFetchNoop() FetchFunc {
	return func(context.Context, httpfetch.FetchRequest, httpfetch.ChainOptions) (*httpfetch.ChainResult, error) {
		return nil, errors.New("нет сценария")
	}
}

// --- the Run loop's smoke -----------------------------------------------------------

// workerFakeQueue — a queue.Queue fake: issues the deliveries once, then
// blocks until the ctx cancellation; the Ack and the read count are recorded.
type workerFakeQueue struct {
	mu         sync.Mutex
	deliveries []queue.Delivery
	acked      []string
	consumes   int
}

func (q *workerFakeQueue) Publish(context.Context, string, queue.Message) (string, error) {
	return "", nil
}

func (q *workerFakeQueue) Consume(ctx context.Context, _, _, _ string, n int) ([]queue.Delivery, error) {
	q.mu.Lock()
	q.consumes++
	if len(q.deliveries) == 0 {
		q.mu.Unlock()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	take := q.deliveries
	if n < len(take) {
		take = take[:n]
		q.deliveries = q.deliveries[n:]
	} else {
		q.deliveries = nil
	}
	q.mu.Unlock()
	return take, nil
}

func (q *workerFakeQueue) Ack(_ context.Context, _, _ string, ids ...string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.acked = append(q.acked, ids...)
	return nil
}

func (q *workerFakeQueue) ackedCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.acked)
}

// ackedSnapshot — a copy of the acknowledged IDs (the order preserved).
func (q *workerFakeQueue) ackedSnapshot() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]string(nil), q.acked...)
}

func (q *workerFakeQueue) consumeCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.consumes
}

// workerDelivery — a delivery with a domain task.
func workerDelivery(t *testing.T, task queue.HTTPTask) queue.Delivery {
	t.Helper()
	payload, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	return queue.Delivery{Stream: queue.StreamHTTP, ID: "e1", Payload: payload}
}

// TestWorkerRunSmoke — the loop in full: a q:http delivery → the processing →
// Options.OnResult (the T-058 write's connection point, before the XACK) → Ack;
// a ctx cancellation — a clean nil return.
func TestWorkerRunSmoke(t *testing.T) {
	body := "<html>" + strings.Repeat("страница для дымового теста цикла воркера http ", 30) + "</html>"
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"smoke.test":         {status: 200, body: body},
		"http://smoke.test/": {status: 200, body: body},
	})
	w := workerWorker(t, fetch, nil)
	q := &workerFakeQueue{deliveries: []queue.Delivery{workerDelivery(t, queue.HTTPTask{
		TaskID: "task-1", ZoneID: 5, DomainID: 42, Name: "smoke.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})}}

	var (
		mu      sync.Mutex
		gotZone int16
		gotName string
		records int
	)
	opts := Options{OnResult: func(_ context.Context, res *DomainResult) error {
		mu.Lock()
		defer mu.Unlock()
		gotZone, gotName, records = res.ZoneID, res.Name, res.RecordCount()
		return nil
	}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, q, w, opts) }()

	deadline := time.After(5 * time.Second)
	for q.ackedCount() == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("доставка не подтверждена за отведённое время")
		case <-time.After(2 * time.Millisecond):
		}
	}
	mu.Lock()
	if gotZone != 5 || gotName != "smoke.test" {
		t.Errorf("OnResult: zone=%d name=%q", gotZone, gotName)
	}
	if records != 1 {
		t.Errorf("OnResult: записей %d, ожидалась 1 (одно тело — один сайт)", records)
	}
	mu.Unlock()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run после отмены: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}
	if q.ackedCount() != 1 {
		t.Errorf("подтверждений %d, ожидалось 1", q.ackedCount())
	}
}

// TestWorkerRunOnResultError — the result handler's error (the future
// T-058 write) — a delivery without an XACK: it will return with a repeated delivery
// (at-least-once, spec 3.4).
func TestWorkerRunOnResultError(t *testing.T) {
	body := "<html>" + strings.Repeat("страница для проверки отказа обработки результата ", 30) + "</html>"
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"err.test":         {status: 200, body: body},
		"http://err.test/": {status: 200, body: body},
	})
	w := workerWorker(t, fetch, nil)
	q := &workerFakeQueue{deliveries: []queue.Delivery{workerDelivery(t, queue.HTTPTask{
		ZoneID: 6, DomainID: 43, Name: "err.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})}}
	opts := Options{OnResult: func(context.Context, *DomainResult) error {
		return errors.New("БД недоступна")
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, q, w, opts) }()

	// The window with an erroneous delivery is over when the loop reads the stream again.
	deadline := time.After(5 * time.Second)
	for q.consumeCount() < 2 {
		select {
		case <-deadline:
			t.Fatal("цикл не дошёл до повторного чтения")
		case <-time.After(2 * time.Millisecond):
		}
	}
	if q.ackedCount() != 0 {
		t.Error("доставка подтверждена при ошибке обработчика результата")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run после отмены: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}
}

// workerPoisonQueue — an endless source of poison deliveries: every
// read immediately returns one invalid record (Decode in
// processDelivery fails, the delivery stays without an XACK). The read rate
// is bounded only by the loop's fuse — a perfect meter of its
// work without processing timings.
type workerPoisonQueue struct {
	mu       sync.Mutex
	consumes int
	acked    []string
}

func (q *workerPoisonQueue) Publish(context.Context, string, queue.Message) (string, error) {
	return "", nil
}

func (q *workerPoisonQueue) Consume(_ context.Context, _, _, _ string, n int) ([]queue.Delivery, error) {
	if n < 1 {
		n = 1
	}
	out := make([]queue.Delivery, n)
	for i := range out {
		out[i] = queue.Delivery{Stream: queue.StreamHTTP, ID: fmt.Sprintf("poison-%d", i), Payload: []byte("{")}
	}
	q.mu.Lock()
	q.consumes++
	q.mu.Unlock()
	return out, nil
}

func (q *workerPoisonQueue) Ack(_ context.Context, _, _ string, ids ...string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.acked = append(q.acked, ids...)
	return nil
}

func (q *workerPoisonQueue) consumeCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.consumes
}

func (q *workerPoisonQueue) ackedCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.acked)
}

// TestWorkerRunBackoffOnFailures — the sliding loop's fuse
// (T-139): at a systematic processing failure (every delivery falls —
// here poison records) the q:http reading is suspended with a pause doubling,
// otherwise the loop would burn the delivery attempts (XAUTOCLAIM → q:dead,
// T-025) at full speed; the reads continue at that — the counter does not
// block the loop forever. The pause is shrunk to 100 ms: seconds are not
// waited in a unit test, the boundaries are taken with a multiple headroom against the CI jitter.
func TestWorkerRunBackoffOnFailures(t *testing.T) {
	old := workerFailWaitBase
	workerFailWaitBase = 100 * time.Millisecond
	t.Cleanup(func() { workerFailWaitBase = old })

	// The pool and the read chunk are minimal: the fuse threshold — Consume
	// failures in a row (1), the read pipeline is no deeper than a couple of deliveries. The
	// load seam is not called (the decode falls earlier), but New requires it.
	dummyFetch, _ := workerFakeFetch(nil)
	w := workerWorker(t, dummyFetch, nil)
	q := &workerPoisonQueue{}
	opts := Options{Concurrency: 1, Consume: 1}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, q, w, opts) }()

	// The first reads go before the counter fires (the failure
	// registration is asynchronous) — a 450 ms window covers them with headroom; then the
	// 100/200/400 ms pauses throttle the read rate to units.
	deadline := time.After(5 * time.Second)
	waitCount := func(n int) int {
		for {
			got := q.consumeCount()
			if got >= n {
				return got
			}
			select {
			case <-deadline:
				t.Fatalf("чтений %d, ждалось ≥%d — предохранитель заблокировал цикл", got, n)
			case <-time.After(2 * time.Millisecond):
			}
		}
	}
	waitCount(1)
	time.Sleep(450 * time.Millisecond)
	first := q.consumeCount()
	if first > 10 {
		t.Errorf("чтений %d за 450 мс — предохранитель не душит темп (ждалось ≤10)", first)
	}

	// The pauses double: the next interval adds a few reads,
	// not tens. The growth between the two measurements — no more than a couple of reads.
	time.Sleep(450 * time.Millisecond)
	if grew := q.consumeCount() - first; grew > 2 {
		t.Errorf("за следующие 450 мс ещё %d чтений — пауза не растёт с серией неудач", grew)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run после отмены: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}
	if q.ackedCount() != 0 {
		t.Errorf("подтверждений %d при ядовитых доставках", q.ackedCount())
	}
}

// workerInfraQueue — an endless source of valid q:http tasks (T-152):
// every read returns one delivery, the processing reaches
// Options.OnResult (in this test an InfraError falls — the model of the
// result publication's OOM). The read rate is bounded only by
// the loop's infrastructural pause — the meter of its work.
type workerInfraQueue struct {
	mu       sync.Mutex
	consumes int
	acked    int
}

func (q *workerInfraQueue) Publish(context.Context, string, queue.Message) (string, error) {
	return "", nil
}

func (q *workerInfraQueue) Consume(_ context.Context, _, _, _ string, n int) ([]queue.Delivery, error) {
	if n < 1 {
		n = 1
	}
	payload, err := json.Marshal(queue.HTTPTask{
		TaskID: "t-infra", ZoneID: 30, DomainID: 1, Name: "infra.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})
	if err != nil {
		panic(err)
	}
	out := make([]queue.Delivery, n)
	for i := range out {
		out[i] = queue.Delivery{Stream: queue.StreamHTTP, ID: fmt.Sprintf("infra-%d", i), Payload: payload}
	}
	q.mu.Lock()
	q.consumes++
	q.mu.Unlock()
	return out, nil
}

func (q *workerInfraQueue) Ack(_ context.Context, _, _ string, _ ...string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.acked++
	return nil
}

func (q *workerInfraQueue) consumeCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.consumes
}

func (q *workerInfraQueue) ackedCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.acked
}

// workerProbeRedis — a SET/XAUTOCLAIM/XPENDING client for the Reliab.Writable
// write probe (T-152): a switchable SET error; the claim is always empty
// (the gate must not let it further anyway).
type workerProbeRedis struct {
	mu     sync.Mutex
	setErr error
}

func (f *workerProbeRedis) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	f.mu.Lock()
	err := f.setErr
	f.mu.Unlock()
	return redis.NewStatusResult("OK", err)
}

func (f *workerProbeRedis) setProbeErr(err error) {
	f.mu.Lock()
	f.setErr = err
	f.mu.Unlock()
}

func (f *workerProbeRedis) XAutoClaim(ctx context.Context, a *redis.XAutoClaimArgs) *redis.XAutoClaimCmd {
	cmd := redis.NewXAutoClaimCmd(ctx, "xautoclaim")
	cmd.SetVal(nil, "0-0")
	return cmd
}

func (f *workerProbeRedis) XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	return redis.NewXPendingExtCmd(ctx, "xpending")
}

// TestWorkerRunInfraPause — the consumption pause at a sustained InfraError
// of publishing (T-152): a result publication error (the Redis OOM) is not
// a processing error, drawing new deliveries is pointless: the q:http reading
// stops until the write recovers (Reliab.Writable), after
// the recovery — it resumes and brings the deliveries to the XACK.
func TestWorkerRunInfraPause(t *testing.T) {
	old := workerFailWaitBase
	workerFailWaitBase = 5 * time.Millisecond
	t.Cleanup(func() { workerFailWaitBase = old })

	infraErr := &queue.InfraError{
		Op:  "publish в q:analyze",
		Err: errors.New("OOM command not allowed when used memory > 'maxmemory'"),
	}
	var failMu sync.Mutex
	failPublish := true
	onResult := func(context.Context, *DomainResult) error {
		failMu.Lock()
		defer failMu.Unlock()
		if failPublish {
			return infraErr
		}
		return nil
	}
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"infra.test": {status: 200, body: "<html>страница инфраструктурной паузы</html>"},
	})
	// OnResult is set in the Run options (not New): the loop passes its opts
	// into processDelivery — as the cmd/http-worker build does.
	w := workerWorker(t, fetch, nil)

	q := &workerInfraQueue{}
	probe := &workerProbeRedis{}
	probe.setProbeErr(errors.New("OOM command not allowed when used memory > 'maxmemory'"))
	reliab := queue.NewReliab(probe, q, queue.ReliabOptions{MinIdle: time.Hour, MaxAttempts: 3})
	opts := Options{Concurrency: 1, Consume: 1, Recover: reliab, OnResult: onResult}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, q, w, opts) }()

	// The OOM window: the first chunk is read, its publication fell — the reading
	// is stopped by the probe (without the pause there would be hundreds of reads per window).
	deadline := time.After(5 * time.Second)
	for q.consumeCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("первое чтение q:http не состоялось")
		case <-time.After(2 * time.Millisecond):
		}
	}
	time.Sleep(300 * time.Millisecond)
	if n := q.consumeCount(); n > 6 {
		t.Errorf("чтений %d за 300 мс инфраструктурной паузы — чтение не остановлено (ждалось ≤6)", n)
	}
	if q.ackedCount() != 0 {
		t.Errorf("подтверждений %d при падающей публикации", q.ackedCount())
	}

	// The write recovered (the memory drainage in the incident): the reading
	// resumes, the deliveries are brought to the XACK.
	probe.setProbeErr(nil)
	failMu.Lock()
	failPublish = false
	failMu.Unlock()
	for q.ackedCount() < 2 {
		select {
		case <-deadline:
			cancel()
			t.Fatalf("после восстановления записи подтверждено %d, хочу ≥2", q.ackedCount())
		case <-time.After(2 * time.Millisecond):
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run после отмены: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}
}

// TestWorkerRunPanicRecovered — the delivery panics' recover parity
// (T-262, TZ-fsm spec §3 FR-8; the model — TestRenderWorker_RunPanicRecovered):
// one delivery's processing panic (Options.OnResult — the result write
// point, the delivery goroutine's synchronous stack) does not drop the process —
// the panicking write is not acknowledged, the window's neighboring delivery
// finishes and is acknowledged, the loop lives until the ctx cancellation. Before T-262
// a panic in the delivery goroutine killed the http-worker unit entirely.
// (A panic in the load seam lives in the PerDomain goroutines — this
// recover never covered those; not modeled here.)
func TestWorkerRunPanicRecovered(t *testing.T) {
	panicBody := "<html>" + strings.Repeat("страница паникующей доставки ", 30) + "</html>"
	okBody := "<html>" + strings.Repeat("страница здоровой доставки цикла ", 30) + "</html>"
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"panic.test": {status: 200, body: panicBody},
		"ok.test":    {status: 200, body: okBody},
	})
	w := workerWorker(t, fetch, nil)

	task1 := queue.HTTPTask{TaskID: "task-1", ZoneID: 5, DomainID: 42, Name: "panic.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")}}
	task2 := queue.HTTPTask{TaskID: "task-2", ZoneID: 5, DomainID: 43, Name: "ok.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")}}
	payload2, err := json.Marshal(task2)
	if err != nil {
		t.Fatal(err)
	}
	q := &workerFakeQueue{deliveries: []queue.Delivery{
		workerDelivery(t, task1),
		{Stream: queue.StreamHTTP, ID: "e2", Payload: payload2},
	}}

	// The panicking delivery must reach the result processing BEFORE
	// the cancellation (on the model of the T-142 P2-4 review): a cancellation right after the
	// neighbor's XACK left the recover branch non-deterministic.
	panicHit := make(chan struct{}, 1)
	opts := Options{Concurrency: 2, Consume: 2, OnResult: func(_ context.Context, res *DomainResult) error {
		if res.Name == "panic.test" {
			select {
			case panicHit <- struct{}{}:
			default:
			}
			panic("обработка результата упала: запись в БД")
		}
		return nil
	}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, q, w, opts) }()

	deadline := time.After(5 * time.Second)
	select {
	case <-panicHit:
	case <-deadline:
		cancel()
		t.Fatal("паникующая доставка не дошла до обработки результата")
	}

	// The neighboring delivery must finish and be acknowledged despite
	// the first one's panic.
	for q.ackedCount() == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("XACK здоровой доставки не наступил (паника убила цикл?)")
		case <-time.After(2 * time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run после отмены: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}
	if acked := q.ackedSnapshot(); len(acked) != 1 || acked[0] != "e2" {
		t.Errorf("XACK = %v, нужен только e2 (паникующая e1 — без XACK)", acked)
	}
}

// workerPanicQueue — an endless source of valid q:http tasks
// of a panicking domain (T-262): Decode passes, the load panics
// (a recover in the delivery goroutine, the parity with renderworker) — the deliveries
// without an XACK; the read rate is throttled by the fuse, the panics are accounted
// by the failed counter as failures.
type workerPanicQueue struct {
	payload []byte // the panicking domain's task

	mu       sync.Mutex
	consumes int
}

func (q *workerPanicQueue) Publish(context.Context, string, queue.Message) (string, error) {
	return "", nil // a panicking delivery does not reach the publication
}

func (q *workerPanicQueue) Consume(_ context.Context, _, _, _ string, n int) ([]queue.Delivery, error) {
	if n < 1 {
		n = 1
	}
	out := make([]queue.Delivery, n)
	for i := range out {
		out[i] = queue.Delivery{Stream: queue.StreamHTTP, ID: fmt.Sprintf("panic-%d", i), Payload: q.payload}
	}
	q.mu.Lock()
	q.consumes++
	q.mu.Unlock()
	return out, nil
}

func (q *workerPanicQueue) Ack(context.Context, string, string, ...string) error {
	panic("workerPanicQueue: Ack недостижим — паникующие доставки не подтверждаются")
}

func (q *workerPanicQueue) consumeCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.consumes
}

// TestWorkerRunPanicBackoff — the panics in the fuse counter
// (T-262, the parity with TestRenderWorker_RunPanicBackoff): a series of panicking
// deliveries without a single acknowledgment — the same failure series as
// the processing errors (the recover accounts the panic in failed): the q:http reading
// is throttled by a pause with a doubling, the process does not fall; without accounting the panics the loop
// would burn the delivery attempts (XAUTOCLAIM → q:dead, T-025) at full
// speed.
func TestWorkerRunPanicBackoff(t *testing.T) {
	old := workerFailWaitBase
	workerFailWaitBase = 100 * time.Millisecond
	t.Cleanup(func() { workerFailWaitBase = old })

	panicBody := "<html>" + strings.Repeat("страница серии паник цикла ", 30) + "</html>"
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"panic.test": {status: 200, body: panicBody},
	})
	w := workerWorker(t, fetch, nil)

	payload, err := json.Marshal(queue.HTTPTask{
		TaskID: "t-panic", ZoneID: 5, DomainID: 42, Name: "panic.test",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")},
	})
	if err != nil {
		t.Fatal(err)
	}
	q := &workerPanicQueue{payload: payload}

	// The panicking result processing (the delivery goroutine's
	// synchronous stack) — the signal that the deliveries reach the failure.
	panicHit := make(chan struct{}, 1)
	opts := Options{Concurrency: 1, Consume: 1, OnResult: func(_ context.Context, res *DomainResult) error {
		select {
		case panicHit <- struct{}{}:
		default:
		}
		panic("обработка результата упала: запись в БД")
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, q, w, opts) }()

	// The panic happened (the delivery reached the processing), the reads are throttled —
	// the same boundaries as at TestWorkerRunBackoffOnFailures.
	select {
	case <-panicHit:
	case <-time.After(5 * time.Second):
		t.Fatal("паникующая доставка не дошла до обработки результата — тест ничего не проверил")
	}
	deadline := time.After(5 * time.Second)
	waitCount := func(n int) int {
		for {
			got := q.consumeCount()
			if got >= n {
				return got
			}
			select {
			case <-deadline:
				t.Fatalf("чтений %d, ждалось ≥%d — предохранитель заблокировал цикл", got, n)
			case <-time.After(2 * time.Millisecond):
			}
		}
	}
	waitCount(1)
	time.Sleep(450 * time.Millisecond)
	first := q.consumeCount()
	if first > 10 {
		t.Errorf("чтений %d за 450 мс — паники не душат темп (ждалось ≤10)", first)
	}
	time.Sleep(450 * time.Millisecond)
	if grew := q.consumeCount() - first; grew > 2 {
		t.Errorf("за следующие 450 мс ещё %d чтений — пауза не растёт с серией паник", grew)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run после отмены: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}
}

// TestWorkerRunSlidingConsumption — the essence of T-139: the q:http reading does not wait
// for the whole chunk's completion. The window's first delivery blocks in the network until
// the ctx cancellation (the durations' tail — a tarpit up to total_timeout); at
// a window barrier (before T-139) the wg.Wait would hold the loop on it — the chunk's
// tail deliveries and the next read would not start. The sliding
// consumption must acknowledge all the fast deliveries while
// the blocking one is in flight: the slots are freed by the finished tasks,
// the reader moves on.
func TestWorkerRunSlidingConsumption(t *testing.T) {
	fetch, log := workerFakeFetch(map[string]*workerPageScript{
		"block.test": {blockCtx: true},
		"f1.test":    {status: 200, body: "<html>страница 1 скользящего потребления</html>"},
		"f2.test":    {status: 200, body: "<html>страница 2 скользящего потребления</html>"},
		"f3.test":    {status: 200, body: "<html>страница 3 скользящего потребления</html>"},
		"f4.test":    {status: 200, body: "<html>страница 4 скользящего потребления</html>"},
	})
	w := workerWorker(t, fetch, nil)
	task := func(id int64, name string) queue.Delivery {
		return workerDelivery(t, queue.HTTPTask{ZoneID: 20, DomainID: id, Name: name,
			Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")}})
	}
	q := &workerFakeQueue{deliveries: []queue.Delivery{
		task(71, "block.test"), task(72, "f1.test"), task(73, "f2.test"),
		task(74, "f3.test"), task(75, "f4.test"),
	}}
	opts := Options{Concurrency: 2, Consume: 4}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, q, w, opts) }()

	// The blocking delivery went to the network and holds a slot — the fast
	// ones finish and are acknowledged past it.
	deadline := time.After(5 * time.Second)
	for log.countHost("block.test") == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("блокирующая доставка не дошла до сети")
		case <-time.After(2 * time.Millisecond):
		}
	}
	for q.ackedCount() < 4 {
		select {
		case <-deadline:
			cancel()
			t.Fatalf("скользящее потребление не подхватило хвост пачки: подтверждено %d из 4 быстрых", q.ackedCount())
		case <-time.After(2 * time.Millisecond):
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run после отмены: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}
	// The blocking delivery is not acknowledged — it will return with a repeated
	// delivery (at-least-once, spec 3.4), the fast ones are acknowledged exactly
	// once each.
	if got := q.ackedCount(); got != 4 {
		t.Errorf("подтверждений %d, ожидалось 4 (блокирующая — без XACK)", got)
	}
}

// TestWorkerRunCancelKeepsBufferUnacked — the sliding buffer's guarantee
// (T-139, the P3-3 review): the read ahead but not started deliveries (the slots
// taken by blocking tasks) at the ctx cancellation are NOT launched and NOT
// acknowledged — together with the reader's current chunks they stay in
// the stream's pending and return by an XAUTOCLAIM claim (at-least-once,
// spec 3.4). At the same time — the slots' cap: the third and fourth tasks do
// not start while both blocking ones hold Concurrency=2.
func TestWorkerRunCancelKeepsBufferUnacked(t *testing.T) {
	fetch, log := workerFakeFetch(map[string]*workerPageScript{
		"block1.test": {blockCtx: true},
		"block2.test": {blockCtx: true},
		"buf1.test":   {status: 200, body: "<html>буферная доставка 1</html>"},
		"buf2.test":   {status: 200, body: "<html>буферная доставка 2</html>"},
	})
	w := workerWorker(t, fetch, nil)
	task := func(id int64, name string) queue.Delivery {
		return workerDelivery(t, queue.HTTPTask{ZoneID: 21, DomainID: id, Name: name,
			Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")}})
	}
	// The chunk is larger than the parallelism: buf1/buf2 are read by the reader ahead
	// (the buffer + the chunk in hand), but there are no slots for them while the
	// blocking tasks hold both.
	q := &workerFakeQueue{deliveries: []queue.Delivery{
		task(81, "block1.test"), task(82, "block2.test"),
		task(83, "buf1.test"), task(84, "buf2.test"),
	}}
	opts := Options{Concurrency: 2, Consume: 4}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, q, w, opts) }()

	// Both blocking tasks are in the network (they hold the slots), the buffer ones must
	// stay unstarted — without touching the network, no matter how long the
	// loop runs.
	deadline := time.After(5 * time.Second)
	for log.countHost("block1.test") == 0 || log.countHost("block2.test") == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("блокирующиеся доставки не дошли до сети")
		case <-time.After(2 * time.Millisecond):
		}
	}
	time.Sleep(50 * time.Millisecond) // the loop has time to read the chunk and hit the slots

	if got := log.countHost("buf1.test") + log.countHost("buf2.test"); got != 0 {
		t.Errorf("буферные доставки запущены при занятых слотах: обращений %d", got)
	}
	if q.ackedCount() != 0 {
		t.Errorf("подтверждений %d до отмены — не должно быть", q.ackedCount())
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run после отмены: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}
	if got := log.countHost("buf1.test") + log.countHost("buf2.test"); got != 0 {
		t.Errorf("буферные доставки запущены после отмены: обращений %d", got)
	}
	if q.ackedCount() != 0 {
		t.Errorf("подтверждений %d — не начатые доставки обязаны вернуться очереди", q.ackedCount())
	}
}

// TestWorkerOptionsDefaults — the worker's task parallelism defaults (T-139)
// and the configuration match: the production build (cmd/http-worker)
// passes http.task_* into Options, the isolated builds take
// withDefaults — a divergence would silently change the production
// instance's behavior against the documentation.
func TestWorkerOptionsDefaults(t *testing.T) {
	o := Options{}.withDefaults()
	if o.Concurrency != DefaultConcurrency || o.Consume != DefaultConsume || o.PerDomain != DefaultPerDomain {
		t.Errorf("withDefaults: concurrency=%d consume=%d per_domain=%d, дефолты %d/%d/%d",
			o.Concurrency, o.Consume, o.PerDomain, DefaultConcurrency, DefaultConsume, DefaultPerDomain)
	}
	cfg := config.Default()
	if cfg.HTTP.TaskConcurrency != DefaultConcurrency {
		t.Errorf("http.task_concurrency=%d расходится с DefaultConcurrency=%d", cfg.HTTP.TaskConcurrency, DefaultConcurrency)
	}
	if cfg.HTTP.TaskConsume != DefaultConsume {
		t.Errorf("http.task_consume=%d расходится с DefaultConsume=%d", cfg.HTTP.TaskConsume, DefaultConsume)
	}
	if cfg.HTTP.TaskPerDomain != DefaultPerDomain {
		t.Errorf("http.task_per_domain=%d расходится с DefaultPerDomain=%d", cfg.HTTP.TaskPerDomain, DefaultPerDomain)
	}
	// Explicit values are not overwritten by the defaults.
	o = Options{Concurrency: 3, Consume: 2, PerDomain: 1}.withDefaults()
	if o.Concurrency != 3 || o.Consume != 2 || o.PerDomain != 1 {
		t.Errorf("withDefaults перетёр явные значения: %+v", o)
	}
}

// TestWorkerRunCancelWhileWaitingSlot — a ctx cancellation during the semaphore
// slot wait does not launch a delivery with an already cancelled ctx: a window of two tasks
// at Concurrency=1, the first blocks in the network until the cancellation — the second must
// stay unstarted (its host is not read from the network), the window returns
// to the queue by a repeated delivery.
func TestWorkerRunCancelWhileWaitingSlot(t *testing.T) {
	fetch, log := workerFakeFetch(map[string]*workerPageScript{
		"block.test": {blockCtx: true},
		"next.test":  {status: 200, body: "<html>страница второй доставки окна</html>"},
	})
	w := workerWorker(t, fetch, nil)
	q := &workerFakeQueue{deliveries: []queue.Delivery{
		workerDelivery(t, queue.HTTPTask{ZoneID: 11, DomainID: 61, Name: "block.test",
			Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")}}),
		workerDelivery(t, queue.HTTPTask{ZoneID: 11, DomainID: 62, Name: "next.test",
			Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, "")}}),
	}}
	// Concurrency — the Run loop's parameter (the window's semaphore), not the worker's.
	opts := Options{Concurrency: 1}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, q, w, opts) }()

	// The first delivery went to the network (holds the only slot) — we cancel:
	// the second is waiting for the semaphore at that moment.
	deadline := time.After(5 * time.Second)
	for log.countHost("block.test") == 0 {
		select {
		case <-deadline:
			t.Fatal("первая доставка не дошла до сети")
		case <-time.After(2 * time.Millisecond):
		}
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run после отмены: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены ctx")
	}
	if got := log.countHost("next.test"); got != 0 {
		t.Errorf("доставка запущена после отмены ctx: обращений к next.test %d", got)
	}
	if q.ackedCount() != 0 {
		t.Errorf("подтверждений %d — окно обязано вернуться очереди", q.ackedCount())
	}
}

// --- the DNS resolver adapter (spec 5.5 item 2) ---------------------------------------

// workerAddr — a scenario address's parse (a fall — the test's error).
func workerAddr(t *testing.T, s string) netip.Addr {
	t.Helper()
	a, err := netip.ParseAddr(s)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// workerFakeDNS — a scenario dnsclient.Resolver: answers A/AAAA
// with the given addresses; withCNAME mixes a CNAME record into the A answer
// (the recursors serve the alias and the target in one answer); aErr/aaaaErr —
// an exchange failure over only one record type (a transient AAAA refusal).
type workerFakeDNS struct {
	mu        sync.Mutex
	asks      []uint16
	a, aaaa   []netip.Addr
	rcode     dnsclient.RCode
	err       error
	aErr      error
	aaaaErr   error
	withCNAME bool
}

func (d *workerFakeDNS) Exchange(_ context.Context, q dnsclient.Question) (*dnsclient.Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.asks = append(d.asks, q.Type)
	if d.err != nil {
		return nil, d.err
	}
	if q.Type == dns.TypeA && d.aErr != nil {
		return nil, d.aErr
	}
	if q.Type == dns.TypeAAAA && d.aaaaErr != nil {
		return nil, d.aaaaErr
	}
	res := &dnsclient.Result{RCode: d.rcode}
	if d.withCNAME && q.Type == dns.TypeA {
		res.Answer = append(res.Answer, &dns.CNAME{
			Hdr:    dns.RR_Header{Name: q.Name + ".", Rrtype: dns.TypeCNAME, Class: dns.ClassINET, Ttl: 60},
			Target: "cdn.example.",
		})
	}
	addrs := d.a
	if q.Type == dns.TypeAAAA {
		addrs = d.aaaa
	}
	for _, a := range addrs {
		if q.Type == dns.TypeA {
			res.Answer = append(res.Answer, &dns.A{
				Hdr: dns.RR_Header{Name: q.Name + ".", Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60},
				A:   a.AsSlice(),
			})
		} else {
			res.Answer = append(res.Answer, &dns.AAAA{
				Hdr:  dns.RR_Header{Name: q.Name + ".", Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: 60},
				AAAA: a.AsSlice(),
			})
		}
	}
	return res, nil
}

func (d *workerFakeDNS) asked() []uint16 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]uint16(nil), d.asks...)
}

// workerSlowDNS — a resolver with a per-type answer delay (T-101): the
// A/AAAA exchange "rides" aDelay/aaaaDelay, each exchange's entry/exit moments
// are recorded — by the intervals' overlap the test proves the polling's
// concurrency without an exact dependence on the overall time. The sleep — outside the mutex:
// the parallel goroutines are not serialized by the lock.
type workerSlowDNS struct {
	mu                sync.Mutex
	entered, exited   map[uint16]time.Time
	aDelay, aaaaDelay time.Duration
	a, aaaa           []netip.Addr
}

func (d *workerSlowDNS) Exchange(ctx context.Context, q dnsclient.Question) (*dnsclient.Result, error) {
	delay := d.aDelay
	addrs := d.a
	if q.Type == dns.TypeAAAA {
		delay = d.aaaaDelay
		addrs = d.aaaa
	}
	enter := time.Now()
	if err := sleepCtx(ctx, delay); err != nil {
		return nil, err
	}
	exit := time.Now()
	d.mu.Lock()
	if d.entered == nil {
		d.entered = make(map[uint16]time.Time)
		d.exited = make(map[uint16]time.Time)
	}
	d.entered[q.Type] = enter
	d.exited[q.Type] = exit
	d.mu.Unlock()
	res := &dnsclient.Result{RCode: dnsclient.RCodeNOERROR}
	for _, a := range addrs {
		if q.Type == dns.TypeA {
			res.Answer = append(res.Answer, &dns.A{
				Hdr: dns.RR_Header{Name: q.Name + ".", Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60},
				A:   a.AsSlice(),
			})
		} else {
			res.Answer = append(res.Answer, &dns.AAAA{
				Hdr:  dns.RR_Header{Name: q.Name + ".", Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: 60},
				AAAA: a.AsSlice(),
			})
		}
	}
	return res, nil
}

// workerBlockDNS — a resolver not answering until the ctx cancellation (the
// T-101 cancellation test): both parallel exchanges must return a context error,
// not wait for the resolver's timeouts.
type workerBlockDNS struct {
	mu   sync.Mutex
	asks int
}

func (d *workerBlockDNS) Exchange(ctx context.Context, q dnsclient.Question) (*dnsclient.Result, error) {
	d.mu.Lock()
	d.asks++
	d.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (d *workerBlockDNS) asked() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.asks
}

// sleepCtx — a sleep with an exit by the ctx cancellation (the scenario resolvers' helper).
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func TestWorkerDNSLookup(t *testing.T) {
	t.Run("A и AAAA при ipv6", func(t *testing.T) {
		d := &workerFakeDNS{
			a:    []netip.Addr{workerAddr(t, "192.0.2.10")},
			aaaa: []netip.Addr{workerAddr(t, "2001:db8::1")},
		}
		got, err := NewDNSLookup(d, true).LookupNetIP(context.Background(), "redirect.example")
		if err != nil {
			t.Fatalf("LookupNetIP: %v", err)
		}
		if len(got) != 2 || got[0].String() != "192.0.2.10" || got[1].String() != "2001:db8::1" {
			t.Errorf("адреса: %v (порядок стабилен: A, затем AAAA — T-101)", got)
		}
		// The polling is concurrent (T-101): the questions' arrival order at
		// the resolver is non-deterministic — both types are checked as
		// a set, not a sequence (two different of the two
		// possible = exactly A and AAAA).
		if asked := d.asked(); len(asked) != 2 || asked[0] == asked[1] {
			t.Errorf("вопросы: %v (ожидались A и AAAA в любом порядке)", asked)
		}
	})
	t.Run("только A без ipv6", func(t *testing.T) {
		d := &workerFakeDNS{a: []netip.Addr{workerAddr(t, "192.0.2.10")}}
		got, err := NewDNSLookup(d, false).LookupNetIP(context.Background(), "redirect.example")
		if err != nil {
			t.Fatalf("LookupNetIP: %v", err)
		}
		if len(got) != 1 || got[0].String() != "192.0.2.10" {
			t.Errorf("адреса: %v", got)
		}
		if asked := d.asked(); len(asked) != 1 || asked[0] != dns.TypeA {
			t.Errorf("вопросы: %v (AAAA отключён конфигом)", asked)
		}
	})
	t.Run("CNAME в ответе отфильтрован", func(t *testing.T) {
		d := &workerFakeDNS{a: []netip.Addr{workerAddr(t, "192.0.2.11")}, withCNAME: true}
		got, err := NewDNSLookup(d, false).LookupNetIP(context.Background(), "alias.example")
		if err != nil {
			t.Fatalf("LookupNetIP: %v", err)
		}
		if len(got) != 1 || got[0].String() != "192.0.2.11" {
			t.Errorf("адреса: %v (CNAME не должен попасть в итог)", got)
		}
	})
	t.Run("IP-литерал без обмена", func(t *testing.T) {
		d := &workerFakeDNS{}
		got, err := NewDNSLookup(d, true).LookupNetIP(context.Background(), "192.0.2.7")
		if err != nil {
			t.Fatalf("LookupNetIP: %v", err)
		}
		if len(got) != 1 || got[0].String() != "192.0.2.7" {
			t.Errorf("адреса: %v", got)
		}
		if len(d.asked()) != 0 {
			t.Errorf("резолвер опрошен для IP-литерала: %v", d.asked())
		}
	})
	t.Run("сбой AAAA не роняет резолв с живым A", func(t *testing.T) {
		d := &workerFakeDNS{
			a:       []netip.Addr{workerAddr(t, "192.0.2.10")},
			aaaaErr: errors.New("servfail"),
		}
		got, err := NewDNSLookup(d, true).LookupNetIP(context.Background(), "mixed.example")
		if err != nil {
			t.Fatalf("LookupNetIP: %v (сбой AAAA при живом A — резолв работает)", err)
		}
		if len(got) != 1 || got[0].String() != "192.0.2.10" {
			t.Errorf("адреса: %v (ожидался только A)", got)
		}
		if asked := d.asked(); len(asked) != 2 {
			t.Errorf("вопросы: %v (оба типа опрошены)", asked)
		}
	})
	t.Run("сбой A при живом AAAA — адреса AAAA", func(t *testing.T) {
		d := &workerFakeDNS{
			aErr: errors.New("servfail"),
			aaaa: []netip.Addr{workerAddr(t, "2001:db8::2")},
		}
		got, err := NewDNSLookup(d, true).LookupNetIP(context.Background(), "v6.example")
		if err != nil {
			t.Fatalf("LookupNetIP: %v", err)
		}
		if len(got) != 1 || got[0].String() != "2001:db8::2" {
			t.Errorf("адреса: %v (ожидался только AAAA)", got)
		}
	})
	t.Run("оба типа упали — ошибка", func(t *testing.T) {
		d := &workerFakeDNS{err: errors.New("сеть")}
		if _, err := NewDNSLookup(d, true).LookupNetIP(context.Background(), "down2.example"); err == nil {
			t.Error("резолв без единого ответа прошёл как успех")
		}
	})
	t.Run("NXDOMAIN — ошибка", func(t *testing.T) {
		d := &workerFakeDNS{rcode: dnsclient.RCodeNXDOMAIN}
		if _, err := NewDNSLookup(d, false).LookupNetIP(context.Background(), "nx.example"); err == nil {
			t.Error("NXDOMAIN принят как успех")
		}
	})
	t.Run("пустой ответ — ошибка", func(t *testing.T) {
		d := &workerFakeDNS{}
		if _, err := NewDNSLookup(d, false).LookupNetIP(context.Background(), "empty.example"); err == nil {
			t.Error("пустой ответ принят как успех")
		}
	})
	t.Run("сбой резолвера — ошибка", func(t *testing.T) {
		d := &workerFakeDNS{err: errors.New("сеть")}
		if _, err := NewDNSLookup(d, false).LookupNetIP(context.Background(), "down.example"); err == nil {
			t.Error("сбой резолвера принят как успех")
		}
	})
	t.Run("отказ обоих — первая по порядку типов ошибка (A)", func(t *testing.T) {
		d := &workerFakeDNS{
			aErr:    errors.New("a-servfail"),
			aaaaErr: errors.New("aaaa-servfail"),
		}
		_, err := NewDNSLookup(d, true).LookupNetIP(context.Background(), "both-down.example")
		if err == nil {
			t.Fatal("резолв без единого ответа прошёл как успех")
		}
		if !strings.Contains(err.Error(), "a-servfail") {
			t.Errorf("ошибка: %v (детерминизм T-101 — ошибка A, ранняя по порядку типов, а не прибывшая первой)", err)
		}
	})
	t.Run("параллельный опрос — латентность max, не сумма", func(t *testing.T) {
		// A slow A + a fast AAAA: at a sequential polling
		// the exchanges' intervals do not overlap and the delays' sum is eaten
		// entirely; the concurrent one — the exchanges overlap, the overall time ≈
		// the slow type's time. The overlap is checked by the entry/exit
		// moments (the sleeps outside the fake's mutex), not by the overall time
		// — the moment is more reliable than a measurement.
		const (
			aDelay    = 180 * time.Millisecond
			aaaaDelay = 40 * time.Millisecond
		)
		d := &workerSlowDNS{
			aDelay:    aDelay,
			aaaaDelay: aaaaDelay,
			a:         []netip.Addr{workerAddr(t, "192.0.2.20")},
			aaaa:      []netip.Addr{workerAddr(t, "2001:db8::20")},
		}
		start := time.Now()
		got, err := NewDNSLookup(d, true).LookupNetIP(context.Background(), "slow-a.example")
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("LookupNetIP: %v", err)
		}
		// The outcome's order is stable regardless of the completion: A first,
		// even though it answered longer.
		if len(got) != 2 || got[0].String() != "192.0.2.20" || got[1].String() != "2001:db8::20" {
			t.Errorf("адреса: %v (порядок A, затем AAAA — как при последовательном опросе)", got)
		}
		// AAAA was asked while A was still in flight — the exchanges are concurrent.
		if !(d.entered[dns.TypeAAAA].Before(d.exited[dns.TypeA])) {
			t.Errorf("обмены не перекрываются: A [%v..%v], AAAA [%v..%v] — опрос последовательный",
				d.entered[dns.TypeA], d.exited[dns.TypeA], d.entered[dns.TypeAAAA], d.exited[dns.TypeAAAA])
		}
		// The overall time ≈ max(A, AAAA), not the sum: not faster than the slow
		// type (it must finish) and with headroom below the sum.
		if elapsed < aDelay {
			t.Errorf("время %v < задержки медленного типа %v — быстрый ответ не ждёт медленного соседа... или фейка", elapsed, aDelay)
		}
		if sum := aDelay + aaaaDelay; elapsed >= sum {
			t.Errorf("время %v ≥ суммы задержек %v — опрос последовательный", elapsed, sum)
		}
	})
	t.Run("отмена ctx — оба обмена отменяются", func(t *testing.T) {
		d := &workerBlockDNS{}
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(60 * time.Millisecond)
			cancel()
		}()
		start := time.Now()
		_, err := NewDNSLookup(d, true).LookupNetIP(ctx, "block.example")
		elapsed := time.Since(start)
		if err == nil {
			t.Fatal("отменённый резолв прошёл как успех")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("ошибка: %v (ожидалась отмена ctx, завёрнутая в сбой обмена)", err)
		}
		if n := d.asked(); n != 2 {
			t.Errorf("обменов %d — опрошены оба типа с общим ctx", n)
		}
		// The return — by the cancellation, not a foreign timeout: with headroom below
		// a second against the dnsclient timeouts (T-035).
		if elapsed > time.Second {
			t.Errorf("время %v — резолв не отменился сразу", elapsed)
		}
	})
}

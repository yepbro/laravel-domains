// Юнит-тесты http-worker (T-053): обработка задачи домена (пачка субдоменов,
// обе проверки протоколов), кэш итоговых URL («двойной URL — одна
// загрузка»), варианты/алиасы сквозью, SPA-флаг, ошибки субдомена не роняют
// домен, отмена ctx, smoke цикла Run, адаптер DNS-резолвера. Загрузка
// подменяется сценарием (шов FetchFunc), очередь — фейком queue.Queue;
// сети и Redis нет — за единственным исключением: настоящий
// *httpfetch.BodyLimiter собирает только реальный транспорт, поэтому
// TestWorkerBodyLimiterCeiling работает с loopback-сервером httptest.
// Идентификаторы с префиксом worker* против коллизий с соседними
// пакетами.

package httpworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/miekg/dns"

	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/dnsclient"
	"crawler/internal/httpfetch"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// --- фейк загрузки (шов FetchFunc) ------------------------------------------

// workerTimeoutErr — ошибка-таймаут (net.Error) для сценария тарпита/общего
// таймаута: классифицируется httpfetch.IsTimeout → ErrTimeout.
type workerTimeoutErr struct{}

func (workerTimeoutErr) Error() string   { return "i/o timeout" }
func (workerTimeoutErr) Timeout() bool   { return true }
func (workerTimeoutErr) Temporary() bool { return true }

// workerPageScript — одна страница сценария. Один вызов фейка изображает
// весь проход цепочки редиректов: steps — предыстория, финальный шаг
// дописывается сам.
type workerPageScript struct {
	status    int
	header    http.Header
	body      string
	final     string // итоговый URL; "" — как запрошенный
	steps     []httpfetch.RedirectStep
	err       error         // транспортная ошибка вместо ответа
	tooMany   bool          // ErrTooManyRedirects (Result nil, Chain — steps)
	blockCtx  bool          // ждать отмены ctx (тест отмены)
	setCookie []string      // значения Set-Cookie, если header не задан
	elapsed   time.Duration // Elapsed итогового Fetch (ResponseMS проверки)
}

// workerFetchLog — журнал вызовов фейка: какие URL реально ушли «в сеть».
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

// workerFakeFetch — сценарий «ключ → страница». Точный ключ
// "схема://хост/" имеет приоритет, ключ-хост — fallback обеих схем.
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

// workerWorker — сборка воркера над сценарием. Хранилище по умолчанию —
// фейк (раннее сохранение тел T-147 обязательно; конкретный фейк —
// newStoreFake из store_test.go, тем же пакетом).
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

// --- сквозная обработка: варианты, алиасы, кэш -------------------------------

// TestWorkerProcessDomain — задача из трёх субдоменов:
//
//   - апекс bare: https → редирект на https://www.example.com/ (тело home);
//     http ведёт туда же — варианты сворачиваются в одну запись variant 0
//     (https);
//   - www: https-проверка стартует на URL, который уже скачан проверкой
//     bare (итог её цепочки) — кэш итоговых URL, БЕЗ сети (§5.5 п. 6);
//     http ведёт на другой сайт (http://old.example.com/) → вторая запись
//     variant 1;
//   - api: https не отвечает (таймаут), http работает → единственная
//     запись variant 0 со scheme http.
//
// Алиасы (T-055): запись апекса и вариант 0 www сходятся на одном итоговом
// URL → класс эквивалентности, оригинал — «домашняя» запись www, апекс
// помечается редиректом на субдомен www.
func TestWorkerProcessDomain(t *testing.T) {
	home := "<html><body>" + strings.Repeat("достаточно видимого текста главной страницы сайта ", 12) + "</body></html>"
	old := "<html><body>совсем другой сайт на старом адресе, много текста</body></html>"
	fetch, log := workerFakeFetch(map[string]*workerPageScript{
		"example.com": { // обе проверки bare: редирект на www
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
	// PerDomain=1: проверки выполняются строго по порядку запуска —
	// кэш-попадание www https (итог bare) детерминировано, без гонки
	// «www стартовал раньше, чем bare закончил цепочку».
	w := workerWorker(t, fetch, func(o *Options) { o.PerDomain = 1 })

	res, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 1, DomainID: 7, Name: "example.com",
		Subdomains: []queue.HTTPSubdomain{workerSubMsg(0, ""), workerSubMsg(1, "www"), workerSubMsg(2, "api")},
	})
	if err != nil {
		t.Fatalf("ProcessDomain: %v", err)
	}

	// Сеть: bare обе проверки (2) + www http (1) + api обе (2); https www
	// взят из кэша итоговых URL — скачиваний www по https нет.
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

	// Кэш-попадание помечено; итоговый URL и синтетическая цепочка на месте.
	if !www.HTTPS.Cached {
		t.Error("https-проверка www не взята из кэша итоговых URL")
	}
	if bare.HTTP.Cached {
		t.Error("http-проверка bare стартует на своём URL — кэш неприменим")
	}
	if bare.HTTPS.FinalURL == nil || bare.HTTPS.FinalURL.String() != "https://www.example.com/" {
		t.Errorf("итоговый URL bare https: %v", bare.HTTPS.FinalURL)
	}
	if len(bare.HTTPS.Redirects) != 2 { // старт + итог
		t.Errorf("цепочка bare https: %d шагов, ожидалось 2", len(bare.HTTPS.Redirects))
	}
	if len(www.HTTPS.Redirects) != 1 { // синтетический единственный шаг
		t.Errorf("цепочка кэш-итога www https: %d шагов, ожидался 1", len(www.HTTPS.Redirects))
	}

	// bare: оба протокола — один сайт (тот же итоговый host+path) → 1 запись.
	if len(bare.Records) != 1 || bare.Records[0].Variant != model.VariantMain ||
		bare.Records[0].Scheme != model.SchemeHTTPS {
		t.Fatalf("записи bare: %+v", bare.Records)
	}
	// www: два варианта — 0 (https, кэш-итог) и 1 (http, другой сайт).
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
	// api: только http → одна запись variant 0 scheme http.
	if len(api.Records) != 1 || api.Records[0].Scheme != model.SchemeHTTP ||
		api.Records[0].Variant != model.VariantMain {
		t.Fatalf("записи api: %+v", api.Records)
	}
	if api.HTTPS.ErrClass != ErrTimeout || !api.HTTPS.ErrClass.IsTemp() {
		t.Errorf("класс ошибки api https: %v", api.HTTPS.ErrClass)
	}

	// Алиасы: bare v0 и www v0 — один итоговый URL; оригинал — www
	// («дома»), bare — редирект на субдомен www; контент хранит оригинал.
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
	// Хэш кэш-итога совпадает с хэшем первоисточника (то же тело).
	if bareRec.ContentHash == nil || *bareRec.ContentHash != *wwwMain.ContentHash {
		t.Error("хэши одной страницы (первоисточник и кэш) расходятся")
	}
	// Вариант 1 www и api независимы.
	if www.Records[1].AliasKind != model.AliasNone || api.Records[0].AliasKind != model.AliasNone {
		t.Error("чужие записи помечены алиасами")
	}
	// Данные DNS-этапа перенесены в результат (ТЗ 5.5 п. 7).
	if len(api.Addrs) != 1 || api.Addrs[0].String() != "192.0.2.1" {
		t.Errorf("адреса api: %v", api.Addrs)
	}
}

// TestWorkerCacheKey — нормализация ключа кэша: схема+хост+путь; query,
// фрагмент и дефолтные порты схем (80/443) не входят, нестандартный порт
// различает ключи, регистр хоста не важен (критерий T-054).
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
		"http://www.example.com/some/path",          // другая схема
		"https://old.example.com/some/path",         // другой хост
		"https://www.example.com/other",             // другой путь
		"https://www.example.com",                   // пустой путь — корень «/»
		"https://www.example.com:8443/some/path",    // нестандартный порт — другой сервис
		"https://www.example.com:444/some/path?q=1", // порт не глушится query
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

// TestWorkerCacheQueryAndTaskScope — итоговый URL с query кладётся в кэш
// под ключом без query (согласовано с критерием вариантов T-054): старт
// проверки park без query попадает в кэш. Кэш живёт одну задачу: вторая
// задача того же домена качает заново.
func TestWorkerCacheQueryAndTaskScope(t *testing.T) {
	body := "<html>" + strings.Repeat("текст парковочной страницы для проверки кэша ", 40) + "</html>"
	fetch, log := workerFakeFetch(map[string]*workerPageScript{
		"bare.test":              {status: 301, body: body, final: "https://park.bare.test/?i=1"},
		"park.bare.test":         {status: 200, body: body, final: "https://park.bare.test/"},
		"http://bare.test/":      {status: 301, body: body, final: "https://park.bare.test/?i=2"},
		"http://park.bare.test/": {status: 200, body: body, final: "https://park.bare.test/"},
	})
	// PerDomain=1 — детерминированный порядок проверок (см. TestWorkerProcessDomain).
	w := workerWorker(t, fetch, func(o *Options) { o.PerDomain = 1 })
	subs := []queue.HTTPSubdomain{workerSubMsg(0, ""), workerSubMsg(3, "park")}

	res1, err := w.ProcessDomain(context.Background(), queue.HTTPTask{
		ZoneID: 1, DomainID: 7, Name: "bare.test", Subdomains: subs,
	})
	if err != nil {
		t.Fatalf("ProcessDomain(1): %v", err)
	}
	// bare: обе проверки уводят на https://park.bare.test/?i=N — ключ
	// https://park.bare.test/ (query не входит); проверка park https
	// стартует ровно на нём → кэш, без сети. В сеть по park ходил
	// только http-запрос.
	if !res1.Subdomains[1].HTTPS.Cached {
		t.Error("итог с query не найден в кэше по ключу без query")
	}
	if got := log.countHost("park.bare.test"); got != 1 {
		t.Errorf("обращений к park в первой задаче: %d, ожидался 1 (только http)", got)
	}

	// Кэш per-задачи: вторая задача БЕЗ bare — её кэш пуст в момент старта,
	// https park качается по-настоящему (если бы кэш пережил задачу,
	// проверки park снова попали бы в записи первой задачи).
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
	if got := log.countHost("park.bare.test"); got != 3 { // 1 (http первой) + https + http второй
		t.Errorf("обращений к park после второй задачи: %d, ожидалось 3", got)
	}
}

// TestWorkerCacheHitKeepsErrorClass — обрезанная страница в кэше
// отдаёт кэш-попаданию свой класс ошибки (ErrBody): один и тот же
// контент не должен классифицироваться по-разному в зависимости от
// попадания в кэш (запись-источник — error=body, запись кэша — NULL).
func TestWorkerCacheHitKeepsErrorClass(t *testing.T) {
	huge := strings.Repeat("a", int(workerBodyHardLimit)+128)
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		// Обе проверки апекса сводятся к обрезанной странице www
		// (https — скачивание, http — сбой сети).
		"trunc.test":         {status: 200, body: huge, final: "https://www.trunc.test/"},
		"http://trunc.test/": {err: errors.New("connection refused")},
	})
	// PerDomain=1 — детерминированный порядок: https апекса наполняет
	// кэш до старта https www (см. TestWorkerProcessDomain).
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
	// Cached=true — сеть не трогалась вовсе (http-проверка www при
	// сценарии-ошибке честно несёт ErrNetwork, не подменяясь кэшем).
	if www.HTTP.ErrClass != ErrNetwork {
		t.Errorf("http www: класс %v (ожидался ErrNetwork)", www.HTTP.ErrClass)
	}
	// Класс доходит и до записи кэш-источника.
	if len(www.Records) == 0 {
		t.Fatal("у www нет записей, хотя https-итог доступен")
	}
	if www.Records[0].ErrClass != ErrBody {
		t.Errorf("запись кэш-источника: класс %v (ожидался ErrBody)", www.Records[0].ErrClass)
	}
}

// TestWorkerCacheHitResponseMS — кэш-попадание переносит измеренное
// первоисточником время ответа: кэш-итог часто становится записью-
// оригиналом алиас-класса (bare→www), её response_ms не должен быть
// нулём при живом измерении. Метрики кэш-попадания по-прежнему не
// наблюдают (Cached).
func TestWorkerCacheHitResponseMS(t *testing.T) {
	body := "<html>" + strings.Repeat("страница для теста времени ответа кэша ", 30) + "</html>"
	fetch, _ := workerFakeFetch(map[string]*workerPageScript{
		"rt.test": {status: 200, body: body, final: "https://www.rt.test/", elapsed: 150 * time.Millisecond},
		// http-проверка апекса — другой итог (не кэш), своё время.
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
	// Запись кэш-источника несёт время ответа в subdomain_results.
	if len(www.Records) == 0 {
		t.Fatal("у www нет записей")
	}
	if got := www.Records[0].ResponseMS; got != 150 {
		t.Errorf("ResponseMS записи кэш-источника: %d, ожидалось 150", got)
	}
}

// --- ошибки и особые случаи ----------------------------------------------------

// TestWorkerErrorsClassified — ошибки субдоменов (цикл редиректов, отказ
// сети, обрезанное тело) классифицируются в результат и не роняют домен
// (§8): другой субдомен даёт записи.
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
	// big: https — обрезанное тело, http — другой сайт → 2 записи; класс
	// ошибки проверки-источника доходит до записи.
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

// TestWorkerBodyLimiterCeiling — потолок тела задаёт BodyLimiter T-049
// (http.max_body_bytes), а не страховочная константа воркера: страница
// 3 МиБ при потолке 4 МиБ читается целиком (страховка 2 МиБ резала бы
// её с ложным ErrBody); страница 5 МиБ при потолке 4 МиБ режется
// лимитёром. Только реальный транспорт: настоящий BodyLimiter не
// построить сценарием шва FetchFunc — loopback-сервер httptest.
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
		// Тела ProcessDomain не удерживает (ранний Put T-147) —
		// ожидаемые байты сравниваются хэшем.
		wantLen := serve
		if wantLen > ceiling {
			wantLen = ceiling
		}
		return &res.Subdomains[0].HTTP, []byte(strings.Repeat("a", int(wantLen))) // сервер httptest — http
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

// TestWorkerReadBody — предвыделение буфера чтения тела по Content-Length
// (T-147): байты читаются целиком одним буфером, Content-Length без
// Content-Encoding задаёт ёмкость (враньё сервера в обе стороны не
// ломает чтение), сжатый ответ и отсутствие заголовка — обычный путь,
// страховочный потолок без BodyLimiter режет чтение (workerBodyHardLimit).
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
		maxCap int // потолок ёмкости буфера (0 — не проверять): одно
		// выделение по Content-Length плюс служебный slack ReadFrom
		// (minRead 512), без удвоений io.ReadAll
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
	// Гигантский Content-Length не резервирует гигабайт — потолок
	// предвыделения workerPreallocMax, чтение идёт дальше ростом буфера.
	res := mkRes("1073741824", "", strings.Repeat("x", 128), nil)
	got, err := workerReadBody(res)
	if err != nil || len(got) != 128 {
		t.Fatalf("гигантский Content-Length: байтов %d, ошибка %v", len(got), err)
	}
	// Без BodyLimiter чтение ограничено страховочным потолком воркера.
	big := strings.Repeat("y", int(workerBodyHardLimit)+64)
	got, err = workerReadBody(mkRes("", "", big, nil))
	if err != nil {
		t.Fatalf("workerReadBody без лимитёра: %v", err)
	}
	if int64(len(got)) != workerBodyHardLimit+1 {
		t.Errorf("прочитано %d байт, ожидалось %d (LimitReader потолка+1)", len(got), workerBodyHardLimit+1)
	}
}

// TestWorkerAllFailedNoHTTP — все субдомены ошибочны → записей нет («нет
// ответа ни у кого» по §7 — статус присвоит T-058); таймаут-класс виден
// через AnyTimeout (материал для temp_error, ТЗ 6.2).
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

// TestWorkerSPADetect — SPA-флаг по телу итоговой страницы (T-057):
// оболочка с корневым контейнером и требованием JS получает SPA=true.
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
	// Оба протокола апекса — одно тело на одном host+path → 1 запись.
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

// TestWorkerCookies — cookies итогового ответа разбираются в результат
// (ТЗ 5.5 п. 7 «cookies»), заголовки итоговой страницы переносятся записью.
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
	// Сценарий без TLS-соединения — TLSInfo записи nil (не ошибка, §8).
	if rec.TLS != nil {
		t.Errorf("TLS без TLS-соединения: %+v", rec.TLS)
	}
	if rec.Charset != "utf-8" {
		t.Errorf("кодировка: %q", rec.Charset)
	}
}

// TestWorkerContextCancel — отмена ctx в полёте: ProcessDomain возвращает
// ошибку (эпизод не завершён), результат не выдаётся — ложного «нет HTTP»
// не будет; доставка вернётся повторной доставкой (ТЗ 3.4).
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

// TestWorkerTaskValidation — негодные задачи отвергаются до сети.
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

// workerFakeFetchNoop — минимальный шов загрузки для проверок конструктора.
func workerFakeFetchNoop() FetchFunc {
	return func(context.Context, httpfetch.FetchRequest, httpfetch.ChainOptions) (*httpfetch.ChainResult, error) {
		return nil, errors.New("нет сценария")
	}
}

// --- smoke цикла Run -----------------------------------------------------------

// workerFakeQueue — фейк queue.Queue: выдаёт доставки один раз, дальше
// блокируется до отмены ctx; Ack и число чтений фиксируются.
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

func (q *workerFakeQueue) consumeCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.consumes
}

// workerDelivery — доставка с задачей домена.
func workerDelivery(t *testing.T, task queue.HTTPTask) queue.Delivery {
	t.Helper()
	payload, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	return queue.Delivery{Stream: queue.StreamHTTP, ID: "e1", Payload: payload}
}

// TestWorkerRunSmoke — цикл целиком: доставка q:http → обработка →
// Options.OnResult (точка подключения записи T-058, до XACK) → Ack;
// отмена ctx — чистый возврат nil.
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

// TestWorkerRunOnResultError — ошибка обработчика результата (будущая
// запись T-058) — доставка без XACK: вернётся повторной доставкой
// (at-least-once, ТЗ 3.4).
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

	// Окно с ошибочной доставкой завершено, когда цикл читает стрим снова.
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

// workerPoisonQueue — бесконечный источник ядовитых доставок: каждое
// чтение немедленно возвращает одну невалидную запись (Decode в
// processDelivery падает, доставка остаётся без XACK). Скорость чтений
// ограничивает только предохранитель цикла — идеальный измеритель его
// работы без таймингов обработки.
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

// TestWorkerRunBackoffOnFailures — предохранитель скользящего цикла
// (T-139): при систематическом сбое обработки (каждая доставка падает —
// здесь ядовитые записи) чтение q:http приостанавливается с удвоением
// паузы, иначе цикл пережигал бы попытки доставок (XAUTOCLAIM → q:dead,
// T-025) в горячем режиме; чтения при этом продолжаются — счётчик не
// блокирует цикл навсегда. Пауза сжата до 100 мс: секунды в юнит-тесте
// не ждать, границы взяты с многократным запасом против джиттера CI.
func TestWorkerRunBackoffOnFailures(t *testing.T) {
	old := workerFailWaitBase
	workerFailWaitBase = 100 * time.Millisecond
	t.Cleanup(func() { workerFailWaitBase = old })

	// Пул и пачка чтения минимальны: порог предохранителя — Consume
	// подряд неудач (1), конвейер чтения не глубже пары доставок. Шов
	// загрузки не вызывается (декод падает раньше), но New требует его.
	dummyFetch, _ := workerFakeFetch(nil)
	w := workerWorker(t, dummyFetch, nil)
	q := &workerPoisonQueue{}
	opts := Options{Concurrency: 1, Consume: 1}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, q, w, opts) }()

	// Первые чтения уходят до срабатывания счётчика (регистрация неудач
	// асинхронна) — окно в 450 мс покрывает их с запасом; дальше паузы
	// 100/200/400 мс душат темп чтений до единиц.
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

	// Паузы удваиваются: следующий интервал добавляет считанные чтения,
	// а не десятки. Рост между двумя замерами — не более пары чтений.
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

// TestWorkerRunSlidingConsumption — суть T-139: чтение q:http не ждёт
// завершения пачки целиком. Первая доставка окна блокируется в сети до
// отмены ctx (хвост длительностей — тарпит до total_timeout); при
// барьере окна (до T-139) wg.Wait держал бы цикл на ней — хвостовые
// доставки пачки и следующее чтение не начинались. Скользящее
// потребление обязано подтвердить все быстрые доставки, пока
// блокирующая в полёте: слоты освобождаются завершившимися задачами,
// читатель двигается дальше.
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

	// Блокирующая доставка ушла в сеть и держит слот — быстрые
	// дорабатывают и подтверждаются мимо неё.
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
	// Блокирующая доставка не подтверждена — вернётся повторной
	// доставкой (at-least-once, ТЗ 3.4), быстрые подтверждены ровно
	// по одному разу.
	if got := q.ackedCount(); got != 4 {
		t.Errorf("подтверждений %d, ожидалось 4 (блокирующая — без XACK)", got)
	}
}

// TestWorkerRunCancelKeepsBufferUnacked — гарантия скользящего буфера
// (T-139, ревью P3-3): прочитанные вперёд, но не начатые доставки (слоты
// заняты блокирующимися задачами) при отмене ctx НЕ запускаются и НЕ
// подтверждаются — вместе с текущими пачками читателя они остаются в
// pending стрима и возвращаются клеймом XAUTOCLAIM (at-least-once,
// ТЗ 3.4). Заодно — потолок слотов: третья и четвёртая задачи не
// стартуют, пока обе блокирующиеся держат Concurrency=2.
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
	// Пачка больше параллелизма: buf1/buf2 прочитаны читателем вперёд
	// (буфер + пачка в руке), но слотов для них нет, пока блокирующиеся
	// задачи держат оба.
	q := &workerFakeQueue{deliveries: []queue.Delivery{
		task(81, "block1.test"), task(82, "block2.test"),
		task(83, "buf1.test"), task(84, "buf2.test"),
	}}
	opts := Options{Concurrency: 2, Consume: 4}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, q, w, opts) }()

	// Обе блокирующиеся задачи в сети (держат слоты), буферные обязаны
	// остаться не начатыми — не обращаясь к сети, сколько бы цикл ни
	// работал.
	deadline := time.After(5 * time.Second)
	for log.countHost("block1.test") == 0 || log.countHost("block2.test") == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("блокирующиеся доставки не дошли до сети")
		case <-time.After(2 * time.Millisecond):
		}
	}
	time.Sleep(50 * time.Millisecond) // цикл успевает прочитать пачку и упереться в слоты

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

// TestWorkerOptionsDefaults — дефолты задачного параллелизма (T-139)
// воркера и конфигурации совпадают: боевая сборка (cmd/http-worker)
// передаёт http.task_* в Options, изолированные сборки берут
// withDefaults — расхождение молча меняло бы поведение боевого
// экземпляра против документации.
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
	// Явные значения не перетираются дефолтами.
	o = Options{Concurrency: 3, Consume: 2, PerDomain: 1}.withDefaults()
	if o.Concurrency != 3 || o.Consume != 2 || o.PerDomain != 1 {
		t.Errorf("withDefaults перетёр явные значения: %+v", o)
	}
}

// TestWorkerRunCancelWhileWaitingSlot — отмена ctx во время ожидания слота
// семафора не запускает доставку с уже отменённым ctx: окно из двух задач
// при Concurrency=1, первая блокируется в сети до отмены — вторая обязана
// остаться не начатой (её хост не читается из сети), окно возвращается
// очереди повторной доставкой.
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
	// Concurrency — параметр цикла Run (семафор окна), не воркера.
	opts := Options{Concurrency: 1}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, q, w, opts) }()

	// Первая доставка ушла в сеть (держит единственный слот) — отменяем:
	// вторая в этот момент ждёт семафор.
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

// --- адаптер DNS-резолвера (ТЗ 5.5 п. 2) ---------------------------------------

// workerAddr — парсинг адреса сценария (падение — ошибка теста).
func workerAddr(t *testing.T, s string) netip.Addr {
	t.Helper()
	a, err := netip.ParseAddr(s)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// workerFakeDNS — сценарный dnsclient.Resolver: отвечает на A/AAAA
// заданными адресами; withCNAME подмешивает CNAME-запись в ответ A
// (рекурсор отдаёт псевдоним и цель одним ответом); aErr/aaaaErr —
// сбой обмена только по одному типу записи (транзиентный отказ AAAA).
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

// workerSlowDNS — резолвер с задержкой ответа по типам (T-101): обмен
// A/AAAA «едёт» aDelay/aaaaDelay, моменты входа/выхода каждого обмена
// записываются — по перекрытию интервалов тест доказывает параллельность
// опроса без точной зависимости от общего времени. Сон — вне мьютекса:
// параллельные горутины не сериализуются замком.
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

// workerBlockDNS — резолвер, не отвечающий до отмены ctx (тест отмены
// T-101): оба параллельных обмена обязаны вернуться ошибкой контекста,
// а не ждать таймауты резолвера.
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

// sleepCtx — сон с выходом по отмене ctx (хелпер сценарных резолверов).
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
		// Опрос параллельный (T-101): порядок прибытия вопросов к
		// резолверу недетерминирован — проверяются оба типа как
		// множество, а не последовательность (два разных из двух
		// возможных = ровно A и AAAA).
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
		// Медленный A + быстрый AAAA: при последовательном опросе
		// интервалы обменов не пересекаются и сумма задержек съедается
		// целиком; параллельный — обмены перекрываются, общее время ≈
		// времени медленного типа. Перекрытие проверяется по moments
		// входа/выхода (сна вне мьютекса фейка), а не по общему времени
		// — момент надёжнее замера.
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
		// Порядок итога стабилен независимо от завершения: A первым,
		// хоть он и отвечал дольше.
		if len(got) != 2 || got[0].String() != "192.0.2.20" || got[1].String() != "2001:db8::20" {
			t.Errorf("адреса: %v (порядок A, затем AAAA — как при последовательном опросе)", got)
		}
		// AAAA спросили, пока A ещё был в полёте — обмены параллельны.
		if !(d.entered[dns.TypeAAAA].Before(d.exited[dns.TypeA])) {
			t.Errorf("обмены не перекрываются: A [%v..%v], AAAA [%v..%v] — опрос последовательный",
				d.entered[dns.TypeA], d.exited[dns.TypeA], d.entered[dns.TypeAAAA], d.exited[dns.TypeAAAA])
		}
		// Общее время ≈ max(A, AAAA), не сумма: не быстрее медленного
		// типа (он обязан завершиться) и с запасом ниже суммы.
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
		// Возврат — по отмене, а не по чужому таймауту: с запасом ниже
		// секунды против секунд таймаутов dnsclient (T-035).
		if elapsed > time.Second {
			t.Errorf("время %v — резолв не отменился сразу", elapsed)
		}
	})
}

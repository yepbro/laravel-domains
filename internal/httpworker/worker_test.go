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
	err       error    // транспортная ошибка вместо ответа
	tooMany   bool     // ErrTooManyRedirects (Result nil, Chain — steps)
	blockCtx  bool     // ждать отмены ctx (тест отмены)
	setCookie []string // значения Set-Cookie, если header не задан
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
		}
		chain := append([]httpfetch.RedirectStep(nil), p.steps...)
		chain = append(chain, httpfetch.RedirectStep{URL: final, Status: p.status})
		return &httpfetch.ChainResult{Result: res, Chain: chain}, nil
	}, log
}

// workerWorker — сборка воркера над сценарием.
func workerWorker(t *testing.T, fetch FetchFunc, mutate func(*Options)) *Worker {
	t.Helper()
	opt := Options{}
	if mutate != nil {
		mutate(&opt)
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
		w, err := New(ChainFetch(f), Options{})
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
		// Тела проверок ProcessDomain отпускает после разметки алиасов
		// (Body живёт у записи-оригинала) — ожидаемые байты сравниваются
		// хэшем; целая страница дополнительно видна в теле записи.
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
// (рекурсор отдаёт псевдоним и цель одним ответом).
type workerFakeDNS struct {
	mu        sync.Mutex
	asks      []uint16
	a, aaaa   []netip.Addr
	rcode     dnsclient.RCode
	err       error
	withCNAME bool
}

func (d *workerFakeDNS) Exchange(_ context.Context, q dnsclient.Question) (*dnsclient.Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.asks = append(d.asks, q.Type)
	if d.err != nil {
		return nil, d.err
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
			t.Errorf("адреса: %v", got)
		}
		if asked := d.asked(); len(asked) != 2 || asked[0] != dns.TypeA || asked[1] != dns.TypeAAAA {
			t.Errorf("вопросы: %v", asked)
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
}

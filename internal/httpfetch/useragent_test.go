package httpfetch

// Тесты T-052: честный User-Agent с контактами из конфигурации и
// контракт «минимум запросов» (ТЗ 5.5, §9):
//   - UA из http.user_agent (в т.ч. дефолт) доходит до сервера;
//   - ровно один GET «/» на Fetch, /robots.txt не запрашивается;
//   - UA стоит на каждом запросе цепочки редиректов, новые пути —
//     только по Location сервера (его инициатива, §9).

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"crawler/internal/config"
)

// uaRequest — метод, путь и User-Agent одного запроса тестового сервера.
type uaRequest struct {
	method string
	path   string
	ua     string
}

// uaLog — потокобезопасная запись всех запросов тестового сервера:
// контракт «минимум запросов» проверяется по полному журналу.
type uaLog struct {
	mu   sync.Mutex
	reqs []uaRequest
}

func (l *uaLog) add(r *http.Request) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.reqs = append(l.reqs, uaRequest{
		method: r.Method,
		path:   r.URL.Path,
		ua:     r.Header.Get("User-Agent"),
	})
}

func (l *uaLog) all() []uaRequest {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]uaRequest(nil), l.reqs...)
}

// TestUserAgentFromConfigReachesServer: UA секции http доходит до
// сервера — и заданный оператором, и дефолт config.DefaultUserAgent.
func TestUserAgentFromConfigReachesServer(t *testing.T) {
	custom := "OpCrawler/2.0 (+https://operator.example/contact; noc@example.example)"
	for _, tc := range []struct {
		name string
		cfg  config.HTTP
		want string
	}{
		{"кастомный из конфига", config.HTTP{UserAgent: custom}, custom},
		{"дефолт конфигурации", config.Default().HTTP, config.DefaultUserAgent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gotUA atomicString
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotUA.Store(r.Header.Get("User-Agent"))
				_, _ = w.Write([]byte("ok"))
			}))
			defer srv.Close()

			o := NewOptions(tc.cfg)
			if o.UserAgent != tc.want {
				t.Fatalf("NewOptions: UA %q, ожидался %q", o.UserAgent, tc.want)
			}
			f := NewFetcher(o)
			res, err := f.Fetch(context.Background(), FetchRequest{
				URL:   hostURL(srv.URL, "www.example-test.net"),
				Addrs: loopbackAddr(),
			})
			if err != nil {
				t.Fatalf("Fetch: %v", err)
			}
			defer func() { _ = res.Body.Close() }()
			if gotUA.Load() != tc.want {
				t.Errorf("сервер увидел User-Agent %q, ожидался %q", gotUA.Load(), tc.want)
			}
		})
	}
}

// TestNewOptionsUserAgentFallback: пустое поле конфигурации (секция,
// собранная руками, без Default/Validate) не оставляет транспорт
// анонимным — подставляется дефолт с контактами-заглушками.
func TestNewOptionsUserAgentFallback(t *testing.T) {
	o := NewOptions(config.HTTP{})
	if o.UserAgent != config.DefaultUserAgent {
		t.Errorf("NewOptions(пустая секция): UA %q, ожидался дефолт %q", o.UserAgent, config.DefaultUserAgent)
	}
	if strings.TrimSpace(o.UserAgent) == "" {
		t.Error("UA при сборке из конфигурации не должен быть пустым")
	}
}

// TestFetchRootOnlyNoRobots: контракт «минимум запросов» (ТЗ 5.5, §9) —
// один Fetch = ровно один GET «/»; URL без пути и с путём «/» — один и
// тот же запрос; никаких /robots.txt и иных путей транспорт не делает.
func TestFetchRootOnlyNoRobots(t *testing.T) {
	log := &uaLog{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.add(r)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	f := NewFetcher(Options{UserAgent: "RootCrawler/1.0 (+https://example.example/contact; admin@example.example)"})

	// URL из хоста без пути.
	u := hostURL(srv.URL, "www.example-test.net")
	res, err := f.Fetch(context.Background(), FetchRequest{URL: u, Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("Fetch без пути: %v", err)
	}
	_ = res.Body.Close()

	// Тот же URL с явным «/» — эквивалентный запрос главной страницы.
	u2 := *u
	u2.Path = "/"
	res, err = f.Fetch(context.Background(), FetchRequest{URL: &u2, Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("Fetch с путём \"/\": %v", err)
	}
	_ = res.Body.Close()

	reqs := log.all()
	if len(reqs) != 2 {
		t.Fatalf("запросов: %d, ожидалось ровно 2 (по одному на Fetch), журнал: %+v", len(reqs), reqs)
	}
	for i, r := range reqs {
		if r.method != http.MethodGet {
			t.Errorf("запрос %d: метод %q, ожидался GET", i, r.method)
		}
		if r.path != "/" {
			t.Errorf("запрос %d: путь %q — запрашивается только главная страница «/», robots.txt и иные пути исключены (ТЗ 5.5, §9)", i, r.path)
		}
		if r.ua != "RootCrawler/1.0 (+https://example.example/contact; admin@example.example)" {
			t.Errorf("запрос %d: User-Agent %q", i, r.ua)
		}
	}
}

// TestUserAgentOnRedirectChain: цепочка редиректов T-048 — UA стоит на
// каждом запросе; путь меняется только по Location сервера (инициатива
// сайта, §9), а не по решению краулера.
func TestUserAgentOnRedirectChain(t *testing.T) {
	const chainUA = "ChainCrawler/1.0 (+https://example.example/contact; admin@example.example)"
	log := &uaLog{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.add(r)
		switch r.URL.Path {
		case "/":
			w.Header().Set("Location", "/landing")
			w.WriteHeader(http.StatusMovedPermanently)
		default:
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte("<html><body>ok</body></html>"))
		}
	}))
	defer srv.Close()

	f := NewFetcher(Options{UserAgent: chainUA})
	res, err := FetchChain(context.Background(), f, FetchRequest{
		URL:   hostURL(srv.URL, "www.example-test.net"),
		Addrs: loopbackAddr(),
	})
	if err != nil {
		t.Fatalf("FetchChain: %v", err)
	}
	defer func() { _ = res.Result.Body.Close() }()

	if res.Result.StatusCode != http.StatusOK {
		t.Errorf("итоговый статус: %d, ожидался 200", res.Result.StatusCode)
	}
	if len(res.Chain) != 2 {
		t.Errorf("шагов цепочки: %d, ожидалось 2, цепочка: %+v", len(res.Chain), res.Chain)
	}

	reqs := log.all()
	if len(reqs) != 2 {
		t.Fatalf("запросов: %d, ожидалось 2 (стартовый «/» и ход по Location), журнал: %+v", len(reqs), reqs)
	}
	wantPaths := []string{"/", "/landing"}
	for i, r := range reqs {
		if r.method != http.MethodGet || r.path != wantPaths[i] {
			t.Errorf("запрос %d: %s %q, ожидался GET %q", i, r.method, r.path, wantPaths[i])
		}
		if r.ua != chainUA {
			t.Errorf("запрос %d (%s): User-Agent %q, ожидался конфигурационный", i, r.path, r.ua)
		}
	}
}

// TestUserAgentConfigValidation: http.user_agent непуст, без переводов
// строк и управляющих символов (значение идёт в заголовок), разумной
// длины (до 512 байт).
func TestUserAgentConfigValidation(t *testing.T) {
	base := func() *config.Config {
		c := config.Default()
		c.Postgres.DSN = "postgres://crawler:crawler@postgres:5432/crawler"
		return c
	}
	if err := base().Validate(); err != nil {
		t.Fatalf("дефолтная конфигурация должна быть валидна: %v", err)
	}

	for _, tc := range []struct {
		name string
		ua   string
	}{
		{"пустой", "   "},
		{"перевод строки", "Bot/1.0\nX:Injected"},
		{"управляющий символ", "Bot/1.0\x00"},
		{"слишком длинный", strings.Repeat("a", 513)},
	} {
		c := base()
		c.HTTP.UserAgent = tc.ua
		err := c.Validate()
		if err == nil || !strings.Contains(err.Error(), "http.user_agent") {
			t.Errorf("%s: ожидалась ошибка валидации http.user_agent, получено: %v", tc.name, err)
		}
	}

	// Граница длины допустима: 512 байт — ещё валидное значение.
	c := base()
	c.HTTP.UserAgent = strings.Repeat("a", 512)
	if err := c.Validate(); err != nil {
		t.Errorf("UA в 512 байт должен проходить валидацию: %v", err)
	}
}

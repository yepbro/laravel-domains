package httpfetch

// T-052 tests: an honest User-Agent with contacts from the configuration
// and the "minimum of requests" contract (spec 5.5, §9):
//   - the UA from http.user_agent (including the default) reaches the
//     server;
//   - exactly one GET "/" per Fetch, /robots.txt is never requested;
//   - the UA is on every request of the redirect chain, new paths come
//     only from the server's Location (its initiative, §9).

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"crawler/internal/config"
)

// uaRequest is the method, path and User-Agent of one request the test
// server received.
type uaRequest struct {
	method string
	path   string
	ua     string
}

// uaLog is a thread-safe record of all the test server's requests: the
// "minimum of requests" contract is verified against the full log.
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

// TestUserAgentFromConfigReachesServer: the UA of the http section
// reaches the server — both the operator-set one and the
// config.DefaultUserAgent default.
func TestUserAgentFromConfigReachesServer(t *testing.T) {
	custom := "OpCrawler/2.0 (+https://operator.example/contact; noc@example.example)"
	for _, tc := range []struct {
		name string
		cfg  config.HTTP
		want string
	}{
		{"a custom one from the config", config.HTTP{UserAgent: custom}, custom},
		{"the config default", config.Default().HTTP, config.DefaultUserAgent},
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
				t.Fatalf("NewOptions: UA %q, %q expected", o.UserAgent, tc.want)
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
				t.Errorf("the server saw the User-Agent %q, %q expected", gotUA.Load(), tc.want)
			}
		})
	}
}

// TestNewOptionsUserAgentFallback: an empty configuration field (a
// section assembled by hand, without Default/Validate) does not leave the
// transport anonymous — the default with stub contacts is substituted.
func TestNewOptionsUserAgentFallback(t *testing.T) {
	o := NewOptions(config.HTTP{})
	if o.UserAgent != config.DefaultUserAgent {
		t.Errorf("NewOptions(an empty section): UA %q, the default %q expected", o.UserAgent, config.DefaultUserAgent)
	}
	if strings.TrimSpace(o.UserAgent) == "" {
		t.Error("the UA assembled from the configuration must not be empty")
	}
}

// TestFetchRootOnlyNoRobots: the "minimum of requests" contract
// (spec 5.5, §9) — one Fetch = exactly one GET "/"; a URL without a path
// and with the path "/" are the same request; the transport makes no
// /robots.txt or other paths at all.
func TestFetchRootOnlyNoRobots(t *testing.T) {
	log := &uaLog{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.add(r)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	f := NewFetcher(Options{UserAgent: "RootCrawler/1.0 (+https://example.example/contact; admin@example.example)"})

	// A URL from the host, no path.
	u := hostURL(srv.URL, "www.example-test.net")
	res, err := f.Fetch(context.Background(), FetchRequest{URL: u, Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("Fetch without a path: %v", err)
	}
	_ = res.Body.Close()

	// The same URL with an explicit "/" — an equivalent main page request.
	u2 := *u
	u2.Path = "/"
	res, err = f.Fetch(context.Background(), FetchRequest{URL: &u2, Addrs: loopbackAddr()})
	if err != nil {
		t.Fatalf("Fetch with the \"/\" path: %v", err)
	}
	_ = res.Body.Close()

	reqs := log.all()
	if len(reqs) != 2 {
		t.Fatalf("requests: %d, exactly 2 expected (one per Fetch), the log: %+v", len(reqs), reqs)
	}
	for i, r := range reqs {
		if r.method != http.MethodGet {
			t.Errorf("request %d: the method %q, GET expected", i, r.method)
		}
		if r.path != "/" {
			t.Errorf("request %d: the path %q — only the main page \"/\" is requested, robots.txt and other paths are excluded (spec 5.5, §9)", i, r.path)
		}
		if r.ua != "RootCrawler/1.0 (+https://example.example/contact; admin@example.example)" {
			t.Errorf("request %d: the User-Agent %q", i, r.ua)
		}
	}
}

// TestUserAgentOnRedirectChain: the T-048 redirect chain — the UA is on
// every request; the path changes only by the server's Location (the
// site's initiative, §9), not by the crawler's decision.
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
		t.Errorf("final status: %d, 200 expected", res.Result.StatusCode)
	}
	if len(res.Chain) != 2 {
		t.Errorf("chain steps: %d, 2 expected, the chain: %+v", len(res.Chain), res.Chain)
	}

	reqs := log.all()
	if len(reqs) != 2 {
		t.Fatalf("requests: %d, 2 expected (the start \"/\" and the hop by Location), the log: %+v", len(reqs), reqs)
	}
	wantPaths := []string{"/", "/landing"}
	for i, r := range reqs {
		if r.method != http.MethodGet || r.path != wantPaths[i] {
			t.Errorf("request %d: %s %q, GET %q expected", i, r.method, r.path, wantPaths[i])
		}
		if r.ua != chainUA {
			t.Errorf("request %d (%s): the User-Agent %q, the configured one expected", i, r.path, r.ua)
		}
	}
}

// TestUserAgentConfigValidation: http.user_agent is non-empty, without
// line breaks and control characters (the value goes into a header), and
// of a reasonable length (up to 512 bytes).
func TestUserAgentConfigValidation(t *testing.T) {
	base := func() *config.Config {
		c := config.Default()
		c.Postgres.DSN = "postgres://crawler:crawler@postgres:5432/crawler"
		return c
	}
	if err := base().Validate(); err != nil {
		t.Fatalf("the default configuration must be valid: %v", err)
	}

	for _, tc := range []struct {
		name string
		ua   string
	}{
		{"empty", "   "},
		{"a line break", "Bot/1.0\nX:Injected"},
		{"a control character", "Bot/1.0\x00"},
		{"too long", strings.Repeat("a", 513)},
	} {
		c := base()
		c.HTTP.UserAgent = tc.ua
		err := c.Validate()
		if err == nil || !strings.Contains(err.Error(), "http.user_agent") {
			t.Errorf("%s: an http.user_agent validation error expected, got: %v", tc.name, err)
		}
	}

	// The length boundary is acceptable: 512 bytes is still a valid
	// value.
	c := base()
	c.HTTP.UserAgent = strings.Repeat("a", 512)
	if err := c.Validate(); err != nil {
		t.Errorf("a 512-byte UA must pass validation: %v", err)
	}
}

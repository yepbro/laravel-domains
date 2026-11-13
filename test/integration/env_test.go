// Integration tests of the dev environment on the testbed (spec 11).
//
// Run only when CRAWLER_INTEGRATION=1 (make test-integration): they check
// DNS scenarios, HTTP fixtures, "nasty" mockserver responses, TLS
// scenarios, DB/Redis/Chromium availability, and isolation from the
// internet.
package integration

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/miekg/dns"
	"golang.org/x/net/idna"
)

const (
	unboundAddr  = "172.28.100.10:53"
	testwebIP    = "172.28.100.12"
	mockserverIP = "172.28.100.13"
	redisAddr    = "172.28.100.3:6379"
	// DevTools of the new Chromium rejects a non-IP Host, so we address it by IP.
	chromiumURL = "http://172.28.100.14:9222/json/version"
)

func skipUnlessIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("CRAWLER_INTEGRATION") != "1" {
		t.Skip("integration tests: run make test-integration")
	}
}

// ---------------------------------------------------------------- DNS

func dnsQuery(t *testing.T, name string, qtype uint16) *dns.Msg {
	t.Helper()
	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(name), qtype)
	m.RecursionDesired = true
	resp, err := dns.Exchange(m, unboundAddr)
	if err != nil {
		t.Fatalf("DNS query %s: %v", name, err)
	}
	return resp
}

func aRecord(t *testing.T, msg *dns.Msg) string {
	t.Helper()
	for _, rr := range msg.Answer {
		if a, ok := rr.(*dns.A); ok {
			return a.A.String()
		}
	}
	return ""
}

func TestDNS_TestbedScenarios(t *testing.T) {
	skipUnlessIntegration(t)

	t.Run("a regular A", func(t *testing.T) {
		if got := aRecord(t, dnsQuery(t, "same-site.crawl.test", dns.TypeA)); got != testwebIP {
			t.Errorf("same-site.crawl.test = %q, want %s", got, testwebIP)
		}
	})

	t.Run("the apex NXDOMAIN", func(t *testing.T) {
		if rcode := dnsQuery(t, "nxdomain-case.crawl.test", dns.TypeA).Rcode; rcode != dns.RcodeNameError {
			t.Errorf("rcode = %d (%s), want NXDOMAIN", rcode, dns.RcodeToString[rcode])
		}
	})

	t.Run("SERVFAIL", func(t *testing.T) {
		if rcode := dnsQuery(t, "servfail.crawl.test", dns.TypeA).Rcode; rcode != dns.RcodeServerFailure {
			t.Errorf("rcode = %d (%s), want SERVFAIL", rcode, dns.RcodeToString[rcode])
		}
	})

	t.Run("wildcard", func(t *testing.T) {
		name := fmt.Sprintf("rnd%d.wildcard.crawl.test", time.Now().UnixNano()%100000)
		if got := aRecord(t, dnsQuery(t, name, dns.TypeA)); got != testwebIP {
			t.Errorf("the random label %s = %q, want %s (wildcard)", name, got, testwebIP)
		}
	})

	t.Run("a CNAME chain unfolds", func(t *testing.T) {
		if got := aRecord(t, dnsQuery(t, "cname-chain.crawl.test", dns.TypeA)); got != testwebIP {
			t.Errorf("cname-chain = %q, want %s", got, testwebIP)
		}
	})

	t.Run("a looping CNAME does not hang the resolver", func(t *testing.T) {
		m := new(dns.Msg)
		m.SetQuestion(dns.Fqdn("cname-loop.crawl.test"), dns.TypeA)
		m.RecursionDesired = true
		done := make(chan *dns.Msg, 1)
		go func() {
			resp, err := dns.Exchange(m, unboundAddr)
			if err != nil {
				t.Errorf("the cname-loop query: %v", err)
				done <- nil
				return
			}
			done <- resp
		}()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Fatal("the resolver hung on a cyclic CNAME")
		}
	})

	t.Run("delegated without addresses — NODATA", func(t *testing.T) {
		resp := dnsQuery(t, "www.delegated-no-address.crawl.test", dns.TypeA)
		if resp.Rcode != dns.RcodeSuccess {
			t.Fatalf("rcode = %s, want NOERROR", dns.RcodeToString[resp.Rcode])
		}
		if len(resp.Answer) != 0 {
			t.Errorf("empty answers expected (NODATA), got %d", len(resp.Answer))
		}
	})

	t.Run("closed ports — the black hole address", func(t *testing.T) {
		if got := aRecord(t, dnsQuery(t, "closed-ports.crawl.test", dns.TypeA)); got != "172.28.100.250" {
			t.Errorf("closed-ports = %q, want 172.28.100.250", got)
		}
	})

	t.Run("the composite zone co.test", func(t *testing.T) {
		for _, name := range []string{"example.co.test", "www.example.co.test", "shop.co.test"} {
			if got := aRecord(t, dnsQuery(t, name, dns.TypeA)); got != testwebIP {
				t.Errorf("%s = %q, want %s", name, got, testwebIP)
			}
		}
	})

	t.Run("an IDN domain in punycode", func(t *testing.T) {
		ascii, err := idna.Lookup.ToASCII("сайт.проверка.test")
		if err != nil {
			t.Fatal(err)
		}
		if got := aRecord(t, dnsQuery(t, ascii, dns.TypeA)); got != testwebIP {
			t.Errorf("%s (сайт.проверка.test) = %q, want %s", ascii, got, testwebIP)
		}
	})

	t.Run("the second zone", func(t *testing.T) {
		if got := aRecord(t, dnsQuery(t, "alpha.crawl2.test", dns.TypeA)); got != testwebIP {
			t.Errorf("alpha.crawl2.test = %q, want %s", got, testwebIP)
		}
	})
}

// Bench isolation (spec 11.2): services do not resolve external domains
// in testbed mode (acceptance criterion 5 — the laravel_* tables,
// TestAccept05; this test is about DNS isolation).
func TestDNS_NoInternetInTestbed(t *testing.T) {
	skipUnlessIntegration(t)
	for _, name := range []string{"example.com", "google.com", "ya.ru"} {
		for _, qtype := range []uint16{dns.TypeA, dns.TypeAAAA} {
			resp := dnsQuery(t, name, qtype)
			resolved := resp.Rcode == dns.RcodeSuccess && len(resp.Answer) > 0
			if resolved {
				t.Errorf("%s %s: an external domain resolves (rcode=%s, answers=%d) — the isolation broken",
					dns.TypeToString[qtype], name, dns.RcodeToString[resp.Rcode], len(resp.Answer))
			}
		}
	}
}

// ---------------------------------------------------------------- HTTP

// clientTo returns a client that connects to a fixed IP (as http-worker
// does after its own resolution) while keeping Host/SNI from the URL.
func clientTo(ip string, insecure bool, extra func(*http.Transport)) *http.Client {
	tr := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			_, port, _ := net.SplitHostPort(addr)
			var d net.Dialer
			return d.DialContext(ctx, network, net.JoinHostPort(ip, port))
		},
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: insecure}, //nolint:gosec // testbed
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		DisableKeepAlives:     true,
	}
	if extra != nil {
		extra(tr)
	}
	return &http.Client{Transport: tr, Timeout: 20 * time.Second}
}

func getBody(t *testing.T, c *http.Client, rawURL string) (int, string) {
	t.Helper()
	resp, err := c.Get(rawURL)
	if err != nil {
		t.Fatalf("GET %s: %v", rawURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatalf("reading the %s body: %v", rawURL, err)
	}
	return resp.StatusCode, string(b)
}

func TestHTTP_TestwebScenarios(t *testing.T) {
	skipUnlessIntegration(t)
	c := clientTo(testwebIP, true, nil)

	t.Run("the http→https redirect leads to one site", func(t *testing.T) {
		client := clientTo(testwebIP, true, nil)
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return http.ErrUseLastResponse
			}
			return nil
		}
		resp, err := client.Get("http://same-site.crawl.test/")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("the final status = %d, want 200", resp.StatusCode)
		}
		if !strings.HasPrefix(resp.Request.URL.String(), "https://same-site.crawl.test") {
			t.Errorf("the final URL = %s, want https://same-site...", resp.Request.URL)
		}
	})

	t.Run("http and https — different sites", func(t *testing.T) {
		_, httpBody := getBody(t, c, "http://different-sites.crawl.test/")
		_, httpsBody := getBody(t, c, "https://different-sites.crawl.test/")
		if httpBody == httpsBody {
			t.Error("the http and https content matches — the different-sites scenario broken")
		}
	})

	t.Run("http-only and https-only", func(t *testing.T) {
		if code, _ := getBody(t, c, "http://http-only.crawl.test/"); code != 200 {
			t.Errorf("http-only over http = %d, want 200", code)
		}
		if code, _ := getBody(t, c, "https://https-only.crawl.test/"); code != 200 {
			t.Errorf("https-only over https = %d, want 200", code)
		}
	})

	t.Run("the redirect chain reaches the final", func(t *testing.T) {
		client := clientTo(testwebIP, true, nil)
		resp, err := client.Get("http://redirect-chain.crawl.test/")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != 200 || !strings.HasSuffix(resp.Request.URL.Path, "/final.html") {
			t.Errorf("the outcome = %d %s, want 200 .../final.html", resp.StatusCode, resp.Request.URL)
		}
	})

	t.Run("the meta refresh in place", func(t *testing.T) {
		_, body := getBody(t, c, "http://meta-refresh.crawl.test/")
		if !strings.Contains(body, `http-equiv="refresh"`) {
			t.Error("the page has no meta refresh")
		}
	})

	t.Run("the bare→www redirect", func(t *testing.T) {
		client := &http.Client{Transport: c.Transport.(*http.Transport)} //nolint:forcetypeassert // our transport
		client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		resp, err := client.Get("http://bare-to-www.crawl.test/")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != 301 {
			t.Fatalf("status = %d, want 301 (Caddy permanent)", resp.StatusCode)
		}
		if loc := resp.Header.Get("Location"); !strings.HasPrefix(loc, "https://www.bare-to-www.crawl.test") {
			t.Errorf("Location = %q, want https://www.bare-to-www...", loc)
		}
	})

	t.Run("www and bare identical without a redirect", func(t *testing.T) {
		codeA, bodyA := getBody(t, c, "http://equal-pair.crawl.test/")
		codeW, bodyW := getBody(t, c, "http://www.equal-pair.crawl.test/")
		if codeA != 200 || codeW != 200 {
			t.Fatalf("statuses: apex=%d www=%d, want 200/200", codeA, codeW)
		}
		if bodyA != bodyW {
			t.Error("the apex and www content must match (alias_kind = 1)")
		}
	})

	t.Run("parked pages identical — dedup by hash", func(t *testing.T) {
		_, b1 := getBody(t, c, "http://parked-1.crawl.test/")
		_, b2 := getBody(t, c, "http://parked-2.crawl.test/")
		if b1 != b2 {
			t.Error("the parked-1 and parked-2 content must be byte-identical")
		}
	})

	t.Run("the SPA shell", func(t *testing.T) {
		_, body := getBody(t, c, "https://spa-shell.crawl.test/")
		for _, marker := range []string{`id="root"`, "<noscript>", "document.cookie"} {
			if !strings.Contains(body, marker) {
				t.Errorf("the SPA fixture without the marker %q", marker)
			}
		}
	})

	t.Run("the Laravel samples served with headers", func(t *testing.T) {
		client := clientTo(testwebIP, true, nil)
		resp, err := client.Get("https://laravel-11-vite.crawl.test/")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != 200 {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		if len(resp.Cookies()) == 0 {
			t.Error("the laravel-11-vite sample must get the cookies from headers.json")
		}
		found := false
		for _, ck := range resp.Cookies() {
			if ck.Name == "XSRF-TOKEN" {
				found = true
			}
		}
		if !found {
			t.Error("the XSRF-TOKEN cookie not set")
		}
	})
}

func TestHTTP_MockserverEvil(t *testing.T) {
	skipUnlessIntegration(t)
	base := "http://" + mockserverIP

	t.Run("the tarpit breaks on timeout", func(t *testing.T) {
		client := clientTo(mockserverIP, true, nil)
		client.Timeout = 3 * time.Second
		resp, err := client.Get(base + "/tarpit")
		if err != nil {
			t.Fatalf("the tarpit query: %v", err)
		}
		_, err = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if err == nil {
			t.Error("reading the infinite body must have broken by timeout")
		}
	})

	t.Run("slow headers give a header timeout", func(t *testing.T) {
		client := clientTo(mockserverIP, true, func(tr *http.Transport) {
			tr.ResponseHeaderTimeout = 2 * time.Second
		})
		_, err := client.Get(base + "/slow?delay=30s")
		if err == nil {
			t.Error("a header timeout expected on /slow")
		}
	})

	t.Run("an infinite redirect hits the limit", func(t *testing.T) {
		client := clientTo(mockserverIP, true, nil)
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("stopped after %d redirects: %w", len(via), http.ErrUseLastResponse)
			}
			return nil
		}
		resp, err := client.Get(base + "/redirect/infinite")
		if err == nil {
			_ = resp.Body.Close()
			t.Fatal("the redirects must not have ended")
		}
	})

	t.Run("a cyclic redirect detected", func(t *testing.T) {
		client := clientTo(mockserverIP, true, nil)
		seen := map[string]bool{}
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			if seen[req.URL.String()] {
				return errors.New("the redirect loop")
			}
			seen[req.URL.String()] = true
			if len(via) >= 10 {
				return http.ErrUseLastResponse
			}
			return nil
		}
		_, err := client.Get(base + "/redirect/cycle/a")
		if err == nil {
			t.Error("the /a→/b→/a cycle detection expected")
		}
	})

	t.Run("a connection break in the middle of the body", func(t *testing.T) {
		client := clientTo(mockserverIP, true, nil)
		resp, err := client.Get(base + "/truncate")
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if err == nil {
			t.Error("the read must have ended with an error (unexpected EOF)")
		}
	})

	t.Run("a huge body read no more than the limit", func(t *testing.T) {
		client := clientTo(mockserverIP, true, nil)
		resp, err := client.Get(base + "/huge")
		if err != nil {
			t.Fatal(err)
		}
		n, err := io.Copy(io.Discard, io.LimitReader(resp.Body, 2<<20))
		_ = resp.Body.Close()
		if err != nil || n != 2<<20 {
			t.Fatalf("read %d bytes (err=%v), want %d without an error", n, err, 2<<20)
		}
	})

	t.Run("a zip bomb limited on unpacking", func(t *testing.T) {
		client := clientTo(mockserverIP, true, nil)
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, base+"/zipbomb", nil)
		req.Header.Set("Accept-Encoding", "gzip") // we take the raw gzip and decompress it ourselves
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		zr, err := gzip.NewReader(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		n, err := io.Copy(io.Discard, io.LimitReader(zr, 2<<20))
		_ = resp.Body.Close()
		if err != nil || n != 2<<20 {
			t.Fatalf("unpacked %d bytes (err=%v), want %d without an error", n, err, 2<<20)
		}
	})

	t.Run("the Laravel cookies set", func(t *testing.T) {
		client := clientTo(mockserverIP, true, nil)
		resp, err := client.Get(base + "/cookies")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		names := map[string]bool{}
		for _, ck := range resp.Cookies() {
			names[ck.Name] = true
		}
		if !names["XSRF-TOKEN"] || !names["laravel_session"] {
			t.Errorf("cookies = %v, want XSRF-TOKEN and laravel_session", names)
		}
	})
}

func TestTLS_BadCertificates(t *testing.T) {
	skipUnlessIntegration(t)

	fetch := func(host string, insecure bool) (*http.Response, error) {
		client := clientTo(mockserverIP, insecure, nil)
		req, _ := http.NewRequest(http.MethodGet, "https://"+host+"/host", nil)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		_ = resp.Body.Close()
		return resp, nil
	}

	cases := []string{
		"self-signed.bad-tls.crawl.test",
		"expired.bad-tls.crawl.test",
		"wrong-san.bad-tls.crawl.test",
	}
	for _, host := range cases {
		t.Run(host, func(t *testing.T) {
			if _, err := fetch(host, false); err == nil {
				t.Error("the verification must have failed (the certificate invalid)")
			}
			resp, err := fetch(host, true)
			if err != nil {
				t.Fatalf("InsecureSkipVerify: %v", err)
			}
			if resp.TLS == nil || len(resp.TLS.PeerCertificates) == 0 {
				t.Error("the certificate data must be available")
			}
		})
	}
}

// ---------------------------------------------------------------- infrastructure

func TestInfra_Postgres(t *testing.T) {
	skipUnlessIntegration(t)
	dsn := os.Getenv("APP_PG_DSN")
	if dsn == "" {
		dsn = "postgres://crawler:dev-only-crawler@172.28.100.2:5432/crawler?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("PostgreSQL connection: %v", err)
	}
	defer pool.Close()

	var ok bool
	if err := pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'domains')").Scan(&ok); err != nil || !ok {
		t.Fatalf("the domains table missing — the migrations not applied (err=%v)", err)
	}

	var subdomains int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM subdomains WHERE enabled").Scan(&subdomains); err != nil {
		t.Fatalf("the subdomains table: %v", err)
	}
	if subdomains != 6 {
		t.Errorf("enabled subdomains = %d, want 6 (migration 000001)", subdomains)
	}

	if _, err := pool.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatalf("SELECT 1: %v", err)
	}
}

func TestInfra_Redis(t *testing.T) {
	skipUnlessIntegration(t)
	conn, err := net.DialTimeout("tcp", redisAddr, 5*time.Second)
	if err != nil {
		t.Fatalf("Redis connection: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(line, "PONG") {
		t.Errorf("the Redis reply = %q, want +PONG", strings.TrimSpace(line))
	}
}

func TestChromium_CDP(t *testing.T) {
	skipUnlessIntegration(t)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(chromiumURL)
	if err != nil {
		t.Fatalf("the chromium CDP endpoint unavailable: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != 200 || !strings.Contains(string(body), "Browser") {
		t.Errorf("the /json/version reply: %d %s", resp.StatusCode, string(body))
	}
}

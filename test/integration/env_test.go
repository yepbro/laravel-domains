// Интеграционные тесты dev-окружения на testbed (ТЗ 11).
//
// Запускаются только при CRAWLER_INTEGRATION=1 (make test-integration):
// проверяют DNS-сценарии, HTTP-фикстуры, «злые» ответы mockserver,
// TLS-сценарии, доступность БД/Redis/Chromium и изоляцию от интернета.
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
	// DevTools нового Chromium отклоняет не-IP Host, поэтому обращаемся по IP.
	chromiumURL = "http://172.28.100.14:9222/json/version"
)

func skipUnlessIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("CRAWLER_INTEGRATION") != "1" {
		t.Skip("интеграционные тесты: запустите make test-integration")
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
		t.Fatalf("DNS-запрос %s: %v", name, err)
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

	t.Run("обычный A", func(t *testing.T) {
		if got := aRecord(t, dnsQuery(t, "same-site.crawl.test", dns.TypeA)); got != testwebIP {
			t.Errorf("same-site.crawl.test = %q, want %s", got, testwebIP)
		}
	})

	t.Run("NXDOMAIN апекса", func(t *testing.T) {
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
			t.Errorf("случайная метка %s = %q, want %s (wildcard)", name, got, testwebIP)
		}
	})

	t.Run("CNAME-цепочка разворачивается", func(t *testing.T) {
		if got := aRecord(t, dnsQuery(t, "cname-chain.crawl.test", dns.TypeA)); got != testwebIP {
			t.Errorf("cname-chain = %q, want %s", got, testwebIP)
		}
	})

	t.Run("зацикленный CNAME не подвешивает резолвер", func(t *testing.T) {
		m := new(dns.Msg)
		m.SetQuestion(dns.Fqdn("cname-loop.crawl.test"), dns.TypeA)
		m.RecursionDesired = true
		done := make(chan *dns.Msg, 1)
		go func() {
			resp, err := dns.Exchange(m, unboundAddr)
			if err != nil {
				t.Errorf("запрос cname-loop: %v", err)
				done <- nil
				return
			}
			done <- resp
		}()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Fatal("резолвер завис на циклическом CNAME")
		}
	})

	t.Run("delegated без адресов — NODATA", func(t *testing.T) {
		resp := dnsQuery(t, "www.delegated-no-address.crawl.test", dns.TypeA)
		if resp.Rcode != dns.RcodeSuccess {
			t.Fatalf("rcode = %s, want NOERROR", dns.RcodeToString[resp.Rcode])
		}
		if len(resp.Answer) != 0 {
			t.Errorf("ожидались пустые ответы (NODATA), получено %d", len(resp.Answer))
		}
	})

	t.Run("закрытые порты — адрес чёрной дыры", func(t *testing.T) {
		if got := aRecord(t, dnsQuery(t, "closed-ports.crawl.test", dns.TypeA)); got != "172.28.100.250" {
			t.Errorf("closed-ports = %q, want 172.28.100.250", got)
		}
	})

	t.Run("составная зона co.test", func(t *testing.T) {
		for _, name := range []string{"example.co.test", "www.example.co.test", "shop.co.test"} {
			if got := aRecord(t, dnsQuery(t, name, dns.TypeA)); got != testwebIP {
				t.Errorf("%s = %q, want %s", name, got, testwebIP)
			}
		}
	})

	t.Run("IDN-домен в punycode", func(t *testing.T) {
		ascii, err := idna.Lookup.ToASCII("сайт.проверка.test")
		if err != nil {
			t.Fatal(err)
		}
		if got := aRecord(t, dnsQuery(t, ascii, dns.TypeA)); got != testwebIP {
			t.Errorf("%s (сайт.проверка.test) = %q, want %s", ascii, got, testwebIP)
		}
	})

	t.Run("вторая зона", func(t *testing.T) {
		if got := aRecord(t, dnsQuery(t, "alpha.crawl2.test", dns.TypeA)); got != testwebIP {
			t.Errorf("alpha.crawl2.test = %q, want %s", got, testwebIP)
		}
	})
}

// Критерий приёмки 5: сервисы не резолвят внешние домены в режиме testbed.
func TestDNS_NoInternetInTestbed(t *testing.T) {
	skipUnlessIntegration(t)
	for _, name := range []string{"example.com", "google.com", "ya.ru"} {
		for _, qtype := range []uint16{dns.TypeA, dns.TypeAAAA} {
			resp := dnsQuery(t, name, qtype)
			resolved := resp.Rcode == dns.RcodeSuccess && len(resp.Answer) > 0
			if resolved {
				t.Errorf("%s %s: внешний домен резолвится (rcode=%s, answers=%d) — изоляция нарушена",
					dns.TypeToString[qtype], name, dns.RcodeToString[resp.Rcode], len(resp.Answer))
			}
		}
	}
}

// ---------------------------------------------------------------- HTTP

// clientTo возвращает клиент, который соединяется с фиксированным IP
// (как это делает http-worker после собственного резолва), сохраняя
// Host/SNI из URL.
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
		t.Fatalf("чтение тела %s: %v", rawURL, err)
	}
	return resp.StatusCode, string(b)
}

func TestHTTP_TestwebScenarios(t *testing.T) {
	skipUnlessIntegration(t)
	c := clientTo(testwebIP, true, nil)

	t.Run("http→https редирект ведёт на один сайт", func(t *testing.T) {
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
			t.Fatalf("итоговый статус = %d, want 200", resp.StatusCode)
		}
		if !strings.HasPrefix(resp.Request.URL.String(), "https://same-site.crawl.test") {
			t.Errorf("итоговый URL = %s, want https://same-site...", resp.Request.URL)
		}
	})

	t.Run("http и https — разные сайты", func(t *testing.T) {
		_, httpBody := getBody(t, c, "http://different-sites.crawl.test/")
		_, httpsBody := getBody(t, c, "https://different-sites.crawl.test/")
		if httpBody == httpsBody {
			t.Error("контент http и https совпадает — сценарий different-sites сломан")
		}
	})

	t.Run("только http и только https", func(t *testing.T) {
		if code, _ := getBody(t, c, "http://http-only.crawl.test/"); code != 200 {
			t.Errorf("http-only по http = %d, want 200", code)
		}
		if code, _ := getBody(t, c, "https://https-only.crawl.test/"); code != 200 {
			t.Errorf("https-only по https = %d, want 200", code)
		}
	})

	t.Run("цепочка редиректов доходит до финала", func(t *testing.T) {
		client := clientTo(testwebIP, true, nil)
		resp, err := client.Get("http://redirect-chain.crawl.test/")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != 200 || !strings.HasSuffix(resp.Request.URL.Path, "/final.html") {
			t.Errorf("итог = %d %s, want 200 .../final.html", resp.StatusCode, resp.Request.URL)
		}
	})

	t.Run("meta refresh на месте", func(t *testing.T) {
		_, body := getBody(t, c, "http://meta-refresh.crawl.test/")
		if !strings.Contains(body, `http-equiv="refresh"`) {
			t.Error("на странице нет meta refresh")
		}
	})

	t.Run("bare→www редирект", func(t *testing.T) {
		client := &http.Client{Transport: c.Transport.(*http.Transport)} //nolint:forcetypeassert // наш транспорт
		client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		resp, err := client.Get("http://bare-to-www.crawl.test/")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != 301 {
			t.Fatalf("статус = %d, want 301 (Caddy permanent)", resp.StatusCode)
		}
		if loc := resp.Header.Get("Location"); !strings.HasPrefix(loc, "https://www.bare-to-www.crawl.test") {
			t.Errorf("Location = %q, want https://www.bare-to-www...", loc)
		}
	})

	t.Run("www и bare одинаковы без редиректа", func(t *testing.T) {
		codeA, bodyA := getBody(t, c, "http://equal-pair.crawl.test/")
		codeW, bodyW := getBody(t, c, "http://www.equal-pair.crawl.test/")
		if codeA != 200 || codeW != 200 {
			t.Fatalf("статусы: apex=%d www=%d, want 200/200", codeA, codeW)
		}
		if bodyA != bodyW {
			t.Error("контент apex и www должен совпадать (alias_kind = 1)")
		}
	})

	t.Run("парковки идентичны — дедупликация по хэшу", func(t *testing.T) {
		_, b1 := getBody(t, c, "http://parked-1.crawl.test/")
		_, b2 := getBody(t, c, "http://parked-2.crawl.test/")
		if b1 != b2 {
			t.Error("контент parked-1 и parked-2 должен быть побайтово одинаков")
		}
	})

	t.Run("SPA-оболочка", func(t *testing.T) {
		_, body := getBody(t, c, "https://spa-shell.crawl.test/")
		for _, marker := range []string{`id="root"`, "<noscript>", "document.cookie"} {
			if !strings.Contains(body, marker) {
				t.Errorf("SPA-фикстура без маркера %q", marker)
			}
		}
	})

	t.Run("образцы Laravel отдаются с заголовками", func(t *testing.T) {
		client := clientTo(testwebIP, true, nil)
		resp, err := client.Get("https://laravel-11-vite.crawl.test/")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != 200 {
			t.Fatalf("статус = %d", resp.StatusCode)
		}
		if len(resp.Cookies()) == 0 {
			t.Error("у образца laravel-11-vite должны проставляться cookies из headers.json")
		}
		found := false
		for _, ck := range resp.Cookies() {
			if ck.Name == "XSRF-TOKEN" {
				found = true
			}
		}
		if !found {
			t.Error("cookie XSRF-TOKEN не выставлена")
		}
	})
}

func TestHTTP_MockserverEvil(t *testing.T) {
	skipUnlessIntegration(t)
	base := "http://" + mockserverIP

	t.Run("тарпит обрывается по таймауту", func(t *testing.T) {
		client := clientTo(mockserverIP, true, nil)
		client.Timeout = 3 * time.Second
		resp, err := client.Get(base + "/tarpit")
		if err != nil {
			t.Fatalf("запрос к тарпиту: %v", err)
		}
		_, err = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if err == nil {
			t.Error("чтение бесконечного тела должно было прерваться по таймауту")
		}
	})

	t.Run("медленные заголовки дают header timeout", func(t *testing.T) {
		client := clientTo(mockserverIP, true, func(tr *http.Transport) {
			tr.ResponseHeaderTimeout = 2 * time.Second
		})
		_, err := client.Get(base + "/slow?delay=30s")
		if err == nil {
			t.Error("ожидался header timeout на /slow")
		}
	})

	t.Run("бесконечный редирект упирается в лимит", func(t *testing.T) {
		client := clientTo(mockserverIP, true, nil)
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("остановлено после %d редиректов: %w", len(via), http.ErrUseLastResponse)
			}
			return nil
		}
		resp, err := client.Get(base + "/redirect/infinite")
		if err == nil {
			_ = resp.Body.Close()
			t.Fatal("редиректы не должны были закончиться")
		}
	})

	t.Run("циклический редирект обнаруживается", func(t *testing.T) {
		client := clientTo(mockserverIP, true, nil)
		seen := map[string]bool{}
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			if seen[req.URL.String()] {
				return errors.New("цикл редиректов")
			}
			seen[req.URL.String()] = true
			if len(via) >= 10 {
				return http.ErrUseLastResponse
			}
			return nil
		}
		_, err := client.Get(base + "/redirect/cycle/a")
		if err == nil {
			t.Error("ожидался детект цикла /a→/b→/a")
		}
	})

	t.Run("обрыв соединения посреди тела", func(t *testing.T) {
		client := clientTo(mockserverIP, true, nil)
		resp, err := client.Get(base + "/truncate")
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if err == nil {
			t.Error("чтение должно было завершиться ошибкой (unexpected EOF)")
		}
	})

	t.Run("огромное тело читается не больше лимита", func(t *testing.T) {
		client := clientTo(mockserverIP, true, nil)
		resp, err := client.Get(base + "/huge")
		if err != nil {
			t.Fatal(err)
		}
		n, err := io.Copy(io.Discard, io.LimitReader(resp.Body, 2<<20))
		_ = resp.Body.Close()
		if err != nil || n != 2<<20 {
			t.Fatalf("прочитано %d байт (err=%v), want %d без ошибки", n, err, 2<<20)
		}
	})

	t.Run("zip-бомба ограничивается при распаковке", func(t *testing.T) {
		client := clientTo(mockserverIP, true, nil)
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, base+"/zipbomb", nil)
		req.Header.Set("Accept-Encoding", "gzip") // берём сырой gzip, разжимаем сами
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
			t.Fatalf("распаковано %d байт (err=%v), want %d без ошибки", n, err, 2<<20)
		}
	})

	t.Run("cookies Laravel выставляются", func(t *testing.T) {
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
			t.Errorf("cookies = %v, want XSRF-TOKEN и laravel_session", names)
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
				t.Error("верификация должна была провалиться (сертификат невалиден)")
			}
			resp, err := fetch(host, true)
			if err != nil {
				t.Fatalf("InsecureSkipVerify: %v", err)
			}
			if resp.TLS == nil || len(resp.TLS.PeerCertificates) == 0 {
				t.Error("данные сертификата должны быть доступны")
			}
		})
	}
}

// ---------------------------------------------------------------- инфраструктура

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
		t.Fatalf("подключение к PostgreSQL: %v", err)
	}
	defer pool.Close()

	var ok bool
	if err := pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'domains')").Scan(&ok); err != nil || !ok {
		t.Fatalf("таблица domains отсутствует — миграции не применены (err=%v)", err)
	}

	var subdomains int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM subdomains WHERE enabled").Scan(&subdomains); err != nil {
		t.Fatalf("таблица subdomains: %v", err)
	}
	if subdomains != 6 {
		t.Errorf("включённых субдоменов = %d, want 6 (миграция 000001)", subdomains)
	}

	if _, err := pool.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatalf("SELECT 1: %v", err)
	}
}

func TestInfra_Redis(t *testing.T) {
	skipUnlessIntegration(t)
	conn, err := net.DialTimeout("tcp", redisAddr, 5*time.Second)
	if err != nil {
		t.Fatalf("подключение к Redis: %v", err)
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
		t.Errorf("ответ Redis = %q, want +PONG", strings.TrimSpace(line))
	}
}

func TestChromium_CDP(t *testing.T) {
	skipUnlessIntegration(t)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(chromiumURL)
	if err != nil {
		t.Fatalf("CDP-эндпоинт chromium недоступен: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != 200 || !strings.Contains(string(body), "Browser") {
		t.Errorf("ответ /json/version: %d %s", resp.StatusCode, string(body))
	}
}

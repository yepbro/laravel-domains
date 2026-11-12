// mockserver — a Go application of the test bench for the scenarios that
// cannot be described by statics: tarpit, slow responses, huge bodies,
// a zip bomb, a connection drop, infinite/cyclic redirects, crooked TLS
// certificates (spec 4.4).
//
// It listens on :80 (HTTP), :443 (TLS with self-made certificates by SNI)
// and :8080 (healthcheck/metrics).
package main

import (
	"compress/gzip"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// hostRoutes — the virtual hosts of the scenarios from scenarios.yaml
// (the scenarios with site: {template: mock, mock_path: …}): a "/" request
// by the scenario Host is routed to its handler, as a dedicated scenario
// server would do — the HTTP worker requests only the main page "/"
// (spec 5.5, §9), and the "nasty" response must arrive exactly there.
// The direct paths (env_test.go, manual debugging) work as before. The
// map being in sync with scenarios.yaml is a testbed contract (changing
// the set of mock scenarios — editing the map in the same change).
var hostRoutes = map[string]string{
	"tarpit.crawl.test":               "/tarpit",
	"slow-headers.crawl.test":         "/slow?delay=30s",
	"huge-body.crawl.test":            "/huge",
	"zip-bomb.crawl.test":             "/zipbomb",
	"infinite-redirect.crawl.test":    "/redirect/infinite",
	"cycle-redirect.crawl.test":       "/redirect/cycle/a",
	"truncated.crawl.test":            "/truncate",
	"garbage-content.crawl.test":      "/garbage",
	"bad-charset-declared.crawl.test": "/bad-charset/declared",
	"bad-charset-wrong.crawl.test":    "/bad-charset/wrong",
	"cookies.crawl.test":              "/cookies",
	"tls-self-signed.crawl.test":      "/host",
	"tls-expired.crawl.test":          "/host",
	"tls-wrong-san.crawl.test":        "/host",
}

// routeByHost serves the virtual hosts of the scenarios: "/" by a Host
// from hostRoutes is rewritten to the target path (with a query, if set)
// and passed on to the mux. The other requests — unchanged. The routing
// is checked by the integration tests of the HTTP stage
// (test/integration/http_test.go, T-060): every "nasty" scenario must
// arrive at the worker on "/".
func routeByHost(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			host := strings.ToLower(r.Host)
			if h, _, err := net.SplitHostPort(host); err == nil {
				host = h
			}
			if target, ok := hostRoutes[host]; ok {
				rr := r.Clone(r.Context())
				if i := strings.IndexByte(target, '?'); i >= 0 {
					rr.URL.Path, rr.URL.RawQuery = target[:i], target[i+1:]
				} else {
					rr.URL.Path = target
				}
				r = rr
			}
		}
		next.ServeHTTP(w, r)
	})
}

func main() {
	certs, err := buildCerts()
	if err != nil {
		log.Fatalf("mockserver: генерация сертификатов: %v", err)
	}

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("ok\n"))
		})
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = fmt.Fprint(w, "# HELP mockserver_up Сервер запущен.\n# TYPE mockserver_up gauge\nmockserver_up 1\n")
		})
		srv := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		if err := srv.ListenAndServe(); err != nil {
			log.Fatalf("mockserver: health: %v", err)
		}
	}()

	handler := http.NewServeMux()
	registerRoutes(handler)
	routed := routeByHost(handler)

	httpSrv := &http.Server{Addr: ":80", Handler: routed, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Println("mockserver: HTTP на :80")
		if err := httpSrv.ListenAndServe(); err != nil {
			log.Fatalf("mockserver: http: %v", err)
		}
	}()

	tlsCfg := &tls.Config{
		GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
			name := hello.ServerName
			if c, ok := certs[name]; ok {
				return c, nil
			}
			return certs["*"], nil
		},
	}
	tlsSrv := &http.Server{Addr: ":443", Handler: routed, ReadHeaderTimeout: 10 * time.Second, TLSConfig: tlsCfg}
	log.Println("mockserver: TLS на :443")
	if err := tlsSrv.ListenAndServeTLS("", ""); err != nil {
		log.Fatalf("mockserver: tls: %v", err)
	}
}

// The parameter bounds of the "nasty" handlers. The direct paths are
// reachable from the common bench network (172.28.100.0/24): one request
// must not order a gigabyte allocation (?rate=) or hold a goroutine for
// days (?delay=). The normal scenarios have plenty with values far below
// (512 bytes/s, 30 s).
const (
	// tarpitMaxRate — the upper bar of the tarpit rate, bytes/s.
	tarpitMaxRate = 1 << 20
	// slowMaxDelay — the upper bar of the /slow delay.
	slowMaxDelay = 10 * time.Minute
)

// tarpitRate — the tarpit rate from ?rate=: by default 512 bytes/s (below
// the minimum rate of 2 KB/s from the main spec), capped at the top by
// tarpitMaxRate — the chunk buffer is allocated by the value from the
// request.
func tarpitRate(raw string) int {
	rate, _ := strconv.Atoi(raw)
	if rate <= 0 {
		return 512
	}
	if rate > tarpitMaxRate {
		return tarpitMaxRate
	}
	return rate
}

// slowDelay — the /slow delay from ?delay=: by default 30 s, capped at
// the top by slowMaxDelay.
func slowDelay(raw string) time.Duration {
	delay, _ := time.ParseDuration(raw)
	if delay <= 0 {
		return 30 * time.Second
	}
	if delay > slowMaxDelay {
		return slowMaxDelay
	}
	return delay
}

func registerRoutes(mux *http.ServeMux) {
	// Slow response: waits for the delay (taking the client disconnect
	// into account), then responds (a header timeout check).
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		delay := slowDelay(r.URL.Query().Get("delay"))
		select {
		case <-time.After(delay):
		case <-r.Context().Done(): // the client is gone — we do not hold the goroutine
			return
		}
		writePage(w, "slow", "наконец-то")
	})

	// Tarpit: serves the body at the rate bytes/s, endlessly.
	mux.HandleFunc("/tarpit", func(w http.ResponseWriter, r *http.Request) {
		rate := tarpitRate(r.URL.Query().Get("rate"))
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Content-Length", "1073741824")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		chunk := make([]byte, rate)
		for i := range chunk {
			chunk[i] = 'x'
		}
		for {
			if _, err := w.Write(chunk); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			select {
			case <-r.Context().Done():
				return
			case <-time.After(time.Second):
			}
		}
	})

	// Huge body: a gigabyte is declared, we pour zeros (the client must
	// truncate by the limit).
	mux.HandleFunc("/huge", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", "1073741824")
		w.WriteHeader(http.StatusOK)
		buf := make([]byte, 64*1024)
		for {
			if _, err := w.Write(buf); err != nil {
				return
			}
		}
	})

	// Zip bomb: a gzip stream of zeros (the decompression must be bounded
	// by the limit).
	mux.HandleFunc("/zipbomb", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusOK)
		zw := gzip.NewWriter(w)
		buf := make([]byte, 64*1024)
		if _, err := zw.Write(buf); err != nil {
			return
		}
		for {
			if _, err := zw.Write(buf); err != nil {
				return
			}
		}
	})

	// Infinite redirect: every response — a Location to itself.
	mux.HandleFunc("/redirect/infinite", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Query().Get("n"))
		n++
		w.Header().Set("Location", fmt.Sprintf("/redirect/infinite?n=%d", n))
		w.WriteHeader(http.StatusFound)
	})

	// Cyclic redirect: /a -> /b -> /a.
	mux.HandleFunc("/redirect/cycle/", func(w http.ResponseWriter, r *http.Request) {
		next := map[string]string{"/redirect/cycle/a": "/redirect/cycle/b", "/redirect/cycle/b": "/redirect/cycle/a"}
		if loc, ok := next[r.URL.Path]; ok {
			w.Header().Set("Location", loc)
			w.WriteHeader(http.StatusFound)
			return
		}
		http.NotFound(w, r)
	})

	// Connection drop in the middle of the body.
	mux.HandleFunc("/truncate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Content-Length", "1048576")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<html><body>тут будет очень много контент"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		panic(http.ErrAbortHandler)
	})

	// Junk bytes with text/html.
	mux.HandleFunc("/garbage", func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 64*1024)
		_, _ = rand.Read(buf)
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(buf)
	})

	// Encodings: a correct 1251 one and a "lying" meta charset.
	mux.HandleFunc("/bad-charset/", func(w http.ResponseWriter, r *http.Request) {
		// "Hello, world!" in windows-1251
		body := []byte{
			'<', 'h', 't', 'm', 'l', '>', 0xcf, 0xf0, 0xe8, 0xe2, 0xe5, 0xf2,
			',', ' ', 0xec, 0xe8, 0xf0, '!', '<', '/', 'h', 't', 'm', 'l', '>',
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/declared"):
			w.Header().Set("Content-Type", "text/html; charset=windows-1251")
		case strings.HasSuffix(r.URL.Path, "/wrong"):
			w.Header().Set("Content-Type", "text/html; charset=utf-8") // mismatches
		default:
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})

	// Cookies, like Laravel's (XSRF-TOKEN + *_session with an encrypted
	// payload).
	mux.HandleFunc("/cookies", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Set-Cookie", "XSRF-TOKEN=eyJpdiI6ZmFrZVRva2VuOjE2Oml7InZhbCI6IjEyMzQ1Njc4OTAiLCJpdiI6ImFiY2RlZmFhYmNkZWYiLCJ0YWciOiIifQ==; expires=Wed, 21 Oct 2026 07:28:00 GMT; path=/; secure")
		w.Header().Add("Set-Cookie", "laravel_session=eyJpdiI6c2Vzc2lvbjoxNjppeyJpc3MiOiJodHRwczovL2V4YW1wbGUifQ==; path=/; httponly; samesite=lax")
		writePage(w, "cookies", "страница с куками Laravel")
	})

	// Echo of the Host header — useful when debugging proxies/virtual
	// hosts.
	mux.HandleFunc("/host", func(w http.ResponseWriter, r *http.Request) {
		writePage(w, "host", "Host: "+r.Host)
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			writePage(w, "mockserver", "запросы уходят на /tarpit, /huge, /zipbomb, ...")
			return
		}
		http.NotFound(w, r)
	})
}

func writePage(w http.ResponseWriter, title, body string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, "<!DOCTYPE html><html><head><title>%s</title></head><body><h1>%s</h1><p>%s</p></body></html>\n", title, title, body)
}

// buildCerts generates the self-made certificates for the TLS scenarios:
// a valid self-signed one, an expired one and one with a foreign SAN.
// The scenario names (scenarios.yaml: tls-self-signed/tls-expired/
// tls-wrong-san) and the bad-tls debug names (env_test.go) get their own
// certificates by SNI.
func buildCerts() (map[string]*tls.Certificate, error) {
	mk := func(cn string, sans []string, notAfter time.Time) (*tls.Certificate, error) {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return nil, err
		}
		tmpl := &x509.Certificate{
			SerialNumber:          big.NewInt(time.Now().UnixNano()),
			Subject:               pkix.Name{CommonName: cn},
			NotBefore:             time.Now().AddDate(-1, 0, 0),
			NotAfter:              notAfter,
			KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
			ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			BasicConstraintsValid: true,
			IsCA:                  true,
			DNSNames:              sans,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		if err != nil {
			return nil, err
		}
		return &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
	}

	out := map[string]*tls.Certificate{}
	far := time.Now().AddDate(10, 0, 0)
	past := time.Now().AddDate(-2, 0, 0)

	// A valid self-signed one: the "*" key — the default SNI.
	name := "self-signed.bad-tls.crawl.test"
	c, err := mk(name, []string{name}, far)
	if err != nil {
		return nil, err
	}
	out[name] = c
	out["*"] = c

	// An expired one.
	name = "expired.bad-tls.crawl.test"
	c, err = mk(name, []string{name}, past)
	if err != nil {
		return nil, err
	}
	out[name] = c

	// A foreign SAN: the certificate is issued for another host.
	c, err = mk("other-host.test", []string{"other-host.test", "www.other-host.test"}, far)
	if err != nil {
		return nil, err
	}
	out["wrong-san.bad-tls.crawl.test"] = c

	// The scenario domains of the testbed (scenarios.yaml): each gets its
	// own "nasty" property — self-signedness, expiry, a foreign SAN.
	scenarioCerts := []struct {
		host string
		cn   string
		sans []string
		end  time.Time
	}{
		{"tls-self-signed.crawl.test", "tls-self-signed.crawl.test", []string{"tls-self-signed.crawl.test"}, far},
		{"tls-expired.crawl.test", "tls-expired.crawl.test", []string{"tls-expired.crawl.test"}, past},
		{"tls-wrong-san.crawl.test", "other-host.test", []string{"other-host.test", "www.other-host.test"}, far},
	}
	for _, sc := range scenarioCerts {
		c, err := mk(sc.cn, sc.sans, sc.end)
		if err != nil {
			return nil, err
		}
		out[sc.host] = c
	}

	return out, nil
}

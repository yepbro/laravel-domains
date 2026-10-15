// mockserver — Go-приложение тестового стенда для сценариев, которые нельзя
// описать статикой: тарпит, медленные ответы, огромные тела, zip-бомба,
// обрыв соединения, бесконечные/циклические редиректы, кривые TLS-сертификаты
// (ТЗ 4.4).
//
// Слушает :80 (HTTP), :443 (TLS с самодельными сертификатами по SNI) и
// :8080 (healthcheck/метрики).
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
	"net/http"
	"strconv"
	"strings"
	"time"
)

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

	httpSrv := &http.Server{Addr: ":80", Handler: handler, ReadHeaderTimeout: 10 * time.Second}
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
	tlsSrv := &http.Server{Addr: ":443", Handler: handler, ReadHeaderTimeout: 10 * time.Second, TLSConfig: tlsCfg}
	log.Println("mockserver: TLS на :443")
	if err := tlsSrv.ListenAndServeTLS("", ""); err != nil {
		log.Fatalf("mockserver: tls: %v", err)
	}
}

func registerRoutes(mux *http.ServeMux) {
	// Медленный ответ: ждёт delay, потом отвечает (проверка header timeout).
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		delay, _ := time.ParseDuration(r.URL.Query().Get("delay"))
		if delay <= 0 {
			delay = 30 * time.Second
		}
		time.Sleep(delay)
		writePage(w, "slow", "наконец-то")
	})

	// Тарпит: отдаёт тело со скоростью rate байт/с, бесконечно.
	mux.HandleFunc("/tarpit", func(w http.ResponseWriter, r *http.Request) {
		rate, _ := strconv.Atoi(r.URL.Query().Get("rate"))
		if rate <= 0 {
			rate = 512 // ниже минимальной скорости 2 КБ/с из основного ТЗ
		}
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

	// Огромное тело: заявлен гигабайт, льём нули (клиент обязан обрезать по лимиту).
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

	// Zip-бомба: gzip-поток нулей (распаковка обязана ограничиваться лимитом).
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

	// Бесконечный редирект: каждый ответ — Location на самого себя.
	mux.HandleFunc("/redirect/infinite", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Query().Get("n"))
		n++
		w.Header().Set("Location", fmt.Sprintf("/redirect/infinite?n=%d", n))
		w.WriteHeader(http.StatusFound)
	})

	// Циклический редирект: /a -> /b -> /a.
	mux.HandleFunc("/redirect/cycle/", func(w http.ResponseWriter, r *http.Request) {
		next := map[string]string{"/redirect/cycle/a": "/redirect/cycle/b", "/redirect/cycle/b": "/redirect/cycle/a"}
		if loc, ok := next[r.URL.Path]; ok {
			w.Header().Set("Location", loc)
			w.WriteHeader(http.StatusFound)
			return
		}
		http.NotFound(w, r)
	})

	// Обрыв соединения посреди тела.
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

	// Мусорные байты с text/html.
	mux.HandleFunc("/garbage", func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 64*1024)
		_, _ = rand.Read(buf)
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(buf)
	})

	// Кодировки: корректная 1251 и «соврущая» meta charset.
	mux.HandleFunc("/bad-charset/", func(w http.ResponseWriter, r *http.Request) {
		// «Привет, мир!» в windows-1251
		body := []byte{
			'<', 'h', 't', 'm', 'l', '>', 0xcf, 0xf0, 0xe8, 0xe2, 0xe5, 0xf2,
			',', ' ', 0xec, 0xe8, 0xf0, '!', '<', '/', 'h', 't', 'm', 'l', '>',
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/declared"):
			w.Header().Set("Content-Type", "text/html; charset=windows-1251")
		case strings.HasSuffix(r.URL.Path, "/wrong"):
			w.Header().Set("Content-Type", "text/html; charset=utf-8") // соврамши
		default:
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})

	// Cookies, как у Laravel (XSRF-TOKEN + *_session с зашифрованным payload).
	mux.HandleFunc("/cookies", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Set-Cookie", "XSRF-TOKEN=eyJpdiI6ZmFrZVRva2VuOjE2Oml7InZhbCI6IjEyMzQ1Njc4OTAiLCJpdiI6ImFiY2RlZmFhYmNkZWYiLCJ0YWciOiIifQ==; expires=Wed, 21 Oct 2026 07:28:00 GMT; path=/; secure")
		w.Header().Add("Set-Cookie", "laravel_session=eyJpdiI6c2Vzc2lvbjoxNjppeyJpc3MiOiJodHRwczovL2V4YW1wbGUifQ==; path=/; httponly; samesite=lax")
		writePage(w, "cookies", "страница с куками Laravel")
	})

	// Эхо Host-заголовка — полезно при отладке прокси/виртуальных хостов.
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

// buildCerts генерирует самодельные сертификаты для TLS-сценариев:
// самоподписанный валидный, просроченный и с чужим SAN.
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

	c, err := mk("self-signed.bad-tls.crawl.test", []string{"self-signed.bad-tls.crawl.test"}, far)
	if err != nil {
		return nil, err
	}
	out["self-signed.bad-tls.crawl.test"] = c
	out["*"] = c // SNI по умолчанию

	c, err = mk("expired.bad-tls.crawl.test", []string{"expired.bad-tls.crawl.test"}, past)
	if err != nil {
		return nil, err
	}
	out["expired.bad-tls.crawl.test"] = c

	c, err = mk("other-host.test", []string{"other-host.test", "www.other-host.test"}, far)
	if err != nil {
		return nil, err
	}
	out["wrong-san.bad-tls.crawl.test"] = c

	return out, nil
}

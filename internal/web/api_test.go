// Юнит-тесты формы ручной проверки (T-123): таблица валидатора ввода
// (URL/IDN/мусор — по образцу CheckDomainRequest larasites), рейт-лимит
// token bucket (4/мин, 429 + Retry-After, восстановление), e2e handler
// с фейковым чекером (шов liveCheck): JSON-ответ, stored-секция,
// запись web_checks, ошибки валидации 422.
package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"crawler/internal/checker"
	"crawler/internal/config"
	"crawler/internal/rules"
)

// TestValidateCheckInput — таблица валидатора: trim, точка, схема+порт,
// IDN, мусор.
func TestValidateCheckInput(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"example.com", "example.com", false},
		{"  Example.COM.  ", "example.com", false},
		{"https://example.com/page?q=1", "example.com", false},
		{"http://example.com:8080/", "example.com", false},
		{"домен.рф", "xn--d1acufc.xn--p1ai", false},
		{"", "", true},
		{"   ", "", true},
		{"not a domain", "", true},
		{"https://", "", true},
		{"user@example.com", "", true},
		{"-", "", true},
		{"single", "", true},
	}
	for _, tc := range cases {
		got, err := validateCheckInput(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("validate(%q) = %q, хочу ошибку", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("validate(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("validate(%q) = %q, хочу %q", tc.in, got, tc.want)
		}
	}
}

// TestRateLimit — 4 запроса проходят, пятый — отказ; окно восстанавливает
// один токен за четверть минуты.
func TestRateLimit(t *testing.T) {
	l := newLimiter(4, time.Minute)
	now := time.Now()
	for i := 0; i < 4; i++ {
		if !l.allow("1.2.3.4", now) {
			t.Fatalf("запрос %d отклонён, хочу пропуск (лимит 4)", i+1)
		}
	}
	if l.allow("1.2.3.4", now) {
		t.Fatal("пятый запрос в минуту прошёл, хочу отказ")
	}
	if l.allow("5.6.7.8", now) {
		// Другой IP — свой бакет.
	} else {
		t.Fatal("чужой IP отклонён общим бакетом")
	}
	if !l.allow("1.2.3.4", now.Add(16*time.Second)) {
		t.Fatal("через четверть минуты бакет не восстановил один токен")
	}
}

// TestClientIP — адрес посетителя за reverse-proxy (T-124): X-Real-IP
// доверяется только от loopback-пира (все соединения nginx) и только
// валидному IP; снаружи и с чужого адреса заголовок игнорируется —
// подделка не расходует чужой бакет рейт-лимита.
func TestClientIP(t *testing.T) {
	req := func(remote, real string) *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/api/check-domain", nil)
		r.RemoteAddr = remote
		if real != "" {
			r.Header.Set("X-Real-IP", real)
		}
		return r
	}
	for _, tt := range []struct {
		name           string
		remote, realIP string
		want           string
	}{
		{"прямой адрес без заголовка", "203.0.113.7:44321", "", "203.0.113.7"},
		{"nginx loopback + X-Real-IP", "127.0.0.1:51234", "198.51.100.9", "198.51.100.9"},
		{"nginx ipv6-loopback + X-Real-IP", "[::1]:51234", "198.51.100.9", "198.51.100.9"},
		{"loopback + мусорный заголовок", "127.0.0.1:51234", "not-an-ip", "127.0.0.1"},
		{"loopback + заголовок с портом", "127.0.0.1:51234", "198.51.100.9:443", "127.0.0.1"},
		{"чужой адрес + подделка заголовка", "203.0.113.7:44321", "198.51.100.9", "203.0.113.7"},
	} {
		if got := clientIP(req(tt.remote, tt.realIP)); got != tt.want {
			t.Errorf("%s: clientIP = %q, хочу %q", tt.name, got, tt.want)
		}
	}
}

// TestCheckDomainE2E — фейковый чекер: 200 + JSON с found/score/записями,
// запись web_checks, повторные запросы в пределах лимита.
func TestCheckDomainE2E(t *testing.T) {
	db := &fakeDB{seed: seedLanding()}
	db.seed.domainInfo = []byte(`{"domain":{"status":0,"status_name":"new","last_checked_at":"2026-10-18"},
		"laravel":{"score":0.71,"ver_min":"9.19","ver_max":"11"}}`)
	cfg := config.Default()
	cfg.Web.RateLimit = 10
	srv, err := New(db, cfg, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	score := 0.7231
	srv.liveCheck = func(context.Context, *config.Config, checker.Pool, string, checker.Logger) (*checker.Result, error) {
		return &checker.Result{
			Domain: "example.com", Zone: "com", Apex: "delegated",
			Found: true, Score: score, VerMin: "9.19", VerMax: "12",
			Records: []checker.SubRecord{{
				Label: "www", Host: "www.example.com", Variant: "main", Scheme: "https",
				Status: "200", FinalURL: "https://example.com/",
				Signals: []rules.Evidence{{Rule: "cookie_xsrf_token", Weight: 0.25, Evidence: "XSRF-TOKEN"}},
			}},
		}, nil
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/api/check-domain", "application/json",
		strings.NewReader(`{"domain":"Example.COM."}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("статус = %d, хочу 200", resp.StatusCode)
	}
	body := make([]byte, 1<<16)
	n, _ := resp.Body.Read(body)
	got := string(body[:n])
	for _, want := range []string{
		`"found":true`, `"score":0.723`, `"ver_min":"9.19"`, `"ver_max":"12"`,
		`"subdomain":"www"`, `"final_url":"https://example.com/"`,
		`"rule":"cookie_xsrf_token"`, `"stored"`, `"status":"new"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("ответ не содержит %s:\n%s", want, got)
		}
	}
	// web_checks записан: в фейке нет семантики INSERT — фиксируем
	// обращение (контракт появляется интеграцией; здесь шов).
	if len(db.queries) == 0 {
		t.Error("к БД не было обращений (stored/web_checks)")
	}
}

// TestCheckDomainInvalid — мусорный ввод: 422 в формате landing.js.
func TestCheckDomainInvalid(t *testing.T) {
	srv := newTestServer(t, &fakeDB{seed: seedLanding()})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/api/check-domain", "application/json",
		strings.NewReader(`{"domain":"not a domain"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("статус = %d, хочу 422", resp.StatusCode)
	}
	body := make([]byte, 256)
	n, _ := resp.Body.Read(body)
	if !strings.Contains(string(body[:n]), `"invalid"`) {
		t.Errorf("тело 422 не в формате формы: %s", string(body[:n]))
	}
}

// TestCheckDomainRateLimited — 429 с Retry-After.
func TestCheckDomainRateLimited(t *testing.T) {
	db := &fakeDB{seed: seedLanding()}
	cfg := config.Default()
	cfg.Web.RateLimit = 1
	srv, err := New(db, cfg, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv.liveCheck = func(context.Context, *config.Config, checker.Pool, string, checker.Logger) (*checker.Result, error) {
		return &checker.Result{Domain: "example.com"}, nil
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	post := func() *http.Response {
		resp, err := http.Post(ts.URL+"/api/check-domain", "application/json",
			strings.NewReader(`{"domain":"example.com"}`))
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	first := post()
	defer func() { _ = first.Body.Close() }()
	if first.StatusCode != http.StatusOK {
		t.Fatalf("первый запрос = %d, хочу 200", first.StatusCode)
	}
	second := post()
	defer func() { _ = second.Body.Close() }()
	if second.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("второй запрос = %d, хочу 429 (лимит 1)", second.StatusCode)
	}
	if ra := second.Header.Get("Retry-After"); ra == "" {
		t.Error("429 без Retry-After")
	}
}

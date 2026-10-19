// Юнит-тесты каркаса и данных публичного лендинга (T-121/T-122):
// маршрутизация, healthz, рендер ru/en на фейковом пуле с живой
// семантикой SQL данных, SQL-контракты, graceful stop — httptest без
// PostgreSQL.
package web

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"crawler/internal/config"
)

// --- фейковый пул ------------------------------------------------------------

// webSeed — данные лендинга в фейке.
type webSeed struct {
	zones      int64
	total      int64
	checked    int64
	laravel    int64
	last24h    int64
	cloud      [][]any // (name, unicode, laravel)
	table      [][]any // (name, unicode, checked, laravel)
	latest     [][]any // (name, mask, ver_min, ver_max, updated)
	subdomains [][]any // (id, label)
	domainInfo []byte  // ответ domain_info(text) или nil (домен неизвестен)
}

// fakeDB — Pinger + Querier с живой семантикой SQL данных лендинга.
type fakeDB struct {
	seed webSeed
	ping error

	mu      sync.Mutex
	queries []string
}

func (d *fakeDB) Ping(context.Context) error { return d.ping }

func (d *fakeDB) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("CopyFrom не должен вызываться")
}

func (d *fakeDB) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, fmt.Errorf("fakeDB: неожидаемый Exec: %s", sql)
}

func (d *fakeDB) Query(ctx context.Context, sql string, _ ...any) (pgx.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.queries = append(d.queries, sql)
	switch sql {
	case statsSQL:
		return webRowsOf([][]any{{d.seed.zones, d.seed.total, d.seed.checked, d.seed.laravel}}), nil
	case last24hSQL:
		return webRowsOf([][]any{{d.seed.last24h}}), nil
	case cloudSQL:
		return webRowsOf(d.seed.cloud), nil
	case tableSQL:
		return webRowsOf(d.seed.table), nil
	case latestSQL:
		return webRowsOf(d.seed.latest), nil
	case subdomainsSQL:
		return webRowsOf(d.seed.subdomains), nil
	case "SELECT domain_info($1::text)":
		if d.seed.domainInfo == nil {
			return webRowsOf(nil), nil // NULL — домен неизвестен
		}
		return webRowsOf([][]any{{d.seed.domainInfo}}), nil
	default:
		return nil, fmt.Errorf("fakeDB: неожидаемый SQL: %s", sql)
	}
}

// webRows — pgx.Rows с заготовленными строками (позиционный Scan по
// типам значений, стиль anAggRows).
type webRows struct {
	rows [][]any
	i    int
}

func webRowsOf(rows [][]any) *webRows { return &webRows{rows: rows} }

func (r *webRows) Close()                        {}
func (r *webRows) Err() error                    { return nil }
func (r *webRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (r *webRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *webRows) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *webRows) Scan(dest ...any) error {
	if r.i == 0 || r.i > len(r.rows) || len(dest) != len(r.rows[r.i-1]) {
		return fmt.Errorf("webRows: Scan без Next или арность %d ≠ колонок", len(dest))
	}
	row := r.rows[r.i-1]
	for i, d := range dest {
		switch dst := d.(type) {
		case *int64:
			*dst = row[i].(int64)
		case *int16:
			*dst = row[i].(int16)
		case *string:
			*dst = row[i].(string)
		case *time.Time:
			*dst = row[i].(time.Time)
		case *[]byte:
			*dst = row[i].([]byte)
		default:
			return fmt.Errorf("webRows: приёмник[%d] %T", i, d)
		}
	}
	return nil
}

func (r *webRows) Values() ([]any, error) { return r.rows[r.i-1], nil }
func (r *webRows) RawValues() [][]byte    { return nil }
func (r *webRows) Conn() *pgx.Conn        { return nil }
func (r *webRows) TypeMap() *pgtype.Map   { return nil }

// seedLanding — типовые данные страницы (зоны/находки).
func seedLanding() webSeed {
	return webSeed{
		zones: 12, total: 1_000_000, checked: 800_000, laravel: 4_242, last24h: 17,
		cloud: [][]any{
			{"com", "", int64(2000)},
			{"de", "", int64(300)},
			{"рф-зона", "xn--", int64(40)},
		},
		table: [][]any{
			{"com", "", int64(800_000), int64(2000)},
			{"de", "", int64(90_000), int64(300)},
		},
		latest: [][]any{
			{"shop.example.com", int64(0b11), "9.19", "12", time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)},
		},
		subdomains: [][]any{{int16(0), ""}, {int16(1), "www"}, {int16(4), "api"}},
	}
}

func newTestServer(t *testing.T, db DB) *Server {
	t.Helper()
	cfg := config.Default()
	cfg.Web.RateLimit = 100 // тесты маршрутизации не упираются в лимит
	s, err := New(db, cfg, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// --- каркас -------------------------------------------------------------------

// TestNewNilDB — без БД сервис не собирается.
func TestNewNilDB(t *testing.T) {
	cfg := config.Default()
	if _, err := New(nil, cfg, Options{}); err == nil {
		t.Fatal("New(nil) = nil error, хочу ошибку")
	}
	if _, err := New(&fakeDB{}, nil, Options{}); err == nil {
		t.Fatal("New(nil cfg) = nil error, хочу ошибку")
	}
}

// TestRoutingLanding — лендинг локалей рендерится с данными, корень
// редиректит, чужой префикс — 404, статика отдаётся.
func TestRoutingLanding(t *testing.T) {
	srv := newTestServer(t, &fakeDB{seed: seedLanding()})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	for locale, want := range map[string][]string{
		"ru": {"Мы находим Laravel", "Статистика", `data-count="4242"`, "shop.example.com", "Сканируем миллионы"},
		"en": {"We find Laravel", "Statistics", `data-count="4242"`, "shop.example.com"},
	} {
		resp, err := http.Get(ts.URL + "/" + locale)
		if err != nil {
			t.Fatalf("/%s: %v", locale, err)
		}
		body := make([]byte, 1<<20)
		n, _ := resp.Body.Read(body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("/%s: статус = %d", locale, resp.StatusCode)
			continue
		}
		for _, w := range want {
			if !strings.Contains(string(body[:n]), w) {
				t.Errorf("/%s: тело не содержит %q", locale, w)
			}
		}
	}

	// Чужая локаль — 404; статика — 200 с типом; корень — Location.
	resp, _ := http.Get(ts.URL + "/de")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("/de = %d, хочу 404", resp.StatusCode)
	}
	_ = resp.Body.Close()

	resp, err := http.Get(ts.URL + "/static/favicon.svg")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("favicon = %d, хочу 200 (go:embed)", resp.StatusCode)
	}

	req, _ := http.NewRequest("GET", ts.URL+"/", nil)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if loc := resp.Header.Get("Location"); loc != "/"+DefaultLocale {
		t.Errorf("Location корня = %q, хочу /%s", loc, DefaultLocale)
	}
}

// TestLandingDBError — сбой данных: 500 без внутренних деталей.
func TestLandingDBError(t *testing.T) {
	srv := newTestServer(t, brokenDB{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/ru")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("сбой БД: статус = %d, хочу 500", resp.StatusCode)
	}
}

// brokenDB — Querier, отказывающий на любой запрос.
type brokenDB struct{}

func (brokenDB) Ping(context.Context) error { return nil }
func (brokenDB) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("no")
}
func (brokenDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("no")
}
func (brokenDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("бд недоступна")
}

// TestHealthzDB — недоступная БД: 503.
func TestHealthzDB(t *testing.T) {
	srv := newTestServer(t, &fakeDB{ping: errors.New("connection refused")})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("healthz = %d, хочу 503", resp.StatusCode)
	}
}

// TestMethodNotAllowed — не-GET на страницах и не-POST на API — 405.
func TestMethodNotAllowed(t *testing.T) {
	srv := newTestServer(t, &fakeDB{seed: seedLanding()})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	for _, tc := range []struct{ method, path string }{
		{"POST", "/ru"},
		{"PUT", "/"},
		{"GET", "/api/check-domain"},
	} {
		req, _ := http.NewRequest(tc.method, ts.URL+tc.path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", tc.method, tc.path, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s %s: статус = %d, хочу 405", tc.method, tc.path, resp.StatusCode)
		}
	}
}

// TestRunGracefulStop — Run слушает, отменённый ctx останавливает
// сервер чистым nil в окне остановки.
func TestRunGracefulStop(t *testing.T) {
	srv := newTestServer(t, &fakeDB{seed: seedLanding()})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx, addr) }()

	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("сервер не поднялся за 2 с")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run после отмены: %v, хочу nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("сервер не остановился после отмены ctx")
	}
}

// TestRunUnixSocket — listen абсолютным путём = unix-сокет (вариант
// деплоя за nginx без TCP-порта, T-124): сервер отвечает, чистая
// остановка убирает файл сокета. Осиротевший файл (крах прошлой серии —
// SIGKILL, defer не отработал) снимается до Listen: иначе рестарт по
// Restart=on-failure циклился бы на address already in use.
func TestRunUnixSocket(t *testing.T) {
	srv := newTestServer(t, &fakeDB{seed: seedLanding()})
	sock := filepath.Join(t.TempDir(), "web.sock")

	// «Осиротевший» сокет прошлой серии: обычный файл на пути Listen.
	if err := os.WriteFile(sock, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx, sock) }()

	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", sock)
		},
	}}
	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err := client.Get("http://unix/healthz")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("unix-сокет не поднялся за 2 с")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run(unix) после отмены: %v, хочу nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("unix-сокет не остановился после отмены ctx")
	}
	if _, err := os.Stat(sock); !os.IsNotExist(err) {
		t.Errorf("файл сокета %s не убран после остановки (stat err: %v)", sock, err)
	}
}

// TestRobotsSitemap — служебные файлы поисковых роботов у корня (T-124):
// robots.txt — статика из embed; sitemap.xml — минимальный набор
// локалей с абсолютным URL из запроса (схему за reverse-proxy передаёт
// X-Forwarded-Proto); POST на маршруты — 405.
func TestRobotsSitemap(t *testing.T) {
	srv := newTestServer(t, &fakeDB{seed: seedLanding()})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/robots.txt")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("robots.txt = %d, хочу 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Errorf("robots.txt Content-Type = %q, хочу text/plain", ct)
	}
	for _, want := range []string{"User-agent: *", "Disallow: /api/"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("robots.txt не содержит %q", want)
		}
	}

	// sitemap: URL обеих локалей с хостом запроса; ничего лишнего.
	resp, err = http.Get(ts.URL + "/sitemap.xml")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("sitemap.xml = %d, хочу 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/xml; charset=utf-8" {
		t.Errorf("sitemap.xml Content-Type = %q, хочу application/xml", ct)
	}
	s := string(body)
	if !strings.Contains(s, `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`) {
		t.Errorf("sitemap.xml без схемы sitemap 0.9:\n%s", s)
	}
	for _, locale := range Locales {
		if !strings.Contains(s, "<loc>"+ts.URL+"/"+locale+"</loc>") {
			t.Errorf("sitemap.xml без URL локали /%s:\n%s", locale, s)
		}
	}
	if n := strings.Count(s, "<loc>"); n != len(Locales) {
		t.Errorf("sitemap.xml содержит %d URL, хочу %d (только локали):\n%s", n, len(Locales), s)
	}

	// За reverse-proxy схему передаёт X-Forwarded-Proto (публичный вход
	// — TLS), Host берётся из запроса. Чужое значение заголовка
	// игнорируется (остаётся схема соединения).
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/sitemap.xml", nil)
	req.Host = "crawler.example.com"
	req.Header.Set("X-Forwarded-Proto", "https")
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("sitemap.xml за прокси = %d, хочу 200", rec.Code)
	}
	if want := "<loc>https://crawler.example.com/ru</loc>"; !strings.Contains(rec.Body.String(), want) {
		t.Errorf("sitemap.xml за прокси без %q:\n%s", want, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/sitemap.xml", nil)
	req.Host = "crawler.example.com"
	req.Header.Set("X-Forwarded-Proto", "javascript") // мусор — не web-схема
	srv.Handler().ServeHTTP(rec, req)
	if want := "<loc>http://crawler.example.com/ru</loc>"; !strings.Contains(rec.Body.String(), want) {
		t.Errorf("sitemap.xml с мусорной схемой без %q:\n%s", want, rec.Body.String())
	}

	// Маршруты GET-only: POST отдаётся mux-ом как 405.
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/robots.txt", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST /robots.txt = %d, хочу 405", resp.StatusCode)
	}
}

// Юнит-тесты каркаса дашборда (T-128): авторизация Basic + cookie-сессия
// (ТЗД §7), защита от подбора и её экспирация (§7.6), роуты/методы/
// заголовки (§6.1–6.3), служебные роуты без авторизации, стартовая
// проверка кредов (§7.7), graceful stop — httptest без PostgreSQL.
package dashboard

import (
	"bytes"
	"context"
	"encoding/base64"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"crawler/internal/config"
	"crawler/internal/dashboard/auth"
)

// testPassword — пароль оператора в тестах; testUser — его логин.
const (
	testUser     = "operator"
	testPassword = "secret-pass-123"
)

// fastHash — строка password_hash с дешёвой KDF (1000 итераций, стиль
// secret_test.go): тесты проверяют протокол авторизации, а не стоимость
// деривации (600 000 итераций ≈ полсекунды на сверку).
func fastHash(t *testing.T, password string) string {
	t.Helper()
	h, err := auth.Generate(password, 1000)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return h
}

// fakeClock — управляемые часы (cookie-сессии §7.5, окно подбора §7.6).
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

// fakePinger — readiness-зависимость /healthz.
type fakePinger struct{ err error }

func (p fakePinger) Ping(context.Context) error { return p.err }

// newTestServer — сервер с типовыми кредами; мутаторы настраивают
// Options (часы, логгер).
func newTestServer(t *testing.T, mutate func(*Options)) *Server {
	t.Helper()
	cfg := config.Default()
	cfg.Dashboard.Username = testUser
	cfg.Dashboard.PasswordHash = fastHash(t, testPassword)
	var opts Options
	if mutate != nil {
		mutate(&opts)
	}
	s, err := New(fakePinger{}, cfg, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// doReq — запрос через Handler с заданным RemoteAddr (per-IP защита
// от подбора различает клиентов по адресу) и заголовками.
func doReq(h http.Handler, method, target, remoteAddr string, header map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	req.RemoteAddr = remoteAddr
	for k, v := range header {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// basic — заголовок Authorization для пары кредов.
func basic(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

// authed — заголовок правильных кредов.
func authed() map[string]string {
	return map[string]string{"Authorization": basic(testUser, testPassword)}
}

// sessionCookie — cookie-сессии из успешного ответа (первая по имени).
func sessionCookie(t *testing.T, rec *httptest.ResponseRecorder) *http.Cookie {
	t.Helper()
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookieName {
			return c
		}
	}
	t.Fatalf("ответ без Set-Cookie %s: %v", sessionCookieName, rec.Result().Cookies())
	return nil
}

// --- стартовая проверка кредов (§7.7) -----------------------------------------

// TestNewCredentials — пустые username/password_hash и неразборимый
// хэш отказывают сборке сервера (вызов cmd/dashboard до service.Run
// использует ту же ParseCredentials — запуск падает до слушателей).
func TestNewCredentials(t *testing.T) {
	valid := fastHash(t, testPassword)
	for _, tc := range []struct {
		name string
		user string
		hash string
	}{
		{"пустое имя", "", valid},
		{"пустой хэш", testUser, ""},
		{"обе пустые", "", ""},
		{"имя с двоеточием", "op:erator", valid}, // Basic режет по первому «:» — вход был бы невозможен
		{"мусор вместо хэша", testUser, " plaintext "},
		{"хэш без схемы", testUser, "sha256$1000$c2FsdA==$a2V5"},
		{"хэш короткой солью", testUser, "pbkdf2-sha256$1000$c2FsdA==$MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="},
		{"хэш коротким ключом", testUser, "pbkdf2-sha256$1000$MDEyMzQ1Njc4OWFiY2RlZg==$a2V5"},
		{"хэш нулевыми итерациями", testUser, "pbkdf2-sha256$0$MDEyMzQ1Njc4OWFiY2RlZg==$MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="},
	} {
		cfg := config.Default()
		cfg.Dashboard.Username, cfg.Dashboard.PasswordHash = tc.user, tc.hash
		if _, err := New(fakePinger{}, cfg, Options{}); err == nil {
			t.Errorf("%s: New = nil error, хочу ошибку", tc.name)
		}
	}
	if _, err := New(nil, config.Default(), Options{}); err == nil {
		t.Error("New(nil db) = nil error, хочу ошибку")
	}
	if _, err := New(fakePinger{}, nil, Options{}); err == nil {
		t.Error("New(nil cfg) = nil error, хочу ошибку")
	}
	// Валидные креды собираются.
	cfg := config.Default()
	cfg.Dashboard.Username, cfg.Dashboard.PasswordHash = testUser, valid
	if _, err := New(fakePinger{}, cfg, Options{}); err != nil {
		t.Errorf("New(валидные креды): %v", err)
	}
}

// --- авторизация Basic (§7.4) --------------------------------------------------

// TestAuthRequired — защищённые роуты без кредов: 401 +
// WWW-Authenticate (вызов браузером формы пароля) на каждом.
func TestAuthRequired(t *testing.T) {
	h := newTestServer(t, nil).Handler()
	for _, path := range []string{"/", "/api/status", "/assets/app.js"} {
		rec := doReq(h, http.MethodGet, path, "10.0.0.1:1111", nil)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("GET %s без кредов = %d, хочу 401", path, rec.Code)
		}
		chal := rec.Header().Get("WWW-Authenticate")
		if !strings.HasPrefix(chal, `Basic realm="dashboard"`) {
			t.Errorf("GET %s: WWW-Authenticate = %q, хочу Basic realm=\"dashboard\"", path, chal)
		}
	}
}

// TestAuthWrongCredentials — неверное имя или пароль: 401; Warn пишется
// с IP, но без кредов (§9.2).
func TestAuthWrongCredentials(t *testing.T) {
	var buf bytes.Buffer
	srv := newTestServer(t, func(o *Options) {
		o.Log = slog.New(slog.NewTextHandler(&buf, nil))
	})
	h := srv.Handler()

	for _, tc := range []struct {
		name string
		user string
		pass string
	}{
		{"неверный пароль", testUser, "wrong"},
		{"неверное имя", "admin", testPassword},
		{"оба неверны", "admin", "wrong"},
		{"пустые", "", ""},
	} {
		rec := doReq(h, http.MethodGet, "/", "10.0.0.2:2222",
			map[string]string{"Authorization": basic(tc.user, tc.pass)})
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s: статус = %d, хочу 401", tc.name, rec.Code)
		}
		if rec.Header().Get("WWW-Authenticate") == "" {
			t.Errorf("%s: 401 без WWW-Authenticate", tc.name)
		}
	}

	logs := buf.String()
	if !strings.Contains(logs, "10.0.0.2") {
		t.Errorf("Warn без IP-адреса (§9.2):\n%s", logs)
	}
	for _, secret := range []string{testPassword, "wrong", "Basic ", srv.cfgHashFragment(t)} {
		if strings.Contains(logs, secret) {
			t.Errorf("лог содержит креды (%q), §9.2:\n%s", secret, logs)
		}
	}
}

// cfgHashFragment — узнаваемый кусок строки password_hash (соль в b64)
// для проверки «хэш не утекает в лог» (§9.2).
func (s *Server) cfgHashFragment(t *testing.T) string {
	t.Helper()
	if len(s.hash.Salt) < 8 {
		t.Fatalf("соль хэша короче 8 байт: %d", len(s.hash.Salt))
	}
	return base64.StdEncoding.EncodeToString(s.hash.Salt)
}

// TestAuthSuccess — правильные креды: 200 и cookie-сессия с атрибутами
// §7.5 (HttpOnly, SameSite=Lax, 24 ч).
func TestAuthSuccess(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)}
	h := newTestServer(t, func(o *Options) { o.Now = clock.Now }).Handler()

	rec := doReq(h, http.MethodGet, "/", "10.0.0.3:3333", authed())
	if rec.Code != http.StatusOK {
		t.Fatalf("правильные креды: статус = %d, хочу 200 (тело: %s)", rec.Code, rec.Body.String())
	}
	c := sessionCookie(t, rec)
	if !c.HttpOnly {
		t.Error("cookie без HttpOnly (§7.5)")
	}
	if c.SameSite != http.SameSiteLaxMode {
		t.Errorf("SameSite = %v, хочу Lax (§7.5)", c.SameSite)
	}
	if c.MaxAge != int(sessionTTL.Seconds()) {
		t.Errorf("MaxAge = %d, хочу %d (§7.5: 24 ч)", c.MaxAge, int(sessionTTL.Seconds()))
	}
	// Формат значения: <unix-expiry>.<hex HMAC-SHA256>.
	expiry, sig, ok := strings.Cut(c.Value, ".")
	if !ok || len(sig) != 64 {
		t.Fatalf("значение cookie %q: ожидался <expiry>.<hex-64>", c.Value)
	}
	if exp, err := strconv.ParseInt(expiry, 10, 64); err != nil || exp != clock.now.Add(sessionTTL).Unix() {
		t.Errorf("expiry %q: хочу unix-время now+24ч", expiry)
	}
}

// --- cookie-сессия (§7.5) -------------------------------------------------------

// TestSessionCookie — валидная cookie пропускает без Basic; тамперена
// (подпись или expiry) — 401 с вызовом пароля; просрочена — 401; смена
// password_hash (другой производный ключ) инвалидирует сессии.
func TestSessionCookie(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)}
	srv := newTestServer(t, func(o *Options) { o.Now = clock.Now })
	h := srv.Handler()

	rec := doReq(h, http.MethodGet, "/", "10.0.0.4:4444", authed())
	valid := sessionCookie(t, rec)

	// (1) Валидная: без Basic — /api/status отдаёт 503 (не 401):
	// KDF не выполняется вовсе, роут за авторизацией пройден (сервер
	// без источника снимков — семантика §5.9).
	if good := doReqCookie(h, http.MethodGet, "/api/status", "10.0.0.4:4445", valid); good.Code != http.StatusServiceUnavailable {
		t.Fatalf("валидная cookie: статус = %d, хочу 503 (нет источника снимков, не 401)", good.Code)
	}

	// (2) Тамперена подпись.
	e0, s0, _ := strings.Cut(valid.Value, ".")
	tamperedSig := &http.Cookie{Name: sessionCookieName, Value: e0 + "." + flipHexNibble(s0)}
	if tamperedSig.Value == valid.Value {
		t.Fatal("тампер не изменил значение подписи")
	}
	if rec2 := doReqCookie(h, http.MethodGet, "/", "10.0.0.4:4446", tamperedSig); rec2.Code != http.StatusUnauthorized {
		t.Errorf("тамперена подпись: статус = %d, хочу 401", rec2.Code)
	}

	// (3) Тамперен expiry (подпись больше не сходится).
	bumped, _ := strconv.ParseInt(e0, 10, 64)
	bumped += 3600
	if rec3 := doReqCookie(h, http.MethodGet, "/", "10.0.0.4:4447", &http.Cookie{
		Name: sessionCookieName, Value: strconv.FormatInt(bumped, 10) + "." + s0,
	}); rec3.Code != http.StatusUnauthorized {
		t.Errorf("тамперен expiry: статус = %d, хочу 401", rec3.Code)
	}

	// (4) Просрочена: та же подпись, часы ушли за expiry.
	clock.now = clock.now.Add(sessionTTL + time.Second)
	if rec4 := doReqCookie(h, http.MethodGet, "/", "10.0.0.4:4448", valid); rec4.Code != http.StatusUnauthorized {
		t.Errorf("просроченная cookie: статус = %d, хочу 401", rec4.Code)
	}
	// За секунду до истечения — ещё валидна.
	clock.now = clock.now.Add(-2 * time.Second)
	if rec5 := doReqCookie(h, http.MethodGet, "/", "10.0.0.4:4449", valid); rec5.Code != http.StatusOK {
		t.Errorf("cookie на секунду до истечения: статус = %d, хочу 200", rec5.Code)
	}

	// (5) Смена password_hash — новый производный ключ: прежняя cookie
	// больше не подписана им (§7.5: смена пароля инвалидирует сессии).
	cfg2 := config.Default()
	cfg2.Dashboard.Username = testUser
	cfg2.Dashboard.PasswordHash = fastHash(t, testPassword) // новая соль → новый ключ
	srv2, err := New(fakePinger{}, cfg2, Options{Now: clock.Now})
	if err != nil {
		t.Fatalf("New(новый хэш): %v", err)
	}
	if rec6 := doReqCookie(srv2.Handler(), http.MethodGet, "/", "10.0.0.4:4450", valid); rec6.Code != http.StatusUnauthorized {
		t.Errorf("cookie после смены password_hash: статус = %d, хочу 401", rec6.Code)
	}
}

// flipHexNibble — первый hex-символ подписи заменён на другой
// (детерминированная «порча» для теста тамперинга).
func flipHexNibble(hexStr string) string {
	b := []byte(hexStr)
	if b[0] == 'f' {
		b[0] = '0'
	} else {
		b[0] = 'f'
	}
	return string(b)
}

// doReqCookie — запрос с cookie-сессией и без Basic.
func doReqCookie(h http.Handler, method, target, remoteAddr string, c *http.Cookie) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	req.RemoteAddr = remoteAddr
	req.AddCookie(c)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// --- защита от подбора (§7.6) ---------------------------------------------------

// TestBruteForceBlock — 5 неудачных сверок в минуту с IP: шестая
// попытка — 429 + Retry-After (остаток окна); чужой IP не задет; окно
// истекло — попытки снова принимаются (блокировка до конца окна,
// не навсегда).
func TestBruteForceBlock(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)}
	h := newTestServer(t, func(o *Options) { o.Now = clock.Now }).Handler()
	bad := map[string]string{"Authorization": basic(testUser, "wrong")}

	for i := 0; i < maxLoginFailures; i++ {
		rec := doReq(h, http.MethodGet, "/", "10.0.0.5:5555", bad)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("попытка %d: статус = %d, хочу 401 (окно ещё не заполнено)", i+1, rec.Code)
		}
	}

	// Шестая: 429 + Retry-After с остатком окна (прошло 30 с → 30 с).
	clock.now = clock.now.Add(30 * time.Second)
	blocked := doReq(h, http.MethodGet, "/", "10.0.0.5:5555", bad)
	if blocked.Code != http.StatusTooManyRequests {
		t.Fatalf("после %d неудач: статус = %d, хочу 429", maxLoginFailures, blocked.Code)
	}
	if ra := blocked.Header().Get("Retry-After"); ra != "30" {
		t.Errorf("Retry-After = %q, хочу \"30\" (остаток окна)", ra)
	}
	// Блокировка действует и на правильные креды — подбор не тратит KDF.
	blockedOK := doReq(h, http.MethodGet, "/", "10.0.0.5:5555", authed())
	if blockedOK.Code != http.StatusTooManyRequests {
		t.Errorf("блокировка не держится на верных кредах: %d, хочу 429", blockedOK.Code)
	}
	// Чужой IP в тот же момент — обычный 401.
	other := doReq(h, http.MethodGet, "/", "10.0.0.6:6666", bad)
	if other.Code != http.StatusUnauthorized {
		t.Errorf("чужой IP: статус = %d, хочу 401 (per-IP окно)", other.Code)
	}

	// Валидная cookie проходит и с заблокированного IP: оператор с
	// живой сессией не заперт чужими неудачами (порядок auth).
	loggedIn := doReq(h, http.MethodGet, "/", "10.0.0.7:7777", authed())
	if loggedIn.Code != http.StatusOK {
		t.Fatalf("вход для cookie: %d", loggedIn.Code)
	}
	withSession := doReqCookie(h, http.MethodGet, "/", "10.0.0.5:5555", sessionCookie(t, loggedIn))
	if withSession.Code != http.StatusOK {
		t.Errorf("валидная cookie с заблокированного IP: %d, хочу 200", withSession.Code)
	}

	// Окно истекло целиком — сверка снова выполняется (снова 401,
	// не 429); неудача открывает новое окно.
	clock.now = clock.now.Add(31 * time.Second) // 30 + 31 > 60 с от первой неудачи
	expired := doReq(h, http.MethodGet, "/", "10.0.0.5:5555", bad)
	if expired.Code != http.StatusUnauthorized {
		t.Errorf("после истечения окна: статус = %d, хочу 401 (не 429)", expired.Code)
	}
}

// TestBruteForceSuccessResets — успешная сверка сбрасывает окно неудач
// IP (зафиксированное решение по §7.6): 4 неудач → вход → ещё 4 неудач
// укладываются в окно заново.
func TestBruteForceSuccessResets(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)}
	h := newTestServer(t, func(o *Options) { o.Now = clock.Now }).Handler()
	bad := map[string]string{"Authorization": basic(testUser, "wrong")}
	const ip = "10.0.0.8:8888"

	for i := 0; i < 4; i++ {
		if rec := doReq(h, http.MethodGet, "/", ip, bad); rec.Code != http.StatusUnauthorized {
			t.Fatalf("неудача %d: %d", i+1, rec.Code)
		}
	}
	if rec := doReq(h, http.MethodGet, "/", ip, authed()); rec.Code != http.StatusOK {
		t.Fatalf("успешный вход после опечаток: %d, хочу 200", rec.Code)
	}
	for i := 0; i < 4; i++ {
		if rec := doReq(h, http.MethodGet, "/", ip, bad); rec.Code != http.StatusUnauthorized {
			t.Fatalf("неудача после сброса %d: %d, хочу 401 (окно началось заново)", i+1, rec.Code)
		}
	}
}

// --- роуты, методы, заголовки (§6.1–6.3) ----------------------------------------

// TestRoutes — каркас поверх фронта T-130: / — страница дашборда
// (assets/page.html), /api/status без источника снимков — 503 +
// no-store (§5.9/§6.2), /assets/* — 404 на неизвестный файл (известные
// имена — сами файлы, assets_test.go), неизвестный путь — 404;
// всё за авторизацией.
func TestRoutes(t *testing.T) {
	h := newTestServer(t, nil).Handler()

	rec := doReq(h, http.MethodGet, "/", "10.0.0.9:9999", authed())
	if rec.Code != http.StatusOK {
		t.Errorf("GET / = %d, хочу 200 (страница T-130)", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "Дашборд Domain Crawler") {
		t.Errorf("GET /: не страница дашборда:\n%s", body)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("GET /: Content-Type = %q, хочу text/html", ct)
	}

	rec = doReq(h, http.MethodGet, "/api/status", "10.0.0.9:9999", authed())
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("GET /api/status = %d, хочу 503 (снимок ещё не собран, §5.9)", rec.Code)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q, хочу no-store (§6.2)", cc)
	}
	if !strings.Contains(rec.Body.String(), "snapshot_not_ready") {
		t.Errorf("тело /api/status без JSON-ошибки: %s", rec.Body.String())
	}

	if rec = doReq(h, http.MethodGet, "/assets/app.css", "10.0.0.9:9999", authed()); rec.Code != http.StatusNotFound {
		t.Errorf("GET /assets/app.css = %d, хочу 404 (файла в embed нет)", rec.Code)
	}
	if rec = doReq(h, http.MethodGet, "/nope", "10.0.0.9:9999", authed()); rec.Code != http.StatusNotFound {
		t.Errorf("GET /nope = %d, хочу 404", rec.Code)
	}
}

// TestMethodNotAllowed — не GET/HEAD на роутах — 405 (§6.3).
func TestMethodNotAllowed(t *testing.T) {
	h := newTestServer(t, nil).Handler()
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/"},
		{http.MethodPut, "/api/status"},
		{http.MethodDelete, "/assets/x.js"},
		{http.MethodPost, "/healthz"},
		{http.MethodPost, "/livez"},
		{http.MethodPost, "/robots.txt"},
	} {
		rec := doReq(h, tc.method, tc.path, "10.0.0.10:1010", authed())
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s %s = %d, хочу 405", tc.method, tc.path, rec.Code)
		}
	}
	// HEAD обслуживается как GET (тело подавляет сервер).
	if rec := doReq(h, http.MethodHead, "/", "10.0.0.10:1010", authed()); rec.Code != http.StatusOK {
		t.Errorf("HEAD / = %d, хочу 200", rec.Code)
	}
}

// TestSecurityHeaders — nosniff и CSP (§6.3, дословно) на всех ответах:
// служебных, защищённых и ошибках авторизации.
func TestSecurityHeaders(t *testing.T) {
	h := newTestServer(t, nil).Handler()
	for _, tc := range []struct {
		name   string
		method string
		path   string
		header map[string]string
	}{
		{"livez", http.MethodGet, "/livez", nil},
		{"healthz", http.MethodGet, "/healthz", nil},
		{"robots", http.MethodGet, "/robots.txt", nil},
		{"страница", http.MethodGet, "/", authed()},
		{"статика", http.MethodGet, "/assets/style.css", authed()},
		{"без кредов", http.MethodGet, "/", nil},
	} {
		rec := doReq(h, tc.method, tc.path, "10.0.0.11:1111", tc.header)
		if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
			t.Errorf("%s: X-Content-Type-Options = %q, хочу nosniff", tc.name, got)
		}
		if got := rec.Header().Get("Content-Security-Policy"); got != contentSecurityPolicy {
			t.Errorf("%s: CSP = %q, хочу %q", tc.name, got, contentSecurityPolicy)
		}
	}
}

// --- служебные роуты без авторизации (§6.1) --------------------------------------

// TestServiceRoutes — healthz (ping/отказ), livez, robots.txt без
// Authorization.
func TestServiceRoutes(t *testing.T) {
	h := newTestServer(t, nil).Handler()

	rec := doReq(h, http.MethodGet, "/healthz", "10.0.0.12:1212", nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "ok") {
		t.Errorf("healthz = %d %q, хочу 200 ok", rec.Code, rec.Body.String())
	}

	rec = doReq(h, http.MethodGet, "/livez", "10.0.0.12:1212", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("livez = %d, хочу 200", rec.Code)
	}

	rec = doReq(h, http.MethodGet, "/robots.txt", "10.0.0.12:1212", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("robots.txt = %d, хочу 200", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, "User-agent: *") || !strings.Contains(body, "Disallow: /") {
		t.Errorf("robots.txt без полного disallow:\n%s", body)
	}

	// healthz при недоступной БД — 503.
	cfg := config.Default()
	cfg.Dashboard.Username = testUser
	cfg.Dashboard.PasswordHash = fastHash(t, testPassword)
	srv, err := New(fakePinger{err: context.DeadlineExceeded}, cfg, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if rec = doReq(srv.Handler(), http.MethodGet, "/healthz", "10.0.0.12:1213", nil); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("healthz при сбое БД = %d, хочу 503", rec.Code)
	}
}

// TestClientIP — адрес клиента для per-IP окна подбора (§7.6):
// X-Real-IP доверяем только от loopback-пира и только валидному IP;
// иначе — RemoteAddr (по образцу web.TestClientIP, T-123).
func TestClientIP(t *testing.T) {
	for _, tc := range []struct {
		name       string
		remoteAddr string
		realIP     string
		want       string
	}{
		{"loopback + валидный X-Real-IP", "127.0.0.1:40000", "203.0.113.7", "203.0.113.7"},
		{"loopback + мусор в заголовке", "127.0.0.1:40000", "не-ip", "127.0.0.1"},
		{"loopback + пустой заголовок", "127.0.0.1:40000", "", "127.0.0.1"},
		{"не-loopback + заголовок игнорируется", "203.0.113.9:40000", "198.51.100.1", "203.0.113.9"},
		{"прямой адрес без порта", "192.0.2.10", "", "192.0.2.10"},
	} {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.RemoteAddr = tc.remoteAddr
		if tc.realIP != "" {
			r.Header.Set("X-Real-IP", tc.realIP)
		}
		if got := clientIP(r); got != tc.want {
			t.Errorf("%s: clientIP = %q, хочу %q", tc.name, got, tc.want)
		}
	}
}

// --- жизненный цикл ---------------------------------------------------------------

// TestRunGracefulStop — Run слушает dashboard.listen, отменённый ctx
// останавливает сервер чистым nil в окне остановки.
func TestRunGracefulStop(t *testing.T) {
	srv := newTestServer(t, nil)

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
		resp, err := http.Get("http://" + addr + "/livez")
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

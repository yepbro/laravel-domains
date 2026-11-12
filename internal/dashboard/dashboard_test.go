// Unit tests of the dashboard framework (T-128): Basic authorization +
// a cookie session (dashboard spec §7), brute-force protection and its
// expiration (§7.6), routes/methods/headers (§6.1–6.3), service routes
// without authorization, the startup credential check (§7.7), graceful
// stop — httptest without PostgreSQL.
package dashboard

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"crawler/internal/config"
	"crawler/internal/dashboard/auth"
)

// testPassword — the operator password in tests; testUser — its login.
const (
	testUser     = "operator"
	testPassword = "secret-pass-123"
)

// fastHash — a password_hash string with a cheap KDF (1000 iterations,
// secret_test.go style): the tests check the authorization protocol,
// not the derivation cost (600,000 iterations ≈ half a second per
// verification).
func fastHash(t *testing.T, password string) string {
	t.Helper()
	h, err := auth.Generate(password, 1000)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return h
}

// fakeClock — controllable clock (cookie sessions §7.5, the brute-force
// window §7.6).
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

// fakePinger — the /healthz readiness dependency.
type fakePinger struct{ err error }

func (p fakePinger) Ping(context.Context) error { return p.err }

// syncBuf — a bytes.Buffer under a lock: slog writes from the Run
// lifecycle goroutine while the test reads the log — without
// synchronization this is a race. Modeled on
// scheduler/backlog_test.go (T-M06).
type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newTestServer — a server with typical credentials; mutators configure
// Options (the clock, the logger).
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

// doReq — a request through the Handler with the given RemoteAddr (the
// per-IP brute-force protection tells clients apart by address) and
// headers.
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

// basic — the Authorization header for a credential pair.
func basic(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

// authed — the header of the correct credentials.
func authed() map[string]string {
	return map[string]string{"Authorization": basic(testUser, testPassword)}
}

// sessionCookie — the cookie session of a successful response (the
// first one by name).
func sessionCookie(t *testing.T, rec *httptest.ResponseRecorder) *http.Cookie {
	t.Helper()
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookieName {
			return c
		}
	}
	t.Fatalf("the response without Set-Cookie %s: %v", sessionCookieName, rec.Result().Cookies())
	return nil
}

// --- startup credential check (§7.7) -------------------------------------------

// TestNewCredentials — an empty username/password_hash and an
// unparseable hash refuse the server build with an exact diagnosis
// (the cmd/dashboard call before service.Run uses the same
// ParseCredentials — the startup fails before listeners; the texts are
// the operator contract).
func TestNewCredentials(t *testing.T) {
	valid := fastHash(t, testPassword)
	for _, tc := range []struct {
		name    string
		user    string
		hash    string
		wantErr string
	}{
		{"an empty name", "", valid, "the username is empty"},
		{"an empty hash", testUser, "", "password_hash is empty"},
		{"both empty", "", "", "the username is empty"},                        // the name is checked first
		{"a name with a colon", "op:erator", valid, "must not contain"}, // Basic splits at the first ":" — a login would be impossible
		{"junk instead of a hash", testUser, " plaintext ", "password_hash: "},
		{"a hash without the scheme", testUser, "sha256$1000$c2FsdA==$a2V5", "password_hash: "},
		{"a hash with a short salt", testUser, "pbkdf2-sha256$1000$c2FsdA==$MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=", "password_hash: "},
		{"a hash with a short key", testUser, "pbkdf2-sha256$1000$MDEyMzQ1Njc4OWFiY2RlZg==$a2V5", "password_hash: "},
		{"a hash with zero iterations", testUser, "pbkdf2-sha256$0$MDEyMzQ1Njc4OWFiY2RlZg==$MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=", "password_hash: "},
	} {
		cfg := config.Default()
		cfg.Dashboard.Username, cfg.Dashboard.PasswordHash = tc.user, tc.hash
		if _, err := New(fakePinger{}, cfg, Options{}); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Errorf("%s: New = %v, want an error with %q", tc.name, err, tc.wantErr)
		}
	}
	// Without a DB pool the server is pointless (the credentials are
	// valid — the check reaches exactly the pool, not the credential
	// parsing).
	cfg := config.Default()
	cfg.Dashboard.Username, cfg.Dashboard.PasswordHash = testUser, valid
	if _, err := New(nil, cfg, Options{}); err == nil || !strings.Contains(err.Error(), "the DB pool is not set") {
		t.Errorf("New(nil db, valid credentials) = %v, want the DB pool is not set", err)
	}
	if _, err := New(fakePinger{}, nil, Options{}); err == nil || !strings.Contains(err.Error(), "the configuration is not set") {
		t.Errorf("New(nil cfg) = %v, want the configuration is not set", err)
	}
	// Valid credentials assemble.
	if _, err := New(fakePinger{}, cfg, Options{}); err != nil {
		t.Errorf("New(valid credentials): %v", err)
	}
}

// --- Basic authorization (§7.4) --------------------------------------------------

// TestAuthRequired — protected routes without credentials: 401 +
// WWW-Authenticate (the browser password form prompt) on each.
func TestAuthRequired(t *testing.T) {
	h := newTestServer(t, nil).Handler()
	for _, path := range []string{"/", "/api/status", "/assets/app.js"} {
		rec := doReq(h, http.MethodGet, path, "10.0.0.1:1111", nil)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("GET %s without credentials = %d, want 401", path, rec.Code)
		}
		chal := rec.Header().Get("WWW-Authenticate")
		if !strings.HasPrefix(chal, `Basic realm="dashboard"`) {
			t.Errorf("GET %s: WWW-Authenticate = %q, want Basic realm=\"dashboard\"", path, chal)
		}
	}
}

// TestAuthWrongCredentials — a wrong name or password: 401; a Warn is
// written with the IP but without credentials (§9.2).
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
		{"a wrong password", testUser, "wrong"},
		{"a wrong name", "admin", testPassword},
		{"both wrong", "admin", "wrong"},
		{"empty", "", ""},
	} {
		rec := doReq(h, http.MethodGet, "/", "10.0.0.2:2222",
			map[string]string{"Authorization": basic(tc.user, tc.pass)})
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s: status = %d, want 401", tc.name, rec.Code)
		}
		if rec.Header().Get("WWW-Authenticate") == "" {
			t.Errorf("%s: a 401 without WWW-Authenticate", tc.name)
		}
	}

	logs := buf.String()
	if !strings.Contains(logs, "10.0.0.2") {
		t.Errorf("a Warn without the IP address (§9.2):\n%s", logs)
	}
	for _, secret := range []string{testPassword, "wrong", "Basic ", srv.cfgHashFragment(t)} {
		if strings.Contains(logs, secret) {
			t.Errorf("the log holds credentials (%q), §9.2:\n%s", secret, logs)
		}
	}
}

// cfgHashFragment — a recognizable piece of the password_hash string
// (the salt in b64) for the "the hash does not leak into the log"
// check (§9.2).
func (s *Server) cfgHashFragment(t *testing.T) string {
	t.Helper()
	if len(s.hash.Salt) < 8 {
		t.Fatalf("the hash salt is shorter than 8 bytes: %d", len(s.hash.Salt))
	}
	return base64.StdEncoding.EncodeToString(s.hash.Salt)
}

// TestAuthSuccess — correct credentials: 200 and a cookie session with
// the §7.5 attributes (HttpOnly, SameSite=Lax, 24 h).
func TestAuthSuccess(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)}
	h := newTestServer(t, func(o *Options) { o.Now = clock.Now }).Handler()

	rec := doReq(h, http.MethodGet, "/", "10.0.0.3:3333", authed())
	if rec.Code != http.StatusOK {
		t.Fatalf("correct credentials: status = %d, want 200 (the body: %s)", rec.Code, rec.Body.String())
	}
	c := sessionCookie(t, rec)
	if !c.HttpOnly {
		t.Error("a cookie without HttpOnly (§7.5)")
	}
	if c.SameSite != http.SameSiteLaxMode {
		t.Errorf("SameSite = %v, want Lax (§7.5)", c.SameSite)
	}
	if c.MaxAge != int(sessionTTL.Seconds()) {
		t.Errorf("MaxAge = %d, want %d (§7.5: 24 h)", c.MaxAge, int(sessionTTL.Seconds()))
	}
	// Value format: <unix-expiry>.<hex HMAC-SHA256>.
	expiry, sig, ok := strings.Cut(c.Value, ".")
	if !ok || len(sig) != 64 {
		t.Fatalf("the cookie value %q: <expiry>.<hex-64> expected", c.Value)
	}
	if exp, err := strconv.ParseInt(expiry, 10, 64); err != nil || exp != clock.now.Add(sessionTTL).Unix() {
		t.Errorf("expiry %q: want the unix time now+24h", expiry)
	}
}

// TestAuthCookieSecure — the Secure flag of the cookie session follows
// the TLS fact (T-227): a direct TLS connection or a reverse proxy with
// X-Forwarded-Proto https from a loopback peer (the nginx sample —
// docs/deploy.md §6) — Secure; a plain deployment and a spoofed header
// from a foreign address — without Secure (otherwise an http cookie
// would never be returned at all).
func TestAuthCookieSecure(t *testing.T) {
	for _, tc := range []struct {
		name       string
		remoteAddr string
		xfp        string
		unix       bool
		directTLS  bool
		want       bool
	}{
		{"a loopback proxy + XFP https", "127.0.0.1:4101", "https", false, false, true},
		{"a loopback proxy + XFP HTTP (case)", "127.0.0.1:4102", "HTTPS", false, false, true},
		{"a loopback proxy + an XFP list (client, proxy)", "127.0.0.1:4103", "http, https", false, false, true},
		{"a loopback proxy + XFP http (an entry without TLS)", "127.0.0.1:4104", "http", false, false, false},
		{"loopback without XFP (a direct dev access)", "127.0.0.1:4105", "", false, false, false},
		{"a foreign peer + a forged XFP https", "203.0.113.9:4106", "https", false, false, false},
		{"a direct TLS connection", "203.0.113.9:4107", "", false, true, true},
		// A unix peer — a trusted proxy (the dashboard listens on TCP
		// only — the parity branch with internal/web, T-221/T-227).
		{"a unix peer + XFP https", "@", "https", true, false, true},
		{"a unix peer without XFP", "@", "", true, false, false},
	} {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = tc.remoteAddr
		if tc.unix {
			req = req.WithContext(context.WithValue(req.Context(), http.LocalAddrContextKey,
				&net.UnixAddr{Name: "@", Net: "unix"}))
		}
		if tc.xfp != "" {
			req.Header.Set("X-Forwarded-Proto", tc.xfp)
		}
		if tc.directTLS {
			req.TLS = &tls.ConnectionState{}
		}
		req.SetBasicAuth(testUser, testPassword)
		rec := httptest.NewRecorder()
		newTestServer(t, nil).Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status = %d, want 200 (the body: %s)", tc.name, rec.Code, rec.Body.String())
		}
		if got := sessionCookie(t, rec).Secure; got != tc.want {
			t.Errorf("%s: the cookie Secure = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// --- cookie session (§7.5) -------------------------------------------------------

// TestSessionCookie — a valid cookie passes without Basic; tampered
// (the signature or expiry) — 401 with a password prompt; expired —
// 401; a password_hash change (another derived key) invalidates
// sessions.
func TestSessionCookie(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)}
	srv := newTestServer(t, func(o *Options) { o.Now = clock.Now })
	h := srv.Handler()

	rec := doReq(h, http.MethodGet, "/", "10.0.0.4:4444", authed())
	valid := sessionCookie(t, rec)

	// (1) Valid: without Basic — /api/status serves 503 (not 401):
	// the KDF is not executed at all, the route behind authorization
	// is passed (a server without a snapshot source — the §5.9
	// semantics).
	if good := doReqCookie(h, http.MethodGet, "/api/status", "10.0.0.4:4445", valid); good.Code != http.StatusServiceUnavailable {
		t.Fatalf("a valid cookie: status = %d, want 503 (no snapshot source, not 401)", good.Code)
	}

	// (2) The signature is tampered.
	e0, s0, _ := strings.Cut(valid.Value, ".")
	tamperedSig := &http.Cookie{Name: sessionCookieName, Value: e0 + "." + flipHexNibble(s0)}
	if tamperedSig.Value == valid.Value {
		t.Fatal("the tamper did not change the signature value")
	}
	if rec2 := doReqCookie(h, http.MethodGet, "/", "10.0.0.4:4446", tamperedSig); rec2.Code != http.StatusUnauthorized {
		t.Errorf("a tampered signature: status = %d, want 401", rec2.Code)
	}

	// (3) The expiry is tampered (the signature no longer matches).
	bumped, _ := strconv.ParseInt(e0, 10, 64)
	bumped += 3600
	if rec3 := doReqCookie(h, http.MethodGet, "/", "10.0.0.4:4447", &http.Cookie{
		Name: sessionCookieName, Value: strconv.FormatInt(bumped, 10) + "." + s0,
	}); rec3.Code != http.StatusUnauthorized {
		t.Errorf("a tampered expiry: status = %d, want 401", rec3.Code)
	}

	// (4) Expired: the same signature, the clock moved past the expiry.
	clock.now = clock.now.Add(sessionTTL + time.Second)
	if rec4 := doReqCookie(h, http.MethodGet, "/", "10.0.0.4:4448", valid); rec4.Code != http.StatusUnauthorized {
		t.Errorf("an expired cookie: status = %d, want 401", rec4.Code)
	}
	// A second before expiry — still valid.
	clock.now = clock.now.Add(-2 * time.Second)
	if rec5 := doReqCookie(h, http.MethodGet, "/", "10.0.0.4:4449", valid); rec5.Code != http.StatusOK {
		t.Errorf("a cookie a second before the expiry: status = %d, want 200", rec5.Code)
	}

	// (5) A password_hash change — a new derived key: the previous
	// cookie is no longer signed by it (§7.5: a password change
	// invalidates sessions).
	cfg2 := config.Default()
	cfg2.Dashboard.Username = testUser
	cfg2.Dashboard.PasswordHash = fastHash(t, testPassword) // a new salt → a new key
	srv2, err := New(fakePinger{}, cfg2, Options{Now: clock.Now})
	if err != nil {
		t.Fatalf("New(a new hash): %v", err)
	}
	if rec6 := doReqCookie(srv2.Handler(), http.MethodGet, "/", "10.0.0.4:4450", valid); rec6.Code != http.StatusUnauthorized {
		t.Errorf("a cookie after the password_hash change: status = %d, want 401", rec6.Code)
	}
}

// flipHexNibble — the first hex character of the signature replaced
// with another (a deterministic "corruption" for the tampering test).
func flipHexNibble(hexStr string) string {
	b := []byte(hexStr)
	if b[0] == 'f' {
		b[0] = '0'
	} else {
		b[0] = 'f'
	}
	return string(b)
}

// doReqCookie — a request with a cookie session and without Basic.
func doReqCookie(h http.Handler, method, target, remoteAddr string, c *http.Cookie) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	req.RemoteAddr = remoteAddr
	req.AddCookie(c)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// --- brute-force protection (§7.6) -----------------------------------------------

// TestBruteForceBlock — 5 failed verifications per minute from an IP:
// the sixth attempt — 429 + Retry-After (the window remainder); a
// foreign IP is untouched; the window expired — attempts are accepted
// again (blocking until the end of the window, not forever).
func TestBruteForceBlock(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)}
	h := newTestServer(t, func(o *Options) { o.Now = clock.Now }).Handler()
	bad := map[string]string{"Authorization": basic(testUser, "wrong")}

	for i := 0; i < maxLoginFailures; i++ {
		rec := doReq(h, http.MethodGet, "/", "10.0.0.5:5555", bad)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d: status = %d, want 401 (the window is not full yet)", i+1, rec.Code)
		}
	}

	// The sixth: 429 + Retry-After with the window remainder (30 s
	// passed → 30 s).
	clock.now = clock.now.Add(30 * time.Second)
	blocked := doReq(h, http.MethodGet, "/", "10.0.0.5:5555", bad)
	if blocked.Code != http.StatusTooManyRequests {
		t.Fatalf("after %d failures: status = %d, want 429", maxLoginFailures, blocked.Code)
	}
	if ra := blocked.Header().Get("Retry-After"); ra != "30" {
		t.Errorf("Retry-After = %q, want \"30\" (the window remainder)", ra)
	}
	// Blocking applies to correct credentials too — brute force does
	// not spend the KDF.
	blockedOK := doReq(h, http.MethodGet, "/", "10.0.0.5:5555", authed())
	if blockedOK.Code != http.StatusTooManyRequests {
		t.Errorf("the block does not hold on correct credentials: %d, want 429", blockedOK.Code)
	}
	// A foreign IP at the same moment — a normal 401.
	other := doReq(h, http.MethodGet, "/", "10.0.0.6:6666", bad)
	if other.Code != http.StatusUnauthorized {
		t.Errorf("a foreign IP: status = %d, want 401 (a per-IP window)", other.Code)
	}

	// A valid cookie passes even from a blocked IP: an operator with a
	// live session is not locked out by someone else's failures (the
	// auth order).
	loggedIn := doReq(h, http.MethodGet, "/", "10.0.0.7:7777", authed())
	if loggedIn.Code != http.StatusOK {
		t.Fatalf("the login for the cookie: %d", loggedIn.Code)
	}
	withSession := doReqCookie(h, http.MethodGet, "/", "10.0.0.5:5555", sessionCookie(t, loggedIn))
	if withSession.Code != http.StatusOK {
		t.Errorf("a valid cookie from a blocked IP: %d, want 200", withSession.Code)
	}

	// The window has fully expired — verification works again (401
	// again, not 429); a failure opens a new window.
	clock.now = clock.now.Add(31 * time.Second) // 30 + 31 > 60 s since the first failure
	expired := doReq(h, http.MethodGet, "/", "10.0.0.5:5555", bad)
	if expired.Code != http.StatusUnauthorized {
		t.Errorf("after the window expiry: status = %d, want 401 (not 429)", expired.Code)
	}
}

// TestBruteForceSuccessResets — a successful verification resets the
// IP failure window (the fixed decision on §7.6): 4 failures → a
// login → 4 more failures fit into the window anew.
func TestBruteForceSuccessResets(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)}
	h := newTestServer(t, func(o *Options) { o.Now = clock.Now }).Handler()
	bad := map[string]string{"Authorization": basic(testUser, "wrong")}
	const ip = "10.0.0.8:8888"

	for i := 0; i < 4; i++ {
		if rec := doReq(h, http.MethodGet, "/", ip, bad); rec.Code != http.StatusUnauthorized {
			t.Fatalf("failure %d: %d", i+1, rec.Code)
		}
	}
	if rec := doReq(h, http.MethodGet, "/", ip, authed()); rec.Code != http.StatusOK {
		t.Fatalf("a successful login after typos: %d, want 200", rec.Code)
	}
	for i := 0; i < 4; i++ {
		if rec := doReq(h, http.MethodGet, "/", ip, bad); rec.Code != http.StatusUnauthorized {
			t.Fatalf("a failure after the reset %d: %d, want 401 (the window started anew)", i+1, rec.Code)
		}
	}
}

// countingVerifier — a password verification stub (the passwordVerifier
// seam, T-231): it counts Verify calls — each production verification
// is one PBKDF2 (~half a second of CPU), the counter observes the KDF
// cost without a real derivation. cost imitates the KDF time: a
// request is held between the limit check and the failure record, so
// the race of the non-atomic window (separate check and record around
// the verification) manifests deterministically; verdict — the
// verification outcome.
type countingVerifier struct {
	calls   atomic.Int64
	cost    time.Duration
	verdict bool
}

func (v *countingVerifier) Verify(string) bool {
	v.calls.Add(1)
	time.Sleep(v.cost)
	return v.verdict
}

// TestBruteForceConcurrentBatch — a parallel burst of failed Basic
// requests from one IP does not reach the KDF in full (T-231): the
// failure slot is reserved atomically with the limit check, before the
// verification, so of 10 simultaneous attempts exactly maxLoginFailures
// pass it, the rest — 429 without calling Verify. Before the fix
// (check and record on opposite sides of the verification) the whole
// burst saw the window unfilled and spent the KDF on every request —
// only sequential attempts kept the limit (a CPU DoS).
func TestBruteForceConcurrentBatch(t *testing.T) {
	verifier := &countingVerifier{cost: 5 * time.Millisecond}
	srv := newTestServer(t, nil)
	srv.verify = verifier
	h := srv.Handler()

	const n = 10
	codes := make([]int, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // the common barrier: the burst arrives at once
			codes[i] = doReq(h, http.MethodGet, "/", "10.0.0.13:1313",
				map[string]string{"Authorization": basic(testUser, "wrong")}).Code
		}(i)
	}
	close(start)
	wg.Wait()

	unauthorized, limited := 0, 0
	for _, c := range codes {
		switch c {
		case http.StatusUnauthorized:
			unauthorized++
		case http.StatusTooManyRequests:
			limited++
		default:
			t.Fatalf("a concurrent batch: an unexpected status %d (want 401 or 429)", c)
		}
	}
	if unauthorized != maxLoginFailures || limited != n-maxLoginFailures {
		t.Errorf("a concurrent batch %d: 401=%d, 429=%d, want %d and %d — the slots go to exactly %d attempts",
			n, unauthorized, limited, maxLoginFailures, n-maxLoginFailures, maxLoginFailures)
	}
	if calls := verifier.calls.Load(); calls > maxLoginFailures {
		t.Errorf("Verify called %d times in a concurrent batch of %d, want ≤%d (the KDF does not run beyond the limit)",
			calls, n, maxLoginFailures)
	}
}

// TestBruteForceConcurrentSuccessBatch — the reservation boundary
// (T-231): the slot is taken before verification, so of simultaneous
// valid attempts beyond maxLoginFailures the extra ones get 429 — the
// ceiling on concurrent verifications from an IP, a deliberate
// compromise of option A (the browser proceeds with a cookie after the
// first verification). Successes do not accumulate in the window:
// after the burst a login passes freely.
func TestBruteForceConcurrentSuccessBatch(t *testing.T) {
	verifier := &countingVerifier{cost: 5 * time.Millisecond, verdict: true}
	srv := newTestServer(t, nil)
	srv.verify = verifier
	h := srv.Handler()
	authHeader := authed()

	const n = maxLoginFailures + 1
	codes := make([]int, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			codes[i] = doReq(h, http.MethodGet, "/", "10.0.0.15:1515", authHeader).Code
		}(i)
	}
	close(start)
	wg.Wait()

	passed, limited := 0, 0
	for _, c := range codes {
		switch c {
		case http.StatusOK:
			passed++
		case http.StatusTooManyRequests:
			limited++
		default:
			t.Fatalf("concurrent valid attempts: an unexpected status %d", c)
		}
	}
	if passed != maxLoginFailures || limited != 1 {
		t.Errorf("concurrent valid %d: 200=%d, 429=%d, want %d and 1 (the ceiling on concurrent verifications)",
			n, passed, limited, maxLoginFailures)
	}
	// Successes charge the slots by resetting the window — the next
	// attempt is free.
	if rec := doReq(h, http.MethodGet, "/", "10.0.0.15:1515", authHeader); rec.Code != http.StatusOK {
		t.Errorf("a login after the burst of successes: %d, want 200 (the window is not filled with successes)", rec.Code)
	}
}

// TestBruteForcePartialWindowExpiry — a partially filled window expires
// as a whole: after a pause longer than the window the counter does not
// inherit the "tail" of past failures (the P1 regression of the T-231
// review — the expired window reset must work at fails < max too, like
// in the old record). One failure → a pause > the window → the new
// window accepts exactly maxLoginFailures verifications, the next one
// — 429.
func TestBruteForcePartialWindowExpiry(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)}
	verifier := &countingVerifier{}
	srv := newTestServer(t, func(o *Options) { o.Now = clock.Now })
	srv.verify = verifier
	h := srv.Handler()
	bad := map[string]string{"Authorization": basic(testUser, "wrong")}
	const ip = "10.0.0.16:1616"

	// One failure: the window started but is not full.
	if rec := doReq(h, http.MethodGet, "/", ip, bad); rec.Code != http.StatusUnauthorized {
		t.Fatalf("a single failure: %d, want 401", rec.Code)
	}
	// A pause longer than the window: the next minute starts with a
	// clean counter.
	clock.now = clock.now.Add(loginWindow + time.Second)
	for i := 0; i < maxLoginFailures; i++ {
		if rec := doReq(h, http.MethodGet, "/", ip, bad); rec.Code != http.StatusUnauthorized {
			t.Fatalf("a new-window failure %d: %d, want 401 (the old window tail is not carried over)", i+1, rec.Code)
		}
	}
	if calls := verifier.calls.Load(); calls != maxLoginFailures+1 {
		t.Fatalf("Verify: %d calls, want %d (one + the new window without the tail)", calls, maxLoginFailures+1)
	}
	if rec := doReq(h, http.MethodGet, "/", ip, bad); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("an attempt beyond the new window: %d, want 429", rec.Code)
	}
	if calls := verifier.calls.Load(); calls != maxLoginFailures+1 {
		t.Errorf("the 429 spent a verification: Verify %d, want the previous %d", calls, maxLoginFailures+1)
	}
}

// TestBruteForceVerifyBudget — the verification budget per window is
// exhausted at exactly maxLoginFailures attempts (the T-231 criterion):
// a sixth failure and correct credentials with a full window — 429
// without calling Verify; after the window expires a success resets it
// (the reserved slot charged), and a failed attempt spends a
// verification again.
func TestBruteForceVerifyBudget(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)}
	verifier := &countingVerifier{}
	srv := newTestServer(t, func(o *Options) { o.Now = clock.Now })
	srv.verify = verifier
	h := srv.Handler()
	bad := map[string]string{"Authorization": basic(testUser, "wrong")}
	const ip = "10.0.0.14:1414"

	// Five sequential failures — a verification each, the window full.
	for i := 0; i < maxLoginFailures; i++ {
		if rec := doReq(h, http.MethodGet, "/", ip, bad); rec.Code != http.StatusUnauthorized {
			t.Fatalf("failure %d: %d", i+1, rec.Code)
		}
	}
	if calls := verifier.calls.Load(); calls != maxLoginFailures {
		t.Fatalf("after %d failures Verify was called %d times, want %d", maxLoginFailures, calls, maxLoginFailures)
	}

	// The sixth (wrong and correct credentials) — 429 without a
	// verification.
	for i, header := range []map[string]string{bad, authed()} {
		if rec := doReq(h, http.MethodGet, "/", ip, header); rec.Code != http.StatusTooManyRequests {
			t.Errorf("attempt %d with a full window: %d, want 429", i+1, rec.Code)
		}
	}
	if calls := verifier.calls.Load(); calls != maxLoginFailures {
		t.Errorf("the 429 spent a verification: Verify %d times, want the previous %d", calls, maxLoginFailures)
	}

	// The window expired — a successful login resets it, the next
	// failure performs a verification again (the window started
	// anew).
	clock.now = clock.now.Add(loginWindow + time.Second)
	verifier.verdict = true
	if rec := doReq(h, http.MethodGet, "/", ip, authed()); rec.Code != http.StatusOK {
		t.Fatalf("a login after the window expiry: %d, want 200", rec.Code)
	}
	verifier.verdict = false
	if rec := doReq(h, http.MethodGet, "/", ip, bad); rec.Code != http.StatusUnauthorized {
		t.Fatalf("a failure after a successful login: %d, want 401 (the window was reset)", rec.Code)
	}
	if calls := verifier.calls.Load(); calls != maxLoginFailures+2 {
		t.Errorf("Verify after the window reset: %d calls, want %d (a login + a failure)", calls, maxLoginFailures+2)
	}
}

// --- routes, methods, headers (§6.1–6.3) ------------------------------------------

// TestRoutes — the framework on top of the T-130 front end: / — the
// dashboard page (assets/page.html), /api/status without a snapshot
// source — 503 + no-store (§5.9/§6.2), /assets/* — 404 on an unknown
// file (known names — the files themselves, assets_test.go), an
// unknown path — 404; all behind authorization.
func TestRoutes(t *testing.T) {
	h := newTestServer(t, nil).Handler()

	rec := doReq(h, http.MethodGet, "/", "10.0.0.9:9999", authed())
	if rec.Code != http.StatusOK {
		t.Errorf("GET / = %d, want 200 (the T-130 page)", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "Domain Crawler Dashboard") {
		t.Errorf("GET /: not a dashboard page:\n%s", body)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("GET /: Content-Type = %q, want text/html", ct)
	}

	rec = doReq(h, http.MethodGet, "/api/status", "10.0.0.9:9999", authed())
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("GET /api/status = %d, want 503 (the snapshot is not collected yet, §5.9)", rec.Code)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store (§6.2)", cc)
	}
	if !strings.Contains(rec.Body.String(), "snapshot_not_ready") {
		t.Errorf("the /api/status body without a JSON error: %s", rec.Body.String())
	}

	if rec = doReq(h, http.MethodGet, "/assets/app.css", "10.0.0.9:9999", authed()); rec.Code != http.StatusNotFound {
		t.Errorf("GET /assets/app.css = %d, want 404 (the file is not in embed)", rec.Code)
	}
	if rec = doReq(h, http.MethodGet, "/nope", "10.0.0.9:9999", authed()); rec.Code != http.StatusNotFound {
		t.Errorf("GET /nope = %d, want 404", rec.Code)
	}
}

// TestMethodNotAllowed — non-GET/HEAD on the routes — 405 (§6.3).
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
			t.Errorf("%s %s = %d, want 405", tc.method, tc.path, rec.Code)
		}
	}
	// HEAD is served as GET (the server suppresses the body).
	if rec := doReq(h, http.MethodHead, "/", "10.0.0.10:1010", authed()); rec.Code != http.StatusOK {
		t.Errorf("HEAD / = %d, want 200", rec.Code)
	}
}

// TestSecurityHeaders — nosniff and the CSP (§6.3, verbatim) on all
// responses: service ones, protected ones and authorization errors.
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
		{"the page", http.MethodGet, "/", authed()},
		{"the statics", http.MethodGet, "/assets/style.css", authed()},
		{"without credentials", http.MethodGet, "/", nil},
	} {
		rec := doReq(h, tc.method, tc.path, "10.0.0.11:1111", tc.header)
		if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
			t.Errorf("%s: X-Content-Type-Options = %q, want nosniff", tc.name, got)
		}
		if got := rec.Header().Get("Content-Security-Policy"); got != contentSecurityPolicy {
			t.Errorf("%s: CSP = %q, want %q", tc.name, got, contentSecurityPolicy)
		}
	}
}

// --- service routes without authorization (§6.1) ----------------------------------

// TestServiceRoutes — healthz (a ping/failure), livez, robots.txt
// without Authorization: the exact Content-Type of the text probes and
// the livez body — the framework probe contract (k8s/monitoring read
// them literally).
func TestServiceRoutes(t *testing.T) {
	h := newTestServer(t, nil).Handler()

	rec := doReq(h, http.MethodGet, "/healthz", "10.0.0.12:1212", nil)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "ok") {
		t.Errorf("healthz = %d %q, want 200 ok", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Errorf("healthz: Content-Type = %q, want text/plain; charset=utf-8", ct)
	}

	rec = doReq(h, http.MethodGet, "/livez", "10.0.0.12:1212", nil)
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Errorf("livez = %d %q, want 200 with an ok body", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Errorf("livez: Content-Type = %q, want text/plain; charset=utf-8", ct)
	}

	rec = doReq(h, http.MethodGet, "/robots.txt", "10.0.0.12:1212", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("robots.txt = %d, want 200", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, "User-agent: *") || !strings.Contains(body, "Disallow: /") {
		t.Errorf("robots.txt without the full disallow:\n%s", body)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Errorf("robots.txt: Content-Type = %q, want text/plain; charset=utf-8", ct)
	}

	// healthz with the DB unavailable — 503.
	cfg := config.Default()
	cfg.Dashboard.Username = testUser
	cfg.Dashboard.PasswordHash = fastHash(t, testPassword)
	srv, err := New(fakePinger{err: context.DeadlineExceeded}, cfg, Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if rec = doReq(srv.Handler(), http.MethodGet, "/healthz", "10.0.0.12:1213", nil); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("healthz on a DB failure = %d, want 503", rec.Code)
	}
}

// TestClientIP — the client address for the per-IP brute-force window
// (§7.6): X-Real-IP is trusted only from a loopback peer and only if it
// is a valid IP; otherwise — RemoteAddr (modeled on web.TestClientIP,
// T-123).
func TestClientIP(t *testing.T) {
	for _, tc := range []struct {
		name       string
		remoteAddr string
		realIP     string
		want       string
	}{
		{"loopback + a valid X-Real-IP", "127.0.0.1:40000", "203.0.113.7", "203.0.113.7"},
		{"loopback + junk in the header", "127.0.0.1:40000", "not-an-ip", "127.0.0.1"},
		{"loopback + an empty header", "127.0.0.1:40000", "", "127.0.0.1"},
		{"not loopback + the header ignored", "203.0.113.9:40000", "198.51.100.1", "203.0.113.9"},
		{"a direct address without a port", "192.0.2.10", "", "192.0.2.10"},
	} {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.RemoteAddr = tc.remoteAddr
		if tc.realIP != "" {
			r.Header.Set("X-Real-IP", tc.realIP)
		}
		if got := clientIP(r); got != tc.want {
			t.Errorf("%s: clientIP = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// --- lifecycle ----------------------------------------------------------------------

// TestRunGracefulStop — Run listens on dashboard.listen, a cancelled ctx
// stops the server with a clean nil within the shutdown window; the
// startup is recorded in the log with the listener (startup
// observability for the operator), and the Serve goroutine finishes
// after Run returns — the done channel buffer (capacity 1) lets the
// send finish, the goroutine counter returns to baseline.
func TestRunGracefulStop(t *testing.T) {
	var logs syncBuf
	srv := newTestServer(t, func(o *Options) { o.Log = slog.New(slog.NewTextHandler(&logs, nil)) })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	base := runtime.NumGoroutine() // before Run: everything it starts must go away
	go func() { done <- srv.Run(ctx, addr) }()

	// The readiness probe — a client with a short timeout: a hung Get
	// must not block the waiting deadline between iterations.
	probe := &http.Client{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err := probe.Get("http://" + addr + "/livez")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("the server did not start within 2 s")
		}
		time.Sleep(5 * time.Millisecond)
	}
	probe.CloseIdleConnections()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after the cancellation: %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the server did not stop after the ctx cancellation")
	}
	if s := logs.String(); !strings.Contains(s, "the public dashboard service started") || !strings.Contains(s, addr) {
		t.Errorf("the startup log without the listener %s:\n%s", addr, s)
	}
	gDeadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > base {
		if time.Now().After(gDeadline) {
			t.Errorf("the Run goroutines did not finish after the stop: baseline %d, now %d (a done capacity of 0 would leave Serve hanging)", base, runtime.NumGoroutine())
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestRunListenError — an invalid address (a port outside 0–65535): Run
// refuses before the server starts, the error names the listener — the
// operator sees the configured address, not a bare net.Listen refusal.
func TestRunListenError(t *testing.T) {
	srv := newTestServer(t, nil)
	err := srv.Run(context.Background(), "127.0.0.1:99999")
	if err == nil {
		t.Fatal("Run(port 99999) = nil, want a listener error")
	}
	if !strings.Contains(err.Error(), "listener") || !strings.Contains(err.Error(), "127.0.0.1:99999") {
		t.Errorf("the error without the listener address: %v", err)
	}
}

// Dashboard authorization (dashboard spec §7): the Basic middleware
// (§7.4 — constant-time name, password — PBKDF2 verification of the
// auth package, T-127), a cookie session
// `<expiry>.<HMAC-SHA256(expiry, KDF derived key)>`
// (§7.5 — the fast HMAC check keeps the KDF cost off the periodic
// polling) and per-IP brute-force protection (§7.6 — 5 failures per
// minute → 429 until the end of the window). The password, hash and
// cookie values are never logged (§9.2).
package dashboard

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Cookie session parameters (dashboard spec §7.5) and brute-force
// protection parameters (dashboard spec §7.6).
const (
	// sessionCookieName — the dashboard cookie session name.
	sessionCookieName = "dashboard_session"
	// sessionTTL — the session lifetime (dashboard spec §7.5: 24 h).
	sessionTTL = 24 * time.Hour
	// maxLoginFailures — verification failures per window before
	// blocking (dashboard spec §7.6: 5 per minute).
	maxLoginFailures = 5
	// loginWindow — the window for counting failures and blocking
	// (dashboard spec §7.6: a minute).
	loginWindow = time.Minute
)

// auth — the middleware of protected routes (dashboard spec §6.1). Order:
//
//  1. a valid cookie session lets the request through without the KDF
//     (§7.5) and does not hit the brute-force protection — an operator
//     with a live session is not locked out by someone else's failures
//     from their IP (shared NAT/proxy);
//  2. per-IP failure window (§7.6): 5 failed verifications within a
//     minute — 429 + Retry-After until the end of the window; the
//     failure slot is reserved atomically with the limit check and
//     before the KDF — a blocked address does not spend half a second
//     of CPU on every attempt, and a parallel burst of requests from
//     one IP does not get through to verification in full (T-231:
//     separate check and record calls around the KDF let a burst past
//     the limit — only sequential attempts kept it);
//  3. Basic verification (§7.4): the name — subtle.ConstantTimeCompare,
//     the password — PBKDF2 from the T-127 format, constant-time;
//     success issues a fresh cookie and resets the IP failure window —
//     which also charges the reserved slot (the fixed decision on
//     §7.6: an operator who logs in after typos is not left at the
//     blocking threshold).
//
// A failed verification — Warn with the IP address, without credentials
// (§9.2).
func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		now := s.opt.Now()
		ip := clientIP(r)

		// (1) Cookie session.
		if c, err := r.Cookie(sessionCookieName); err == nil && s.sessionValid(c.Value, now) {
			next(w, r)
			return
		}

		// (2) Brute-force protection: reserving a failure slot together
		// with the limit check (T-231).
		if ok, retry := s.limiter.reserve(ip, now); !ok {
			// Retry-After — the window remainder rounded up, at least 1 s.
			secs := int((retry + time.Second - 1) / time.Second)
			w.Header().Set("Retry-After", strconv.Itoa(secs))
			http.Error(w, "too many failed login attempts", http.StatusTooManyRequests)
			return
		}

		// (3) Basic verification. The failure slot is already reserved
		// (reserve): a failure leaves it in the window, success resets
		// the whole window.
		user, pass, ok := r.BasicAuth()
		if !ok || !s.verifyCredentials(user, pass) {
			s.opt.Log.Warn("a failed dashboard credential verification", "ip", ip)
			w.Header().Set("WWW-Authenticate", `Basic realm="dashboard", charset="UTF-8"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		s.limiter.reset(ip)
		http.SetCookie(w, &http.Cookie{
			Name:     sessionCookieName,
			Value:    s.newSession(now),
			Path:     "/",
			HttpOnly: true,
			// Secure in TLS mode (T-227): when running behind a TLS
			// proxy the browser will not leave with the session over
			// plaintext on a direct visit bypassing the proxy; not set
			// in a plain deployment — an http cookie with Secure would
			// never be returned at all.
			Secure:   requestIsHTTPS(r),
			SameSite: http.SameSiteLaxMode,
			MaxAge:   int(sessionTTL.Seconds()),
		})
		next(w, r)
	}
}

// verifyCredentials — Basic credential verification (§7.4): the name —
// subtle.ConstantTimeCompare, the password — Verify (KDF +
// ConstantTimeCompare, T-127; the verify field is a test seam, New sets
// hash). Both verifications are always computed, without short
// circuiting: skipping the KDF on a wrong name would reveal its
// correctness through the response time.
func (s *Server) verifyCredentials(user, pass string) bool {
	userOK := subtle.ConstantTimeCompare([]byte(user), []byte(s.username)) == 1
	passOK := s.verify.Verify(pass)
	return userOK && passOK
}

// newSession — the cookie session value (§7.5):
// `<expiry>.<HMAC-SHA256(expiry, KDF derived key)>`: expiry is the
// expiration unix time (decimal), the signature is hex. The key is the
// hash key (Key from the T-127 string) — it equals the derived key of
// the correct password (Verify verifies Derive(password) against it,
// §7.4), so changing password_hash changes the HMAC key and invalidates
// all sessions — no separate server-side secret is needed (§7.5).
func (s *Server) newSession(now time.Time) string {
	expiry := strconv.FormatInt(now.Add(sessionTTL).Unix(), 10)
	return expiry + "." + signSession(s.hash.Key, expiry)
}

// sessionValid — cookie session validation: compact format, a future
// expiry, a constant-time signature check (hmac.Equal). Any error
// simply means "no session": the request continues with Basic
// verification.
func (s *Server) sessionValid(v string, now time.Time) bool {
	if len(v) > 128 { // 10-digit expiry + dot + 64 hex — with margin
		return false
	}
	expiry, sig, ok := strings.Cut(v, ".")
	if !ok {
		return false
	}
	exp, err := strconv.ParseInt(expiry, 10, 64)
	if err != nil || now.Unix() >= exp {
		return false
	}
	return hmac.Equal([]byte(signSession(s.hash.Key, expiry)), []byte(sig))
}

// signSession — HMAC-SHA256(expiry, key) in hex.
func signSession(key []byte, expiry string) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(expiry))
	return hex.EncodeToString(mac.Sum(nil))
}

// --- per-IP failed-verification window (§7.6) ----------------------------------
//
// The model is the T-123 token bucket (internal/web/api.go): an
// in-memory map under a mutex with lazy cleanup (a single service
// instance, no external storage needed). The semantics are a failure
// window, not a refilling bucket: "5 failed verifications within a
// minute — refusal until the end of the window" does not allow
// sustained brute force at 5 attempts per minute forever: the window
// starts with the first failure of a burst and expires as a whole.

type loginLimiter struct {
	max    int
	window time.Duration
	mu     sync.Mutex
	state  map[string]*loginWindowState
}

// loginWindowState — the IP failure counter inside the current window.
// since — the moment of the window's first reserved attempt (before the
// T-231 fix — the first recorded failure; the shift is at most the
// verification duration, ~half a second).
type loginWindowState struct {
	fails int
	since time.Time
}

func newLoginLimiter(max int, window time.Duration) *loginLimiter {
	return &loginLimiter{max: max, window: window, state: make(map[string]*loginWindowState)}
}

// reserve — a credential verification attempt from ip at moment now:
// the failure slot is taken (counter increment) and the limit checked
// atomically, under one mutex (T-231) — a parallel burst of requests
// from one IP does not slip past the limit in the gap between separate
// check and record calls. A refusal carries the window remainder for
// Retry-After; a fully expired window starts anew regardless of
// fill — the "tail" of past failures is not carried into the new
// minute (P1 review of T-231; blocking is "until the end of the
// window", not forever; a forgotten 429 response does not extend the
// window — its slot is not reserved at all). A successful verification
// charges the reserved slot by resetting the window (reset).
//
// Reservation boundary: the slot is taken before verification, so
// beyond maxLoginFailures simultaneous attempts from an IP the extra
// ones get 429 even with correct credentials (the ceiling on
// concurrent verifications; a deliberate compromise of option A —
// after the first successful verification the client proceeds with a
// cookie and never touches the limiter, and successes do not
// accumulate in the window: reset immediately).
func (l *loginLimiter) reserve(ip string, now time.Time) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	st, ok := l.state[ip]
	if !ok {
		st = &loginWindowState{fails: 1, since: now}
		l.state[ip] = st
		// Lazy cleanup: the map grows with the number of unique IPs;
		// records older than the window are deleted on the pass
		// (authorization is not a hot path).
		if len(l.state) > 4096 {
			for k, v := range l.state {
				if now.Sub(v.since) > l.window*4 {
					delete(l.state, k)
				}
			}
		}
		return true, 0
	}
	// The window has fully expired — a new attempt opens the next one
	// with a clean counter (unconditional reset, not only when the
	// window is full).
	if now.Sub(st.since) >= l.window {
		st.fails = 0
		st.since = now
	}
	if st.fails >= l.max {
		// The window is still alive here (otherwise reset above) — the
		// remainder is positive.
		return false, l.window - now.Sub(st.since)
	}
	st.fails++
	return true, 0
}

// reset — a successful verification resets the IP window (the fixed
// decision on §7.6).
func (l *loginLimiter) reset(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.state, ip)
}

// clientIP — the client address for brute-force protection and the
// failure log (§7.6/§9.2). Behind a reverse proxy (dashboard spec
// §9.1) all connections come from nginx loopback — then the operator
// address is carried by X-Real-IP (the nginx sample rewrites it with
// $remote_addr). The header is trusted only from a loopback peer and
// only if it is a valid IP; otherwise — RemoteAddr.
//
// The direct-access-without-proxy caveat (the same pattern as the
// landing page form, T-123): a peer from a loopback address can
// substitute an arbitrary X-Real-IP and thereby change "its" IP from
// request to request. This is accepted: in the documented topology
// nginx stands in front of the service and rewrites the header, and
// direct dev access is the operator's own loopback (a trusted side);
// in compose the published port yields a non-loopback peer and trust
// is not enabled at all.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		if pip := net.ParseIP(strings.TrimSpace(r.Header.Get("X-Real-IP"))); pip != nil {
			return pip.String()
		}
	}
	return host
}

// requestIsHTTPS — the client request arrived over TLS (T-227): either
// a direct TLS connection (r.TLS — the dashboard does not listen that
// way, a caveat for the future) or a reverse proxy terminating TLS and
// passing the client scheme in X-Forwarded-Proto (the nginx sample —
// docs/deploy.md §6; the header is trusted only from a trusted peer —
// the same pattern as clientIP). Determines the Secure flag of the
// cookie session: without it the cookie would leave over plaintext
// when running behind TLS on direct access bypassing the proxy.
func requestIsHTTPS(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	if !trustedPeer(r) {
		return false
	}
	// Value list: the last position — appended by our proxy
	// (modeled on X-Forwarded-For in internal/web, T-221); the nginx
	// sample sets a single $scheme value.
	proto := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto"))
	return strings.EqualFold(strings.TrimSpace(proto[strings.LastIndex(proto, ",")+1:]), "https")
}

// trustedPeer — the connection peer is a trusted reverse proxy of this
// host: loopback TCP (dashboard.listen behind nginx, docs/deploy.md §6)
// or a unix socket. Today the dashboard listens on TCP only
// (dashboard.Run: net.Listen("tcp"), config validation — [host]:port);
// the unix branch is parity with internal/web (T-221), so that the
// Secure flag does not silently disappear if a unix listener appears.
func trustedPeer(r *http.Request) bool {
	if _, ok := r.Context().Value(http.LocalAddrContextKey).(*net.UnixAddr); ok {
		return true
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

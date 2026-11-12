// Manual landing check form (T-123): POST /api/check-domain — a hybrid
// of the stored pipeline data (domain_info) and a live check
// (internal/checker, the same pipeline as crawlerctl check). The rate
// limit of 4 requests/min per IP — an in-memory token bucket (a single
// service instance, stdlib; a stage-13 fixed decision — like the
// larasites throttling). The result is written to web_checks (T-120) —
// who/what/when checked. The visitor address behind the proxy and the
// form outcomes/failures/duration — T-124 (the TCP-loopback peer) and
// T-221 (the unix-socket peer — a deploy variant from
// deploy/nginx/README.md; the outcomes — the metrics.go metrics
// on :9107).
//
// Live check resources (T-220): the pipeline runs on a long-lived
// checker.Runtime circuit (a single resolver chain with a small UDP
// pool, a shared Fetcher with shared limits, a rule cache, one
// render.Manager — assembled lazily by the first check,
// web.New/liveFunc), and the cap on concurrent pipelines is held by the
// web.live_concurrency semaphore: an overload is a fast 503 with
// Retry-After, not a queue up to web.timeout.
package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/checker"
	"crawler/internal/idn"
)

// checkerDB — an adapter from web.DB to the checker package's db.DB
// (the form does not need CopyFrom/Exec and checker does not call them —
// a read-only pipeline; Exec is a fuse returning an error, see the
// implementation).
type checkerDB struct{ db DB }

func (c checkerDB) Ping(ctx context.Context) error { return c.db.Ping(ctx) }

func (c checkerDB) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("web: CopyFrom is not used by the form")
}

func (c checkerDB) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	// Read-only fuse (P3-1 of the T-221 review): the live pipeline does
	// not call Exec; the form log is written by its own path (recordCheck)
	// bypassing the adapter. If checker ever starts writing, the error
	// will expose it immediately instead of passing silently.
	return pgconn.CommandTag{}, fmt.Errorf("web: an unexpected live check Exec: %s", sql)
}

func (c checkerDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return c.db.Query(ctx, sql, args...)
}

// checkResponse — the single JSON response of the form: found/score/
// versions plus per-subdomain records (the contract of the ported
// landing.js).
type checkResponse struct {
	Found      bool             `json:"found"`
	Score      float64          `json:"score"`
	VerMin     string           `json:"ver_min,omitempty"`
	VerMax     string           `json:"ver_max,omitempty"`
	Stored     *storedSummary   `json:"stored,omitempty"`
	Subdomains []checkSubRecord `json:"subdomains"`
	DurationMS int64            `json:"duration_ms"`
}

// storedSummary — the latest crawler episode for the domain (domain_info,
// reduced to the form fields).
type storedSummary struct {
	Status    string   `json:"status"`
	CheckedAt string   `json:"checked_at,omitempty"`
	Score     *float64 `json:"score,omitempty"`
	VerMin    string   `json:"ver_min,omitempty"`
	VerMax    string   `json:"ver_max,omitempty"`
}

// checkSubRecord — a subdomain record in the response.
type checkSubRecord struct {
	Subdomain string             `json:"subdomain"`
	Variant   string             `json:"variant"`
	Status    string             `json:"status"`
	FinalURL  string             `json:"final_url,omitempty"`
	Redirects []redirectStepJSON `json:"redirects,omitempty"`
	TLSValid  *bool              `json:"tls_valid,omitempty"`
	Score     float64            `json:"score"`
	VerMin    string             `json:"ver_min,omitempty"`
	VerMax    string             `json:"ver_max,omitempty"`
	Signals   []signalJSON       `json:"signals"`
	Rendered  bool               `json:"rendered"`
}

type redirectStepJSON struct {
	URL    string `json:"url"`
	Status int    `json:"status"`
}

type signalJSON struct {
	Rule     string  `json:"rule"`
	Weight   float64 `json:"weight"`
	Evidence string  `json:"evidence"`
}

// errLiveOverloaded — the semaphore of concurrent live checks is full
// (T-220): a fast 503 with Retry-After instead of waiting for a slot up
// to web.timeout.
var errLiveOverloaded = errors.New("the live check is overloaded")

// handleCheckDomainLive — T-123: validation → stored → live (timeout
// web.timeout, the web.live_concurrency semaphore) → a web_checks
// record → JSON. Errors: 422 (validation, in the landing.js format),
// 429 (rate limit, +Retry-After), 503 (live check overload,
// +Retry-After — to all clients: a 200 with found=false would read as
// "Laravel not found" for a domain with stored data, and landing.js
// does not show the stored part), 502 (the live check failed
// infrastructurally and the domain is unknown to the pipeline). All
// error outcomes — JSON {"error": code} with Content-Type
// application/json (checkWriteError, T-227).
func (s *Server) handleCheckDomainLive(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ip, isIP := clientIP(r)
	if !s.limiter.allow(ip, time.Now()) {
		s.met.Outcome(checkResultRateLimited)
		w.Header().Set("Retry-After", "60")
		checkWriteError(w, "rate_limited", http.StatusTooManyRequests)
		return
	}

	var req struct {
		Domain string `json:"domain"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		s.met.Outcome(checkResultInvalid)
		checkWriteError(w, "invalid", http.StatusUnprocessableEntity)
		return
	}
	normalized, err := validateCheckInput(req.Domain)
	if err != nil {
		s.met.Outcome(checkResultInvalid)
		checkWriteError(w, "invalid", http.StatusUnprocessableEntity)
		return
	}

	resp := checkResponse{Subdomains: []checkSubRecord{}}

	// (1) stored: the latest crawler episode, if the domain is known.
	if st := s.fetchStored(r.Context(), normalized); st != nil {
		resp.Stored = st
	}

	// (2) live: the full pipeline with the web.timeout cap under the
	// concurrency semaphore (T-220): the pipeline holds network/memory,
	// an overload answers fast (503 with Retry-After) instead of building
	// up a queue until the timeout. The pipeline itself runs on the shared
	// long-lived checker.Runtime circuit — a single resolver chain, a
	// shared Fetcher, a rule cache, one render.Manager per service.
	var (
		live    *checker.Result
		liveErr error
	)
	select {
	case s.liveSem <- struct{}{}:
		func() {
			defer func() { <-s.liveSem }()
			fn, err := s.liveFunc()
			if err != nil {
				liveErr = err
				return
			}
			liveCtx, cancel := context.WithTimeout(r.Context(), s.cfg.Web.Timeout.D())
			defer cancel()
			live, liveErr = fn(liveCtx, s.cfg, checkerDB{s.db}, normalized, s.opt.Log)
		}()
	default:
		liveErr = errLiveOverloaded
		s.opt.Log.Warn("form: the live check is overloaded", "domain", normalized,
			"limit", cap(s.liveSem))
	}

	if liveErr == nil {
		resp.Found = live.Found
		resp.Score = live.Score
		resp.VerMin, resp.VerMax = live.VerMin, live.VerMax
		resp.DurationMS = live.TotalMS
		for i := range live.Records {
			rec := &live.Records[i]
			sub := checkSubRecord{
				Subdomain: rec.Label,
				Variant:   rec.Variant,
				Status:    rec.Status,
				FinalURL:  rec.FinalURL,
				Score:     rec.Score,
				VerMin:    rec.VerMin,
				VerMax:    rec.VerMax,
				Signals:   make([]signalJSON, 0, len(rec.Signals)),
				Rendered:  rec.Rendered,
			}
			if rec.HasTLS {
				v := rec.TLSValid
				sub.TLSValid = &v
			}
			for _, step := range rec.Redirects {
				sub.Redirects = append(sub.Redirects, redirectStepJSON{URL: step.URL, Status: step.Status})
			}
			for _, ev := range rec.Signals {
				sub.Signals = append(sub.Signals, signalJSON{Rule: ev.Rule, Weight: ev.Weight, Evidence: ev.Evidence})
			}
			resp.Subdomains = append(resp.Subdomains, sub)
		}
	}
	// A live check failure does not collapse the response: we return the
	// stored part with a marker; a fully empty result without stored —
	// 502.

	// (3) the web_checks log (T-120): best-effort — a write failure does
	// not break the response to the visitor. An overload is not logged:
	// there was no check, and a found=false/score=0 row would corrupt the
	// audit and add a DB INSERT to the "fast" 503 path. An address without
	// an IP (a unix-socket peer without a valid proxy header, T-221) is
	// not written at all: the ip_address inet NOT NULL column will not
	// accept an arbitrary string — the skip is explicit (Warn + the
	// non_ip metric), not silent.
	if !errors.Is(liveErr, errLiveOverloaded) {
		if isIP {
			s.recordCheck(r.Context(), normalized, resp.Found, resp.Score, resp.DurationMS, ip)
		} else {
			s.opt.Log.Warn("form: the web_checks journal entry skipped — the client address is not an IP (a peer without a valid proxy header)",
				"domain", normalized)
			s.met.Failure(checkErrNonIP)
		}
		s.met.Duration(time.Since(start))
	}

	if liveErr != nil {
		if errors.Is(liveErr, errLiveOverloaded) {
			// Overload — a fast rejection to all clients with a retry
			// hint: regardless of the stored part (landing.js does not
			// show it; a 200 with found=false would read as "Laravel
			// not found").
			s.met.Outcome(checkResultBusy)
			w.Header().Set("Retry-After", "5")
			checkWriteError(w, "busy", http.StatusServiceUnavailable)
			return
		}
		s.met.Failure(checkErrLive)
		if resp.Stored == nil {
			s.met.Outcome(checkResultFailed)
			s.opt.Log.Warn("form: the live check failed", "domain", normalized, "err", liveErr)
			checkWriteError(w, "check_failed", http.StatusBadGateway)
			return
		}
		s.met.Outcome(checkResultStoredOnly)
	} else {
		s.met.Outcome(checkResultOK)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(resp)
}

// validateCheckInput — validation modeled on the larasites
// CheckDomainRequest: trim, a trailing dot cut, an http(s) prefix → host,
// an ASCII domain or an IDN (≥2 non-empty labels), a length cap. Returns
// the punycode name.
func validateCheckInput(input string) (string, error) {
	s := strings.TrimSpace(input)
	s = strings.TrimSuffix(s, ".")
	if s == "" || len(s) > 2048 {
		return "", fmt.Errorf("an empty or oversized input")
	}
	s = strings.ToLower(s)
	// http(s)://host/… → host (the port is cut off).
	if i := strings.Index(s, "://"); i > 0 {
		rest := s[i+3:]
		if host, _, ok := strings.Cut(rest, "/"); ok || host != "" {
			s = host
		}
		if h, _, err := net.SplitHostPort(s); err == nil {
			s = h
		}
	}
	if strings.ContainsAny(s, " /\\@:") {
		return "", fmt.Errorf("junk in the name")
	}
	res, err := idn.Normalize(s)
	if err != nil {
		return "", fmt.Errorf("not a domain: %w", err)
	}
	return res.ASCII, nil
}

// fetchStored — reading domain_info (T-082): reduced to the form fields.
func (s *Server) fetchStored(ctx context.Context, domain string) *storedSummary {
	rows, err := s.db.Query(ctx,
		"SELECT domain_info($1::text)", domain)
	if err != nil {
		return nil // best-effort: the stored part is optional
	}
	defer rows.Close()
	if !rows.Next() {
		return nil
	}
	var raw []byte
	if err := rows.Scan(&raw); err != nil || len(raw) == 0 {
		return nil
	}
	var full struct {
		Domain struct {
			StatusName string `json:"status_name"`
			LastCheck  string `json:"last_checked_at"`
		} `json:"domain"`
		Laravel *struct {
			Score  float64 `json:"score"`
			VerMin string  `json:"ver_min"`
			VerMax string  `json:"ver_max"`
		} `json:"laravel"`
	}
	if err := json.Unmarshal(raw, &full); err != nil {
		return nil
	}
	st := &storedSummary{Status: full.Domain.StatusName, CheckedAt: full.Domain.LastCheck}
	if full.Laravel != nil {
		sc := full.Laravel.Score
		st.Score = &sc
		st.VerMin, st.VerMax = full.Laravel.VerMin, full.Laravel.VerMax
	}
	return st
}

// recordCheck — an INSERT into web_checks (T-120): best-effort — a write
// failure does not break the response to the visitor, but it is logged
// and counted as a metric (T-221: before that, the INSERT went through
// Query without reading rows — the error was swallowed and the log
// silently went empty). Exec, not Query: an INSERT returns no rows.
// The request context is canceled by the visitor leaving (a live check
// up to web.timeout), while the log is the audit of "who/what/when
// checked": the INSERT lives in its own short window independent of the
// client disconnect (P3-5 of the T-221 review).
func (s *Server) recordCheck(ctx context.Context, punycode string, found bool, score float64, durMS int64, ip string) {
	jctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	if _, err := s.db.Exec(jctx,
		"INSERT INTO web_checks (domain, punycode_domain, ip_address, found, score, duration_ms) "+
			"VALUES ($1::text, $2::text, $3::inet, $4, $5::real, $6::int4)",
		punycode, punycode, ip, found, score, int32(durMS)); err != nil {
		// The domain and the cause — without the visitor address (no
		// personal data in the log; the address stays only in
		// web_checks).
		s.opt.Log.Warn("form: the web_checks journal entry was not written", "domain", punycode, "err", err)
		s.met.Failure(checkErrJournal)
	}
}

// unixLocalKey — the rate limit bucket key for a unix-socket peer
// without a valid X-Real-IP/X-Forwarded-For (T-221): a string that is
// definitely not an IP — it is not written into inet, and all such
// visitors share one bucket (the proxy is configured without headers —
// a shared window is fairer than circumventing the limit).
const unixLocalKey = "unix-local"

// clientIP — the client address for the rate limit and the web_checks
// log. The second return value — whether the result is an IP at all:
// false for a unix-socket peer without a valid proxy header and a junk
// RemoteAddr (it is not written into inet).
//
// A unix socket (T-221, deploy/nginx/README.md §"The unix-socket
// variant") is recognized positively — by the connection's local
// address, which http.Server puts into the context: for a unix-socket
// peer it is a *net.UnixAddr (the RemoteAddr in that case is the "@"
// autobind or the socket path — not an IP; a path with a colon would
// even parse as host:port by SplitHostPort). The unix-socket peer is
// nginx itself on the same host: a trusted proxy, just like loopback
// TCP. The visitor address — X-Real-IP, then X-Forwarded-For (a fallback
// for configurations without X-Real-IP), only a valid net.ParseIP;
// invalid or missing — the unixLocalKey label key, without a log record
// (inet NOT NULL).
//
// The TCP branch (T-124) unchanged: behind the reverse proxy all
// connections come from the nginx loopback — the visitor address is
// carried by X-Real-IP (the sample nginx config overwrites it with the
// $remote_addr value — it cannot be forged from outside, and the service
// itself listens on loopback, with no direct visitor connections). The
// header is trusted only from a loopback peer and only for a valid IP;
// otherwise — RemoteAddr (a header forged from outside and from foreign
// addresses does not spend someone else's bucket).
func clientIP(r *http.Request) (string, bool) {
	if isUnixPeer(r) {
		// A unix-socket peer: a trusted proxy.
		if pip := proxyHeaderIP(r.Header); pip != "" {
			return pip, true
		}
		return unixLocalKey, false
	}
	host := remoteHost(r)
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		if pip := net.ParseIP(strings.TrimSpace(r.Header.Get("X-Real-IP"))); pip != nil {
			return pip.String(), true
		}
	}
	return host, net.ParseIP(host) != nil
}

// isUnixPeer — the connection came through a unix socket (T-221): the
// peer is nginx itself on this host, a trusted proxy. Positive
// recognition — by the connection's local address from the context,
// which http.Server puts there: for a unix-socket peer it is a
// *net.UnixAddr (the RemoteAddr in that case is the "@" autobind or the
// socket path — not an IP; a path with a colon would even parse as
// host:port by SplitHostPort).
func isUnixPeer(r *http.Request) bool {
	_, ok := r.Context().Value(http.LocalAddrContextKey).(*net.UnixAddr)
	return ok
}

// remoteHost — the RemoteAddr host without the port; a string not in
// the host:port form is returned as is (SplitHostPort errors on it).
func remoteHost(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// trustedProxyPeer — the connection peer is a trusted reverse proxy of
// this host (T-227): nginx behind TCP loopback (deploy/nginx/
// crawler.example.conf) or behind a unix socket (deploy/nginx/README.md).
// The headers the proxy sets (X-Real-IP, X-Forwarded-For,
// X-Forwarded-Proto) are trusted only from such a peer — a direct
// visitor and a foreign address may send their own values, but they are
// not read (the same principle as the visitor address in clientIP).
func trustedProxyPeer(r *http.Request) bool {
	if isUnixPeer(r) {
		return true
	}
	ip := net.ParseIP(remoteHost(r))
	return ip != nil && ip.IsLoopback()
}

// proxyHeaderIP — the visitor address from the proxy headers (a unix
// peer): X-Real-IP, then X-Forwarded-For. XFF takes the LAST entry — it
// was appended by our nginx ($proxy_add_x_forwarded_for, P1-1 of the
// T-221 review): the start of the list came from the client and is not
// trusted — otherwise a visitor would send each request with a new
// value and circumvent the token bucket. Only a valid net.ParseIP is
// returned; an invalid or missing header — "".
func proxyHeaderIP(h http.Header) string {
	if pip := net.ParseIP(strings.TrimSpace(h.Get("X-Real-IP"))); pip != nil {
		return pip.String()
	}
	if xff := strings.TrimSpace(h.Get("X-Forwarded-For")); xff != "" {
		last := xff[strings.LastIndex(xff, ",")+1:]
		if pip := net.ParseIP(strings.TrimSpace(last)); pip != nil {
			return pip.String()
		}
	}
	return ""
}

// checkWriteError — the JSON error body of the form with a correct
// Content-Type (a single format for all outcomes, including 429/503/502 —
// before T-227 they went through http.Error with text/plain; landing.js
// distinguishes the outcomes by status, the body is the contract for
// other JSON clients). nosniff — as http.Error set it, the behavior
// parity before the unification.
func checkWriteError(w http.ResponseWriter, code string, status int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"error":"` + code + `"}`))
}

// --- token bucket: 4 requests/min per IP ------------------------------------

type tokenBucketLimiter struct {
	rate  int
	per   time.Duration
	mu    sync.Mutex
	state map[string]*bucketState
}

type bucketState struct {
	tokens float64
	last   time.Time
}

func newLimiter(rate int, per time.Duration) *tokenBucketLimiter {
	return &tokenBucketLimiter{rate: rate, per: per, state: make(map[string]*bucketState)}
}

func (l *tokenBucketLimiter) allow(ip string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	st, ok := l.state[ip]
	if !ok {
		st = &bucketState{tokens: float64(l.rate) - 1, last: now}
		l.state[ip] = st
		// Lazy cleanup: the map grows with the number of unique IPs;
		// entries older than the window are deleted on a pass (the form
		// is not a hot path).
		if len(l.state) > 4096 {
			for k, v := range l.state {
				if now.Sub(v.last) > l.per*4 {
					delete(l.state, k)
				}
			}
		}
		return true
	}
	refill := float64(l.rate) * now.Sub(st.last).Seconds() / l.per.Seconds()
	st.tokens = min(float64(l.rate), st.tokens+refill)
	st.last = now
	if st.tokens < 1 {
		return false
	}
	st.tokens--
	return true
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

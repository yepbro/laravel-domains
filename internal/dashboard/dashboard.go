// Package dashboard — the operator web dashboard of the pipeline
// (dashboard spec; stage 15): a single pipeline status page behind a
// password, strictly read-only (dashboard spec §1.3). The T-128
// framework — a public http.Server on dashboard.listen with graceful
// stop on ctx cancellation (the lifecycle precedent — internal/web,
// T-121), Basic authorization + a cookie session and brute-force
// protection (dashboard spec §7). Snapshot collection for /api/status
// — T-129 (snapshot.go — data, collector.go — the background loop),
// the front end — assets.go (T-130), deployment — T-131.
//
// The internal/service framework gives one HTTP port per service and
// attaches /metrics with probe endpoints to it — exposing it outside
// is forbidden (metrics are an internal surface, spec §9), so
// MetricsAddr stays on loopback (127.0.0.1:9108, dashboard spec §4.2),
// and the operator is served by dashboard.listen.
//
// Routes (dashboard spec §6.1):
//
//   - GET /            → the dashboard HTML page (assets/page.html);
//   - GET /assets/*    — front-end statics from embed (app.js,
//     style.css);
//   - GET /api/status  — the JSON of the latest snapshot (T-129):
//     until the first successful collection — 503 (dashboard spec
//     §5.9);
//   - GET /healthz     — readiness: a PostgreSQL ping (503 on
//     failure);
//   - GET /livez       — liveness;
//   - GET /robots.txt  — `Disallow: /` (an internal service).
//
// The first three are behind authorization, the utility ones are not.
// Non-GET/HEAD — 405 (ServeMux method patterns); the security headers
// of all responses and the server timeouts — dashboard spec §6.3.
package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"crawler/internal/config"
	"crawler/internal/dashboard/auth"
)

// Pinger — the DB readiness dependency (*pgxpool.Pool satisfies it):
// the pool is needed by the /healthz probe (dashboard spec §6.1) and
// the snapshot collector (collector.go).
type Pinger interface {
	Ping(ctx context.Context) error
}

// Options — service parameters.
type Options struct {
	// Log — the service logger; nil — records suppressed
	// (slog.DiscardHandler).
	Log *slog.Logger
	// Now — the clock: issuing/checking cookie sessions (§7.5) and the
	// brute-force protection window (§7.6); nil — time.Now. A test
	// seam.
	Now func() time.Time
	// Snapshots — the source of /api/status snapshots (the T-129
	// collection loop, *Collector); nil — no snapshots: 503
	// "snapshot_not_ready" (the §5.9 semantics before the first
	// collection — the T-128 stub precedent).
	Snapshots SnapshotSource
}

// passwordVerifier — operator password verification (§7.4);
// auth.PasswordHash satisfies the interface. A separate type rather
// than the concrete structure — a seam for the brute-force protection
// tests (§7.6, T-231): the substitution counts KDF calls without
// performing a real PBKDF2.
type passwordVerifier interface {
	Verify(password string) bool
}

// Server — the public HTTP server of the dashboard.
type Server struct {
	db       Pinger
	snaps    SnapshotSource
	opt      Options
	username string
	hash     auth.PasswordHash
	// verify — verification of Basic request passwords; New sets hash.
	// The hash and verify fields are separated: hash.Key is needed by
	// the cookie session signature (§7.5) and stays a concrete type.
	verify  passwordVerifier
	limiter *loginLimiter
}

// ParseCredentials — the startup check of credentials (dashboard spec
// §7.7): an empty username/password_hash or an unparseable hash string
// is a startup error. Called from cmd/dashboard before service.Run
// (failing before migrations and listeners) and again by New (tests);
// the common config.Validate deliberately does not check credentials —
// otherwise the requirement would apply to every service of the
// project.
func ParseCredentials(cfg *config.Config) (auth.PasswordHash, error) {
	if cfg.Dashboard.Username == "" {
		return auth.PasswordHash{}, errors.New("dashboard: username пуст — креды оператора обязательны (ТЗД §7.7)")
	}
	// The Basic scheme splits credentials at the first ":" (r.BasicAuth):
	// a name with a colon can never match — we refuse immediately, with
	// a clear error, instead of a "silent" login failure.
	if strings.Contains(cfg.Dashboard.Username, ":") {
		return auth.PasswordHash{}, errors.New("dashboard: username не может содержать «:» (синтаксис Basic user:pass)")
	}
	if cfg.Dashboard.PasswordHash == "" {
		return auth.PasswordHash{}, errors.New("dashboard: password_hash пуст — креды оператора обязательны (ТЗД §7.7)")
	}
	hash, err := auth.Parse(cfg.Dashboard.PasswordHash)
	if err != nil {
		return auth.PasswordHash{}, fmt.Errorf("dashboard: password_hash: %w", err)
	}
	return hash, nil
}

// New assembles the server: db — the DB pool (the /healthz probe),
// cfg — the dashboard section configuration; credentials go through
// the §7.7 startup check (see ParseCredentials).
func New(db Pinger, cfg *config.Config, opts Options) (*Server, error) {
	if db == nil {
		return nil, errors.New("dashboard: пул БД не задан (postgres.dsn обязателен)")
	}
	if cfg == nil {
		return nil, errors.New("dashboard: конфигурация не задана")
	}
	hash, err := ParseCredentials(cfg)
	if err != nil {
		return nil, err
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Server{
		db:       db,
		snaps:    opts.Snapshots,
		opt:      opts,
		username: cfg.Dashboard.Username,
		hash:     hash,
		verify:   hash,
		limiter:  newLoginLimiter(maxLoginFailures, loginWindow),
	}, nil
}

// contentSecurityPolicy — the CSP of all dashboard responses (dashboard
// spec §6.3, verbatim): the outside world is closed,
// scripts/styles/connections — own only, data: images — the front-end
// inline SVG (T-130).
const contentSecurityPolicy = `default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:`

// Handler — the router of the public surface (tests — httptest).
// ServeMux method patterns themselves answer 405 to non-GET/HEAD
// (§6.3), HEAD is served by the GET patterns.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	// Without authorization (dashboard spec §6.1).
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /livez", s.handleLivez)
	mux.HandleFunc("GET /robots.txt", s.handleRobots)
	// Behind authorization (dashboard spec §6.1, §7).
	mux.Handle("GET /{$}", s.auth(s.handleIndex))
	mux.Handle("GET /assets/", s.auth(s.handleAssets))
	mux.Handle("GET /api/status", s.auth(s.handleAPIStatus))
	return s.secureHeaders(mux)
}

// secureHeaders — security headers of all responses, including
// 401/429/405 (dashboard spec §6.3).
func (s *Server) secureHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Content-Security-Policy", contentSecurityPolicy)
		next.ServeHTTP(w, r)
	})
}

// Run — the lifecycle: the dashboard.listen listener ([host]:port,
// dashboard spec §8.3), graceful stop on ctx cancellation (the
// framework shutdown budget is 10 s: a Shutdown with a smaller window,
// started responses finish). Called from OnRun of cmd/dashboard.
// Server timeouts — dashboard spec §6.3: ReadHeader 5 s, Read 10 s,
// Write 15 s, Idle 60 s.
func (s *Server) Run(ctx context.Context, listen string) error {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("dashboard: слушатель %s: %w", listen, err)
	}
	srv := &http.Server{
		Handler: s.Handler(),
		// Server timeouts (dashboard spec §6.3): the 5/10/15/60 s values
		// are fixed by the spec and observable only through connection
		// behavior over time (a slow client is cut off after N
		// seconds) — distinguishing 4/5/6 s, let alone a disabled
		// timeout (5/time.Second = 0), requires real pauses in
		// seconds, incompatible with hermetic tests (T-M11).
		// mutator-disable-next-line arithmetic/base, numbers/decrementer, numbers/incrementer
		ReadHeaderTimeout: 5 * time.Second,
		// mutator-disable-next-line arithmetic/base, numbers/decrementer, numbers/incrementer
		ReadTimeout: 10 * time.Second,
		// mutator-disable-next-line arithmetic/base, numbers/decrementer, numbers/incrementer
		WriteTimeout: 15 * time.Second,
		// mutator-disable-next-line arithmetic/base, numbers/decrementer, numbers/incrementer
		IdleTimeout: 60 * time.Second,
	}
	// Capacity 2 is equivalent to 1 (T-M11): the Serve goroutine makes
	// exactly one send, the receiver is the single select in Run; at
	// capacity ≥ 1 the send never blocks, and nobody reads from the
	// channel afterwards. Capacity 0 is not equivalent (the sender
	// would hang after Run returned — a leak) and is covered by a
	// test: TestRunGracefulStop waits for the goroutine counter to
	// return to baseline.
	// mutator-disable-next-line numbers/incrementer
	done := make(chan error, 1)
	go func() {
		err := srv.Serve(ln)
		// Serve closes the listener itself; ErrServerClosed — the normal
		// exit after Shutdown, not an error.
		//
		// Kill (T-M11): "err != nil" → "true" is equivalent — Serve
		// always returns a non-nil error (the net/http contract). The
		// Serve error branch is unreachable hermetically: only a
		// non-temporary Accept failure can return an error (the
		// listener closed outside Shutdown; temporary failures like
		// EMFILE are retried by Serve itself), while the listener is
		// created and closed inside Run and is not available to the
		// test; ErrServerClosed happens only after Shutdown, which Run
		// calls already after selecting ctx.Done — sending the error
		// into done after Run returned is unobservable.
		// mutator-disable-next-line expression/remove, branch/if
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			done <- fmt.Errorf("dashboard: сервер %s: %w", listen, err)
			return
		}
		done <- nil
	}()
	s.opt.Log.Info("публичный сервис дашборда запущен", "listen", listen)

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		// Graceful shutdown (spec 3.4): started responses finish within
		// a window smaller than the framework shutdown budget.
		// The 8 s budget is observable only by a Shutdown error with
		// responses that did not finish within the budget — requires a
		// real live connection and a 7–8 s pause, incompatible with
		// hermetic tests; the 8/time.Second = 0 variant (the budget
		// expired instantly) is distinguishable only by the same pause
		// (T-M11).
		// mutator-disable-next-line arithmetic/base, numbers/decrementer, numbers/incrementer
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		// A Shutdown error — only the budget expiring with a response
		// still hanging: unreachable in CI (see the budget comment
		// above, T-M11).
		// mutator-disable-next-line branch/if
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("dashboard: остановка: %w", err)
		}
		return nil
	}
}

// handleIndex and handleAssets (the page and statics from embed) —
// assets.go (T-130).

// handleAPIStatus — GET /api/status: the JSON of the latest snapshot
// (dashboard spec §6.2): the zones/totals/throughput/rates sections —
// like crawlerctl status --json (status.SummaryJSON, T-129), plus
// statuses/Laravel/ingest/health and the map of section errors; null
// for unavailable values. Until the first successful collection — 503
// (dashboard spec §5.9); Cache-Control: no-store — periodic polling
// caches neither the snapshot nor the refusal.
func (s *Server) handleAPIStatus(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	var snap *Snapshot
	if s.snaps != nil {
		snap = s.snaps.Current()
	}
	if snap == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"error":"snapshot_not_ready"}`+"\n")
		return
	}
	// The snapshot is published by whole atomic replacement — the
	// marshaling is consistent to a single collection moment;
	// EscapeHTML off — CLI style. Encoding into a buffer: an error (a
	// hypothetical NaN from real columns) must not serve a partial
	// JSON with a 200 status.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(snap); err != nil {
		http.Error(w, "snapshot encode failed", http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(buf.Bytes())
}

// handleHealthz — readiness without authorization (dashboard spec
// §6.1): a PostgreSQL ping with a short timeout (the operator does
// not wait longer than a second for DB recovery — 503 comes faster;
// internal/web style).
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.db.Ping(ctx); err != nil {
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	// Equivalent mutant (T-M11): writing the body without a
	// Content-Type, Go detects the type itself (http.DetectContentType)
	// — for "ok" it is the same text/plain; charset=utf-8; the explicit
	// Set merely fixes the probe contract in advance.
	// mutator-disable-next-line statement/remove
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok"))
}

// handleLivez — liveness without authorization (dashboard spec §6.1):
// the process and its HTTP loop are alive; dependencies are
// deliberately not checked — the internal/service framework probe
// semantics.
func (s *Server) handleLivez(w http.ResponseWriter, _ *http.Request) {
	// Equivalent mutant (T-M11): sniffing "ok" yields the same
	// text/plain; charset=utf-8 (see handleHealthz).
	// mutator-disable-next-line statement/remove
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok"))
}

// handleRobots — GET /robots.txt without authorization (dashboard spec
// §6.1): the dashboard is an internal service, a full disallow.
func (s *Server) handleRobots(w http.ResponseWriter, _ *http.Request) {
	// Equivalent mutant (T-M11): sniffing the robots body yields the
	// same text/plain; charset=utf-8 (see handleHealthz).
	// mutator-disable-next-line statement/remove
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "User-agent: *\nDisallow: /\n")
}

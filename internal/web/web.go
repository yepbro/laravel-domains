// Package web — the public API service (stage 13, T-121/T-122; a product
// extension outside the main spec — ported from larasites). Until L-08 it
// also served the SSR landing (the locale pages, the static assets, the
// SEO service files); the landing now lives as static sites in the
// separate ld-landing repo (TZ-ld-landing §7.3), and this package serves
// only the JSON surface those sites fetch.
//
// Framework (T-121): the public listener is its own http.Server inside
// OnRun with a graceful stop on ctx cancellation. The internal/service
// framework provides one HTTP port per service and hangs /metrics with
// probe endpoints off it — exposing it publicly is not allowed (metrics
// are an internal surface, spec §9), so MetricsAddr stays on loopback
// (:9107, cmd/web) while web.listen serves the visitor. The precedent
// for lifecycle management inside OnRun — the worker clients (Redis
// queues).
//
// Routes:
//
//   - POST /api/check-domain — the live check of the manual form
//     (T-123; the form UI itself is a part of the static landing);
//   - GET /api/v1/landing/stats — the landing data as JSON for the
//     static landing of the ld-landing repo (L-03: the same
//     FetchLandingData pass, a stats view model, max-age=60);
//   - GET /healthz     — readiness: a PostgreSQL ping (503 on failure).
//
// The former landing routes (GET /, /{locale}, /static/*, /robots.txt,
// /sitemap.xml) are gone: a request to them is the router's honest 404.
//
// The /api/* routes are wrapped with the CORS allowlist
// web.cors.allowed_origins (L-04, TZ-ld-landing §7.2; internal/web/cors.go):
// an empty list — CORS is off, the behavior before L-04.
package web

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"crawler/internal/checker"
	"crawler/internal/config"
	"crawler/internal/metrics"
)

// Pinger — the readiness part of the DB dependency.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Querier — the landing data selects (*pgxpool.Pool satisfies it
// together with Pinger). Split out for tests without PostgreSQL.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Execer — the form writes (*pgxpool.Pool satisfies it): the web_checks
// log (T-120) — the only write point of the service.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// DB — the service dependency: a readiness ping + the page data + the
// form log.
type DB interface {
	Pinger
	Querier
	Execer
}

// Options — the service options.
type Options struct {
	// Log — the service logger; nil — records suppressed
	// (slog.DiscardHandler).
	Log *slog.Logger
	// Metrics — the service Prometheus registry (T-221): New builds the
	// form instruments from it (outcomes/failures/duration); nil — no
	// metrics (isolated tests, no-op methods).
	Metrics *metrics.Registry
}

// Server — the public API HTTP server.
type Server struct {
	db      DB
	opt     Options
	cfg     *config.Config
	limiter *tokenBucketLimiter
	// met — the form instruments (T-221); nil — no-op.
	met *Metrics
	// liveSem — the semaphore of concurrent live checks (T-220): the
	// pipeline holds network/memory (requests of both schemes with page
	// bodies, the SPA render); the excess is a fast 503 rejection with
	// Retry-After, not a queue up to web.timeout. The size —
	// web.live_concurrency.
	liveSem chan struct{}

	// liveCheck — the live check seam (T-123): nil — the production path
	// through the lazily assembled checker.Runtime circuit (T-220, one
	// per service); tests substitute a network-free fake (in the style
	// of the workers' ContentGetter).
	liveCheck checker.LiveFunc

	// liveMu guards the lazy circuit assembly, its close, and lifeCtx.
	liveMu      sync.Mutex
	liveCircuit checker.LiveCircuit
	liveClosed  bool            // the circuit is closed forever (the Run exit) — no more assembly
	lifeCtx     context.Context // the circuit lifetime context (set in Run)

	// corsOrigins — the CORS allowlist of the /api/* surface (L-04):
	// an exact origin → allowed; the set is built once in New from
	// web.cors.allowed_origins. nil/empty — CORS is off (no headers, no
	// OPTIONS special-casing — the behavior before L-04).
	corsOrigins map[string]bool
}

// New assembles the server: db — the DB pool (a ping + the page data +
// the form log). cfg — the web section configuration (the rate limit,
// the live check timeout, the concurrent live checks cap,
// T-123/T-220). New does not
// assemble the live check circuit (checker.Runtime) — it is created
// lazily by the first live check (liveFunc) and lives until the service
// stops (Run → closeLive); assembly errors do not affect the other
// endpoints.
func New(db DB, cfg *config.Config, opts Options) (*Server, error) {
	if db == nil {
		return nil, errors.New("web: the DB pool is not set (postgres.dsn is mandatory)")
	}
	if cfg == nil {
		return nil, errors.New("web: the configuration is not set")
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	live := cfg.Web.LiveConcurrency
	if live < 1 {
		live = 1 // config without Default/Validate: at least one slot
	}
	// The CORS allowlist (L-04): an exact-match set built once; the
	// empty list leaves it empty — apiCORS then passes the router
	// through unchanged (CORS off). An empty entry is skipped: the
	// lookup of an absent Origin header is also "" and must not match
	// (normalize drops empties; a hand-built Config is also covered).
	cors := make(map[string]bool, len(cfg.Web.CORS.AllowedOrigins))
	for _, o := range cfg.Web.CORS.AllowedOrigins {
		if o == "" {
			continue
		}
		cors[o] = true
	}
	return &Server{
		db:          db,
		opt:         opts,
		cfg:         cfg,
		limiter:     newLimiter(cfg.Web.RateLimit, time.Minute),
		liveSem:     make(chan struct{}, live),
		met:         NewMetrics(opts.Metrics),
		corsOrigins: cors,
	}, nil
}

// newLiveCircuit — the live check circuit assembly seam (T-220):
// production — checker.NewRuntime (a single resolver chain with a small
// UDP pool, a shared Fetcher, a rule cache with reloading, one
// render.Manager); the web tests substitute it, verifying "one circuit
// per service" without the network.
var newLiveCircuit = func(ctx context.Context, cfg *config.Config, log checker.Logger) (checker.LiveCircuit, error) {
	return checker.NewRuntime(ctx, cfg, log, checker.RuntimeOptions{})
}

// errLiveStopped — the live check circuit is closed forever (the Run
// exit, the service is stopping): requests no longer assemble a new
// circuit.
var errLiveStopped = errors.New("the live check is stopped: the service is shutting down")

// liveFunc returns the LiveFunc of the live check: the substituted
// liveCheck seam (tests) or the Run of the lazily assembled circuit.
// The circuit is created on the first live check and reused until the
// service stops; a failed assembly is not remembered — the next request
// tries again (rules/sockets may appear), and the error goes into the
// live check failure semantics (502/stored-only, like a pipeline
// failure). After closeLive there is no assembly (P3-2 of the T-220
// review): Run has already returned, and nobody would close a new
// circuit.
func (s *Server) liveFunc() (checker.LiveFunc, error) {
	if s.liveCheck != nil {
		return s.liveCheck, nil
	}
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	if s.liveClosed {
		return nil, errLiveStopped
	}
	if s.liveCircuit != nil {
		return s.liveCircuit.Run, nil
	}
	life := s.lifeCtx
	if life == nil {
		// A handler without Run — a direct unit call; the production path
		// always goes through Run (lifeCtx is set before the first
		// request).
		life = context.Background()
	}
	c, err := newLiveCircuit(life, s.cfg, s.opt.Log)
	if err != nil {
		return nil, err
	}
	s.liveCircuit = c
	return c.Run, nil
}

// closeLive stops the live check circuit (the Run exit — the service is
// stopping): the UDP pools of the chain, the render, the rule
// reloading. Idempotent; concurrent assembly is excluded by the liveMu
// mutex, and after the close no new circuits are assembled (liveClosed).
func (s *Server) closeLive() {
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	s.liveClosed = true
	if s.liveCircuit == nil {
		return
	}
	c := s.liveCircuit
	s.liveCircuit = nil
	_ = c.Close()
}

// Handler — the router of the public surface (tests — httptest).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("POST /api/check-domain", s.handleCheckDomainLive)
	// The stats endpoint of the static landing (L-03): the path patterns
	// are GET-only, a non-GET method — the mux answers an honest 405.
	mux.HandleFunc("GET /api/v1/landing/stats", s.handleLandingStats)
	// The /api/* surface carries the CORS policy (L-04, TZ-ld-landing
	// §7.2): with a non-empty web.cors.allowed_origins the wrapper adds
	// the allowlist echo and answers preflights; /healthz passes through
	// untouched. A non-POST method on /api/check-domain — the mux's own
	// 405 (Go 1.22: the path is registered with another method).
	return s.apiCORS(mux)
}

// Run — the lifecycle: the web.listen listener, a graceful stop on ctx
// cancellation (the framework stop budget — 10s: a Shutdown with a
// smaller window, in-flight responses finish). Called from OnRun of
// cmd/web. The live check circuit lifetime context (T-220) also lives
// here: the circuit receives it on lazy assembly, and on the Run exit
// it is closed (closeLive — the sockets of the resolver chain, the
// render, the rule reloading).
//
// The address is [host]:port (TCP) or an absolute path (a unix socket —
// a deploy variant behind nginx without occupying a TCP port, T-124;
// the config validation distinguishes the form). An orphaned socket
// file (a crash/SIGKILL of the previous run — the defer would no longer
// fire) is removed before Listen, otherwise a restart by
// Restart=on-failure would loop on address already in use until
// StartLimitBurst; it is also removed after a graceful stop.
func (s *Server) Run(ctx context.Context, listen string) error {
	s.liveMu.Lock()
	s.lifeCtx = ctx
	s.liveMu.Unlock()
	defer s.closeLive()
	network := "tcp"
	if strings.HasPrefix(listen, "/") {
		network = "unix"
	}
	if network == "unix" {
		// The path belongs to the service (the unit's RuntimeDirectory /
		// t.TempDir in tests) — removing someone else's live socket is
		// not a scenario.
		if err := os.Remove(listen); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("web: a stale socket %s: %w", listen, err)
		}
	}
	ln, err := net.Listen(network, listen)
	if err != nil {
		return fmt.Errorf("web: listener %s: %w", listen, err)
	}
	if network == "unix" {
		// A socket file removal error is no reason to fail the Run
		// result — a restart will overwrite it / point the operator to
		// it anyway.
		defer func() { _ = os.Remove(listen) }()
	}
	srv := &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second, // slowloris cap (§9)
	}
	done := make(chan error, 1)
	go func() {
		err := srv.Serve(ln)
		// Serve closes the listener itself; ErrServerClosed — the normal
		// exit after Shutdown, not an error.
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			done <- fmt.Errorf("web: server %s: %w", listen, err)
			return
		}
		done <- nil
	}()
	s.opt.Log.Info("the public web API service started", "listen", listen)

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		// Graceful shutdown (spec 3.4): in-flight responses finish within
		// a window smaller than the framework stop budget.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("web: shutdown: %w", err)
		}
		return nil
	}
}

// The former landing handlers (GET /, /{locale}, /static/*, /robots.txt,
// /sitemap.xml) were removed in L-08 (TZ-ld-landing §7.3): the landing is
// static now — the ld-landing repo; those paths are the router's 404.

// handleCheckDomain removed (T-123): the form is served by
// handleCheckDomainLive (api.go) — a stored/live hybrid with a rate
// limit.

// handleHealthz — readiness: a PostgreSQL ping with a short timeout
// (the visitor does not wait for the DB recovery longer than a second —
// a 503 is faster).
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.db.Ping(ctx); err != nil {
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok"))
}

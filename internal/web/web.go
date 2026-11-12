// Package web — the public landing service (stage 13, T-121/T-122;
// a product extension outside the main spec — ported from larasites):
// precomputed pipeline counters, the manual domain check form,
// i18n ru/en.
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
//   - GET /            → redirect to the default locale /ru;
//   - GET /{locale}    — the ru|en locale landing (T-122: the ported
//     larasites markup — stats/cloud/zone table, latest finds, the
//     form, SEO canonical/hreflang/OG/Twitter);
//   - POST /api/check-domain — the live check (T-123, a 501 stub);
//   - GET /healthz     — readiness: a PostgreSQL ping (503 on failure);
//   - GET /robots.txt  — static assets from embed (T-124);
//   - GET /sitemap.xml — a minimal sitemap of the locales (T-124);
//   - GET /static/*    — the landing static assets (go:embed; app.css —
//     a Tailwind generated artifact, not stored in git — make web-css /
//     the css stage of Dockerfile.go, T-124).
package web

import (
	"context"
	"embed"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"html/template"
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

// Locales — the locales supported by the landing (a stage-13 pinned
// decision: i18n ru+en; ru is the default).
var Locales = []string{"ru", "en"}

// DefaultLocale — where GET / redirects to.
const DefaultLocale = "ru"

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

// Server — the public landing HTTP server.
type Server struct {
	db      DB
	opt     Options
	tpl     *template.Template
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
}

// landingVM — the landing render data: the locale, the locale list, the
// page data, and the locale dictionary (the Msg/MsgRaw/ToJSON methods
// are called from the template as {{$.Msg "key"}} — data methods, not a
// FuncMap: the template is parsed once without a locale).
type landingVM struct {
	Locale  string
	Locales []string
	Data    LandingData
	msgs    messages
}

// Msg — a locale string, safely escaped.
func (v landingVM) Msg(key string) template.HTML {
	return template.HTML(template.HTMLEscapeString(v.msgs[key]))
}

// MsgRaw — a locale string without escaping (for JSON inserts).
func (v landingVM) MsgRaw(key string) string { return v.msgs[key] }

// ToJSON — the JSON literal of a string for <script type="application/json">.
func (v landingVM) ToJSON(s string) template.HTML {
	b, err := json.Marshal(s)
	if err != nil {
		return template.HTML(`""`)
	}
	return template.HTML(b)
}

// New assembles the server: db — the DB pool (a ping + the page data +
// the form log). cfg — the web section configuration (the rate limit,
// the live check timeout, the concurrent live checks cap,
// T-123/T-220). The template is parsed once: the locale arrives as
// data (landingVM), the dictionary is a render method. New does not
// assemble the live check circuit (checker.Runtime) — it is created
// lazily by the first live check (liveFunc) and lives until the service
// stops (Run → closeLive); assembly errors do not affect the other
// pages.
func New(db DB, cfg *config.Config, opts Options) (*Server, error) {
	if db == nil {
		return nil, errors.New("web: пул БД не задан (postgres.dsn обязателен)")
	}
	if cfg == nil {
		return nil, errors.New("web: конфигурация не задана")
	}
	tpl, err := template.ParseFS(templates, "templates/landing.html")
	if err != nil {
		return nil, fmt.Errorf("web: шаблоны: %w", err)
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	live := cfg.Web.LiveConcurrency
	if live < 1 {
		live = 1 // config without Default/Validate: at least one slot
	}
	return &Server{
		db:      db,
		opt:     opts,
		tpl:     tpl,
		cfg:     cfg,
		limiter: newLimiter(cfg.Web.RateLimit, time.Minute),
		liveSem: make(chan struct{}, live),
		met:     NewMetrics(opts.Metrics),
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
var errLiveStopped = errors.New("живая проверка остановлена: сервис завершается")

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

//go:embed templates
var templates embed.FS

//go:embed static
var staticFS embed.FS

// Handler — the router of the public surface (tests — httptest).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("POST /api/check-domain", s.handleCheckDomainLive)
	// A GET on the API endpoint would be intercepted by the landing
	// prefix (GET /) with a misleading 404 — an honest 405 without
	// revealing details.
	mux.HandleFunc("GET /api/check-domain", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	})
	// Exact root routes win over the landing catch-all (GET /).
	mux.HandleFunc("GET /robots.txt", s.handleRobots)
	mux.HandleFunc("GET /sitemap.xml", s.handleSitemap)
	mux.HandleFunc("GET /static/", s.handleStatic)
	mux.HandleFunc("GET /{$}", s.handleRoot)
	mux.HandleFunc("GET /", s.handleLanding)
	return mux
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
			return fmt.Errorf("web: старый сокет %s: %w", listen, err)
		}
	}
	ln, err := net.Listen(network, listen)
	if err != nil {
		return fmt.Errorf("web: слушатель %s: %w", listen, err)
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
			done <- fmt.Errorf("web: сервер %s: %w", listen, err)
			return
		}
		done <- nil
	}()
	s.opt.Log.Info("публичный сервис лендинга запущен", "listen", listen)

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		// Graceful shutdown (spec 3.4): in-flight responses finish within
		// a window smaller than the framework stop budget.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("web: остановка: %w", err)
		}
		return nil
	}
}

// handleRoot — GET / — a redirect to the default locale (the locale is
// chosen by the path prefix, not a cookie — canonical addresses for
// SEO).
func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/"+DefaultLocale, http.StatusFound)
}

// handleLanding — GET /{locale}: the page data from the caches (T-120)
// and the locale template render. A DB failure — a 500 without
// revealing details: a page without data is meaningless (the section
// degradation — the pattern of the internal dashboard T-129, not a
// public page).
func (s *Server) handleLanding(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	locale, _, _ := strings.Cut(path, "/")
	if _, ok := localeMessagesMap[locale]; !ok {
		http.NotFound(w, r)
		return
	}
	data, err := FetchLandingData(r.Context(), s.db)
	if err != nil {
		s.opt.Log.Warn("данные лендинга", "locale", locale, "err", err)
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tpl.ExecuteTemplate(w, "landing.html", landingVM{
		Locale:  locale,
		Locales: Locales,
		Data:    data,
		msgs:    localeMessages(locale),
	}); err != nil {
		// The header may already be gone — the status cannot be changed;
		// we log and abort.
		s.opt.Log.Warn("рендер лендинга", "locale", locale, "err", err)
	}
}

// handleStatic — static assets from embed: built into the binary,
// served with the immutable header (hash-less names — cached by the
// common rule).
func (s *Server) handleStatic(w http.ResponseWriter, r *http.Request) {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		http.NotFound(w, r)
		return
	}
	// app.css — a Tailwind generated artifact (make web-css / the css
	// stage of the Dockerfile, T-124); not stored in git (.gitignore):
	// on a fresh clone the file is absent — /static/app.css answers 404
	// (a page without styles, not a 500).
	http.StripPrefix("/static/", http.FileServer(http.FS(sub))).ServeHTTP(w, r)
}

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

// handleRobots — GET /robots.txt: static assets from embed (T-124).
// The locale pages and the static assets are indexed; the check form
// API and healthz are closed to bots. There is still no Sitemap line (a
// deliberate decision, T-227): the address is at the standard
// /sitemap.xml path — web.public_host sets the <loc> host, a separate
// line adds nothing.
func (s *Server) handleRobots(w http.ResponseWriter, r *http.Request) {
	b, err := staticFS.ReadFile("static/robots.txt")
	if err != nil {
		// embed — the file is in the binary; its absence is a build error.
		s.opt.Log.Error("robots.txt отсутствует в embed", "err", err)
		http.Error(w, "temporarily unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write(b)
}

// handleSitemap — GET /sitemap.xml: a minimal sitemap — only the static
// locale pages (T-124); the landing has no domain pages. <loc> must be
// an absolute URL. The host is the canonical web.public_host (T-227):
// behind the reverse proxy nginx passes the visitor's host into the
// request Host as the $host value, and without the canonical host <loc>
// would depend on a header from an untrusted party (host-header
// poisoning); an empty field — the previous fallback to the request
// Host. The scheme is publicScheme (X-Forwarded-Proto only from a
// trusted peer). Hence the sitemap is assembled per request rather than
// served as a static file.
func (s *Server) handleSitemap(w http.ResponseWriter, r *http.Request) {
	scheme := publicScheme(r)
	host := strings.TrimSpace(s.cfg.Web.PublicHost)
	if host == "" {
		host = r.Host
	}
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	_, _ = fmt.Fprint(w, xml.Header, `<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">`+"\n")
	for _, loc := range Locales {
		url := template.HTMLEscapeString(fmt.Sprintf("%s://%s/%s", scheme, host, loc))
		_, _ = fmt.Fprintf(w, "\t<url>\n\t\t<loc>%s</loc>\n\t\t<changefreq>daily</changefreq>\n\t\t<priority>1.0</priority>\n\t</url>\n", url)
	}
	_, _ = fmt.Fprint(w, "</urlset>\n")
}

// publicScheme — the public URL scheme (<loc> of the sitemap, T-227):
// a direct TLS connection — https; behind the reverse proxy the client
// scheme is carried by X-Forwarded-Proto, but only from a trusted peer
// (loopback/unix — trustedProxyPeer, the same principle as the visitor
// address in clientIP): a direct visitor's header does not control the
// scheme, the default by the TLS fact (http) stays. The values may be a
// list — the LAST entry is taken (our proxy appended it; the client
// start of the list is not trusted — modeled on X-Forwarded-For in
// proxyHeaderIP), the sample nginx sets a single $scheme value; junk or
// empty — the default.
func publicScheme(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	if !trustedProxyPeer(r) {
		return "http"
	}
	proto := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto"))
	last := strings.ToLower(strings.TrimSpace(proto[strings.LastIndex(proto, ",")+1:]))
	switch last {
	case "http", "https":
		return last
	}
	return "http"
}

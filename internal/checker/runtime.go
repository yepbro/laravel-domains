// The long-lived circuit of the live check (T-220). Before T-220 the Run
// pipeline assembled the whole environment per call: a resolver chain
// (1 primary + dns.fallback_resolvers clients, each with a udp_pool_size
// pool of 2000 — up to 6000 UDP sockets per one form request at
// LimitNOFILE=65535 of the crawler-web unit: ~10 parallel checks
// exhausted the fds together with /healthz), its own httpfetch.Fetcher
// with local limitState (the ceilings of the http section acted only
// inside one pipeline — there was no service-wide limit), the rules from
// the disk and a render.Manager with a Start() with render.remote_url
// configured.
//
// Runtime assembles this environment once for the lifetime of the service
// (the public web form): one resolver chain with a small UDP pool
// (LiveUDPPoolSize), one Fetcher with shared limitState, a rules Reloader
// with a background reload and one render.Manager. Run runs the pipeline
// on the ready circuit and builds no fund of its own; the owner closes
// the circuit with Close at shutdown (web.Server.Run → closeLive). A
// one-off consumer (the CLI crawlerctl check) assembles the circuit for
// one call with the Run function.
package checker

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"crawler/internal/config"
	"crawler/internal/dnsclient"
	"crawler/internal/httpfetch"
	"crawler/internal/httpworker"
	"crawler/internal/render"
	"crawler/internal/rules"
)

// LiveUDPPoolSize — the UDP socket pool of each client of the live check
// chain: the form parallelism is limited by the web semaphore (a few
// checks), and the DNS phase of one check is sequential (CheckSubdomains
// goes over the subdomains one by one) — the udp_pool_size fund of the
// production pipeline is not needed here. 3 clients of the chain × 16
// sockets = 48 fds per service vs 6000 per request before T-220.
const LiveUDPPoolSize = 16

// DefaultRulesReload — the period of the background rules reload of the
// circuit. Nobody controls the public form (the rules SIGHUP is sent by
// the operator to the analyzer, make rules-reload); the set is kept fresh
// by a timer; an unchanged rules_version is a no-op (the Reloader
// contract).
const DefaultRulesReload = 5 * time.Minute

// RuntimeOptions — the circuit parameters; zero values are the norms of
// the live form check.
type RuntimeOptions struct {
	// UDPPoolSize — the UDP socket pool of each client of the chain;
	// 0 — LiveUDPPoolSize.
	UDPPoolSize int
	// RulesReload — the period of the background rules reload;
	// 0 — DefaultRulesReload; negative — without a timer (a one-off Run
	// call: the set is read at assembly and does not change).
	RulesReload time.Duration
}

// LiveCircuit — the long-lived live check circuit for the owning service
// (T-220): Run is signature-compatible with LiveFunc (web in production
// calls the circuit method, the tests substitute the liveCheck field);
// Close stops the circuit entirely. Satisfied by *Runtime.
type LiveCircuit interface {
	Run(ctx context.Context, cfg *config.Config, pool Pool, domain string, log Logger) (*Result, error)
	Close() error
}

// Runtime — the pipeline environment assembled once and reused between
// checks. Safe for concurrent use from goroutines: the resolver chain,
// the Fetcher, the Reloader and the render.Manager are thread-safe; the
// state of a separate check (the subdomains reference table, the page
// store) lives inside Run.
type Runtime struct {
	cfg      *config.Config
	log      Logger
	resolve  dnsclient.Resolver
	closeDNS func() error
	fetcher  *httpfetch.Fetcher
	// renderer is nil when the render is not configured
	// (render.remote_url is empty).
	renderer *render.Manager
	rules    *rules.Reloader

	life     context.Context // the life context: the background reload, the render browsers
	stopTick chan struct{}   // stops the background rules reload
	tickDone chan struct{}   // closed when the background loop exits (nil — no loop)

	closeOnce sync.Once
	closeErr  error
}

// compile-time check: *Runtime is a LiveCircuit.
var _ LiveCircuit = (*Runtime)(nil)

// newResolverChainFn — the seam of the resolver chain assembly for the
// circuit tests (counting the clients and the pool sizes without real
// sockets); production — newResolverChain (resolve.go).
var newResolverChainFn = newResolverChain

// NewRuntime assembles the live check circuit: a resolver chain with a
// small UDP pool under the T-035 limiter, one Fetcher with shared
// limitState (the limits of the http section apply to all the checks of
// the service at once), one render.Manager with render.remote_url
// configured and a rules Reloader. The render starts lazily — by the first
// SPA render (Manager.Run raises the generation itself): a Chromium
// unavailable at start does not degrade the service forever, a render
// error leaves the analysis by the original HTML (the Run semantics
// before T-220). ctx is the life context of the circuit (a cancellation
// stops the background reload and the render browsers). An error — the
// circuit was not created, the partially open resources are closed.
func NewRuntime(ctx context.Context, cfg *config.Config, log Logger, opts RuntimeOptions) (*Runtime, error) {
	if cfg == nil {
		return nil, errors.New("checker: live check runtime without configuration")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if log == nil {
		log = nopLogger{}
	}
	if opts.UDPPoolSize <= 0 {
		opts.UDPPoolSize = LiveUDPPoolSize
	}

	chain, closeDNS, err := newResolverChainFn(ctx, cfg.DNS, opts.UDPPoolSize)
	if err != nil {
		return nil, fmt.Errorf("checker: %w", err)
	}
	resolve, err := dnsclient.NewLimitResolver(chain, dnsclient.NewLimitOptions(cfg.DNS))
	if err != nil {
		_ = closeDNS()
		return nil, fmt.Errorf("checker: %w", err)
	}

	// The transport like the production http-worker (T-049/T-050/T-047),
	// but one per circuit: the shared limitState — global connection/RPS
	// ceilings over all the live checks of the service; the resolve cache
	// of the redirect hosts is limited by DefaultCacheEntries, shared per
	// service.
	hopts := httpfetch.NewOptions(cfg.HTTP)
	hopts.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	hopts.Resolver = httpfetch.NewCachingResolver(
		httpworker.NewDNSLookup(resolve, cfg.DNS.IPv6), httpfetch.CacheOptions{})
	fetcher := httpfetch.NewFetcher(hopts)

	var renderer *render.Manager
	if cfg.Render.RemoteURL != "" {
		renderer = render.NewManager(ctx, render.NewOptions(*cfg))
	}

	rulesRel, err := rules.NewReloader(cfg.Analyzer.RulesDir, slogOf(log))
	if err != nil {
		if renderer != nil {
			renderer.Close()
		}
		fetcher.CloseIdleConnections()
		_ = closeDNS()
		return nil, fmt.Errorf("checker: %w", err)
	}

	rt := &Runtime{
		cfg:      cfg,
		log:      log,
		resolve:  resolve,
		closeDNS: closeDNS,
		fetcher:  fetcher,
		renderer: renderer,
		rules:    rulesRel,
		life:     ctx,
	}
	if opts.RulesReload == 0 {
		opts.RulesReload = DefaultRulesReload
	}
	if opts.RulesReload > 0 {
		rt.stopTick = make(chan struct{})
		rt.tickDone = make(chan struct{})
		go rt.reloadLoop(opts.RulesReload)
	}
	return rt, nil
}

// reloadLoop — the background rules reload: exit by Close (stopTick) or
// the life context cancellation; a load error does not change the current
// set (the Reloader contract — the service runs on the previous rules).
func (rt *Runtime) reloadLoop(interval time.Duration) {
	defer close(rt.tickDone)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-rt.stopTick:
			return
		case <-rt.life.Done():
			return
		case <-t.C:
			_ = rt.rules.Reload()
		}
	}
}

// Close stops the circuit: the background rules reload, the render (the
// current generation finishes its pages — graceful, spec 3.4), the
// transport connections and the UDP socket pools of the chain. It is
// idempotent; repeat calls return the first error.
func (rt *Runtime) Close() error {
	rt.closeOnce.Do(func() {
		if rt.tickDone != nil {
			close(rt.stopTick)
			<-rt.tickDone
		}
		if rt.renderer != nil {
			rt.renderer.Close()
		}
		rt.fetcher.CloseIdleConnections()
		rt.closeErr = rt.closeDNS()
	})
	return rt.closeErr
}

// ReloadRules re-reads the rules directory (a set change — for example,
// by the operator following the analyzer pattern); an error does not
// change the current set. The circuit background timer does not need the
// method — an entry point for the owner.
func (rt *Runtime) ReloadRules() error { return rt.rules.Reload() }

// slogOf — a *slog.Logger for the Reloader (its field is a concrete
// type): the circuit logger if it is slog, otherwise a silent one — the
// Reloader writes its own Info/Warn.
func slogOf(log Logger) *slog.Logger {
	if l, ok := log.(*slog.Logger); ok {
		return l
	}
	return slog.New(slog.DiscardHandler)
}

// nopLogger — the silent circuit logger at a nil parameter (NewRuntime
// allows nil, like httpfetch.NewOptions substitutes defaults).
type nopLogger struct{}

func (nopLogger) Info(string, ...any) {}
func (nopLogger) Warn(string, ...any) {}

// Run — the one-off pipeline of one name (the CLI crawlerctl check): the
// circuit is assembled per call and closed on completion — the rules and
// sockets of one domain do not need a long life. The owning service (the
// web form) needs the long-lived circuit: NewRuntime + Run + Close
// (T-220).
func Run(ctx context.Context, cfg *config.Config, pool Pool, domain string, log Logger) (*Result, error) {
	rt, err := NewRuntime(ctx, cfg, log, RuntimeOptions{RulesReload: -1})
	if err != nil {
		return nil, err // the NewRuntime errors already have the checker: prefix
	}
	defer func() { _ = rt.Close() }()
	return rt.Run(ctx, cfg, pool, domain, log)
}

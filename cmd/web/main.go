// The web service — the public API (stage 13, T-121; a production
// extension outside the main spec — moved from larasites; since L-08 —
// API-only: the landing pages are static sites of the separate
// ld-landing repo). The endpoints: POST /api/check-domain (the live
// check of the landing form, T-123), GET /api/v1/landing/stats (the
// landing data as JSON, L-03), GET /healthz. The framework is
// internal/service: migrations, the DB pool (OnDBPool), the probes and
// /metrics on loopback :9107, graceful shutdown (spec 3.4); the public
// listener web.listen — its own http.Server inside OnRun (the metrics are
// not exposed outward, spec §9). The API is the only public port of the
// system.
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
	"crawler/internal/metrics"
	"crawler/internal/service"
	"crawler/internal/web"
)

func main() {
	// A configuration error — before the logger is initialized: stderr and exit code 1.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[web] configuration: %v", err)
	}
	logger := service.NewLogger("web", cfg)

	// The registry is created here (not by the framework) so that the form
	// instruments (outcomes/failures/duration, T-221) live in the same
	// registry that serves /metrics. Metrics are loopback (spec §9: the
	// public entry point is one — web.listen; 9107 is the next free one
	// after analyzer :9106). The pool comes from the framework after the
	// migrations (OnDBPool): postgres.dsn is required (config.Validate),
	// the readiness /healthz of the public mux pings it.
	reg := metrics.New("web")
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "web",
		Config:      cfg,
		Logger:      logger,
		Metrics:     reg,
		MetricsAddr: service.EnvOr("METRICS_ADDR", "127.0.0.1:9107"),
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		OnRun:       func(ctx context.Context) error { return run(ctx, cfg, logger, pool, reg) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — the public listener inside OnRun: the web service owns the
// lifecycle (a graceful stop on ctx cancellation — the precedent of the
// OnRun clients of the workers), the completion returns control to the
// framework.
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, reg *metrics.Registry) error {
	if pool == nil {
		return errors.New("web: the DB pool is not provided (postgres.dsn is required)")
	}
	srv, err := web.New(pool, cfg, web.Options{Log: logger, Metrics: reg})
	if err != nil {
		return err
	}
	return srv.Run(ctx, cfg.Web.Listen)
}

// Package service — the shared framework of the Domain Crawler Go
// services.
//
// The framework takes care of everything not related to the working logic
// of a service: structured logging (log/slog, the level from the
// configuration), applying the migrations and the PostgreSQL connection
// pool (internal/migrate, internal/db), an HTTP server with the /livez +
// /healthz probes and the /metrics metrics (internal/metrics, spec 4.1,
// 5.9; T-110) and graceful shutdown per spec 3.4: on SIGTERM/SIGINT the
// context of the working logic (Options.OnRun) is cancelled first and the
// framework waits for it to finish, then the metrics HTTP server stops,
// and the DB pool is closed last. A repeat signal interrupts the wait — a
// forced exit. SIGHUP — a rules reload without a restart (spec 5.8). The
// working logic is plugged in via the OnRun option (the workers of the
// stages 2+); for now the stub services live until the shutdown signal.
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/migrate"
)

// DefaultShutdownTimeout — the default graceful shutdown budget: the time
// within which the working logic must finish the current task or return
// it to the queue (spec 3.4).
const DefaultShutdownTimeout = 10 * time.Second

// Options — the service startup parameters.
type Options struct {
	// Name — the service name (ingest, analyzer, ...), used in the logs
	// and metrics.
	Name string
	// MetricsAddr — the address of the metrics HTTP server, for example
	// ":9101".
	MetricsAddr string
	// Config — the service configuration (internal/config, spec 5.10):
	// loaded in main via config.Load (the priority: env > YAML >
	// defaults). If set, the PostgreSQL DSN is taken from it.
	Config *config.Config
	// PGDSN — the PostgreSQL DSN directly; it has priority over Config
	// (used by utilities that do not need a config file). If set, the
	// service applies the embedded schema migrations before start
	// (internal/migrate): all the long-lived services of the project work
	// with the DB (spec 6.3), the healthcheck answers only on the current
	// schema. An empty string together with an empty Config.Postgres.DSN —
	// a service without a DB.
	PGDSN string
	// OnDBPool — the initialization of the DB part of the service (T-007):
	// if set and the DSN is known, the framework creates the internal/db
	// connection pool after applying the migrations (postgres.dsn +
	// postgres.pool_size from Config, or PGDSN with a default size) and
	// calls OnDBPool with the ready pool. The pool is closed by the
	// framework at graceful shutdown — last, after OnRun finishes and the
	// metrics HTTP server stops. nil — the service needs no pool (the
	// current stubs): connections are not opened.
	OnDBPool func(pool *pgxpool.Pool)
	// DBAfterConnect — the setup of every pool connection of the DB
	// (db.AfterConnectFunc, T-171): nil — the default. The session hooks:
	// db.NoParallelGather — services with aggregate/scanning background
	// passes (T-171 — monitoring; T-201 — the http-worker retention
	// drain), the pipeline write pools (db.LockTimeout — the ceiling of
	// waiting for row locks, T-192); several hooks — via a composition of
	// db.JoinHooks (the list per service — docs/db.md §"AfterConnect
	// hooks").
	DBAfterConnect db.AfterConnectFunc
	// OnRun — the working logic of the service (the workers of the
	// stages 2+): called after the full initialization (the migrations,
	// the pool, the HTTP metrics). The received context is cancelled on
	// SIGTERM/SIGINT — on cancellation the worker finishes the current
	// task or returns it to the queue (spec 3.4) and returns; the
	// framework waits for the return within ShutdownTimeout. A return
	// without a signal stops the service: nil — a clean completion
	// (one-off tasks), an error — a completion with exit code 1. nil — a
	// stub without working logic: the service lives until the shutdown
	// signal.
	OnRun func(ctx context.Context) error
	// OnSIGHUP is called on receiving SIGHUP (a reload of the Laravel
	// rules).
	OnSIGHUP func()
	// ExtraMux allows registering additional handlers.
	ExtraMux func(mux *http.ServeMux)
	// Metrics — the Prometheus registry of the service (internal/metrics):
	// if set, the framework serves exactly it via /metrics instead of its
	// own metrics.New(Name). main creates the registry in advance, to
	// build the instruments of the working logic before OnRun starts (the
	// workers and the collectors get the same one, T-077); nil — the
	// framework creates the registry itself (for services without their
	// own instruments).
	Metrics *metrics.Registry
	// Logger — the structured logger of the service (see NewLogger). nil —
	// the framework creates the logger itself and makes it the default
	// (slog.SetDefault).
	Logger *slog.Logger
	// ShutdownTimeout — the graceful shutdown budget (waiting for OnRun
	// and stopping the metrics HTTP server); 0 or negative —
	// DefaultShutdownTimeout.
	ShutdownTimeout time.Duration
}

// errForcedExit — a repeat shutdown signal during graceful shutdown: Run
// returns it to the caller immediately, without waiting for OnRun and the
// resource cleanup (the metrics HTTP server, the pool) — main exits with
// code 1, as with any Run error.
var errForcedExit = errors.New("second shutdown signal")

// migrateUp — applying the migrations at start (internal/migrate.Up);
// the variable is for substitution in the lifecycle tests (a seam in the
// spirit of lockTimeout/connectTimeout in internal/migrate).
var migrateUp = migrate.Up

// newRegistry — the metrics registry of the service: Options.Metrics if
// main created it itself (the instruments of the working logic are built
// before OnRun starts, T-077), otherwise its own metrics.New by the
// service name.
func newRegistry(opts Options) *metrics.Registry {
	if opts.Metrics != nil {
		return opts.Metrics
	}
	return metrics.New(opts.Name)
}

// EnvOr returns the value of the environment variable or defaultVal.
func EnvOr(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// Run brings up the service (the migrations → the DB pool → the HTTP
// metrics → OnRun) and blocks until completion: SIGTERM/SIGINT (a graceful
// shutdown), an error of the working logic or the HTTP server, or a normal
// OnRun completion. Run logs all of its errors itself (at the Error level,
// with the service context) and returns them to the calling main — it only
// needs to exit with code 1:
//
//	if err := service.Run(opts); err != nil {
//		os.Exit(1)
//	}
//
// A shutdown signal during the migration application does not kill the
// process in the middle of the migration: the waiting phases are
// cancelled, and the in-flight migration is interrupted by cancelling the
// SQL query on the PostgreSQL side (internal/migrate, T-112) — the
// unfinished file rolls back entirely, the schema version returns to the
// previous one, and the service stops normally (Run returns nil); the next
// start finishes the application. If the query could not be cancelled, the
// migration runs to the end (the C-04 behavior).
//
// A repeat shutdown signal during graceful shutdown — Run immediately
// returns the forced exit error without waiting for OnRun and the resource
// cleanup; main exits with code 1, as with any Run error.
func Run(opts Options) error {
	if opts.Name == "" {
		return errors.New("service: service name is required")
	}
	if opts.MetricsAddr == "" {
		opts.MetricsAddr = ":9090"
	}
	if opts.ShutdownTimeout <= 0 {
		opts.ShutdownTimeout = DefaultShutdownTimeout
	}
	if opts.Logger == nil {
		opts.Logger = NewLogger(opts.Name, opts.Config)
	}
	// The default logger is the service logger: the migrations (below) and
	// the service code via slog.Default write with the name context.
	slog.SetDefault(opts.Logger)

	// The signal handlers are installed BEFORE the migrations: the default
	// reaction to SIGTERM/SIGINT would kill the process in the middle of a
	// migration, and golang-migrate marks the version dirty before
	// executing the file and clears the flag only after success — the next
	// start of ALL the services would fail on migrate.ErrDirty until a
	// manual TRUNCATE schema_migrations. A signal during the migrations
	// cancels the waiting phases (the DB connection, the advisory lock) by
	// the context; the application is interrupted by cancelling the SQL
	// query on the PostgreSQL side (internal/migrate, T-112): the
	// unfinished migration rolls back entirely, the version — to the
	// previous one. If the cancellation is impossible (a non-URL DSN, the
	// DB unavailable) — the migration runs to the end (C-04). In both
	// cases the service stops normally, without starting the working
	// logic.
	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(term)
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)

	// The DB schema is applied before the HTTP server starts: the
	// healthcheck answers only on the current schema. A parallel start of
	// several services is safe (the advisory locks in internal/migrate).
	// The DSN: an explicit Options.PGDSN > the configuration (the env
	// overrides are already applied in config.Load:
	// CRAWLER_POSTGRES_DSN/APP_PG_DSN > postgres.dsn).
	dsn := opts.PGDSN
	if dsn == "" && opts.Config != nil {
		dsn = opts.Config.Postgres.DSN
	}
	if dsn != "" {
		// A separate channel for the migration phase: term stays untouched
		// for serve (the signal is delivered to all the registered
		// channels).
		migTerm := make(chan os.Signal, 1)
		signal.Notify(migTerm, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(migTerm)

		migCtx, cancelMig := context.WithCancel(context.Background())
		defer cancelMig()
		var migSignalled atomic.Bool
		migDone := make(chan struct{})
		go func() {
			select {
			case <-migTerm:
				// The flag before cancelMig: a migration that returned by
				// cancellation already sees it set — no race with the
				// check below.
				migSignalled.Store(true)
				cancelMig()
			case <-migDone:
			}
		}()
		migErr := migrateUp(migCtx, dsn)
		close(migDone)

		switch {
		case errors.Is(migErr, migrate.ErrInterrupted):
			// The signal interrupted the application: the migration rolled
			// back on the PostgreSQL side, the schema version is the last
			// applied one. This is a normal fast shutdown, not a failure
			// (T-112).
			opts.Logger.Info("shutdown signal during schema apply — the migration was interrupted and rolled back; the next start will finish it")
			return nil
		case migSignalled.Load():
			opts.Logger.Info("shutdown signal during schema apply — stopping after the migrations")
			return nil
		case migErr != nil:
			opts.Logger.Error("migrations", "err", migErr)
			return migErr
		}
	}

	// The connection pool is created after the migrations and only if the
	// service requested it (OnDBPool): the stubs keep working without a
	// DB. The size — from the configuration if it exists (the budget of
	// spec 6.3 is 20–50 connections in total for all the services;
	// pool_size is set per service).
	var pool *pgxpool.Pool
	if dsn != "" && opts.OnDBPool != nil {
		poolSize := 0
		if opts.Config != nil {
			poolSize = opts.Config.Postgres.PoolSize
		}
		openCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		p, err := db.Open(openCtx, db.Options{
			DSN:          dsn,
			PoolSize:     poolSize,
			AfterConnect: opts.DBAfterConnect,
			// The service label in pg_stat_activity (T-177: the slow query
			// loop of monitor excludes its own aggregates and reads the
			// service label in the messages to the operator).
			AppName: opts.Name,
		})
		cancel()
		if err != nil {
			opts.Logger.Error("PostgreSQL pool", "err", err)
			return err
		}
		pool = p
		opts.OnDBPool(pool)
	}

	mux := http.NewServeMux()
	registerProbes(mux, pool)
	// /metrics — the Prometheus registry (internal/metrics): the base
	// framework metrics + the Go/runtime collectors; the working logic
	// metrics are added by the registry factory (Counter/Gauge/Histogram
	// with the automatic service label, T-076; the stage instruments —
	// T-077). Options.Metrics — the registry created by main for the
	// instruments of the working logic; nil — the framework's own
	// registry (newRegistry).
	registry := newRegistry(opts)
	mux.Handle("/metrics", metrics.Handler(registry))
	if opts.ExtraMux != nil {
		opts.ExtraMux(mux)
	}

	srv := &http.Server{
		Addr:              opts.MetricsAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ErrorLog:          slog.NewLogLogger(opts.Logger.Handler(), slog.LevelError),
	}

	// The pool is closed by the framework at graceful shutdown — last,
	// after OnRun finishes and the metrics HTTP server stops, and also on
	// the error paths after its creation (the busy metrics address below).
	closePool := func() {}
	if pool != nil {
		closePool = func() {
			pool.Close()
			opts.Logger.Info("PostgreSQL pool closed")
		}
	}

	// Listen is separated from Serve: a busy address error is before OnRun
	// starts.
	ln, err := net.Listen("tcp", opts.MetricsAddr)
	if err != nil {
		opts.Logger.Error("metrics HTTP server", "addr", opts.MetricsAddr, "err", err)
		closePool() // the pool is already created — do not leave its connections
		return err
	}

	err = serve(opts, srv, ln, closePool, term, hup)
	if errors.Is(err, errForcedExit) {
<<<<<<< HEAD
		// Раньше здесь был прямой os.Exit(1) — обход defer'ов и
		// непроверяемость в тестах. Ошибка доезжает до main: его реакция
		// на любую ошибку Run и так os.Exit(1), код выхода не меняется.
		opts.Logger.Warn("second signal — forcing exit without waiting for shutdown")
=======
		// Previously there was a direct os.Exit(1) here — bypassing the
		// defers and untestable. The error reaches main: its reaction to
		// any Run error is os.Exit(1) anyway; the exit code does not
		// change.
		opts.Logger.Warn("повторный сигнал — принудительное завершение без ожидания остановки")
>>>>>>> 1588289 (docs: translate small internal package comments to English)
	}
	return err
}

// registerProbes — the framework probe routes on the shared mux (the
// metrics port; there is no separate listener for the probes — spec 5.9,
// one HTTP endpoint per service). /livez — liveness, /healthz — readiness
// (C-03 → T-110).
func registerProbes(mux *http.ServeMux, pool *pgxpool.Pool) {
	mux.HandleFunc("/livez", livezHandler())
	mux.HandleFunc("/healthz", healthzHandler(pool))
}

// livezHandler — the liveness probe: the process is alive and its HTTP
// loop answers. The dependencies (the DB, Redis) are deliberately not
// checked: their unavailability is not a process death; a 503 here would
// tell the orchestrator "restart" on a PostgreSQL/Redis failure and would
// mask the cause with worker restarts.
//
// A separate "the background loop has not panicked" sign is not needed: in
// the framework such a state does not live longer than the shutdown
// budget — a panic/error/completion of OnRun is intercepted by serve and
// stops the whole process (Run returns, main exits, the orchestrator
// restart policy brings the service up), so an answer of this probe
// already means "Run is active" (in the graceful shutdown window, ≤
// ShutdownTimeout, the process is stopping anyway).
func livezHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}
}

// healthzHandler — the readiness probe (the dependencies; checked by the
// operator and the monitoring): a service with a DB pool checks PostgreSQL
// with a short Ping — an unavailable DB is visible as 503/unhealthy, not
// as "ready" with endless errors in the worker log (C-03). A service
// without a pool (Options without OnDBPool/DSN) is always ready: there are
// no dependencies.
//
// Redis is not part of the probe (T-110): the Redis clients live inside
// the OnRun of the workers — the framework does not see them, and pulling
// the dependency into the probe path would change the readiness to "both
// infrastructures answered" at the cost of new coupling in the framework.
// There is no "admit tasks" consumer either — the workers pull them from
// the streams themselves; the Redis availability is visible in the queue
// metrics (crawler_queue_depth) and the logs.
func healthzHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if pool != nil {
			ctx, cancel := context.WithTimeout(r.Context(), time.Second)
			defer cancel()
			if err := pool.Ping(ctx); err != nil {
				http.Error(w, "db unavailable", http.StatusServiceUnavailable)
				return
			}
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}
}

// serve — the lifecycle after initialization: the metrics HTTP server on
// ln and the working logic opts.OnRun. It finishes on the first of: the
// term signal, an HTTP server error, an OnRun return. The shutdown order
// (spec 3.4):
//  1. the working logic: cancelling the OnRun context and waiting for it
//     to finish within ShutdownTimeout (a repeat term interrupts the
//     wait);
//  2. the metrics HTTP server — waits for the started requests;
//  3. the DB pool (closePool).
//
// The shutdown errors are logged but do not interrupt the sequence.
func serve(opts Options, srv *http.Server, ln net.Listener, closePool func(), term <-chan os.Signal, hup <-chan os.Signal) error {
	logger := opts.Logger
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	// SIGHUP — a rules reload without a restart (make rules-reload).
	hupDone := make(chan struct{})
	defer close(hupDone)
	go func() {
		for {
			select {
			case <-hupDone:
				return
			case <-hup:
				logger.Info("SIGHUP received")
				if opts.OnSIGHUP != nil {
					opts.OnSIGHUP()
				}
			}
		}
	}()

	httpErr := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErr <- err
		}
	}()

	var onRunDone <-chan error
	if opts.OnRun != nil {
		done := make(chan error, 1)
		onRunDone = done
		go func() {
			defer func() {
				// A panic of the working logic must not bring the process
				// down without a log and cleanup: we turn it into an
				// error; then — a regular graceful shutdown (it will
				// convert it into exit code 1).
				if r := recover(); r != nil {
					logger.Error("panic in the working logic", "panic", r, "stack", string(debug.Stack()))
					done <- fmt.Errorf("OnRun: panic: %v", r)
				}
			}()
			done <- opts.OnRun(runCtx)
		}()
	}

	// The waiting stage: we run until a signal, an HTTP/OnRun error or an
	// independent OnRun completion (a one-off task).
	var (
		onRunErr      error
		onRunFinished = opts.OnRun == nil
		httpFail      error
	)
	select {
	case sig := <-term:
		logger.Info("shutdown signal — graceful shutdown", "signal", sig.String())
	case err := <-httpErr:
		logger.Error("metrics HTTP server", "err", err)
		httpFail = fmt.Errorf("metrics HTTP server: %w", err)
	case err := <-onRunDone:
		onRunFinished = true
		onRunErr = err
		if err != nil {
			logger.Error("working logic", "err", err)
		} else {
			logger.Info("working logic finished — stopping the service")
		}
	}
	cancelRun()

	// The shutdown stage. 1) The working logic: finish the current task or
	// return it to the queue (spec 3.4) within the shared shutdown budget;
	// a repeat signal — a forced exit.
	deadline := time.Now().Add(opts.ShutdownTimeout)
	timedOut := false
	if !onRunFinished {
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		select {
		case err := <-onRunDone:
			onRunErr = err
			// With httpFail the main exit error remains the HTTP failure
			// (it is what stopped the service), but the worker
			// finalization (returning the tasks to the queue, spec 3.4)
			// also deserves a log.
			if err != nil {
				logger.Error("working logic during shutdown", "err", err)
			}
		case <-term:
			return errForcedExit
		case <-timer.C:
			timedOut = true
			logger.Warn("working logic did not stop within the timeout — continuing shutdown",
				"timeout", opts.ShutdownTimeout.String())
		}
	}

	// 2) The metrics HTTP server: the rest of the budget — finish the
	// started /metrics scrapes; if the budget is exhausted by a stuck
	// working logic, the connections are closed immediately (the shared
	// shutdown budget is not exceeded).
	if remain := time.Until(deadline); remain > 0 {
		httpCtx, cancelHTTP := context.WithTimeout(context.Background(), remain)
		defer cancelHTTP()
		if err := srv.Shutdown(httpCtx); err != nil {
			logger.Warn("metrics HTTP server shutdown", "err", err)
		}
	} else if err := srv.Close(); err != nil {
		logger.Warn("metrics HTTP server close", "err", err)
	}

	// 3) The DB pool — last.
	closePool()

	switch {
	case httpFail != nil:
		return httpFail
	case timedOut:
		return fmt.Errorf("working logic did not stop within %s", opts.ShutdownTimeout)
	default:
		return onRunErr
	}
}

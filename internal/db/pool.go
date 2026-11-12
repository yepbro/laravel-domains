package db

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
)

// compile-time check: *pgxpool.Pool satisfies the DB interface
// (the package helpers accept it directly).
var _ DB = (*pgxpool.Pool)(nil)

// Default pool settings. The values are conservative: connections in the
// dev environment and production live on one machine with the services,
// long idleness and "eternal" connections are not needed, and regular
// connection rebuilding guards against stale TCP sessions and state
// accumulation.
const (
	// DefaultPoolSize — the pool connection limit when pool_size is unset
	// (matches config.Default().Postgres.PoolSize, spec 5.10).
	DefaultPoolSize = 20
	// DefaultMaxConnLifetime — connection re-establishment regardless of
	// activity (pgxpool default 1h).
	DefaultMaxConnLifetime = 30 * time.Minute
	// DefaultMaxConnIdleTime — closing an idle connection
	// (pgxpool default 30m).
	DefaultMaxConnIdleTime = 5 * time.Minute
	// DefaultHealthCheckPeriod — background check of idle connections
	// before checkout from the pool and on a schedule (the pgxpool value).
	DefaultHealthCheckPeriod = time.Minute
	// DefaultConnectTimeout — establishing a single connection; must fit
	// within the service's startup context.
	DefaultConnectTimeout = 10 * time.Second
)

// MaxPoolSize — the upper bound of a single service's pool_size: the
// spec 6.3 budget (≤ 20–50 connections across ALL services combined)
// makes no sense to give to one pool. config.Validate limits the same
// field to the 1..100 range; the bound here is stricter and protects
// calls that bypass the config.
const MaxPoolSize = 100

// AfterConnectFunc — a hook configuring a new pool connection (passed
// through to pgxpool.Config.AfterConnect): it runs once per opened
// connection (including reopenings due to MaxConnLifetime), before first
// use. A hook failure is a connection-open error.
type AfterConnectFunc func(ctx context.Context, conn *pgx.Conn) error

// noParallelGatherSQL — disables the session's parallel query workers.
const noParallelGatherSQL = "SET max_parallel_workers_per_gather = 0"

// NoParallelGather — the AfterConnect hook for services with
// aggregate/scan background passes (T-171 — monitoring; T-201 —
// http-worker): the session-level SET max_parallel_workers_per_gather = 0
// means aggregate passes (zone recalculation, summary windows, backlog,
// retention drain) do not consume PostgreSQL parallel workers and do not
// grab all cores of the shared CPU at once from the crawler pipeline
// (production incident 23–24.10: 4 cores, load 21–23, each parallel
// aggregate took 3 cores). The session's total work is the same, the wall
// time of a pass grows — for monitoring a freshness of tens of seconds is
// not critical (the stalled-progress alert threshold is 5 zone
// recalculation periods), for background drain the pace is set by the loop
// interval and chunk size (T-201), and workers and transport get even
// shares of CPU instead of spikes.
//
// Applicable to the monitoring pools (monitor, dashboard, metric
// collectors), scheduler (seq passes over backlog/retry temp_error) and
// http-worker (retention drain of T-201: the T-185 time-form collected
// Gather Merge on the same service's starved CPU): the OLTP worker path
// (chunk reads/writes by indexes) does not use parallel plans, but the
// same service's background seq passes slow down — the decision is made
// deliberately per service.
func NoParallelGather(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, noParallelGatherSQL); err != nil {
		return fmt.Errorf("db: %s: %w", noParallelGatherSQL, err)
	}
	return nil
}

// DefaultLockTimeout — the cap on waiting for row locks in the pipeline's
// write pools (T-192). Production incident 25.10: a long transaction held
// row locks on domains for ≈37 min, point write statements (all by PK,
// perfect plans) queued behind it for up to 39 min without a cap — the
// pipeline silently stalled for ≈55 minutes (q:dns at the watermark, no
// XACK). 30s is three orders of magnitude above normal waits
// (milliseconds to write a result; the non-incident maximum — enqueue
// 10.6s) and an order of magnitude below the incident's 15–39 min: it cuts
// only a lock convoy. Exceeding it yields SQLSTATE 55P03, a rollback of
// the write transaction and a normal delivery retry (at-least-once,
// spec §3.4) instead of a silent wait. The cap is an incident constant,
// not a knob: no need to move it to the config.
const DefaultLockTimeout = 30 * time.Second

// lockTimeoutSQLFmt — the session-level cap on waiting for foreign locks.
// The <N>ms form is valid for PostgreSQL GUC time at any duration;
// Duration.String() does not fit: it prints 90s as "1m30s", and "m" is
// not a unit of the GUC parser (ms/s/min/h/d).
const lockTimeoutSQLFmt = "SET lock_timeout = '%dms'"

// LockTimeout — the AfterConnect hook of the pipeline's write pools
// (T-192): a session-level SET lock_timeout on every pool connection
// (including reopenings due to MaxConnLifetime). Enabled in dns-worker,
// http-worker and scheduler — their transactions take row locks on
// domains; readers (monitor, dashboard, web) and the analyzer are not
// touched. A non-positive duration is a hook error (the connection does
// not open, the service fails at startup): a cap must be a cap, "0" here
// would mean silently disabling it.
func LockTimeout(d time.Duration) AfterConnectFunc {
	return func(ctx context.Context, conn *pgx.Conn) error {
		if d <= 0 {
			return fmt.Errorf("db: SET lock_timeout: non-positive duration %s", d)
		}
		sql := fmt.Sprintf(lockTimeoutSQLFmt, d.Milliseconds())
		if _, err := conn.Exec(ctx, sql); err != nil {
			return fmt.Errorf("db: %s: %w", sql, err)
		}
		return nil
	}
}

// JoinHooks — composition of AfterConnect hooks (T-192): runs them in
// order, any failure is a connection-open error (as with a single hook);
// nil elements are skipped (equivalent to a nil AfterConnect — the setting
// is unset). Needed by pools with several session settings:
// scheduler — NoParallelGather (T-171) + LockTimeout (T-192), http-worker
// — the same two hooks (T-201, retention drain) through a single
// Options.DBAfterConnect wiring.
func JoinHooks(hooks ...AfterConnectFunc) AfterConnectFunc {
	return func(ctx context.Context, conn *pgx.Conn) error {
		for _, h := range hooks {
			if h == nil {
				continue
			}
			if err := h(ctx, conn); err != nil {
				return err
			}
		}
		return nil
	}
}

// Options — connection pool parameters. Zero durations and PoolSize are
// replaced with defaults; DSN is required.
type Options struct {
	// DSN — the PostgreSQL connection string (the config's postgres.dsn).
	DSN string
	// PoolSize — the pool's maximum connections: overrides pool_max_conns
	// from the DSN (the source of truth is the service configuration).
	// 0 — the default (DefaultPoolSize); the all-services limit — spec 6.3.
	PoolSize int
	// MinPoolSize — connections kept in the background (0 — lazy opening:
	// idle time occupies no connections). Default 0.
	MinPoolSize int
	// MaxConnLifetime — connection lifetime. Default 30m.
	MaxConnLifetime time.Duration
	// MaxConnIdleTime — connection idle time before closing. Default 5m.
	MaxConnIdleTime time.Duration
	// HealthCheckPeriod — background connection check period. Default 1m.
	HealthCheckPeriod time.Duration
	// ConnectTimeout — establishing a single connection. Default 10s.
	ConnectTimeout time.Duration
	// AppName — the pool connections' application_name: the service label
	// in pg_stat_activity (server log; monitoring of T-177 — the
	// slow-query loop uses it to tell its own aggregates from pipeline
	// pathologies). "" — not set; an explicit application_name in the DSN
	// is not overwritten (the operator's choice in the connection string
	// wins).
	AppName string
	// AfterConnect — configuration of each new connection (nil — none):
	// see AfterConnectFunc. The value also applies to reopenings due to
	// MaxConnLifetime — SET-like session settings survive connection
	// rebuilding.
	AfterConnect AfterConnectFunc
}

// NewOptions assembles pool parameters from the config's postgres section
// (T-006): dsn and pool_size; the service settings take their defaults
// (set explicitly on top when needed).
func NewOptions(pg config.Postgres) Options {
	return Options{DSN: pg.DSN, PoolSize: pg.PoolSize}
}

// buildPoolConfig parses the DSN and applies the parameters: pool_size
// sets the maximum connections (explicitly overriding pool_max_conns from
// the DSN), the rest — health-check and connection lifetimes.
func buildPoolConfig(opts Options) (*pgxpool.Config, error) {
	if strings.TrimSpace(opts.DSN) == "" {
		return nil, errors.New("db: empty PostgreSQL DSN")
	}
	switch {
	case opts.PoolSize == 0:
		opts.PoolSize = DefaultPoolSize
	case opts.PoolSize < 0 || opts.PoolSize > MaxPoolSize:
		return nil, fmt.Errorf("db: pool_size %d out of range 1..%d (spec 6.3 budget: 20–50 connections across ALL services)", opts.PoolSize, MaxPoolSize)
	}

	cfg, err := pgxpool.ParseConfig(opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("db: parsing DSN: %w", err)
	}

	cfg.MaxConns = int32(opts.PoolSize)
	cfg.MinConns = int32(opts.MinPoolSize)
	cfg.MaxConnLifetime = defaultIfZero(opts.MaxConnLifetime, DefaultMaxConnLifetime)
	cfg.MaxConnIdleTime = defaultIfZero(opts.MaxConnIdleTime, DefaultMaxConnIdleTime)
	cfg.HealthCheckPeriod = defaultIfZero(opts.HealthCheckPeriod, DefaultHealthCheckPeriod)
	cfg.ConnConfig.ConnectTimeout = defaultIfZero(opts.ConnectTimeout, DefaultConnectTimeout)
	// The service label in pg_stat_activity: only if the DSN did not set
	// it (an explicit value in the connection string — the operator's
	// choice).
	if opts.AppName != "" && cfg.ConnConfig.RuntimeParams["application_name"] == "" {
		cfg.ConnConfig.RuntimeParams["application_name"] = opts.AppName
	}
	cfg.AfterConnect = opts.AfterConnect
	return cfg, nil
}

// Open creates a connection pool and checks DB availability with a single
// connection (fail-fast: the service fails at startup with a clear error
// instead of on the first query; the check connection is returned to the
// pool). The context bounds the connection-establishment wait.
func Open(ctx context.Context, opts Options) (*pgxpool.Pool, error) {
	cfg, err := buildPoolConfig(opts)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("db: creating pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db: PostgreSQL unreachable (%s): %w", redactDSN(opts.DSN), err)
	}
	return pool, nil
}

func defaultIfZero(v, def time.Duration) time.Duration {
	if v == 0 {
		return def
	}
	return v
}

// RedactDSN hides connection-string secrets for error messages: the
// userinfo password and the password/sslpassword query parameters
// (pgx/pgconn, like libpq, accepts them in the URL query too —
// parseURLQueryParams moves the values into the connection settings).
// An unparseable DSN is replaced with the fallback message: not even the
// structure is disclosed. A single implementation for internal/db and
// internal/migrate (previously two diverged copies — one did not strip
// the query password).
func RedactDSN(dsn, fallback string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.User == nil {
		return fallback
	}
	// url.UserPassword encodes mask characters, so the password is simply
	// dropped: what matters is that it never reaches the log, not what the
	// replacement looks like.
	u.User = url.User(u.User.Username())
	// Secret query parameters are masked (a mask without special
	// characters — url.Values.Encode escapes them); the rest of the query
	// (sslmode, pool_max_conns, ...) is preserved. RawQuery is rewritten
	// only on an actual replacement — otherwise Encode() would reorder
	// the parameters.
	q := u.Query()
	redacted := false
	for _, k := range []string{"password", "sslpassword"} {
		if q.Has(k) {
			q.Set(k, "redacted")
			redacted = true
		}
	}
	if redacted {
		u.RawQuery = q.Encode()
	}
	return u.String()
}

// redactDSN — RedactDSN with the db package's fallback message.
func redactDSN(dsn string) string {
	return RedactDSN(dsn, "see postgres.dsn (configuration)")
}

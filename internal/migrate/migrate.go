// Package migrate — applying PostgreSQL migrations when services start.
//
// The DB schema is stored in the repository (migrations/) and embedded
// into the binary via go:embed (migrations.FS), so a service applies the
// migrations itself, without an external directory and a separate
// deployment step (spec 6.3).
//
// Runner properties:
//   - the race of several services starting at once is excluded by a
//     session advisory lock pg_advisory_lock on a dedicated connection:
//     it covers both the driver initialization (creating
//     schema_migrations) and the application; inside golang-migrate
//     there is its own advisory lock for the duration of Up (the key is
//     derived from the DB name + schema_migrations), so the CLI migrate
//     and the runner do not conflict either;
//   - a rerun on the up-to-date schema is not an error (migrate.ErrNoChange
//     counts as success) — hot reload (air) and restarts are safe;
//   - a shutdown signal (a context cancellation) interrupts the
//     application too: golang-migrate Up does not accept a context, so
//     the runner itself cancels the running SQL with a pg_cancel_backend
//     query (the runner's connections are marked with a unique
//     application_name; attempts are repeated so as not to miss the gap
//     between marking dirty and the start of the file); an unfinished
//     migration file is rolled back by PostgreSQL entirely, and the
//     schema version returns to the last applied one — the next start
//     applies it again (T-112);
//   - a dirty state (an interrupted migration) is an error with a clear
//     text; recovery: make psql → TRUNCATE schema_migrations (see docs).
package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"crawler/internal/db"
	"crawler/migrations"

	// Registration of the database/sql driver "pgx/v5" (stdlib pgx).
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Runner parameters. The values have headroom: applying the schema on an
// empty DB takes seconds, and lockTimeout must cover the slowest migration,
// so services starting in parallel wait for it instead of failing.
// Variables (not constants) — for overriding in tests.
var (
	// lockTimeout — the wait for the advisory lock while another instance
	// is applying migrations (a parallel start of services).
	lockTimeout = 5 * time.Minute
	// connectTimeout — the wait for PostgreSQL availability at start
	// (outside compose a service may come up before the DB).
	connectTimeout = 30 * time.Second
	// cancelTimeout — the total budget for cancelling an in-flight
	// migration by signal: pg_cancel_backend over the runner's own
	// connections via pg_stat_activity, with retries (a single query may
	// land in the gap when the migration file has not started executing
	// yet). Out of budget — the cancellation did not happen, the migration
	// runs to the end (the C-04 behavior).
	cancelTimeout = 5 * time.Second
)

// connectRetryInterval — the pause between connection attempts.
const connectRetryInterval = time.Second

// cancelRetryInterval — the pause between attempts to cancel an in-flight
// migration (see cancelApplying): it covers the gap where the previous
// runner step (marking the dirty version) has already completed while the
// migration file has not started executing yet — the connection is idle
// at that moment.
const cancelRetryInterval = 50 * time.Millisecond

// sqlDriver — the name of the database/sql driver for the runner's
// connections (a variable for substituting in tests of the error paths).
var sqlDriver = "pgx/v5"

// advisoryLockKey — the runner's pg_advisory_lock key. The session lock on
// a dedicated connection serializes not only the migration application
// (golang-migrate can do that itself) but also the driver initialization —
// creating schema_migrations — with a parallel start of services.
// The constant is arbitrary but fixed for all instances.
const advisoryLockKey = 0x63726177 // "craw"

// ErrDirty is returned when the schema stayed in a dirty state after an
// interrupted migration: golang-migrate refuses to apply the next ones
// until the version is confirmed manually.
var ErrDirty = errors.New("миграции: схема в dirty-состоянии (прерванная миграция); выполните TRUNCATE schema_migrations или `migrate force <версия>` после разбора причины")

// ErrInterrupted is returned when the application is interrupted by a
// context cancellation (a shutdown signal): the running SQL was cancelled
// on the PostgreSQL side, the unfinished migration file rolled back
// entirely, the schema version returned to the last applied one. The next
// start will apply the interrupted migration again — this is a normal fast
// shutdown, not a failure.
var ErrInterrupted = errors.New("миграции: применение прервано сигналом завершения")

// runnerAppPrefix — the prefix of the application_name of the runner's
// connections; tagSeq makes the label unique per Up call: the migration
// cancellation by signal addresses strictly its own connections and does
// not touch the migration applied by another instance with a parallel
// start of services.
var (
	runnerAppPrefix = "crawler-migrations"
	tagSeq          atomic.Uint64
)

// Up applies all embedded migrations (up) up to the current version.
// It blocks the caller until completion (including waiting for the DB and
// locks). A ctx cancellation interrupts not only the waiting phases but
// the application too: golang-migrate Up does not accept a context, so
// the runner cancels the running SQL on the PostgreSQL side (see
// cancelApplying) and then returns ErrInterrupted — the next start
// finishes the application.
func Up(ctx context.Context, dsn string) error {
	if dsn == "" {
		return errors.New("миграции: пустой DSN PostgreSQL")
	}

	// The connections label: pg_cancel_backend addresses by it only this
	// Up call. A non-URL DSN gets no label — the application cancellation
	// is unavailable, the C-04 behavior remains (the migration runs to
	// the end).
	tag := fmt.Sprintf("%s-%d-%d", runnerAppPrefix, os.Getpid(), tagSeq.Add(1))

	db, err := openAndWait(ctx, withRunnerTag(dsn, tag))
	if err != nil {
		return err
	}
	defer db.Close() //nolint:errcheck // closing after the application, the DB is no longer needed

	// A dedicated connection: a session advisory lock lives until the end
	// of the session, so it cannot be held on a pool connection.
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("миграции: соединение для блокировки: %w", err)
	}
	defer conn.Close() //nolint:errcheck // also releases the advisory lock

	if err := acquireAdvisoryLock(ctx, conn); err != nil {
		return err
	}
	defer releaseAdvisoryLock(conn)

	src, err := newSource()
	if err != nil {
		return fmt.Errorf("миграции: встроенные файлы: %w", err)
	}

	// One migration = one multi-line SQL file: pgx executes it entirely
	// via the simple protocol (without arguments), like the CLI migrate.
	driver, err := pgxmigrate.WithInstance(db, &pgxmigrate.Config{})
	if err != nil {
		return fmt.Errorf("миграции: драйвер БД: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", src, "postgres", driver)
	if err != nil {
		// The driver already holds the dedicated connection — close it,
		// so it does not leak on this path either.
		if cerr := driver.Close(); cerr != nil {
			slog.Warn("миграции: закрытие драйвера", "err", cerr)
		}
		return fmt.Errorf("миграции: инициализация: %w", err)
	}
	// m.Close closes the source and the driver: the connection held by
	// the driver returns to the pool immediately instead of living without
	// an owner until garbage collection (db.Close closes only idle
	// connections; an in-use one stays hanging).
	defer func() {
		if _, cerr := m.Close(); cerr != nil {
			slog.Warn("миграции: закрытие раннера", "err", cerr)
		}
	}()
	m.LockTimeout = lockTimeout

	// The connection and advisory-lock phases above are interruptible by
	// ctx itself; the driver initialization and m.Up are not
	// (golang-migrate does not accept a context). The watcher, on ctx
	// cancellation, cancels the running SQL with a pg_cancel_backend
	// query — this interrupts both the wait for the internal golang-migrate
	// lock and the in-flight migration. The cancellation is repeated until
	// success, the end of the application, or the budget: a single query
	// could land in the gap between marking dirty and the start of the
	// file, when the runner's connection is not yet active (T-112).
	upDone := make(chan struct{})
	watchDone := make(chan struct{})
	var cancelled atomic.Bool
	go func() {
		defer close(watchDone)
		select {
		case <-ctx.Done():
			cancelled.Store(cancelApplying(db, tag, upDone))
		case <-upDone:
		}
	}()

	err = m.Up()
	close(upDone)
	<-watchDone // the cancellation finished before parsing the result — no race with the flag

	switch {
	case err == nil:
		version, _, verr := m.Version()
		if verr != nil {
			return fmt.Errorf("миграции: версия после применения: %w", verr)
		}
		slog.Info("миграции: схема применена", "version", version)
		return nil
	case errors.Is(err, migrate.ErrNoChange):
		slog.Info("миграции: схема уже актуальна, применение не требуется")
		return nil
	case cancelled.Load():
		return interrupted(driver, m, err)
	case errors.As(err, new(migrate.ErrDirty)):
		return ErrDirty
	default:
		return fmt.Errorf("миграции: применение: %w", err)
	}
}

// interrupted formats the result of an application interrupted by cancellation.
//
// If the migration body itself was cancelled (the driver marks such an
// error as "migration failed"), the dirty version row is guaranteed to
// belong to this interrupted application: PostgreSQL executes the
// migration file as one simple-protocol query — all instructions in one
// implicit transaction, a cancellation rolls back the file entirely. The
// runner returns the version to the previously applied one (the semantics
// of `migrate force <v−1>`), and the next start applies the migration
// again instead of hitting dirty.
//
// A cancellation in the other phases (waiting for the internal
// golang-migrate lock, driver initialization) did not change the schema
// state — no reset needed.
func interrupted(driver database.Driver, m *migrate.Migrate, err error) error {
	if !interruptedInRun(err) {
		return fmt.Errorf("%w: применение остановлено до тела миграции, состояние схемы не менялось: %w", ErrInterrupted, err)
	}

	version, dirty, verr := m.Version()
	if verr != nil || !dirty || version == 0 {
		return fmt.Errorf("%w: состояние версии не удалось подтвердить: %w", ErrInterrupted, err)
	}

	prev := int(version) - 1
	if prev == 0 {
		prev = database.NilVersion // the migration was the first: the version is an empty table
	}
	if serr := driver.SetVersion(prev, false); serr != nil {
		// The schema was rolled back (an implicit transaction), but the
		// version row stayed dirty: the next start hits ErrDirty — the
		// recovery direction is the same as for any interrupted migration.
		return fmt.Errorf("%w: миграция %d отменена и откатана, но версия не восстановлена (осталась dirty): %w; откат версии: %v",
			ErrInterrupted, version, err, serr)
	}
	slog.Warn("миграции: применение прервано сигналом, миграция откатана, схема на предыдущей версии",
		"interrupted_version", version, "rolled_back_to", prev)
	return fmt.Errorf("%w: миграция %d отменена и откатана целиком, схема на версии %d, повторный старт применит её заново: %w",
		ErrInterrupted, version, prev, err)
}

// interruptedInRun — an Up error from the migration body: the pgx driver
// marks an SQL execution failure with the "migration failed" label (for a
// PgError — with the server text after the label, for example "migration
// failed: canceling statement…"). Only such an error after our
// cancellation means the dirty version row was set by this application
// (SetVersion(v, true) — before executing the file, cleared after).
func interruptedInRun(err error) bool {
	var dbErr database.Error
	return errors.As(err, &dbErr) && strings.HasPrefix(dbErr.Err, "migration failed")
}

// cancelApplying cancels the running SQL queries of this Up call's
// connections (the tag label in pg_stat_activity): this is how an
// in-flight migration is interrupted — golang-migrate does not accept a
// context, but PostgreSQL cancels the query with pg_cancel_backend, and
// the migration file rolls back entirely. The state = 'active' filter
// spares the idle runner connections (the advisory lock), and the unique
// label spares the migration applied by another instance and the service
// connections.
//
// A single query can miss: between marking dirty and the start of the
// migration file (SetVersion and Run at the driver are two separate uses
// of one connection) the connection is still idle and does not match the
// filter. Therefore the attempts are repeated until at least one query is
// cancelled, the application itself finishes (done), or the cancelTimeout
// budget is exhausted. Returns true only if at least one query was
// actually cancelled.
func cancelApplying(db *sql.DB, tag string, done <-chan struct{}) bool {
	ctx, cancel := context.WithTimeout(context.Background(), cancelTimeout)
	defer cancel()
	ticker := time.NewTicker(cancelRetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done: // the application has already finished — nothing to cancel
			return false
		default:
		}
		const query = `
			SELECT count(*) FILTER (WHERE pg_cancel_backend(pid))
			FROM pg_stat_activity
			WHERE application_name = $1
			  AND state = 'active'
			  AND pid <> pg_backend_pid()`
		var n int
		if err := db.QueryRowContext(ctx, query, tag).Scan(&n); err != nil {
			if ctx.Err() == nil {
				slog.Warn("миграции: попытка снять применяющуюся миграцию не удалась, повторю", "err", err)
			}
		} else if n > 0 {
			return true
		}
		// n == 0 — no active queries: the migration has not started
		// executing yet (the gap after marking dirty) or has already
		// finished; silently repeating until a cancellation, the end of
		// the application, or the budget.
		select {
		case <-ctx.Done():
			slog.Warn("миграции: снять применяющуюся миграцию не удалось — доработает до конца", "tag", tag)
			return false
		case <-done:
			return false
		case <-ticker.C:
		}
	}
}

// withRunnerTag adds the application_name label to a URL DSN: the runner's
// connections are visible in pg_stat_activity and addressable for
// cancellation on a shutdown signal. A non-URL DSN is returned unchanged —
// no label, the application cancellation is unavailable (the migration
// runs to the end, the C-04 behavior).
func withRunnerTag(dsn, tag string) string {
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme == "" || u.Opaque != "" {
		return dsn
	}
	q := u.Query()
	q.Set("application_name", tag)
	u.RawQuery = q.Encode()
	return u.String()
}

// acquireAdvisoryLock waits for pg_advisory_lock no longer than lockTimeout:
// if another instance's migrations are stuck, it is better to fail (the
// service restarts per the restart policy) than to wait indefinitely.
func acquireAdvisoryLock(ctx context.Context, conn *sql.Conn) error {
	lockCtx, cancel := context.WithTimeout(ctx, lockTimeout)
	defer cancel()
	const query = `SELECT pg_advisory_lock($1)`
	if _, err := conn.ExecContext(lockCtx, query, advisoryLockKey); err != nil {
		return fmt.Errorf("миграции: ожидание advisory-блокировки (её держит другой экземпляр?): %w", err)
	}
	return nil
}

// releaseAdvisoryLock releases the lock; an error is not critical —
// closing the connection releases it itself.
func releaseAdvisoryLock(conn *sql.Conn) {
	const query = `SELECT pg_advisory_unlock($1)`
	if _, err := conn.ExecContext(context.Background(), query, advisoryLockKey); err != nil {
		slog.Warn("миграции: снятие advisory-блокировки не удалось (снимется при закрытии соединения)", "err", err)
	}
}

// newSource returns the migration source over the embedded set.
// Extracted separately so unit tests can check the set the same way
// golang-migrate reads it.
func newSource() (source.Driver, error) {
	return iofs.New(migrations.FS, ".")
}

// openAndWait opens a pool to PostgreSQL and waits for its availability
// no longer than connectTimeout: on a developer machine the DB is usually
// already healthy (compose depends_on); outside compose the start can
// overlap in time.
func openAndWait(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open(sqlDriver, dsn)
	if err != nil {
		return nil, fmt.Errorf("миграции: разбор DSN: %w", err)
	}
	// On error paths (a timeout, a cancellation) the *sql.DB is closed:
	// otherwise a background opener goroutine and the connections that
	// managed to open remain.
	connected := false
	defer func() {
		if !connected {
			_ = db.Close()
		}
	}()

	deadline := time.Now().Add(connectTimeout)
	for {
		pingCtx, cancel := context.WithTimeout(ctx, connectRetryInterval)
		err = db.PingContext(pingCtx)
		cancel()
		if err == nil {
			connected = true
			return db, nil
		}
		if time.Now().After(deadline) {
			redacted := redactDSN(dsn)
			return nil, fmt.Errorf("миграции: PostgreSQL недоступен (%s): %w", redacted, err)
		}
		slog.Info("миграции: PostgreSQL ещё недоступен, повторю попытку", "err", err.Error(), "retry_in", connectRetryInterval.String())
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("миграции: ожидание PostgreSQL прервано: %w", ctx.Err())
		case <-time.After(connectRetryInterval):
		}
	}
}

// redactDSN hides DSN secrets in error messages (db.RedactDSN — a single
// implementation with internal/db; the fallback mentions the env source of
// the runner's DSN too — APP_PG_DSN).
func redactDSN(dsn string) string {
	return db.RedactDSN(dsn, "см. postgres.dsn (конфигурация) / APP_PG_DSN")
}

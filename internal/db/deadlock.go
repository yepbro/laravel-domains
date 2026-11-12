// Application-level retry for PostgreSQL deadlocks (T-268). A 40P01
// deadlock is a normal outcome of concurrent transactions: the server
// detects the lock cycle itself (deadlock_timeout, 1s), the victim rolls
// back — data is not corrupted and the operation can be retried. The
// structural lock-ordering invariant of counter updates (docs/schema.md,
// crawler_counters section; T-267) removes the known cycles but does not
// guarantee no new ones: the wrapper absorbs residual and future deadlocks
// in pipeline hot spots without sending the task to redelivery (each
// q:http delivery retry is a full HTTP episode; the 07.11 incident —
// runbooks.md §2.8).
//
// Only idempotent operations are retried (spec 3.4): the http-worker
// result-write transaction (guards/overwrite), the autocommit write
// statements of a DNS episode (done/deleted guards), and the scheduler
// decision-transaction retry (both UPDATE guards). The pause range is
// jittered so chunks colliding at the same time do not retry in sync.
package db

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// SQLSTATE 40P01 — deadlock_detected (codes as literals, the
// resultUpdateStatusSQL style of T-039; pgerrcode is not pulled in for a
// single constant).
const sqlstateDeadlock = "40P01"

// Default retry parameters (T-268).
const (
	// DefaultDeadlockRetries — re-executions after the first failure: the
	// deadlock victim has already waited out deadlock_timeout (1s), so a
	// retry almost always makes it — two are enough with margin.
	DefaultDeadlockRetries = 2

	// DefaultDeadlockBase — the base of the retry pause: milliseconds
	// against the seconds of deadlock detection — the competing
	// transaction has time to commit and release the rows.
	DefaultDeadlockBase = 50 * time.Millisecond

	// DefaultDeadlockMax — the cap on a single retry's pause: even during
	// a long deadlock bout the added transaction latency is fractions of
	// a second, and the pipeline does not stall.
	DefaultDeadlockMax = 250 * time.Millisecond
)

// MaxDeadlockRetries — a hard cap on the wrapper's retries: the retry is a
// guard for residual cases, not a cure for a permanent lock cycle (a
// persistent 40P01 means a lock-ordering inversion somewhere in the code,
// runbooks.md §2.8: fix the cause, do not swallow it with retries).
const MaxDeadlockRetries = 2

// DeadlockRetry — retry wrapper parameters; zero values are replaced with
// defaults, and Retries above MaxDeadlockRetries is clamped to the cap
// (withDefaults). Tests speed up the pause with an explicitly small Base.
type DeadlockRetry struct {
	// Retries — re-executions after the first failure (0 — default;
	// > MaxDeadlockRetries — clamped to the cap).
	Retries int
	// Base — the base pause before a retry; the actual pause is jittered
	// within [Base/2, Base].
	Base time.Duration
	// Max — the cap on a single retry's pause.
	Max time.Duration
}

// DefaultDeadlockRetry — parameters for pipeline hot spots (T-268): up to
// two retries with pauses of tens of milliseconds.
var DefaultDeadlockRetry = DeadlockRetry{
	Retries: DefaultDeadlockRetries,
	Base:    DefaultDeadlockBase,
	Max:     DefaultDeadlockMax,
}

// withDefaults normalizes the parameters.
func (r DeadlockRetry) withDefaults() DeadlockRetry {
	if r.Retries <= 0 {
		r.Retries = DefaultDeadlockRetries
	}
	if r.Retries > MaxDeadlockRetries {
		r.Retries = MaxDeadlockRetries
	}
	if r.Base <= 0 {
		r.Base = DefaultDeadlockBase
	}
	if r.Max <= 0 {
		r.Max = DefaultDeadlockMax
	}
	return r
}

// delay — the pause before the next retry: jitter [Base/2, Base],
// cap Max.
func (r DeadlockRetry) delay() time.Duration {
	half := r.Base / 2
	d := half + time.Duration(rand.Int63n(int64(half)+1))
	if d > r.Max {
		return r.Max
	}
	return d
}

// IsDeadlock reports whether the error is a PostgreSQL deadlock
// (SQLSTATE 40P01): errors go through errors.As — an error wrapped by
// callers with fmt.Errorf("%w") is recognized too.
func IsDeadlock(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == sqlstateDeadlock
}

// RetryDeadlock runs fn and retries it on a 40P01 deadlock: up to
// r.Retries retries (default — DefaultDeadlockRetry), each preceded by a
// jittered, capped pause interruptible by ctx cancellation. onRetry (may
// be nil) is called before each actual retry — the metric point of
// crawler_db_deadlock_retried_total (T-268). Other errors are returned
// immediately; exhausting the retries returns the last error to the
// caller, which decides the delivery fate (at-least-once, spec 3.4).
// Cancelling ctx before a retry returns the original deadlock error: the
// attempt was interrupted by a shutdown, not exhausted.
func RetryDeadlock(ctx context.Context, r DeadlockRetry, onRetry func(attempt int), fn func() error) error {
	if fn == nil {
		return errors.New("db: ретрай 40P01: повторяемая функция не задана")
	}
	o := r.withDefaults()
	err := fn()
	for attempt := 1; IsDeadlock(err) && attempt <= o.Retries; attempt++ {
		if ctx.Err() != nil {
			return err
		}
		if d := o.delay(); d > 0 {
			timer := time.NewTimer(d)
			select {
			case <-ctx.Done():
				timer.Stop()
				return err
			case <-timer.C:
			}
		}
		if onRetry != nil {
			onRetry(attempt)
		}
		err = fn()
	}
	return err
}

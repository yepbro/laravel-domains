// Unit tests of the 40P01 retry wrapper (T-268): SQLSTATE recognition
// (raw and wrapped pgconn.PgError — yes; foreign codes and errors — no),
// retry boundaries (success / deadlock-then-success / persistent deadlock
// / non-deadlock), the Retries cap, the pause cap, retry hook behavior and
// ctx cancellation. Behavior against a live DB — integration tests
// (test/integration).
package db

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// deadlockErr — a literal 40P01, as pgx returns it.
func deadlockErr() error {
	return &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}
}

func TestIsDeadlock(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"40P01", deadlockErr(), true},
		{"40P01 in a %w wrapper", fmt.Errorf("result write: %w", deadlockErr()), true},
		{"a double wrapper", fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", deadlockErr())), true},
		{"55P03 lock_timeout", &pgconn.PgError{Code: "55P03"}, false},
		{"23505 unique_violation", &pgconn.PgError{Code: "23505"}, false},
		{"a regular error", errors.New("db unavailable"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsDeadlock(tt.err); got != tt.want {
				t.Errorf("IsDeadlock(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestDeadlockRetryParams(t *testing.T) {
	// Zero values — defaults; the Retries cap is clamped.
	got := DeadlockRetry{}.withDefaults()
	if got != DefaultDeadlockRetry {
		t.Errorf("DeadlockRetry{} = %+v, want the default %+v", got, DefaultDeadlockRetry)
	}
	if got := (DeadlockRetry{Retries: 9}).withDefaults(); got.Retries != MaxDeadlockRetries {
		t.Errorf("Retries 9 = %d, want the cap %d", got.Retries, MaxDeadlockRetries)
	}
	// Pause cap: a base of hours, a cap of milliseconds — delay must hit
	// the cap (otherwise the test would hang on the pause).
	d := DeadlockRetry{Base: time.Hour, Max: time.Millisecond}.withDefaults().delay()
	if d != time.Millisecond {
		t.Errorf("delay = %s, want the cap 1ms", d)
	}
	// A zero base — the default (zero values are replaced).
	if got := (DeadlockRetry{Base: 0}).withDefaults(); got.Base != DefaultDeadlockBase {
		t.Errorf("Base 0 = %s, want the default %s", got.Base, DefaultDeadlockBase)
	}
	// The jitter stays within [Base/2, Base].
	for _, b := range []time.Duration{time.Millisecond, 50 * time.Millisecond, DefaultDeadlockBase} {
		r := DeadlockRetry{Base: b, Max: time.Hour}.withDefaults()
		for i := 0; i < 100; i++ {
			if d := r.delay(); d < b/2 || d > b {
				t.Fatalf("delay = %s outside [Base/2, Base] for Base=%s", d, b)
			}
		}
	}
}

func TestRetryDeadlock(t *testing.T) {
	t.Run("success on the first attempt — one call, the hook is not invoked", func(t *testing.T) {
		calls, hooks := 0, 0
		err := RetryDeadlock(context.Background(), DefaultDeadlockRetry,
			func(int) { hooks++ },
			func() error { calls++; return nil })
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if calls != 1 || hooks != 0 {
			t.Errorf("calls=%d hooks=%d, want 1 and 0", calls, hooks)
		}
	})

	t.Run("a deadlock then success — a retry, the hook with the attempt number", func(t *testing.T) {
		calls := 0
		var attempts []int
		err := RetryDeadlock(context.Background(),
			DeadlockRetry{Base: time.Millisecond}, // a minimal pause — the test does not notice the sleep
			func(attempt int) { attempts = append(attempts, attempt) },
			func() error {
				calls++
				if calls == 1 {
					return deadlockErr()
				}
				return nil
			})
		if err != nil {
			t.Fatalf("err = %v, want nil (the deadlock is absorbed)", err)
		}
		if calls != 2 {
			t.Errorf("calls = %d, want 2", calls)
		}
		if len(attempts) != 1 || attempts[0] != 1 {
			t.Errorf("the hook = %v, want [1]", attempts)
		}
	})

	t.Run("not a deadlock — no retry", func(t *testing.T) {
		calls, hooks := 0, 0
		boom := errors.New("db unavailable")
		err := RetryDeadlock(context.Background(), DefaultDeadlockRetry,
			func(int) { hooks++ },
			func() error { calls++; return boom })
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v, want the original", err)
		}
		if calls != 1 || hooks != 0 {
			t.Errorf("calls=%d hooks=%d, want 1 and 0", calls, hooks)
		}
	})

	t.Run("a persistent deadlock — the error escapes after exhaustion", func(t *testing.T) {
		calls, hooks := 0, 0
		err := RetryDeadlock(context.Background(),
			DeadlockRetry{Base: time.Millisecond},
			func(int) { hooks++ },
			func() error { calls++; return deadlockErr() })
		if !IsDeadlock(err) {
			t.Fatalf("err = %v, want 40P01 out", err)
		}
		if want := 1 + DefaultDeadlockRetries; calls != want {
			t.Errorf("calls = %d, want %d (the first + retries)", calls, want)
		}
		if hooks != DefaultDeadlockRetries {
			t.Errorf("hooks = %d, want %d", hooks, DefaultDeadlockRetries)
		}
	})

	t.Run("a nil hook — works without the metric", func(t *testing.T) {
		calls := 0
		err := RetryDeadlock(context.Background(), DeadlockRetry{Base: time.Millisecond}, nil,
			func() error {
				calls++
				if calls == 1 {
					return deadlockErr()
				}
				return nil
			})
		if err != nil || calls != 2 {
			t.Errorf("err=%v calls=%d, want nil and 2", err, calls)
		}
	})

	t.Run("ctx cancellation — no retry runs, the deadlock escapes", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		calls, hooks := 0, 0
		err := RetryDeadlock(ctx, DefaultDeadlockRetry,
			func(int) { hooks++ },
			func() error {
				calls++
				cancel() // the cancellation arrived before the retry
				return deadlockErr()
			})
		if !IsDeadlock(err) {
			t.Fatalf("err = %v, want the original 40P01", err)
		}
		if calls != 1 || hooks != 0 {
			t.Errorf("calls=%d hooks=%d, want 1 and 0 (no retry)", calls, hooks)
		}
	})
}

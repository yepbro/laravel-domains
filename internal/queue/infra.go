// Classification of the queue's infrastructure errors (T-152, the post-
// mortem docs/load-measurements.md §11.1): a failure of Redis itself —
// OOM under noeviction ("OOM command not allowed …", the incident of
// Oct 22–23, 2026), a connection drop, a transport timeout — as opposed
// to processing errors (the task's business logic). Before T-152, a
// result publication error (XADD) was handled as an error of the task
// itself: the delivery stayed without XACK, RecoverLoop claimed it in a
// circle, attempts burned — one OOM window of 2.25 h produced 5,507 dead
// tasks with fully healthy processing.
//
// The InfraError marker is attached to transport errors in Publish/Ack
// and passes transparently through the workers' %w wrappers (IsInfra):
// the error consumer distinguishes "Redis failed to write" from "the
// task was not processed" — the former is treated by pausing consumption
// and re-writing (the result is already computed), not by spending
// delivery attempts.

package queue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/redis/go-redis/v9"
)

// InfraError — the marker of a queue infrastructure failure: Redis could
// not execute the command for its own reason (OOM noeviction, a
// connection drop, a transport timeout, LOADING after a restart), the
// task is not at fault. It determines the behavior of error consumers
// (T-152):
//
//   - workers pause reading new deliveries on the marker (the fuse
//     pattern of T-139/T-142): Redis rejects entries anyway, drawing
//     tasks is pointless;
//   - the at-least-once semantics do not change: InfraError ≠ success,
//     XACK after a successful publication — but delivery attempts are
//     also not spent by claims with no chance to write the result (the
//     RecoverLoop gate).
//
// Processing errors (business logic: decoding, the result DB, render) are
// not wrapped with the marker — their redelivery is meaningful and burns
// attempts by the common redis.max_attempts policy → q:dead (T-025).
type InfraError struct {
	// Op — the queue operation that failed ("publish to q:http",
	// "ack q:render (the render-worker group)") — context in the message.
	Op string
	// Err — the original transport/server error.
	Err error
}

func (e *InfraError) Error() string {
	return fmt.Sprintf("queue: %s: Redis infrastructure failure: %v", e.Op, e.Err)
}

func (e *InfraError) Unwrap() error { return e.Err }

// IsInfra reports that the error chain (including the workers' %w
// wrappers — from OnResult to Processing) contains a queue infrastructure
// failure.
func IsInfra(err error) bool {
	var ie *InfraError
	return errors.As(err, &ie)
}

// infraCause classifies a go-redis error as an infrastructure failure:
// server answers about its own unavailability (OOM — the subject of the
// T-152 incident; LOADING — a start after a restart; READONLY/
// MASTERDOWN/CLUSTERDOWN/TRYAGAIN — topology and replicas) and transport
// (a connection drop, a read/write timeout, pool exhaustion). The
// classification is typed: go-redis has no real untyped errors, and text
// markers would wrongly catch business texts. The caller's context
// cancellation/deadline is not a Redis failure (context.DeadlineExceeded
// implements net.Error and must be cut off before the network check).
func infraCause(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	// Server answers (the go-redis Is* helpers understand wrappers,
	// including the string fallback of the test errors "OOM …").
	if redis.IsOOMError(err) || redis.IsLoadingError(err) || redis.IsReadOnlyError(err) ||
		redis.IsMasterDownError(err) || redis.IsClusterDownError(err) || redis.IsTryAgainError(err) {
		return true
	}
	// An answer wrapped in a foreign type preserving the redis.Error
	// interface (the Is* helpers look for a concrete proto.RedisError and
	// a bare string — the interface walk covers %w chains).
	var rErr redis.Error
	if errors.As(err, &rErr) {
		m := rErr.Error()
		for _, prefix := range []string{"OOM ", "LOADING ", "READONLY ", "MASTERDOWN ", "CLUSTERDOWN ", "TRYAGAIN "} {
			if strings.HasPrefix(m, prefix) {
				return true
			}
		}
	}
	// Transport: a closed connection and the pool.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, redis.ErrPoolTimeout) || errors.Is(err, redis.ErrClosed) {
		return true
	}
	// *net.OpError (a refusal/drop/reset of the connection) and socket
	// timeouts implement net.Error.
	var netErr net.Error
	return errors.As(err, &netErr)
}

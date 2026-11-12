// The counter of the DB statement retries after the 40P01 deadlock
// (T-268): the db.RetryDeadlock wrapper absorbs the residual and future
// counter lock convoys (a structural invariant: T-267/T-267 fixes remove
// the known ones but do not prove the absence of new ones) in the hot
// spots of the pipeline — the result record transaction of the
// http-worker, the retry decision transaction of the scheduler, the
// autocommit record statements of the DNS episode. The counter is
// monotonic: each event is one actual retry (the deadlock victim has
// already waited out the deadlock_timeout; the retry almost always makes
// it in time). Single events are the norm (the guard works; the task
// does not go into a redelivery as a full episode); a steady growth is a
// new lock convoy: the triage — runbooks.md §2.8; the retry is a guard,
// not a cure.
//
// Architecturally — the same registration point as the stage instruments
// (T-077): each service creates the counter in ITS OWN registry
// (NewDBDeadlockRetried), the service label is substituted by the
// factory — a per-service breakdown for free; the name and the HELP are
// the same for all three services (http-worker, dns-worker, scheduler) —
// a registration in one registry (combined builds) does not fall on a
// descriptor conflict.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// NewDBDeadlockRetried — the crawler_db_deadlock_retried_total counter in
// the reg registry (the service label is substituted by the factory);
// a nil reg — nil (the callers keep it in a nil-safe wrapper of their own
// instruments). Idempotent (T-076): a repeated call shares the already
// created vector.
func NewDBDeadlockRetried(reg *Registry) prometheus.Counter {
	if reg == nil {
		return nil
	}
	return reg.MustCounter(prometheus.CounterOpts{
		Name: "crawler_db_deadlock_retried_total",
		Help: "DB statement retries after the 40P01 deadlock (db.RetryDeadlock, T-268): a guard on top of the counter lock-order invariant (T-267) — a repeat of an idempotent transaction/statement in the pipeline's hot spots (the http-worker result write, the scheduler retry decision, the DNS-episode write statements), the task does not go to redelivery as a full episode. Single events — the norm (the guard works); sustained growth — a new lock cycle, triage in runbooks.md §2.8.",
	}).WithLabelValues()
}

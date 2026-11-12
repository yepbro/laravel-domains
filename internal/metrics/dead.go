// The counters of the moves into q:dead (T-154, T-260; spec 4.2/5.9):
// the bury of the queue reliability layer (queue.Reliab) was silent up to
// T-154 — the 5507 deaths of the 10/22 incident
// (docs/load-measurements.md §11.1) passed without a single alert.
// The counters are monotonic: a death is an accomplished fact; only the
// whole q:dead is "triaged" (an XTRIM changes the depth, not the history
// of the deaths). Per-stream rows: the queue label is the source stream
// of the dead task (q:dns/q:http/q:render/q:analyze), as with
// crawler_queue_depth.
//
// Two events — two counters: crawler_queue_dead_buried_total counts the
// deaths of tasks with a task_id; crawler_queue_dead_unidentified_total
// (T-260, FR-6.1) — the quarantines of unidentifiable entries (without a
// task_id, a body outside the pack formats): before T-260 such entries
// spun in an eternal claim loop in the PEL; now they leave it with the
// synthetic identifier unknown:<stream>:<entry-id> (the triage —
// runbooks §2).
//
// Architecturally — the queue.ReliabOptions.OnBury/OnBuryUnidentified
// hooks, mounted by the services (the cmd workers: the RecoverLoop lives
// in them): the queue package does not depend on Prometheus — the same
// seam function as the depth collector's LenFunc (collect.go); the
// instrument lives in the registry of the service where the recoverer
// runs, and the event is visible in /metrics of the worker whose
// RecoverLoop produced it. The depth of q:dead (the untriaged remainder)
// and the queue_dead alert are held by monitor (T-154): the counters are
// the event pace (rate), the depth is the sink.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// NewDeadBuriedHook builds the hook for queue.ReliabOptions.OnBury: an
// increment of the crawler_queue_dead_buried_total{queue} counter in the
// reg registry. A function ready to be passed into the options is
// returned; a nil reg — nil (a service without a registry — isolated
// builds: Reliab checks the hook for nil and works without it).
// Idempotent (T-076): a repeated registration of an identical instrument
// shares the already created vector.
func NewDeadBuriedHook(reg *Registry) func(stream string) {
	if reg == nil {
		return nil
	}
	buried := reg.MustCounter(prometheus.CounterOpts{
		Name: "crawler_queue_dead_buried_total",
		Help: "Tasks moved to q:dead (Reliab.bury, T-154): the entry exhausted its attempt limit of deliveries without acknowledgement and went to the manual triage quarantine. The queue label is the task's source stream. Growth — task deaths: the rate — the death flow, the unresolved remainder — the monitor's crawler_queue_depth{queue=\"q:dead\"} (the queue_dead alert); triage — runbooks §2.",
	}, LabelQueue)
	return func(stream string) { buried.WithLabelValues(stream).Inc() }
}

// NewDeadUnidentifiedHook builds the hook for
// queue.ReliabOptions.OnBuryUnidentified: an increment of the
// crawler_queue_dead_unidentified_total{queue} counter in the reg
// registry (T-260, FR-6.1). A nil reg — nil, the T-076 idempotency — as
// with NewDeadBuriedHook.
func NewDeadUnidentifiedHook(reg *Registry) func(stream string) {
	if reg == nil {
		return nil
	}
	unidentified := reg.MustCounter(prometheus.CounterOpts{
		Name: "crawler_queue_dead_unidentified_total",
		Help: "Unidentifiable entries moved to q:dead (Reliab.bury, T-260/FR-6.1): a PEL entry without a task_id (a body outside the package formats) exhausted its delivery attempt limit; moved with the synthetic task_id unknown:<stream>:<entry-id>, without subject fields — the triage is manual (runbooks §2). The queue label is the entry's source stream. Steadily zero: growth — entries are published bypassing the package contract (a manual XADD, a writer bug) — find the source by entry_id.",
	}, LabelQueue)
	return func(stream string) { unidentified.WithLabelValues(stream).Inc() }
}

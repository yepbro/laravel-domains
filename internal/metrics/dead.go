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
		Help: "Переводы задач в q:dead (Reliab.bury, T-154): запись исчерпала лимит попыток доставок без подтверждения и ушла в карантин ручного разбора. Метка queue — стрим-источник задачи. Рост — смерти задач: темп(rate) — поток смертей, неразобранный остаток — crawler_queue_depth{queue=\"q:dead\"} монитора (алерт queue_dead); разбор — runbooks §2.",
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
		Help: "Неидентифицируемые записи, переведённые в q:dead (Reliab.bury, T-260/ФТ-6.1): запись PEL без task_id (тело вне форматов пакета) исчерпала лимит попыток доставок; переведена с синтетическим task_id unknown:<stream>:<entry-id>, без предметных полей — разбор ручной (runbooks §2). Метка queue — стрим-источник записи. Стационарно ноль: рост — записи публикуются мимо контракта пакета (ручной XADD, баг писателя) — найти источник по entry_id.",
	}, LabelQueue)
	return func(stream string) { unidentified.WithLabelValues(stream).Inc() }
}

// Crawler unit event metrics (T-145, spec 5.9): the crawler_unit_oom_events
// {unit,kind} gauge (oom/oom_kill from memory.events) and
// crawler_unit_restarts{unit} (NRestarts). The values are monotonic within
// the life of a cgroup/unit but reset when the cgroup is recreated on a
// restart — a gauge, not a counter: a counter reset in Prometheus falsely
// looks like a rate/increase jump, while a gauge honestly reflects "the
// current number of unit events" (the counter semantics of
// crawler_laravel_appeared_total are correct precisely because its log is
// append-only and has no resets).
//
// Architecturally this is a wrapper over the UnitEventsSource, not a hook
// on AlertsOptions.OnPass: the data feeding the instruments is the input
// of the alert pass, not the outcome (AlertSnapshot carries no per-unit
// values), and duplicating the source reads with a separate loop is
// expensive. One pass reads the source once — the wrapper mirrors the
// result into the metrics and returns it to the evaluator; the same seam
// as CleanupMetrics.Observe (T-099), with the difference that the seam is
// at the source, not at the hook.
package monitor

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// UnitEventsMetrics is crawler unit event instruments on top of another
// UnitEventsSource (transit + mirroring into metrics).
type UnitEventsMetrics struct {
	// events is crawler_unit_oom_events{unit,kind}: oom/oom_kill.
	events *prometheus.GaugeVec
	// memHigh is crawler_unit_memory_high_events{unit}: memory.events high.
	memHigh *prometheus.GaugeVec
	// restarts is crawler_unit_restarts{unit}: NRestarts.
	restarts *prometheus.GaugeVec
	// src is the underlying source (required).
	src UnitEventsSource
}

// compile-time: UnitEventsMetrics is a UnitEventsSource.
var _ UnitEventsSource = (*UnitEventsMetrics)(nil)

// NewUnitEventsMetrics wraps src with a metrics source in reg;
// a nil reg is pure transit (isolated builds without a registry, the
// NewNearGauge style). Idempotent (T-076): re-registering identical
// instruments shares the already created vectors.
func NewUnitEventsMetrics(reg *metrics.Registry, src UnitEventsSource) *UnitEventsMetrics {
	m := &UnitEventsMetrics{src: src}
	if reg == nil {
		return m
	}
	m.events = reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_unit_oom_events",
		Help: "Счётчики memory.events cgroup v2 юнитов crawler (T-145): kind=oom — срабатывания OOM-killer, kind=oom_kill — убитые процессы; обнуляются пересозданием cgroup на рестарте юнита (gauge, не counter). Рост — события OOM, алерт unit_oom монитора.",
	}, metrics.LabelUnit, metrics.LabelKind)
	m.memHigh = reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_unit_memory_high_events",
		Help: "Счётчик memory.events high юнитов crawler (T-162): попадания за мягкий потолок MemoryHigh (троттлинг reclaim'ом); накапливается за жизнь cgroup (gauge, не counter). Рост между проходами алертов — устойчивый троттлинг, алерт memory_high (предвестник фаталов T-157 у http-worker).",
	}, metrics.LabelUnit)
	m.restarts = reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_unit_restarts",
		Help: "NRestarts systemd-юнитов crawler (T-145): крэш-рестарты с последнего старта/reset-failed; обнуляются сбросом состояния юнита (gauge, не counter). Рост — падения юнитов, алерт unit_oom монитора.",
	}, metrics.LabelUnit)
	return m
}

// UnitEvents reads the underlying source and mirrors the result into
// the metrics. A source error leaves the metrics untouched — they keep
// the last value (hold-last, like the t092 sampler): a read failure
// does not tear the graph.
func (m *UnitEventsMetrics) UnitEvents(ctx context.Context) ([]UnitEvents, error) {
	list, err := m.src.UnitEvents(ctx)
	if err != nil {
		return nil, err
	}
	if m.events != nil {
		for _, u := range list {
			m.events.WithLabelValues(u.Unit, unitEventOOM).Set(float64(u.OOM))
			m.events.WithLabelValues(u.Unit, unitEventOOMKill).Set(float64(u.OOMKill))
			m.memHigh.WithLabelValues(u.Unit).Set(float64(u.High))
			m.restarts.WithLabelValues(u.Unit).Set(float64(u.NRestarts))
		}
	}
	return list, nil
}

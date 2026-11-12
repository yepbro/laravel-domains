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
		Help: "The memory.events counters of the crawler units' cgroup v2 (T-145): kind=oom — OOM-killer firings, kind=oom_kill — killed processes; zeroed by the cgroup recreation on a unit restart (gauge, not counter). Growth — OOM events, the monitor unit_oom alert.",
	}, metrics.LabelUnit, metrics.LabelKind)
	m.memHigh = reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_unit_memory_high_events",
		Help: "The memory.events high counter of the crawler units (T-162): hits over the soft MemoryHigh cap (reclaim throttling); accumulates over the cgroup lifetime (gauge, not counter). Growth between the alert passes — sustained throttling, the memory_high alert (the harbinger of the http-worker T-157 fatals).",
	}, metrics.LabelUnit)
	m.restarts = reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_unit_restarts",
		Help: "NRestarts of the crawler systemd units (T-145): crash restarts since the last start/reset-failed; zeroed by a unit state reset (gauge, not counter). Growth — unit crashes, the monitor unit_oom alert.",
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

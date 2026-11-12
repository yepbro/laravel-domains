// The q:dead depth metric (T-154, spec 5.9): monitor is the designated
// observer of the quarantine. q:dead has no consumers (manual triage,
// spec 4.2), so T-077 did not publish the depth of this stream ("the metric
// will appear together with a consumer"); T-154 makes the monitor itself the
// observer: the depth gate wrapper mirrors Len(q:dead) into the shared gauge
// crawler_queue_depth{queue="q:dead"} in the monitor registry — the same
// metric and label as the worker collectors (collect.go); the scalar is
// consistent with the dashboard's q:dead highlight (T-126/T-144: depth > 0)
// and the queue_dead alert (alerts.go).
//
// The other streams pass through without a mirror: their depth is published
// by the consumer services (T-077); a duplicate from monitor would double the
// rows in the PromQL aggregates. A read error does not touch the metric (hold-last).
package monitor

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
	"crawler/internal/queue"
)

// DeadDepthMetrics — a StreamLener mirroring the q:dead depth into the
// crawler_queue_depth gauge (pass-through for the other streams).
type DeadDepthMetrics struct {
	// depth — crawler_queue_depth{queue}: only q:dead.
	depth *prometheus.GaugeVec
	// src — the underlying depth source (required).
	src StreamLener
}

// compile-time: DeadDepthMetrics — StreamLener.
var _ StreamLener = (*DeadDepthMetrics)(nil)

// NewDeadDepthMetrics wraps src: Len(q:dead) is mirrored into the metric,
// the other streams — pure pass-through. A nil reg — pass-through for all (the
// tests seam, the NewUnitEventsMetrics style); the same gauge vector as the
// worker collectors (metrics.NewQueueGauge, idempotent per T-076).
func NewDeadDepthMetrics(reg *metrics.Registry, src StreamLener) *DeadDepthMetrics {
	m := &DeadDepthMetrics{src: src}
	if reg != nil {
		m.depth = metrics.NewQueueGauge(reg)
	}
	return m
}

// Len reads the underlying source; the q:dead depth is additionally mirrored
// into crawler_queue_depth.
func (m *DeadDepthMetrics) Len(ctx context.Context, stream string) (int64, error) {
	n, err := m.src.Len(ctx, stream)
	if err == nil && stream == queue.StreamDead && m.depth != nil {
		m.depth.WithLabelValues(stream).Set(float64(n))
	}
	return n, err
}

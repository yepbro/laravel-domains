// The queue depth collector (T-077, spec 5.9 "the queue lengths"): a
// periodic depth of the working q:* streams → the gauge
// crawler_queue_depth{queue}.
//
// The architectural decision (fixed by spec 5.9): the collector lives in
// the service that consumes the stream — the dns-worker measures q:dns,
// the http-worker — q:http, the render-worker — q:render, the analyzer —
// q:analyze. The depth is the backlog of the consumer group (the unread
// lag remainder capped by the stream length plus the unacknowledged
// pending deliveries; the source is the backpressure gate T-026, which
// already holds the Redis client): this is the true "queue length" — the
// XLEN of the stream also counts the acknowledged history up to the
// MAXLEN trim. Capping the lag by the stream length is a clamp against
// the phantom lag of Redis 7.0.x after an XTRIM of an undelivered entry
// (T-168): the lag counter does not subtract what was trimmed. Binding
// to the consumer guarantees: the queue lives (is read) ⇔ its depth is
// published. The q:dead stream has no consumer (manual triage, spec 4.2)
// — its depth is shown by crawlerctl status (T-079); the metric will
// appear together with the consumer. The depth source is injected by the
// LenFunc function (the Len method of the queue.BackpressureGate gate) —
// the package does not depend on Redis.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// DefaultQueueCollectInterval — the depth polling period: 15 s is a
// typical Prometheus scrape interval (the dev config
// deploy/docker/prometheus): every scrape sees a fresh value; more often
// — needless Redis calls.
const DefaultQueueCollectInterval = 15 * time.Second

// LenFunc — the source of the stream depth: satisfied by the Len method
// of the backpressure gate (*queue.BackpressureGate, T-026) — the same
// Redis client as the service's queue; the unit tests substitute a fake.
type LenFunc func(ctx context.Context, stream string) (int64, error)

// NewQueueGauge creates the queue depth gauge crawler_queue_depth{queue}
// in the reg registry (the service label is substituted by the factory);
// nil — nil: a service without a registry (isolated builds) does not
// raise the collector. Idempotent (the duplicate registration of T-076
// returns the existing vector).
func NewQueueGauge(reg *Registry) *prometheus.GaugeVec {
	if reg == nil {
		return nil
	}
	return reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_queue_depth",
		Help: "Глубина очереди задач: непрочитанный группой остаток (ограничен длиной стрима) и неподтверждённые доставки (backlog групп потребления).",
	}, LabelQueue)
}

// CollectQueueDepths — one pass of the collector: the depth of each
// stream of streams via lenOf → Set the gauge value (the queue label is
// the stream name). The stream errors are independent: a failure of one
// does not disturb the others; all the errors are collected into one
// return (errors.Join); the failed stream's gauge keeps its last
// successful value.
func CollectQueueDepths(ctx context.Context, gauge *prometheus.GaugeVec, lenOf LenFunc, streams []string) error {
	if gauge == nil || lenOf == nil {
		return errors.New("metrics: сбор глубины очередей: gauge и источник глубины обязательны")
	}
	var errs []error
	for _, stream := range streams {
		n, err := lenOf(ctx, stream)
		if err != nil {
			errs = append(errs, fmt.Errorf("metrics: глубина %s: %w", stream, err))
			continue
		}
		gauge.WithLabelValues(stream).Set(float64(n))
	}
	return errors.Join(errs...)
}

// CollectQueueLoop — the periodic loop of the queue depth collections:
// the first pass right at the start (fresh values after a restart do not
// wait for a tick), then — by the ticker with the interval period
// (0 — DefaultQueueCollectInterval). An error of a pass interrupts only
// that pass: a Warn is logged and the loop lives — the next pass will
// retry (the OnRun contract of the framework, T-012); in the meantime the
// gauge hands out the last known values. A cancellation of ctx is a
// clean exit (graceful shutdown, spec 3.4). Nil dependencies and an empty
// stream set — a no-op: a service without a registry must not fall at
// startup.
func CollectQueueLoop(
	ctx context.Context,
	reg *Registry,
	lenOf LenFunc,
	streams []string,
	interval time.Duration,
	log *slog.Logger,
) error {
	gauge := NewQueueGauge(reg)
	if gauge == nil || lenOf == nil || len(streams) == 0 {
		return nil
	}
	if interval <= 0 {
		interval = DefaultQueueCollectInterval
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("коллектор глубины очередей", "streams", streams, "interval", interval.String())

	pass := func() {
		if err := CollectQueueDepths(ctx, gauge, lenOf, streams); err != nil && ctx.Err() == nil {
			log.Warn("сбор глубины очередей", "err", err)
		}
	}

	pass()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pass()
		}
	}
}

// Redis memory metrics (T-154, spec 5.9): the crawler_redis_memory_ratio
// gauge (used_memory/maxmemory — a proxy metric for upcoming OOM write
// failures under noeviction) and crawler_redis_memory_bytes{kind=used|max}.
//
// Architecturally this is a wrapper over the RedisMemorySource, not a
// hook on AlertsOptions.OnPass: the same seam as UnitEventsMetrics
// (T-145) — the data feeding the instruments is the input of the alert
// pass, duplicating the source read with a separate loop is expensive,
// so one pass reads the source once and mirrors the snapshot into the
// metrics. A source error leaves the metrics untouched (hold-last, the
// t092 sampler style): a read failure does not tear the graph. With
// maxmemory 0 (no cap) the ratio is undefined and is not written, while
// bytes are: absolutes are honest even without a cap.
package monitor

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// Values of the kind label of the crawler_redis_memory_bytes metric.
const (
	// redisMemKindUsed is used_memory.
	redisMemKindUsed = "used"
	// redisMemKindMax is maxmemory.
	redisMemKindMax = "max"
)

// RedisMemoryMetrics is Redis memory instruments on top of another
// RedisMemorySource (transit + mirroring into metrics).
type RedisMemoryMetrics struct {
	// ratio is crawler_redis_memory_ratio: used_memory/maxmemory.
	ratio *prometheus.GaugeVec
	// bytes is crawler_redis_memory_bytes{kind}: used/max.
	bytes *prometheus.GaugeVec
	// src is the underlying source (required).
	src RedisMemorySource
}

// compile-time: RedisMemoryMetrics is a RedisMemorySource.
var _ RedisMemorySource = (*RedisMemoryMetrics)(nil)

// NewRedisMemoryMetrics wraps src with a metrics source in reg;
// a nil reg is pure transit (isolated builds without a registry, the
// NewUnitEventsMetrics style). Idempotent (T-076).
func NewRedisMemoryMetrics(reg *metrics.Registry, src RedisMemorySource) *RedisMemoryMetrics {
	m := &RedisMemoryMetrics{src: src}
	if reg == nil {
		return m
	}
	m.ratio = reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_redis_memory_ratio",
		Help: "Доля памяти Redis used_memory/maxmemory (INFO memory, T-154). При noeviction — прокси-метрика предстоящих OOM-отказов записи (алерт redis_memory монитора при ≥ 0.8). maxmemory не задан (0) — ряд не пишется: доля не определена.",
	})
	m.bytes = reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_redis_memory_bytes",
		Help: "Память Redis из INFO memory (T-154): kind=used — used_memory, kind=max — maxmemory (0 — потолка нет).",
	}, metrics.LabelKind)
	return m
}

// RedisMemory reads the underlying source and mirrors the snapshot into the metrics.
func (m *RedisMemoryMetrics) RedisMemory(ctx context.Context) (RedisMemory, error) {
	snap, err := m.src.RedisMemory(ctx)
	if err != nil {
		return RedisMemory{}, err
	}
	if m.bytes != nil {
		m.bytes.WithLabelValues(redisMemKindUsed).Set(float64(snap.UsedBytes))
		m.bytes.WithLabelValues(redisMemKindMax).Set(float64(snap.MaxBytes))
	}
	if m.ratio != nil && snap.MaxBytes > 0 {
		m.ratio.WithLabelValues().Set(float64(snap.UsedBytes) / float64(snap.MaxBytes))
	}
	return snap, nil
}

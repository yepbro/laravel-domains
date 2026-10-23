// Метрики памяти Redis (T-154, ТЗ 5.9): gauge crawler_redis_memory_ratio
// (used_memory/maxmemory — прокси-метрика предстоящих OOM-отказов записи
// при noeviction) и crawler_redis_memory_bytes{kind=used|max}.
//
// Архитектурно — обёртка источника RedisMemorySource, а не хук
// AlertsOptions.OnPass: тот же шов, что UnitEventsMetrics (T-145), —
// данные для приборов это вход прохода алертов, чтение источника
// дорого дублировать отдельным циклом, один проход читает источник один
// раз и зеркалит снимок в метрики. Ошибка источника метрики не трогает
// (hold-last, стиль t092-сэмплера): сбой чтения не рвёт график.
// maxmemory 0 (без потолка) — ratio не определён и не пишется, bytes
// пишутся: абсолюты честны и без потолка.
package monitor

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// Значения метки kind метрики crawler_redis_memory_bytes.
const (
	// redisMemKindUsed — used_memory.
	redisMemKindUsed = "used"
	// redisMemKindMax — maxmemory.
	redisMemKindMax = "max"
)

// RedisMemoryMetrics — приборы памяти Redis поверх другого
// RedisMemorySource (транзит + зеркало в метрики).
type RedisMemoryMetrics struct {
	// ratio — crawler_redis_memory_ratio: used_memory/maxmemory.
	ratio *prometheus.GaugeVec
	// bytes — crawler_redis_memory_bytes{kind}: used/max.
	bytes *prometheus.GaugeVec
	// src — исходный источник (обязателен).
	src RedisMemorySource
}

// compile-time: RedisMemoryMetrics — RedisMemorySource.
var _ RedisMemorySource = (*RedisMemoryMetrics)(nil)

// NewRedisMemoryMetrics оборачивает src источником с метриками в reg;
// nil reg — чистый транзит (изолированные сборки без реестра, стиль
// NewUnitEventsMetrics). Идемпотентен (T-076).
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

// RedisMemory читает исходный источник и зеркалит снимок в метрики.
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

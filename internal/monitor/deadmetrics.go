// Метрика глубины q:dead (T-154, ТЗ 5.9): monitor — назначенный
// наблюдатель карантина. Потребителей у q:dead нет (ручной разбор,
// ТЗ 4.2), поэтому T-077 глубину этого стрима не публиковал («метрика
// появится вместе с потребителем»); T-154 наблюдателем становится сам
// монитор: обёртка гейта глубин зеркалит Len(q:dead) в общий gauge
// crawler_queue_depth{queue="q:dead"} в реестре monitor — та же
// метрика и метка, что у коллекторов воркеров (collect.go), скаляр
// согласован с подсветкой q:dead дашборда (T-126/T-144: глубина > 0)
// и алертом queue_dead (alerts.go).
//
// Прочие стримы проходят транзитом без зеркала: их глубину публикуют
// сервисы-потребители (T-077), дубль из monitor удваивал бы ряды
// в PromQL-агрегатах. Ошибка чтения метрику не трогает (hold-last).
package monitor

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
	"crawler/internal/queue"
)

// DeadDepthMetrics — StreamLener, зеркалящий глубину q:dead в gauge
// crawler_queue_depth (транзит для остальных стримов).
type DeadDepthMetrics struct {
	// depth — crawler_queue_depth{queue}: только q:dead.
	depth *prometheus.GaugeVec
	// src — исходный источник глубины (обязателен).
	src StreamLener
}

// compile-time: DeadDepthMetrics — StreamLener.
var _ StreamLener = (*DeadDepthMetrics)(nil)

// NewDeadDepthMetrics оборачивает src: Len(q:dead) зеркалится в метрику,
// остальные стримы — чистый транзит. nil reg — транзит для всех (шов
// тестов, стиль NewUnitEventsMetrics); тот же вектор прибора, что у
// коллекторов воркеров (metrics.NewQueueGauge, идемпотентен T-076).
func NewDeadDepthMetrics(reg *metrics.Registry, src StreamLener) *DeadDepthMetrics {
	m := &DeadDepthMetrics{src: src}
	if reg != nil {
		m.depth = metrics.NewQueueGauge(reg)
	}
	return m
}

// Len читает исходный источник; глубина q:dead дополнительно зеркалится
// в crawler_queue_depth.
func (m *DeadDepthMetrics) Len(ctx context.Context, stream string) (int64, error) {
	n, err := m.src.Len(ctx, stream)
	if err == nil && stream == queue.StreamDead && m.depth != nil {
		m.depth.WithLabelValues(stream).Set(float64(n))
	}
	return n, err
}

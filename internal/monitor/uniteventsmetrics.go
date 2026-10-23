// Метрики событий юнитов crawler (T-145, ТЗ 5.9): gauge
// crawler_unit_oom_events{unit,kind} (oom/oom_kill из memory.events) и
// crawler_unit_restarts{unit} (NRestarts). Значения монотонны внутри
// жизни cgroup/юнита, но обнуляются пересозданием cgroup на рестарте —
// gauge, а не counter: сброс counter'а в Prometheus лживо выглядит
// скачком rate/increase, gauge честно отражает «текущее число событий
// юнита» (counter-семантика у crawler_laravel_appeared_total корректна
// ровно потому, что там журнал append-only и сбросов нет).
//
// Архитектурно — обёртка источника UnitEventsSource, а не хук
// AlertsOptions.OnPass: данные для приборов — вход прохода алертов, а
// не итог (AlertSnapshot не несёт пер-юнитных значений), и чтение
// источников дорого дублировать отдельным циклом. Один проход читает
// источник один раз — обёртка зеркалит результат в метрики и возвращает
// его оценщику; тот же шов, что CleanupMetrics.Observe (T-099), с той
// разницей, что шов у источника, а не у хука.
package monitor

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// UnitEventsMetrics — приборы событий юнитов поверх другого
// UnitEventsSource (транзит + зеркало в метрики).
type UnitEventsMetrics struct {
	// events — crawler_unit_oom_events{unit,kind}: oom/oom_kill.
	events *prometheus.GaugeVec
	// memHigh — crawler_unit_memory_high_events{unit}: memory.events high.
	memHigh *prometheus.GaugeVec
	// restarts — crawler_unit_restarts{unit}: NRestarts.
	restarts *prometheus.GaugeVec
	// src — исходный источник (обязателен).
	src UnitEventsSource
}

// compile-time: UnitEventsMetrics — UnitEventsSource.
var _ UnitEventsSource = (*UnitEventsMetrics)(nil)

// NewUnitEventsMetrics оборачивает src источником с метриками в reg;
// nil reg — чистый транзит (изолированные сборки без реестра, стиль
// NewNearGauge). Идемпотентен (T-076): повторная регистрация идентичных
// приборов делит уже созданные векторы.
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

// UnitEvents читает исходный источник и зеркалит результат в метрики.
// Ошибка источника метрики не трогает — держат последнее значение
// (hold-last, как сэмплер t092): сбой чтения не рвёт график.
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

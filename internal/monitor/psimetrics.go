// Метрики давления CPU по PSI (T-162): gauge
// crawler_psi_cpu_some_percent{kind=avg10|avg60|avg300} — some-доля
// времени задержки CPU из /proc/pressure/cpu, тот же проход, что питает
// алерт psi_cpu. Обёртка источника PSISource, а не отдельный цикл:
// чтение /proc дёшево, но точка одна — значения метрики и порог алерта
// не расходятся (стиль UnitEventsMetrics, T-145).
package monitor

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// Значения метки kind метрики crawler_psi_cpu_some_percent.
const (
	psiKindAvg10  = "avg10"
	psiKindAvg60  = "avg60"
	psiKindAvg300 = "avg300"
)

// PSIMetrics — приборы PSI поверх другого PSISource (транзит + зеркало
// в метрики).
type PSIMetrics struct {
	// gauge — crawler_psi_cpu_some_percent{kind}.
	gauge *prometheus.GaugeVec
	// src — исходный источник (обязателен).
	src PSISource
}

// compile-time: PSIMetrics — PSISource.
var _ PSISource = (*PSIMetrics)(nil)

// NewPSIMetrics оборачивает src источником с метриками в reg; nil reg —
// чистый транзит (стиль NewUnitEventsMetrics). Идемпотентен (T-076).
func NewPSIMetrics(reg *metrics.Registry, src PSISource) *PSIMetrics {
	m := &PSIMetrics{src: src}
	if reg == nil {
		return m
	}
	m.gauge = reg.MustGauge(prometheus.GaugeOpts{
		Name: "crawler_psi_cpu_some_percent",
		Help: "Доля времени задержки CPU по PSI some (%) из /proc/pressure/cpu (T-162): kind=avg10/avg60/avg300 — средние за окно. Устойчивый avg300 выше ~90 % — голод планировщика, алерт psi_cpu монитора (тонут все этапы; замер §12: some 91–95 %).",
	}, metrics.LabelKind)
	return m
}

// PSI читает исходный источник и зеркалит результат в метрики. Ошибка
// источника метрики не трогает — держат последнее значение (hold-last,
// стиль UnitEventsMetrics): сбой чтения не рвёт график.
func (m *PSIMetrics) PSI(ctx context.Context) (PSIStat, error) {
	st, err := m.src.PSI(ctx)
	if err != nil {
		return PSIStat{}, err
	}
	if m.gauge != nil {
		m.gauge.WithLabelValues(psiKindAvg10).Set(st.Avg10)
		m.gauge.WithLabelValues(psiKindAvg60).Set(st.Avg60)
		m.gauge.WithLabelValues(psiKindAvg300).Set(st.Avg300)
	}
	return st, nil
}

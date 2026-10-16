// Package metrics — метрики Prometheus сервисов Domain Crawler (ТЗ 5.9).
//
// Пока является заготовкой: создаёт реестр с базовыми метриками каркаса
// (crawler_service_up, crawler_build_info) и стандартными коллекторами
// Go/runtime. Метрики этапов, очередей и ошибок добавляются по мере
// реализации сервисов (T-076, T-077).
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// New создаёт реестр Prometheus для сервиса name и регистрирует базовые
// метрики каркаса и коллекторы процесса/runtime. Возвращённый реестр можно
// пополнять метриками самого сервиса.
func New(name string) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	up := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "crawler_service_up",
		Help: "Сервис запущен и отвечает.",
	}, []string{"service"})
	up.WithLabelValues(name).Set(1)

	build := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "crawler_build_info",
		Help: "Информация о сборке (режим dev-каркаса).",
	}, []string{"service", "mode"})
	build.WithLabelValues(name, "dev-stub").Set(1)

	reg.MustRegister(up, build)
	return reg
}

// Handler возвращает HTTP-хендлер, отдающий метрики реестра reg в формате
// Prometheus (эндпоинт /metrics каждого сервиса, ТЗ 5.9).
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}

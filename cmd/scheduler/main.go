// Сервис scheduler (см. основное ТЗ). Сейчас это заглушка dev-каркаса:
// healthcheck/metrics + graceful shutdown; рабочая логика добавляется
// по мере реализации этапов основного ТЗ.
package main

import (
	"crawler/internal/service"
)

func main() {
	service.Run(service.Options{
		Name:        "scheduler",
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9102"),
	})
}

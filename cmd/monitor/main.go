// Сервис monitor (см. основное ТЗ). Сейчас это заглушка dev-каркаса:
// healthcheck/metrics + graceful shutdown; рабочая логика добавляется
// по мере реализации этапов основного ТЗ.
package main

import (
	"crawler/internal/service"
)

func main() {
	service.Run(service.Options{
		Name:        "monitor",
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9100"),
		PGDSN:       service.EnvOr("APP_PG_DSN", ""),
	})
}

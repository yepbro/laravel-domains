// Сервис http-worker (см. основное ТЗ). Сейчас это заглушка dev-каркаса:
// healthcheck/metrics + graceful shutdown; рабочая логика добавляется
// по мере реализации этапов основного ТЗ.
package main

import (
	"crawler/internal/service"
)

func main() {
	service.Run(service.Options{
		Name:        "http-worker",
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9104"),
		PGDSN:       service.EnvOr("APP_PG_DSN", ""),
	})
}

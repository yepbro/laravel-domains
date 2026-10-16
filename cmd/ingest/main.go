// Сервис ingest (см. основное ТЗ). Сейчас это заглушка dev-каркаса:
// healthcheck/metrics + graceful shutdown; рабочая логика добавляется
// по мере реализации этапов основного ТЗ.
package main

import (
	"log"

	"crawler/internal/config"
	"crawler/internal/service"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[ingest] %v", err)
	}
	service.Run(service.Options{
		Name:        "ingest",
		Config:      cfg,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9101"),
	})
}

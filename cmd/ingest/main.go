// Сервис ingest (см. основное ТЗ). Сейчас это заглушка dev-каркаса:
// healthz/metrics + graceful shutdown; рабочая логика (этап 1 ТЗ)
// подключается через service.Options.OnRun.
package main

import (
	"log"
	"os"

	"crawler/internal/config"
	"crawler/internal/service"
)

func main() {
	// Ошибка конфигурации — до инициализации логгера: stderr и код 1.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[ingest] конфигурация: %v", err)
	}
	logger := service.NewLogger("ingest", cfg)
	if err := service.Run(service.Options{
		Name:        "ingest",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9101"),
	}); err != nil {
		os.Exit(1)
	}
}

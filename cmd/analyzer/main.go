// Сервис analyzer (см. основное ТЗ). Сейчас это заглушка dev-каркаса:
// healthz/metrics + graceful shutdown; рабочая логика (этап 7 ТЗ)
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
		log.Fatalf("[analyzer] конфигурация: %v", err)
	}
	logger := service.NewLogger("analyzer", cfg)
	if err := service.Run(service.Options{
		Name:        "analyzer",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9106"),
	}); err != nil {
		os.Exit(1)
	}
}

// Сервис monitor (см. основное ТЗ). Сейчас это заглушка dev-каркаса:
// healthz/metrics + graceful shutdown; рабочая логика (агрегация прогресса
// зон, ТЗ 5.9) подключается через service.Options.OnRun.
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
		log.Fatalf("[monitor] конфигурация: %v", err)
	}
	logger := service.NewLogger("monitor", cfg)
	// Адрес мониторинга — monitor.listen из конфигурации (ТЗ 5.10);
	// METRICS_ADDR из окружения важнее (адреса метрик остальных сервисов
	// задаются только переменной, ТЗ 5.9).
	if err := service.Run(service.Options{
		Name:        "monitor",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", cfg.Monitor.Listen),
	}); err != nil {
		os.Exit(1)
	}
}

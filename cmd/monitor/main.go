// Сервис monitor (см. основное ТЗ). Сейчас это заглушка dev-каркаса:
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
		log.Fatalf("[monitor] %v", err)
	}
	// Адрес мониторинга — monitor.listen из конфигурации (ТЗ 5.10);
	// METRICS_ADDR из окружения важнее (адреса метрик остальных сервисов
	// задаются только переменной, ТЗ 5.9).
	service.Run(service.Options{
		Name:        "monitor",
		Config:      cfg,
		MetricsAddr: service.EnvOr("METRICS_ADDR", cfg.Monitor.Listen),
	})
}

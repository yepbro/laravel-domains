// Сервис monitor (ТЗ 5.9): периодический пакетный пересчёт кэширующих
// счётчиков прогресса зон zones.domains_total / domains_checked
// (internal/monitor.ZoneStatsLoop, T-078) с периодом
// monitor.zone_stats_interval. Каркас — internal/service: миграции, пул БД
// (OnDBPool), /healthz и /metrics, graceful shutdown (ТЗ 3.4).
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
	"crawler/internal/monitor"
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
	//
	// Пул приходит от каркаса после миграций (OnDBPool) и гарантированно
	// готов до старта OnRun: postgres.dsn обязателен (config.Validate).
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "monitor",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", cfg.Monitor.Listen),
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		OnRun:       func(ctx context.Context) error { return run(ctx, cfg, logger, pool) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — рабочая логика monitor: единственный цикл пересчёта счётчиков
// зон (ТЗ 5.9, ТЗ 11.4 п. 6). Первый проход — сразу при старте, далее —
// по ticker; ошибки прохода — Warn, цикл живёт; отмена ctx (graceful
// shutdown, ТЗ 3.4) — чистый nil.
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("monitor: пул БД не получен (postgres.dsn обязателен)")
	}
	return monitor.ZoneStatsLoop(ctx, pool, monitor.ZoneStatsOptions{
		Interval: cfg.Monitor.ZoneStatsInterval.D(),
		Log:      logger,
	})
}

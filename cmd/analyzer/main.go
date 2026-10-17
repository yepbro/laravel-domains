// Сервис analyzer (ТЗ 5.8, этап 7): анализ сохранённого контента по
// правилам Laravel. Подключена фоновая постановка переанализа (T-074);
// воркер потребления q:analyze и запись laravel_findings — T-071;
// каркас — healthz/metrics + graceful shutdown (internal/service).
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/analyzer"
	"crawler/internal/config"
	"crawler/internal/queue"
	"crawler/internal/rules"
	"crawler/internal/service"
)

func main() {
	// Ошибка конфигурации — до инициализации логгера: stderr и код 1.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[analyzer] конфигурация: %v", err)
	}
	logger := service.NewLogger("analyzer", cfg)

	// Набор правил (T-068): стартовая загрузка до каркаса — сервис без
	// валидного набора не стартует; перезагрузка — SIGHUP (make
	// rules-reload). Потребление q:analyze — T-071.
	reloader, err := rules.NewReloader(cfg.Analyzer.RulesDir, logger)
	if err != nil {
		log.Fatalf("[analyzer] правила: %v", err)
	}

	// Пул приходит от каркаса после миграций (OnDBPool) и гарантированно
	// готов до старта OnRun: postgres.dsn обязателен (config.Validate).
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "analyzer",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9106"),
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		OnSIGHUP:    reloader.HandleSIGHUP,
		// Фоновый цикл переанализа (T-074, ТЗ 5.8) — единственная
		// горутина OnRun: ставит записи с устаревшим rules_version в
		// q:analyze без повторного скачивания. Сборку воркера потребления
		// (T-071) подключат к этому же OnRun.
		OnRun: func(ctx context.Context) error {
			return runReanalyze(ctx, cfg, logger, reloader, pool)
		},
	}); err != nil {
		os.Exit(1)
	}
}

// runReanalyze — подключение цикла переанализа (T-074): очередь с гейтом
// backpressure (T-026) → analyzer.ReanalyzeLoop (стартовый дренаж +
// тикер + внеочередной проход по смене набора правил). Возврат — по
// отмене ctx (чистый nil) либо ошибке подключения.
func runReanalyze(ctx context.Context, cfg *config.Config, logger *slog.Logger, reloader *rules.Reloader, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("analyzer: пул БД не получен (postgres.dsn обязателен)")
	}
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(cfg.Scheduler.QueueHighWatermark),
		Log:       logger,
	})
	q := queue.NewWatermarkPublisher(queue.NewRedisQueue(rdb, queue.RedisOptions{}), gate)
	return analyzer.ReanalyzeLoop(ctx, pool, q, reloader, analyzer.ReanalyzeOptions{
		Log: logger,
	})
}

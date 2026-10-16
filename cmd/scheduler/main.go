// Сервис scheduler (ТЗ 5.3, 4.2): главный цикл постановки доменов на
// проверку — выборка пачек pending с чередованием зон (internal/scheduler
// DispatchBatch, T-027), перевод в queued с lease и публикация пачек
// DNSBatch в q:dns (EnqueueBatch/EnqueueLoop, T-028) — и фоновые циклы в
// тех же OnRun: суточная перепроверка done-доменов (RecheckDaily, T-030)
// и повторные проверки temp_error (RetryTempErrors, T-031). Возврат
// доменов с истёкшим lease — T-029 (параллельная задача, подключается
// своей сборкой). Очередь — Redis Streams (internal/queue, клиент
// queue.NewClient/RedisQueue); каркас — internal/service: миграции, пул БД
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
	"crawler/internal/queue"
	"crawler/internal/scheduler"
	"crawler/internal/service"
)

func main() {
	// Ошибка конфигурации — до инициализации логгера: stderr и код 1.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[scheduler] конфигурация: %v", err)
	}
	logger := service.NewLogger("scheduler", cfg)

	// Пул приходит от каркаса после миграций (OnDBPool) и гарантированно
	// готов до старта OnRun: postgres.dsn обязателен (config.Validate).
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "scheduler",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9102"),
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		OnRun:       func(ctx context.Context) error { return run(ctx, cfg, logger, pool) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — рабочая логика scheduler: главный цикл постановки пачек в q:dns
// и фоновые циклы перепроверок (T-030/T-031) горутинами в том же OnRun —
// они возвращают домены в pending, главный цикл подберёт их следующей
// пачкой. Завершение — отмена ctx (graceful shutdown, ТЗ 3.4): цикл
// дорабатывает текущую пачку, фоновые — текущий проход, все возвращают nil.
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("scheduler: пул БД не получен (postgres.dsn обязателен)")
	}

	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	// MaxLen/backpressure — политика надёжности очереди T-025/T-026;
	// постановке нужен только Publish.
	dnsq := queue.NewRedisQueue(rdb, queue.RedisOptions{})

	bg := make(chan error, 2)
	go func() {
		bg <- scheduler.RecheckDaily(ctx, pool, scheduler.RecheckOptions{
			Interval: cfg.Scheduler.RecheckInterval.D(),
			Log:      logger,
		})
	}()
	go func() {
		bg <- scheduler.RetryTempErrors(ctx, pool, scheduler.RetryOptions{
			RetryAfter: cfg.Scheduler.TempErrorRetryAfter.D(),
			MaxRetries: cfg.Scheduler.TempErrorMaxRetries,
			Log:        logger,
		})
	}()

	// Главный цикл живёт до отмены ctx; после него фоновые циклы уже
	// завершаются по той же отмене — забираем их результаты.
	err := scheduler.EnqueueLoop(ctx, pool, dnsq, scheduler.EnqueueOptions{
		BatchSize: cfg.Scheduler.BatchSize,
		Log:       logger,
	})
	<-bg
	<-bg
	return err
}

// Сервис monitor (ТЗ 5.9): два цикла в OnRun — периодический пакетный
// пересчёт кэширующих счётчиков прогресса зон zones.domains_total /
// domains_checked (internal/monitor.ZoneStatsLoop, T-078) с периодом
// monitor.zone_stats_interval и оценка алертов минимального набора ТЗ 5.9
// (internal/monitor.AlertsLoop, T-080): доля ошибок DNS, переполнение
// очередей, остановка прогресса, свободное место каталога контента,
// износ NVMe (SMART). Каркас — internal/service: миграции, пул БД
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
	"crawler/internal/queue"
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

// run — рабочая логика monitor: пересчёт счётчиков зон (ТЗ 5.9, T-078)
// фоновым циклом и оценка алертов (ТЗ 5.9, T-080) циклом первого плана —
// алерт остановки прогресса читает zones.updated_at, который пишет именно
// фоновый цикл. Оба завершаются отменой ctx (graceful shutdown, ТЗ 3.4);
// после своего цикла забираем результат фонового (стиль cmd/scheduler).
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("monitor: пул БД не получен (postgres.dsn обязателен)")
	}

	// Глубина стримов для алерта очередей — через BackpressureGate
	// (T-026): тот же узкий API над go-redis, что у планировщика и
	// crawlerctl status (T-079); сам гейт ничего не ждёт — только Len.
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(cfg.Scheduler.QueueHighWatermark),
		Log:       logger,
	})

	// Производная отмена: ранний выход своего цикла останавливает
	// и фоновый — иначе <-bg ждал бы вечно. Сегодня оба цикла возвращают
	// ошибку только на nil-пуле (выше исключён), но контракт фиксируем
	// явно, а не подразумеваем.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	bg := make(chan error, 1)
	go func() {
		bg <- monitor.ZoneStatsLoop(ctx, pool, monitor.ZoneStatsOptions{
			Interval: cfg.Monitor.ZoneStatsInterval.D(),
			Log:      logger,
		})
	}()

	// Период алертов — период пересчёта зон: одна пара циклов
	// мониторинга (коридор ТЗ 5.9 30–60 с), и порог остановки прогресса
	// считается в тех же периодах; отдельного поля конфигурации под
	// период алертов не заводим.
	err := monitor.AlertsLoop(ctx, monitor.AlertsDeps{
		Pool:  pool,
		Redis: gate,
	}, monitor.AlertsOptions{
		Interval:          cfg.Monitor.ZoneStatsInterval.D(),
		ZoneStatsInterval: cfg.Monitor.ZoneStatsInterval.D(),
		QueueWatermark:    int64(cfg.Scheduler.QueueHighWatermark),
		QueueMaxLen:       int64(cfg.Redis.MaxLen),
		ContentDir:        cfg.Content.Dir,
		DiskFreePercent:   cfg.Content.DiskFreePercent,
		Log:               logger,
	})
	// Отмена (своим defer или от каркаса) уже останавливает фоновый цикл;
	// его сбой не прячем за nil алертов — забираем результат.
	bgErr := <-bg
	if err == nil {
		err = bgErr
	}
	return err
}

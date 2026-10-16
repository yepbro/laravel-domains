// Сервис dns-worker (ТЗ 5.4, 4.2, 3.4): DNS-этап проверки доменов.
// Потребляет пачки q:dns (DNSBatch) группой потребителей, для каждого
// домена выполняет эпизод — апекс (internal/dnsworker CheckApex, T-036),
// субдомены (CheckSubdomains, T-037) и wildcard-детект (DetectWildcard/
// MarkWildcardSubdomains, T-038) — и записывает результат (dns_mask,
// статус, done) с публикацией задач q:http по найденным адресам
// (T-039). Обмены идут через internal/dnsclient: клиент с собственным
// пулом UDP-сокетов (T-034) под декоратором лимитов параллелизма/QPS
// с адаптивным снижением (LimitResolver, T-035); параллелизм эпизодов —
// dns.concurrency. Публикация q:http — под гейтом backpressure
// (WatermarkPublisher, T-026). Подтверждение q:dns — паттерном Processing
// (XACK только после записи в БД, T-025). Каркас — internal/service:
// миграции, пул БД (OnDBPool), /healthz и /metrics, graceful shutdown.
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
	"crawler/internal/dnsclient"
	"crawler/internal/dnsworker"
	"crawler/internal/queue"
	"crawler/internal/service"
)

func main() {
	// Ошибка конфигурации — до инициализации логгера: stderr и код 1.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[dns-worker] конфигурация: %v", err)
	}
	logger := service.NewLogger("dns-worker", cfg)

	// Пул приходит от каркаса после миграций (OnDBPool) и гарантированно
	// готов до старта OnRun: postgres.dsn обязателен (config.Validate).
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "dns-worker",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9103"),
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		OnRun:       func(ctx context.Context) error { return run(ctx, cfg, logger, pool) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — рабочая логика dns-worker: клиент DNS (пул сокетов fail-fast —
// недоступный резолвер ошибка старта, T-034) под лимитами T-035 и цикл
// воркера dnsworker.Run. Завершение — отмена ctx (graceful shutdown,
// ТЗ 3.4): начатые эпизоды дорабатывают или возвращаются очереди
// повторной доставкой (Processing, T-025).
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("dns-worker: пул БД не получен (postgres.dsn обязателен)")
	}

	client, err := dnsclient.NewClient(ctx, dnsclient.NewOptions(cfg.DNS))
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	resolve, err := dnsclient.NewLimitResolver(client, dnsclient.NewLimitOptions(cfg.DNS))
	if err != nil {
		return err
	}

	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	// Публикация q:http — под гейтом backpressure (T-026: порог единый
	// для рабочих стримов); потребление q:dns и XACK проходят прозрачно.
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(cfg.Scheduler.QueueHighWatermark),
		Log:       logger,
	})
	q := queue.NewWatermarkPublisher(queue.NewRedisQueue(rdb, queue.RedisOptions{}), gate)

	return dnsworker.Run(ctx, pool, q, resolve, dnsworker.Options{
		Concurrency: cfg.DNS.Concurrency, // параллелизм эпизодов = dns.concurrency (ТЗ 5.10)
		AskAAAA:     cfg.DNS.IPv6,        // AAAA отключаются конфигом без IPv6 (ТЗ 5.4)
		Log:         logger,
	})
}

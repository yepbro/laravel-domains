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
	"crawler/internal/metrics"
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
	// Реестр метрик — здесь же (T-077): приборы DNS-этапа и коллектор
	// длины q:dns получают его до старта OnRun, каркас отдаёт тот же
	// реестр через /metrics.
	reg := metrics.New("dns-worker")
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "dns-worker",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9103"),
		Metrics:     reg,
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		OnRun:       func(ctx context.Context) error { return run(ctx, cfg, logger, pool, reg) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — рабочая логика dns-worker: клиент DNS (пул сокетов fail-fast —
// недоступный резолвер ошибка старта, T-034) под лимитами T-035, цикл
// воркера dnsworker.Run с приборами этапа (T-077) и коллектор длины
// q:dns (метрика глубины очередей, ТЗ 5.9: backlog стрима, который потребляет
// этот воркер). Завершение — отмена ctx (graceful shutdown,
// ТЗ 3.4): начатые эпизоды дорабатывают или возвращаются очереди
// повторной доставкой (Processing, T-025).
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, reg *metrics.Registry) error {
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
		MaxLen:    int64(cfg.Redis.MaxLen), // пределы обрезки — для порога гейта (T-026/T-025)
		Log:       logger,
	})
	q := queue.NewWatermarkPublisher(queue.NewRedisQueue(rdb, queue.RedisOptions{
		MaxLen: int64(cfg.Redis.MaxLen), // XADD MAXLEN ~ на каждом стриме (T-025)
	}), gate)

	// Возврат зависших доставок q:dns (T-025): RecoverLoop в воркере —
	// клейм XAUTOCLAIM, исчерпание попыток — q:dead; параметры надёжности
	// — секция redis (max_attempts, claim_min_idle).
	rel := queue.NewReliab(rdb, q, queue.ReliabOptions{
		MaxAttempts: cfg.Redis.MaxAttempts,
		MinIdle:     cfg.Redis.ClaimMinIdle.D(),
	})

	// Длина потребляемого стрима q:dns → crawler_queue_depth (T-077):
	// источник глубины — гейт (тот же клиент Redis), завершается отменой ctx.
	go func() {
		_ = metrics.CollectQueueLoop(ctx, reg, gate.Len,
			[]string{queue.StreamDNS}, metrics.DefaultQueueCollectInterval, logger)
	}()

	// Состояние лимитёра DNS → gauges (T-035 → T-077): эффективные
	// потолки после адаптива, доля таймаутов в окне, обмены в полёте —
	// тихая деградация резолвера (снижение до пола) видна метрикой,
	// а не только ростом temp_error (DNS-06).
	go func() {
		_ = dnsworker.CollectLimitLoop(ctx, reg, resolve.Stats, 0, logger)
	}()

	return dnsworker.Run(ctx, pool, q, resolve, dnsworker.Options{
		Concurrency: cfg.DNS.Concurrency, // параллелизм эпизодов = dns.concurrency (ТЗ 5.10)
		AskAAAA:     cfg.DNS.IPv6,        // AAAA отключаются конфигом без IPv6 (ТЗ 5.4)
		Recover:     rel,                 // возврат зависших пачек q:dns (T-025)
		Metrics:     reg,                 // приборы этапа: эпизоды, длительности, статусы (T-077)
		Log:         logger,
	})
}

// Сервис http-worker (ТЗ 5.5, 4.2, 3.4): HTTP-этап проверки доменов.
// Потребляет задачи q:http (HTTPTask T-024: домен + DNS-живые субдомены с
// адресами и CNAME) группой потребителей; для каждого субдомена — проверки
// https и http цепочками редиректов (internal/httpfetch: транспорт с
// лимитами T-049, редиректы и meta refresh T-048, TLS-атрибуты T-050,
// кодировки T-051, честный UA T-052), параллельно внутри задачи с общим
// кэшем «итоговый URL → результат» (ТЗ 5.5 п. 6); вариант-логика T-054,
// алиасы T-055, SPA-эвристика T-057 — internal/httpworker (T-053).
// Подтверждение q:http — паттерном Processing (T-025): XACK после
// полного цикла обработки домена; запись результата в БД и публикация
// q:render/q:analyze — T-058/T-067, точка подключения — хук
// Options.OnResult (вызывается до XACK). Хосты редиректов резолвятся
// «через тот же резолвер» (ТЗ 5.5 п. 2): dnsclient с лимитами T-035 →
// адаптер httpworker.DNSLookup → кэш CachingResolver. Каркас —
// internal/service: миграции, пул БД, /healthz и /metrics, graceful
// shutdown.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
	"crawler/internal/dnsclient"
	"crawler/internal/httpfetch"
	"crawler/internal/httpworker"
	"crawler/internal/queue"
	"crawler/internal/service"
)

func main() {
	// Ошибка конфигурации — до инициализации логгера: stderr и код 1.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[http-worker] конфигурация: %v", err)
	}
	logger := service.NewLogger("http-worker", cfg)

	// Пул приходит от каркаса после миграций (OnDBPool) и гарантированно
	// готов до старта OnRun: postgres.dsn обязателен (config.Validate).
	// Записи результатов в БД пока нет (T-058 подключит её хуком
	// Options.OnResult до XACK) — пул держится сборкой сервиса, чтобы
	// добавление записи не меняло окружение.
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "http-worker",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9104"),
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		OnRun:       func(ctx context.Context) error { return run(ctx, cfg, logger, pool) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — рабочая логика http-worker: клиент DNS (резолв хостов редиректов,
// ТЗ 5.5 п. 2) под лимитами T-035 → транспорт httpfetch с лимитами T-049
// и TLS без проверки сертификата (ТЗ 5.5 п. 3: контент нужен всегда,
// результат проверки сертификата сохраняет TLSInfo T-050) → воркер
// (кэш итоговых URL, варианты, алиасы, SPA) → цикл q:http. Завершение —
// отмена ctx (graceful shutdown, ТЗ 3.4): начатые задачи дорабатывают
// или возвращаются очереди повторной доставкой (Processing, T-025).
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("http-worker: пул БД не получен (postgres.dsn обязателен)")
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

	// Транспорт этапа: NewOptions включает лимиты всегда (T-049) и
	// подставляет дефолтный честный UA (T-052); резолв хостов редиректов —
	// тем же резолвером DNS-этапа с кэшем (ТЗ 5.5 п. 2, CachingResolver
	// T-047, адаптер httpworker.DNSLookup); InsecureSkipVerify — ТЗ 5.5
	// п. 3.
	opts := httpfetch.NewOptions(cfg.HTTP)
	opts.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	opts.Resolver = httpfetch.NewCachingResolver(
		httpworker.NewDNSLookup(resolve, cfg.DNS.IPv6), httpfetch.CacheOptions{})
	fetcher := httpfetch.NewFetcher(opts)
	defer fetcher.CloseIdleConnections()

	opts2 := httpworker.Options{
		MaxRedirects: cfg.HTTP.MaxRedirects, // 0 — не следовать (T-006/T-048)
		Log:          logger,
	}
	w, err := httpworker.New(httpworker.ChainFetch(fetcher), opts2)
	if err != nil {
		return err
	}

	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{})

	// Публикаций из http-worker пока нет (q:render/q:analyze — T-058),
	// поэтому гейт backpressure не подключался: Consume/Ack проходят
	// напрямую. T-058 при добавлении публикаций обернёт очередь
	// WatermarkPublisher'ом (как dns-worker).
	return httpworker.Run(ctx, q, w, opts2)
}

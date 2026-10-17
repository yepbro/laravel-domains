// Сервис http-worker (ТЗ 5.5, 4.2, 3.4): HTTP-этап проверки доменов.
// Потребляет задачи q:http (HTTPTask T-024: домен + DNS-живые субдомены с
// адресами и CNAME) группой потребителей; для каждого субдомена — проверки
// https и http цепочками редиректов (internal/httpfetch: транспорт с
// лимитами T-049, редиректы и meta refresh T-048, TLS-атрибуты T-050,
// кодировки T-051, честный UA T-052), параллельно внутри задачи с общим
// кэшем «итоговый URL → результат» (ТЗ 5.5 п. 6); вариант-логика T-054,
// алиасы T-055, SPA-эвристика T-057 — internal/httpworker (T-053).
// Контент оригинальных записей сохраняется в ContentStore (T-056: паки
// T-042, словарь zstd T-043, дедуп по хэшу §8; контроль диска T-046 —
// DiskWatchLoop), затем результат домена записывается в subdomain_results
// одной транзакцией со свопом ссылок на контент (Recorder T-058: статус
// alive/no_http/temp_error, домен done, §7) и каждая запись публикуется
// в q:analyze (Result, T-024) — вся цепочка в хуке Options.OnResult до
// XACK (ТЗ 4.2), подтверждение q:http — паттерном Processing (T-025).
// Публикация q:render по SPA-вердиктам — рендер-этап (T-066/T-067), не
// здесь. Хосты редиректов резолвятся «через тот же резолвер» (ТЗ 5.5
// п. 2): dnsclient с лимитами T-035 → адаптер httpworker.DNSLookup → кэш
// CachingResolver. Каркас — internal/service: миграции, пул БД,
// /healthz и /metrics, graceful shutdown.
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
	"crawler/internal/content"
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
	// готов до старта OnRun: postgres.dsn обязателен (config.Validate);
	// им пользуется и хранилище контента (строки contents, T-042/T-056),
	// и запись результата (subdomain_results/domains, T-058).
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
// результат проверки сертификата сохраняет TLSInfo T-050) → хранилище
// контента (T-056: паки T-042 + словарь T-043 + контроль диска T-046) →
// воркер (кэш итоговых URL, варианты, алиасы, SPA, сохранение контента
// в OnResult) → цикл q:http. Завершение — отмена ctx (graceful
// shutdown, ТЗ 3.4): начатые задачи дорабатывают или возвращаются
// очереди повторной доставкой (Processing, T-025).
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

	// Хранилище контента (T-056, ТЗ 5.7): словарь zstd (T-043)
	// активируется до открытия — блоки сжимаются активным словарём
	// (CompressWithDict), его ID попадает в заголовок создаваемых паков;
	// отсутствующий файл — не ошибка (сжатие без словаря). Лимиты —
	// секция content: max_store_bytes (ErrTooLarge), disk_free_percent
	// (ErrStoreFull, ТЗ 8), pack_size (ротация).
	if _, err := content.SetDictFile(cfg.Content.ZstdDictionary, logger); err != nil {
		return err
	}
	store, err := content.OpenPackStore(pool, content.PackOptions{
		Dir:             cfg.Content.Dir,
		PackSize:        cfg.Content.PackSize.B(),
		MaxStoreBytes:   cfg.Content.MaxStoreBytes.B(),
		DiskFreePercent: cfg.Content.DiskFreePercent,
		// OnStoreFull — хук алерта T-080 (метрики): до него события
		// «место ниже порога» видны в Warn-логах DiskWatchLoop и
		// ContentSaver.
	})
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	// Контроль свободного места каталога контента (T-046, ТЗ 8):
	// периодический наблюдатель горутиной (не зависит от цикла задач),
	// Warn и событие при входе в паузу записи/восстановлении; завершается
	// с отменой ctx.
	go func() { _ = content.DiskWatchLoop(ctx, store, content.DiskWatchOptions{Log: logger}) }()

	saver, err := httpworker.NewContentSaver(store, logger)
	if err != nil {
		return err
	}

	// Очередь: потребление q:http и публикация q:analyze — под гейтом
	// backpressure (T-026: порог единый для рабочих стримов; Consume/Ack
	// проходят прозрачно, как у dns-worker).
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(cfg.Scheduler.QueueHighWatermark),
		Log:       logger,
	})
	q := queue.NewWatermarkPublisher(queue.NewRedisQueue(rdb, queue.RedisOptions{}), gate)

	recorder, err := httpworker.NewRecorder(pool, q, logger)
	if err != nil {
		return err
	}

	opts2 := httpworker.Options{
		MaxRedirects: cfg.HTTP.MaxRedirects, // 0 — не следовать (T-006/T-048)
		Log:          logger,
		// Цепочка OnResult (внутри обработки задачи, до XACK, ТЗ 4.2):
		// сохранение контента (T-056) → запись subdomain_results со свопом
		// ссылок contents (T-058/T-044) → публикация q:analyze (T-058,
		// §5.5 п. 9). Сбой любого звена — доставка без подтверждения,
		// повторная доставка переигрывает цепочку идемпотентно (ТЗ 3.4).
		OnResult: func(c context.Context, res *httpworker.DomainResult) error {
			if err := saver.Save(c, res); err != nil {
				return err
			}
			_, err := recorder.RecordAndPublish(c, res)
			return err
		},
	}
	w, err := httpworker.New(httpworker.ChainFetch(fetcher), opts2)
	if err != nil {
		return err
	}

	return httpworker.Run(ctx, q, w, opts2)
}

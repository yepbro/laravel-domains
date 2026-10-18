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
// SPA-записи публикуются в q:render (T-066, renderq.go): свободная
// очередь — сразу после фиксации, переполнение — флаг render_pending той
// же транзакцией без задержки XACK (§5.6), доработка — фоновым проходом
// RequeueRenderPending (повторный SPA-вердикт по сохранённому контенту).
// Хосты редиректов резолвятся «через тот же резолвер» (ТЗ 5.5 п. 2):
// dnsclient с лимитами T-035 → адаптер httpworker.DNSLookup → кэш
// CachingResolver. Каркас — internal/service: миграции, пул БД,
// пробы /livez + /healthz, /metrics, graceful shutdown.
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
	"crawler/internal/metrics"
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
	// и запись результата (subdomain_results/domains, T-058). Реестр
	// метрик — здесь же (T-077): приборы HTTP-этапа и коллектор длины
	// q:http получают его до старта OnRun, каркас отдаёт тот же реестр
	// через /metrics.
	reg := metrics.New("http-worker")
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "http-worker",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9104"),
		Metrics:     reg,
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		OnRun:       func(ctx context.Context) error { return run(ctx, cfg, logger, pool, reg) },
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
// в OnResult) с приборами этапа (T-077) и коллектор длины q:http
// (метрика длин очередей, ТЗ 5.9) → цикл q:http. Завершение — отмена
// ctx (graceful shutdown, ТЗ 3.4): начатые задачи дорабатывают или
// возвращаются очереди повторной доставкой (Processing, T-025).
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, reg *metrics.Registry) error {
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
	// Словарь против новейшего пака (T-043): переобучение делает кадры
	// прежнего словаря нечитаемыми — расхождение видно сразу при старте.
	content.WarnDictMismatch(store.Dir(), logger)

	// Фоновые циклы сервиса (наблюдатель диска, коллектор длин очередей,
	// проход доработки рендера) живут до отмены ctx; их результаты
	// забираются в bg после главного цикла — до defer-закрытий store,
	// rdb и транспорта ниже (стиль cmd/scheduler): незаджойненная
	// горутина могла трогать закрытые PackStore/Redis/Fetcher.
	bg := make(chan error, 3)

	// Контроль свободного места каталога контента (T-046, ТЗ 8):
	// периодический наблюдатель горутиной (не зависит от цикла задач),
	// Warn и событие при входе в паузу записи/восстановлении; завершается
	// с отменой ctx.
	go func() { bg <- content.DiskWatchLoop(ctx, store, content.DiskWatchOptions{Log: logger}) }()

	// Фоновая компактификация паков (T-045, ТЗ 5.7): единственный
	// механизм физического освобождения места и удаления мёртвых строк
	// contents — без неё ErrStoreFull не снимается никогда. Цикл один на
	// хранилище — здесь, рядом с DiskWatchLoop (проходам не нужен
	// параллелизм; одновременный запуск во втором писателе формально
	// допустим, но бесполезен). Завершается с отменой ctx.
	go func() { _ = content.CompactLoop(ctx, store, content.CompactOptions{Log: logger}) }()

	saver, err := httpworker.NewContentSaver(store, logger)
	if err != nil {
		return err
	}

	// Очередь: потребление q:http и публикация q:analyze — под гейтом
	// backpressure (T-026: порог единый для рабочих стримов; Consume/Ack
	// проходят прозрачно, как у dns-worker). Публикация q:render идёт
	// мимо WatermarkPublisher, через ту же RedisQueue (T-066, §5.6):
	// ожидание ёмкости задержало бы XACK задачи q:http — при переполнении
	// домен получает флаг render_pending вместо публикации (renderq.go).
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(cfg.Scheduler.QueueHighWatermark),
		MaxLen:    int64(cfg.Redis.MaxLen), // пределы обрезки — для порога гейта (T-026/T-025)
		Log:       logger,
	})
	rawQ := queue.NewRedisQueue(rdb, queue.RedisOptions{
		MaxLen: int64(cfg.Redis.MaxLen), // XADD MAXLEN ~ на каждом стриме (T-025)
	})
	q := queue.NewWatermarkPublisher(rawQ, gate)

	// Возврат зависших доставок q:http (T-025): RecoverLoop в воркере —
	// клейм XAUTOCLAIM, исчерпание попыток — q:dead; параметры надёжности
	// — секция redis (max_attempts, claim_min_idle).
	rel := queue.NewReliab(rdb, rawQ, queue.ReliabOptions{
		MaxAttempts: cfg.Redis.MaxAttempts,
		MinIdle:     cfg.Redis.ClaimMinIdle.D(),
	})

	// Очередь рендера: неблокирующая проверка ёмкости (глубина через гейт
	// T-026, общий порог scheduler.queue_high_watermark) + публикация
	// без ожидания ёмкости.
	renderQ, err := httpworker.NewRenderQueue(rawQ, gate, int64(cfg.Scheduler.QueueHighWatermark), logger)
	if err != nil {
		return err
	}

	// Длина потребляемого стрима q:http → crawler_queue_depth (T-077):
	// источник глубины — гейт (тот же клиент Redis), завершается отменой ctx.
	go func() {
		bg <- metrics.CollectQueueLoop(ctx, reg, gate.Len,
			[]string{queue.StreamHTTP}, metrics.DefaultQueueCollectInterval, logger)
	}()

	recorder, err := httpworker.NewRenderRecorder(pool, q, renderQ, logger)
	if err != nil {
		return err
	}

	// Проход доработки рендера (T-066, §5.6 «дорабатываются позже»):
	// домены с флагом render_pending → повторный SPA-вердикт по
	// сохранённому контенту → публикация q:render при свободной очереди.
	// Отдельной горутиной в том же OnRun (образец CleanupDeleted
	// T-020/T-021): не зависит от цикла задач q:http, завершается
	// отменой ctx.
	go func() {
		bg <- httpworker.RequeueRenderPending(ctx, pool, store, renderQ, httpworker.RequeueOptions{Log: logger})
	}()

	opts2 := httpworker.Options{
		MaxRedirects: cfg.HTTP.MaxRedirects, // 0 — не следовать (T-006/T-048)
		Recover:      rel,                   // возврат зависших задач q:http (T-025)
		Metrics:      reg,                   // приборы этапа: задачи, ошибки, статусы, время ответа, rendered (T-077)
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

	err = httpworker.Run(ctx, q, w, opts2)
	// Главный цикл завершён отменой ctx — фоновые циклы видят ту же
	// отмену и заканчиваются сами; дождаться их до defer-закрытий
	// store/rdb/соединений транспорта (гонка времени жизни ресурсов).
	<-bg
	<-bg
	<-bg
	return err
}

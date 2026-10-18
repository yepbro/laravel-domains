// Сервис render-worker (ТЗ 5.6, §5.7, §7; T-067): этап рендера.
// Потребляет задачи q:render (Result — ключ строки subdomain_results,
// T-024; публикуют HTTP-этап при SPA-вердиктах T-057/T-066 и проход
// доработки renderq.go) группой потребителей; итоговый URL читается из
// строки, страница рендерится браузером Chromium под менеджером поколений
// (internal/render.Manager, T-065: один браузер T-061, блокировка ресурсов
// T-062, ожидание готовности T-063, съём DOM/cookies/заголовков T-064,
// перезапуск по страницам/памяти и реконнект после потери Chromium),
// DOM заменяет сырой HTML в хранилище контента (ContentStore.Put,
// T-056/T-042), строка получает rendered = true, итоговые
// URL/cookies/заголовки рендера и новую ссылку на контент одной
// транзакцией со свопом refcount старого (SwapRefTx, T-044); после
// фиксации запись публикуется в q:analyze на переанализ уже итогового
// контента (§7). Подтверждение q:render — паттерном Processing (XACK
// только после записи в БД, T-025); повторные доставки идемпотентны
// (перезапись, ТЗ 3.4). Публикация q:analyze — под гейтом backpressure
// (WatermarkPublisher, T-026). Перезапуск браузера — менеджер T-065;
// при недоступном Chromium цикл лишь приостанавливает потребление
// (зонд Manager.Alive — реконнект и перезапуск делает сам зонд).
// Каркас — internal/service: миграции, пул БД (OnDBPool), /healthz
// и /metrics, graceful shutdown.
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/render"
	"crawler/internal/renderworker"
	"crawler/internal/service"
)

func main() {
	// Ошибка конфигурации — до инициализации логгера: stderr и код 1.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[render-worker] конфигурация: %v", err)
	}
	logger := service.NewLogger("render-worker", cfg)

	// Пул приходит от каркаса после миграций (OnDBPool) и гарантированно
	// готов до старта OnRun: postgres.dsn обязателен (config.Validate);
	// им пользуется и хранилище контента (строки contents, T-042), и
	// замена ссылки в subdomain_results (T-067). Реестр метрик — здесь
	// же (T-077): приборы этапа рендера и коллектор длины q:render
	// получают его до старта OnRun, каркас отдаёт тот же реестр через
	// /metrics.
	reg := metrics.New("render-worker")
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "render-worker",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9105"),
		Metrics:     reg,
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		OnRun:       func(ctx context.Context) error { return run(ctx, cfg, logger, pool, reg) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — рабочая логика render-worker: Chromium под менеджером поколений
// (T-065: внешний по render.remote_url или собственный процесс T-061,
// перезапуски по страницам/памяти и замена мёртвого поколения; контексты
// задач — изолированные профили) → хранилище контента (то же, что у
// HTTP-этапа: паки T-042 + словарь zstd T-043 + контроль диска T-046) →
// цикл q:render с приборами этапа (T-077) и коллектор длины q:render
// (метрика длин очередей, ТЗ 5.9). Завершение — отмена ctx (graceful
// shutdown, ТЗ 3.4): задачи дорабатываются (каждая ограничена
// render.timeout) или возвращаются очереди повторной доставкой
// (Processing, T-025).
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, reg *metrics.Registry) error {
	if pool == nil {
		return errors.New("render-worker: пул БД не получен (postgres.dsn обязателен)")
	}

	// Chromium — менеджер поколений (T-065): он владеет текущим «браузер +
	// пул», перезапускает браузер по порогам страниц/RSS и заменяет
	// поколение после потери Chromium (реконнект). ctx OnRun — контекст
	// жизненного цикла всех поколений (T-061: браузер живёт, пока жив
	// контекст стартового вызова). Старт — синхронно: ошибка старта —
	// ошибка OnRun (код 1, systemd Restart=on-failure), без браузера этап
	// не работает; дальнейшие перезапуски — без остановки потребления.
	manager := render.NewManager(ctx, render.NewOptions(*cfg))
	defer manager.Close()
	if err := manager.Start(); err != nil {
		return err
	}

	// Хранилище контента (T-056, ТЗ 5.7) — то же, что у HTTP-этапа:
	// словарь zstd (T-043) активируется до открытия, отсутствующий файл —
	// не ошибка; лимиты — секция content (max_store_bytes — ErrTooLarge
	// на раздутом DOM, disk_free_percent — ErrStoreFull, ТЗ 8).
	if _, err := content.SetDictFile(cfg.Content.ZstdDictionary, logger); err != nil {
		return err
	}
	store, err := content.OpenPackStore(pool, content.PackOptions{
		Dir:             cfg.Content.Dir,
		PackSize:        cfg.Content.PackSize.B(),
		MaxStoreBytes:   cfg.Content.MaxStoreBytes.B(),
		DiskFreePercent: cfg.Content.DiskFreePercent,
		// OnStoreFull — хук алерта T-080 (метрики): до него события
		// «место ниже порога» видны в Warn-логах DiskWatchLoop.
	})
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	// Контроль свободного места каталога контента (T-046, ТЗ 8):
	// периодический наблюдатель горутиной, завершается с отменой ctx.
	go func() { _ = content.DiskWatchLoop(ctx, store, content.DiskWatchOptions{Log: logger}) }()

	// Очередь: потребление q:render и XACK проходят прозрачно, публикация
	// q:analyze — под гейтом backpressure (T-026: порог единый для
	// рабочих стримов; как у dns-worker с q:http и http-worker с
	// q:analyze).
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(cfg.Scheduler.QueueHighWatermark),
		Log:       logger,
	})
	q := queue.NewWatermarkPublisher(queue.NewRedisQueue(rdb, queue.RedisOptions{}), gate)

	// Длина потребляемого стрима q:render → crawler_queue_depth (T-077):
	// источник XLEN — гейт (тот же клиент Redis), завершается отменой ctx.
	go func() {
		_ = metrics.CollectQueueLoop(ctx, reg, gate.Len,
			[]string{queue.StreamRender}, metrics.DefaultQueueCollectInterval, logger)
	}()

	// Опции воркера — одни на сборку (стиль http-worker): Worker хранит
	// их для ProcessResult, цикл Run берёт те же Group/Consume/Alive.
	opts := renderworker.Options{
		Metrics: reg, // приборы этапа: задачи, длительности, ошибки классов, rendered (T-077)
		Log:     logger,
		// Зонд браузера: мёртвое поколение менеджер заменяет прямо в зонде
		// (Manager.Alive, T-065 — реконнект после потери Chromium), здесь —
		// только пауза потребления, пока старт не удастся: доставки
		// остаются в стриме, не в PEL, и не расходуют попытки.
		Alive: manager.Alive,
	}
	w, err := renderworker.New(pool, q, store, manager, opts)
	if err != nil {
		return err
	}
	return renderworker.Run(ctx, w, opts)
}

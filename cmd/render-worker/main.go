// Сервис render-worker (ТЗ 5.6, §5.7, §7; T-067): этап рендера.
// Потребляет задачи q:render (Result — ключ строки subdomain_results,
// T-024; публикуют HTTP-этап при SPA-вердиктах T-057/T-066 и проход
// доработки renderq.go) группой потребителей; итоговый URL читается из
// строки, страница рендерится пулом Chromium (internal/render: один
// браузер T-061, блокировка ресурсов T-062, ожидание готовности T-063,
// съём DOM/cookies/заголовков T-064), DOM заменяет сырой HTML в
// хранилище контента (ContentStore.Put, T-056/T-042), строка получает
// rendered = true, итоговые URL/cookies/заголовки рендера и новую ссылку
// на контент одной транзакцией со свопом refcount старого (SwapRefTx,
// T-044); после фиксации запись публикуется в q:analyze на переанализ
// уже итогового контента (§7). Подтверждение q:render — паттерном
// Processing (XACK только после записи в БД, T-025); повторные доставки
// идемпотентны (перезапись, ТЗ 3.4). Публикация q:analyze — под гейтом
// backpressure (WatermarkPublisher, T-026). Перезапуск браузера —
// T-065; здесь при недоступном Chromium цикл приостанавливает
// потребление (зонд browser.Alive). Каркас — internal/service: миграции,
// пул БД (OnDBPool), /healthz и /metrics, graceful shutdown.
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

// run — рабочая логика render-worker: браузер Chromium (один на воркер,
// T-061; внешний по render.remote_url или собственный процесс) с пулом
// изолированных контекстов → хранилище контента (то же, что у HTTP-этапа:
// паки T-042 + словарь zstd T-043 + контроль диска T-046) → цикл
// q:render с приборами этапа (T-077) и коллектор длины q:render
// (метрика длин очередей, ТЗ 5.9). Завершение — отмена ctx (graceful
// shutdown, ТЗ 3.4): пул дорабатывает начатые страницы (каждая ограничена
// render.timeout) или возвращает их очереди повторной доставкой
// (Processing, T-025).
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, reg *metrics.Registry) error {
	if pool == nil {
		return errors.New("render-worker: пул БД не получен (postgres.dsn обязателен)")
	}

	// Chromium: ctx OnRun — контекст жизненного цикла (T-061: браузер
	// живёт, пока жив контекст стартового вызова). Ошибка старта — ошибка
	// OnRun (код 1, systemd Restart=on-failure): без браузера этап не
	// работает; восстановление и перезапуски — T-065.
	browser, err := render.StartBrowser(ctx, render.NewOptions(*cfg))
	if err != nil {
		return err
	}
	rpool := render.NewPool(browser)
	// Close дорабатывает начатые страницы, затем останавливает браузер
	// (graceful shutdown, ТЗ 3.4).
	defer rpool.Close()

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

	// Возврат зависших доставок q:render (T-025): RecoverLoop в воркере —
	// клейм XAUTOCLAIM, исчерпание попыток — q:dead; параметры надёжности
	// — секция redis (max_attempts, claim_min_idle).
	rel := queue.NewReliab(rdb, q, queue.ReliabOptions{
		MaxAttempts: cfg.Redis.MaxAttempts,
		MinIdle:     cfg.Redis.ClaimMinIdle.D(),
	})

	// Длина потребляемого стрима q:render → crawler_queue_depth (T-077):
	// источник глубины — гейт (тот же клиент Redis), завершается отменой ctx.
	go func() {
		_ = metrics.CollectQueueLoop(ctx, reg, gate.Len,
			[]string{queue.StreamRender}, metrics.DefaultQueueCollectInterval, logger)
	}()

	// Опции воркера — одни на сборку (стиль http-worker): Worker хранит
	// их для ProcessResult, цикл Run берёт те же Group/Consume/Alive.
	opts := renderworker.Options{
		Recover: rel, // возврат зависших задач q:render (T-025)
		Metrics: reg, // приборы этапа: задачи, длительности, ошибки классов, rendered (T-077)
		Log:     logger,
		// Зонд браузера: недоступный Chromium — пауза потребления
		// (доставки остаются в стриме, не в PEL); перезапуск — T-065.
		Alive: browser.Alive,
	}
	w, err := renderworker.New(pool, q, store, rpool, opts)
	if err != nil {
		return err
	}
	return renderworker.Run(ctx, w, opts)
}

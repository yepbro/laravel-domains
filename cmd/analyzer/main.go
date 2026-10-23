// Сервис analyzer (ТЗ 5.8, §7; T-071): этап анализа Laravel. Потребляет
// задачи q:analyze (Result — ключ строки subdomain_results, T-024;
// публикуют HTTP-этап T-058, render-worker T-067 и переанализ T-074)
// группой потребителей; вход пассивного анализа читается из БД
// (заголовки/cookies jsonb, content_hash) и хранилища контента (HTML/DOM,
// ТЗ 5.7), правила применяются из текущего снапшота набора (Reloader,
// T-068: SIGHUP перезагружает каталог analyzer.rules_dir без рестарта —
// хук OnSIGHUP каркаса), срабатывания записываются в laravel_findings
// (все, независимо от порога; is_laravel = score ≥ порога, §13.1) одной
// транзакцией со штампом subdomain_results.rules_version; подтверждение
// q:analyze — паттерном Processing (XACK только после записи в БД,
// T-025). Фоновый цикл переанализа (T-074) ставит записи с устаревшим
// rules_version в q:analyze без повторного скачивания. Каркас —
// internal/service: миграции, пул БД (OnDBPool), пробы /livez + /healthz, /metrics,
// graceful shutdown.
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
	"crawler/internal/content"
	"crawler/internal/metrics"
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

	// Набор правил обязателен уже при старте (T-068): analyzer без правил
	// молча не находил бы Laravel — ошибка загрузки останавливает сервис
	// (код 1, systemd Restart=on-failure). Снапшот для каждой доставки
	// берёт воркер через Set(); SIGHUP перезагружает каталог на живом
	// сервисе (HandleSIGHUP — OnSIGHUP каркаса, make rules-reload).
	reloader, err := rules.NewReloader(cfg.Analyzer.RulesDir, logger)
	if err != nil {
		log.Fatalf("[analyzer] правила %s: %v", cfg.Analyzer.RulesDir, err)
	}

	// Пул приходит от каркаса после миграций (OnDBPool) и гарантированно
	// готов до старта OnRun: postgres.dsn обязателен (config.Validate);
	// им пользуется и хранилище контента (строки contents, T-042), и
	// чтение subdomain_results/запись findings. Реестр метрик — здесь же
	// (T-077): приборы этапа и коллектор длины q:analyze получают его до
	// старта OnRun, каркас отдаёт тот же реестр через /metrics.
	reg := metrics.New("analyzer")
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "analyzer",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9106"),
		Metrics:     reg,
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		OnRun:       func(ctx context.Context) error { return run(ctx, cfg, logger, pool, reloader, reg) },
		OnSIGHUP:    reloader.HandleSIGHUP,
	}); err != nil {
		os.Exit(1)
	}
}

// run — рабочая логика analyzer: хранилище контента (чтение — тот же
// PackStore, что пишет HTTP/рендер-этапы: словарь zstd T-043 обязан
// совпадать, иначе распаковка чужих блоков невозможна) → очередь →
// воркер q:analyze (передний план) с приборами этапа (T-077) и коллектор
// длины q:analyze (метрика длин очередей, ТЗ 5.9) + фоновый цикл
// переанализа (T-074). Завершение — отмена ctx (graceful shutdown,
// ТЗ 3.4): начатые доставки дорабатывают или возвращаются очереди
// повторной доставкой (Processing, T-025); ReanalyzeLoop на отмене
// возвращает чистый nil.
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, reloader *rules.Reloader, reg *metrics.Registry) error {
	if pool == nil {
		return errors.New("analyzer: пул БД не получен (postgres.dsn обязателен)")
	}

	// Хранилище контента (ТЗ 5.7, T-042): этап только читает (контент для
	// матчинга целей html/meta), запись и DiskWatchLoop — территория
	// пишущих этапов; лимиты Put этому воркеру не нужны, каталог и файл
	// блокировки открываются те же.
	if _, err := content.SetDictFile(cfg.Content.ZstdDictionary, logger); err != nil {
		return err
	}
	store, err := content.OpenPackStore(pool, content.PackOptions{Dir: cfg.Content.Dir})
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	// Словарь против новейшего пака (T-043): прежний словарь реестра
	// даёт читаемость старых кадров (T-103); расхождение без файла
	// в реестре видно Warn-ом при старте, а не ошибками чтения.
	content.WarnDictMismatch(store.Dir(), logger)

	// Очередь: воркер только потребляет q:analyze и XACK (конец конвейера
	// §7); циклу переанализа публикация нужна под гейтом backpressure
	// (WatermarkPublisher, T-026) — один клиент Redis на сервис.
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{
		MaxLen: int64(cfg.Redis.MaxLen), // XADD MAXLEN ~ на каждом стриме (T-025)
	})

	// Возврат зависших доставок q:analyze (T-025): RecoverLoop в воркере —
	// клейм XAUTOCLAIM, исчерпание попыток — q:dead; параметры надёжности
	// и темпа — секция redis (max_attempts, claim_min_idle,
	// recover_batch, recover_interval — T-153).
	rel := queue.NewReliab(rdb, q, queue.ReliabOptions{
		MaxAttempts: cfg.Redis.MaxAttempts,
		MinIdle:     cfg.Redis.ClaimMinIdle.D(),
	})

	// Фоновый цикл переанализа (T-074, ТЗ 5.8): стартовый дренаж + тикер +
	// внеочередной проход по смене набора правил (Reloader.Subscribe);
	// ошибки проходов цикл логирует Warn сам, возвращает только по отмене
	// ctx — живёт до остановки сервиса рядом с воркером.
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(cfg.Scheduler.QueueHighWatermark),
		MaxLen:    int64(cfg.Redis.MaxLen), // пределы обрезки — для порога гейта (T-026/T-025)
		Log:       logger,
	})
	publish := queue.NewWatermarkPublisher(q, gate)
	go func() {
		if err := analyzer.ReanalyzeLoop(ctx, pool, publish, reloader, analyzer.ReanalyzeOptions{
			Log: logger,
		}); err != nil {
			logger.Warn("переанализ: цикл остановлен", slog.Any("err", err))
		}
	}()

	// Длина потребляемого стрима q:analyze → crawler_queue_depth (T-077):
	// источник глубины — гейт (тот же клиент Redis), завершается отменой ctx.
	go func() {
		_ = metrics.CollectQueueLoop(ctx, reg, gate.Len,
			[]string{queue.StreamAnalyze}, metrics.DefaultQueueCollectInterval, logger)
	}()

	// Опции воркера — одни на сборку (стиль render-worker): Worker хранит
	// их для ProcessResult, цикл Run берёт те же Group/Consume.
	opts := analyzer.Options{
		MinScore: cfg.Analyzer.LaravelMinScore,
		Recover:  rel, // возврат зависших задач q:analyze (T-025)
		// Темп возврата зависших (T-153): пачка/интервал клейма —
		// redis.recover_*.
		RecoverBatch:    cfg.Redis.RecoverBatch,
		RecoverInterval: cfg.Redis.RecoverInterval.D(),
		Metrics:         reg, // приборы этапа: задачи и длительности (T-077)
		Log:             logger,
	}
	w, err := analyzer.New(pool, q, store, reloader, opts)
	if err != nil {
		return err
	}
	return analyzer.Run(ctx, w, opts)
}

// Сервис scheduler (ТЗ 5.3, 4.2): главный цикл постановки доменов на
// проверку — выборка пачек pending с чередованием зон (internal/scheduler
// DispatchBatch, T-027), перевод в queued с lease и публикация пачек
// DNSBatch в q:dns (EnqueueBatch/EnqueueLoop, T-028) — и фоновые циклы в
// тех же OnRun: суточная перепроверка done-доменов (RecheckDaily, T-030),
// повторные проверки temp_error (RetryTempErrors, T-031) и возврат
// доменов с истёкшим lease (LeaseWatch, T-029). Публикация — под гейтом
// backpressure (WatermarkPublisher, T-026). Очередь — Redis Streams
// (internal/queue, клиент queue.NewClient/RedisQueue); каркас —
// internal/service: миграции, пул БД (OnDBPool), пробы /livez + /healthz, /metrics,
// graceful shutdown (ТЗ 3.4).
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/metrics"
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

	// Реестр метрик создаётся здесь (T-077): коллектору глубины backlog
	// (run ниже) нужны приборы до старта OnRun, каркас отдаёт тот же
	// реестр через /metrics (Options.Metrics).
	reg := metrics.New("scheduler")

	// Пул приходит от каркаса после миграций (OnDBPool) и гарантированно
	// готов до старта OnRun: postgres.dsn обязателен (config.Validate).
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "scheduler",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9102"),
		Metrics:     reg,
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		// Агрегатный коллектор backlog (T-077) не разбирает параллельные
		// воркеры (T-171): путь постановки пачек — индексные сканы
		// domains_pending_idx без параллельных планов, сессионный SET
		// его не замедляет, а seq-проходы (backlog, retry temp_error)
		// перестают забирать все ядра у конвейера.
		DBAfterConnect: db.NoParallelGather,
		OnRun:          func(ctx context.Context) error { return run(ctx, cfg, logger, pool, reg) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — рабочая логика scheduler: главный цикл постановки пачек в q:dns
// и фоновые циклы перепроверок (T-030/T-031) горутинами в том же OnRun —
// они возвращают домены в pending, главный цикл подберёт их следующей
// пачкой; рядом — коллектор глубины backlog (T-077, ТЗ 5.9): gauge
// pending/queued из БД, живёт у владельца состояний domains, — цикл
// освобождения подтверждённого префикса стримов (queue.TrimLoop, T-141:
// XACK не удаляет записи из стримов, XTRIM MINID по старейшей
// неподтверждённой записи группы не даёт истории копиться до потолка
// MAXLEN и бюджета Redis, ТЗ 3.2; scheduler — одиночный хозяин стримов
// очередей, цикл дёшев: три команды на стрим раз в redis.trim_interval)
// и цикл гигиены консьюмеров групп (queue.CleanupConsumersLoop, T-155:
// после рестартов воркеров их pid-консьюмеры остаются в группах
// навсегда; строго пустые (pending = 0) и неактивные дольше
// redis.consumer_idle удаляются XGROUP DELCONSUMER — те же дворники
// стримов тем же темпом trim_interval).
// Завершение — отмена ctx (graceful shutdown, ТЗ 3.4): цикл дорабатывает
// текущую пачку, фоновые — текущий проход, все возвращают nil.
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, reg *metrics.Registry) error {
	if pool == nil {
		return errors.New("scheduler: пул БД не получен (postgres.dsn обязателен)")
	}

	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	// Публикация пачек — под гейтом backpressure (T-026): длина q:dns
	// ниже scheduler.queue_high_watermark; MAXLEN/надёжность — T-025 в
	// RedisQueue.Publish.
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(cfg.Scheduler.QueueHighWatermark),
		MaxLen:    int64(cfg.Redis.MaxLen), // пределы обрезки — для порога гейта (T-026/T-025)
		Log:       logger,
	})
	dnsq := queue.NewWatermarkPublisher(queue.NewRedisQueue(rdb, queue.RedisOptions{
		MaxLen: int64(cfg.Redis.MaxLen), // XADD MAXLEN ~ на каждом стриме (T-025)
	}), gate)

	// Веса зон выборки (ТЗ 5.3: «опционально в конфиге», T-106): имена
	// из scheduler.zone_weights разрешаются в zone_id по справочнику
	// один раз на старте — как и остальной конфиг, веса не меняются на
	// лету. Имени может не быть в справочнике (зону создаёт ingest
	// первым файлом, ТЗ 5.1) — Warn: зона проверяется с весом 1 до
	// рестарта с появившейся зоной. Неактивные зоны разрешаются тоже:
	// их вес вступит в силу при включении зоны (crawlerctl zones
	// enable, T-084) без рестарта.
	zoneWeights, unknownZones, err := scheduler.ResolveZoneWeights(ctx, pool, cfg.Scheduler.ZoneWeights)
	if err != nil {
		return err
	}
	if len(unknownZones) > 0 {
		logger.Warn("веса зон: нет в справочнике zones (создаёт ingest первым файлом) — проверяются с весом 1 до рестарта с появившейся зоной",
			"zones", strings.Join(unknownZones, ", "))
	}

	bg := make(chan error, 6)
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
	go func() {
		bg <- scheduler.LeaseWatch(ctx, pool, scheduler.LeaseOptions{
			Timeout: cfg.Scheduler.Lease.D(),
			Log:     logger,
		})
	}()
	go func() {
		// Период — scheduler.backlog_interval (T-161: дефолт 5 мин,
		// замер §12; 30 с коридора ТЗ 5.9 доступны конфигом).
		bg <- scheduler.BacklogLoop(ctx, pool, reg, scheduler.BacklogOptions{
			Interval: cfg.Scheduler.BacklogInterval.D(),
			Log:      logger,
		})
	}()
	// Освобождение подтверждённого префикса стримов (T-141, ТЗ 4.2
	// «Дополнение 2026-10-23»): XTRIM MINID по старейшей записи, обязанной
	// остаться, — рядом с остальными циклами-дворниками планировщика.
	go func() {
		bg <- queue.TrimLoop(ctx, rdb, queue.TrimOptions{
			Interval: cfg.Redis.TrimInterval.D(),
			MaxLen:   int64(cfg.Redis.MaxLen), // предел для сигнала застрявшего префикса
			Log:      logger,
		})
	}()
	// Гигиена консьюмеров групп (T-155): строго пустые (pending = 0 —
	// DELCONSUMER с pending теряет PEL навсегда) и неактивные дольше
	// redis.consumer_idle pid-консьюмеры мёртвых после рестартов воркеров
	// вычищаются тем же темпом дворников, что и освобождение префикса
	// (redis.trim_interval).
	go func() {
		bg <- queue.CleanupConsumersLoop(ctx, rdb, queue.ConsumerOptions{
			Interval: cfg.Redis.TrimInterval.D(),
			Idle:     cfg.Redis.ConsumerIdle.D(),
			Log:      logger,
		})
	}()

	// Главный цикл живёт до отмены ctx; после него фоновые циклы уже
	// завершаются по той же отмене — забираем их результаты.
	err = scheduler.EnqueueLoop(ctx, pool, dnsq, scheduler.EnqueueOptions{
		BatchSize:   cfg.Scheduler.BatchSize,
		ZoneWeights: zoneWeights,
		Log:         logger,
	})
	<-bg
	<-bg
	<-bg
	<-bg
	<-bg
	<-bg
	return err
}

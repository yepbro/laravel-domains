// Сервис monitor (ТЗ 5.9): два цикла в OnRun — периодический пакетный
// пересчёт кэширующих счётчиков прогресса зон zones.domains_total /
// domains_checked (internal/monitor.ZoneStatsLoop, T-078) с периодом
// monitor.zone_stats_interval и оценка алертов минимального набора ТЗ 5.9
// (internal/monitor.AlertsLoop, T-080): доля ошибок DNS, переполнение
// очередей, остановка прогресса, свободное место каталога контента,
// износ NVMe (SMART), всплеск мусорных content_hash очистки ingest
// (T-099, зонд /metrics), OOM и рестарты юнитов crawler (T-145,
// systemctl + cgroup v2), неразобранный q:dead и память Redis (T-154,
// XLEN + INFO memory); недостающие пороги T-162 — абсолютный минимум
// диска (Warn/эскалация), троттлинг MemoryHigh, замороженная очередь
// (last-delivered-id), PSI CPU; переходы алертов доставляются в
// Telegram (TelegramNotifier → telegram-notify.sh --message). Ещё два
// цикла — отчёт системной нагрузки (T-173) и медленные запросы
// PostgreSQL (T-177: pg_stat_activity → Telegram той же доставкой).
// Каркас — internal/service: миграции, пул БД (OnDBPool), пробы /livez +
// /healthz, /metrics, graceful shutdown (ТЗ 3.4).
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/metrics"
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
	// Адрес мониторинга — monitor.listen из конфигурации (ТЗ 5.10):
	// важнее METRICS_ADDR — docs/services.md (приоритет приведён к
	// документации; прежде переменная ошибочно побеждала конфиг).
	// METRICS_ADDR — страховка при пустом monitor.listen (на практике не
	// пуст: Default/Validate); адреса метрик остальных сервисов задаются
	// только переменной (ТЗ 5.9).
	//
	// Пул приходит от каркаса после миграций (OnDBPool) и гарантированно
	// готов до старта OnRun: postgres.dsn обязателен (config.Validate).
	listen := cfg.Monitor.Listen
	if listen == "" {
		listen = service.EnvOr("METRICS_ADDR", ":9100")
	}
	// Реестр метрик (T-115): gauge серой зоны Laravel-находок
	// (crawler_laravel_near_findings) регистрируется до старта OnRun,
	// каркас отдаёт тот же реестр через /metrics (стиль cmd/analyzer).
	reg := metrics.New("monitor")
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "monitor",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: listen,
		Metrics:     reg,
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		// Пересчёт счётчиков зон и окна алертов — агрегаты по всей
		// domains (T-171): без параллельных воркеров проход не
		// забирает все ядра у конвейера; свежесть счётчиков в
		// десятки секунд достаточна (порог алерта остановки
		// прогресса — 5 периодов пересчёта).
		DBAfterConnect: db.NoParallelGather,
		OnRun:          func(ctx context.Context) error { return run(ctx, cfg, logger, pool, reg) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — рабочая логика monitor: пересчёт счётчиков зон (ТЗ 5.9, T-078)
// фоновым циклом и оценка алертов (ТЗ 5.9, T-080) циклом первого плана —
// алерт остановки прогресса читает zones.updated_at, который пишет именно
// фоновый цикл. Оба завершаются отменой ctx (graceful shutdown, ТЗ 3.4);
// после своего цикла забираем результат фонового (стиль cmd/scheduler).
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, reg *metrics.Registry) error {
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
	// и фоновый — иначе <-bg ждал бы вечно. Отмена выполняется ДО
	// ожидания (defer остаётся страховкой на путь с паникой): если
	// AlertsLoop когда-нибудь научится возвращаться ошибкой без
	// отмены родительского ctx, зависание не наступит.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Фоновые циклы: пересчёт счётчиков зон (T-078; период —
	// monitor.zone_stats_interval, T-161 поднял дефолт до 5 мин по замеру
	// §12; реестр — метрики длительности прохода и пропусков тиков),
	// gauge серой зоны Laravel-находок (T-115) и счётчики истории
	// детекции appeared/lost (T-118) — все периодические проходы по БД,
	// завершаются отменой ctx. Четвёртый цикл — отчёт системной
	// нагрузки (T-173), пятый — медленные запросы PostgreSQL (T-177):
	// оба подключаются ниже, после сборки Telegram-доставки.
	bg := make(chan error, 5)
	go func() {
		bg <- monitor.ZoneStatsLoop(ctx, pool, reg, monitor.ZoneStatsOptions{
			Interval: cfg.Monitor.ZoneStatsInterval.D(),
			Log:      logger,
		})
	}()
	go func() {
		bg <- monitor.NearStatsLoop(ctx, pool, reg, monitor.NearStatsOptions{
			MinScore: cfg.Analyzer.LaravelMinScore,
			Log:      logger,
		})
	}()
	go func() {
		bg <- monitor.HistoryStatsLoop(ctx, pool, reg, monitor.HistoryStatsOptions{
			Log: logger,
		})
	}()

	// Период алертов — период пересчёта зон: одна пара циклов
	// мониторинга (коридор ТЗ 5.9 30–60 с), и порог остановки прогресса
	// считается в тех же периодах; отдельного поля конфигурации под
	// период алертов не заводим.
	//
	// Глубина q:dead (T-154) — через тот же гейт обёрткой
	// NewDeadDepthMetrics: Len(q:dead) зеркалится в
	// crawler_queue_depth{queue="q:dead"} (monitor — назначенный
	// наблюдатель карантина, потребителей у него нет), рабочие стримы
	// идут транзитом — их глубину публикуют сами воркеры (T-077).
	//
	// Источник мусорных content_hash (T-099) — зонд экспозиции /metrics
	// сервиса ingest: счётчик растёт в процессе ingest, монитор читает
	// его тем же адресом, что скрейпит Prometheus. Пустой адрес — алерт
	// «не измеряется» (тихо), а не Warn зондом на каждый проход.
	deps := monitor.AlertsDeps{Pool: pool, Redis: monitor.NewDeadDepthMetrics(reg, gate)}
	if cfg.Monitor.IngestMetricsURL != "" {
		deps.BadHashes = monitor.IngestBadHashes{URL: cfg.Monitor.IngestMetricsURL}
	}
	// События юнитов (T-145): systemctl show + cgroup v2 memory.events
	// (unitevents.go), обёрнуты в метрики — одно чтение на проход питает
	// алерты unit_oom и memory_high (T-162: рост счётчика high) и gauge
	// crawler_unit_oom_events/crawler_unit_memory_high_events/
	// crawler_unit_restarts. В dev-контейнере без systemd источник
	// деградирует тихо (как SMART).
	deps.UnitEvents = monitor.NewUnitEventsMetrics(reg, monitor.OSUnitEvents{})
	// Память Redis (T-154): INFO memory того же клиента, что у гейта,
	// обёрнута в метрики — одно чтение питает алерт redis_memory и gauge
	// crawler_redis_memory_ratio/_bytes.
	deps.RedisMemory = monitor.NewRedisMemoryMetrics(reg, monitor.RedisInfo{RDB: rdb})
	// Группы стримов для алерта замороженной очереди (T-162): XINFO
	// GROUPS + XLEN того же клиента (last-delivered-id и глубина группы
	// с клампом T-168); стрим без групп — пустой вклад.
	deps.StreamGroups = monitor.RedisStreamGroups{RDB: rdb}
	// PSI CPU (T-162): /proc/pressure/cpu хоста, обёрнуто в метрики —
	// gauge crawler_psi_cpu_some_percent и алерт psi_cpu одним чтением;
	// ядро без PSI деградирует тихо.
	deps.PSI = monitor.NewPSIMetrics(reg, monitor.OSPSI{})
	// Доставка алертов в Telegram (T-162): переходы состояний уходят
	// exec-ом telegram-notify.sh --message; секреты берёт из окружения
	// юнита (EnvironmentFile /etc/crawler/crawler.env), нет секретов или
	// скрипта — доставка выключена, алерты остаются Warn в журнале.
	telegram := monitor.NewTelegramNotifier(monitor.TelegramOptions{
		Enabled: cfg.Monitor.Alerts.Telegram,
		Script:  cfg.Monitor.Alerts.TelegramScript,
		Log:     logger,
	})

	// Отчёт системной нагрузки оператору (T-173): раз в
	// monitor.load_report_interval — снимок /proc (система + топ
	// процессов «htop»-видом), места каталога контента и очередей с
	// темпами потребления — в Telegram той же доставкой, что алерты
	// (нет секретов/скрипта — только журнал и gauge crawler_os_*).
	// Период 0 — цикл выключен (LoadReportLoop молча выйдет).
	go func() {
		bg <- monitor.LoadReportLoop(ctx, monitor.OSLoad{}, monitor.LoadReportOptions{
			Interval: cfg.Monitor.LoadReportInterval.D(),
			Log:      logger,
			Metrics:  reg,
			Notify:   telegram.Send,
			Queues:   monitor.RedisQueues{RDB: rdb},
			Space:    monitor.OSStatfs{},
			Dirs:     []string{cfg.Content.Dir},
		})
	}()

	// Медленные запросы PostgreSQL (T-177): цикл опрашивает
	// pg_stat_activity — выполняемые дольше monitor.slow_query_min_duration
	// клиентские операторы уходят в Telegram той же доставкой, что алерты
	// и отчёт нагрузки (дебаунс pid+query_start; агрегаты monitor —
	// UPDATE по 47 млн строк каждые 5 мин, замер §12 — исключены
	// application_name пула). Полный посмертный след — журнал сервера
	// (log_min_duration_statement, deploy/postgres). 0 — цикл выключен
	// (SlowQueryLoop молча выйдет).
	go func() {
		bg <- monitor.SlowQueryLoop(ctx, monitor.PGSlowQueries{
			Pool:       pool,
			ExcludeApp: "monitor",
		}, monitor.SlowQueryOptions{
			Interval:    cfg.Monitor.SlowQueryInterval.D(),
			MinDuration: cfg.Monitor.SlowQueryMinDuration.D(),
			Log:         logger,
			Metrics:     reg,
			Notify:      telegram.Send,
		})
	}()

	err := monitor.AlertsLoop(ctx, deps, monitor.AlertsOptions{
		Interval:          cfg.Monitor.ZoneStatsInterval.D(),
		ZoneStatsInterval: cfg.Monitor.ZoneStatsInterval.D(),
		QueueWatermark:    int64(cfg.Scheduler.QueueHighWatermark),
		QueueMaxLen:       int64(cfg.Redis.MaxLen),
		ContentDir:        cfg.Content.Dir,
		DiskFreePercent:   cfg.Content.DiskFreePercent,
		DiskFreeMinBytes:  cfg.Monitor.Alerts.DiskFreeMinBytes.B(),
		DiskFreeCritBytes: cfg.Monitor.Alerts.DiskFreeCritBytes.B(),
		QueueFreeze:       cfg.Monitor.Alerts.QueueFreeze.D(),
		PSICPUPercent:     cfg.Monitor.Alerts.PSICPUPercent,
		PSIWindow:         cfg.Monitor.Alerts.PSIWindow.D(),
		MemHighMinPasses:  cfg.Monitor.Alerts.MemHighPasses,
		Log:               logger,
		OnPass:            telegram.OnPass,
	})
	// Свой цикл закончился — отменяем фоновые до ожидания результатов
	// (отмена идемпотентна и при уже остановленных циклах). Их сбой не
	// прячем за nil алертов — забираем результаты.
	cancel()
	for i := 0; i < 5; i++ {
		bgErr := <-bg
		if err == nil {
			err = bgErr
		}
	}
	return err
}

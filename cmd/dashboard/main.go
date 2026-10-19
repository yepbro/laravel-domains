// Сервис dashboard — операторский веб-дашборд конвейера (ТЗД, этап 15):
// единая страница состояния конвейера с доступом по паролю (HTTP Basic,
// ТЗД §7), строго read-only (ТЗД §1.3). Каркас — internal/service
// (миграции, пул БД через OnDBPool, пробы и /metrics на loopback
// 127.0.0.1:9108 — ТЗД §4.2, следующий за web :9107); публичный
// слушатель dashboard.listen — собственный http.Server внутри OnRun
// с graceful stop по отмене ctx (прецедент cmd/web, T-121). Рядом со
// слушателем — фоновый цикл сборки снимков (ТЗД §4.4, T-129): период
// dashboard.refresh, атомарная замена, /api/status отдаёт последний
// готовый снимок. Фронт — internal/dashboard/assets (T-130),
// развёртывание — T-131.
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
	"crawler/internal/dashboard"
	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/service"
)

func main() {
	// Ошибка конфигурации — до инициализации логгера: stderr и код 1.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[dashboard] конфигурация: %v", err)
	}
	logger := service.NewLogger("dashboard", cfg)
	// ТЗД §7.7: креды оператора обязательны — пустые username/
	// password_hash (или неразборимый хэш) отказывают запуску ДО старта
	// каркаса: миграции и слушатели не поднимаются. Общий config.Validate
	// их сознательно не проверяет — иначе требование коснулось бы всех
	// сервисов проекта.
	if _, err := dashboard.ParseCredentials(cfg); err != nil {
		logger.Error("креды дашборда", "err", err)
		os.Exit(1)
	}

	// Метрики — loopback (ТЗД §4.2: публичная точка входа одна —
	// dashboard.listen); счётчики/гистограмма сборки снимков (ТЗД §10)
	// регистрируются до старта OnRun, каркас отдаёт тот же реестр через
	// /metrics (прецедент cmd/monitor). Пул приходит от каркаса после
	// миграций (OnDBPool): postgres.dsn обязателен (config.Validate),
	// readiness /healthz публичного mux пингует его.
	reg := metrics.New("dashboard")
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "dashboard",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", "127.0.0.1:9108"),
		Metrics:     reg,
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		OnRun:       func(ctx context.Context) error { return run(ctx, cfg, logger, pool, reg) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — публичный слушатель и цикл сборки снимков внутри OnRun: сервер
// в первом плане, сборщик — фоновым циклом (стиль cmd/monitor: ранняя
// остановка одного останавливает другого производной отменой ctx);
// завершение возвращается каркасу.
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, reg *metrics.Registry) error {
	if pool == nil {
		return errors.New("dashboard: пул БД не получен (postgres.dsn обязателен)")
	}

	// Глубина стримов — через тот же узкий API, что у monitor и
	// crawlerctl status (ТЗД §4.3.2): клиент Redis по секции redis
	// конфигурации, гейт без порога — сборщику нужен только Len.
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{Log: logger})

	coll, err := dashboard.NewCollector(pool, gate, cfg.Dashboard, dashboard.CollectorOptions{
		Log:            logger,
		Metrics:        reg,
		QueueWatermark: int64(cfg.Scheduler.QueueHighWatermark), // подсветка очередей, §5.4
	})
	if err != nil {
		return err
	}
	srv, err := dashboard.New(pool, cfg, dashboard.Options{
		Log:       logger,
		Snapshots: coll, // /api/status отдаёт последний готовый снимок
	})
	if err != nil {
		return err
	}

	// Производная отмена: ранний выход своего цикла останавливает
	// и фоновый — иначе ожидание результата висело бы вечно (стиль
	// cmd/monitor).
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	bg := make(chan error, 1)
	go func() { bg <- coll.Loop(ctx) }()

	err = srv.Run(ctx, cfg.Dashboard.Listen)
	cancel()
	if bgErr := <-bg; err == nil {
		err = bgErr
	}
	return err
}

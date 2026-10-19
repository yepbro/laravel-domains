// Сервис dashboard — операторский веб-дашборд конвейера (ТЗД, этап 15):
// единая страница состояния конвейера с доступом по паролю (HTTP Basic,
// ТЗД §7), строго read-only (ТЗД §1.3). Каркас — internal/service
// (миграции, пул БД через OnDBPool, пробы и /metrics на loopback
// 127.0.0.1:9108 — ТЗД §4.2, следующий за web :9107); публичный
// слушатель dashboard.listen — собственный http.Server внутри OnRun
// с graceful stop по отмене ctx (прецедент cmd/web, T-121). Снимки
// /api/status — T-129, фронт — T-130, развёртывание — T-131.
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
	// dashboard.listen). Пул приходит от каркаса после миграций
	// (OnDBPool): postgres.dsn обязателен (config.Validate), readiness
	// /healthz публичного mux пингует его.
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "dashboard",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", "127.0.0.1:9108"),
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		OnRun:       func(ctx context.Context) error { return run(ctx, cfg, logger, pool) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — публичный слушатель внутри OnRun: жизненным циклом владеет
// сервис dashboard (graceful stop по отмене ctx), завершение
// возвращается каркасу.
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("dashboard: пул БД не получен (postgres.dsn обязателен)")
	}
	srv, err := dashboard.New(pool, cfg, dashboard.Options{Log: logger})
	if err != nil {
		return err
	}
	return srv.Run(ctx, cfg.Dashboard.Listen)
}

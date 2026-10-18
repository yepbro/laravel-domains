// Сервис web — публичный лендинг (этап 13, T-121; продуктовое расширение
// вне основного ТЗ — перенос из larasites): предподсчитанные счётчики
// конвейера (карточки, облако/таблица зон, последние находки — T-122) и
// форма ручной проверки домена (T-123). Каркас — internal/service:
// миграции, пул БД (OnDBPool), пробы и /metrics на loopback :9107,
// graceful shutdown (ТЗ 3.4); публичный слушатель web.listen —
// собственный http.Server внутри OnRun (метрики наружу не выставляются,
// ТЗ §9). Лендинг — единственный публичный порт системы.
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
	"crawler/internal/service"
	"crawler/internal/web"
)

func main() {
	// Ошибка конфигурации — до инициализации логгера: stderr и код 1.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[web] конфигурация: %v", err)
	}
	logger := service.NewLogger("web", cfg)

	// Метрики — loopback (ТЗ §9: публичная точка входа одна — web.listen;
	// 9107 — следующий свободный за analyzer :9106). Пул приходит от
	// каркаса после миграций (OnDBPool): postgres.dsn обязателен
	// (config.Validate), readiness /healthz публичного mux пингует его.
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "web",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", "127.0.0.1:9107"),
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		OnRun:       func(ctx context.Context) error { return run(ctx, cfg, logger, pool) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — публичный слушатель внутри OnRun: жизненным циклом владеет
// сервис web (graceful stop по отмене ctx — прецедент OnRun-клиентов
// воркеров), завершение возвращает каркасу.
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("web: пул БД не получен (postgres.dsn обязателен)")
	}
	srv, err := web.New(pool, cfg, web.Options{Log: logger})
	if err != nil {
		return err
	}
	return srv.Run(ctx, cfg.Web.Listen)
}

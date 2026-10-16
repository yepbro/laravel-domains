// Сервис ingest (ТЗ 5.1): наблюдает каталог incoming/ (internal/watch),
// прогоняет готовые файлы через конвейер приёма (internal/ingest:
// разбор пути → зона → разбор файла с sha256 в один проход → валидация →
// staging → применение add/delete → отчёт ingest_files → processed/failed)
// и держит фоновую очистку удалённых доменов (CleanupDeleted, T-020).
// Каркас — internal/service: миграции, пул БД (OnDBPool), /healthz и
// /metrics, graceful shutdown (ТЗ 3.4).
package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/config"
	"crawler/internal/ingest"
	"crawler/internal/service"
	"crawler/internal/watch"
)

func main() {
	// Ошибка конфигурации — до инициализации логгера: stderr и код 1.
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[ingest] конфигурация: %v", err)
	}
	logger := service.NewLogger("ingest", cfg)

	// Пул приходит от каркаса после миграций (OnDBPool) и гарантированно
	// готов до старта OnRun: postgres.dsn обязателен (config.Validate).
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "ingest",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9101"),
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		OnRun:       func(ctx context.Context) error { return run(ctx, cfg, logger, pool) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — рабочий цикл ingest (ТЗ 5.1): наблюдение incoming (fsnotify +
// обход-страховка, стабильность размера/mtime — internal/watch, T-013),
// конвейер обработки каждого готового файла (internal/ingest, T-014…T-021)
// и фоновая очистка удалённых доменов в отдельной горутине (T-020;
// интервал и пачка — дефолты пакета). Завершение — отмена ctx (graceful
// shutdown, ТЗ 3.4): канал готовых файлов закрывается, конвейер
// дорабатывает текущий файл, очистка выходит по отмене.
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("ingest: пул БД не получен (postgres.dsn обязателен)")
	}

	pipe, err := ingest.NewPipeline(
		ingest.NewZoneStore(pool),
		ingest.NewFileStore(pool),
		pool,
		ingest.PipelineOptions{
			Dir:           cfg.Ingest.Dir,
			BatchSize:     cfg.Ingest.BatchSize,
			MaxErrorRatio: cfg.Ingest.MaxErrorRatio,
			// Лог rejected — в общий stderr сервиса (ТЗ 5.10 пути не
			// задаёт): записи отличимы атрибутом service=rejected (T-016).
			RejectLog: ingest.NewRejectedLogger(os.Stderr, slog.LevelInfo),
			Log:       logger,
		},
	)
	if err != nil {
		return err
	}

	files, err := watch.Dir(ctx, watch.Options{
		Dir:          cfg.Ingest.Dir,
		StableAfter:  time.Duration(cfg.Ingest.StableSeconds) * time.Second,
		ScanInterval: cfg.Ingest.ScanInterval.D(),
		Log:          logger,
	})
	if err != nil {
		return err
	}

	cleanupDone := make(chan error, 1)
	go func() {
		cleanupDone <- ingest.CleanupDeleted(ctx, pool, ingest.CleanupOptions{Log: logger})
	}()

	for f := range files {
		// Итоги обработки (перемещения processed/failed, дубли, порог
		// ошибок) Pipeline логирует сам; ошибка возврата — только файлы,
		// оставшиеся в incoming из-за сбоя инфраструктуры: цикл продолжает
		// следующие файлы, оставшиеся переработает рестарт (at-least-once,
		// ТЗ 3.4).
		if _, err := pipe.Process(ctx, f.Path); err != nil {
			logger.Error("файл не обработан — останется в incoming",
				"path", f.Path, "err", err)
		}
	}
	// Канал закрыт (отмена ctx) — конвейер завершён; ждём очистку
	// (выходит по той же отмене).
	return <-cleanupDone
}

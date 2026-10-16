// Сервис ingest (ТЗ 5.1): наблюдает каталог incoming/ и передаёт готовые
// файлы в обработку. Сейчас обработка — логирование события готовности:
// это точка интеграции задач T-014+ (разбор пути, парсинг, загрузка в БД).
package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"time"

	"crawler/internal/config"
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
	if err := service.Run(service.Options{
		Name:        "ingest",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9101"),
		OnRun:       func(ctx context.Context) error { return run(ctx, cfg, logger) },
	}); err != nil {
		os.Exit(1)
	}
}

// run — рабочий цикл ingest: наблюдение incoming (fsnotify + обход-
// страховка, стабильность размера/mtime — internal/watch, ТЗ 5.1) и
// логирование готовых файлов. Канал закрывается при отмене ctx
// (graceful shutdown, ТЗ 3.4).
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger) error {
	files, err := watch.Dir(ctx, watch.Options{
		Dir:          cfg.Ingest.Dir,
		StableAfter:  time.Duration(cfg.Ingest.StableSeconds) * time.Second,
		ScanInterval: cfg.Ingest.ScanInterval.D(),
		Log:          logger,
	})
	if err != nil {
		return err
	}
	for f := range files {
		// T-014: разбор пути <зона>/add|delete/*.txt;
		// T-015+ — парсинг, валидация и загрузка.
		logger.Info("входной файл готов к обработке", "path", f.Path, "size", f.Size)
	}
	return nil
}

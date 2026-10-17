// Сервис analyzer (ТЗ 5.8, §7; T-071): этап анализа Laravel. Потребляет
// задачи q:analyze (Result — ключ строки subdomain_results, T-024;
// публикуют HTTP-этап T-058 и render-worker T-067) группой потребителей;
// вход пассивного анализа читается из БД (заголовки/cookies jsonb,
// content_hash) и хранилища контента (HTML/DOM, ТЗ 5.7), правила
// применяются из текущего снапшота набора (Reloader, T-068: SIGHUP
// перезагружает каталог analyzer.rules_dir без рестарта — хук
// OnSIGHUP каркаса), срабатывания записываются в laravel_findings
// (все, независимо от порога; is_laravel = score ≥ порога, §13.1) одной
// транзакцией со штампом subdomain_results.rules_version (переанализ
// T-074); подтверждение q:analyze — паттерном Processing (XACK только
// после записи в БД, T-025). Каркас — internal/service: миграции, пул БД
// (OnDBPool), /healthz и /metrics, graceful shutdown.
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
	// чтение subdomain_results/запись findings.
	var pool *pgxpool.Pool
	if err := service.Run(service.Options{
		Name:        "analyzer",
		Config:      cfg,
		Logger:      logger,
		MetricsAddr: service.EnvOr("METRICS_ADDR", ":9106"),
		OnDBPool:    func(p *pgxpool.Pool) { pool = p },
		OnRun:       func(ctx context.Context) error { return run(ctx, cfg, logger, pool, reloader) },
		OnSIGHUP:    reloader.HandleSIGHUP,
	}); err != nil {
		os.Exit(1)
	}
}

// run — рабочая логика analyzer: хранилище контента (чтение — тот же
// PackStore, что пишет HTTP/рендер-этапы: словарь zstd T-043 обязан
// совпадать, иначе распаковка чужих блоков невозможна) → цикл q:analyze.
// Завершение — отмена ctx (graceful shutdown, ТЗ 3.4): начатые доставки
// дорабатывают или возвращаются очереди повторной доставкой (Processing,
// T-025).
func run(ctx context.Context, cfg *config.Config, logger *slog.Logger, pool *pgxpool.Pool, reloader *rules.Reloader) error {
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

	// Очередь: только потребление q:analyze и XACK — analyzer ничего не
	// публикует (конец конвейера §7), гейт backpressure не нужен.
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{})

	// Опции воркера — одни на сборку (стиль render-worker): Worker хранит
	// их для ProcessResult, цикл Run берёт те же Group/Consume.
	opts := analyzer.Options{
		MinScore: cfg.Analyzer.LaravelMinScore,
		Log:      logger,
	}
	w, err := analyzer.New(pool, q, store, reloader, opts)
	if err != nil {
		return err
	}
	return analyzer.Run(ctx, w, opts)
}

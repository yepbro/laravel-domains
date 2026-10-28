// ---------------------------------------------------------------- zones collapse
//
// crawlerctl zones collapse (T-196, ТЗТ §7): перенос данных 2LD-зон в
// TLD-зоны — обслуживание окна простоя после перехода на TLD-семантику
// зоны (internal/idn, T-194). Сухой запуск по умолчанию: инвентаризация
// и план без изменений; --apply выполняет перенос батчами (§7.3).
// Логика — internal/collapse (переиспользуется интеграционными тестами
// против живой схемы); здесь — флаги, конфигурация (DSN и allowlist
// ingest.allowed_zone_paths — его зоны не сворачиваются) и пул.

package main

import (
	"fmt"
	"io"

	"crawler/internal/collapse"
	"crawler/internal/config"
	"crawler/internal/db"
)

// zonesCollapse — crawlerctl zones collapse: dry-run по умолчанию,
// --apply/--skip-reconcile/--force (§7.1). Пул — как у соседних команд
// управления (openManagePool, но конфиг нужен и для allowlist — читается
// здесь же, стиль counters.go).
func zonesCollapse(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("zones collapse")
	apply := fs.Bool("apply", false, "выполнить перенос (без флага — dry-run: инвентаризация и план)")
	skipReconcile := fs.Bool("skip-reconcile", false, "пропустить реконсиляцию счётчиков после переноса (только с --apply)")
	force := fs.Bool("force", false, "игнорировать домены в очереди (check_state = 1) в затрагиваемых зонах")
	if handled, err := parseFlags(fs, zonesHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() > 0 {
		return usagef("zones collapse: лишние аргументы %v (зоны выбирает инвентаризация, команда без позиционных аргументов)", fs.Args())
	}
	if *skipReconcile && !*apply {
		return usagef("zones collapse: --skip-reconcile дополняет --apply (dry-run ничего не переносит и не реконсилярует)")
	}

	// Одноразовый CLI: Ctrl-C/SIGTERM прерывают перенос — завершённые
	// батчи остаются, повтор команды продолжает с места остановки.
	ctx, stop := signalCtx()
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("zones collapse: %w", err)
	}
	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2           // бюджет соединений ТЗ 6.3 — у сервисов
	poolOpts.AppName = "crawlerctl" // метка в pg_stat_activity (T-177)
	pool, err := db.Open(ctx, poolOpts)
	if err != nil {
		return fmt.Errorf("zones collapse: %w", err)
	}
	defer pool.Close()

	opts := collapse.Options{
		Allowlist:     cfg.Ingest.AllowedZonePaths, // нормализация — internal/config (T-195)
		Apply:         *apply,
		Force:         *force,
		SkipReconcile: *skipReconcile,
	}
	if err := collapse.Run(ctx, pool, opts, stdout); err != nil {
		return fmt.Errorf("zones collapse: %w", err)
	}
	return nil
}

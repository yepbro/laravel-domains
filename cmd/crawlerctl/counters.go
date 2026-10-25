// ---------------------------------------------------------------- counters
//
// crawlerctl counters (T-178, ТЗИ §9): управление инкрементальными
// счётчиками конвейера — значения (list), ручной пересчёт реконсиляцией
// (recount переиспользует Once-функции monitor — SQL в одной точке
// правды) и сверка без записи (verify). Назначение: восстановление
// после инцидентов (сбой между переходом и дельтой чинит очередной
// проход monitor; здесь — руками), cron-пересчёт для малых корпусов без
// постоянного monitor, послеоперационная сверка (миграции,
// восстановление из бэкапа).

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/analyzer"
	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/idn"
	"crawler/internal/monitor"
)

// countersHelp — справка crawlerctl counters --help.
const countersHelp = `crawlerctl counters — инкрементальные счётчики конвейера (T-178)

Использование:
  crawlerctl counters list
  crawlerctl counters recount [--zone ИМЯ]
  crawlerctl counters verify [--zone ИМЯ] [--tolerance N]

Счётчики состояний конвейера (колонки зон + строки crawler_counters)
ведутся дельтами в транзакциях переходов; авторитет — периодическая
реконсиляция monitor (совмещённый проход раз в
monitor.zone_stats_interval).

  list     значения всех счётчиков: зоны — итогами, глобальные —
           построчно, с возрастом последней записи
  recount  один проход реконсиляции (те же Once-функции monitor):
           вывод «до → после» по позициям с ненулевым дрейфом;
           --zone — перезаписать колонки только выбранной зоны
           (глобальные счётчики пересчитываются всегда)
  verify   сверка счётчиков с живыми агрегатами БЕЗ записи;
           код возврата 0 — расхождений нет (в пределах допуска
           --tolerance, по умолчанию 0), 1 — есть дрейф

Коды возврата: 0 — успех, 1 — ошибка выполнения или дрейф (verify),
2 — использование.
`

// ---------------------------------------------------------------- SQL list

// countersListZonesSQL — итоги справочника зон для list: суммы колонок
// (все живые домены корпуса) и живость обновления (max(updated_at) —
// момент последней реконсиляции).
const countersListZonesSQL = "SELECT coalesce(sum(domains_total), 0), " +
	"coalesce(sum(domains_pending), 0), coalesce(sum(domains_queued), 0), " +
	"coalesce(sum(domains_checked), 0), coalesce(sum(domains_laravel), 0), " +
	"max(updated_at), count(*) FROM zones"

// countersListRowsSQL — строки глобальных счётчиков для list.
const countersListRowsSQL = "SELECT name, value, updated_at FROM crawler_counters ORDER BY name"

// cmdCounters — роутинг подкоманд: list | recount | verify; help-токены
// до имени подкоманды печатают общую справку (контракт TestRunHelp-
// PerCommand: crawlerctl counters --help → код 0).
func cmdCounters(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usagef("укажите подкоманду: crawlerctl counters list|recount|verify")
	}
	switch args[0] {
	case "help", "-h", "--help":
		out(stdout, "%s", countersHelp)
		return flag.ErrHelp
	case "list":
		return countersList(args[1:], stdout, stderr)
	case "recount":
		return countersRecount(args[1:], stdout, stderr)
	case "verify":
		return countersVerify(args[1:], stdout, stderr)
	default:
		return usagef("counters: неизвестная подкоманда %q (list, recount, verify)", args[0])
	}
}

// openCountersPool — пул команды (стиль near): бюджет соединений ТЗ 6.3,
// метка в pg_stat_activity (T-177).
func openCountersPool(ctx context.Context) (*pgxpool.Pool, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}
	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2
	poolOpts.AppName = "crawlerctl"
	pool, err := db.Open(ctx, poolOpts)
	if err != nil {
		return nil, err
	}
	return pool, nil
}

// countersZoneName — punycode-имя зоны: нормализация/синтаксис ДО
// обращения к БД (некорректное имя — ошибка использования, код 2,
// без зависимости от доступности стека); пустое имя — все зоны.
func countersZoneName(name string) (string, error) {
	if name == "" {
		return "", nil
	}
	normalized, err := idn.NormalizeZone(name)
	if err != nil {
		return "", fmt.Errorf("некорректное имя зоны %q: %w", name, err)
	}
	return normalized, nil
}

// countersResolveZone — нормализованное имя зоны → id справочника;
// пустое имя — nil (все зоны).
func countersResolveZone(ctx context.Context, pool *pgxpool.Pool, normalized string) (*int16, string, error) {
	if normalized == "" {
		return nil, "все", nil
	}
	id, found, err := analyzer.ZoneIDByName(ctx, pool, normalized)
	if err != nil {
		return nil, "", err
	}
	if !found {
		return nil, "", fmt.Errorf("зона %s не найдена в справочнике (зоны создаёт ingest при первом файле)", normalized)
	}
	return &id, normalized, nil
}

// countersAge — возраст момента относительно сейчас («5м назад», «—»
// для NULL).
func countersAge(at time.Time) string {
	if at.IsZero() {
		return "—"
	}
	d := time.Since(at)
	switch {
	case d < 0:
		d = 0
	case d < time.Minute:
		return fmt.Sprintf("%dс назад", int(d.Seconds()))
	}
	return fmt.Sprintf("%s назад", monitorAgeDuration(d))
}

// monitorAgeDuration — округлый интервал возраста (формат
// status.FormatDuration без зависимости от пакета status в CLI-таблице:
// словарь тот же).
func monitorAgeDuration(d time.Duration) string {
	switch {
	case d < time.Hour:
		return fmt.Sprintf("%dм", (d+30*time.Second)/time.Minute)
	case d < 24*time.Hour:
		return fmt.Sprintf("%dч %02dм", d/time.Hour, (d%time.Hour)/time.Minute)
	default:
		return fmt.Sprintf("%dд %dч", d/(24*time.Hour), (d%(24*time.Hour))/time.Hour)
	}
}

// ---------------------------------------------------------------- list

func countersList(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("counters list")
	if handled, err := parseFlags(fs, countersHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() > 0 {
		return usagef("counters list: лишние аргументы %v (подкоманда без параметров)", fs.Args())
	}

	ctx, stop := signalCtx()
	defer stop()

	pool, err := openCountersPool(ctx)
	if err != nil {
		return fmt.Errorf("counters list: %w", err)
	}
	defer pool.Close()

	// Зоны — итоги справочника (id, total, pending, queued, checked,
	// laravel, возраст последней реконсиляции; count — отличить пустой
	// справочник от «реконсиляции ещё не было», ревью P3-6).
	var z struct {
		total, pending, queued, checked, laravel int64
		updated                                  *time.Time
		count                                    int64
	}
	rows, err := pool.Query(ctx, countersListZonesSQL)
	if err != nil {
		return fmt.Errorf("counters list: зоны: %w", err)
	}
	if !rows.Next() {
		rows.Close()
		return fmt.Errorf("counters list: зоны: пустой ответ агрегата")
	}
	if err := rows.Scan(&z.total, &z.pending, &z.queued, &z.checked, &z.laravel, &z.updated, &z.count); err != nil {
		rows.Close()
		return fmt.Errorf("counters list: зоны: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("counters list: зоны: %w", err)
	}
	rows.Close()

	// Глобальные счётчики — построчно.
	crows, err := pool.Query(ctx, countersListRowsSQL)
	if err != nil {
		return fmt.Errorf("counters list: счётчики: %w", err)
	}
	defer crows.Close()
	type counterRow struct {
		name    string
		value   int64
		updated time.Time
	}
	counters := make([]counterRow, 0, 16)
	for crows.Next() {
		var r counterRow
		var updated time.Time
		if err := crows.Scan(&r.name, &r.value, &updated); err != nil {
			return fmt.Errorf("counters list: счётчики: чтение: %w", err)
		}
		r.updated = updated
		counters = append(counters, r)
	}
	if err := crows.Err(); err != nil {
		return fmt.Errorf("counters list: счётчики: %w", err)
	}

	out(stdout, "counters: зоны — суммы справочника\n")
	out(stdout, "  всего: %d   pending: %d   queued: %d   проверено: %d   laravel: %d\n",
		z.total, z.pending, z.queued, z.checked, z.laravel)
	if z.updated != nil {
		out(stdout, "  реконсиляция зон: %s\n", countersAge(*z.updated))
	} else if z.count > 0 {
		out(stdout, "  реконсиляция зон: ещё не было (зоны: %d)\n", z.count)
	} else {
		out(stdout, "  реконсиляция зон: — (зон нет)\n")
	}

	out(stdout, "\ncounters: глобальные — crawler_counters\n")
	if len(counters) == 0 {
		out(stdout, "  (пусто — строки создаёт первая реконсиляция, ТЗИ §5)\n")
		return nil
	}
	width := 0
	for _, r := range counters {
		if w := cellWidth(r.name); w > width {
			width = w
		}
	}
	for _, r := range counters {
		out(stdout, "  %-*s  %12d   %s\n", width, r.name, r.value, countersAge(r.updated))
	}
	return nil
}

// ---------------------------------------------------------------- recount

func countersRecount(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("counters recount")
	zone := fs.String("zone", "", "зона (punycode; пусто — все зоны; глобальные счётчики пересчитываются всегда)")
	if handled, err := parseFlags(fs, countersHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() > 0 {
		return usagef("counters recount: лишние аргументы %v", fs.Args())
	}
	normalized, err := countersZoneName(*zone)
	if err != nil {
		return usagef("counters recount: %v", err)
	}

	ctx, stop := signalCtx()
	defer stop()

	pool, err := openCountersPool(ctx)
	if err != nil {
		return fmt.Errorf("counters recount: %w", err)
	}
	defer pool.Close()

	zoneID, zoneDesc, err := countersResolveZone(ctx, pool, normalized)
	if err != nil {
		return usagef("counters recount: %v", err)
	}

	res, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{Zone: zoneID})
	if err != nil {
		return fmt.Errorf("counters recount: %w", err)
	}

	out(stdout, "counters recount: проход реконсиляции (zone_stats_interval — тот же проход monitor), зона: %s, перезаписано зон: %d\n",
		zoneDesc, res.Zones)
	drifted := 0
	for _, p := range res.Positions {
		if p.Drift() == 0 {
			continue
		}
		drifted++
		out(stdout, "  %-32s %12d → %d (Δ%d)\n", p.Counter, p.Old, p.New, p.Drift())
	}
	if drifted == 0 {
		out(stdout, "  дрейфа нет — счётчики уже сходились с агрегатами\n")
	} else {
		out(stdout, "итог: позиций с дрейфом: %d (исправлены проходом)\n", drifted)
	}
	return nil
}

// ---------------------------------------------------------------- verify

func countersVerify(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("counters verify")
	zone := fs.String("zone", "", "зона (punycode; пусто — все зоны; глобальные счётчики сверяются всегда)")
	tolerance := fs.Int64("tolerance", 0, "допуск расхождения (0 — точное совпадение)")
	if handled, err := parseFlags(fs, countersHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() > 0 {
		return usagef("counters verify: лишние аргументы %v", fs.Args())
	}
	if *tolerance < 0 {
		return usagef("counters verify: допуск %d < 0", *tolerance)
	}
	normalized, err := countersZoneName(*zone)
	if err != nil {
		return usagef("counters verify: %v", err)
	}

	ctx, stop := signalCtx()
	defer stop()

	pool, err := openCountersPool(ctx)
	if err != nil {
		return fmt.Errorf("counters verify: %w", err)
	}
	defer pool.Close()

	zoneID, zoneDesc, err := countersResolveZone(ctx, pool, normalized)
	if err != nil {
		return usagef("counters verify: %v", err)
	}

	mismatch, err := monitor.VerifyCounters(ctx, pool, monitor.VerifyOptions{
		Zone:      zoneID,
		Tolerance: *tolerance,
	})
	if err != nil {
		return fmt.Errorf("counters verify: %w", err)
	}

	out(stdout, "counters verify: сверка сохранённых счётчиков с живыми агрегатами (без записи), зона: %s, допуск: %d\n",
		zoneDesc, *tolerance)
	if len(mismatch) == 0 {
		out(stdout, "  расхождений нет\n")
		return nil
	}
	out(stdout, "  %-32s %14s %14s %s\n", "счётчик", "сохранено", "агрегат", "Δ")
	for _, p := range mismatch {
		out(stdout, "  %-32s %14d %14d %d\n", p.Counter, p.Old, p.New, p.Drift())
	}
	// Код 1: дрейф есть — вызывающий скрипт обязан видеть расхождение.
	return fmt.Errorf("counters verify: дрейф счётчиков — %d позиций (лечит crawlerctl counters recount)",
		len(mismatch))
}

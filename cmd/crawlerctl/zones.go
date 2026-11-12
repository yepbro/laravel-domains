// ---------------------------------------------------------------- zones
//
// crawlerctl zones (T-084, spec 4.1, 5.3): zone management — list/enable/
// disable of the zones.is_active flag and sync — filling the reference
// table from the IANA registry (T-125, zonessync.go). A zone is a
// reference row (created by ingest on the first file, spec 5.1, or by
// zones sync); is_active — the zone participation
// in the scheduler selection: DispatchBatch (T-027) joins zones with the
// z.is_active condition, so a disabled zone is never selected at all — its
// domains stay pending and wait for the zone to be enabled (no datafix
// needed: enable returns the zone into the selection automatically, the
// domain states did not change).
//
// disable does not touch domains (spec 13.1: deleted/disabled — the state
// is preserved): neither check_state, nor statuses, nor results; the
// pause is expressed by the single zone flag. Ingest of a disabled zone
// keeps working: file ingest does not depend on is_active (spec 5.1),
// domains accumulate and are checked after enabling. The progress
// counters domains_total/domains_checked are updated by monitor (T-078,
// spec 5.9) — list only displays them.

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"

	"crawler/internal/db"
	"crawler/internal/idn"
)

// zonesHelp — the crawlerctl zones [--help] text and per-action help.
const zonesHelp = `crawlerctl zones — управление зонами проверки (ТЗ 4.1, 5.3)

Использование:
  crawlerctl zones list
  crawlerctl zones enable <зона>
  crawlerctl zones disable <зона>
  crawlerctl zones sync [--file ПУТЬ] [--url URL] [--deactivate-missing]
  crawlerctl zones collapse [--apply] [--skip-reconcile] [--force]

Зона — имя в любой форме (unicode/punycode, регистр, точка на конце),
как каталог incoming/<зона>; в БД хранится punycode (ТЗ 5.2).

  list                таблица зон: id, имя (punycode и unicode), флаг
                      активности, счётчики всего/проверено (обновляет
                      monitor, ТЗ 5.9)
  enable <зона>       включить проверку зоны (is_active = true): домены
                      зоны возвращаются в выборку планировщика — те, что
                      накопились pending за время паузы, проверяются
                      в общем порядке
  disable <зона>      приостановить проверку зоны (is_active = false):
                      планировщик не выбирает домены зоны; сами домены,
                      их состояния и результаты не меняются (ТЗ 13.1),
                      приём файлов зоны работает как прежде
  sync [--file ПУТЬ] [--url URL] [--deactivate-missing]
                      наполнить справочник зон из среза реестра корневой
                      зоны IANA (tlds-alpha-by-domain.txt, ~1460 TLD,
                      IDN уже в punycode). Новые зоны добавляются
                      включёнными (is_active = true — как автосоздание
                      ingest'ом, ТЗ 5.1), существующие строки не меняются
                      (is_active — владение оператора, счётчики —
                      monitor), повторный запуск ничего не меняет.
                      Источник: --url (по умолчанию data.iana.org) или
                      заранее скачанный файл --file (офлайн-режим, прод
                      без исходящего интернета). --deactivate-missing
                      дополнительно выключает зоны БД, отсутствующие
                      в срезе (retired TLD), с их списком в отчёте.
                      Это справочник имён TLD, а не источник доменов:
                      домены — только через ingest (var/incoming)
  collapse [--apply] [--skip-reconcile] [--force]
                      перенести данные 2LD-зон в TLD-зоны (T-196, окно
                      обслуживания): зона — последняя метка имени
                      (example.5g.in → зона in, label example.5g). Без
                      флагов — dry-run: инвентаризация и план, ничего не
                      меняет. --apply выполняет перенос батчами по всем
                      таблицам с zone_id (домены с доклейкой префикса к
                      label, результаты, находки, история), перепривязывает
                      ingest_files с разруливанием дедупа, дропает партиции
                      2LD и удаляет их строки; идемпотентно и возобновляемо
                      (повтор продолжает с места остановки). После переноса
                      — реконсиляция счётчиков (общая с monitor);
                      --skip-reconcile её пропускает, --force игнорирует
                      домены в очереди (check_state = 1). Зоны
                      ingest.allowed_zone_paths (тестовые) не сворачиваются.
                      Перенос необратим: откат — восстановление бэкапа
                      (runbook — docs/runbooks.md)

Коды возврата: 0 — успех, 1 — ошибка (неизвестная зона, БД, источник
среза, мусорная строка), 2 — использование.
`

// ---------------------------------------------------------------- SQL

// zonesSelectSQL — the whole zone reference table by ascending name (the
// zoneStatsSQL style from internal/status, T-079): name — punycode (the canonical form), coalesce
// gives an empty string instead of NULL for ASCII zones (name_unicode is
// stored only for display and for names differing from name, spec 5.2 item 7).
const zonesSelectSQL = "SELECT id, name, coalesce(name_unicode, '') AS name_unicode, " +
	"is_active, domains_total, domains_checked FROM zones ORDER BY name"

// zoneByNameSQL — a zone row by punycode name (UNIQUE, migration 000001):
// the dereference of the enable/disable argument and the "zone already in
// the state" check.
const zoneByNameSQL = "SELECT id, name, coalesce(name_unicode, '') AS name_unicode, " +
	"is_active, domains_total, domains_checked FROM zones WHERE name = $1"

// zoneSetActiveSQL — enabling/disabling a zone with a single UPDATE by id:
// only is_active changes, domains and counters are not touched (spec 13.1;
// updated_at is monitor's ownership, T-078). RETURNING — the row after
// the update (empty — a race with zone deletion between the lookup and
// the UPDATE).
const zoneSetActiveSQL = "UPDATE zones SET is_active = $1 WHERE id = $2 " +
	"RETURNING id, name, coalesce(name_unicode, '') AS name_unicode, is_active"

// ---------------------------------------------------------------- data

// zoneRow — a zone reference table row for the CLI (the migration 000001
// columns; the counters are updated by monitor, T-078).
type zoneRow struct {
	id          int16
	name        string // punycode — the canonical form (spec 5.2)
	nameUnicode string // "" — NULL; display only
	active      bool
	total       int64 // domains_total
	checked     int64 // domains_checked
}

// scanZoneRow reads a reference table row in the column order of the SQL constant.
func scanZoneRow(scan func(dest ...any) error) (zoneRow, error) {
	var z zoneRow
	if err := scan(&z.id, &z.name, &z.nameUnicode, &z.active, &z.total, &z.checked); err != nil {
		return zoneRow{}, err
	}
	return z, nil
}

// fetchZones — the whole reference table by ascending name (list).
func fetchZones(ctx context.Context, pool db.DB) ([]zoneRow, error) {
	rows, err := pool.Query(ctx, zonesSelectSQL)
	if err != nil {
		return nil, fmt.Errorf("справочник зон: %w", err)
	}
	defer rows.Close()
	var zones []zoneRow
	for rows.Next() {
		z, err := scanZoneRow(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("справочник зон: чтение: %w", err)
		}
		zones = append(zones, z)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("справочник зон: %w", err)
	}
	return zones, nil
}

// lookupZoneByName — a zone row by punycode name; found = false — no such zone.
func lookupZoneByName(ctx context.Context, pool db.DB, name string) (zoneRow, bool, error) {
	rows, err := pool.Query(ctx, zoneByNameSQL, name)
	if err != nil {
		return zoneRow{}, false, fmt.Errorf("зона по имени %s: %w", name, err)
	}
	defer rows.Close()
	if rows.Next() {
		z, err := scanZoneRow(rows.Scan)
		if err != nil {
			return zoneRow{}, false, fmt.Errorf("зона по имени %s: чтение: %w", name, err)
		}
		return z, true, nil
	}
	if err := rows.Err(); err != nil {
		return zoneRow{}, false, fmt.Errorf("зона по имени %s: %w", name, err)
	}
	return zoneRow{}, false, nil
}

// setZoneActive — enabling/disabling by id; returns the row after the
// update. An empty answer — the row disappeared between the lookup and
// the UPDATE (nobody deletes zones, so the rejection is treated as a DB
// contract error).
func setZoneActive(ctx context.Context, pool db.DB, id int16, active bool) (zoneRow, error) {
	rows, err := pool.Query(ctx, zoneSetActiveSQL, active, id)
	if err != nil {
		return zoneRow{}, fmt.Errorf("переключение зоны (id %d): %w", id, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return zoneRow{}, fmt.Errorf("переключение зоны (id %d): строка не обновлена", id)
	}
	var z zoneRow
	if err := rows.Scan(&z.id, &z.name, &z.nameUnicode, &z.active); err != nil {
		return zoneRow{}, fmt.Errorf("переключение зоны (id %d): чтение: %w", id, err)
	}
	if err := rows.Err(); err != nil {
		return zoneRow{}, fmt.Errorf("переключение зоны (id %d): %w", id, err)
	}
	return z, nil
}

// zoneDesc — the zone name for messages: punycode + the unicode form if
// it differs (name_unicode is empty for ASCII zones, spec 5.2 item 7).
func zoneDesc(z zoneRow) string {
	if z.nameUnicode != "" && z.nameUnicode != z.name {
		return fmt.Sprintf("%s (%s)", z.name, z.nameUnicode)
	}
	return z.name
}

// ---------------------------------------------------------------- actions

// zonesListRun — prints the zone reference table: id, name, unicode, act, counters.
func zonesListRun(ctx context.Context, pool db.DB, stdout io.Writer) error {
	zones, err := fetchZones(ctx, pool)
	if err != nil {
		return err
	}
	active := 0
	var total, checked int64
	head := []string{"id", "зона", "unicode", "акт", "всего", "проверено"}
	rows := [][]string{head}
	for _, z := range zones {
		mark := "-"
		if z.active {
			mark = "+"
			active++
		}
		total += z.total
		checked += z.checked
		uni := z.nameUnicode
		if uni == z.name {
			uni = "" // equals the punycode — no column duplication
		}
		if uni == "" {
			uni = "—"
		}
		rows = append(rows, []string{
			strconv.FormatInt(int64(z.id), 10),
			z.name, uni, mark,
			strconv.FormatInt(z.total, 10), strconv.FormatInt(z.checked, 10),
		})
	}
	widths := make([]int, len(head))
	for _, r := range rows {
		for i, c := range r {
			if w := cellWidth(c); w > widths[i] {
				widths[i] = w
			}
		}
	}
	// Numeric columns (id, counters) — right-aligned, text — left-aligned
	// (the writeStatusTable style, T-079).
	for _, r := range rows {
		var b strings.Builder
		for i, c := range r {
			if i > 0 {
				b.WriteByte(' ')
			}
			if i == 0 || i >= 4 { // id and counters are numbers
				fmt.Fprintf(&b, "%*s", widths[i], c)
			} else {
				fmt.Fprintf(&b, "%-*s", widths[i], c)
			}
		}
		out(stdout, "%s\n", b.String())
	}
	out(stdout, "зон %d, включено %d, доменов всего %d, проверено %d\n",
		len(zones), active, total, checked)
	return nil
}

// zonesToggleRun — enabling/disabling a zone by name (is_active).
// Idempotent: a repeated enable/disable reports "already enabled/disabled"
// (the subdomains disable style, T-083).
func zonesToggleRun(ctx context.Context, pool db.DB, action string, name string, stdout io.Writer) error {
	z, found, err := lookupZoneByName(ctx, pool, name)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("зона %s не найдена в справочнике (зоны создаёт ingest при первом файле; см. crawlerctl zones list)", name)
	}
	want := action == "enable"
	if z.active == want {
		state := "включена"
		if !z.active {
			state = "выключена"
		}
		out(stdout, "зона %s (id %d) уже %s\n", zoneDesc(z), z.id, state)
		return nil
	}
	after, err := setZoneActive(ctx, pool, z.id, want)
	if err != nil {
		return err
	}
	if want {
		out(stdout, "зона %s (id %d) включена: домены возвращаются в выборку планировщика (pending проверяется в общем порядке)\n",
			zoneDesc(after), after.id)
		return nil
	}
	out(stdout, "зона %s (id %d) выключена: проверка доменов приостановлена; домены, состояния и результаты не меняются (ТЗ 13.1)\n",
		zoneDesc(after), after.id)
	return nil
}

// ---------------------------------------------------------------- commands

// cmdZones — routing of the zone actions: list / enable / disable / sync
// (sync — T-125, zonessync.go) / collapse (T-196, zonescollapse.go).
func cmdZones(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usagef("укажите действие: crawlerctl zones list|enable|disable|sync|collapse")
	}
	action, rest := args[0], args[1:]
	switch action {
	case "list":
		return zonesList(rest, stdout, stderr)
	case "enable", "disable":
		return zonesToggle(action, rest, stdout, stderr)
	case "sync":
		return zonesSync(rest, stdout, stderr)
	case "collapse":
		return zonesCollapse(rest, stdout, stderr)
	case "-h", "--help", "help":
		out(stdout, "%s", zonesHelp)
		return flag.ErrHelp
	default:
		return usagef("неизвестное действие %q (допустимы: list, enable, disable, sync, collapse)", action)
	}
}

func zonesList(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("zones list")
	if handled, err := parseFlags(fs, zonesHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() > 0 {
		return usagef("zones list: лишние аргументы %v (команда без аргументов)", fs.Args())
	}
	ctx, stop := signalCtx()
	defer stop()
	pool, err := openManagePool(ctx)
	if err != nil {
		return fmt.Errorf("zones list: %w", err)
	}
	defer pool.Close()
	if err := zonesListRun(ctx, pool, stdout); err != nil {
		return fmt.Errorf("zones list: %w", err)
	}
	return nil
}

func zonesToggle(action string, args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("zones " + action)
	if handled, err := parseFlags(fs, zonesHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() != 1 {
		return usagef("укажите зону: crawlerctl zones %s crawl.test", action)
	}
	// Input validation — through the same pipeline as the incoming
	// directories (internal/idn.NormalizeZone): the zone is stored in
	// punycode (spec 5.2).
	zone, err := idn.NormalizeZone(fs.Arg(0))
	if err != nil {
		return usagef("zones %s: некорректное имя зоны %q: %v", action, fs.Arg(0), err)
	}
	ctx, stop := signalCtx()
	defer stop()
	pool, err := openManagePool(ctx)
	if err != nil {
		return fmt.Errorf("zones %s %s: %w", action, zone, err)
	}
	defer pool.Close()
	if err := zonesToggleRun(ctx, pool, action, zone, stdout); err != nil {
		return fmt.Errorf("zones %s %s: %w", action, zone, err)
	}
	return nil
}

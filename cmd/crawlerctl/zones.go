// ---------------------------------------------------------------- zones
//
// crawlerctl zones (T-084, ТЗ 4.1, 5.3): управление зонами проверки —
// list/enable/disable флага zones.is_active и sync — наполнение
// справочника из реестра IANA (T-125, zonessync.go). Зона — справочная
// строка (создаёт ingest при первом файле, ТЗ 5.1, или zones sync);
// is_active — участие зоны
// в выборке планировщика: DispatchBatch (T-027) джойнит зоны с условием
// z.is_active, поэтому выключенная зона не выбирается вовсе — её домены
// остаются pending и ждут включения (datafix не нужен: enable возвращает
// зону в выборку автоматически, состояние доменов не менялось).
//
// disable НЕ трогает домены (ТЗ 13.1: удалённый/выключенный — состояние
// сохраняется): ни check_state, ни статусы, ни результаты; приостановка
// выразилась единственным флагом зоны. Ingest выключенной зоны продолжает
// работать: приём файлов не зависит от is_active (ТЗ 5.1), домены
// накапливаются и проверятся после включения. Счётчики прогресса
// domains_total/domains_checked обновляет monitor (T-078, ТЗ 5.9) —
// list их только показывает.

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

// zonesHelp — справка crawlerctl zones [--help] и по действиям.
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

// zonesSelectSQL — весь справочник зон по возрастанию имени (стиль
// zoneStatsSQL из internal/status, T-079): name — punycode (каноническая форма), coalesce
// даёт пустую строку вместо NULL у ASCII-зон (name_unicode хранится
// только для отображения и отличающихся от name, ТЗ 5.2 п. 7).
const zonesSelectSQL = "SELECT id, name, coalesce(name_unicode, '') AS name_unicode, " +
	"is_active, domains_total, domains_checked FROM zones ORDER BY name"

// zoneByNameSQL — строка зоны по punycode-имени (UNIQUE, миграция 000001):
// разыменование аргумента enable/disable и проверка «зона уже в состоянии».
const zoneByNameSQL = "SELECT id, name, coalesce(name_unicode, '') AS name_unicode, " +
	"is_active, domains_total, domains_checked FROM zones WHERE name = $1"

// zoneSetActiveSQL — включение/выключение зоны одним UPDATE по id:
// меняется только is_active, домены и счётчики не трогаются (ТЗ 13.1;
// updated_at — владение monitor, T-078). RETURNING — строка после
// обновления (пусто — гонка удаления зоны между lookup и UPDATE).
const zoneSetActiveSQL = "UPDATE zones SET is_active = $1 WHERE id = $2 " +
	"RETURNING id, name, coalesce(name_unicode, '') AS name_unicode, is_active"

// ---------------------------------------------------------------- данные

// zoneRow — строка справочника зон для CLI (колонки миграции 000001;
// счётчики обновляет monitor, T-078).
type zoneRow struct {
	id          int16
	name        string // punycode — каноническая форма (ТЗ 5.2)
	nameUnicode string // "" — NULL; только отображение
	active      bool
	total       int64 // domains_total
	checked     int64 // domains_checked
}

// scanZoneRow читает строку справочника в порядке колонок SQL-константы.
func scanZoneRow(scan func(dest ...any) error) (zoneRow, error) {
	var z zoneRow
	if err := scan(&z.id, &z.name, &z.nameUnicode, &z.active, &z.total, &z.checked); err != nil {
		return zoneRow{}, err
	}
	return z, nil
}

// fetchZones — весь справочник по возрастанию имени (list).
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

// lookupZoneByName — строка зоны по punycode-имени; found = false — зоны нет.
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

// setZoneActive — включение/выключение по id; возвращает строку после
// обновления. Пустой ответ — строка исчезла между lookup и UPDATE
// (зоны никто не удаляет, поэтому отказ трактуется как ошибка БД-контракта).
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

// zoneDesc — имя зоны для сообщений: punycode + unicode-форма, если
// отличается (name_unicode пуст для ASCII-зон, ТЗ 5.2 п. 7).
func zoneDesc(z zoneRow) string {
	if z.nameUnicode != "" && z.nameUnicode != z.name {
		return fmt.Sprintf("%s (%s)", z.name, z.nameUnicode)
	}
	return z.name
}

// ---------------------------------------------------------------- действия

// zonesListRun — вывод справочника зон: id, имя, unicode, акт, счётчики.
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
			uni = "" // совпадает с punycode — колонку не дублируем
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
	// Числовые колонки (id, счётчики) — вправо, текстовые — влево
	// (стиль writeStatusTable, T-079).
	for _, r := range rows {
		var b strings.Builder
		for i, c := range r {
			if i > 0 {
				b.WriteByte(' ')
			}
			if i == 0 || i >= 4 { // id и счётчики — числа
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

// zonesToggleRun — включение/выключение зоны по имени (is_active).
// Идемпотентно: повторный enable/disable сообщает «уже включена/выключена»
// (стиль subdomains disable, T-083).
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

// ---------------------------------------------------------------- команды

// cmdZones — роутинг действий зон: list / enable / disable / sync
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
	// Валидация входа — тем же конвейером, что и каталоги incoming
	// (internal/idn.NormalizeZone): зона хранится в punycode (ТЗ 5.2).
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

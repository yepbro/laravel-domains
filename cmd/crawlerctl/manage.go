// ---------------------------------------------------------------- управление
//
// Команды управления из ТЗ 4.1: status (T-079), reanalyze (реализован,
// T-074), subdomains (T-083), zones и recheck (T-084). Команда разбирает
// аргументы и валидирует вход (имена — через internal/idn); каркасы
// возвращают notImplemented с номером задачи, выборки из БД выполнят
// сами задачи.

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"crawler/internal/analyzer"
	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/idn"
	"crawler/internal/queue"
	"crawler/internal/rules"
)

// ---------------------------------------------------------------- status

// statusHelp — справка crawlerctl status --help.
const statusHelp = `crawlerctl status — сводка по зонам, очередям, скорости (ТЗ 5.9)

Использование:
  crawlerctl status

Покажет: прогресс по зонам (domains_total/domains_checked), длины очередей
q:dns/q:http/q:render/q:analyze, скорость этапов и оценку времени
завершения цикла. Реализация — задача T-079 (после планировщика и monitor).
`

func cmdStatus(args []string, stdout, stderr io.Writer) error {
	if handled, err := parseFlags(newFlagSet("status"), statusHelp, args, stdout); handled {
		return err
	}
	out(stdout, "%s", `crawlerctl status: сводка появится после реализации планировщика
и monitor (задача T-079).

Пока можно смотреть:
  make ps                    — состояние контейнеров
  make psql                  — консоль PostgreSQL (таблицы zones/domains)
  make redis-cli             — консоль Redis (XLEN q:dns и т.п.)
  http://127.0.0.1:9090      — Prometheus (профиль monitoring: make up && docker compose --profile monitoring up -d)
`)
	return nil
}

// ---------------------------------------------------------------- reanalyze

// reanalyzeHelp — справка crawlerctl reanalyze --help.
const reanalyzeHelp = `crawlerctl reanalyze — переанализ сохранённых результатов (ТЗ 5.8)

Использование:
  crawlerctl reanalyze [--zone ИМЯ]

Находит subdomain_results с устаревшим rules_version (включая никогда не
анализированные) и ставит их в q:analyze пачками — повторное скачивание
не выполняется, анализ идёт по сохранённому контенту. Без --zone — все
зоны. Версия набора правил берётся из каталога analyzer.rules_dir на
момент запуска (живой сервис analyzer перезагружает правила SIGHUP и
переанализирует сам); воркер analyzer фиксирует новые результаты.
Коды возврата: 0 — успех, 1 — ошибка, 2 — использование.
`

func cmdReanalyze(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("reanalyze")
	zone := fs.String("zone", "", "переанализ по зоне (пусто — все)")
	if handled, err := parseFlags(fs, reanalyzeHelp, args, stdout); handled {
		return err
	}
	zoneName := ""
	if *zone != "" {
		// Валидация входа — тем же конвейером, что и каталоги incoming
		// (internal/idn.NormalizeZone): зона хранится в punycode.
		normalized, err := idn.NormalizeZone(*zone)
		if err != nil {
			return usagef("reanalyze: некорректное имя зоны %q: %v", *zone, err)
		}
		zoneName = normalized
	}
	if fs.NArg() > 0 {
		return usagef("reanalyze: лишние аргументы %v (команда принимает только --zone)", fs.Args())
	}

	// Конфигурация — единый YAML + окружение (internal/config): DSN
	// PostgreSQL, адрес Redis, каталог правил и порог backpressure.
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("reanalyze: %w", err)
	}

	// Версия набора — критерий устаревания; каталог читается разово.
	set, err := rules.LoadDir(cfg.Analyzer.RulesDir)
	if err != nil {
		return fmt.Errorf("reanalyze: %w", err)
	}

	// Одноразовый CLI: Ctrl-C/SIGTERM прерывают дренаж (опубликованное
	// остаётся, at-least-once).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2 // бюджет соединений ТЗ 6.3 — у сервисов
	pool, err := db.Open(ctx, poolOpts)
	if err != nil {
		return fmt.Errorf("reanalyze: %w", err)
	}
	defer pool.Close()

	zoneDesc := "все"
	var zoneID *int16
	if zoneName != "" {
		id, found, err := analyzer.ZoneIDByName(ctx, pool, zoneName)
		if err != nil {
			return fmt.Errorf("reanalyze: %w", err)
		}
		if !found {
			return fmt.Errorf("reanalyze: зона %s не найдена в справочнике (зоны создаёт ingest при первом файле)", zoneName)
		}
		zoneID, zoneDesc = &id, zoneName
	}

	// Очередь — как у сервисов (RedisQueue + гейт backpressure T-026);
	// публикация q:analyze ждёт ёмкости (Warn о паузе — в stderr), отмена
	// ctx прерывает.
	log := slog.New(slog.NewTextHandler(stderr, nil))
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(cfg.Scheduler.QueueHighWatermark),
		Log:       log,
	})
	q := queue.NewWatermarkPublisher(queue.NewRedisQueue(rdb, queue.RedisOptions{}), gate)

	out(stdout, "reanalyze: набор правил v%d, зона: %s\n", set.Version, zoneDesc)
	res, err := analyzer.ReanalyzeDrain(ctx, pool, q, set.Version, zoneID, analyzer.ReanalyzeOptions{})
	if err != nil {
		return fmt.Errorf("reanalyze: поставлено в q:analyze %d, прервано: %w", res.Published, err)
	}
	if res.Published == 0 {
		out(stdout, "reanalyze: устаревших результатов нет — переанализ не требуется\n")
		return nil
	}
	out(stdout, "reanalyze: поставлено в q:analyze: %d\n", res.Published)
	return nil
}

// ---------------------------------------------------------------- subdomains

// subdomainsHelp — справка crawlerctl subdomains [--help]; она же печатается
// для действий subdomains <действие> --help.
const subdomainsHelp = `crawlerctl subdomains — управление проверяемыми субдоменами (ТЗ 5.10)

Использование:
  crawlerctl subdomains list
  crawlerctl subdomains add <метка>
  crawlerctl subdomains disable <метка>

Справочник субдоменов — таблица subdomains: каждый субдомен занимает бит
dns_mask (максимум 63, бит 63 знаковый), '' — апекс (id 0). Добавление
субдомена не требует миграции; проверка нового субдомена по зоне/всем
доменам — отдельная команда (T-083). Метка — короткое имя без точек
(www, admin, …), unicode-метки приводятся к punycode.
Реализация — задача T-083.
`

func cmdSubdomains(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usagef("укажите действие: crawlerctl subdomains list|add|disable")
	}
	action, rest := args[0], args[1:]
	switch action {
	case "list":
		return subdomainsList(rest, stdout, stderr)
	case "add":
		return subdomainsAdd(rest, stdout, stderr)
	case "disable":
		return subdomainsDisable(rest, stdout, stderr)
	case "-h", "--help", "help":
		out(stdout, "%s", subdomainsHelp)
		return flag.ErrHelp
	default:
		return usagef("неизвестное действие %q (допустимы: list, add, disable)", action)
	}
}

// Внутренние функции групп subdomains/zones принимают stderr, как и сами
// команды: реализации T-083/T-084 будут писать диагностику туда — сигнатуры
// не поменяются.

func subdomainsList(args []string, stdout, stderr io.Writer) error {
	if handled, err := parseFlags(newFlagSet("subdomains list"), subdomainsHelp, args, stdout); handled {
		return err
	}
	return notImplemented("subdomains list", "T-083")
}

func subdomainsAdd(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("subdomains add")
	if handled, err := parseFlags(fs, subdomainsHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() != 1 {
		return usagef("укажите метку субдомена: crawlerctl subdomains add www")
	}
	label, err := normalizeSubdomainLabel(fs.Arg(0))
	if err != nil {
		return usagef("subdomains add: %v", err)
	}
	return notImplemented("subdomains add "+label, "T-083")
}

func subdomainsDisable(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("subdomains disable")
	if handled, err := parseFlags(fs, subdomainsHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() != 1 {
		return usagef("укажите метку субдомена: crawlerctl subdomains disable www")
	}
	label, err := normalizeSubdomainLabel(fs.Arg(0))
	if err != nil {
		return usagef("subdomains disable: %v", err)
	}
	return notImplemented("subdomains disable "+label, "T-083")
}

// normalizeSubdomainLabel приводит метку субдомена к канонической форме:
// unicode → punycode (ТЗ 5.2, HostToASCII — короткая метка допустимый
// «хост»), затем проверка DNS-метки: латиница/цифры/дефис внутри,
// 1..63 октета. Пустая метка — апекс (id 0) — уже есть в справочнике,
// добавлению не подлежит.
func normalizeSubdomainLabel(label string) (string, error) {
	if strings.TrimSpace(label) == "" {
		return "", fmt.Errorf("метка субдомена пуста: апекс '' уже есть в справочнике (id 0)")
	}
	ascii, err := idn.HostToASCII(label)
	if err != nil {
		return "", fmt.Errorf("некорректная метка субдомена %q: %v", label, err)
	}
	if !validSubdomainLabel(ascii) {
		return "", fmt.Errorf("некорректная метка субдомена %q: допустимы a-z, 0-9 и дефис внутри, длина 1..63", label)
	}
	return ascii, nil
}

// validSubdomainLabel проверяет ASCII-метку: [a-z0-9-], дефис только
// внутри, длина 1..63 (длина DNS-метки, ТЗ 5.2 п. 4).
func validSubdomainLabel(ascii string) bool {
	if len(ascii) == 0 || len(ascii) > 63 {
		return false
	}
	for i := 0; i < len(ascii); i++ {
		c := ascii[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '-':
			if i == 0 || i == len(ascii)-1 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------- zones

// zonesHelp — справка crawlerctl zones [--help] и по действиям.
const zonesHelp = `crawlerctl zones — управление зонами проверки (ТЗ 4.1)

Использование:
  crawlerctl zones list
  crawlerctl zones enable <зона>
  crawlerctl zones disable <зона>

Зона — имя в любой форме (unicode/punycode, регистр, точка на конце),
как каталог incoming/<зона>. Выключенная зона не проверяется, данные
сохраняются. Реализация — задача T-084.
`

func cmdZones(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return usagef("укажите действие: crawlerctl zones list|enable|disable")
	}
	action, rest := args[0], args[1:]
	switch action {
	case "list":
		return zonesList(rest, stdout, stderr)
	case "enable", "disable":
		return zonesToggle(action, rest, stdout, stderr)
	case "-h", "--help", "help":
		out(stdout, "%s", zonesHelp)
		return flag.ErrHelp
	default:
		return usagef("неизвестное действие %q (допустимы: list, enable, disable)", action)
	}
}

func zonesList(args []string, stdout, stderr io.Writer) error {
	if handled, err := parseFlags(newFlagSet("zones list"), zonesHelp, args, stdout); handled {
		return err
	}
	return notImplemented("zones list", "T-084")
}

func zonesToggle(action string, args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("zones " + action)
	if handled, err := parseFlags(fs, zonesHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() != 1 {
		return usagef("укажите зону: crawlerctl zones %s crawl.test", action)
	}
	zone, err := idn.NormalizeZone(fs.Arg(0))
	if err != nil {
		return usagef("zones %s: некорректное имя зоны %q: %v", action, fs.Arg(0), err)
	}
	return notImplemented("zones "+action+" "+zone, "T-084")
}

// ---------------------------------------------------------------- recheck

// recheckHelp — справка crawlerctl recheck --help.
const recheckHelp = `crawlerctl recheck — принудительная постановка домена на проверку

Использование:
  crawlerctl recheck <домен> [домен …]

Домены (одно имя или несколько) немедленно ставятся в очередь q:dns,
не дожидаясь истечения интервала перепроверки. Это «ручные задачи»
crawlerctl из ТЗ 4.1; реализация — задача T-084 (после планировщика
и очереди).
`

func cmdRecheck(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("recheck")
	if handled, err := parseFlags(fs, recheckHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() == 0 {
		return usagef("укажите домен: crawlerctl recheck example.crawl.test")
	}
	names := make([]string, 0, fs.NArg())
	for _, a := range fs.Args() {
		res, err := idn.Normalize(a)
		if err != nil {
			return usagef("recheck: некорректное имя домена %q: %v", a, err)
		}
		names = append(names, res.ASCII)
	}
	return notImplemented("recheck "+summarizeNames(names), "T-084")
}

// summarizeNames сжимает список имён для сообщения-заглушки: первые три
// полностью, остаток счётчиком (+N) — строка ошибки не простраивается.
func summarizeNames(names []string) string {
	const show = 3
	if len(names) <= show {
		return strings.Join(names, " ")
	}
	return fmt.Sprintf("%s (+%d)", strings.Join(names[:show], " "), len(names)-show)
}

// ---------------------------------------------------------------- status
//
// crawlerctl status (T-079, ТЗ 5.9): вывод сводки по зонам, очередям,
// скорости и ожидаемому времени завершения первого цикла проверок.
// Данные и расчёты — пакет internal/status (ТЗД §4.5, T-126): общая
// библиотека сводки CLI и дашборда, один источник правды о смысле чисел;
// SQL-тексты, выборки и производные величины — там, здесь — только
// форматирование (таблица/JSON) и склейка команды с инфраструктурой
// (конфигурация, пул БД, Redis).
//
// Недоступный Redis не прячет сводку по БД: секция очередей печатается с
// «н/д», команда завершается кодом 1.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/queue"
	"crawler/internal/status"
)

// statusHelp — справка crawlerctl status --help.
const statusHelp = `crawlerctl status — сводка по зонам, очередям, скорости (ТЗ 5.9)

Использование:
  crawlerctl status [--json]

Сводка: по каждой зоне — счётчики domains_total/domains_checked (обновляет
monitor) и агрегат domains по check_state (pending без учёта deleted,
queued, done); длины очередей q:dns/q:http/q:render/q:analyze/q:dead
(XLEN); скорость проверок (доменов/с — средняя с начала суток:
last_checked_at имеет точность дня; HTTP-результаты и рендеры — за окно
темпа 1–2 ч: часовые корзины crawler_counters, T-203) и ETA завершения
первого цикла: (pending + queued) / скорость. Темп этапов DNS и анализа
из БД не
измерим — их видно в Prometheus. --json — машинный вывод. Команда работает
на стеке dev-окружения (PostgreSQL/Redis); при недоступном Redis сводка
по БД печатается, код возврата — 1.
Коды возврата: 0 — успех, 1 — ошибка, 2 — использование.
`

// ---------------------------------------------------------------- вывод

// actMark — колонка «акт»: зона участвует в проверках.
func actMark(active bool) string {
	if active {
		return "+"
	}
	return "-"
}

// percentMark — доля проверенных по счётчикам зоны; «—» при пустой зоне.
func percentMark(checked, total int64) string {
	if total <= 0 {
		return "—"
	}
	return fmt.Sprintf("%.0f%%", float64(checked)*100/float64(total))
}

// writeStatusTable — человекочитаемая сводка: моноширинная таблица зон
// (текстовые колонки влево, числа вправо), очереди, скорость, цикл.
// lens == nil — Redis недоступен, очереди «н/д».
func writeStatusTable(w io.Writer, zones []status.ZoneStats, tot status.Totals, thr status.Throughput, rates status.Rates, lens map[string]int64) {
	out(w, "зоны: всего/проверено/pending/queued/done — инкрементальные счётчики zones\n"+
		"(дельты переходов + реконсиляция monitor, T-178; deleted вне базового множества)\n\n")

	// Колонки: зона и акт — текст, остальные — числа (выравнивание
	// вправо). Имя зоны — punycode (каноническая форма, как каталог
	// incoming/<зона>); unicode-форма — в --json.
	head := []string{"зона", "акт", "всего", "проверено", "%", "pending", "queued", "done"}
	rows := [][]string{head}
	row := func(cells ...string) []string { return cells }
	for _, z := range zones {
		rows = append(rows, row(z.Name, actMark(z.IsActive),
			strconv.FormatInt(z.DomainsTotal, 10), strconv.FormatInt(z.DomainsChecked, 10),
			percentMark(z.DomainsChecked, z.DomainsTotal),
			strconv.FormatInt(z.Pending, 10), strconv.FormatInt(z.Queued, 10), strconv.FormatInt(z.Done, 10)))
	}
	rows = append(rows, row("итого", "",
		strconv.FormatInt(tot.DomainsTotal, 10), strconv.FormatInt(tot.DomainsChecked, 10),
		percentMark(tot.DomainsChecked, tot.DomainsTotal),
		strconv.FormatInt(tot.Pending, 10), strconv.FormatInt(tot.Queued, 10), strconv.FormatInt(tot.Done, 10)))

	widths := make([]int, len(head))
	for _, r := range rows {
		for i, c := range r {
			if w := cellWidth(c); w > widths[i] {
				widths[i] = w
			}
		}
	}
	for _, r := range rows {
		var b strings.Builder
		for i, c := range r {
			if i > 0 {
				b.WriteByte(' ')
			}
			if i >= 2 { // числовые колонки — вправо
				fmt.Fprintf(&b, "%*s", widths[i], c)
			} else {
				fmt.Fprintf(&b, "%-*s", widths[i], c)
			}
		}
		out(w, "%s\n", b.String())
	}

	out(w, "\nочереди (глубина): ")
	if lens == nil {
		out(w, "н/д (Redis недоступен)\n")
	} else {
		parts := make([]string, 0, len(status.Streams))
		for _, s := range status.Streams {
			parts = append(parts, fmt.Sprintf("%s %d", s, lens[s]))
		}
		out(w, "%s\n", strings.Join(parts, "   "))
	}

	out(w, "\nскорость:\n")
	if rates.DomainsKnown {
		out(w, "  доменов/с          %.1f   (%d проверено с начала суток, прошло %s)\n",
			rates.DomainsPerSec, thr.DoneToday, status.FormatDuration(int64(thr.ElapsedSec)))
	} else {
		out(w, "  доменов/с          н/д   (окно с начала суток короче %s)\n", status.FormatDuration(status.MinSpeedWindowSec))
	}
	// Окно темпа результатов/рендеров — корзины календарных часов
	// (T-203): фактическая длина покрытого окна 1–2 ч печатается рядом —
	// «N за окно» без длины вводило бы в заблуждение скользящим «часом»;
	// нулевое окно (ручной сбор) — фиксированный час, тот же фолбэк, что
	// ComputeRates.
	window := thr.HourWindowSec
	if window <= 0 {
		window = 3600
	}
	out(w, "  HTTP-результатов/с %.1f   (%d за окно темпа %s)\n",
		rates.ResultsPerSec, thr.ResultsHour, status.FormatDuration(int64(window)))
	out(w, "  рендеров/с         %.1f   (%d с итоговым DOM за то же окно)\n",
		rates.RenderPerSec, thr.RenderedHour)

	out(w, "\n")
	switch {
	case rates.Remaining == 0:
		out(w, "цикл: осталось 0 (pending и queued пусты) — проверять нечего\n")
	case !rates.ETAKnown:
		out(w, "цикл: осталось %d (pending+queued); ETA н/д (скорость неизвестна или 0)\n", rates.Remaining)
	default:
		out(w, "цикл: осталось %d (pending+queued); ETA ≈ %s (по %.1f дом./с)\n",
			rates.Remaining, status.FormatDuration(rates.ETASec), rates.DomainsPerSec)
	}
}

// writeStatusJSON — машинный вывод (стиль domain --json): те же данные,
// что в таблице; «н/д» — null (скорость без окна, ETA без скорости,
// очереди без Redis). Wire-формат — status.BuildSummaryJSON (ТЗД §6.2,
// T-129): тот же контракт полей отдаёт /api/status дашборда.
func writeStatusJSON(w io.Writer, zones []status.ZoneStats, tot status.Totals, thr status.Throughput, rates status.Rates, lens map[string]int64) error {
	doc := status.BuildSummaryJSON(zones, tot, thr, rates, lens)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc.Encode(doc)
}

// ---------------------------------------------------------------- команда

// cmdStatus — crawlerctl status: сборка сводки из БД и Redis и вывод.
func cmdStatus(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("status")
	asJSON := fs.Bool("json", false, "машинный вывод (для скриптов)")
	if handled, err := parseFlags(fs, statusHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() > 0 {
		return usagef("status: лишние аргументы %v (сводка берётся целиком, без позиционных аргументов)", fs.Args())
	}

	// Конфигурация — единый YAML + окружение (internal/config): DSN
	// PostgreSQL и адрес Redis (стиль reanalyze, T-074).
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("status: %w", err)
	}
	// Ctrl-C/SIGTERM отменяют тяжёлые агрегаты (полные проходы больших
	// таблиц) — единый хелпер команд (signalCtx).
	ctx, stop := signalCtx()
	defer stop()

	// CLI pool_size = 2 — бюджет соединений ТЗ 6.3 у сервисов (стиль
	// manage.go, T-074).
	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2
	poolOpts.AppName = "crawlerctl" // метка в pg_stat_activity (T-177)
	pool, err := db.Open(ctx, poolOpts)
	if err != nil {
		return fmt.Errorf("status: %w", err)
	}
	defer pool.Close()

	zones, err := status.FetchZoneStats(ctx, pool)
	if err != nil {
		return err
	}
	thr, err := status.FetchThroughput(ctx, pool)
	if err != nil {
		return err
	}
	tot := status.ComputeTotals(zones)
	rates := status.ComputeRates(thr, tot.Pending, tot.Queued)

	// Глубина очередей — через гейт backpressure (T-026): узкий API
	// Len над go-redis; порог гейта здесь не участвует. Недоступный Redis
	// не прячет сводку по БД: печатаем с «н/д», завершаемся кодом 1.
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	lens, qErr := status.FetchQueueLens(ctx, queue.NewBackpressureGate(rdb, queue.BackpressureOptions{}))

	if *asJSON {
		if err := writeStatusJSON(stdout, zones, tot, thr, rates, lens); err != nil {
			return fmt.Errorf("status: вывод json: %w", err)
		}
	} else {
		writeStatusTable(stdout, zones, tot, thr, rates, lens)
	}
	if qErr != nil {
		return fmt.Errorf("status: очереди: %w", qErr)
	}
	return nil
}

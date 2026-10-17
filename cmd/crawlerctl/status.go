// ---------------------------------------------------------------- status
//
// crawlerctl status (T-079, ТЗ 5.9): сводка по зонам, очередям, скорости
// и ожидаемому времени завершения первого цикла проверок. Источники —
// PostgreSQL (счётчики zones + агрегаты domains + окна темпа) и Redis
// (длины стримов, XLEN); всё читается разово, без состояния.
//
// Что измеримо из БД (решения зафиксированы, остальное — честно не
// показывается, ТЗ 5.9 «скорость… ожидаемое время завершения цикла»):
//
//   - прогресс зон — счётчики zones.domains_total/domains_checked (их
//     обновляет сервис monitor, ТЗ 5.9) плюс живой агрегат domains по
//     check_state: pending без учёта deleted (удалённый домен планировщик
//     не выбирает, T-028 — ждать его проверки нечего), queued и done без
//     фильтра статуса;
//   - скорость доменов — средняя с начала суток: last_checked_at имеет
//     точность дня (date, ТЗ 6.1), часового окна нет; короче 10 минут
//     окно не используется (сразу после полуночи оценка скачет);
//   - HTTP-результаты и рендеры — за последний час по checked_at
//     (timestamptz пишет http-worker, T-058); собственной отметки времени
//     у рендера нет — считается доля rendered среди результатов окна;
//   - темп DNS-этапа отдельно и этапа анализа из БД не измерим (нет
//     отметок времени; завершение DNS+HTTP и есть «доменов/с») — их
//     метрики в Prometheus (T-076/T-077);
//   - ETA первого цикла = (pending + queued) / скорость доменов, защита
//     от деления на ноль — скорости нет или она 0 → «н/д».
//
// Недоступный Redis не прячет сводку по БД: секция очередей печатается с
// «н/д», команда завершается кодом 1. Агрегаты и окна — полные проходы
// таблиц (last_checked_at/checked_at не индексируются, ТЗ 6): для разовой
// диагностики допустимо, сервисы так не опрашивают.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"crawler/internal/config"
	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// statusHelp — справка crawlerctl status --help.
const statusHelp = `crawlerctl status — сводка по зонам, очередям, скорости (ТЗ 5.9)

Использование:
  crawlerctl status [--json]

Сводка: по каждой зоне — счётчики domains_total/domains_checked (обновляет
monitor) и агрегат domains по check_state (pending без учёта deleted,
queued, done); длины очередей q:dns/q:http/q:render/q:analyze/q:dead
(XLEN); скорость проверок (доменов/с — средняя с начала суток:
last_checked_at имеет точность дня; HTTP-результаты и рендеры — за
последний час по subdomain_results.checked_at) и ETA завершения первого
цикла: (pending + queued) / скорость. Темп этапов DNS и анализа из БД не
измерим — их видно в Prometheus. --json — машинный вывод. Команда работает
на стеке dev-окружения (PostgreSQL/Redis); при недоступном Redis сводка
по БД печатается, код возврата — 1.
Коды возврата: 0 — успех, 1 — ошибка, 2 — использование.
`

// ---------------------------------------------------------------- SQL

// zoneStatsSQL — сводка по зонам одним проходом: счётчики zones +
// агрегат domains по check_state (LEFT JOIN — зона без доменов остаётся
// в сводке). Коды check_state/status — константы model, вшитые в текст
// (стиль scheduler/dispatch.go); count(d.id) игнорирует строку зоны без
// доменов, FILTER — NULL-колонки непрошедших условий LEFT JOIN.
var zoneStatsSQL = fmt.Sprintf(
	"SELECT z.id, z.name, coalesce(z.name_unicode, '') AS name_unicode, "+
		"z.is_active, z.domains_total, z.domains_checked, "+
		"count(d.id) FILTER (WHERE d.check_state = %d AND d.status <> %d) AS pending, "+
		"count(d.id) FILTER (WHERE d.check_state = %d) AS queued, "+
		"count(d.id) FILTER (WHERE d.check_state = %d) AS done "+
		"FROM zones AS z LEFT JOIN domains AS d ON d.zone_id = z.id "+
		"GROUP BY z.id ORDER BY z.name",
	int16(model.CheckStatePending), int16(model.StatusDeleted),
	int16(model.CheckStateQueued), int16(model.CheckStateDone))

// throughputSQL — замеры темпа одним запросом. now() — время транзакции:
// оба интервала согласованы внутри одного выполнения; current_date и
// date_trunc — одна таймзона сервера БД, как и now() в записи
// last_checked_at (T-058). done_today считает проверки, а не домены:
// повторные проверки одного домена за сутки сходятся в «темп работы».
const throughputSQL = "SELECT " +
	"(SELECT count(*) FROM domains WHERE last_checked_at = current_date) AS done_today, " +
	"extract(epoch FROM (now() - date_trunc('day', now())))::float8 AS elapsed_sec, " +
	"(SELECT count(*) FROM subdomain_results WHERE checked_at >= now() - interval '1 hour') AS results_hour, " +
	"(SELECT count(*) FROM subdomain_results WHERE checked_at >= now() - interval '1 hour' AND rendered) AS rendered_hour"

// ---------------------------------------------------------------- данные

// zoneStats — строка сводки зоны.
type zoneStats struct {
	ID             int16
	Name           string // punycode — каноническая форма (ТЗ 5.2)
	NameUnicode    string // "" — NULL; только отображение (в JSON)
	IsActive       bool
	DomainsTotal   int64 // счётчик zones.domains_total (обновляет monitor)
	DomainsChecked int64 // счётчик zones.domains_checked (обновляет monitor)
	Pending        int64 // агрегат domains: ожидает выборки (без deleted)
	Queued         int64 // в работе (lease)
	Done           int64 // DNS/HTTP-этап завершён (ТЗ 7)
}

// throughput — замеры темпа из БД (одно выполнение throughputSQL).
type throughput struct {
	DoneToday    int64   // проверок доменов с начала суток (last_checked_at)
	ElapsedSec   float64 // прошло с начала суток по часам БД
	ResultsHour  int64   // HTTP-результатов за последний час (checked_at)
	RenderedHour int64   // из них с итоговым DOM (rendered)
}

// fetchZoneStats выполняет zoneStatsSQL и собирает строки зон.
func fetchZoneStats(ctx context.Context, pool db.DB) ([]zoneStats, error) {
	if pool == nil {
		return nil, errors.New("status: зоны: пул не задан")
	}
	rows, err := pool.Query(ctx, zoneStatsSQL)
	if err != nil {
		return nil, fmt.Errorf("status: зоны: %w", err)
	}
	defer rows.Close()
	var zones []zoneStats
	for rows.Next() {
		var z zoneStats
		if err := rows.Scan(&z.ID, &z.Name, &z.NameUnicode, &z.IsActive,
			&z.DomainsTotal, &z.DomainsChecked, &z.Pending, &z.Queued, &z.Done); err != nil {
			return nil, fmt.Errorf("status: зоны: чтение: %w", err)
		}
		zones = append(zones, z)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("status: зоны: %w", err)
	}
	return zones, nil
}

// fetchThroughput выполняет throughputSQL (ровно одна строка — скалярный
// SELECT без FROM).
func fetchThroughput(ctx context.Context, pool db.DB) (throughput, error) {
	if pool == nil {
		return throughput{}, errors.New("status: темп: пул не задан")
	}
	rows, err := pool.Query(ctx, throughputSQL)
	if err != nil {
		return throughput{}, fmt.Errorf("status: темп: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return throughput{}, errors.New("status: темп: пустой ответ")
	}
	var t throughput
	if err := rows.Scan(&t.DoneToday, &t.ElapsedSec, &t.ResultsHour, &t.RenderedHour); err != nil {
		return throughput{}, fmt.Errorf("status: темп: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return throughput{}, fmt.Errorf("status: темп: %w", err)
	}
	return t, nil
}

// ---------------------------------------------------------------- очереди

// streamLener — длины стримов (XLEN); *queue.BackpressureGate (T-026)
// удовлетворяет интерфейсу. Отдельный интерфейс, а не go-redis напрямую:
// тесты подменяют фейком, транспорт заменяем (ТЗ 4.2).
type streamLener interface {
	Len(ctx context.Context, stream string) (int64, error)
}

// statusStreams — рабочие стримы очереди в порядке вывода (ТЗ 4.2).
var statusStreams = []string{
	queue.StreamDNS, queue.StreamHTTP, queue.StreamRender, queue.StreamAnalyze, queue.StreamDead,
}

// fetchQueueLens собирает длины очередей (XLEN). Ошибка любого стрима
// останавливает сбор: Redis один, повторные обращения к недоступному
// серверу бессмысленны.
func fetchQueueLens(ctx context.Context, lenner streamLener) (map[string]int64, error) {
	if lenner == nil {
		return nil, errors.New("status: очереди: клиент не задан")
	}
	lens := make(map[string]int64, len(statusStreams))
	for _, s := range statusStreams {
		n, err := lenner.Len(ctx, s)
		if err != nil {
			return nil, err
		}
		lens[s] = n
	}
	return lens, nil
}

// ---------------------------------------------------------------- расчёты

const (
	// minSpeedWindowSec — минимальное окно оценки скорости доменов: сразу
	// после полуночи среднее «с начала суток» нестабильно (окно в минуты
	// при тысячах проверок в секунду, ТЗ 3.3).
	minSpeedWindowSec = 600
	// speedWindowSec — фиксированное окно часовых счётчиков (checked_at).
	speedWindowSec = 3600.0
)

// statusTotals — суммы по зонам (итого таблицы и вход расчёта ETA).
type statusTotals struct {
	DomainsTotal   int64
	DomainsChecked int64
	Pending        int64
	Queued         int64
	Done           int64
}

// computeTotals суммирует строки зон.
func computeTotals(zones []zoneStats) statusTotals {
	var t statusTotals
	for _, z := range zones {
		t.DomainsTotal += z.DomainsTotal
		t.DomainsChecked += z.DomainsChecked
		t.Pending += z.Pending
		t.Queued += z.Queued
		t.Done += z.Done
	}
	return t
}

// statusRates — производные скорости/ETA. Чистая функция от замеров:
// короткие окна и нулевые скорости не дают «н/д»-оценок (защита от
// деления на ноль — флаги Known, а не паника).
type statusRates struct {
	DomainsPerSec float64 // средняя с начала суток
	DomainsKnown  bool    // окно достаточно (≥ minSpeedWindowSec)
	ResultsPerSec float64 // HTTP-результатов/с за час
	RenderPerSec  float64 // рендеров/с за час
	Remaining     int64   // pending + queued — объём первого цикла
	ETASec        int64
	ETAKnown      bool
}

// computeRates выводит скорости и ETA первого цикла. Пустой цикл
// (нечего проверять) завершён независимо от скорости;ETA при остатке
// не меньше секунды — остаток в одну домену не должен выглядеть «0с».
func computeRates(t throughput, pending, queued int64) statusRates {
	r := statusRates{
		ResultsPerSec: float64(t.ResultsHour) / speedWindowSec,
		RenderPerSec:  float64(t.RenderedHour) / speedWindowSec,
		Remaining:     pending + queued,
	}
	if r.Remaining == 0 {
		r.ETAKnown, r.ETASec = true, 0
	}
	if t.ElapsedSec >= minSpeedWindowSec {
		r.DomainsKnown = true
		r.DomainsPerSec = float64(t.DoneToday) / t.ElapsedSec
		if r.Remaining > 0 && r.DomainsPerSec > 0 {
			r.ETASec = int64(math.Round(float64(r.Remaining) / r.DomainsPerSec))
			if r.ETASec < 1 {
				r.ETASec = 1
			}
			r.ETAKnown = true
		}
	}
	return r
}

// ---------------------------------------------------------------- вывод

// formatDuration — округлый человекочитаемый интервал: «45с», «13м»,
// «5ч 20м», «3д 4ч» (сутки — потолок единиц: недели скрывают масштаб
// хуже, чем трёхзначные дни).
func formatDuration(sec int64) string {
	switch {
	case sec < 60:
		return fmt.Sprintf("%dс", sec)
	case sec < 3600:
		return fmt.Sprintf("%dм", (sec+30)/60)
	case sec < 86400:
		return fmt.Sprintf("%dч %02dм", sec/3600, (sec%3600)/60)
	default:
		return fmt.Sprintf("%dд %dч", sec/86400, (sec%86400)/3600)
	}
}

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
func writeStatusTable(w io.Writer, zones []zoneStats, tot statusTotals, thr throughput, rates statusRates, lens map[string]int64) {
	out(w, "зоны: всего/проверено — счётчики zones (обновляет monitor, ТЗ 5.9);\n"+
		"pending/queued/done — агрегат domains по check_state (deleted не ждёт проверки)\n\n")

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
			if len(c) > widths[i] {
				widths[i] = len(c)
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

	out(w, "\nочереди (XLEN): ")
	if lens == nil {
		out(w, "н/д (Redis недоступен)\n")
	} else {
		parts := make([]string, 0, len(statusStreams))
		for _, s := range statusStreams {
			parts = append(parts, fmt.Sprintf("%s %d", s, lens[s]))
		}
		out(w, "%s\n", strings.Join(parts, "   "))
	}

	out(w, "\nскорость:\n")
	if rates.DomainsKnown {
		out(w, "  доменов/с          %.1f   (%d проверено с начала суток, прошло %s)\n",
			rates.DomainsPerSec, thr.DoneToday, formatDuration(int64(thr.ElapsedSec)))
	} else {
		out(w, "  доменов/с          н/д   (окно с начала суток короче %s)\n", formatDuration(minSpeedWindowSec))
	}
	out(w, "  HTTP-результатов/с %.1f   (%d за последний час)\n", rates.ResultsPerSec, thr.ResultsHour)
	out(w, "  рендеров/с         %.1f   (%d с итоговым DOM за час)\n", rates.RenderPerSec, thr.RenderedHour)

	out(w, "\n")
	switch {
	case rates.Remaining == 0:
		out(w, "цикл: осталось 0 (pending и queued пусты) — проверять нечего\n")
	case !rates.ETAKnown:
		out(w, "цикл: осталось %d (pending+queued); ETA н/д (скорость неизвестна или 0)\n", rates.Remaining)
	default:
		out(w, "цикл: осталось %d (pending+queued); ETA ≈ %s (по %.1f дом./с)\n",
			rates.Remaining, formatDuration(rates.ETASec), rates.DomainsPerSec)
	}
}

// writeStatusJSON — машинный вывод (стиль domain --json): те же данные,
// что в таблице; «н/д» — null (скорость без окна, ETA без скорости,
// очереди без Redis).
func writeStatusJSON(w io.Writer, zones []zoneStats, tot statusTotals, thr throughput, rates statusRates, lens map[string]int64) error {
	type zoneJSON struct {
		Name           string `json:"name"`
		NameUnicode    string `json:"name_unicode,omitempty"`
		Active         bool   `json:"active"`
		DomainsTotal   int64  `json:"domains_total"`
		DomainsChecked int64  `json:"domains_checked"`
		Pending        int64  `json:"pending"`
		Queued         int64  `json:"queued"`
		Done           int64  `json:"done"`
	}
	type totalsJSON struct {
		DomainsTotal   int64 `json:"domains_total"`
		DomainsChecked int64 `json:"domains_checked"`
		Pending        int64 `json:"pending"`
		Queued         int64 `json:"queued"`
		Done           int64 `json:"done"`
	}
	type throughputJSON struct {
		DoneToday    int64   `json:"done_today"`
		ElapsedSec   float64 `json:"elapsed_sec"`
		ResultsHour  int64   `json:"results_hour"`
		RenderedHour int64   `json:"rendered_hour"`
	}
	type ratesJSON struct {
		DomainsPerSec *float64 `json:"domains_per_sec"`
		ResultsPerSec float64  `json:"results_per_sec"`
		RendersPerSec float64  `json:"renders_per_sec"`
		Remaining     int64    `json:"remaining"`
		ETASec        *int64   `json:"eta_sec"`
	}
	round3 := func(v float64) *float64 {
		r := math.Round(v*1000) / 1000
		return &r
	}
	var doc struct {
		Zones      []zoneJSON       `json:"zones"`
		Totals     totalsJSON       `json:"totals"`
		Queues     map[string]int64 `json:"queues"`
		Throughput throughputJSON   `json:"throughput"`
		Rates      ratesJSON        `json:"rates"`
	}
	doc.Zones = make([]zoneJSON, 0, len(zones))
	for _, z := range zones {
		doc.Zones = append(doc.Zones, zoneJSON{
			Name: z.Name, NameUnicode: z.NameUnicode, Active: z.IsActive,
			DomainsTotal: z.DomainsTotal, DomainsChecked: z.DomainsChecked,
			Pending: z.Pending, Queued: z.Queued, Done: z.Done,
		})
	}
	doc.Totals = totalsJSON(tot)
	doc.Queues = lens // nil → null (Redis недоступен)
	doc.Throughput = throughputJSON(thr)
	doc.Rates = ratesJSON{ResultsPerSec: math.Round(rates.ResultsPerSec*1000) / 1000,
		RendersPerSec: math.Round(rates.RenderPerSec*1000) / 1000, Remaining: rates.Remaining}
	if rates.DomainsKnown {
		doc.Rates.DomainsPerSec = round3(rates.DomainsPerSec)
	}
	if rates.ETAKnown {
		eta := rates.ETASec
		doc.Rates.ETASec = &eta
	}
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
	ctx := context.Background()

	// CLI pool_size = 2 — бюджет соединений ТЗ 6.3 у сервисов (стиль
	// manage.go, T-074).
	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2
	pool, err := db.Open(ctx, poolOpts)
	if err != nil {
		return fmt.Errorf("status: %w", err)
	}
	defer pool.Close()

	zones, err := fetchZoneStats(ctx, pool)
	if err != nil {
		return err
	}
	thr, err := fetchThroughput(ctx, pool)
	if err != nil {
		return err
	}
	tot := computeTotals(zones)
	rates := computeRates(thr, tot.Pending, tot.Queued)

	// Длины очередей — XLEN через гейт backpressure (T-026): узкий API
	// Len над go-redis; порог гейта здесь не участвует. Недоступный Redis
	// не прячет сводку по БД: печатаем с «н/д», завершаемся кодом 1.
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	lens, qErr := fetchQueueLens(ctx, queue.NewBackpressureGate(rdb, queue.BackpressureOptions{}))

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

// Канонический JSON-формат сводки (ТЗД §6.2, T-129): wire-контракт полей
// зон/итогов/очередей/темпа/скоростей, общий для CLI `crawlerctl status
// --json` и снимка дашборда /api/status — один источник имён полей и
// правил округления, чтобы машинные потребители не расходились (заметка
// ревью T-126). До T-129 DTO жили внутри writeStatusJSON CLI; дашборд
// сделал расхождение реальным — типы подняты в общую библиотеку сводки.
//
// «н/д» — null: скорости без окна (domains_per_sec), ETA без скорости
// (eta_sec), очереди без Redis (queues — nil-карта). Секции целиком
// (Totals/Throughput/Rates) — указатели: дашборд кладёт nil при ошибке
// секции (ТЗД §5.9), CLI всегда собирает их полностью.
package status

import (
	"math"
)

// ZoneJSON — строка зоны (§5.3): имена полей CLI --json.
type ZoneJSON struct {
	Name           string `json:"name"`
	NameUnicode    string `json:"name_unicode,omitempty"`
	Active         bool   `json:"active"`
	DomainsTotal   int64  `json:"domains_total"`
	DomainsChecked int64  `json:"domains_checked"`
	Pending        int64  `json:"pending"`
	Queued         int64  `json:"queued"`
	Done           int64  `json:"done"`
}

// TotalsJSON — итоги по зонам (§5.1); поле-в-поле Totals.
type TotalsJSON struct {
	DomainsTotal   int64 `json:"domains_total"`
	DomainsChecked int64 `json:"domains_checked"`
	Pending        int64 `json:"pending"`
	Queued         int64 `json:"queued"`
	Done           int64 `json:"done"`
}

// ThroughputJSON — замеры темпа (§5.2); поле-в-поле Throughput.
type ThroughputJSON struct {
	DoneToday     int64   `json:"done_today"`
	ElapsedSec    float64 `json:"elapsed_sec"`
	ResultsHour   int64   `json:"results_hour"`
	RenderedHour  int64   `json:"rendered_hour"`
	HourWindowSec float64 `json:"hour_window_sec"`
}

// RatesJSON — производные скорости/ETA (§5.1–5.2); Known-флаги Rates
// уходят в null-поля (нет окна → domains_per_sec null, нет скорости →
// eta_sec null).
type RatesJSON struct {
	DomainsPerSec *float64 `json:"domains_per_sec"`
	ResultsPerSec float64  `json:"results_per_sec"`
	RendersPerSec float64  `json:"renders_per_sec"`
	Remaining     int64    `json:"remaining"`
	ETASec        *int64   `json:"eta_sec"`
}

// SummaryJSON — общие секции сводки (§5.1–5.4): зоны, итоги, очереди,
// темп, скорости. Дашборд встраивает структуру в снимок (поля
// поднимаются на верхний уровень JSON), CLI отдаёт как есть.
type SummaryJSON struct {
	Zones      []ZoneJSON       `json:"zones"`
	Totals     *TotalsJSON      `json:"totals"`
	Queues     map[string]int64 `json:"queues"`
	Throughput *ThroughputJSON  `json:"throughput"`
	Rates      *RatesJSON       `json:"rates"`
}

// ZonesToJSON — строки зон в wire-формат; пустой срез остаётся пустым
// («[]», не null) — зоны есть, просто их ноль.
func ZonesToJSON(zones []ZoneStats) []ZoneJSON {
	out := make([]ZoneJSON, 0, len(zones))
	for _, z := range zones {
		out = append(out, ZoneJSON{
			Name: z.Name, NameUnicode: z.NameUnicode, Active: z.IsActive,
			DomainsTotal: z.DomainsTotal, DomainsChecked: z.DomainsChecked,
			Pending: z.Pending, Queued: z.Queued, Done: z.Done,
		})
	}
	return out
}

// TotalsToJSON — итоги в wire-формат.
func TotalsToJSON(t Totals) TotalsJSON {
	return TotalsJSON(t)
}

// ThroughputToJSON — замеры темпа в wire-формат.
func ThroughputToJSON(t Throughput) ThroughputJSON {
	return ThroughputJSON(t)
}

// RatesToJSON — скорости/ETA в wire-формат с округлением до трёх знаков
// (как таблица CLI печатает один знак, машинный вывод — три).
func RatesToJSON(r Rates) RatesJSON {
	out := RatesJSON{
		ResultsPerSec: round3(r.ResultsPerSec),
		RendersPerSec: round3(r.RenderPerSec),
		Remaining:     r.Remaining,
	}
	if r.DomainsKnown {
		out.DomainsPerSec = ptrOf(round3(r.DomainsPerSec))
	}
	if r.ETAKnown {
		out.ETASec = ptrOf(r.ETASec)
	}
	return out
}

// BuildSummaryJSON — полный wire-документ CLI status --json: все секции
// собраны (nil-карта очередей остаётся null). Дашборд собирает SummaryJSON
// по секциям сам (nil-секции при ошибках, ТЗД §5.9).
func BuildSummaryJSON(zones []ZoneStats, tot Totals, thr Throughput, rates Rates, lens map[string]int64) SummaryJSON {
	t, th := TotalsToJSON(tot), ThroughputToJSON(thr)
	return SummaryJSON{
		Zones:      ZonesToJSON(zones),
		Totals:     &t,
		Queues:     lens, // nil → null (Redis недоступен)
		Throughput: &th,
		Rates:      ptrOf(RatesToJSON(rates)),
	}
}

// round3 — округление float до трёх знаков.
func round3(v float64) float64 {
	return math.Round(v*1000) / 1000
}

// ptrOf — адрес значения (композиция указательных полей SummaryJSON).
func ptrOf[T any](v T) *T {
	return &v
}

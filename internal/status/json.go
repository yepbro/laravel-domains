// The canonical JSON format of the summary (dashboard spec §6.2, T-129):
// the wire contract of the zones/totals/queues/throughput/rates fields,
// shared by the CLI `crawlerctl status --json` and the dashboard snapshot
// /api/status — a single source of the field names and the rounding
// rules, so that the machine consumers do not diverge (the T-126 review
// note). Before T-129 the DTOs lived inside the writeStatusJSON of the
// CLI; the dashboard made the divergence real — the types were lifted
// into the shared summary library.
//
// "n/a" — null: the rates without a window (domains_per_sec), the ETA
// without a rate (eta_sec), the queues without Redis (queues — a nil
// map). The sections as a whole (Totals/Throughput/Rates) are pointers:
// the dashboard puts nil on a section error (dashboard spec §5.9), the
// CLI always assembles them fully.
package status

import (
	"math"
)

// ZoneJSON — a zone row (§5.3): the field names of the CLI --json.
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

// TotalsJSON — the totals over the zones (§5.1); field-by-field Totals.
type TotalsJSON struct {
	DomainsTotal   int64 `json:"domains_total"`
	DomainsChecked int64 `json:"domains_checked"`
	Pending        int64 `json:"pending"`
	Queued         int64 `json:"queued"`
	Done           int64 `json:"done"`
}

// ThroughputJSON — the throughput measurements (§5.2); field-by-field
// Throughput.
type ThroughputJSON struct {
	DoneToday     int64   `json:"done_today"`
	ElapsedSec    float64 `json:"elapsed_sec"`
	ResultsHour   int64   `json:"results_hour"`
	RenderedHour  int64   `json:"rendered_hour"`
	HourWindowSec float64 `json:"hour_window_sec"`
}

// RatesJSON — the derived rates/ETA (§5.1–5.2); the Known flags of Rates
// go into the null fields (no window → domains_per_sec null, no rate →
// eta_sec null).
type RatesJSON struct {
	DomainsPerSec *float64 `json:"domains_per_sec"`
	ResultsPerSec float64  `json:"results_per_sec"`
	RendersPerSec float64  `json:"renders_per_sec"`
	Remaining     int64    `json:"remaining"`
	ETASec        *int64   `json:"eta_sec"`
}

// SummaryJSON — the common summary sections (§5.1–5.4): the zones, the
// totals, the queues, the throughput, the rates. The dashboard embeds the
// struct into the snapshot (the fields are lifted to the top level of the
// JSON), the CLI hands it out as is.
type SummaryJSON struct {
	Zones      []ZoneJSON       `json:"zones"`
	Totals     *TotalsJSON      `json:"totals"`
	Queues     map[string]int64 `json:"queues"`
	Throughput *ThroughputJSON  `json:"throughput"`
	Rates      *RatesJSON       `json:"rates"`
}

// ZonesToJSON — the zone rows into the wire format; an empty slice stays
// empty ("[]", not null) — there are zones, just zero of them.
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

// TotalsToJSON — the totals into the wire format.
func TotalsToJSON(t Totals) TotalsJSON {
	return TotalsJSON(t)
}

// ThroughputToJSON — the throughput measurements into the wire format.
func ThroughputToJSON(t Throughput) ThroughputJSON {
	return ThroughputJSON(t)
}

// RatesToJSON — the rates/ETA into the wire format, rounded to three
// digits (the CLI table prints one digit, the machine output — three).
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

// BuildSummaryJSON — the full wire document of the CLI status --json: all
// the sections are assembled (a nil queue map stays null). The dashboard
// assembles SummaryJSON by sections itself (nil sections on errors,
// dashboard spec §5.9).
func BuildSummaryJSON(zones []ZoneStats, tot Totals, thr Throughput, rates Rates, lens map[string]int64) SummaryJSON {
	t, th := TotalsToJSON(tot), ThroughputToJSON(thr)
	return SummaryJSON{
		Zones:      ZonesToJSON(zones),
		Totals:     &t,
		Queues:     lens, // nil → null (Redis unavailable)
		Throughput: &th,
		Rates:      ptrOf(RatesToJSON(rates)),
	}
}

// round3 — rounding a float to three digits.
func round3(v float64) float64 {
	return math.Round(v*1000) / 1000
}

// ptrOf — the address of the value (the composition of the SummaryJSON
// pointer fields).
func ptrOf[T any](v T) *T {
	return &v
}

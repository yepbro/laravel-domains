// ---------------------------------------------------------------- status
//
// crawlerctl status (T-079, spec 5.9): prints a summary of zones, queues,
// check rate and the expected completion time of the first check cycle.
// The data and the calculations live in the internal/status package
// (dashboard spec §4.5, T-126): the shared summary library of the CLI and
// the dashboard, a single source of truth about the meaning of the numbers;
// the SQL texts, the selections and the derived values are there, and this
// file only formats (table/JSON) and wires the command to the
// infrastructure (configuration, DB pool, Redis).
//
// An unavailable Redis does not hide the DB summary: the queues section is
// printed with "n/a" and the command exits with code 1.

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

// statusHelp — the crawlerctl status --help text.
const statusHelp = `crawlerctl status — a summary over zones, queues, rates (spec 5.9)

Usage:
  crawlerctl status [--json]

The summary: per zone — the domains_total/domains_checked counters (updated
by monitor) and the domains aggregate over check_state (pending not counting
deleted, queued, done); the depths of the q:dns/q:http/q:render/q:render-fallback/
q:analyze/q:dead queues (XLEN); the check rate (domains/s — the average since
the start of the day: last_checked_at has day precision; the HTTP results
and renders — over the 1–2 h rate window: the crawler_counters hourly
buckets, T-203) and the ETA of the first cycle's completion:
(pending + queued) / rate. The DNS and analysis stage rates are not
measurable from the DB — their metrics are in Prometheus. --json — machine
output. The command runs on the dev environment stack (PostgreSQL/Redis);
with Redis unavailable the DB summary is still printed, the exit code is 1.
Exit codes: 0 — success, 1 — error, 2 — usage.
`

// ---------------------------------------------------------------- output

// actMark — the "act" column: the zone participates in checks.
func actMark(active bool) string {
	if active {
		return "+"
	}
	return "-"
}

// percentMark — the checked share by the zone counters; "—" for an empty zone.
func percentMark(checked, total int64) string {
	if total <= 0 {
		return "—"
	}
	return fmt.Sprintf("%.0f%%", float64(checked)*100/float64(total))
}

// writeStatusTable — a human-readable summary: a monospace zone table
// (text columns left-aligned, numbers right-aligned), queues, rate, cycle.
// lens == nil — Redis is unavailable, queues are "n/a".
func writeStatusTable(w io.Writer, zones []status.ZoneStats, tot status.Totals, thr status.Throughput, rates status.Rates, lens map[string]int64) {
	out(w, "zones: total/checked/pending/queued/done — the incremental counters of zones\n"+
		"(transition deltas + the monitor reconciliation, T-178; deleted is outside the base set)\n\n")

	// Columns: zone and act — text, the rest — numbers (right-aligned).
	// The zone name is punycode (the canonical form, like the
	// incoming/<zone> directory); the unicode form — in --json.
	head := []string{"zone", "act", "total", "checked", "%", "pending", "queued", "done"}
	rows := [][]string{head}
	row := func(cells ...string) []string { return cells }
	for _, z := range zones {
		rows = append(rows, row(z.Name, actMark(z.IsActive),
			strconv.FormatInt(z.DomainsTotal, 10), strconv.FormatInt(z.DomainsChecked, 10),
			percentMark(z.DomainsChecked, z.DomainsTotal),
			strconv.FormatInt(z.Pending, 10), strconv.FormatInt(z.Queued, 10), strconv.FormatInt(z.Done, 10)))
	}
	rows = append(rows, row("total", "",
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
			if i >= 2 { // numeric columns — right-aligned
				fmt.Fprintf(&b, "%*s", widths[i], c)
			} else {
				fmt.Fprintf(&b, "%-*s", widths[i], c)
			}
		}
		out(w, "%s\n", b.String())
	}

	out(w, "\nqueues (depth): ")
	if lens == nil {
		out(w, "n/a (Redis unavailable)\n")
	} else {
		parts := make([]string, 0, len(status.Streams))
		for _, s := range status.Streams {
			parts = append(parts, fmt.Sprintf("%s %d", s, lens[s]))
		}
		out(w, "%s\n", strings.Join(parts, "   "))
	}

	out(w, "\nrates:\n")
	if rates.DomainsKnown {
		out(w, "  domains/s          %.1f   (%d checked since the start of the day, elapsed %s)\n",
			rates.DomainsPerSec, thr.DoneToday, status.FormatDuration(int64(thr.ElapsedSec)))
	} else {
		out(w, "  domains/s          n/a   (the window since the start of the day is shorter than %s)\n", status.FormatDuration(status.MinSpeedWindowSec))
	}
	// The results/renders rate window — calendar hour buckets (T-203): the
	// actual length of the covered 1-2 h window is printed next to it —
	// "N per window" without the length would mislead with a sliding
	// "hour"; a zero window (manual collection) — a fixed hour, the same
	// fallback as ComputeRates.
	window := thr.HourWindowSec
	if window <= 0 {
		window = 3600
	}
	out(w, "  HTTP results/s     %.1f   (%d over the rate window %s)\n",
		rates.ResultsPerSec, thr.ResultsHour, status.FormatDuration(int64(window)))
	out(w, "  renders/s          %.1f   (%d with a final DOM over the same window)\n",
		rates.RenderPerSec, thr.RenderedHour)

	out(w, "\n")
	switch {
	case rates.Remaining == 0:
		out(w, "cycle: 0 remaining (pending and queued are empty) — nothing to check\n")
	case !rates.ETAKnown:
		out(w, "cycle: %d remaining (pending+queued); ETA n/a (the rate is unknown or 0)\n", rates.Remaining)
	default:
		out(w, "cycle: %d remaining (pending+queued); ETA ≈ %s (at %.1f domains/s)\n",
			rates.Remaining, status.FormatDuration(rates.ETASec), rates.DomainsPerSec)
	}
}

// writeStatusJSON — machine output (the domain --json style): the same
// data as in the table; "n/a" — null (a rate without a window, an ETA
// without a rate, queues without Redis). The wire format —
// status.BuildSummaryJSON (dashboard spec §6.2, T-129): /api/status of the
// dashboard serves the same field contract.
func writeStatusJSON(w io.Writer, zones []status.ZoneStats, tot status.Totals, thr status.Throughput, rates status.Rates, lens map[string]int64) error {
	doc := status.BuildSummaryJSON(zones, tot, thr, rates, lens)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc.Encode(doc)
}

// ---------------------------------------------------------------- command

// cmdStatus — crawlerctl status: builds the summary from the DB and Redis and prints it.
func cmdStatus(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("status")
	asJSON := fs.Bool("json", false, "machine output (for scripts)")
	if handled, err := parseFlags(fs, statusHelp, args, stdout); handled {
		return err
	}
	if fs.NArg() > 0 {
		return usagef("status: extra arguments %v (the summary is taken as a whole, no positional arguments)", fs.Args())
	}

	// The configuration — a single YAML + environment (internal/config):
	// the PostgreSQL DSN and the Redis address (the reanalyze style, T-074).
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("status: %w", err)
	}
	// Ctrl-C/SIGTERM cancel the heavy aggregates (full passes over large
	// tables) — the shared command helper (signalCtx).
	ctx, stop := signalCtx()
	defer stop()

	// CLI pool_size = 2 — the connection budget of spec 6.3, as in the
	// services (the manage.go style, T-074).
	poolOpts := db.NewOptions(cfg.Postgres)
	poolOpts.PoolSize = 2
	poolOpts.AppName = "crawlerctl" // label in pg_stat_activity (T-177)
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

	// The queue depth — through the backpressure gate (T-026): the narrow
	// Len API over go-redis; the gate threshold is not involved here. An
	// unavailable Redis does not hide the DB summary: we print with "n/a"
	// and exit with code 1.
	rdb := queue.NewClient(cfg.Redis.Addr)
	defer func() { _ = rdb.Close() }()
	lens, qErr := status.FetchQueueLens(ctx, queue.NewBackpressureGate(rdb, queue.BackpressureOptions{}))

	if *asJSON {
		if err := writeStatusJSON(stdout, zones, tot, thr, rates, lens); err != nil {
			return fmt.Errorf("status: json output: %w", err)
		}
	} else {
		writeStatusTable(stdout, zones, tot, thr, rates, lens)
	}
	if qErr != nil {
		return fmt.Errorf("status: queues: %w", qErr)
	}
	return nil
}

// Package status — the shared library of the pipeline summary (dashboard
// spec §4.5, T-126): SQL selections, types and status calculations, used
// both by the CLI `crawlerctl status` (T-079, spec 5.9) and by the
// operator dashboard (T-129) — a single source of truth about the meaning
// of the numbers; duplicating the SQL between consumers is forbidden
// (dashboard spec §4.5).
//
// The sources are PostgreSQL (the zones counters + the domains aggregates
// + the throughput windows) and Redis (the stream depth — the backlog of
// the consumer groups); everything is read ad hoc, statelessly. The
// package knows nothing about the output: the table/JSON stay in the CLI,
// the snapshots — in the dashboard. The dashboard sections without a CLI
// counterpart (the status distribution, the Laravel finds, the ingest log
// — dashboard spec §5.5–5.7) live here too: the summary SQL is a single
// source of truth, while the degradation of the sections and the snapshot
// as a whole belong to the dashboard assembly (dashboard spec §4.4, §5.9).
//
// What is measurable from the DB (the decisions are fixed; the rest is
// honestly not shown, spec 5.9 "the rate… the expected time to complete
// the circle"):
//
//   - the zone progress — the zones.domains_total/domains_pending/
//     domains_queued/domains_checked counters (the transition deltas +
//     the monitor reconciliation, T-178; the base set is
//     status <> deleted, incremental-counters spec §4.1), read from the
//     reference table without a pass over domains;
//   - the domain rate — the average since the start of the day by the
//     daily check counter checks:YYYY-MM-DD (T-179; last_checked_at has
//     day precision, spec 6.1, there is no hourly window); a window
//     shorter than 10 minutes is not used (right after midnight the
//     estimate jumps);
//   - the HTTP results and renders — the hourly throughput buckets
//     crawler_counters['results:YYYY-MM-DDTHH24'/'rendered:…'] (T-203):
//     TWO live buckets are summed — the previous and the current calendar
//     hour; the rate divisor is the actual length of the covered window
//     (1–2 h, HourWindowSec), the "per second" estimate is unbiased;
//     previously the live subqueries over subdomain_results.checked_at
//     degraded with heap fetches of fresh pages (the stage 27 checkpoint);
//     a render has no timestamp of its own — the rendered share among the
//     results of the window is computed;
//   - the throughput of the DNS stage separately and of the analysis
//     stage is not measurable from the DB (no timestamps; finishing
//     DNS+HTTP is exactly "domains/s") — their metrics are in Prometheus
//     (T-076/T-077);
//   - the ETA of the first circle = (pending + queued) / the domain rate;
//     the division-by-zero protection — there is no rate or it is 0 →
//     "n/a".
//
// Windows and distributions: the hourly throughput windows are the hourly
// crawler_counters results:/rendered: buckets (T-203: live subqueries over
// subdomain_results.checked_at degraded with heap fetches of fresh pages —
// the stage 27 checkpoint, the same mechanism as the daily window T-179);
// the daily throughput window is the incremental counter checks:YYYY-MM-DD
// (T-179: the live count of last_checked_at = current_date degraded with
// heap fetches of fresh pages — the measurement of §2 TZ_slow_done_today);
// the distributions and states are incremental counters (T-178: zones +
// crawler_counters, the transition deltas + reconciliation once per
// monitor.zone_stats_interval — the only full pass over domains in the
// steady state). The dashboard caches snapshots with a background loop
// (dashboard spec §4.4, the TTL cache T-161 — it now caches cheap reads,
// protecting from traffic between the refresh cycles), and its period
// (dashboard.refresh) sets the price of the monitoring.
package status

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// ---------------------------------------------------------------- SQL

// zoneStatsSQL — the per-zone summary from the reference table counters
// (T-178, incremental-counters spec §8.1): without a JOIN over domains —
// the full pass over 47M rows left the read paths, the summary reads
// thousands of zone rows; the counters keep the transition deltas
// (incremental-counters spec §6), and the monitor reconciliation does
// (incremental-counters spec §7). The base set of all columns is
// status <> deleted (the unification of incremental-counters spec §4.1:
// queued/done were previously counted by the live aggregate without a
// status filter — a deleted domain with a stuck lease fell into queued;
// now a deleted domain is neither work nor progress).
var zoneStatsSQL = "SELECT z.id, z.name, coalesce(z.name_unicode, '') AS name_unicode, " +
	"z.is_active, z.domains_total, z.domains_pending, z.domains_queued, z.domains_checked " +
	"FROM zones AS z ORDER BY z.name"

// throughputSQL — the throughput measurements in one query; every number
// is a point read / a sum of crawler_counters rows, there are no passes
// over large tables in the read path. now() is the transaction time: the
// intervals are consistent within a single execution; current_date,
// date_trunc and now() share the time zone of the DB server, the same as
// the now() of the last_checked_at/checked_at record times (T-058).
// done_today is a point read of the daily counter
// crawler_counters['checks:YYYY-MM-DD'] (T-179): the day name is computed
// by the DB with the same current_date as the reconciliation window —
// Go clocks do not participate in the name; the counter is fed by the
// result record increments (httpworker/record.go), the authority is the
// monitor reconciliation. coalesce is OUTSIDE the subquery: before the
// first record of the day the subquery is empty (NULL), not a row with a
// NULL value — the idle pipeline hours show an honest zero
// (TZ_slow_done_today §4/§7). done_today counts the result record events,
// not domains: repeated episodes of the same domain within a day converge
// into the "work throughput" (the discrepancy with the live count(*) —
// spec §5.1, restored by the reconciliation).
//
// results_hour/rendered_hour — the sums of the HOURLY throughput buckets
// crawler_counters['results:YYYY-MM-DD"T"HH24'/'rendered:…'] (T-203):
// the former live subqueries over subdomain_results (count(*) over
// checked_at >= now() − 1 h) degraded with heap fetches of the fresh pages
// of the window — the stage 27 checkpoint (the median 6.0 s > 3 s, 108
// events; the same mechanism as the daily window T-179 — the measurement
// of §2 TZ_slow_done_today). A bucket is a calendar hour; the sliding
// "last hour" window is not assembled exactly from buckets: the reader
// sums TWO live buckets — the previous and the current hour (the minimum
// covering the window [now() − 1 h, now()]), and the honest rate divisor
// is hour_window_sec, the actual length of the covered window
// [date_trunc('hour', now() − 1 h), now()] (1–2 h): dividing by 3600 would
// overestimate the throughput sawtooth-wise 1–2× toward the hour boundary.
// The increments are fed by the record transactions: results — the
// insertion of result rows (httpworker/record.go, the delta is the chunk
// row count), rendered — the rendered = true flag set by the render stage
// (renderworker/worker.go, by the same content-replacement transaction);
// the bucket of the rendered family is the checked_at HOUR of the ROW
// (the hour of the episode, not of the render — that is what the live
// window counted: the rendered share among the results of the window).
// The convergence of the buckets with the live count(*) is per calendar
// hour (the monitor reconciliation, the FILTER positions of the pass);
// between passes the discrepancy is the streaming difference of the
// semantics (duplicate deliveries, episode rewrites), as with the daily
// rows. coalesce OUTSIDE: before the first increments there are no
// buckets — an honest zero. The bucket names are computed by the DB
// (date_trunc from now()) — the time zone is consistent with the
// increments and the reconciliation; Go clocks do not participate in the
// name (the T-179 style).
const throughputSQL = "SELECT " +
	"coalesce((SELECT value FROM crawler_counters " +
	"WHERE name = 'checks:' || to_char(current_date, 'YYYY-MM-DD')), 0) AS done_today, " +
	"extract(epoch FROM (now() - date_trunc('day', now())))::float8 AS elapsed_sec, " +
	"(SELECT coalesce(sum(value), 0) FROM crawler_counters " +
	"WHERE name IN ('results:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24'), " +
	"'results:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24'))) AS results_hour, " +
	"(SELECT coalesce(sum(value), 0) FROM crawler_counters " +
	"WHERE name IN ('rendered:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24'), " +
	"'rendered:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24'))) AS rendered_hour, " +
	"extract(epoch FROM (now() - date_trunc('hour', now() - interval '1 hour')))::float8 AS hour_window_sec"

// domainStatusSQL — the distribution of domains over statuses (dashboard
// spec §5.5) from the status_* rows of the crawler_counters table (T-178,
// incremental-counters spec §8.2): the rows are written by the monitor
// reconciliation (the status distribution is not incremented on the
// transitions — the old statuses are not pinned by the operator
// predicates, incremental-counters spec §13.3; the FILTER columns in the
// reconciliation scan are free), the freshness is
// monitor.zone_stats_interval (30 min, T-199; previously the live
// aggregate was cached for a minute, T-161). A LIKE pattern with an
// escaped underscore: the row name is status_<name>; a one-character _
// mask would catch foreign names. ORDER BY name is a deterministic row
// order (the names and the legend completeness are in FetchDomainStatuses).
const domainStatusSQL = "SELECT name, value FROM crawler_counters WHERE name LIKE 'status\\_%' ORDER BY name"

// laravelTotalsSQL — the Laravel find counters (dashboard spec §5.6):
// the total — the sum of the incremental zone counters (T-178,
// incremental-counters spec §8.3: sum(zones.domains_laravel), as the
// landing reads since T-122; the deltas are written by the analyzer, the
// authority is the reconciliation); the 24 h window — a live aggregate
// over laravel_domains (refreshed when the aggregate is recomputed,
// spec 5.8) by the laravel_domains_updated_at_idx index (T-120): the cost
// is proportional to the window, not to the table (the bounds of
// incremental-counters spec §3.1). The 24 h window is the same as the
// "finds in 24 h" of the landing (T-118/T-120), but over laravel_domains —
// dashboard spec §5.6 names this table explicitly: the section shows the
// live aggregate, not the transition log.
const laravelTotalsSQL = "SELECT (SELECT coalesce(sum(domains_laravel), 0) FROM zones), " +
	"count(*) FILTER (WHERE updated_at >= now() - interval '24 hours') " +
	"FROM laravel_domains"

// laravelRecentSQL — the recent finds (dashboard spec §5.6): the domain is
// assembled from the label and the zone (a JOIN by the laravel_domains
// key); the sorting and the LIMIT go by the laravel_domains_updated_at_idx
// index (T-120). ver_min/ver_max are NULL-able (the range may have failed
// to be determined) — FetchLaravelStats unfolds them into empty strings.
const laravelRecentSQL = "SELECT d.label || '.' || z.name AS domain, z.name AS zone, " +
	"ld.score, ld.ver_min, ld.ver_max, ld.updated_at " +
	"FROM laravel_domains AS ld " +
	"JOIN domains AS d ON d.zone_id = ld.zone_id AND d.id = ld.domain_id " +
	"JOIN zones AS z ON z.id = ld.zone_id " +
	"ORDER BY ld.updated_at DESC LIMIT 10"

// ingestRecentSQL — the recently processed files (dashboard spec §5.7).
// NULLS LAST is mandatory: DESC in PostgreSQL lifts NULLs to the top by
// default — a row without processed_at (the schema allows it) must not
// head the "recent"; id DESC is a deterministic order within one second.
const ingestRecentSQL = "SELECT path, action, lines_total, added, existed, deleted, rejected, processed_at " +
	"FROM ingest_files ORDER BY processed_at DESC NULLS LAST, id DESC LIMIT 10"

// ---------------------------------------------------------------- data

// ZoneStats — a zone summary row.
type ZoneStats struct {
	ID             int16
	Name           string // punycode — the canonical form (spec 5.2)
	NameUnicode    string // "" — NULL; display only (in JSON)
	IsActive       bool
	DomainsTotal   int64 // the zones.domains_total counter (deltas + reconciliation, T-178)
	DomainsChecked int64 // the zones.domains_checked counter (deltas + reconciliation, T-178)
	Pending        int64 // the zones.domains_pending counter: waiting for selection
	Queued         int64 // the zones.domains_queued counter: in work (lease)
	Done           int64 // the DNS/HTTP stage is complete (spec 7) — now the same
	// domains_checked counter (T-178: previously the live aggregate counted
	// done without a status filter — the unification of the base set of
	// incremental-counters spec §4.1 closed the discrepancy in favor of the
	// reconciliation); the field is kept — the JSON wire format (§5.3)
	// shows both columns.
}

// Throughput — the throughput measurements from the DB (one execution of
// throughputSQL).
type Throughput struct {
	DoneToday     int64   // the result record events since the start of the day (the checks: counter, T-179)
	ElapsedSec    float64 // elapsed since the start of the day by the DB clock
	ResultsHour   int64   // the result rows in the throughput window buckets (results:, T-203): the previous + the current hour
	RenderedHour  int64   // of those with a final DOM (the rendered: buckets, T-203)
	HourWindowSec float64 // the covered bucket window length by the DB clock: [trunc(now−1h), now], 1–2 h (T-203)
}

// FetchZoneStats executes zoneStatsSQL and collects the zone rows.
func FetchZoneStats(ctx context.Context, pool db.DB) ([]ZoneStats, error) {
	if pool == nil {
		return nil, errors.New("status: зоны: пул не задан")
	}
	rows, err := pool.Query(ctx, zoneStatsSQL)
	if err != nil {
		return nil, fmt.Errorf("status: зоны: %w", err)
	}
	defer rows.Close()
	var zones []ZoneStats
	for rows.Next() {
		var z ZoneStats
		if err := rows.Scan(&z.ID, &z.Name, &z.NameUnicode, &z.IsActive,
			&z.DomainsTotal, &z.Pending, &z.Queued, &z.DomainsChecked); err != nil {
			return nil, fmt.Errorf("status: зоны: чтение: %w", err)
		}
		z.Done = z.DomainsChecked // one counter column — both semantics (T-178)
		zones = append(zones, z)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("status: зоны: %w", err)
	}
	return zones, nil
}

// FetchThroughput executes throughputSQL (exactly one row — a scalar
// SELECT without FROM).
func FetchThroughput(ctx context.Context, pool db.DB) (Throughput, error) {
	if pool == nil {
		return Throughput{}, errors.New("status: темп: пул не задан")
	}
	rows, err := pool.Query(ctx, throughputSQL)
	if err != nil {
		return Throughput{}, fmt.Errorf("status: темп: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return Throughput{}, errors.New("status: темп: пустой ответ")
	}
	var t Throughput
	if err := rows.Scan(&t.DoneToday, &t.ElapsedSec, &t.ResultsHour, &t.RenderedHour, &t.HourWindowSec); err != nil {
		return Throughput{}, fmt.Errorf("status: темп: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return Throughput{}, fmt.Errorf("status: темп: %w", err)
	}
	return t, nil
}

// ---------------------------------------------------------------- dashboard section data (dashboard spec §5.5–5.7)

// LaravelRecent — a "recent finds" row (dashboard spec §5.6): the domain,
// the zone, the score, the version range (an empty string — NULL: the
// range is undetermined), the update moment.
type LaravelRecent struct {
	Domain    string
	Zone      string
	Score     float32
	VerMin    string // "" — NULL
	VerMax    string // "" — NULL
	UpdatedAt time.Time
}

// LaravelStats — the Laravel finds section (dashboard spec §5.6): the
// total, the last 24 hours, the recent 10.
type LaravelStats struct {
	Total   int64
	Last24h int64
	Recent  []LaravelRecent // an empty slice — no finds (not an error)
}

// IngestReport — a "recent ingest files" row (dashboard spec §5.7): the
// file, the action ("add"/"delete" per model.Action; a code outside the
// spec 6.1 table — the name "Action(N)", NULL — empty), the row counters
// and the processing moment.
type IngestReport struct {
	Path        string // "" — NULL
	Action      string
	LinesTotal  int64
	Added       int64
	Existed     int64
	Deleted     int64
	Rejected    int64
	ProcessedAt time.Time // zero — NULL
}

// FetchDomainStatuses — the distribution of domains over statuses
// (dashboard spec §5.5) from the status_* rows of crawler_counters
// (T-178). The keys are the names of spec 6.2 of the main spec
// (model.Statuses); the statuses without rows enter with zero — a full
// legend for display without the client knowing the table; the rows with
// a name outside the legend (an unknown schema code) are not displayed —
// the reconciliation writes only model.Statuses; a foreign row means a
// manual fix of the table (runbooks §6).
func FetchDomainStatuses(ctx context.Context, pool db.DB) (map[string]int64, error) {
	if pool == nil {
		return nil, errors.New("status: статусы: пул не задан")
	}
	rows, err := pool.Query(ctx, domainStatusSQL)
	if err != nil {
		return nil, fmt.Errorf("status: статусы: %w", err)
	}
	defer rows.Close()
	counts := make(map[string]int64, len(model.Statuses))
	for _, s := range model.Statuses {
		counts[s.String()] = 0
	}
	const statusPrefix = "status_"
	for rows.Next() {
		var name string
		var n int64
		if err := rows.Scan(&name, &n); err != nil {
			return nil, fmt.Errorf("status: статусы: чтение: %w", err)
		}
		if _, ok := counts[strings.TrimPrefix(name, statusPrefix)]; !ok {
			continue // a name outside the model.Statuses legend — the comment above
		}
		counts[strings.TrimPrefix(name, statusPrefix)] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("status: статусы: %w", err)
	}
	return counts, nil
}

// FetchLaravelStats collects the Laravel finds section (dashboard spec
// §5.6): the counters in one pass over laravel_domains, the recent 10 —
// a separate query with a JOIN (the T-120 index). An error of any query
// is an error of the whole section (the caller puts null, dashboard spec
// §5.9). The first result set is closed BEFORE the second query (the
// web.FetchLandingData style): a scalar SELECT does not finish reading by
// itself — holding the set would occupy a pool connection for the entire
// duration of the second query (with pool_size=1 — a deadlock).
func FetchLaravelStats(ctx context.Context, pool db.DB) (LaravelStats, error) {
	if pool == nil {
		return LaravelStats{}, errors.New("status: laravel: пул не задан")
	}
	var st LaravelStats
	rows, err := pool.Query(ctx, laravelTotalsSQL)
	if err != nil {
		return LaravelStats{}, fmt.Errorf("status: laravel: %w", err)
	}
	if !rows.Next() {
		// An empty set of a scalar aggregate is an anomaly: we distinguish
		// a row-level error (the real cause is more valuable than a
		// synthetic one) from an honest "empty".
		err := rows.Err()
		rows.Close()
		if err != nil {
			return LaravelStats{}, fmt.Errorf("status: laravel: %w", err)
		}
		return LaravelStats{}, errors.New("status: laravel: пустой ответ агрегата")
	}
	if err := rows.Scan(&st.Total, &st.Last24h); err != nil {
		rows.Close()
		return LaravelStats{}, fmt.Errorf("status: laravel: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return LaravelStats{}, fmt.Errorf("status: laravel: %w", err)
	}
	rows.Close()

	rows, err = pool.Query(ctx, laravelRecentSQL)
	if err != nil {
		return LaravelStats{}, fmt.Errorf("status: laravel: последние: %w", err)
	}
	defer rows.Close()
	st.Recent = make([]LaravelRecent, 0, 10)
	for rows.Next() {
		var (
			it      LaravelRecent
			verMin  *string
			verMax  *string
			updated time.Time
		)
		if err := rows.Scan(&it.Domain, &it.Zone, &it.Score, &verMin, &verMax, &updated); err != nil {
			return LaravelStats{}, fmt.Errorf("status: laravel: последние: чтение: %w", err)
		}
		if verMin != nil {
			it.VerMin = *verMin
		}
		if verMax != nil {
			it.VerMax = *verMax
		}
		it.UpdatedAt = updated
		st.Recent = append(st.Recent, it)
	}
	if err := rows.Err(); err != nil {
		return LaravelStats{}, fmt.Errorf("status: laravel: последние: %w", err)
	}
	return st, nil
}

// FetchIngestReports — the recent 10 records of the ingest_files log
// (dashboard spec §5.7). The NULL schema columns
// (path/action/counters/processed_at) are unfolded into zero values —
// the pipeline always writes the log with full rows (internal/ingest
// RecordFile); NULL is legal only for manually inserted rows.
func FetchIngestReports(ctx context.Context, pool db.DB) ([]IngestReport, error) {
	if pool == nil {
		return nil, errors.New("status: ingest: пул не задан")
	}
	rows, err := pool.Query(ctx, ingestRecentSQL)
	if err != nil {
		return nil, fmt.Errorf("status: ingest: %w", err)
	}
	defer rows.Close()
	out := make([]IngestReport, 0, 10)
	for rows.Next() {
		var (
			it        IngestReport
			path      *string
			action    *int16
			lines     *int64
			added     *int64
			existed   *int64
			deleted   *int64
			rejected  *int64
			processed *time.Time
		)
		if err := rows.Scan(&path, &action, &lines, &added, &existed, &deleted,
			&rejected, &processed); err != nil {
			return nil, fmt.Errorf("status: ingest: чтение: %w", err)
		}
		if path != nil {
			it.Path = *path
		}
		if action != nil {
			it.Action = model.Action(*action).String()
		}
		it.LinesTotal, it.Added, it.Existed = derefInt64(lines), derefInt64(added), derefInt64(existed)
		it.Deleted, it.Rejected = derefInt64(deleted), derefInt64(rejected)
		if processed != nil {
			it.ProcessedAt = *processed
		}
		out = append(out, it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("status: ingest: %w", err)
	}
	return out, nil
}

// derefInt64 — the pointer value or 0 (a NULL ingest_files counter).
func derefInt64(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

// ---------------------------------------------------------------- queues

// StreamLener — the stream depth; *queue.BackpressureGate (T-026)
// satisfies the interface. A separate interface, not go-redis directly:
// the tests substitute a fake, the transport is replaceable (spec 4.2).
type StreamLener interface {
	Len(ctx context.Context, stream string) (int64, error)
}

// Streams — the working queue streams in the output order (spec 4.2):
// q:dns → q:http → q:render → q:render-fallback → q:analyze → q:dead.
// The single source is queue.AllStreams (S-P3-1): the list does not
// diverge from the queue; a stream added to allStreams appears in the
// CLI/dashboard summaries automatically (the T-237 promise about
// "monitoring that picks it up automatically").
var Streams = queue.AllStreams()

// FetchQueueLens collects the queue depths. An error of any stream stops
// the collection: there is a single Redis; repeated calls to an
// unavailable server are pointless.
func FetchQueueLens(ctx context.Context, lenner StreamLener) (map[string]int64, error) {
	if lenner == nil {
		return nil, errors.New("status: очереди: клиент не задан")
	}
	lens := make(map[string]int64, len(Streams))
	for _, s := range Streams {
		n, err := lenner.Len(ctx, s)
		if err != nil {
			return nil, err
		}
		lens[s] = n
	}
	return lens, nil
}

// ---------------------------------------------------------------- calculations

const (
	// MinSpeedWindowSec — the minimum window of the domain rate estimate:
	// right after midnight the "since the start of the day" average is
	// unstable (a window of minutes at thousands of checks per second,
	// spec 3.3).
	MinSpeedWindowSec = 600
	// speedWindowSec — the fixed throughput divisor when HourWindowSec is
	// zero (a manual Throughput assembly without reading the DB — the
	// tests; the live reading brings the actual bucket window length of
	// 1–2 h, T-203).
	speedWindowSec = 3600.0
)

// Totals — the sums over the zones (the table totals and the input of the
// ETA calculation).
type Totals struct {
	DomainsTotal   int64
	DomainsChecked int64
	Pending        int64
	Queued         int64
	Done           int64
}

// ComputeTotals sums the zone rows.
func ComputeTotals(zones []ZoneStats) Totals {
	var t Totals
	for _, z := range zones {
		t.DomainsTotal += z.DomainsTotal
		t.DomainsChecked += z.DomainsChecked
		t.Pending += z.Pending
		t.Queued += z.Queued
		t.Done += z.Done
	}
	return t
}

// Rates — the derived rates/ETA. A pure function of the measurements:
// short windows and zero rates do not produce "n/a" estimates (the
// division-by-zero protection is the Known flags, not a panic).
type Rates struct {
	DomainsPerSec float64 // the average since the start of the day
	DomainsKnown  bool    // the window is sufficient (≥ MinSpeedWindowSec)
	ResultsPerSec float64 // the HTTP results/s per hour
	RenderPerSec  float64 // the renders/s per hour
	Remaining     int64   // pending + queued — the volume of the first circle
	ETASec        int64
	ETAKnown      bool
}

// ComputeRates derives the rates and the ETA of the first circle. The
// throughput divisor of the results/renders is the actual bucket window
// length HourWindowSec (T-203): the buckets are calendar ones, the reader
// sums the previous and the current hour — a 1–2 h window; dividing by
// its length keeps the "per second" unbiased; a zero window (a manual
// assembly of the values) — a fixed hour. An empty circle (nothing to
// check) is complete regardless of the rate; the ETA with a remainder is
// at least a second — a remainder of one domain must not look like "0s".
func ComputeRates(t Throughput, pending, queued int64) Rates {
	window := t.HourWindowSec
	if window <= 0 {
		window = speedWindowSec
	}
	r := Rates{
		ResultsPerSec: float64(t.ResultsHour) / window,
		RenderPerSec:  float64(t.RenderedHour) / window,
		Remaining:     pending + queued,
	}
	if r.Remaining == 0 {
		r.ETAKnown, r.ETASec = true, 0
	}
	if t.ElapsedSec >= MinSpeedWindowSec {
		r.DomainsKnown = true
		r.DomainsPerSec = float64(t.DoneToday) / t.ElapsedSec
		if r.Remaining > 0 && r.DomainsPerSec > 0 {
			r.ETASec = int64(math.Round(float64(r.Remaining) / r.DomainsPerSec))
			// The "minimum 1s" clamp: before the line ETASec =
			// Round(remainder/rate) ≥ 0 with positive operands; zero and
			// one give 1, a larger value is unchanged — "< 1" and
			// "<= 1"/"< 2" are indistinguishable on any reachable value.
			// mutator-disable-next-line expression/comparison, numbers/incrementer
			if r.ETASec < 1 {
				r.ETASec = 1
			}
			r.ETAKnown = true
		}
	}
	return r
}

// FormatDuration — a rounded human-readable interval: "45s", "13m",
// "5h 20m", "3d 4h" (days are the ceiling of the units: weeks hide the
// scale worse than three-digit days).
func FormatDuration(sec int64) string {
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

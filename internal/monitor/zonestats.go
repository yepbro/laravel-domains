// Package monitor — the monitoring utility service (spec 5.9). The
// zonestats file — reconciliation of the pipeline's incremental counters
// (T-178, the incremental-counters spec, tasks/TZ_incremental_counters.md
// §7): one combined pass per monitor.zone_stats_interval — the only full
// domains pass in steady state (measurement §12 docs/load-measurements.md:
// the live aggregates zoneStatsSQL/domainStatusSQL/backlogSQL/
// historyStatsSQL are most of the postgres CPU; T-161 softened it with
// intervals/cache, T-178 removed the passes themselves from the read
// paths — readers go by the counters, the incremental-counters spec §8).
//
// The pass is authoritative: the value of each counter is overwritten in
// full by the aggregate, the transition increments (the incremental-counters
// spec §6, scheduler/ingest/httpworker/analyzer) give freshness between
// passes. The pass composition (one transaction, one snapshot — the
// consistency of all positions at one moment):
//
//   - zones: domains_total/domains_pending/domains_queued/
//     domains_checked — one FILTER aggregate of domains GROUP BY zone_id
//     (the partitioning key — a partitionwise aggregate, like T-078),
//     domains_laravel — the laravel_domains aggregate (T-120). The base
//     set of all columns is status <> deleted (the unification of the
//     incremental-counters spec §4.1: a deleted domain is neither work
//     nor progress);
//   - crawler_counters: the status_<name> rows by model.Statuses (all
//     domains rows, deleted included — the legend is complete, the
//     incremental-counters spec §4.2),
//     render_pending — the bit counter of T-175 (the same predicate as
//     the backfill of migration 000015 and the renderq increments: no
//     status filter; the bit of a deleted domain is cleared by applying
//     the delete in the same transaction with a decrement, T-186 — rare
//     desyncs are extinguished by this pass),
//     laravel_appeared_total/laravel_lost_total — the laravel_history
//     aggregate (the T-118 cumulatives), checks:YYYY-MM-DD — the daily
//     checks counter (T-179, TZ_slow_done_today §6: the live analog of
//     the former count(*) … WHERE last_checked_at = current_date
//     subquery; the day name is computed by the DB — a separate
//     statement after the scalars, not put into the unnest batch with
//     the Go name: a Go/DB timezone desync would silently split the row
//     and the FILTER window),
//     temp_error:YYYY-MM-DD — the daily numerator of the temp_error
//     share (T-182, TZ_slow_dns_share §6: the same zero FILTER cost, the
//     denominator of the share alert is the checks: row), the retry
//     maturity baskets retry_mature:YYYY-MM-DD (T-202: mature temp_error
//     candidates by episode dates — the guard of the heavy retry
//     selection; the FILTER columns "mature today/today's episodes",
//     stale baskets the pass deletes — two rows live, yesterday's and
//     today's), the hourly tempo baskets results:/rendered:YYYY-MM-DD"T"HH24
//     (T-203: the "last hour" windows of the dashboard tempo; the live
//     subqueries over subdomain_results degraded with heap fetches — the
//     checkpoint of stage 27; the two-live-hours window aggregate by
//     checked_at — the prev/cur FILTER positions, rendered — the async
//     flag of the render stage in the EPISODE hour of the row; the pass
//     keeps two baskets of each family — the previous hour (the tail of
//     the reading window: late renders keep working it all through the
//     current hour) and the current one, stale ones are deleted) and the
//     hourly baskets of the unanalyzed reanalysis tails
//     reanalyze:YYYY-MM-DD"T"HH24 (T-204: rows with rules_version IS
//     NULL by episode hours — the guard of the hourly reanalysis tick;
//     the same prev/cur FILTER positions of the window aggregate, two
//     live baskets, stale ones are deleted), new_done_stale — the
//     "status hung" domains (status = new ∧ check_state = done ∧
//     last_checked_at older than a day, FR-2.2 of the TZ-fsm spec: the
//     f_new_done_stale FILTER column of the same scan; a steady-state
//     zero is the norm; healed by the T-270 compensator —
//     scheduler/stalerequeue.go, the guard of its hourly pass reads this
//     row; the gauge/alert is the signal of new losses);
//   - the drift |old − new| of each position: before the overwrite the
//     old values are read, after — the new ones (the transaction sees
//     its own writes); a non-zero drift — the crawler_counters_drift
//     gauge + a Warn (the incremental-counters spec §7.1.3). The first
//     pass of the loop does not count as drift: after a migration/
//     downtime it fills in the zeros (the incremental-counters spec §5).
//
// The domains scan in the pass is one: the per-zone aggregate dom (the
// FILTER by check_state without the status filter) serves both the zone
// columns (total/pending/queued/checked — status <> deleted in FILTER),
// and the global status distribution + the render bit (the sums over
// zones), and the daily check windows with the temp_error numerator (the
// FILTER last_checked_at = current_date × status, T-179/T-182), and the
// retry maturity baskets of temp_error by episode dates (T-202) — the
// extra cost of the FILTER columns is zero (the incremental-counters
// spec §13.3). The hourly tempo baskets (T-203) and the reanalysis tail
// ones (T-204) are computed by a SEPARATE subdomain_results aggregate
// over the two-live-hours window (the checked_at index range — the cost
// is proportional to the window, at most 2× the former per-minute tempo
// reader, here — once per zone_stats_interval).
//
// The semantics of the zone columns — the package comment below; the
// changes against T-078 are only extensions (pending/queued), the
// existing columns are read by the landing (T-122), crawlerctl status,
// partitioning.
package monitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/model"
)

// DefaultZoneStatsInterval — the reconciliation period: 30 minutes. The
// original 60 s (the top of the spec 5.9 corridor) targeted a small
// corpus: measurement §12 docs/load-measurements.md (47 M rows) — the
// UPDATE aggregate is in the top three of the postgres CPU items; T-161
// raised it to 5 min, T-178 kept the reconciliation period the same: in
// steady state it is the only full domains pass (the incremental-counters
// spec §1.2 criterion). The operator's decision of Oct 26 (T-199) per
// the incremental-counters spec §7.1 template "after the wave's break-in
// the interval may be thinned out to 15–30 min": the measurement of the
// slow query window after the deploy 3bffc62 (Oct 25 18:16 → Oct 26
// 03:05 UTC) — the reconciliation is the only remaining full pass, 48%
// of the total slow DB time (100 events >3 s, median 39 s, max 501 s)
// at a pass every 5 min; the interval was thinned out to 30 min, the
// drift between passes must be zero — by the drift measurement the
// operator sees when the interval can be brought back shorter. The
// spec corridor remains available as the monitor.zone_stats_interval
// config; since T-206 the alert loop runs on its own interval
// (monitor.alerts_interval, DefaultAlertsInterval 5 min) — the progress
// stall threshold remains in reconciliation periods
// (AlertStallFactor × the interval = 2.5 h): zones.updated_at is written
// by this loop.
const DefaultZoneStatsInterval = 30 * time.Minute

// ---------------------------------------------------------------- SQL

// statusAggCols — the status legend and their column positions in the
// per-zone domainsAggSQL aggregate: ONE source for both the SQL
// generation (the FILTER columns) and the Go reading (the result column
// order) — a desync of the model.Statuses legend and the columns is
// impossible by construction (review P3-4).
var statusAggCols = []struct {
	status model.Status
	col    string
}{
	{model.StatusNew, "f_new"},
	{model.StatusNotDelegated, "f_not_delegated"},
	{model.StatusDelegatedNoAddress, "f_delegated_no_address"},
	{model.StatusNoHTTP, "f_no_http"},
	{model.StatusAlive, "f_alive"},
	{model.StatusTempError, "f_temp_error"},
	{model.StatusDeleted, "f_deleted"},
	{model.StatusInvalid, "f_invalid"},
}

func init() {
	// A check by indexes, not only by length (review R2 P3-6): replacing
	// one status with another would keep the length equal — the legend
	// would drift apart silently and the aggregate would write foreign
	// values.
	if len(statusAggCols) != len(model.Statuses) {
		panic("monitor: statusAggCols не совпадает с model.Statuses по длине — легенда агрегата неполна")
	}
	for i, c := range statusAggCols {
		if c.status != model.Statuses[i] {
			panic(fmt.Sprintf("monitor: statusAggCols[%d] = %v, want %v (порядок model.Statuses)", i, c.status, model.Statuses[i]))
		}
	}
}

// domainsAggSQL — the per-zone aggregate of the live distribution,
// statuses and the render bit by ONE scan of domains (the
// incremental-counters spec §7.1): the FILTER variant of count counts all
// the positions in one pass. The check_state/status/flags codes are
// model constants inlined into the text as literals (schema codes, not
// data — the lease.go style); GROUP BY on the partitioning key (zone_id
// in the HASH subpartition leaves of T-005 is part of the PK) enables a
// partitionwise aggregate. There is no zone filter: the aggregate serves
// both the zone columns and the global sums (statuses, the render bit) —
// a zone filter would cap the global counters (the incremental-counters
// spec §9: recount --zone always recomputes the global counters).
// f_render — the predicate of the 000015 backfill and the T-175
// increments (no status filter: the bit of deleted domains is cleared by
// applying the delete in the same transaction, T-186, — the predicate
// stays status-neutral, like the backfill and the increments).
// f_checks_today — the daily checks window (T-179): the same predicate
// last_checked_at = current_date as the former done_today throughputSQL
// subquery (deleted rows included — the live analog did not filter
// them); the FILTER column costs zero on the already running scan (the
// incremental-counters spec §13.3), the sum over zones is the value of
// the checks:YYYY-MM-DD row. f_temp_today — the numerator of the daily
// temp_error share (T-182, TZ_slow_dns_share §6): the same window × the
// temp_error status, a zero FILTER cost (the former dnsShareSQL live
// analog counted the window in full and went to the heap for the status
// — query #6 of the slow-query log of Oct 24), the sum over zones is the
// value of the temp_error:YYYY-MM-DD row.
// f_retry_mature_today/f_retry_mature_past — the retry maturity baskets
// of temp_error (T-202): the mature candidates (status = temp_error ×
// check_state = done) by episode dates — today's episodes
// (last_checked_at = current_date, they mature at midnight of the next
// day) and the matured cohort (last_checked_at < current_date — the
// retry window "older than 24 h" by the date column matures in cohorts
// at midnight); the sums over zones are the values of the
// retry_mature:YYYY-MM-DD baskets read by the guard of the heavy retry
// selection (scheduler/retry.go, retryMatureSQL), a zero FILTER cost on
// the same scan. f_new_done_stale — the "status hung" domains (FR-2.2 of
// the TZ-fsm spec, T-256): status = new ∧ check_state = done ∧
// last_checked_at < current_date — the DNS stage wrote the "addresses
// exist" branch (O6 of the general status table,
// internal/model/enums.go), and the HTTP outcome (O7–O9) did not arrive
// (the q:http task is lost). The day boundary is the precision of the
// last_checked_at column (date, spec 6.1), the same "older than 24 h"
// implementation as the retry window T-031/T-202 (retrySelectSQL:
// date < now() − 24 h is equivalent to date < current_date). A NULL
// last_checked_at does not count: the done writers always set the
// stamp, a NULL at new∧done is manual SQL junk, not a hung status. The
// sum over zones is the value of the new_done_stale row and the
// crawler_domains_new_done_stale gauge; steady-state zero is the norm.
var domainsAggSQL = func() string {
	cols := make([]string, 0, len(statusAggCols)+10)
	cols = append(cols,
		fmt.Sprintf("count(*) FILTER (WHERE status <> %d) AS total", int16(model.StatusDeleted)),
		fmt.Sprintf("count(*) FILTER (WHERE status <> %d AND check_state = %d) AS pending", int16(model.StatusDeleted), int16(model.CheckStatePending)),
		fmt.Sprintf("count(*) FILTER (WHERE status <> %d AND check_state = %d) AS queued", int16(model.StatusDeleted), int16(model.CheckStateQueued)),
		fmt.Sprintf("count(*) FILTER (WHERE status <> %d AND check_state = %d) AS checked", int16(model.StatusDeleted), int16(model.CheckStateDone)))
	for _, c := range statusAggCols {
		cols = append(cols, fmt.Sprintf("count(*) FILTER (WHERE status = %d) AS %s", int16(c.status), c.col))
	}
	cols = append(cols,
		fmt.Sprintf("count(*) FILTER (WHERE (flags & %d) <> 0) AS f_render", int16(model.FlagRenderPending)),
		"count(*) FILTER (WHERE last_checked_at = current_date) AS f_checks_today",
		fmt.Sprintf("count(*) FILTER (WHERE last_checked_at = current_date AND status = %d) AS f_temp_today", int16(model.StatusTempError)),
		fmt.Sprintf("count(*) FILTER (WHERE status = %d AND check_state = %d AND last_checked_at = current_date) AS f_retry_mature_today", int16(model.StatusTempError), int16(model.CheckStateDone)),
		fmt.Sprintf("count(*) FILTER (WHERE status = %d AND check_state = %d AND last_checked_at < current_date) AS f_retry_mature_past", int16(model.StatusTempError), int16(model.CheckStateDone)),
		fmt.Sprintf("count(*) FILTER (WHERE status = %d AND check_state = %d AND last_checked_at < current_date) AS f_new_done_stale", int16(model.StatusNew), int16(model.CheckStateDone)))
	return "SELECT zone_id, " + strings.Join(cols, ", ") + " FROM domains GROUP BY zone_id"
}()

// zoneAggRow — a row of the per-zone aggregate (the domainsAggSQL
// columns): the state values and the status legend in the statusAggCols
// order + the render bit + the daily checks window (T-179), its
// temp_error numerator (T-182), the retry maturity baskets (T-202:
// today's/matured temp_error × done episodes) and the "status hung"
// domains (FR-2.2: new ∧ done ∧ older than a day).
type zoneAggRow struct {
	zone                            int16
	total, pending, queued, checked int64
	statuses                        []int64 // in the statusAggCols order
	render                          int64
	checksToday                     int64 // FILTER last_checked_at = current_date (T-179)
	tempToday                       int64 // FILTER last_checked_at = current_date AND status = temp_error (T-182)
	retryToday                      int64 // FILTER status = temp_error AND done AND last_checked_at = current_date (T-202)
	retryPast                       int64 // FILTER status = temp_error AND done AND last_checked_at < current_date (T-202)
	newDoneStale                    int64 // FILTER status = new AND done AND last_checked_at < current_date (FR-2.2, T-256)
}

// recountAggQuerySQL — reading the per-zone aggregate of the pass (the
// only full domains scan of the reconciliation, the incremental-counters
// spec §7.1).
var recountAggQuerySQL = "SELECT * FROM (" + domainsAggSQL + ") AS agg ORDER BY zone_id"

// laravelAggQuerySQL — the aggregate of the zone Laravel counters
// (T-120): the laravel_domains rows have already passed the analyzer
// threshold (§13.1), a separate threshold is not needed.
const laravelAggQuerySQL = "SELECT zone_id, count(*) AS laravel FROM laravel_domains GROUP BY zone_id ORDER BY zone_id"

// historyAggSQL — the appeared/lost cumulatives of the detection log.
// The incremental-counters spec trade-off (§6.5 "they remove the hourly
// seq scan" against §7.1.2 "the laravel_history aggregate in the
// pass"): the log is append-only and an order of magnitude smaller than
// domains (the events are only the Laravel find transitions), a pass
// once per zone_stats_interval over it is seconds against the tens of
// seconds of the domains scan; the analyzer increments give freshness,
// the aggregate is the authority.
const historyAggSQL = "SELECT count(*) FILTER (WHERE appeared) AS h_appeared, " +
	"count(*) FILTER (WHERE NOT appeared) AS h_lost FROM laravel_history"

// hourlyAggSQL — the live aggregate of the hourly baskets (T-203): the
// two-live-hours window over subdomain_results (the bounds are computed
// by the DB — date_trunc('hour', now()), the checked_at index serves the
// range), the FILTER positions are the same predicates as the former
// live results_hour/rendered_hour subqueries of throughputSQL (the same
// "last hour" boundary, calendar one: prev — the rows of the hour
// [trunc(now−1h), trunc(now)), cur — [trunc(now), …]; WHERE — the lower
// window bound). rendered — the async flag of the render stage: the hour
// position counts the rows rendered LATER than their check — like the
// renderworker increment (the hour of the row's episode), which is why
// the baskets converge with the live count(*) over the calendar hour.
// prev_reanalyze/cur_reanalyze (T-204) — the reanalysis tails: rows with
// rules_version IS NULL (never analyzed — an insert of a result record
// or a content replacement by render) — the predicate of the tick pass
// guard (analyzer/reanalyze.go); the FILTER column costs zero on the
// same scan. A scalar aggregate without GROUP BY — exactly one row (an
// empty window — zeros, not an empty set).
const hourlyAggSQL = "SELECT " +
	"count(*) FILTER (WHERE checked_at < date_trunc('hour', now())) AS prev_results, " +
	"count(*) FILTER (WHERE checked_at < date_trunc('hour', now()) AND rendered) AS prev_rendered, " +
	"count(*) FILTER (WHERE checked_at >= date_trunc('hour', now())) AS cur_results, " +
	"count(*) FILTER (WHERE checked_at >= date_trunc('hour', now()) AND rendered) AS cur_rendered, " +
	"count(*) FILTER (WHERE checked_at < date_trunc('hour', now()) AND rules_version IS NULL) AS prev_reanalyze, " +
	"count(*) FILTER (WHERE checked_at >= date_trunc('hour', now()) AND rules_version IS NULL) AS cur_reanalyze " +
	"FROM subdomain_results " +
	"WHERE checked_at >= date_trunc('hour', now() - interval '1 hour')"

// recountZonesSQL — overwriting the zone columns with the aggregate
// values from Go (an unnest batch; a zone without live domains gets
// zeros — the Go value was computed with zeros by its absence from the
// aggregate). It locks the zone rows FIRST — before the crawler_counters
// rows (recountCountersSQL): the "zones → counters" order is shared with
// the transition writers (httpworker/record.go, analyzer) and rules out
// lock cycles (review P3-2). updated_at — the moment of the pass (the
// monitor liveness, spec 5.9 "progress stall").
// RETURNING — the new column values for the drift (PG16 without
// RETURNING OLD: the old ones are read before the statement in the same
// transaction).
const recountZonesSQL = "UPDATE zones AS z " +
	"SET domains_total = u.total, " +
	"domains_pending = u.pending, " +
	"domains_queued = u.queued, " +
	"domains_checked = u.checked, " +
	"domains_laravel = u.laravel, " +
	"updated_at = $6 " +
	"FROM unnest($1::int2[], $2::int8[], $3::int8[], $4::int8[], $5::int8[], $7::int8[]) " +
	"AS u(id, total, pending, queued, checked, laravel) " +
	"WHERE z.id = u.id " +
	"RETURNING z.id, z.domains_total, z.domains_pending, z.domains_queued, z.domains_checked, z.domains_laravel"

// recountCountersSQL — upsert of the global scalars with the values from
// Go (the rows are created by the first pass, the incremental-counters
// spec §5); it locks the crawler_counters rows LAST — after the zones
// (the anti-deadlock order, the recountZonesSQL comment).
const recountCountersSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
	"SELECT u.name, u.value, $3 " +
	"FROM unnest($1::text[], $2::int8[]) AS u(name, value) " +
	"ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
	"RETURNING name, value"

// readZonesSQL — the old zone column values before the overwrite
// (drift). $1 — the zone filter (NULL — all).
const readZonesSQL = "SELECT id, domains_total, domains_pending, domains_queued, domains_checked, domains_laravel " +
	"FROM zones WHERE ($1::int2 IS NULL OR id = $1)"

// readCountersSQL — the old global counter values before the upsert
// (drift); a missing row — an old value of 0 (the first pass fills it).
const readCountersSQL = "SELECT name, value FROM crawler_counters WHERE name = ANY($1)"

// readDailyChecksSQL — the old value of the daily checks counter row
// before the overwrite (drift, T-179): the day name is computed by the
// DB (current_date) — the same timezone as the f_checks_today FILTER
// column of the pass and the now() of the last_checked_at write (T-058);
// an empty result — the day row does not exist yet (the write increment
// or this pass creates it — a fill-in, not drift, the "first pass fills"
// precedent, the incremental-counters spec §5).
const readDailyChecksSQL = "SELECT value FROM crawler_counters " +
	"WHERE name = 'checks:' || to_char(current_date, 'YYYY-MM-DD')"

// readDailyTempSQL — the old value of the daily temp_error numerator row
// before the overwrite (drift, T-182): a pair of readDailyChecksSQL, the
// temp_error:YYYY-MM-DD row (the increment of a result record with a
// temp_error verdict, TZ_slow_dns_share §5); an empty result — the day
// row does not exist yet (the first temp_error episode of the day, or
// the pass will create it) — a fill-in, not drift.
const readDailyTempSQL = "SELECT value FROM crawler_counters " +
	"WHERE name = 'temp_error:' || to_char(current_date, 'YYYY-MM-DD')"

// recountDailyChecksSQL — overwriting the current day row with the pass
// aggregate value (T-179, TZ_slow_done_today §6): value := the sum of
// the f_checks_today FILTER columns of the per-zone aggregate, the day
// name — again by the DB (a Go/DB timezone desync would split the row
// and the window). The row lock is the last one, after the zones and the
// global scalars (the anti-deadlock order, the recountZonesSQL comment;
// shared with the result record increment record.go: zones →
// render_pending → checks:). RETURNING — the new value for the drift.
const recountDailyChecksSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
	"VALUES ('checks:' || to_char(current_date, 'YYYY-MM-DD'), $1::int8, $2) " +
	"ON CONFLICT (name) DO UPDATE " +
	"SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
	"RETURNING value"

// recountDailyTempSQL — overwriting the daily temp_error numerator with
// the pass aggregate value (T-182, TZ_slow_dns_share §6): value := the sum
// of the f_temp_today FILTER columns; a pair of recountDailyChecksSQL —
// the lock AFTER the denominator row (the same order as the record.go
// increments: checks: → temp_error: — no lock cycles), RETURNING — the
// new value for the drift. The pass creates the day row without
// temp_error events with the window value (possibly zero) — symmetry
// with the denominator, the share reader before the pass sees the
// coalesce zero of the increments.
const recountDailyTempSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
	"VALUES ('temp_error:' || to_char(current_date, 'YYYY-MM-DD'), $1::int8, $2) " +
	"ON CONFLICT (name) DO UPDATE " +
	"SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
	"RETURNING value"

// The retry maturity baskets of temp_error (T-202). The name is the
// episode date (the last_checked_at of the temp_error result record):
// the basket becomes "mature" (summed by the retryMatureSQL guard of the
// scheduler) at midnight of the next day — the moment the cohort
// matures at the date precision of the column. The pass maintains
// EXACTLY TWO live baskets — yesterday's (the mature cohort,
// value := the f_retry_mature_past FILTER) and today's (the episodes of
// the day, value := the f_retry_mature_today FILTER): it deletes the
// older ones (the stale accounting of the past days' drains and the tail
// decrements of episode dates older than yesterday's — their value is
// already in the sum of the yesterday basket recomputed by the
// aggregate). The names are computed by the DB
// (current_date/current_date − 1) — a Go/DB timezone desync would split
// the basket and the FILTER window (the T-179 style). The lock order is
// last in the pass, after the daily rows, in the "cleanup → mature →
// today" order: shared with the writers (record.go: … → retry_mature:;
// retry.go: a basket decrement of episode dates; monitor — here), with
// no lock cycles. A caveat (the T-202 review): the execution order of
// non-referencing DML CTEs of one PostgreSQL statement is not guaranteed
// by the documentation ("executed concurrently… unpredictable") — the
// Nov 07 incident showed an inversion in production (rm → zc of a
// dns-worker result record: the "counters → zones" edge, 40P01 deadlocks
// ~3–4/h), and the hot writers were moved to separate statements
// (T-267: the DNS stage increment — autocommit after the write, the
// requeue decrement — the second statement of the decision transaction);
// for the remaining multi-CTE writers the enumeration order (the zones
// before the counters) is still the reliance for the absence of cycles;
// if PG changes the behavior, look here (the radical cure — separate
// statements of the transaction, the T-267 precedent).
const (
	// readRetryPastSQL — the old value of the mature (yesterday's) basket
	// before the overwrite (drift, T-202); an empty result — the basket
	// does not exist yet (a fill-in, not drift).
	readRetryPastSQL = "SELECT value FROM crawler_counters " +
		"WHERE name = 'retry_mature:' || to_char(current_date - 1, 'YYYY-MM-DD')"

	// readRetryTodaySQL — the old value of today's basket (drift, T-202);
	// an empty result — no basket until the first temp_error episode of
	// the day (the increment or the pass will create it).
	readRetryTodaySQL = "SELECT value FROM crawler_counters " +
		"WHERE name = 'retry_mature:' || to_char(current_date, 'YYYY-MM-DD')"

	// retryMatureCleanupSQL — deleting the baskets older than yesterday's:
	// their cohorts are already merged into the mature basket by the
	// overwrite below (the f_retry_mature_past aggregate counts the
	// statuses in full, not by dates), and the rows fall into the guard's
	// sum range — the tails of decrements of old episode dates with
	// negative values would distort the sum until the next pass; the
	// baskets are the live drain schedule, not a tempo history (unlike
	// the daily rows, T-179 §10.4).
	// RETURNING — for the unified Query protocol of the pass (the values
	// are not read).
	retryMatureCleanupSQL = "DELETE FROM crawler_counters " +
		"WHERE name >= 'retry_mature:' " +
		"AND name < 'retry_mature:' || to_char(current_date - 1, 'YYYY-MM-DD') " +
		"RETURNING value"

	// recountRetryPastSQL — overwriting the mature basket with the value
	// of the f_retry_mature_past FILTER column (the sum over zones),
	// RETURNING — the new value for the drift.
	recountRetryPastSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('retry_mature:' || to_char(current_date - 1, 'YYYY-MM-DD'), $1::int8, $2) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
		"RETURNING value"

	// recountRetryTodaySQL — overwriting today's basket with the value of
	// the f_retry_mature_today FILTER column (the sum over zones); a pair
	// of recountRetryPastSQL, the lock is the last of the baskets.
	recountRetryTodaySQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('retry_mature:' || to_char(current_date, 'YYYY-MM-DD'), $1::int8, $2) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
		"RETURNING value"
)

// The baskets of the unanalyzed reanalysis tails (T-204):
// reanalyze:YYYY-MM-DD"T"HH24 — the subdomain_results rows with
// rules_version IS NULL (never analyzed) by the EPISODE hour
// (checked_at). The guard of the hourly reanalysis tick
// (analyzer/reanalyze.go, reanalyzeGuardSQL) sums the baskets older than
// the current hour: the immature in-flight flow (write → q:analyze →
// stamp — seconds) lives in the current-hour basket and the guard does
// not open; a publication that survived the hour boundary without a
// stamp — a lost tail — opens it, the tick reads out the window and
// publishes. A lossless steady state costs one sum of an index range out
// of a pair of rows — the heavy window selection (a heap visit of ~84 k
// rows/h for the non-indexable rules_version, the HOT invariant) is not
// performed at all (a refinement of the same class as T-191 → T-202: a
// naive "written without analysis" counter does not distinguish the
// tails from a continuous flow — the HOURLY baskets give the maturity,
// the retry_mature precedent). The increments are the result record
// transactions (record.go, the hour of now() = the rows' checked_at) and
// the content replacement by render (renderworker, the episode hour from
// RETURNING — a render delay resurrects the old-hour basket and the
// guard opens in vain: the tail is beyond the cursor, the next pass
// deletes the basket);
// a decrement is the analyzer stamping only stale rows (the version
// guard, worker.go): repeated deliveries do not double, the stamps of an
// old version (the rule-change drains) give a negative drift until the
// pass — it keeps the guard closed during the drain, the tick has its
// own work there.
// The pass maintains EXACTLY TWO live baskets — the previous hour
// (recomputed, not only the current one: late stamp decrements keep
// working it all through the current hour) and the current one; stale
// ones are deleted (after the retry_mature/hourly families of
// T-202/T-203). The lock order is between the retry baskets and the
// rendered tempo ones (a single family order of all writers
// "retry_mature: → reanalyze: → rendered: → results:" — a convention,
// NOT the lexicographics of the names
// ('retry_mature:' is lexicographically last: 't' > 'a'/'n'/'s');
// against deadlocks only the order consistency across all writers
// matters), inside a family "cleanup → prev → cur": shared with the
// writers (record.go: … → retry_mature: → reanalyze: → results:;
// renderworker: reanalyze: → rendered:; analyzer: the single reanalyze:
// point) — no cycles. The guard sees the sums of the live baskets right
// after the commit.
const (
	// readHourlyReanalyzePrevSQL — the old value of the previous-hour tail
	// basket before the overwrite (drift, T-204); an empty result — the
	// basket does not exist yet (before the first increment/pass; a
	// fill-in, not drift).
	readHourlyReanalyzePrevSQL = "SELECT value FROM crawler_counters " +
		"WHERE name = 'reanalyze:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24')"

	// readHourlyReanalyzeCurSQL — the old value of the current-hour tail
	// basket (drift, T-204); a pair of readHourlyReanalyzePrevSQL.
	readHourlyReanalyzeCurSQL = "SELECT value FROM crawler_counters " +
		"WHERE name = 'reanalyze:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24')"

	// hourlyReanalyzeCleanupSQL — deleting the tail baskets older than the
	// previous hour: the live tail of hour H is served by the tick of
	// hour H+1 (the H basket = prev, the guard open); a tail not picked up
	// within two hours (failed ticks) beyond the baskets is the residual
	// time-form class (T-193 §4.2): a restart with the 24 h anchor or a
	// full rule-change drain. RETURNING — the Query protocol of the pass
	// (the values are not read).
	hourlyReanalyzeCleanupSQL = "DELETE FROM crawler_counters " +
		"WHERE name >= 'reanalyze:' " +
		"AND name < 'reanalyze:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24') " +
		"RETURNING value"

	// recountHourlyReanalyzePrevSQL — overwriting the previous-hour tail
	// basket with the value of the prev_reanalyze FILTER position of the
	// hourlyAggSQL aggregate, RETURNING — the new value for the drift.
	recountHourlyReanalyzePrevSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('reanalyze:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24'), $1::int8, $2) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
		"RETURNING value"

	// recountHourlyReanalyzeCurSQL — overwriting the current-hour tail
	// basket (the cur_reanalyze FILTER position); the lock after the
	// previous one (the names ascend).
	recountHourlyReanalyzeCurSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('reanalyze:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24'), $1::int8, $2) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
		"RETURNING value"
)

// The hourly tempo baskets (T-203): results:YYYY-MM-DD"T"HH24 — the
// subdomain_results rows inserted by the HTTP stage result record
// transaction (the httpworker/record.go increment, the delta — the
// RowsAffected of the INSERT batch;
// the basket hour is the date_trunc('hour', now()) of the same
// transaction, the rows' checked_at is the same now()), rendered:… — the
// rows switched to rendered = true by the render stage (the
// renderworker/worker.go increment by the row's checked_at from the
// UPDATE RETURNING — the EPISODE hour, not the render one: that is how
// the live window of the former reader counted). The replacement of the
// live results_hour/rendered_hour subqueries of throughputSQL (count(*)
// over checked_at >= now() − 1 h): the index-only scan of the fresh
// window pages degraded with heap fetches — the checkpoint of stage 27
// (a median of 6.0 s > 3 s, 108 events; the same mechanism as the daily
// window of T-179 — measurement §2 of TZ_slow_done_today). The basket is
// a calendar hour:
// the "last hour" sliding window reader (status.throughputSQL) assembles
// it as the sum of the TWO live baskets — the previous and the current
// one, the tempo divisor is the actual length of the covered window; the
// baskets' convergence with the live count(*) is guaranteed over the
// calendar hour — here (hourlyAggSQL,
// the prev/cur FILTER positions). The pass keeps EXACTLY TWO live
// baskets of each family: the previous hour is the "tail of the reading
// window" (late rendered-increments of the past hour's rows keep working
// its basket all through the current hour — hence it is recomputed, not
// only the current one) and the current hour; stale baskets are deleted
// (after retry_mature, T-202 — the baskets are the live tempo window,
// not a history). The names are computed by the DB (date_trunc of
// now()) — a Go/DB timezone desync would split the basket and the
// aggregate window (the T-179/T-202 style). The lock order is the end of
// the pass after the retry baskets, the families lexicographically
// "rendered: → results:", inside a family "cleanup → previous →
// current" (the names ascend: the hour is lexicographic): shared with
// the writers (record.go: … → retry_mature: → results: last;
// renderworker: the single rendered: point; monitor — here), no cycles.
// A boundary case: a rendered-increment delay > 1 h (a row of hour
// H−2 rendered in hour H) resurrects the H−2 basket by an upsert — the
// reader does not read it (the IN list of the two live hours), the next
// pass deletes it;
// it did not belong to the live window before either (the "last hour"
// window).
const (
	// readHourlyRenderedPrevSQL — the old value of the previous-hour
	// rendered-tempo basket before the overwrite (drift, T-203); an empty
	// result — the basket does not exist yet (before the first rendered
	// episode of the hour; a fill-in, not drift).
	readHourlyRenderedPrevSQL = "SELECT value FROM crawler_counters " +
		"WHERE name = 'rendered:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24')"

	// readHourlyRenderedCurSQL — the old value of the current-hour
	// rendered-tempo basket (drift, T-203); a pair of
	// readHourlyRenderedPrevSQL.
	readHourlyRenderedCurSQL = "SELECT value FROM crawler_counters " +
		"WHERE name = 'rendered:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24')"

	// readHourlyResultsPrevSQL — the old value of the previous-hour
	// results-tempo basket (drift, T-203).
	readHourlyResultsPrevSQL = "SELECT value FROM crawler_counters " +
		"WHERE name = 'results:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24')"

	// readHourlyResultsCurSQL — the old value of the current-hour
	// results-tempo basket (drift, T-203).
	readHourlyResultsCurSQL = "SELECT value FROM crawler_counters " +
		"WHERE name = 'results:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24')"

	// hourlyRenderedCleanupSQL — deleting the rendered-tempo baskets older
	// than the previous hour: their rows are outside the live reading
	// window (the reader sums prev+cur), the tail value is not needed; for
	// late increments of stale hours see the family comment. RETURNING —
	// the Query protocol of the pass (the values are not read).
	hourlyRenderedCleanupSQL = "DELETE FROM crawler_counters " +
		"WHERE name >= 'rendered:' " +
		"AND name < 'rendered:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24') " +
		"RETURNING value"

	// hourlyResultsCleanupSQL — deleting the results-tempo baskets older
	// than the previous hour; a pair of hourlyRenderedCleanupSQL
	// (results-increments are always of the current hour — there are no
	// delays, the tail is only the rows recomputed into prev).
	hourlyResultsCleanupSQL = "DELETE FROM crawler_counters " +
		"WHERE name >= 'results:' " +
		"AND name < 'results:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24') " +
		"RETURNING value"

	// recountHourlyRenderedPrevSQL — overwriting the previous-hour
	// rendered-tempo basket with the value of the prev_rendered FILTER
	// position of the hourlyAggSQL aggregate, RETURNING — the new value
	// for the drift.
	recountHourlyRenderedPrevSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('rendered:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24'), $1::int8, $2) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
		"RETURNING value"

	// recountHourlyRenderedCurSQL — overwriting the current-hour
	// rendered-tempo basket (the cur_rendered FILTER position); the lock
	// after the previous one (the names ascend).
	recountHourlyRenderedCurSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('rendered:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24'), $1::int8, $2) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
		"RETURNING value"

	// recountHourlyResultsPrevSQL — overwriting the previous-hour
	// results-tempo basket (the prev_results FILTER position); the
	// results: family — after rendered: (the prefix lexicographics — the
	// lock order).
	recountHourlyResultsPrevSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('results:' || to_char(date_trunc('hour', now() - interval '1 hour'), 'YYYY-MM-DD\"T\"HH24'), $1::int8, $2) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
		"RETURNING value"

	// recountHourlyResultsCurSQL — overwriting the current-hour
	// results-tempo basket (the cur_results FILTER position); the end of
	// the pass.
	recountHourlyResultsCurSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('results:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24'), $1::int8, $2) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at " +
		"RETURNING value"
)

// CounterNames — the names of the global counter rows maintained by the
// reconciliation (the legend for reading/verification). The daily rows
// checks:YYYY-MM-DD and temp_error:YYYY-MM-DD (T-179/T-182), the retry
// maturity baskets retry_mature:YYYY-MM-DD (T-202) and the hourly tempo
// baskets results:/rendered: and the reanalysis tail ones reanalyze:
// (T-203/T-204) are not here: the day/hour names are computed by the DB
// (readDailyChecksSQL/recountDailyChecksSQL, their temp pairs, the retry
// basket and hourly basket pairs), they are not in the unnest batch with
// the Go names.
// new_done_stale (FR-2.2) — an absolute level, like status_*: written
// only by the reconciliation (no increments), read by the point lookup
// of the domains_new_done_stale alert, the loop's gauge and the guard of
// the T-270 compensator (scheduler/stalerequeue.go).
func CounterNames() []string {
	names := make([]string, 0, len(model.Statuses)+4)
	for _, s := range model.Statuses {
		names = append(names, "status_"+s.String())
	}
	return append(names, "render_pending", "laravel_appeared_total", "laravel_lost_total", "new_done_stale")
}

// driftCounterNames — the global rows with DELTA-driven semantics:
// the transition increments maintain the value between the passes, the
// drift in steady state must be zero (the incremental-counters spec
// §7.1.3, §11). The status_* rows are not here: the status distribution
// is written ONLY by the reconciliation (the incremental-counters spec
// §13.3 — the old transition statuses are not pinned by the statement
// predicates), under live load their "drift" is the freshwater lag of
// the interval, not an anomaly: a Warn/gauge on them would be noise on
// every pass, verify would never converge without a tolerance. The
// daily checks:/temp_error: rows (T-179/T-182) are delta-driven but with
// a streaming mismatch semantics (the HTTP result record events against
// the domains of the day of all the pipeline branches,
// TZ_slow_done_today §5.1): their daily:checks/daily:temp_error
// positions are maintained by the pass separately (the day name is
// computed by the DB), and verify does not cross-check them — under load
// the mismatch equals the difference of the stage flows since the last
// pass (freshness, not an anomaly).
func driftCounterNames() []string {
	return []string{"render_pending", "laravel_appeared_total", "laravel_lost_total"}
}

// dailyChecksCounter — the label of the daily checks counter position in
// the pass drift (T-179, TZ_slow_done_today §8). Streaming, not
// zero-at-steady-state: it does not take part in the aggregated Warn
// (reportDrift), only the gauge observes it.
const dailyChecksCounter = "daily:checks"

// dailyTempCounter — the label of the daily temp_error numerator position
// in the pass drift (T-182, TZ_slow_dns_share §6): the same streaming
// semantics as dailyChecksCounter (the numerator and the denominator
// diverge from the live window together — the repeated episodes of the
// day), outside the Warn, observed by the gauge.
const dailyTempCounter = "daily:temp_error"

// retryMaturePastCounter — the label of the mature retry temp_error
// basket in the pass drift (T-202): the increments/decrements count
// events (result record episodes against retries), the live FILTER
// window — domains (status = temp_error × done × date); duplicate
// deliveries and repeated episodes of the day give +1 to the live zero,
// candidates that bypassed the retry (a deletion of a temp_error domain
// by the delete file, the age recheck of T-030) leave +1 in the basket
// until the pass — a streaming semantics (like daily:*,
// TZ_slow_done_today §5.1), outside the Warn,
// observed by the gauge.
const retryMaturePastCounter = "retry_mature:past"

// retryMatureTodayCounter — the label of today's retry temp_error basket
// (T-202): the same streaming semantics as retryMaturePastCounter
// (today's episodes: the increments are events, the FILTER is domains).
const retryMatureTodayCounter = "retry_mature:today"

// dailyStreamingPositions — the daily positions of the streaming
// semantics (T-179/T-182): the increments count the HTTP result record
// events, the live pass window is the domains of the day of all the
// pipeline branches; there is no steady-state zero, the aggregated drift
// Warn does not raise them (noise under live load), the gauge exposes
// them.
func dailyStreamingPositions() []string {
	return []string{dailyChecksCounter, dailyTempCounter}
}

// retryMatureStreamingPositions — the retry maturity basket positions
// (T-202) of the same streaming semantics as the daily rows: the episode
// events against the live filtered domains; they are not in the Warn,
// observed by the gauge (the retryMaturePastCounter comment).
func retryMatureStreamingPositions() []string {
	return []string{retryMaturePastCounter, retryMatureTodayCounter}
}

// hourlyRenderedPrevCounter — the label of the previous-hour
// rendered-tempo basket in the pass drift (T-203): the renderworker
// increments count events (row switches to rendered), the live
// aggregate — the window rows;
// a re-render of an already rendered row (a repeated delivery, the DOM
// by the same hash) and the episode overwrites (a result record deletes
// the rendered rows) give a mismatch until the pass — a streaming
// semantics (like daily:*, TZ_slow_done_today §5.1), outside the Warn,
// observed by the gauge.
const hourlyRenderedPrevCounter = "rendered:prev"

// hourlyRenderedCurCounter — the label of the current-hour rendered-tempo
// basket (T-203): the same streaming semantics as
// hourlyRenderedPrevCounter.
const hourlyRenderedCurCounter = "rendered:cur"

// hourlyResultsPrevCounter — the label of the previous-hour results-tempo
// basket (T-203): the result record increments count the row insertion
// events, the live aggregate is the rows (an episode overwrite deletes
// the old window rows without decrementing the basket) — a streaming
// semantics, outside the Warn, observed by the gauge.
const hourlyResultsPrevCounter = "results:prev"

// hourlyResultsCurCounter — the label of the current-hour results-tempo
// basket (T-203); a pair of hourlyResultsPrevCounter.
const hourlyResultsCurCounter = "results:cur"

// hourlyReanalyzePrevCounter — the label of the previous-hour unanalyzed
// reanalysis tail basket in the pass drift (T-204): the record/render
// increments and the stamp decrements count the transition events, the
// live aggregate — the rows with rules_version IS NULL; an episode
// overwrite (a record.go DELETE without a decrement), a render of an
// unstamped row (a double increment of one tail) and the stamps of an
// old version (the drains) give a mismatch until the pass — a streaming
// semantics (the T-202/T-203 family), outside the Warn, observed by the
// gauge.
const hourlyReanalyzePrevCounter = "reanalyze:prev"

// hourlyReanalyzeCurCounter — the label of the current-hour reanalysis
// tail basket (T-204); a pair of hourlyReanalyzePrevCounter.
const hourlyReanalyzeCurCounter = "reanalyze:cur"

// hourlyStreamingPositions — the hourly tempo basket positions (T-203)
// and the reanalysis tail ones (T-204) of the streaming semantics: the
// events of row insertions/switches to rendered/stamps against the live
// window rows;
// there is no steady-state zero (duplicate deliveries, episode
// overwrites, drains), the aggregated drift Warn does not raise them
// (noise under live load), the gauge exposes them.
func hourlyStreamingPositions() []string {
	return []string{hourlyReanalyzePrevCounter, hourlyReanalyzeCurCounter,
		hourlyRenderedPrevCounter, hourlyRenderedCurCounter, hourlyResultsPrevCounter, hourlyResultsCurCounter}
}

// ---------------------------------------------------------------- pass

// ZoneStatsDB — the pool for the pass: a db.DB with transactions (the
// drift and the overwrite are one snapshot); *pgxpool.Pool satisfies the
// interface (the RetryDB/RecordDB style).
type ZoneStatsDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// compile-time: the real pool fits the interface.
var _ ZoneStatsDB = (*pgxpool.Pool)(nil)

// ZoneStatsOptions — the reconciliation options; zero values are
// replaced by defaults (withDefaults, the scheduler.RecheckOptions style).
type ZoneStatsOptions struct {
	// Interval is the period of the ZoneStatsLoop loop
	// (DefaultZoneStatsInterval);
	// the working value is monitor.zone_stats_interval from the config. It
	// does not affect a one-off ZoneStatsOnce.
	Interval time.Duration
	// Now is the time source for zones.updated_at and
	// crawler_counters.updated_at; nil — time.Now. Tests substitute fixed
	// hours (the scheduler style, T-030).
	Now func() time.Time
	// Zone is the filter of a one-off pass (recount --zone, the
	// incremental-counters spec §9):
	// only the chosen zone's columns are overwritten, the global
	// counters are always recomputed. nil — all zones; it does not affect
	// the ZoneStatsLoop loop (the periodic pass is always full).
	Zone *int16
	// Log is the logger of the ZoneStatsLoop loop; nil — records
	// suppressed (slog.DiscardHandler). ZoneStatsOnce does not log — the
	// result is returned as ZoneStatsResult.
	Log *slog.Logger
}

// withDefaults replaces the non-positive values with defaults (Now is
// checked separately: functions are nil).
func (o ZoneStatsOptions) withDefaults() ZoneStatsOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultZoneStatsInterval
	}
	return o
}

// CounterPosition — a reconciliation position: the name for the
// metric/CLI (zone:<id>:pending, global:status_alive — the
// incremental-counters spec §11) and the old/new pair. New is the
// written value, Old is the one read before the overwrite (for verify —
// the live aggregate against the stored one).
type CounterPosition struct {
	Counter string
	Old     int64
	New     int64
}

// Drift — |old − new| of the position.
func (p CounterPosition) Drift() int64 {
	d := p.New - p.Old
	if d < 0 {
		return -d
	}
	return d
}

// ZoneStatsResult — the result of one ZoneStatsOnce pass.
type ZoneStatsResult struct {
	// Zones is the zones whose columns were overwritten (the RETURNING
	// row count — each zone row is updated once).
	Zones int64
	// Positions is all the positions of the pass (the zone columns + the
	// global scalars) with the old/new values; Positions[i].Drift() > 0 is
	// drift. It can be empty only for a pass with a filter of a
	// non-existent zone.
	Positions []CounterPosition
	// NewDoneStale — the "status hung" domains (FR-2.2 of the TZ-fsm spec,
	// T-256): status = new ∧ check_state = done ∧ last_checked_at older
	// than a day — the sum of the f_new_done_stale FILTER column of the
	// per-zone aggregate; the value of the new_done_stale row and the
	// crawler_domains_new_done_stale gauge (the ZoneStatsLoop loop). A
	// steady-state zero is the norm; a non-zero one is a lost q:http task
	// (O6 without O7–O9, the general status table
	// internal/model/enums.go) — healed by the T-270 compensator (its
	// hourly pass guard is a read of this row), the gauge remains the
	// signal of new losses.
	NewDoneStale int64
}

// zoneColumnPositions — the metrics of the zone column positions (the
// order is the columns of readZonesSQL/recountZonesSQL RETURNING, no id).
var zoneColumnPositions = [...]string{"total", "pending", "queued", "checked", "laravel"}

// ZoneStatsOnce performs one combined reconciliation pass (the
// incremental-counters spec §7.1) in one transaction: (1) it reads the
// old values of the zones, the counters, the daily rows (checks and
// temp_error, T-179/T-182), the retry maturity baskets (T-202), the
// hourly tempo baskets (T-203) and the reanalysis tail baskets (T-204);
// (2) the only full domains scan — the per-zone aggregate
// (recountAggQuerySQL, including the FILTER columns of last_checked_at
// = current_date and the maturity baskets, T-179/T-182/T-202), the
// laravel_domains and laravel_history aggregates and the hourly basket
// aggregate (hourlyAggSQL — the two-live-hours window of
// subdomain_results, the T-203 tempo positions and the T-204 reanalysis
// tail ones); (3) it overwrites the zone columns with an unnest batch of
// values (the zone locks first); (4) it upserts the global scalars (the
// crawler_counters locks after the zones, the anti-deadlock order —
// recountZonesSQL), the daily rows (in the checks: → temp_error: order),
// the retry maturity baskets (cleanup of stale ones → mature → today's,
// T-202), the reanalysis tail baskets and the hourly tempo baskets (the
// end of the pass: the reanalyze family, then the rendered family, then
// the results family, in each one "cleanup → previous → current",
// T-203/T-204); the RETURNING of the statements gives the new values for
// the drift. The pass is idempotent (spec 3.4): a repeat gives the same
// values; a failure rolls back the transaction in full — there are no
// half-done states. The Zone filter (recount --zone) limits the zone
// overwrite; the global scalars, the daily rows and the baskets are
// always recomputed (the incremental-counters spec §9).
func ZoneStatsOnce(ctx context.Context, pool ZoneStatsDB, opts ZoneStatsOptions) (ZoneStatsResult, error) {
	if pool == nil {
		return ZoneStatsResult{}, errors.New("monitor: реконсиляция счётчиков: пул не задан")
	}
	o := opts.withDefaults()
	now := time.Now
	if o.Now != nil {
		now = o.Now
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return ZoneStatsResult{}, fmt.Errorf("monitor: реконсиляция счётчиков: транзакция: %w", err)
	}
	// An unconditional rollback: after Commit it is a no-op (the T-044 style).
	defer func() { _ = tx.Rollback(ctx) }()

	oldZones, err := readZonePositions(ctx, tx, o.Zone)
	if err != nil {
		return ZoneStatsResult{}, err
	}
	oldCounters, err := readCounterValues(ctx, tx, CounterNames())
	if err != nil {
		return ZoneStatsResult{}, err
	}
	// The daily rows — in the same block of old reads (one snapshot): the
	// day name is computed by the DB (T-179/T-182), presence is the fill-in
	// sign (drift only for an existing row).
	oldDaily, dailyExists, err := readDailyValue(ctx, tx, readDailyChecksSQL, "суточная строка проверок")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	oldTemp, tempExists, err := readDailyValue(ctx, tx, readDailyTempSQL, "суточная строка temp_error")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	// The retry maturity baskets (T-202) — the same block: presence is
	// the fill-in sign (the basket is created by an increment or this
	// pass).
	oldRetryPast, retryPastExists, err := readDailyValue(ctx, tx, readRetryPastSQL, "зрелая корзина ретрая")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	oldRetryToday, retryTodayExists, err := readDailyValue(ctx, tx, readRetryTodaySQL, "сегодняшняя корзина ретрая")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	// The hourly tempo baskets (T-203) — the same block: presence is the
	// fill-in sign (the basket is created by a record/render increment or
	// this pass), the hour name is computed by the DB.
	oldHourRenderedPrev, hourRenderedPrevExists, err := readDailyValue(ctx, tx, readHourlyRenderedPrevSQL, "корзина rendered-темпа предыдущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	oldHourRenderedCur, hourRenderedCurExists, err := readDailyValue(ctx, tx, readHourlyRenderedCurSQL, "корзина rendered-темпа текущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	oldHourResultsPrev, hourResultsPrevExists, err := readDailyValue(ctx, tx, readHourlyResultsPrevSQL, "корзина темпа результатов предыдущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	oldHourResultsCur, hourResultsCurExists, err := readDailyValue(ctx, tx, readHourlyResultsCurSQL, "корзина темпа результатов текущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	// The reanalysis tail baskets (T-204) — the same block: presence is
	// the fill-in sign (the basket is created by a record/render
	// increment, a stamp decrement or this pass), the hour name is
	// computed by the DB.
	oldHourReanalyzePrev, hourReanalyzePrevExists, err := readDailyValue(ctx, tx, readHourlyReanalyzePrevSQL, "корзина хвостов переанализа предыдущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	oldHourReanalyzeCur, hourReanalyzeCurExists, err := readDailyValue(ctx, tx, readHourlyReanalyzeCurSQL, "корзина хвостов переанализа текущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}

	// The only full domains scan: the per-zone aggregate + the adjacent ones.
	agg, err := readZoneAgg(ctx, tx)
	if err != nil {
		return ZoneStatsResult{}, err
	}
	lar, err := readLaravelAgg(ctx, tx)
	if err != nil {
		return ZoneStatsResult{}, err
	}
	hAppeared, hLost, err := readHistoryAgg(ctx, tx)
	if err != nil {
		return ZoneStatsResult{}, err
	}
	// The aggregate of the hourly tempo baskets (T-203): the
	// two-live-hours window of subdomain_results — the prev/cur FILTER
	// positions of both families.
	hourly, err := readHourlyAgg(ctx, tx)
	if err != nil {
		return ZoneStatsResult{}, err
	}

	// The zone columns: the values are the aggregate, a zone without live
	// domains is zeros.
	// The traversal order is deterministic (the zoneIDs slice is sorted).
	zoneIDs := make([]int16, 0, len(oldZones))
	for id := range oldZones {
		zoneIDs = append(zoneIDs, id)
	}
	sort.Slice(zoneIDs, func(i, j int) bool { return zoneIDs[i] < zoneIDs[j] })
	zTotal := make([]int64, len(zoneIDs))
	zPending := make([]int64, len(zoneIDs))
	zQueued := make([]int64, len(zoneIDs))
	zChecked := make([]int64, len(zoneIDs))
	zLaravel := make([]int64, len(zoneIDs))
	aggByZone := make(map[int16]*zoneAggRow, len(agg))
	for i := range agg {
		aggByZone[agg[i].zone] = &agg[i]
	}
	larByZone := make(map[int16]int64, len(lar))
	for _, l := range lar {
		larByZone[l.zone] = l.laravel
	}
	for i, id := range zoneIDs {
		if a := aggByZone[id]; a != nil {
			zTotal[i], zPending[i], zQueued[i], zChecked[i] = a.total, a.pending, a.queued, a.checked
		}
		zLaravel[i] = larByZone[id]
	}
	newZones, err := writeZonePositions(ctx, tx, zoneIDs, zTotal, zPending, zQueued, zChecked, zLaravel, now())
	if err != nil {
		return ZoneStatsResult{}, err
	}

	// The global scalars: the sums of the per-zone aggregate + the log
	// cumulatives.
	globals := globalCounters(agg, hAppeared, hLost)
	names := make([]string, 0, len(globals))
	values := make([]int64, 0, len(globals))
	for _, name := range CounterNames() { // a deterministic order
		names = append(names, name)
		values = append(values, globals[name])
	}
	newCounters, err := writeGlobalCounters(ctx, tx, names, values, now())
	if err != nil {
		return ZoneStatsResult{}, err
	}
	// The daily rows — the last writes of the pass (T-179/T-182):
	// the value is the sum of the f_checks_today/f_temp_today FILTER
	// columns, the day row is always global (the Zone filter only touched
	// the zone columns);
	// the "checks: → temp_error:" order is shared with the record.go
	// increments — no lock cycles. The day rotation is simply a new row:
	// the pass after midnight creates it with the window value, the
	// yesterday one stays as the tempo history.
	var checksToday, tempToday, newDoneStale int64
	for i := range agg {
		checksToday += agg[i].checksToday
		tempToday += agg[i].tempToday
		newDoneStale += agg[i].newDoneStale
	}
	newDaily, err := writeDailyCounter(ctx, tx, recountDailyChecksSQL, checksToday, now(), "суточная строка проверок")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	newTemp, err := writeDailyCounter(ctx, tx, recountDailyTempSQL, tempToday, now(), "суточная строка temp_error")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	// The retry maturity baskets (T-202) — the last writes of the pass:
	// the stale baskets are deleted (the mature cohort value is carried in
	// full in the yesterday basket), then the mature one (the FILTER
	// f_retry_mature_past) and today's (f_retry_mature_today) are
	// overwritten — the sums of the same FILTER columns of the per-zone
	// aggregate; the "cleanup → mature → today's" order closes the
	// writers' lock order (record.go/dnsworker: the today increment;
	// retry: the episode date decrement) — no cycles. The guard of the
	// heavy retry selection sees the sum of the mature baskets right
	// after the commit.
	if err := cleanupBaskets(ctx, tx, retryMatureCleanupSQL, "очистка корзин ретрая"); err != nil {
		return ZoneStatsResult{}, err
	}
	var retryTodaySum, retryPastSum int64
	for i := range agg {
		retryTodaySum += agg[i].retryToday
		retryPastSum += agg[i].retryPast
	}
	newRetryPast, err := writeDailyCounter(ctx, tx, recountRetryPastSQL, retryPastSum, now(), "зрелая корзина ретрая")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	newRetryToday, err := writeDailyCounter(ctx, tx, recountRetryTodaySQL, retryTodaySum, now(), "сегодняшняя корзина ретрая")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	// The reanalysis tail baskets (T-204) — after the retry baskets, before
	// the rendered-tempo ones (the family lock order
	// "retry_mature: → reanalyze: → rendered: → results:", the family
	// comment): the stale baskets are deleted, then prev is overwritten
	// (the prev_reanalyze FILTER position — late stamp decrements keep
	// working it all through the current hour) and cur. The guard of the
	// reanalysis tick sees the sums of the live baskets right after the
	// commit.
	if err := cleanupBaskets(ctx, tx, hourlyReanalyzeCleanupSQL, "очистка корзин хвостов переанализа"); err != nil {
		return ZoneStatsResult{}, err
	}
	newHourReanalyzePrev, err := writeDailyCounter(ctx, tx, recountHourlyReanalyzePrevSQL, hourly.prevReanalyze, now(), "корзина хвостов переанализа предыдущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	newHourReanalyzeCur, err := writeDailyCounter(ctx, tx, recountHourlyReanalyzeCurSQL, hourly.curReanalyze, now(), "корзина хвостов переанализа текущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	// The hourly tempo baskets (T-203) — the end of the pass: the stale
	// baskets are deleted (outside the reading window), then the live ones
	// are overwritten — prev (the tail of the reading window: late
	// rendered-increments keep working it all through the current hour)
	// and cur; the families in the prefix lexicographics
	// "rendered: → results:", inside — "cleanup → prev → cur" — the lock
	// order is shared with the writers (record.go: … → results: last;
	// renderworker: rendered:) — no cycles. The tempo reader
	// (status.throughputSQL) sees the sums of the live baskets right
	// after the commit.
	if err := cleanupBaskets(ctx, tx, hourlyRenderedCleanupSQL, "очистка корзин rendered-темпа"); err != nil {
		return ZoneStatsResult{}, err
	}
	newHourRenderedPrev, err := writeDailyCounter(ctx, tx, recountHourlyRenderedPrevSQL, hourly.prevRendered, now(), "корзина rendered-темпа предыдущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	newHourRenderedCur, err := writeDailyCounter(ctx, tx, recountHourlyRenderedCurSQL, hourly.curRendered, now(), "корзина rendered-темпа текущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	if err := cleanupBaskets(ctx, tx, hourlyResultsCleanupSQL, "очистка корзин темпа результатов"); err != nil {
		return ZoneStatsResult{}, err
	}
	newHourResultsPrev, err := writeDailyCounter(ctx, tx, recountHourlyResultsPrevSQL, hourly.prevResults, now(), "корзина темпа результатов предыдущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	newHourResultsCur, err := writeDailyCounter(ctx, tx, recountHourlyResultsCurSQL, hourly.curResults, now(), "корзина темпа результатов текущего часа")
	if err != nil {
		return ZoneStatsResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ZoneStatsResult{}, fmt.Errorf("monitor: реконсиляция счётчиков: фиксация: %w", err)
	}

	res := ZoneStatsResult{Zones: int64(len(newZones)), NewDoneStale: newDoneStale, Positions: make([]CounterPosition, 0, len(newZones)*len(zoneColumnPositions)+len(driftCounterNames())+len(dailyStreamingPositions())+len(retryMatureStreamingPositions())+len(hourlyStreamingPositions()))}
	for _, id := range zoneIDs {
		nz, ok := newZones[id]
		if !ok {
			continue // RETURNING did not return it (the zone was deleted past the code) — the old one stays
		}
		oz := oldZones[id]
		newVals := [...]int64{nz.total, nz.pending, nz.queued, nz.checked, nz.laravel}
		oldVals := [...]int64{oz.total, oz.pending, oz.queued, oz.checked, oz.laravel}
		for i, col := range zoneColumnPositions {
			res.Positions = append(res.Positions, CounterPosition{
				Counter: fmt.Sprintf("zone:%d:%s", id, col),
				Old:     oldVals[i],
				New:     newVals[i],
			})
		}
	}
	// The drift positions — only the delta-driven counters
	// (driftCounterNames):
	// the status_* ones are excluded (written by one reconciliation —
	// their "drift" is the interval freshness, not an anomaly).
	for _, name := range driftCounterNames() {
		res.Positions = append(res.Positions, CounterPosition{
			Counter: "global:" + name,
			Old:     oldCounters[name],
			New:     newCounters[name],
		})
	}
	// The daily positions (T-179/T-182): the drift is a streaming
	// difference of semantics, not an anomaly: the increments count the
	// HTTP stage result record events, the live window is the domains of
	// the day of all the branches (the DNS terminal completions —
	// not_delegated and the like — have no HTTP episode), between them —
	// the repeated episodes of the day. The temp_error numerator
	// additionally diverges by the retries that closed the error (the
	// event stayed in the history).
	// There is no steady-state zero (unlike render_pending), the positions
	// are observable. The day row did not exist yet — a fill-in, not drift
	// (Old := New): a mismatch with zero would count the in-flight DNS
	// stage writes, unrelated to the increments (the precedent — the
	// starting pass of the loop, the incremental-counters spec §7.1.3).
	dailyOld := oldDaily
	if !dailyExists {
		dailyOld = newDaily
	}
	res.Positions = append(res.Positions, CounterPosition{
		Counter: dailyChecksCounter,
		Old:     dailyOld,
		New:     newDaily,
	})
	tempOld := oldTemp
	if !tempExists {
		tempOld = newTemp
	}
	res.Positions = append(res.Positions, CounterPosition{
		Counter: dailyTempCounter,
		Old:     tempOld,
		New:     newTemp,
	})
	// The retry maturity baskets (T-202): the drift is a streaming
	// difference of semantics (the episode/retry events against the live
	// filtered domains — duplicate deliveries,
	// repeated episodes of the day, candidates bypassing the retry), not an
	// anomaly: there is no steady-state zero, the positions are observable,
	// they are not in the Warn (retryMatureStreamingPositions). No basket
	// existed — a fill-in, not drift (Old := New), the starting-pass
	// precedent (the incremental-counters spec §7.1.3).
	retryPastOld := oldRetryPast
	if !retryPastExists {
		retryPastOld = newRetryPast
	}
	res.Positions = append(res.Positions, CounterPosition{
		Counter: retryMaturePastCounter,
		Old:     retryPastOld,
		New:     newRetryPast,
	})
	retryTodayOld := oldRetryToday
	if !retryTodayExists {
		retryTodayOld = newRetryToday
	}
	res.Positions = append(res.Positions, CounterPosition{
		Counter: retryMatureTodayCounter,
		Old:     retryTodayOld,
		New:     newRetryToday,
	})
	// The hourly tempo baskets (T-203): the drift is a streaming
	// difference of semantics (the row insertion/switch-to-rendered events
	// against the live window rows
	// — duplicate deliveries, episode overwrites, re-renders), not an
	// anomaly: there is no steady-state zero, the positions are observable,
	// they are not in the Warn (hourlyStreamingPositions). No basket
	// existed — a fill-in, not drift (Old := New), the starting-pass
	// precedent (the incremental-counters spec §7.1.3).
	appendHourly := func(counter string, old int64, exists bool, newV int64) {
		if !exists {
			old = newV
		}
		res.Positions = append(res.Positions, CounterPosition{
			Counter: counter,
			Old:     old,
			New:     newV,
		})
	}
	appendHourly(hourlyRenderedPrevCounter, oldHourRenderedPrev, hourRenderedPrevExists, newHourRenderedPrev)
	appendHourly(hourlyRenderedCurCounter, oldHourRenderedCur, hourRenderedCurExists, newHourRenderedCur)
	appendHourly(hourlyResultsPrevCounter, oldHourResultsPrev, hourResultsPrevExists, newHourResultsPrev)
	appendHourly(hourlyResultsCurCounter, oldHourResultsCur, hourResultsCurExists, newHourResultsCur)
	// The reanalysis tail baskets (T-204): the same streaming semantics —
	// the transition events against the live NULL rows of the window (the
	// hourlyReanalyzePrevCounter comment); no basket existed — a fill-in,
	// not drift (Old := New), the starting-pass precedent (the
	// incremental-counters spec §7.1.3).
	appendHourly(hourlyReanalyzePrevCounter, oldHourReanalyzePrev, hourReanalyzePrevExists, newHourReanalyzePrev)
	appendHourly(hourlyReanalyzeCurCounter, oldHourReanalyzeCur, hourReanalyzeCurExists, newHourReanalyzeCur)
	return res, nil
}

// laravelAggRow — a row of the laravel_domains aggregate.
type laravelAggRow struct {
	zone    int16
	laravel int64
}

// readZoneAgg — the per-zone domainsAggSQL aggregate: the domain columns
// in order, then the statusAggCols legend, the render bit, the daily
// checks window, its temp_error numerator (T-179/T-182), the retry
// maturity baskets (T-202) and the "status hung" domains (FR-2.2). The
// receiver is a db.DB
// (the pass transaction or the verify pool: the aggregate is one, the
// point of truth — the incremental-counters spec §9).
func readZoneAgg(ctx context.Context, q db.DB) ([]zoneAggRow, error) {
	rows, err := q.Query(ctx, recountAggQuerySQL)
	if err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: агрегат: %w", err)
	}
	defer rows.Close()
	var out []zoneAggRow
	for rows.Next() {
		var r zoneAggRow
		r.statuses = make([]int64, len(statusAggCols))
		dest := make([]any, 0, 5+len(statusAggCols)+6)
		dest = append(dest, &r.zone, &r.total, &r.pending, &r.queued, &r.checked)
		for i := range r.statuses {
			dest = append(dest, &r.statuses[i])
		}
		dest = append(dest, &r.render, &r.checksToday, &r.tempToday, &r.retryToday, &r.retryPast, &r.newDoneStale)
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("monitor: реконсиляция счётчиков: агрегат: чтение: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: агрегат: %w", err)
	}
	return out, nil
}

// readLaravelAgg — the laravel_domains aggregate over zones.
func readLaravelAgg(ctx context.Context, tx pgx.Tx) ([]laravelAggRow, error) {
	rows, err := tx.Query(ctx, laravelAggQuerySQL)
	if err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: laravel-агрегат: %w", err)
	}
	defer rows.Close()
	var out []laravelAggRow
	for rows.Next() {
		var r laravelAggRow
		if err := rows.Scan(&r.zone, &r.laravel); err != nil {
			return nil, fmt.Errorf("monitor: реконсиляция счётчиков: laravel-агрегат: чтение: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: laravel-агрегат: %w", err)
	}
	return out, nil
}

// readHistoryAgg — the appeared/lost cumulatives of the detection log.
func readHistoryAgg(ctx context.Context, tx pgx.Tx) (appeared, lost int64, err error) {
	rows, err := tx.Query(ctx, historyAggSQL)
	if err != nil {
		return 0, 0, fmt.Errorf("monitor: реконсиляция счётчиков: история: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, 0, fmt.Errorf("monitor: реконсиляция счётчиков: история: пустой ответ агрегата")
	}
	if err := rows.Scan(&appeared, &lost); err != nil {
		return 0, 0, fmt.Errorf("monitor: реконсиляция счётчиков: история: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("monitor: реконсиляция счётчиков: история: %w", err)
	}
	return appeared, lost, nil
}

// hourlyAggRow — the FILTER positions of the hourly basket aggregate
// (hourlyAggSQL,
// T-203/T-204): the prev/cur of the tempo families (results — all the
// rows of the hour window,
// rendered — with the final DOM flag) and the reanalysis tails (rows
// with rules_version IS NULL).
type hourlyAggRow struct {
	prevResults, prevRendered   int64
	curResults, curRendered     int64
	prevReanalyze, curReanalyze int64
}

// readHourlyAgg — the hourly basket aggregate (T-203/T-204): a scalar
// aggregate without GROUP BY must return exactly one row (an empty
// window — zeros); an empty set — a protocol break, an error.
func readHourlyAgg(ctx context.Context, tx pgx.Tx) (hourlyAggRow, error) {
	rows, err := tx.Query(ctx, hourlyAggSQL)
	if err != nil {
		return hourlyAggRow{}, fmt.Errorf("monitor: реконсиляция счётчиков: часовые корзины: %w", err)
	}
	defer rows.Close()
	var r hourlyAggRow
	if !rows.Next() {
		return hourlyAggRow{}, fmt.Errorf("monitor: реконсиляция счётчиков: часовые корзины: пустой ответ агрегата")
	}
	if err := rows.Scan(&r.prevResults, &r.prevRendered, &r.curResults, &r.curRendered,
		&r.prevReanalyze, &r.curReanalyze); err != nil {
		return hourlyAggRow{}, fmt.Errorf("monitor: реконсиляция счётчиков: часовые корзины: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return hourlyAggRow{}, fmt.Errorf("monitor: реконсиляция счётчиков: часовые корзины: %w", err)
	}
	return r, nil
}

// writeZonePositions — overwriting the zone columns with an unnest
// batch; the return is the new column values by RETURNING.
func writeZonePositions(ctx context.Context, tx pgx.Tx, ids []int16, total, pending, queued, checked, laravel []int64, at time.Time) (map[int16]zonePositionRow, error) {
	out := make(map[int16]zonePositionRow, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := tx.Query(ctx, recountZonesSQL, ids, total, pending, queued, checked, at, laravel)
	if err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: зоны: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int16
		var r zonePositionRow
		if err := rows.Scan(&id, &r.total, &r.pending, &r.queued, &r.checked, &r.laravel); err != nil {
			return nil, fmt.Errorf("monitor: реконсиляция счётчиков: зоны: чтение: %w", err)
		}
		out[id] = r
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: зоны: %w", err)
	}
	if int64(len(out)) != int64(len(ids)) {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: зоны: перезаписано %d из %d (строки зон обязательны)", len(out), len(ids))
	}
	return out, nil
}

// writeGlobalCounters — the upsert of the global scalars; the return is
// the new values by RETURNING.
func writeGlobalCounters(ctx context.Context, tx pgx.Tx, names []string, values []int64, at time.Time) (map[string]int64, error) {
	rows, err := tx.Query(ctx, recountCountersSQL, names, values, at)
	if err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: глобальные: %w", err)
	}
	defer rows.Close()
	out := make(map[string]int64, len(names))
	for rows.Next() {
		var name string
		var v int64
		if err := rows.Scan(&name, &v); err != nil {
			return nil, fmt.Errorf("monitor: реконсиляция счётчиков: глобальные: чтение: %w", err)
		}
		out[name] = v
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: глобальные: %w", err)
	}
	if len(out) != len(names) {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: глобальные: перезаписано %d из %d строк", len(out), len(names))
	}
	return out, nil
}

// readDailyValue — the old value of a daily row (T-179 checks:,
// T-182 temp_error:) in the pass transaction: sqlQ is a point PK lookup
// with the day name computed by the DB; what is the error context. exists
// distinguishes a "0" from "the day row does not exist yet" (a fill-in is
// not drift). An empty set without a rows.Err error — an honest absence
// of the row.
func readDailyValue(ctx context.Context, tx pgx.Tx, sqlQ, what string) (value int64, exists bool, err error) {
	rows, err := tx.Query(ctx, sqlQ)
	if err != nil {
		return 0, false, fmt.Errorf("monitor: реконсиляция счётчиков: %s: %w", what, err)
	}
	defer rows.Close()
	if !rows.Next() {
		// The read failure first: a connection break is not "the row is
		// missing" (the renderDebtCount style).
		if err := rows.Err(); err != nil {
			return 0, false, fmt.Errorf("monitor: реконсиляция счётчиков: %s: %w", what, err)
		}
		return 0, false, nil
	}
	if err := rows.Scan(&value); err != nil {
		return 0, false, fmt.Errorf("monitor: реконсиляция счётчиков: %s: чтение: %w", what, err)
	}
	if err := rows.Err(); err != nil {
		return 0, false, fmt.Errorf("monitor: реконсиляция счётчиков: %s: %w", what, err)
	}
	return value, true, nil
}

// cleanupBaskets — deleting the stale baskets (DELETE … RETURNING,
// the T-202/T-203 style): retryMatureCleanupSQL/hourlyRenderedCleanupSQL/
// hourlyResultsCleanupSQL. The RETURNING value is the Query protocol (the
// pass does not use Exec), the values are not read: the live baskets
// below are overwritten by the aggregate in full, the deleted ones are
// outside the reading window.
func cleanupBaskets(ctx context.Context, tx pgx.Tx, sqlQ, what string) error {
	rows, err := tx.Query(ctx, sqlQ)
	if err != nil {
		return fmt.Errorf("monitor: реконсиляция счётчиков: %s: %w", what, err)
	}
	defer rows.Close()
	var discarded int64
	for rows.Next() {
		// The deleted basket's value is not read: the live positions are
		// rewritten by the aggregate in full.
		if err := rows.Scan(&discarded); err != nil {
			return fmt.Errorf("monitor: реконсиляция счётчиков: %s: чтение: %w", what, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("monitor: реконсиляция счётчиков: %s: %w", what, err)
	}
	return nil
}

// writeDailyCounter — overwriting a daily row with the pass aggregate
// value (recountDailyChecksSQL/recountDailyTempSQL, T-179/T-182);
// the return is the new value by RETURNING (drift). The upsert creates
// the row of the day's first pass (or fixes the one created by the
// increments — the pass is authoritative).
func writeDailyCounter(ctx context.Context, tx pgx.Tx, sqlQ string, value int64, at time.Time, what string) (int64, error) {
	rows, err := tx.Query(ctx, sqlQ, value, at)
	if err != nil {
		return 0, fmt.Errorf("monitor: реконсиляция счётчиков: %s: %w", what, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, fmt.Errorf("monitor: реконсиляция счётчиков: %s: пустой ответ upsert", what)
	}
	var fresh int64
	if err := rows.Scan(&fresh); err != nil {
		return 0, fmt.Errorf("monitor: реконсиляция счётчиков: %s: чтение: %w", what, err)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("monitor: реконсиляция счётчиков: %s: %w", what, err)
	}
	return fresh, nil
}

// globalCounters — the global scalars from the per-zone aggregate (the
// sums of the status legend and the render bit — one domains scan is
// already done)
// and the log cumulatives. The single point of truth of the "zone →
// global sums" mapping for the reconciliation and verify (the
// incremental-counters spec §9).
func globalCounters(agg []zoneAggRow, appeared, lost int64) map[string]int64 {
	out := make(map[string]int64, len(statusAggCols)+4)
	for i, c := range statusAggCols {
		out["status_"+c.status.String()] = 0
		for j := range agg {
			out["status_"+c.status.String()] += agg[j].statuses[i]
		}
	}
	var render, newDoneStale int64
	for j := range agg {
		render += agg[j].render
		newDoneStale += agg[j].newDoneStale
	}
	out["render_pending"] = render
	out["laravel_appeared_total"] = appeared
	out["laravel_lost_total"] = lost
	out["new_done_stale"] = newDoneStale
	return out
}

// zonePositionRow — the column values of one zone (old or new).
type zonePositionRow struct {
	total, pending, queued, checked, laravel int64
}

// readZonePositions — the current zone column values (in the pass
// transaction: before the statement — the old ones, after — its own
// writes).
func readZonePositions(ctx context.Context, tx pgx.Tx, zone *int16) (map[int16]zonePositionRow, error) {
	rows, err := tx.Query(ctx, readZonesSQL, zone)
	if err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: чтение зон: %w", err)
	}
	defer rows.Close()
	out := make(map[int16]zonePositionRow)
	for rows.Next() {
		var id int16
		var r zonePositionRow
		if err := rows.Scan(&id, &r.total, &r.pending, &r.queued, &r.checked, &r.laravel); err != nil {
			return nil, fmt.Errorf("monitor: реконсиляция счётчиков: чтение зон: строка: %w", err)
		}
		out[id] = r
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: чтение зон: %w", err)
	}
	return out, nil
}

// readCounterValues — the current global counter values by names
// (a missing row — 0: the first pass after a migration fills it,
// the incremental-counters spec §5).
func readCounterValues(ctx context.Context, tx pgx.Tx, names []string) (map[string]int64, error) {
	rows, err := tx.Query(ctx, readCountersSQL, names)
	if err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: чтение счётчиков: %w", err)
	}
	defer rows.Close()
	out := make(map[string]int64, len(names))
	for rows.Next() {
		var name string
		var v int64
		if err := rows.Scan(&name, &v); err != nil {
			return nil, fmt.Errorf("monitor: реконсиляция счётчиков: чтение счётчиков: строка: %w", err)
		}
		out[name] = v
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("monitor: реконсиляция счётчиков: чтение счётчиков: %w", err)
	}
	return out, nil
}

// ZoneStatsLoop — the periodic reconciliation loop (spec 5.9, the
// incremental-counters spec §7.2):
// ZoneStatsOnce by ticker, the first pass right at start (fresh counters
// after a downtime/restart and the zero fill-in after a migration —
// hence the first pass does not count as drift: no Warn/gauge is raised
// on it, the incremental-counters spec §7.1.3), it does not wait for a
// tick. A pass error interrupts only that pass: a Warn is logged, the
// loop lives (the OnRun contract, T-012) —
// the next pass will retry against the actual state. A ctx cancellation —
// a clean exit with nil (graceful shutdown, spec 3.4). The pass result is
// Debug, not Info: the pass succeeds whenever the DB is alive.
//
// Single-flight (T-161): the passes run in the loop's own goroutine —
// a new pass structurally cannot start while the previous one has not
// finished; a pass longer than the interval (table growth, measurement
// §12) resets the accumulated ticker tick (otherwise a new pass would
// start right behind the slow one — a continuous aggregate), a Warn and
// the crawler_zonestats_skipped_ticks_total counter raise the noise. reg
// is the service metric registry (the pass and drift metrics; nil — the
// loop without metrics,
// the recount lives: the loop's purpose is reconciliation, not
// instruments).
func ZoneStatsLoop(ctx context.Context, pool db.DB, reg *metrics.Registry, opts ZoneStatsOptions) error {
	if pool == nil {
		return errors.New("monitor: реконсиляция счётчиков: пул не задан")
	}
	txPool, ok := pool.(ZoneStatsDB)
	if !ok {
		return errors.New("monitor: реконсиляция счётчиков: пул без транзакций (ZoneStatsDB)")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	m := newZoneStatsLoopMetrics(reg)
	log.Info("цикл реконсиляции счётчиков конвейера",
		"interval", o.Interval.String())

	// The ticker before the starting pass: the ticks are counted from the
	// loop start, draining an accumulated tick in pass sees the same
	// ticker.
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	first := true
	pass := func() {
		started := time.Now()
		res, err := ZoneStatsOnce(ctx, txPool, o)
		switch {
		// The branch body is empty (only a comment): branch/case would
		// replace it with a noop EmptyStmt whose print is identical to the
		// original — a mutant with an empty diff that no test can kill;
		// suppressing the generation is the only alternative to the checksum
		// blacklist (§4).
		// mutator-disable-next-line branch/case
		case err != nil && ctx.Err() != nil:
			// A cancellation during the pass — part of the shutdown, not an alarm.
		case err != nil:
			log.Warn("проход реконсиляции счётчиков", "err", err)
		default:
			reportDrift(log, m, res, first)
			// The "status hung" gauge (FR-2.2) — a level, not a drift:
			// it is set by every successful pass, including the first
			// filling one (steady state — an honest zero).
			m.setNewDoneStale(res.NewDoneStale)
			log.Debug("проход реконсиляции счётчиков", "zones", res.Zones)
			// "First" is cleared only by a successful pass (review P3-5):
			// a failing start (the DB not ready yet) did not fill the
			// counters —
			// the next pass is still "filling", its difference is not
			// drift (the incremental-counters spec §7.1.3).
			first = false
		}
		took := time.Since(started)
		m.observe(took.Seconds())
		if took > o.Interval {
			// Single-flight: reset the tick accumulated during the slow
			// pass — otherwise a new pass would start right behind this
			// one (select/default — there may be no tick: the Warn is honest
			// about the duration, the drain is a no-op in that case).
			select {
			case <-ticker.C:
			default:
			}
			log.Warn("проход реконсиляции дольше интервала — накопившийся тик пропущен",
				"took", took.String(), "interval", o.Interval.String())
			m.skip()
		}
	}

	pass()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pass()
		}
	}
}

// reportDrift — the drift observability (the incremental-counters spec
// §7.1.3): a gauge of each position
// (steady state — 0), a Warn on a non-zero drift of any position — EXCEPT
// the daily rows (T-179/T-182), the retry maturity baskets (T-202) and
// the hourly tempo baskets (T-203): their "drift" is a streaming
// difference of semantics
// (the HTTP record events against the domains of the day of all the
// branches), non-zero under live load almost always — a Warn on every
// pass (every 30 min) would bury the signal
// of the other positions under the noise (a deviation from §8 of
// TZ_slow_done_today, written under the assumption "drift = repeats, the
// share is small";
// the positions' observability — gauge). The first pass of the loop
// (fillFirst) is not observed at all: after a migration/downtime it fills
// in the zeros — the old values are not a drift but the absence of the
// counters (the incremental-counters spec §5); no metrics are raised on
// it, from the second pass the gauge exposes all the positions
// (the zero ones included). The Warn aggregates: a pass over 2 k zones
// with a full drift (a restart on zeroed counters) must not flood the
// log —
// the position counter, the |Δ| sum and the first five names; the full
// list — crawlerctl counters verify (the incremental-counters spec §9).
func reportDrift(log *slog.Logger, m *zoneStatsLoopMetrics, res ZoneStatsResult, fillFirst bool) {
	if fillFirst {
		return
	}
	if m != nil {
		for _, p := range res.Positions {
			m.drift.WithLabelValues(p.Counter).Set(float64(p.Drift()))
		}
	}
	streaming := append(append(dailyStreamingPositions(), retryMatureStreamingPositions()...), hourlyStreamingPositions()...)
	var drifted []CounterPosition
	var sum int64
	for _, p := range res.Positions {
		if slices.Contains(streaming, p.Counter) || p.Drift() == 0 {
			continue
		}
		drifted = append(drifted, p)
		sum += p.Drift()
	}
	if len(drifted) == 0 {
		return
	}
	top := make([]string, 0, 5)
	for i, p := range drifted {
		if i == 5 {
			break
		}
		top = append(top, fmt.Sprintf("%s: %d → %d", p.Counter, p.Old, p.New))
	}
	log.Warn("реконсиляция: дрейф счётчиков (инкременты разошлись с агрегатом; следующий проход уже перезаписал)",
		"positions", len(drifted), "sum_abs", sum, "top", top)
}

// zoneStatsLoopMetrics — the instruments of the reconciliation loop
// (T-161/T-178):
// the pass duration, the skipped tick counter, the drift gauge and the
// "status hung" domains gauge (FR-2.2, T-256); a nil receiver is a no-op
// (a loop without a registry, the scheduler.backlogLoopMetrics style).
type zoneStatsLoopMetrics struct {
	seconds prometheus.ObserverVec
	skipped *prometheus.CounterVec
	drift   *prometheus.GaugeVec
	// newDoneStale — the value of the pass's new_done_stale row (a level,
	// not drift): it is set by every successful pass, including the first
	// filling one (unlike the drift — the zero norm and there is nothing
	// to "fill").
	newDoneStale *prometheus.GaugeVec
}

// newZoneStatsLoopMetrics — the instruments in the reg registry (nil —
// nil). The buckets are an exponent of 0.5 s…~8.5 min: a pass is an
// aggregate over all domains (measurement
// §12 — tens of seconds on 47 M rows), the default Prometheus buckets
// (a 10 s cap) are small. crawler_counters_recount_seconds — a rename of
// crawler_zonestats_pass_seconds (T-178, the incremental-counters spec
// §7.2: the pass is combined), the buckets unchanged.
func newZoneStatsLoopMetrics(reg *metrics.Registry) *zoneStatsLoopMetrics {
	if reg == nil {
		return nil
	}
	return &zoneStatsLoopMetrics{
		seconds: reg.MustHistogram(prometheus.HistogramOpts{
			Name:    "crawler_counters_recount_seconds",
			Help:    "Длительность совмещённого прохода реконсиляции счётчиков (агрегат domains/laravel_domains/laravel_history → zones + crawler_counters, T-078/T-178).",
			Buckets: prometheus.ExponentialBuckets(0.5, 2, 11),
		}),
		skipped: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_zonestats_skipped_ticks_total",
			Help: "Пропущенные тики цикла реконсиляции: проход длился дольше интервала, накопившийся тик не запускает новый проход (single-flight, T-161).",
		}),
		drift: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_counters_drift",
			Help: "Дрейф инкрементальных счётчиков на проходе реконсиляции: |старое − новое| по позициям (zone:<id>:колонка, global:<имя>; daily:checks и daily:temp_error — суточный темп проверок и его temp_error-числитель, T-179/T-182). Стационар — 0, устойчивый ненулевой дрейф — пропущенный переход или ручной SQL мимо кода (разбор — docs/runbooks.md, T-178); исключения — потоковые позиции (считают события против живых доменов/строк, ненулевые под живой нагрузкой — наблюдаемая величина, в Warn не входят): суточные daily:* (T-179/T-182), корзины зрелости ретрая retry_mature:past/retry_mature:today (T-202), часовые корзины темпа rendered:prev/rendered:cur/results:prev/results:cur (T-203 — prev/cur: предыдущий и текущий календарный час) и хвостов переанализа reanalyze:prev/reanalyze:cur (T-204).",
		}, "counter"),
		newDoneStale: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_domains_new_done_stale",
			Help: "Домены «статус повис» (ФТ-2.2 ТЗ-fsm, T-256): status = new ∧ check_state = done ∧ last_checked_at старше суток — DNS-этап записал ветку «есть адреса», HTTP-итог не пришёл (потеря задачи q:http). Значение — FILTER-колонка прохода реконсиляции (строка crawler_counters new_done_stale, без seq scan по domains), обновляется раз в monitor.zone_stats_interval. Стационарно 0 в норме; ненулевое — алерт domains_new_done_stale (стрик 2 прохода): когорту лечит компенсатор T-270 (ранний recheck new∧done, crawler_scheduler_stale_requeued_total; спецификация — internal/model/enums.go), gauge — сигнал о новых потерях.",
		}),
	}
}

// observe — recording the pass duration; nil — a no-op.
func (m *zoneStatsLoopMetrics) observe(seconds float64) {
	if m == nil {
		return
	}
	m.seconds.WithLabelValues().Observe(seconds)
}

// skip — a skipped tick; nil — a no-op.
func (m *zoneStatsLoopMetrics) skip() {
	if m == nil {
		return
	}
	m.skipped.WithLabelValues().Inc()
}

// setNewDoneStale — sets the "status hung" domains gauge to the value of
// a successful pass (FR-2.2); nil — a no-op.
func (m *zoneStatsLoopMetrics) setNewDoneStale(n int64) {
	if m == nil {
		return
	}
	m.newDoneStale.WithLabelValues().Set(float64(n))
}

// ---------------------------------------------------------------- verify

// verifyZonesSQL — cross-checking the zone columns against the live
// aggregate without writing
// (the incremental-counters spec §9): the stored values next to the
// aggregate; COALESCE — a zone without
// live domains is outside the aggregate. $1 — the zone filter (NULL —
// all).
var verifyZonesSQL = "SELECT z.id, " +
	"z.domains_total, COALESCE(a.total, 0), " +
	"z.domains_pending, COALESCE(a.pending, 0), " +
	"z.domains_queued, COALESCE(a.queued, 0), " +
	"z.domains_checked, COALESCE(a.checked, 0), " +
	"z.domains_laravel, COALESCE(l.laravel, 0) " +
	"FROM zones AS z " +
	"LEFT JOIN (" + domainsAggSQL + ") AS a ON a.zone_id = z.id " +
	"LEFT JOIN (SELECT zone_id, count(*) AS laravel FROM laravel_domains GROUP BY zone_id) AS l ON l.zone_id = z.id " +
	"WHERE ($1::int2 IS NULL OR z.id = $1) ORDER BY z.id"

// VerifyOptions — the cross-check options; used by crawlerctl counters
// verify (the incremental-counters spec §9) and by the integration tests
// (the ready verify logic,
// the incremental-counters spec §12.2).
type VerifyOptions struct {
	// Zone is the zone filter (nil — all zones; the global counters are
	// always checked in full).
	Zone *int16
	// Tolerance is the mismatch tolerance (0 by default — the counters
	// must converge exactly; > 0 — for a check under live load, when
	// increments happen between reading the stored value and the live
	// aggregate).
	Tolerance int64
}

// VerifyCounters cross-checks the counters against the live DB aggregates
// WITHOUT writing
// (the incremental-counters spec §9): the stored zones/crawler_counters
// values against the
// domains/laravel_domains/laravel_history aggregates (the same per-zone
// aggregate and the global sums function as the reconciliation's — one
// point of truth, the incremental-counters spec §9). It returns the
// positions with a mismatch above the tolerance
// (Old — the stored one, New — the live aggregate); an empty slice — the
// counters converge. The snapshot is not atomic between the stored and
// the live read —
// under live load the single mismatches at the transition tempo are
// closed by the tolerance.
func VerifyCounters(ctx context.Context, pool db.DB, opts VerifyOptions) ([]CounterPosition, error) {
	if pool == nil {
		return nil, errors.New("monitor: сверка счётчиков: пул не задан")
	}

	var out []CounterPosition

	rows, err := pool.Query(ctx, verifyZonesSQL, opts.Zone)
	if err != nil {
		return nil, fmt.Errorf("monitor: сверка счётчиков: зоны: %w", err)
	}
	for rows.Next() {
		var id int16
		var vals [10]int64
		dest := make([]any, len(vals)+1)
		dest[0] = &id
		for i := range vals {
			dest[i+1] = &vals[i]
		}
		if err := rows.Scan(dest...); err != nil {
			rows.Close()
			return nil, fmt.Errorf("monitor: сверка счётчиков: зоны: чтение: %w", err)
		}
		for i, col := range zoneColumnPositions {
			old, fresh := vals[2*i], vals[2*i+1]
			if absDiff(old, fresh) > opts.Tolerance {
				out = append(out, CounterPosition{
					Counter: fmt.Sprintf("zone:%d:%s", id, col),
					Old:     old,
					New:     fresh,
				})
			}
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("monitor: сверка счётчиков: зоны: %w", err)
	}
	rows.Close()

	// The live global scalars: the same aggregate and the same sums
	// function as the reconciliation's (one point of truth of the
	// mapping).
	agg, err := readZoneAgg(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("monitor: сверка счётчиков: глобальные: %w", err)
	}
	var appeared, lost int64
	{
		hrows, err := pool.Query(ctx, historyAggSQL)
		if err != nil {
			return nil, fmt.Errorf("monitor: сверка счётчиков: глобальные: история: %w", err)
		}
		if !hrows.Next() {
			hrows.Close()
			return nil, fmt.Errorf("monitor: сверка счётчиков: глобальные: история: пустой ответ агрегата")
		}
		if err := hrows.Scan(&appeared, &lost); err != nil {
			hrows.Close()
			return nil, fmt.Errorf("monitor: сверка счётчиков: глобальные: история: чтение: %w", err)
		}
		if err := hrows.Err(); err != nil {
			hrows.Close()
			return nil, fmt.Errorf("monitor: сверка счётчиков: глобальные: история: %w", err)
		}
		hrows.Close()
	}
	live := globalCounters(agg, appeared, lost)
	stored, err := readStoredCounters(ctx, pool)
	if err != nil {
		return nil, err
	}
	for _, name := range driftCounterNames() {
		// Only the delta-driven scalars: the status distribution is written
		// only by the reconciliation (the incremental-counters spec §13.3) —
		// a "stored vs live" check under load diverges by the status tempo,
		// that is
		// the interval freshness, not drift. The daily checks:/temp_error:
		// rows (T-179/T-182), the retry maturity baskets retry_mature:
		// (T-202) and
		// the hourly tempo baskets results:/rendered: (T-203) and the
		// reanalysis tails reanalyze: (T-204) are also outside the check:
		// the increments
		// count events (result records, switches to rendered,
		// version stamps), the live aggregate — the window rows; under load
		// the mismatch equals the flow difference since the last pass —
		// observed by the daily:*/retry_mature:*/rendered:* /
		// results:*/reanalyze:* positions of the reconciliation pass, a
		// check with a 0 tolerance would flag routine work (the
		// retry/tempo/tail baskets additionally keep only two live rows per
		// family and
		// fold the old ones, a by-name date/hour check is undefined).
		if absDiff(stored[name], live[name]) > opts.Tolerance {
			out = append(out, CounterPosition{
				Counter: "global:" + name,
				Old:     stored[name],
				New:     live[name],
			})
		}
	}
	return out, nil
}

// readStoredCounters — the stored crawler_counters rows (the check:
// a missing row — 0).
const storedCountersSQL = "SELECT name, value FROM crawler_counters"

func readStoredCounters(ctx context.Context, pool db.DB) (map[string]int64, error) {
	rows, err := pool.Query(ctx, storedCountersSQL)
	if err != nil {
		return nil, fmt.Errorf("monitor: сверка счётчиков: сохранённые: %w", err)
	}
	defer rows.Close()
	out := make(map[string]int64)
	for rows.Next() {
		var name string
		var v int64
		if err := rows.Scan(&name, &v); err != nil {
			return nil, fmt.Errorf("monitor: сверка счётчиков: сохранённые: чтение: %w", err)
		}
		out[name] = v
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("monitor: сверка счётчиков: сохранённые: %w", err)
	}
	return out, nil
}

// absDiff returns |a − b|.
func absDiff(a, b int64) int64 {
	if a > b {
		return a - b
	}
	return b - a
}

// PostgreSQL slow query detection (T-177, spec 5.9): the
// monitor.slow_query_interval loop (default 10 s) polls
// pg_stat_activity — client statements running longer than
// monitor.slow_query_min_duration (default 3 s — the production
// log_min_duration_statement threshold, deploy/postgres/postgresql.conf)
// go out in one Telegram message (the Notify hook —
// TelegramNotifier.Send, the same delivery as the T-162 alerts and the
// T-173 load report; delivery is enabled by the
// monitor.slow_query_telegram config — off by default, Notify nil),
// duplicated by Warns in the monitor log and by the
// crawler_pg_slow_queries* metrics.
//
// The two observability surfaces do not duplicate each other:
//
//   - the server log (log_min_duration_statement) — the complete
//     post-mortem trace: every statement above the threshold with its
//     text and duration, it misses nothing, but the operator does not
//     read it;
//   - this loop — the live signal: a query running right now longer
//     than the threshold. The poll is an instant snapshot, not an event
//     stream — episodes shorter than the loop interval are caught
//     partially (3–10 s at a 10 s period), longer ones — always: the
//     pathology of this workload (the pipeline's point index queries)
//     starts at tens of seconds, 3 s — the sensitivity to the very fact
//     of degradation.
//
// The debounce key is the pair (pid, query_start): a statement is
// reported once at first observation; while it runs it stays silent in
// the following passes. query_start is unique to the statement's start
// (a re-execution — a new value); a finished statement that disappeared
// from the snapshot leaves the debounce base: the map holds only
// currently running ones (dozens of records at most — the connection
// limit is 50). A source failure does not drop the base: returning a
// snapshot does not re-report what has not finished.
//
// The monitor's own passes are excluded by the pool's application_name
// ("monitor", internal/service): the reconciliation — an UPDATE
// aggregate over 47 M rows (measurement §12, tens of seconds every
// 30 min, T-199) — is a routine known load, not a pathology; the alert
// passes (every 5 min, T-206) and the render debt ones — point reads
// (T-178/T-182). Foreign
// services are tagged with their own application_name and are read in
// the message by the label.
// The DB role needs pg_read_all_stats (deploy.md §4.1): without the
// grant, foreign sessions are visible but the query text is hidden.
package monitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/db"
	"crawler/internal/metrics"
)

// DefaultSlowQueryInterval — the pg_stat_activity poll period: 10 s.
// A view snapshot is microseconds (≤ 50 connections), the frequency
// costs nothing; longer — episodes shorter than the interval are lost.
const DefaultSlowQueryInterval = 10 * time.Second

// DefaultSlowQueryMinDuration — the slow query threshold: 3 s, the
// production log_min_duration_statement threshold
// (deploy/postgres/postgresql.conf): one "pathology" boundary in the
// server log and in Telegram.
const DefaultSlowQueryMinDuration = 3 * time.Second

// slowQueryLimit — the row cap of one snapshot (max_connections = 50 —
// there are never more; the limit guards against a bloated message).
const slowQueryLimit = 20

// slowQueryTopLines — the number of query lines in the message: the
// three longest, the tail — "…and N more". A lock convoy is visible by
// the number, the text of the top ones leads to the cause, the group
// does not drown in walls of text.
const slowQueryTopLines = 3

// slowQueryMaxChars — the truncation of the query text in the message:
// the beginning of the statement (UPDATE "domains" …) is enough for
// identification, the full text — in the server log; the 4096-character
// Telegram limit is not approached.
const slowQueryMaxChars = 160

// SlowQuery is a foreign session statement running longer than the threshold.
type SlowQuery struct {
	// PID is the PostgreSQL backend (pg_cancel_backend/pg_terminate_backend
	// are addressed by the operator at exactly this one).
	PID int
	// App is the session's application_name (the pipeline service label;
	// empty — a connection without a label: psql, migrations with their
	// own tag).
	App string
	// Seconds is the statement's age at the snapshot moment (now() − query_start).
	Seconds float64
	// StartedAt is query_start: the statement's start moment, half of the
	// debounce key (unique to the execution).
	StartedAt time.Time
	// Query is the statement's text (NULL without pg_read_all_stats — an
	// empty string, the message honestly shows "text unavailable").
	Query string
}

// key — the statement execution identifier for the debounce.
func (q SlowQuery) key() string {
	return strconv.Itoa(q.PID) + "@" + strconv.FormatInt(q.StartedAt.UnixNano(), 10)
}

// slowQuerySQL — a snapshot of the client statements running longer than
// the threshold, the longest first. Its own PID is excluded (the poll
// itself is an active query);
// $1 — the monitor pool's application_name (empty — the filter off),
// $2 — the threshold in seconds, $3 — the row cap.
const slowQuerySQL = "SELECT pid, COALESCE(application_name, ''), " +
	"EXTRACT(epoch FROM now() - query_start), query_start, COALESCE(query, '') " +
	"FROM pg_stat_activity " +
	"WHERE backend_type = 'client backend' " +
	"AND state = 'active' " +
	"AND query_start IS NOT NULL " +
	"AND pid <> pg_backend_pid() " +
	"AND ($1 = '' OR application_name <> $1) " +
	"AND EXTRACT(epoch FROM now() - query_start) > $2 " +
	"ORDER BY now() - query_start DESC " +
	"LIMIT $3"

// SlowQuerySource is a slow queries source; an interface for
// substitution in unit tests (the real read is pg_stat_activity of a
// live PostgreSQL). LoadSource/UnitEventsSource style.
type SlowQuerySource interface {
	// SlowQueries returns the statements running longer than longerThan.
	// An error — "not measured", with the reason attached.
	SlowQueries(ctx context.Context, longerThan time.Duration) ([]SlowQuery, error)
}

// PGSlowQueries is a SlowQuerySource on top of the monitor DB pool: one
// SELECT pg_stat_activity per pass.
type PGSlowQueries struct {
	// Pool is the DB pool; nil — a source error.
	Pool db.DB
	// ExcludeApp is the application_name of the monitoring connections
	// (its own aggregates are a routine load, not a pathology);
	// "" — do not exclude.
	ExcludeApp string
	// Limit is the snapshot row cap (0 — slowQueryLimit).
	Limit int
}

// compile-time: PGSlowQueries is a SlowQuerySource.
var _ SlowQuerySource = PGSlowQueries{}

// SlowQueries captures the statements running longer than the threshold.
// A NULL text (no pg_read_all_stats) is read as an empty string: the
// duration and the session label are honest, the text is marked
// unavailable in the message.
func (s PGSlowQueries) SlowQueries(ctx context.Context, longerThan time.Duration) ([]SlowQuery, error) {
	if s.Pool == nil {
		return nil, errors.New("monitor: slow queries: the pool is not set")
	}
	limit := s.Limit
	if limit <= 0 {
		limit = slowQueryLimit
	}
	rows, err := s.Pool.Query(ctx, slowQuerySQL, s.ExcludeApp, longerThan.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("monitor: slow queries: %w", err)
	}
	defer rows.Close()
	var out []SlowQuery
	for rows.Next() {
		var q SlowQuery
		if err := rows.Scan(&q.PID, &q.App, &q.Seconds, &q.StartedAt, &q.Query); err != nil {
			return nil, fmt.Errorf("monitor: slow queries: scan: %w", err)
		}
		out = append(out, q)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("monitor: slow queries: %w", err)
	}
	return out, nil
}

// SlowQueryOptions is the loop options; Interval <= 0 disables the loop
// (the LoadReportOptions style: the poll frequency has no "correct"
// value — an operator's choice, the monitor.slow_query_interval config).
type SlowQueryOptions struct {
	// Interval is the poll period (DefaultSlowQueryInterval).
	Interval time.Duration
	// MinDuration is the slow query threshold
	// (DefaultSlowQueryMinDuration; 0 — the default).
	MinDuration time.Duration
	// Log is the loop logger; nil — records suppressed (slog.DiscardHandler).
	Log *slog.Logger
	// Metrics is the service registry; nil — no metrics.
	Metrics *metrics.Registry
	// Notify is the message recipient (the Telegram delivery of
	// T-162/T-173); nil — the log only. Called in the pass goroutine —
	// it must be safe for concurrent calls (TelegramNotifier.Send).
	Notify func(text string)
}

// withDefaults replaces a non-positive threshold with the default;
// Interval is checked by the loop itself (0 — off) — there is no
// default here.
func (o SlowQueryOptions) withDefaults() SlowQueryOptions {
	if o.MinDuration <= 0 {
		o.MinDuration = DefaultSlowQueryMinDuration
	}
	return o
}

// SlowQueryLoop is the slow queries loop (T-177): a pass right at start
// (a query already running longer than the threshold at the moment of
// the monitor restart — an event, not a state: it is reported) and then
// by ticker. A source error — Warn, the loop keeps living (the OnRun
// contract, spec 3.4): the snapshot will repeat on the next pass, the
// debounce base is not dropped. A ctx cancellation — a clean nil
// (graceful shutdown).
func SlowQueryLoop(ctx context.Context, src SlowQuerySource, opts SlowQueryOptions) error {
	if src == nil {
		return errors.New("monitor: slow queries: the source is not set")
	}
	if opts.Interval <= 0 {
		return nil // disabled by configuration (monitor.slow_query_interval)
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	m := newSlowQueryMetrics(o.Metrics)
	log.Info("slow queries loop",
		"interval", o.Interval.String(), "threshold", o.MinDuration.String())

	// seen — currently running (the pid@query_start key): a finished
	// statement leaves the snapshot and the base at once, the map's
	// memory — dozens of records at the peak. The passes are sequential
	// in the loop goroutine — no mutex needed (the loadPassState style).
	seen := make(map[string]struct{})
	pass := func() {
		queries, err := src.SlowQueries(ctx, o.MinDuration)
		if err != nil {
			if ctx.Err() == nil {
				log.Warn("slow queries pass", "err", err)
			}
			return
		}
		cur := make(map[string]struct{}, len(queries))
		fresh := make([]SlowQuery, 0, len(queries))
		for _, q := range queries {
			k := q.key()
			cur[k] = struct{}{}
			if _, ok := seen[k]; !ok {
				fresh = append(fresh, q)
			}
		}
		seen = cur

		m.observe(len(queries), fresh)
		if len(fresh) == 0 {
			return
		}
		report := formatSlowQueryReport(fresh, len(queries), o.MinDuration)
		log.Warn("slow queries",
			"running", len(queries), "new", len(fresh),
			"report", strings.ReplaceAll(report, "\n", " | "))
		if o.Notify != nil {
			o.Notify(report)
		}
	}

	pass()
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pass()
		}
	}
}

// formatSlowQueryReport — the message to the operator (Telegram/log):
// a header with the number of new ones and the threshold (ones
// continuing from previous passes — the "total in flight" addition),
// the slowQueryTopLines longest lines "duration [service] pid: text",
// the tail — "…and N more". The snapshot is already sorted by the server
// by duration; fresh is its new elements, the order is preserved.
func formatSlowQueryReport(fresh []SlowQuery, total int, threshold time.Duration) string {
	var b strings.Builder
	fmt.Fprintf(&b, "🐌 PostgreSQL: %d %s (threshold %ss)",
		len(fresh), slowQueriesPhrase(len(fresh)), slowDurString(threshold))
	if total > len(fresh) {
		fmt.Fprintf(&b, ", total in flight %d", total)
	}
	b.WriteByte('\n')
	for i, q := range fresh {
		if i == slowQueryTopLines {
			fmt.Fprintf(&b, "…and %d more\n", len(fresh)-i)
			break
		}
		fmt.Fprintf(&b, "• %.1fs", q.Seconds)
		if q.App != "" {
			fmt.Fprintf(&b, " [%s]", q.App)
		}
		fmt.Fprintf(&b, " pid %d: %s\n", q.PID, slowQueryText(q.Query))
	}
	return b.String()
}

// slowQueriesPhrase — the Russian plural agreement of the slow-query
// phrase with the count (the three returned variants are the string
// literals in the body below).
func slowQueriesPhrase(n int) string {
	switch {
	case n%10 == 1 && n%100 != 11:
		return "slow query"
	case n%10 >= 2 && n%10 <= 4 && (n%100 < 10 || n%100 >= 20):
		return "slow queries"
	default:
		return "slow queries"
	}
}

// slowDurString — the threshold duration as whole seconds without a
// fractional tail (a bare number, not one with trailing zeros);
// non-whole durations — as is.
func slowDurString(d time.Duration) string {
	sec := d.Seconds()
	if sec == float64(int64(sec)) {
		return strconv.FormatInt(int64(sec), 10)
	}
	return strconv.FormatFloat(sec, 'g', -1, 64)
}

// slowQueryText — the statement text as one line: spaces collapsed,
// a tail longer than slowQueryMaxChars truncated with an ellipsis (the
// full text — the server log). An empty text (no pg_read_all_stats) —
// the marker.
func slowQueryText(q string) string {
	if strings.TrimSpace(q) == "" {
		return "(text unavailable — no pg_read_all_stats)"
	}
	q = strings.Join(strings.Fields(q), " ")
	if r := []rune(q); len(r) > slowQueryMaxChars {
		return string(r[:slowQueryMaxChars]) + "…"
	}
	return q
}

// slowQueryMetrics is the loop instruments: the current slow ones gauge
// (the pass snapshot; hold-last on a source failure — the psimetrics
// style) and the discovered counter with the service label; a nil
// receiver is a no-op (a loop without a registry, the
// zoneStatsLoopMetrics style).
type slowQueryMetrics struct {
	current *prometheus.GaugeVec
	total   *prometheus.CounterVec
}

// slowQueryAppUnknown — the app label of connections without an application_name.
const slowQueryAppUnknown = "unknown"

// newSlowQueryMetrics — the instruments in the reg registry (nil — nil).
func newSlowQueryMetrics(reg *metrics.Registry) *slowQueryMetrics {
	if reg == nil {
		return nil
	}
	return &slowQueryMetrics{
		current: reg.MustGauge(prometheus.GaugeOpts{
			Name: "crawler_pg_slow_queries",
			Help: "PostgreSQL client statements running longer than monitor.slow_query_min_duration at the moment of the loop pass (pg_stat_activity, T-177); monitor's own aggregates are excluded.",
		}),
		total: reg.MustCounter(prometheus.CounterOpts{
			Name: "crawler_pg_slow_queries_total",
			Help: "Detected slow queries by the service application_name (new statements above the threshold, the pid+query_start debounce, T-177).",
		}, "app"),
	}
}

// observe publishes the pass snapshot; nil — a no-op.
func (m *slowQueryMetrics) observe(current int, fresh []SlowQuery) {
	if m == nil {
		return
	}
	m.current.WithLabelValues().Set(float64(current))
	for _, q := range fresh {
		app := q.App
		if app == "" {
			app = slowQueryAppUnknown
		}
		m.total.WithLabelValues(app).Inc()
	}
}

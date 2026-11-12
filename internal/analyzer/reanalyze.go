// Package analyzer analyzes saved content against the Laravel rules
// (spec 5.8): passive, over already downloaded pages, cookies and headers.
// Consuming q:analyze and writing laravel_findings — T-071 (worker.go).
//
// This file is re-analysis (T-074, spec 5.8 "Re-analysis": "a background
// task finds rows with a stale rule version and puts them into q:analyze;
// no re-downloading is needed"; the §11.4 item 4 acceptance — "a rule
// change + reanalyze updates the results without re-downloading").
//
// The staleness criterion is rules_version IS DISTINCT FROM the current
// set version (rules.Set.Version, T-068), not "<>": NULL ("never
// analyzed" — rows written before T-071 and new T-058 check episodes)
// would drop out of the comparison NULL <> x = NULL. The §13.1 guard
// keeps deleted domains out of the loop (status <> deleted, the
// scheduler T-030 select style; T-020 purges their results). Disabled
// zones are not filtered: per §5.10/§4.1 disabling a zone stops new
// checks; re-analysis of already saved results touches no network and
// breaks no rules.
//
// No re-downloading happens: a Result message (T-024) carries only the
// row key (domain, subdomain, variant) and the name for logs; the
// T-071 worker takes the content from the store by the key.
//
// Draining in batches with cursor pagination — TWO forms (T-193, the
// diagnosis of slow-query log record #12 of 10/25: an hourly tick with
// a zero PK cursor ran a Parallel Seq Scan of the whole
// subdomain_results — 4.8M rows, 19.9s — for a handful of fresh tails).
// A q:analyze publish does not change the row: the worker commits
// rules_version asynchronously; a "from the beginning" re-select would
// take the same batch of in-flight rows and duplicate it on every
// iteration. Therefore:
//
//   - the PK form (reanalyzeSelectSQL) — the FULL drain of the stale set
//     (ReanalyzeDrain): a rule change (Subscribe, T-068) and crawlerctl
//     reanalyze; it is rare and must cover the whole set from scratch —
//     the seq scan over the non-indexable rules_version (the HOT
//     invariant, the T-104/T-175 class) is deliberately complete;
//   - the time form (reanalyzeTickSQL) — the hourly tick: the candidate
//     set is a fresh slice (new T-058 episodes with a fresh checked_at
//     and a NULL version; tails whose publish lagged or got lost),
//     the composite (checked_at, PK) cursor relies on the
//     subdomain_results_checked_idx index (T-171) — a pass reads only
//     the window ahead of the cursor; the cost is ∝ the inflow, not
//     the table size (the precedent is the T-185 retention cursor).
//
// The tick guard by tail buckets (T-204, reanalyzeGuardSQL): even an
// hour is 84k rows (an inflow of 2–3M/day, the T-201 spec §2.1) and a
// heap visit of each for the non-indexable rules_version cost 5–12s
// in steady state and 86–94s in the midnight storm (a CPU/IO starvation
// multiplier, PSI ~80%) — the T-193 acceptance criterion "the tick
// disappears from the slow-query log" was not met. As with the retry
// (T-191 → T-202: the status_temp_error counter did not tell mature
// candidates from immature ones), a naive "written without analysis"
// counter does not tell TAILS (lost publishes) from the IMMATURE
// in-flight flow (a write → q:analyze → stamp takes seconds, an
// inflow of 23–35 rows/s keeps it non-zero always) — a guard over it
// would be open on every tick. Maturity comes from the HOURLY
// crawler_counters ['reanalyze:YYYY-MM-DD"T"HH24'] buckets (the
// T-202/T-203 family): the guard sums the buckets OLDER than the
// current hour — a fresh in-flight flow lives in the current hour's
// bucket and does not open the guard; a tail that survived the hour
// boundary is visible. A zero sum ⇒ the heavy select does not run at
// all (the retryMatureSQL style); a negative one is an event-vs-rows
// drift (decrements of old-version stamps during a drain, duplicate
// deliveries) — the guard stays silent; the monitor reconciliation evens the buckets out with its own pass.
//
// Duplicates between passes are bounded (the in-flight window) and are
// absorbed by the consumer's per-key idempotency — like T-066.
//
// Backpressure — WatermarkPublisher (T-026): a publish waits for the
// q:analyze capacity (the shared scheduler.queue_high_watermark
// threshold); a ctx cancellation is a clean exit. What was published
// before a failure is not rolled back (at-least-once, §3.4): a
// repeated pass publishes it again — the duplicate is bounded.
//
// The contract with the T-071 worker: for every processed Result the
// worker commits subdomain_results.rules_version = the set version —
// including rows without saved content ("nothing to analyze" is also
// a result with a version), otherwise the background pass would keep
// coming back to them forever.
package analyzer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/internal/queue"
	"crawler/internal/rules"
)

// Pass defaults (the T-020/T-030/T-066 style: constants, no separate
// config knob — spec 5.10 fixes the set; the loop receives the
// working values as options for tests and CLI calls).
const (
	// DefaultReanalyzeInterval is the tick pass period. A rule change is
	// handled immediately — via the Reloader notification (Subscribe,
	// T-068); the ticker is the safety net: lost messages (a MAXLEN ~
	// trim, T-025), a worker crash, rows of new check episodes.
	// An hour, not a minute (the T-020/T-030/T-066 style): a tick pass
	// is the time form over a fresh slice (T-193), but every pass still
	// re-reads the window ahead of the cursor and re-publishes rows of
	// new episodes not yet stamped (duplicates are absorbed by the
	// worker idempotency) — no need to go more often; and not a day (the
	// T-030 style): the safety net must not wait for the recheck interval.
	DefaultReanalyzeInterval = time.Hour

	// DefaultReanalyzeBatch is the rows per batch: a pass reads only the
	// keys (the worker takes the content by the key); 5000 matches the
	// scheduler batches (DefaultRecheckBatch, T-030) — a short select and
	// a bounded publish series before the capacity gate check.
	DefaultReanalyzeBatch = 5000

	// DefaultReanalyzeLookback is the anchor window of the tick pass
	// cursor (T-193 §4.2): the tick cursor starts from now() − lookback,
	// not from the epoch (the loop startup full drain covers everything
	// stale at the start moment — only fresh tails are left to the tick).
	// 24h: the tick safety net serves FRESH losses (the main path is the
	// immediate record.go publish and the Subscribe drain); everything
	// older is either analyzed (rules_version changes only by the T-071
	// worker's analysis write or a rule change; a new episode gives a
	// fresh checked_at, T-058) or it is the residual class — lost and
	// older than the window with no rule change, the territory of the
	// next full drain and of the q:dead/q:analyze observability (T-162;
	// docs/analyzer.md). The window survives a tick interval with slack
	// (loop downtime up to a day between ticks/restarts) and the daily
	// rule change rhythm; it costs more only to the first pass after a
	// cursor loss (one window ∝ the inflow, not the table size). The
	// anchor is computed by the process clock (the Go now()): an
	// app-vs-DB clock skew is immaterial against a 24h window; further
	// on the cursor is led by the checked_at values themselves (the
	// DB server's now() writes them, T-058). Cursor persistence in the
	// DB was deliberately rejected (the T-193 spec §6.5): the anchor makes a cursor loss cheap.
	DefaultReanalyzeLookback = 24 * time.Hour
)

// reanalyzeSelectSQL is a batch of stale rows, the PK FORM — the FULL
// DRAIN (ReanalyzeDrain: Subscribe on a rule change T-068, crawlerctl
// reanalyze; a one-off batch — ReanalyzeOnce): the version differs
// from the current one (IS DISTINCT FROM: NULL — never analyzed), the
// domain is not deleted (§13.1), the zone is optional (a NULL
// parameter — all). The row-value > cursor by the row key (the PK)
// cuts off what the drain already covered; the ORDER BY by the same
// key gives deterministic batches and correct pagination. The drain
// is rare and must cover the whole stale set from scratch: the seq
// scan over the non-indexable rules_version (the HOT invariant, the
// T-104/T-175 class) is deliberately complete — the hourly tick walks
// the time form, reanalyzeTickSQL (T-193). The name is label || '.' ||
// zone (the T-027 style) for Result.Name and logs (spec 4.2).
const reanalyzeSelectSQL = "SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, d.label || '.' || z.name " +
	"FROM subdomain_results AS r " +
	"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id " +
	"JOIN zones AS z ON z.id = r.zone_id " +
	"WHERE r.rules_version IS DISTINCT FROM $1::int4 " +
	"AND d.status <> $2::int2 " +
	"AND ($3::int2 IS NULL OR r.zone_id = $3) " +
	"AND (r.zone_id, r.domain_id, r.subdomain_id, r.variant) > ($4::int2, $5::int8, $6::int2, $7::int2) " +
	"ORDER BY r.zone_id, r.domain_id, r.subdomain_id, r.variant " +
	"LIMIT $8"

// reanalyzeTickSQL is a batch of stale rows, the TIME FORM — the
// HOURLY TICK of the loop (ReanalyzeTick, T-193 §4.1): the same
// selection as reanalyzeSelectSQL (the version, the §13.1 guard, the
// zone filter — form parity; the tick walks all zones, a NULL
// parameter), but the row-value > cursor and the ORDER BY go by the
// composite (checked_at, PK) key: PG16 derives the checked_at lower
// bound from the row-value itself and serves it with the
// subdomain_results_checked_idx index (T-171) — the tick reads only
// the fresh slice ahead of the cursor (a window of ~an hour), not the
// heap of the whole table (the slow-query log record #12 diagnosis of
// 10/25: a zero PK cursor — a Parallel Seq Scan of 4.8M rows, 19.9s,
// for 56 rows). The PK at the key's tail decides the batches within
// equal marks (now() is stable in the write transaction — rows of a
// batch share the mark, T-058) — correct pagination without skips;
// checked_at in the column list is the next batch's cursor (the
// retentionSelectSQL T-185 style; the time parameter typecast is the same as there).
const reanalyzeTickSQL = "SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, d.label || '.' || z.name, r.checked_at " +
	"FROM subdomain_results AS r " +
	"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id " +
	"JOIN zones AS z ON z.id = r.zone_id " +
	"WHERE r.rules_version IS DISTINCT FROM $1::int4 " +
	"AND d.status <> $2::int2 " +
	"AND ($3::int2 IS NULL OR r.zone_id = $3) " +
	"AND (r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant) > ($4::timestamptz, $5::int2, $6::int8, $7::int2, $8::int2) " +
	"ORDER BY r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant " +
	"LIMIT $9"

// reanalyzeGuardSQL is the tick pass guard (T-204): the sum of the tail
// buckets reanalyze:YYYY-MM-DD"T"HH24 older than the current hour +
// the current hour bound from the DB (a double duty: the buckets'
// mature area and the closed guard's cursor jump target — see
// ReanalyzeTick). The buckets collect the result write increments
// (record.go, the hour of the transaction now() = the rows' checked_at)
// and the render content replacement (renderworker, the EPISODE hour
// from the RETURNING); the decrement is the analyzer stamp (worker.go,
// the row's episode hour), the authority is the monitor reconciliation
// (the prev/cur FILTER positions of the two live hours' window
// aggregate, the T-203 style). The range over the 'reanalyze:' PK
// prefix lexicographically cuts off the current hour's bucket (the
// immature in-flight flow: a write → stamp takes seconds, the hour
// bucket is still being worked off by decrements) — the DB computes
// the hour name, no Go clocks in the bound (the T-179/T-202 style).
// coalesce — the buckets may not exist at all (before the first
// increment or pass): an honest zero, "no mature tails", not an error —
// unlike the status rows, nobody guarantees the buckets (the
// retryMatureSQL semantics). A scalar aggregate without GROUP BY
// always answers exactly one row: an empty set is a protocol break, an
// error. Decrement tails and render-resurrected lateness of >1h old-hour
// buckets land in the sum with a minus/plus until the nearest monitor
// pass (the rewrite/cleanup of the live buckets) — the streaming
// semantics of events against live rows, the T-202/T-203 family.
var reanalyzeGuardSQL = "SELECT coalesce(sum(value), 0), date_trunc('hour', now()) FROM crawler_counters " +
	"WHERE name >= 'reanalyze:' " +
	"AND name < 'reanalyze:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24')"

// zoneIDByNameSQL is the zone id by name for the crawlerctl reanalyze
// --zone filter: the name is stored in punycode (EnsureZone, T-014),
// UNIQUE — at most one row.
const zoneIDByNameSQL = "SELECT id FROM zones WHERE name = $1"

// RulesSource is the rule set source of the background loop; satisfied
// by *rules.Reloader (T-068). A narrow interface — the loop knows
// nothing about SIGHUP or files; unit tests substitute a fake.
type RulesSource interface {
	// Set is the current immutable snapshot (a Reloader never returns nil;
	// a fake without a set — the pass is skipped with a Warn).
	Set() *rules.Set
	// Subscribe is the set change notification: an out-of-turn pass.
	Subscribe() (<-chan *rules.Set, func())
}

// ReanalyzeOptions holds the re-analysis parameters; zero values are
// replaced with the defaults (withDefaults, the RecheckOptions T-030 style).
type ReanalyzeOptions struct {
	// Interval is the ReanalyzeLoop period (DefaultReanalyzeInterval);
	// it does not affect one-off passes.
	Interval time.Duration
	// BatchSize is the maximum rows in one batch (DefaultReanalyzeBatch).
	BatchSize int
	// Log is the loop logger; nil — records are dropped (slog.DiscardHandler).
	Log *slog.Logger
}

func (o ReanalyzeOptions) withDefaults() ReanalyzeOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultReanalyzeInterval
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultReanalyzeBatch
	}
	return o
}

// ReanalyzeResult is a pass outcome: rows selected/published. A
// Published < Selected difference is a publish failure in the middle
// of a batch (the pass stopped).
type ReanalyzeResult struct {
	Selected  int
	Published int
}

// resultKey is the subdomain_results row key (the PK, spec 6.1) and
// the drain pagination cursor.
type resultKey struct {
	zone    int16
	domain  int64
	sub     int16
	variant model.Variant
}

// ReanalyzeCursor is the tick pass cursor by time (T-193): the last
// visited row (checked_at, PK), surviving the loop passes — the
// retentionCursor sample (T-185). Time leads (fresh episodes ahead:
// the T-058 writes the DB server's now()), the PK decides the order
// within equal marks. The zero value means "from the anchor": a pass
// starts from now() − DefaultReanalyzeLookback (§4.2: the loop anchors
// the cursor at the start, ReanalyzeTick guards the zero one; a
// cursor loss — a restart — is cheap, one window). The fields are
// not exported: only ReanalyzeTick moves the cursor, the owner is the
// ReanalyzeLoop loop (one goroutine, no locks needed — the
// retentionState T-185 style); external calls hold the value whole
// (a zero struct literal is available without the fields too).
type ReanalyzeCursor struct {
	checkedAt time.Time
	key       resultKey
}

// reanalyzeRow is a batch row: the key, the domain name for the
// message and the checked_at mark — the next time-form batch's cursor
// (the time form reads it from the row; the drain PK form does not select the column — zero).
type reanalyzeRow struct {
	key       resultKey
	name      string
	checkedAt time.Time
}

// ReanalyzeOnce is one pass of the FIRST BATCH of the PK form (from
// the zero key): a select of up to opts.BatchSize stale rows and a
// publish of a Result into q:analyze per row (spec 5.8 "Re-analysis").
// No re-downloading. The pass is idempotent at the "select+publish"
// level: after the T-071 worker commits the new rules_version, rows
// stop being selected. The full drain is ReanalyzeDrain, the fresh
// slice tick pass is ReanalyzeTick (T-193).
func ReanalyzeOnce(ctx context.Context, pool db.DB, q queue.Queue, version int32, zone *int16, opts ReanalyzeOptions) (ReanalyzeResult, error) {
	if pool == nil {
		return ReanalyzeResult{}, errors.New("analyzer: переанализ: пул не задан")
	}
	if q == nil {
		return ReanalyzeResult{}, errors.New("analyzer: переанализ: очередь не задана")
	}
	rows, err := reanalyzeSelect(ctx, pool, version, zone, resultKey{}, opts.withDefaults().BatchSize)
	if err != nil {
		return ReanalyzeResult{}, err
	}
	out := ReanalyzeResult{Selected: len(rows)}
	if err := reanalyzePublish(ctx, q, rows, &out); err != nil {
		return out, err
	}
	return out, nil
}

// ReanalyzeDrain is the FULL drain of the stale set in batches
// (DefaultReanalyzeBatch, the PK form from the zero cursor): a select
// → a publish → the cursor past the batch's last row, until the
// first short batch. This way a rule change (the loop's Subscribe,
// T-068) and the crawlerctl reanalyze command cover the whole set
// without repeats within a pass; only rows that went stale after the
// cursor passed can "ripen" into the next pass.
func ReanalyzeDrain(ctx context.Context, pool db.DB, q queue.Queue, version int32, zone *int16, opts ReanalyzeOptions) (ReanalyzeResult, error) {
	if pool == nil {
		return ReanalyzeResult{}, errors.New("analyzer: переанализ: пул не задан")
	}
	if q == nil {
		return ReanalyzeResult{}, errors.New("analyzer: переанализ: очередь не задана")
	}
	o := opts.withDefaults()
	out := ReanalyzeResult{}
	after := resultKey{} // the zero key is less than any real one (zone_id ≥ 1)
	for {
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("analyzer: переанализ: %w", err)
		}
		rows, err := reanalyzeSelect(ctx, pool, version, zone, after, o.BatchSize)
		if err != nil {
			return out, err
		}
		out.Selected += len(rows)
		if err := reanalyzePublish(ctx, q, rows, &out); err != nil {
			return out, err
		}
		if len(rows) < o.BatchSize {
			return out, nil // the set is exhausted
		}
		after = rows[len(rows)-1].key
	}
}

// ReanalyzeTick is the loop's tick pass: draining the FRESH SLICE in
// batches (the time form, T-193 §4.1) from the cursor up to the
// window being exhausted — a short or empty batch means there is
// nothing stale ahead of the cursor; an empty batch exhausted the
// window, the cursor waits for the next tick. The cursor is loop
// state surviving the passes (the retentionCursor sample, T-185): it
// advances past the last row of every PUBLISHED batch and only
// forward by (checked_at, PK) — a pass costs ∝ the inflow since the
// previous tick, not the table size. A zero cursor means the
// now() − DefaultReanalyzeLookback anchor (§4.2 — recovery after a
// cursor loss; the loop anchors the cursor itself at startup, this is
// the safety net for one-off calls); nil — the anchor without saving
// (a one-off call without state). A publish failure stops the pass
// without moving the cursor past the failed batch: a repeated tick
// publishes its rows again (at-least-once, §3.4; the duplicate is
// bounded by the batch, absorbed by the T-071 worker per-key
// idempotency). A ctx cancellation is a context error with the published outcome (the ReanalyzeDrain style).
//
// The tail buckets guard (T-204, reanalyzeGuardSQL) — BEFORE the heavy
// select: the sum of reanalyze: buckets older than the current hour
// — mature tails without the in-flight flow. A sum ≤ 0 — the heavy
// window select (a heap visit of each row for rules_version, 84k/h)
// does not run at all and the cursor JUMPS to the current hour bound
// (the date from the same guard row): the mature area is declared
// empty by the buckets, the immature one (the current hour, the
// bucket is still worked off by stamp decrements) stays ahead of the
// cursor. Without the jump the cursor would freeze at the last
// publish (an empty batch does not move it), and the first tail after
// quiet hours would cost a pass over a many-hour window — the jump
// keeps every guard-opened window ≤ 2h of inflow. The jump relies on
// the buckets' maturity in the same place as the select skip: the
// hour bound from the DB (not Go clocks), write transactions are short
// (ms) — late commits behind the bound are unique and additionally
// require a lost publish to become a tail: a residual class of the
// same nature as the §4.2 anchor (picked up by a restart with the 24h
// anchor or by the full drain of a rule change). The first tick after
// the loop startup with a closed guard does not read the 24h anchor
// window: the startup drain already covered everything stale (its
// failure is a Warn, the anchor is recreated by the next restart), and the first hour's losses are visible in the buckets.
func ReanalyzeTick(ctx context.Context, pool db.DB, q queue.Queue, version int32, cursor *ReanalyzeCursor, opts ReanalyzeOptions) (ReanalyzeResult, error) {
	if pool == nil {
		return ReanalyzeResult{}, errors.New("analyzer: переанализ: пул не задан")
	}
	if q == nil {
		return ReanalyzeResult{}, errors.New("analyzer: переанализ: очередь не задана")
	}
	if cursor == nil {
		cursor = &ReanalyzeCursor{} // a one-off call: the anchor without saving
	}
	if cursor.checkedAt.IsZero() {
		// The start/recovery anchor (§4.2): now() − lookback, not the epoch —
		// the tick safety net serves fresh losses, anything older is the
		// full drain territory (DefaultReanalyzeLookback, the rationale).
		cursor.checkedAt = time.Now().Add(-DefaultReanalyzeLookback)
	}
	o := opts.withDefaults()
	out := ReanalyzeResult{}
	if err := ctx.Err(); err != nil {
		return out, fmt.Errorf("analyzer: переанализ: %w", err)
	}
	// The tail buckets guard (T-204): before the heavy select — the sum
	// of mature buckets; ≤ 0 (zero — no tails, minus — a decrement drift,
	// silent per the T-202 legacy) — the window is not read, the cursor
	// jumps to the current hour bound (the rationale is the ReanalyzeTick comment).
	tails, hourStart, err := reanalyzeMatureTails(ctx, pool)
	if err != nil {
		return out, err
	}
	if tails <= 0 {
		cursor.checkedAt = hourStart
		cursor.key = resultKey{}
		return out, nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("analyzer: переанализ: %w", err)
		}
		rows, err := reanalyzeTickSelect(ctx, pool, version, *cursor, o.BatchSize)
		if err != nil {
			return out, err
		}
		if len(rows) == 0 {
			return out, nil // the window is exhausted — the cursor waits for the next tick
		}
		out.Selected += len(rows)
		if err := reanalyzePublish(ctx, q, rows, &out); err != nil {
			return out, err
		}
		// The cursor — past the last row of the published batch.
		cursor.checkedAt, cursor.key = rows[len(rows)-1].checkedAt, rows[len(rows)-1].key
		if len(rows) < o.BatchSize {
			return out, nil // a short batch — nothing stale ahead of the cursor anymore
		}
	}
}

// reanalyzeSelect selects a batch of stale rows after the after cursor
// (the PK form, reanalyzeSelectSQL).
func reanalyzeSelect(ctx context.Context, pool db.DB, version int32, zone *int16, after resultKey, limit int) ([]reanalyzeRow, error) {
	var zoneArg any // nil → a NULL parameter → the zone filter is off
	if zone != nil {
		zoneArg = *zone
	}
	rows, err := pool.Query(ctx, reanalyzeSelectSQL,
		version,
		int16(model.StatusDeleted),
		zoneArg,
		int16(after.zone), int64(after.domain), int16(after.sub), int16(after.variant),
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("analyzer: переанализ: выборка: %w", err)
	}
	defer rows.Close()

	var out []reanalyzeRow
	for rows.Next() {
		var r reanalyzeRow
		var variant int16
		if err := rows.Scan(&r.key.zone, &r.key.domain, &r.key.sub, &variant, &r.name); err != nil {
			return nil, fmt.Errorf("analyzer: переанализ: выборка: чтение: %w", err)
		}
		r.key.variant = model.Variant(variant)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("analyzer: переанализ: выборка: %w", err)
	}
	return out, nil
}

// reanalyzeTickSelect selects a batch of stale rows after the after
// cursor (the time form, reanalyzeTickSQL). The tick walks all zones —
// the SQL zone parameter is always NULL (the filter is kept in the
// form for parity with the PK one, T-193 §4.1).
func reanalyzeTickSelect(ctx context.Context, pool db.DB, version int32, after ReanalyzeCursor, limit int) ([]reanalyzeRow, error) {
	rows, err := pool.Query(ctx, reanalyzeTickSQL,
		version,
		int16(model.StatusDeleted),
		nil, // no zone filtering — all
		after.checkedAt,
		int16(after.key.zone), int64(after.key.domain), int16(after.key.sub), int16(after.key.variant),
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("analyzer: переанализ: выборка тика: %w", err)
	}
	defer rows.Close()

	var out []reanalyzeRow
	for rows.Next() {
		var r reanalyzeRow
		var variant int16
		if err := rows.Scan(&r.key.zone, &r.key.domain, &r.key.sub, &variant, &r.name, &r.checkedAt); err != nil {
			return nil, fmt.Errorf("analyzer: переанализ: выборка тика: чтение: %w", err)
		}
		r.key.variant = model.Variant(variant)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("analyzer: переанализ: выборка тика: %w", err)
	}
	return out, nil
}

// reanalyzeMatureTails is the tick pass guard (T-204): the sum of the
// reanalyze: tail buckets older than the current hour and the current
// hour bound from the DB (reanalyzeGuardSQL). The SQL coalesce
// guarantees one row even without bucket rows (an honest zero): an
// empty set is a protocol break, not "no buckets", and must be an error (the retryMatureCount style).
func reanalyzeMatureTails(ctx context.Context, pool db.DB) (tails int64, hourStart time.Time, err error) {
	rows, err := pool.Query(ctx, reanalyzeGuardSQL)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("analyzer: переанализ: корзины хвостов: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		// The read failure first: a connection drop is not an "empty sum"
		// (the SQL coalesce always answers with a row).
		if err := rows.Err(); err != nil {
			return 0, time.Time{}, fmt.Errorf("analyzer: переанализ: корзины хвостов: %w", err)
		}
		return 0, time.Time{}, fmt.Errorf("analyzer: переанализ: корзины хвостов: пустой ответ агрегата")
	}
	if err := rows.Scan(&tails, &hourStart); err != nil {
		return 0, time.Time{}, fmt.Errorf("analyzer: переанализ: корзины хвостов: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, time.Time{}, fmt.Errorf("analyzer: переанализ: корзины хвостов: %w", err)
	}
	return tails, hourStart, nil
}

// reanalyzePublish publishes a Result per batch row (q:analyze, the
// T-024 format): only the key and the name — the worker will take the content from the store.
func reanalyzePublish(ctx context.Context, q queue.Queue, rows []reanalyzeRow, out *ReanalyzeResult) error {
	for i := range rows {
		msg := queue.Result{
			ZoneID:      rows[i].key.zone,
			DomainID:    rows[i].key.domain,
			SubdomainID: rows[i].key.sub,
			Variant:     rows[i].key.variant,
			Name:        rows[i].name,
		}
		if _, err := q.Publish(ctx, queue.StreamAnalyze, msg); err != nil {
			return fmt.Errorf("analyzer: переанализ: публикация q:analyze (домен %s, запись %d/%d): %w",
				rows[i].name, i+1, len(rows), err)
		}
		out.Published++
	}
	return nil
}

// ReanalyzeLoop is the background re-analysis loop (spec 5.8
// "Background task"): a startup FULL drain in batches (it picks up
// what went stale during the downtime — the T-020/T-030/T-066 style;
// including rules that changed during it — Subscribe notifies only a
// running process, T-068), then the Interval ticker — tick passes in
// the time form: the cursor is anchored at the loop start to now() −
// DefaultReanalyzeLookback (T-193 §4.2 — not from the epoch: the
// first tick must not read the whole history) and survives the
// passes, each tick continues from where it stopped instead of
// scanning the table from scratch; a rule set change
// (Reloader.Subscribe, T-068: an SIGHUP without a restart) — an
// out-of-turn FULL drain, a PK cursor from scratch. Every pass takes
// the current version from src.Set(). The tick and the drain run in
// one goroutine's select — the sets overlapping (a rule change during
// a tick) is safe: both are idempotent at the "select+publish" level
// (T-071). A pass error is a Warn, the loop lives (the OnRun contract,
// T-012): what was published is not lost; a next rule change will
// return the unpublished via the drain, a tick will via a repeated
// cursor pass (at-least-once, §3.4). A ctx cancellation is a clean nil
// (a graceful shutdown, §3.4). Only the loop touches the cursor (one goroutine) — no locks are needed (the retentionState T-185 style).
func ReanalyzeLoop(ctx context.Context, pool db.DB, q queue.Queue, src RulesSource, opts ReanalyzeOptions) error {
	if pool == nil {
		return errors.New("analyzer: переанализ: пул не задан")
	}
	if q == nil {
		return errors.New("analyzer: переанализ: очередь не задана")
	}
	if src == nil {
		return errors.New("analyzer: переанализ: источник набора правил не задан (rules.Reloader)")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	sets, cancelSub := src.Subscribe()
	defer cancelSub()
	log.Info("цикл переанализа запущен",
		"interval", o.Interval.String(), "batch", o.BatchSize,
		"lookback", DefaultReanalyzeLookback.String())

	// The tick cursor is loop state surviving the passes (T-193): the
	// anchor is set here, at the loop start (§4.2 — "the loop start is
	// the cursor now() − lookback", not from the epoch: the first tick
	// does not read the whole history); a cursor loss (a restart) is
	// the same anchor in the new loop; ReanalyzeTick additionally anchors the zero cursor itself.
	var cursor ReanalyzeCursor
	cursor.checkedAt = time.Now().Add(-DefaultReanalyzeLookback)

	// tick is the tick pass: the fresh slice from the live cursor (the
	// time form); the version is the current one at the pass moment.
	tick := func() {
		set := src.Set()
		if set == nil {
			log.Warn("проход переанализа: набор правил не загружен — пропуск")
			return
		}
		res, err := ReanalyzeTick(ctx, pool, q, set.Version, &cursor, o)
		if err != nil {
			// A cancellation during a pass is part of the stop, not an alarm.
			if ctx.Err() == nil {
				log.Warn("проход переанализа", "err", err,
					"selected", res.Selected, "published", res.Published)
			}
			return
		}
		if res.Published > 0 {
			log.Info("проход переанализа",
				"rules_version", set.Version,
				"selected", res.Selected, "published", res.Published)
		}
	}

	// drain is the full drain: the whole stale set in batches from the
	// zero PK cursor (a rule change — a rare, deliberately full pass).
	drain := func() {
		set := src.Set()
		if set == nil {
			log.Warn("проход переанализа: набор правил не загружен — пропуск")
			return
		}
		res, err := ReanalyzeDrain(ctx, pool, q, set.Version, nil, o)
		if err != nil {
			// A cancellation during a pass is part of the stop, not an alarm.
			if ctx.Err() == nil {
				log.Warn("проход переанализа", "err", err,
					"selected", res.Selected, "published", res.Published)
			}
			return
		}
		if res.Published > 0 {
			log.Info("проход переанализа",
				"rules_version", set.Version,
				"selected", res.Selected, "published", res.Published)
		}
	}

	drain() // the startup drain: what went stale during the downtime,
	// including rules that changed during it (a deliberately full pass —
	// §4.3); the first ticker tick continues with the cursor from the anchor set above.
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case set, ok := <-sets:
			if !ok {
				// An equivalent mutant (T-M10): cutting the sets = nil out
				// leaves a closed channel in the select — it is still
				// immediately ready; ok=false and continue spin the loop
				// idly without drain and logs, the state and the further
				// branches are the same (the only difference is CPU load —
				// deterministically unobservable). Cutting the whole branch
				// out is covered by the TestReanalyzeLoopClosedSets test
				// (the drain/log flow over the closed channel).
				// mutator-disable-next-line statement/remove
				sets = nil // a closed channel must not loop the select
				// An equivalent mutant (T-M10): continue → break only exits
				// the select — the for loop continues with the next iteration,
				// and the branch body is exhausted anyway: indistinguishable.
				// mutator-disable-next-line loop/break
				continue
			}
			log.Info("смена набора правил — внеочередной переанализ",
				"rules_version", set.Version)
			drain()
		case <-ticker.C:
			tick()
		}
	}
}

// ZoneIDByName is the zone id by punycode name for the crawlerctl
// reanalyze --zone filter. found = false — the zone is not in the
// reference table (ingest creates zones on the first file, T-014).
func ZoneIDByName(ctx context.Context, pool db.DB, name string) (id int16, found bool, err error) {
	if pool == nil {
		return 0, false, errors.New("analyzer: зона по имени: пул не задан")
	}
	rows, err := pool.Query(ctx, zoneIDByNameSQL, name)
	if err != nil {
		return 0, false, fmt.Errorf("analyzer: зона по имени: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		if err := rows.Scan(&id); err != nil {
			return 0, false, fmt.Errorf("analyzer: зона по имени: чтение: %w", err)
		}
		return id, true, nil
	}
	if err := rows.Err(); err != nil {
		return 0, false, fmt.Errorf("analyzer: зона по имени: %w", err)
	}
	return 0, false, nil
}

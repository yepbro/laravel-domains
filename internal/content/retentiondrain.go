// Retention drain state that survives passes (T-185; spec
// TZ_slow_retention_drain.md §4.2–4.4 — the diagnosis of slow-query log
// entry #13 of Oct 24: hourly full circles over PK re-read the heap of the
// entire traversed history; the trail grew with the table at ≈3M rows per
// day, the median 76 s, 63 min of DB time over 2.4 h).
//
// The "once in a lifetime" model (§3): a cursor over (checked_at, PK) is
// initialized to zero at process start — the first pass is the catch-up
// circle over what has accumulated — and advances batch by batch; a
// trimmed row stays behind the cursor, and a re-checked one (DELETE+INSERT,
// fresh checked_at — T-058) enters the drain again when it ages. Since
// T-274 the cursor is additionally persisted in the DB (retentioncursor.go,
// migration 000020): a process restart continues the drain from the saved
// position rather than a new catch-up from zero (the restart penalty — an
// hourly traversal of the clean prefix, measurements 2026-11-10/11 — is
// gone). An "incomplete batch" means the drain ran to the end of the
// current aging window; the next pass (an hour later) continues from the
// stop point — the slice that ages over an hour ≈ the inflow, and the
// window is served by the subdomain_results_checked_idx index (T-171).
//
// Explicit retries instead of full circles (§4.3–4.4): the hourly circle
// was at once a retry (picking up protected records) and a trail
// (re-reading history) — the replacement:
//
//   - a bounded list of keys deferred by the queue guards
//     (retentionpending.go): the next pass offers them with a point probe
//     by PK (retentionProbeSQL); still-protected keys stay in the list;
//     the loss of the list (a restart) is covered by the catch-up circle of
//     the start; an overflow — a Warn and a conservative catch-up circle;
//   - a catch-up circle on the render-debt counter
//     crawler_counters['render_pending'] reset to zero (T-175): records
//     under the render_pending flag are silently filtered out by a JOIN —
//     the time cursor passes them; the counter's transition to 0 (all
//     render protection lifted at once) is read point-wise at the start of
//     every pass and plans one full circle from the zero cursor. A missing
//     counter row (the DB before the T-175 deploy) — a fallback: a circle
//     once per day (a constant; no config knob is introduced, spec §4.4).
//
// What does not change (§4.5): purge/decref/delete with guard re-checks,
// the retentionBlocked interlock, the retentionPendingSnapshot snapshots
// and their long-drain refresh, the statistics and OnPass metrics.
package content

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// DefaultRetentionRetryLimit — the limit of the queue-guard retry key
	// list (T-185): the order of the stream depths at the watermarks (the
	// T-026 gate: 100k; the T-174 render threshold: 10k) — a standstill of
	// a larger scale cannot be cured by the abolished circles; the depth
	// interlock stops the passes earlier. A key costs ≈ tens of bytes —
	// hundreds of thousands of list rows stay cheap; an overflow — a Warn
	// and a conservative catch-up circle on the next pass (the circle will
	// rebuild the list itself). Tests substitute it with the
	// retentionState.retryLimit field.
	DefaultRetentionRetryLimit = 100000

	// DefaultRetentionFallbackCircle — the fallback circle period when the
	// render-debt counter is unavailable (no row or no table — the DB
	// before the T-175 deploy, a read failure; spec §4.4): a day means
	// "once per quiet hour" without a config knob; the anchor is moved by
	// successful counter reads and completed circles. The spec's "quiet
	// hour" is understood as "a day from the anchor", not an hour of the
	// day: a separate schedule was deliberately not introduced (a config
	// knob is forbidden by the spec).
	DefaultRetentionFallbackCircle = 24 * time.Hour
)

// retentionState — the drain state of the cycle that survives passes
// (T-185). The owner is the RetentionLoop cycle (a single ticker
// goroutine); the one-off RetentionOnce works with a clean state. A nil
// retries map means "no retries".
type retentionState struct {
	// cursor — the time-based drain cursor (§4.2): the last visited row;
	// advanced only by committed batches.
	cursor retentionCursor
	// retries — the bounded set of keys deferred by the queue guards
	// (§4.3): the next pass offers them with the retentionProbeSQL probe.
	retries map[retentionKey]struct{}
	// debtSeen — the render-debt counter was observed > 0 after the last
	// circle (§4.4): the next read's transition to 0 — a catch-up circle.
	debtSeen bool
	// lastCircle — the anchor of the fallback day: successful debt counter
	// reads and completed circles (fallback means "unavailable for a day
	// IN A ROW"; a transient blip does not move it); the initial anchor is
	// the state creation (the starting drain goes from zero anyway).
	lastCircle time.Time
	// noCounterTable — the crawler_counters table was not there yet (one
	// Info per process, review of T-185, P3-4 R3).
	noCounterTable bool
	// circle — the next pass starts from the zero cursor (§4.2–4.4):
	// the render-debt lift, the retry list overflow, the fallback day;
	// circleWhy — the reason for the journal.
	circle    bool
	circleWhy string

	// retryLimit — the retry list limit (DefaultRetentionRetryLimit);
	// a field for unit tests — less fiddling with a hundred thousand keys.
	retryLimit int

	// persist — the state belongs to the service cycle (RetentionLoop):
	// the batch cursor is committed to the DB (retention_drain_cursor,
	// T-274, migration 000020); forced circles clear the saved position.
	// The one-off RetentionOnce and the unit tests of the selection
	// criteria work without persistence (the pre-T-274 behavior); reset to
	// false when the table is missing (the migration not applied — one
	// Info, the noCounterTable style).
	persist bool
}

// limit — the effective retry list limit.
func (st *retentionState) limit() int {
	if st.retryLimit > 0 {
		return st.retryLimit
	}
	return DefaultRetentionRetryLimit
}

// retentionPass — one pass of the cycle with the state st (spec §5.1):
// the catch-up decision on the render-debt counter at the start of the
// pass, point retry offers, the drain in batches from the st.cursor cursor
// until the aging window is exhausted, stops by the interlock, the
// queue-guard errors, or a ctx cancellation. Stops keep the progress: the
// cursor and the retry list will survive the pass — the next one continues
// from the stop point.
func retentionPass(ctx context.Context, pool RetentionDB, opts RetentionOptions, st *retentionState) (RetentionStats, error) {
	if pool == nil {
		return RetentionStats{}, errors.New("content: retention: пул не задан")
	}
	if st == nil {
		st = &retentionState{}
	}
	// The fallback-day anchor is the moment of state creation: the starting
	// drain goes from the zero cursor anyway ("the circle has just
	// happened"); without an anchor, the very first transient counter read
	// failure would count as "the day has elapsed" and immediately raise a
	// full circle (review of T-185, P3-3).
	if st.lastCircle.IsZero() {
		st.lastCircle = time.Now()
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	st.catchUp(ctx, pool, log)
	var out RetentionStats
	// The protection snapshot is lazy, AFTER the first interlock check: the
	// "the analyzer cannot keep up" interlock covers both q:analyze and
	// q:render wider than the snapshot — there is no point in running the
	// heavy XRANGE for a pass that is immediately abandoned; besides, a
	// read timeout of a stuck stream with the interlock closed would yield
	// a false SkippedPending instead of an honest Skipped.
	// A read error is not a pass error but its cancellation (the data is
	// untouched): see the retentionpending.go header.
	var pending *retentionPendingSets
	selects := 0 // batch selections since the snapshot (the long-drain refresh)
	probed := false
	for {
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("content: retention: %w", err)
		}
		// The interlock: the analyzer cannot keep up — the bodies and
		// headers/cookies of queued records are still needed; checked on
		// every batch — a long drain sees the fresh depth.
		n, blocked, err := retentionBlocked(ctx, o)
		if err != nil {
			return out, fmt.Errorf("content: retention: интерлок q:analyze: %w", err)
		}
		if blocked {
			out.Skipped = true
			out.Depth = n
			return out, nil
		}
		// The first snapshot — after the interlock, before the first
		// selection: the purge starts only once it is known which records
		// the queues are waiting on.
		if o.Pending != nil && pending == nil {
			p, err := retentionPendingSnapshot(ctx, o.Pending)
			if err != nil {
				if ctx.Err() != nil {
					return out, fmt.Errorf("content: retention: снимок очередей: %w", ctx.Err())
				}
				out.SkippedPending = true
				return out, nil
			}
			pending = p
		}

		// Point retry offers (§4.3) — strictly once per pass, before the
		// drain: the list is below the cursor, the probe does not depend on
		// batches; keys deferred by this same pass are offered by the next.
		if !probed {
			probed = true
			if len(st.retries) > 0 {
				if err := retentionRetryPass(ctx, pool, o, st, pending, &out); err != nil {
					return out, err
				}
			}
		}

		batch, err := retentionSelect(ctx, pool, o, st.cursor)
		if err != nil {
			return out, err
		}
		if len(batch) == 0 {
			return out, nil // the aging window is exhausted — the cursor waits for the next hour
		}
		selects++
		// The cursor and the exhaustion decision — over the original
		// selection: records excluded by the guards are skipped by this
		// pass (the cursor goes past them) and will come back as point
		// offers (§4.3) — only non-waiting records are purged.
		last, full := batch[len(batch)-1], len(batch) >= o.BatchSize
		next := retentionCursor{checkedAt: last.checkedAt, key: last.key}
		st.deferRetries(ctx, pool, batch, pending, log)
		selected := len(batch)
		batch = pending.deferBatch(batch, &out)
		if len(batch) > 0 {
			// The persistent cursor — by the batch transaction (T-274), but
			// only if the batch did NOT jump over rows deferred by the
			// guards: the position in the DB does not move past unpurged
			// rows — the loss of the retry list on a restart is compensated
			// by re-reading the guard window (the keys will be deferred
			// again), and the trimmed rows fall out of the selection on
			// their own (review of T-274, P1). The in-memory cursor moves
			// as before: within the process lifetime the probe will return
			// the keys. A planned circle suppresses the saving (planCircle
			// has already deleted the row — a new position before the
			// circle apply would resurrect it).
			var save *retentionCursor
			if st.persist && !st.circle && len(batch) == selected {
				save = &next
			}
			stp, err := retentionPurge(ctx, pool, o, batch, save)
			if err != nil {
				// The cursor table is missing (migration 000020 not
				// applied — an old bench): persistence is disabled for the
				// process, the batch is rolled back as a whole and retried
				// by the next pass (the cursor did not move — no loss).
				if errors.Is(err, errRetentionNoCursorTable) {
					st.persist = false
					log.Info("retention: таблицы персистентного курсора нет — работаем без сохранения (миграция 000020 не применена?)")
					return out, nil
				}
				return out, err
			}
			out.add(stp)
		}
		st.cursor = next
		if !full {
			return out, nil // an incomplete batch — the drain is exhausted
		}
		// A long drain: the snapshot is stale (the T-066 re-publication
		// race) — re-reading it is cheaper than allowing a window; the
		// refresh covers only future batches, what has been passed is not
		// revisited.
		if o.Pending != nil && selects >= DefaultRetentionPendingRefresh {
			p, err := retentionPendingSnapshot(ctx, o.Pending)
			if err != nil {
				if ctx.Err() != nil {
					return out, fmt.Errorf("content: retention: снимок очередей: %w", ctx.Err())
				}
				out.SkippedPending = true
				return out, nil
			}
			pending, selects = p, 0
		}
	}
}

// catchUp — the catch-up circle decision at the start of a pass (§4.4):
// a point read of the render-debt counter and the application of a deferred
// circle (the retry list overflow, the fallback day). An unavailable
// counter — no row, no table (42P01: the DB before the migration 000015
// deploy — treated as "no counter", without a Warn) or a read failure: the
// fallback is a guard day whose anchor is moved by successful reads —
// fallback means "unavailable for a day IN A ROW"; a transient blip after
// weeks of healthy operation does not raise a full circle (review of
// T-185, P3-2 R3). The hour discretization: a debt era that fit entirely
// between reads (accumulated and drained to zero) will not see the
// transition to 0 — its records are caught up by the first observed zero
// after it (a circle from zero trims everything behind the cursor), the
// list overflow, the fallback, or a restart. The trigger "a substantial
// debt decrease" was rejected deliberately: a long drain (443k, stage 25)
// would decrease every hour — the circles would come back. The persistent
// circle state is cleared by planCircle at the moment of the plan (T-274).
func (st *retentionState) catchUp(ctx context.Context, pool RetentionDB, log *slog.Logger) {
	if debt, ok, err := retentionDebt(ctx, pool); err != nil && errors.Is(err, errRetentionNoCounterTable) {
		// The counter table is missing (migration 000015 not applied) — a
		// normal state of an old bench: one Info per process, then silence
		// (review of T-185, P3-4 R3: full silence would postpone the
		// diagnosis "the migration was not applied" by a day, until the
		// first circle).
		if !st.noCounterTable {
			st.noCounterTable = true
			log.Info("retention: таблицы счётчика рендер-долга нет — работаем fallback-сутками (миграция 000015 не применена?)")
		}
		st.fallbackCircle(ctx, pool, log)
	} else if err != nil {
		if ctx.Err() == nil {
			log.Warn("retention: счётчик рендер-долга не прочитан — состояние долга держится, страховка fallback-суток",
				"err", err)
		}
		st.fallbackCircle(ctx, pool, log)
	} else if !ok {
		// The counter row is missing (the DB before the T-175 deploy) — the
		// same fallback.
		st.fallbackCircle(ctx, pool, log)
	} else {
		st.lastCircle = time.Now() // the fallback-day anchor: the read succeeded
		if debt < 0 {
			// Not produced by code (the bit transitions are a ±1 pair,
			// T-175) — a manual SQL desync (runbooks §6); treated as "no
			// debt", but loudly (the renderDebtCount style, T-181).
			log.Warn("retention: отрицательный счётчик рендер-долга — рассинхрон (runbooks §6)", "debt", debt)
		}
		if debt > 0 {
			st.debtSeen = true
		} else if st.debtSeen {
			// The transition to 0: all render protection is lifted — one
			// full circle catches up everything that accumulated over the
			// debt era behind the cursor.
			st.planCircle(ctx, pool, log, "счётчик рендер-долга обнулился — рендер-защита снята")
		}
	}

	if !st.circle {
		return
	}
	// The circle: the cursor to the start, the retry list will be rebuilt
	// by the circle itself (protected keys are deferred by it too), the
	// debt era is closed. The persistent state has already been reset at
	// the moment of the plan (planCircle, T-274). A noop circle (the cursor
	// is already zero, no list) — Debug: the starting state of a fresh
	// process; it changes no operations.
	why := st.circleWhy
	noop := st.cursor == (retentionCursor{}) && len(st.retries) == 0
	st.cursor = retentionCursor{}
	st.retries = nil
	st.debtSeen = false
	st.lastCircle = time.Now()
	st.circle, st.circleWhy = false, ""
	if noop {
		log.Debug("retention: catch-up-круг уже на месте (нулевой курсор)", "reason", why)
		return
	}
	log.Info("retention: catch-up-круг с нулевого курсора", "reason", why)
}

// fallbackCircle — the guard day when the debt counter is unavailable
// (§4.4): a circle once per DefaultRetentionFallbackCircle from the anchor
// (the state creation or the last circle). The spec's "quiet hour" is
// understood as "a day from the anchor", not an hour of the day: the anchor
// is moved by the circles themselves, and a separate schedule was
// deliberately not introduced (a config knob is forbidden by the spec).
func (st *retentionState) fallbackCircle(ctx context.Context, pool RetentionDB, log *slog.Logger) {
	if time.Since(st.lastCircle) >= DefaultRetentionFallbackCircle {
		st.planCircle(ctx, pool, log, "счётчик рендер-долга недоступен (нет строки/таблицы — миграция 000015) — fallback-сутки")
	}
}

// planCircle — a planned catch-up circle for the next apply turn (the
// start of the current pass); repeated reasons do not overwrite the first
// one. The plan is reflected in the persistent state IMMEDIATELY (T-274,
// review P2): the cursor row is deleted at the moment of the plan — a
// restart in the window between the plan and the apply must land in a full
// catch-up from zero, not continue from the old position (it is exactly the
// circle that trims the records behind it); further position saves are
// suppressed until the circle apply (retentionPass). A deletion failure is
// a Warn without a pass error: the circle will still happen in memory; on a
// restart in the window the cycle degrades to the T-273 behavior
// (continuing from the old position) — costlier, but without data loss.
func (st *retentionState) planCircle(ctx context.Context, pool RetentionDB, log *slog.Logger, why string) {
	if st.circle {
		return
	}
	st.circle, st.circleWhy = true, why
	if !st.persist {
		return
	}
	if err := retentionResetCursor(ctx, pool); err != nil {
		if errors.Is(err, errRetentionNoCursorTable) {
			st.persist = false
			log.Info("retention: таблицы персистентного курсора нет — работаем без сохранения (миграция 000020 не применена?)")
			return
		}
		if ctx.Err() == nil {
			log.Warn("retention: план catch-up-круга не отражён в БД — рестарт в окне продолжит со старого курсора",
				"reason", why, "err", err)
		}
	}
}

// deferRetries — replenishing the retry list with the batch keys deferred
// by the queue guards (§4.3): the next pass will offer them point-wise.
// An overflow — a Warn and a conservative catch-up circle on the next
// pass: the list is bounded by memory, and the circle will rebuild the
// whole set (an overflow means a standstill of the scale of the stream
// depths at the watermarks, where full circles would be the only mechanism
// anyway).
func (st *retentionState) deferRetries(ctx context.Context, pool RetentionDB, batch []retentionCandidate, pending *retentionPendingSets, log *slog.Logger) {
	if pending == nil {
		return // the protection is off — no deferrals
	}
	for _, c := range batch {
		if !pending.waiting(c.key) {
			continue
		}
		if st.retries == nil {
			st.retries = make(map[retentionKey]struct{})
		}
		if _, ok := st.retries[c.key]; ok {
			continue
		}
		if len(st.retries) >= st.limit() {
			// The Warn — once per episode (until the circle apply): the
			// plan is already set, and it stays silent until the next
			// overflow (review of T-185, P3-1 — do not be noisy with every
			// standstill batch).
			if !st.circle {
				log.Warn("retention: список ретраев защиты очередей переполнен — следующий проход catch-up-кругом",
					"limit", st.limit())
			}
			st.planCircle(ctx, pool, log, "список ретраев защиты очередей переполнен — круг пересоберёт множество")
			return
		}
		st.retries[c.key] = struct{}{}
	}
}

// retentionRetryPass — point offers of the retry keys (§4.3): the
// retentionProbeSQL probe by PK returns those still purge candidates; a
// fresh protection snapshot filters out those still waiting (the key stays
// in the list for the next pass), the rest are purged in BatchSize batches.
// Keys not returned by the probe fall out of the list: the protection is
// gone and the purge happened by another mechanism (a circle), the row was
// rewritten (a fresh checked_at — it will return to the drain when it
// ages), it became a find, or it waits for the render (the catch-up circle
// on the debt counter will catch up with it).
// Idempotent: a purge failure — the committed batches stand, and the next
// pass's probe will not return the already-trimmed rows.
func retentionRetryPass(ctx context.Context, pool RetentionDB, o RetentionOptions, st *retentionState, pending *retentionPendingSets, out *RetentionStats) error {
	keys := make([]retentionKey, 0, len(st.retries))
	for k := range st.retries {
		keys = append(keys, k)
	}
	cands, err := retentionProbe(ctx, pool, o, keys)
	if err != nil {
		return err
	}
	// The new list — those still protected; the purge — the rest. The
	// probe's exclusions are counted in Deferred by the holding streams —
	// the same observability as the drain batches (the key honestly waits
	// for consumers, no matter how many passes have offered it).
	next := make(map[retentionKey]struct{}, len(st.retries))
	var waiting, purge []retentionCandidate
	for _, c := range cands {
		if pending != nil && pending.waiting(c.key) {
			waiting = append(waiting, c)
			continue
		}
		purge = append(purge, c)
	}
	_ = pending.deferBatch(waiting, out) // excludes all of them (waiting) and counts Deferred
	for _, c := range waiting {
		next[c.key] = struct{}{}
	}
	// The list is replaced only after the purge as a whole: a failure among
	// the batches keeps the previous list (unpurged keys are not lost
	// behind the cursor), and the next pass repeats the probe
	// idempotently. The probe purges in place — the drain cursor does not
	// move (nil, T-274).
	for i := 0; i < len(purge); i += o.BatchSize {
		j := min(i+o.BatchSize, len(purge))
		stp, err := retentionPurge(ctx, pool, o, purge[i:j], nil)
		if err != nil {
			return err
		}
		out.add(stp)
	}
	st.retries = next
	return nil
}

// errRetentionNoCounterTable — the crawler_counters table is missing
// (SQLSTATE 42P01, the DB before the migration 000015 deploy): a sentinel
// for catchUp — handled as "no counter" with one Info per process, not as a
// read failure with a Warn (review of T-185, P3-2/P3-4 R3).
var errRetentionNoCounterTable = errors.New("content: retention: таблицы счётчика рендер-долга нет (миграция 000015 не применена)")

// retentionDebt — a point read of the render-debt counter
// crawler_counters['render_pending'] (retentionDebtSQL, T-175).
// The second return ok=false — there is no counter: no row (the DB before
// the T-175 deploy) or no table — then err = errRetentionNoCounterTable
// (42P01). Not a pass error: the caller goes to the fallback day (spec
// §4.4); unlike the guard of the rework pass (renderDebtCount, T-181), for
// which a missing row is an error: here the read is an optimization, and a
// circle is costlier than an empty miss.
func retentionDebt(ctx context.Context, pool RetentionDB) (n int64, ok bool, err error) {
	rows, err := pool.Query(ctx, retentionDebtSQL)
	if err != nil {
		// The table is missing — the DB before the migration 000015 deploy
		// (SQLSTATE 42P01, undefined_table — as a literal; schema codes in
		// the project are written as literals, the record.go style): a
		// normal transient state of "no counter", like a missing row
		// (review of T-185, P3-2: a Warn on every hourly pass of an old
		// bench — noise).
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			return 0, false, errRetentionNoCounterTable
		}
		return 0, false, fmt.Errorf("content: retention: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		// A read failure first: a dropped connection is not "no row"
		// (the renderDebtCount style, T-181).
		if err := rows.Err(); err != nil {
			return 0, false, fmt.Errorf("content: retention: %w", err)
		}
		return 0, false, nil
	}
	if err := rows.Scan(&n); err != nil {
		return 0, false, fmt.Errorf("content: retention: счётчик рендер-долга: чтение: %w", err)
	}
	if err := rows.Err(); err != nil {
		return 0, false, fmt.Errorf("content: retention: %w", err)
	}
	return n, true, nil
}

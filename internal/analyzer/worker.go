// Package analyzer is the Laravel analysis stage (T-071; spec 5.8 "Laravel
// analyzer", §7 "analyzer → laravel_findings / laravel_domains", §13.1).
//
// The worker consumes q:analyze (a Result message is the key of a
// subdomain_results row: domain, subdomain, variant; the T-024 format;
// published by the HTTP stage T-058 after writing the result and by the
// render worker T-067 after replacing the content with the final DOM).
// The analysis is passive (spec 5.8): only already downloaded content,
// cookies and headers — no new requests to sites.
// Processing one delivery:
//
//	SELECT of the row (headers/cookies jsonb, content_hash — spec 4.2: the
//	message carries only identifiers, the data is read from the DB; after
//	a render the headers/cookies are the final navigation data, T-067) →
//	content from the ContentStore by content_hash (no hash or content —
//	analysis by headers and cookies; HTML/DOM is the material of the
//	html/meta targets, T-069) → Set.EvaluateCtx with DefaultLimits: a
//	  rule set snapshot is taken per delivery (Reloader.Set; SIGHUP
//	  changes the set without a restart — no change subscription is needed,
//	  the next delivery sees the fresh one) → ONE transaction (spec 3.4):
//	UPDATE subdomain_results.rules_version — the stamp "the row was
//	  analyzed by the rule set version N" (re-analysis T-074 finds rows
//	  with a stale version; T-067 sets NULL on content change) with
//	  freshness guards: content_hash IS NOT DISTINCT FROM the read value
//	  (another episode rewrote the row — the T-058 recheck DELETE+INSERT,
//	  the T-067 render), the version differs from the current one
//	  (a repeated delivery of an already stamped row is a no-op, T-204) and
//	  the domain is not deleted (§8/§13.1); 0 rows — the row slipped
//	  out from under the task: success without a write (the renderworker
//	  style), the new episode has its own publication and analysis;
//	  RETURNING checked_at — the reanalyze tail bucket decrement (T-204)
//	  in the same transaction step → writing laravel_findings by the
//	  same key: ALL hits of any rule regardless of the threshold,
//	  is_laravel = score ≥ the threshold (§13.1/spec 5.8); no hits — the
//	  row is not written, an existing one is deleted (§3.5: no history is
//	  kept, re-analysis rewrites the result from scratch) → XACK
//	  (Processing, T-025: the acknowledgment only after the DB write).
//
// Idempotency of a repeated delivery (spec 3.4): an upsert by the
// laravel_findings PK and a repeated rules_version stamp by the same
// set do not change the state; a failure between the steps rolls back
// the whole transaction, and the repeated delivery runs again.
//
// Domain-level aggregation — T-072 (aggregate.go): in the same
// transaction, right after the findings, the laravel_domains row is
// recalculated (§6.1: the maximum score, the union version range,
// found_on_mask, rules_version; spec 5.8 "at the domain level — an
// aggregate"). A find removal by re-analysis (non-fresh) after the
// commit starts the domain content point drain (T-187, drainUnfound →
// content.DrainDomain): the domain's rows returned to the retention candidates behind the drain cursor (T-185).
package analyzer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/model"
	"crawler/internal/queue"
	"crawler/internal/rules"
)

// Stage SQL. Schema codes are literals, not data (the record.go style,
// T-058); row keys are type literals. The leading PK columns — LIST
// partitioning by zone_id with HASH subpartitioning (T-005) — cut the
// search down to the leaf.
var (
	// analyzeSelectSQL — the analysis input: the headers and cookies of
	// the final response (after a render — the final navigation,
	// T-067/T-058 — one jsonb contract) and the content reference (HTML/DOM).
	analyzeSelectSQL = "SELECT headers, cookies, content_hash FROM subdomain_results " +
		"WHERE zone_id = $1::int2 AND domain_id = $2::int8 " +
		"AND subdomain_id = $3::int2 AND variant = $4::int2"

	// analyzeMarkSQL — the rules_version stamp as the transaction's first
	// command: it locks the row and decides the write's fate (the
	// recordDomainSQL T-058 style). The content_hash IS NOT DISTINCT
	// FROM $6 guard is optimistic (like renderUpdateSQL T-067): the row is
	// rewritten only while its content is the same as what was read (NULL
	// compares correctly — "no content" is legal, T-056). The freshness
	// guard rules_version IS DISTINCT FROM $5 (T-204): the stamp is put
	// only on stale rows — NULL ("never analyzed") and an old rule set
	// version (the T-074 rule change drain); a row already stamped with
	// the current version is not touched (a no-op: the same version ×
	// the same content ⇒ deterministically the same findings; a repeated
	// delivery would rewrite them byte-for-byte) — before T-204 a
	// repeated stamp ran and gave a false decrement to the tail bucket
	// reanalyze: (the RETURNING below): repeated deliveries (tick
	// republications, XAUTOCLAIM) have no paired write increment. The
	// "domain is not deleted" guard — §8/§13.1: no result is written for a
	// deleted domain, the leftover rows and findings will be cleaned by
	// the background cleanup (T-020). RETURNING r.checked_at (T-204) is
	// the tail bucket hour for the decrement (analyzeReanalyzeConsumeSQL):
	// the hour of the row's EPISODE, like the write/render increments; an
	// empty result (pgx.ErrNoRows) means the guards cut it off (no row,
	// the content changed, the domain is deleted, the version is already
	// current): success without a write (the renderworker style).
	analyzeMarkSQL = "UPDATE subdomain_results AS r SET rules_version = $5::int4 " +
		"WHERE r.zone_id = $1::int2 AND r.domain_id = $2::int8 " +
		"AND r.subdomain_id = $3::int2 AND r.variant = $4::int2 " +
		"AND r.content_hash IS NOT DISTINCT FROM $6::bytea " +
		"AND r.rules_version IS DISTINCT FROM $5::int4 " +
		fmt.Sprintf("AND NOT EXISTS (SELECT 1 FROM domains AS d WHERE d.zone_id = r.zone_id "+
			"AND d.id = r.domain_id AND d.status = %d)", int16(model.StatusDeleted)) +
		" RETURNING r.checked_at"

	// analyzeReanalyzeConsumeSQL — the decrement of the unanalyzed tails
	// bucket crawler_counters['reanalyze:YYYY-MM-DD"T"HH24'] (T-204) in
	// the stamp transaction: −1 per row moved stale→current; $1 is the
	// row's checked_at from RETURNING analyzeMarkSQL, the bucket hour is
	// computed by the DB (date_trunc) — the same key as the write
	// increments (record.go, the hour of now() = the rows' checked_at) and
	// the render content replacement (renderworker, the episode hour from
	// the RETURNING). The tick pass guard (reanalyzeGuardSQL) sums the
	// buckets older than the current hour: a published and stamped tail
	// returns the bucket to zero, the next tick does not re-read the
	// window. Stamps of OLD-version rows (the rule change drain, tick
	// republications) decrement without a paired increment — a negative
	// drift until the reconciliation pass (the prev/cur FILTER positions
	// of the window aggregate, monitor): it keeps the guard closed during
	// the drain — the tick's own work there overlaps only with duplicates,
	// streaming semantics (the T-202/T-203 family). The record.go upsert
	// pattern; the lock order is the transaction's single counter row,
	// after the zones/aggregation scalars: shared with the reconciliation
	// pass (reanalyze: after retry_mature:, before rendered:) — no cycles.
	analyzeReanalyzeConsumeSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('reanalyze:' || to_char(date_trunc('hour', $1::timestamptz), 'YYYY-MM-DD\"T\"HH24'), $2::int8, now()) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()"

	// findingUpsertSQL — writing the hits: all rules regardless of the
	// threshold (§13.1), is_laravel separates confident hits from weak ones.
	// An upsert by PK: a repeated delivery and re-analysis rewrite the
	// row (§3.5 — only the last result). NULL ver_min/ver_max when there
	// is no bound and on a range conflict (T-070: an undefined range — both
	// bounds are empty); signals is jsonb text, cast on the server (the
	// record.go style: PostgreSQL does not cast bytea into jsonb).
	findingUpsertSQL = "INSERT INTO laravel_findings (" +
		"zone_id, domain_id, subdomain_id, variant, " +
		"score, ver_min, ver_max, signals, is_laravel, rules_version) " +
		"VALUES ($1::int2, $2::int8, $3::int2, $4::int2, " +
		"$5::float4, $6::text, $7::text, $8::jsonb, $9::bool, $10::int4) " +
		"ON CONFLICT (zone_id, domain_id, subdomain_id, variant) DO UPDATE SET " +
		"score = $5::float4, ver_min = $6::text, ver_max = $7::text, " +
		"signals = $8::jsonb, is_laravel = $9::bool, rules_version = $10::int4"

	// findingDeleteSQL — no rule hit: the row is not written, an existing
	// one (a past analysis found signs) is deleted — §3.5 rewrite: the
	// result of the current episode "no signs".
	findingDeleteSQL = "DELETE FROM laravel_findings " +
		"WHERE zone_id = $1::int2 AND domain_id = $2::int8 " +
		"AND subdomain_id = $3::int2 AND variant = $4::int2"
)

// AnalyzerDB is a pool for reading the result row and writing findings:
// a db.DB with transactions (the httpworker.RecordDB,
// renderworker.RenderDB style). The stamp and the findings go in one
// transaction (spec 3.4); *pgxpool.Pool satisfies the interface.
type AnalyzerDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// ContentGetter is the content store seam (spec 5.7): reading HTML/DOM
// for matching. A narrow subset of the content.ContentStore contract
// (the stage needs only Get); satisfied by *content.PackStore (T-042).
type ContentGetter interface {
	Get(ctx context.Context, hash content.Hash) ([]byte, error)
}

// SetSource is the current rule set snapshot (spec 5.8: reload on
// SIGHUP without a restart). Satisfied by *rules.Reloader (T-068);
// every delivery takes the current set — no change subscription is needed.
// A nil snapshot (a Reloader never has one) skips the delivery with a
// Warn, the row stays unstamped and will come back via re-analysis (T-074).
type SetSource interface {
	Set() *rules.Set
}

// Worker defaults.
const (
	// DefaultGroup is the q:analyze consumer group (spec 4.2: consumer
	// groups scale the workers horizontally).
	DefaultGroup = "analyzer"

	// DefaultConcurrency is the q:analyze deliveries in flight at once.
	// The analysis is light (RE2 matching over a bounded input takes
	// microseconds, T-069; a couple of DB operators), the window keeps the
	// pipeline continuous without queueing at the connection pool; scaling
	// is by instances (spec 4.1), the cap is raised via Options.Concurrency.
	DefaultConcurrency = 8

	// DefaultConsume is the q:analyze records per one read: the read window
	// is no deeper than the processing headroom (the httpworker/renderworker style).
	DefaultConsume = 16

	// DefaultErrorWait is the pause after Redis and DB errors (the
	// httpworker.DefaultErrorWait style): idle rounds do not heat up the
	// logs, XAUTOCLAIM will return the unacknowledged deliveries (T-025).
	DefaultErrorWait = 30 * time.Second
)

// Options holds the worker parameters; zero values are replaced with
// the defaults (withDefaults, the renderworker.Options style).
type Options struct {
	// Concurrency is the q:analyze deliveries in flight at once; 0 —
	// DefaultConcurrency.
	Concurrency int
	// Consume is the q:analyze records per one read; 0 — DefaultConsume.
	Consume int
	// Group is the q:analyze consumer group; "" — DefaultGroup.
	Group string
	// Consumer is the group consumer name; "" — "analyzer-<pid>".
	Consumer string
	// MinScore is the is_laravel threshold (analyzer.laravel_min_score,
	// spec 5.8/§13.1: score ≥ 0.6). A non-positive value is replaced with
	// DefaultMinScore inside Result.IsLaravel (T-070) — config validation
	// allows only (0, 1], this is a guard for direct calls.
	MinScore float64
	// Recover is the stuck delivery recovery layer (Reliab, T-025): Run
	// starts a background RecoverLoop (XAUTOCLAIM → the same processing →
	// XACK) with the same Group/Consumer; nil — no recovery (isolated
	// builds and tests without Redis specifics).
	Recover *queue.Reliab
	// RecoverBatch is the RecoverLoop claim batch per pass; 0 — the queue
	// default (queue.DefaultRecoverBatch). The production build passes
	// redis.recover_batch (T-153): the rate of returning stuck deliveries
	// is a queue knob, not the stage's consumption window.
	RecoverBatch int
	// RecoverInterval is the RecoverLoop polling period for stuck records;
	// 0 — the queue default (queue.DefaultRecoverInterval). The config is
	// redis.recover_interval (T-153).
	RecoverInterval time.Duration
	// Retention is the content lifetime window of non-finds (content.retention,
	// T-166): the DrainDomain point drain uses it to clean a domain's
	// content on a find removal by re-analysis (T-187). ≤ 0 — retention
	// is off (content is kept forever) — no drain, the event is only counted.
	Retention time.Duration
	// DrainDomain is the domain content point drain seam (T-187):
	// satisfied by content.DrainDomain (the worker's pool is
	// content.RetentionDB); nil — the drain is off (isolated builds, unit
	// tests without content semantics) — the event is only counted.
	DrainDomain func(ctx context.Context, pool content.RetentionDB,
		retention time.Duration, zoneID int16, domainID int64) (content.RetentionStats, error)
	// Log is the logger; nil — records are dropped (slog.DiscardHandler).
	Log *slog.Logger
	// Metrics is the service Prometheus registry (T-077, spec 5.9): New
	// builds the stage instruments from it (NewMetrics); nil — no metrics
	// (isolated builds and tests).
	Metrics *metrics.Registry
}

func (o Options) withDefaults() Options {
	if o.Concurrency <= 0 {
		o.Concurrency = DefaultConcurrency
	}
	if o.Consume <= 0 {
		o.Consume = DefaultConsume
	}
	if o.Group == "" {
		o.Group = DefaultGroup
	}
	if o.Consumer == "" {
		o.Consumer = fmt.Sprintf("analyzer-%d", os.Getpid())
	}
	return o
}

func (o Options) log() *slog.Logger {
	if o.Log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return o.Log
}

// Worker is the Laravel analyzer: it processes q:analyze tasks (the
// keys of subdomain_results rows) through the DB, content store and
// rule set seams. Thread-safe: the state is immutable after New.
type Worker struct {
	db       AnalyzerDB
	q        queue.Queue
	contents ContentGetter
	sets     SetSource
	opt      Options
	met      *Metrics // the stage instruments (T-077); nil — no-op
}

// New builds the worker on top of a DB pool, a queue, a content store
// and a rule set source. nil dependencies are an error: a silent loss
// of analysis is invisible to the operator (the T-058/renderworker style).
func New(db AnalyzerDB, q queue.Queue, contents ContentGetter, sets SetSource, opts Options) (*Worker, error) {
	if db == nil {
		return nil, errors.New("analyzer: пул БД не задан")
	}
	if q == nil {
		return nil, errors.New("analyzer: очередь не задана")
	}
	if contents == nil {
		return nil, errors.New("analyzer: хранилище контента не задано")
	}
	if sets == nil {
		return nil, errors.New("analyzer: источник набора правил не задан")
	}
	return &Worker{db: db, q: q, contents: contents, sets: sets, opt: opts.withDefaults(), met: NewMetrics(opts.Metrics)}, nil
}

// analyzeRow is a read subdomain_results row: the input of the passive
// analysis (spec 5.8) — headers, cookies and content.
type analyzeRow struct {
	headers map[string][]string
	cookies []rules.Cookie
	hash    []byte // nil — no content (NULL content_hash, T-056)
}

// ProcessResult processes one q:analyze task (§5.8, §7): the passive
// analysis of the row against the current rule set, writing
// laravel_findings and recalculating the laravel_domains domain
// aggregate (T-072) in one transaction with the rules_version stamp.
// It returns true when the transaction committed — the findings are
// written (all hits regardless of the threshold) and the domain
// aggregate is recalculated, or, on an empty outcome, the old row is
// deleted and the stamp is put (§3.5: "no signs" is also an analysis
// result). false without an error — there is nothing to do: the row
// is gone (the domain was deleted/rechecked without this key) or it
// slipped out from under the task (the content changed after the read
// — the UPDATE guard; the render/recheck will publish their own
// q:analyze) — that is a success, nothing is written, the delivery is
// acknowledged. An error is a failure of any step: the delivery is not
// acknowledged and will come back as a repeated delivery (Processing,
// T-025); repeated processing is idempotent (upsert).
func (w *Worker) ProcessResult(ctx context.Context, task queue.Result) (bool, error) {
	row, found, err := w.selectResult(ctx, task)
	if err != nil {
		return false, err
	}
	if !found {
		w.opt.log().Debug("анализ не требуется — строки нет",
			"zone_id", task.ZoneID, "domain_id", task.DomainID,
			"subdomain_id", task.SubdomainID, "variant", int16(task.Variant))
		return false, nil
	}
	name := analyzeLogName(task)

	html, err := w.fetchContent(ctx, row.hash)
	if err != nil {
		return false, fmt.Errorf("analyzer: %s: %w", name, err)
	}

	// A rule set snapshot per delivery: Reloader.Set() is atomic,
	// EvaluateCtx works with an immutable Set; the set cannot change
	// mid-page, the next delivery takes the fresh one.
	set := w.sets.Set()
	if set == nil {
		// Symmetric to ReanalyzeLoop: a nil snapshot of an alternative
		// SetSource is a skip, not a panic. The delivery is acknowledged:
		// without the version stamp the row stays stale, re-analysis (T-074)
		w.opt.log().Warn("набор правил не загружен — анализ пропущен, строка останется для переанализа",
			"domain", name, "zone_id", task.ZoneID, "domain_id", task.DomainID)
		return false, nil
	}
	res := set.EvaluateCtx(ctx, rules.Page{
		HTML:    html,
		Cookies: row.cookies,
		Headers: row.headers,
	}, rules.Limits{}) // zero Limits → DefaultLimits (T-069)
	if res.Partial {
		// The time budget is exhausted (EvalTimeout) or ctx is cancelled —
		// not all rules were scanned: a truncated result must not be written
		// and stamped as full rules_version. The delivery is acknowledged:
		// the row stays stale, re-analysis (T-074) will return to it
		// (practically unreachable with RE2 on a bounded input).
		w.opt.log().Warn("анализ прерван до полного просмотра правил — findings не записаны, строка останется для переанализа",
			"domain", name, "signals", len(res.Signals), "rules_version", set.Version)
		return false, nil
	}
	isLaravel := res.IsLaravel(w.opt.MinScore)

	signals, err := marshalSignals(res.Signals)
	if err != nil {
		return false, fmt.Errorf("analyzer: %s: %w", name, err)
	}

	tx, err := w.db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("analyzer: %s: запись findings: транзакция: %w", name, err)
	}
	// Rollback unconditionally: after Commit it is a no-op (the T-044/T-058 style).
	defer func() { _ = tx.Rollback(ctx) }()

	// The stamp as the first step: the freshness guards decide whether
	// to write the findings (no row — it slipped out from under the task,
	// the domain is deleted, the content changed, or the version is
	// already current — a repeated delivery, T-204).
	// RETURNING checked_at — the tail bucket hour for the decrement below.
	var stampedAt time.Time
	err = tx.QueryRow(ctx, analyzeMarkSQL,
		task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant),
		set.Version, row.hash).Scan(&stampedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			w.opt.log().Debug("findings не записаны — строка перезаписана другим эпизодом, домен удалён или уже проанализирована этой версией",
				"domain", name, "zone_id", task.ZoneID, "domain_id", task.DomainID,
				"subdomain_id", task.SubdomainID, "variant", int16(task.Variant))
			return false, nil
		}
		return false, fmt.Errorf("analyzer: %s: штамп rules_version: %w", name, err)
	}

	if len(res.Signals) > 0 {
		// All hits regardless of the threshold (§13.1); a repeated
		// delivery and re-analysis rewrite (ON CONFLICT, §3.5).
		tag, err := tx.Exec(ctx, findingUpsertSQL,
			task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant),
			float32(res.Score), nullStr(res.VerMin), nullStr(res.VerMax),
			signals, isLaravel, set.Version)
		if err != nil {
			return false, fmt.Errorf("analyzer: %s: запись findings: %w", name, err)
		}
		if tag.RowsAffected() != 1 {
			return false, fmt.Errorf("analyzer: %s: запись findings: затронуто %d строк", name, tag.RowsAffected())
		}
	} else {
		// No rule hit — the row is not written (spec 5.8); an existing one
		// is deleted: the past analysis saw signs that are absent in the
		// current episode (§3.5).
		if _, err := tx.Exec(ctx, findingDeleteSQL,
			task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant)); err != nil {
			return false, fmt.Errorf("analyzer: %s: удаление findings: %w", name, err)
		}
	}

	// Domain aggregation — T-072: recalculating laravel_domains from
	// the domain's findings in the same transaction (the decisions and
	// the parallel delivery reconciliation are in aggregate.go); fresh
	// is the fresh HTTP episode sign in the task: on a transition it
	// writes the appeared/lost history (T-118); unfound is the re-analysis
	unfound, err := aggregateDomain(ctx, tx, task.ZoneID, task.DomainID, set.Version, task.Fresh)
	if err != nil {
		return false, fmt.Errorf("analyzer: %s: %w", name, err)
	}

	// find removal signal (T-187).
	// The reanalyze: tail bucket decrement (T-204) — the last step of
	// the transaction (the tail delta, the record.go style): the row was
	// moved stale→current — the published tail returns the bucket to zero,
	// the next tick's guard closes. A rollback rolls back the stamp and
	// the delta together (§3.4).
	if err := analyzeConsumeTail(ctx, tx, stampedAt); err != nil {
		return false, fmt.Errorf("analyzer: %s: %w", name, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("analyzer: %s: запись findings: фиксация: %w", name, err)
	}

	// The find removal by re-analysis (T-187) — after the commit: the
	// event has already happened, drain errors do not fail the delivery
	// (the findings and the aggregate are committed, the XACK will
	// happen; without the drain the domain's rows would stay behind the
	w.drainUnfound(ctx, task, name, unfound)

	w.opt.log().Debug("анализ записан",
		"domain", name, "signals", len(res.Signals), "score", res.Score,
		"is_laravel", isLaravel, "ver_min", res.VerMin, "ver_max", res.VerMax,
		"conflict", res.Conflict, "rules_version", set.Version)
	return true, nil
}

// analyzeConsumeTail — the decrement of the unanalyzed tails bucket
// reanalyze: in the OPEN stamp transaction (T-204): stampedAt is the
// bucket hour (RETURNING analyzeMarkSQL), the delta is −1 (one row
// moved stale→current). The upsert creates the hour bucket itself
// (the decrement can theoretically outrun the increment — a duplicate
// delivery after the episode row is lost) — RowsAffected is
// guaranteed to be 1, the check is a guard for direct calls with
// foreign SQL (a silent delta loss would keep the tick guard open
// until the reconciliation). Returning an error rolls back the
// caller's transaction — the stamp and the bucket change only together (§3.4).
func analyzeConsumeTail(ctx context.Context, tx pgx.Tx, stampedAt time.Time) error {
	tag, err := tx.Exec(ctx, analyzeReanalyzeConsumeSQL, stampedAt, int64(-1))
	if err != nil {
		return fmt.Errorf("корзина хвостов переанализа: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("корзина хвостов переанализа: затронуто %d строк (upsert корзины часа обязан 1)",
			tag.RowsAffected())
	}
	return nil
}

// drainUnfound — the domain content point drain on a find removal by
// re-analysis (T-187): the aggregation's unfound signal (aggregateDomain,
// a laravel_domains row transition by a delivery without fresh) means
// the domain's old subdomain_results rows returned to the retention
// candidate set behind the drain cursor (T-185) — the hourly pass will
// not see them anymore. The drain is content.DrainDomain: the same
// selection filters and the T-166 purge mechanics over the PK prefix
// (zone_id, domain_id). It is called after the delivery transaction
// commits: a drain failure is a Warn and a metric, the delivery is
// acknowledged (the analysis is the core work; rows without a drain
// stay in the pre-T-187 class, the rare T-185 catch-up circles or the
// T-030 recheck will pick them up). Idempotent: a repeat finds no
// candidates. A ctx cancellation is part of the stop, not an alarm.
func (w *Worker) drainUnfound(ctx context.Context, task queue.Result, name string, unfound bool) {
	if !unfound {
		return
	}
	w.met.Unfound()
	if w.opt.DrainDomain == nil || w.opt.Retention <= 0 {
		// The T-187 minimum with the drain off: retention = 0 — content
		// is kept forever (no retention cycle at all, nothing piles up
		// behind the cursor); the seam is not set — an isolated build.
		// The event is observable via the metric and the log.
		w.opt.log().Info("снятие находки переанализом — точечный дренаж контента выключен",
			"domain", name, "zone_id", task.ZoneID, "domain_id", task.DomainID)
		return
	}
	stats, err := w.opt.DrainDomain(ctx, w.db, w.opt.Retention, task.ZoneID, task.DomainID)
	if err != nil {
		if ctx.Err() == nil {
			w.opt.log().Warn("снятие находки переанализом: точечный дренаж контента домена не выполнен — записи останутся позади курсора retention",
				"domain", name, "zone_id", task.ZoneID, "domain_id", task.DomainID, "err", err)
		}
		w.met.UnfoundDrainError()
		return
	}
	w.met.UnfoundDrain(stats.Rows)
	if stats.Rows > 0 {
		w.opt.log().Info("снятие находки переанализом — контент домена дренирован точечно",
			"domain", name, "zone_id", task.ZoneID, "domain_id", task.DomainID,
			"rows", stats.Rows, "refs", stats.Refs, "contents", stats.Contents,
			"freed_bytes", stats.FreedBytes)
	}
}

// selectResult reads the analysis input by the task key: headers,
// cookies and the content reference. The SELECT is outside a
// transaction: a concurrent rewrite of the row between the read and
// the UPDATE is caught by the analyzeMarkSQL guard (the
// renderworker.selectTarget style).
func (w *Worker) selectResult(ctx context.Context, task queue.Result) (analyzeRow, bool, error) {
	rows, err := w.db.Query(ctx, analyzeSelectSQL,
		task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant))
	if err != nil {
		return analyzeRow{}, false, fmt.Errorf("analyzer: чтение строки результата: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		var headers, cookies *string
		var hash []byte
		if err := rows.Scan(&headers, &cookies, &hash); err != nil {
			return analyzeRow{}, false, fmt.Errorf("analyzer: чтение строки результата: %w", err)
		}
		row := analyzeRow{hash: hash}
		if row.headers, err = parseHeadersJSON(headers); err != nil {
			return analyzeRow{}, false, fmt.Errorf("analyzer: заголовки строки результата: %w", err)
		}
		if row.cookies, err = parseCookiesJSON(cookies); err != nil {
			return analyzeRow{}, false, fmt.Errorf("analyzer: cookies строки результата: %w", err)
		}
		return row, true, nil // by PK — at most one row
	}
	if err := rows.Err(); err != nil {
		return analyzeRow{}, false, fmt.Errorf("analyzer: чтение строки результата: %w", err)
	}
	return analyzeRow{}, false, nil
}

// fetchContent fetches the content for matching the html/meta targets
// (T-069): a NULL/truncated content_hash and missing content (the
// references are gone) mean no content, the analysis continues by
// headers and cookies (spec 5.8: rules over six targets, html/meta is
// part of the set, not the whole analysis). An empty DOM is the same
// outcome: the engine finds no html/meta signs on an empty input,
// cookies and headers keep working.
func (w *Worker) fetchContent(ctx context.Context, hash []byte) ([]byte, error) {
	if len(hash) != content.HashLen {
		return nil, nil
	}
	var h content.Hash
	copy(h[:], hash)
	data, err := w.contents.Get(ctx, h)
	if errors.Is(err, content.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("чтение контента %s: %w", h.String(), err)
	}
	return data, nil
}

// analyzeLogName is the name for logs: the domain name from the
// message, otherwise the row key (Name is optional, T-024).
func analyzeLogName(task queue.Result) string {
	if task.Name != "" {
		return task.Name
	}
	return fmt.Sprintf("%d/%d/%d/%d", task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant))
}

// nullStr — an empty version bound → NULL (absent or a conflict,
// T-070).
func nullStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// signalJSON is an evidence in the format of the
// laravel_findings.signals column (migration 000001:
// [{rule, weight, evidence}, …]). The tags are lowercase per the §6.1
// schema sketch; rules.Evidence has no tags — the writing stage
type signalJSON struct {
	Rule     string  `json:"rule"`
	Weight   float64 `json:"weight"`
	Evidence string  `json:"evidence"`
}

// introduces the jsonb format.
// marshalSignals serializes the hits; an empty set is an empty string
// (the call is not made: the findings row is not written at all).
func marshalSignals(sigs []rules.Evidence) (string, error) {
	if len(sigs) == 0 {
		return "", nil
	}
	out := make([]signalJSON, 0, len(sigs))
	for _, s := range sigs {
		out = append(out, signalJSON{Rule: s.Rule, Weight: s.Weight, Evidence: s.Evidence})
	}
	b, err := json.Marshal(out)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// parseHeadersJSON parses the subdomain_results.headers jsonb: the
// T-058 contract (http.Header — canonical MIME names, array values)
// and T-067 (the render's final navigation headers). Parsing into
// map[string][]string, not http.Header: the writing stage has already
// canonicalized, and rules.Page.Headers expects exactly a map (T-069).
func parseHeadersJSON(s *string) (map[string][]string, error) {
	if s == nil || *s == "" {
		return nil, nil
	}
	var h map[string][]string
	if err := json.Unmarshal([]byte(*s), &h); err != nil {
		return nil, err
	}
	return h, nil
}

// cookieJSON is a record of the subdomain_results.cookies jsonb: the
// T-058 (recordCookie) and T-067 (render.ExtractCookie) contracts —
// {name, value} plus attributes; the rule engine needs the name and
// the value (spec 5.8), the attributes are ignored (unknown JSON fields are skipped).
type cookieJSON struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func parseCookiesJSON(s *string) ([]rules.Cookie, error) {
	if s == nil || *s == "" {
		return nil, nil
	}
	var raw []cookieJSON
	if err := json.Unmarshal([]byte(*s), &raw); err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	out := make([]rules.Cookie, 0, len(raw))
	for _, c := range raw {
		out = append(out, rules.Cookie{Name: c.Name, Value: c.Value})
	}
	return out, nil
}

// --- work loop --------------------------------------------------------------

// Run is the analyzer work loop (spec 4.2, §5.8, §7): it reads
// q:analyze tasks with the consumer group in a Consume window and
// processes deliveries in parallel up to Concurrency, each with the
// Processing pattern (T-025): analysis → writing the findings and the
// rules_version stamp to the DB → XACK. It runs until ctx is
// cancelled and returns nil — the OnRun contract of the framework
// (T-012; started deliveries finish their work or return to the queue
// for a repeated delivery, spec 3.4). A read or processing error does
func Run(ctx context.Context, w *Worker, opts Options) error {
	if w == nil {
		return errors.New("analyzer: цикл воркера: воркер не задан")
	}
	o := opts.withDefaults()
	log := o.log()
	log.Info("цикл analyzer",
		"group", o.Group, "consumer", o.Consumer,
		"concurrency", o.Concurrency, "consume", o.Consume,
		"min_score", w.opt.MinScore)

	wait := func(d time.Duration) {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
		case <-t.C:
		}
	}

	// not stop the loop: it is logged (a Warn), an unacknowledged delivery
	// will come back via XAUTOCLAIM (spec 4.2).
	// Returning stuck deliveries (T-025): a background RecoverLoop on
	// behalf of the same consumer — an XAUTOCLAIM claim and the same task
	// processing. It lives until ctx is cancelled; Run waits for its exit
	// (a started task finishes or stays unacknowledged, spec 3.4).
	if o.Recover != nil {
		recDone := make(chan struct{})
		go func() {
			defer close(recDone)
			_ = queue.RecoverLoop(ctx, o.Recover, w.processDelivery, queue.RecoverOptions{
				Stream:   queue.StreamAnalyze,
				Group:    o.Group,
				Consumer: o.Consumer,
				// The PEL traversal rate is a queue knob, not a stage one
				// (redis.recover_*, T-153); zeros — the queue defaults.
				Batch:    o.RecoverBatch,
				Interval: o.RecoverInterval,
				Log:      log,
			})
		}()
		defer func() { <-recDone }()
	}

	sem := make(chan struct{}, o.Concurrency)
	for {
		deliveries, err := w.q.Consume(ctx, queue.StreamAnalyze, o.Group, o.Consumer, o.Consume)
		if err != nil {
			if ctx.Err() != nil {
				return nil // graceful shutdown
			}
			log.Warn("цикл analyzer: чтение q:analyze", "err", err)
			wait(DefaultErrorWait)
			continue
		}

		var wg sync.WaitGroup
		for _, d := range deliveries {
			// An equivalent mutant (T-M10): cutting the break out starts the
			// remaining window deliveries even after ctx is cancelled, but
			// their processing fails entirely on the cancelled context
			// (SELECT/transaction/XACK reject ctx both in the drivers and the
			// fakes) with no DB writes and no acknowledgment — the record stays
			// in pending, as with not calling it at all; the Warn is gated by
			// mutator-disable-next-line branch/if
			if ctx.Err() != nil {
				// ctx.Err() == nil below.
				// An equivalent mutant (T-M10): break → continue walks the rest
				// of the window without starting deliveries and reaches the
				// post-check of ctx after wg.Wait — the same state and a nil return.
				// mutator-disable-next-line loop/break
				break // the window deliveries not started stay unacknowledged
			}
			sem <- struct{}{}
			wg.Add(1)
			go func(d queue.Delivery) {
				defer wg.Done()
				defer func() { <-sem }()
				err := queue.Processing(ctx, w.q, o.Group, d, w.processDelivery)
				if err != nil && ctx.Err() == nil {
					log.Warn("задача q:analyze не подтверждена — вернётся повторной доставкой",
						"entry", d.ID, "err", err)
				}
			}(d)
		}
		wg.Wait()
		if ctx.Err() != nil {
			return nil // stopping: the window is drained or returned to the queue
		}
	}
}

// processDelivery processes one q:analyze delivery: decoding the
// Result key and the analysis. Processing does the acknowledgment
// (XACK) after a nil return — "XACK only after the result is written
// to the DB" (spec 4.2); a poison row (outside the T-024 format) is
// returned as an error — the queue will exhaust the attempts and move
// it to q:dead (T-025). A successful processing goes into the stage
// instruments (T-077): the task counter and the duration.
func (w *Worker) processDelivery(ctx context.Context, del queue.Delivery) error {
	task, err := queue.Decode[queue.Result](del)
	if err != nil {
		return fmt.Errorf("analyzer: запись %s стрима %s: %w", del.ID, del.Stream, err)
	}
	start := time.Now()
	if _, err := w.ProcessResult(ctx, task); err != nil {
		return fmt.Errorf("analyzer: домен %s: анализ: %w", task.Name, err)
	}
	w.met.Task(time.Since(start))
	return nil
}

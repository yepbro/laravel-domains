// Content retention and jsonb trimming of non-finds (T-166; specs 5.7, 5.5,
// 6.1, 3.2).
//
// No new capacity on disk (the operator's decision of 2026-10-23): volume
// expansion is unavailable, S3 and archiving are postponed; growth of
// ≈36 GiB/day (content 20.3 + DB 15.5, measurement docs/load-measurements.md
// §12) is stopped by a time window instead of capacity. Content and heavy
// jsonb are needed by the pipeline for a limited time: the analyzer reads
// the bodies and headers/cookies (analyzeSelectSQL, spec 5.8), the render
// replaces the content of SPA records (spec 5.6), the re-analysis of finds
// (T-074) re-applies the rules to the stored pages. A record aged beyond
// content.retention and no longer needed by these consumers loses its
// content reference and jsonb; the row's passport (http_status, final_url,
// error, response_ms, checked_at, scheme, subdomain_id, variant) always
// remains — domain_info (spec 5.11) and the dashboard keep answering.
//
// Selection criteria (all parts are mandatory):
//
//   - checked_at older than content.retention: the time is written by the
//     DB server's now() (T-058), and the same source defines the aging
//     boundary — one source of truth;
//   - the domain is NOT a Laravel find: no laravel_domains row (a find is
//     confident firings, the §13.1 threshold, aggregate.go). Finds keep
//     their content always: the proof and the material of the T-074
//     re-analysis; weak firings (laravel_findings with is_laravel=false)
//     do not count as a find — the deviation is recorded in the task;
//   - the record is not waiting for a render: the domain has no
//     render_pending flag, or the record is already rendered. The T-066
//     rework pass re-reads the content for a repeated SPA verdict;
//     rendered=true means the render has already replaced the content
//     (T-067) and the old one is not needed;
//   - the domain is not deleted (§13.1: deleted — the territory of the
//     T-020 background cleanup).
//
// A single UPDATE removes both the reference and the jsonb:
// content_hash → NULL, headers/cookies/tls/redirects/addrs/cname → NULL.
// The columns are unindexed (no indexes on subdomain_results except the
// PK) — the UPDATE stays HOT (fillfactor 85, migration 000002), and the
// autovacuum returns the pages' space. Cutting the reference and
// decreasing contents.refcount is ONE transaction (spec 3.4): a failure
// between them would leave content without an owner or a refcount
// permanently too high (the T-044 invariants). A contents row whose
// refcount reached zero is deleted immediately, not merely marked dead
// (as in the T-020 cleanup): retention is a mass source of dead blocks,
// and otherwise contents rows would accumulate until the compaction of
// the affected packs (a pack with a live share ≥ the threshold does not
// start it). The physical space of the blocks in packs is returned by the
// T-045 compaction: purge only ends the content's claim on space.
//
// The q:analyze interlock: the analyzer is the consumer of the bodies and
// headers/cookies; the stream depth (the consumption group backlog,
// T-026) at the threshold means "cannot keep up" — the queued records are
// not yet triaged. The pass and every drain batch are skipped while the
// depth ≥ the threshold: the bodies are still needed. A depth read error
// is also a skip (conservatively, like HasCapacity in T-066): purging
// "blindly" is costlier than deferring the pass. A threshold of 0 means
// the interlock is off.
//
// Exact protection by unconsumed messages (retentionpending.go): the
// render_pending flag does not cover records whose messages are ALREADY
// published to q:render/q:analyze (the publication happened — no flag),
// and the interlock does not see stale messages when the depth is below
// the threshold; before a pass, the keys of unconsumed messages are read
// from the streams, and records waiting for consumers are excluded from
// the drain batches.
//
// Batches and the drain: the candidate selection is cursor pagination by
// time (checked_at, PK) > cursor (T-185): the checked_at boundaries are
// served by the subdomain_results_checked_idx index (T-171), and every
// batch is a short transaction (the T-020 style). The cycle cursor
// survives passes: a row is visited once in its lifetime (a trimmed one
// stays behind the cursor; a re-checked DELETE+INSERT with a fresh
// checked_at enters the drain again when it ages), so the pass cost ∝ the
// inflow, not the table size. In exchange for the abolished full circles —
// explicit bounded retries (retentiondrain.go): a bounded list of
// queue-guard keys with point offers, and a catch-up circle on the
// render-debt counter reset to zero. A repeated pass is safe: trimmed
// records fall out of the selection (content_hash and jsonb are already
// NULL), and the UPDATE guards screen the rewrite races (the T-030 recheck
// writes a fresh checked_at and new content — a double check of checked_at
// and content_hash IS NOT DISTINCT FROM).
//
// Recorded deviations (agreed with the customer, T-166): re-analysis of
// non-finds by new rules is impossible (the apply happens on a recheck
// ≈6 months); the recheck loses the "did the site change" comparison (the
// DELETE+INSERT rewrite writes the result from scratch, T-058).
//
// The cycle lives in http-worker (cmd/http-worker): the service already
// holds the DB pool, the stream-depth gate (the interlock), and the
// metrics registry, next to the T-045 compaction that the purge also
// "feeds"; a second owner does not need the cycle (the guards make a
// parallel run safe but useless — as with compaction).
package content

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// Cycle defaults (spec 13.2: the default values are refined by
// measurements; the working values come from configuration, the constants
// are the single source for both the service build and the tests).
const (
	// DefaultRetention — the lifetime window of non-find content: 6 h.
	// Measurement §12: the steady-state content volume = the daily inflow
	// × the window — at 20 GiB/day a 48 h window would hold ≈40 GiB and
	// eat the volume; 6 h ≈ 5 GiB; the window covers the consumers'
	// latencies (analysis — seconds per record) with a margin for failures
	// and queue lags.
	DefaultRetention = 6 * time.Hour

	// DefaultRetentionInterval — the period of the RetentionLoop passes:
	// an hour, like pack compaction (T-045) — the space is returned by its
	// pass, retention only prepares the dead blocks; more often — extra
	// aging scans, less often — the window drifts.
	DefaultRetentionInterval = time.Hour

	// DefaultRetentionBatch — rows in one batch-transaction. The sizing
	// range is 10–50k; the lower bound drains an inflow of ≈3M rows/day
	// (measurement §12) in ~13 batches of an hourly pass, and the
	// transaction holds the batch's row locks for the minimal time.
	DefaultRetentionBatch = 10000

	// DefaultRetentionSelectTimeout — the cap of one batch selection
	// (T-271): the server-side statement_timeout in the plan-pinning tx
	// wrapper and the client-side ctx deadline of the selection — one cap
	// by two levels (statement_timeout cuts the statement itself, ctx —
	// the wrapper and the network black holes); a stuck query is
	// impossible, and a retry is idempotent — the batch is not committed,
	// the cursor does not move. 7200 s (T-274; it was 600 for T-271 and
	// 3600 for T-273): the timeout must cover not only regular batches
	// (measurement T-201: median 13.8 s, max 222.8 s) but also the FIRST
	// catch-up pass from the zero cursor after retention downtime — the
	// "anything to purge" predicate is outside the index, and every row of
	// the clean skip prefix requires a heap visit; production measurements
	// of 2026-11-10/11: ~60 min idle, >60 min under full front load —
	// 3600 s (T-273) sufficed only for a quiet system (passes 1–3 under
	// front load hit the cap, retention_errors_total = 3, and the drain
	// continued only in the quiet window). 7200 s is double the margin
	// over the upper bound of the measurements under load; the working
	// value comes from configuration (content.retention_select_timeout,
	// T-274) — a cap change without a deploy. The persistent cursor
	// (T-274) shortens the catch-up passes to the first one after a
	// downtime but does not abolish them. An overrun — a pass error (the
	// cycle's Warn, the errors_total metric) and the ticker backoff.
	DefaultRetentionSelectTimeout = 7200 * time.Second

	// DefaultRetentionErrorBackoff — the cycle ticker pause after a failed
	// pass (T-271). The 2026-11-08/09 incident: a pass interrupted by an
	// OOM kill of PostgreSQL met a tick that had fired during the long
	// pass (the runtime keeps one undelivered tick and delivers it
	// immediately when the loop returns to select) and restarted
	// instantly into the just-booted DB — the passes went without a pause
	// until they crashed it again (overnight: five kills, not one
	// successful pass). 15 minutes is an order of magnitude longer than
	// the PG boot (hundreds of seconds); shorter than the hourly interval
	// deliberately: a pass error is a signal of DB unhealthiness, the
	// retry comes earlier than the next regular tick but much later than
	// the PG boot; the inflow drain is not lost (the cursor survives the
	// pause), and the first successful pass returns the regular interval.
	DefaultRetentionErrorBackoff = 15 * time.Minute
)

// The retention SQL. Schema codes (the render_pending bit, the deleted
// status) — as literals, not data (the record.go style, T-058), hence var,
// like the renderq.go flag SQL; the texts are pinned by tests
// (retention_test.go). The aging boundary — now() − interval on the
// server: checked_at is written by the same now() of the writing
// transaction (T-058), one time source; inside a transaction now() is
// stable (the moment of Begin).
var (
	// retentionSelectSQL — a batch of candidates: there is something to
	// purge (a live content_hash or jsonb), the record has aged, does not
	// wait for a render, and the domain is alive and not a find. The
	// row-value > cursor over (checked_at, PK) — the drain pagination by
	// time (T-185, formerly by PK): both checked_at boundaries are the
	// conditions of the subdomain_results_checked_idx index (T-171), and
	// the PG16 planner derives the lower one from the row-value itself
	// (verified with EXPLAIN: Index Cond checked_at >= $2 AND checked_at <
	// now()-$1) — the scan must read only the aging window ahead of the
	// cursor, not the heap of the whole history (the diagnosis of
	// slow-query entry #13 of Oct 24); ORDER BY by the same key —
	// deterministic batches: the narrow window of a steady drain is
	// assembled by Incremental Sort over the index order, the wide one
	// (a catch-up circle from zero) — by a Sort of the slice (the EXPLAIN
	// measurement of Oct 24, PG 16.15). checked_at in the column list — the
	// cursor of the next batch.
	//
	// Scale and cost (the T-201 measurement, Oct 26 — so that the next
	// slowness analysis does not look for a "lost index"): the actual
	// aging window is ≈84k rows/h (the subdomain_results inflow ≈2–3M per
	// day), not the "≈15k/h" estimate of T-185 §2 — the constant was low
	// by a factor of 5.6, hence the "0.3–1 s" estimates against the actual
	// 5–25 s. The cost of the shape is the necessary minimum (EXPLAIN
	// ANALYZE T-201 §2.2): the payload columns (content_hash, jsonb) are
	// outside the checked_at index and cannot be indexed (the HOT
	// invariant — the purge is millions of UPDATEs per day,
	// T-104/T-175); every row of the window requires a heap visit (~70k
	// per batch, 65% of them already trimmed); the JOIN domains is point
	// PK probes; the cursor guarantees a row is visited once in its
	// lifetime. The slowness is multiplied by I/O starvation and the pass's
	// parallel workers: the service pool is serial (db.NoParallelGather,
	// T-201); the reference measurement — after the render hardware
	// expansion (TZ T-201 §3.3).
	retentionSelectSQL = "SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, r.content_hash, r.checked_at " +
		"FROM subdomain_results AS r " +
		"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id " +
		"WHERE r.checked_at < now() - $1::interval " +
		"AND (r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant) " +
		"> ($2::timestamptz, $3::int2, $4::int8, $5::int2, $6::int2) " +
		"AND (r.content_hash IS NOT NULL OR r.headers IS NOT NULL OR r.cookies IS NOT NULL " +
		"OR r.tls IS NOT NULL OR r.redirects IS NOT NULL OR r.addrs IS NOT NULL OR r.cname IS NOT NULL) " +
		fmt.Sprintf("AND (r.rendered OR (d.flags & %d) = 0) ", int16(model.FlagRenderPending)) +
		fmt.Sprintf("AND d.status <> %d ", int16(model.StatusDeleted)) +
		"AND NOT EXISTS (SELECT 1 FROM laravel_domains AS ld " +
		"WHERE ld.zone_id = r.zone_id AND ld.domain_id = r.domain_id) " +
		"ORDER BY r.checked_at, r.zone_id, r.domain_id, r.subdomain_id, r.variant " +
		"LIMIT $7"

	// retentionPlanNoSeqScanSQL / retentionPlanNoHashJoinSQL /
	// retentionPlanNoMergeJoinSQL / retentionPlanNoBitmapScanSQL —
	// pinning of the batch selection plan shape
	// (T-271; the sample of T-181/T-191 — SET LOCAL in the selection tx
	// wrapper): seq scan and hash/merge joins are disabled before the
	// selection statement, and the planner keeps an Index Scan over
	// subdomain_results_checked_idx + a Nested Loop of point probes into
	// domains/laravel_domains + a Limit with an early stop. The
	// 2026-11-08/09 incident: without pinning a degraded plan (a seq scan
	// of 9M rows + hash join domains + Sort, ≈1 GiB of working structures
	// on top of 2 GiB shared_buffers), together with the neighboring
	// render-late scan, brought PostgreSQL down with five OOM kills
	// overnight; the batches did not commit — the cursor did not move, and
	// every pass went as a catch-up circle from the zero cursor. The
	// bitmap scan is disabled there as well (the T-271 fine-tuning on the
	// integration EXPLAIN): with seq/hash/merge disabled, the planner
	// picks Bitmap Heap + Bitmap Index Scan on the cursor predicate — the
	// bitmap does not preserve the index order, ORDER BY (checked_at, PK)
	// is closed by an explicit Sort, and the shape stops being
	// deterministic (it depends on which path the planner fancied at the
	// given volume); with the bitmap off there is one path — an Index Scan
	// (+ Incremental Sort when needed).
	// enable_sort is NOT disabled:
	// ORDER BY (checked_at, PK) keeps Incremental Sort over the index
	// order (the narrow drain window); a Sort of the slice — only a wide
	// catch-up circle. SET LOCAL acts for this transaction only; the
	// pool's other queries are unaffected.
	retentionPlanNoSeqScanSQL    = "SET LOCAL enable_seqscan = off"
	retentionPlanNoHashJoinSQL   = "SET LOCAL enable_hashjoin = off"
	retentionPlanNoMergeJoinSQL  = "SET LOCAL enable_mergejoin = off"
	retentionPlanNoBitmapScanSQL = "SET LOCAL enable_bitmapscan = off"

	// retentionSelectTimeoutFmt — the server-side selection cap by the
	// same tx wrapper (T-271): the '<N>ms' format is valid for the GUC
	// time parser at any duration (the style of db.lockTimeoutSQLFmt;
	// Duration.String() does not fit — "1h0m0s" is not parsed). The value
	// is RetentionOptions.SelectTimeout; the text is pinned by a unit test
	// (retention_test.go).
	retentionSelectTimeoutFmt = "SET LOCAL statement_timeout = '%dms'"

	// retentionProbeSQL — the point offer of the queue-guard retries
	// (T-185): keys deferred by the q:render/q:analyze snapshot sit behind
	// the cursor and will not return with a full circle (the circles are
	// abolished) — the next pass offers them with a PK probe carrying the
	// selection filters but no cursor. The plan's pointness is pinned by
	// JOIN LATERAL LIMIT 1 (review of T-185, P2-1): with a plain
	// unnest-join and a long list (a queue standstill — and the list is
	// long then, the limit of 100k), the planner re-picks a hash join +
	// seq scan over the entire aged history — the trail this task removes;
	// LATERAL keeps an Index Scan by PK per key at any list volume
	// (verified with EXPLAIN on PG16); the plan-shape pinning style is
	// like the SET LOCAL of the T-181 selection. Keys that stopped being
	// candidates (the protection is gone and the purge happened by another
	// mechanism, a recheck rewrite with a fresh checked_at, a find, a
	// render wait — the catch-up circle on the debt counter will catch up
	// with it) are not returned by the probe — the key falls out of the
	// list (retentiondrain.go).
	retentionProbeSQL = "SELECT k.zone_id, k.domain_id, k.subdomain_id, k.variant, r.content_hash " +
		"FROM unnest($1::int2[], $2::int8[], $3::int2[], $4::int2[]) " +
		"AS k(zone_id, domain_id, subdomain_id, variant) " +
		"JOIN LATERAL (" +
		"SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, r.content_hash " +
		"FROM subdomain_results AS r " +
		"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id " +
		"WHERE r.zone_id = k.zone_id AND r.domain_id = k.domain_id " +
		"AND r.subdomain_id = k.subdomain_id AND r.variant = k.variant " +
		"AND r.checked_at < now() - $5::interval " +
		"AND (r.content_hash IS NOT NULL OR r.headers IS NOT NULL OR r.cookies IS NOT NULL " +
		"OR r.tls IS NOT NULL OR r.redirects IS NOT NULL OR r.addrs IS NOT NULL OR r.cname IS NOT NULL) " +
		fmt.Sprintf("AND (r.rendered OR (d.flags & %d) = 0) ", int16(model.FlagRenderPending)) +
		fmt.Sprintf("AND d.status <> %d ", int16(model.StatusDeleted)) +
		"AND NOT EXISTS (SELECT 1 FROM laravel_domains AS ld " +
		"WHERE ld.zone_id = r.zone_id AND ld.domain_id = r.domain_id) " +
		"LIMIT 1) AS r ON true"

	// retentionDomainSQL — the point drain of a single domain's content
	// (T-187, retentiondomain.go): removing a Laravel find by re-analysis
	// (aggregateDomain, non-fresh) returns the domain's aged records to the
	// candidate set BEHIND the drain cursor (a row is visited once in its
	// lifetime, T-185) — the hourly pass will no longer see them. The
	// filters are the same as retentionSelectSQL's, but addressing is by
	// the PK prefix (zone_id, domain_id) and without a cursor: the leaf of
	// a partition/subpartition (T-005) cuts the selection down to the
	// domain's rows, and the drain cursor is not touched (trimmed rows
	// will fall out of any future circle on their own). A domain's rows are
	// a bounded set (subdomains 0..62 × variants, §5.10) — no LIMIT is
	// needed; ORDER BY PK — deterministic batch arguments (the
	// cleanupRefcounts style, T-020) and the row lock order.
	retentionDomainSQL = "SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, r.content_hash, r.checked_at " +
		"FROM subdomain_results AS r " +
		"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id " +
		"WHERE r.zone_id = $1::int2 AND r.domain_id = $2::int8 " +
		"AND r.checked_at < now() - $3::interval " +
		"AND (r.content_hash IS NOT NULL OR r.headers IS NOT NULL OR r.cookies IS NOT NULL " +
		"OR r.tls IS NOT NULL OR r.redirects IS NOT NULL OR r.addrs IS NOT NULL OR r.cname IS NOT NULL) " +
		fmt.Sprintf("AND (r.rendered OR (d.flags & %d) = 0) ", int16(model.FlagRenderPending)) +
		fmt.Sprintf("AND d.status <> %d ", int16(model.StatusDeleted)) +
		"AND NOT EXISTS (SELECT 1 FROM laravel_domains AS ld " +
		"WHERE ld.zone_id = r.zone_id AND ld.domain_id = r.domain_id) " +
		"ORDER BY r.zone_id, r.domain_id, r.subdomain_id, r.variant"

	// retentionDebtSQL — the point read of the render-debt counter
	// crawler_counters['render_pending'] (T-175) at the start of every
	// pass (T-185): the counter's transition to 0 is the moment all render
	// protection is lifted at once, and the cycle plans a catch-up circle
	// from the zero cursor. The text is the same point SELECT as the
	// rework pass guard renderDebtCounterSQL (T-181, internal/httpworker/
	// renderq.go): the helper is not exported, and the literal is
	// duplicated with an anchor (the style of pendingFieldData in
	// cmd/http-worker/main.go); the counter contract (migration 000015,
	// the bit transitions are a ±1 pair) does not change — retention only
	// reads.
	retentionDebtSQL = "SELECT value FROM crawler_counters WHERE name = 'render_pending'"

	// retentionPurgeSQL — the batch purge: the content reference and jsonb
	// → NULL by one statement (one row version — one HOT update instead
	// of two). A double guard against the race with a result rewrite (the
	// T-030 recheck rewrites rows DELETE+INSERT with a fresh checked_at
	// and possibly the same content_hash): a re-check of the age and
	// content_hash IS NOT DISTINCT FROM what was read (NULL is correct —
	// "no content" is legal, T-056; the comparison as in renderUpdateSQL
	// T-067). RETURNING k.hash — the hash from the unnest input (the new
	// value of the row is always NULL): the non-NULL returned hashes
	// count how many references were cut and by how much to decrease the
	// refcount. The row's passport (http_status, final_url, error,
	// response_ms, checked_at, scheme, rendered, rules_version) is not
	// touched.
	retentionPurgeSQL = "UPDATE subdomain_results AS r " +
		"SET content_hash = NULL, headers = NULL, cookies = NULL, " +
		"tls = NULL, redirects = NULL, addrs = NULL, cname = NULL " +
		"FROM unnest($1::int2[], $2::int8[], $3::int2[], $4::int2[], $5::bytea[]) " +
		"AS k(zone_id, domain_id, subdomain_id, variant, hash) " +
		"WHERE r.zone_id = k.zone_id AND r.domain_id = k.domain_id " +
		"AND r.subdomain_id = k.subdomain_id AND r.variant = k.variant " +
		"AND r.checked_at < now() - $6::interval " +
		"AND r.content_hash IS NOT DISTINCT FROM k.hash " +
		"RETURNING k.hash"

	// retentionDecrefSQL — decreasing the refcount by the actually cut
	// references (aggregated by hash: one content may be referenced from
	// many records — deduplication, spec 5.7). GREATEST keeps the counter
	// non-negative on a desync (the style of cleanupUpdateSQL T-020: a
	// minus would break the "0 = dead" invariant); RETURNING yields the
	// new value — the rows that reached zero are deleted by the next
	// statement, and len is needed for the freed-bytes counter.
	retentionDecrefSQL = "UPDATE contents AS c " +
		"SET refcount = GREATEST(c.refcount - h.n, 0) " +
		"FROM unnest($1::bytea[], $2::int4[]) AS h(hash, n) " +
		"WHERE c.hash = h.hash " +
		"RETURNING c.hash, c.refcount, c.len"

	// retentionDeleteSQL — deleting the contents rows that lost their last
	// reference. The refcount <= 0 guard (the sqlCompactDead style,
	// T-045): between the decref and the delete of one transaction there
	// are no resurrections; the guard is the insurance of the "0 = dead"
	// invariant.
	retentionDeleteSQL = "DELETE FROM contents " +
		"WHERE hash = ANY($1::bytea[]) AND refcount <= 0"
)

// RetentionDB — the pool for retention: a db.DB from which a transaction
// can be started (the style of ingest.CleanupDB). Cutting the references
// and decreasing the refcount are atomic in one transaction (spec 3.4);
// *pgxpool.Pool satisfies the interface.
type RetentionDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// compile-time: the real pool fits the interface.
var _ RetentionDB = (*pgxpool.Pool)(nil)

// RetentionDepth — the source of the q:analyze depth for the interlock: a
// depth (the consumption group backlog — min(lag, XLEN) + pending,
// T-026/T-168) at the threshold means the analyzer cannot keep up and the
// queued records are not yet triaged.
// A narrow interface — the package does not depend on the gate's Redis
// specifics (the style of RenderLen T-066); the service build wires in an
// adapter over BackpressureGate.
type RetentionDepth interface {
	AnalyzeDepth(ctx context.Context) (int64, error)
}

// RetentionOptions — the cycle parameters; zero values are replaced by the
// defaults (withDefaults, the style of CleanupOptions T-020).
type RetentionOptions struct {
	// Retention — the lifetime window of non-find content
	// (DefaultRetention); 0 — retention is off (RetentionLoop exits at
	// once).
	Retention time.Duration
	// Interval — the period of the cycle passes (DefaultRetentionInterval);
	// the one-off RetentionOnce is not affected.
	Interval time.Duration
	// BatchSize — the maximum rows in one batch-transaction
	// (DefaultRetentionBatch).
	BatchSize int
	// SelectTimeout — the cap of one batch selection
	// (DefaultRetentionSelectTimeout, T-271): the server-side
	// statement_timeout of the plan-pinning tx wrapper and the client-side
	// ctx deadline of the selection; an overrun — a pass error, and the
	// retry is idempotent (the batch is not committed).
	SelectTimeout time.Duration
	// ErrorBackoff — the ticker pause after a failed pass
	// (DefaultRetentionErrorBackoff, T-271): a tick fired during a long
	// pass must not restart it immediately into a just-booted PostgreSQL
	// (the 2026-11-08/09 incident — five OOM kills overnight).
	ErrorBackoff time.Duration
	// Gate — the semaphore of the uniqueness of http-worker's heavy
	// background scans (db.ScanGate, T-271): the retention selection and
	// the render late re-publisher do not coincide in PostgreSQL; nil —
	// without serialization (isolated builds, unit tests).
	Gate db.ScanGate
	// Depth — the source of the q:analyze depth (the interlock); nil —
	// the interlock is off (the pass does not wait for the analyzer — a
	// deliberate mode, driven by AnalyzeWatermark = 0 in the service
	// build).
	Depth RetentionDepth
	// AnalyzeWatermark — the interlock threshold: the q:analyze depth ≥
	// the threshold — the pass/batch is skipped; ≤ 0 — the interlock is
	// off.
	AnalyzeWatermark int64
	// Pending — the source of unconsumed q:render/q:analyze keys (the
	// exact protection of records the queues are waiting on —
	// retentionpending.go); nil — the protection is off (isolated builds,
	// unit tests of the selection criteria). Unlike the interlock it has
	// no threshold: exactly those records are excluded that live stream
	// messages point to.
	Pending RetentionPending
	// Log — the cycle logger; nil — the records are suppressed
	// (slog.DiscardHandler).
	Log *slog.Logger
	// OnPass — the hook of a successful pass outcome: the RetentionStats
	// snapshot and the duration right after the drain (a failed pass is
	// not handed to the hook — a Warn recorded it). The wiring point of
	// the metrics without a cycle dependency on internal/metrics (the
	// style of CleanupOptions.OnPass T-020/T-099). Must be fast and not
	// panic.
	OnPass func(RetentionStats, time.Duration)
	// OnError — the pass error hook (T-273): called after the Warn on
	// every failed pass (a ctx cancellation is a normal stop — the hook
	// is not called). The cycle error counter
	// crawler_content_retention_errors_total — before T-273 pass errors
	// were visible only in journalctl (the :9104 metrics were zeros at
	// 70+ WARNs per day, the 2026-11-09/10 incident). Must be fast and
	// not panic.
	OnError func(error)
}

// withDefaults replaces invalid zeros with the defaults; Retention is not
// touched: 0 is a legal "off".
func (o RetentionOptions) withDefaults() RetentionOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultRetentionInterval
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultRetentionBatch
	}
	if o.SelectTimeout <= 0 {
		o.SelectTimeout = DefaultRetentionSelectTimeout
	}
	if o.ErrorBackoff <= 0 {
		o.ErrorBackoff = DefaultRetentionErrorBackoff
	}
	return o
}

// RetentionStats — the outcome of one RetentionOnce pass.
type RetentionStats struct {
	// Rows — the subdomain_results rows trimmed by the pass (their
	// content_hash and/or jsonb were cut).
	Rows int64
	// Refs — the cut content_hash references (non-NULL hashes of the
	// trimmed rows, including the length-invalid ones).
	Refs int64
	// Contents — the deleted contents rows (the refcount reached zero).
	Contents int64
	// FreedBytes — the space of the freed blocks (the compressed len of
	// the deleted contents): physically it is returned by the pack
	// compaction (T-045).
	FreedBytes int64
	// Batches — the batch-transactions performed.
	Batches int
	// BadHashes — the cut references with a length-invalid content_hash
	// (the contract is 16 octets, spec 6.1): the refcount was not changed
	// for them; normally it never grows — growth means a writer error
	// (T-056), the counter analog of CleanupResult.BadHashes
	// (T-020/T-099).
	BadHashes int64
	// Missing — the cut references without a contents row (a bookkeeping
	// divergence): the refcount was not changed for them; an anomaly for
	// the alert.
	Missing int64
	// Skipped — the pass was stopped by the interlock: the q:analyze
	// depth at the threshold (the analyzer cannot keep up, the bodies are
	// still needed); the data is untouched.
	Skipped bool
	// SkippedPending — the pass was stopped by an error reading the
	// unconsumed q:render/q:analyze keys (the snapshot before the pass or
	// the refresh during a long drain): a conservative skip — the queued
	// records are untouched, and the batches before the error are
	// committed (batches are atomic in themselves); the outcome goes to
	// OnPass — a Warn in the cycle and the
	// crawler_content_retention_pending_errors_total metric.
	SkippedPending bool
	// Depth — the q:analyze depth at the moment of the stop (for the log).
	Depth int64
	// Deferred — the candidate records excluded from the pass purge: their
	// messages wait for the stream consumers (the key — the stream:
	// q:render or q:analyze; a record is published to both — counted in
	// each holder). Grows during queue standstills — the alert material
	// next to skips_total (retentionmetrics.go:
	// crawler_content_retention_deferred_total{stream}).
	Deferred map[string]int64
}

// add sums a batch outcome into the pass accumulator.
func (s *RetentionStats) add(o RetentionStats) {
	s.Rows += o.Rows
	s.Refs += o.Refs
	s.Contents += o.Contents
	s.FreedBytes += o.FreedBytes
	s.Batches += o.Batches
	s.BadHashes += o.BadHashes
	s.Missing += o.Missing
	if len(o.Deferred) > 0 {
		if s.Deferred == nil {
			s.Deferred = make(map[string]int64, len(o.Deferred))
		}
		for k, v := range o.Deferred {
			s.Deferred[k] += v
		}
	}
}

// retentionKey — the key of a subdomain_results row (the PK, spec 6.1):
// the tail of the drain pagination cursor (determinism within identical
// checked_at) and the unit of the queue-guard retry list (T-185).
type retentionKey struct {
	zone    int16
	domain  int64
	sub     int16
	variant int16
}

// less — the lexicographic key order (row-value, spec 6.1): the
// determinism of batches and the sorting of the retry keys for
// reproducible probes.
func (k retentionKey) less(b retentionKey) bool {
	if k.zone != b.zone {
		return k.zone < b.zone
	}
	if k.domain != b.domain {
		return k.domain < b.domain
	}
	if k.sub != b.sub {
		return k.sub < b.sub
	}
	return k.variant < b.variant
}

// retentionCursor — the time-based drain cursor (T-185): the last visited
// row, surviving the cycle passes (retentiondrain.go); since T-274 also
// the process restarts (the persistent reflection — the
// retention_drain_cursor table, retentioncursor.go). Time leads — rows age
// in checked_at order; the PK determines the order within identical stamps
// (now() is stable inside the writing transaction — rows of a batch share
// a stamp, T-058). The zero value means "from the start" (a catch-up
// circle: a process start without saved state, the render-debt lift, the
// retry list overflow, the fallback day); time.Time{} is less than any
// real checked_at.
type retentionCursor struct {
	checkedAt time.Time
	key       retentionKey
}

// retentionCandidate — a batch row: the key, the read content_hash (nil —
// the record has only jsonb, no reference), and the checked_at stamp — the
// cursor of the next batch (T-185).
type retentionCandidate struct {
	key       retentionKey
	hash      []byte
	checkedAt time.Time
}

// RetentionOnce — one pass from the zero cursor (a catch-up circle): the
// drain of the aging in batches of BatchSize until the set is exhausted,
// stops by the interlock, the queue-guard errors, or a ctx cancellation.
// Idempotent (spec 3.4): trimmed rows fall out of the selection, and the
// batches are atomic. Records whose messages wait on q:render/q:analyze
// are excluded from the batches by the snapshot of unconsumed keys
// (retentionpending.go): the snapshot is taken before the pass and
// refreshed every DefaultRetentionPendingRefresh selections of a long
// drain; a stream read error — a conservative skip of the pass remainder
// (SkippedPending). A one-off pass without the cycle state (the
// cursor/retries live in retentionState, retentiondrain.go): the runbooks
// T-167 path A and isolated builds; RetentionLoop calls retentionPass with
// the shared state — its passes continue each other. The persistent cursor
// (T-274) is not read or written by the one-off pass: the state's owner is
// the http-worker cycle, and an isolated drain must not move/roll back its
// position (an idempotent purge from zero is safe even without state).
func RetentionOnce(ctx context.Context, pool RetentionDB, opts RetentionOptions) (RetentionStats, error) {
	return retentionPass(ctx, pool, opts, &retentionState{})
}

// retentionBlocked — the interlock verdict: the q:analyze depth ≥ the
// threshold. A read error is an error (the caller skips the pass:
// conservatively, purging "blindly" is costlier); an off interlock (a
// threshold ≤ 0 or no source) does not block.
func retentionBlocked(ctx context.Context, o RetentionOptions) (int64, bool, error) {
	if o.AnalyzeWatermark <= 0 || o.Depth == nil {
		return 0, false, nil
	}
	n, err := o.Depth.AnalyzeDepth(ctx)
	if err != nil {
		return 0, false, err
	}
	return n, n >= o.AnalyzeWatermark, nil
}

// retentionSelect — a batch of candidates after the after cursor
// (retentionSelectSQL). The single point of the heavy selection: an
// explicit transaction with the plan shape pinning SET LOCAL
// seq/hash/merge/bitmap off + statement_timeout before the SELECT
// (T-271, the sample of retryCandidates T-191 / renderSelectPending
// T-181 — see the retentionPlanNoSeqScanSQL comment), and the client-side
// ctx deadline of SelectTimeout covers the wrapper too
// (BEGIN/SET LOCAL/COMMIT). The transaction is only the pinning wrapper:
// the SELECT reads committed data, and committing/rolling back is
// equivalent by content. The heavy scan runs under the common http-worker
// gate (o.Gate, T-271): the retention selection and the render late
// re-publisher do not coincide in PostgreSQL ("1+1 GiB" on top of
// shared_buffers — the 2026-11-08/09 incident); the gate is acquired
// BEFORE Begin — a pool connection does not idle in the queue.
func retentionSelect(ctx context.Context, pool RetentionDB, o RetentionOptions, after retentionCursor) ([]retentionCandidate, error) {
	o = o.withDefaults()
	if err := o.Gate.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("content: retention: candidate selection: %w", err)
	}
	defer o.Gate.Release()

	ctx, cancel := context.WithTimeout(ctx, o.SelectTimeout)
	defer cancel()

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("content: retention: selection transaction: %w", err)
	}
	// Rollback unconditionally: after Commit it is a no-op (the T-020/T-044
	// style).
	defer func() { _ = tx.Rollback(ctx) }()

	for _, pin := range []string{
		retentionPlanNoSeqScanSQL,
		retentionPlanNoHashJoinSQL,
		retentionPlanNoMergeJoinSQL,
		retentionPlanNoBitmapScanSQL,
		fmt.Sprintf(retentionSelectTimeoutFmt, o.SelectTimeout.Milliseconds()),
	} {
		if _, err := tx.Exec(ctx, pin); err != nil {
			return nil, fmt.Errorf("content: retention: pinning the plan: %w", err)
		}
	}

	rows, err := tx.Query(ctx, retentionSelectSQL,
		o.Retention.String(),
		after.checkedAt, after.key.zone, after.key.domain, after.key.sub, after.key.variant,
		o.BatchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("content: retention: candidate selection: %w", err)
	}
	defer rows.Close()

	var out []retentionCandidate
	for rows.Next() {
		var (
			c       retentionCandidate
			variant int16
		)
		if err := rows.Scan(&c.key.zone, &c.key.domain, &c.key.sub, &variant, &c.hash, &c.checkedAt); err != nil {
			return nil, fmt.Errorf("content: retention: candidate selection: read: %w", err)
		}
		c.key.variant = variant
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("content: retention: candidate selection: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("content: retention: committing the selection: %w", err)
	}
	return out, nil
}

// retentionProbe — the point probe of the retry keys (retentionProbeSQL):
// returns those that are still purge candidates among the offered keys.
// The keys are sorted — deterministic arguments (the cleanupRefcounts
// style, T-020); checkedAt is not read — the probe does not move the
// cursor, its rows are purged in place.
func retentionProbe(ctx context.Context, pool RetentionDB, o RetentionOptions, keys []retentionKey) ([]retentionCandidate, error) {
	sort.Slice(keys, func(i, j int) bool { return keys[i].less(keys[j]) })
	zones := make([]int16, len(keys))
	domains := make([]int64, len(keys))
	subs := make([]int16, len(keys))
	variants := make([]int16, len(keys))
	for i, k := range keys {
		zones[i], domains[i], subs[i], variants[i] = k.zone, k.domain, k.sub, k.variant
	}
	rows, err := pool.Query(ctx, retentionProbeSQL,
		zones, domains, subs, variants, o.Retention.String(),
	)
	if err != nil {
		return nil, fmt.Errorf("content: retention: retry probe: %w", err)
	}
	defer rows.Close()

	var out []retentionCandidate
	for rows.Next() {
		var (
			c       retentionCandidate
			variant int16
		)
		if err := rows.Scan(&c.key.zone, &c.key.domain, &c.key.sub, &variant, &c.hash); err != nil {
			return nil, fmt.Errorf("content: retention: retry probe: read: %w", err)
		}
		c.key.variant = variant
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("content: retention: retry probe: %w", err)
	}
	return out, nil
}

// retentionPurge — the purge of one batch in one transaction: the UPDATE
// of subdomain_results (cutting the reference and the jsonb) → decreasing
// the refcount of the actually cut references → deleting contents without
// references. A crash at any step rolls everything back (spec 3.4): the
// rows and the refcount do not diverge. cursor != nil — a persistent drain
// cursor is committed together with the batch (T-274,
// retentioncursor.go): the atomicity of "batch committed ⟺ position
// saved"; nil — a purge outside the drain cursor (the retry probe, the
// point drain of a domain T-187) or non-persistent state (RetentionOnce).
func retentionPurge(ctx context.Context, pool RetentionDB, o RetentionOptions, batch []retentionCandidate, cursor *retentionCursor) (RetentionStats, error) {
	var st RetentionStats
	zones := make([]int16, len(batch))
	domains := make([]int64, len(batch))
	subs := make([]int16, len(batch))
	variants := make([]int16, len(batch))
	hashes := make([][]byte, len(batch))
	for i, c := range batch {
		zones[i], domains[i], subs[i], variants[i], hashes[i] = c.key.zone, c.key.domain, c.key.sub, c.key.variant, c.hash
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return st, fmt.Errorf("content: retention: transaction: %w", err)
	}
	// Rollback unconditionally: after Commit it is a no-op (the T-020/T-044
	// style). An interrupted batch leaves no half-changes.
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, retentionPurgeSQL,
		zones, domains, subs, variants, hashes, o.Retention.String())
	if err != nil {
		return st, fmt.Errorf("content: retention: row purge: %w", err)
	}
	defer rows.Close()

	// Aggregating the cut references by hash: one content may be
	// referenced from many records (deduplication, spec 5.7) — the
	// decrease is multiplicative.
	refs := make(map[Hash]int32)
	for rows.Next() {
		var hash []byte
		if err := rows.Scan(&hash); err != nil {
			return st, fmt.Errorf("content: retention: row purge: read: %w", err)
		}
		st.Rows++
		if hash == nil {
			continue // the record had only jsonb — no reference (T-056)
		}
		st.Refs++
		switch {
		case len(hash) != HashLen:
			// A software bug of the results writer (T-056): the row is
			// already trimmed; dropping the batch is not allowed — one
			// crooked hash would block retention forever (a poison pill,
			// the ING-P2-4 style).
			// The reference was cut (junk); its refcount is not touched.
			st.BadHashes++
		default:
			var h Hash
			copy(h[:], hash)
			refs[h]++
		}
	}
	if err := rows.Err(); err != nil {
		return st, fmt.Errorf("content: retention: row purge: %w", err)
	}

	if len(refs) > 0 {
		if err := retentionDecrease(ctx, tx, refs, &st); err != nil {
			return st, err
		}
	}
	// The persistent cursor — by the same transaction (T-274): the saved
	// position cannot overtake the committed batch.
	if cursor != nil {
		if err := retentionSaveCursorTx(ctx, tx, *cursor); err != nil {
			return st, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return st, fmt.Errorf("content: retention: commit: %w", err)
	}
	st.Batches = 1
	return st, nil
}

// retentionDecrease — the contents steps of the batch transaction: the
// refcount decrease by the aggregated hashes (retentionDecrefSQL) and the
// deletion of the rows that lost their last reference (retentionDeleteSQL).
// A missing contents row for a cut reference is a bookkeeping divergence
// (Missing): a counter for the alert, not a batch error (a poison pill,
// like BadHashes).
func retentionDecrease(ctx context.Context, tx pgx.Tx, refs map[Hash]int32, st *RetentionStats) error {
	list := make([]Hash, 0, len(refs))
	for h := range refs {
		list = append(list, h)
	}
	// A deterministic argument order — reproducible batches (the
	// cleanupRefcounts style, T-020).
	sort.Slice(list, func(i, j int) bool {
		return compareHash(list[i], list[j]) < 0
	})
	hashes := make([][]byte, len(list))
	counts := make([]int32, len(list))
	for i, h := range list {
		hashes[i], counts[i] = h[:], refs[h]
	}

	rows, err := tx.Query(ctx, retentionDecrefSQL, hashes, counts)
	if err != nil {
		return fmt.Errorf("content: retention: refcount decrement: %w", err)
	}
	defer rows.Close()
	updated := make(map[Hash]struct{}, len(list))
	var dead [][]byte
	for rows.Next() {
		var (
			hash []byte
			refc int32
			clen int32
		)
		if err := rows.Scan(&hash, &refc, &clen); err != nil {
			return fmt.Errorf("content: retention: refcount decrement: read: %w", err)
		}
		var h Hash
		copy(h[:], hash)
		updated[h] = struct{}{}
		if refc <= 0 {
			dead = append(dead, hash)
			st.FreedBytes += int64(clen)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("content: retention: refcount decrement: %w", err)
	}
	// Cut references not found in contents: a bookkeeping divergence (a
	// reference without a counter would break the T-044 invariant) — the
	// refcount was not changed for them, a counter for the alert; not a
	// batch error (a poison pill).
	st.Missing += int64(len(list) - len(updated))

	if len(dead) > 0 {
		tag, err := tx.Exec(ctx, retentionDeleteSQL, dead)
		if err != nil {
			return fmt.Errorf("content: retention: deleting contents: %w", err)
		}
		st.Contents += tag.RowsAffected()
	}
	return nil
}

// compareHash — comparing hashes lexicographically (the batch order).
func compareHash(a, b Hash) int {
	for i := range a {
		switch {
		case a[i] < b[i]:
			return -1
		case a[i] > b[i]:
			return 1
		}
	}
	return 0
}

// RetentionLoop — the background retention cycle (T-166): retentionPass
// passes on a ticker (the first one immediately: the drain of what
// accumulated during the downtime — the CompactLoop/CleanupDeleted style)
// until ctx is cancelled; returning nil is the OnRun contract of the
// service framework (T-012). The pass state (retentionState, T-185) — the
// time-based drain cursor and the retry list — survives passes: a row is
// visited once in its lifetime, and an hourly pass continues from the stop
// point (the cost ∝ the inflow, not the table size); the cursor is
// additionally persisted in the DB (T-274) — it survives a process restart
// too (the retry list is not persistent: its loss on a restart is covered
// by the cursor lag — the position in the DB does not move past the rows
// deferred by the guards, and the start re-reads the guard window); for
// the runbook entry points see retentiondrain.go.
// Retention = 0 — retention is off: the cycle does not start at all (an
// Info in the log). A pass error does not stop the cycle: a Warn and the
// OnError hook (the error metric, T-273), and the ticker switches to the
// DefaultRetentionErrorBackoff backoff (T-271) — after EVERY error
// (T-273): the pause is counted from the end of the failed pass, not from
// the tick anchor (a minimum of "pass + backoff" between starts); the next
// pass after the pause retries — the passes are idempotent (spec 3.4), and
// a success returns the regular interval. Only the cycle touches the state
// (a single ticker goroutine) — no locks are needed.
func RetentionLoop(ctx context.Context, pool RetentionDB, opts RetentionOptions) error {
	if pool == nil {
		return errors.New("content: retention: the pool is not set")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if o.Retention <= 0 {
		log.Info("content retention is disabled (content.retention = 0)")
		return nil
	}
	log.Info("content retention loop started",
		"retention", o.Retention.String(),
		"interval", o.Interval.String(),
		"batch", o.BatchSize,
		"analyze_watermark", o.AnalyzeWatermark,
		"queue_guard", o.Pending != nil,
		"retry_limit", DefaultRetentionRetryLimit)

	// The persistent drain cursor (T-274, retentioncursor.go): the
	// position survives a process restart — the starting pass continues
	// from the saved point, not a full catch-up from zero (the penalty of
	// ~an hour of traversing the clean prefix is abolished). A missing
	// state (the first run, a circle reset it) — the zero cursor, the
	// previous behavior. No table (migration 000020) — an Info and
	// operation without persistence (the render-debt counter style); an
	// invalid state (a stamp in the future) and a read failure — a Warn
	// and a start from zero: full state is more honest than guesses, and
	// the pass is idempotent.
	state := &retentionState{persist: true}
	switch cur, ok, err := retentionLoadCursor(ctx, pool); {
	case err != nil && errors.Is(err, errRetentionNoCursorTable):
		state.persist = false
		log.Info("retention: the persistent cursor table is missing — restarts give a full catch-up (migration 000020 not applied?)")
	case err != nil && errors.Is(err, errRetentionCursorInvalid):
		log.Warn("retention: the saved drain cursor is malformed — starting from zero (a full catch-up)", "err", err)
	case err != nil:
		log.Warn("retention: the persistent cursor was not read — starting from zero (a full catch-up)", "err", err)
	case ok:
		state.cursor = cur
		log.Info("retention: the drain continues from the persistent cursor",
			"checked_at", cur.checkedAt.Format(time.RFC3339),
			"zone_id", cur.key.zone, "domain_id", cur.key.domain)
	}
	pass := func() error {
		start := time.Now()
		stats, err := retentionPass(ctx, pool, o, state)
		if err != nil {
			// A cancellation during the pass is part of the stop, not an
			// alarm.
			if ctx.Err() == nil {
				log.Warn("content retention pass", "err", err)
				if o.OnError != nil {
					o.OnError(err)
				}
			}
			return err
		}
		if stats.Skipped {
			// The interlock: the analyzer cannot keep up, the bodies are
			// still needed — the alert is on the skip metric
			// (retentionmetrics.go).
			log.Warn("content retention pass skipped: the q:analyze depth is at the watermark",
				"depth", stats.Depth, "watermark", o.AnalyzeWatermark)
		}
		if stats.SkippedPending {
			// The queue guard: reading the unconsumed q:render/q:analyze
			// failed — the records waiting for consumers are untouched;
			// the alert is on the
			// crawler_content_retention_pending_errors_total metric.
			// rows_done — the batches committed before the error point
			// (the skip concerns the pass remainder; what was done stays
			// in the DB).
			log.Warn("content retention pass skipped: an error reading q:render/q:analyze",
				"rows_done", stats.Rows)
		}
		if n := stats.Deferred[queue.StreamRender]; n > 0 {
			log.Warn("retention: rows waiting for q:render — untouched", "deferred", n)
		}
		if n := stats.Deferred[queue.StreamAnalyze]; n > 0 {
			log.Warn("retention: rows waiting for q:analyze — untouched", "deferred", n)
		}
		if stats.BadHashes > 0 || stats.Missing > 0 {
			log.Warn("retention: content accounting anomalies",
				"bad_hashes", stats.BadHashes, "missing_refs", stats.Missing)
		}
		if stats.Rows > 0 {
			log.Info("content retention pass",
				"rows", stats.Rows, "refs", stats.Refs,
				"contents", stats.Contents, "freed_bytes", stats.FreedBytes,
				"batches", stats.Batches)
		}
		// The pass outcome — to the consumers (the metrics): after the log.
		if o.OnPass != nil {
			o.OnPass(stats, time.Since(start))
		}
		return nil
	}

	// The starting drain — immediately (the catch-up over the downtime).
	// A pass error — a Warn and the ticker switch to backoff (T-271): a
	// tick fired during a long failed pass must not restart the pass
	// immediately into a just-booted PostgreSQL (the 2026-11-08/09
	// incident); a success of the next pass returns the regular interval.
	backoff := false
	if err := pass(); err != nil && ctx.Err() == nil {
		backoff = true
	}
	period := o.Interval
	if backoff {
		period = o.ErrorBackoff
	}
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := pass()
			switch {
			// A reset after EVERY error (T-273): the ticker's anchor was
			// the end of the first failed pass — a repeated error left a
			// pause of "pass minus period" (a 600 s pass at a 15 min
			// backoff gave a real pause of 300 s), and the WARN cadence
			// masked the duration. The reset here keeps the pause from the
			// END of the failed pass: a minimum of "pass duration +
			// backoff" between starts (honest diagnostics).
			case err != nil && ctx.Err() == nil:
				backoff = true
				retentionResetTicker(ticker, o.ErrorBackoff)
			case err == nil && backoff:
				backoff = false
				retentionResetTicker(ticker, o.Interval)
			}
		}
	}
}

// retentionResetTicker — resetting the cycle ticker (T-271). The mechanics
// of the 2026-11-08/09 incident: a tick that fired during a pass longer
// than the interval is not lost — the runtime keeps one undelivered tick
// and delivers it immediately when the loop returns to select
// (Go ≥1.23: a deferred send on the ticker's unbuffered channel,
// cap(t.C) = 0; earlier — a channel buffer of capacity 1), so the failed
// pass repeated instantly. Reset switches the period to the backoff
// pause; under the current toolchain (Go ≥1.23) Reset synchronizes with
// the deferred tick and removes it itself; the drain below is a harmless
// insurance under older toolchains where the channel buffer was not
// cleared by Reset (a remark of the T-271 review, P3-1).
func retentionResetTicker(t *time.Ticker, d time.Duration) {
	t.Reset(d)
	select {
	case <-t.C: // draining the undelivered tick of the old period
	default:
	}
}

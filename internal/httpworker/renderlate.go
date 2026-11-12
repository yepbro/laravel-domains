// The late re-publisher of the render (T-259, TZ-fsm spec §3 FR-5.3; the
// specification of the row's render-state automaton — the doc comment
// of internal/renderworker/worker.go, this file — the writer of the T4 transition):
// a background pass re-publishing "forever young" records into q:render — rendered = false,
// final_url IS NOT NULL, render_failed_at IS NULL — older than 6 h, whose
// domain has no render_pending bit (the renderq.go rework pass
// deals with those) and no unconsumed q:render/q:render-fallback messages
// (the interlock — a snapshot of the streams on the model of content/retentionpending.go:
// an XRANGE of the undelivered tail from the group's last-delivered-id, the adapter —
// the cmd/http-worker retentionPending, the same as the retention guard).
//
// What it cures: the loss of MAXLEN-trimmed fallback tasks (worker A published
// q:render-fallback, T-238; the T-025 trim removed the record before the delivery —
// a record without a marker and without messages is indistinguishable from "not tried") and
// the lost q:render publications (a Redis failure/restart between the XADD and
// the delivery). The record's state stops depending on the message's survival
// in the stream: while the record is not rendered and not marked, the re-publisher
// will return its task. The duplicates are bounded by the interval and extinguished
// by the consumer's idempotency (spec 3.4, as at the rework pass).
//
// The SPA verdict — over the stored content (DetectSPA T-057, deterministic
// over the same bytes the HTTP stage checked at the write): the age
// predicate selects a SUPERSET (non-SPA records fit the columns
// indefinitely long — the render is not needed for them, §5.5 item 8 publishes only
// SPA), an edition without a verdict would flood q:render by the recheck of all
// the dataset's pages. Therefore the pass repeats the rework pass's decision
// (renderDomainKeys): its own body — its own verdict, an alias inherits the
// original subdomain's verdict (the T-055 equivalence class), the records without
// their own content and without an alias are not re-published at all — there is nowhere
// to take the verdict from (the content was cut off by retention T-166 or not stored,
// T-056), the next recheck episode T-030 will rework them (the same
// deliberate limitation as at the rework pass: older than the retention window
// there is no content — walking there is pointless).
//
// The cursor pagination over the PK (the reanalyzeSelectSQL T-074/T-193 style:
// a row-value > over the row's key + ORDER BY PK + LIMIT): the pass must
// GUARANTEED advance over the whole candidate set — without
// the cursor a LIMIT without sorting (the T-181 style) would return the first chunk
// of the heap forever (the non-SPA candidates do not leave the set — the pass
// would not reach the lost records behind them). The cursor lives in the
// loop (RenderLateCursor); per the chunk of the processed ones — including the ones excluded
// by the interlock/verdict (their holders/episodes will decide themselves, the full
// cycle will come back), a short chunk closes the cycle to the start. A full cycle over
// the table (~40M rows) at a chunk of 1000/min — about a month,
// faster than the domains' natural recheck (scheduler.recheck_interval,
// the default ~6 months): a lost task is restored long before
// the episode's rewrite; the walk over the rows without content (older than retention) —
// a cheap index scan without reading the packs.
//
// The cursor does not move over a chunk that failed to be processed entirely
// (a full queue — Full, a publication/snapshot error): the next tick
// will repeat the chunk; the already published part will repeat as a duplicate —
// bounded by the chunk, extinguished by the consumer's idempotency (spec 3.4).
// A stream snapshot read error — a conservative pass skip, as at the
// retention guard: re-publishing "at random" is dearer than postponing to a tick
// (a live task's duplicate is exactly what the interlock must exclude).
//
// Observability: the crawler_render_late_republish_total counter
// (httpworker.Metrics.LateRepublished) — does not grow in the steady state; growth
// after an incident (a trim/a Redis loss) — the re-publisher drains
// the losses, a fall to zero — the drainage is complete.
package httpworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/httpfetch"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// The pass's defaults (the T-066 renderq.go style: constants, no separate
// config option — spec 5.10, the composition is fixed; the working values the
// loop receives as options for the tests).
const (
	// DefaultRenderLateInterval — the loop's tick period: a minute, like
	// DefaultRenderRequeueInterval. The pass — a guard against loss (not the
	// graceful publication path), more often is pointless: the "older than 6 h" window survives
	// a minute tick with headroom; with a full queue the pass pays one depth
	// read.
	DefaultRenderLateInterval = time.Minute

	// DefaultRenderLateBatch — the rows in one chunk. The cursor must pass
	// the whole candidate set (a superset — almost the whole table)
	// faster than the domains' natural recheck cycle (the default
	// ~6 months): 1000/min ≈ 1.4M rows/day. The verdict reads the body from
	// the packs — only for the rows with content (inside the retention window; on
	// average tens of KiB — a moderate background I/O next to the
	// retention chunks of the same service); the rows without content are filtered
	// without a read.
	DefaultRenderLateBatch = 1000

	// DefaultRenderLateAfter — the age of a "forever young" record (TZ-fsm
	// spec FR-5.3, the default 6 h): younger — the task may have not been delivered yet
	// (a normal queue lag at a depth up to the T-174 threshold ≈ 2 h of buffer),
	// an early re-edition would breed duplicates of live tasks. 6 h — beyond
	// any normal delivery windows (the PEL retries — minutes,
	// T-025), before the episode's rewrite by the recheck (T-030,
	// scheduler.recheck_interval) — months of headroom for the full cursor
	// cycle of the pass.
	DefaultRenderLateAfter = 6 * time.Hour

	// DefaultRenderLateSelectTimeout — the cap of one chunk selection
	// (T-271): the server-side statement_timeout in the selection's tx shell and
	// the client ctx deadline. 300 s (T-273, formerly 60): a catch-up from the zero
	// cursor at the production volume (domains 43M) at any plan shape
	// costs more than seconds, and the deadline must give the pass a chance to complete
	// and move the cursor — a timeout smaller than the selection's real cost
	// turns a slow drainage into an eternal loop of doomed retries
	// (the T-271 regression, the Nov 9–10, 2026 incident: 60 s < the cost of the pinned
	// shape ≈157M — not a single successful pass over 21 h). The normal
	// chunks (the cursor mid-table) — seconds. An overrun — a pass
	// error, a backoff and a chunk repeat from the same cursor (the publication
	// did not happen — no duplicate, spec 3.4). The working value is passed by
	// the configuration (render.late_select_timeout, T-274) — a knob for the case
	// of underestimation, the default stays this one.
	DefaultRenderLateSelectTimeout = 300 * time.Second

	// DefaultRenderLateErrorBackoff — the loop's ticker pause after
	// an erroneous pass (T-271): a tick that fired during a long
	// erroneous pass does not restart the pass immediately (the runtime
	// keeps one undelivered tick and hands it over upon the loop's return to
	// the select; the Nov 8–9, 2026 incident — a pass into a PostgreSQL just
	// raised after an OOM kill); the cursor in place (the chunk not processed
	// entirely), a minute rate returns the first successful pass.
	// 15 minutes — an order of magnitude longer than the PG startup; for the loss
	// guard (the 6 h window, the full cycle — a month) it is imperceptible.
	DefaultRenderLateErrorBackoff = 15 * time.Minute
)

// The pass's SQL. The schema codes — as literals (the T-039/renderq.go style); the
// render_pending bit — model.FlagRenderPending (bit 1), deleted —
// model.StatusDeleted.
var (
	// lateRenderRowsSQL — the chunk of candidates after the cursor (the PK cursor
	// T-074/T-193): not rendered, there is a render target, no
	// "render impossible" marker (T-210 — the finalizations closed the record before the content
	// change), the age beyond the DefaultRenderLateAfter window (checked_at —
	// the episode's hour, T-058; the boundary is computed by the Go clock — the
	// clock divergence of the application and the DB against the 6 h window is insignificant, the
	// T-193 §4.2 anchor style), the domain without the render_pending bit (the debt is the
	// renderq.go rework pass's business — a duplicate of editions is forbidden by the
	// ownership division) and not deleted (§13.1 — outside the check cycle). The JOIN zones —
	// the name for Result.Name (the T-027 style). The cursor's row-value > over the PK
	// cuts off the passed part, the ORDER BY over the same key — deterministic
	// chunks and a correct pagination; the leading PK columns — the LIST/HASH
	// partitions (T-005) cut the search down to a leaf.
	//
	// The plan shape is NOT pinned — deliberately, T-273 (a return to
	// the T-251 decision). The T-271 pinning (SET LOCAL seq/hash/merge/bitmap
	// off) left the planner only the Nested Loop, and at the production volume
	// it unrolled the join from domains (43.4M rows, a cost ≈157M) —
	// always over the 60 s deadline, the eternal loop of doomed selections
	// (the Nov 9–10, 2026 incident, the T-273 investigation). The unpinned shape
	// worked in production for weeks before T-271 and is adaptive by the testbed
	// measurements (PG 16.15, 2M domains / 550k subdomain_results): a zero
	// cursor — a Nested Loop from domains with an early Limit stop
	// (19 ms), the table's middle — a Merge Join from subdomain_results over the
	// PK (42 ms), the domains≫records pressure — an r-driven Merge Join with
	// the domains tail (260 ms). The "SET LOCAL join_collapse_limit
	// = 1" variant was rejected by the measurement: the GUC keeps the join pairs in the FROM order, but
	// does not forbid unrolling the outer/inner INSIDE the pair — the plan
	// unrolls from domains anyway. An OOM cycle's return is not expected:
	// the nodes' working structures are clamped by work_mem 16 MB (docs/db.md),
	// the heavy scans' coincidence excludes a common gate, every selection
	// is under a statement_timeout. The statement_timeout cap stays
	// (the lateSelect tx shell), the structural fix of the shape — a partial
	// index + a persistent cursor (a follow-up, the research's variant C).
	// content_hash and
	// alias_subdomain_id — the verdict (DetectSPA T-057 / the inheritance
	// T-055); a hash NULL — no body, an alias NULL — nobody to inherit from.
	lateRenderRowsSQL = "SELECT r.zone_id, r.domain_id, r.subdomain_id, r.variant, " +
		"r.content_hash, r.alias_subdomain_id, d.label || '.' || z.name " +
		"FROM subdomain_results AS r " +
		"JOIN domains AS d ON d.zone_id = r.zone_id AND d.id = r.domain_id " +
		"JOIN zones AS z ON z.id = r.zone_id " +
		"WHERE r.rendered = false AND r.final_url IS NOT NULL AND r.render_failed_at IS NULL " +
		"AND r.checked_at < $1::timestamptz " +
		"AND (d.flags & " + fmt.Sprintf("%d", int16(model.FlagRenderPending)) + ") = 0 " +
		"AND d.status <> " + fmt.Sprintf("%d", int16(model.StatusDeleted)) + " " +
		"AND (r.zone_id, r.domain_id, r.subdomain_id, r.variant) > ($2::int2, $3::int8, $4::int2, $5::int2) " +
		"ORDER BY r.zone_id, r.domain_id, r.subdomain_id, r.variant " +
		"LIMIT $6"

	// lateSelectTimeoutFmt — the server-side selection cap by the same
	// tx shell (T-271): the '<N>ms' format is valid for the GUC time parser
	// at any duration (the db.lockTimeoutSQLFmt style). The value —
	// LateOptions.SelectTimeout, the text is pinned by a unit test
	// (renderlate_test.go). A shell of one SET LOCAL — the plan
	// pinnings are removed (T-273), the cap remains: the server cuts the operator itself,
	// the client ctx deadline — the shell entirely.
	lateSelectTimeoutFmt = "SET LOCAL statement_timeout = '%dms'"

	// lateRenderAliasSQL — the original subdomains' rows for the verdict
	// inheritance by the chunk's aliases (T-055): the candidates (not rendered, there is
	// a target, no marker — the same guard predicates as at the
	// rework pass's renderPendingRowsSQL, the verdict over the body) PLUS
	// the rendered originals: the very fact of rendered = true proves
	// the class's SPA verdict (the q:render publications go only over SPA —
	// renderSPAResults/renderDomainKeys/our verdict), such an original's
	// content is already a DOM and DetectSPA over it is wrong; the T-259 P2-1 review:
	// without the rendered originals an alias with a lost message (exactly
	// the "lost fallback task" class, publishFallback publishes over the
	// row) was not restored at all. A marked original
	// (rendered = false + the marker) gives no verdict — the marked
	// records are not re-published (the T-210 semantics, as at the rework pass).
	// subdomain_id = ANY — the chunk of one command's aliases in one query;
	// the ORDER BY — determinism.
	lateRenderAliasSQL = "SELECT subdomain_id, variant, content_hash, rendered FROM subdomain_results " +
		"WHERE zone_id = $1::int2 AND domain_id = $2::int8 " +
		"AND subdomain_id = ANY($3::int2[]) " +
		"AND (rendered OR (final_url IS NOT NULL AND render_failed_at IS NULL)) " +
		"ORDER BY subdomain_id, variant"
)

// lateRenderStreams — the interlock's streams: unconsumed messages of any
// of them mean a live record task (q:render — the main path and
// the re-editions, q:render-fallback — "waiting for the fallback", T-238) — the record is
// not re-published. The names — the queue package's constants (the single
// source, like retentionPendingStreams in content).
var lateRenderStreams = []string{queue.StreamRender, queue.StreamRenderFallback}

// RenderLateCursor — the pass's cursor over the PK: the last visited row,
// surviving the loop's passes (the ReanalyzeCursor T-193 model). The zero
// value — "from the table's start" (zone_id ≥ 1 is greater than the zero key).
// The fields are not exported: only LateOnce moves the cursor, the owner —
// the RenderLateLoop loop; external calls keep the value entirely.
type RenderLateCursor struct {
	key lateKey
}

// lateKey — the key of a subdomain_results row (the PK, spec 6.1) and the cursor.
type lateKey struct {
	zone    int16
	domain  int64
	sub     int16
	variant model.Variant
}

// lateRow — a chunk's row: the key, the body for the verdict (nil — no content),
// the alias (not nil — the verdict is inherited from the original), the name for the message.
type lateRow struct {
	key      lateKey
	hash     []byte
	aliasSub *int16
	name     string
}

// LateOptions — the late re-publisher's parameters; the zero values
// are replaced by the defaults (withDefaults, the RequeueOptions T-066 style).
type LateOptions struct {
	// Interval — the RenderLateLoop loop's period (DefaultRenderLateInterval);
	// does not affect a one-shot LateOnce.
	Interval time.Duration
	// BatchSize — the maximum of rows in one chunk
	// (DefaultRenderLateBatch).
	BatchSize int
	// After — the age of a "forever young" record (DefaultRenderLateAfter).
	After time.Duration
	// SelectTimeout — the cap of one chunk selection
	// (DefaultRenderLateSelectTimeout, T-271): the server-side
	// statement_timeout of the plan pinning's tx shell and the client
	// ctx deadline; an overrun — a pass error, the chunk will repeat from the
	// same cursor.
	SelectTimeout time.Duration
	// ErrorBackoff — the ticker's pause after an erroneous pass
	// (DefaultRenderLateErrorBackoff, T-271): a tick that fired during a
	// long pass does not restart it immediately (the
	// Nov 8–9, 2026 incident).
	ErrorBackoff time.Duration
	// Gate — the http-worker heavy background scans' uniqueness semaphore
	// (db.ScanGate, T-271): the late re-publisher's selection and retention do
	// not coincide in PostgreSQL; nil — without serialization (isolated
	// builds, unit tests).
	Gate db.ScanGate
	// Log — the loop's logger and the pass's Warns; nil — the records are suppressed
	// (slog.DiscardHandler).
	Log *slog.Logger
}

func (o LateOptions) withDefaults() LateOptions {
	if o.Interval <= 0 {
		o.Interval = DefaultRenderLateInterval
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultRenderLateBatch
	}
	if o.After <= 0 {
		o.After = DefaultRenderLateAfter
	}
	if o.SelectTimeout <= 0 {
		o.SelectTimeout = DefaultRenderLateSelectTimeout
	}
	if o.ErrorBackoff <= 0 {
		o.ErrorBackoff = DefaultRenderLateErrorBackoff
	}
	return o
}

// LateResult — the outcome of one LateOnce pass.
type LateResult struct {
	// Selected — the rows selected by the chunk (after the cursor, under the predicates).
	Selected int
	// Skipped — excluded without an edition: a live task (the stream
	// snapshot's interlock), a non-SPA verdict, no verdict source (the content cut off/
	// not stored, an alias without an original candidate), an unreadable block.
	// The cursor passes them by: their holders and the next cycle will decide.
	Skipped int
	// Published — the q:render messages re-published (the
	// crawler_render_late_republish_total metric grows by this number). The count —
	// of a chunk confirmed entirely: a Publish failure mid-chunk leaves
	// the published prefix in the stream unaccounted (the duplicate is bounded by the chunk,
	// extinguished by the idempotency, spec 3.4; the next tick will publish the chunk
	// again — then the count is honest).
	Published int
	// Full — q:render is at the threshold: the pass stopped BEFORE the publication, the
	// cursor did not move — the next tick will repeat the chunk.
	Full bool
	// Wrapped — the chunk is shorter than the limit: the set behind the cursor is exhausted,
	// the cursor is closed to the table's start (the next pass — from zero).
	Wrapped bool
}

// LateOnce — one late re-publisher pass: a chunk of candidates after
// the cursor (the lateRenderRowsSQL predicate) → the interlock by the snapshot of the unconsumed
// q:render/q:render-fallback (a live task — do not touch) → the SPA verdict
// over the stored content (its own body / the alias inheritance) →
// the q:render re-edition with a free queue. The cursor moves over the chunk
// processed entirely (the published + the excluded); a short chunk closes
// the cycle. cursor nil — a one-shot call from the zero key without saving.
//
// The "snapshot → edition" order keeps the race window minimal: a publication
// after the snapshot (a live task appeared between them) gives a duplicate —
// bounded by the chunk, extinguished by the consumer's idempotency (spec 3.4);
// a message delivered to the group BEFORE the snapshot is not seen by the snapshot (the
// tail is read from the last-delivered-id, the retention model) — such a task is in
// the worker's PEL and will return by a claim/duplicate within minutes (T-025).
func LateOnce(ctx context.Context, pool RecordDB, contents RenderContent, render *RenderQueue,
	pending content.RetentionPending, met *Metrics, opts LateOptions, cursor *RenderLateCursor) (LateResult, error) {
	if pool == nil {
		return LateResult{}, errors.New("httpworker: late re-publisher: the pool is not set")
	}
	if contents == nil {
		return LateResult{}, errors.New("httpworker: late re-publisher: the content store is not set")
	}
	if render == nil {
		return LateResult{}, errors.New("httpworker: late re-publisher: the render queue is not set")
	}
	if pending == nil {
		return LateResult{}, errors.New("httpworker: late re-publisher: the unconsumed stream keys source is not set (the interlock is required, see retentionpending.go)")
	}
	if cursor == nil {
		cursor = &RenderLateCursor{} // a one-shot call: the cursor without saving
	}
	o := opts.withDefaults()
	if o.Log == nil {
		o.Log = slog.New(slog.DiscardHandler)
	}
	out := LateResult{}

	// A full queue — the DB is not read (one depth read): a re-edition
	// into a full stream — an overflow amplifier (the records would risk
	// a MAXLEN trim again); the "it freed up" signal — the next tick.
	free, err := render.HasCapacity(ctx)
	if err != nil {
		return out, fmt.Errorf("httpworker: late re-publisher: q:render capacity: %w", err)
	}
	if !free {
		return LateResult{Full: true}, nil
	}

	rows, err := lateSelect(ctx, pool, o, time.Now().Add(-o.After), cursor.key)
	if err != nil {
		return out, err
	}
	if len(rows) == 0 {
		// There are no candidates behind the cursor — the cycle is closed: the next chunk
		// will start the table anew (new losses could appear behind the cursor
		// during the tick — they do not wait for the full cycle).
		cursor.key = lateKey{}
		return LateResult{Wrapped: true}, nil
	}
	out.Selected = len(rows)

	// The interlock: a snapshot of the guarded streams' unconsumed keys — ONE
	// per chunk (the retentionPendingSnapshot style). Any stream's error —
	// the whole pass's error: a partial guard is worse than a skip (a live
	// q:render-fallback task without a snapshot would be re-published as a duplicate).
	keys := make(map[content.RetentionPendingKey]struct{})
	for _, s := range lateRenderStreams {
		ks, err := pending.PendingKeys(ctx, s)
		if err != nil {
			return out, fmt.Errorf("httpworker: late re-publisher: unconsumed %s: %w", s, err)
		}
		for k := range ks {
			keys[k] = struct{}{}
		}
	}

	publish := make([]queue.Result, 0, len(rows))
	// The verdicts' cache by the content hash: the T-055 equivalence
	// class's records share the body — the body is read and verdicted once per chunk.
	verdict := make(map[content.Hash]bool)
	// The chunk's aliases — the verdict from the originals by one query per domain
	// (lateRenderAliasSQL takes an ANY chunk of subdomains).
	type aliasRef struct {
		row  lateRow
		orig int16
	}
	origSubs := make(map[lateKey][]int16) // key (zone, domain) → the original subdomains
	var aliases []aliasRef
	for i := range rows {
		r := &rows[i]
		live := content.RetentionPendingKey{Zone: r.key.zone, Domain: r.key.domain, Sub: r.key.sub, Variant: int16(r.key.variant)}
		if _, ok := keys[live]; ok {
			// A live task: the message waits for the consumer — the record is not
			// touched (the FR-5.3 acceptance criterion).
			out.Skipped++
			continue
		}
		if r.aliasSub != nil {
			aliases = append(aliases, aliasRef{row: *r, orig: *r.aliasSub})
			dom := r.key
			dom.sub, dom.variant = 0, 0
			origSubs[dom] = append(origSubs[dom], *r.aliasSub)
			continue
		}
		if len(r.hash) != content.HashLen {
			// No content (T-056 or cut off by retention T-166) — there is nowhere
			// to take the verdict from; the rework pass's limitation, the file header.
			out.Skipped++
			continue
		}
		var h content.Hash
		copy(h[:], r.hash)
		spa, err := lateBodySPA(ctx, contents, verdict, h, r, o.Log)
		if err != nil {
			return out, err
		}
		if !spa {
			out.Skipped++ // non-SPA: the render is not required (§5.5 item 8)
			continue
		}
		publish = append(publish, lateResult(*r))
	}

	// The chunk aliases' originals' verdicts: one query per domain, the bodies —
	// through the same cache (the original and the alias share the T-055 class — but the
	// alias's content is NULL, the original's body is read).
	origSPA := make(map[lateKey]bool) // (zone, domain, orig sub) → SPA
	for dom, subs := range origSubs {
		spa, err := lateOriginsSPA(ctx, pool, contents, verdict, dom, subs, o.Log)
		if err != nil {
			return out, err
		}
		for sub, ok := range spa {
			origSPA[lateKey{zone: dom.zone, domain: dom.domain, sub: sub}] = ok
		}
	}
	for _, a := range aliases {
		if origSPA[lateKey{zone: a.row.key.zone, domain: a.row.key.domain, sub: a.orig}] {
			publish = append(publish, lateResult(a.row))
			continue
		}
		out.Skipped++ // the original is not a candidate/not SPA/unreadable —
		// nothing to inherit: the next cycle/episode will decide
	}

	if len(publish) > 0 {
		// The capacity right before the publication: the snapshot and the chunk's
		// verdicts took minutes — the queue could have filled (the rework
		// pass checks the same way before every domain).
		free, err := render.HasCapacity(ctx)
		if err != nil {
			return out, fmt.Errorf("httpworker: late re-publisher: q:render capacity: %w", err)
		}
		if !free {
			out.Full = true
			return out, nil
		}
		if err := render.Publish(ctx, publish); err != nil {
			return out, fmt.Errorf("httpworker: late re-publisher: %w", err)
		}
		out.Published = len(publish)
		met.LateRepublished(out.Published)
	}

	// The chunk is processed entirely — the cursor over the last row (including
	// the excluded ones: the interlock/verdict decide their fate without the pass).
	cursor.key = rows[len(rows)-1].key
	if len(rows) < o.BatchSize {
		cursor.key = lateKey{} // a short chunk — the set is exhausted, the cycle is closed
		out.Wrapped = true
	}
	return out, nil
}

// lateResult — the re-edition Result message (the row's key + the name, T-024):
// the consumer reads the target from the row (renderSelectSQL), the
// episode guards guarantee the freshness.
func lateResult(r lateRow) queue.Result {
	return queue.Result{
		ZoneID: r.key.zone, DomainID: r.key.domain,
		SubdomainID: r.key.sub, Variant: r.key.variant, Name: r.name,
	}
}

// lateBodySPA — the SPA verdict over the content body h with the chunk's cache:
// a cache hit does not read the store (the T-055 class's records share the body). An
// error: a ctx cancellation — a pass error; a transient failure — false without
// an error (the row is skipped, the next cycle will come back), a Warn into the log.
func lateBodySPA(ctx context.Context, contents RenderContent, cache map[content.Hash]bool, h content.Hash, r *lateRow, log *slog.Logger) (bool, error) {
	if spa, ok := cache[h]; ok {
		return spa, nil
	}
	body, err := contents.Get(ctx, h)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return false, fmt.Errorf("httpworker: late re-publisher: content read: %w", err)
		}
		log.Warn("late re-publisher: the record content is unavailable — the row is skipped",
			"domain", r.name, "subdomain_id", r.key.sub, "err", err)
		cache[h] = false // a failure is not a verdict: unreadability is not "non-SPA",
		// but a repeated read in this chunk is pointless; the skip's price —
		// the whole cursor cycle until the return to the row (deliberate: a pass
		// error would stall the cursor on an unreadable block forever)
		return false, nil
	}
	spa := httpfetch.DetectSPA(body).SPA
	cache[h] = spa
	return spa, nil
}

// lateOriginsSPA — the original subdomains' verdicts of one domain's
// aliases (T-055): the candidates (lateRenderAliasSQL — the rework
// pass's guard predicates) are verdicted over the body; a rendered original — the verdict
// by itself (the fact of the render proves the class's SPA, the content is already a DOM —
// the T-259 P2-1 review), without reading the store. Any original variant with
// an SPA verdict makes the equivalence class SPA (the rework pass
// has the same semantics: both variants of different sites are inherited).
// Returns a subdomain → verdict map; an empty one — nothing to inherit.
// The body read errors — as at lateBodySPA (an original skip).
func lateOriginsSPA(ctx context.Context, pool db.DB, contents RenderContent, cache map[content.Hash]bool,
	dom lateKey, subs []int16, log *slog.Logger) (map[int16]bool, error) {
	rows, err := pool.Query(ctx, lateRenderAliasSQL, dom.zone, dom.domain, subs)
	if err != nil {
		return nil, fmt.Errorf("httpworker: late re-publisher: alias originals: %w", err)
	}
	defer rows.Close()
	out := make(map[int16]bool, len(subs))
	for rows.Next() {
		var (
			sub      int16
			variant  int16
			hash     []byte
			rendered bool
		)
		if err := rows.Scan(&sub, &variant, &hash, &rendered); err != nil {
			return nil, fmt.Errorf("httpworker: late re-publisher: alias originals: scan: %w", err)
		}
		if rendered {
			out[sub] = true // the render happened — the class was SPA, the body is not read
			continue
		}
		if len(hash) != content.HashLen {
			continue // there is nowhere to take the verdict from
		}
		var h content.Hash
		copy(h[:], hash)
		row := lateRow{name: "origin"} // for the Warn in lateBodySPA
		spa, err := lateBodySPA(ctx, contents, cache, h, &row, log)
		if err != nil {
			return nil, err
		}
		if spa {
			out[sub] = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("httpworker: late re-publisher: alias originals: %w", err)
	}
	return out, nil
}

// lateSelect — a chunk of candidates after the after cursor (lateRenderRowsSQL).
// The heavy selection's single point: an explicit transaction with the server-side cap
// SET LOCAL statement_timeout before the SELECT (T-271; the plan pinnings removed
// by T-273 — see the lateRenderRowsSQL comment: the unpinned
// T-251 shape), the client ctx deadline SelectTimeout covers the shell
// too (BEGIN/SET LOCAL/COMMIT). The transaction — only the cap's shell:
// SELECT reads committed data, commit/rollback are equivalent by content.
// The heavy scan under the http-worker's common gate (o.Gate,
// T-271): the late re-publisher's selection and retention do not coincide in
// PostgreSQL ("1+1 GiB" over shared_buffers — the Nov 8–9, 2026 incident);
// the gate is taken BEFORE Begin — the pool's connection does not idle in a queue.
func lateSelect(ctx context.Context, pool RecordDB, o LateOptions, before time.Time, after lateKey) ([]lateRow, error) {
	o = o.withDefaults()
	if err := o.Gate.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("httpworker: late re-publisher: selection: %w", err)
	}
	defer o.Gate.Release()

	ctx, cancel := context.WithTimeout(ctx, o.SelectTimeout)
	defer cancel()

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("httpworker: late re-publisher: selection transaction: %w", err)
	}
	// The rollback unconditionally: after the Commit — a no-op (the T-044 style).
	defer func() { _ = tx.Rollback(ctx) }()

	// The only SET LOCAL — the operator's cap (T-273): the shape
	// pinning (seq/hash/merge/bitmap off, T-271) unrolled the join from
	// domains at the production volume — the eternal loop over the deadline.
	if _, err := tx.Exec(ctx, fmt.Sprintf(lateSelectTimeoutFmt, o.SelectTimeout.Milliseconds())); err != nil {
		return nil, fmt.Errorf("httpworker: late re-publisher: selection cap: %w", err)
	}

	rows, err := tx.Query(ctx, lateRenderRowsSQL,
		before,
		int16(after.zone), int64(after.domain), int16(after.sub), int16(after.variant),
		o.BatchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("httpworker: late re-publisher: selection: %w", err)
	}
	defer rows.Close()

	var out []lateRow
	for rows.Next() {
		var (
			r        lateRow
			variant  int16
			hash     []byte
			aliasSub *int16
		)
		if err := rows.Scan(&r.key.zone, &r.key.domain, &r.key.sub, &variant, &hash, &aliasSub, &r.name); err != nil {
			return nil, fmt.Errorf("httpworker: late re-publisher: selection: scan: %w", err)
		}
		r.key.variant, r.hash, r.aliasSub = model.Variant(variant), hash, aliasSub
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("httpworker: late re-publisher: selection: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("httpworker: late re-publisher: selection commit: %w", err)
	}
	return out, nil
}

// RenderLateLoop — the late re-publisher's loop: an Interval ticker, one
// LateOnce pass per tick (a full cursor cycle over the table —
// hours/days; there is deliberately no initial drainage: running the whole table
// by chunks in a row — hours of background I/O at the service's start). The cursor —
// the loop's state (one goroutine, the retentionState T-185 style). A pass
// error — a Warn, the crawler_render_late_errors_total metric (T-273) and
// the ticker is switched into the DefaultRenderLateErrorBackoff backoff after
// EVERY error (T-271 introduced the backoff, T-273 made the reset per each —
// the pause is counted from the erroneous pass's end): the loop lives
// (the OnRun contract, T-012), the cursor stands in place, the pass after the pause
// will repeat the chunk, a success returns the normal interval. A ctx cancellation —
// a clean nil (graceful shutdown, §3.4).
func RenderLateLoop(ctx context.Context, pool RecordDB, contents RenderContent, render *RenderQueue,
	pending content.RetentionPending, met *Metrics, opts LateOptions) error {
	if pool == nil {
		return errors.New("httpworker: late re-publisher: the pool is not set")
	}
	if contents == nil {
		return errors.New("httpworker: late re-publisher: the content store is not set")
	}
	if render == nil {
		return errors.New("httpworker: late re-publisher: the render queue is not set")
	}
	if pending == nil {
		return errors.New("httpworker: late re-publisher: the unconsumed stream keys source is not set")
	}
	o := opts.withDefaults()
	if o.Log == nil {
		o.Log = slog.New(slog.DiscardHandler)
	}
	log := o.Log
	log.Info("render late re-publisher started",
		"interval", o.Interval.String(), "batch", o.BatchSize, "after", o.After.String())

	cursor := &RenderLateCursor{}
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	backoff := false
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			res, err := LateOnce(ctx, pool, contents, render, pending, met, o, cursor)
			if err != nil {
				// A cancellation during the pass — part of the stop, not an alarm.
				if ctx.Err() == nil {
					log.Warn("render late re-publisher", "err", err)
					met.LateError()
				}
				// A backoff after EVERY error (T-271 introduced the backoff,
				// T-273 made the reset per each — the ticker's anchor was the end
				// of the first erroneous pass, repeated errors left
				// a "pass minus period" pause, the WARN cadence masked
				// the duration): the pause is counted from the erroneous
				// pass's end — a minimum of "pass + backoff" between the starts;
				// the chunk's repeat — by the first tick of the pause, no duplicates
				// (the publication did not happen), a success will return the normal
				// interval.
				if ctx.Err() == nil {
					backoff = true
					lateResetTicker(ticker, o.ErrorBackoff)
				}
				continue
			}
			if backoff {
				backoff = false
				lateResetTicker(ticker, o.Interval)
			}
			// Wrapped on an exhausted set — every steady-state tick:
			// not noted by a log line (the T-259 P3-2 review), only
			// Debug; the significant events — publications and a stalled queue.
			if res.Published > 0 || res.Full {
				log.Info("render late re-publisher",
					"selected", res.Selected, "skipped", res.Skipped,
					"published", res.Published, "full", res.Full, "wrapped", res.Wrapped)
			} else if res.Wrapped {
				log.Debug("render late re-publisher: the set is exhausted, the cycle is wrapped")
			}
		}
	}
}

// lateResetTicker — the loop ticker's reset (T-271, the
// retentionResetTicker style — the mechanics and the justification there): a tick that fired
// during a pass longer than the interval is kept by the runtime (Go ≥1.23 —
// a deferred send into an unbuffered channel, cap(t.C) = 0; formerly —
// a channel buffer) and without draining the period would repeat the erroneous pass
// immediately; under the current Reset chain it removes the deferred tick itself,
// the drain — a harmless guard under the old chains (the T-271 P3-1 review).
func lateResetTicker(t *time.Ticker, d time.Duration) {
	t.Reset(d)
	select {
	case <-t.C: // draining the old period's undelivered tick
	default:
	}
}

// Package renderworker — the render stage (T-067; spec 5.6 "the final DOM replaces
// the raw HTML in the storage; rendered = true is set in the result", §5.7,
// §7 "render-worker → DOM + cookies → update of the result → q:analyze").
//
// The worker consumes the Options.Stream stream — q:render by default; the
// fallback worker (FR-12, spec §4.5) — the same binary with a different stream and a separate
// consumer group (the Result message — the key of the row
// subdomain_results: domain, subdomain, variant; the T-024 format, published by
// the HTTP stage T-066 on an SPA verdict T-057 and by the rework pass of renderq.go).
// The result itself stays in the DB, the message only points at the row —
// the final URL for the render is read from the row (the message carries only
// identifiers, spec 4.2). Processing of one delivery:
//
//	SELECT of the row (final_url, content_hash — the render target and the old reference)
//	→ render.Pool.Run (T-061…T-064: an isolated context, blocking
//	  of resources, a readiness wait, DOM + cookies + headers)
//	→ ContentStore.Put(DOM) — a new reference +1 (T-056/T-042, dedup by hash)
//	→ ONE transaction: UPDATE of the row (rendered = true, a new content_hash,
//	  final_url/cookies/headers of the render, rules_version = NULL; RETURNING
//	  checked_at — the hour of the rendered-rate bucket and the reanalysis tails) +
//	  −1 refcount of the old hash (SwapRefTx, T-044) + an increment of the
//	  reanalysis tails bucket crawler_counters['reanalyze:…'] (T-204: NULL
//	  returned the row to unanalyzed) and the hour bucket of the rate
//	  crawler_counters['rendered:YYYY-MM-DD"T"HH24'] (T-203: in the same
//	  transaction — the flag and the bucket change only together)
//	→ after the commit — a q:analyze publication (Result, the same key; §7)
//	→ XACK (Processing, T-025: an acknowledgment only after the write to the DB).
//
// Idempotency of a re-delivery (spec 3.4): a rewrite without the rendered guard —
// a repeated render overwrites the content and the refcount (a balance
// in the spirit of T-058: a repeated Put gives +1, the UPDATE removes the old reference).
// Against the race with a fresh domain recheck (the recheck T-030 rewrites
// rows DELETE+INSERT, T-058) the row is protected by an optimistic UPDATE
// guard on content_hash: a changed row is not overwritten (0
// rows — the Put reference is removed, the delivery is acknowledged: the task was about
// the previous episode, the new one has its own publication and render).
//
// The render_pending flag (T-066) is not touched by the render-worker: the flag's owner is
// the HTTP stage, its rework pass clears the bit after a q:render publication;
// the render changes only the result row (§7 — the domain is already done, the render
// is an asynchronous addition). The q:render consumption is sliding (T-142,
// the T-139 pattern): a read when a delivery slot frees up, without a
// chunk window barrier.
//
// The timeout policy (T-148, tarpit counting — T-261): a page timeout
// (render.timeout) is a predictable tarpit, not a random error. Until
// the number of the episode's tarpit-classified deliveries (the cardinality of the set
// render:tarpit:{task_id}: SADD by the delivery id — idempotent, a 48 h TTL)
// reaches Options.TimeoutAttempts (render.timeout_attempts), a tarpit
// lives the common path of re-deliveries; on exhaustion the entry
// is finalized without a render: the result is the raw HTML of the HTTP phase
// (rendered = false), a q:analyze publication, XACK — work does not fall into
// q:dead, q:dead stays for infrastructure errors (DB, browser,
// publication; runbooks §2). Infrastructure failures between tarpits grow the
// delivery number, not the set: mixed series are not finalized
// before the tarpit threshold. Losing the Redis counter degrades to a decision by
// the PEL delivery number (the behavior before T-261); the overall limit
// redis.max_attempts works by the delivery number as before. The decision table
// of the outcome policy FSM — outcomestate.go (T-261, TZ-fsm §3
// FR-7).
//
// The permanent host certificate error policy (T-159, an extension of T-172):
// ERR_CERT_AUTHORITY_INVALID / ERR_CERT_COMMON_NAME_INVALID — permanent
// properties of the host (the certificate is not issued by a trusted CA / does not cover the name),
// a retry is pointless; ERR_CERT_DATE_INVALID (expired/not yet valid)
// was added by the T-172 measurement: 0 successful retries out of 321 first failures
// per day, 92% exhausted the attempts in q:dead — a parked certificate's renewal
// on a scale of days does not make it within 3 deliveries (~half an hour). The cert class
// is finalized like a tarpit, but from the very first delivery (without the tarpit limit):
// the result is the raw HTML of the HTTP phase, a q:analyze publication, XACK; the daily
// q:dead growth from the cert class is zero, the q:render queue does not accumulate
// the attempts of cert hosts.
//
// The DNS refusal policy at render (T-211, from the T-207 outcomes):
// ERR_NAME_NOT_RESOLVED — the death of the host at the DNS level. The entry got into
// q:render through a live HTTP episode (the HTTP phase resolved the name itself —
// otherwise there would be no episode and no row), while the browser resolves
// with the system resolver — the same unbound that serves the DNS stage: a refusal between
// the episode and the render means the host's DNS died; DNS recovery is an event
// on a scale of days, the retry window (3 deliveries, ~20 min in the PEL by claim_min_idle)
// does not cover it (the T-207 measurement: 252 NAME_NOT_RESOLVED finalizations per
// ~7 h, each ate all 3 deliveries). The DNS class is finalized following
// the cert class pattern from the very first delivery: the result is the raw HTML
// of the HTTP phase, a q:analyze publication, XACK; a host with recovered DNS
// will render on the next recheck episode (T-030 — the row rewrite
// resets the marker and publishes a new task). ERR_CONNECTION_REFUSED
// is not in the class: a connection refusal is a property of the host's service (the port
// is closed/filtered/restarted), recovery is observed (the T-172 measurement:
// 6.6% of retries succeed) — until the last delivery it is retryable (T-172).
//
// The last attempt × task class policy (T-172): other render errors
// of the task class (a connection refusal, a load error — properties
// of the host, not the infrastructure) live the common path of re-deliveries while
// the delivery number is below redis.max_attempts (Options.MaxAttempts; a
// connection refusal recovers — the T-172 measurement: 6.6% of retries succeed), and
// on the last delivery the entry is finalized without a render following the
// tarpit pattern: the result is the raw HTML of the HTTP phase, a q:analyze publication, XACK.
// The q:render deaths are left only to infrastructure failures (DB,
// storage, publication, browser) — in the spirit of T-148 "q:dead is for real
// infrastructure errors".
//
// The fallback publication (T-238, spec §4.4): with
// render.fallback.enabled on (Options.Fallback, T-233/FR-14) the tarpit T-148
// and the task class T-172 on the last attempt are engine-branched reasons
// (a property of the "site × engine" pair: a page stuck on a light engine
// may open in chromium, spec §4.4 item 2) — they are not finalized but
// published to q:render-fallback (publishFallback): the entry stays
// without a marker ("awaiting fallback" — the intermediate state is expressed
// by the absence of the marker and the queue depth, spec §4.4 item 1) and is reworked
// by the chromium worker B — the same binary on the fallback stream (FR-12,
// T-236); a recursive fallback of the fallback is forbidden: publications go only
// from the q:render stream (FR-10). The DNS class T-211 and the cert class T-159 are
// engine-independent host properties and get no fallback under any
// flag (FR-7: Chromium will not fix them). The publication's idempotency —
// a dedup filter by the result row identifier (the row key +
// the episode's checked_at, spec §4.4 item 5); the publication decision is a pure
// function, fallbackPublishDecision (candidate × gate × budget, FR-9):
// the gate by the streak of infra errors of ClassBrowser (render.fallback
// .health_gate_streak — publications suspended until the engine
// recovers, T-239) and the share budget of publications over the window (render.fallback
// .share_percent — suspended until the window boundary, T-239); a decision
// rejection — finalization as today (the marker), no tasks lost.
//
// All four finalizations stamp the "render impossible" marker
// subdomain_results.render_failed_at (T-210, migration 000017) — except
// the fallback paths above: there the primary worker does not set the marker at all,
// worker B sets it after the fallback is exhausted (FR-10):
// an entry finalized without a render is forever rendered = false and before
// the marker it was indistinguishable from "not tried" — the rework pass
// (httpworker/renderq.go) re-published such rows indiscriminately (the
// T-207 measurement: a tail of ~180–220 thousand broken rows). The marker stops the
// REPEATED publications of the pass — a repeated pass over a domain whose flag
// survived the publication (a partial publication failure — the published part
// is finalized within seconds, a pass repeat would publish it again; the Full
// exit after the publication of the chunk's neighbors; keeping unreadable content;
// a restart between the publication and the removal); the first attempt of a fresh episode
// is legitimate: an episode rewrite resets the marker, new content has
// the right to a render — a recovered host will render on the next
// recheck episode T-030; a successful render removes the marker
// (renderUpdateSQL).
//
// # The render-state FSM of the subdomain_results row (T-259, TZ-fsm §3 FR-5)
//
// The single source of truth about the states and transitions of the row's
// render life. The state is encoded by THREE carriers: the row's columns
// (rendered, render_failed_at, final_url), the domain's render_pending bit
// (flags bit 1, T-066 — "the publication is deferred, debt") and the
// messages of the q:render/q:render-fallback streams (the row key, the Result format T-024).
// The formalization does not rewrite the transitions on the FSM framework (TZ-fsm §1):
// they remain SQL guards and publications; this table is what
// all the writers (listed below) check against, writers from other
// packages refer here in one line.
//
// ## States
//
//		E0 "new episode"   the row is written by the HTTP stage (T-058, DELETE+INSERT):
//		                    rendered = false, render_failed_at = NULL, final_url —
//	                   the final URL of the check. The SPA verdict is a property
//	                   of the CONTENT (T-057), not stored in the row: non-SPA rows
//	                   do not require a render at all and stay in E0 until the
//	                   episode is rewritten.
//		E1 "queued"        the Result message of the row in the undelivered tail
//		                    of q:render, OR the domain carries the render_pending bit
//		                    (the queue was full — the publication was deferred, T-066;
//		                    the holder — the bit, not the stream). The columns are indistinguishable from E0:
//	                   the difference lives outside the row, re-publishers must
//	                   interlock (invariant I4).
//		E1f "awaiting fallback" a sub-state of E1: worker A published the task to
//		                    q:render-fallback (T-238), worker B has not reworked it yet.
//		                    Column-free — THERE IS NO SEPARATE COLUMN (spec §4.8):
//		                    rendered = false, no marker, no messages in q:render.
//		                    The observation — the fallback balance
//		                    crawler_render_fallback_published_total =
//		                    crawler_render_fallback_consumed_total + the depth
//		                    of q:render-fallback (+ those buried in q:dead, FR-11)
//		                    and the late re-publisher's interlock (T4 does not touch
//		                    rows with live stream messages).
//		E2 "rendered"      rendered = true, render_failed_at = NULL
//		                    (invariant I1 — the CHECK of migration 000018, FR-5.2).
//		E3 "impossible"    rendered = false, render_failed_at NOT NULL — the marker
//		                    of T-210: a finalization without a render closed the row until
//		                    the content changed; render passes do not re-publish it.
//		E4 "slipped out from under the task"
//		                    the row is rewritten by a new episode AFTER the publication
//		                    of the message: the deliveries of the old episode see foreign
//		                    content/checked_at and are acknowledged without effects
//		                    (the guards below); the row's new state — E0 again
//		                    with its own publications.
//
// ## Transitions: state × event × guards × target × side effects
//
//	| # | Source | Event | Guards | Target | Side effects | Writer |
//	|---|--------|-------|--------|--------|--------------|--------|
//	| T1 | E0 | the HTTP stage publishes an SPA row, the queue has room | DetectSPA(body) = SPA (T-057, deterministic over the same bytes) | E1 | an XADD to q:render after the result transaction is committed (Result, T-024) | httpworker/record.go OnResult → renderSPAResults (T-066) |
//	| T2 | E0 | the HTTP stage publishes an SPA row, the queue is full | the same + the q:render depth ≥ render.queue_high_watermark (a read error = full, conservatively) | E1 (debt) | the render_pending bit of the domain by THE SAME result transaction (+1 of the debt counter T-175), no publication happens | httpworker/renderq.go renderSetPendingSQL (the bit × counter FSM — specified there too, T-258) |
//	| T3 | E1 (debt) | the rework pass | the queue has room, the domain is not deleted, the verdict over the stored content = SPA | E1 | a q:render publication → the bit removal (−1 of the counter) in one transaction; marked rows (E3) are not published; aliases inherit the original's verdict | httpworker/renderq.go RequeueOnce (T-066; the D3/D4 transitions of the bit FSM, T-258) |
//	| T4 | E0/E1 | the late re-publisher | the age (checked_at) > 6 h, no render_pending bit, no unconsumed q:render/q:render-fallback messages (a stream snapshot, the pattern of content/retentionpending.go), the verdict over the content = SPA | E1 | a q:render re-publication (the crawler_render_late_republish_total metric); cures MAXLEN truncation of fallback tasks and lost publications — the row's state stops depending on the message's survival; duplicates are bounded and suppressed by the consumer's idempotency (spec 3.4) | httpworker/renderlate.go LateOnce (T-259, FR-5.3) |
//	| T5 | E1 | worker A: a successful render | the content_hash guard (the row of that very episode whose message it is; a T-058 rewrite has already changed it — 0 rows, the Put reference is removed, XACK) | E2 | the content is replaced by the DOM (Put +1 / SwapRefTx −1 of the old one), render_failed_at = NULL, rules_version = NULL, the rendered:/reanalyze: buckets (T-203/T-204), a q:analyze publication, XACK | renderUpdateSQL (this file) |
//	| T6 | E1 | worker A: a finalization without a render — an engine-independent reason (the tarpit budget T-148, cert T-159, DNS T-211, the task class T-172 on the last one) OR the fallback is off/another stream/the chromium engine/a T-239 gate-budget rejection | rendered = false, the content_hash guard (coalesce — the first one stamps) | E3 | the render_failed_at marker, a q:analyze publication, XACK; q:dead — only for infrastructure errors | renderFailMarkerSQL, finalizeNoRender |
//	| T7 | E1 | worker A: an engine-branched reason, the fallback is on, the fallbackPublishDecision is positive | the consumption stream is q:render (fallback recursion is forbidden, FR-10), a candidate engine, no row/rendered — do not publish | E1f | a q:render-fallback publication (no marker, no q:analyze), XACK; a delivery duplicate — the dedup filter by the row key + the episode's checked_at | publishFallback (T-238) |
//	| T8 | E1f | worker B: a successful render | as T5 | E2 | as T5 | renderUpdateSQL (the B build, the q:render-fallback stream) |
//	| T9 | E1f | worker B: the fallback is exhausted | rendered = false, the content_hash guard | E3 | as T6 | finalizeNoRender (the B build, FR-10) |
//	| T10 | any | the episode is rewritten by a recheck | a new episode of the domain (T-030 → the HTTP stage T-058) | E0 | DELETE + INSERT of rows: rendered = false, the marker NULL (does not take part in the INSERT), a fresh checked_at; live messages of the old episode are extinguished by the T5/T6 guards/the T7 dedup filter | httpworker/record.go |
//	| T11 | E2 | a successful render of a re-delivery (a concurrent duplicate) | the content_hash guard lets a rewrite with the same content through | E2 | the repeated rewrite is idempotent (spec 3.4): a repeated Put +1 / the UPDATE removes the same reference | renderUpdateSQL |
//
// E4 is not a transition but an outcome of deliveries: the content_hash
// guards (renderUpdateSQL/renderFailMarkerSQL, IS NOT DISTINCT FROM — NULL
// "no content" is legal) and the fallback dedup filter (the episode's
// checked_at) recognize the rewritten row, the delivery is acknowledged without effects.
//
// Non-transitions (they do not change the render state): retention (T-166/T-167)
// trims the content/jsonb of non-finds in E0/E3 — E1/E1f is protected by the
// interlock (unconsumed q:render/q:render-fallback messages + the bit);
// the analyzer stamps rules_version; monitor reads. collapse
// (moveResultsSQL — moving rows on a zone merge) and the dump import
// (cmd/crawlerctl fixtures_import, ON CONFLICT DO UPDATE) write the row
// under invariant I1: they do not change the render columns, but they are the first
// to meet 23514 if the row's source is a violator (manual SQL).
//
// ## Invariants
//
//	I1  E2 ⇒ render_failed_at IS NULL. The SQL guards always held this
//	    (renderUpdateSQL writes the pair together, the marker — only with
//	    rendered = false); since T-259 — the CHECK subdomain_results_rendered_check
//	    (migration 000018, NOT VALID — without scanning 40+ million rows;
//	    VALIDATE — per the migration regulations, docs/schema.md).
//	I2  the marker ⇒ rendered = false (E3): finalizations only mark
//	    non-rendered rows (the rendered = false guard), a successful render
//	    removes the marker; the exit from E3 — only by a content change (T10) or
//	    a successful render of a fresh episode.
//	I3  "awaiting fallback" (E1f) is column-free by construction (spec §4.8):
//	    observable by the fallback balance (published = consumed + the depth
//	    of q:render-fallback + buried) and does not bother the re-publisher — the
//	    T4 interlock excludes writing over a live stream message.
//	I4  E0 and E1 are indistinguishable by columns: every re-publisher must
//	    interlock over the task holders — the debt pass T3 by the bit guard,
//	    the late re-publisher T4 by a snapshot of unconsumed messages; publication
//	    duplicates are bounded by the pass window and suppressed by the consumer's
//	    idempotency (spec 3.4).
package renderworker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"crawler/internal/config"
	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/metrics"
	"crawler/internal/queue"
	"crawler/internal/render"
)

// Stage SQL. rendered = true — the literal of spec 5.6; rules_version = NULL —
// the content changed, the analyzer's previous conclusion (rules_version is set
// by T-068+ when writing findings) does not match the new content: the repeated
// q:analyze publication below re-analyzes the row and sets a fresh
// version. Keys — as literals of types (the record.go T-058 style).
var (
	// renderSelectSQL — the render target: the final URL of the result row, its
	// current content reference (the one disputed by the optimistic
	// renderUpdateSQL guard), the rendered flag (the fallback publication T-238
	// skips already rendered rows — there is nothing to rework) and checked_at —
	// the hour of the rendered-rate bucket (T-203) and the episode identifier
	// of the row for the fallback dedup filter (T-238: an episode rewrite T-058 writes
	// a new checked_at, the episode's re-deliveries read the same one).
	// The leading PK columns — LIST partitioning by zone_id and
	// HASH subpartitioning (T-005) cut the search down to a leaf.
	renderSelectSQL = "SELECT final_url, content_hash, rendered, checked_at FROM subdomain_results " +
		"WHERE zone_id = $1::int2 AND domain_id = $2::int8 " +
		"AND subdomain_id = $3::int2 AND variant = $4::int2"

	// renderUpdateSQL — replacing the raw HTML with the final DOM (spec 5.6, §5.7):
	// rendered = true, content_hash — the reference to the DOM (Put above has already given it
	// +1, here the row switches to it), final_url/cookies/headers —
	// the render data (a JS redirect is visible only to the render — T-061; cookies —
	// all of them, including httpOnly and those set by JS, spec 5.6/§11.4 item 3 — the single
	// jsonb contract with T-058; headers — the response of the final navigation, spec 5.8).
	// render_failed_at = NULL — a successful render removes the "render
	// impossible" marker (T-210): a concurrent re-delivery rendered
	// what another finalization had managed to mark; the marker invariant
	// "set ⇒ rendered = false" holds without relying on the delivery
	// order.
	// The guard content_hash IS NOT DISTINCT FROM $9 — optimistic:
	// the row is rewritten only while its content is the same as the one read
	// (NULL compares correctly — "no content" is legal, T-056).
	// RETURNING r.checked_at (T-203) — the hour of the rendered-rate hourly bucket
	// (renderBumpRenderedSQL of the same transaction): the hour of the row's EPISODE, not
	// the hour of the render — that is how the live window of the former rate reader
	// counted (the rendered share among the window's results by checked_at); an empty
	// result — the guard cut it off (the former RowsAffected 0): the row slipped
	// out from under the task.
	renderUpdateSQL = "UPDATE subdomain_results AS r " +
		"SET rendered = true, final_url = $5::text, content_hash = $6::bytea, " +
		"cookies = $7::jsonb, headers = $8::jsonb, rules_version = NULL, render_failed_at = NULL " +
		"WHERE r.zone_id = $1::int2 AND r.domain_id = $2::int8 " +
		"AND r.subdomain_id = $3::int2 AND r.variant = $4::int2 " +
		"AND r.content_hash IS NOT DISTINCT FROM $9::bytea " +
		"RETURNING r.checked_at"

	// renderFailMarkerSQL — the "render impossible" marker (T-210):
	// the finalizations without a render (the tarpit T-148 / the cert class T-159 / the task
	// class T-172 / the DNS class T-211) stamp the row's render_failed_at —
	// the rework pass selection (httpworker/renderq.go,
	// renderPendingRowsSQL) excludes marked rows UNTIL THE
	// CONTENT CHANGES: the cert class, the DNS class and the task class are permanent
	// host properties (the T-172/T-207 measurements), the tarpit's render.timeout_attempts budget is exhausted —
	// the pass's repeated publications burn deliveries and slot-time (the
	// T-207 measurement: a tail of ~180–220 thousand broken rows; the marker stops
	// exactly the REPEATED publications of the pass — a repeated pass over a domain
	// whose flag survived the publication: a partial publication failure
	// (the published part is finalized within seconds, a repeat would publish
	// it again), a Full exit after the publication of the chunk's neighbors, keeping
	// unreadable content, a restart between the publication and the removal;
	// the first attempt of a fresh episode is legitimate — an episode rewrite
	// resets the marker). coalesce — the stamp
	// of the FIRST finalization ("how long ago it was recognized as impossible" in diagnostics),
	// a repeated finalization of the same row (a publication failure returned
	// the delivery) does not change the value, RowsAffected stays 1.
	// The guards — the same principles as renderUpdateSQL's:
	// content_hash IS NOT DISTINCT FROM $5 — optimistic (only the row of that
	// content whose render failed is marked; the recheck
	// T-058 has already rewritten it — RowsAffected 0, the new episode's stamp is not
	// set; a known window: the content is byte-identical — the hash
	// was deduplicated T-056, the stamp will land on the new episode's row;
	// the price — the pass will not re-publish it until the next episode, a direct
	// publication of the result row renders it even with the stamp),
	// rendered = false — a successful concurrent render has settled the render
	// question, the stamp is not needed for it. The reset — by a content change: a
	// result write of the HTTP stage (T-058) rewrites the domain's rows
	// DELETE + INSERT — the new rows get NULL (a recovered host
	// will render on the next episode), a successful render removes the stamp
	// (renderUpdateSQL above). Autocommit without a transaction: the statement is single,
	// atomic by itself (the style of targeted UPDATEs of record.go).
	renderFailMarkerSQL = "UPDATE subdomain_results AS r " +
		"SET render_failed_at = coalesce(r.render_failed_at, now()) " +
		"WHERE r.zone_id = $1::int2 AND r.domain_id = $2::int8 " +
		"AND r.subdomain_id = $3::int2 AND r.variant = $4::int2 " +
		"AND r.rendered = false " +
		"AND r.content_hash IS NOT DISTINCT FROM $5::bytea"

	// renderBumpReanalyzeSQL — an increment of the unanalyzed tails bucket
	// crawler_counters['reanalyze:YYYY-MM-DD"T"HH24'] (T-204): a content
	// replacement sets rules_version = NULL — the analyzer's previous
	// conclusion does not match the new content, the row is a tail again
	// until the stamp of the repeated q:analyze publication below. $1 — the
	// row's checked_at from RETURNING renderUpdateSQL: the hour of the EPISODE, not the render —
	// the same key as the record increment (record.go: the hour of now() =
	// the rows' checked_at) and the analyzer stamp decrement (worker.go:
	// RETURNING checked_at) — a key desync would leave the decrement
	// without a target. The guard of the reanalysis tick pass
	// (analyzer/reanalyze.go) sums the buckets older than the current hour:
	// a fresh render (an episode of the current hour) lands in the immature bucket and
	// the guard does not open; a render lateness > 1 h resurrects the
	// old hour's bucket — the guard opens, the tick reads the window out, no tail behind
	// the cursor is found (the row behind the cursor — the residual time-shape
	// class of T-193 §4.2), a monitor pass removes the bucket. Rendering a row
	// not yet stamped by a record (a race with the analyzer: NULL →
	// render(NULL) → the first stamp) gives +1 without a paired decrement —
	// the streaming semantics of events (the T-202/T-203 family), the absolutes
	// are aligned by reconciliation. The upsert pattern of renderBumpRenderedSQL;
	// the lock order — BEFORE rendered: (the reconciliation pass holds
	// the families "retry_mature: → reanalyze: → rendered: → results:") — without
	// cycles.
	renderBumpReanalyzeSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('reanalyze:' || to_char(date_trunc('hour', $1::timestamptz), 'YYYY-MM-DD\"T\"HH24'), $2::int8, now()) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()"

	// renderBumpRenderedSQL — an increment of the rendered-rate hourly bucket
	// crawler_counters['rendered:YYYY-MM-DD"T"HH24'] (T-203): +1 per
	// row moved by this transaction to rendered = true; $1 —
	// the row's checked_at from RETURNING renderUpdateSQL, the DB computes
	// the bucket hour (date_trunc) — the timezone is consistent with the result
	// record increment (record.go), the rate reader and the reconciliation.
	// The live analog — the former rendered_hour subquery of throughputSQL
	// (count(*) over subdomain_results, checked_at >= now() − 1 h AND
	// rendered): index-only over fresh pages degraded
	// by heap fetches — the checkpoint of stage 27 (the mechanism of the daily
	// window T-179). A repeated render of an already rendered row (a re-delivery
	// on a lost XACK; the hash guard lets a rewrite
	// with the same DOM through) gives +1 to the live zero — the streaming semantics of events
	// (like the daily rows of T-179), the absolutes are aligned by reconciliation.
	// The upsert pattern of record.go; the counter row — the ONLY lock
	// point of the transaction after the row's UPDATE: a single point does not create
	// lock cycles (the order is common with all writers
	// of crawler_counters — reconciliation takes rendered: after
	// retry_mature: and before results:).
	renderBumpRenderedSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('rendered:' || to_char(date_trunc('hour', $1::timestamptz), 'YYYY-MM-DD\"T\"HH24'), $2::int8, now()) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()"
)

// RenderDB — the pool for reading the target and writing the result: a db.DB with
// transactions (the httpworker.RecordDB, content.RefDB style). The row
// replacement and the reference swap go in one transaction (spec 3.4);
// *pgxpool.Pool satisfies the interface.
type RenderDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// ContentStore — the content storage seam (spec 5.7): writing the DOM and removing
// the reference when the row slipped out from under the task. A narrow subset of the contract of
// content.ContentStore (the stage does not need Get — the DOM is not read back);
// satisfied by *content.PackStore (T-042).
type ContentStore interface {
	Put(ctx context.Context, data []byte) (content.PutResult, error)
	Delete(ctx context.Context, hash content.Hash) error
}

// Renderer — the render seam: satisfied by *render.Manager (T-065 — the production
// build of cmd/render-worker: the live "browser + pool" generation, restarts
// per pages/memory, a reconnect and a task retry) and *render.Pool directly
// (T-061 — the integration tests). One task — one page in a fresh
// isolated context; the parallelism is limited by the pool's own slots
// (render.contexts, spec 5.6).
type Renderer interface {
	Run(ctx context.Context, task render.RenderTask) (render.RenderOutput, error)
}

// Worker defaults.
const (
	// DefaultStream — the worker's default consumption stream (q:render,
	// spec 4.2): the Run loop's read and the RecoverLoop claim. The fallback
	// worker (spec §4.5 item 1, FR-12) — the same binary built differently: Options.Stream
	// is overridden to the fallback stream (q:render-fallback, the constant
	// queue.StreamRenderFallback — task T-237), Options.Group — to
	// a separate group; no second worker code is written.
	DefaultStream = queue.StreamRender

	// DefaultGroup — the consumer group of the worker's consumption stream (spec
	// 4.2: consumer groups scale the workers horizontally);
	// the default build — the q:render group, the fallback worker (FR-12)
	// is built with a separate group.
	DefaultGroup = "render-worker"

	// DefaultConcurrency — q:render deliveries in flight at once.
	// Above render.contexts it does not speed up the render itself (the pool slots are the stage's
	// bottleneck, spec 5.6), but it overlaps its DB part (Put, the transaction,
	// the publication) — we take it by the former upper bound of the spec-recommended
	// contexts (4; the recommendation itself, spec 5.6 — 2–4): the value is
	// a floor for isolated builds and tests without a config. The default
	// render.contexts since T-169 is 8, the production build passes
	// max(DefaultConcurrency, render.contexts): the pool slots must not
	// idle at contexts > 4 (cmd/render-worker).
	DefaultConcurrency = 4

	// DefaultConsume — q:render entries per one read: the size of the XREADGROUP
	// chunk and of the prefetch buffer of the sliding consumption (T-142,
	// the T-139 pattern) — the read runs ahead of processing by no more than a couple of chunks
	// (the current one in the reader's hands + the buffer), the next read happens when
	// delivery slots free up, the read window does not limit the
	// parallelism; the same number of deliveries in a row without an acknowledgment — the threshold
	// of the pause fuse (renderFailWaitBase below).
	DefaultConsume = 16

	// DefaultTimeoutAttempts — the number of delivery attempts a q:render task
	// may end with a page timeout (render.timeout) before
	// the entry is finalized without a render (T-148): a timeout is a predictable
	// property of a heavy page, not a random error; burning the overall
	// redis.max_attempts limit on it (3 deliveries → q:dead) means losing work.
	// The second attempt after claim_min_idle (10 min) gives the page a chance
	// in changed conditions (the first one may time out because of a jam —
	// the 2026-10-23 measurement: the slots are busy with timeouts, 0.34 entries/s), after that
	// the repeat is predictable. 2 of 3 deliveries — the tarpit budget is smaller than the overall one,
	// q:dead stays for infrastructure errors (DB, browser, publication).
	DefaultTimeoutAttempts = 2

	// DefaultTarpitTTL — the lifetime of the task's tarpit counter key
	// render:tarpit:{task_id} (T-261, FR-7): 2× the episode's retry horizon
	// (TZ-fsm FR-7 fixes the 48 h default). The horizon — the window of PEL deliveries
	// (max_attempts × claim_min_idle ≈ half an hour) plus the fallback queue
	// (hours — worker B's rate); the episode's re-publications publish a new
	// task_id with a fresh budget and do not extend the old key's horizon.
	// The counter must survive all the episode's deliveries (claims and worker
	// restarts happen between the marks) and must not accumulate forever: the TTL
	// is refreshed by every mark, the key dies 48 h after
	// the task's last tarpit (docs/redis.md §2).
	DefaultTarpitTTL = 48 * time.Hour

	// DefaultErrorWait — the pause after Redis errors and with an unavailable
	// browser (the httpworker.DefaultErrorWait style): idle circles do not
	// heat the logs, unacknowledged deliveries will be returned by XAUTOCLAIM (T-025).
	DefaultErrorWait = 30 * time.Second
)

// Options — the worker's parameters; zero values are replaced by defaults
// (withDefaults, the httpworker.Options style).
type Options struct {
	// Concurrency — q:render deliveries in flight at once; 0 —
	// DefaultConcurrency.
	Concurrency int
	// Consume — q:render entries per one read; 0 — DefaultConsume.
	Consume int
	// Rate — the consumption rate cap: no more than Rate entries per
	// the sliding window renderRateWindow (T-240, FR-13 — the batch
	// "collect and rework" mode without scaling up chromium capacity; the window is
	// RateWindow = 1 min). The rate parameter is shared, with no binding to an engine
	// or a stream (FR-4: there are no engine branches) — applied whenever
	// set; the consumption is limited (XREADGROUP), the
	// RecoverLoop claim is not touched by the cap (the return rate of stuck deliveries —
	// redis.recover_*, T-153). The production wiring sets the cap on the
	// worker B build (render.fallback.worker_rate, the q:render-fallback stream,
	// cmd/render-worker); an empty read does not spend the cap. ≤ 0 — without
	// a cap (the default: the main q:render build reads as before).
	Rate int
	// Stream — the worker's consumption stream: the Run loop's read and the
	// RecoverLoop claim (FR-12: the fallback worker — the same binary with the
	// q:render-fallback stream and a separate group); "" — DefaultStream (q:render,
	// zero behavior change of the main build). The name must be a working
	// queue stream (knownStream, internal/queue) — the transport rejects
	// others; the q:analyze publication does not depend on the consumption stream
	// (the Result format is shared by both streams).
	Stream string
	// Group — the consumer group of the Stream stream; "" — DefaultGroup.
	Group string
	// Consumer — the group's consumer name; "" — "render-worker-<pid>".
	Consumer string
	// Alive — the browser probe: false — the queue is not read (deliveries
	// stay in the stream unread, not in the PEL without XACK — without
	// claims an entry in the PEL hangs for a long time). Set by the service build
	// (render.Manager.Alive, T-061/T-065: the probe restarts a dead
	// generation itself — a reconnect); nil — the probe is off.
	Alive func() bool
	// Recover — the layer of returning stuck deliveries (Reliab, T-025): Run
	// starts a background RecoverLoop (XAUTOCLAIM → the same processing →
	// XACK) with the same Group/Consumer; nil — no recovery
	// (isolated builds and tests without Redis specifics).
	Recover *queue.Reliab
	// TimeoutAttempts — how many deliveries of a task may end
	// with a page timeout (render.timeout, a predictable tarpit) before
	// the entry is finalized without a render — the result stays the raw HTML
	// of the HTTP phase, the entry goes to q:analyze, the delivery is acknowledged
	// (T-148). Since T-261 the threshold is counted by the number of the episode's tarpit-classified
	// deliveries — the cardinality of the render:tarpit:{task_id} set
	// (tarpitsExhausted, Options.Tarpits): mixed series (an infra failure
	// between tarpits) are not finalized before the threshold; the counter
	// unavailable/not mounted — degradation to the PEL delivery number
	// (the behavior before T-261). Must be ≤ redis.max_attempts — otherwise
	// the tarpit would die in q:dead before finalization (checked by config.Validate).
	// 0 — DefaultTimeoutAttempts.
	TimeoutAttempts int
	// MaxAttempts — the maximum deliveries of a task (redis.max_attempts,
	// T-172): a render error of the task class (a connection refusal, a load error —
	// a host property) on the delivery with this number finalizes the entry without
	// a render (following the tarpit pattern T-148) instead of taking it to q:dead; below
	// the number — ordinary retryable errors (host recovery is
	// observed: the T-172 measurement — 6.6% of ERR_CONNECTION_REFUSED retries
	// succeed). Must equal redis.max_attempts of the same Reliab
	// (the production build passes cfg.Redis.MaxAttempts): smaller — entries
	// would be finalized before exhaustion, larger — the policy would not
	// make it before the burying. 0 — queue.DefaultMaxAttempts.
	MaxAttempts int
	// Tarpits — the counter of tasks' tarpit-classified deliveries
	// (T-261, TZ-fsm §3 FR-7): the tarpit-finalization decision is made
	// by the cardinality of the render:tarpit:{task_id} set (SCARD ≥
	// TimeoutAttempts), not by the PEL delivery number — the number is grown by all
	// deliveries (including infra failures), the set only by tarpits. The production
	// build passes queue.NewTarpitCounter(rdb) (the same Redis client
	// as the queue's); nil — degradation to the PEL delivery number,
	// the behavior before T-261 (isolated builds and tests without
	// Redis specifics — like Recover/Metrics).
	Tarpits TarpitStore
	// RecoverBatch — the RecoverLoop claim chunk per pass; 0 — the queue
	// default (queue.DefaultRecoverBatch). The production build passes
	// redis.recover_batch (T-153): the return rate of stuck deliveries is a
	// queue-configurable value, not the stage's consumption window. The invariant with
	// claim_min_idle — queue.DefaultRecoverBatch (the render is the slowest
	// sequential crawl: 5–25 s/entry).
	RecoverBatch int
	// RecoverInterval — the poll period of RecoverLoop for stuck entries;
	// 0 — the queue default (queue.DefaultRecoverInterval). The config —
	// redis.recover_interval (T-153).
	RecoverInterval time.Duration
	// Fallback — the fallback publication from the finalization paths without a render
	// (T-238, spec §4.4, FR-8/FR-14): turning it on (render.fallback.enabled,
	// T-233) switches the tarpit T-148 and the task class T-172 (the last
	// attempt) from setting render_failed_at to publishing the task to
	// q:render-fallback. The zero value (the default) — no fallback,
	// the finalization behavior stays as before.
	Fallback FallbackOptions
	// Engine — the render engine of the build (render.engine, T-234; the vocabulary
	// config.EngineChromium|EngineMoli|EngineLightpanda): the value of the
	// engine label of the stage metrics (T-235, spec §4.6 item 1, FR-15 — wired in
	// NewMetrics at metrics initialization; cardinality 3) and the input
	// of the engine guard of the fallback publication (S-P1-1: chromium, including
	// an empty value, does not publish the fallback — fallbackArmed). It does not affect
	// the render's own behavior: only the configuration and the
	// allocator see the engine (FR-4); here — observability and the fallback guard; the vocabulary
	// and the "engine × remote_url × memory threshold" bundle were already checked by
	// config.Validate and render.Options.validate. "" — chromium (like
	// render.Options.Engine, builds without configuration).
	Engine string
	// Log — the logger; nil — records suppressed (slog.DiscardHandler).
	Log *slog.Logger
	// Metrics — the service's Prometheus registry (T-077, spec 5.9): New
	// builds the stage metrics from it (NewMetrics); nil — without metrics (isolated
	// builds and tests).
	Metrics *metrics.Registry
}

// FallbackOptions — the fallback publication parameters (T-238, spec §4.4
// items 1–2/5, FR-7/8/10/14). The cap configuration fields (worker_rate,
// share_percent, health_gate_streak — T-233) are not here: they
// belong to the gate/budget/worker B (T-239/T-240); here — only
// the switch the worker must honor on the finalization paths.
type FallbackOptions struct {
	// Enabled — render.fallback.enabled (T-233, FR-14): false (the default,
	// zero behavior change of the production circuit until explicitly enabled) —
	// the finalizations without a render work as today: the tarpit T-148 and
	// the task class T-172 on the last attempt stamp the
	// render_failed_at marker (T-210) and publish to q:analyze; true — instead of
	// the marker the task is published to q:render-fallback (publishFallback),
	// the entry is reworked by the chromium worker B (spec §4.5). The DNS class T-211
	// and the cert class T-159 are never fallbacked under any value (FR-7).
	// Turning it off — without deploying new entities: what accumulated in the queue
	// is reworked by worker B until empty (FR-14).
	Enabled bool
	// Gate — the health gate input of the fallbackPublishDecision function
	// (FR-9a: a streak of infra errors of the browser class — "the main engine
	// is unavailable" — suspends the fallback publication until the engine
	// recovers, the overflow of the whole flow onto the expensive chromium is excluded).
	// The threshold — Threshold (render.fallback.health_gate_streak, T-233);
	// the live streak is kept by the worker (fbObserveOutcome), the decision reads
	// the snapshot (T-239).
	Gate FallbackGate
	// Budget — the share budget input of the decision function
	// (FR-9b: the share of fallback publications over the window beyond the
	// share_percent cap suspends the fallback until the end of the window — tasks
	// are finalized regularly with the marker). The cap — Ceiling
	// (render.fallback.share_percent, T-233); the worker keeps the window and
	// the share accounting (fbObserveTask/fbRecordPublish), the decision reads
	// the snapshot (T-239).
	Budget FallbackBudget
}

func (o Options) withDefaults() Options {
	if o.Concurrency <= 0 {
		o.Concurrency = DefaultConcurrency
	}
	if o.Consume <= 0 {
		o.Consume = DefaultConsume
	}
	if o.Stream == "" {
		o.Stream = DefaultStream
	}
	if o.Group == "" {
		o.Group = DefaultGroup
	}
	if o.Consumer == "" {
		o.Consumer = fmt.Sprintf("render-worker-%d", os.Getpid())
	}
	if o.TimeoutAttempts <= 0 {
		o.TimeoutAttempts = DefaultTimeoutAttempts
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = queue.DefaultMaxAttempts
	}
	// Rate cannot be negative by meaning (a consumption cap);
	// withDefaults does not substitute a default: 0 — an honest "without a cap",
	// a default would limit the main q:render build (Rate is a build
	// parameter, not a stage one).
	if o.Rate < 0 {
		o.Rate = 0
	}
	// An empty Engine — chromium, like render.Options.Engine (builds without
	// configuration): the normalization here keeps the engine guard
	// fallbackArmed (S-P1-1) and the metric labels on one vocabulary of 3 engines.
	if o.Engine == "" {
		o.Engine = config.EngineChromium
	}
	return o
}

func (o Options) log() *slog.Logger {
	if o.Log == nil {
		return slog.New(slog.DiscardHandler)
	}
	return o.Log
}

// Worker — the render worker: processes the tasks of the consumption stream
// Options.Stream (the default q:render; the subdomain_results row keys) through
// the render and storage seams. Thread-safe: everything is immutable after New except
// fbDedup — the dedup filter of fallback publications (T-238) — and the
// fallback gate/budget states fbGate/fbBudget (T-239) under the shared
// fbMu mutex (deliveries go in parallel; the metrics are synchronized).
type Worker struct {
	db       RenderDB
	q        queue.Queue
	contents ContentStore
	renderer Renderer
	opt      Options
	met      *Metrics       // stage metrics (T-077); nil — no-op
	fbDedup  *fallbackDedup // the dedup filter of fallback publications (T-238); built in New
	fbMu     sync.Mutex     // the fallback gate/budget states (T-239): the streak and the window under it
	fbGate   FallbackGate   // the health gate (T-239): a copy of Options.Fallback.Gate with the live streak
	fbBudget FallbackBudget // the share budget (T-239): a copy of Options.Fallback.Budget with the window counters
}

// New builds the worker over the DB pool, the queue (the q:analyze publication — under
// the backpressure gate in the build, T-026), the content storage and the render
// pool. nil dependencies — an error: a silent loss of a result/publication
// is invisible to the operator (the T-058 style).
func New(db RenderDB, q queue.Queue, contents ContentStore, renderer Renderer, opts Options) (*Worker, error) {
	if db == nil {
		return nil, errors.New("renderworker: the DB pool is not set")
	}
	if q == nil {
		return nil, errors.New("renderworker: the queue is not set")
	}
	if contents == nil {
		return nil, errors.New("renderworker: the content store is not set")
	}
	if renderer == nil {
		return nil, errors.New("renderworker: the render pool is not set")
	}
	return &Worker{db: db, q: q, contents: contents, renderer: renderer,
		opt: opts.withDefaults(), met: NewMetrics(opts.Metrics, opts.Engine), fbDedup: newFallbackDedup(),
		fbGate: opts.Fallback.Gate, fbBudget: opts.Fallback.Budget}, nil
}

// renderTarget — the read result row: the render target and the old
// content reference; rendered/checkedAt — the fallback filters (T-238):
// nobody publishes a rendered row to the fallback, checked_at
// identifies the row's episode (a T-058 episode rewrite — a new one).
type renderTarget struct {
	finalURL  string
	hash      []byte    // nil — no content (NULL content_hash, T-056)
	rendered  bool      // a successful render has already settled the question (T-238)
	checkedAt time.Time // the row's episode — the dedup filter key (T-238), the bucket hour (T-203)
}

// tarpitError — the timeout-tarpit marker (T-148): a render error of the
// task class whose cause is the overall page timeout render.timeout
// expiring (session.wrap: "page timeout …" → context.DeadlineExceeded in
// the chain). Such a timeout is a predictable property of a heavy page:
// a retry on a new context almost surely gives the same outcome, and every
// attempt costs a slot the full timeout (the 2026-10-23 measurement: the slots are busy
// with timeouts, the stage's effective rate is 0.34 entries/s). The wrapper does not change
// the error text (Error/Unwrap — the original's) and serves only as a label:
// processDelivery uses it to choose finalizing the entry without a render instead of
// burning the delivery attempts down to q:dead.
type tarpitError struct {
	err error
}

func (e *tarpitError) Error() string { return e.err.Error() }
func (e *tarpitError) Unwrap() error { return e.err }

// isPageTimeout — the page timeout sign: the task class (a retry on a new
// context is pointless) and context.DeadlineExceeded in the chain — that is how
// the render itself marks the render.timeout expiry (session.wrap). The
// caller context's cancellation (graceful shutdown → context.Canceled) does not
// count as a tarpit: it is an infrastructure event, the delivery will return
// with a re-delivery after the restart.
func isPageTimeout(err error) bool {
	return render.ClassOf(err) == render.ClassTask && errors.Is(err, context.DeadlineExceeded)
}

// certErrorCodes — the Chromium network codes (net/base/net_error_list.h)
// meaning a permanent TLS certificate error of the host: a property of the
// host itself, not of the network or the infrastructure. The list is closed on purpose (T-159):
// ERR_CONNECTION_REFUSED and other network codes are not included (a
// connection refusal may be temporary — the host will come up; the T-172 measurement: 6.6%
// of retries after a refusal succeed); a DNS refusal is a separate class T-211
// (dnsErrorCodes, finalized the same way from the first delivery). The time-dependent certificate code
// ERR_CERT_DATE_INVALID (expired/not yet valid) is included by the
// T-172 measurement: the share of successful retries 0 of 321 first failures per day (92%
// exhausted the attempts in q:dead) — the certificate renewal window (days) is much
// wider than the retry window (3 deliveries, ~half an hour), and the next recheck
// episode (T-030) will publish a new q:render task anyway.
// Extending the list — only with a determinism justification
// (by a measurement) and a test on the code.
var certErrorCodes = []string{
	// The certificate is not issued by a trusted certification authority: self-signed
	// or an unknown root — a certificate replacement on the host's side.
	"net::ERR_CERT_AUTHORITY_INVALID",
	// The certificate does not cover the host name (CN/SAN do not match) — a
	// host certificate configuration error.
	"net::ERR_CERT_COMMON_NAME_INVALID",
	// The certificate is expired or not yet valid. In T-159 it was excluded
	// as "time-dependent"; the T-172 measurement showed stability over the retry
	// window (0 successful of 321) — included.
	"net::ERR_CERT_DATE_INVALID",
}

// isCertError — the permanent host certificate error sign (T-159,
// T-172): the task class (a page property — that is how classifyCtx marks load
// failures of net::ERR_*, not a dead context/browser where a retry makes sense)
// and a load error code from certErrorCodes. Network codes (a
// connection refusal — recovery is observed) miss: until the
// determinism is exhausted they are retryable errors (T-172 finalizes them
// only on the last delivery).
func isCertError(err error) bool {
	if render.ClassOf(err) != render.ClassTask {
		return false
	}
	return slices.Contains(certErrorCodes, render.LoadErrorCode(err))
}

// certError — the permanent host certificate error marker (T-159): an error
// of the task class whose cause is Chromium refusing to load the page because of
// the host certificate (isCertError: a net::ERR_CERT_* code from certErrorCodes in
// the chain). The property is permanent: the certificate is not issued by a trusted CA or does not
// cover the host name — a retry on a new context (and in general until the
// host replaces the certificate itself) will give the same outcome, and every attempt would burn
// a render slot and the overall redis.max_attempts delivery limit down to q:dead (the
// §12 measurement: ~27% of render attempts are cert errors, each up to 4 deliveries; 366
// q:dead entries "attempt limit 3 exhausted"). The wrapper does not change the
// error text (Error/Unwrap — the original's) and serves only as a label:
// processDelivery finalizes the entry without a render from the very
// first delivery by it — waiting for attempts, unlike the tarpits T-148, is pointless.
type certError struct {
	err error
}

func (e *certError) Error() string { return e.err.Error() }
func (e *certError) Unwrap() error { return e.err }

// dnsErrorCodes — the Chromium network codes (net/base/net_error_list.h)
// meaning the death of the host at the DNS level. A q:render entry exists
// only through a recent live HTTP episode (the HTTP phase resolved the
// name itself — otherwise there would be no episode and no row), while the browser resolves
// with the system resolver — the same unbound that serves the DNS stage:
// a refusal at render means the host's DNS died BETWEEN the episode and
// the render. DNS recovery is an event on a scale of days (re-registration,
// delegation recovery), the retry window (3 deliveries, ~20 min in the
// PEL by claim_min_idle) does not cover it — the same determinism
// argument as for ERR_CERT_DATE_INVALID (T-172: the
// certificate renewal window is much wider than the retry window). The T-207 measurement (Oct 29):
// 252 NAME_NOT_RESOLVED finalizations per ~7 h, each ate all 3
// deliveries and ~20 min of PEL; a DNS refusal at render with a live HTTP episode
// is a rarity, the class is small. ERR_CONNECTION_REFUSED is NOT included here: a
// connection refusal is a property of the host's service (the port is closed, a filter,
// a restart), recovery is observed (the T-172 measurement: 6.6% of retries
// succeed) — it stays retryable until the last delivery (T-172).
// Extending the list — only with a determinism justification
// (by a measurement) and a test on the code, following the certErrorCodes pattern.
var dnsErrorCodes = []string{
	// The host name does not resolve at all: the zone died/the delegation is removed —
	// an event on a scale of days, the retry window does not cover it.
	"net::ERR_NAME_NOT_RESOLVED",
}

// isDNSError — the DNS refusal at render sign (T-211): the task class (a
// page load failure net::ERR_* — that is how classifyCtx marks them; a dead
// context/browser — infrastructure, a retry makes sense) and a load error
// code from dnsErrorCodes. Other network codes (a connection refusal —
// recovery is observed, the T-172 measurement: 6.6% of retries succeed) miss:
// until the determinism is exhausted they are retryable errors (T-172
// finalizes them only on the last delivery).
func isDNSError(err error) bool {
	if render.ClassOf(err) != render.ClassTask {
		return false
	}
	return slices.Contains(dnsErrorCodes, render.LoadErrorCode(err))
}

// dnsError — the DNS refusal at render marker (T-211): an error of the task class,
// whose cause is the host name not resolving (isDNSError: the
// net::ERR_NAME_NOT_RESOLVED code from dnsErrorCodes in the chain). The
// property is permanent over the retry window: the entry got into q:render through a live
// HTTP episode, the host's DNS died between the episode and the render — a delivery
// retry would burn ~20 min of PEL and 2 deliveries per every dead host
// (the T-207 measurement: 252 hosts per ~7 h). The wrapper does not change the error text
// (Error/Unwrap — the original's) and serves only as a label: processDelivery
// finalizes the entry without a render from the very first delivery by it — following
// the cert class T-159 pattern (waiting for attempts is pointless: the chance
// appears only on the next recheck episode T-030).
type dnsError struct {
	err error
}

func (e *dnsError) Error() string { return e.err.Error() }
func (e *dnsError) Unwrap() error { return e.err }

// isTaskRenderError — the task class render error sign (T-172):
// there is a *render.Error in the chain (a failure of the render itself, not of the DB/storage/
// publication — those carry no *render.Error, but render.ClassOf silently treats
// foreign errors as task, so it is the presence that is checked), the
// task class (a host property: a timeout/load error — a retry on a new context
// is pointless) and it is not the caller context's cancellation (a graceful shutdown —
// an infrastructure event, like in isPageTimeout). Such errors until
// redis.max_attempts is exhausted live the common path of re-deliveries
// (host recovery is observed), on the last one they are finalized
// without a render (processDelivery).
func isTaskRenderError(err error) bool {
	var re *render.Error
	if !errors.As(err, &re) {
		return false
	}
	return re.Class == render.ClassTask && !errors.Is(err, context.Canceled)
}

// taskFinalizeReason — the reason label of a finalization without a render on the last
// attempt (T-172, the crawler_render_task_finalized_total metric): the
// net::ERR_* load error code (structurally, render.LoadErrorCode — a closed
// Chromium vocabulary, the label's cardinality is bounded by the list itself),
// "timeout" for a timeout not caught by the tarpit limit (the regular
// case since T-261 — a mixed series: fewer tarpits than TimeoutAttempts, the
// delivery number grew to max_attempts on infra failures; the config guard
// "TimeoutAttempts ≤ max_attempts" covers only the case of zero tarpits
// on the last delivery), "other" — the rest (a page failure without a
// load code).
func taskFinalizeReason(err error) string {
	if code := render.LoadErrorCode(err); code != "" {
		return code
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "other"
}

// --- fallback publication (T-238, spec §4.4) ----------------------------------

// fallbackCandidate — a fallback candidate (FR-7, spec §4.4 items 1–2): a task
// of the finalization path without a render by an engine-branched reason — a tarpit
// T-148 (the render.timeout_attempts exhaustion) or the task class T-172 on
// the last attempt (redis.max_attempts): a property of the "site ×
// engine" pair — a page stuck on a light engine may open in
// chromium. reason — the taskFinalizeReason vocabulary (the
// crawler_render_fallback_published_total metric, spec §4.6 item 2); the
// row's episode (the dedup filter identifier) is not needed by the candidate — the caller
// holds it (the row's checked_at, fallbackDedupKey).
type fallbackCandidate struct {
	task   queue.Result
	reason string // taskFinalizeReason(cause)
}

// FallbackGate — the fallback health gate state (FR-9a, spec §4.4 item 3):
// a streak of render infra errors of the browser class (the main engine
// unavailable) reaching Threshold suspends the fallback publication until
// the engine recovers — the overflow of the whole flow onto the expensive chromium
// on a light engine's fall is excluded. The decision function reads the value
// snapshot (fallbackPublishDecision), the worker keeps the live streak
// (fbObserveOutcome under fbMu — deliveries are parallel; the
// streak logic pattern is renderFailBackoff: growth by a failure, reset
// by a success). Threshold 0 — no gate (zero builds without configuration:
// the always-publish stub behavior of T-238); cmd/render-worker fills the production
// build from render.fallback.health_gate_streak (T-233,
// the range is checked by config.Validate).
type FallbackGate struct {
	// Threshold — the ClassBrowser infra error streak threshold
	// (render.fallback.health_gate_streak, T-233).
	Threshold int
	// streak — the ClassBrowser infra errors since the last successful
	// render (ProcessResult processed the entry entirely — the DOM written and
	// q:analyze published, T-225); a success resets it (the engine showed
	// a sign of life — the renderFailBackoff pattern: reset by a success,
	// growth by failures), the task/context class errors and regular
	// rejections without a render (no row / it slipped out from under the task) do not touch it
	// — there was no render, no sign of the engine's life (the T-239 review, P1).
	streak int
}

// suspended — the gate is closed: the infra error streak reached the threshold, the engine
// is considered unhealthy, the fallback publication is suspended until
// recovery (a successful render will reset the streak).
func (g FallbackGate) suspended() bool {
	return g.Threshold > 0 && g.streak >= g.Threshold
}

// fallbackBudgetWindow — the window of the fallback share budget (FR-9b, spec §4.4
// item 4): the stage's acknowledged deliveries in one window. Windows follow
// one after another without overlap: a filled window is closed by the share estimate
// (publications/deliveries beyond the share_percent cap suspend
// the next window's publications — "until the end of the window", FR-9b), the counters
// are reset; a window with zero publications (a suspended one) does not pass
// the estimate and the fallback resumes — the suspension is self-resetting,
// without manual intervention. The size is code, not configuration:
// the cap is configurable (share_percent, T-233), the window is a measure of the
// stage's flow; 1000 ≈ 12 min of flow (the Oct 24 measurement: ≈5.1 thousand tasks/h) —
// enough share statistics (less — the live flow's noise would flip the
// suspension), a tolerable reaction delay of the storm breaker
// (fast cases the health gate closes by the streak within seconds).
const fallbackBudgetWindow = 1000

// FallbackBudget — the fallback share budget state (FR-9b, spec §4.4
// item 4): the share of publications over the fallbackBudgetWindow window beyond the
// Ceiling cap suspends the fallback until the end of the next window — tasks
// are finalized with the marker as today (FR-9b), the metric event
// is raised (the alert — spec §4.6 item 3). The decision function reads the value snapshot,
// the worker keeps the window counters (fbObserveTask/fbRecordPublish under fbMu).
// Ceiling 0 — without a cap (zero builds, the "0 — without a
// cap" configuration, T-233); cmd/render-worker fills the production
// build from render.fallback.share_percent.
type FallbackBudget struct {
	// Ceiling — the publications share cap, % of the window's
	// acknowledged deliveries (render.fallback.share_percent, T-233); 0 — without a cap.
	Ceiling float64
	// tasks — the deliveries in the current window that ended with processing
	// without an error (the share's denominator; the stage's flow — the same units as
	// crawler_stage_tasks_total: the fallback rate = publications/flow,
	// FR-15; the accounting is at the handler's exit, before the XACK at the queue level:
	// an Ack failure or a panic give an extra +1, a re-delivery
	// will be counted again — a tolerance of a few units, not changing the share's order;
	// the tolerance at the window boundary is ≤ Concurrency publications, the T-239 review).
	tasks int
	// published — the publications to q:render-fallback in the current window
	// (the share's numerator; only the XADDs that happened are counted — decision
	// rejections and delivery duplicates do not spend the budget).
	published int
	// hold — the window is suspended: the previous window overspent
	// the cap, the publications are off until this window's boundary (the counter
	// reset at closing returns the fallback).
	hold bool
}

// suspended — the budget is suspended: the previous window went beyond the
// share cap, the current window's publications are off ("until the end of the window", FR-9b).
func (b FallbackBudget) suspended() bool {
	return b.Ceiling > 0 && b.hold
}

// overCeiling — the window overspent the cap: the publications share is above
// Ceiling percent (by multiplication, without division — precision at the boundary).
func (b FallbackBudget) overCeiling() bool {
	return b.Ceiling > 0 && float64(b.published)*100 > float64(b.tasks)*b.Ceiling
}

// fallbackPublishDecision — the pure decision function about publishing
// a candidate to q:render-fallback (spec §4.4 items 3–4, FR-9): candidate ×
// health gate × share budget. A rejection of any input suspends the
// publication — the caller finalizes the task as today (the
// render_failed_at marker + q:analyze, spec §4.4 items 3–4), without losses:
//
//   - the gate (FR-9a, T-239): the ClassBrowser infra error streak reached
//     the threshold — the main engine is unhealthy, the flow overflow onto the expensive
//     chromium is forbidden until recovery (the browser class itself lives in
//     the q:render redelivery — not covered by a finalization);
//   - the budget (FR-9b, T-239): the share of publications over the window went beyond the
//     share_percent cap — the window is suspended until its boundary.
//
// Pure and stateless on purpose: the streak and window state is kept by
// the worker (fbObserveOutcome/fbObserveTask/fbRecordPublish under fbMu),
// the function folds the state snapshots with the candidate — tested by a table
// without an environment. The candidate does not affect the decision: engine-branched
// reasons (the tarpit T-148, the task class T-172) are symmetric — in
// the signature by the T-238 contract "candidate × gate × budget".
func fallbackPublishDecision(cand fallbackCandidate, gate FallbackGate, budget FallbackBudget) bool {
	if gate.suspended() {
		return false
	}
	if budget.suspended() {
		return false
	}
	return true
}

// fallbackDedupCap — the capacity of the fallback publication dedup filter: 32,768 —
// with a margin above the MAXLEN cap of q:render-fallback (15,000, T-237):
// the dedup window of interest — entries worker B has not consumed yet
// (the stream depth ≤ the cap), everything older is either consumed or trimmed
// by the trim — a duplicate there is extra work of consumer B, not a loss (FR-11).
// Without a capacity the filter would grow without a limit (tens of thousands of keys per day).
const fallbackDedupCap = 1 << 15

// fallbackDedup — the dedup filter of publications to q:render-fallback by
// the result row identifier (spec §4.4 item 5): a re-delivery
// of the same row (an XACK failure after a successful publication; a concurrent
// duplicate of the episode's message — the sources are listed in T-210) does not give
// a second entry in the stream. The pattern — the parking dedup (the render_pending bit
// T-066/T-175: the "published" transition is recorded exactly once —
// an UPDATE guard, — a repeated execution sees "already done" and
// skips the side effect). The identifier — the row key +
// the episode's checked_at (fallbackDedupKey): checked_at is stable over the
// episode's life (re-deliveries read the same one) and changes on an episode
// rewrite (the recheck T-058) — a new episode legitimately goes through
// the fallback again; the content's content_hash does NOT
// work for identifying the episode: the hash is content-addressed (the T-056 dedup), and a new episode with
// byte-identical content would match the old one — the filter would falsely suppress
// the fresh episode's fallback (the T-238 review, P1).
//
// Process memory, not Redis/DB: the fallback state lives in the queue and
// the metrics (spec §4.8 — the DB schema without new columns/tables), and the dedup
// must survive only the re-delivery window of that row
// (seconds–minutes of PEL), not worker restarts — a duplicate after a restart
// or an eviction from the capacity is allowed and is suppressed by consumer B (FR-11).
// Thread-safe: deliveries are processed in parallel.
type fallbackDedup struct {
	mu    sync.Mutex
	seen  map[string]struct{}
	order []string // keys in insertion order — eviction of the oldest (FIFO)
}

// newFallbackDedup builds an empty filter.
func newFallbackDedup() *fallbackDedup {
	return &fallbackDedup{seen: make(map[string]struct{})}
}

// claim records the identifier as published: true — the first
// record (the publication may proceed), false — already recorded
// (a duplicate: a second entry in the stream is not needed). Beyond the capacity
// the oldest key is evicted (its duplicate becomes allowed — FR-11).
func (d *fallbackDedup) claim(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.seen[key]; ok {
		return false
	}
	if len(d.order) >= fallbackDedupCap {
		// Eviction of the oldest: order — the insertion queue, seen holds
		// exactly the keys of order (release removes from both).
		delete(d.seen, d.order[0])
		d.order = d.order[1:]
	}
	d.seen[key] = struct{}{}
	d.order = append(d.order, key)
	return true
}

// known — whether the identifier was already recorded (read-only, without recording):
// a re-delivery of an already published row learns about it BEFORE
// the gate/budget decision — a suspension does not cancel a publication that
// happened, the duplicate is acknowledged without a marker and without q:analyze (FR-11;
// the T-239 review, P3).
func (d *fallbackDedup) known(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.seen[key]
	return ok
}

// release removes the identifier's record — the publication over it did NOT
// happen (an XADD failure): a re-delivery must keep the right to
// a new attempt, otherwise the task would be lost between the filter and the stream
// (a claim without a publication would close the row's fallback forever until the
// episode changes). The pair of the claim of the same key; an unknown key — a no-op
// (call symmetry on errors before the claim).
func (d *fallbackDedup) release(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.seen[key]; !ok {
		return
	}
	delete(d.seen, key)
	for i, k := range d.order {
		if k == key {
			d.order = append(d.order[:i], d.order[i+1:]...)
			break
		}
	}
}

// fallbackDedupKey — the result row identifier for the dedup filter:
// the subdomain_results row key + the episode's checked_at (UnixNano —
// a compact printing of timestamptz; the episode = the row's record T-058, all
// of the episode's deliveries read the same checked_at, an episode rewrite
// writes a new one).
func fallbackDedupKey(task queue.Result, checkedAt time.Time) string {
	return fmt.Sprintf("%d/%d/%d/%d/%d",
		task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant), checkedAt.UnixNano())
}

// --- gate and budget states (T-239, spec §4.4 items 3–4, FR-9) ----------------

// fallbackArmed — the build publishes the fallback: render.fallback
// .enabled is on, the consumption stream is q:render (the same conditions as the
// publishFallback guard, FR-10/FR-12 — worker B does not publish the fallback) and the
// build's engine is not chromium (S-P1-1, spec §6.2 item 2/§6.4 item 1, FR-14):
// publications go only from the candidate engine — chromium does not
// fallback itself (acceptance step 1: enabling the fallback without changing the engine
// leaves q:render-fallback empty), on a rollback to chromium
// the publication goes dark by itself, without editing the flag configuration.
// The state observations and suspension events go only in the armed
// build: the down engine's metrics (worker B, chromium) must not
// raise the primary's fallback alerts.
func (w *Worker) fallbackArmed() bool {
	return w.opt.Fallback.Enabled &&
		w.opt.Stream == queue.StreamRender &&
		w.opt.Engine != config.EngineChromium
}

// fallbackStates — the gate and budget state snapshots for the decision function
// (fallbackPublishDecision reads the values; the live counters — under fbMu,
// the deliveries are parallel).
func (w *Worker) fallbackStates() (FallbackGate, FallbackBudget) {
	w.fbMu.Lock()
	defer w.fbMu.Unlock()
	return w.fbGate, w.fbBudget
}

// fbObserveOutcome — the delivery outcome for the health gate (T-239, FR-9a):
// a render error of the browser class (the main engine unavailable) grows the
// streak, a successful render (rendered — ProcessResult processed the entry
// entirely: the DOM written and q:analyze published, T-225) resets it — the engine
// showed a sign of life (the renderFailBackoff pattern: growth by failures,
// reset by a success); the task/context class errors and regular
// rejections without a render (no row / it slipped out from under the task — rendered=false) do not touch the
// streak — there was no render, no sign of the engine's life (the T-239 review,
// P1: a no-op between browser errors would open the gate for a down engine).
// Reaching the threshold — the "open → closed" transition — the suspension event
// crawler_render_fallback_suspended_total{gate} (spec §4.6 item 2) and a Warn;
// while the gate is closed, fallback candidates are finalized
// as today (the marker), the browser class itself waits in the
// q:render redelivery — not covered by a finalization, tasks without losses.
func (w *Worker) fbObserveOutcome(err error, rendered bool) {
	if !w.fallbackArmed() {
		return
	}
	w.fbMu.Lock()
	defer w.fbMu.Unlock()
	switch {
	case err == nil && rendered:
		w.fbGate.streak = 0
	case render.ClassOf(err) == render.ClassBrowser:
		w.fbGate.streak++
		if w.fbGate.Threshold > 0 && w.fbGate.streak == w.fbGate.Threshold {
			w.met.FallbackSuspended(fbReasonGate)
			w.opt.log().Warn("fallback suspended: the health gate — the browser-class infra error streak (the primary engine unavailable)",
				"streak", w.fbGate.streak, "threshold", w.fbGate.Threshold)
		}
	}
}

// fbObserveTask — a delivery that ended with processing without an error entered
// the stage's flow (T-239, FR-9b): the share budget window counts it
// in the denominator (the share = publications/flow — the same flow as
// crawler_stage_tasks_total; re-deliveries are counted once —
// at the successful end of processing). A filled window is closed
// by the estimate: an overspend beyond the cap suspends the next window's
// publications (the crawler_render_fallback_suspended_total
// {budget} event and a Warn — spec §4.6 item 3), the counters are reset —
// a suspended window does not accumulate publications, the next estimate
// returns the fallback ("until the end of the window", self-resetting without manual
// intervention).
func (w *Worker) fbObserveTask() {
	if !w.fallbackArmed() {
		return
	}
	w.fbMu.Lock()
	defer w.fbMu.Unlock()
	w.fbBudget.tasks++
	if w.fbBudget.tasks >= fallbackBudgetWindow {
		over := w.fbBudget.overCeiling()
		if over {
			w.met.FallbackSuspended(fbReasonBudget)
			w.opt.log().Warn("fallback suspended: the share budget — the window exceeded the cap",
				"published", w.fbBudget.published, "window", w.fbBudget.tasks,
				"ceiling_percent", w.fbBudget.Ceiling)
		}
		w.fbBudget.hold = over
		w.fbBudget.tasks, w.fbBudget.published = 0, 0
	}
}

// fbRecordPublish — the publication to q:render-fallback happened (T-239):
// the budget share's numerator. Called after a successful XADD: decision
// rejections (the gate/budget) and delivery duplicates do not spend the budget — the
// share is counted over the entries that actually went to the queue.
func (w *Worker) fbRecordPublish() {
	w.fbMu.Lock()
	defer w.fbMu.Unlock()
	w.fbBudget.published++
}

// fbConsuming — the build consumes the fallback queue: the consumption stream is
// q:render-fallback (worker B, FR-12, T-236; Options.Stream). Only the B build keeps the
// consumption accounting — the counter series of
// crawler_render_fallback_consumed_total are pre-created with zeros in all
// builds (S-P1-4), only B's grow: the fallback consumption is the rework by
// worker B, not a stage of the primary worker (the streams are mutually exclusive with
// fallbackArmed — each build counts its own side of publications and consumption).
func (w *Worker) fbConsuming() bool {
	return w.opt.Stream == queue.StreamRenderFallback
}

// fbConsume — the accounting of a fallback delivery outcome that ended with processing
// without an error (T-245, spec §4.6 item 2, FR-15): +1 into
// crawler_render_fallback_consumed_total{result} — the denominator of the
// published balance (worker A, T-238) = consumed (worker B) + the
// q:render-fallback remainder (the B build's crawler_queue_depth — the lag + the PEL) +
// the buried ones (crawler_queue_dead_buried_total{queue="q:render-fallback"}
// — the browser/infrastructure class errors on the exhaustion of
// redis.max_attempts) + the unidentifiable ones
// (crawler_queue_dead_unidentified_total{queue="q:render-fallback"},
// T-260 — entries without a task_id, steadily zero; acceptance step 4
// of spec §6.2). The count is in the handler, before the
// XACK at the queue level: an Ack failure or a re-delivery give a duplicate — a tolerance of a few
// at-least-once units (like the T-239 budget window); failed
// deliveries are not counted at all — they stay in the PEL until a claim return or a
// bury in q:dead (the third member of the balance).
func (w *Worker) fbConsume(result string) {
	if w.fbConsuming() {
		w.met.FallbackConsumed(result)
	}
}

// ProcessResult processes one q:render task (§5.6, §7): rendering
// the row's final URL, the DOM into the storage, an atomic reference replacement and
// rendered = true, a q:analyze publication after the commit. Returns false
// without an error when there is nothing to do for the task: no row, no final
// URL or it slipped out from under the task (the recheck rewrote it — the
// UPDATE guard) — that is a success, no publication needed, the delivery is acknowledged.
// true is returned only together with a nil error — the entry is processed
// entirely (the DOM written and q:analyze published, T-225); every failing
// path — false, an error. An error — a failure of any step (including the publication
// after the committed transaction): the q:render delivery is not acknowledged
// and will return with a re-delivery (Processing, T-025); repeated processing
// is idempotent (the rewrite, see the package comment).
func (w *Worker) ProcessResult(ctx context.Context, task queue.Result) (bool, error) {
	target, found, err := w.selectTarget(ctx, task)
	if err != nil {
		return false, err
	}
	if !found || target.finalURL == "" {
		// No row (the domain deleted/rechecked without this key) or
		// no final URL — nothing to render; a regular rejection without an error.
		w.opt.log().Debug("render not needed — no row or no final URL",
			"zone_id", task.ZoneID, "domain_id", task.DomainID,
			"subdomain_id", task.SubdomainID, "variant", int16(task.Variant))
		return false, nil
	}
	name := renderLogName(task, target.finalURL)

	out, err := w.renderer.Run(ctx, render.RenderTask{URL: target.finalURL})
	if err != nil {
		// A page (timeout, net::ERR_*) or browser error — a re-delivery
		// at the queue level (the task class) / a wait or a future
		// restart (the browser class, T-065); there are no partial results per spec §8
		// (T-063). A permanent host certificate error
		// is marked certError (T-159), a DNS refusal — dnsError (T-211),
		// a page timeout — tarpitError (T-148): processDelivery decides by the
		// markers — finalize the entry without a render or
		// retry with a delivery.
		if isCertError(err) {
			return false, fmt.Errorf("renderworker: %s: render: %w", name, &certError{err: err})
		}
		if isDNSError(err) {
			return false, fmt.Errorf("renderworker: %s: render: %w", name, &dnsError{err: err})
		}
		if isPageTimeout(err) {
			return false, fmt.Errorf("renderworker: %s: render: %w", name, &tarpitError{err: err})
		}
		return false, fmt.Errorf("renderworker: %s: render: %w", name, err)
	}
	if out.DOM == "" || out.FinalURL == "" {
		// The T-064 contract guard: a live page always has
		// documentElement and location; empty values are an abnormal
		// result, the row is not touched.
		return false, fmt.Errorf("renderworker: %s: an empty render result (DOM %d bytes, final_url %q)",
			name, len(out.DOM), out.FinalURL)
	}
	// The DOM — a single copy of the bytes (R-02): the hash and Put from one slice, the
	// final DOM itself is already bounded by the render (render.Options.MaxDOMBytes);
	// multiple full copies of the row are not needed.
	dom := []byte(out.DOM)
	newHash := content.HashOf(dom)

	var oldHash content.Hash
	hasOld := len(target.hash) == content.HashLen
	if hasOld {
		copy(oldHash[:], target.hash)
	}

	// Put before the transaction (the T-056 → T-058 order: the new reference must
	// exist before the swap, SwapRefTx requires a live newHash). The content did not
	// change (the DOM is byte-identical to the stored one) — Put is skipped:
	// the row already has the reference, an extra +1 is not needed; the UPDATE guard below
	// compares against the same hash and passes.
	// Errors: ErrStoreFull — temporary (a delivery retry will wait for room),
	// ErrTooLarge — a permanent page property (the DOM is bloated by JS): the delivery
	// attempts will be exhausted and the task will go to q:dead for manual
	// triage (T-025) — a signal to the operator to raise content.max_store_bytes.
	if !hasOld || newHash != oldHash {
		if _, err := w.contents.Put(ctx, dom); err != nil {
			return false, fmt.Errorf("renderworker: %s: DOM saving: %w", name, err)
		}
	}
	cookies, err := renderCookiesJSON(out.Cookies)
	if err != nil {
		return false, fmt.Errorf("renderworker: %s: cookies: %w", name, err)
	}
	headers, err := renderHeadersJSON(out.ResponseHeaders)
	if err != nil {
		return false, fmt.Errorf("renderworker: %s: headers: %w", name, err)
	}

	tx, err := w.db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("renderworker: %s: content replacement: transaction: %w", name, err)
	}
	// Rollback unconditionally: after Commit — a no-op (the T-044/T-058 style).
	defer func() { _ = tx.Rollback(ctx) }()

	// The content replacement: the UPDATE with the optimistic guard returns
	// the checked_at of the moved row (T-203 — the hour of the rendered-rate bucket);
	// an empty result (pgx.ErrNoRows) — the guard cut it off, the row is not ours
	// (the former sign — RowsAffected 0).
	var checkedAt time.Time
	err = tx.QueryRow(ctx, renderUpdateSQL,
		task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant),
		out.FinalURL, newHash[:], cookies, headers, target.hash).Scan(&checkedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// The optimistic guard: the row's content changed after the read
			// (the domain recheck rewrote the result T-058, a concurrent
			// delivery has already written its DOM) — the row is not ours, the transaction
			// is rolled back by the deferred Rollback. The Put reference is removed: without
			// a row it would hang as a space leak (the content lives while there are
			// references; Delete removes exactly the one Put added). A removal error is
			// not a task error: a Warn, the delivery is acknowledged.
			_ = tx.Rollback(ctx)
			if !hasOld || newHash != oldHash {
				if err := w.contents.Delete(ctx, newHash); err != nil && !errors.Is(err, content.ErrNotFound) {
					w.opt.log().Warn("render: the content reference was not released (the row slipped out from under the task)",
						"domain", name, "hash", newHash.String(), "err", err)
				}
			}
			w.opt.log().Debug("render not written — the row was rewritten by another episode",
				"domain", name, "zone_id", task.ZoneID, "domain_id", task.DomainID,
				"subdomain_id", task.SubdomainID, "variant", int16(task.Variant))
			return false, nil
		}
		return false, fmt.Errorf("renderworker: %s: content replacement: %w", name, err)
	}

	// −1 to the old content (spec 5.7 "on a result rewrite the reference counter
	// of the old content decreases"): SwapRefTx(old, zero) —
	// the "only −to-the-old" primitive, the new one already has +1 from Put (T-058).
	// Alias rows (a NULL hash, T-055) fall into hasOld=false — nothing
	// to remove; an alias's DOM is its own reference.
	if hasOld && newHash != oldHash {
		if err := content.SwapRefTx(ctx, tx, oldHash, content.Hash{}); err != nil {
			return false, fmt.Errorf("renderworker: %s: %w", name, err)
		}
	}
	// The reanalysis tails bucket (T-204) — before the rendered-rate bucket
	// (the reconciliation order "reanalyze: → rendered:", the
	// renderBumpReanalyzeSQL comment): the replaced content returned the row to
	// unanalyzed before the repeated publication stamp; the hour — the checked_at
	// from RETURNING UPDATE above (the episode's hour).
	if err := renderBumpReanalyzeCounter(ctx, tx, checkedAt); err != nil {
		return false, fmt.Errorf("renderworker: %s: %w", name, err)
	}
	// The rendered-rate bucket (T-203) — the transaction's last step (the same
	// commit: a rollback rolls back the flag and the delta together, §3.4; the
	// lock window of the counter row — minimal, the style of the tail deltas
	// of record.go). The delta is 1 (one row moved), the hour — the checked_at
	// from RETURNING UPDATE above.
	if err := renderBumpRenderedCounter(ctx, tx, checkedAt); err != nil {
		return false, fmt.Errorf("renderworker: %s: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		// The rollback will return the row; the +1 from Put will remain a space leak —
		// the safe side (T-044), a re-delivery will rewrite it.
		return false, fmt.Errorf("renderworker: %s: content replacement: commit: %w", name, err)
	}

	// The q:analyze publication after the write (§7): the Result points at the row —
	// it must exist by the moment the analyzer consumes it in the new
	// content. A publication failure → the q:render delivery without an XACK: a
	// re-delivery will rewrite the result idempotently and publish again
	// (at-least-once, spec 3.4; the duplicates are bounded by the delivery attempts).
	// Fresh: the render reworks the current HTTP episode's entry (T-066),
	// not a re-analysis — the detection history is written (T-118).
	task.Fresh = true
	if _, err := w.q.Publish(ctx, queue.StreamAnalyze, task); err != nil {
		// The row is already committed (the Commit above), but the processing is not finished:
		// the publication is part of the task, hence false, like on all failing
		// paths (the contract "true = the entry processed entirely and err == nil",
		// T-225); the delivery will return and publish again. The fact of the written
		// DOM — by a Warn log (the Rendered metric counts only fully
		// finished tasks, a failure will go into the stage error counter in
		// processDelivery).
		w.opt.log().Warn("DOM written, the q:analyze publication failed — a re-delivery will publish again",
			"domain", name, "final_url", out.FinalURL, "err", err)
		return false, fmt.Errorf("renderworker: %s: q:analyze publication: %w", name, err)
	}
	w.opt.log().Debug("DOM written",
		"domain", name, "final_url", out.FinalURL, "dom_bytes", len(out.DOM),
		"cookies", len(out.Cookies))
	return true, nil
}

// renderBumpReanalyzeCounter — an increment of the unanalyzed
// tails bucket in the OPEN transaction of the content replacement (T-204): checkedAt — the
// bucket hour (RETURNING renderUpdateSQL), delta — 1 (one row returned
// to unanalyzed by the content replacement). The upsert creates the hour bucket itself
// (or resurrects an outdated one — a render lateness, the
// renderBumpReanalyzeSQL comment) — RowsAffected is guaranteed 1, the check is the
// guard for direct calls with foreign SQL (a silent loss of a delta
// would understate the tick guard's window). A return with an error rolls back
// the caller's transaction — the content replacement and the bucket change only
// together (§3.4).
func renderBumpReanalyzeCounter(ctx context.Context, tx pgx.Tx, checkedAt time.Time) error {
	tag, err := tx.Exec(ctx, renderBumpReanalyzeSQL, checkedAt, int64(1))
	if err != nil {
		return fmt.Errorf("reanalysis tails bucket: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("reanalysis tails bucket: %d rows affected (the hour bucket upsert must affect 1)",
			tag.RowsAffected())
	}
	return nil
}

// renderBumpRenderedCounter — an increment of the rendered-rate bucket in
// the OPEN transaction of the content replacement (T-203): checkedAt — the bucket
// hour (RETURNING renderUpdateSQL), delta — 1 (one row moved to
// rendered). The upsert creates the hour bucket itself (the first rendered episode
// of the hour); RowsAffected is guaranteed 1, the check is the guard for direct
// calls with foreign SQL (a silent loss of a delta would understate the rate).
// A return with an error rolls back the caller's transaction — the flag and the bucket
// change only together (§3.4).
func renderBumpRenderedCounter(ctx context.Context, tx pgx.Tx, checkedAt time.Time) error {
	tag, err := tx.Exec(ctx, renderBumpRenderedSQL, checkedAt, int64(1))
	if err != nil {
		return fmt.Errorf("rendered rate bucket: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("rendered rate bucket: %d rows affected (the hour bucket upsert must affect 1)",
			tag.RowsAffected())
	}
	return nil
}

// selectTarget reads the render target by the task key. The SELECT is outside a transaction:
// a concurrent row rewrite between the read and the UPDATE is caught by the
// renderUpdateSQL guard, a re-read gives a retry with an actual
// URL (the delivery idempotency).
func (w *Worker) selectTarget(ctx context.Context, task queue.Result) (renderTarget, bool, error) {
	rows, err := w.db.Query(ctx, renderSelectSQL,
		task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant))
	if err != nil {
		return renderTarget{}, false, fmt.Errorf("renderworker: result row read: %w", err)
	}
	defer rows.Close()

	var t renderTarget
	if rows.Next() {
		var finalURL *string
		if err := rows.Scan(&finalURL, &t.hash, &t.rendered, &t.checkedAt); err != nil {
			return renderTarget{}, false, fmt.Errorf("renderworker: result row read: %w", err)
		}
		if finalURL != nil {
			t.finalURL = *finalURL
		}
		return t, true, nil // the PK — no more than one row
	}
	if err := rows.Err(); err != nil {
		return renderTarget{}, false, fmt.Errorf("renderworker: result row read: %w", err)
	}
	return renderTarget{}, false, nil
}

// renderLogName — the name for logs: the domain name from the message, otherwise the
// row key with the target URL (Name is optional, T-024).
func renderLogName(task queue.Result, finalURL string) string {
	if task.Name != "" {
		return task.Name
	}
	return fmt.Sprintf("%d/%d/%d/%d → %s", task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant), finalURL)
}

// renderCookiesJSON serializes the render cookies into jsonb (the single
// subdomain_results.cookies contract: ExtractCookie repeats the HTTP stage's record,
// T-064/T-058); an empty set — NULL.
func renderCookiesJSON(cookies []render.ExtractCookie) (*string, error) {
	if len(cookies) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(cookies)
	if err != nil {
		return nil, err
	}
	s := string(b)
	return &s, nil
}

// renderHeadersJSON serializes the final navigation's headers into jsonb
// (canonical http.Header names, like T-058's); an empty set — NULL.
func renderHeadersJSON(h http.Header) (*string, error) {
	if len(h) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(h)
	if err != nil {
		return nil, err
	}
	s := string(b)
	return &s, nil
}

// --- worker loop --------------------------------------------------------------

// renderFailWaitBase — the starting pause of the worker loop's fuse
// (Run): deliveries left without an XACK in a row in the amount of a whole read
// chunk (Consume) on a systematic processing failure — render/DB/publication —
// suspend the q:render reading with the pause doubling up to DefaultErrorWait
// (the pattern — workerFailWaitBase of httpworker, T-139). Without the pause the loop
// would keep drawing tasks: every delivery would burn an attempt (XAUTOCLAIM
// after DefaultClaimMinIdle, T-025) and take it to q:dead. A variable, not
// a constant: unit tests shrink the pause without waiting out seconds.
var renderFailWaitBase = time.Second

// renderFailBackoff — the pause after streak fuse pauses endured in a row
// (the series grows until the reader sees not a single
// success — the failure counter is reset by an acknowledged delivery):
// doubling from renderFailWaitBase, the DefaultErrorWait cap (the shift is
// bounded — a Duration overflow). streak < 1 is clamped to 1 —
// a negative Duration shift panics (the T-142 review).
func renderFailBackoff(streak int) time.Duration {
	if streak < 1 {
		streak = 1
	}
	d := renderFailWaitBase << min(streak-1, 5)
	if d > DefaultErrorWait {
		d = DefaultErrorWait
	}
	return d
}

// Run — the render-worker loop (spec 4.2, §5.6, §7): reads the tasks of the
// Options.Stream stream (the default q:render; the fallback worker — FR-12 —
// is built with the q:render-fallback stream) with a consumer group in chunks of
// Consume and processes the deliveries
// in parallel up to Concurrency, each — with the Processing pattern (T-025):
// render → write to the DB → q:analyze publication → XACK. The consumption is
// sliding (T-142, the T-139 pattern of httpworker): the next read
// happens when delivery slots free up (the read runs ahead of
// processing by no more than a couple of Consume chunks), the loop does not wait for the whole
// chunk to finish — a window barrier on the duration tail (pages up to
// render.timeout, 20–25 s) would keep the slots idle and cut the
// stage's ceiling over the Chromium bottleneck (the T-092 §4 measurement: 5.1 s/page).
// The browser's liveness (Options.Alive) is checked before every read:
// an unavailable Chromium — a consumption pause, the deliveries stay in the stream
// unread (not in the PEL), the browser restart is T-065. It runs until
// ctx is cancelled and returns nil — the OnRun contract of the framework (T-012; the started
// tasks finish or return to the queue with a re-delivery,
// spec 3.4). A read or processing error does not stop the loop:
// it is logged (Warn), the unacknowledged delivery will return by XAUTOCLAIM
// (spec 4.2); Consume deliveries in a row without a single acknowledgment —
// a systematic processing failure — endures a fuse pause
// (renderFailWaitBase): drawing new tasks while the processing is down is
// pointless, the delivery attempts burn. The Options.Rate rate cap
// (T-240, FR-13) — before every read: the chunk is trimmed to the free slots of the
// sliding window (RateWindow), an exhausted window holds a pause until
// the oldest entry leaves; an empty read does not spend the cap (the queue
// is empty — the worker idles in the XREADGROUP block with a full budget).
// An infrastructure publication failure (InfraError: a Redis OOM/connection,
// T-152) stops the reading immediately and until the write recovers
// (Reliab.Writable). The reader modes (reading | browser pause |
// infra pause | fuse pause | rate pause) are formalized
// by the readerstate.go FSM (T-262, TZ-fsm §3 FR-8): one function over the observations
// chooses the pass's mode, the pause priority order —
// the transition table there.
func Run(ctx context.Context, w *Worker, opts Options) error {
	if w == nil {
		return errors.New("renderworker: the worker loop: the worker is not set")
	}
	o := opts.withDefaults()
	log := o.log()
	log.Info("render-worker loop",
		"stream", o.Stream, "group", o.Group, "consumer", o.Consumer,
		"concurrency", o.Concurrency, "consume", o.Consume, "alive", o.Alive != nil)

	wait := func(d time.Duration) {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
		case <-t.C:
		}
	}

	// The return of stuck deliveries (T-025): a background RecoverLoop on behalf of
	// the same consumer — an XAUTOCLAIM claim and the same task processing.
	// RecoverLoopAttempt (T-148): the handler receives the delivery number
	// (Claimed.Attempts) for the timeout policy. Lives until ctx is cancelled;
	// Run waits for its exit (a started task finishes or stays
	// unacknowledged, spec 3.4).
	if o.Recover != nil {
		recDone := make(chan struct{})
		go func() {
			defer close(recDone)
			_ = queue.RecoverLoopAttempt(ctx, o.Recover, w.processDelivery, queue.RecoverOptions{
				Stream:   o.Stream,
				Group:    o.Group,
				Consumer: o.Consumer,
				// The PEL traversal rate — the queue's, not the stage's (redis.recover_*,
				// T-153); zeros — the queue defaults.
				Batch:    o.RecoverBatch,
				Interval: o.RecoverInterval,
				Log:      log,
			})
		}()
		defer func() { <-recDone }()
	}

	sem := make(chan struct{}, o.Concurrency)
	// The sliding consumption (T-142, the T-139 pattern): the reader keeps
	// ahead of the processing no more than a couple of Consume chunks of deliveries (the current
	// chunk in hand + the pending buffer), the dispatcher below occupies a
	// semaphore slot for one delivery and immediately takes the next —
	// a task's completion frees a slot and moves the read, nobody
	// waits for a whole chunk. The read but not started deliveries (the buffer
	// is full, the slots are busy with the duration tail) stay without an XACK on ctx
	// cancellation and return to the queue by an XAUTOCLAIM claim — just like the not
	// started deliveries of the former window (at-least-once, spec 3.4).
	//
	// The window of deliveries held by the live worker (read and not
	// acknowledged) — up to 2×Consume + Concurrency ≈ 40 entries: it
	// must fit into redis.claim_min_idle with a margin (otherwise
	// the claim intercepts live work); at the default contexts=8
	// (Concurrency 8) and render.timeout=25 s the tail waits ≤ ~2.1 min
	// against the 10 min threshold, at Concurrency 1 the window grows to ~13.8 min —
	// raise claim_min_idle together with lowering the parallelism
	// (the T-142 review; the numbers are recalculated for the default 8, T-169).
	pending := make(chan queue.Delivery, o.Consume)
	var failed atomic.Int64 // deliveries in a row without an acknowledgment
	// rateLim — the sliding consumption rate window (T-240, FR-13):
	// Options.Rate > 0 — the read is limited by the cap of entries per window
	// (worker B's "collect and rework" batch mode, render.fallback.
	// worker_rate); nil — no cap, the read as before. Only
	// the reader below keeps it — without locks (the reader's local counters).
	var rateLim *rateWindow
	if o.Rate > 0 {
		rateLim = newRateWindow(o.Rate, renderRateWindow)
		log.Info("the consumption rate cap",
			"stream", o.Stream, "rate", o.Rate, "window", renderRateWindow.String())
	}
	// infraDown — a sustained infrastructure publication error
	// (a Redis OOM/connection, T-152): the q:render reading is suspended
	// until the write recovers (Reliab.Writable) — the publications fail
	// anyway, new deliveries would only pile up in pending. The
	// infra observation of the reader mode FSM (readerstate.go, T-262): the
	// flag is set by the dispatcher below (queue.IsInfra), extinguished by a write probe or
	// a processing success.
	var infraDown atomic.Bool
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		failStreak := 0  // fuse pauses endured in a row
		infraStreak := 0 // infra pauses endured in a row
		for {
			// The pass's observations (FR-8, readerstate.go): the sensors
			// are polled only when mounted (Alive — the
			// browser probe; Writable — a write probe with the flag
			// lit, T-152, one SET; allowance — the T-240 rate window), the pass's
			// mode is chosen by readerDecide — the pause priority order
			// lives in the transition table, not in the if order (I4).
			obs := readerObs{
				browser: o.Alive != nil,
				infra:   infraDown.Load(),
				rate:    rateLim != nil,
				failed:  failed.Load(),
				batch:   o.Consume,
			}
			if obs.browser {
				obs.alive = o.Alive()
			}
			if obs.infra {
				// A write probe — only with the flag lit (T-152):
				// after the write recovers the reading resumes
				// immediately. There is no Recover sensor — the flag is extinguished
				// by the resumption below (a processing success will remove it anyway).
				obs.writable = o.Recover == nil || o.Recover.Writable(ctx)
			}
			if obs.rate {
				obs.allowance = rateLim.allowance(time.Now())
			}
			switch readerDecide(obs) {
			case modePauseBrowser:
				// The browser's liveness — before every read: a dead
				// Chromium accepts no tasks, new reads
				// stop — the unread deliveries stay
				// in the stream (not in the PEL — claims do not burn attempts,
				// T-025); the already read ones in the prefetch buffer
				// are finished by the dispatcher and on a failure will settle
				// in the PEL until a claim — like the window of the former barrier
				// loop, the behavior is no worse. The manager's probes replace the generations
				// themselves (a reconnect, T-065).
				log.Warn("Chromium unavailable — the consumption stream is not read (the restart is the manager's job, T-065)",
					"stream", o.Stream)
				wait(DefaultErrorWait)
				if ctx.Err() != nil {
					return // graceful shutdown
				}
				continue
			case modePauseInfra:
				// The consumption pause on a sustained InfraError of publication
				// (T-152): while the write probe fails — no reading
				// (the fuse below would count the same failures longer and
				// would read chunks between pauses).
				infraStreak++
				pause := renderFailBackoff(infraStreak)
				log.Warn("reading pause: the result publication hits the Redis infrastructure (OOM/connection) — new deliveries are not read (T-152)",
					"stream", o.Stream, "streak", infraStreak, "wait", pause.String())
				wait(pause)
				if ctx.Err() != nil {
					return // graceful shutdown: the wait exited on cancellation
				}
				continue
			case modePauseFail:
				// The fuse (renderFailWaitBase): deliveries left in a row
				// without an acknowledgment in the amount of a whole read
				// chunk (Consume) without a single success — a systematic
				// processing failure (render/DB/publication); a pause with
				// doubling up to DefaultErrorWait. After the pause the pass
				// reads (FR-8): the fuse throttles the rate but does not
				// block the reading until a zero counter. An acknowledged
				// delivery resets the failure counter immediately, the pause series
				// is reset on the next reader pass that sees
				// the counter at zero.
				if ctx.Err() != nil {
					return // graceful shutdown: there was and will be no pause
				}
				failStreak++
				pause := renderFailBackoff(failStreak)
				// The downtime observability: the pause stops the stage's reading
				// — a Warn with the series and the failure count (the
				// frequency is bounded by the pause doubling, no more often than ~2/min per series).
				log.Warn("reading fuse pause: deliveries fail in a row",
					"stream", o.Stream, "failed", obs.failed, "streak", failStreak, "wait", pause.String())
				wait(pause)
				if ctx.Err() != nil {
					return // graceful shutdown: the wait exited on cancellation
				}
			case modePauseRate:
				// The rate pause is endured by the shared rate step below —
				// AFTER the series resets: before T-262 the rate check ran
				// after the infra flag resumption and the fuse series
				// reset (the browser/infra pauses did not perform the resets even
				// before — they go to continue). The rate step anyway
				// must re-observe the window after a
				// fuse pause (during the wait the window could have been exhausted),
				// so there is no separate pause branch here — the case
				// marks the pass as without reading by the rate.
			}
			// The resumption after an infra pause (T-152): the flag was lit
			// in the pass's observations, but the mode is no longer the infra pause:
			// the write probe passed (or there is no sensor — a processing
			// success extinguishes the flag). The flag is extinguished, the infra pause series
			// starts over (a flag extinguished by a success does not reset the
			// series — as before T-262).
			if obs.infra && infraDown.Swap(false) {
				infraStreak = 0
				if o.Recover != nil {
					log.Info("the Redis write is back — reading resumes", "stream", o.Stream)
				}
			}
			// The fuse pause series is reset by the observation
			// of a zero failure counter (an acknowledged delivery).
			if failed.Load() == 0 {
				failStreak = 0
			}
			// The consumption rate (T-240, FR-13): before the read the chunk
			// is trimmed to the window's free slots (do not take at once
			// more than the cap); an exhausted window — a pause until
			// the oldest entry leaves (mode R: a fresh one or re-observed
			// after a fuse pause — the stream is not read, attempts
			// are not spent). A Debug, not a Warn: the cap is a regular
			// mode of the B build (which entries to wait for is visible from the
			// queue depth), not a failure; the frequency — once or twice per window.
			batch := o.Consume
			if rateLim != nil {
				now := time.Now()
				if allow := rateLim.allowance(now); allow <= 0 {
					pause := rateLim.retry(now)
					log.Debug("the consumption rate cap is exhausted — reading waits for the window",
						"stream", o.Stream, "rate", o.Rate,
						"window", renderRateWindow.String(), "wait", pause.String())
					wait(pause)
					if ctx.Err() != nil {
						return // graceful shutdown: the wait exited on cancellation
					}
					continue
				} else if allow < batch {
					batch = allow
				}
			}
			deliveries, err := w.q.Consume(ctx, o.Stream, o.Group, o.Consumer, batch)
			if err != nil {
				if ctx.Err() != nil {
					return // graceful shutdown
				}
				log.Warn("render-worker loop: reading the consumption stream", "stream", o.Stream, "err", err)
				wait(DefaultErrorWait)
				continue
			}
			// The read entries enter the rate window (T-240): an empty
			// read — a no-op, idleness does not spend the budget
			// (rateWindow.record).
			if rateLim != nil {
				rateLim.record(time.Now(), len(deliveries))
			}
			for _, d := range deliveries {
				select {
				case pending <- d:
				case <-ctx.Done():
					return // the buffered deliveries without XACK will be returned by XAUTOCLAIM
				}
			}
		}
	}()

	// The dispatcher: waits for a free slot (or cancellation) and starts the delivery.
	// The semaphore slot — with an exit on cancellation: a ctx cancellation during the slot
	// wait must not start a delivery with an already cancelled ctx (the select
	// channel choice is non-deterministic, so the cancellation is checked after
	// receiving the slot too).
	var wg sync.WaitGroup
dispatch:
	for {
		var d queue.Delivery
		select {
		case <-ctx.Done():
			break dispatch // the buffered deliveries are not started — without XACK
		case d = <-pending:
		}
		select {
		case <-ctx.Done():
			break dispatch
		case sem <- struct{}{}:
		}
		if ctx.Err() != nil {
			<-sem
			break dispatch
		}
		wg.Add(1)
		go func(d queue.Delivery) {
			defer wg.Done()
			defer func() { <-sem }()
			// A delivery panic must not crash the process without a log and
			// cleanup — the same invariant as the OnRun goroutine's
			// (internal/service): panic → an Error log, the delivery without
			// XACK will return with a re-delivery (XAUTOCLAIM, T-025);
			// the in-flight neighbors and the graceful shutdown are preserved.
			// For the fuse a panic is the same unacknowledged
			// delivery: a panic series would burn attempts without a pause.
			defer func() {
				if r := recover(); r != nil {
					failed.Add(1)
					log.Error("delivery processing panic",
						"stream", o.Stream, "entry", d.ID, "panic", r, "stack", string(debug.Stack()))
				}
			}()
			// A fresh XREADGROUP delivery — the first by the PEL counter: attempts=1
			// (T-148, the timeout policy in processDelivery).
			err := queue.ProcessingAttempt(ctx, w.q, o.Group, d, 1, w.processDelivery)
			if err != nil {
				failed.Add(1)
				if queue.IsInfra(err) {
					// An infrastructure publication failure (T-152):
					// the reader's infraDown branch stops the reading.
					infraDown.Store(true)
				}
				if ctx.Err() == nil {
					log.Warn("the task is not acknowledged — it will come back with a re-delivery",
						"stream", o.Stream, "entry", d.ID, "err", err)
				}
			} else {
				failed.Store(0)
				// A success means the result publication happened —
				// the Redis write is available, the pause is lifted early.
				infraDown.Store(false)
			}
		}(d)
	}
	wg.Wait()    // the started deliveries finish (graceful stop, spec 3.4)
	<-readerDone // the reader finishes with the same ctx cancellation (Consume/wait)
	return nil
}

// TarpitStore — the seam of the counter of tasks' tarpit-classified deliveries
// (T-261, TZ-fsm §3 FR-7): marks delivery of task taskID
// (idempotently — re-marking the same delivery does not grow the cardinality)
// and returns how many of the task's deliveries are marked, including the current one.
// ttl — the lifetime of the task's key, refreshed by every mark (the episode cannot
// end earlier). The production build — queue.TarpitCounter
// (the Redis set render:tarpit:{task_id}, the specification —
// outcomestate.go); nil — the decision degrades to the PEL delivery number
// (tarpitsExhausted).
type TarpitStore interface {
	Track(ctx context.Context, taskID, delivery string, ttl time.Duration) (int, error)
}

// tarpitDeliveryID — the delivery identifier for the tarpit counter (T-261):
// "stream entry × PEL delivery number" (del.ID#attempts). The number is part of the
// member so that re-deliveries of one entry differ (del.ID is the same
// across the claims of repeats), while a replay of the same number (an attempt
// refund T-222/FR-6.2, reprocessing of a stuck one) is not: one
// delivery — one set member. The q:render and q:render-fallback entries
// carry different entry-ids and do not intersect in one set: an entry-id
// is the "ms-sequence" of the XADD time, and the fallback publication (T-238) happens
// after the original entry's deliveries (the claims ripen over claim_min_idle) — the pairs
// of entries of one task_id are spread by minutes at least, the same
// millisecond of two publications is excluded; the deliveries of workers A and B of one
// task_id do not mix into one member.
func tarpitDeliveryID(del queue.Delivery, attempts int) string {
	return fmt.Sprintf("%s#%d", del.ID, attempts)
}

// tarpitsExhausted — the tarpit finalization decision of a delivery (T-261,
// TZ-fsm §3 FR-7): true — the episode's tarpit budget is exhausted, the delivery
// is finalized (or published to the fallback, publishFallback). The count is the
// cardinality of the render:tarpit:{task_id} set AFTER marking the current
// delivery: the threshold ≥ Options.TimeoutAttempts. True tarpits (the timeout
// of every delivery) finalize exactly on the TimeoutAttempts delivery, as
// before T-261; mixed series (an infra failure between tarpits) — only after
// TimeoutAttempts tarpits: all deliveries grow the delivery number,
// the set — only tarpit-classified ones.
//
// Degradations (the decision by the PEL delivery number — the behavior before T-261,
// the D1–D3 table of outcomestate.go): the counter is not mounted (isolated
// builds), a message without a task_id (the formats before T-024 — no set
// identifier), a Redis error (a Warn: losing the counter does not crash the
// pipeline). A degradation does not finalize earlier than the delivery with the number
// TimeoutAttempts and does not touch the overall redis.max_attempts limit — it
// works by the delivery number (the task class branch below).
func (w *Worker) tarpitsExhausted(ctx context.Context, task queue.Result, del queue.Delivery, attempts int) bool {
	if w.opt.Tarpits == nil || task.TaskID == "" {
		return attempts >= w.opt.TimeoutAttempts
	}
	n, err := w.opt.Tarpits.Track(ctx, task.TaskID, tarpitDeliveryID(del, attempts), DefaultTarpitTTL)
	if err != nil {
		w.opt.log().Warn("the tarpit counter is unavailable — the decision degrades to the PEL delivery number (T-261)",
			"stream", del.Stream, "entry", del.ID, "attempts", attempts, "err", err)
		return attempts >= w.opt.TimeoutAttempts
	}
	return n >= w.opt.TimeoutAttempts
}

// processDelivery processes one q:render delivery: decoding
// the Result key and the content replacement. attempts — the delivery number by the
// PEL counter (1 — a fresh XREADGROUP, Claimed.Attempts — a claim; ProcessingAttempt/
// RecoverLoopAttempt, T-148). The acknowledgment (XACK) is done by Processing
// after a nil return — "XACK only after the result is written to the DB"
// (spec 4.2); a poisoned entry (outside the T-024 format) returns an error —
// the queue will exhaust the attempts and take it to q:dead (T-025).
//
// The timeout policy (T-148, tarpit counting — T-261/FR-7): a page
// timeout (render.timeout, tarpitError) is a predictable tarpit, not a
// random error: while the number of the episode's tarpit-classified deliveries
// has not reached TimeoutAttempts (tarpitsExhausted: SADD
// render:tarpit:{task_id} by the delivery id, the SCARD threshold), the delivery
// returns an error like any retryable one (no XACK → claim → a new
// attempt), on exhaustion — the entry is finalized without a render
// (finalizeSkipRender) and the delivery is acknowledged: work does not fall out of the
// pipeline into q:dead, q:dead stays for real infrastructure
// errors (DB, browser, publication). Before T-261 the threshold was the
// PEL delivery number — a mixed sequence (an infrastructure failure,
// then a timeout) finalized on an attempt earlier: all deliveries
// grow the delivery number, the tarpit set — only tarpits. Losing
// the Redis counter — degradation to the delivery number (tarpitsExhausted),
// the decision table — outcomestate.go.
//
// Permanent host certificate errors (T-159, certError) and a host DNS
// refusal (T-211, dnsError) are finalized from the very first delivery —
// regardless of the number and the tarpit limit: a host property does not change
// between attempts, waiting is pointless.
//
// The last attempt × task class policy (T-172): another render error
// of the task class (isTaskRenderError — a connection refusal, a
// load error, a host property) on the delivery with the number redis.max_attempts
// (Options.MaxAttempts) is finalized following the tarpit pattern instead of
// burying — on early attempts the error is retryable (host recovery
// is observed), a host that exhausted the attempts gets an honest raw
// HTML of the HTTP phase and analysis. The infrastructure class (DB, storage,
// publication, browser, ctx cancellation) dies the former way — q:dead
// stays for real infrastructure errors.
//
// A successful processing goes into the stage metrics (T-077): the task counter and
// the duration, processed=true — the entry processed entirely (the DOM written
// and q:analyze published, T-225); an error — into the
// render class counter (the delivery will return with a re-delivery, spec 3.4),
// a finalization without a render — into crawler_render_tarpit_finalized_total /
// crawler_render_cert_finalized_total /
// crawler_render_dns_finalized_total /
// crawler_render_task_finalized_total{reason}, a fallback publication
// (T-238, instead of the tarpit/task class finalization) — into
// crawler_render_fallback_published_total{reason} (the
// taskFinalizeReason vocabulary; the metric does not grow on a delivery duplicate — no
// publication happened), the fallback suspension events (T-239) — into
// crawler_render_fallback_suspended_total{gate|budget} (the
// ClassBrowser streak and the budget window overspend), the fallback consumption
// (T-245, only the worker B build — the q:render-fallback stream) — into
// crawler_render_fallback_consumed_total{rendered|finalized|skipped}
// by the outcome of the delivery that ended with processing without an error (the
// published = consumed + the q:render-fallback remainder + the buried ones balance,
// acceptance step 4 of spec §6.2).
func (w *Worker) processDelivery(ctx context.Context, del queue.Delivery, attempts int) (err error) {
	task, err := queue.Decode[queue.Result](del)
	if err != nil {
		return fmt.Errorf("renderworker: entry %s of stream %s: %w", del.ID, del.Stream, err)
	}
	start := time.Now()
	// The fallback share budget (T-239): the window's denominator — deliveries
	// that ended with processing without an error (err == nil at the
	// handler's exit — before the XACK at the level above, ProcessingAttempt): an
	// Ack failure or a panic give an extra +1, a re-delivery will be counted
	// again — a tolerance of a few units over the 1000-delivery window, not changing the share's
	// order (the T-239 review, P2).
	defer func() {
		if err == nil {
			w.fbObserveTask()
		}
	}()
	// processed (not "rendered"): true = the entry processed entirely,
	// only with err == nil (T-225) — otherwise it would diverge from the
	// Rendered() metric and the rendered column (the DOM written, the publication failed).
	processed, err := w.ProcessResult(ctx, task)
	// The fallback health gate (T-239): the browser class — a sign of the engine's
	// ill health (the streak grows), a successful render (processed) — a sign of
	// life (a reset); the task/context class errors and a no-op do not touch the
	// streak.
	w.fbObserveOutcome(err, processed)
	if err != nil {
		var ce *certError
		if errors.As(err, &ce) {
			// The cert class is finalized from the very first delivery (T-159): a
			// certificate error is a permanent host property, waiting for attempts
			// (the T-148 tarpit limit gives the page a chance in changed conditions —
			// there is no chance here) would only burn deliveries toward q:dead. The result is the raw
			// HTML of the HTTP phase, the pipeline continues with analysis (q:analyze),
			// the delivery is acknowledged.
			if ferr := w.finalizeCertError(ctx, task, ce); ferr != nil {
				return fmt.Errorf("renderworker: domain %s: %w", task.Name, ferr)
			}
			w.met.Task(time.Since(start))
			w.met.Error(render.ClassTask)
			w.met.CertFinalized()
			w.fbConsume(fbResultFinalized)
			return nil
		}
		var de *dnsError
		if errors.As(err, &de) {
			// The DNS class is finalized from the very first delivery (T-211): the
			// host name does not resolve, the entry got into q:render through a live
			// HTTP episode — the host's DNS died between the episode and the render,
			// the retry window (~20 min) does not affect the DNS recovery (a scale
			// of days), the attempts would only burn deliveries and PEL. The result is the raw
			// HTML of the HTTP phase, the pipeline continues with analysis (q:analyze),
			// the delivery is acknowledged.
			if ferr := w.finalizeDNSError(ctx, task, de); ferr != nil {
				return fmt.Errorf("renderworker: domain %s: %w", task.Name, ferr)
			}
			w.met.Task(time.Since(start))
			w.met.Error(render.ClassTask)
			w.met.DNSFinalized()
			w.fbConsume(fbResultFinalized)
			return nil
		}
		var tp *tarpitError
		if errors.As(err, &tp) && w.tarpitsExhausted(ctx, task, del, attempts) {
			// The fallback (T-238, FR-8): a tarpit is an engine-branched candidate,
			// with the fallback on the task is published to
			// q:render-fallback instead of a finalization (no marker —
			// worker B does the rework and the finalization, FR-10).
			fb, ferr := w.publishFallback(ctx, task, tp)
			if ferr != nil {
				return fmt.Errorf("renderworker: domain %s: %w", task.Name, ferr)
			}
			if fb {
				w.met.Task(time.Since(start))
				w.met.Error(render.ClassTask)
				return nil // in the fallback: no marker, no q:analyze (worker B)
			}
			// The timeout limit is exhausted (T-148): the entry is finalized without
			// a render — the result is the raw HTML of the HTTP phase, the pipeline continues
			// with analysis (q:analyze), the delivery is acknowledged.
			if ferr := w.finalizeSkipRender(ctx, task, tp, attempts); ferr != nil {
				return fmt.Errorf("renderworker: domain %s: %w", task.Name, ferr)
			}
			w.met.Task(time.Since(start))
			w.met.Error(render.ClassTask)
			w.met.TarpitFinalized()
			w.fbConsume(fbResultFinalized)
			return nil
		}
		if attempts >= w.opt.MaxAttempts && isTaskRenderError(err) {
			// The last attempt × task class (T-172): the error is a
			// host property (a connection refusal, a load error), ahead only q:dead;
			// the retries were already given (recovery is observed — the T-172
			// measurement: 6.6% of ERR_CONNECTION_REFUSED retries succeed), we do not
			// lose the work: the result is the raw HTML of the HTTP phase, the pipeline continues
			// with analysis (q:analyze), the delivery is acknowledged.
			// The fallback (T-238, FR-8): the same engine-branched reason —
			// with the fallback on a publication to q:render-fallback
			// instead of a finalization (the marker — to worker B, FR-10).
			fb, ferr := w.publishFallback(ctx, task, err)
			if ferr != nil {
				return fmt.Errorf("renderworker: domain %s: %w", task.Name, ferr)
			}
			if fb {
				w.met.Task(time.Since(start))
				w.met.Error(render.ClassTask)
				return nil // in the fallback: no marker, no q:analyze (worker B)
			}
			if ferr := w.finalizeTaskError(ctx, task, err, attempts); ferr != nil {
				return fmt.Errorf("renderworker: domain %s: %w", task.Name, ferr)
			}
			w.met.Task(time.Since(start))
			w.met.Error(render.ClassTask)
			w.met.TaskFinalized(taskFinalizeReason(err))
			w.fbConsume(fbResultFinalized)
			return nil
		}
		w.met.Error(render.ClassOf(err))
		return fmt.Errorf("renderworker: domain %s: content replacement: %w", task.Name, err)
	}
	w.met.Task(time.Since(start))
	if processed {
		w.met.Rendered()
		// The fallback consumption (T-245): the success of worker B's rework —
		// rendered=true and a q:analyze publication (T-225: processed = the entry
		// processed entirely).
		w.fbConsume(fbResultRendered)
	} else {
		// A regular rejection without side effects (no row / no final
		// URL / the UPDATE guard cut it off — the row slipped out from under the task):
		// the delivery is closed — the fallback consumption accounts for it
		// (skipped), otherwise the published = consumed + remainder balance
		// would diverge on the "empty" deliveries.
		w.fbConsume(fbResultSkipped)
	}
	return nil
}

// finalizeNoRender — the common mechanics of finalizing an entry without a render
// (T-148/T-159/T-172/T-211): no row (the recheck rewrote it) or no final
// URL — a regular rejection without a publication; otherwise the row
// gets the "render impossible" marker render_failed_at (T-210 —
// an optimistic guard on content_hash, until the content changes the rework
// pass does not re-publish the entry) and goes to q:analyze with the same key
// (Fresh=true — reworking the current episode, like a successful render);
// a Warn log with the reason details (msg and the "key-value" pairs of the calling
// policy). The marker — before the publication, the publication — before the XACK: a failure of any
// step — an error, the delivery will return and the finalization will repeat (a
// publication duplicate is bounded by the attempts, the analyzer is idempotent by the key —
// the same pattern as the render after a publication failure; the stamp
// is re-set idempotently). A row already rendered by a concurrent delivery
// is not marked (the rendered = false guard) and is analyzed
// again — an analysis duplicate with the same content is harmless (at-least-once,
// spec 3.4).
func (w *Worker) finalizeNoRender(ctx context.Context, task queue.Result, msg string, fields ...any) error {
	target, found, err := w.selectTarget(ctx, task)
	if err != nil {
		return fmt.Errorf("finalize without render: result row read: %w", err)
	}
	if !found || target.finalURL == "" {
		// Nothing to render/analyze: no row or it slipped out from under
		// the task (the recheck) — a success without a publication.
		w.opt.log().Debug("finalize without render not needed — no row or no final URL",
			"zone_id", task.ZoneID, "domain_id", task.DomainID,
			"subdomain_id", task.SubdomainID, "variant", int16(task.Variant))
		return nil
	}
	// The "render impossible" marker (T-210): an autocommit UPDATE before the publication.
	// RowsAffected 0 — the guard cut it off (a new episode rewrote the row
	// or a concurrent render): not an error, the q:analyze publication below
	// addresses the current row, the stamp does not belong to it.
	if tag, err := w.db.Exec(ctx, renderFailMarkerSQL,
		task.ZoneID, task.DomainID, task.SubdomainID, int16(task.Variant), target.hash); err != nil {
		return fmt.Errorf("finalize without render: the render_failed_at marker: %w", err)
	} else if tag.RowsAffected() == 0 {
		w.opt.log().Debug("finalize without render: the row slipped out from under the task — no marker",
			"zone_id", task.ZoneID, "domain_id", task.DomainID,
			"subdomain_id", task.SubdomainID, "variant", int16(task.Variant))
	}
	task.Fresh = true
	if _, err := w.q.Publish(ctx, queue.StreamAnalyze, task); err != nil {
		return fmt.Errorf("finalize without render: q:analyze publication: %w", err)
	}
	args := append([]any{"domain", renderLogName(task, target.finalURL), "final_url", target.finalURL}, fields...)
	w.opt.log().Warn(msg, args...)
	return nil
}

// finalizeSkipRender — the finalization of an entry without a render by a tarpit (T-148,
// the fixed decision): called from processDelivery after
// publishFallback (T-238) rejected — the fallback is off, another
// consumption stream, rendered=true or a gate/budget decision rejection (T-239 —
// a ClassBrowser infra error streak / a share overspend over the window: the
// publication is suspended, the task without losses):
// a tarpit is an engine-branched candidate, with the fallback on it
// is published to q:render-fallback instead of a finalization. The page time after time
// does not fit into
// render.timeout, the render attempts are exhausted — the entry's result stays
// the raw HTML of the HTTP phase (rendered = false), the row gets the
// "render impossible" marker render_failed_at (T-210 — until the content changes the
// rework pass does not re-publish it), the entry goes to q:analyze, the pipeline
// finishes it with analysis, the work is not lost. The delivery is acknowledged
// (nil to the calling processDelivery) — the entry does not reach q:dead.
// The mechanics and idempotency — finalizeNoRender.
func (w *Worker) finalizeSkipRender(ctx context.Context, task queue.Result, cause *tarpitError, attempts int) error {
	return w.finalizeNoRender(ctx, task,
		"render tarpit: the record finalized without render (the outcome is the raw HTML of the HTTP phase)",
		"attempts", attempts, "timeout_attempts", w.opt.TimeoutAttempts,
		"err", cause.Error())
}

// finalizeCertError — the finalization of an entry without a render by a permanent
// host certificate error (T-159, the fixed decision): rendering this URL
// is impossible until the host replaces the certificate itself, retries are pointless and
// would burn the overall delivery limit down to q:dead (the §12 measurement). The entry's result is
// the raw HTML of the HTTP phase (rendered = false), the "render impossible"
// marker render_failed_at (T-210) closes the re-publications until the content changes
// (we do not touch the error column — it belongs to the HTTP check, ErrorClass
// T-058, a rewrite would lie about it; the domain's HTTP result is already saved,
// spec §8 — a bad certificate does not hinder loading, InsecureSkipVerify).
// The marker lives until the row's content changes, not forever: the next recheck
// episode (T-030) rewrites the row (the marker — NULL) and publishes
// a new q:render task — with the certificate fixed the render will pass.
// The delivery is acknowledged (nil to the calling processDelivery) — the
// entry does not reach q:dead. The mechanics and idempotency — finalizeNoRender.
func (w *Worker) finalizeCertError(ctx context.Context, task queue.Result, cause *certError) error {
	return w.finalizeNoRender(ctx, task,
		"render impossible: a permanent host certificate error — the record finalized without render (the outcome is the raw HTML of the HTTP phase)",
		"err", cause.Error())
}

// finalizeDNSError — the finalization of an entry without a render by a host DNS
// refusal (T-211, from the T-207 outcomes, the fixed decision): the host name does
// not resolve, and the entry got into q:render through a recent live HTTP episode
// (the HTTP phase resolved the name itself; the browser resolves with the same system
// unbound) — the host's DNS died between the episode and the render, retries over the
// 3-delivery window (~20 min in the PEL) are pointless: a DNS recovery is a scale of
// days (the T-207 measurement: 252 hosts per ~7 h ate 3 deliveries each). The entry's result is
// the raw HTML of the HTTP phase (rendered = false), the "render impossible"
// marker render_failed_at (T-210) closes the pass's re-publications until the
// content changes (we do not touch the error column — it belongs to the HTTP check,
// ErrorClass T-058, a rewrite would lie about it). The marker lives until the
// row's content changes, not forever: the next recheck episode (T-030)
// rewrites the row (the marker — NULL) and publishes a new q:render task
// — a host with recovered DNS will render. The delivery is acknowledged (nil
// to the calling processDelivery) — the entry does not reach q:dead. The mechanics and
// idempotency — finalizeNoRender.
func (w *Worker) finalizeDNSError(ctx context.Context, task queue.Result, cause *dnsError) error {
	return w.finalizeNoRender(ctx, task,
		"render impossible: a host DNS failure (the name does not resolve) — the record finalized without render (the outcome is the raw HTML of the HTTP phase)",
		"err", cause.Error())
}

// finalizeTaskError — the finalization of an entry without a render by a task class
// error on the last attempt (T-172, the fixed decision):
// called from processDelivery after publishFallback (T-238)
// rejected — the fallback is off, another consumption stream, rendered=true or
// a gate/budget decision rejection (T-239 — a ClassBrowser infra error streak
// / a share overspend over the window: the publication is suspended, the task without
// losses): an engine-branched reason with
// the fallback on is published to q:render-fallback instead of
// a finalization. The host
// exhausted the deliveries (redis.max_attempts) with a render error of the task class —
// a connection refusal, a load error (isTaskRenderError) — the retries were already given,
// ahead the path only to q:dead; instead of burying the entry's result stays
// an honest raw HTML of the HTTP phase (rendered = false), the row gets the
// "render impossible" marker render_failed_at (T-210 — until the content changes the
// rework pass does not re-publish it), the entry goes to q:analyze — the pipeline
// finishes it with analysis, the work is not lost (the Oct 23 incident: a mixed
// timeout→refused series burned all the deliveries and took 459 entries to q:dead
// in 11 minutes, every death ≈ 3 extra render slot-times).
// Infrastructure errors (DB, storage, publication, browser) do not get
// here — not being the task class they die the former way. The marker lives until
// the row's content changes, not forever: the next recheck episode (T-030)
// rewrites the row (the marker — NULL) and publishes a new q:render task
// — a host that came up will render. The delivery is acknowledged (nil
// to the calling processDelivery) — the entry does not reach q:dead. The mechanics and
// idempotency — finalizeNoRender.
func (w *Worker) finalizeTaskError(ctx context.Context, task queue.Result, cause error, attempts int) error {
	return w.finalizeNoRender(ctx, task,
		"render impossible: a task class error exhausted the deliveries — the record finalized without render (the outcome is the raw HTML of the HTTP phase)",
		"attempts", attempts, "max_attempts", w.opt.MaxAttempts,
		"err", cause.Error())
}

// publishFallback — an attempt to send the task to q:render-fallback instead of
// finalizing the entry without a render (T-238, spec §4.4 items 1/5, FR-7/8/10):
// the engine-branched candidate (the tarpit T-148 or the task class T-172 on
// the last attempt — their common sign "the render would be finalized without
// a result", FR-7) is reworked by the chromium worker B (spec §4.5), and
// the primary worker does NOT set the render_failed_at marker and does NOT publish
// q:analyze — worker B does both (a success — renderUpdateSQL
// with a NULL marker and a q:analyze publication; the fallback exhaustion — the
// finalizeNoRender marker, FR-10). The entry's intermediate "awaiting
// fallback" state — the marker's absence and the q:render-fallback depth, no separate
// column (spec §4.8 — the DB schema does not change, the fallback state lives
// in the queue and the metrics).
//
// The "publication before XACK" order — following the finalization connection with
// the q:analyze publication pattern (finalizeNoRender: the stamp → the publication →
// the acknowledgment, a failure of any step — a repeat of the whole attempt): here
// the publication → the acknowledgment, a publication failure — an error without a marker and
// without an XACK, the delivery will return and replay the attempt (the entry was not
// changed — nothing to repeat, idempotent by construction).
//
// The filters before the publication:
//   - Fallback.Enabled (render.fallback.enabled, FR-14), the
//     consumption stream and the build's engine: publications go only from q:render and
//     only from the candidate engine — worker B (the same binary on
//     q:render-fallback, FR-12) does not publish the fallback: a recursive
//     fallback of the fallback is forbidden (FR-10), its engine-branched failures
//     are finalized by finalizeNoRender (spec §4.5 item 2); a chromium build engine
//     (including an empty value) does not publish either (S-P1-1,
//     spec §6.2 item 2/§6.4 item 1) — chromium does not fallback itself, a rollback
//     to chromium puts the publication out by itself, without changing the flag;
//   - no row / no final URL — the fallback has nothing to rework:
//     the caller finalizes the entry (the same regular rejection there);
//   - the row is already rendered=true (worker B reworked it between
//     the publication and the late delivery duplicate, a concurrent render) —
//     no publication needed: nothing to rework, the delivery is acknowledged
//     without publications (closes the window of an extra repeated render
//     by worker B — the T-238 review, P3);
//   - the fallbackDedup dedup filter by the result row identifier
//     (the row key + the episode's checked_at): a re-delivery of an already
//     published row (an XACK failure after the publication) does not give
//     a second entry in the stream (spec §4.4 item 5) — the delivery is acknowledged
//     without publications. The duplicate check is BEFORE the gate/budget decision:
//     the entry is already waiting for worker B, a suspension does not cancel it and no
//     marker is needed for it (the T-239 review, P3); the claim — after
//     the decision: a claim on a rejection without a publication would close the
//     row's fallback until the episode changes;
//   - the fallbackPublishDecision decision (candidate × gate × budget,
//     FR-9, T-239): the health gate is closed by a ClassBrowser infra error
//     streak (the engine is unhealthy — recovery will reset it) or the budget
//     window overspent the share cap — the publication is suspended,
//     the task is finalized as today (the marker, spec §4.4 items 3–4).
//
// The returns:
//   - (true, nil): the delivery is acknowledged WITHOUT finalizeNoRender —
//     a successful publication (+1 of the
//     crawler_render_fallback_published_total{reason} metric), a duplicate (the
//     stream already holds the entry, the metric does not grow) or rendered=true (nothing
//     to publish);
//   - (false, nil): no publication happened (the fallback is off / the
//     consumption stream is not q:render / the chromium engine / no row / the
//     decision rejected) — the caller finalizes the entry as today;
//   - (_, err): a row read or publication failure — the caller
//     returns the error: the delivery without an XACK will return (a redelivery),
//     no marker, the entry's state did not change.
func (w *Worker) publishFallback(ctx context.Context, task queue.Result, cause error) (bool, error) {
	if !w.fallbackArmed() {
		return false, nil
	}
	target, found, err := w.selectTarget(ctx, task)
	if err != nil {
		return false, fmt.Errorf("fallback publication: result row read: %w", err)
	}
	if !found || target.finalURL == "" {
		// The fallback has nothing to rework: the caller finalizes the entry —
		// the same regular rejection without publications there.
		return false, nil
	}
	if target.rendered {
		// The row is already rendered (worker B reworked it, a concurrent
		// render): neither the fallback nor the finalization is needed — the
		// delivery is acknowledged without publications and a marker.
		w.opt.log().Debug("fallback: the row is already rendered — no publication needed",
			"zone_id", task.ZoneID, "domain_id", task.DomainID,
			"subdomain_id", task.SubdomainID, "variant", int16(task.Variant))
		return true, nil
	}
	cand := fallbackCandidate{task: task, reason: taskFinalizeReason(cause)}
	// The duplicate BEFORE the gate/budget decision (the T-239 review, P3): the
	// episode's entry is already in the stream (an XACK failure after the publication) — a suspension
	// does not cancel a publication that happened, a second entry and a marker are not
	// needed; the delivery is acknowledged (FR-11). The claim — below,
	// after the decision: a claim without a publication would close the row's fallback.
	key := fallbackDedupKey(task, target.checkedAt)
	if w.fbDedup.known(key) {
		w.opt.log().Debug("fallback: the record is already published to q:render-fallback — a duplicate delivery is acknowledged",
			"zone_id", task.ZoneID, "domain_id", task.DomainID,
			"subdomain_id", task.SubdomainID, "variant", int16(task.Variant))
		return true, nil
	}
	gate, budget := w.fallbackStates()
	if !fallbackPublishDecision(cand, gate, budget) {
		// The gate/budget suspended the publication (FR-9, T-239): the task
		// is finalized as today — the marker and q:analyze
		// by the caller, without losses (spec §4.4 items 3–4).
		return false, nil
	}
	if !w.fbDedup.claim(key) {
		// A duplicate: the episode's entry is already in the stream (an XACK failure
		// after the publication) — a second entry is not needed (spec §4.4 item 5), the
		// delivery is acknowledged.
		w.opt.log().Debug("fallback: the record is already published to q:render-fallback — a duplicate delivery is acknowledged",
			"zone_id", task.ZoneID, "domain_id", task.DomainID,
			"subdomain_id", task.SubdomainID, "variant", int16(task.Variant))
		return true, nil
	}
	// Fresh=true — reworking the current episode, like the finalization and
	// a successful render: worker B will publish q:analyze with the same key.
	task.Fresh = true
	if _, err := w.q.Publish(ctx, queue.StreamRenderFallback, task); err != nil {
		// The publication did not happen: the duplicate record is removed — otherwise
		// a re-delivery would see "already published" and lose
		// the task between the filter and the stream; no marker, no XACK —
		// the entry's state did not change, a repeat replays everything.
		w.fbDedup.release(key)
		return false, fmt.Errorf("the fallback publication to q:render-fallback: %w", err)
	}
	w.met.FallbackPublished(cand.reason)
	// The budget share's numerator (T-239): the publication actually went to the queue.
	w.fbRecordPublish()
	w.opt.log().Warn("render fallback: the task published to q:render-fallback — chromium worker B reworks it, the record stays without the marker",
		"domain", renderLogName(task, target.finalURL), "final_url", target.finalURL,
		"reason", cand.reason, "err", cause.Error())
	return true, nil
}

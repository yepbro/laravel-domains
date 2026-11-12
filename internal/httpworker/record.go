// Writing the HTTP episode's result into subdomain_results and the q:analyze publication
// (T-058; spec 5.5 items 7, 9, §6.1, §7, §8, §3.4). The file closes the
// stage's pipeline: DomainResult (T-053) after storing the content (ContentSaver,
// T-056) turns into subdomain_results rows by one batch, the domain
// receives the final status and done, every record is published into q:analyze
// (Result, T-024). The call point — Options.OnResult in the service build
// (cmd/http-worker), right after ContentSaver and before the XACK.
//
// The domain status (§5.5 item 9, §7): there are records (at least one subdomain answered
// over at least one protocol) → alive; no records and among the checks there are
// timeouts (ErrTimeout) → temp_error — "SERVFAIL, timeouts" (spec 6.2),
// the scheduler will schedule the episode's repeat (T-031); no records and the web is silent
// constantly → no_http. check_state = done — the domain is finished after DNS/HTTP
// (§7: "a domain is considered done after the DNS/HTTP stage completes, and
// the render and analysis results complement it asynchronously"); rendered =
// false (the render will replace the raw HTML with the DOM, T-067), rules_version = NULL
// (the analyzer will set it, T-068+), render_failed_at — DEFAULT NULL:
// the episode's rewrite (DELETE + INSERT) resets the "render
// impossible" marker (T-210) — the new content has the right to a render.
// queued_at = NULL and last_checked_at =
// now() — the T-039 style: the lease is finished together with the episode (the
// T-018/T-029/T-030 invariant), the time — the DB server's; temp_attempts is reset at
// every non-temp_error outcome (the T-031 contract, as at the DNS stage T-039).
//
// The operation order (spec 3.4, §5.5 item 9 "after writing the result
// a task is published into q:analyze"; spec 4.2 "the XACK only after writing
// the result into the DB"):
//
//		the write into the DB (one transaction) → the q:analyze publication → the XACK
//
//	  - the write and the content reference swap — ONE transaction (SwapRefTx, T-044:
//	    separate transactions are dangerous in both directions — a row without a reference or
//	    a reference without a row). Inside: UPDATE domains (the deletion guard) →
//	    DELETE of the old subdomain×variant rows with RETURNING content_hash →
//	    −1 refcount of every old hash → INSERT of the new rows; the tail
//	    steps — the counter deltas: the zones (T-178), the render debt (T-175),
//	    the daily checks counter checks:YYYY-MM-DD, its numerator
//	    temp_error:YYYY-MM-DD (T-179/T-182 — write events, the authority —
//	    the monitor's reconciliation), the retry maturity
//	    bucket retry_mature:YYYY-MM-DD (T-202 — temp_error episodes maturing toward
//	    midnight; the decrement — the scheduler's retry) and the hourly
//	    throughput bucket results:YYYY-MM-DD"T"HH24 (T-203 — the inserted rows
//	    of the episode; the rendered-throughput bucket is kept by the render-worker);
//	  - the q:analyze publication — AFTER the transaction commit: the Result
//	    consumer (the analyzer, T-068+) reads the record from the DB by key — a message
//	    earlier than the row would stare into the void. A publication failure → the q:http delivery
//	    without an XACK: a repeated delivery will rewrite the same result (idempotently,
//	    see below) and publish q:analyze again — the message duplicates
//	    are bounded by the delivery attempts (at-least-once, spec 3.4; the consumer
//	    must be idempotent by key — the same contract as an XACK
//	    that lost its answer);
//	  - the q:render publication (the SPA verdicts T-057) — T-066, renderq.go:
//	    a non-blocking capacity check of q:render BEFORE the transaction; an overflow
//	    — the render_pending flag by THE SAME transaction (§3.4), with a free
//	    queue — the publication after the commit bypassing WatermarkPublisher
//	    (§5.6: the render does not delay the other stages — the XACK does not wait
//	    for the capacity).
//
// The idempotency of a repeated delivery (spec 3.4): the result's rewrite —
// DELETE+INSERT of the domain's rows (§3.5 "the check history is not stored: only
// the last result" — ALL the domain's rows are deleted, including the keys
// absent from the new result, for example a vanished subdomain). The guard
// is one — status <> deleted (§8/§13.1: a domain deleted during the check
// gets no result written; the row is locked by the UPDATE and the delete
// T-019 either makes it in time earlier — the write is skipped, or later — the status
// is overwritten into deleted, T-020 will clean up the remaining rows). There is NO
// check_state <> done guard (as at T-039), and this is a deliberate choice: the order
// "write → publish" leaves the window "written, q:analyze not
// published" — a delivery repeat must-publish again, and the done
// filter would drop the publication forever (the analysis would wait for the
// T-030 recheck, ~half a year). The price — a theoretical race with a fresh recheck
// (a message stuck without an XACK would survive a full recheck cycle): the delivery
// attempts are exhausted into q:dead within minutes (T-025), and the recheck interval
// is measured in months (§3.3) — the window is practically unreachable. The early
// recheck "the status hangs" (T-270, R15 of the check_state specification)
// shortens the "months" to days: a duplicate task of a revived worker (including
// the cohort's midnight boarding — the stalerequeue.go comment) is extinguished by this
// same idempotency: the episode's rewrite DELETE+INSERT, the q:analyze
// re-publication (the canonical asymmetry text — internal/model/enums.go,
// A3).
//
// The refcount accounting (T-044/T-056): the new content_hash values already received +1 from the
// ContentSaver's Put (T-056, before the write) — here only −1 to the deleted rows:
// SwapRefTx(old, a zero Hash) — the "a new zero → only −to the old" primitive.
// A delivery repeat keeps the balance: a repeated Put gives an extra +1 (a safe
// space leak, T-056), the DELETE of the same row — −1; going negative is impossible
// (the refcount > 0 guard, a divergence — ErrRefIntegrity with a rollback, T-044).
package httpworker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/netip"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"crawler/internal/content"
	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// The result write SQL. The schema codes — as literals, not data (the
// T-039 style); now() — the DB server's transaction time: the rows' checked_at and
// the domain's last_checked_at — one moment (transaction_timestamp is stable
// inside a transaction).
//
// The check_state transition here — row R4 of the automaton specification (the
// single source of truth: internal/scheduler/checkstate.go, T-255).
//
// # The HTTP stage's branch of the status automaton's decision table (T-256, TZ-fsm spec §3 FR-2)
//
// The common part (all the stages and writers) — one table in
// internal/model/enums.go at the model.Status codes; here — the deployment of
// the HTTP branches (O7–O9, the A2/A3 asymmetry rows). The verdict is computed by a pure
// function recordStatus (at the bottom of the file; the contract — the decision
// table's unit test TestRecordStatusDecision), the write is done by recordDomainSQL:
//
//	| Branch | The episode's condition (DomainResult) | Status | The condition's source |
//	|-------|-------------------------------|--------|------------------|
//	| B1 (O7) | HasRecords: at least one subdomain×protocol answered (a live result record) | alive | the DomainResult records (recordInsertSQL inserts) |
//	| B2 (O8) | no records AND AnyTimeout: among the checks there is the ErrTimeout timeout class — a connect timeout, TLS, headers, the overall, the anti-tarpit | temp_error | workerClassifyError → ErrTimeout (httpfetch.IsTimeout: net.Error.Timeout) |
//	| B3 (O9) | no records, no timeouts — the web is silent constantly: connection refused (ErrNetwork), a TLS handshake failure (ErrTLS), the redirects limit (ErrRedirects), a body truncation (ErrBody), the dead-host cutoff of synthetic checks (ErrCutoff, IsTemp false) | no_http | workerClassifyError → the other classes (IsTemp false) |
//
// The stage's deliberate asymmetries (the common table's decisions, NOT changed):
//
//	A2  connect-refused → B3 (no_http), a connect timeout → B2 (temp_error):
//	    an RST on a SYN — the port is closed, a permanent host property; a SYN silence —
//	    a network/firewall transient, a repeat makes sense. The classes are separated by
//	    workerClassifyError (ECONNREFUSED is not a net.Error timeout
//	    → ErrNetwork; a dial timeout → ErrTimeout).
//	A3  recordDomainSQL writes the outcome WITHOUT the check_state <> done guard (unlike
//	    the DNS operators): the "write → publish" order
//	    leaves the window "written, q:analyze not published" — a repeated
//	    delivery must re-publish, the done filter would drop
//	    it forever (the file comment, the spec 3.4 idempotency).
var (
	// recordDomainSQL — the HTTP episode's final status (alive/no_http/
	// temp_error, §7), done, the lease and retry counter reset (the
	// T-031 contract), the dead_host_cutoff bit of the last episode (T-164). The
	// status <> deleted guard — §8/§13.1; the UPDATE as the transaction's first step
	// locks the domain's row: the concurrent episode writes
	// are serialized, the deletion decision is made on the current
	// row version. There is no check_state <> done guard — a repeated delivery
	// rewrites the result (the file comment).
	recordDomainSQL = "UPDATE domains AS t " +
		"SET status = $3::int2, " +
		fmt.Sprintf("temp_attempts = CASE WHEN $3::int2 = %d THEN t.temp_attempts ELSE 0 END, ", int16(model.StatusTempError)) +
		fmt.Sprintf("check_state = %d, queued_at = NULL, last_checked_at = now(), ", int16(model.CheckStateDone)) +
		// The dead_host_cutoff bit (T-164) — the sign of the LAST episode:
		// a cutoff episode sets it (the control sample of false cutoffs
		// selects the domains by the bit mask), a usual one removes it. The bitwise
		// PostgreSQL operators promote smallint to int — the return
		// ::int2 (the renderq.go style), the removal mask — a signed ~of the bit.
		fmt.Sprintf("flags = CASE WHEN $4::bool THEN (t.flags | %d)::int2 ELSE (t.flags & %d)::int2 END ",
			int16(model.FlagDeadHostCutoff), int16(^int16(model.FlagDeadHostCutoff))) +
		fmt.Sprintf("WHERE t.zone_id = $1::int2 AND t.id = $2::int8 AND t.status <> %d", int16(model.StatusDeleted))

	// recordSelectStateSQL — a point pre-read of the domain row's old values
	// before the UPDATE (T-178, incremental-counters spec §6.4): PG 16 has no RETURNING OLD, and
	// recordDomainSQL does not pin the old check_state (the guard only by
	// the status). SELECT … FOR UPDATE as the transaction's first step takes over
	// the row locking role ("the UPDATE as the first step locks the row"
	// formerly): the concurrent episode writes are serialized, the deletion
	// decision is made on the current row version. A PK lookup —
	// the extra price is one point PK read per domain (the
	// completion rate — tens per second by the §12 measurement) against the exact
	// zone counter deltas (a duplicate delivery, a lease return mid-processing).
	recordSelectStateSQL = "SELECT status, check_state FROM domains " +
		"WHERE zone_id = $1::int2 AND id = $2::int8 FOR UPDATE"

	// recordZoneShiftSQL — the zone counters' delta by the old state
	// of the translated row (T-178): queued/…−1 (the deltas as parameters:
	// −1 of the old column, +1 checked; an old done — a duplicate delivery, the deltas
	// zero — a repeated result write already led the domain out of queued
	// at the first pass, incremental-counters spec §6.4). By the same transaction as the result
	// write: a rollback rolls back the pair. RowsAffected != 1 — the zone row
	// is absent (possible only with a manual edit of the reference table,
	// runbooks §6): a silent drift is unacceptable, the error is louder (the
	// renderBumpDebtCounter, T-175).
	recordZoneShiftSQL = "UPDATE zones " +
		"SET domains_pending = domains_pending + $2::int8, " +
		"domains_queued = domains_queued + $3::int8, " +
		"domains_checked = domains_checked + $4::int8 " +
		"WHERE id = $1::int2"

	// recordChecksBumpSQL — the daily checks counter increment
	// crawler_counters['checks:YYYY-MM-DD'] (T-179, TZ_slow_done_today
	// §5): an upsert with an increment — the new day's row is created by the
	// first increment after midnight (the rotation creates no work, the yesterday's
	// row remains the throughput history — 365 rows/year, no cleaning needed).
	// The day name is computed by the DB ('checks:' || current_date): the timezone
	// is agreed with the last_checked_at write's now() (T-058) and with the
	// monitor reconciliation window — the Go clocks do not participate in the name. The counter
	// counts the result write events (§5.1): an episode repeat of the same
	// domain on the same day gives +1 with a live count(*) giving 0 —
	// the divergence is repaired by the reconciliation (the FILTER positions of the
	// T-178 pass), the drift — an observable metric.
	recordChecksBumpSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('checks:' || to_char(current_date, 'YYYY-MM-DD'), $1::int8, now()) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()"

	// recordTempErrBumpSQL — the daily temp_error share numerator increment
	// crawler_counters['temp_error:YYYY-MM-DD'] (T-182, TZ_slow_dns_share
	// §5): the same upsert pattern as recordChecksBumpSQL (T-179); the
	// day name is computed by the DB — the timezone is agreed with the
	// last_checked_at write's now() and the reconciliation window. The delta — 1 per episode
	// written with the final status temp_error (the recordStatus verdict —
	// one per domain, not by the number of subdomain×variant rows); the outcomes without
	// temp_error do not run the operator at all — the new day's row is created by
	// the first temp_error episode, before it the share reader sees an honest zero.
	// The semantics — the write events, as at the denominator (§5.1): a retry
	// that closed the error stays in the events history — the share is resilient to
	// the delivery duplicates (the numerator and the denominator grow together), the absolutes
	// are fixed by the reconciliation.
	recordTempErrBumpSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('temp_error:' || to_char(current_date, 'YYYY-MM-DD'), $1::int8, now()) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()"

	// recordRetryMatureBumpSQL — the temp_error maturity bucket increment
	// crawler_counters['retry_mature:YYYY-MM-DD'] (T-202): an episode
	// written today with a temp_error outcome matures toward the next
	// day's midnight (the "older than 24 h" retry window by the last_checked_at date column —
	// the bucket key is computed by the DB from current_date, the same key the retry
	// decrement derives from last_checked_at, and the reconciliation — by the FILTER
	// positions of the pass; the retryMatureSQL guard sums the past day's buckets and does
	// not let the heavy candidate selection run at a zero sum — the
	// guard update of T-191, which used to distinguish "there are temp_errors" from "there are mature ones").
	// The recordChecksBumpSQL/recordTempErrBumpSQL upsert pattern: the
	// bucket's row is created by the first episode of the day, the delta — the final
	// UPDATE's RowsAffected (1 per episode); only the temp_error outcomes run it — the
	// maturity event is produced exactly by a temp_error episode, the other outcomes do not
	// touch the buckets. Duplicate deliveries and repeated episodes of the same day give +1 to a
	// live zero — the streaming event semantics (as at the daily rows,
	// TZ_slow_done_today §5.1), the reconciliation levels the absolutes.
	recordRetryMatureBumpSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('retry_mature:' || to_char(current_date, 'YYYY-MM-DD'), $1::int8, now()) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()"

	// recordReanalyzeBumpSQL — the unanalyzed tails bucket increment
	// crawler_counters['reanalyze:YYYY-MM-DD"T"HH24'] (T-204): +N — the
	// subdomain_results rows inserted by this transaction with rules_version =
	// NULL (the delta — recordInsertSQL's RowsAffected; the hour —
	// date_trunc('hour', now()) of the same transaction = the hour of the rows' checked_at,
	// the bucket name is computed by the DB). The bucket is maintained by both the analyzer's stamp
	// decrement (worker.go, the row's episode hour) and the render's content replacement
	// increment (renderworker, the episode hour from RETURNING);
	// the authority — the monitor reconciliation (the prev/cur FILTER positions of the
	// two live hours' window aggregate). The reanalysis tick pass's guard
	// (analyzer/reanalyze.go, reanalyzeGuardSQL) sums the buckets older than
	// the current hour — what is written now lands in the immature
	// current-hour bucket and the guard does not open (the write →
	// q:analyze → stamp in-flight flow takes seconds); a survivor past the hour boundary without
	// a stamp — a lost publication, a mature tail — opens the guard,
	// the tick reads out the window and publishes. Silent episodes (without rows)
	// do not write a zero delta at all. The episode's rewrite deletes the old rows
	// without a decrement — the streaming event semantics against live rows
	// (as at results:, T-203), the reconciliation levels the absolutes.
	// The recordChecksBumpSQL upsert pattern; the lock order — after
	// retry_mature: and before results: (the reconciliation pass keeps the families
	// "… → retry_mature: → reanalyze: → rendered: → results:", rendered:
	// is not touched by this transaction) — without cycles.
	recordReanalyzeBumpSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('reanalyze:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24'), $1::int8, now()) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()"

	// recordResultsBumpSQL — the hourly throughput bucket increment
	// crawler_counters['results:YYYY-MM-DD"T"HH24'] (T-203): +N — the
	// subdomain_results rows inserted by this transaction (the delta —
	// recordInsertSQL's RowsAffected; the rows' checked_at = the same
	// transaction's now(), therefore date_trunc('hour', now()) — the rows' hour, the
	// bucket name is computed by the DB — the timezone is agreed with the write's now(),
	// the reader and the reconciliation). The live analog — the former
	// results_hour throughputSQL subquery (count(*) over subdomain_results,
	// checked_at >= now() − 1 h): index-only over the window's fresh pages
	// degraded by heap fetches — the stage 27 checkpoint (the same
	// mechanism as the daily window of T-179). The bucket — a calendar hour:
	// the sliding window is assembled by the reader as the sum of the two live buckets
	// (status.go throughputSQL), the authority — the monitor reconciliation
	// (the pass's FILTER positions over subdomain_results). The episodes without rows
	// (temp_error/no_http) do not write a zero delta at all — the hour's bucket is created by
	// the first inserted row. A duplicate delivery rewrites the
	// episode's rows: the rows leaving the window are not decremented — the streaming
	// event semantics (as at the daily rows, TZ_slow_done_today §5.1),
	// the reconciliation levels the absolutes. The recordChecksBumpSQL upsert pattern;
	// the lock order — the last among the transaction's counter rows
	// (after retry_mature: and reanalyze:), shared with the
	// reconciliation pass "… → retry_mature: → reanalyze: → rendered:
	// → results:" — without cycles (rendered: is not touched by this transaction —
	// it is kept by the renderworker).
	recordResultsBumpSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('results:' || to_char(date_trunc('hour', now()), 'YYYY-MM-DD\"T\"HH24'), $1::int8, now()) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()"

	// recordDeleteSQL — the result's rewrite (§3.5): all the domain's rows are deleted,
	// RETURNING collects the content_hash values for the reference swap (T-044).
	// The leading PK columns (zone_id, domain_id) — the LIST partitioning by
	// zone_id and the HASH subpartitioning (T-005) cut the search down to a leaf.
	recordDeleteSQL = "DELETE FROM subdomain_results " +
		"WHERE zone_id = $1::int2 AND domain_id = $2::int8 " +
		"RETURNING content_hash"

	// recordInsertSQL — the new rows by one operator: INSERT … SELECT
	// FROM unnest($n::type[]…) — every column is one array parameter
	// (the db.Update style, T-007), a chunk ≤ 126 rows (subdomains ≤ 63 ×
	// variants ≤ 2) does not hit the protocol limit. COPY is rejected:
	// the copying win shows on thousands of rows, while the unnest-INSERT is
	// one transaction operator with an exact SQL contract in the unit tests
	// (fantasizing with a CopyFrom source over a transaction is harder
	// than arrays). rendered = false (the render — T-067), rules_version =
	// NULL (the analyzer — T-068+), checked_at = now(). render_failed_at
	// is not in the list — DEFAULT NULL: the episode's rewrite (DELETE +
	// INSERT) resets the "render impossible" marker (T-210) — the new
	// content has the right to a render, a host that fixed itself will render at
	// the next episode.
	recordInsertSQL = "INSERT INTO subdomain_results (" +
		"zone_id, domain_id, subdomain_id, variant, scheme, " +
		"http_status, final_url, alias_kind, alias_subdomain_id, " +
		"addrs, cname, tls, redirects, headers, cookies, " +
		"rendered, content_hash, error, response_ms, checked_at, rules_version) " +
		"SELECT z.zone_id, z.domain_id, z.subdomain_id, z.variant, z.scheme, " +
		"z.http_status, z.final_url, z.alias_kind, z.alias_subdomain_id, " +
		"z.addrs, z.cname, z.tls, z.redirects, z.headers, z.cookies, " +
		"false, z.content_hash, z.error, z.response_ms, now(), NULL " +
		"FROM unnest(" +
		"$1::int2[], $2::int8[], $3::int2[], $4::int2[], $5::int2[], " +
		"$6::int2[], $7::text[], $8::int2[], $9::int2[], " +
		"$10::bytea[], $11::text[], $12::jsonb[], $13::jsonb[], $14::jsonb[], $15::jsonb[], " +
		"$16::bytea[], $17::int2[], $18::int4[]) " +
		"AS z(zone_id, domain_id, subdomain_id, variant, scheme, " +
		"http_status, final_url, alias_kind, alias_subdomain_id, " +
		"addrs, cname, tls, redirects, headers, cookies, " +
		"content_hash, error, response_ms)"
)

// RecordDB — the pool for writing the result: db.DB with transactions (the
// content.RefDB, ingest.CleanupDB style). The result write and the reference swap go
// in one transaction (spec 3.4); *pgxpool.Pool satisfies the interface.
type RecordDB interface {
	db.DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Recorder — writes the HTTP episode's result into subdomain_results,
// moves the domain into the final status and publishes q:analyze (T-058), and with
// a render queue wired — q:render over the SPA records with the
// render_pending flag on an overflow (T-066, renderq.go). Wired
// by the service build into Options.OnResult — right after ContentSaver (T-056:
// the new content already has +1 refcount from the Put) and before the XACK (spec 4.2).
// Concurrency-safe: the state is immutable after New.
type Recorder struct {
	pool RecordDB
	q    queue.Queue
	// render — the render queue (T-066): nil at NewRecorder — the render
	// stage is off (q:render is not published, the flag is not set); set by
	// NewRenderRecorder.
	render *RenderQueue
	// met — the stage's instruments (T-268): the crawler_db_deadlock_retried_total write
	// transaction 40P01-retry counter hook; nil — a no-op (isolated
	// builds/tests).
	met *Metrics
	log *slog.Logger
}

// NewRecorder builds the writer over a DB pool and a queue (production —
// *pgxpool.Pool and a queue under WatermarkPublisher, T-026). nil dependencies
// (the pool/the queue) — an error: a silent loss of a result/publication is invisible
// to the operator; met may be nil — without metrics. The production writer
// is assembled by NewRenderRecorder.
func NewRecorder(pool RecordDB, q queue.Queue, met *Metrics, log *slog.Logger) (*Recorder, error) {
	if pool == nil {
		return nil, errors.New("httpworker: result write: pool is not set")
	}
	if q == nil {
		return nil, errors.New("httpworker: result write: queue is not set")
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Recorder{pool: pool, q: q, met: met, log: log}, nil
}

// RecordAndPublish writes the domain result by one transaction (UPDATE
// domains + the render_pending flag on a q:render overflow, T-066 + DELETE
// of the old rows + the contents reference swap + INSERT of the new, spec 3.4/§5.5 item 7)
// and after the commit publishes q:analyze with one Result per
// subdomain×variant record (the T-024 format) and q:render over the SPA records with
// a free queue (T-066). The transaction runs under a 40P01 retry (T-268,
// recordTx): a deadlock is repeated entirely — the write's idempotency (spec 3.4)
// makes the repeat safe, and a q:http redelivery would cost a full
// HTTP episode (the Nov 7 incident — runbooks.md §2.8).
// Returns false without an error when the guard cut off the write (the domain is deleted
// — §8/§13.1, or the row is absent): this is a success, no publication is needed. An error —
// a failure of any step: the q:http delivery is not acknowledged and will come back
// with a repeated delivery (Processing, T-025); the repeated delivery will rewrite
// the result idempotently and decide the render question again (the flag is CASE-free:
// it is set only on an overflow, the renderq.go rework pass removes it).
func (r *Recorder) RecordAndPublish(ctx context.Context, res *DomainResult) (bool, error) {
	if res == nil {
		return false, nil // does not happen (processDelivery); the guard of direct calls
	}
	batch, err := newRecordBatch(res)
	if err != nil {
		return false, err
	}
	status := recordStatus(res)

	// T-066: the q:render SPA keys (§5.5 item 8) and the capacity decision — BEFORE
	// the transaction: on an overflow the render_pending flag is committed by the same
	// result transaction (§3.4), therefore the decision is needed before its opening.
	// The check is non-blocking (HasCapacity, not WatermarkPublisher):
	// waiting for the capacity would delay the XACK — the render must not delay
	// the other stages (§5.6).
	var renderKeys []queue.Result
	renderPending := false
	if r.render != nil {
		renderKeys = renderSPAResults(res)
		if len(renderKeys) > 0 {
			free, err := r.render.HasCapacity(ctx)
			if err != nil {
				// XLEN is unavailable — conservatively "no room": the
				// flag preserves the render tasks, the episode is not delayed;
				// a real Redis unavailability will still return
				// the delivery at the q:analyze publication below.
				r.log.Warn("q:render capacity not checked — the domain is flagged render_pending",
					"domain", res.Name, "err", err)
				free = false
			}
			renderPending = !free
		}
	}

	// The write transaction — under a 40P01 retry (T-268): the repeat is WHOLE,
	// the transaction is idempotent (the deletion guard §8/§13.1, the rewrite
	// DELETE+INSERT — spec 3.4), a q:http redelivery would cost a full
	// HTTP episode (the Nov 7 incident — runbooks.md §2.8). The publications —
	// after the ATTEMPT's commit and outside the retry: the failed transaction had none,
	// for the committed one the repeat does not run (a publication error is
	// not a deadlock).
	var (
		recorded bool
		swapped  int
	)
	if err := db.RetryDeadlock(ctx, db.DefaultDeadlockRetry, r.met.DBDeadlockRetried, func() error {
		var err error
		recorded, swapped, err = r.recordTx(ctx, res, batch, status, renderPending)
		return err
	}); err != nil {
		return recorded, err
	}
	if !recorded {
		return false, nil
	}

	// The q:analyze publication after the write (§5.5 item 9): Result points to
	// the row — it must exist by the moment of consumption. All the
	// domain's records: the analysis goes over every subdomain×variant result, the
	// render (T-067) will later replace the content and publish again — the stages' boundary,
	// not a filter here.
	for i := range batch.pub {
		if _, err := r.q.Publish(ctx, queue.StreamAnalyze, batch.pub[i]); err != nil {
			return true, fmt.Errorf("httpworker: domain %s: q:analyze publication (record %d/%d): %w",
				res.Name, i+1, len(batch.pub), err)
		}
	}

	// q:render over the SPA records (T-066, §5.5 item 8): after the commit — as
	// q:analyze, Result points to the row. Only with a free queue:
	// the overflow is already closed by the flag in the transaction. The publication bypassing
	// WatermarkPublisher (T-026) — the capacity was checked above, waiting here
	// would delay the XACK (§5.6).
	if len(renderKeys) > 0 && !renderPending {
		if err := r.render.Publish(ctx, renderKeys); err != nil {
			return true, fmt.Errorf("httpworker: domain %s: %w", res.Name, err)
		}
	}

	r.log.Debug("HTTP result written",
		"domain", res.Name, "status", status.String(), "records", len(batch.zone),
		"swapped_refs", swapped, "cutoff", res.Cutoff,
		"render_pending", renderPending, "q_render", len(renderKeys))
	return true, nil
}

// recordTx — ONE attempt of the result write transaction (T-058; the 40P01
// retry point — T-268): the FOR UPDATE pre-read of the domain's row → the UPDATE
// of the final status (+ the render_pending flag on a q:render overflow,
// T-066) → DELETE of the old rows + the contents reference swap (T-044) → INSERT
// of the new → the tail counter deltas (the zones T-178, the render debt T-175,
// the day/buckets T-179/T-182/T-202/T-203/T-204) → Commit. Returns
// false without an error when the write is not needed (the row is absent or the domain is deleted —
// §8/§13.1); swapped — the number of removed content references (for the journal).
// The rollback of an interrupted attempt — a defer Rollback (after the Commit — a no-op):
// the transaction is repeated from a clean slate, there are no half-done states.
func (r *Recorder) recordTx(ctx context.Context, res *DomainResult, batch *recordBatch, status model.Status, renderPending bool) (bool, int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, 0, fmt.Errorf("httpworker: domain %s: result write: transaction: %w", res.Name, err)
	}
	// The rollback unconditionally: after the Commit — a no-op (the T-044 style).
	defer func() { _ = tx.Rollback(ctx) }()

	// The old row values as the first step (T-178, incremental-counters spec §6.4): FOR UPDATE
	// locks the domain's row (the role the UPDATE used to play),
	// the status/check_state give the zone counters' deltas. An absent row and
	// deleted — the same graceful outcomes the UPDATE's RowsAffected = 0
	// used to give: an empty result — a rollback of an empty transaction, false without
	// an error (§8/§13.1: a domain deleted during the check gets no
	// result written).
	var oldStatus, oldState int16
	err = tx.QueryRow(ctx, recordSelectStateSQL, res.ZoneID, res.DomainID).Scan(&oldStatus, &oldState)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			r.log.Debug("result not written — the domain is absent",
				"domain", res.Name, "zone_id", res.ZoneID, "domain_id", res.DomainID)
			return false, 0, nil
		}
		return false, 0, fmt.Errorf("httpworker: domain %s: result write: row read: %w", res.Name, err)
	}
	if model.Status(oldStatus) == model.StatusDeleted {
		r.log.Debug("result not written — the domain is deleted",
			"domain", res.Name, "zone_id", res.ZoneID, "domain_id", res.DomainID)
		return false, 0, nil
	}

	// The domain's outcome: the row is locked by the pre-read and not deleted — the
	// UPDATE's status guard is preserved (the second line of defense against manual
	// edits between the operators). The fourth parameter — the dead_host_cutoff
	// bit of the last episode (T-164).
	tag, err := tx.Exec(ctx, recordDomainSQL, res.ZoneID, res.DomainID, int16(status), res.Cutoff)
	if err != nil {
		return false, 0, fmt.Errorf("httpworker: domain %s: result write: status: %w", res.Name, err)
	}
	if tag.RowsAffected() != 1 {
		// The row is locked and not deleted — unreachable (a guard
		// for direct calls/manual edits); a silent skip of the deltas would make
		// the counters a lie.
		return false, 0, fmt.Errorf("httpworker: domain %s: result write: status: %d rows affected", res.Name, tag.RowsAffected())
	}
	// The final UPDATE's RowsAffected — the daily checks counter's delta
	// (T-179, TZ_slow_done_today §5): not the number of the chunk's
	// subdomain_results records (a domain with five subdomain×variants — one episode, +1); the
	// deleted guard/the absent row cut off the write above — only the written
	// results reach this point.
	recorded := tag.RowsAffected()

	// The render_pending flag — by the same transaction (T-066, §5.6): the decision
	// belongs to this result (the q:render overflow at the moment
	// of the write), a commit failure rolls back both the rows and the flag. The bit's
	// guard
	// (T-175): RowsAffected 1 — a real 0→1 transition (the debt counter +1
	// here as well, by the same transaction, renderBumpDebtCounter), 0 — the flag is
	// already set (a repeated episode delivery while the rework pass has
	// not cleared it): the OR semantics is idempotent, the counter is incremented only on
	// the transitions. The domain is locked by the UPDATE above — a zero from the SET
	// happens only from an already set bit; an absent counter row — a separate
	// step error below, rolling back the whole transaction.
	// The counter shift runs as the last operator before the commit
	// (bumpDebt below): the counter's row — the single serialization point of all
	// flagging transactions (the T-175 review, P3-4); holding its lock through the
	bumpDebt := false
	if renderPending {
		tag, err := tx.Exec(ctx, renderSetPendingSQL, res.ZoneID, res.DomainID)
		if err != nil {
			return false, 0, fmt.Errorf("httpworker: domain %s: render_pending flag: %w", res.Name, err)
		}
		if tag.RowsAffected() > 1 {
			return false, 0, fmt.Errorf("httpworker: domain %s: render_pending flag: %d rows affected", res.Name, tag.RowsAffected())
		}
		bumpDebt = tag.RowsAffected() == 1
	}

	// The rewrite (§3.5): the domain's old rows go entirely, their content
	// references are removed right here — the row and the counter do not diverge (T-044).
	// (T-044).
	oldHashes, err := recordDeleteOld(ctx, tx, res.ZoneID, res.DomainID)
	if err != nil {
		return false, 0, err
	}
	for _, h := range oldHashes {
		if err := content.SwapRefTx(ctx, tx, h, content.Hash{}); err != nil {
			return false, 0, fmt.Errorf("httpworker: domain %s: %w", res.Name, err)
		}
	}

	// inserted — this transaction's subdomain_results rows: the delta
	// of the results: hourly throughput bucket (T-203); the episodes without records
	// (silent checks) do not touch the bucket.
	var inserted int64

	if len(batch.zone) > 0 {
		tag, err := tx.Exec(ctx, recordInsertSQL, batch.args()...)
		if err != nil {
			return false, 0, fmt.Errorf("httpworker: domain %s: result write: rows: %w", res.Name, err)
		}
		if int(tag.RowsAffected()) != len(batch.zone) {
			return false, 0, fmt.Errorf("httpworker: domain %s: inserted %d of %d rows", res.Name, tag.RowsAffected(), len(batch.zone))
		}
		// The episode's rows — the hourly throughput bucket's delta (T-203): all
		// the inserted rows carry checked_at = the transaction's now() — the
		// bucket's hour is one (date_trunc in the increment below).
		inserted = tag.RowsAffected()
	}
	// The zone counters' delta — by the same transaction (T-178, incremental-counters spec §6.4): by
	// the pre-read's old check_state (an old done — a duplicate delivery, zeros).
	// The last steps before the commit — the zone's row is locked for
	// a minimum (like the debt counter's row below, the T-175 review, P3-4).
	shiftZone := func() error {
		var dPending, dQueued, dChecked int64
		switch model.CheckState(oldState) {
		case model.CheckStatePending:
			dPending, dChecked = -1, 1
		case model.CheckStateQueued:
			dQueued, dChecked = -1, 1
		case model.CheckStateDone:
			// A duplicate delivery: a repeated result write already led
			// the domain out of queued at the first pass — no deltas (incremental-counters spec §6.4).
			return nil
		default:
			// A code outside the enum — garbage in the schema; a silent skip
			// of the deltas would make the counters a lie, the error is louder.
			return fmt.Errorf("old state %d is outside the check_state enum", oldState)
		}
		tag, err := tx.Exec(ctx, recordZoneShiftSQL, res.ZoneID, dPending, dQueued, dChecked)
		if err != nil {
			return fmt.Errorf("zone counters: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("zone counters: %d rows affected (the zone row %d is required)", tag.RowsAffected(), res.ZoneID)
		}
		return nil
	}
	if err := shiftZone(); err != nil {
		return false, 0, fmt.Errorf("httpworker: domain %s: %w", res.Name, err)
	}
	// The debt counter — the transaction's last step (the same commit, §3.4:
	// a rollback rolls back the pair; the counter row's lock window — a minimum).
	if bumpDebt {
		if err := renderBumpDebtCounter(ctx, tx, 1); err != nil {
			return false, 0, fmt.Errorf("httpworker: domain %s: %w", res.Name, err)
		}
	}
	// The daily checks counter — the same "tail" step (T-179):
	// the last operator before the commit, after the zone and the render debt —
	// the current day's row serializes all the result writes (like
	// render_pending, the T-175 P3-4 review), holding its lock through the
	// whole heavy part of the transaction is pointless; the "render_pending →
	// checks:" order is shared with the monitor reconciliation (the zones → the scalars → the
	// day row) — without lock cycles. The delta — the UPDATE's RowsAffected above
	// (the != 1 check guarantees 1; the parameter kept by the contract
	// "$1 — RowsAffected", spec §5). A rollback rolls back the result and the delta
	// together (§3.4); an XACK failure will return the delivery — a repeated write
	// will add one more event (§5.1, the reconciliation levels it out).
	if err := recordBumpChecksCounter(ctx, tx, recorded); err != nil {
		return false, 0, fmt.Errorf("httpworker: domain %s: %w", res.Name, err)
	}
	// The temp_error share numerator (T-182) — right after the checks denominator:
	// the delta — only at a temp_error episode (the final UPDATE's
	// verdict is known above), the other outcomes do not touch the day's row — the
	// temp_error transactions' serialization locks do not touch the alive/no_http flow.
	// The "checks: → temp_error:" order is shared with the reconciliation
	// pass (the zones → the scalars → the day rows in this same order) — without lock
	// cycles; a rollback rolls back the result and both deltas together (§3.4).
	if status == model.StatusTempError {
		if err := recordBumpTempErrCounter(ctx, tx, recorded); err != nil {
			return false, 0, fmt.Errorf("httpworker: domain %s: %w", res.Name, err)
		}
		// The maturity bucket (T-202) — closes the delta chain: the last
		// among the counter rows (the reconciliation order "… → temp_error: →
		// retry_mature:" — without lock cycles). Today's episode
		// matures toward midnight — today's bucket is incremented both by the
		// DNS stage (dnsworker/result.go) and by this operator: both write
		// the same name from current_date of the same write transaction.
		if err := recordBumpRetryMatureCounter(ctx, tx, recorded); err != nil {
			return false, 0, fmt.Errorf("httpworker: domain %s: %w", res.Name, err)
		}
	}
	// The reanalysis tails' hourly bucket (T-204) — after the retry bucket,
	// before the throughput bucket (the reconciliation order, the
	// recordReanalyzeBumpSQL comment): all the inserted rows — the
	// unanalyzed tails of the now() hour (= the hour of the rows' checked_at); a zero delta (an
	// episode without records) — the operator does not run at all.
	if inserted > 0 {
		if err := recordBumpReanalyzeCounter(ctx, tx, inserted); err != nil {
			return false, 0, fmt.Errorf("httpworker: domain %s: %w", res.Name, err)
		}
	}
	// The hourly throughput bucket (T-203) — the finale of the counter delta chain:
	// the transaction's last row (the reconciliation order
	// "… → retry_mature: → reanalyze: → rendered: → results:", rendered:
	// is kept by the renderworker — here one results: point); the delta — the inserted
	// episode rows, the hour — the transaction's now() (= the hour of the rows' checked_at).
	// A zero delta (an episode without records) — the operator does not run at all —
	// empty upsert passes do not warm the bucket's lock.
	if inserted > 0 {
		if err := recordBumpResultsCounter(ctx, tx, inserted); err != nil {
			return false, 0, fmt.Errorf("httpworker: domain %s: %w", res.Name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, 0, fmt.Errorf("httpworker: domain %s: result write: commit: %w", res.Name, err)
	}
	return true, len(oldHashes), nil
}

// recordDeleteOld deletes the domain's old rows and returns the aggregated
// content_hash values for the reference swap (the sort — a deterministic
// operator order; a hash's multiplicity = the number of referencing rows — a repeated
// occurrence gives a repeated −1, SwapRefTx is called for each).
func recordDeleteOld(ctx context.Context, tx pgx.Tx, zoneID int16, domainID int64) ([]content.Hash, error) {
	rows, err := tx.Query(ctx, recordDeleteSQL, zoneID, domainID)
	if err != nil {
		return nil, fmt.Errorf("httpworker: domain %d/%d: deleting old rows: %w", zoneID, domainID, err)
	}
	defer rows.Close()

	counts := make(map[content.Hash]int)
	for rows.Next() {
		var hash []byte
		if err := rows.Scan(&hash); err != nil {
			return nil, fmt.Errorf("httpworker: domain %d/%d: deleting old rows: scan: %w", zoneID, domainID, err)
		}
		if hash == nil {
			continue // a row without content — nothing to remove
		}
		var h content.Hash
		copy(h[:], hash)
		counts[h]++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("httpworker: domain %d/%d: deleting old rows: %w", zoneID, domainID, err)
	}

	out := make([]content.Hash, 0, len(counts))
	for h, n := range counts {
		for i := 0; i < n; i++ {
			out = append(out, h)
		}
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i][:], out[j][:]) < 0 })
	return out, nil
}

// recordBumpChecksCounter — the daily checks counter increment in the
// OPEN result write transaction (T-179): delta — the final
// domain UPDATE's RowsAffected (1 per episode). The upsert creates the day's row
// itself (the first increment after midnight) — RowsAffected is guaranteed 1,
// the check — a guard for direct calls with someone else's SQL (a silent loss
// of the delta would understate the throughput). Returning with an error rolls back
// the caller's transaction — the result and the counter change only together (§3.4).
func recordBumpChecksCounter(ctx context.Context, tx pgx.Tx, delta int64) error {
	tag, err := tx.Exec(ctx, recordChecksBumpSQL, delta)
	if err != nil {
		return fmt.Errorf("daily checks counter: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("daily checks counter: %d rows affected (the day row upsert must affect 1)",
			tag.RowsAffected())
	}
	return nil
}

// recordBumpTempErrCounter — the daily temp_error numerator increment in
// the OPEN result write transaction (T-182): delta — the final
// domain UPDATE's RowsAffected (the caller guarantees the temp_error status,
// the verdict is known before the transaction). The upsert creates the day's row itself (the first
// temp_error episode after midnight) — RowsAffected is guaranteed 1,
// the check — a guard for direct calls with someone else's SQL (a silent loss
// of the delta would understate the share). Returning with an error rolls back
// the caller's transaction — the result and the counter change only together (§3.4).
func recordBumpTempErrCounter(ctx context.Context, tx pgx.Tx, delta int64) error {
	tag, err := tx.Exec(ctx, recordTempErrBumpSQL, delta)
	if err != nil {
		return fmt.Errorf("daily temp_error numerator: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("daily temp_error numerator: %d rows affected (the day row upsert must affect 1)",
			tag.RowsAffected())
	}
	return nil
}

// recordBumpRetryMatureCounter — the temp_error maturity bucket increment
// in the OPEN result write transaction (T-202): delta — the final
// domain UPDATE's RowsAffected (the caller guarantees the temp_error status).
// The upsert creates the day's bucket itself (the first temp_error episode after
// midnight) — RowsAffected is guaranteed 1, the check — a guard
// for direct calls with someone else's SQL (a silent delta loss would keep the
// retry guard closed until the reconciliation). Returning with an error rolls back
// the caller's transaction — the result and the bucket change only together (§3.4).
// (§3.4).
func recordBumpRetryMatureCounter(ctx context.Context, tx pgx.Tx, delta int64) error {
	tag, err := tx.Exec(ctx, recordRetryMatureBumpSQL, delta)
	if err != nil {
		return fmt.Errorf("temp_error maturity bucket: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("temp_error maturity bucket: %d rows affected (the day bucket upsert must affect 1)",
			tag.RowsAffected())
	}
	return nil
}

// recordBumpReanalyzeCounter — the unanalyzed tails' bucket increment
// in the OPEN result write transaction (T-204): delta —
// the INSERT chunk's RowsAffected (all the rows with rules_version = NULL).
// The upsert creates the hour's bucket itself (the first inserted row after
// the hour boundary) — RowsAffected is guaranteed 1, the check — a guard
// for direct calls with someone else's SQL (a silent delta loss would understate
// the tick's guard window). Returning with an error rolls back
// the caller's transaction — the result and the bucket change only together (§3.4).
func recordBumpReanalyzeCounter(ctx context.Context, tx pgx.Tx, delta int64) error {
	tag, err := tx.Exec(ctx, recordReanalyzeBumpSQL, delta)
	if err != nil {
		return fmt.Errorf("reanalysis tails bucket: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("reanalysis tails bucket: %d rows affected (the hour bucket upsert must affect 1)",
			tag.RowsAffected())
	}
	return nil
}

// recordBumpResultsCounter — the results hourly throughput bucket increment in
// the OPEN result write transaction (T-203): delta — the number of the
// episode's subdomain_results rows (the INSERT chunk's RowsAffected; the rows' checked_at
// — the transaction's now(), the bucket's hour is computed by the DB with the same now()). The upsert itself
// creates the hour's bucket (the first inserted row after the hour boundary) —
// RowsAffected is guaranteed 1, the check — a guard for direct calls with
// someone else's SQL (a silent delta loss would understate the throughput). Returning with
// an error rolls back the caller's transaction — the result and the bucket
// change only together (§3.4).
func recordBumpResultsCounter(ctx context.Context, tx pgx.Tx, delta int64) error {
	tag, err := tx.Exec(ctx, recordResultsBumpSQL, delta)
	if err != nil {
		return fmt.Errorf("hourly rate bucket: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("hourly rate bucket: %d rows affected (the hour bucket upsert must affect 1)",
			tag.RowsAffected())
	}
	return nil
}

// recordStatus — the HTTP episode's final domain status (§5.5 item 9, §7).
func recordStatus(res *DomainResult) model.Status {
	switch {
	case res.HasRecords():
		return model.StatusAlive
	case res.AnyTimeout():
		return model.StatusTempError
	default:
		return model.StatusNoHTTP
	}
}

// recordBatch — the new rows' unnest chunk: the parallel column arrays
// (one column — one recordInsertSQL parameter, the T-007 style) and the
// q:analyze messages (one per record). The NULL columns — the arrays' nil elements
// (pgx encodes them into NULL); jsonb — the JSON text (text → jsonb is cast
// on the server; PostgreSQL does not cast bytea into jsonb).
type recordBatch struct {
	zone       []int16
	domain     []int64
	subdomain  []int16
	variant    []int16
	scheme     []int16
	httpStatus []*int16
	finalURL   []*string
	aliasKind  []int16
	aliasSub   []*int16
	addrs      [][]byte
	cname      []*string
	tls        []*string
	redirects  []*string
	headers    []*string
	cookies    []*string
	hash       [][]byte
	errCode    []*int16
	responseMS []*int32

	pub []queue.Result
}

func (b *recordBatch) args() []any {
	return []any{
		b.zone, b.domain, b.subdomain, b.variant, b.scheme,
		b.httpStatus, b.finalURL, b.aliasKind, b.aliasSub,
		b.addrs, b.cname, b.tls, b.redirects, b.headers, b.cookies,
		b.hash, b.errCode, b.responseMS,
	}
}

// newRecordBatch assembles the chunk from the domain result and validates the records
// (the model codes, the subdomain×variant PK uniqueness): an error — before opening
// the transaction, the DB is not touched.
func newRecordBatch(res *DomainResult) (*recordBatch, error) {
	if res.ZoneID <= 0 || res.DomainID <= 0 {
		return nil, fmt.Errorf("httpworker: result write: zone %d, domain %d — the identifiers must be positive", res.ZoneID, res.DomainID)
	}
	n := res.RecordCount()
	b := &recordBatch{
		zone: make([]int16, 0, n), domain: make([]int64, 0, n),
		subdomain: make([]int16, 0, n), variant: make([]int16, 0, n),
		scheme: make([]int16, 0, n), httpStatus: make([]*int16, 0, n),
		finalURL: make([]*string, 0, n), aliasKind: make([]int16, 0, n),
		aliasSub: make([]*int16, 0, n), addrs: make([][]byte, 0, n),
		cname: make([]*string, 0, n), tls: make([]*string, 0, n),
		redirects: make([]*string, 0, n), headers: make([]*string, 0, n),
		cookies: make([]*string, 0, n), hash: make([][]byte, 0, n),
		errCode: make([]*int16, 0, n), responseMS: make([]*int32, 0, n),
		pub: make([]queue.Result, 0, n),
	}
	seen := make(map[recordKey]struct{}, n)
	for i := range res.Subdomains {
		sub := &res.Subdomains[i]
		addrs := recordPackAddrs(sub.Addrs)
		for j := range sub.Records {
			rec := &sub.Records[j]
			key := recordKey{sub: sub.SubdomainID, variant: rec.Variant}
			if _, dup := seen[key]; dup {
				return nil, fmt.Errorf("httpworker: domain %s: subdomain %d, variant %d: a duplicate subdomain_results key", res.Name, sub.SubdomainID, int16(rec.Variant))
			}
			seen[key] = struct{}{}
			// The stage boundary (T-266, TZ-fsm spec §3 FR-12): the record's content part
			// invariants (AliasNone ⇔ alias_id,
			// ContentNotStored ⇒ hash=nil and the others — the R1–R8 matrix)
			// are checked by the automaton's centralized validator
			// (schemestate.go), formerly — recordValidateRow.
			if err := rec.validate(); err != nil {
				return nil, fmt.Errorf("httpworker: domain %s: %w", res.Name, err)
			}

			var httpStatus *int16
			if rec.HTTPStatus > 0 {
				v := int16(rec.HTTPStatus)
				httpStatus = &v
			}
			var finalURL *string
			if rec.FinalURL != "" {
				finalURL = &rec.FinalURL
			}
			var cname *string
			if sub.CNAME != "" {
				cname = &sub.CNAME
			}
			tls, err := recordJSON(rec.TLS != nil, rec.TLS)
			if err != nil {
				return nil, fmt.Errorf("httpworker: domain %s: subdomain %s: TLS: %w", res.Name, sub.Hostname, err)
			}
			redirects, err := recordJSON(len(rec.Redirects) > 0, rec.Redirects)
			if err != nil {
				return nil, fmt.Errorf("httpworker: domain %s: subdomain %s: the redirect chain: %w", res.Name, sub.Hostname, err)
			}
			headers, err := recordJSON(len(rec.Header) > 0, rec.Header)
			if err != nil {
				return nil, fmt.Errorf("httpworker: domain %s: subdomain %s: headers: %w", res.Name, sub.Hostname, err)
			}
			cookies, err := recordCookies(rec.Cookies)
			if err != nil {
				return nil, fmt.Errorf("httpworker: domain %s: subdomain %s: cookies: %w", res.Name, sub.Hostname, err)
			}
			var errCode *int16
			if rec.ErrClass != ErrNone {
				v := int16(rec.ErrClass)
				errCode = &v
			}
			var hash []byte
			if rec.ContentHash != nil {
				hash = rec.ContentHash[:]
			}
			responseMS := int32(rec.ResponseMS)

			b.zone = append(b.zone, res.ZoneID)
			b.domain = append(b.domain, res.DomainID)
			b.subdomain = append(b.subdomain, sub.SubdomainID)
			b.variant = append(b.variant, int16(rec.Variant))
			b.scheme = append(b.scheme, int16(rec.Scheme))
			b.httpStatus = append(b.httpStatus, httpStatus)
			b.finalURL = append(b.finalURL, finalURL)
			b.aliasKind = append(b.aliasKind, int16(rec.AliasKind))
			b.aliasSub = append(b.aliasSub, rec.AliasSubdomainID)
			b.addrs = append(b.addrs, addrs)
			b.cname = append(b.cname, cname)
			b.tls = append(b.tls, tls)
			b.redirects = append(b.redirects, redirects)
			b.headers = append(b.headers, headers)
			b.cookies = append(b.cookies, cookies)
			b.hash = append(b.hash, hash)
			b.errCode = append(b.errCode, errCode)
			b.responseMS = append(b.responseMS, &responseMS)
			b.pub = append(b.pub, queue.Result{
				ZoneID:      res.ZoneID,
				DomainID:    res.DomainID,
				SubdomainID: sub.SubdomainID,
				Variant:     rec.Variant,
				Name:        res.Name,
				Fresh:       true, // a new HTTP episode — the detection history is written (T-118)
			})
		}
	}
	return b, nil
}

// recordKey — the subdomain_results PK uniqueness key.
type recordKey struct {
	sub     int16
	variant model.Variant
}

// recordJSON serializes a value into a jsonb column when it exists
// (some — the sign); an array's nil element → NULL. The values with JSON tags:
// TLSInfo (T-050), RedirectStep (T-048); http.Header — the canonical header
// names as is.
func recordJSON(some bool, value any) (*string, error) {
	if !some {
		return nil, nil
	}
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	s := string(b)
	return &s, nil
}

// recordCookie — a cookie of the final response for subdomain_results.cookies
// (jsonb, spec 5.5 item 7): the name and the value — entirely at the analyzer rules' disposal
// (spec 5.8: the cookie name/value), the attributes — the domain/path/expiry — for a rainy day;
// the snake_case tags (the TLSInfo style).
type recordCookie struct {
	Name     string `json:"name"`
	Value    string `json:"value,omitempty"`
	Path     string `json:"path,omitempty"`
	Domain   string `json:"domain,omitempty"`
	Expires  string `json:"expires,omitempty"` // RFC 3339, UTC
	MaxAge   int    `json:"max_age,omitempty"`
	Secure   bool   `json:"secure,omitempty"`
	HTTPOnly bool   `json:"http_only,omitempty"`
	SameSite string `json:"same_site,omitempty"`
}

// recordSameSite — the SameSite attribute's name (the jsonb string codes): the
// default and the zero value — the attribute is not set (an empty string, omitempty).
func recordSameSite(s http.SameSite) string {
	switch s {
	case http.SameSiteLaxMode:
		return "lax"
	case http.SameSiteStrictMode:
		return "strict"
	case http.SameSiteNoneMode:
		return "none"
	default:
		return ""
	}
}

// recordCookies serializes the final response's Set-Cookie (spec 5.5 item 7):
// nil at an empty set → NULL.
func recordCookies(cookies []*http.Cookie) (*string, error) {
	if len(cookies) == 0 {
		return nil, nil
	}
	out := make([]recordCookie, 0, len(cookies))
	for _, c := range cookies {
		rc := recordCookie{
			Name:     c.Name,
			Value:    c.Value,
			Path:     c.Path,
			Domain:   c.Domain,
			MaxAge:   c.MaxAge,
			Secure:   c.Secure,
			HTTPOnly: c.HttpOnly,
			SameSite: recordSameSite(c.SameSite),
		}
		if !c.Expires.IsZero() {
			rc.Expires = c.Expires.UTC().Format(time.RFC3339)
		}
		out = append(out, rc)
	}
	return recordJSON(true, out)
}

// recordPackAddrs packs the subdomain's addresses into the subdomain_results bytea.
// addrs (§6.1 "the packed A/AAAA", the format — the stage laying down the row):
// 16 octets per address, IPv4 — in the v4-mapped form (::ffff:a.b.c.d,
// netip.Addr.As16) — the mixed A/AAAA composition is not ambiguous (raw 4/16
// octets would give length collisions), the bytea length is always a multiple of 16. The order
// — the q:http tasks (A, then AAAA, the DNS stage's answer order, T-037).
func recordPackAddrs(addrs []netip.Addr) []byte {
	if len(addrs) == 0 {
		return nil
	}
	out := make([]byte, 0, 16*len(addrs))
	for _, a := range addrs {
		b16 := a.Unmap().As16()
		out = append(out, b16[:]...)
	}
	return out
}

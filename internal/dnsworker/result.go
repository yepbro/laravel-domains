// Recording the DNS episode result and the dns-worker working loop
// (T-039; spec 5.4 items 4–5, §7, 6.1, 6.2, 3.4). The file closes the
// stage pipeline: an episode (CheckApex T-036 → CheckSubdomains T-037 →
// DetectWildcard T-038) turns into a domains row and — when addresses
// are found — into a q:http task (the T-024 format), and the Run loop
// assembles all of that on the consumption of q:dns batches
// (T-024/T-025). The service assembly — cmd/dns-worker.
//
// Statuses per §7 (the decisions fixed per the spec text):
//
//   - an apex NXDOMAIN → status = not_delegated, the episode done
//     (§5.4 item 1, RFC 8020): subdomains were not checked, the mask is
//     zeroed — only the last result is stored (§3.5);
//   - an apex SERVFAIL/timeout → status = temp_error, done: the
//     scheduler will schedule the retry (T-031 by
//     temp_error_retry_after);
//   - addresses present (the mask without wildcard exclusions is
//     non-empty, §5.4 item 5) → a q:http task, done; a terminal status
//     is NOT written: alive/no_http — the HTTP stage outcome (§6.2),
//     and the domain keeps its previous status (new for the first
//     check) until that stage finishes. An analogue of T-030: the
//     status stores the outcome of the past check, check_state — the
//     state of the loop;
//   - no addresses → status = delegated_no_address, done ("an NS/an
//     answer is present, but no A/AAAA at any subdomain", §5.4 item 5).
//     The corner case "only wildcard-synthesized names resolve" lands
//     here too: all DNS-live subdomains are explained by the wildcard
//     (except the apex/www, T-038), publishing an empty q:http is not
//     allowed (the T-024 contract) — there are no HTTP-checkable names,
//     which in meaning is exactly "no addresses"; dns_mask still
//     honestly stores the resolving bits (§5.4 item 4: a bit is "a
//     resolving subdomain", the wildcard filter excludes from the HTTP
//     check, not from the mask);
//   - a subdomain resolver failure (transport after retries, SERVFAIL,
//     other codes besides NOERROR/NXDOMAIN) with addresses found — the
//     episode finishes with a q:http task on the partial outcome
//     (T-037: an outcome per subdomain separately; the lost name will
//     be covered by the regular T-030 recheck), without addresses —
//     temp_error: "no addresses" cannot be classified from a silent
//     resolver (a false terminal status), and the T-031 retry will
//     recheck the episode. Defects of the zone itself — a CNAME loop
//     and a depth overrun (ErrSubdomCNAMELoop/Depth) — are not the
//     peers of failures: the resolver answered, the domain is what is
//     broken; the subdomain outcome is "no addresses" without
//     temp_error (the sentinel errors are parsed with errors.Is,
//     T-037).
//
// The "publish q:http → write done" order (§3.4, in the spirit of
// T-028: PostgreSQL is the source of truth, done is the episode's
// commit point):
//
//   - the q:http task is published BEFORE the domain is moved to done.
//     A write failure after the publication leaves the domain queued
//     and the q:dns delivery without XACK (Processing, T-025): a
//     redelivery will repeat the episode, but the task is not lost. The
//     reverse order would leave the domain done without a q:http task
//     forever on a publication failure (the HTTP stage would never
//     start, the currency filter drops done rows, only the T-030
//     recheck would fix it — half a year later) — rejected by the
//     DNS-04 review and rechecked for Q-02: RecoverLoop returns only
//     the stuck deliveries, it cannot change a row's state;
//   - the unit of retry is narrowed to the domain (DNS-04, T-108):
//     right after a successful q:http publication the episode sets the
//     http_published staging mark (bit 2 of domains.flags, the
//     model.Flags model — like render_pending T-066; without a
//     migration: spec 6.1 leaves the flags bits open, "…"). A batch
//     redelivery reads the bit with the currency filter and does NOT
//     publish q:http again — it only reaches the write (the DNS episode
//     repeats: the result was not stored anywhere, §3.5, and q:http
//     tasks have no deduplication on the consumer side — a duplicate =
//     a full repeated HTTP check). Before T-108, a single write failure
//     cost up to redis.max_attempts × |batch| repeated episodes and
//     q:http duplicates for the already published domains; now a
//     duplicate is possible only in the window between the XADD and
//     setting the bit (a worker crash or a DB failure in one UPDATE)
//     and in the cross-episode lease races (they existed before T-108
//     too). The formalized bit specification (states, 4 transitions,
//     the duplicate window) — the table at resultMarkPublishedSQL
//     (T-257, spec tasks/TZ-fsm.md §3 FR-3);
//   - clearing the bit: by the result write (the same UPDATE — done ⇒
//     the bit is clean) and by the scheduler enqueue (pending → queued,
//     T-028: every new episode starts with a clean bit — the single
//     reset point also covers the lease return of T-029, the
//     resurrection of T-018 and the recheck of T-030 without editing
//     every pass). On a done/pending row the bit may stay set until the
//     next enqueue — only the batch filter reads it (done is cut off),
//     outside the episode the bit has no value;
//   - setting the bit is non-fatal: an UPDATE failure after the
//     publication does not drop the episode (the result write matters
//     more); in that case a redelivery publishes a duplicate — the
//     price of the window, not a loss;
//   - from the invariant "done ⇒ the task is published" the currency
//     filter derives idempotency: a batch redelivery processes only the
//     unfinished domains (not done and not deleted); the already
//     recorded ones are skipped without a DNS exchange and without a
//     q:http duplicate.
//
// Wildcard detection (T-038) does not drop the episode on a failure:
// continuing without filtering — a few extra HTTP requests to the stub;
// temp_error for the domain is not needed (the apex is already
// classified, T-036).

package dnsworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"crawler/internal/db"
	"crawler/internal/dnsclient"
	"crawler/internal/metrics"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// The result SQL. One statement per domain (the guard style of T-029/
// T-030): the guards recheck the row state at the moment of locking —
// a domain deleted during the check is not overwritten (§13.1, §8:
// "the result is not written (a status check at write time)"), and an
// already recorded result is not overwritten by a redelivery (the
// idempotency of spec 3.4). Schema codes — as literals, not data (the
// dispatchZoneSQL style, T-027); last_checked_at = now() is the DB
// server time (T-028), the date column receives the date by the
// assignment cast.
//
// The check_state transitions here are rows R2/R3/R4/R13 of the machine
// specification (the single point of truth:
// internal/scheduler/checkstate.go, T-255).
//
// # The DNS stage decision table of the status machine (T-256, the TZ-fsm spec §3 FR-2)
//
// The common part (all stages and writers) — one table in
// internal/model/enums.go at the model.Status codes; here — the
// expansion of the DNS branches (O2–O6, O10 of the common table) bound
// to the statements and inputs. The decision is made by the pure
// function DecideEpisode (the table is its contract, covered by the
// TestResultDecideEpisode unit test):
//
//	| Branch | Episode input | EpisodeResult decision | Write statement |
//	|-------|--------------|----------------------|-----------------|
//	| S1 (O2) | apex NXDOMAIN | SetStatus: not_delegated; mask empty, subdomains not checked (RFC 8020) | resultUpdateStatusSQL |
//	| S2 (O3) | apex SERVFAIL/timeout | SetStatus: temp_error; mask empty, subdomains not checked | resultUpdateStatusSQL |
//	| S3 (O6) | the HTTP set is non-empty after the wildcard filter (a partial subdomain failure will not sink the task, T-037) | SetStatus = false: the status will be decided by the HTTP stage (O7–O9), the domain keeps its previous one (the first check — new); the q:http publication | resultUpdateKeepStatusSQL |
//	| S4 (O4) | the HTTP set is empty, no resolver failures (all NOERROR/NXDOMAIN — a subdomain NXDOMAIN is authoritative; zone defects — a CNAME loop/depth — are not a failure; wildcard-only — "no addresses") | SetStatus: delegated_no_address | resultUpdateStatusSQL |
//	| S5 (O5) | the HTTP set is empty, there was a resolver failure (transport after retries or a code outside NOERROR/NXDOMAIN) | SetStatus: temp_error — "no addresses" is not derived from a silent resolver (a false terminal status) | resultUpdateStatusSQL |
//	| S6 (O10) | poison row: ErrEmptyDomain (an empty name) | SetStatus: invalid — outside DecideEpisode, the diagnosis is set by RecordInvalid (T-109) | resultInvalidSQL |
//
// The asymmetries are recorded in the common table (model/enums.go):
// temp_error without addresses ripens toward the scheduler's A1
// finalization (retry); the other branches are terminal by themselves.
// The "stuck status" anomaly — S3 without an HTTP outcome
// (status = new ∧ check_state = done) — is observable by the gauge of
// FR-2.2 (the specification — model/enums.go).
//
// Zone counter deltas (T-178, review P1-1): the DNS stage is the main
// queued/pending → done transition of the pipeline; the statement keeps
// domains_queued/domains_pending −n and domains_checked +n by the old
// row state (the incremental-counters spec §6.1.2: the check_state <>
// done predicate does not pin the old state — the FOR UPDATE pre-read
// in the CTE s of the same statement-transaction). A domain in pending
// (the lease returned it mid-episode) correctly yields pending −1 /
// checked +1. RowsAffected of the zone counter is not checked
// separately: zone rows are created by ingest before the domains
// (EnsureZone, T-014).
//
// temp_attempts (T-031): the retry counter is reset to 0 on every
// non-temp_error outcome — a new temporary-error episode starts with a
// clean counter (the T-031 contract is assigned to the DNS stage);
// temp_error keeps the accumulated value — the scheduler increments it
// when scheduling the retry.
//
// queued_at = NULL ends the lease together with the episode: a non-null
// stamp remains only at queued rows (the T-018/T-029/T-030 invariant).
//
// flags: both statements clear the http_published bit (T-108) — a done
// row is always clean of the staging mark; the other bits (is_idn,
// render_pending) are preserved by the sign mask ~bit (PG bitwise
// operators promote smallint to int — hence the ::int2 cast, the
// renderq.go style of T-066).
var (
	// resultUpdateStatusSQL — the terminal status of a DNS episode
	// (not_delegated / temp_error / delegated_no_address). The outer
	// SELECT is the number of actually transitioned rows (the
	// idempotency of redelivery: a guard cut it off — 0). The statement
	// is strictly "domains → zones" (T-267): the retry maturity bucket
	// increment of crawler_counters['retry_mature: <current date>']
	// (T-202) is derived from it into a separate autocommit statement,
	// resultMatureIncSQL — PostgreSQL does not guarantee the execution
	// order of the unreferenced DML CTEs of one statement (the caveat
	// in monitor/zonestats.go); in production on Nov 07 the CTEs rm/zc
	// executed inversely (rm → zc): the statement held the counter row
	// and waited on the zone — the edge "counters → zones" against
	// http-worker RecordAndPublish (holds the zones until Commit, waits
	// on retry_mature:today) closed the 40P01 deadlock cycle (~3–4/h;
	// the breakdown — runbooks.md §2.8).
	// The SERVFAIL branch of the apex and a temp_error outcome without
	// addresses are full-fledged sources of temp_error episodes (the
	// mass DNS incident of Oct 24 left 491K domains exactly through this
	// branch); their episodes ripen by midnight and must land in the
	// bucket under the same key the HTTP stage writes (the current_date
	// of the record) — otherwise the retry guard would miss the DNS
	// cohort; the decrement of the same bucket is the scheduler's
	// (retryRequeueMatureSQL/retryFinalizeSQL), the authority — the
	// monitor reconciliation (the FILTER positions of the pass).
	resultUpdateStatusSQL = "WITH s AS (" +
		"SELECT zone_id, id, check_state FROM domains " +
		fmt.Sprintf("WHERE zone_id = $1::int2 AND id = $2::int8 AND status <> %d AND check_state <> %d ",
			int16(model.StatusDeleted), int16(model.CheckStateDone)) +
		"FOR UPDATE), " +
		"upd AS (UPDATE domains AS t " +
		"SET dns_mask = $3::int8, status = $4::int2, " +
		fmt.Sprintf("temp_attempts = CASE WHEN $4::int2 = %d THEN t.temp_attempts ELSE 0 END, ", int16(model.StatusTempError)) +
		fmt.Sprintf("flags = (t.flags & %d)::int2, ", int16(^int16(model.FlagHTTPPublished))) +
		fmt.Sprintf("check_state = %d, queued_at = NULL, last_checked_at = now() ", int16(model.CheckStateDone)) +
		"FROM s WHERE t.zone_id = s.zone_id AND t.id = s.id " +
		"RETURNING s.check_state, t.zone_id), " +
		"zc AS (UPDATE zones AS z " +
		"SET domains_pending = z.domains_pending - c.n_pending, " +
		"domains_queued = z.domains_queued - c.n_queued, " +
		"domains_checked = z.domains_checked + c.n " +
		fmt.Sprintf("FROM (SELECT zone_id, count(*) AS n, count(*) FILTER (WHERE check_state = %d) AS n_pending, ", int16(model.CheckStatePending)) +
		fmt.Sprintf("count(*) FILTER (WHERE check_state = %d) AS n_queued FROM upd GROUP BY zone_id) AS c ", int16(model.CheckStateQueued)) +
		"WHERE z.id = c.zone_id) " +
		"SELECT count(*) FROM upd"

	// resultMatureIncSQL — the retry maturity bucket increment of
	// crawler_counters['retry_mature: <current date>'] for a temp_error
	// outcome of a DNS episode: +1 by a separate autocommit statement
	// AFTER the successful write (T-267; before — the CTE rm of
	// resultUpdateStatusSQL, whose unreferenced execution order created
	// the deadlock edge — see the comment above). The upsert pattern of
	// the HTTP stage's recordRetryMatureBumpSQL (internal/httpworker
	// record.go), the delta — the literal 1 (one domain per call), the
	// current_date key is computed by the DB — the same as the HTTP
	// stage increment and the retry decrement. The non-atomicity window
	// of ±1 until the nearest reconciliation (≤
	// monitor.zone_stats_interval): a failure between the statements
	// leaves the transition recorded (done) and the bucket understated —
	// safe; the family's authority is the monitor reconciliation, and
	// the retry guard reads only the buckets of past days
	// (retryMatureSQL).
	resultMatureIncSQL = "INSERT INTO crawler_counters (name, value, updated_at) " +
		"VALUES ('retry_mature:' || to_char(current_date, 'YYYY-MM-DD'), 1, now()) " +
		"ON CONFLICT (name) DO UPDATE " +
		"SET value = crawler_counters.value + EXCLUDED.value, updated_at = now()"

	// resultUpdateKeepStatusSQL — the "addresses present" branch: the
	// status is not written (the HTTP stage will set the outcome, §7),
	// temp_attempts is reset — the episode succeeded. The zone deltas
	// and the pre-read — as in the terminal branch (T-178).
	resultUpdateKeepStatusSQL = "WITH s AS (" +
		"SELECT zone_id, id, check_state FROM domains " +
		fmt.Sprintf("WHERE zone_id = $1::int2 AND id = $2::int8 AND status <> %d AND check_state <> %d ",
			int16(model.StatusDeleted), int16(model.CheckStateDone)) +
		"FOR UPDATE), " +
		"upd AS (UPDATE domains AS t " +
		"SET dns_mask = $3::int8, temp_attempts = 0, " +
		fmt.Sprintf("flags = (t.flags & %d)::int2, ", int16(^int16(model.FlagHTTPPublished))) +
		fmt.Sprintf("check_state = %d, queued_at = NULL, last_checked_at = now() ", int16(model.CheckStateDone)) +
		"FROM s WHERE t.zone_id = s.zone_id AND t.id = s.id " +
		"RETURNING s.check_state, t.zone_id), " +
		"zc AS (UPDATE zones AS z " +
		"SET domains_pending = z.domains_pending - c.n_pending, " +
		"domains_queued = z.domains_queued - c.n_queued, " +
		"domains_checked = z.domains_checked + c.n " +
		fmt.Sprintf("FROM (SELECT zone_id, count(*) AS n, count(*) FILTER (WHERE check_state = %d) AS n_pending, ", int16(model.CheckStatePending)) +
		fmt.Sprintf("count(*) FILTER (WHERE check_state = %d) AS n_queued FROM upd GROUP BY zone_id) AS c ", int16(model.CheckStateQueued)) +
		"WHERE z.id = c.zone_id) " +
		"SELECT count(*) FROM upd"

	// resultMarkPublishedSQL — setting the http_published staging bit
	// (T-108, DNS-04): the bit is set right after a successful XADD of
	// q:http and before the result write — a batch redelivery tells by
	// the bit "published, not recorded" from "never got there" and does
	// not publish a duplicate. The guards are as in the result write: a
	// done row receives no mark (the episode is already fixed), deleted
	// is outside the loop (§13.1); rows in pending (the lease returned
	// them mid-episode) are marked — the task is published regardless of
	// the lease. OR preserves the other bits of flags.
	//
	// # The http_published bit machine (T-257, spec tasks/TZ-fsm.md §3 FR-3)
	//
	// The single point of truth about the staging bit lifecycle (bit 2
	// of domains.flags, model.FlagHTTPPublished — T-108). The
	// transitions remain SQL statements with guard predicates
	// (the TZ-fsm spec §1): the specification is the table below, the bit writers
	// (the last column) check against it rather than duplicate it.
	// Adjacent machines: check_state (internal/scheduler/checkstate.go,
	// T-255 — the result writes below are its rows R2/R3/R4/R13, the
	// scheduler enqueue is R1), status (the TZ-fsm spec §3 FR-2). The
	// formalization does not change behavior: the specification + a
	// scenario integration test (test/integration/httpbit_test.go,
	// the TZ-fsm spec §4).
	//
	// ## States (bit × episode phase)
	//
	//	clean (0)         no q:http publication in the current episode:
	//	                  the row is outside the episode, the episode is
	//	                  before the XADD or is already recorded by the
	//	                  DNS result write
	//	published (1)     the XADD q:http of the current episode went
	//	                  through, the DNS result is not recorded yet;
	//	                  a single reader — the batch filter
	//	                  (resultPendingSQL)
	//
	// The bit has a value only inside an episode (from the enqueue into
	// queued until the result write). Outside an episode it may stay
	// set: at pending — until the result write from pending (R4, the
	// DNS statements) or a new episode enqueue (B3) clears it; at done —
	// if done was written by the HTTP stage after a lost DNS write (the
	// HTTP branch of R4 does not touch the bit, I2); at deleted — until
	// resurrection (R9 preserves flags — the nearest enqueue B3 cleans
	// it). Only the batch filter reads the bit (done/deleted are cut
	// off by its guard predicates) — outside an episode the bit has no
	// value.
	//
	// ## Transitions: state × event × guards × target × side effects
	//
	//	| # | From | Event | Guards | Target | Side effects | Writer |
	//	|---|------|-------|--------|--------|--------------|--------|
	//	| B1 | clean | a successful XADD q:http (an episode with addresses) | status <> deleted, check_state <> done | published | flags OR bit, the others preserved; non-fatal — a mark failure does not drop the episode (the result write matters more) | resultMarkPublishedSQL, MarkHTTPPublished |
	//	| B2 | published | a DNS episode result write from any non-done state: a terminal status (R2), "addresses present" (R3), the lease returned mid-episode — a write from pending (R4), an invalid diagnosis (R13) | status <> deleted, check_state <> done | clean | flags AND ~bit (the others preserved); queued_at = NULL; last_checked_at = now() — by the same statements that write done | resultUpdateStatusSQL, resultUpdateKeepStatusSQL, resultInvalidSQL |
	//	| B3 | published | the scheduler enqueue (a new episode: pending → queued, R1) | check_state = pending, status <> deleted | clean | flags AND ~bit — the single reset point at the entry to an episode: any return of the row to work (a lease, a resurrection, a recheck, a manual recheck) goes through pending → queued; the mark of a past episode would otherwise suppress the publication of the new one — a lost q:http task | scheduler/enqueue.go enqueueUpdate |
	//	| B4 | published | a batch q:dns redelivery (the past episode's write did not happen — a failure/crash before done) | the batch filter: status <> deleted, check_state <> done | published (unchanged — the publication is SKIPPED) | a q:http duplicate is excluded: the batch's domain arrives with HTTPPublished, ProcessDomain skips Publish and reaches the write (B2); the episode's DNS exchanges repeat (the result was not stored, §3.5) | resultPendingSQL + ProcessDomain |
	//
	// ## The "XADD without the bit" window: a publication duplicate is allowed
	//
	// Between a successful XADD and setting the bit (B1) — a worker
	// crash or an UPDATE failure: the redelivery finds the bit clean,
	// B4 does not fire — a q:http duplicate is published. The duplicate
	// is ALLOWED: the price of the window, not a loss — a full repeated
	// HTTP check of the domain without a record (no task deduplication
	// on the q:http consumer side, spec 3.4 at-least-once), bounded by
	// the delivery attempts. A mark BEFORE the XADD would suppress the
	// publication forever on an XADD failure (a lost q:http task) — the
	// duplicate window was chosen over the loss window; for the same
	// reason setting the bit is non-fatal. The second source of
	// duplicates — cross-episode lease races (they existed before
	// T-108): the lease returned a row, the new episode enqueue cleared
	// the bit (B3), and the old delivery arrived after — a duplicate of
	// the new episode, bounded by the same price.
	//
	// ## Invariants
	//
	//	I1  the bit is set only after a successful XADD q:http (B1) —
	//	    before the publication the bit does not exist.
	//	I2  a DNS episode result write always clears the bit (B2, all
	//	    three statements — from queued and from pending, the DNS
	//	    branch of R4); the HTTP stage write (the HTTP branch of R4,
	//	    recordDomainSQL) does NOT touch the bit — it preserves the
	//	    other bits except dead_host_cutoff (T-164): a done row may
	//	    carry the bit from a lost DNS write, the next enqueue (B3)
	//	    will clear it. delete (R14) and resurrection (R9) do not
	//	    touch the bit either — for a resurrected row the nearest
	//	    enqueue cleans it.
	//	I3  every new episode starts with a clean bit: the only
	//	    transition into queued is the scheduler enqueue, and it
	//	    clears the bit (B3; I4 of the check_state specification).
	//	I4  a q:http duplicate is possible only in the "XADD without the
	//	    bit" window and the cross-episode lease races — bounded by
	//	    the delivery attempts (spec 3.4); a task loss is excluded by
	//	    the "publication → write" order (T-028, the file comment).
	resultMarkPublishedSQL = "UPDATE domains AS t " +
		fmt.Sprintf("SET flags = (t.flags | %d)::int2 ", int16(model.FlagHTTPPublished)) +
		fmt.Sprintf("WHERE t.zone_id = $1::int2 AND t.id = $2::int8 AND t.status <> %d AND t.check_state <> %d",
			int16(model.StatusDeleted), int16(model.CheckStateDone))

	// resultInvalidSQL — the terminal diagnosis of a poison row (T-109,
	// the remainder of DNS-03): the invalid status for a domain whose
	// check is deterministically impossible (an empty name,
	// ErrEmptyDomain). The write ends the lease and takes the row out of
	// the infinite queued → lease-return → dispatch loop: a recheck
	// (T-030) does not return invalid — the rows are not republished.
	// dns_mask is not written: the mask is a fact of the DNS episode,
	// which a junk row does not have; temp_attempts is reset (there
	// will be no retries); the http_published bit is cleared — a done
	// row is clean of the staging mark (T-108). The guards are as in
	// the result write: deleted is not overwritten (§13.1), the
	// redelivery of an already recorded diagnosis is idempotent
	// (spec 3.4). The zone deltas and the pre-read — as in the result
	// write (T-178).
	resultInvalidSQL = "WITH s AS (" +
		"SELECT zone_id, id, check_state FROM domains " +
		fmt.Sprintf("WHERE zone_id = $1::int2 AND id = $2::int8 AND status <> %d AND check_state <> %d ",
			int16(model.StatusDeleted), int16(model.CheckStateDone)) +
		"FOR UPDATE), " +
		"upd AS (UPDATE domains AS t " +
		fmt.Sprintf("SET status = %d::int2, temp_attempts = 0, ", int16(model.StatusInvalid)) +
		fmt.Sprintf("flags = (t.flags & %d)::int2, ", int16(^int16(model.FlagHTTPPublished))) +
		fmt.Sprintf("check_state = %d, queued_at = NULL, last_checked_at = now() ", int16(model.CheckStateDone)) +
		"FROM s WHERE t.zone_id = s.zone_id AND t.id = s.id " +
		"RETURNING s.check_state, t.zone_id), " +
		"zc AS (UPDATE zones AS z " +
		"SET domains_pending = z.domains_pending - c.n_pending, " +
		"domains_queued = z.domains_queued - c.n_queued, " +
		"domains_checked = z.domains_checked + c.n " +
		fmt.Sprintf("FROM (SELECT zone_id, count(*) AS n, count(*) FILTER (WHERE check_state = %d) AS n_pending, ", int16(model.CheckStatePending)) +
		fmt.Sprintf("count(*) FILTER (WHERE check_state = %d) AS n_queued FROM upd GROUP BY zone_id) AS c ", int16(model.CheckStateQueued)) +
		"WHERE z.id = c.zone_id) " +
		"SELECT count(*) FROM upd"

	// resultPendingSQL — the currency filter of a q:dns batch: the
	// domains not yet recorded remain (check_state <> done — the
	// redelivery, idempotency) and the not deleted ones (§13.1); flags —
	// the q:http publication staging mark (T-108): the bit tells the
	// redelivery to skip the publication. unnest WITH ORDINALITY keeps
	// the batch order (the zone interleaving of T-027) deterministic.
	resultPendingSQL = "SELECT u.zone_id, u.id, d.flags " +
		"FROM unnest($1::int2[], $2::int8[]) WITH ORDINALITY AS u(zone_id, id, ord) " +
		"JOIN domains AS d ON d.zone_id = u.zone_id AND d.id = u.id " +
		fmt.Sprintf("WHERE d.status <> %d AND d.check_state <> %d ", int16(model.StatusDeleted), int16(model.CheckStateDone)) +
		"ORDER BY u.ord"

	// subdomainsSelectSQL — the reference table of the checked
	// subdomains (spec 5.10).
	subdomainsSelectSQL = "SELECT id, label, enabled FROM subdomains ORDER BY id"
)

// ErrSubdomainsEmpty — the subdomains reference table is empty (a
// migration failure/a cleanup): the state is deterministic and
// redelivery does not cure it — the worker loop acknowledges the batch
// with a Warn (the lease pass of T-029/T-030 will bring the domains
// back after the reference table is fixed) instead of spinning retries
// until q:dead (DNS-03).
var ErrSubdomainsEmpty = errors.New("dnsworker: the subdomains reference table is empty — at least the apex is required (spec 5.10)")

// EpisodeResult — the DNS episode decision for one domain: what to write
// into domains and what to publish into q:http (spec 5.4 items 4–5,
// §7). Built by the pure function DecideEpisode from the outcomes of
// T-036/037/038.
type EpisodeResult struct {
	// SetStatus — whether to write domains.status: the terminal
	// branches of the DNS stage
	// (not_delegated/temp_error/delegated_no_address); false —
	// addresses present, the outcome belongs to the HTTP stage (§7).
	SetStatus bool
	// Status — the status of the terminal branch (valid with
	// SetStatus).
	Status model.Status
	// Mask — domains.dns_mask: the bits of this episode's resolving
	// subdomains (§5.4 item 4), including the wildcard-synthesized
	// ones (T-038 excludes them from the HTTP check, not from the
	// mask).
	Mask model.DNSMask
	// HTTPSubdomains — the DNS-live subdomains of the q:http task
	// (T-024): A/AAAA addresses and CNAME (§5.4 item 4), the
	// wildcard-excluded ones except the apex and www, which are never
	// marked (T-038). Empty — no task.
	HTTPSubdomains []queue.HTTPSubdomain
	// TempFailed — whether the subdomains had resolver failures
	// (transport, SERVFAIL/other codes): diagnostics for the logs; it
	// affects the decision only with an empty HTTP set (temp_error
	// instead of delegated_no_address).
	TempFailed bool
}

// HasHTTPTask reports whether there are addresses for the HTTP stage
// (§5.4 item 5: "addresses → a task into q:http") — by the set after
// the wildcard filter.
func (r EpisodeResult) HasHTTPTask() bool { return len(r.HTTPSubdomains) > 0 }

// DecideEpisode reduces the episode step outcomes to a recording
// decision (spec §7). A pure function — no DB and no queue; the apex is
// required, the subdomain report is for the delegation branches (the
// Stop branches of the apex do not check subdomains, RFC 8020/T-036).
// The status rules are in the file comment.
func DecideEpisode(apex *ApexResult, rep *SubdomReport, wild WildcardResult) (EpisodeResult, error) {
	if apex == nil {
		return EpisodeResult{}, errors.New("dnsworker: episode decision: no apex result (T-036)")
	}
	var out EpisodeResult

	// Terminal apex branches: NXDOMAIN (not_delegated) and
	// SERVFAIL/timeout (temp_error) — subdomains were not checked, the
	// episode mask is empty (§3.5 — only the last result).
	if status, terminal := apex.Status.DomainStatus(); terminal {
		out.SetStatus = true
		out.Status = status
		out.TempFailed = status == model.StatusTempError
		return out, nil
	}

	if rep == nil {
		return EpisodeResult{}, errors.New("dnsworker: episode decision: no subdomain report (T-037)")
	}
	out.Mask = rep.Mask

	temp := false
	for _, r := range rep.Results {
		// A resolver failure: transport (Err) or a code outside
		// {NOERROR, NXDOMAIN} — a subdomain NXDOMAIN is authoritative
		// (the name does not exist, RFC 8020), including
		// CNAME→NXDOMAIN (T-037). CNAME chain defects
		// (ErrSubdomCNAMELoop/Depth, the sentinel errors of T-037) are
		// not a resolver failure: the resolver answered correctly, the
		// zone is permanently broken — an episode repeat will not
		// change the outcome, the subdomain outcome is "no addresses",
		// not temp_error (three full T-031 repeats here are extra work
		// and a noisy status).
		if r.Err != nil {
			if !errors.Is(r.Err, ErrSubdomCNAMELoop) && !errors.Is(r.Err, ErrSubdomCNAMEDepth) {
				temp = true
			}
		} else if r.RCode != dnsclient.RCodeNOERROR && r.RCode != dnsclient.RCodeNXDOMAIN {
			temp = true
		}
		if !r.HasAddrs() || wild.IsWildcard(r.ID) {
			continue // not DNS-live or explained by the wildcard (T-038)
		}
		out.HTTPSubdomains = append(out.HTTPSubdomains, queue.HTTPSubdomain{
			SubdomainID: int16(r.ID),
			Label:       r.Label,
			Addrs:       resultAddrStrings(r),
			CNAME:       resultCNAME(r.CNAME()), // the direct alias, the presentation form
		})
	}
	out.TempFailed = temp

	if out.HasHTTPTask() {
		// Addresses → q:http; the status is not written (the HTTP stage
		// will decide, §7). A partial subdomain failure will not sink
		// the task (T-037).
		return out, nil
	}
	out.SetStatus = true
	if temp {
		out.Status = model.StatusTempError
	} else {
		out.Status = model.StatusDelegatedNoAddress
	}
	return out, nil
}

// resultAddrStrings — the string addresses of a subdomain for the q:http
// task (queue.HTTPSubdomain.Addrs, the queue checks the netip parsing,
// T-024): first A, then AAAA, in answer record order (T-037).
func resultAddrStrings(r SubdomResult) []string {
	addrs := make([]string, 0, len(r.A)+len(r.AAAA))
	for _, a := range r.A {
		addrs = append(addrs, a.String())
	}
	for _, a := range r.AAAA {
		addrs = append(addrs, a.String())
	}
	return addrs
}

// resultCNAME — the CNAME target for the q:http task without a trailing
// dot: names in queue messages are stored in presentation form without
// the root dot (like DNSDomain.Name, spec 5.2); otherwise the extreme
// 253-octet alias plus the dot would not pass the queue length
// validation (T-024).
func resultCNAME(c string) string {
	return strings.TrimSuffix(c, ".")
}

// RecordEpisode records the DNS episode outcome of one domain (spec 5.4
// item 4): a transition into done by a single UPDATE statement with a
// currency guard — a domain deleted during the check is not overwritten
// (§13.1), an already recorded result is not touched by a redelivery
// (idempotency, spec 3.4 — XACK only after the write, T-025); the same
// statement clears the http_published bit (T-108): done is the point
// where the publication staging mark has served its purpose; the CTEs of
// the same statement keep the zone counter deltas by the old row state
// (T-178, review P1-1).
// A temp_error outcome additionally increments the retry maturity
// bucket crawler_counters['retry_mature: <current date>'] — by a
// SEPARATE autocommit statement resultMatureIncSQL after the successful
// write (T-267): the counter and the zones are different statements, the
// order of their locks is deterministic (one PG statement does not
// guarantee the order of its unreferenced DML CTEs — the Nov 07
// incident, runbooks.md §2.8). The non-atomicity window of ±1 until the
// nearest reconciliation (≤ monitor.zone_stats_interval): an increment
// failure does not cancel the recorded transition and is not cured by a
// redelivery (the done guard will cut off both statements) — it returns
// (true, err): the caller finishes the episode (the monitor will catch
// the bucket up) instead of dropping the delivery.
// Both statements are autocommit under a 40P01 retry (T-268,
// resultRetry): the command repeats as is, WITHOUT repeating the DNS
// episode (an episode is orders of magnitude more expensive than a
// command, and the guards make the repeat safe — spec 3.4).
// Returns false when the guards cut the row off (done/deleted) — that
// is success, not an error.
func RecordEpisode(ctx context.Context, pool db.DB, zoneID int16, domainID int64, res EpisodeResult, met *Metrics) (bool, error) {
	if pool == nil {
		return false, errors.New("dnsworker: result recording: the pool is not set")
	}
	if res.SetStatus && !res.Status.Valid() {
		return false, fmt.Errorf("dnsworker: result recording of domain %d/%d: invalid status %d", zoneID, domainID, int16(res.Status))
	}

	var sql, what string
	args := []any{zoneID, domainID}
	if res.SetStatus {
		sql = resultUpdateStatusSQL
		what = "result recording"
		args = append(args, int64(res.Mask), int16(res.Status))
	} else {
		sql = resultUpdateKeepStatusSQL
		what = "result recording"
		args = append(args, int64(res.Mask))
	}
	var n int64
	if err := resultRetry(ctx, met, func() error {
		var err error
		n, err = resultCount(ctx, pool, sql, args, zoneID, domainID, what)
		return err
	}); err != nil {
		return false, err
	}
	if n > 0 && res.SetStatus && res.Status == model.StatusTempError {
		// The maturity bucket — a separate autocommit AFTER the write
		// (T-267): a failure here does not drop the episode — the
		// transition is recorded, a redelivery will not improve it (the
		// done guard), and the monitor reconciliation will catch the
		// bucket up by ±1. The 40P01 retry (T-268) comes before the
		// failure: a bucket deadlock is absorbed by repeating the
		// upsert, the non-atomicity window remains only for other
		// errors.
		if err := resultRetry(ctx, met, func() error {
			_, err := pool.Exec(ctx, resultMatureIncSQL)
			return err
		}); err != nil {
			return true, fmt.Errorf("dnsworker: retry_mature bucket increment of domain %d/%d: %w", zoneID, domainID, err)
		}
	}
	return n > 0, nil
}

// resultRetry — the 40P01 retry wrapper of the DNS stage write point
// (T-268): the command (the autocommit write statement) repeats as is —
// the currency guards make the repeat idempotent (spec 3.4), the DNS
// episode is not replayed; met may be nil (isolated builds — the
// instruments are no-ops). The parameters — DefaultDeadlockRetry.
func resultRetry(ctx context.Context, met *Metrics, fn func() error) error {
	return db.RetryDeadlock(ctx, db.DefaultDeadlockRetry, met.DBDeadlockRetried, fn)
}

// MarkHTTPPublished sets the http_published bit — the staging mark "the
// q:http task of this episode is published" (T-108). Called strictly
// after a successful publication and before the result write: the batch
// redelivery will skip the publication by the bit (no q:http duplicate)
// and reach the write.
// Returns false when the guards cut the row off (done/deleted) —
// success.
func MarkHTTPPublished(ctx context.Context, pool db.DB, zoneID int16, domainID int64) (bool, error) {
	if pool == nil {
		return false, errors.New("dnsworker: publication mark: the pool is not set")
	}
	tag, err := pool.Exec(ctx, resultMarkPublishedSQL, zoneID, domainID)
	if err != nil {
		return false, fmt.Errorf("dnsworker: publication mark of domain %d/%d: %w", zoneID, domainID, err)
	}
	return tag.RowsAffected() > 0, nil
}

// RecordInvalid records the terminal diagnosis of a poison row — the
// invalid status (T-109): a domain whose check is deterministically
// impossible (an empty name, ErrEmptyDomain) leaves the re-publication
// loop instead of an infinite lease return. The resultInvalidSQL
// statement with currency guards: a deleted domain is not overwritten
// (§13.1), the redelivery of an already recorded diagnosis is a no-op
// (idempotency, spec 3.4); the CTE keeps the zone counter deltas
// (T-178). An autocommit statement under a 40P01 retry (T-268,
// resultRetry) — like the result write.
// Returns false when the guards cut the row off (done/deleted) — that
// is success, not an error.
func RecordInvalid(ctx context.Context, pool db.DB, zoneID int16, domainID int64, met *Metrics) (bool, error) {
	if pool == nil {
		return false, errors.New("dnsworker: invalid diagnosis: the pool is not set")
	}
	var n int64
	if err := resultRetry(ctx, met, func() error {
		var err error
		n, err = resultCount(ctx, pool, resultInvalidSQL, []any{zoneID, domainID}, zoneID, domainID, "invalid diagnosis")
		return err
	}); err != nil {
		return false, err
	}
	return n > 0, nil
}

// resultCount executes the CTE statement of the transition into done and
// returns the number of actually transitioned rows (the outer SELECT;
// for a single UPDATE CommandTag is inapplicable — the statement is now
// composite, T-178). what — the operation context in the error message.
func resultCount(ctx context.Context, pool db.DB, sql string, args []any, zoneID int16, domainID int64, what string) (int64, error) {
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return 0, fmt.Errorf("dnsworker: %s of domain %d/%d: %w", what, zoneID, domainID, err)
	}
	n, err := pgx.CollectOneRow(rows, pgx.RowTo[int64])
	if err != nil {
		return 0, fmt.Errorf("dnsworker: %s of domain %d/%d: %w", what, zoneID, domainID, err)
	}
	return n, nil
}

// ProcessDomain — the DNS episode of one domain in full (spec 5.4, §7):
// the apex (T-036) → subdomains and wildcard detection (T-037/T-038,
// for the delegation branches only) → the decision (DecideEpisode) →
// the q:http publication → the result write (RecordEpisode). The
// "publication → write" order and the price of a retry on a write
// failure are in the file comment; an error of any step is an episode
// error: the domain stays unwritten, the q:dns delivery is not
// acknowledged and will be redelivered (Processing, T-025).
//
// dom — a q:dns batch domain with the staging mark state
// (PendingDomain, the T-108 filter): dom.HTTPPublished = true means the
// previous delivery attempt managed to publish the q:http task — the
// publication is skipped (no duplicate is created), the episode reaches
// the write;
// subs — the subdomains reference table (all rows; CheckSubdomains
// skips the disabled ones); askAAAA — dns.ipv6 (spec 5.4: AAAA is
// disabled by config when there is no IPv6); met — the stage
// instruments (T-077; nil — isolated builds without metrics); log —
// nil suppresses the records.
func ProcessDomain(
	ctx context.Context,
	pool db.DB,
	q queue.Queue,
	resolve dnsclient.Resolver,
	dom PendingDomain,
	subs []model.Subdomain,
	askAAAA bool,
	met *Metrics,
	log *slog.Logger,
) error {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if q == nil {
		return fmt.Errorf("dnsworker: domain %s: the queue is not set", dom.Name)
	}
	if resolve == nil {
		return fmt.Errorf("dnsworker: domain %s: the resolver is not set", dom.Name)
	}

	apex, err := CheckApex(ctx, resolve, dom.Name)
	if err != nil {
		return fmt.Errorf("dnsworker: domain %s: apex: %w", dom.Name, err)
	}

	var (
		rep  *SubdomReport
		wild WildcardResult
	)
	if !apex.Status.Stop() {
		rep, err = CheckSubdomains(ctx, resolve, dom.Name, subs, askAAAA)
		if err != nil {
			return fmt.Errorf("dnsworker: domain %s: subdomains: %w", dom.Name, err)
		}
		info, werr := DetectWildcard(ctx, resolve, dom.Name)
		switch {
		case werr != nil && ctx.Err() != nil:
			// A cancellation/deadline — the episode did not finish (not
			// "the detection failed").
			return fmt.Errorf("dnsworker: domain %s: wildcard detect: %w", dom.Name, werr)
		case werr != nil:
			// The T-038 policy: continue without filtering — all
			// DNS-live subdomains go to HTTP.
			log.Warn("the wildcard detect failed — continuing without filtering",
				"domain", dom.Name, "err", werr)
		case info.Detected:
			wild, err = MarkWildcardSubdomains(info.Addrs, resultWildSubs(rep))
			if err != nil {
				return fmt.Errorf("dnsworker: domain %s: wildcard marking: %w", dom.Name, err)
			}
		}
	}

	res, err := DecideEpisode(apex, rep, wild)
	if err != nil {
		return fmt.Errorf("dnsworker: domain %s: %w", dom.Name, err)
	}

	// First the q:http publication, then the done write (the order is in
	// the file comment): a write failure returns the episode via
	// redelivery. The redelivery of an episode with an already published
	// task (the http_published bit, T-108) skips the publication — it
	// reaches the write.
	switch {
	case !res.HasHTTPTask():
		// A terminal branch — nothing to publish.
	case dom.HTTPPublished:
		log.Debug("q:http was already published by a previous attempt — publication skipped",
			"domain", dom.Name, "zone_id", dom.ZoneID, "domain_id", dom.DomainID)
	default:
		task := queue.HTTPTask{
			ZoneID:     dom.ZoneID,
			DomainID:   dom.DomainID,
			Name:       dom.Name,
			Subdomains: res.HTTPSubdomains,
		}
		if _, err := q.Publish(ctx, queue.StreamHTTP, task); err != nil {
			return fmt.Errorf("dnsworker: domain %s: q:http publication: %w", dom.Name, err)
		}
		// The staging mark (T-108): a failsafe for the "published, not
		// recorded" window. Non-fatal: a mark failure does not drop the
		// episode — the result write matters more; the price is a
		// possible q:http duplicate on redelivery (bounded by the
		// attempts, spec 3.4), not a loss.
		marked, merr := MarkHTTPPublished(ctx, pool, dom.ZoneID, dom.DomainID)
		switch {
		case merr != nil:
			log.Warn("the http_published mark was not written — redelivery may publish a duplicate q:http",
				"domain", dom.Name, "zone_id", dom.ZoneID, "domain_id", dom.DomainID, "err", merr)
		case !marked:
			log.Debug("the http_published mark was not set — the domain is deleted or already done",
				"domain", dom.Name, "zone_id", dom.ZoneID, "domain_id", dom.DomainID)
		}
	}

	recorded, err := RecordEpisode(ctx, pool, dom.ZoneID, dom.DomainID, res, met)
	if err != nil && !recorded {
		return err
	}
	if !recorded {
		// A race with a delete (§13.1) or a redelivery of an already
		// recorded outcome — both cases are normal.
		log.Debug("the result was not recorded — the domain is deleted or already done",
			"domain", dom.Name, "zone_id", dom.ZoneID, "domain_id", dom.DomainID)
		return nil
	}
	if err != nil {
		// The transition is recorded, the retry_mature bucket increment
		// is not (T-267): a ±1 non-atomicity window, redelivery is
		// powerless (the done guard will cut off the write and the
		// increment), the monitor reconciliation will catch the bucket
		// up — the episode is finished, non-fatal (the T-108 mark
		// style).
		log.Warn("the retry_mature bucket increment was not written — reconciliation will catch up the bucket",
			"domain", dom.Name, "zone_id", dom.ZoneID, "domain_id", dom.DomainID, "err", err)
	}
	if res.SetStatus {
		// A terminal branch of the DNS stage — the final status counter
		// (T-077, spec 5.9); the "addresses present" branch writes no
		// status.
		met.DomainStatus(res.Status)
	}
	log.Debug("DNS episode recorded",
		"domain", dom.Name, "mask", int64(res.Mask), "http_subs", len(res.HTTPSubdomains),
		"status", res.Status.String(), "set_status", res.SetStatus)
	return nil
}

// resultWildSubs — the T-038 input from the T-037 report: the A/AAAA
// addresses of a subdomain as net.IP (MarkWildcardSubdomains compares
// with net.IP.Equal — the 4/16-octet forms of one point are not
// distinguished).
func resultWildSubs(rep *SubdomReport) []WildSubdomain {
	out := make([]WildSubdomain, 0, len(rep.Results))
	for _, r := range rep.Results {
		addrs := make([]net.IP, 0, len(r.A)+len(r.AAAA))
		for _, a := range r.A {
			addrs = append(addrs, net.IP(a.AsSlice()))
		}
		for _, a := range r.AAAA {
			addrs = append(addrs, net.IP(a.AsSlice()))
		}
		out = append(out, WildSubdomain{ID: r.ID, Label: r.Label, Addrs: addrs})
	}
	return out
}

// LoadSubdomains reads the reference table of the checked subdomains
// (spec 5.10): the source of the list is the subdomains table (managed
// by crawlerctl, T-083). Re-read for every batch: the reference table is
// tiny (≤63 rows), and changes (add/disable) are picked up without a
// worker restart.
func LoadSubdomains(ctx context.Context, pool db.DB) ([]model.Subdomain, error) {
	if pool == nil {
		return nil, errors.New("dnsworker: the subdomain reference table: the pool is not set")
	}
	rows, err := pool.Query(ctx, subdomainsSelectSQL)
	if err != nil {
		return nil, fmt.Errorf("dnsworker: the subdomain reference table: %w", err)
	}
	defer rows.Close()

	subs := make([]model.Subdomain, 0, model.MaxSubdomains)
	for rows.Next() {
		var s model.Subdomain
		if err := rows.Scan(&s.ID, &s.Label, &s.Enabled); err != nil {
			return nil, fmt.Errorf("dnsworker: the subdomain reference table: read: %w", err)
		}
		subs = append(subs, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dnsworker: the subdomain reference table: %w", err)
	}
	if len(subs) == 0 {
		return nil, ErrSubdomainsEmpty
	}
	return subs, nil
}

// resultKey — the key of a domains row in the batch filter.
type resultKey struct {
	zone int16
	id   int64
}

// PendingDomain — a q:dns batch domain awaiting processing, with the
// publication staging mark state (T-108): HTTPPublished is the
// http_published bit of the row — the previous delivery attempt managed
// to publish the q:http task, a repeat skips the publication
// (ProcessDomain) and reaches the write.
type PendingDomain struct {
	queue.DNSDomain
	HTTPPublished bool
}

// PendingDomains filters the q:dns batch by row currency (a single
// SELECT, an unnest batch — the §3.5 style): the domains whose result is
// not yet recorded (check_state <> done) and which are not deleted
// (§13.1) remain, together with the http_published bit (T-108) — the
// marker "the q:http task of this episode is already published". A batch
// redelivery (Processing, T-025) processes only them — without q:http
// duplicates (the published domains are rewritten without a publication)
// and without re-doing the recorded ones; an empty result — the batch is
// already done, the delivery is simply acknowledged.
func PendingDomains(ctx context.Context, pool db.DB, batch []queue.DNSDomain) ([]PendingDomain, error) {
	if pool == nil {
		return nil, errors.New("dnsworker: the batch filter: the pool is not set")
	}
	if len(batch) == 0 {
		return nil, nil
	}
	zones := make([]int16, len(batch))
	ids := make([]int64, len(batch))
	byKey := make(map[resultKey]queue.DNSDomain, len(batch))
	for i, d := range batch {
		zones[i] = d.ZoneID
		ids[i] = d.DomainID
		byKey[resultKey{zone: d.ZoneID, id: d.DomainID}] = d
	}

	rows, err := pool.Query(ctx, resultPendingSQL, zones, ids)
	if err != nil {
		return nil, fmt.Errorf("dnsworker: the batch filter (%d domains): %w", len(batch), err)
	}
	defer rows.Close()

	pending := make([]PendingDomain, 0, len(batch))
	for rows.Next() {
		var (
			k     resultKey
			flags int16
		)
		if err := rows.Scan(&k.zone, &k.id, &flags); err != nil {
			return nil, fmt.Errorf("dnsworker: the batch filter: read: %w", err)
		}
		if d, ok := byKey[k]; ok {
			pending = append(pending, PendingDomain{
				DNSDomain:     d, // ORDER BY u.ord — the batch order
				HTTPPublished: model.Flags(flags).IsHTTPPublished(),
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dnsworker: the batch filter: %w", err)
	}
	return pending, nil
}

// The dns-worker loop defaults.
const (
	// DefaultGroup — the q:dns consumer group: one for all dns-worker
	// instances (spec 4.2 — consumer groups scale the workers
	// horizontally; records are distributed inside the group).
	DefaultGroup = "dns-worker"

	// DefaultConcurrency — the limit of concurrent episodes: mirrors
	// dns.concurrency (spec 5.10) — the planned parallelism of DNS
	// exchanges of LimitResolver (T-035). A domain episode is a
	// sequence of exchanges, so the episode cap equals the exchange
	// cap; the pace of an individual episode is additionally
	// bounded by the resolver decorator.
	DefaultConcurrency = dnsclient.DefaultLimitConcurrency

	// DefaultConsume — q:dns records per single read: a record is a
	// batch of up to scheduler.batch_size domains (T-028), the episodes
	// inside run in parallel up to Concurrency; a larger read window
	// adds no work while the slots are busy.
	DefaultConsume = 1

	// DefaultErrorWait — a pause after DB/Redis errors: idle loops warm
	// neither the DB nor the logs (the T-028 style); the unacknowledged
	// deliveries will be returned by XAUTOCLAIM (T-025) — no hurry.
	DefaultErrorWait = 30 * time.Second
)

// Options — the dns-worker loop parameters; zero values are replaced by
// the defaults (withDefaults, the scheduler.EnqueueOptions style).
type Options struct {
	// Concurrency — the maximum of concurrent DNS episodes (domains);
	// 0 — DefaultConcurrency (dns.concurrency, spec 5.10).
	Concurrency int
	// Group — the q:dns consumer group; "" — DefaultGroup.
	Group string
	// Consumer — the name of the group's consumer; "" —
	// "dns-worker-<pid>" (unique within an instance — distinguishes the
	// PELs of the owners).
	Consumer string
	// Consume — q:dns records per single read; 0 — DefaultConsume.
	Consume int
	// AskAAAA — whether to query the subdomains' AAAA (dns.ipv6,
	// spec 5.4).
	AskAAAA bool
	// Recover — the stuck delivery return layer (Reliab, T-025): Run
	// starts a background RecoverLoop (XAUTOCLAIM → the same processing
	// → XACK) with the same Group/Consumer; nil — no recovery
	// (isolated builds and tests without Redis specifics).
	Recover *queue.Reliab
	// RecoverBatch — the RecoverLoop's claim batch per pass; 0 — the
	// queue default (queue.DefaultRecoverBatch). The production build
	// passes redis.recover_batch (T-153): the pace of returning stuck
	// deliveries is a queue-configurable value, not a stage consumption
	// window.
	RecoverBatch int
	// RecoverInterval — the RecoverLoop's polling period for stuck
	// records; 0 — the queue default (queue.DefaultRecoverInterval).
	// The config — redis.recover_interval (T-153).
	RecoverInterval time.Duration
	// Metrics — the service's Prometheus registry (T-077, spec 5.9):
	// Run builds the stage instruments from it (NewMetrics — the
	// episode counter, the duration histogram, the final status
	// counter); nil — without metrics (isolated builds and tests).
	Metrics *metrics.Registry
	// Log — the loop's logger; nil — the records are suppressed
	// (slog.DiscardHandler).
	Log *slog.Logger
}

func (o Options) withDefaults() Options {
	if o.Concurrency <= 0 {
		o.Concurrency = DefaultConcurrency
	}
	if o.Group == "" {
		o.Group = DefaultGroup
	}
	if o.Consumer == "" {
		o.Consumer = fmt.Sprintf("dns-worker-%d", os.Getpid())
	}
	if o.Consume <= 0 {
		o.Consume = DefaultConsume
	}
	return o
}

// Run — the dns-worker working loop (spec 5.4): reads q:dns batches with
// a consumer group and processes every delivery with the Processing
// pattern (T-025): domain episodes → the q:http publication → the result
// write → XACK. Runs until ctx is cancelled and returns nil — the OnRun
// contract of the service framework (T-012: graceful shutdown — the started
// episodes finish their work or return to the queue via redelivery). A
// read or processing error does not stop the loop: it is logged (Warn),
// and the unacknowledged delivery will come back via XAUTOCLAIM
// (spec 4.2).
func Run(ctx context.Context, pool db.DB, q queue.Queue, resolve dnsclient.Resolver, opts Options) error {
	if pool == nil {
		return errors.New("dnsworker: the worker loop: the pool is not set")
	}
	if q == nil {
		return errors.New("dnsworker: the worker loop: the queue is not set")
	}
	if resolve == nil {
		return errors.New("dnsworker: the worker loop: the resolver is not set")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	met := NewMetrics(o.Metrics) // nil without a registry — the instruments are no-ops (T-077)
	log.Info("dns-worker loop",
		"group", o.Group, "consumer", o.Consumer,
		"concurrency", o.Concurrency, "consume", o.Consume, "ask_aaaa", o.AskAAAA)

	wait := func(d time.Duration) {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
		case <-t.C:
		}
	}

	// Return of stuck deliveries (T-025): a background RecoverLoop on
	// behalf of the same consumer — an XAUTOCLAIM claim and the same
	// batch processing. Lives until ctx is cancelled; Run waits for its
	// exit (a started batch finishes its work or stays unacknowledged,
	// spec 3.4).
	if o.Recover != nil {
		recDone := make(chan struct{})
		go func() {
			defer close(recDone)
			_ = queue.RecoverLoop(ctx, o.Recover, func(c context.Context, del queue.Delivery) error {
				return processBatch(c, pool, q, resolve, del, o, met, log)
			}, queue.RecoverOptions{
				Stream:   queue.StreamDNS,
				Group:    o.Group,
				Consumer: o.Consumer,
				// The PEL traversal pace — the queue's, not the stage's
				// (redis.recover_*, T-153); zeros — the queue defaults.
				Batch:    o.RecoverBatch,
				Interval: o.RecoverInterval,
				Log:      log,
			})
		}()
		defer func() { <-recDone }()
	}

	for {
		deliveries, err := q.Consume(ctx, queue.StreamDNS, o.Group, o.Consumer, o.Consume)
		if err != nil {
			if ctx.Err() != nil {
				return nil // graceful shutdown
			}
			log.Warn("dns-worker loop: q:dns read", "err", err)
			wait(DefaultErrorWait)
			continue
		}
		for _, d := range deliveries {
			if ctx.Err() != nil {
				return nil // stopping: the delivery was not read — not acknowledged
			}
			err := queue.Processing(ctx, q, o.Group, d, func(c context.Context, del queue.Delivery) error {
				return processBatch(c, pool, q, resolve, del, o, met, log)
			})
			if err == nil {
				continue
			}
			if ctx.Err() != nil {
				return nil // a cancellation mid-work — a redelivery, not an alarm
			}
			log.Warn("the q:dns batch was not acknowledged — it will return via redelivery",
				"entry", d.ID, "err", err)
		}
	}
}

// processBatch processes a single q:dns delivery (a batch of domains,
// T-024): the subdomains reference table → the currency filter
// (PendingDomains) → the domain episodes in parallel up to Concurrency
// (dns.concurrency, spec 5.10). An error of any episode is the error of
// the whole delivery (no XACK): a redelivery will run the batch again,
// the filter will cut off the already recorded domains, and the
// http_published bit (T-108) will exclude publication duplicates — only
// the unfinished work repeats (idempotency, spec 3.4).
// Finished episodes go into the met instruments (T-077): the stage task
// counter, the duration histogram (the statuses of the terminal branches
// are counted by ProcessDomain after the write).
//
// Deterministic states that redelivery does not cure do not drop the
// delivery (DNS-03): an empty, entirely disabled or malformed subdomains
// reference table is configuration, not poison (the batch is
// acknowledged with a Warn, and the lease pass of T-029/T-030 will bring
// the domains back after the reference table is fixed — with retries
// until q:dead the queue would be littered by every batch, and a
// terminal status would freeze the domains forever: they are healthy
// and must be processed after the fix); a domain with an empty name is
// a junk row (T-109): the terminal invalid diagnosis is recorded
// (RecordInvalid), a Warn per domain and continuation — the other
// domains of the batch are processed, the delivery is acknowledged; the
// row leaves the republication loop (a recheck does not return
// invalid). Transport failures (DB/Redis) and a corrupted message
// (Decode) — still an error without XACK: their redelivery cures them
// (T-025).
func processBatch(
	ctx context.Context,
	pool db.DB,
	q queue.Queue,
	resolve dnsclient.Resolver,
	del queue.Delivery,
	o Options,
	met *Metrics,
	log *slog.Logger,
) error {
	batch, err := queue.Decode[queue.DNSBatch](del)
	if err != nil {
		// A poison record: a redelivery is pointless, but the queue will
		// try — exhaust the attempts and go to q:dead (T-025).
		return fmt.Errorf("dnsworker: record %s of stream %s: %w", del.ID, del.Stream, err)
	}
	subs, err := LoadSubdomains(ctx, pool)
	if err != nil {
		if errors.Is(err, ErrSubdomainsEmpty) {
			log.Warn("the subdomains reference table is empty — the q:dns batch acknowledged without processing",
				"entry", del.ID, "batch", len(batch.Domains), "err", err)
			return nil
		}
		return err
	}
	enabled, err := filterEnabledSubdomains(subs)
	if err != nil || len(enabled) == 0 {
		// Malformed rows or all subdomains disabled (a normal crawlerctl
		// subdomains disable operation) — configuration: a delivery
		// repeat will not change the outcome.
		if err != nil {
			log.Warn("the subdomains reference table is malformed — the q:dns batch acknowledged without processing",
				"entry", del.ID, "batch", len(batch.Domains), "err", err)
		} else {
			log.Warn("no enabled subdomains — the q:dns batch acknowledged without processing",
				"entry", del.ID, "batch", len(batch.Domains))
		}
		return nil
	}
	pending, err := PendingDomains(ctx, pool, batch.Domains)
	if err != nil {
		return err
	}
	if len(pending) == 0 {
		log.Debug("the q:dns batch was already processed or excluded (done/deleted)",
			"entry", del.ID, "batch", len(batch.Domains))
		return nil
	}

	// The semaphore bounds the concurrent episodes (the depth —
	// Concurrency, acquired before starting the goroutine — the queue
	// does not grow); the errors are collected up to the first one (a
	// mutex, no errgroup — the dependency is indirect).
	sem := make(chan struct{}, o.Concurrency)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for i := range pending {
		if err := ctx.Err(); err != nil {
			// Stopping: the not started domains will come back via
			// redelivery.
			mu.Lock()
			if firstErr == nil {
				firstErr = err
			}
			mu.Unlock()
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(dom PendingDomain) {
			defer wg.Done()
			defer func() { <-sem }()
			start := time.Now()
			err := ProcessDomain(ctx, pool, q, resolve, dom, subs, o.AskAAAA, met, log)
			switch {
			case err == nil:
				// The episode is finished — the stage throughput and
				// duration (T-077, spec 5.9).
				met.Episode(time.Since(start))
			case errors.Is(err, ErrEmptyDomain):
				// A poison domain: a deterministic error, a delivery
				// repeat will not change the outcome — the terminal
				// invalid diagnosis (T-109) instead of the eternal
				// queued→lease→dispatch loop. A Warn and continuation:
				// the other domains of the batch are processed, the
				// delivery is acknowledged. A diagnosis write failure is
				// an episode error (no XACK): a redelivery will finish
				// the fixation.
				recorded, rerr := RecordInvalid(ctx, pool, dom.ZoneID, dom.DomainID, met)
				switch {
				case rerr != nil:
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("domain %s: %w", dom.Name, rerr)
					}
					mu.Unlock()
				case recorded:
					met.DomainStatus(model.StatusInvalid)
					log.Warn("a domain with an empty name — the invalid diagnosis recorded, the row is taken out of the re-publishing loop",
						"entry", del.ID, "zone_id", dom.ZoneID, "domain_id", dom.DomainID)
				default:
					// The guards cut it off (deleted/already done) — a
					// redelivery or a concurrent delete: normal.
					log.Debug("the invalid diagnosis was not recorded — the domain is deleted or already done",
						"entry", del.ID, "zone_id", dom.ZoneID, "domain_id", dom.DomainID)
				}
			default:
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("domain %s: %w", dom.Name, err)
				}
				mu.Unlock()
			}
		}(pending[i])
	}
	wg.Wait()

	if firstErr != nil {
		return fmt.Errorf("dnsworker: batch %s: %d/%d domains: %w", del.ID, len(pending), len(batch.Domains), firstErr)
	}
	log.Debug("the q:dns batch processed", "entry", del.ID,
		"batch", len(batch.Domains), "pending", len(pending))
	return nil
}

// Delivery reliability on Redis Streams (T-025, spec 4.2 and 3.4): the
// default MAXLEN stream truncation policy, the explicit "XACK only after
// the result is written to the DB" pattern (Processing) and the return
// of stuck entries via XAUTOCLAIM with an attempt counter and a move to
// q:dead.

package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultMaxAttempts — the default maximum number of task delivery
// attempts (spec 4.2: "the maximum number of attempts per task is
// configurable (3 by default), then q:dead"). An attempt is a delivery
// of an entry to a consumer: the first read by the group plus every
// return of a stuck entry by a claim.
const DefaultMaxAttempts = 3

// DefaultClaimMinIdle — the default minimum idle time of a pending entry
// before it returns into the work (the MINIDLE of XAUTOCLAIM). It must
// exceed the worst time a live worker holds a delivery unacknowledged,
// otherwise someone else's work is snatched mid-flight: this is not the
// time of one task but the consumption window — the Consume window of
// entries processed by the loop sequentially at minimal parallelism. The
// worst configured case — render: a window of 16 × the 25 s timeout at
// contexts = 1 ≈ 6.7 min (q:http at concurrency 1 — 16 × 20 s ≈ 5.3 min);
// 10 minutes gives a 1.5x margin. The knob — redis.claim_min_idle
// (Q-03).
const DefaultClaimMinIdle = 10 * time.Minute

// The default MAXLEN policy stream length limits (spec 4.2: "MAXLEN on
// every stream"). MAXLEN ~ is the emergency length cap: without it the
// streams grow unboundedly (350 million domains, several streams per
// domain), while an untruncated unprocessed entry is not lost without a
// trace: the source of truth is PostgreSQL, the scheduler restores the
// task via lease (spec 3.4, 5.3). The working depth boundary is the
// backpressure gate: its threshold is bounded by half of the stream
// limit (BackpressureGate.watermarkFor), the cap is reachable only with
// the gate disabled; the acknowledged prefix between the gate and the
// cap is released by TrimLoop (T-141, trim.go).
//
// The limits are reconciled with the Redis budget of ≤ 0.5 GB (spec 3.2)
// against the actual entry sizes. The T-141 measurement (Redis 7.4,
// MEMORY USAGE): a q:dns batch of 1000 domains (scheduler.batch_size)
// ≈ 64 KiB — ~66 B per domain with the stream overhead; the earlier
// estimate "237 KiB per batch" referred to the batches of the
// batch_size 5000 era (the incident's 1735 batches = 410 MiB ≈ 3.7k
// domains per batch on average). Unit entries 0.3–0.7 KiB (the T-092
// measurement, docs/load-measurements.md §11). The sum of the caps must
// fit the budget: with noeviction a cap above the budget would mean a
// write rejection (OOM) before the trim — exactly what happened in the
// T-093 incident. The arithmetic:
//
//	q:dns     250 batches × 64 KiB ≈ 16 MiB (gate 125 batches ≈ 8 MiB)
//	q:http    150,000 × 0.7 KiB ≈ 103 MiB (gate 75,000 ≈ 52 MiB)
//	q:render  150,000 × 0.7 KiB ≈ 103 MiB (the capacity gate — a separate
//	          render.queue_high_watermark, T-174: 10,000 ≈ 7 MiB; 0 —
//	          shared → 75,000 ≈ 52 MiB)
//	q:render-fallback 15,000 × 0.7 KiB ≈ 10 MiB (not gated: the cap —
//	          the share budget and the worker B rate, spec 4.3 item 3)
//	q:analyze 150,000 × 0.7 KiB ≈ 103 MiB (gate 75,000 ≈ 52 MiB)
//	q:dead    10,000 × ~0.2 KiB ≈ 2 MiB (no payload, T-105)
//
//	In total the caps ≈ 337 MiB < 512 MiB (the margin ≈ 175 MiB — Redis
//	overhead memory, fragmentation, client buffers and the growth of
//	entries with long names; the between-trims remainder of TrimLoop is
//	itself bounded by the caps); at the same time the gates-full
//	≈ 119 MiB (T-174 lowered the q:render gate from 52 to ≈7 MiB). The
//	batch size is configurable (scheduler.batch_size 1000..10000): the
//	q:dns cap is proportional to it — the "sum of caps ≤ budget"
//	invariant for the whole normative batch range (the worst case of
//	10,000 domains ≈ 478 MiB) is pinned by the pin tests
//	TestStreamPolicyFitsBudget (reliab_test.go) and
//	TestSchedulerBatchFitsStreamBudget (config_test.go, from the
//	validation boundary).
const (
	// defaultMaxLenDNS — the q:dns entries are scheduler.batch_size
	// (1000) domain batches ≈ 64 KiB (the T-141 measurement): 250 batches
	// ≈ 16 MiB cap, the gate — 125 batches ≈ 8 MiB ≈ 125k domains "in
	// flight" when the DNS stage stops (≈10 minutes of stage work per the
	// T-092 measurement). 250, not more: the cap is proportional to the
	// configurable batch — at the maximum normative batch (10,000
	// domains, spec 5.3) 250 batches ≈ 157 MiB, and the sum of the caps
	// ≈ 478 MiB stays within the budget.
	defaultMaxLenDNS = 250
	// defaultMaxLenUnit — the unit tasks of q:http, q:render, q:analyze
	// (measured 0.3–0.7 KiB, not the "~100–200 bytes" of the earlier
	// estimate): the cap 150,000 × 0.7 KiB ≈ 103 MiB per stream; the
	// three unit streams, the fallback and q:dns add up to ≈ 337 MiB of
	// the budget. The q:http/q:analyze gate — min(watermark, half) =
	// 75,000 entries ≈ 52 MiB; the q:render capacity gate — the separate
	// render.queue_high_watermark (T-174: 10,000 by default, the
	// arithmetic — in the header table).
	defaultMaxLenUnit = 150_000
	// defaultMaxLenRenderFallback — the render fallback queue (T-237,
	// spec 4.3): the tasks that did not pass the main engine are reworked
	// by the chromium worker B at a limited rate (FR-13). The cap is not
	// the full stage flow but its limited share, so 15,000 — a tenth of
	// the unit cap ≈ 10 MiB. The budget constraint: with the worst
	// normative q:dns batch (10,000 domains, spec 5.3) the sum of the
	// caps ≈ 478 MiB must stay below the 480 MiB limit of the pin tests
	// (the budget 512 MiB, spec 3.2) — 150,000, as with the unit streams,
	// would break the budget (≈570 MiB, a write rejection before the trim
	// — the T-093 incident scenario). The cap is the last memory line of
	// defense, not a working regulator: the standing depth of the
	// fallback in normal mode is bounded by the share budget (T-239/FR-9)
	// and the worker B rate (T-240), uncontrolled growth is caught by the
	// depth alert (spec 4.6 item 3, T-245); truncated unacknowledged data
	// is recovered outside the queue, as with the other MAXLEN caps: unit
	// tasks — only via recheck re-verification (T-030, months) — that is
	// why the cap must stay a backup.
	defaultMaxLenRenderFallback = 15_000
	// defaultMaxLenDead — the q:dead flow is rare (exhausted attempts),
	// while manual triage needs a long history. The entries carry no
	// payload of the original tasks (T-105: the subject identifiers
	// instead of the message — q:dns batches of up to ~250 KB gave a
	// worst-case 10,000 × 250 KB ≈ 2.5 GB against the Redis budget of
	// ≤ 0.5 GB, spec 3.2): an entry is ~150–300 B, 10,000 ≈ 2–3 MB, two
	// orders of magnitude below the budget.
	defaultMaxLenDead = 10_000
)

// Budget estimates of the Redis memory spent per stream entry (the
// T-141 measurement, Redis 7.4, MEMORY USAGE; the unit ones — the top of
// the T-092 measurement range). Exported for the pin test of the budget
// invariant (TestStreamPolicyFitsBudget: the q:dns cap depends on the
// configurable scheduler.batch_size) and for the arithmetic of
// docs/redis.md §3.1.
const (
	// DNSStreamBytesPerDomain — bytes per domain of a q:dns batch with
	// the stream overhead: an entry of 1000 domains ≈ 64 KiB (JSON ~60 B
	// per domain + the entry list nodes).
	DNSStreamBytesPerDomain int64 = 66
	// UnitStreamBytesWorst — the worst unit entry of q:http/q:render/
	// q:analyze (0.7 KiB; the typical one — 0.3–0.5 KiB).
	UnitStreamBytesWorst int64 = 717
	// DeadStreamBytesWorst — a q:dead entry without payload (T-105,
	// ~150–300 B).
	DeadStreamBytesWorst int64 = 205
)

// DefaultStreamMaxLen — the stream length limit of the T-025 policy:
// applied by RedisQueue.Publish for every XADD when no explicit limit is
// set (RedisOptions.MaxLen == 0). Exported for the configuration and
// future tasks (integration, monitoring), the composition — the
// constants above.
func DefaultStreamMaxLen(stream string) int64 {
	switch stream {
	case StreamDNS:
		return defaultMaxLenDNS
	case StreamHTTP, StreamRender, StreamAnalyze:
		return defaultMaxLenUnit
	case StreamRenderFallback:
		return defaultMaxLenRenderFallback
	default: // StreamDead and everything else
		return defaultMaxLenDead
	}
}

// Processing — the pattern of reliable processing of one delivery
// (spec 4.2: "XACK only after the result is written to the DB";
// spec 3.4): the process handler does the work and writes the result to
// the DB, and only after it completes successfully is the entry
// acknowledged. The wrapper guarantees the order physically: Ack is
// unreachable before process returns without an error.
//
// A process error — a return without XACK: the entry stays in the
// group's pending and will come back via redelivery (Reliab.Claim),
// meaning the delivery is at-least-once and the result writing must be
// idempotent (spec 3.4). An Ack error is returned too — the result is
// already in the DB while the entry stayed in pending and will be
// redelivered: the repeated process call must go through unchanged (the
// same idempotency).
//
// The worker loop: Consume → Processing for every delivery → on error —
// a log and continue (does not stop the others, spec 3.4).
func Processing(ctx context.Context, q Queue, group string, d Delivery, process func(context.Context, Delivery) error) error {
	if process == nil {
		return fmt.Errorf("queue: Processing of entry %s of stream %s: handler not set", d.ID, d.Stream)
	}
	return ProcessingAttempt(ctx, q, group, d, 1, func(c context.Context, del Delivery, _ int) error {
		return process(c, del)
	})
}

// ProcessingAttempt — the Processing pattern for a handler that needs
// the delivery attempt number (T-148): an attempt is the entry's
// delivery counter in the group's PEL (the first XREADGROUP read = 1,
// every XAUTOCLAIM claim +1, Claimed.Attempts). The acknowledgement
// semantics are the same as Processing's: Ack only after process returns
// without an error, an error — without XACK (redelivery); classifying
// errors by the counter is the handler's business (the render timeout
// policy, T-148), the wrapper only carries the number. A nil handler —
// an error (the Processing style).
func ProcessingAttempt(ctx context.Context, q Queue, group string, d Delivery, attempts int, process func(context.Context, Delivery, int) error) error {
	if process == nil {
		return fmt.Errorf("queue: Processing of entry %s of stream %s: handler not set", d.ID, d.Stream)
	}
	if err := process(ctx, d, attempts); err != nil {
		return fmt.Errorf("queue: entry %s of stream %s not acknowledged (no XACK, it will come back via redelivery): %w", d.ID, d.Stream, err)
	}
	return q.Ack(ctx, d.Stream, group, d.ID)
}

// claimStreams — the go-redis window for returning stuck entries:
// XAUTOCLAIM (the claim, Redis 6.2+) and XPENDING EXTENDED (the delivery
// counter). *redis.Client (queue.NewClient) and *redis.ClusterClient
// satisfy the interface; tests substitute a fake — go-redis has no
// New*Result factories for these commands, the values are set by
// SetVal/SetErr on stubbed commands.
type claimStreams interface {
	XAutoClaim(ctx context.Context, a *redis.XAutoClaimArgs) *redis.XAutoClaimCmd
	XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd
}

// writeProbe — the go-redis window for the write probe (T-152): a SET of
// a short-lived key. An interface separate from claimStreams: the probe
// is optional (the production build clients can do it, the unit-test
// fakes cannot, in which case Reliab.Writable answers true and the claim
// gate is disabled — the behavior before T-152).
type writeProbe interface {
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
}

// The write probe parameters (T-152).
const (
	// WriteProbeKey — the write availability probe key: a SET with a
	// TTL, one per Redis instance (the value does not matter); the TTL
	// itself removes the keys of stopped services. The noeviction answer
	// to the SET is the same rejection the result XADD would meet: the
	// probe is more honest than "reading the error of the previous step"
	// — it spends neither delivery attempts nor handler calls.
	WriteProbeKey = "crawler:queue:write-probe"
	// writeProbeTTL — the lifetime of the probe key: with a margin above
	// the maximum redis.recover_interval (1 h, configuration) — the key
	// lives as long as the live RecoverLoop keeps refreshing it.
	writeProbeTTL = 2 * time.Hour
)

// claimRefunder — the go-redis window for refunding attempts to
// unprocessed entries (T-222): XCLAIM … JUSTID RETRYCOUNT via Do. An
// interface separate from claimStreams, modeled after writeProbe: the
// typed XClaimArgs of go-redis v9.23 does not pass RETRYCOUNT
// (XClaim/XClaimJustID serialize only the stream/group/consumer/idles/
// ids), while the semantics of rolling back the delivery counter is
// exactly in RETRYCOUNT; *redis.Client (production builds,
// queue.NewClient) and the unit-test fakes satisfy the interface
// (Reliab.Refund on a client without Do is transparent — the behavior
// before T-222).
type claimRefunder interface {
	Do(ctx context.Context, args ...interface{}) *redis.Cmd
}

// ReliabOptions — the parameters of returning stuck entries (T-025).
type ReliabOptions struct {
	// MaxAttempts — the maximum deliveries of a task (the first + the
	// claims) before the move to q:dead; 0 — DefaultMaxAttempts.
	MaxAttempts int
	// MinIdle — the minimum idle time of a pending entry before a claim
	// when Claim is called with minIdle 0; 0 — DefaultClaimMinIdle.
	MinIdle time.Duration
	// OnBury — the hook of a completed move of an entry to q:dead
	// (T-154): called after the successful publication of the Dead and
	// the XACK of the original entry — exactly on the fact of the task's
	// death, with the source stream name. The services mount it (the
	// death counter crawler_queue_dead_buried_total,
	// metrics.NewDeadBuriedHook): the queue package does not know about
	// metrics — the codebase's queue-metrics style (a seam function, like
	// the LenFunc of the T-077 depth collector). nil — the move is silent
	// (as before T-154). The hook is called only for entries with a
	// task_id; unidentifiable ones (without a task_id, the terminal bury
	// branch, T-260/FR-6.1) are marked by OnBuryUnidentified.
	OnBury func(stream string)
	// OnBuryUnidentified — the hook of a completed move to q:dead of an
	// entry without a task_id (T-260, FR-6.1): a synthetic TaskID
	// unknown:<stream>:<entry-id>, without subject fields. A seam
	// separate from OnBury: the death of a task and the quarantine of an
	// unidentifiable entry are different events
	// (crawler_queue_dead_buried_total counts tasks with a task_id,
	// crawler_queue_dead_unidentified_total — unidentifiable entries;
	// metrics.NewDeadUnidentifiedHook). nil — the move is silent.
	OnBuryUnidentified func(stream string)
}

// Reliab — the return of stuck Redis Streams entries with an attempt
// counter (T-025, spec 4.2: "stuck messages are taken via XAUTOCLAIM …
// the maximum number of attempts per task is configurable, then
// q:dead"). It works with the same consumer group the workers read from:
// entries stuck in pending (read and not acknowledged — the worker died
// or hung) are re-assigned to the consumer consumer and return into the
// work until the attempt limit is exhausted; those that exhausted the
// limit are published to q:dead (Dead) and acknowledged in the source
// stream, leaving the pending.
type Reliab struct {
	rdb    claimStreams // XAUTOCLAIM + XPENDING
	q      Queue        // the publication of Dead and the XACK of the original entries
	opt    ReliabOptions
	probe  writeProbe    // the SET write probe (T-152); nil — the probe is unavailable, the gate is disabled
	refund claimRefunder // XCLAIM JUSTID RETRYCOUNT (T-222); nil — the refund is unavailable, a cancel burns attempts as before T-222
}

// NewReliab builds the reliability layer on top of the queue q and the
// client rdb — this must be one and the same Redis (the Dead is
// published and acknowledged through the Queue, the claim and the
// counter — directly): for example NewReliab(rdb,
// NewRedisQueue(rdb, RedisOptions{}), opt). Zero option values are
// replaced with the defaults. A client that can SET (production
// *redis.Client / *redis.ClusterClient) gets the write probe
// (Writable, T-152).
func NewReliab(rdb claimStreams, q Queue, opt ReliabOptions) *Reliab {
	if opt.MaxAttempts <= 0 {
		opt.MaxAttempts = DefaultMaxAttempts
	}
	if opt.MinIdle <= 0 {
		opt.MinIdle = DefaultClaimMinIdle
	}
	r := &Reliab{rdb: rdb, q: q, opt: opt}
	if p, ok := rdb.(writeProbe); ok {
		r.probe = p
	}
	if rf, ok := rdb.(claimRefunder); ok {
		r.refund = rf
	}
	return r
}

// Writable — a probe of the Redis write availability (T-152): a SET of
// the short-lived WriteProbeKey key. false — writing is impossible (OOM
// noeviction, a connection break): claiming stuck deliveries is
// pointless — their processing would come back with the same publication
// error, and every claim spends a delivery attempt toward q:dead (the
// root of the October 22–23 incident, §11.1). The probe costs one SET
// and does not touch the PEL. true also when the probe is unsupported
// (a client without SET — the unit-test fakes): the gate is disabled,
// the behavior before T-152.
func (r *Reliab) Writable(ctx context.Context) bool {
	if r.probe == nil {
		return true
	}
	return r.probe.Set(ctx, WriteProbeKey, 1, writeProbeTTL).Err() == nil
}

// Claimed — a stuck entry returned into the work (Reliab.Claim).
// Attempts — the number of the current attempt: the entry's delivery
// counter in the group's PEL (the first XREADGROUP read + every claim).
// Processed with the Processing pattern, as a fresh delivery: processing
// and writing the result to the DB → XACK; the result not acknowledged —
// the counter will grow on the next claim.
type Claimed struct {
	Delivery
	Attempts int
}

// Claim returns up to n stuck entries of the stream stream — read by the
// group group and unacknowledged for at least minIdle (0 —
// ReliabOptions.MinIdle), assigning them to the consumer consumer. The
// PEL scan starts at the start cursor ("" or "0-0" — from the
// beginning); next in the return — the cursor of the next scan ("0-0" —
// the PEL walk is finished): the calling loop continues from it without
// re-reading the skipped (not yet ripened past MinIdle) entries from the
// beginning on every call.
//
// The choice of XAUTOCLAIM (not XPENDING+XCLAIM): spec 4.2 names
// XAUTOCLAIM directly; go-redis v9 supports it fully. One atomic command
// filters the PEL by idle time, re-assigns the entries and returns the
// walk cursor; XPENDING+XCLAIM would need two rounds with a race window
// between the listing and the claim, and XCLAIM does not filter by idle
// time. Additionally XAUTOCLAIM (Redis 7+) purges from the PEL the
// entries already truncated by MAXLEN: such entries come with empty
// values and are skipped.
//
// The attempt counter is maintained by Redis itself — the entry's PEL
// deliveries counter (the RetryCount field of the XPENDING EXTENDED
// answer: incremented on every delivery — the group's first read and
// every claim; XAUTOCLAIM increments the whole claimed batch at once, at
// the moment of the claim, not at the moment processing starts — the
// batch tail left unprocessed on a cancel gets its attempt back with a
// separate command, Reliab.Refund, T-222). The stream entry body is
// immutable, no separate counter keys are created: the counter lives in
// the PEL, survives worker restarts and is consistent across multiple
// recovery loops. The counter is read by XPENDING EXTENDED for every
// returned entry (the id..id range: XAUTOCLAIM does not return it); the
// read has no side effects, so it is safe in races with other recovery
// loops.
//
// An entry with a counter above MaxAttempts is not returned but moved to
// q:dead: Claim publishes the Dead (the task_id and the subject
// identifiers from the body, without the original payload — T-105) and
// acknowledges (XACK) the entry in the source stream — the message
// leaves the pending. The Dead is published before the XACK: an XACK
// failure would leave the entry in pending and give a duplicate in
// q:dead on the next pass — the q:dead flow is rare and triaged
// manually, a duplicate is preferable to a loss (spec 3.4). An entry
// without a task_id (outside the package formats) is moved with a
// synthetic identifier — see bury.
//
// An XPENDING error for one entry does not wreck the batch: XAUTOCLAIM
// has already assigned the entries to the consumer and reset their idle
// — losing the whole batch over one failed check would mean another
// MinIdle of downtime for entries whose counter is known. Errors are
// collected into the return (errors.Join), the valid part is handed
// over; an entry with an unknown counter is returned as the second
// result (unclassified): its claim already spent an attempt, RecoverLoop
// returns it with a RefundUnclassified refund (a RETRYCOUNT rollback,
// FR-6.2 — the T-222 window is widened from "ctx cancel" to "any
// unfinished classification"); if the refund did not happen (the Do
// window unavailable, command failures) — the entry stays assigned and
// will come back with the next claim after a new MinIdle.
// A ctx cancel after a successful claim does not interrupt the counter
// reads (WithoutCancel + its own budget, T-222): an interruption would
// leave the entries unclassified — refunding them would need a repeated
// counter read (RefundUnclassified does the read itself).
//
// An empty result without an error — there are no stuck entries.
func (r *Reliab) Claim(ctx context.Context, stream, group, consumer string, minIdle time.Duration, n int, start string) ([]Claimed, []Delivery, string, error) {
	if !knownStream(stream) {
		return nil, nil, "", fmt.Errorf("queue: unknown stream %q", stream)
	}
	if group == "" {
		return nil, nil, "", fmt.Errorf("queue: consumer group of stream %s not set", stream)
	}
	if consumer == "" {
		return nil, nil, "", fmt.Errorf("queue: consumer name of group %s not set", group)
	}
	if minIdle <= 0 {
		minIdle = r.opt.MinIdle
	}
	if n < 1 {
		return nil, nil, "", fmt.Errorf("queue: claim batch size %d < 1", n)
	}
	if start == "" {
		start = "0-0"
	}
	msgs, next, err := r.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    start, // the calling loop's cursor: continuing the PEL walk, not always from the beginning
		Count:    int64(n),
	}).Result()
	if err != nil {
		return nil, nil, "", fmt.Errorf("queue: claim %s (group %s): %w", stream, group, err)
	}
	if next == "" {
		next = "0-0" // the PEL walk is finished — the next one from the beginning
	}
	// The counter classification survives the caller's ctx cancel
	// (T-222, the P2-2 review): the XAUTOCLAIM above has already assigned
	// the entries and spent an attempt on each — interrupting the counter
	// reads on a cancel is pointless, and unclassified entries come back
	// as the second result (unclassified) and need a repeated counter
	// read when the attempt is refunded (RefundUnclassified, FR-6.2).
	// That is why after a successful claim the reads run under a separate
	// ctx — without the caller's cancel (WithoutCancel), with their own
	// short budget; XAUTOCLAIM itself respects the cancel: do not start a
	// new taking out of the PEL after the shutdown.
	cctx, cancelClassification := context.WithTimeout(context.WithoutCancel(ctx), classifyTimeout)
	defer cancelClassification()
	ctx = cctx
	out := make([]Claimed, 0, len(msgs))
	var unclassified []Delivery
	var errs []error
	for _, m := range msgs {
		d := Delivery{Stream: stream, ID: m.ID, Payload: payloadBytes(m.Values)}
		if len(d.Payload) == 0 {
			// The entry was truncated by MAXLEN while it hung in pending:
			// the values are empty, XAUTOCLAIM has already purged it from
			// the PEL — not processed.
			continue
		}
		pending, err := r.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: stream,
			Group:  group,
			Start:  m.ID,
			End:    m.ID, // exactly this entry: the counter read only, without triaging anyone else's pending
			Count:  1,
		}).Result()
		if err != nil {
			errs = append(errs, fmt.Errorf("queue: delivery counter of entry %s (%s): %w", m.ID, stream, err))
			// The claim has already spent an attempt — the entry is
			// returned to have it refunded (FR-6.2): without this a read
			// failure burned the attempt without a trace.
			unclassified = append(unclassified, d)
			continue
		}
		if len(pending) == 0 {
			// The entry left the PEL between the commands (acknowledged by
			// whoever was still processing it) — already handled,
			// skipping.
			continue
		}
		attempts := int(pending[0].RetryCount)
		if attempts > r.opt.MaxAttempts {
			if err := r.bury(ctx, group, d, attempts); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		out = append(out, Claimed{Delivery: d, Attempts: attempts})
	}
	if len(out) == 0 && len(unclassified) == 0 {
		// Equivalent mutants (T-M04): `> 0` → `>= 0`/`> -1` are
		// indistinguishable — errors.Join() without errors returns nil,
		// the branch with empty errs matches falling into the return
		// below. `> 1` (numbers/incrementer) is distinguishable by a
		// single error and is not suppressed.
		// mutator-disable-next-line expression/comparison, numbers/decrementer
		if len(errs) > 0 {
			return nil, nil, next, errors.Join(errs...)
		}
		return nil, nil, next, nil
	}
	return out, unclassified, next, errors.Join(errs...)
}

// The parameters of refunding attempts to unprocessed entries (T-222).
const (
	// classifyTimeout — the budget for classifying a batch after the
	// claim (XPENDING per entry, burying the exhausted ones): the reads
	// survive the caller's ctx cancel (see Claim) but do not hang longer
	// than the graceful shutdown budget; up to Batch short reads with a
	// margin.
	classifyTimeout = 5 * time.Second
)

// refundKey — the group of entries of one attempt refund (Refund):
// RETRYCOUNT is set by one number per XCLAIM command, entries with
// different counters (and streams) are refunded by separate commands.
type refundKey struct {
	stream   string
	attempts int
}

// Refund refunds the delivery attempt to entries claimed by XAUTOCLAIM
// but not processed (T-222). The counter semantics (see Claim):
// XAUTOCLAIM increments the PEL deliveries counter of the whole batch at
// the moment of the claim — not at the moment processing starts — so a
// RecoverLoop cancel in the middle of a batch (graceful shutdown: the
// 10 s stop budget is less than the batch walk time; render — up to 25 s
// per entry) left the batch tail with a burned attempt without a single
// handler call: minus an attempt on every restart, and after
// redis.max_attempts — q:dead without processing. The write gate
// (T-152) covers the neighboring scenario (an unavailable Redis), it
// does not see the context cancel. Called from RecoverLoop for the
// unprocessed batch tail under a separate short context — the loop
// context is already canceled by then. For entries whose classification
// did not finish (an XPENDING read failure after the claim, Claim →
// unclassified) the counter is unknown — RefundUnclassified returns
// those (T-260/FR-6.2): the T-222 window is widened from "ctx cancel"
// to "any unfinished classification".
//
// The mechanism — XCLAIM <stream> <group> <consumer> 0 <ids…> JUSTID
// RETRYCOUNT <attempts-1> to the same consumer the entries were assigned
// to by the claim:
//
//   - RETRYCOUNT sets the delivery counter to the exact value (the Redis
//     semantics, t_stream.c xclaimGenericCommand: RETRYCOUNT overrides
//     the regular claim increment; JUSTID without RETRYCOUNT does not
//     touch it) — attempts-1 returns the counter to its pre-claim value:
//     the next delivery sees the same attempt number as the canceled
//     pass;
//   - JUSTID — no entry bodies in the answer: the bodies are not needed,
//     the counter is set by the explicit RETRYCOUNT;
//   - min-idle 0 — the entries were just assigned to this consumer,
//     FORCE is not needed: the entries are in the group's PEL. A
//     re-claim by a neighboring worker's RecoverLoop is impossible while
//     the batch walk fits into claim_min_idle (the T-153 invariant) —
//     RecoverLoop checks the time since the claim and skips the refund
//     outside the invariant: a refund after someone else's claim would
//     also undo that claim's counter increment;
//
// A side effect: XCLAIM refreshes the delivery time — the idle of the
// refunded entries starts over, the next claim no earlier than after
// claim_min_idle. The price of an honest counter is a refund delay, not
// a correctness issue (at-least-once, spec 3.4).
//
// Entries are grouped by (stream, counter); command errors are collected
// into the return (errors.Join), one group's failure does not cancel the
// others — returning at least a part is better than nothing (an
// unrefunded attempt — a burn-down to q:dead, which this fix avoids). A
// client without the Do window (nil) — a transparent nil: the attempts
// stay spent, as before T-222.
func (r *Reliab) Refund(ctx context.Context, group, consumer string, claimed []Claimed) error {
	if r.refund == nil || len(claimed) == 0 {
		return nil
	}
	if group == "" || consumer == "" {
		return fmt.Errorf("queue: attempt refund: group (%q) or consumer (%q) not set", group, consumer)
	}
	// The stream — the production constants of the package, as in Claim:
	// we do not send XCLAIM for an arbitrary name.
	for _, c := range claimed {
		if !knownStream(c.Stream) {
			return fmt.Errorf("queue: attempt refund: unknown stream %q", c.Stream)
		}
	}
	ids := make(map[refundKey][]string, len(claimed))
	for _, c := range claimed {
		n := c.Attempts - 1
		if n < 0 {
			n = 0 // a guard bound: the delivery counter is never negative
		}
		k := refundKey{stream: c.Stream, attempts: n}
		ids[k] = append(ids[k], c.ID)
	}
	var errs []error
	for k, list := range ids {
		args := make([]interface{}, 0, 8+len(list))
		args = append(args, "xclaim", k.stream, group, consumer, int64(0))
		for _, id := range list {
			args = append(args, id)
		}
		args = append(args, "justid", "retrycount", int64(k.attempts))
		if err := r.refund.Do(ctx, args...).Err(); err != nil {
			errs = append(errs, fmt.Errorf("queue: attempt refund to entries %s of stream %s (group %s): %w",
				strings.Join(list, ","), k.stream, group, err))
		}
	}
	return errors.Join(errs...)
}

// RefundUnclassified refunds the delivery attempt to entries claimed by
// XAUTOCLAIM that never got classified — the XPENDING counter read after
// the claim failed (T-260, FR-6.2; the T-222 window widened from "ctx
// cancel" to "any unfinished classification"). Before the fix such an
// attempt burned without a trace: RecoverLoop could no longer return the
// counter to the entries (Refund requires a known Attempts), and the
// entry silently burned down to q:dead through repeated claims without a
// single handler call.
//
// The counter is read at the moment of the refund (XPENDING EXTENDED per
// entry, the id..id range — like the Claim classification): the claim
// has already spent the attempt, the read has no side effects, so the
// window between the classification failure and the refund does not
// distort the value. An entry that left the PEL during that time
// (acknowledged by whoever processed it, or buried by a neighbor) needs
// no refund — it is skipped. A repeated read error — in the return
// (errors.Join), the entry's attempt stays spent: no worse than the
// behavior before T-260, the next classification will decide its fate.
// Entry processing never started (classification runs before
// processing), so the refund is legitimate for the whole passed batch;
// entries with a finished classification do not land here — Refund
// returns them from the unprocessed tail (T-222).
//
// The refund itself — the same Refund mechanism (XCLAIM JUSTID RETRYCOUNT
// <counter−1> to the claim's consumer): the grouping, the zero clamp,
// the Do window (nil — a transparent nil, the attempts spent as before
// T-222) and the T-153 invariant (the refund undoes exactly its own
// increment — RecoverLoop calls it immediately after Claim) — see
// Refund.
func (r *Reliab) RefundUnclassified(ctx context.Context, group, consumer string, ds []Delivery) error {
	if r.refund == nil || len(ds) == 0 {
		return nil
	}
	if group == "" || consumer == "" {
		return fmt.Errorf("queue: attempt refund: group (%q) or consumer (%q) not set", group, consumer)
	}
	// The stream — the production constants of the package, as in
	// Claim/Refund: we do not send XPENDING for an arbitrary name.
	for _, d := range ds {
		if !knownStream(d.Stream) {
			return fmt.Errorf("queue: attempt refund: unknown stream %q", d.Stream)
		}
	}
	classified := make([]Claimed, 0, len(ds))
	var errs []error
	for _, d := range ds {
		pending, err := r.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: d.Stream,
			Group:  group,
			Start:  d.ID,
			End:    d.ID, // exactly this entry: the counter read only, without triaging anyone else's pending
			Count:  1,
		}).Result()
		if err != nil {
			errs = append(errs, fmt.Errorf("queue: delivery counter of entry %s (%s): %w", d.ID, d.Stream, err))
			continue
		}
		if len(pending) == 0 {
			// The entry left the PEL between the claim and the refund —
			// the attempt is gone, there is nothing to refund.
			continue
		}
		classified = append(classified, Claimed{Delivery: d, Attempts: int(pending[0].RetryCount)})
	}
	return errors.Join(append(errs, r.Refund(ctx, group, consumer, classified))...)
}

// Process — the Processing pattern over the layer's queue: an entry
// claimed by Claim is processed by the same "result to the DB → XACK"
// rule (spec 4.2) as the worker's fresh deliveries. RecoverLoop uses the
// handler shared with the worker for retries — the entries' idempotency
// (spec 3.4) makes the retry safe.
func (r *Reliab) Process(ctx context.Context, group string, d Delivery, process func(context.Context, Delivery) error) error {
	return Processing(ctx, r.q, group, d, process)
}

// ProcessAttempt — Process for a handler with the delivery attempt
// number (T-148): RecoverLoopAttempt passes Claimed.Attempts, the
// worker's handler decides the error policy by the counter (the render
// timeout tarpits).
func (r *Reliab) ProcessAttempt(ctx context.Context, group string, d Delivery, attempts int, process func(context.Context, Delivery, int) error) error {
	return ProcessingAttempt(ctx, r.q, group, d, attempts, process)
}

// bury moves an entry that exhausted the attempt limit to q:dead and
// acknowledges it in the source stream of the group group (T-025): the
// Dead — with the task_id, the source and the subject identifiers of the
// task, extracted from the original message body (deadSubjectOf),
// without payload — the byte guard of T-105 (see Dead); the Reason
// records the cause — hanging without an acknowledgement (workers do not
// report queue processing errors, spec 4.2, so the "last error" boils
// down to the fact of exhaustion). The Dead publication happens before
// the XACK (see Claim). A completed move (the Dead published, the
// original entry acknowledged) is marked by the OnBury hook — the
// observability of deaths (T-154: bury used to be silent, the 5507
// deaths of the October 22 incident went by without a single alert).
//
// An entry without a task_id (a message outside the package formats)
// gets a terminal branch instead of an error (T-260, FR-6.1): before the
// fix bury returned an error, the entry stayed in the PEL and spun in an
// eternal claim loop — every RecoverLoop pass claimed it again (+1 to
// the counter) and hit the same move error, the XACK never happened.
// Now the Dead is published with the synthetic identifier unknownTaskID
// — unknown:<stream>:<entry-id>, without subject fields (an extraction
// from a body that does not parse as a stream message does not identify
// the task's subject), the original entry is acknowledged — the entry
// leaves the PEL, the triage stays manual (q:dead — quarantine). The
// move is marked by the separate OnBuryUnidentified hook (the counter
// crawler_queue_dead_unidentified_total): the death of a task and the
// quarantine of an unidentifiable entry are different events. Entries
// with a task_id are processed as before.
func (r *Reliab) bury(ctx context.Context, group string, d Delivery, attempts int) error {
	taskID := taskIDOf(d.Payload)
	unidentified := taskID == ""
	if unidentified {
		taskID = unknownTaskID(d.Stream, d.ID)
	}
	reason := fmt.Sprintf("attempt limit %d exhausted: %d deliveries without ack", r.opt.MaxAttempts, attempts)
	if unidentified {
		reason = fmt.Sprintf("entry outside package formats (no task_id), attempt limit %d exhausted: %d deliveries without ack",
			r.opt.MaxAttempts, attempts)
	}
	dead := Dead{
		TaskID:   taskID,
		Stream:   d.Stream,
		EntryID:  d.ID,
		Attempts: attempts,
		Reason:   reason,
	}
	// The subject identifiers (T-105) — only for identifiable entries: an
	// extraction from a foreign body can produce invalid values (a
	// variant outside 0/1, a name longer than the limit) — Publish would
	// reject such an entry, the XACK would not happen, and a task with a
	// task_id would stay stuck in pending forever. Then we move the base
	// without identifiers: q:dead — the quarantine, it must accept any
	// task with a task_id (the base is always valid — checked by the
	// bury/Claim fields). An extraction is not trusted at all for a body
	// without a task_id: the subject cannot be classified (FR-6.1 — an
	// entry without subject fields).
	if !unidentified {
		subject := dead
		deadSubjectOf(&subject, d.Stream, d.Payload)
		if subject.validate() == nil {
			dead = subject
		}
	}
	if _, err := r.q.Publish(ctx, StreamDead, dead); err != nil {
		return fmt.Errorf("queue: publishing entry %s of stream %s to q:dead: %w", d.ID, d.Stream, err)
	}
	if err := r.q.Ack(ctx, d.Stream, group, d.ID); err != nil {
		return fmt.Errorf("queue: acknowledging the q:dead-moved entry %s of stream %s: %w", d.ID, d.Stream, err)
	}
	if unidentified {
		if r.opt.OnBuryUnidentified != nil {
			r.opt.OnBuryUnidentified(d.Stream)
		}
		return nil
	}
	if r.opt.OnBury != nil {
		r.opt.OnBury(d.Stream)
	}
	return nil
}

// unknownTaskID — the synthetic identifier of a q:dead entry from which
// no task_id can be extracted (T-260, FR-6.1): unknown:<stream>:
// <entry-id>. It identifies the entry itself (the source stream + the
// entry identifier), not the task: a link to the original task is
// impossible — the body is outside the package formats. The unknown
// prefix distinguishes the synthetic ids from real task_ids (the hex
// NewTaskID) in manual q:dead triage; the entry's subject fields stay
// zero.
func unknownTaskID(stream, entryID string) string {
	return "unknown:" + stream + ":" + entryID
}

// taskIDOf extracts the task_id from the entry body — the envelope of
// all the package's messages (message.go). An empty value — a message
// outside the package formats or a broken one: the Dead cannot be linked
// to the original task.
func taskIDOf(payload []byte) string {
	var env struct {
		TaskID string `json:"task_id"`
	}
	// An equivalent mutant (T-M04): cutting the return out of the error
	// branch is indistinguishable — on error Unmarshal does not fill the
	// fields (the syntax of the whole input is checked before decoding,
	// a type failure does not write the field), env.TaskID is "" anyway.
	// mutator-disable-next-line branch/if
	if err := json.Unmarshal(payload, &env); err != nil {
		return ""
	}
	return env.TaskID
}

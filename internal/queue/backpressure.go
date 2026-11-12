// Backpressure by queue depth (spec 4.2: "the scheduler publishes new
// tasks only when the queue length is below the threshold (backpressure)",
// §5.3: "publishes new tasks only if the queue length is below the threshold").
//
// Mechanism: before publishing, the producer checks the stream depth and
// publishes only while it is below the scheduler.queue_high_watermark
// threshold (config; the value is passed via BackpressureOptions). The gate is
// closed — pause for Interval and re-check; ctx cancellation — a clean exit
// with a context error (graceful shutdown, spec 3.4).
//
// What the threshold applies to — common to all gated streams:
//   - q:dns — the scheduler (T-028): the stream unit is a batch of domains
//     (spec 4.2), so the check before each message is exactly the
//     "check before publishing a batch";
//   - q:http, q:analyze — workers: one q:dns batch produces up to
//     scheduler.batch_size q:http tasks and one q:analyze task per
//     subdomain; without the gate these streams grow several times faster
//     than q:dns and threaten the Redis memory budget (≤ 0.5 GB, spec 3.2);
//   - q:render — publication always bypasses the gate (T-066: waiting
//     would delay the XACK of a q:http task); capacity is checked
//     separately for RenderQueue against the render.queue_high_watermark
//     threshold (T-174; 0 — the common scheduler.queue_high_watermark):
//     the stream depth is read by the same BackpressureGate.Len, the
//     threshold clamp — the same StreamWatermark (wiring in
//     cmd/http-worker);
//   - q:render-fallback is not gated (spec 4.3 p.3): publication is part
//     of the render worker's finalization, waiting for capacity would
//     delay the XACK of a q:render task (the same pathology T-066
//     removed); the flow into the fallback is limited by the share budget
//     and the pace of worker B, memory — by the MAXLEN cap (T-237);
//   - q:dead is not limited: moving tasks that exhausted their attempts
//     (T-025) is part of the reliability mechanism and must not be
//     blocked by overflow.
//
// The threshold lives in the scheduler section, at the place from spec 5.10
// (the set of sections is fixed, renaming would break configs); exception —
// the q:render capacity threshold: render.queue_high_watermark, T-174 (see
// the item above). The semantics are fixed in docs/configuration.md.
//
// The stream depth is the group consumers' unread remainder plus
// unacknowledged deliveries: XINFO GROUPS returns, for each group, lag
// (entries not yet delivered to consumers, Redis ≥ 7.0 — docs/redis.md
// fixes 7.4) and pending (delivered without XACK). Both components
// decrease as entries are read and acknowledged — the gate opens together
// with consumer progress. "Raw" XLEN instead of them is no good: between
// MAXLEN trims the stream length is monotonic (XACK does not decrease it),
// and the threshold is below the trim cap — after the watermark, publication
// would stop forever (latching); even above the cap, XLEN counts the
// already acknowledged history too, i.e. it does not measure the queue at
// all.
//
// Lag clamp by XLEN (T-168): lag = entries_added − entries_read and does
// not subtract entries cut off by XTRIM/XDEL while undelivered. Redis
// 7.0.x (production 7.0.15) holds a positive phantom lag forever in this
// scenario — incident of Oct 22–23, 2026: after the drain of the legacy
// batches of T-149 (XTRIM q:dns MAXLEN) the lag of the dns-worker group
// froze at 1753 with a single entry in the stream and closed the q:dns gate
// forever (effective threshold 125) — the scheduler was silent for ~15.5 h
// with 40.3M pending. Redis 7.4+ adjusts the lag on trim itself, but the
// gate does not depend on the server version. Undelivered entries are a
// subset of the stream entries: a group's real unread remainder is not
// greater than XLEN, so the depth counts min(lag, XLEN) for each group.
// The clamp never understates the real backlog (while undelivered entries
// are in the stream, lag ≤ XLEN and the clamp is inactive), and the phantom
// is cut off (min(1753, 1) = 1). Pending is not clamped: the
// unacknowledged-delivery counter is always exact.
//
// Special cases of depth:
//   - the stream has no groups (consumers have not created them yet): no
//     entry has been delivered — the depth equals the stream length (XLEN);
//   - lag is undefined (−1: a MAXLEN trim cut off undelivered entries or
//     Redis < 7.0) — only pending is counted: what was cut from the stream
//     is no longer a queue (the loss is restored by lease, spec 3.4), and
//     the unacknowledged-delivery counter is always exact.
//
// Relation of the threshold to MAXLEN: the gate must fire BEFORE the trim —
// the trim removes entries regardless of whether they have been read (soft
// regulation turns into silent task loss). The effective threshold of a
// stream does not rise above half of its MAXLEN cap (watermarkFor): a
// margin of half a cap covers entries published between the gate check and
// the trim firing. For q:dns this also makes the threshold reachable:
// 100,000 entries in the stream cannot be reached with a cap of 250 —
// without clamping the threshold the q:dns gate would never engage at all
// (the regulator would be only silent trimming).
//
// The gate itself is soft regulation (between the check and the publication
// other producers may add entries); the hard cap on length is MAXLEN ~ of
// the same T-025.

package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultBackpressureInterval — the pause between repeated stream depth
// checks while the gate is closed (BackpressureOptions.Interval). One
// second: the stages push thousands of tasks per second (spec 3.3); a
// longer window needlessly holds the producer, a shorter one — extra calls
// to Redis.
const DefaultBackpressureInterval = time.Second

// redisDepth — the go-redis window for stream depth: XINFO GROUPS (group
// lag and pending) and XLEN (the fallback for "no groups yet"); narrow,
// like redisStreams (redisq.go): unit tests substitute a fake, real
// *redis.Client and *redis.ClusterClient satisfy the interface.
type redisDepth interface {
	XLen(ctx context.Context, key string) *redis.IntCmd
	XInfoGroups(ctx context.Context, key string) *redis.XInfoGroupsCmd
}

// BackpressureOptions — gate parameters; non-positive Interval and nil Log
// are replaced with defaults in the constructor (RedisOptions style).
type BackpressureOptions struct {
	// Watermark — the stream depth threshold (scheduler.queue_high_watermark):
	// publication is allowed when depth < Watermark, depth == Watermark —
	// the gate is closed ("strictly below the threshold", spec 4.2/5.3). A
	// value ≤ 0 disables the gate (publication without waiting) — like
	// MaxLen = 0 in RedisOptions. The effective threshold of a specific
	// stream is no higher than half of its MAXLEN cap (see the file header
	// and watermarkFor).
	Watermark int64
	// MaxLen — the stream length cap of the MAXLEN policy, mirrors
	// RedisOptions.MaxLen (0 — per-stream caps of T-025,
	// queue.DefaultStreamMaxLen). The gate needs it so that the threshold
	// does not rise above the stream trim cap.
	MaxLen int64
	// Interval — the pause between repeated checks while the gate is
	// closed; 0 — DefaultBackpressureInterval.
	Interval time.Duration
	// Log — the gate logger: one Warn entry when entering the wait (queue
	// overflow is visible in the logs next to the spec 5.9 metrics); nil —
	// entries are suppressed (slog.DiscardHandler, RecheckOptions style).
	Log *slog.Logger
}

// BackpressureGate — the stream capacity gate: waits until the stream depth
// drops below the threshold (backpressure, spec 4.2/5.3). Used on its own
// (for the scheduler of T-028 it pays to check capacity before selecting a
// batch from the DB: the stream is full — the batch is not selected) and
// inside WatermarkPublisher.
type BackpressureGate struct {
	rdb redisDepth
	opt BackpressureOptions
}

// NewBackpressureGate builds the gate on top of the go-redis client — the
// same one as RedisQueue (a single source of truth about stream depth).
func NewBackpressureGate(rdb redisDepth, opt BackpressureOptions) *BackpressureGate {
	if opt.Interval <= 0 {
		opt.Interval = DefaultBackpressureInterval
	}
	if opt.Log == nil {
		opt.Log = slog.New(slog.DiscardHandler)
	}
	return &BackpressureGate{rdb: rdb, opt: opt}
}

// streamMaxLen — the stream length cap of the MAXLEN policy: explicit from
// the options (RedisOptions.MaxLen of the same service) or per-stream of
// T-025.
func (g *BackpressureGate) streamMaxLen(stream string) int64 {
	if g.opt.MaxLen > 0 {
		return g.opt.MaxLen
	}
	return DefaultStreamMaxLen(stream)
}

// watermarkFor — the effective stream threshold: the configured Watermark,
// but no higher than half of the stream MAXLEN cap (the gate fires before
// the trim, see the file header). Watermark ≤ 0 — the gate is disabled (0).
func (g *BackpressureGate) watermarkFor(stream string) int64 {
	return StreamWatermark(g.opt.Watermark, g.streamMaxLen(stream), stream)
}

// StreamWatermark — the effective stream depth threshold: the configured
// watermark capped at half of the stream length cap maxLen (0 — per-stream
// caps of the T-025 policy, DefaultStreamMaxLen). The gate must fire BEFORE
// the MAXLEN trim: the trim removes entries regardless of whether they have
// been read — a threshold above the cap would turn soft regulation into
// silent task loss. Exported for monitoring (the queue overflow alert,
// T-080): the alert threshold coincides with the gate threshold for each
// stream. watermark ≤ 0 — 0 (the gate is disabled).
func StreamWatermark(watermark, maxLen int64, stream string) int64 {
	// Equivalent mutants (T-M04): `<= 0` → `< 0`/`<= -1` are
	// indistinguishable — watermark 0 without the early return gives
	// min(0, maxLen/2) = 0 (maxLen after the replacement ≤ 0 — a positive
	// policy cap), negative values return zero in both branches. `<= 1`
	// (numbers/incrementer) is distinguishable by a test on watermark 1
	// and is not suppressed.
	// mutator-disable-next-line expression/comparison, numbers/decrementer
	if watermark <= 0 {
		return 0
	}
	if maxLen <= 0 {
		maxLen = DefaultStreamMaxLen(stream)
	}
	return min(watermark, maxLen/2)
}

// GroupDepth — the depth of a single consumer group: the unread remainder
// (min(lag, xlen) — the T-168 clamp, see the file header) plus
// unacknowledged deliveries (pending). The common source of truth of the
// depth formula: the gate (BackpressureGate.Len), the frozen-queue alert
// (T-162, monitor.RedisStreamGroups) and the "Queues" line of the load
// report (T-173, monitor.RedisQueues) compute depth only through it — the
// observation (status, crawler_queue_depth, the report) and the gate must
// not disagree about whether there is work. lag < 0 (−1 — undefined: the
// trim cut off undelivered entries or Redis < 7.0) — only the pending
// contribution: what was cut from the stream is no longer a queue, and the
// unacknowledged-delivery counter is always exact.
func GroupDepth(g redis.XInfoGroup, xlen int64) int64 {
	depth := g.Pending // delivered without XACK — queue as well
	// Equivalent mutants (T-M04): `>= 0` → `> 0`/`>= 1` differ only when
	// Lag == 0 — adding zero does not change the sum. `>= -1`
	// (numbers/decrementer) is distinguishable at Lag == −1 (undefined)
	// and is not suppressed.
	// mutator-disable-next-line expression/comparison, numbers/incrementer
	if g.Lag >= 0 { // −1 — undefined (trim of undelivered entries)
		depth += min(g.Lag, xlen)
	}
	return depth
}

// Len returns the stream depth — the backlog of its consumer groups: the
// unread remainder (lag, capped by the stream length — the T-168 clamp, see
// the file header and GroupDepth) plus unacknowledged deliveries (pending),
// across all groups of the stream (in production a working stream has one
// group — the stage's consumers). No groups — the depth equals the stream
// length (XLEN): no entry has been delivered. Details and the rationale of
// the metric — in the file header.
func (g *BackpressureGate) Len(ctx context.Context, stream string) (int64, error) {
	if !knownStream(stream) {
		return 0, fmt.Errorf("queue: неизвестный стрим %q (рабочие стримы — константы пакета: StreamDNS, StreamHTTP, StreamRender, StreamRenderFallback, StreamAnalyze, StreamDead)", stream)
	}
	if g.rdb == nil {
		return 0, fmt.Errorf("queue: глубина %s: клиент Redis не задан", stream)
	}
	groups, err := g.rdb.XInfoGroups(ctx, stream).Result()
	if err != nil {
		// The stream does not exist yet — Redis answers with the no such
		// key error (not an empty list): no groups, depth without
		// deliveries equals the stream length, XLEN of a nonexistent key
		// returns 0.
		if errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "no such key") {
			groups = nil
		} else {
			return 0, fmt.Errorf("queue: глубина %s: %w", stream, err)
		}
	}
	if len(groups) == 0 {
		n, err := g.rdb.XLen(ctx, stream).Result()
		if err != nil {
			return 0, fmt.Errorf("queue: глубина %s (групп нет, XLEN): %w", stream, err)
		}
		return n, nil
	}
	// The lag clamp (T-168) — in GroupDepth: XLEN is the upper bound of
	// each group's unread remainder (undelivered entries live in the
	// stream). It is read once per call and clamps every group with the
	// common XLEN: several groups of a stream read the same entries, the
	// sum of their undelivered may overlap, but each is an honest backlog
	// of its own group; a group without deliveries (lag = entries_added)
	// is limited by what physically remains in the stream. A stream that
	// disappeared between XINFO and XLEN (DEL/trim to 0) gives XLEN 0 —
	// the depth degenerates into pending, conservatively.
	xlen, err := g.rdb.XLen(ctx, stream).Result()
	if err != nil {
		return 0, fmt.Errorf("queue: глубина %s (кламп лага, XLEN): %w", stream, err)
	}
	var depth int64
	for _, grp := range groups {
		depth += GroupDepth(grp, xlen)
	}
	return depth, nil
}

// AwaitCapacity waits until the stream depth drops below the threshold and
// returns nil — publishing is allowed. The stream is at or above the
// threshold — pause Interval and retry; ctx cancellation — a context error
// (clean exit, spec 3.4). A depth read error returns immediately: the gate
// does not guess the depth (an unavailable Redis is not "the queue is
// empty"), the calling loop decides whether to retry.
//
// A disabled gate (Watermark ≤ 0), q:dead and q:render-fallback (spec 4.3
// p.3) pass without touching Redis (see the file header).
func (g *BackpressureGate) AwaitCapacity(ctx context.Context, stream string) error {
	watermark := g.watermarkFor(stream)
	if watermark <= 0 || stream == StreamDead || stream == StreamRenderFallback {
		return nil
	}
	waited := false
	for {
		n, err := g.Len(ctx, stream)
		if err != nil {
			return err
		}
		if n < watermark {
			return nil
		}
		if !waited {
			waited = true
			g.opt.Log.Warn("публикация приостановлена: глубина очереди у порога",
				"stream", stream, "depth", n, "watermark", watermark)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("queue: ожидание ёмкости %s: %w", stream, ctx.Err())
		case <-time.After(g.opt.Interval):
		}
	}
}

// WatermarkPublisher — a wrapper over Queue: publication passes only under
// the watermark (backpressure, spec 4.2/5.3). It implements the Queue
// interface itself (a decorator): Consume and Ack are passed to the wrapped
// queue unchanged — the gate concerns only producers; the scheduler (T-028)
// and workers work with the usual Queue, unaware of the Redis-specific
// depth (the transport is replaceable, spec 4.2 — that is why the gate is
// not built into RedisQueue.Publish).
type WatermarkPublisher struct {
	Queue
	gate *BackpressureGate
}

// NewWatermarkPublisher wraps the queue q with the gate gate: every
// Publish call first waits for stream capacity, then publishes. A nil gate
// — publication is an error: a wrapper without flow control is meaningless,
// and silently losing it is worse.
func NewWatermarkPublisher(q Queue, gate *BackpressureGate) *WatermarkPublisher {
	return &WatermarkPublisher{Queue: q, gate: gate}
}

// Publish publishes the message after waiting for stream capacity
// (AwaitCapacity): q:dead and a disabled gate publish immediately. The cost
// is one extra depth read per publication: the local Redis handles tens of
// thousands of operations per second, and a fresh depth matters more than a
// cache (other producers can close the threshold too).
func (p *WatermarkPublisher) Publish(ctx context.Context, stream string, msg Message) (string, error) {
	if p.gate == nil {
		return "", fmt.Errorf("queue: публикация %s: гейт не задан (NewWatermarkPublisher)", stream)
	}
	if err := p.gate.AwaitCapacity(ctx, stream); err != nil {
		return "", err
	}
	return p.Queue.Publish(ctx, stream, msg)
}

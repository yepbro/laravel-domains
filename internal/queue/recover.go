// Returning stalled deliveries to work (T-025, spec 4.2: "stalled
// messages are taken over via XAUTOCLAIM"): the RecoverLoop background
// loop next to the worker's consumption. Reliab.Claim returns entries
// stalled in the group's pending (read and not acknowledged — the worker
// died or hung), RecoverLoop processes them with the same Processing
// pattern — processing and writing the result to the DB → XACK — the
// worker uses for fresh deliveries: the idempotence of the entries
// (spec 3.4) allows mixing repeats with fresh tasks without a separate
// handler.
//
// Entries that exhausted the attempt limit (redis.max_attempts) Claim
// moves to q:dead itself (T-025) — RecoverLoop does not see them. The
// recoverer is started in every worker instance on behalf of its own
// consumer: the claim is concurrent and safe (XAUTOCLAIM atomically
// re-pins the entry), a single owner is not needed.
//
// Before the recoverer existed, an entry left unacknowledged by a worker
// crash stayed in the PEL forever: there was no redelivery, q:dead was
// unreachable, and lease (T-029) returns only q:dns tasks — the domains
// of the other stages are already in check_state = done (spec 7) and
// have no recovery.

package queue

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// DefaultRecoverInterval — the polling period for stalled entries. One
// XAUTOCLAIM per worker per period — cheap; the return latency is MinIdle
// plus up to one interval (an entry becomes eligible for a claim only
// after MinIdle of idleness, polling more often is pointless). Setting —
// redis.recover_interval (T-153).
const DefaultRecoverInterval = 30 * time.Second

// DefaultRecoverBatch — the claim batch per pass. T-025 took the workers'
// consumption window (16 entries) — the recoverer lived at the pace of
// the main loop; T-153 raises the batch to 64 and takes the traversal
// pace (batch/interval) out to redis.recover_batch/recover_interval: at
// 16/30 s a PEL traversal of ~5k entries takes ≈2.6 h — in the Oct 22
// incident (Redis OOM) the attempt burn-down of a single cohort stretched
// to 12 h (stage 23, tasks.md), healthy tasks waited hours for their
// return. The cost of re-processing claimed entries is bounded by
// idempotence (spec 3.4: a repeat of Processing — the same "result in
// the DB → XACK") — raising the batch costs only stage time, not
// correctness.
//
// Selection invariant: RecoverLoop traverses the batch sequentially and
// keeps its entries in the PEL for the whole traversal, so the batch
// processing time (the worst entry × batch) must stay below
// claim_min_idle (10 min), otherwise the batch tail is intercepted by the
// neighbouring worker's RecoverLoop (+1 attempt per round — the same
// burn-down mechanism). A typical render entry is ~5 s (the measurement
// T-092 §4: 4 contexts ≈ 0.78 pages/s): 64 × 5 s ≈ 5.3 min < 10 min — a
// ×1.9 margin. A limiting entry (tarpit, render.timeout 25 s): 64 × 25 s
// ≈ 26.7 min — a batch made entirely of tarpits will be re-claimed in the
// tail, a deliberate limit of the default (not 128+: 128 × 5 s ≈ 10.7 min
// no longer fits the typical case either); when raising batch, raise
// claim_min_idle proportionally — the calculation is in
// docs/configuration.md, the redis section.
const DefaultRecoverBatch = 64

// refundTimeout — the budget of attempt refunds of one claim pass
// (T-222/T-260): a separate context modeled after discardTimeout (ingest,
// pipeline.discard) — the refund must survive the cancellation of the
// loop's context (already cancelled), but must not hang longer than the
// graceful shutdown budget (10 s, service.DefaultShutdownTimeout). The
// budget is one for all refunds of the pass — the unprocessed tail on
// cancellation AND entries without classification (RefundUnclassified,
// T-260): independent budgets would sum up to 2 × 5 s — the whole stop
// budget; one or two Redis commands per refund fit into the common
// budget with a wide margin.
const refundTimeout = 5 * time.Second

// RecoverOptions — the RecoverLoop parameters; zero values are replaced
// with defaults (withDefaults, the ReliabOptions style).
type RecoverOptions struct {
	// Stream — the worker's working consumption stream (package
	// constants).
	Stream string
	// Group — the stream's consumer group (the same as the worker's
	// Consume).
	Group string
	// Consumer — the consumer name: the same as the worker's Consume —
	// the claim pins the entries to it too, the PEL does not sprawl
	// across names.
	Consumer string
	// Batch — the claim batch size; 0 — DefaultRecoverBatch. Setting
	// production builds — redis.recover_batch (T-153); the default
	// selection invariant — DefaultRecoverBatch above.
	Batch int
	// Interval — the polling period on an empty claim; 0 —
	// DefaultRecoverInterval. Setting — redis.recover_interval (T-153).
	Interval time.Duration
	// Log — the loop logger; nil — entries are suppressed
	// (slog.DiscardHandler).
	Log *slog.Logger
}

func (o RecoverOptions) withDefaults() RecoverOptions {
	if o.Batch <= 0 {
		o.Batch = DefaultRecoverBatch
	}
	if o.Interval <= 0 {
		o.Interval = DefaultRecoverInterval
	}
	return o
}

// RecoverLoop — the background loop returning stalled deliveries (T-025):
// a tick — a batch Claim → Processing of each entry with the process
// handler (the same one as the worker's main loop). A full batch — an
// immediate repeat (draining what stalled during idleness), a partial/
// empty one — wait for a tick: a live PEL is small. A claim or processing
// error does not stop the loop: it is logged as Warn, the next tick will
// retry (the OnRun contract of the framework, T-012; an entry not
// confirmed by processing will come back with the next claim, the attempt
// counter will speed up the move to q:dead). It runs until ctx is
// cancelled and returns nil — graceful shutdown (spec 3.4): processing
// already started finishes or stays unacknowledged, and the unprocessed
// batch tail gets its attempt back (Refund, T-222) — cancellation does
// not burn the delivery of an entry the traversal did not reach; entries
// without a finished classification (a counter read failure after the
// claim) get their attempt back in the same pass (RefundUnclassified,
// T-260/FR-6.2).
func RecoverLoop(ctx context.Context, r *Reliab, process func(context.Context, Delivery) error, opt RecoverOptions) error {
	if process == nil {
		return errors.New("queue: stalled delivery recovery loop: handler not set")
	}
	return RecoverLoopAttempt(ctx, r, func(c context.Context, d Delivery, _ int) error {
		return process(c, d)
	}, opt)
}

// RecoverLoopAttempt — RecoverLoop for a handler with the delivery
// attempt number (T-148): each claimed entry is passed Claimed.Attempts
// (the PEL delivery counter: the group's first read + each claim; a fresh
// delivery would correspond to 1). The handler sees which delivery this
// is and applies a split error policy (a timeout tarpit vs a retryable
// one, T-148) without keeping state between deliveries: the counter lives
// in Redis and survives worker restarts.
//
// The entry gate (T-152): before each claim pass, Redis write
// availability is checked (Reliab.Writable — a SET of a short-lived
// key). A Redis unavailable for writing (OOM noeviction, a drop) makes
// the claim pointless: processing a stalled entry would come back with
// the same result publication error, and the delivery counter would grow
// — a claim with no chance to write the result only burns attempts
// toward q:dead (the root of the Oct 22–23 incident: 5,507 deaths with
// healthy processing, §11.1). The pass is skipped until the next tick;
// if Redis recovered — the claim and processing continue, attempts are
// not lost (at-least-once, spec 3.4).
func RecoverLoopAttempt(ctx context.Context, r *Reliab, process func(context.Context, Delivery, int) error, opt RecoverOptions) error {
	if r == nil {
		return errors.New("queue: stalled delivery recovery loop: reliability layer not set (NewReliab)")
	}
	if process == nil {
		return errors.New("queue: stalled delivery recovery loop: handler not set")
	}
	o := opt.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("stalled delivery recovery loop",
		"stream", o.Stream, "group", o.Group, "consumer", o.Consumer,
		"batch", o.Batch, "interval", o.Interval.String())

	// claim — one pass: the claim of a batch and processing of each
	// entry with the Processing pattern (the result in the DB → XACK).
	// Returns the claim batch size for the "drain or tick" decision. A
	// partial Claim failure (one entry's counter) does not cancel the
	// valid part — it is processed, the error is logged. The PEL
	// traversal cursor is carried between passes: a large PEL is re-read
	// in pages, not from the start on every tick (Q-07).
	cursor := "0-0"
	// refund — returning attempts to the unprocessed batch tail on
	// cancellation (T-222): XAUTOCLAIM increments the delivery counter of
	// the whole batch at the moment of the claim, and a cancellation in
	// the middle of the traversal (graceful shutdown: the stop budget is
	// smaller than the batch processing time) left entries the traversal
	// did not reach with a burned attempt and not a single handler call —
	// an attempt per restart, toward q:dead without processing. The
	// refund — XCLAIM JUSTID RETRYCOUNT (Reliab.Refund) under a short
	// ctx: the loop's context is already cancelled by then. An entry
	// started by processing (ProcessAttempt had time to return) spends
	// the attempt — the delivery happened; only an entry whose
	// processing did not start gets the attempt back.
	refund := func(rctx context.Context, rest []Claimed) {
		if err := r.Refund(rctx, o.Group, o.Consumer, rest); err != nil {
			log.Warn("stalled delivery recovery: unprocessed tail attempts not returned — entries are vulnerable to burning down to q:dead (T-222)",
				"n", len(rest), "err", err)
		} else {
			log.Info("stalled delivery recovery: unprocessed tail attempts returned", "n", len(rest))
		}
	}
	// refundUnclassified — returning attempts to entries without
	// classification (T-260, FR-6.2): the XAUTOCLAIM claim debited the
	// attempt, and the XPENDING counter read failed — before the fix
	// such an attempt burned without a trace (Refund requires a known
	// counter). It is called immediately after Claim in the same loop
	// pass: the T-222 window is widened from "ctx cancellation" to "any
	// unfinished classification". The context — the pass budget common
	// with refund (see claim), the loop's context may already be
	// cancelled by then.
	refundUnclassified := func(rctx context.Context, rest []Delivery) {
		if err := r.RefundUnclassified(rctx, o.Group, o.Consumer, rest); err != nil {
			log.Warn("stalled delivery recovery: unclassified entries' attempts not returned — entries are vulnerable to burning down to q:dead (T-260)",
				"n", len(rest), "err", err)
		} else {
			log.Info("stalled delivery recovery: unclassified entries' attempts returned", "n", len(rest))
		}
	}
	claim := func() int {
		// Equivalent mutant (T-M04): minIdle 0 → −1 is indistinguishable
		// — Claim replaces any non-positive value with the layer option.
		// One (numbers/incrementer) is distinguishable by a test on the
		// MinIdle of XAUTOCLAIM.
		// mutator-disable-next-line numbers/decrementer
		claimAt := time.Now() // the start of the batch entries' idleness (idle reset by the claim)
		// The pass refund budget — ONE refundTimeout for all refunds
		// (entries without classification and the unprocessed tail on
		// cancellation): independent budgets would sum up to 2 × 5 s and
		// exceed the graceful shutdown budget of 10 s (P3-5 of the T-260
		// review); each refund is one or two Redis commands, the common
		// budget is enough with a margin. The context is separate from
		// the loop's ctx: by the moment of the tail refund it is already
		// cancelled.
		rctx, cancelRefunds := context.WithTimeout(context.Background(), refundTimeout)
		defer cancelRefunds()
		claimed, unclassified, next, err := r.Claim(ctx, o.Stream, o.Group, o.Consumer, 0, o.Batch, cursor)
		if err != nil && ctx.Err() == nil {
			log.Warn("stalled delivery recovery: claim", "err", err)
		}
		cursor = next
		if len(unclassified) > 0 {
			// The T-153 invariant — the same as the tail refund on
			// cancellation: the refund extinguishes exactly its own
			// claim increment; outside the claim_min_idle window the
			// entry could have been re-claimed by a neighbouring
			// worker — its increment must not be extinguished (P2-1 of
			// the T-222 review).
			if elapsed := time.Since(claimAt); elapsed < r.opt.MinIdle {
				refundUnclassified(rctx, unclassified) // FR-6.2: classification not finished — attempt back
			} else {
				log.Warn("stalled delivery recovery: classification took longer than claim_min_idle — entries may have been re-claimed by a neighbouring worker, attempt refund skipped (T-260)",
					"n", len(unclassified), "elapsed", elapsed.String(), "min_idle", r.opt.MinIdle.String())
			}
		}
		for i, c := range claimed {
			if ctx.Err() != nil {
				// The T-153 invariant: batch × the worst entry time <
				// claim_min_idle (Claim is called with minIdle 0 —
				// r.opt.MinIdle is effective). While it holds, the
				// batch tail cannot have been re-claimed by the
				// neighbouring worker's RecoverLoop, and the refund
				// extinguishes exactly its own increment; outside the
				// invariant the refund would also extinguish somebody
				// else's claim (understating the counter, P2-1 of the
				// review) — skip, as before T-222.
				if elapsed := time.Since(claimAt); elapsed < r.opt.MinIdle {
					refund(rctx, claimed[i:]) // T-222: the tail did not start — attempt back
				} else {
					log.Warn("stalled delivery recovery: batch traversal took longer than claim_min_idle — the tail may have been re-claimed by a neighbouring worker, attempt refund skipped (T-222)",
						"n", len(claimed)-i, "elapsed", elapsed.String(), "min_idle", r.opt.MinIdle.String())
				}
				return len(claimed)
			}
			if err := r.ProcessAttempt(ctx, o.Group, c.Delivery, c.Attempts, process); err != nil && ctx.Err() == nil {
				log.Warn("stalled delivery not acknowledged — it will come back via a claim",
					"entry", c.ID, "attempts", c.Attempts, "err", err)
			}
		}
		return len(claimed)
	}

	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
		// Cancellation between passes (including right after the tail
		// refund of a full batch above) — a clean exit without a write
		// probe: a SET on a cancelled ctx would fail and would log
		// "Redis is not available for writing" on every stop in the
		// middle of a full batch.
		if ctx.Err() != nil {
			return nil
		}
		// The write gate (T-152): while Redis rejects entries, the claim
		// is skipped — delivery attempts are not spent. The probe is one
		// SET (Reliab.Writable); if the probe is unsupported the gate is
		// transparent (true — the pre-T-152 behavior).
		if !r.Writable(ctx) {
			log.Warn("stalled delivery recovery: Redis is not writable (OOM/connection) — claim skipped, delivery attempts are not spent (T-152)")
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
			continue
		}
		if n := claim(); n < o.Batch {
			// Nothing to drain — wait for a tick (a full batch — the
			// next claim immediately: a PEL stalled during idleness is
			// processed without pauses).
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	}
}

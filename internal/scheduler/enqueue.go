// Scheduler: enqueueing selected domains into the DNS check queue (T-028;
// spec 5.3, 4.2, 3.4).
//
// EnqueueBatch moves a selection batch (DispatchBatch, T-027) to
// check_state = queued with queued_at = now() — from that moment the
// domain lease is in effect (spec 5.3: "moves the selected domains to queued with a
// timestamp (lease)") — and publishes the moved domains as a single
// DNSBatch message to q:dns (spec 4.2: the unit of a q:dns task is a batch of domains:
// id + name). EnqueueLoop is the working enqueue loop for OnRun (framework
// T-012): selection → enqueue, a pause between batches, waiting for replenishment
// on an empty batch; the service assembly is cmd/scheduler.
//
// The order of operations is the move first, then publication (spec 3.4: "the source
// of truth is PostgreSQL. Redis loss must not lead to data loss
// (tasks are restored by the scheduler)"):
//
//   - the move records in the DB that the domain has been taken into work. A failure after the move
//     but before publication leaves rows queued without a task in Redis — the
//     expired-lease return (T-029, scheduler.lease,
//     30 min by default) will return them to pending: a bounded delay without reprocessing;
//
//   - the reverse order (publication → move) with a failure before the UPDATE would leave
//     a task in q:dns for a domain that remained pending: the next cycle
//     would select it again and publish it again — unbounded duplicate work
//     in a crash loop, and the result would be written for a domain without an active lease;
//
//   - a publication failure does not roll back the move: XADD may have succeeded while the client
//     saw an error (a lost response) — a rollback to pending would give a second
//     task for a domain already taken into work. The recovery is the same —
//     lease (T-029); redelivery is allowed and bounded (at-least-once,
//     spec 3.4), result writes are idempotent.
//
// The guard of the move is check_state = pending at the moment of the UPDATE: between selection
// (T-027, it does not change the state) and the move the row could leave pending
// (a concurrent lease return T-029, resurrection by an add file T-018, a second
// scheduler instance). RETURNING returns keys of only the rows actually
// moved — publication is built from them; unmoved ones (a race)
// are excluded from publication: the next cycle will pick them up (delivery
// at-least-once, spec 3.4).

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"crawler/internal/db"
	"crawler/internal/model"
	"crawler/internal/queue"
)

// enqueueMaxNameLen — limit of the full domain name in ASCII form (spec 5.2
// item 4). Duplicates queue.maxNameLen (not exportable): checking before the move
// does not put into queued the rows the queue would reject at publication.
const enqueueMaxNameLen = 253

// enqueueUpdate — a batch move of a batch to queued: a single
// UPDATE … FROM unnest statement (the internal/db builder, spec 6: "writing in batches …
// UPDATE … FROM unnest(...)"; each key array is one parameter, the batch
// size does not hit the protocol limit). The keys are (zone_id, id), the PK of domains
// (spec 6.1). Assignments are batch-wide expressions: check_state as a literal
// of the model code (schema codes, not data — the dispatchZoneSQL style),
// queued_at = now() — the DB server time (no client clock skew)
// and a single lease start moment for the whole batch; flags = (t.flags & ~the
// http_published bit)::int2 — the q:http publication staging label (T-108)
// is cleared at the entry point into a new episode (transition B3 of the
// bit specification — dnsworker/result.go, the table at resultMarkPublishedSQL,
// spec tasks/TZ-fsm.md §3 FR-3): the enqueue is the only
// pending → queued transition, any return of a row
// into work goes through it (lease T-029, resurrection T-018, recheck T-030/T-031,
// crawlerctl recheck) — a fresh episode always starts with a clean bit,
// otherwise the previous episode's label would suppress its publication (loss
// of the q:http task). The signed mask ~bit and ::int2 are PG smallint arithmetic
// (the renderq.go T-066 style). The guard t.check_state = pending AND t.status <>
// deleted re-checks the state at the moment the row is locked: pending —
// that the row is still a selection candidate, not-deleted — that the
// delete file (T-019) did not delete it between selection and the move: apply_delete
// does not change check_state, and moving a deleted row would give a double
// pending decrement and a phantom queued (the T-178 deltas, review P2-1);
// RETURNING — the keys of the rows actually moved: publication is built
// exactly from them.
var enqueueUpdate = db.Update("domains").
	Key(
		db.Column{Name: "zone_id", Type: "int2"},
		db.Column{Name: "id", Type: "int8"},
	).
	SetExpr(
		fmt.Sprintf("check_state = %d", int16(model.CheckStateQueued)),
		"queued_at = now()",
		fmt.Sprintf("flags = (t.flags & %d)::int2", int16(^int16(model.FlagHTTPPublished))),
	).
	Where(fmt.Sprintf("t.check_state = %d AND t.status <> %d", int16(model.CheckStatePending), int16(model.StatusDeleted))).
	Returning("t.zone_id", "t.id")

// enqueueBatchSQL — the move of a batch + zone counter deltas with ONE
// statement (T-178, incremental-counters spec §6.1: the CTE of the same statement — the rollback/commit
// of the "transition + delta" pair is atomic without an explicit transaction): upd — the move
// (the text of enqueueUpdate, the builder's single source of truth),
// zc — the pending −n / queued +n shift over the zones of the moved rows
// (RETURNING zone_id → GROUP BY; the move predicate pins the old
// pending state — the delta is computed by the number of rows actually
// moved, incremental-counters spec §6.1.2), the outer SELECT — the keys of the moved
// rows for publication (like the move's RETURNING before).
var enqueueBatchSQL = func() string {
	inner, err := enqueueUpdate.SQL()
	if err != nil {
		panic("scheduler: building enqueueUpdate: " + err.Error())
	}
	return "WITH upd AS (" + inner + "), " +
		"zc AS (UPDATE zones AS z " +
		"SET domains_pending = z.domains_pending - c.n, " +
		"domains_queued = z.domains_queued + c.n " +
		"FROM (SELECT zone_id, count(*) AS n FROM upd GROUP BY zone_id) AS c " +
		"WHERE z.id = c.zone_id) " +
		"SELECT u.zone_id, u.id FROM upd AS u"
}()

// enqueueKey — the key of a domains row: a batch is addressed by the (zone_id, id) pair.
type enqueueKey struct {
	zone int16
	id   int64
}

// EnqueueResult — the outcome of enqueueing a batch.
type EnqueueResult struct {
	// Queued — rows moved to queued (by the move's RETURNING).
	Queued int
	// Published — domains in the published q:dns message: equal to Queued
	// on successful publication. 0 on a publication error, while Queued
	// shows how many rows went to queued without a task (lease
	// will return them, T-029).
	Published int
	// Skipped — batch rows not moved because of a race (no longer
	// pending at the moment of the UPDATE), including duplicate keys in the batch.
	Skipped int
}

// EnqueueBatch moves a batch of selected domains to check_state = queued with
// queued_at = now() (the lease starts, spec 5.3) with a single batch UPDATE with
// a pending guard and publishes the moved domains as a single DNSBatch message
// to q:dns (spec 4.2). Guarantees and the order of operations — in the file comment;
// an empty batch is a no-op without touching the DB or the queue.
func EnqueueBatch(ctx context.Context, pool db.DB, q queue.Queue, batch []DispatchRow) (EnqueueResult, error) {
	if pool == nil {
		return EnqueueResult{}, errors.New("scheduler: batch enqueue: pool not set")
	}
	if q == nil {
		return EnqueueResult{}, errors.New("scheduler: batch enqueue: queue not set")
	}
	if len(batch) == 0 {
		// Nothing to move or publish; an empty DNSBatch is also
		// forbidden by the queue contract (queue.DNSBatch.validate).
		return EnqueueResult{}, nil
	}
	if err := enqueueValidate(batch); err != nil {
		return EnqueueResult{}, err
	}

	// Batch keys — parallel arrays (the db.Update builder columns —
	// []any with int2/int8 elements; each array is one parameter).
	zoneIDs := make([]any, len(batch))
	domainIDs := make([]any, len(batch))
	for i, r := range batch {
		zoneIDs[i] = r.ZoneID
		domainIDs[i] = r.DomainID
	}

	// The move. RETURNING — the keys of the rows actually moved; the zone
	// counter deltas (pending −n / queued +n) are performed by the CTE of the same
	// statement (T-178): the move and the deltas are atomic.
	rows, err := pool.Query(ctx, enqueueBatchSQL, zoneIDs, domainIDs)
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("scheduler: batch enqueue (%d domains): %w", len(batch), err)
	}

	translated := make(map[enqueueKey]struct{}, len(batch))
	for rows.Next() {
		var k enqueueKey
		if err := rows.Scan(&k.zone, &k.id); err != nil {
			// Close on each read branch without defer: defer would keep
			// the cursor until the function exits, i.e. also during
			// publication (the VerifyCounters style, T-217).
			rows.Close()
			return EnqueueResult{}, fmt.Errorf("scheduler: batch enqueue: reading moved rows: %w", err)
		}
		translated[k] = struct{}{}
	}
	// rows.Err before Close: a connection break is not "nothing moved"
	// (the readDailyValue precedent, T-217).
	if err := rows.Err(); err != nil {
		rows.Close()
		return EnqueueResult{}, fmt.Errorf("scheduler: batch enqueue: %w", err)
	}
	// The cursor is closed BEFORE assembling the message and publishing (T-224): Publish is
	// network I/O to Redis (XADD with retries and pauses up to ~0.75 s on
	// failures, redisq.go); an open cursor would hold a pool connection for
	// all that time — with a slow Redis the scheduler hits the connection
	// budget rather than the queue speed. The keys are fully read —
	// the cursor is no longer needed (the VerifyCounters style, T-217).
	rows.Close()

	res := EnqueueResult{Queued: len(translated), Skipped: len(batch) - len(translated)}

	// Publication: one q:dns message from the moved rows in the
	// selection order (the zone interleaving of T-027 is preserved for the consumer). The key
	// is deleted after use — a duplicate key in the batch is published
	// once.
	msg := queue.DNSBatch{Domains: make([]queue.DNSDomain, 0, res.Queued)}
	for _, r := range batch {
		k := enqueueKey{zone: r.ZoneID, id: r.DomainID}
		if _, ok := translated[k]; !ok {
			continue
		}
		delete(translated, k)
		msg.Domains = append(msg.Domains, queue.DNSDomain{
			ZoneID:   r.ZoneID,
			DomainID: r.DomainID,
			Name:     r.Name, // the full name, punycode (dispatch.go, spec 5.2)
		})
	}
	if len(msg.Domains) == 0 {
		// All rows lost the race — nothing to publish; the next
		// cycle will pick them up (at-least-once, spec 3.4).
		return res, nil
	}
	if _, err := q.Publish(ctx, queue.StreamDNS, msg); err != nil {
		// The move is committed and not rolled back (the file comment):
		// res.Queued rows await the lease return (T-029).
		return res, fmt.Errorf("scheduler: batch enqueue: publishing %d domains to q:dns: %w", len(msg.Domains), err)
	}
	res.Published = len(msg.Domains)
	return res, nil
}

// enqueueValidate checks the batch rows before touching the DB: the q:dns publication
// rejects invalid values (queue.DNSBatch.validate), while the move is
// already committed by then — checking upfront does not put rows
// into queued without a message. The conditions match the queue contract
// (zone/domain > 0, the name is non-empty within the spec 5.2 limit).
func enqueueValidate(batch []DispatchRow) error {
	for i, r := range batch {
		if r.ZoneID <= 0 {
			return fmt.Errorf("scheduler: batch enqueue: domain #%d: zone_id %d ≤ 0", i+1, r.ZoneID)
		}
		if r.DomainID <= 0 {
			return fmt.Errorf("scheduler: batch enqueue: domain #%d: domain_id %d ≤ 0", i+1, r.DomainID)
		}
		if r.Name == "" {
			return fmt.Errorf("scheduler: batch enqueue: domain #%d: empty name", i+1)
		}
		if len(r.Name) > enqueueMaxNameLen {
			return fmt.Errorf("scheduler: batch enqueue: domain #%d: name longer than %d octets", i+1, enqueueMaxNameLen)
		}
	}
	return nil
}

// Defaults of the enqueue loop.
const (
	// DefaultEnqueueBatch — the selection batch size: mirrors the default
	// scheduler.batch_size (spec 5.10); the loop gets the working value from
	// the config.
	DefaultEnqueueBatch = 1000

	// DefaultEnqueueInterval — the pause between batches. So far the only
	// throttle of the q:dns fill rate: the queue-length
	// backpressure gate is T-026 (a parallel task),
	// the TODO below in EnqueueLoop.
	DefaultEnqueueInterval = time.Second

	// DefaultEnqueueIdleWait — waiting for pending replenishment on an empty
	// batch and the pause after loop errors (DB or Redis unavailable):
	// idle circles warm neither the DB nor the logs, and unpublished rows
	// await the lease return (T-029) — there is no rush.
	DefaultEnqueueIdleWait = 30 * time.Second
)

// EnqueueOptions — enqueue loop parameters; zero values
// are replaced with defaults (withDefaults, the RecheckOptions style).
type EnqueueOptions struct {
	// BatchSize — the selection batch size (scheduler.batch_size, spec 5.3).
	BatchSize int
	// Interval — the pause between batches (DefaultEnqueueInterval).
	Interval time.Duration
	// IdleWait — waiting for replenishment on an empty batch and the pause after
	// errors (DefaultEnqueueIdleWait).
	IdleWait time.Duration
	// ZoneWeights — the zone selection weights (DispatchQuery.ZoneWeights, T-027):
	// the source is scheduler.zone_weights, names are resolved to zone_id by
	// assembly time (ResolveZoneWeights, T-106); nil — equal weights.
	ZoneWeights map[int16]int
	// Log — loop logger; nil — records suppressed (slog.DiscardHandler).
	Log *slog.Logger
}

func (o EnqueueOptions) withDefaults() EnqueueOptions {
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultEnqueueBatch
	}
	if o.Interval <= 0 {
		o.Interval = DefaultEnqueueInterval
	}
	if o.IdleWait <= 0 {
		o.IdleWait = DefaultEnqueueIdleWait
	}
	return o
}

// EnqueueLoop — the working enqueue loop (spec 5.3): DispatchBatch (selection
// of a pending batch, T-027) → EnqueueBatch (the move to queued with a lease +
// q:dns publication) → a pause; an empty batch or a step error — waiting
// for replenishment/the pause and a new circle. Runs until ctx is cancelled and returns nil —
// the OnRun framework contract (T-012: graceful shutdown finishes the current
// batch; a loop error does not stop the service — a Warn is logged, the next
// iteration will retry, the DB state is consistent: the move and publication
// are idempotent from the next circle's point of view, hung rows are returned by
// lease T-029). The backpressure gate is connected by a queue wrapper in the service
// assembly (queue.WatermarkPublisher, T-026) — the loop publishes through
// the Queue interface and does not know about the gate.
//
// Logging mutants (T-M05): the loop steps' Warn and the batch outcome's Debug do not
// change the observable behavior (errors are already handled by wait/continue) —
// suppressed per docs/mutation-testing.md §4 (logging is not tested).
// mutator-disable-regexp log\.Warn\("enqueue loop statement/remove
// mutator-disable-regexp log\.Debug\("batch enqueued statement/remove
func EnqueueLoop(ctx context.Context, pool db.DB, q queue.Queue, opts EnqueueOptions) error {
	if pool == nil {
		return errors.New("scheduler: enqueue loop: pool not set")
	}
	if q == nil {
		return errors.New("scheduler: enqueue loop: queue not set")
	}
	o := opts.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("batch enqueue loop into q:dns",
		"batch", o.BatchSize, "interval", o.Interval.String(), "idle_wait", o.IdleWait.String(),
		"zone_weights", len(o.ZoneWeights))

	// wait is interrupted by ctx cancellation: leaving the wait is part of the stop.
	wait := func(d time.Duration) {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
		case <-t.C:
		}
	}

	for {
		batch, err := DispatchBatch(ctx, pool, DispatchQuery{
			Limit:       o.BatchSize,
			ZoneWeights: o.ZoneWeights,
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Warn("enqueue loop: batch selection", "err", err)
			wait(o.IdleWait)
			continue
		}
		if len(batch) == 0 {
			// pending is empty — wait for replenishment (recheck T-030,
			// retry T-031, lease T-029, ingest T-018 return it).
			wait(o.IdleWait)
			continue
		}

		res, err := EnqueueBatch(ctx, pool, q, batch)
		if err != nil {
			// Equivalent mutant (T-M05): without this return the loop
			// ends on the next revolution — wait is interrupted by ctx.Done,
			// DispatchBatch on a cancelled ctx returns an error, the exit there
			// is nil too (the OnRun contract); the difference is an extra Warn.
			// mutator-disable-next-line branch/if
			if ctx.Err() != nil {
				return nil
			}
			// The res.Queued rows left unpublished will be returned
			// by lease (T-029): the DB state is consistent; the pause does not
			// push new batches into queued while the queue is unavailable.
			log.Warn("enqueue loop: batch enqueue",
				"err", err, "batch", len(batch), "queued", res.Queued)
			wait(o.IdleWait)
			continue
		}
		log.Debug("batch enqueued into q:dns",
			"published", res.Published, "skipped", res.Skipped)
		wait(o.Interval)
	}
}

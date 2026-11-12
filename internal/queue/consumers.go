// Consumer group hygiene (T-155): every XREADGROUP read pins entries to
// a group consumer (the name — the worker process with a pid), and Redis
// does not clean consumers up itself — after restarts (wave deploys,
// crashes, OOM killers) they accumulate in the groups forever: 4 deploys
// of wave 21 left 11 dead pid-consumers in q:render and 13 in q:http
// (docs/operations.md §5). On their own they are metadata strings
// (pennies of memory), but they pollute the XINFO CONSUMERS/XPENDING
// triage during incidents, and some dead consumers hold dangling pending
// (430 entries on http-worker-455745): such entries are saved only by a
// live worker's claim (XAUTOCLAIM, T-153) — the cleanup does NOT touch
// them.
//
// The mechanism — a periodic XGROUP DELCONSUMER of strictly empty
// consumers (pending = 0 and inactive ≥ redis.consumer_idle):
//
//   - pending ≠ 0 — never: DELCONSUMER removes the consumer together
//     with its PEL entries — an undelivery forever (the one-off tasks of
//     q:http/q:render/q:analyze are not restored by lease — only by a
//     claim from the PEL; PostgreSQL as the source of truth saves only a
//     future recheck re-check, months away). A dead consumer's dangling
//     pending does not hinder the claim triage (XAUTOCLAIM re-pins
//     entries to the live one regardless of the owner) — there is no
//     reason to delete it;
//   - inactivity (Idle from XINFO CONSUMERS, the time since the
//     consumer's last interaction) — protection against deleting a live
//     worker's consumer under load: an active stage touches the consumer
//     in batches of reads, the lull between deliveries is seconds. The
//     2 h threshold covers everything except an honest stage idle; a
//     live consumer of an empty stream may idle longer (XREADGROUP
//     without entries does not touch the consumer — idle grows), but
//     deleting it is harmless: the name is recreated by the first read
//     with entries, and the consumer produces no empty reads.
//
// The "revival" race: between XINFO CONSUMERS and DELCONSUMER a lurking
// worker may receive a delivery (an entry appeared, XREADGROUP pinned it
// to the candidate consumer) — DELCONSUMER would take away its PEL. The
// window is narrowed by a re-check: immediately before the deletion,
// XPENDING EXTENDED for one consumer (Start "-", End "+", Count 1, the
// Consumer filter) confirms the PEL is empty — the "revival" window of
// the whole XINFO pass over the group shrinks to a couple of commands
// per candidate. The DELCONSUMER answer (the number of PEL entries
// removed together with the consumer) is checked: > 0 — the race did
// happen anyway, an error is returned for the loop's Warn log — the
// entries need manual triage (there is no re-issue by the identifiers
// from the task, see above about the undelivery).
//
// The CleanupConsumersLoop loop — a sibling of TrimLoop (T-141): it
// lives in the scheduler service (a single owner of queue stream
// hygiene), the period is the same redis.trim_interval (the pass is
// cheap: XINFO CONSUMERS per stream × group, the other commands — only
// for candidate consumers, so a separate pace knob is not needed), the
// inactivity threshold — redis.consumer_idle. While the scheduler is
// stopped, dead consumers accumulate harmlessly (metadata; the cleanup
// will catch up).

package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultConsumerIdle — the consumer inactivity threshold for deletion
// (T-155): 2 h with a margin above the lull between deliveries of an
// active stage (seconds) and shorter than the between-deploy interval
// (a consumer dead after a restart is cleaned up in the nearest passes).
// Setting — redis.consumer_idle.
const DefaultConsumerIdle = 2 * time.Hour

// DefaultConsumerInterval — the cleanup pass period
// (CleanupConsumersLoop). A minute — the same price category of hygiene
// as freeing the prefix (DefaultTrimInterval): a pass — XINFO CONSUMERS
// per stream × group plus commands only for candidate consumers;
// production wiring passes redis.trim_interval explicitly (one pace for
// the scheduler's sweepers).
const DefaultConsumerInterval = time.Minute

// WorkingGroups — the working stream → consumer group pairs (T-155):
// each stage has one group per stream (spec 4.2; the invariant of the
// freeing mechanism of T-141 "one consumer group per working stream",
// docs/redis.md §2). The names mirror the DefaultGroup of the worker
// packages (the queue package cannot import them directly — workers are
// built on top of queue, an import cycle); the match is checked by the
// pin test consumers_pin_test.go (the external queue_test package).
// q:render-fallback — the render-fallback-worker group of the worker B
// profile (FR-12: the same renderworker binary, a separate group).
// q:dead is not included: a stream without a group (manual triage) — no
// consumers.
func WorkingGroups() map[string]string {
	return map[string]string{
		StreamDNS:            "dns-worker",             // dnsworker.DefaultGroup
		StreamHTTP:           "http-worker",            // httpworker.DefaultGroup
		StreamRender:         "render-worker",          // renderworker.DefaultGroup
		StreamRenderFallback: "render-fallback-worker", // the worker B profile (T-240, FR-12)
		StreamAnalyze:        "analyzer",               // analyzer.DefaultGroup
	}
}

// consumerGroups — the go-redis window for consumer hygiene: XINFO
// CONSUMERS (the group's list with each member's pending and idle),
// XPENDING EXTENDED (re-checking the emptiness of a candidate's PEL) and
// XGROUP DELCONSUMER. Narrow, like trimStreams (trim.go): *redis.Client
// and *redis.ClusterClient satisfy the interface, tests substitute a
// fake.
type consumerGroups interface {
	XInfoConsumers(ctx context.Context, key, group string) *redis.XInfoConsumersCmd
	XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd
	XGroupDelConsumer(ctx context.Context, stream, group, consumer string) *redis.IntCmd
}

// ConsumerOptions — the consumer hygiene parameters; zero values are
// replaced with defaults (withDefaults, the TrimOptions style).
type ConsumerOptions struct {
	// Groups — the stream → group pairs to clean; nil — the working task
	// pairs (WorkingGroups). A pair with a stream unknown to the package
	// — a pass error (as in TrimOnce: the names are package constants).
	Groups map[string]string
	// Idle — the consumer inactivity threshold for deletion; 0 —
	// DefaultConsumerIdle. Consumers with pending ≠ 0 are not deleted at
	// any threshold.
	Idle time.Duration
	// Interval — the CleanupConsumersLoop pass period; 0 —
	// DefaultConsumerInterval. The field controls only the loop,
	// CleanupConsumersOnce always performs a single pass.
	Interval time.Duration
	// Log — the CleanupConsumersLoop loop logger; nil — entries are
	// suppressed (slog.DiscardHandler, the RecoverOptions style). Once
	// does not log.
	Log *slog.Logger
}

func (o ConsumerOptions) withDefaults() ConsumerOptions {
	if len(o.Groups) == 0 {
		o.Groups = WorkingGroups()
	}
	if o.Idle <= 0 {
		o.Idle = DefaultConsumerIdle
	}
	if o.Interval <= 0 {
		o.Interval = DefaultConsumerInterval
	}
	return o
}

// CleanupConsumersOnce — one hygiene pass over the option pairs: XINFO
// CONSUMERS of each group, XGROUP DELCONSUMER of strictly empty and
// inactive consumers (see the file header). Returns the number removed
// across all groups. One group's error does not cancel the others (the
// Reliab.Claim/TrimOnce style): the errors are collected into the return
// (errors.Join), the valid part is processed — the next pass will retry
// the failed one.
//
// A stream without a key (not created yet) and a group without a key
// (lazy creation at the first read, redisq.go) — an empty pass without
// an error: nothing to clean.
func CleanupConsumersOnce(ctx context.Context, rdb consumerGroups, opt ConsumerOptions) (int, error) {
	if rdb == nil {
		return 0, errors.New("queue: consumer cleanup: Redis client not set")
	}
	o := opt.withDefaults()
	// The pairs are traversed in ascending stream-name order — a
	// deterministic order of the pass's errors and logs (a Go map is
	// iterated in random order).
	streams := make([]string, 0, len(o.Groups))
	for s := range o.Groups {
		streams = append(streams, s)
	}
	sort.Strings(streams)
	var (
		removed int
		errs    []error
	)
	for _, stream := range streams {
		group := o.Groups[stream]
		if !knownStream(stream) {
			errs = append(errs, fmt.Errorf("queue: consumer cleanup: unknown stream %q (queue streams are package constants: StreamDNS, StreamHTTP, StreamRender, StreamRenderFallback, StreamAnalyze, StreamDead)", stream))
			continue
		}
		if group == "" {
			errs = append(errs, fmt.Errorf("queue: consumer cleanup %s: group not set", stream))
			continue
		}
		// The group counter is exact under any outcome: the increment
		// only after a successful DELCONSUMER — it is counted even when
		// neighbouring consumers error (the "valid part is processed"
		// style).
		n, err := cleanupGroup(ctx, rdb, stream, group, o)
		removed += n
		if err != nil {
			errs = append(errs, err)
		}
	}
	return removed, errors.Join(errs...)
}

// cleanupGroup cleans the strictly empty consumers of one group: a
// candidate — pending = 0 and idle ≥ the threshold (XINFO CONSUMERS);
// the deletion is preceded by a PEL re-check of the candidate (the
// revival race — the file header). The DELCONSUMER answer is the number
// of PEL entries removed: zero is required by construction, non-zero —
// the race happened, an error to signal the operator.
func cleanupGroup(ctx context.Context, rdb consumerGroups, stream, group string, opt ConsumerOptions) (int, error) {
	consumers, err := rdb.XInfoConsumers(ctx, stream, group).Result()
	if err != nil {
		// No stream (not created yet) or no group (created lazily at the
		// first read, redisq.go) — nothing to clean, not an error (as a
		// missing key in TrimOnce).
		if errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "no such key") || isNoGroup(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("queue: consumer cleanup %s (group %s): %w", stream, group, err)
	}
	var (
		removed int
		errs    []error
	)
	for _, c := range consumers {
		if c.Pending != 0 || c.Idle < opt.Idle {
			continue // holds a PEL or is active — not touched (the file header)
		}
		pend, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream:   stream,
			Group:    group,
			Start:    "-",
			End:      "+",
			Count:    1,
			Consumer: c.Name, // exactly this consumer: confirming the PEL is empty before the deletion
		}).Result()
		if err != nil {
			// Emptiness not confirmed — do not delete: the next pass
			// will retry (the consumer stays in the group, nothing is
			// lost).
			errs = append(errs, fmt.Errorf("queue: consumer cleanup %s (group %s): pending check %s: %w", stream, group, c.Name, err))
			continue
		}
		if len(pend) > 0 {
			// It received deliveries between XINFO and the re-check —
			// now it holds a PEL, not a candidate.
			continue
		}
		pel, err := rdb.XGroupDelConsumer(ctx, stream, group, c.Name).Result()
		if err != nil {
			errs = append(errs, fmt.Errorf("queue: consumer cleanup %s (group %s): deleting %s: %w", stream, group, c.Name, err))
			continue
		}
		removed++
		if pel > 0 {
			// The revival race did happen: PEL entries were removed
			// together with the consumer — an undelivery forever (the
			// file header), triage by the journal is required.
			errs = append(errs, fmt.Errorf("queue: consumer cleanup %s (group %s): deleting %s removed %d pending entries (the revival race) — the entries will never be delivered, triage them manually by the subject identifiers", stream, group, c.Name, pel))
		}
	}
	return removed, errors.Join(errs...)
}

// CleanupConsumersLoop — the background consumer hygiene loop (T-155):
// a CleanupConsumersOnce pass at start (a scheduler restart immediately
// cleans up what worker restarts accumulated) and then every Interval.
// The entries removed per pass are summed and logged as Info (a pass
// without victims is silent — the norm); a pass error does not stop the
// loop: it is logged as Warn, the next pass will retry — a late cleanup
// costs only metadata strings in the groups. It runs until ctx is
// cancelled and returns nil — the OnRun contract of the framework
// (T-012, graceful shutdown, spec 3.4).
func CleanupConsumersLoop(ctx context.Context, rdb consumerGroups, opt ConsumerOptions) error {
	if rdb == nil {
		return errors.New("queue: consumer cleanup loop: Redis client not set")
	}
	o := opt.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	pairs := make([]string, 0, len(o.Groups))
	for s, g := range o.Groups {
		pairs = append(pairs, s+"="+g)
	}
	sort.Strings(pairs)
	log.Info("dead consumer group cleanup loop",
		"groups", strings.Join(pairs, ","), "idle", o.Idle.String(), "interval", o.Interval.String())

	cleanup := func() {
		removed, err := CleanupConsumersOnce(ctx, rdb, o)
		if err != nil && ctx.Err() == nil {
			log.Warn("dead consumer group cleanup", "err", err)
		}
		if removed > 0 {
			log.Info("dead consumer groups removed", "removed", removed, "idle", o.Idle.String())
		}
	}

	cleanup()
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			cleanup()
		}
	}
}

// Releasing the acknowledged prefix of streams (T-141, spec 4.2
// "Addendum 2026-10-23"): XACK removes an entry from the group's pending
// list, but not from the stream — the acknowledged history would
// accumulate up to the MAXLEN cap (T-025) and overflow the Redis budget
// of ≤ 0.5 GB (spec 3.2): the daily run of T-093 hit
// "OOM command not allowed" with the q:dns sink at 1735 batches
// ≈ 410 MiB (docs/load-measurements.md §11). The release is a periodic
// XTRIM MINID at the oldest entry that must remain in the stream: only
// delivered and acknowledged data is removed, unprocessed tasks and
// stuck PEL entries are not touched.
//
// The trim point is the minimum across the groups of the stream (in
// production a working stream has one group — the stage's consumers,
// docs/redis.md §2):
//
//   - a group with stuck entries (pending without XACK): the oldest
//     pending entry. Everything below it is already delivered —
//     XREADGROUP ">" hands out entries in id order — and acknowledged
//     (otherwise it would be in pending): a stuck PEL entry is never
//     removed;
//   - a group with an empty pending: the last delivered id (XINFO GROUPS
//     last-delivered-id). Everything below is delivered and
//     acknowledged, everything above is not delivered yet — and stays.
//
// Why XTRIM MINID and not XDEL after every Ack (spec 4.2 allows both):
//
//   - the load on Redis: one pass — XINFO GROUPS + XPENDING (for groups
//     with pending) + XTRIM per stream, ~15 commands per minute across
//     five streams, versus an extra XDEL command per acknowledged entry
//     (thousands per minute at the HTTP stage's peak, the T-139
//     measurement);
//   - atomicity: XTRIM is one command, the trim point is computed from
//     data that cannot get worse by the time it executes: new pending
//     appear only on entries above last-delivered (entry ids are
//     monotonic), below the computed point the acknowledged prefix only
//     grows. The race "read the point → trim" is safe without a
//     transaction;
//   - consumer groups: the PEL is not touched (by construction it holds
//     no entries below the oldest pending), lag is not distorted
//     (undelivered data is not trimmed). A group created later with the
//     start "0" (lazy creation, redisq.go) will not see the trimmed
//     data — but that is already acknowledged work with the result in
//     the DB: redelivery is not needed (the source of truth is
//     PostgreSQL, spec 3.4).
//
// The mechanism repeats the proven server-side incident fix (the unit
// t092-trimhist: XTRIM MINID at the oldest pending every 2 min) — T-141
// builds it into the code: the TrimLoop loop lives in the scheduler
// service (the single owner of the queue streams, next to the lease pass
// and the backpressure gate; redis.trim_interval). While the scheduler
// is stopped, the acknowledged history accumulates no higher than the
// MAXLEN caps — the sum of the caps fits the budget (reliab.go, T-141).
//
// A stream without groups is not released and does not count as an
// error: acknowledgments never happen (q:dead — manual triage without a
// consumer group), the stream cap is MAXLEN with tiny entries (T-105).
// XTRIM is not run against such a stream at all — between service
// restarts the streams live with lazily created groups, and the trim
// does not depend on them.

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

// DefaultTrimInterval — the period of release passes (TrimLoop). A minute
// balances memory and load: between passes at most a minute of
// acknowledged entries accumulates (at the current stage rates:
// q:dns ≤ 60 batches/min ≈ 3.8 MiB with a batch published every second,
// unit ≈ 2 MiB per stream) — single-digit percentages of the spec 3.2
// budget, while a pass costs three commands per stream.
const DefaultTrimInterval = time.Minute

// workingStreams — the working task streams (spec 4.2) released by
// acknowledgement: allStreams without q:dead (without a consumer group
// there is no acknowledged prefix — the stream cap is MAXLEN with tiny
// entries, T-105). Filtering over allStreams — a new working stream added
// to allStreams is released automatically (the pin test
// TestTrimOptionsDefaults cross-checks the sets).
func workingStreams() []string {
	out := make([]string, 0, len(allStreams)-1)
	for _, s := range allStreams {
		if s != StreamDead {
			out = append(out, s)
		}
	}
	return out
}

// trimStreams — the go-redis window for releasing the acknowledged
// prefix: XINFO GROUPS (the groups' pending and last-delivered-id),
// XPENDING (the oldest pending entry), XTRIM MINID and XLEN (the stuck
// prefix signal). Narrow, like redisStreams (redisq.go): *redis.Client
// and *redis.ClusterClient satisfy the interface, tests substitute a
// fake.
type trimStreams interface {
	XInfoGroups(ctx context.Context, key string) *redis.XInfoGroupsCmd
	XPending(ctx context.Context, stream, group string) *redis.XPendingCmd
	XTrimMinID(ctx context.Context, key, minID string) *redis.IntCmd
	XLen(ctx context.Context, key string) *redis.IntCmd
}

// TrimOptions — the release parameters; zero values are replaced with
// defaults (withDefaults, the RecoverOptions style).
type TrimOptions struct {
	// Streams — the streams to release; nil — the working task streams
	// (q:dns, q:http, q:render, q:render-fallback, q:analyze). q:dead is
	// not included: without a consumer group there is no acknowledged
	// prefix.
	Streams []string
	// Interval — the period of TrimLoop passes; 0 — DefaultTrimInterval.
	// TrimOnce makes a pass anyway — the field only controls the loop.
	Interval time.Duration
	// MaxLen — the stream length limit for the stuck prefix signal
	// (TrimLoop): a stream length above three quarters of the limit with
	// live groups — the trim point is not advancing (for example, an
	// orphaned group with a frozen last-delivered-id holds the minimum
	// across groups). Mirrors redis.maxlen; 0 — the per-stream limits of
	// the T-025/T-141 policy (DefaultStreamMaxLen).
	MaxLen int64
	// Log — the logger of the TrimLoop loop; nil — records are suppressed
	// (slog.DiscardHandler, the RecoverOptions style). TrimOnce does not
	// log.
	Log *slog.Logger
}

func (o TrimOptions) withDefaults() TrimOptions {
	if len(o.Streams) == 0 {
		o.Streams = workingStreams()
	}
	if o.Interval <= 0 {
		o.Interval = DefaultTrimInterval
	}
	return o
}

// streamMaxLen — the stream length limit for the stuck prefix signal:
// explicit from the options or the per-stream policy
// (DefaultStreamMaxLen).
func (o TrimOptions) streamMaxLen(stream string) int64 {
	if o.MaxLen > 0 {
		return o.MaxLen
	}
	return DefaultStreamMaxLen(stream)
}

// TrimOnce — one release pass over the option streams: for each it
// computes the safe trim point (the minimum across groups — see the file
// header) and runs XTRIM MINID. Returns the number of removed entries
// across all streams and the list of stuck streams — a length (XLEN)
// above three quarters of the limit with live groups: the acknowledged
// prefix grows while the point is not advancing (a signal to the
// operator — TrimLoop logs a Warn; the gate depth (min(lag, XLEN) +
// pending, T-168) does not see this: the groups' backlog is at zero).
// An error of one stream does not cancel the others (the Reliab.Claim
// style): errors are collected into the return (errors.Join), the valid
// part is processed — the next pass will repeat the failed one.
//
// A stream without a key (never created yet) and a stream without groups
// — an empty pass without an error: there is nothing to release.
func TrimOnce(ctx context.Context, rdb trimStreams, opt TrimOptions) (int64, []string, error) {
	if rdb == nil {
		return 0, nil, errors.New("queue: освобождение стримов: клиент Redis не задан")
	}
	o := opt.withDefaults()
	var (
		trimmed int64
		stuck   []string
		errs    []error
	)
	for _, stream := range o.Streams {
		n, isStuck, err := trimStream(ctx, rdb, stream, o)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		trimmed += n
		if isStuck {
			stuck = append(stuck, stream)
		}
	}
	return trimmed, stuck, errors.Join(errs...)
}

// trimStream releases the acknowledged prefix of one stream: the trim
// point is the oldest entry that must remain (the minimum across
// groups), XTRIM MINID removes strictly below it. Pending entries and
// the undelivered tail are never removed (at-least-once, spec 3.4).
// isStuck — the stream length above three quarters of the
// opt.streamMaxLen limit with live groups: the trim point is not
// advancing and the prefix accumulates — a regular trim does not cure
// this, the stream's groups need triage.
func trimStream(ctx context.Context, rdb trimStreams, stream string, opt TrimOptions) (int64, bool, error) {
	if !knownStream(stream) {
		return 0, false, fmt.Errorf("queue: освобождение: неизвестный стрим %q (стримы очереди — константы пакета: StreamDNS, StreamHTTP, StreamRender, StreamRenderFallback, StreamAnalyze, StreamDead)", stream)
	}
	groups, err := rdb.XInfoGroups(ctx, stream).Result()
	if err != nil {
		// The stream does not exist yet — Redis answers with a no such key
		// error, not an empty list (as with the gate depth,
		// backpressure.go): there is nothing to release, not an error.
		if errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "no such key") {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("queue: освобождение %s: группы: %w", stream, err)
	}
	if len(groups) == 0 {
		// No groups (as with q:dead): there is no acknowledged prefix, the
		// stream cap is MAXLEN. An empty pass, not an error.
		return 0, false, nil
	}
	safe := ""
	for _, g := range groups {
		cand := g.LastDeliveredID
		if g.Pending > 0 {
			p, err := rdb.XPending(ctx, stream, g.Name).Result()
			if err != nil {
				// A PEL read error — trimming by last-delivered-id is
				// unsafe: pending entries may sit below it. The stream
				// is skipped entirely: the next pass will retry.
				return 0, false, fmt.Errorf("queue: освобождение %s (группа %s): pending: %w", stream, g.Name, err)
			}
			if p.Count > 0 {
				// A race: between XINFO and XPENDING everything was
				// acknowledged — the point stays at last-delivered-id
				// (delivered data is acknowledged).
				cand = p.Lower
			}
		}
		if cand == "" {
			continue // the group delivered nothing — no constraint
		}
		if safe == "" || streamIDGreater(safe, cand) {
			// The numerically smaller id: the lexicographic order "999-0" >
			// "1000-0" is deceptive (trackProgress, redisq.go).
			safe = cand
		}
	}
	var (
		n       int64
		isStuck bool
	)
	if safe != "" {
		n, err = rdb.XTrimMinID(ctx, stream, safe).Result()
		if err != nil {
			return 0, false, fmt.Errorf("queue: освобождение %s (XTRIM MINID %s): %w", stream, safe, err)
		}
	}
	// A stuck prefix: the groups are alive (they exist) while the length
	// is at the cap — the backlog depth can be zero at the same time, and
	// the gate with the alerts stay silent: the trim point is frozen (the
	// minimum across groups is held by an orphaned group or a stuck
	// last-delivered-id).
	length, err := rdb.XLen(ctx, stream).Result()
	if err != nil {
		// The length is a signal, not an invariant: an XLEN error does not
		// cancel the trim.
		return n, false, fmt.Errorf("queue: освобождение %s: длина: %w", stream, err)
	}
	if limit := opt.streamMaxLen(stream); length*4 >= limit*3 {
		isStuck = true
	}
	return n, isStuck, nil
}

// TrimLoop — the background loop releasing the acknowledged prefix
// (T-141): a TrimOnce pass at startup (a service restart immediately
// releases the accumulated history) and then every Interval. A pass
// error does not stop the loop: a Warn is logged, the next pass will
// retry — unacknowledged data is not removed, a late trim only costs
// memory up to the MAXLEN caps. A stuck stream (the length at the cap
// with live groups — the trim point is frozen, see TrimOnce) is
// signaled with a Warn once per episode: the acknowledged prefix is not
// visible to the gate depth (min(lag, XLEN) + pending — the groups'
// backlog is at zero, T-168), and without the signal the silent growth
// would repeat T-093.
// Runs until ctx is canceled and returns nil — the OnRun contract of the
// service framework (T-012, graceful shutdown, spec 3.4).
func TrimLoop(ctx context.Context, rdb trimStreams, opt TrimOptions) error {
	if rdb == nil {
		return errors.New("queue: цикл освобождения: клиент Redis не задан")
	}
	o := opt.withDefaults()
	log := o.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	log.Info("цикл освобождения подтверждённого префикса стримов",
		"streams", strings.Join(o.Streams, ","), "interval", o.Interval.String())

	warned := make(map[string]bool, len(o.Streams)) // a Warn once per stuck episode
	trim := func() {
		_, stuck, err := TrimOnce(ctx, rdb, o)
		if err != nil && ctx.Err() == nil {
			log.Warn("освобождение подтверждённого префикса", "err", err)
		}
		stuckSet := make(map[string]bool, len(stuck))
		for _, s := range stuck {
			stuckSet[s] = true
			if !warned[s] {
				warned[s] = true
				log.Warn("подтверждённый префикс стрима у потолка, точка обрезки не двигается — проверьте группы стрима (осиротевшая группа с замороженным last-delivered-id держит обрезку)",
					"stream", s, "maxlen", o.streamMaxLen(s))
			}
		}
		for s := range warned { // the episode is over — we can warn again
			if !stuckSet[s] {
				delete(warned, s)
			}
		}
	}

	trim()
	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			trim()
		}
	}
}

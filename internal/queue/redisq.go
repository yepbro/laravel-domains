// The Queue implementation on Redis Streams (spec 4.2): publishing via
// XADD, batch reads via XREADGROUP ">" through a consumer group,
// acknowledgement via XACK. The go-redis commands stay within the narrow
// redisStreams interface — the queue can be replaced with a fake in unit
// tests (without a live Redis).

package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// fieldData — the only field of a stream entry: a JSON message
// (message.go).
const fieldData = "data"

// DefaultRedisBlock — the default wait interval for new entries in one
// Consume call. Small rather than infinite: the worker loop wakes up
// regularly (the context is canceled between iterations), and the share
// of "empty" wake-ups is small relative to the batch processing time.
const DefaultRedisBlock = 2 * time.Second

// DefaultPublishRetries — extra XADD attempts on a Redis infrastructure
// failure (T-152): the result is already computed, a short retry series
// covers second-long connection flaps and borderline OOM moments without
// handing the delivery into the claim circle (every claim is +1 attempt
// toward q:dead).
const DefaultPublishRetries = 2

// DefaultPublishRetryPause — the pause before the first XADD retry
// (doubling on each next one, the shift is capped — Duration overflow):
// 250 ms + 500 ms ≈ 0.75 s worst case on top of publishing — nothing
// against the processing time of a task of any stage (seconds).
const DefaultPublishRetryPause = 250 * time.Millisecond

// redisStreams — a window into go-redis: only the stream commands the
// queue needs. *redis.Client (queue.NewClient) and *redis.ClusterClient
// satisfy the interface; tests substitute a fake — the go-redis cmd
// results are built by the New*Result factories without a connection.
type redisStreams interface {
	XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd
	XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd
	XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd
	XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd
}

// RedisOptions — the parameters of the queue's Redis implementation.
type RedisOptions struct {
	// Block — the wait time for new entries in one Consume call
	// (XREADGROUP BLOCK); 0 — DefaultRedisBlock.
	Block time.Duration
	// MaxLen — an explicit length limit for all streams at publishing
	// (XADD MAXLEN ~); 0 — the per-stream limits of the reliability
	// policy (DefaultStreamMaxLen, T-025: spec 4.2 requires MAXLEN on
	// every stream, so a "do not trim" mode is not provided — the source
	// of truth is PostgreSQL, truncation is recoverable via lease,
	// spec 3.4).
	MaxLen int64
	// PublishRetries — extra XADD attempts on a Redis infrastructure
	// failure (T-152, InfraError); 0 and below — DefaultPublishRetries.
	// Value/serialization errors are not retried: a retry would produce
	// the same answer.
	PublishRetries int
	// PublishRetryPause — the pause before the first XADD retry
	// (doubling on each next one); 0 and below —
	// DefaultPublishRetryPause.
	PublishRetryPause time.Duration
}

// RedisQueue — the Queue implementation on Redis Streams. The consumer
// group is created lazily on the first read of a stream: XGROUP CREATE
// with MKSTREAM (the stream is created empty) and the start identifier
// "0" — the first consumers also receive the entries published before
// they started; a BUSYGROUP answer (another worker already created the
// group) is not an error. When the group is lost (NOGROUP: the group or
// stream was deleted, a Redis restart without persistence) the group is
// recreated from the last id read by this instance — not from "0":
// replaying up to a million already processed entries of the stream
// (the MAXLEN of unit streams) is not needed, duplicates with other
// instances are bounded by their read window and absorbed by
// idempotency (spec 3.4).
type RedisQueue struct {
	rdb    redisStreams
	opt    RedisOptions
	groups sync.Map // "stream|group" → the last read id ("0" — from the start; the group is created)
}

// NewRedisQueue builds the queue on top of a go-redis client (NewClient
// or a ready-made *redis.Client / *redis.ClusterClient).
func NewRedisQueue(rdb redisStreams, opt RedisOptions) *RedisQueue {
	if opt.Block <= 0 {
		opt.Block = DefaultRedisBlock
	}
	if opt.PublishRetries <= 0 {
		opt.PublishRetries = DefaultPublishRetries
	}
	if opt.PublishRetryPause <= 0 {
		opt.PublishRetryPause = DefaultPublishRetryPause
	}
	return &RedisQueue{rdb: rdb, opt: opt}
}

// Publish serializes the message (the data field) and appends an entry
// to the stream with XADD, returning its identifier. Every XADD trims
// the stream with the approximate MAXLEN ~ limit (spec 4.2): the
// explicit one from RedisOptions.MaxLen or the stream limit of the
// T-025 policy (DefaultStreamMaxLen). The stream must match the message
// type, the values are validated before writing (an error — the message
// is not published). An infrastructure failure of XADD (OOM/connection,
// T-152) is retried PublishRetries times with a doubling pause;
// sustained unavailability — InfraError.
func (q *RedisQueue) Publish(ctx context.Context, stream string, msg Message) (string, error) {
	if !knownStream(stream) {
		return "", fmt.Errorf("queue: неизвестный стрим %q (рабочие стримы — константы пакета: StreamDNS, StreamHTTP, StreamRender, StreamRenderFallback, StreamAnalyze, StreamDead)", stream)
	}
	if !streamAllowed(msg, stream) {
		return "", fmt.Errorf("queue: сообщение %T публикуется в %v, а не в %q", msg, msg.Streams(), stream)
	}
	if err := msg.validate(); err != nil {
		return "", err
	}
	data, err := marshalMessage(msg)
	if err != nil {
		return "", fmt.Errorf("queue: сериализовать %T: %w", msg, err)
	}
	args := &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{fieldData: data},
	}
	maxLen := q.opt.MaxLen
	if maxLen == 0 {
		maxLen = DefaultStreamMaxLen(stream) // the T-025 policy (spec 4.2)
	}
	args.MaxLen = maxLen
	args.Approx = true // MAXLEN ~: trimming without an exact count (T-025)
	id, err := q.rdb.XAdd(ctx, args).Result()
	if err != nil && infraCause(err) {
		// An infrastructure failure (T-152): the result is already
		// computed — a short series of XADD retries with a doubling pause
		// covers connection flaps and borderline OOM moments without
		// handing the delivery into the claim circle (every claim is +1
		// attempt toward q:dead, the October 22 incident).
		// Non-infrastructure errors are not retried: a retry would produce
		// the same answer.
		for attempt := 1; err != nil && attempt <= q.opt.PublishRetries && ctx.Err() == nil; attempt++ {
			pause := q.opt.PublishRetryPause << min(attempt-1, 5)
			select {
			case <-ctx.Done():
			case <-time.After(pause):
			}
			if ctx.Err() != nil {
				break
			}
			id, err = q.rdb.XAdd(ctx, args).Result()
			if err != nil && !infraCause(err) {
				break
			}
		}
	}
	if err != nil {
		if infraCause(err) {
			// Sustained write unavailability: the InfraError marker —
			// workers stop reading on it, RecoverLoop does not claim
			// (T-152); no XACK — the publication did not happen.
			return "", &InfraError{Op: "publish в " + stream, Err: err}
		}
		return "", fmt.Errorf("queue: publish в %s: %w", stream, err)
	}
	return id, nil
}

// Consume reads up to n entries of the stream not yet read by the group
// (XREADGROUP … ">") on behalf of the consumer consumer. No new entries
// — an empty result without an error (the Block expired).
func (q *RedisQueue) Consume(ctx context.Context, stream, group, consumer string, n int) ([]Delivery, error) {
	if !knownStream(stream) {
		return nil, fmt.Errorf("queue: неизвестный стрим %q", stream)
	}
	if group == "" {
		return nil, fmt.Errorf("queue: не задана группа потребителей стрима %s", stream)
	}
	if consumer == "" {
		return nil, fmt.Errorf("queue: не задано имя потребителя группы %s", group)
	}
	if n < 1 {
		return nil, fmt.Errorf("queue: размер пачки %d < 1", n)
	}
	if err := q.ensureGroup(ctx, stream, group, "0"); err != nil {
		return nil, err
	}
	read := &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    int64(n),
		Block:    q.opt.Block,
	}
	res, err := q.rdb.XReadGroup(ctx, read).Result()
	if isNoGroup(err) {
		// Losing Redis with live workers (spec 3.4): the group disappeared
		// together with the stream, and the cache of created groups is
		// stale. We recreate the group — from the last id read by this
		// instance (not from "0": the history before it is already
		// processed, replaying it as an avalanche of duplicate work is not
		// needed, the missing part will be re-published by the scheduler
		// via lease) — and read again: the consumer keeps working without
		// a process restart.
		last, _ := q.groups.Load(groupKey(stream, group))
		start, _ := last.(string)
		if start == "" {
			start = "0" // not a single read — as the first creation
		}
		q.groups.Delete(groupKey(stream, group))
		if err := q.ensureGroup(ctx, stream, group, start); err != nil {
			return nil, err
		}
		res, err = q.rdb.XReadGroup(ctx, read).Result()
	}
	if isNil(err) { // the wait interval expired — no new entries
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("queue: read %s (группа %s): %w", stream, group, err)
	}
	out := make([]Delivery, 0, len(res))
	for _, xs := range res {
		for _, m := range xs.Messages {
			out = append(out, Delivery{
				Stream:  xs.Stream,
				ID:      m.ID,
				Payload: payloadBytes(m.Values),
			})
		}
		q.trackProgress(stream, group, xs.Messages)
	}
	return out, nil
}

// Ack acknowledges entry processing (XACK). Acknowledge only after the
// result is written to the DB (spec 4.2): an unacknowledged entry will
// be redelivered (the return of stuck entries — XAUTOCLAIM, T-025).
func (q *RedisQueue) Ack(ctx context.Context, stream, group string, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	if err := q.rdb.XAck(ctx, stream, group, ids...).Err(); err != nil {
		if infraCause(err) {
			// An infrastructure failure (T-152): the result is already
			// written, the entry stayed in pending and will come back via
			// redelivery (idempotency, spec 3.4); the marker tells the
			// worker this is not a processing error.
			return &InfraError{Op: fmt.Sprintf("ack %s (группа %s)", stream, group), Err: err}
		}
		return fmt.Errorf("queue: ack %s (группа %s): %w", stream, group, err)
	}
	return nil
}

// ensureGroup creates the consumer group on first use
// (XGROUP CREATE … MKSTREAM): start — the identifier from which the
// group delivers entries ("0" — from the beginning: the first creation
// also receives the entries published before the consumers started; the
// last read id — recovery after NOGROUP, see Consume). Repeated group
// creations (BUSYGROUP from another worker) are not an error. Created
// groups are remembered so as not to spend a round of requests on every
// Consume.
func (q *RedisQueue) ensureGroup(ctx context.Context, stream, group, start string) error {
	key := groupKey(stream, group)
	if _, ok := q.groups.Load(key); ok {
		return nil
	}
	err := q.rdb.XGroupCreateMkStream(ctx, stream, group, start).Err()
	if err != nil && !isBusyGroup(err) {
		return fmt.Errorf("queue: создать группу %s в %s: %w", group, stream, err)
	}
	q.groups.Store(key, start)
	return nil
}

// trackProgress remembers the group's last read id — the starting point
// for recreating the group after NOGROUP (see Consume). Entry ids grow
// (ms-seq), the comparison is numeric: the lexicographic order
// "999-0" > "1000-0" is wrong. Parse errors are silently skipped:
// progress is a recovery optimization, not an invariant.
func (q *RedisQueue) trackProgress(stream, group string, msgs []redis.XMessage) {
	if len(msgs) == 0 {
		return
	}
	key := groupKey(stream, group)
	last, _ := q.groups.LoadOrStore(key, "0")
	cur, _ := last.(string)
	if cur == "" || cur == "0" {
		cur = "0-0" // start marks — in ms-seq form for comparison
	}
	for _, m := range msgs {
		if streamIDGreater(m.ID, cur) {
			cur = m.ID
		}
	}
	if cur != last {
		q.groups.Store(key, cur)
	}
}

// streamIDGreater compares stream entry identifiers "ms-seq" numerically
// by parts.
func streamIDGreater(a, b string) bool {
	ams, aseq, err := splitStreamID(a)
	if err != nil {
		return false
	}
	bms, bseq, err := splitStreamID(b)
	if err != nil {
		return false
	}
	if ams != bms {
		// An equivalent mutant (T-M04): `>` → `>=` is indistinguishable —
		// the branch is reachable only when ams != bms, where the
		// comparisons agree.
		// mutator-disable-next-line expression/comparison
		return ams > bms
	}
	return aseq > bseq
}

// splitStreamID parses "ms-seq" into an int64 pair.
func splitStreamID(id string) (ms, seq int64, err error) {
	s1, s2, ok := strings.Cut(id, "-")
	if !ok {
		return 0, 0, fmt.Errorf("queue: идентификатор записи %q не вида ms-seq", id)
	}
	if ms, err = strconv.ParseInt(s1, 10, 64); err != nil {
		return 0, 0, err
	}
	if seq, err = strconv.ParseInt(s2, 10, 64); err != nil {
		return 0, 0, err
	}
	return ms, seq, nil
}

// payloadBytes extracts the data field from the entry values: go-redis
// returns strings; []byte is supported for direct writes to a stream.
func payloadBytes(values map[string]any) []byte {
	switch v := values[fieldData].(type) {
	case string:
		return []byte(v)
	case []byte:
		return v
	default:
		return nil
	}
}

// groupKey — the key of the created-groups cache.
func groupKey(stream, group string) string { return stream + "|" + group }

// knownStream reports that the name is one of the queue's working
// streams (spec 4.2): the list of names is allStreams (queue.go), the
// single source.
func knownStream(stream string) bool {
	for _, s := range allStreams {
		if s == stream {
			return true
		}
	}
	return false
}

// isBusyGroup recognizes the BUSYGROUP answer — the group already
// exists.
func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

// isNoGroup recognizes the NOGROUP answer — the consumer group is
// missing: the stream or the group was deleted (Redis loss/cleanup with
// live workers).
func isNoGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOGROUP")
}

// isNil — "nothing was read" (including a BLOCK timeout).
func isNil(err error) bool { return errors.Is(err, redis.Nil) }

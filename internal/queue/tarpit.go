// Counter of tarpit-classified deliveries of render tasks (T-261,
// FR-7 of tasks/TZ-fsm.md §3): the application-level Redis key
// render:tarpit:{task_id} is a set of delivery identifiers that ended
// with a page timeout (render.timeout, the worker's tarpitError). The
// tarpit finalization decision is made by the set cardinality
// (SCARD ≥ render.timeout_attempts), not by the raw PEL delivery number:
// mixed series (tarpit alternating with infrastructure failures) stop
// finalizing "by attempt" prematurely — the delivery number grows on
// every delivery, the set only on tarpit-classified ones. The consumer
// is internal/renderworker (the TarpitStore seam, the worker mounts it
// through Options.Tarpits; the specification of the outcome policy
// machine is the doc comment of the internal/renderworker package);
// here — only the key mechanism.

package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// TarpitKey — the key of the set of tarpit-classified deliveries of the
// task taskID: render:tarpit:{task_id}. The task identifier (message.go)
// survives the stream entry and is carried over by the fallback
// publication (T-238 forwards the message whole) — the counter is one
// for all deliveries of the task: the main q:render stream and the
// rework by worker B in q:render-fallback (the tarpit budget is per task
// episode, not per stream; the set members — delivery ids — distinguish
// the entries of the streams). Episode re-publications (the T-066 rework
// pass, the late re-publisher T-259) publish a NEW task_id — their
// counter starts from zero.
func TarpitKey(taskID string) string {
	return "render:tarpit:" + taskID
}

// tarpitClient — the go-redis window for the tarpit counter:
// SADD/EXPIRE/SCard. A separate interface modeled after claimStreams
// (reliab.go): *redis.Client (queue.NewClient), *redis.ClusterClient and
// the unit-test fakes satisfy it without exposing the whole Cmdable.
type tarpitClient interface {
	SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	SCard(ctx context.Context, key string) *redis.IntCmd
}

// TarpitCounter — the counter of tarpit-classified deliveries of render
// tasks (T-261, FR-7) on top of the application-level key
// render:tarpit:{task_id} (TarpitKey). The set is maintained by Redis
// itself (SADD is idempotent per member), the TTL is refreshed by every
// mark — the key lives ttl past the LAST tarpit delivery of the task: an
// episode cannot end earlier than the TTL (the retry horizon is hours),
// and forgotten keys are removed by Redis, not an operator. Losing the
// key/Redis does not corrupt the consumer's decision: Track returns an
// error or a smaller cardinality — the worker degrades to the PEL
// delivery number (the behavior before T-261) and does not finalize
// before the threshold (spec 3.4 — losing Redis is not fatal, the source
// of truth is PostgreSQL).
type TarpitCounter struct {
	rdb tarpitClient
}

// NewTarpitCounter builds the counter on top of the client rdb — the
// same one as the worker's queue (NewClient/NewReliab): the commands are
// targeted, the pool is shared.
func NewTarpitCounter(rdb tarpitClient) *TarpitCounter {
	return &TarpitCounter{rdb: rdb}
}

// Track marks the delivery delivery of the task taskID as
// tarpit-classified and returns the task's set cardinality — how many
// deliveries (including the current one) are classified as tarpit.
// Idempotency — the SADD semantics: marking the same delivery again (a
// delivery-number replay after an attempt refund, T-222; reprocessing of
// a stuck entry) does not grow the counter. The delivery identifier is
// built by the consumer (stream entry × PEL delivery number) — here it
// is an opaque string.
//
// An error of any command is the error of the whole call: the consumer
// degrades to the decision by the PEL delivery number. Part of the
// commands may have applied (SADD without EXPIRE on a break between
// them) — a key without a TTL would survive the cleanup; the next mark
// of the same task sets the TTL again (EXPIRE is refreshed by every
// mark), and the orphaned key of a single delivery is a handful of
// connection-break incidents, with the application-key budget as the
// upper bound (docs/redis.md §2).
func (c *TarpitCounter) Track(ctx context.Context, taskID, delivery string, ttl time.Duration) (int, error) {
	if taskID == "" {
		return 0, fmt.Errorf("queue: tarpit counter: empty task_id")
	}
	if delivery == "" {
		return 0, fmt.Errorf("queue: tarpit counter %s: empty delivery identifier", TarpitKey(taskID))
	}
	if ttl <= 0 {
		return 0, fmt.Errorf("queue: tarpit counter %s: TTL %s ≤ 0 (EXPIRE would delete the key)", TarpitKey(taskID), ttl)
	}
	key := TarpitKey(taskID)
	if _, err := c.rdb.SAdd(ctx, key, delivery).Result(); err != nil {
		return 0, fmt.Errorf("queue: tarpit counter %s: delivery mark %s: %w", key, delivery, err)
	}
	if _, err := c.rdb.Expire(ctx, key, ttl).Result(); err != nil {
		return 0, fmt.Errorf("queue: tarpit counter %s: TTL %s: %w", key, ttl, err)
	}
	n, err := c.rdb.SCard(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("queue: tarpit counter %s: set cardinality: %w", key, err)
	}
	return int(n), nil
}

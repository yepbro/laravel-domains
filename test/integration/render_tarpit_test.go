// Integration test of the render tarpit counter (T-261, TZ-fsm spec
// §3 FR-7) against the live Redis dev environment: the application
// key render:tarpit:{task_id} — SADD is idempotent by delivery id
// (replaying a delivery number after an attempt refund T-222 does not
// grow the cardinality), SCARD returns the number of
// tarpit-classified deliveries, EXPIRE keeps the TTL (48 h, refreshed
// by every mark) — the counter survives worker restarts. The worker
// decision on the set cardinality (tarpitsExhausted) is covered by
// unit tests (internal/renderworker/tarpitcount_test.go) — here we
// check the key semantics itself. Isolation — Redis DB 15
// (resilienceRedisEnv), cleanup removes the created keys.

package integration

import (
	"context"
	"testing"
	"time"

	"crawler/internal/queue"
)

func TestRenderTarpitCounterRedis(t *testing.T) {
	skipUnlessIntegration(t)
	rdb := resilienceRedisEnv(t)
	c := queue.NewTarpitCounter(rdb)
	ctx := context.Background()

	taskID := queue.NewTaskID()
	key := queue.TarpitKey(taskID)
	t.Cleanup(func() { _ = rdb.Del(context.Background(), key).Err() })

	ttl := 48 * time.Hour
	// First tarpit delivery: cardinality 1.
	if n, err := c.Track(ctx, taskID, "1719-1#1", ttl); err != nil || n != 1 {
		t.Fatalf("Track #1 = (%d, %v), want (1, nil)", n, err)
	}
	// Replay of the same delivery (the same entry-id × number): SADD is idempotent.
	if n, err := c.Track(ctx, taskID, "1719-1#1", ttl); err != nil || n != 1 {
		t.Fatalf("Track of a duplicate = (%d, %v), want (1, nil) — one delivery one member", n, err)
	}
	// A new delivery of the same task: cardinality 2 — the
	// render.timeout_attempts threshold (default 2) is reached.
	if n, err := c.Track(ctx, taskID, "1719-1#2", ttl); err != nil || n != 2 {
		t.Fatalf("Track #2 = (%d, %v), want (2, nil)", n, err)
	}
	// A different task — a different set, cardinalities do not mix.
	other := queue.NewTaskID()
	t.Cleanup(func() { _ = rdb.Del(context.Background(), queue.TarpitKey(other)).Err() })
	if n, err := c.Track(ctx, other, "9999-9#1", ttl); err != nil || n != 1 {
		t.Fatalf("Track of another task = (%d, %v), want (1, nil)", n, err)
	}

	// The key TTL is alive and within the horizon: the mark refreshes
	// EXPIRE, the key is not eternal and will not disappear before the
	// end of the episode.
	got := rdb.TTL(ctx, key).Val()
	if got <= 0 || got > ttl {
		t.Errorf("TTL %s = %s, want (0, %s]", key, got, ttl)
	}
	// The key type is a set (manual inspection via redis-cli: SMEMBERS).
	if kind, err := rdb.Type(ctx, key).Result(); err != nil || kind != "set" {
		t.Errorf("TYPE %s = %q (%v), want set", key, kind, err)
	}

	// Validation before commands: invalid arguments do not create keys.
	if _, err := c.Track(ctx, "", "e#1", ttl); err == nil {
		t.Error("empty task_id must be an error")
	}
	if _, err := c.Track(ctx, queue.NewTaskID(), "e#1", 0); err == nil {
		t.Error("TTL 0 must be an error (EXPIRE would delete the key)")
	}
}

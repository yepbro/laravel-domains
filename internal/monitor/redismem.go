// Redis memory source for the redis_memory alert (T-154, spec 5.9/3.2):
// used_memory/maxmemory from INFO memory of the same go-redis client
// that reads stream depths (cmd/monitor). Under the noeviction policy
// (the Redis budget ≤ 0.5 GB, spec 3.2 — docs/redis.md) filling up
// maxmemory means XADD write failures ("OOM command not allowed",
// the T-093 and Oct 22 incidents: docs/load-measurements.md §11.1):
// INFO exposes no direct failure counter; the memory share is a proxy
// metric for upcoming failures.
//
// Parsing is manual, over "key:value" lines (the "# …" sections are
// comments): only three fields are needed, the format is fixed by the
// server; a full INFO parser does not pay for the dependency
// (parseBadHashesExposition style).
package monitor

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// RedisMemory is a Redis memory snapshot from INFO memory.
type RedisMemory struct {
	// UsedBytes is used_memory: the byte size of the whole dataset
	// (server overhead structures included — this is exactly the value
	// capped by maxmemory, not the "useful" stream data).
	UsedBytes int64
	// MaxBytes is maxmemory: the cap in bytes; 0 — no cap
	// (the share alert does not apply then).
	MaxBytes int64
	// Policy is maxmemory_policy (production runs noeviction, spec 3.2).
	Policy string
}

// RedisMemorySource is a Redis memory source; an interface for
// substitution in unit tests (the real read is INFO from a live Redis,
// which tests do not have). WearSource/UnitEventsSource style.
type RedisMemorySource interface {
	// RedisMemory returns an INFO memory snapshot. An error means
	// "not measured", with the reason attached.
	RedisMemory(ctx context.Context) (RedisMemory, error)
}

// redisInfoCmd is a window into go-redis: INFO only (narrow, like
// claimStreams/redisDepth in internal/queue); *redis.Client
// (queue.NewClient) and *redis.ClusterClient satisfy the interface.
type redisInfoCmd interface {
	Info(ctx context.Context, section ...string) *redis.StringCmd
}

// RedisInfo is a RedisMemorySource on top of INFO memory.
type RedisInfo struct {
	// RDB is the Redis client (the same one the depth gate uses,
	// cmd/monitor); nil — a source error.
	RDB redisInfoCmd
}

// compile-time: RedisInfo is a RedisMemorySource.
var _ RedisMemorySource = RedisInfo{}

// RedisMemory reads the memory section of the INFO response and parses it.
func (s RedisInfo) RedisMemory(ctx context.Context) (RedisMemory, error) {
	if s.RDB == nil {
		return RedisMemory{}, fmt.Errorf("monitor: Redis memory: the client is not set")
	}
	raw, err := s.RDB.Info(ctx, "memory").Result()
	if err != nil {
		return RedisMemory{}, fmt.Errorf("monitor: INFO memory: %w", err)
	}
	return parseRedisMemoryINFO(raw)
}

// parseRedisMemoryINFO parses the INFO response: "key:value" lines,
// section headers ("# Memory") and empty lines are skipped; the fields
// used_memory, maxmemory, maxmemory_policy are required. A missing field
// or a non-numeric/negative byte value is an error: the format is fixed
// by the server, a malformed response must not be silently ignored
// (parseMemoryEvents style). Extracted as a pure function for a table
// test without Redis.
func parseRedisMemoryINFO(raw string) (RedisMemory, error) {
	fields := make(map[string]string)
	for _, line := range strings.Split(raw, "\n") {
		if k, v, ok := strings.Cut(strings.TrimRight(line, "\r"), ":"); ok {
			fields[k] = v
		}
	}
	used, err := infoBytes(fields, "used_memory")
	if err != nil {
		return RedisMemory{}, err
	}
	max, err := infoBytes(fields, "maxmemory")
	if err != nil {
		return RedisMemory{}, err
	}
	policy := fields["maxmemory_policy"]
	if policy == "" {
		return RedisMemory{}, fmt.Errorf("monitor: INFO memory: the maxmemory_policy field is missing")
	}
	return RedisMemory{UsedBytes: used, MaxBytes: max, Policy: policy}, nil
}

// infoBytes extracts a numeric byte field from the parsed INFO.
func infoBytes(fields map[string]string, key string) (int64, error) {
	v, ok := fields[key]
	if !ok {
		return 0, fmt.Errorf("monitor: INFO memory: the %s field is missing", key)
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("monitor: INFO memory: %s=%q is not a byte count", key, v)
	}
	return n, nil
}

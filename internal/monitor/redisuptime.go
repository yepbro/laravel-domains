// Redis uptime source for the redis_restart alert (T-270, the TZ-fsm
// spec §3 FR-2.2): uptime_in_seconds from INFO server of the same
// go-redis client that reads memory (redismem.go) and stream depths
// (cmd/monitor). A Redis restart without persistence (docs/redis.md §5 —
// a deliberate choice) loses ALL undelivered stream entries: lost
// q:dns entries self-heal by the T-029 lease pass (the lines return to
// queued), while q:http entries with an already written done hang until
// the T-270 compensator — a restart remains the main cause of the
// "status hung" loss class (the Oct 22 incident: a cohort of 1865
// domains, the T-256 gauge). The alert gives the operator a signal at
// the moment of the event, not a gauge cohort a day later.
//
// Parsing is manual, over "key:value" lines (the "# …" sections are
// comments), like parseRedisMemoryINFO: one field is needed, the format
// is fixed by the server.
package monitor

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// RedisUptime is a Redis uptime snapshot from INFO server.
type RedisUptime struct {
	// Seconds is uptime_in_seconds: seconds since the server started;
	// a restart resets it — "below the window" catches a recent restart
	// without relying on machine memory.
	Seconds int64
}

// RedisUptimeSource is a Redis uptime source; an interface for
// substitution in unit tests (the real read is INFO from a live Redis,
// which tests do not have). RedisMemorySource style.
type RedisUptimeSource interface {
	// RedisUptime returns an INFO server snapshot. An error means
	// "not measured", with the reason attached.
	RedisUptime(ctx context.Context) (RedisUptime, error)
}

// compile-time: RedisInfo is both a memory source and an uptime source
// (INFO of different sections of the same client).
var _ RedisUptimeSource = RedisInfo{}

// RedisUptime reads the server section of the INFO response and parses it.
func (s RedisInfo) RedisUptime(ctx context.Context) (RedisUptime, error) {
	if s.RDB == nil {
		return RedisUptime{}, fmt.Errorf("monitor: аптайм Redis: клиент не задан")
	}
	raw, err := s.RDB.Info(ctx, "server").Result()
	if err != nil {
		return RedisUptime{}, fmt.Errorf("monitor: INFO server: %w", err)
	}
	return parseRedisUptimeINFO(raw)
}

// parseRedisUptimeINFO parses the INFO response: "key:value" lines,
// section headers ("# Server") and empty lines are skipped; the field
// uptime_in_seconds is required. A missing field or a non-numeric/
// negative value is an error: the format is fixed by the server, a
// malformed response must not be silently ignored
// (parseRedisMemoryINFO style). Extracted as a pure function for a
// table test without Redis.
func parseRedisUptimeINFO(raw string) (RedisUptime, error) {
	fields := make(map[string]string)
	for _, line := range strings.Split(raw, "\n") {
		if k, v, ok := strings.Cut(strings.TrimRight(line, "\r"), ":"); ok {
			fields[k] = v
		}
	}
	secs, err := infoSeconds(fields, "uptime_in_seconds")
	if err != nil {
		return RedisUptime{}, err
	}
	return RedisUptime{Seconds: secs}, nil
}

// infoSeconds is a numeric seconds field from the parsed INFO
// (non-negative; its counterpart infoBytes handles bytes, redismem.go).
func infoSeconds(fields map[string]string, key string) (int64, error) {
	v, ok := fields[key]
	if !ok {
		return 0, fmt.Errorf("monitor: INFO server: поле %s отсутствует", key)
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("monitor: INFO server: %s=%q не число секунд", key, v)
	}
	return n, nil
}

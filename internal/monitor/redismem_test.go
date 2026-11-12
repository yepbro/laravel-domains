// Redis memory source unit tests (T-154): INFO memory parsing
// (a table: sections/CRLF, missing and malformed fields), RedisInfo
// against a fake go-redis client (a nil client, a command error, the
// requested section), the metrics wrapper (the ratio/bytes gauges,
// hold-last on an error, maxmemory 0 — the ratio is not written, transit
// without a registry). A live Redis is outside the unit tests
// (integration, T-032).
package monitor

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"

	"crawler/internal/metrics"
)

// fakeInfoRedis — INFO from a preset; the section protocol, an error queue.
type fakeInfoRedis struct {
	mu       sync.Mutex
	info     string
	err      error
	sections [][]string
}

// compile-time: the fake is interchangeable with the go-redis client.
var _ redisInfoCmd = (*fakeInfoRedis)(nil)

func (f *fakeInfoRedis) Info(_ context.Context, section ...string) *redis.StringCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sections = append(f.sections, section)
	return redis.NewStringResult(f.info, f.err)
}

// fakeRedisMem — a memory snapshot from a preset, an error queue, the protocol.
type fakeRedisMem struct {
	mu    sync.Mutex
	mem   RedisMemory
	errs  []error
	calls int
}

// compile-time: the fake is a RedisMemorySource.
var _ RedisMemorySource = (*fakeRedisMem)(nil)

func (f *fakeRedisMem) RedisMemory(context.Context) (RedisMemory, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if err := popErr(&f.errs); err != nil {
		return RedisMemory{}, err
	}
	return f.mem, nil
}

// --- INFO parsing -------------------------------------------------------------

// TestParseRedisMemoryINFO — the "key:value" lines, section headers
// and CRLF; a missing field, a non-numeric and a negative byte value, a
// missing policy are errors (the format is fixed by the server).
func TestParseRedisMemoryINFO(t *testing.T) {
	raw := "# Memory\r\n" +
		"used_memory:104857600\r\n" +
		"used_memory_human:100.00M\r\n" +
		"used_memory_rss:120000000\r\n" +
		"maxmemory:536870912\r\n" +
		"maxmemory_policy:noeviction\r\n" +
		"mem_fragmentation_ratio:1.15\r\n"
	m, err := parseRedisMemoryINFO(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := RedisMemory{UsedBytes: 104857600, MaxBytes: 536870912, Policy: "noeviction"}
	if m != want {
		t.Errorf("parse:\n получил  %+v\n хочу     %+v", m, want)
	}

	// Without \r (unix output) — the same result.
	m, err = parseRedisMemoryINFO(strings.ReplaceAll(raw, "\r", ""))
	if err != nil || m != want {
		t.Errorf("без CR: (%+v, %v), хочу (%+v, nil)", m, err, want)
	}

	// maxmemory 0 — a valid "no cap" value (not a parse error:
	// the share is undefined, but the absolutes and the policy are read).
	m, err = parseRedisMemoryINFO("# Memory\nused_memory:10\nmaxmemory:0\nmaxmemory_policy:noeviction\n")
	if err != nil {
		t.Fatalf("maxmemory 0: %v, want валидный снимок", err)
	}
	if m.MaxBytes != 0 || m.UsedBytes != 10 || m.Policy != "noeviction" {
		t.Errorf("maxmemory 0: %+v, want нулевой потолок с абсолютами", m)
	}

	for name, bad := range map[string]string{
		"нет used_memory":       "# Memory\nmaxmemory:1\nmaxmemory_policy:noeviction\n",
		"нет maxmemory":         "# Memory\nused_memory:1\nmaxmemory_policy:noeviction\n",
		"нет политики":          "# Memory\nused_memory:1\nmaxmemory:1\n",
		"used не число":         "# Memory\nused_memory:100M\nmaxmemory:1\nmaxmemory_policy:noeviction\n",
		"maxmemory отрицателен": "# Memory\nused_memory:1\nmaxmemory:-1\nmaxmemory_policy:noeviction\n",
		"пустой ответ":          "",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseRedisMemoryINFO(bad); err == nil {
				t.Fatalf("кривой INFO разобран: %q", bad)
			}
		})
	}
}

// --- source ------------------------------------------------------------------

// TestRedisInfoSource — a nil client is an error; the fake — parsing the
// answer; a command error is propagated; the request goes with the
// memory section.
func TestRedisInfoSource(t *testing.T) {
	if _, err := (RedisInfo{}).RedisMemory(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "клиент не задан") {
		t.Errorf("nil клиент: %v, want ошибка источника", err)
	}

	fake := &fakeInfoRedis{info: "# Memory\nused_memory:10\nmaxmemory:100\nmaxmemory_policy:noeviction\n"}
	m, err := (RedisInfo{RDB: fake}).RedisMemory(context.Background())
	if err != nil {
		t.Fatalf("RedisMemory: %v", err)
	}
	if m.UsedBytes != 10 || m.MaxBytes != 100 || m.Policy != "noeviction" {
		t.Errorf("снимок = %+v, want значения из INFO", m)
	}
	if len(fake.sections) != 1 || fake.sections[0][0] != "memory" {
		t.Errorf("секции запроса = %v, want [memory]", fake.sections)
	}

	fake.err = errors.New("connection refused")
	if _, err := (RedisInfo{RDB: fake}).RedisMemory(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "INFO memory") {
		t.Errorf("ошибка команды: %v, want обёрнутая ошибка INFO memory", err)
	}
}

// --- metrics wrapper -------------------------------------------------------------

// TestRedisMemoryMetrics — the wrapper mirrors the snapshot into the
// ratio/bytes gauges (the kind label), a source error keeps the last
// values, maxmemory 0 — the ratio is not written (undefined), bytes are
// written; a nil registry — pure transit.
func TestRedisMemoryMetrics(t *testing.T) {
	reg := metrics.New("monitor")
	src := &fakeRedisMem{mem: RedisMemory{UsedBytes: 4096, MaxBytes: 8192, Policy: "noeviction"}}
	m := NewRedisMemoryMetrics(reg, src)

	if _, err := m.RedisMemory(context.Background()); err != nil {
		t.Fatalf("RedisMemory: %v", err)
	}
	if v, ok := gaugeLabels(t, reg, "crawler_redis_memory_ratio", nil); !ok || v != 0.5 {
		t.Errorf("ratio = (%v, %v), want 0.5", v, ok)
	}
	for kind, want := range map[string]float64{"used": 4096, "max": 8192} {
		if v, ok := gaugeLabels(t, reg, "crawler_redis_memory_bytes", map[string]string{"kind": kind}); !ok || v != want {
			t.Errorf("bytes{kind=%s} = (%v, %v), want %v", kind, v, ok, want)
		}
	}

	// A source error — the error transit, the metrics keep the last values.
	src.errs = []error{errors.New("redis недоступен")}
	if _, err := m.RedisMemory(context.Background()); err == nil {
		t.Error("ошибка источника не прокинута")
	}
	if v, ok := gaugeLabels(t, reg, "crawler_redis_memory_ratio", nil); !ok || v != 0.5 {
		t.Errorf("после ошибки ratio = (%v, %v), want прежние 0.5", v, ok)
	}

	// New values overwrite the gauge.
	src.errs = nil
	src.mem = RedisMemory{UsedBytes: 6144, MaxBytes: 8192, Policy: "noeviction"}
	if _, err := m.RedisMemory(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v, _ := gaugeLabels(t, reg, "crawler_redis_memory_ratio", nil); v != 0.75 {
		t.Errorf("перезапись ratio = %v, want 0.75", v)
	}

	// maxmemory 0 (no cap) — the ratio is undefined and is not written,
	// the absolutes are honest: used updated, max — 0.
	src.mem = RedisMemory{UsedBytes: 2048, MaxBytes: 0, Policy: "noeviction"}
	if _, err := m.RedisMemory(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v, _ := gaugeLabels(t, reg, "crawler_redis_memory_ratio", nil); v != 0.75 {
		t.Errorf("ratio при maxmemory 0 = %v, want прежний (не определён, не пишется)", v)
	}
	if v, _ := gaugeLabels(t, reg, "crawler_redis_memory_bytes", map[string]string{"kind": "max"}); v != 0 {
		t.Errorf("bytes{max} = %v, want 0", v)
	}

	// A nil registry — transit without instruments.
	plain := NewRedisMemoryMetrics(nil, &fakeRedisMem{mem: RedisMemory{UsedBytes: 1, MaxBytes: 2, Policy: "noeviction"}})
	snap, err := plain.RedisMemory(context.Background())
	if err != nil || snap.UsedBytes != 1 || snap.MaxBytes != 2 {
		t.Errorf("транзит без реестра = (%+v, %v), want источник как есть", snap, err)
	}
}

// Юнит-тесты источника памяти Redis (T-154): разбор INFO memory
// (таблица: секции/CRLF, отсутствующие и кривые поля), RedisInfo против
// фейкового клиента go-redis (nil-клиент, ошибка команды, секция
// запроса), метрики-обёртка (gauge ratio/bytes, hold-last на ошибке,
// maxmemory 0 — ratio не пишется, транзит без реестра). Живой Redis —
// за границами юнит-тестов (интеграционные, T-032).
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

// fakeInfoRedis — INFO по заготовке; протокол секций, очередь ошибок.
type fakeInfoRedis struct {
	mu       sync.Mutex
	info     string
	err      error
	sections [][]string
}

// compile-time: фейк взаимозаменяем с клиентом go-redis.
var _ redisInfoCmd = (*fakeInfoRedis)(nil)

func (f *fakeInfoRedis) Info(_ context.Context, section ...string) *redis.StringCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sections = append(f.sections, section)
	return redis.NewStringResult(f.info, f.err)
}

// fakeRedisMem — снимок памяти по заготовке, очередь ошибок, протокол.
type fakeRedisMem struct {
	mu    sync.Mutex
	mem   RedisMemory
	errs  []error
	calls int
}

// compile-time: фейк — RedisMemorySource.
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

// --- разбор INFO -------------------------------------------------------------

// TestParseRedisMemoryINFO — строки «ключ:значение», заголовки секций
// и CRLF; отсутствующее поле, нечисловое и отрицательное значения байт,
// отсутствующая политика — ошибки (формат фиксирован сервером).
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

	// Без \r (юникс-вывод) — тот же результат.
	m, err = parseRedisMemoryINFO(strings.ReplaceAll(raw, "\r", ""))
	if err != nil || m != want {
		t.Errorf("без CR: (%+v, %v), хочу (%+v, nil)", m, err, want)
	}

	// maxmemory 0 — валидное значение «потолка нет» (не ошибка парса:
	// доля не определена, но абсолюты и политика читаются).
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

// --- источник ------------------------------------------------------------------

// TestRedisInfoSource — nil-клиент ошибка; фейк — разбор ответа; ошибка
// команды прокидывается; запрос идёт секцией memory.
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

// --- метрики-обёртка -------------------------------------------------------------

// TestRedisMemoryMetrics — обёртка зеркалит снимок в gauge ratio/bytes
// (метка kind), ошибка источника держит последние значения, maxmemory 0
// — ratio не пишется (не определён), bytes пишутся; nil-реестр —
// чистый транзит.
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

	// Ошибка источника — транзит ошибки, метрики держат последнее.
	src.errs = []error{errors.New("redis недоступен")}
	if _, err := m.RedisMemory(context.Background()); err == nil {
		t.Error("ошибка источника не прокинута")
	}
	if v, ok := gaugeLabels(t, reg, "crawler_redis_memory_ratio", nil); !ok || v != 0.5 {
		t.Errorf("после ошибки ratio = (%v, %v), want прежние 0.5", v, ok)
	}

	// Новые значения перезаписывают gauge.
	src.errs = nil
	src.mem = RedisMemory{UsedBytes: 6144, MaxBytes: 8192, Policy: "noeviction"}
	if _, err := m.RedisMemory(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v, _ := gaugeLabels(t, reg, "crawler_redis_memory_ratio", nil); v != 0.75 {
		t.Errorf("перезапись ratio = %v, want 0.75", v)
	}

	// maxmemory 0 (без потолка) — ratio не определён и не пишется,
	// абсолюты честны: used обновлён, max — 0.
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

	// nil-реестр — транзит без приборов.
	plain := NewRedisMemoryMetrics(nil, &fakeRedisMem{mem: RedisMemory{UsedBytes: 1, MaxBytes: 2, Policy: "noeviction"}})
	snap, err := plain.RedisMemory(context.Background())
	if err != nil || snap.UsedBytes != 1 || snap.MaxBytes != 2 {
		t.Errorf("транзит без реестра = (%+v, %v), want источник как есть", snap, err)
	}
}

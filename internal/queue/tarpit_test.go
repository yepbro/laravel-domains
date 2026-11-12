// Tests of the counter of tarpit-classified deliveries (T-261, FR-7)
// without a live Redis: the fake emulates the set semantics (SADD is
// idempotent per member, SCARD — the cardinality) and the TTL, covered
// are the command arguments, the key format, the validation and the
// degradation on command errors. The live semantics of the
// render:tarpit:* key (SADD/EXPIRE/SCARD against a real Redis) —
// test/integration/render_tarpit_test.go.

package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// fakeTarpitRedis — an emulation of Redis sets for the tarpit counter:
// SAdd adds members (a repeated one — a no-op), Expire remembers the
// TTL, SCard answers with the cardinality. Command errors — stubs.
type fakeTarpitRedis struct {
	sets map[string]map[string]struct{}
	ttls map[string]time.Duration

	saddErr   error
	expireErr error
	scardErr  error

	sadds  []string // the recorded SADD members in call order
	expiry []time.Duration
}

func newFakeTarpitRedis() *fakeTarpitRedis {
	return &fakeTarpitRedis{
		sets: make(map[string]map[string]struct{}),
		ttls: make(map[string]time.Duration),
	}
}

func (f *fakeTarpitRedis) SAdd(_ context.Context, key string, members ...interface{}) *redis.IntCmd {
	cmd := redis.NewIntCmd(context.Background(), "sadd")
	var added int64
	if f.sets[key] == nil {
		f.sets[key] = make(map[string]struct{})
	}
	for _, m := range members {
		id, _ := m.(string)
		if _, ok := f.sets[key][id]; !ok {
			f.sets[key][id] = struct{}{}
			added++
		}
		f.sadds = append(f.sadds, id)
	}
	if f.saddErr != nil {
		cmd.SetErr(f.saddErr)
	} else {
		cmd.SetVal(added)
	}
	return cmd
}

func (f *fakeTarpitRedis) Expire(_ context.Context, key string, ttl time.Duration) *redis.BoolCmd {
	cmd := redis.NewBoolCmd(context.Background(), "expire")
	if f.expireErr != nil {
		cmd.SetErr(f.expireErr)
	} else {
		f.ttls[key] = ttl
		f.expiry = append(f.expiry, ttl)
		cmd.SetVal(true)
	}
	return cmd
}

func (f *fakeTarpitRedis) SCard(_ context.Context, key string) *redis.IntCmd {
	cmd := redis.NewIntCmd(context.Background(), "scard")
	if f.scardErr != nil {
		cmd.SetErr(f.scardErr)
	} else {
		cmd.SetVal(int64(len(f.sets[key])))
	}
	return cmd
}

// A compile-time check: the fake is interchangeable with the real
// client.
var _ tarpitClient = (*fakeTarpitRedis)(nil)

// TestTarpitKey — the format of the application counter key:
// render:tarpit:{task_id} (the contract of the docs/redis.md §2
// documentation and of manual redis-cli triage).
func TestTarpitKey(t *testing.T) {
	if got, want := TarpitKey("abc123"), "render:tarpit:abc123"; got != want {
		t.Errorf("TarpitKey = %q, хочу %q", got, want)
	}
}

// TestTarpitCounterTrack — a delivery mark returns the set cardinality
// and refreshes the key's TTL: the first delivery — 1, another delivery
// of the same task — 2, a mark of another task does not mix in.
func TestTarpitCounterTrack(t *testing.T) {
	f := newFakeTarpitRedis()
	c := NewTarpitCounter(f)
	ctx := context.Background()
	ttl := 48 * time.Hour

	if n, err := c.Track(ctx, "t1", "e1#1", ttl); err != nil || n != 1 {
		t.Errorf("Track(t1, e1#1) = (%d, %v), хочу (1, nil)", n, err)
	}
	if n, err := c.Track(ctx, "t1", "e1#2", ttl); err != nil || n != 2 {
		t.Errorf("Track(t1, e1#2) = (%d, %v), хочу (2, nil)", n, err)
	}
	if n, err := c.Track(ctx, "t2", "e9#1", ttl); err != nil || n != 1 {
		t.Errorf("Track(t2, e9#1) = (%d, %v), хочу (1, nil) — задачи не смешиваются", n, err)
	}
	key := TarpitKey("t1")
	if len(f.ttls) != 2 {
		t.Errorf("TTL поставлены %d ключам, хочу 2", len(f.ttls))
	}
	if got := f.ttls[key]; got != ttl {
		t.Errorf("TTL %s = %s, хочу %s", key, got, ttl)
	}
	if len(f.expiry) != 3 {
		t.Errorf("EXPIRE вызван %d раз, хочу 3 (освежается каждой отметкой)", len(f.expiry))
	}
}

// TestTarpitCounterTrackIdempotent — the SADD semantics: marking THE SAME
// delivery again (a delivery-number replay after an attempt refund,
// T-222) does not grow the cardinality — one delivery, one set member.
func TestTarpitCounterTrackIdempotent(t *testing.T) {
	f := newFakeTarpitRedis()
	c := NewTarpitCounter(f)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if n, err := c.Track(ctx, "t1", "e1#2", time.Hour); err != nil || n != 1 {
			t.Errorf("Track №%d той же доставки = (%d, %v), хочу (1, nil)", i+1, n, err)
		}
	}
	if got := f.sadds; len(got) != 3 {
		t.Errorf("SADD вызван %d раз, хочу 3", len(got))
	}
}

// TestTarpitCounterTrackValidation — an empty task_id/delivery and a
// non-positive TTL — an error before the Redis commands (an EXPIRE with
// a TTL ≤ 0 would delete the key).
func TestTarpitCounterTrackValidation(t *testing.T) {
	f := newFakeTarpitRedis()
	c := NewTarpitCounter(f)
	ctx := context.Background()
	cases := []struct {
		name     string
		taskID   string
		delivery string
		ttl      time.Duration
	}{
		{"пустой task_id", "", "e1#1", time.Hour},
		{"пустая доставка", "t1", "", time.Hour},
		{"нулевой TTL", "t1", "e1#1", 0},
		{"отрицательный TTL", "t1", "e1#1", -time.Second},
	}
	for _, cs := range cases {
		t.Run(cs.name, func(t *testing.T) {
			if _, err := c.Track(ctx, cs.taskID, cs.delivery, cs.ttl); err == nil {
				t.Fatal("ожидаюсь ошибка валидации")
			}
		})
	}
	if len(f.sadds) != 0 || len(f.expiry) != 0 {
		t.Errorf("команды Redis выполнялись (%d SADD, %d EXPIRE) — валидация обязана отсекать до них",
			len(f.sadds), len(f.expiry))
	}
}

// TestTarpitCounterTrackErrors — a failure of any command
// (SADD/EXPIRE/SCARD) — an error of the call: the consumer degrades to
// the decision by the PEL delivery number, the partially applied state
// does not distort the cardinality in the answer.
func TestTarpitCounterTrackErrors(t *testing.T) {
	cases := []struct {
		name   string
		inject func(f *fakeTarpitRedis)
	}{
		{"SADD", func(f *fakeTarpitRedis) { f.saddErr = errors.New("redis: connection closed") }},
		{"EXPIRE", func(f *fakeTarpitRedis) { f.expireErr = errors.New("redis: OOM command not allowed") }},
		{"SCARD", func(f *fakeTarpitRedis) { f.scardErr = errors.New("redis: loading") }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFakeTarpitRedis()
			c.inject(f)
			if _, err := NewTarpitCounter(f).Track(context.Background(), "t1", "e1#1", time.Hour); err == nil {
				t.Fatal("сбой команды обязан возвращаться ошибкой")
			}
		})
	}
}

// Тесты счётчика тарпит-классифицированных доставок (T-261, ФТ-7) без
// живого Redis: фейк эмулирует семантику множества (SADD идемпотентен
// по члену, SCARD — мощность) и TTL, проверяются аргументы команд,
// формат ключа, валидация и деградация по ошибкам команд. Живая
// семантика ключа render:tarpit:* (SADD/EXPIRE/SCARD против настоящего
// Redis) — test/integration/render_tarpit_test.go.

package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// fakeTarpitRedis — эмуляция множеств Redis для счётчика тарпитов:
// SAdd добавляет членов (повторный — no-op), Expire запоминает TTL,
// SCard отвечает мощностью. Ошибки команд — заготовки.
type fakeTarpitRedis struct {
	sets map[string]map[string]struct{}
	ttls map[string]time.Duration

	saddErr   error
	expireErr error
	scardErr  error

	sadds  []string // зафиксированные члены SADD в порядке вызовов
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

// Компиляционная проверка: фейк взаимозаменяем с реальным клиентом.
var _ tarpitClient = (*fakeTarpitRedis)(nil)

// TestTarpitKey — формат ключа прикладного счётчика: render:tarpit:
// {task_id} (контракт документации docs/redis.md §2 и ручного разбора
// redis-cli).
func TestTarpitKey(t *testing.T) {
	if got, want := TarpitKey("abc123"), "render:tarpit:abc123"; got != want {
		t.Errorf("TarpitKey = %q, хочу %q", got, want)
	}
}

// TestTarpitCounterTrack — отметка доставки возвращает мощность множества
// и освежает TTL ключа: первая доставка — 1, другая доставка той же
// задачи — 2, отметка другой задачи не смешивается.
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

// TestTarpitCounterTrackIdempotent — семантика SADD: повторная отметка
// ТОЙ ЖЕ доставки (replay номера после возврата попытки, T-222) мощность
// не растит — одна доставка один член множества.
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

// TestTarpitCounterTrackValidation — пустой task_id/доставка и неположительный
// TTL — ошибка до команд Redis (EXPIRE с TTL ≤ 0 удалил бы ключ).
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

// TestTarpitCounterTrackErrors — сбой любой команды (SADD/EXPIRE/SCARD)
// — ошибка вызова: потребитель деградирует к решению по номеру доставки
// PEL, частично применённое состояние не искажает мощность в ответе.
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

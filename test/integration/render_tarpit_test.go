// Интеграционный тест тарпит-счётчика рендера (T-261, ТЗ-fsm §3 ФТ-7)
// против живого Redis dev-окружения: прикладной ключ render:tarpit:
// {task_id} — SADD идемпотентен по id доставки (replay номера доставки
// после возврата попытки T-222 мощность не растит), SCARD отвечает
// числом тарпит-классифицированных доставок, EXPIRE держит TTL
// (48 ч, освежается каждой отметкой) — счётчик переживает рестарты
// воркеров. Решение воркера по мощности множества (tarpitsExhausted)
// покрыто юнит-тестами (internal/renderworker/tarpitcount_test.go) —
// здесь проверяется сама семантика ключа. Изоляция — Redis БД 15
// (resilienceRedisEnv), созданные ключи убирает cleanup.

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
	// Первая тарпит-доставка: мощность 1.
	if n, err := c.Track(ctx, taskID, "1719-1#1", ttl); err != nil || n != 1 {
		t.Fatalf("Track №1 = (%d, %v), хочу (1, nil)", n, err)
	}
	// Replay той же доставки (тот же entry-id × номер): SADD идемпотентен.
	if n, err := c.Track(ctx, taskID, "1719-1#1", ttl); err != nil || n != 1 {
		t.Fatalf("Track дубля = (%d, %v), хочу (1, nil) — одна доставка один член", n, err)
	}
	// Новая доставка той же задачи: мощность 2 — порог
	// render.timeout_attempts (дефолт 2) достигается.
	if n, err := c.Track(ctx, taskID, "1719-1#2", ttl); err != nil || n != 2 {
		t.Fatalf("Track №2 = (%d, %v), хочу (2, nil)", n, err)
	}
	// Другая задача — другое множество, мощности не смешиваются.
	other := queue.NewTaskID()
	t.Cleanup(func() { _ = rdb.Del(context.Background(), queue.TarpitKey(other)).Err() })
	if n, err := c.Track(ctx, other, "9999-9#1", ttl); err != nil || n != 1 {
		t.Fatalf("Track чужой задачи = (%d, %v), хочу (1, nil)", n, err)
	}

	// TTL ключа жив и в границах горизонта: отметка освежает EXPIRE,
	// ключ не вечный и не исчезнет до конца эпизода.
	got := rdb.TTL(ctx, key).Val()
	if got <= 0 || got > ttl {
		t.Errorf("TTL %s = %s, хочу (0, %s]", key, got, ttl)
	}
	// Тип ключа — множество (ручной разбор redis-cli: SMEMBERS).
	if kind, err := rdb.Type(ctx, key).Result(); err != nil || kind != "set" {
		t.Errorf("TYPE %s = %q (%v), хочу set", key, kind, err)
	}

	// Валидация до команд: недопустимые аргументы не создают ключей.
	if _, err := c.Track(ctx, "", "e#1", ttl); err == nil {
		t.Error("пустой task_id — ошибка")
	}
	if _, err := c.Track(ctx, queue.NewTaskID(), "e#1", 0); err == nil {
		t.Error("TTL 0 — ошибка (EXPIRE удалил бы ключ)")
	}
}

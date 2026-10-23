// Источник памяти Redis для алерта redis_memory (T-154, ТЗ 5.9/3.2):
// used_memory/maxmemory из INFO memory того же клиента go-redis, что
// читает глубины стримов (cmd/monitor). При политике noeviction (бюджет
// Redis ≤ 0,5 ГБ, ТЗ 3.2 — docs/redis.md) заполнение maxmemory —
// отказ записи XADD («OOM command not allowed», инциденты T-093 и 22.10:
// docs/load-measurements.md §11.1): прямого счётчика отказов INFO не
// отдаёт, доля памяти — прокси-метрика предстоящих отказов.
//
// Разбор — вручную по строкам «ключ:значение» (секции «# …» — комментарии):
// нужны три поля, формат фиксирован сервером; полноценный парсер INFO
// не окупает зависимость (стиль parseBadHashesExposition).
package monitor

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// RedisMemory — снимок памяти Redis из INFO memory.
type RedisMemory struct {
	// UsedBytes — used_memory: байты набора данных целиком (вместе со
	// служебными структурами сервера — именно эта величина ограничена
	// maxmemory, а не «полезные» данные стримов).
	UsedBytes int64
	// MaxBytes — maxmemory: потолок в байтах; 0 — потолка нет
	// (алерт по доле тогда не применим).
	MaxBytes int64
	// Policy — maxmemory_policy (боевая — noeviction, ТЗ 3.2).
	Policy string
}

// RedisMemorySource — источник памяти Redis; интерфейс для подмены в
// юнит-тестах (реальное чтение — INFO живого Redis, в тестах его нет).
// Стиль WearSource/UnitEventsSource.
type RedisMemorySource interface {
	// RedisMemory возвращает снимок INFO memory. Ошибка = «не измеряется»
	// с причиной.
	RedisMemory(ctx context.Context) (RedisMemory, error)
}

// redisInfoCmd — окно в go-redis: только INFO (узкое, как claimStreams/
// redisDepth в internal/queue); *redis.Client (queue.NewClient)
// и *redis.ClusterClient удовлетворяют интерфейсу.
type redisInfoCmd interface {
	Info(ctx context.Context, section ...string) *redis.StringCmd
}

// RedisInfo — RedisMemorySource поверх INFO memory.
type RedisInfo struct {
	// RDB — клиент Redis (тот же, что у гейта глубин, cmd/monitor);
	// nil — ошибка источника.
	RDB redisInfoCmd
}

// compile-time: RedisInfo — RedisMemorySource.
var _ RedisMemorySource = RedisInfo{}

// RedisMemory читает секцию memory ответа INFO и разбирает её.
func (s RedisInfo) RedisMemory(ctx context.Context) (RedisMemory, error) {
	if s.RDB == nil {
		return RedisMemory{}, fmt.Errorf("monitor: память Redis: клиент не задан")
	}
	raw, err := s.RDB.Info(ctx, "memory").Result()
	if err != nil {
		return RedisMemory{}, fmt.Errorf("monitor: INFO memory: %w", err)
	}
	return parseRedisMemoryINFO(raw)
}

// parseRedisMemoryINFO разбирает ответ INFO: строки «ключ:значение»,
// заголовки секций («# Memory») и пустые строки пропускаются; нужны поля
// used_memory, maxmemory, maxmemory_policy. Отсутствие поля или
// нечисловое/отрицательное значение байт — ошибка: формат фиксирован
// сервером, кривой ответ — не молчать (стиль parseMemoryEvents).
// Выделен чистой функцией для табличного теста без Redis.
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
		return RedisMemory{}, fmt.Errorf("monitor: INFO memory: поле maxmemory_policy отсутствует")
	}
	return RedisMemory{UsedBytes: used, MaxBytes: max, Policy: policy}, nil
}

// infoBytes достаёт числовое поле байт из разобранного INFO.
func infoBytes(fields map[string]string, key string) (int64, error) {
	v, ok := fields[key]
	if !ok {
		return 0, fmt.Errorf("monitor: INFO memory: поле %s отсутствует", key)
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("monitor: INFO memory: %s=%q не число байт", key, v)
	}
	return n, nil
}

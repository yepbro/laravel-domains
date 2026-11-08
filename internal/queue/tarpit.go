// Счётчик тарпит-классифицированных доставок задач рендера (T-261,
// ТЗ tasks/TZ-fsm.md §3 ФТ-7): прикладной ключ Redis render:tarpit:{task_id}
// — множество идентификаторов доставок, завершившихся таймаутом страницы
// (render.timeout, tarpitError воркера). Решение тарпит-финализации
// принимается по мощности множества (SCARD ≥ render.timeout_attempts),
// а не по общему номеру доставки PEL: смешанные серии (тарпит чередуется
// с инфраструктурными сбоями) перестают финализировать «на попытку»
// раньше — номер доставки растёт от каждой доставки, множество — только
// от тарпит-классифицированных. Потребитель — internal/renderworker
// (шов TarpitStore, воркер монтирует его через Options.Tarpits;
// спецификация автомата политики исходов — doc-комментарий пакета
// internal/renderworker); здесь — только механизм ключа.

package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// TarpitKey — ключ множества тарпит-классифицированных доставок задачи
// taskID: render:tarpit:{task_id}. Идентификатор задачи (message.go)
// переживает запись стрима и переносится публикацией фолбэка (T-238
// передаёт сообщение целиком) — счётчик один на все доставки задачи:
// основной стрим q:render и доработка воркером B в q:render-fallback
// (бюджет тарпитов — на эпизод задачи, не на стрим; члены множества —
// id доставок — различают записи стримов). Переиздания эпизода (проход
// доработки T-066, поздний переиздатель T-259) публикуют НОВЫЙ task_id —
// их счётчик начинается с нуля.
func TarpitKey(taskID string) string {
	return "render:tarpit:" + taskID
}

// tarpitClient — окно go-redis для счётчика тарпитов: SADD/EXPIRE/SCard.
// Отдельный интерфейс по образцу claimStreams (reliab.go): *redis.Client
// (queue.NewClient), *redis.ClusterClient и фейки юнит-тестов
// удовлетворяют ему без раскрытия всего Cmdable.
type tarpitClient interface {
	SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	SCard(ctx context.Context, key string) *redis.IntCmd
}

// TarpitCounter — счётчик тарпит-классифицированных доставок задач
// рендера (T-261, ФТ-7) поверх прикладного ключа render:tarpit:{task_id}
// (TarpitKey). Множество ведёт сам Redis (SADD идемпотентен по члену),
// TTL освежается каждой отметкой — ключ живёт ttl после ПОСЛЕДНЕЙ
// тарпит-доставки задачи: эпизод не может закончиться раньше TTL
// (горизонт ретраев — часы), а забытые ключи убирает Redis, не оператор.
// Потеря ключа/Redis не портит решение потребителя: Track возвращает
// ошибку или меньшую мощность — воркер деградирует к номеру доставки
// PEL (поведение до T-261), раньше порога не финализирует (ТЗ 3.4 —
// потеря Redis не фатальна, источник истины — PostgreSQL).
type TarpitCounter struct {
	rdb tarpitClient
}

// NewTarpitCounter строит счётчик поверх клиента rdb — того же, что и
// очередь воркера (NewClient/NewReliab): команды точечные, пул общий.
func NewTarpitCounter(rdb tarpitClient) *TarpitCounter {
	return &TarpitCounter{rdb: rdb}
}

// Track помечает доставку delivery задачи taskID тарпит-классифицированной
// и возвращает мощность множества задачи — сколько доставок (включая
// текущую) классифицировано тарпитом. Идемпотентность — семантика SADD:
// повторная отметка той же доставки (replay номера после возврата
// попытки, T-222; повторная обработка зависшей) счётчик не растит.
// Идентификатор доставки строит потребитель (запись стрима × номер
// доставки PEL) — здесь это непрозрачная строка.
//
// Ошибка любой команды — ошибка всего вызова: потребитель деградирует к
// решению по номеру доставки PEL. Часть команд могла примениться
// (SADD без EXPIRE при обрыве между ними) — ключ без TTL переживёт
// очистку; следующая отметка той же задачи поставит TTL заново
// (EXPIRE освежается каждой отметкой), осиротевший ключ одиночной
// доставки — единицы инцидентов обрыва соединения, ограничение сверху —
// бюджет прикладных ключей (docs/redis.md §2).
func (c *TarpitCounter) Track(ctx context.Context, taskID, delivery string, ttl time.Duration) (int, error) {
	if taskID == "" {
		return 0, fmt.Errorf("queue: тарпит-счётчик: пустой task_id")
	}
	if delivery == "" {
		return 0, fmt.Errorf("queue: тарпит-счётчик %s: пустой идентификатор доставки", TarpitKey(taskID))
	}
	if ttl <= 0 {
		return 0, fmt.Errorf("queue: тарпит-счётчик %s: TTL %s ≤ 0 (EXPIRE удалил бы ключ)", TarpitKey(taskID), ttl)
	}
	key := TarpitKey(taskID)
	if _, err := c.rdb.SAdd(ctx, key, delivery).Result(); err != nil {
		return 0, fmt.Errorf("queue: тарпит-счётчик %s: отметка доставки %s: %w", key, delivery, err)
	}
	if _, err := c.rdb.Expire(ctx, key, ttl).Result(); err != nil {
		return 0, fmt.Errorf("queue: тарпит-счётчик %s: TTL %s: %w", key, ttl, err)
	}
	n, err := c.rdb.SCard(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("queue: тарпит-счётчик %s: мощность множества: %w", key, err)
	}
	return int(n), nil
}

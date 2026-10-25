// Источник состояния групп потребителей рабочих стримов для алерта
// замороженной очереди (T-162; сигнатура отравленной задачи T-159:
// XREADGROUP не отдаёт записи — last-delivered-id группы стоит, а глубина
// (backlog) ненулевая; замер §12 — все P0 находили вручную именно по
// таким «тихим» простоям). Глубина группы — queue.GroupDepth: общий
// расчёт с гейтом backpressure (min(lag, XLEN) + pending, кламп
// фантомного лага после обрезки недоставленного — T-168) — источник
// обязан соглашаться с алертом переполнения очередей о том, есть ли
// работа.
//
// Данные — XINFO GROUPS по каждому рабочему стриму (имя группы, lag,
// pending, last-delivered-id) и XLEN для клампа: то же окно в go-redis,
// что у redisDepth в internal/queue, но своим интерфейсом — пакет queue
// не тянет проверок алертов в API гейта (гейт — механизм продюсера,
// монитор — наблюдатель, ТЗ 5.9); сама формула глубины — общая
// (queue.GroupDepth), чтобы наблюдение и гейт не разошлись.
package monitor

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"

	"crawler/internal/queue"
)

// StreamGroupState — снимок одной группы потребления рабочего стрима.
type StreamGroupState struct {
	// Stream — имя стрима (q:http).
	Stream string
	// Group — имя группы (http-worker).
	Group string
	// Depth — глубина очереди группы: непрочитанный остаток
	// (min(lag, XLEN) — кламп T-168) плюс неподтверждённые доставки
	// (pending). Формула — queue.GroupDepth (общая с BackpressureGate.Len).
	Depth int64
	// LastDeliveredID — last-delivered-id группы из XINFO GROUPS: ID
	// последней записи, доставленной потребителям (XREADGROUP ">").
	// Движется только новыми доставками: стоящий ID при Depth > 0 —
	// очередь не читается. "0-0" — доставок не было вовсе.
	LastDeliveredID string
}

// StreamGroupsSource — источник состояния групп рабочих стримов;
// интерфейс для подмены в юнит-тестах (реальное чтение — XINFO/XLEN живого
// Redis, в тестах его нет). Стиль StreamLener/RedisMemorySource.
type StreamGroupsSource interface {
	// StreamGroups возвращает снимки всех групп рабочих стримов.
	// Стримы без групп (не существует или потребители не создавались)
	// в список не входят: замерять движение доставок не по кому.
	// Ошибка = «не измеряется» с причиной.
	StreamGroups(ctx context.Context) ([]StreamGroupState, error)
}

// redisGroupsCmd — окно в go-redis для групп стримов: XINFO GROUPS и
// XLEN (узкое, как redisDepth в internal/queue); *redis.Client
// (queue.NewClient) и *redis.ClusterClient удовлетворяют интерфейсу.
type redisGroupsCmd interface {
	XLen(ctx context.Context, key string) *redis.IntCmd
	XInfoGroups(ctx context.Context, key string) *redis.XInfoGroupsCmd
}

// RedisStreamGroups — StreamGroupsSource поверх XINFO GROUPS + XLEN того
// же клиента go-redis, что читает глубины стримов (cmd/monitor).
type RedisStreamGroups struct {
	// RDB — клиент Redis; nil — ошибка источника.
	RDB redisGroupsCmd
}

// compile-time: RedisStreamGroups — StreamGroupsSource.
var _ StreamGroupsSource = RedisStreamGroups{}

// StreamGroups обходит рабочие стримы (alertStreams): стрим без групп
// (нет ключа — Redis отвечает ошибкой, или группы не создавались) —
// пустой вклад, глубина такого стрима — территория алерта переполнения
// (Len читает XLEN). Ошибка чтения любого стрима останавливает проход
// целиком: непрочитанные состояния не подменяются пустотой, иначе сбой
// ложно «оживлял» бы замороженную группу (сброс трекинга в evalQueueFreeze).
func (s RedisStreamGroups) StreamGroups(ctx context.Context) ([]StreamGroupState, error) {
	if s.RDB == nil {
		return nil, fmt.Errorf("monitor: группы стримов: клиент Redis не задан")
	}
	var out []StreamGroupState
	for _, stream := range alertStreams {
		groups, err := s.RDB.XInfoGroups(ctx, stream).Result()
		if err != nil {
			// Стрима ещё нет — Redis отвечает ошибкой no such key (не пустым
			// списком): групп нет, стрим не заморожен (та же ветка, что у
			// BackpressureGate.Len).
			if errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "no such key") {
				continue
			}
			return nil, fmt.Errorf("monitor: группы %s: %w", stream, err)
		}
		if len(groups) == 0 {
			continue
		}
		// XLEN — верхняя грань непрочитанного остатка каждой группы
		// (кламп T-168): недоставленные записи живут в стриме.
		xlen, err := s.RDB.XLen(ctx, stream).Result()
		if err != nil {
			return nil, fmt.Errorf("monitor: группы %s (XLEN): %w", stream, err)
		}
		for _, g := range groups {
			out = append(out, StreamGroupState{
				Stream: stream,
				Group:  g.Name,
				// −1 — lag неопределён (обрезка недоставленного, Redis < 7.0):
				// GroupDepth учитывает только точный pending.
				Depth:           queue.GroupDepth(g, xlen),
				LastDeliveredID: g.LastDeliveredID,
			})
		}
	}
	return out, nil
}

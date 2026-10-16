// Backpressure по длине очереди (ТЗ 4.2: «планировщик публикует новые
// задачи только когда длина очереди ниже порога (backpressure)», §5.3:
// «публикует новые задачи только если длина очереди ниже порога»).
//
// Механизм: перед публикацией продюсер проверяет длину стрима (XLEN) и
// публикует, только пока она ниже порога scheduler.queue_high_watermark
// (конфиг; значение передаётся в BackpressureOptions). Гейт закрыт —
// пауза Interval и повторная проверка; отмена ctx — чистый выход с
// ошибкой контекста (graceful shutdown, ТЗ 3.4).
//
// К чему применяется порог — один на все рабочие стримы:
//   - q:dns — планировщик (T-028): единица стрима — пачка доменов
//     (ТЗ 4.2), поэтому проверка перед каждым сообщением и есть
//     «проверка перед публикацией пачки»;
//   - q:http, q:render, q:analyze — воркеры: одна пачка q:dns порождает
//     до scheduler.batch_size задач q:http и по задаче q:analyze на
//     субдомен, без гейта эти стримы растут кратно быстрее q:dns и
//     угрожают бюджету памяти Redis (≤ 0,5 ГБ, ТЗ 3.2);
//   - q:dead не ограничивается: перенос исчерпавших попытки задач
//     (T-025) — часть механизма надёжности и блокироваться
//     переполнением не должен.
//
// Порог живёт в секции scheduler по месту из ТЗ 5.10 (состав секций
// фиксирован, переименование ломало бы конфиги); семантика зафиксирована
// в docs/configuration.md.
//
// Длина стрима — XLEN: все записи, включая уже доставленные потребителям
// и не подтверждённые XACK (после XACK записи не исчезают — стрим чистит
// только обрезка MAXLEN ~, T-025). Мера консервативна: гейт реагирует и
// на реальный backlog, и на темп обрезки. Сам гейт — мягкое регулирование
// (между проверкой и публикацией другие продюсеры могут добавить записи);
// жёсткий потолок длины — MAXLEN ~ того же T-025.

package queue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultBackpressureInterval — пауза между повторными проверками длины
// стрима, пока гейт закрыт (BackpressureOptions.Interval). Секунда:
// этапы пропускают тысячи задач в секунду (ТЗ 3.3), более длинное окно
// зря держит продюсера, более короткое — лишние обращения XLEN.
const DefaultBackpressureInterval = time.Second

// redisLen — окно в go-redis для длины стрима (XLEN); узкое, как
// redisStreams (redisq.go): юнит-тесты подменяют фейком, реальные
// *redis.Client и *redis.ClusterClient удовлетворяют интерфейсу.
type redisLen interface {
	XLen(ctx context.Context, key string) *redis.IntCmd
}

// BackpressureOptions — параметры гейта; неположительные Interval и
// nil Log заменяются дефолтами в конструкторе (стиль RedisOptions).
type BackpressureOptions struct {
	// Watermark — порог длины стрима (scheduler.queue_high_watermark):
	// публикация разрешена при len < Watermark, len == Watermark — гейт
	// закрыт («строго ниже порога», ТЗ 4.2/5.3). Значение ≤ 0 отключает
	// гейт (публикация без ожидания) — как MaxLen = 0 в RedisOptions.
	Watermark int64
	// Interval — пауза между повторными XLEN при закрытом гейте;
	// 0 — DefaultBackpressureInterval.
	Interval time.Duration
	// Log — логгер гейта: одна запись Warn при входе в ожидание
	// (переполнение очередей видно в логах рядом с метриками ТЗ 5.9);
	// nil — записи подавлены (slog.DiscardHandler, стиль RecheckOptions).
	Log *slog.Logger
}

// BackpressureGate — гейт ёмкости стрима: ждёт, пока длина стрима
// опустится ниже порога (backpressure, ТЗ 4.2/5.3). Применяется сам по
// себе (планировщику T-028 выгодно проверить ёмкость до выборки пачки
// из БД: полон стрим — пачка не выбирается) и внутри WatermarkPublisher.
type BackpressureGate struct {
	rdb redisLen
	opt BackpressureOptions
}

// NewBackpressureGate строит гейт поверх клиента go-redis — того же,
// что у RedisQueue (одна точка правды о длинах стримов).
func NewBackpressureGate(rdb redisLen, opt BackpressureOptions) *BackpressureGate {
	if opt.Interval <= 0 {
		opt.Interval = DefaultBackpressureInterval
	}
	if opt.Log == nil {
		opt.Log = slog.New(slog.DiscardHandler)
	}
	return &BackpressureGate{rdb: rdb, opt: opt}
}

// Len возвращает длину стрима (XLEN) — число всех записей, включая
// доставленные и не подтверждённые XACK (см. шапку файла).
func (g *BackpressureGate) Len(ctx context.Context, stream string) (int64, error) {
	if !knownStream(stream) {
		return 0, fmt.Errorf("queue: неизвестный стрим %q (рабочие стримы — константы пакета: StreamDNS, StreamHTTP, StreamRender, StreamAnalyze, StreamDead)", stream)
	}
	if g.rdb == nil {
		return 0, fmt.Errorf("queue: длина %s: клиент Redis не задан", stream)
	}
	n, err := g.rdb.XLen(ctx, stream).Result()
	if err != nil {
		return 0, fmt.Errorf("queue: длина %s: %w", stream, err)
	}
	return n, nil
}

// AwaitCapacity ждёт, пока длина стрима станет ниже порога, и возвращает
// nil — публиковать можно. Стрим не ниже порога — пауза Interval и
// повтор; отмена ctx — ошибка контекста (чистый выход, ТЗ 3.4). Ошибка
// XLEN возвращается сразу: гейт не гадает о длине (недоступный Redis —
// не «стрим пуст»), решение о повторе принимает вызывающий цикл.
//
// Отключённый гейт (Watermark ≤ 0) и q:dead проходят без обращения
// к Redis (см. шапку файла).
func (g *BackpressureGate) AwaitCapacity(ctx context.Context, stream string) error {
	if g.opt.Watermark <= 0 || stream == StreamDead {
		return nil
	}
	waited := false
	for {
		n, err := g.Len(ctx, stream)
		if err != nil {
			return err
		}
		if n < g.opt.Watermark {
			return nil
		}
		if !waited {
			waited = true
			g.opt.Log.Warn("публикация приостановлена: длина стрима у порога",
				"stream", stream, "len", n, "watermark", g.opt.Watermark)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("queue: ожидание ёмкости %s: %w", stream, ctx.Err())
		case <-time.After(g.opt.Interval):
		}
	}
}

// WatermarkPublisher — обёртка над Queue: публикация проходит только под
// водяным знаком (backpressure, ТЗ 4.2/5.3). Сам реализует интерфейс
// Queue (декоратор): Consume и Ack передаются обёрнутой очереди без
// изменений — гейт касается только производителей; планировщик (T-028)
// и воркеры работают с обычной Queue, не зная о Redis-специфике XLEN
// (транспорт заменяем, ТЗ 4.2 — потому гейт и не встроен в
// RedisQueue.Publish).
type WatermarkPublisher struct {
	Queue
	gate *BackpressureGate
}

// NewWatermarkPublisher оборачивает очередь q гейтом gate: каждый вызов
// Publish сначала ждёт ёмкости стрима, затем публикует. Гейт nil —
// публикация ошибкой: обёртка без контроля потока бессмысленна, молча
// терять его хуже.
func NewWatermarkPublisher(q Queue, gate *BackpressureGate) *WatermarkPublisher {
	return &WatermarkPublisher{Queue: q, gate: gate}
}

// Publish публикует сообщение после ожидания ёмкости стрима
// (AwaitCapacity): q:dead и отключённый гейт публикуют сразу. Цена —
// один дополнительный XLEN на публикацию: локальный Redis держит десятки
// тысяч операций в секунду, а свежая длина важнее кэша (порог могут
// закрыть и другие продюсеры).
func (p *WatermarkPublisher) Publish(ctx context.Context, stream string, msg Message) (string, error) {
	if p.gate == nil {
		return "", fmt.Errorf("queue: публикация %s: гейт не задан (NewWatermarkPublisher)", stream)
	}
	if err := p.gate.AwaitCapacity(ctx, stream); err != nil {
		return "", err
	}
	return p.Queue.Publish(ctx, stream, msg)
}

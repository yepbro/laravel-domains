// Backpressure по глубине очереди (ТЗ 4.2: «планировщик публикует новые
// задачи только когда длина очереди ниже порога (backpressure)», §5.3:
// «публикует новые задачи только если длина очереди ниже порога»).
//
// Механизм: перед публикацией продюсер проверяет глубину стрима и
// публикует, только пока она ниже порога scheduler.queue_high_watermark
// (конфиг; значение передаётся в BackpressureOptions). Гейт закрыт —
// пауза Interval и повторная проверка; отмена ctx — чистый выход с
// ошибкой контекста (graceful shutdown, ТЗ 3.4).
//
// К чему применяется порог — общий для гейтируемых стримов:
//   - q:dns — планировщик (T-028): единица стрима — пачка доменов
//     (ТЗ 4.2), поэтому проверка перед каждым сообщением и есть
//     «проверка перед публикацией пачки»;
//   - q:http, q:analyze — воркеры: одна пачка q:dns порождает
//     до scheduler.batch_size задач q:http и по задаче q:analyze на
//     субдомен, без гейта эти стримы растут кратно быстрее q:dns и
//     угрожают бюджету памяти Redis (≤ 0,5 ГБ, ТЗ 3.2);
//   - q:render — публикация всегда мимо гейта (T-066: ожидание
//     задержало бы XACK задачи q:http), ёмкость проверяет отдельно
//     RenderQueue по порогу render.queue_high_watermark (T-174;
//     0 — общий scheduler.queue_high_watermark): глубину стрима читает
//     тот же BackpressureGate.Len, кламп порога — тот же
//     StreamWatermark (проводка cmd/http-worker);
//   - q:dead не ограничивается: перенос исчерпавших попытки задач
//     (T-025) — часть механизма надёжности и блокироваться
//     переполнением не должен.
//
// Порог живёт в секции scheduler по месту из ТЗ 5.10 (состав секций
// фиксирован, переименование ломало бы конфиги); исключение — порог
// ёмкости q:render: render.queue_high_watermark, T-174 (см. пункт
// выше). Семантика зафиксирована в docs/configuration.md.
//
// Глубина стрима — непрочитанный группой потребителей остаток плюс
// неподтверждённые доставки: XINFO GROUPS отдаёт по каждой группе lag
// (записи, ещё не доставленные потребителям, Redis ≥ 7.0 — docs/redis.md
// фиксирует 7.4) и pending (доставленные без XACK). Обе составляющие
// убывают по мере вычитывания и подтверждения — гейт открывается
// вместе с прогрессом потребителей. «Сырой» XLEN вместо них не годится:
// между обрезками MAXLEN длина стрима монотонна (XACK её не уменьшает),
// а порог ниже предела обрезки — после watermark публикация
// останавливалась бы навсегда (защёлкивание); даже выше предела XLEN
// считает и уже подтверждённую историю, т.е. не измеряет очередь вовсе.
//
// Кламп лага по XLEN (T-168): lag = entries_added − entries_read и не
// вычитает записи, срезанные XTRIM'ом/XDEL'ем недоставленными. Redis
// 7.0.x (прод 7.0.15) в этом сценарии держит положительный фантомный
// лаг вечно — инцидент 22–23.10.2026: после дренажа легаси-пачек T-149
// (XTRIM q:dns MAXLEN) лаг группы dns-worker застыл на 1753 при
// единственной записи в стриме и навсегда закрыл гейт q:dns (эффективный
// порог 125) — планировщик молчал ~15,5 ч при 40,3 млн pending. Redis
// 7.4+ корректирует лаг при обрезке сам, но гейт не зависит от версии
// сервера. Недоставленные записи — подмножество записей стрима: реальный
// непрочитанный остаток группы не больше XLEN, поэтому глубина считает
// min(lag, XLEN) по каждой группе. Кламп никогда не занижает реальный
// бэклог (пока недоставленные записи в стриме, lag ≤ XLEN и кламп
// неактивен), а фантом отсекается (min(1753, 1) = 1). Pending не
// клампится: счётчик неподтверждённых доставок всегда точен.
//
// Частные случаи глубины:
//   - групп у стрима нет (потребители ещё не создавали): ни одна запись
//     не доставлена — глубина равна длине стрима (XLEN);
//   - lag неопределён (−1: обрезка MAXLEN срезала недоставленные записи
//     или Redis < 7.0) — учитывается только pending: срезанное из стрима
//     уже не очередь (потерю восстанавливает lease, ТЗ 3.4), а счётчик
//     неподтверждённых доставок всегда точен.
//
// Отношение порога к MAXLEN: гейт обязан срабатывать ДО обрезки —
// обрезка удаляет записи независимо от того, прочитаны они (мягкое
// регулирование превращается в молчаливую потерю задач). Эффективный
// порог стрима не поднимается выше половины его предела MAXLEN
// (watermarkFor): запас в полпредела покрывает записи, опубликованные
// между проверкой гейта и срабатыванием обрезки. Для q:dns это
// заодно делает порог достижимым: 100 000 записей в стриме пределом
// 250 недостижимы — без ограничения порога гейт для q:dns не
// включался бы никогда (регулятор — только молчаливая обрезка).
//
// Сам гейт — мягкое регулирование (между проверкой и публикацией другие
// продюсеры могут добавить записи); жёсткий потолок длины — MAXLEN ~
// того же T-025.

package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultBackpressureInterval — пауза между повторными проверками глубины
// стрима, пока гейт закрыт (BackpressureOptions.Interval). Секунда:
// этапы пропускают тысячи задач в секунду (ТЗ 3.3), более длинное окно
// зря держит продюсера, более короткое — лишние обращения к Redis.
const DefaultBackpressureInterval = time.Second

// redisDepth — окно в go-redis для глубины стрима: XINFO GROUPS (lag и
// pending групп) и XLEN (запас «групп ещё нет»); узкое, как redisStreams
// (redisq.go): юнит-тесты подменяют фейком, реальные *redis.Client и
// *redis.ClusterClient удовлетворяют интерфейсу.
type redisDepth interface {
	XLen(ctx context.Context, key string) *redis.IntCmd
	XInfoGroups(ctx context.Context, key string) *redis.XInfoGroupsCmd
}

// BackpressureOptions — параметры гейта; неположительные Interval и
// nil Log заменяются дефолтами в конструкторе (стиль RedisOptions).
type BackpressureOptions struct {
	// Watermark — порог глубины стрима (scheduler.queue_high_watermark):
	// публикация разрешена при depth < Watermark, depth == Watermark —
	// гейт закрыт («строго ниже порога», ТЗ 4.2/5.3). Значение ≤ 0 отключает
	// гейт (публикация без ожидания) — как MaxLen = 0 в RedisOptions.
	// Эффективный порог конкретного стрима не выше половины его предела
	// MAXLEN (см. шапку файла и watermarkFor).
	Watermark int64
	// MaxLen — предел длины стримов политики MAXLEN, зеркалит
	// RedisOptions.MaxLen (0 — пределы по стримам T-025,
	// queue.DefaultStreamMaxLen). Гейту нужен, чтобы порог не поднялся
	// выше предела обрезки стрима.
	MaxLen int64
	// Interval — пауза между повторными проверками при закрытом гейте;
	// 0 — DefaultBackpressureInterval.
	Interval time.Duration
	// Log — логгер гейта: одна запись Warn при входе в ожидание
	// (переполнение очередей видно в логах рядом с метриками ТЗ 5.9);
	// nil — записи подавлены (slog.DiscardHandler, стиль RecheckOptions).
	Log *slog.Logger
}

// BackpressureGate — гейт ёмкости стрима: ждёт, пока глубина стрима
// опустится ниже порога (backpressure, ТЗ 4.2/5.3). Применяется сам по
// себе (планировщику T-028 выгодно проверить ёмкость до выборки пачки
// из БД: полон стрим — пачка не выбирается) и внутри WatermarkPublisher.
type BackpressureGate struct {
	rdb redisDepth
	opt BackpressureOptions
}

// NewBackpressureGate строит гейт поверх клиента go-redis — того же,
// что у RedisQueue (одна точка правды о глубине стримов).
func NewBackpressureGate(rdb redisDepth, opt BackpressureOptions) *BackpressureGate {
	if opt.Interval <= 0 {
		opt.Interval = DefaultBackpressureInterval
	}
	if opt.Log == nil {
		opt.Log = slog.New(slog.DiscardHandler)
	}
	return &BackpressureGate{rdb: rdb, opt: opt}
}

// streamMaxLen — предел длины стрима политики MAXLEN: явный из опций
// (RedisOptions.MaxLen того же сервиса) или по стримам T-025.
func (g *BackpressureGate) streamMaxLen(stream string) int64 {
	if g.opt.MaxLen > 0 {
		return g.opt.MaxLen
	}
	return DefaultStreamMaxLen(stream)
}

// watermarkFor — эффективный порог стрима: настроенный Watermark, но не
// выше половины предела MAXLEN стрима (гейт срабатывает до обрезки,
// см. шапку файла). Watermark ≤ 0 — гейт отключён (0).
func (g *BackpressureGate) watermarkFor(stream string) int64 {
	return StreamWatermark(g.opt.Watermark, g.streamMaxLen(stream), stream)
}

// StreamWatermark — эффективный порог глубины стрима: настроенный
// watermark, ограниченный половиной предела длины стрима maxLen
// (0 — предел по стримам политики T-025, DefaultStreamMaxLen). Гейт
// обязан срабатывать ДО обрезки MAXLEN: обрезка удаляет записи независимо
// от того, прочитаны они, — порог выше предела превращал бы мягкое
// регулирование в молчаливую потерю задач. Экспортировано для мониторинга
// (алерт переполнения очередей, T-080): порог алерта совпадает с порогом
// гейта по каждому стриму. watermark ≤ 0 — 0 (гейт отключён).
func StreamWatermark(watermark, maxLen int64, stream string) int64 {
	// Эквивалентные мутанты (T-M04): `<= 0` → `< 0`/`<= -1` неотличимы —
	// watermark 0 без раннего возврата даёт min(0, maxLen/2) = 0 (maxLen
	// после замены ≤ 0 — положительный предел политики), отрицательные
	// возвращаются нулём в обеих ветках. `<= 1` (numbers/incrementer)
	// отличим тестом на watermark 1 и не гасится.
	// mutator-disable-next-line expression/comparison, numbers/decrementer
	if watermark <= 0 {
		return 0
	}
	if maxLen <= 0 {
		maxLen = DefaultStreamMaxLen(stream)
	}
	return min(watermark, maxLen/2)
}

// Len возвращает глубину стрима — backlog его групп потребителей:
// непрочитанный остаток (lag, ограниченный длиной стрима — кламп T-168,
// см. шапку файла) плюс неподтверждённые доставки (pending), по всем
// группам стрима (в бою у рабочего стрима одна группа — потребители
// этапа). Групп нет — глубина равна длине стрима (XLEN): ни одна запись
// не доставлена. Подробности и обоснование метрики — в шапке файла.
func (g *BackpressureGate) Len(ctx context.Context, stream string) (int64, error) {
	if !knownStream(stream) {
		return 0, fmt.Errorf("queue: неизвестный стрим %q (рабочие стримы — константы пакета: StreamDNS, StreamHTTP, StreamRender, StreamAnalyze, StreamDead)", stream)
	}
	if g.rdb == nil {
		return 0, fmt.Errorf("queue: глубина %s: клиент Redis не задан", stream)
	}
	groups, err := g.rdb.XInfoGroups(ctx, stream).Result()
	if err != nil {
		// Стрима ещё нет — Redis отвечает ошибкой no such key (а не
		// пустым списком): групп нет, глубина без доставок равна длине
		// стрима, XLEN несуществующего ключа вернёт 0.
		if errors.Is(err, redis.Nil) || strings.Contains(err.Error(), "no such key") {
			groups = nil
		} else {
			return 0, fmt.Errorf("queue: глубина %s: %w", stream, err)
		}
	}
	if len(groups) == 0 {
		n, err := g.rdb.XLen(ctx, stream).Result()
		if err != nil {
			return 0, fmt.Errorf("queue: глубина %s (групп нет, XLEN): %w", stream, err)
		}
		return n, nil
	}
	// Кламп лага (T-168): XLEN — верхняя грань непрочитанного остатка
	// каждой группы (недоставленные записи живут в стриме). Читается один
	// раз на вызов и клампует каждую группу общим XLEN: несколько групп
	// стрима читают одни и те же записи, сумма их недоставленных может
	// перекрываться, но каждая — честный бэклог своей группы; группа без
	// доставок (lag = entries_added) ограничивается тем, что физически
	// осталось в стриме. Стрим, исчезнувший между XINFO и XLEN (DEL/обрезка
	// в 0), даёт XLEN 0 — глубина вырождается в pending, консервативно.
	xlen, err := g.rdb.XLen(ctx, stream).Result()
	if err != nil {
		return 0, fmt.Errorf("queue: глубина %s (кламп лага, XLEN): %w", stream, err)
	}
	var depth int64
	for _, grp := range groups {
		// Эквивалентные мутанты (T-M04): `>= 0` → `> 0`/`>= 1` отличаются
		// только при Lag == 0 — прибавка нуля сумму не меняет. `>= -1`
		// (numbers/decrementer) отличим при Lag == −1 (неопределён) и
		// не гасится.
		// mutator-disable-next-line expression/comparison, numbers/incrementer
		if grp.Lag >= 0 { // −1 — неопределён (обрезка недоставленного)
			depth += min(grp.Lag, xlen)
		}
		depth += grp.Pending // доставленные без XACK — тоже очередь
	}
	return depth, nil
}

// AwaitCapacity ждёт, пока глубина стрима станет ниже порога, и
// возвращает nil — публиковать можно. Стрим не ниже порога — пауза
// Interval и повтор; отмена ctx — ошибка контекста (чистый выход,
// ТЗ 3.4). Ошибка чтения глубины возвращается сразу: гейт не гадает о
// глубине (недоступный Redis — не «очередь пуста»), решение о повторе
// принимает вызывающий цикл.
//
// Отключённый гейт (Watermark ≤ 0) и q:dead проходят без обращения
// к Redis (см. шапку файла).
func (g *BackpressureGate) AwaitCapacity(ctx context.Context, stream string) error {
	watermark := g.watermarkFor(stream)
	if watermark <= 0 || stream == StreamDead {
		return nil
	}
	waited := false
	for {
		n, err := g.Len(ctx, stream)
		if err != nil {
			return err
		}
		if n < watermark {
			return nil
		}
		if !waited {
			waited = true
			g.opt.Log.Warn("публикация приостановлена: глубина очереди у порога",
				"stream", stream, "depth", n, "watermark", watermark)
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
// и воркеры работают с обычной Queue, не зная о Redis-специфике глубины
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
// одно дополнительное чтение глубины на публикацию: локальный Redis
// держит десятки тысяч операций в секунду, а свежая глубина важнее кэша
// (порог могут закрыть и другие продюсеры).
func (p *WatermarkPublisher) Publish(ctx context.Context, stream string, msg Message) (string, error) {
	if p.gate == nil {
		return "", fmt.Errorf("queue: публикация %s: гейт не задан (NewWatermarkPublisher)", stream)
	}
	if err := p.gate.AwaitCapacity(ctx, stream); err != nil {
		return "", err
	}
	return p.Queue.Publish(ctx, stream, msg)
}

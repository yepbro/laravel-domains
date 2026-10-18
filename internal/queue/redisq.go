// Реализация Queue на Redis Streams (ТЗ 4.2): публикация XADD, чтение
// пачками XREADGROUP ">" через consumer group, подтверждение XACK.
// Команды go-redis не выходят за пределы узкого интерфейса redisStreams —
// очередь можно подменить фейком в юнит-тестах (без живого Redis).

package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// fieldData — единственное поле записи стрима: JSON-сообщение (message.go).
const fieldData = "data"

// DefaultRedisBlock — интервал ожидания новых записей в одном вызове
// Consume по умолчанию. Небольшой, а не бесконечный: цикл воркера
// регулярно просыпается (контекст отменяется между итерациями), а доля
// «пустых» пробуждений мала относительно времени обработки пачки.
const DefaultRedisBlock = 2 * time.Second

// redisStreams — окно в go-redis: только команды стримов, нужные
// очереди. *redis.Client (queue.NewClient) и *redis.ClusterClient
// удовлетворяют интерфейсу; тесты подменяют фейком — cmd-результаты
// go-redis строятся фабриками New*Result без соединения.
type redisStreams interface {
	XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd
	XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd
	XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd
	XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd
}

// RedisOptions — параметры Redis-реализации очереди.
type RedisOptions struct {
	// Block — время ожидания новых записей в одном вызове Consume
	// (XREADGROUP BLOCK); 0 — DefaultRedisBlock.
	Block time.Duration
	// MaxLen — явный предел длины всех стримов при публикации
	// (XADD MAXLEN ~); 0 — пределы политики надёжности по стримам
	// (DefaultStreamMaxLen, T-025: ТЗ 4.2 требует MAXLEN на каждом
	// стриме, поэтому режим «не обрезать» не предусмотрен — источник
	// истины PostgreSQL, усечение восстановимо по lease, ТЗ 3.4).
	MaxLen int64
}

// RedisQueue — реализация Queue на Redis Streams. Группа потребителей
// создаётся лениво при первом чтении стрима: XGROUP CREATE c MKSTREAM
// (стрим создаётся пустым) и стартовым идентификатором "0" — первые
// потребители получают и записи, опубликованные до их запуска; ответ
// BUSYGROUP (группу уже создал другой воркер) — не ошибка. При потере
// группы (NOGROUP: удалили группу/стрим, рестарт Redis без персистентности)
// группа пересоздаётся с последнего прочитанного этим экземпляром id —
// не с "0": переигрывать до миллиона уже обработанных записей стрима
// (MAXLEN штучных стримов) не нужно, дубли с другими экземплярами
// ограничены их окном чтения и гасятся идемпотентностью (ТЗ 3.4).
type RedisQueue struct {
	rdb    redisStreams
	opt    RedisOptions
	groups sync.Map // "stream|group" → последний прочитанный id ("0" — с начала; группа создана)
}

// NewRedisQueue строит очередь поверх клиента go-redis (NewClient или
// готовый *redis.Client / *redis.ClusterClient).
func NewRedisQueue(rdb redisStreams, opt RedisOptions) *RedisQueue {
	if opt.Block <= 0 {
		opt.Block = DefaultRedisBlock
	}
	return &RedisQueue{rdb: rdb, opt: opt}
}

// Publish сериализует сообщение (поле data) и добавляет запись в стрим
// командой XADD, возвращая её идентификатор. Каждый XADD усекает стрим
// приблизительным пределом MAXLEN ~ (ТЗ 4.2): явным из RedisOptions.MaxLen
// или пределом стрима по политике T-025 (DefaultStreamMaxLen). Стрим обязан
// соответствовать типу сообщения, значения проходят проверку до записи
// (ошибка — сообщение не публикуется).
func (q *RedisQueue) Publish(ctx context.Context, stream string, msg Message) (string, error) {
	if !knownStream(stream) {
		return "", fmt.Errorf("queue: неизвестный стрим %q (рабочие стримы — константы пакета: StreamDNS, StreamHTTP, StreamRender, StreamAnalyze, StreamDead)", stream)
	}
	if !streamAllowed(msg, stream) {
		return "", fmt.Errorf("queue: сообщение %T публикуется в %v, а не в %q", msg, msg.Streams(), stream)
	}
	if err := msg.validate(); err != nil {
		return "", err
	}
	data, err := marshalMessage(msg)
	if err != nil {
		return "", fmt.Errorf("queue: сериализовать %T: %w", msg, err)
	}
	args := &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{fieldData: data},
	}
	maxLen := q.opt.MaxLen
	if maxLen == 0 {
		maxLen = DefaultStreamMaxLen(stream) // политика T-025 (ТЗ 4.2)
	}
	args.MaxLen = maxLen
	args.Approx = true // MAXLEN ~: обрезка без точного подсчёта (T-025)
	id, err := q.rdb.XAdd(ctx, args).Result()
	if err != nil {
		return "", fmt.Errorf("queue: publish в %s: %w", stream, err)
	}
	return id, nil
}

// Consume читает до n записей стрима, ещё не прочитанных группой
// (XREADGROUP … ">"), от имени потребителя consumer. Новых записей нет —
// пустой результат без ошибки (истёк Block).
func (q *RedisQueue) Consume(ctx context.Context, stream, group, consumer string, n int) ([]Delivery, error) {
	if !knownStream(stream) {
		return nil, fmt.Errorf("queue: неизвестный стрим %q", stream)
	}
	if group == "" {
		return nil, fmt.Errorf("queue: не задана группа потребителей стрима %s", stream)
	}
	if consumer == "" {
		return nil, fmt.Errorf("queue: не задано имя потребителя группы %s", group)
	}
	if n < 1 {
		return nil, fmt.Errorf("queue: размер пачки %d < 1", n)
	}
	if err := q.ensureGroup(ctx, stream, group, "0"); err != nil {
		return nil, err
	}
	read := &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    int64(n),
		Block:    q.opt.Block,
	}
	res, err := q.rdb.XReadGroup(ctx, read).Result()
	if isNoGroup(err) {
		// Потеря Redis при живых воркерах (ТЗ 3.4): группа исчезла вместе
		// со стримом, а кэш созданных групп устарел. Пересоздаём группу —
		// с последнего прочитанного этим экземпляром id (не с "0":
		// история до него уже обработана, переигрывать её лавиной
		// дублирующей работы не нужно, недостающее переиздаст планировщик
		// по lease) — и читаем повторно: потребитель продолжает работу
		// без рестарта процесса.
		last, _ := q.groups.Load(groupKey(stream, group))
		start, _ := last.(string)
		if start == "" {
			start = "0" // ни одного чтения — как первое создание
		}
		q.groups.Delete(groupKey(stream, group))
		if err := q.ensureGroup(ctx, stream, group, start); err != nil {
			return nil, err
		}
		res, err = q.rdb.XReadGroup(ctx, read).Result()
	}
	if isNil(err) { // истёк интервал ожидания — новых записей нет
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("queue: read %s (группа %s): %w", stream, group, err)
	}
	out := make([]Delivery, 0, len(res))
	for _, xs := range res {
		for _, m := range xs.Messages {
			out = append(out, Delivery{
				Stream:  xs.Stream,
				ID:      m.ID,
				Payload: payloadBytes(m.Values),
			})
		}
		q.trackProgress(stream, group, xs.Messages)
	}
	return out, nil
}

// Ack подтверждает обработку записей (XACK). Подтверждать следует только
// после записи результата в БД (ТЗ 4.2): неподтверждённая запись будет
// доставлена повторно (возврат зависших — XAUTOCLAIM, T-025).
func (q *RedisQueue) Ack(ctx context.Context, stream, group string, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	if err := q.rdb.XAck(ctx, stream, group, ids...).Err(); err != nil {
		return fmt.Errorf("queue: ack %s (группа %s): %w", stream, group, err)
	}
	return nil
}

// ensureGroup создаёт группу потребителей при первом обращении
// (XGROUP CREATE … MKSTREAM): start — идентификатор, с которого группа
// доставляет записи ("0" — с начала: первое создание получает и записи,
// опубликованные до запуска потребителей; последний прочитанный id —
// восстановление после NOGROUP, см. Consume). Повторные создания группы
// (BUSYGROUP от другого воркера) — не ошибка. Созданные группы
// запоминаются, чтобы не тратить круг обращения на каждый Consume.
func (q *RedisQueue) ensureGroup(ctx context.Context, stream, group, start string) error {
	key := groupKey(stream, group)
	if _, ok := q.groups.Load(key); ok {
		return nil
	}
	err := q.rdb.XGroupCreateMkStream(ctx, stream, group, start).Err()
	if err != nil && !isBusyGroup(err) {
		return fmt.Errorf("queue: создать группу %s в %s: %w", group, stream, err)
	}
	q.groups.Store(key, start)
	return nil
}

// trackProgress запоминает последний прочитанный id группы — стартовая
// точка пересоздания группы после NOGROUP (см. Consume). Идентификаторы
// записей растут (ms-seq), сравнение числовое: лексикографический
// порядок "999-0" > "1000-0" ошибочен. Ошибки парсинга тихо пропускаются:
// прогресс — оптимизация восстановления, а не инвариант.
func (q *RedisQueue) trackProgress(stream, group string, msgs []redis.XMessage) {
	if len(msgs) == 0 {
		return
	}
	key := groupKey(stream, group)
	last, _ := q.groups.LoadOrStore(key, "0")
	cur, _ := last.(string)
	if cur == "" || cur == "0" {
		cur = "0-0" // стартовые отметки — в форме ms-seq для сравнения
	}
	for _, m := range msgs {
		if streamIDGreater(m.ID, cur) {
			cur = m.ID
		}
	}
	if cur != last {
		q.groups.Store(key, cur)
	}
}

// streamIDGreater сравнивает идентификаторы записей стрима "ms-seq"
// численно по частям.
func streamIDGreater(a, b string) bool {
	ams, aseq, err := splitStreamID(a)
	if err != nil {
		return false
	}
	bms, bseq, err := splitStreamID(b)
	if err != nil {
		return false
	}
	if ams != bms {
		return ams > bms
	}
	return aseq > bseq
}

// splitStreamID разбирает "ms-seq" в пару int64.
func splitStreamID(id string) (ms, seq int64, err error) {
	s1, s2, ok := strings.Cut(id, "-")
	if !ok {
		return 0, 0, fmt.Errorf("queue: идентификатор записи %q не вида ms-seq", id)
	}
	if ms, err = strconv.ParseInt(s1, 10, 64); err != nil {
		return 0, 0, err
	}
	if seq, err = strconv.ParseInt(s2, 10, 64); err != nil {
		return 0, 0, err
	}
	return ms, seq, nil
}

// payloadBytes достаёт поле data из значений записи: go-redis возвращает
// строки; []byte поддержан на случай прямой записи в стрим.
func payloadBytes(values map[string]any) []byte {
	switch v := values[fieldData].(type) {
	case string:
		return []byte(v)
	case []byte:
		return v
	default:
		return nil
	}
}

// groupKey — ключ кэша созданных групп.
func groupKey(stream, group string) string { return stream + "|" + group }

// knownStream сообщает, что имя — один из рабочих стримов очереди (ТЗ 4.2).
func knownStream(stream string) bool {
	switch stream {
	case StreamDNS, StreamHTTP, StreamRender, StreamAnalyze, StreamDead:
		return true
	default:
		return false
	}
}

// isBusyGroup распознаёт ответ BUSYGROUP — группа уже существует.
func isBusyGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

// isNoGroup распознаёт ответ NOGROUP — группа потребителей отсутствует:
// стрим или группу удалили (потеря/очистка Redis при живых воркерах).
func isNoGroup(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOGROUP")
}

// isNil — «ничего не прочитано» (в том числе таймаут BLOCK).
func isNil(err error) bool { return errors.Is(err, redis.Nil) }

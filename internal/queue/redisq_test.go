// Тесты реализации RedisQueue без живого Redis (T-024): фейк реализует
// то же узкое окно команд redisStreams, что и *redis.Client, — результаты
// команд go-redis строятся фабриками New*Result. Проверяется маппинг
// аргументов и результатов: XADD (поле data, MAXLEN ~), ленивое создание
// группы с поглощением BUSYGROUP, XREADGROUP ">", XACK. Реальная
// семантика Redis — интеграционные тесты T-032.

package queue

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// fakeRedis — запись вызовов команд + заготовленные результаты.
type fakeRedis struct {
	adds   []redis.XAddArgs
	addID  string
	addErr error

	creates   []string // "stream group start" на каждый вызов
	createErr error

	reads   []redis.XReadGroupArgs
	readRes []redis.XStream
	readErr error
	// readErrs — ошибки чтения по вызовам подряд (очередь); пусто —
	// постоянная readErr. Нужно сценариям с меняющимся ответом Redis
	// (NOGROUP после потери стрима, T-032).
	readErrs []error

	acks     []ackCall
	ackCount int64
	ackErr   error
}

// ackCall — зафиксированный вызов XACK.
type ackCall struct {
	stream, group string
	ids           []string
}

func (f *fakeRedis) XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd {
	f.adds = append(f.adds, *a)
	return redis.NewStringResult(f.addID, f.addErr)
}

func (f *fakeRedis) XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd {
	f.creates = append(f.creates, stream+" "+group+" "+start)
	return redis.NewStatusResult("OK", f.createErr)
}

func (f *fakeRedis) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	f.reads = append(f.reads, *a)
	err := f.readErr
	if len(f.readErrs) > 0 {
		err = f.readErrs[0]
		f.readErrs = f.readErrs[1:]
	}
	return redis.NewXStreamSliceCmdResult(f.readRes, err)
}

func (f *fakeRedis) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	f.acks = append(f.acks, ackCall{stream, group, ids})
	return redis.NewIntResult(f.ackCount, f.ackErr)
}

// Компиляционные проверки: фейк и реальный клиент взаимозаменяемы,
// RedisQueue реализует Queue.
var (
	_ redisStreams = (*fakeRedis)(nil)
	_ redisStreams = (*redis.Client)(nil)
	_ Queue        = (*RedisQueue)(nil)
)

func TestRedisQueuePublish(t *testing.T) {
	fake := &fakeRedis{addID: "1718-0"}
	q := NewRedisQueue(fake, RedisOptions{})

	id, err := q.Publish(context.Background(), StreamDNS, DNSBatch{
		Domains: []DNSDomain{{ZoneID: 3, DomainID: 42, Name: "example.com"}},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if id != "1718-0" {
		t.Errorf("Publish вернул %q, хочу идентификатор записи %q", id, fake.addID)
	}
	if len(fake.adds) != 1 {
		t.Fatalf("вызовов XADD: %d, хочу 1", len(fake.adds))
	}
	add := fake.adds[0]
	if add.Stream != StreamDNS {
		t.Errorf("XADD в стрим %q, хочу %q", add.Stream, StreamDNS)
	}
	// Политика T-025: без явного предела применяется предел стрима.
	if add.MaxLen != DefaultStreamMaxLen(StreamDNS) || !add.Approx {
		t.Errorf("XADD без MaxLen в опциях: MaxLen=%d Approx=%v, хочу MAXLEN ~ %d",
			add.MaxLen, add.Approx, DefaultStreamMaxLen(StreamDNS))
	}
	values, ok := add.Values.(map[string]any)
	if !ok {
		t.Fatalf("XADD values: %T, хочу map[string]any", add.Values)
	}
	data, ok := values[fieldData].([]byte)
	if !ok {
		t.Fatalf("XADD values[%q]: %T, хочу []byte", fieldData, values[fieldData])
	}
	var probe struct {
		TaskID  string      `json:"task_id"`
		Domains []DNSDomain `json:"domains"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		t.Fatalf("поле data не JSON: %v", err)
	}
	if len(probe.TaskID) != 32 {
		t.Errorf("пустой task_id не заполнен при публикации: %q", probe.TaskID)
	}
	if len(probe.Domains) != 1 || probe.Domains[0].Name != "example.com" {
		t.Errorf("поле data: %+v", probe)
	}
}

func TestRedisQueuePublishMaxLen(t *testing.T) {
	fake := &fakeRedis{}
	q := NewRedisQueue(fake, RedisOptions{MaxLen: 100000})
	if _, err := q.Publish(context.Background(), StreamRender,
		Result{ZoneID: 1, DomainID: 1, SubdomainID: 0}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	add := fake.adds[0]
	if add.MaxLen != 100000 || !add.Approx {
		t.Errorf("MAXLEN ~ не передан: MaxLen=%d Approx=%v", add.MaxLen, add.Approx)
	}
}

func TestRedisQueuePublishError(t *testing.T) {
	cases := []struct {
		name   string
		stream string
		msg    Message
	}{
		{"чужой стрим", StreamHTTP, DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: "a.test"}}}},
		{"невалидное сообщение", StreamDNS, DNSBatch{}},
		{"неизвестный стрим", "q:typo", DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: "a.test"}}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeRedis{}
			q := NewRedisQueue(fake, RedisOptions{})
			if _, err := q.Publish(context.Background(), c.stream, c.msg); err == nil {
				t.Fatal("Publish прошёл без ошибки")
			}
			if len(fake.adds) != 0 {
				t.Errorf("невалидное сообщение дошло до XADD: %+v", fake.adds)
			}
		})
	}

	fake := &fakeRedis{addErr: errors.New("connection refused")}
	q := NewRedisQueue(fake, RedisOptions{})
	if _, err := q.Publish(context.Background(), StreamDNS, DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: "a.test"}}}); err == nil {
		t.Fatal("ошибка XADD не возвращена")
	}
}

func TestRedisQueueConsume(t *testing.T) {
	fake := &fakeRedis{
		readRes: []redis.XStream{{
			Stream: StreamDNS,
			Messages: []redis.XMessage{
				{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
				{ID: "1719-0", Values: map[string]any{fieldData: []byte(`{"task_id":"t2"}`)}},
				{ID: "1720-0", Values: map[string]any{"other": "x"}}, // без поля data
			},
		}},
	}
	q := NewRedisQueue(fake, RedisOptions{})

	got, err := q.Consume(context.Background(), StreamDNS, "dns", "dns-1", 10)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// Группа создаётся при первом чтении: с начала стрима, MKSTREAM.
	if want := []string{StreamDNS + " dns 0"}; !reflect.DeepEqual(fake.creates, want) {
		t.Errorf("XGROUP CREATE: %v, хочу %v", fake.creates, want)
	}

	// Аргументы XREADGROUP: только новые записи группы, пачка и ожидание.
	if len(fake.reads) != 1 {
		t.Fatalf("вызовов XREADGROUP: %d, хочу 1", len(fake.reads))
	}
	args := fake.reads[0]
	if args.Group != "dns" || args.Consumer != "dns-1" {
		t.Errorf("группа/потребитель: %q/%q", args.Group, args.Consumer)
	}
	if want := []string{StreamDNS, ">"}; !reflect.DeepEqual(args.Streams, want) {
		t.Errorf("Streams: %v, хочу %v", args.Streams, want)
	}
	if args.Count != 10 {
		t.Errorf("Count: %d, хочу 10", args.Count)
	}
	if args.Block != DefaultRedisBlock {
		t.Errorf("Block: %s, хочу по умолчанию %s", args.Block, DefaultRedisBlock)
	}

	want := []Delivery{
		{Stream: StreamDNS, ID: "1718-0", Payload: []byte(`{"task_id":"t1"}`)},
		{Stream: StreamDNS, ID: "1719-0", Payload: []byte(`{"task_id":"t2"}`)},
		{Stream: StreamDNS, ID: "1720-0", Payload: nil},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Consume:\n получил  %+v\n хочу     %+v", got, want)
	}
}

func TestRedisQueueConsumeGroupCached(t *testing.T) {
	fake := &fakeRedis{readErr: redis.Nil}
	q := NewRedisQueue(fake, RedisOptions{})
	for i := 0; i < 3; i++ {
		if _, err := q.Consume(context.Background(), StreamHTTP, "http", "http-1", 5); err != nil {
			t.Fatalf("Consume №%d: %v", i+1, err)
		}
	}
	if len(fake.creates) != 1 {
		t.Errorf("XGROUP CREATE вызван %d раз, хочу 1 (группа запоминается)", len(fake.creates))
	}
	if len(fake.reads) != 3 {
		t.Errorf("XREADGROUP вызван %d раз, хочу 3", len(fake.reads))
	}

	// Другая группа того же стрима — отдельное создание.
	if _, err := q.Consume(context.Background(), StreamHTTP, "http-2", "http-1", 5); err != nil {
		t.Fatalf("Consume другой группы: %v", err)
	}
	if len(fake.creates) != 2 {
		t.Errorf("XGROUP CREATE вызван %d раз, хочу 2", len(fake.creates))
	}
}

func TestRedisQueueConsumeBusyGroup(t *testing.T) {
	fake := &fakeRedis{
		createErr: errors.New("BUSYGROUP Consumer Group name already exists"),
		readErr:   redis.Nil,
	}
	q := NewRedisQueue(fake, RedisOptions{})
	if _, err := q.Consume(context.Background(), StreamAnalyze, "analyzer", "analyzer-1", 1); err != nil {
		t.Fatalf("BUSYGROUP — не ошибка: %v", err)
	}
}

// TestRedisQueueConsumeGroupLost — потеря Redis при живом воркере
// (ТЗ 3.4, T-032): группа исчезла вместе со стримом, кэш созданных групп
// устарел — XREADGROUP отвечает NOGROUP. Consume пересоздаёт группу
// (XGROUP CREATE MKSTREAM) и повторяет чтение, а не падает до рестарта
// процесса; задачи в это время переиздаёт планировщик по lease.
func TestRedisQueueConsumeGroupLost(t *testing.T) {
	fake := &fakeRedis{readErrs: []error{
		nil, // чтение до потери: пусто, без ошибки
		errors.New("NOGROUP No such consumer group 'dns' for key name 'q:dns'"),
		redis.Nil, // повторное чтение: новых записей нет
	}}
	q := NewRedisQueue(fake, RedisOptions{})

	// Первое чтение создаёт группу и попадает в кэш.
	if _, err := q.Consume(context.Background(), StreamDNS, "dns", "dns-1", 1); err != nil {
		t.Fatalf("Consume до потери: %v", err)
	}
	// Второе — после «потери» стрима: NOGROUP, пересоздание, повтор.
	got, err := q.Consume(context.Background(), StreamDNS, "dns", "dns-1", 1)
	if err != nil || got != nil {
		t.Fatalf("Consume после потери: (%v, %v), хочу (nil, nil)", got, err)
	}
	if len(fake.creates) != 2 {
		t.Errorf("XGROUP CREATE вызван %d раз, хочу 2 (пересоздание после NOGROUP)", len(fake.creates))
	}
	if len(fake.reads) != 3 {
		t.Errorf("XREADGROUP вызван %d раз, хочу 3", len(fake.reads))
	}
}

// TestRedisQueueConsumeGroupLostAtProgress — NOGROUP после успешно
// прочитанных записей: группа пересоздаётся с последнего прочитанного id,
// а не с "0" — переигрывать обработанную историю стрима (до MAXLEN
// записей) лавиной дублей не нужно (Q-06).
func TestRedisQueueConsumeGroupLostAtProgress(t *testing.T) {
	fake := &fakeRedis{
		readRes: []redis.XStream{{
			Stream: StreamDNS,
			Messages: []redis.XMessage{
				{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
				{ID: "1719-0", Values: map[string]any{fieldData: `{"task_id":"t2"}`}},
			},
		}},
		readErrs: []error{
			nil, // первое чтение: две записи (прогресс — 1719-0)
			errors.New("NOGROUP No such consumer group 'dns' for key name 'q:dns'"),
			redis.Nil, // повторное чтение: новых записей нет
		},
	}
	q := NewRedisQueue(fake, RedisOptions{})

	if got, err := q.Consume(context.Background(), StreamDNS, "dns", "dns-1", 10); err != nil || len(got) != 2 {
		t.Fatalf("первое чтение: (%v, %v)", got, err)
	}
	// «Потеря» группы: пересоздание с прогресса первого чтения.
	if got, err := q.Consume(context.Background(), StreamDNS, "dns", "dns-1", 10); err != nil || got != nil {
		t.Fatalf("чтение после потери: (%v, %v), хочу (nil, nil)", got, err)
	}
	want := []string{
		StreamDNS + " dns 0",      // первое создание — с начала стрима
		StreamDNS + " dns 1719-0", // восстановление — с последнего прочитанного
	}
	if !reflect.DeepEqual(fake.creates, want) {
		t.Errorf("XGROUP CREATE: %v, хочу %v", fake.creates, want)
	}
}

func TestRedisQueueConsumeErrors(t *testing.T) {
	ctx := context.Background()

	fake := &fakeRedis{createErr: errors.New("ERR connection refused")}
	if _, err := NewRedisQueue(fake, RedisOptions{}).Consume(ctx, StreamDNS, "dns", "dns-1", 1); err == nil ||
		!strings.Contains(err.Error(), "создать группу") {
		t.Errorf("ошибка создания группы: %v", err)
	}

	fake = &fakeRedis{readErr: errors.New("ERR bad group")}
	if _, err := NewRedisQueue(fake, RedisOptions{}).Consume(ctx, StreamDNS, "dns", "dns-1", 1); err == nil ||
		!strings.Contains(err.Error(), "read") {
		t.Errorf("ошибка чтения: %v", err)
	}
}

func TestRedisQueueConsumeBadArgs(t *testing.T) {
	cases := []struct {
		name                    string
		stream, group, consumer string
		n                       int
	}{
		{"неизвестный стрим", "q:typo", "g", "c", 1},
		{"без группы", StreamDNS, "", "c", 1},
		{"без потребителя", StreamDNS, "g", "", 1},
		{"пачка 0", StreamDNS, "g", "c", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeRedis{}
			if _, err := NewRedisQueue(fake, RedisOptions{}).Consume(context.Background(), c.stream, c.group, c.consumer, c.n); err == nil {
				t.Fatal("ожидалась ошибка аргументов")
			}
			if len(fake.reads)+len(fake.creates) != 0 {
				t.Errorf("Redis вызван с плохими аргументами: %+v %+v", fake.creates, fake.reads)
			}
		})
	}
}

func TestRedisQueueConsumeBlockOption(t *testing.T) {
	fake := &fakeRedis{readErr: redis.Nil}
	q := NewRedisQueue(fake, RedisOptions{Block: 500 * time.Millisecond})
	if _, err := q.Consume(context.Background(), StreamDNS, "dns", "dns-1", 1); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if got := fake.reads[0].Block; got != 500*time.Millisecond {
		t.Errorf("Block: %s, хочу 500ms", got)
	}
}

func TestRedisQueueConsumeEmpty(t *testing.T) {
	fake := &fakeRedis{readErr: redis.Nil}
	got, err := NewRedisQueue(fake, RedisOptions{}).Consume(context.Background(), StreamDNS, "dns", "dns-1", 1)
	if err != nil || got != nil {
		t.Errorf("пустое чтение: (%v, %v), хочу (nil, nil)", got, err)
	}
}

func TestRedisQueueAck(t *testing.T) {
	fake := &fakeRedis{ackCount: 2}
	q := NewRedisQueue(fake, RedisOptions{})
	if err := q.Ack(context.Background(), StreamDNS, "dns", "1718-0", "1719-0"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if want := []ackCall{{StreamDNS, "dns", []string{"1718-0", "1719-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK: %+v, хочу %+v", fake.acks, want)
	}

	// Без идентификаторов — обращение к Redis не нужно.
	fake.acks = nil
	if err := q.Ack(context.Background(), StreamDNS, "dns"); err != nil {
		t.Fatalf("Ack без ids: %v", err)
	}
	if len(fake.acks) != 0 {
		t.Errorf("XACK с пустым списком ids: %+v", fake.acks)
	}

	fake.ackErr = errors.New("ERR no such group")
	if err := q.Ack(context.Background(), StreamDNS, "dns", "1-0"); err == nil || !strings.Contains(err.Error(), "ack") {
		t.Errorf("ошибка XACK: %v", err)
	}
}

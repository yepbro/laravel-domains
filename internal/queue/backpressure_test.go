// Тесты backpressure-гейта (T-026) без живого Redis (стиль redisq_test.go):
// фейк расширяет fakeRedis командой XLEN — то же узкое окно redisLen, что
// и у *redis.Client. Проверяются маппинг XLEN, границы порога (ниже/
// ровно/выше), ожидание с повтором через интервал, пачечная публикация
// с проверкой перед каждым сообщением, отмена ctx, ошибки XLEN,
// q:dead и отключённый гейт. Реальная семантика XLEN — интеграционные
// тесты T-032.

package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// fakeBackpressure — fakeRedis (redisq_test.go) плюс XLEN: vals — длины
// стрима по очереди (последняя повторяется — «стрим полон»), log —
// порядок обращений к Redis (xlen до xadd — сам предмет проверки).
type fakeBackpressure struct {
	*fakeRedis
	vals    []int64
	xlenErr error

	lens []string // ключи XLEN
	log  []string // порядок команд: "xlen:<стрим>" / "xadd:<стрим>"
}

func (f *fakeBackpressure) XLen(ctx context.Context, key string) *redis.IntCmd {
	f.lens = append(f.lens, key)
	f.log = append(f.log, "xlen:"+key)
	var v int64
	if len(f.vals) > 0 {
		v = f.vals[0]
		if len(f.vals) > 1 {
			f.vals = f.vals[1:]
		}
	}
	return redis.NewIntResult(v, f.xlenErr)
}

func (f *fakeBackpressure) XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd {
	f.log = append(f.log, "xadd:"+a.Stream)
	return f.fakeRedis.XAdd(ctx, a)
}

// Компиляционные проверки: фейк взаимозаменяем с реальным клиентом
// в окне XLEN и в окне очереди, обёртка — полноценная Queue.
var (
	_ redisLen     = (*redis.Client)(nil)
	_ redisLen     = (*fakeBackpressure)(nil)
	_ redisStreams = (*fakeBackpressure)(nil)
	_ Queue        = (*WatermarkPublisher)(nil)
)

// newFakeBackpressure — фейк с инициализированной очередью и
// заготовленными длинами стрима.
func newFakeBackpressure(vals ...int64) *fakeBackpressure {
	return &fakeBackpressure{fakeRedis: &fakeRedis{}, vals: vals}
}

// newTestGate — гейт над фейком: порог watermark, интервал повтора 1 мс.
func newTestGate(f *fakeBackpressure, watermark int64) *BackpressureGate {
	return NewBackpressureGate(f, BackpressureOptions{Watermark: watermark, Interval: time.Millisecond})
}

// newTestPublisher — обёртка RedisQueue над тем же фейком: гейт с порогом
// 100 против длин из f.vals.
func newTestPublisher(f *fakeBackpressure) *WatermarkPublisher {
	return NewWatermarkPublisher(NewRedisQueue(f, RedisOptions{}), newTestGate(f, 100))
}

// dnsBatch — валидное сообщение q:dns на один домен.
func dnsBatch() DNSBatch {
	return DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: "a.test"}}}
}

func TestBackpressureLen(t *testing.T) {
	fake := newFakeBackpressure(42)
	gate := NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})

	n, err := gate.Len(context.Background(), StreamHTTP)
	if err != nil {
		t.Fatalf("Len: %v", err)
	}
	if n != 42 {
		t.Errorf("Len = %d, хочу 42 (значение XLEN)", n)
	}
	if want := []string{StreamHTTP}; !reflect.DeepEqual(fake.lens, want) {
		t.Errorf("XLEN ключи: %v, хочу %v", fake.lens, want)
	}

	if _, err := gate.Len(context.Background(), "q:typo"); err == nil ||
		!strings.Contains(err.Error(), "неизвестный стрим") {
		t.Errorf("Len чужого стрима: %v", err)
	}

	gate = NewBackpressureGate(nil, BackpressureOptions{Watermark: 100})
	if _, err := gate.Len(context.Background(), StreamDNS); err == nil ||
		!strings.Contains(err.Error(), "клиент Redis не задан") {
		t.Errorf("Len без клиента: %v", err)
	}
}

func TestBackpressureGateBelowWatermark(t *testing.T) {
	fake := newFakeBackpressure(99)
	gate := newTestGate(fake, 100)

	if err := gate.AwaitCapacity(context.Background(), StreamDNS); err != nil {
		t.Fatalf("AwaitCapacity ниже порога: %v", err)
	}
	if want := []string{StreamDNS}; !reflect.DeepEqual(fake.lens, want) {
		t.Errorf("XLEN: %d вызовов (%v), хочу 1 — ниже порога проходит сразу", len(fake.lens), fake.lens)
	}
}

func TestBackpressureGateAtWatermark(t *testing.T) {
	// len == watermark — «строго ниже порога» не выполнено: гейт закрыт,
	// публикация ждёт, пока длина не опустится (99 < 100).
	fake := newFakeBackpressure(100, 100, 99)
	gate := newTestGate(fake, 100)

	if err := gate.AwaitCapacity(context.Background(), StreamDNS); err != nil {
		t.Fatalf("AwaitCapacity на пороге: %v", err)
	}
	if len(fake.lens) != 3 {
		t.Errorf("XLEN вызовов %d, хочу 3 (два отказа, третий ниже порога): %v", len(fake.lens), fake.lens)
	}
}

func TestBackpressureGateAboveWatermark(t *testing.T) {
	fake := newFakeBackpressure(150, 7)
	gate := newTestGate(fake, 100)

	if err := gate.AwaitCapacity(context.Background(), StreamRender); err != nil {
		t.Fatalf("AwaitCapacity выше порога: %v", err)
	}
	if len(fake.lens) != 2 {
		t.Errorf("XLEN вызовов %d, хочу 2 (ожидание и повтор): %v", len(fake.lens), fake.lens)
	}
}

func TestBackpressureGateRetriesOverInterval(t *testing.T) {
	// Гейт закрыт (5 ≥ 1 всё время): повторные XLEN идут с интервалом;
	// ctx истекает в ожидании — ошибка контекста. За 100 мс при интервале
	// 40 мс обязано случиться не меньше двух проверок.
	fake := newFakeBackpressure(5)
	gate := NewBackpressureGate(fake, BackpressureOptions{Watermark: 1, Interval: 40 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := gate.AwaitCapacity(ctx, StreamDNS)
	if err == nil {
		t.Fatal("AwaitCapacity при закрытом гейте прошёл без ошибки")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("ошибка ожидания: %v, хочу context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Errorf("вышел раньше двух интервалов ожидания: %s", elapsed)
	}
	if len(fake.lens) < 2 {
		t.Errorf("XLEN вызовов %d, хочу ≥ 2 (повторы через интервал): %v", len(fake.lens), fake.lens)
	}
}

func TestBackpressureGateCanceledContext(t *testing.T) {
	// Отмена до вызова: первая проверка, затем выход по ctx без ожидания
	// интервала (чистый выход, ТЗ 3.4).
	fake := newFakeBackpressure(100)
	gate := NewBackpressureGate(fake, BackpressureOptions{Watermark: 10, Interval: time.Minute})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := gate.AwaitCapacity(ctx, StreamDNS)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка: %v, хочу context.Canceled", err)
	}
	if len(fake.lens) != 1 {
		t.Errorf("XLEN вызовов %d, хочу 1", len(fake.lens))
	}
}

func TestBackpressureGateXLenError(t *testing.T) {
	fake := &fakeBackpressure{
		fakeRedis: &fakeRedis{},
		xlenErr:   errors.New("ERR connection refused"),
	}
	gate := newTestGate(fake, 10)

	if err := gate.AwaitCapacity(context.Background(), StreamDNS); err == nil ||
		!strings.Contains(err.Error(), "длина") {
		t.Errorf("ошибка XLEN не возвращена: %v", err)
	}
	if len(fake.lens) != 1 {
		t.Errorf("XLEN вызовов %d, хочу 1 — ошибку гейт не ретраит", len(fake.lens))
	}
}

func TestBackpressureGateUngated(t *testing.T) {
	// Отключённый гейт, q:dead и чужой стрим — без обращения к Redis.
	fake := newFakeBackpressure(1000)
	disabled := NewBackpressureGate(fake, BackpressureOptions{Watermark: 0})
	if err := disabled.AwaitCapacity(context.Background(), StreamDNS); err != nil {
		t.Errorf("отключённый гейт: %v", err)
	}
	if len(fake.lens) != 0 {
		t.Errorf("отключённый гейт полез в Redis: %v", fake.lens)
	}

	gate := newTestGate(fake, 10)
	if err := gate.AwaitCapacity(context.Background(), StreamDead); err != nil {
		t.Errorf("q:dead: %v", err)
	}
	if len(fake.lens) != 0 {
		t.Errorf("q:dead не должен гейтироваться: %v", fake.lens)
	}

	if err := gate.AwaitCapacity(context.Background(), "q:typo"); err == nil ||
		!strings.Contains(err.Error(), "неизвестный стрим") {
		t.Errorf("чужой стрим: %v", err)
	}
	if len(fake.lens) != 0 {
		t.Errorf("чужой стрим не должен проверяться: %v", fake.lens)
	}
}

func TestBackpressureGateLogsWait(t *testing.T) {
	var buf bytes.Buffer
	fake := newFakeBackpressure(100, 50)
	gate := NewBackpressureGate(fake, BackpressureOptions{
		Watermark: 100,
		Interval:  time.Millisecond,
		Log:       slog.New(slog.NewTextHandler(&buf, nil)),
	})

	if err := gate.AwaitCapacity(context.Background(), StreamDNS); err != nil {
		t.Fatalf("AwaitCapacity: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, StreamDNS) || !strings.Contains(out, "100") {
		t.Errorf("запись о приостановке не оставлена или без контекста: %q", out)
	}
	if n := strings.Count(out, "приостановлена"); n != 1 {
		t.Errorf("записей о приостановке %d, хочу 1 (без спама на каждый повтор): %q", n, out)
	}
}

func TestWatermarkPublisherPublish(t *testing.T) {
	fake := newFakeBackpressure(99)
	fake.addID = "1718-0"
	pub := newTestPublisher(fake)

	id, err := pub.Publish(context.Background(), StreamDNS, dnsBatch())
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if id != "1718-0" {
		t.Errorf("Publish вернул %q, хочу %q", id, fake.addID)
	}
	want := []string{"xlen:" + StreamDNS, "xadd:" + StreamDNS}
	if !reflect.DeepEqual(fake.log, want) {
		t.Errorf("порядок команд: %v, хочу %v (ёмкость — затем публикация)", fake.log, want)
	}
	if len(fake.adds) != 1 || fake.adds[0].Stream != StreamDNS {
		t.Errorf("XADD: %+v", fake.adds)
	}
}

func TestWatermarkPublisherWaitsThenPublishes(t *testing.T) {
	fake := newFakeBackpressure(100, 3)
	fake.addID = "1719-0"
	pub := newTestPublisher(fake)

	if _, err := pub.Publish(context.Background(), StreamDNS, dnsBatch()); err != nil {
		t.Fatalf("Publish после ожидания: %v", err)
	}
	want := []string{"xlen:" + StreamDNS, "xlen:" + StreamDNS, "xadd:" + StreamDNS}
	if !reflect.DeepEqual(fake.log, want) {
		t.Errorf("порядок команд: %v, хочу %v (публикация только после ёмкости)", fake.log, want)
	}
}

func TestWatermarkPublisherChecksBeforeEveryBatch(t *testing.T) {
	// Пачечная публикация: перед каждым сообщением (для q:dns сообщение —
	// пачка, ТЗ 4.2) своя проверка ёмкости — «публикует новые задачи
	// только если длина очереди ниже порога» (ТЗ 5.3), а не один раз
	// перед серией.
	fake := newFakeBackpressure(7) // последняя повторяется
	fake.addID = "1720-0"
	pub := newTestPublisher(fake)

	want := make([]string, 0, 6)
	for i := 0; i < 3; i++ {
		if _, err := pub.Publish(context.Background(), StreamDNS, dnsBatch()); err != nil {
			t.Fatalf("Publish пачки №%d: %v", i+1, err)
		}
		want = append(want, "xlen:"+StreamDNS, "xadd:"+StreamDNS)
	}
	if !reflect.DeepEqual(fake.log, want) {
		t.Errorf("порядок команд:\n получил  %v\n хочу     %v", fake.log, want)
	}
	if len(fake.adds) != 3 {
		t.Errorf("XADD вызовов %d, хочу 3", len(fake.adds))
	}
}

func TestWatermarkPublisherDeadNotGated(t *testing.T) {
	fake := newFakeBackpressure(1000)
	fake.addID = "1721-0"
	pub := newTestPublisher(fake)

	msg := Dead{
		TaskID:   NewTaskID(),
		Stream:   StreamDNS,
		EntryID:  "1718-0",
		Attempts: 3,
		Payload:  json.RawMessage(`{"task_id":"t"}`),
	}
	if _, err := pub.Publish(context.Background(), StreamDead, msg); err != nil {
		t.Fatalf("Publish в q:dead: %v", err)
	}
	if len(fake.lens) != 0 {
		t.Errorf("q:dead не должен гейтироваться, XLEN: %v", fake.lens)
	}
	if len(fake.adds) != 1 {
		t.Errorf("XADD в q:dead: %+v", fake.adds)
	}
}

func TestWatermarkPublisherErrors(t *testing.T) {
	ctx := context.Background()

	// Ошибка гейта — публикация не доходит до XADD.
	fake := &fakeBackpressure{
		fakeRedis: &fakeRedis{},
		vals:      []int64{5},
		xlenErr:   errors.New("ERR connection refused"),
	}
	pub := newTestPublisher(fake)
	if _, err := pub.Publish(ctx, StreamDNS, dnsBatch()); err == nil ||
		!strings.Contains(err.Error(), "длина") {
		t.Errorf("ошибка гейта: %v", err)
	}
	if len(fake.adds) != 0 {
		t.Errorf("при ошибке гейта публикация не выполняется: %+v", fake.adds)
	}

	// Отмена ctx в гейте — чистый выход без публикации.
	fake = newFakeBackpressure(100)
	pub = newTestPublisher(fake)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := pub.Publish(canceled, StreamDNS, dnsBatch()); !errors.Is(err, context.Canceled) {
		t.Errorf("отмена ctx: %v", err)
	}
	if len(fake.adds) != 0 {
		t.Errorf("при отмене ctx публикация не выполняется: %+v", fake.adds)
	}

	// Ошибка самой публикации — после пройденного гейта.
	fake = &fakeBackpressure{
		fakeRedis: &fakeRedis{addErr: errors.New("connection refused")},
		vals:      []int64{5},
	}
	pub = newTestPublisher(fake)
	if _, err := pub.Publish(ctx, StreamDNS, dnsBatch()); err == nil {
		t.Error("ошибка XADD не возвращена")
	}

	// Невалидное сообщение — ошибка обёрнутой очереди (гейт пройден).
	fake = newFakeBackpressure(5)
	pub = newTestPublisher(fake)
	if _, err := pub.Publish(ctx, StreamDNS, DNSBatch{}); err == nil {
		t.Error("невалидное сообщение опубликовано")
	}
	if len(fake.adds) != 0 {
		t.Errorf("невалидное сообщение дошло до XADD: %+v", fake.adds)
	}

	// Обёртка без гейта — явная ошибка: контроль потока не теряется молча.
	fake = newFakeBackpressure(5)
	pub = NewWatermarkPublisher(NewRedisQueue(fake, RedisOptions{}), nil)
	if _, err := pub.Publish(ctx, StreamDNS, dnsBatch()); err == nil ||
		!strings.Contains(err.Error(), "гейт") {
		t.Errorf("обёртка без гейта: %v", err)
	}
}

func TestWatermarkPublisherPassthrough(t *testing.T) {
	// Consume и Ack продюсера не касаются: гейт открывается только
	// публикациям.
	fake := &fakeBackpressure{fakeRedis: &fakeRedis{readErr: redis.Nil}}
	pub := newTestPublisher(fake)

	got, err := pub.Consume(context.Background(), StreamDNS, "dns", "dns-1", 1)
	if err != nil || got != nil {
		t.Errorf("Consume: (%v, %v), хочу (nil, nil)", got, err)
	}
	if err := pub.Ack(context.Background(), StreamDNS, "dns", "1718-0"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if len(fake.lens) != 0 {
		t.Errorf("Consume/Ack не должны звать XLEN: %v", fake.lens)
	}
	if want := []ackCall{{StreamDNS, "dns", []string{"1718-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK: %+v, хочу %+v", fake.acks, want)
	}
}

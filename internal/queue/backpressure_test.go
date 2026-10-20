// Тесты backpressure-гейта (T-026) без живого Redis (стиль redisq_test.go):
// фейк расширяет fakeRedis окном глубины redisDepth — XINFO GROUPS (lag и
// pending групп) и XLEN (запас «групп нет»). Проверяются суммирование
// глубины (lag + pending, неопределённый lag, отсутствие групп → XLEN),
// границы порога (ниже/ровно/выше), ограничение порога половиной MAXLEN
// стрима, ожидание с повтором через интервал, пачечная публикация с
// проверкой перед каждым сообщением, отмена ctx, ошибки чтения, q:dead и
// отключённый гейт. Реальная семантика XINFO GROUPS — интеграционные
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

// fakeBackpressure — fakeRedis (redisq_test.go) плюс окно глубины:
// seq — ответы XINFO GROUPS по очереди проверок (последний повторяется;
// nil-элемент — групп нет: глубина читается XLEN, поле xlen), log —
// порядок обращений к Redis (глубина до xadd — сам предмет проверки).
type fakeBackpressure struct {
	*fakeRedis
	seq       [][]redis.XInfoGroup
	groupsErr error
	xlen      int64
	xlenErr   error

	infos []string // ключи XINFO GROUPS
	xlens []string // ключи XLEN
	log   []string // порядок команд: "xinfo:<стрим>" / "xlen:<стрим>" / "xadd:<стрим>"
}

func (f *fakeBackpressure) XInfoGroups(ctx context.Context, key string) *redis.XInfoGroupsCmd {
	f.infos = append(f.infos, key)
	f.log = append(f.log, "xinfo:"+key)
	groups := []redis.XInfoGroup{}
	if len(f.seq) > 0 {
		groups = f.seq[0]
		if len(f.seq) > 1 {
			f.seq = f.seq[1:]
		}
	}
	cmd := redis.NewXInfoGroupsCmd(ctx, key)
	cmd.SetVal(groups)
	cmd.SetErr(f.groupsErr)
	return cmd
}

func (f *fakeBackpressure) XLen(ctx context.Context, key string) *redis.IntCmd {
	f.xlens = append(f.xlens, key)
	f.log = append(f.log, "xlen:"+key)
	return redis.NewIntResult(f.xlen, f.xlenErr)
}

func (f *fakeBackpressure) XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd {
	f.log = append(f.log, "xadd:"+a.Stream)
	return f.fakeRedis.XAdd(ctx, a)
}

// Компиляционные проверки: фейк взаимозаменяем с реальным клиентом
// в окне глубины и в окне очереди, обёртка — полноценная Queue.
var (
	_ redisDepth   = (*redis.Client)(nil)
	_ redisDepth   = (*fakeBackpressure)(nil)
	_ redisStreams = (*fakeBackpressure)(nil)
	_ Queue        = (*WatermarkPublisher)(nil)
)

// grp — ответ XINFO GROUPS с одной группой (lag, pending).
func grp(lag, pending int64) []redis.XInfoGroup {
	return []redis.XInfoGroup{{Name: "g", Lag: lag, Pending: pending}}
}

// newFakeBackpressure — фейк с инициализированной очередью и
// заготовленной последовательностью ответов глубины (последний
// повторяется; nil — групп нет).
func newFakeBackpressure(seq ...[]redis.XInfoGroup) *fakeBackpressure {
	return &fakeBackpressure{fakeRedis: &fakeRedis{}, seq: seq}
}

// newTestGate — гейт над фейком: порог watermark, интервал повтора 1 мс.
func newTestGate(f *fakeBackpressure, watermark int64) *BackpressureGate {
	return NewBackpressureGate(f, BackpressureOptions{Watermark: watermark, Interval: time.Millisecond})
}

// newTestPublisher — обёртка RedisQueue над тем же фейком: гейт с порогом
// 100 против глубин из f.seq.
func newTestPublisher(f *fakeBackpressure) *WatermarkPublisher {
	return NewWatermarkPublisher(NewRedisQueue(f, RedisOptions{}), newTestGate(f, 100))
}

// dnsBatch — валидное сообщение q:dns на один домен.
func dnsBatch() DNSBatch {
	return DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: "a.test"}}}
}

// TestNewBackpressureGateIntervalDefault — интервал повтора: 0
// и отрицательные — DefaultBackpressureInterval, положительное
// наносекундное ожидание валидно и не заменяется.
func TestNewBackpressureGateIntervalDefault(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		g := NewBackpressureGate(nil, BackpressureOptions{Watermark: 1, Interval: interval})
		if g.opt.Interval != DefaultBackpressureInterval {
			t.Errorf("Interval %s не заменён дефолтом: %s", interval, g.opt.Interval)
		}
	}
	g := NewBackpressureGate(nil, BackpressureOptions{Watermark: 1, Interval: time.Nanosecond})
	if g.opt.Interval != time.Nanosecond {
		t.Errorf("Interval 1нс заменён дефолтом: %s, хочу 1нс (положительный интервал валиден)", g.opt.Interval)
	}
}

// TestBackpressureStreamMaxLen — предел стрима: 0 и отрицательные
// MaxLen — политика DefaultStreamMaxLen, явный предел 1 — валиден
// и не подменяется политикой.
func TestBackpressureStreamMaxLen(t *testing.T) {
	g := NewBackpressureGate(nil, BackpressureOptions{})
	if got := g.streamMaxLen(StreamDNS); got != 10_000 {
		t.Errorf("streamMaxLen без MaxLen = %d, хочу 10000 (политика q:dns)", got)
	}
	if got := g.streamMaxLen(StreamHTTP); got != 1_000_000 {
		t.Errorf("streamMaxLen без MaxLen = %d, хочу 1000000 (политика штучных стримов)", got)
	}
	g = NewBackpressureGate(nil, BackpressureOptions{MaxLen: 1})
	if got := g.streamMaxLen(StreamHTTP); got != 1 {
		t.Errorf("streamMaxLen при MaxLen=1 = %d, хочу 1 (явный предел валиден)", got)
	}
	g = NewBackpressureGate(nil, BackpressureOptions{MaxLen: -7})
	if got := g.streamMaxLen(StreamHTTP); got != 1_000_000 {
		t.Errorf("streamMaxLen при отрицательном MaxLen = %d, хочу 1000000 (политика)", got)
	}
}

// TestStreamWatermark — эффективный порог: ≤0 отключает гейт (0),
// maxLen ≤ 0 — предел политики стрима, порог — минимум watermark
// и половины предела.
func TestStreamWatermark(t *testing.T) {
	cases := []struct {
		name      string
		watermark int64
		maxLen    int64
		stream    string
		want      int64
	}{
		{"нулевый watermark отключает", 0, 1000, StreamHTTP, 0},
		{"отрицательный watermark отключает", -5, 1000, StreamHTTP, 0},
		{"предел политики q:dns", 100_000, 0, StreamDNS, 5_000},
		{"предел политики штучных", 600_000, 0, StreamHTTP, 500_000},
		{"явный предел 1 — порог 0", 100_000, 1, StreamDNS, 0},
		{"минимум — watermark", 7, 100, StreamHTTP, 7},
		{"минимум — половина предела", 200, 100, StreamHTTP, 50},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := StreamWatermark(c.watermark, c.maxLen, c.stream); got != c.want {
				t.Errorf("StreamWatermark(%d, %d, %s) = %d, хочу %d",
					c.watermark, c.maxLen, c.stream, got, c.want)
			}
		})
	}
}

func TestBackpressureLen(t *testing.T) {
	// Глубина группы — lag плюс неподтверждённые; групп несколько —
	// суммируются (у рабочих стримов группа одна, обобщённо — все).
	fake := newFakeBackpressure(
		[]redis.XInfoGroup{{Name: "a", Lag: 30, Pending: 12}, {Name: "b", Lag: 7, Pending: 3}},
	)
	gate := NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})

	n, err := gate.Len(context.Background(), StreamHTTP)
	if err != nil {
		t.Fatalf("Len: %v", err)
	}
	if n != 52 {
		t.Errorf("Len = %d, хочу 52 (lag 30+7 + pending 12+3)", n)
	}
	if want := []string{StreamHTTP}; !reflect.DeepEqual(fake.infos, want) {
		t.Errorf("XINFO GROUPS ключи: %v, хочу %v", fake.infos, want)
	}
	if len(fake.xlens) != 0 {
		t.Errorf("XLEN не нужен при живых группах: %v", fake.xlens)
	}

	// lag неопределён (−1: обрезка срезала недоставленное) — считается
	// только pending.
	fake = newFakeBackpressure(grp(-1, 8))
	gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(context.Background(), StreamHTTP); err != nil || n != 8 {
		t.Errorf("Len при неопределённом lag: (%d, %v), хочу (8, nil)", n, err)
	}

	// Групп нет — ни одна запись не доставлена: глубина равна XLEN.
	fake = newFakeBackpressure(nil)
	fake.xlen = 34
	gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(context.Background(), StreamHTTP); err != nil || n != 34 {
		t.Errorf("Len без групп: (%d, %v), хочу XLEN (34, nil)", n, err)
	}
	if want := []string{StreamHTTP}; !reflect.DeepEqual(fake.xlens, want) {
		t.Errorf("XLEN ключи: %v, хочу %v", fake.xlens, want)
	}

	// Стрима ещё нет (redis.Nil / «no such key») — групп нет: глубина
	// без доставок равна XLEN, значение XINFO GROUPS при ошибке
	// не используется.
	for name, groupsErr := range map[string]error{
		"redis.Nil":   redis.Nil,
		"no such key": errors.New("ERR no such key"),
	} {
		fake = &fakeBackpressure{
			fakeRedis: &fakeRedis{},
			seq:       [][]redis.XInfoGroup{grp(5, 7)}, // val при err не используется
			groupsErr: groupsErr,
			xlen:      34,
		}
		gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
		n, err := gate.Len(context.Background(), StreamHTTP)
		if err != nil || n != 34 {
			t.Errorf("Len несуществующего стрима (%s): (%d, %v), хочу XLEN (34, nil)", name, n, err)
		}
		if want := []string{StreamHTTP}; !reflect.DeepEqual(fake.xlens, want) {
			t.Errorf("Len несуществующего стрима (%s): XLEN ключи %v, хочу %v", name, fake.xlens, want)
		}
	}

	// Ошибки — нулевая глубина и точный текст.
	if n, err := gate.Len(context.Background(), "q:typo"); err == nil || n != 0 ||
		!strings.Contains(err.Error(), "неизвестный стрим") {
		t.Errorf("Len чужого стрима: (%d, %v), хочу (0, неизвестный стрим)", n, err)
	}

	gate = NewBackpressureGate(nil, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(context.Background(), StreamDNS); err == nil || n != 0 ||
		!strings.Contains(err.Error(), "клиент Redis не задан") {
		t.Errorf("Len без клиента: (%d, %v), хочу (0, клиент не задан)", n, err)
	}

	fake = &fakeBackpressure{
		fakeRedis: &fakeRedis{},
		groupsErr: errors.New("ERR connection refused"),
	}
	gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(context.Background(), StreamDNS); err == nil || n != 0 ||
		!strings.Contains(err.Error(), "глубина") {
		t.Errorf("Len при недоступном Redis: (%d, %v), хочу (0, ошибка глубины)", n, err)
	}

	// Ошибка XLEN при отсутствии групп возвращается (гейт не гадает
	// о глубине несуществующего ключа).
	fake = newFakeBackpressure(nil)
	fake.xlenErr = errors.New("ERR connection refused")
	gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(context.Background(), StreamDNS); err == nil || n != 0 ||
		!strings.Contains(err.Error(), "групп нет") {
		t.Errorf("Len с ошибкой XLEN: (%d, %v), хочу (0, ошибка XLEN)", n, err)
	}
}

// TestBackpressureWatermarkCappedByMaxLen — эффективный порог стрима не
// выше половины его предела MAXLEN: гейт обязан срабатывать до обрезки.
// Для q:dns настроенные 100 000 записей недостижимы (MAXLEN 10 000) —
// порог ограничивается 5 000, гейт становится рабочим.
func TestBackpressureWatermarkCappedByMaxLen(t *testing.T) {
	// q:dns: MAXLEN по политике 10 000 → порог 5 000.
	fake := newFakeBackpressure(grp(4999, 0))
	gate := newTestGate(fake, 100000)
	if err := gate.AwaitCapacity(context.Background(), StreamDNS); err != nil {
		t.Fatalf("AwaitCapacity ниже ограниченного порога: %v", err)
	}

	fake = newFakeBackpressure(grp(5000, 0), grp(4999, 0))
	gate = newTestGate(fake, 100000)
	if err := gate.AwaitCapacity(context.Background(), StreamDNS); err != nil {
		t.Fatalf("AwaitCapacity q:dns: %v", err)
	}
	if len(fake.infos) != 2 {
		t.Errorf("проверок %d, хочу 2 (5 000 ≥ порог 5 000 — гейт закрыт): %v", len(fake.infos), fake.infos)
	}

	// Штучный стрим: MAXLEN 1 000 000, половина выше порога — порог
	// не ограничивается (100 000).
	fake = newFakeBackpressure(grp(99999, 0))
	gate = newTestGate(fake, 100000)
	if err := gate.AwaitCapacity(context.Background(), StreamHTTP); err != nil {
		t.Fatalf("AwaitCapacity ниже порога 100 000: %v", err)
	}

	// Явный предел из опций (redis.maxlen, Q-03) ограничивает и его.
	fake = newFakeBackpressure(grp(2000, 0), grp(1999, 0))
	gate = NewBackpressureGate(fake, BackpressureOptions{
		Watermark: 100000, MaxLen: 4000, Interval: time.Millisecond,
	})
	if err := gate.AwaitCapacity(context.Background(), StreamHTTP); err != nil {
		t.Fatalf("AwaitCapacity с явным MaxLen: %v", err)
	}
	if len(fake.infos) != 2 {
		t.Errorf("проверок %d, хочу 2 (порог ограничен 2 000): %v", len(fake.infos), fake.infos)
	}
}

func TestBackpressureGateBelowWatermark(t *testing.T) {
	fake := newFakeBackpressure(grp(99, 0))
	gate := newTestGate(fake, 100)

	if err := gate.AwaitCapacity(context.Background(), StreamDNS); err != nil {
		t.Fatalf("AwaitCapacity ниже порога: %v", err)
	}
	if want := []string{StreamDNS}; !reflect.DeepEqual(fake.infos, want) {
		t.Errorf("XINFO GROUPS: %d вызовов (%v), хочу 1 — ниже порога проходит сразу", len(fake.infos), fake.infos)
	}
}

func TestBackpressureGateAtWatermark(t *testing.T) {
	// depth == watermark — «строго ниже порога» не выполнено: гейт закрыт,
	// публикация ждёт, пока глубина не опустится (99 < 100).
	fake := newFakeBackpressure(grp(100, 0), grp(100, 0), grp(99, 0))
	gate := newTestGate(fake, 100)

	if err := gate.AwaitCapacity(context.Background(), StreamDNS); err != nil {
		t.Fatalf("AwaitCapacity на пороге: %v", err)
	}
	if len(fake.infos) != 3 {
		t.Errorf("проверок %d, хочу 3 (два отказа, третья ниже порога): %v", len(fake.infos), fake.infos)
	}
}

func TestBackpressureGateAboveWatermark(t *testing.T) {
	fake := newFakeBackpressure(grp(150, 0), grp(7, 0))
	gate := newTestGate(fake, 100)

	if err := gate.AwaitCapacity(context.Background(), StreamRender); err != nil {
		t.Fatalf("AwaitCapacity выше порога: %v", err)
	}
	if len(fake.infos) != 2 {
		t.Errorf("проверок %d, хочу 2 (ожидание и повтор): %v", len(fake.infos), fake.infos)
	}
}

func TestBackpressureGateRetriesOverInterval(t *testing.T) {
	// Гейт закрыт (5 ≥ 1 всё время): повторные проверки идут с интервалом;
	// ctx истекает в ожидании — ошибка контекста. За 100 мс при интервале
	// 40 мс обязано случиться не меньше двух проверок.
	fake := newFakeBackpressure(grp(5, 0))
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
	if len(fake.infos) < 2 {
		t.Errorf("проверок %d, хочу ≥ 2 (повторы через интервал): %v", len(fake.infos), fake.infos)
	}
}

func TestBackpressureGateCanceledContext(t *testing.T) {
	// Отмена до вызова: первая проверка, затем выход по ctx без ожидания
	// интервала (чистый выход, ТЗ 3.4).
	fake := newFakeBackpressure(grp(100, 0))
	gate := NewBackpressureGate(fake, BackpressureOptions{Watermark: 10, Interval: time.Minute})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := gate.AwaitCapacity(ctx, StreamDNS)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("ошибка: %v, хочу context.Canceled", err)
	}
	if len(fake.infos) != 1 {
		t.Errorf("проверок %d, хочу 1", len(fake.infos))
	}
}

func TestBackpressureGateDepthError(t *testing.T) {
	fake := &fakeBackpressure{
		fakeRedis: &fakeRedis{},
		groupsErr: errors.New("ERR connection refused"),
	}
	gate := newTestGate(fake, 10)

	if err := gate.AwaitCapacity(context.Background(), StreamDNS); err == nil ||
		!strings.Contains(err.Error(), "глубина") {
		t.Errorf("ошибка чтения глубины не возвращена: %v", err)
	}
	if len(fake.infos) != 1 {
		t.Errorf("проверок %d, хочу 1 — ошибку гейт не ретраит", len(fake.infos))
	}
}

func TestBackpressureGateUngated(t *testing.T) {
	// Отключённый гейт, q:dead и чужой стрим — без обращения к Redis.
	fake := newFakeBackpressure(grp(1000, 0))
	disabled := NewBackpressureGate(fake, BackpressureOptions{Watermark: 0})
	if err := disabled.AwaitCapacity(context.Background(), StreamDNS); err != nil {
		t.Errorf("отключённый гейт: %v", err)
	}
	if len(fake.infos)+len(fake.xlens) != 0 {
		t.Errorf("отключённый гейт полез в Redis: %v %v", fake.infos, fake.xlens)
	}

	gate := newTestGate(fake, 10)
	if err := gate.AwaitCapacity(context.Background(), StreamDead); err != nil {
		t.Errorf("q:dead: %v", err)
	}
	if len(fake.infos)+len(fake.xlens) != 0 {
		t.Errorf("q:dead не должен гейтироваться: %v %v", fake.infos, fake.xlens)
	}

	if err := gate.AwaitCapacity(context.Background(), "q:typo"); err == nil ||
		!strings.Contains(err.Error(), "неизвестный стрим") {
		t.Errorf("чужой стрим: %v", err)
	}
	if len(fake.infos)+len(fake.xlens) != 0 {
		t.Errorf("чужой стрим не должен проверяться: %v %v", fake.infos, fake.xlens)
	}
}

func TestBackpressureGateLogsWait(t *testing.T) {
	// Два закрытых прохода (100 и 100 на пороге 100) — запись о
	// приостановке одна: лог не спамится на каждый повтор.
	var buf bytes.Buffer
	fake := newFakeBackpressure(grp(100, 0), grp(100, 0), grp(50, 0))
	gate := NewBackpressureGate(fake, BackpressureOptions{
		Watermark: 100,
		Interval:  time.Millisecond,
		Log:       slog.New(slog.NewTextHandler(&buf, nil)),
	})

	if err := gate.AwaitCapacity(context.Background(), StreamDNS); err != nil {
		t.Fatalf("AwaitCapacity: %v", err)
	}
	if len(fake.infos) != 3 {
		t.Fatalf("проверок %d, хочу 3 (два отказа, третья ниже порога): %v", len(fake.infos), fake.infos)
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
	fake := newFakeBackpressure(grp(99, 0))
	fake.addID = "1718-0"
	pub := newTestPublisher(fake)

	id, err := pub.Publish(context.Background(), StreamDNS, dnsBatch())
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if id != "1718-0" {
		t.Errorf("Publish вернул %q, хочу %q", id, fake.addID)
	}
	want := []string{"xinfo:" + StreamDNS, "xadd:" + StreamDNS}
	if !reflect.DeepEqual(fake.log, want) {
		t.Errorf("порядок команд: %v, хочу %v (ёмкость — затем публикация)", fake.log, want)
	}
	if len(fake.adds) != 1 || fake.adds[0].Stream != StreamDNS {
		t.Errorf("XADD: %+v", fake.adds)
	}
}

func TestWatermarkPublisherWaitsThenPublishes(t *testing.T) {
	fake := newFakeBackpressure(grp(100, 0), grp(3, 0))
	fake.addID = "1719-0"
	pub := newTestPublisher(fake)

	if _, err := pub.Publish(context.Background(), StreamDNS, dnsBatch()); err != nil {
		t.Fatalf("Publish после ожидания: %v", err)
	}
	want := []string{"xinfo:" + StreamDNS, "xinfo:" + StreamDNS, "xadd:" + StreamDNS}
	if !reflect.DeepEqual(fake.log, want) {
		t.Errorf("порядок команд: %v, хочу %v (публикация только после ёмкости)", fake.log, want)
	}
}

func TestWatermarkPublisherChecksBeforeEveryBatch(t *testing.T) {
	// Пачечная публикация: перед каждым сообщением (для q:dns сообщение —
	// пачка, ТЗ 4.2) своя проверка ёмкости — «публикует новые задачи
	// только если длина очереди ниже порога» (ТЗ 5.3), а не один раз
	// перед серией.
	fake := newFakeBackpressure(grp(7, 0)) // последний ответ повторяется
	fake.addID = "1720-0"
	pub := newTestPublisher(fake)

	want := make([]string, 0, 6)
	for i := 0; i < 3; i++ {
		if _, err := pub.Publish(context.Background(), StreamDNS, dnsBatch()); err != nil {
			t.Fatalf("Publish пачки №%d: %v", i+1, err)
		}
		want = append(want, "xinfo:"+StreamDNS, "xadd:"+StreamDNS)
	}
	if !reflect.DeepEqual(fake.log, want) {
		t.Errorf("порядок команд:\n получил  %v\n хочу     %v", fake.log, want)
	}
	if len(fake.adds) != 3 {
		t.Errorf("XADD вызовов %d, хочу 3", len(fake.adds))
	}
}

func TestWatermarkPublisherDeadNotGated(t *testing.T) {
	fake := newFakeBackpressure(grp(1000, 0))
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
	if len(fake.infos)+len(fake.xlens) != 0 {
		t.Errorf("q:dead не должен гейтироваться: %v %v", fake.infos, fake.xlens)
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
		groupsErr: errors.New("ERR connection refused"),
	}
	pub := newTestPublisher(fake)
	if _, err := pub.Publish(ctx, StreamDNS, dnsBatch()); err == nil ||
		!strings.Contains(err.Error(), "глубина") {
		t.Errorf("ошибка гейта: %v", err)
	}
	if len(fake.adds) != 0 {
		t.Errorf("при ошибке гейта публикация не выполняется: %+v", fake.adds)
	}

	// Отмена ctx в гейте — чистый выход без публикации.
	fake = newFakeBackpressure(grp(100, 0))
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
		seq:       [][]redis.XInfoGroup{grp(5, 0)},
	}
	pub = newTestPublisher(fake)
	if _, err := pub.Publish(ctx, StreamDNS, dnsBatch()); err == nil {
		t.Error("ошибка XADD не возвращена")
	}

	// Невалидное сообщение — ошибка обёрнутой очереди (гейт пройден).
	fake = newFakeBackpressure(grp(5, 0))
	pub = newTestPublisher(fake)
	if _, err := pub.Publish(ctx, StreamDNS, DNSBatch{}); err == nil {
		t.Error("невалидное сообщение опубликовано")
	}
	if len(fake.adds) != 0 {
		t.Errorf("невалидное сообщение дошло до XADD: %+v", fake.adds)
	}

	// Обёртка без гейта — явная ошибка: контроль потока не теряется молча.
	fake = newFakeBackpressure(grp(5, 0))
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
	if len(fake.infos)+len(fake.xlens) != 0 {
		t.Errorf("Consume/Ack не должны мерить глубину: %v %v", fake.infos, fake.xlens)
	}
	if want := []ackCall{{StreamDNS, "dns", []string{"1718-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK: %+v, хочу %+v", fake.acks, want)
	}
}

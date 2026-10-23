// Тесты надёжности доставки (T-025) без живого Redis: фейк расширяет окно
// команд redisq_test.go командами claimStreams — XAUTOCLAIM и XPENDING
// EXTENDED. У этих команд go-redis не имеет фабрик New*Result, поэтому фейк
// строит команды redis.NewXAutoClaimCmd/NewXPendingExtCmd и заполняет их
// SetVal/SetErr. Проверяются аргументы команд, счётчик попыток, перевод в
// q:dead (формат Dead), порядок «обработка → БД → XACK». Реальная
// семантика Redis — интеграционные тесты T-032.

package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// fakeClaimRedis — фейк redisq_test.go плюс команды возврата зависших.
type fakeClaimRedis struct {
	fakeRedis

	claims    []redis.XAutoClaimArgs
	claimMsgs []redis.XMessage
	claimNext string // курсор следующего сканирования из ответа
	claimErr  error

	pends      []redis.XPendingExtArgs
	pendRes    map[string][]redis.XPendingExt // ответ XPENDING по Start-идентификатору
	pendErr    error
	pendOneErr map[string]error // ошибка XPENDING по конкретному Start-идентификатору
}

func (f *fakeClaimRedis) XAutoClaim(ctx context.Context, a *redis.XAutoClaimArgs) *redis.XAutoClaimCmd {
	f.claims = append(f.claims, *a)
	msgs := f.claimMsgs
	if f.claimErr == nil && len(msgs) > 0 {
		// Живой клейм возвращает до Count записей и закрепляет их
		// (idle сброшен — повторный вызов не вернёт); остаток PEL —
		// следующим вызовам.
		if int64(len(msgs)) > a.Count {
			msgs = msgs[:a.Count]
			f.claimMsgs = f.claimMsgs[a.Count:]
		} else {
			f.claimMsgs = nil
		}
	}
	cmd := redis.NewXAutoClaimCmd(ctx, "xautoclaim")
	cmd.SetVal(msgs, f.claimNext)
	cmd.SetErr(f.claimErr)
	return cmd
}

func (f *fakeClaimRedis) XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	f.pends = append(f.pends, *a)
	cmd := redis.NewXPendingExtCmd(ctx, "xpending")
	if err, ok := f.pendOneErr[a.Start]; ok {
		cmd.SetVal(nil)
		cmd.SetErr(err)
		return cmd
	}
	cmd.SetVal(f.pendRes[a.Start])
	cmd.SetErr(f.pendErr)
	return cmd
}

// Компиляционные проверки: расширенный фейк взаимозаменяем с реальным
// клиентом, Reliab собирается из клиента и Queue.
var (
	_ claimStreams = (*fakeClaimRedis)(nil)
	_ claimStreams = (*redis.Client)(nil)
)

// newTestReliab — связка как в бою: очередь и слой надёжности на одном
// фейковом клиенте.
func newTestReliab(fake *fakeClaimRedis, opt ReliabOptions) *Reliab {
	return NewReliab(fake, NewRedisQueue(fake, RedisOptions{}), opt)
}

func TestNewReliabDefaults(t *testing.T) {
	if DefaultMaxAttempts != 3 {
		t.Errorf("DefaultMaxAttempts = %d, хочу 3 (ТЗ 4.2; литерал — пин константы)", DefaultMaxAttempts)
	}
	if DefaultClaimMinIdle != 10*time.Minute {
		t.Errorf("DefaultClaimMinIdle = %s, хочу 10m (литерал — пин константы)", DefaultClaimMinIdle)
	}
	r := newTestReliab(&fakeClaimRedis{}, ReliabOptions{})
	if r.opt.MaxAttempts != DefaultMaxAttempts {
		t.Errorf("MaxAttempts: %d, хочу %d", r.opt.MaxAttempts, DefaultMaxAttempts)
	}
	if r.opt.MinIdle != DefaultClaimMinIdle {
		t.Errorf("MinIdle: %s, хочу %s", r.opt.MinIdle, DefaultClaimMinIdle)
	}
	// Единичные значения валидны и не заменяются дефолтами.
	r = newTestReliab(&fakeClaimRedis{}, ReliabOptions{MaxAttempts: 1, MinIdle: time.Nanosecond})
	if r.opt.MaxAttempts != 1 {
		t.Errorf("MaxAttempts = %d, хочу 1 (одна попытка — валидная настройка)", r.opt.MaxAttempts)
	}
	if r.opt.MinIdle != time.Nanosecond {
		t.Errorf("MinIdle = %s, хочу 1нс (положительный простой валиден)", r.opt.MinIdle)
	}
}

// TestStreamPolicyFitsBudget — инвариант бюджета стримов (T-141, ТЗ 3.2):
// сумма аварийных потолков политики MAXLEN обязана укладываться в бюджет
// Redis ≤ 512 МиБ (с запасом на служебную память) даже при большой
// пачке q:dns — при noeviction потолок выше бюджета означал бы отказ
// записи (OOM) раньше обрезки, как в инциденте T-093. Литерал пачки
// здесь — верх нормативной вилки ТЗ 5.3; связка с фактической границей
// валидации конфигурации — пин-тестом
// config.TestSchedulerBatchFitsStreamBudget (config_test.go), который
// считает от maxSchedulerBatchSize. Дефолт (пачка 1000): q:dns
// 250 × 1000 × 66 Б ≈ 16 МиБ + 3 × 103 МиБ + 2 МиБ ≈ 327 МиБ; худший
// случай (пачка 10 000) ≈ 467 МиБ — оба ниже предела 480 МиБ.
func TestStreamPolicyFitsBudget(t *testing.T) {
	const (
		worstBatch  = 10_000 // верх нормативной вилки ТЗ 5.3
		streamLimit = 480 << 20
	)
	total := DefaultStreamMaxLen(StreamDNS)*worstBatch*DNSStreamBytesPerDomain +
		3*DefaultStreamMaxLen(StreamHTTP)*UnitStreamBytesWorst +
		DefaultStreamMaxLen(StreamDead)*DeadStreamBytesWorst
	if total > streamLimit {
		t.Errorf("сумма потолков стримов при пачке %d ≈ %d МиБ > %d МиБ — бюджет Redis 512 МиБ (ТЗ 3.2) пробит: пересогласуйте defaultMaxLen* / scheduler.batch_size (docs/redis.md §3.1)",
			worstBatch, total>>20, streamLimit>>20)
	}
	// Дефолтная пачка — на порядок свободнее: проконтролировать запас.
	def := DefaultStreamMaxLen(StreamDNS)*1000*DNSStreamBytesPerDomain +
		3*DefaultStreamMaxLen(StreamHTTP)*UnitStreamBytesWorst +
		DefaultStreamMaxLen(StreamDead)*DeadStreamBytesWorst
	if def > 400<<20 {
		t.Errorf("сумма потолков при дефолтной пачке ≈ %d МиБ — съедает запас бюджета (межобрезочный остаток TrimLoop, служебная память)", def>>20)
	}
}

// TestReliabClaim — аргументы XAUTOCLAIM и XPENDING, возврат записей с их
// счётчиками попыток.
func TestReliabClaim(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: []byte(`{"task_id":"t1"}`)}},
			{ID: "1719-0", Values: map[string]any{fieldData: `{"task_id":"t2"}`}},
		},
		claimNext: "1720-0",
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", Consumer: "dead-1", RetryCount: 2}},
			"1719-0": {{ID: "1719-0", Consumer: "dead-1", RetryCount: 3}}, // последняя попытка — ещё в работу
		},
	}
	r := newTestReliab(fake, ReliabOptions{})

	got, next, err := r.Claim(context.Background(), StreamHTTP, "http", "recover-1", 15*time.Minute, 10, "0-0")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if next != "1720-0" {
		t.Errorf("курсор следующего сканирования = %q, хочу %q (из ответа XAUTOCLAIM)", next, "1720-0")
	}

	// XAUTOCLAIM: тот же стрим/группа, закрепление за восстановителем,
	// фильтр по простою, сканирование PEL с начала, лимит пачки.
	if len(fake.claims) != 1 {
		t.Fatalf("вызовов XAUTOCLAIM: %d, хочу 1", len(fake.claims))
	}
	args := fake.claims[0]
	if args.Stream != StreamHTTP || args.Group != "http" || args.Consumer != "recover-1" {
		t.Errorf("XAUTOCLAIM стрим/группа/потребитель: %q/%q/%q", args.Stream, args.Group, args.Consumer)
	}
	if args.MinIdle != 15*time.Minute {
		t.Errorf("XAUTOCLAIM MinIdle: %s, хочу 15m", args.MinIdle)
	}
	if args.Start != "0-0" || args.Count != 10 {
		t.Errorf("XAUTOCLAIM Start/Count: %q/%d, хочу 0-0/10", args.Start, args.Count)
	}

	// Явный курсор передаётся в XAUTOCLAIM без искажений (Q-07):
	// вызывающий цикл продолжает обход PEL с сохранённой позиции.
	if _, _, err := r.Claim(context.Background(), StreamHTTP, "http", "recover-1", 15*time.Minute, 10, "1750-3"); err != nil {
		t.Fatalf("Claim с курсором: %v", err)
	}
	if got := fake.claims[1].Start; got != "1750-3" {
		t.Errorf("XAUTOCLAIM Start с курсором: %q, хочу 1750-3", got)
	}

	// Счётчик попыток — XPENDING EXTENDED по каждой записи (ровно её
	// диапазон), поле RetryCount (deliveries-счётчик PEL).
	if len(fake.pends) != 2 {
		t.Fatalf("вызовов XPENDING: %d, хочу 2", len(fake.pends))
	}
	for i, id := range []string{"1718-0", "1719-0"} {
		p := fake.pends[i]
		if p.Stream != StreamHTTP || p.Group != "http" {
			t.Errorf("XPENDING №%d стрим/группа: %q/%q", i+1, p.Stream, p.Group)
		}
		if p.Start != id || p.End != id || p.Count != 1 {
			t.Errorf("XPENDING №%d диапазон: %q..%q count=%d, хочу ровно %q", i+1, p.Start, p.End, p.Count, id)
		}
	}

	want := []Claimed{
		{Delivery{StreamHTTP, "1718-0", []byte(`{"task_id":"t1"}`)}, 2},
		{Delivery{StreamHTTP, "1719-0", []byte(`{"task_id":"t2"}`)}, 3},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Claim:\n получил  %+v\n хочу     %+v", got, want)
	}

	// Обработанные записи не трогают q:dead и не подтверждаются клеймом:
	// XACK — забота Processing после записи результата в БД.
	if len(fake.adds) != 0 || len(fake.acks) != 0 {
		t.Errorf("Claim не публикует и не подтверждает рабочие записи: %+v %+v", fake.adds, fake.acks)
	}
}

// TestReliabClaimMinIdleDefault — minIdle 0 заменяется опцией, опция 0 —
// значением по умолчанию; пустой старт нормализуется в "0-0", явный
// минимальный minIdle не заменяется.
func TestReliabClaimMinIdleDefault(t *testing.T) {
	fake := &fakeClaimRedis{}
	r := newTestReliab(fake, ReliabOptions{})
	if _, _, err := r.Claim(context.Background(), StreamDNS, "dns", "recover-1", 0, 1, ""); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got := fake.claims[0].MinIdle; got != DefaultClaimMinIdle {
		t.Errorf("MinIdle: %s, хочу %s", got, DefaultClaimMinIdle)
	}
	if got := fake.claims[0].Start; got != "0-0" {
		t.Errorf("Start: %q, хочу 0-0 (пустой старт — с начала PEL)", got)
	}

	fake = &fakeClaimRedis{}
	r = newTestReliab(fake, ReliabOptions{MinIdle: 2 * time.Minute})
	if _, _, err := r.Claim(context.Background(), StreamDNS, "dns", "recover-1", 0, 1, ""); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got := fake.claims[0].MinIdle; got != 2*time.Minute {
		t.Errorf("MinIdle: %s, хочу 2m из опции", got)
	}

	// Явный minIdle 1нс валиден и не подменяется опцией слоя.
	fake = &fakeClaimRedis{}
	r = newTestReliab(fake, ReliabOptions{MinIdle: time.Hour})
	if _, _, err := r.Claim(context.Background(), StreamDNS, "dns", "recover-1", time.Nanosecond, 1, ""); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got := fake.claims[0].MinIdle; got != time.Nanosecond {
		t.Errorf("MinIdle: %s, хочу 1нс (положительный явный minIdle валиден)", got)
	}
}

// TestReliabClaimDead — запись, чей счётчик выше лимита, не возвращается,
// а переносится в q:dead (Dead с идентификаторами предмета задачи,
// выжатыми из тела, без payload — T-105) и подтверждается в исходном
// стриме.
func TestReliabClaimDead(t *testing.T) {
	origPayload := `{"task_id":"t1","zone_id":3,"domain_id":42,"name":"example.com"}`
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: []byte(origPayload)}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", Consumer: "old", RetryCount: 4}}, // лимит 3 исчерпан
		},
	}
	r := newTestReliab(fake, ReliabOptions{})

	got, _, err := r.Claim(context.Background(), StreamRender, "render", "recover-1", time.Minute, 5, "0-0")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got != nil {
		t.Errorf("исчерпавшая попытки запись возвращена в работу: %+v", got)
	}

	// Dead публикуется в q:dead до XACK исходной записи.
	if len(fake.adds) != 1 {
		t.Fatalf("публикаций в q:dead: %d, хочу 1", len(fake.adds))
	}
	add := fake.adds[0]
	if add.Stream != StreamDead || add.MaxLen != DefaultStreamMaxLen(StreamDead) || !add.Approx {
		t.Errorf("XADD: стрим %q MaxLen=%d Approx=%v, хочу %q MAXLEN ~ %d",
			add.Stream, add.MaxLen, add.Approx, StreamDead, DefaultStreamMaxLen(StreamDead))
	}
	data := add.Values.(map[string]any)[fieldData].([]byte)
	dead, err := Decode[Dead](Delivery{Stream: StreamDead, ID: add.ID, Payload: data})
	if err != nil {
		t.Fatalf("поле data не декодируется как Dead: %v\n%s", err, data)
	}
	wantDead := Dead{
		TaskID:   "t1",
		Stream:   StreamRender,
		EntryID:  "1718-0",
		Attempts: 4,
		Reason:   "исчерпан лимит попыток 3: доставок без подтверждения — 4",
		ZoneID:   3,
		DomainID: 42,
		Name:     "example.com",
	}
	if !reflect.DeepEqual(dead, wantDead) {
		t.Errorf("Dead:\n получил  %+v\n хочу     %+v", dead, wantDead)
	}

	// Исходная запись покидает pending: XACK из стрима-источника.
	if want := []ackCall{{StreamRender, "render", []string{"1718-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK: %+v, хочу %+v", fake.acks, want)
	}
}

// TestReliabClaimDeadByteGuard (T-105) — запись q:dead не раздувается
// payload исходной задачи: пачка q:dns (~250 КБ worst-case) переносится
// идентификаторами (task_id, стрим, запись, размер пачки), байты записи —
// сотни, а не сотни тысяч. Потолок 10 000 записей с payload пачек давал
// worst-case ~2,5 ГБ против бюджета Redis 0,5 ГБ (docs/redis.md §3);
// идентификаторов хватает оператору для разбора (docs/runbooks.md §2).
func TestReliabClaimDeadByteGuard(t *testing.T) {
	big := DNSBatch{TaskID: "big", Domains: make([]DNSDomain, 2000)}
	for i := range big.Domains {
		big.Domains[i] = DNSDomain{
			ZoneID:   int16(i%3 + 1), // пачка многозонная (T-027)
			DomainID: int64(i + 1),
			Name:     fmt.Sprintf("domain%04d.example.com", i),
		}
	}
	payload, err := json.Marshal(big)
	if err != nil {
		t.Fatalf("сериализация пачки: %v", err)
	}
	if len(payload) < 100_000 { // ~70 Б на домен с JSON-обвязкой
		t.Fatalf("пачка неожиданно мала: %d байт — тест не про worst-case", len(payload))
	}
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: payload}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 9}}, // лимит 3 исчерпан
		},
	}

	got, _, err := newTestReliab(fake, ReliabOptions{}).Claim(
		context.Background(), StreamDNS, "dns", "recover-1", time.Minute, 5, "0-0")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got != nil {
		t.Fatalf("исчерпавшая попытки запись возвращена в работу: %+v", got)
	}
	if len(fake.adds) != 1 || fake.adds[0].Stream != StreamDead {
		t.Fatalf("Dead не опубликован: %+v", fake.adds)
	}
	data := fake.adds[0].Values.(map[string]any)[fieldData].([]byte)
	if bound := 512; len(data) > bound {
		t.Errorf("запись q:dead %d байт > %d — payload исходной задачи перенесён (T-105):\n%s",
			len(data), bound, data)
	}

	dead, err := Decode[Dead](Delivery{Stream: StreamDead, ID: fake.adds[0].ID, Payload: data})
	if err != nil {
		t.Fatalf("поле data не декодируется как Dead: %v\n%s", err, data)
	}
	// Идентификаторы для операторского разбора на месте, payload нет.
	if dead.TaskID != "big" || dead.Stream != StreamDNS || dead.EntryID != "1718-0" || dead.Attempts != 9 {
		t.Errorf("идентификаторы записи q:dead: %+v", dead)
	}
	if dead.Domains != len(big.Domains) {
		t.Errorf("размер пачки в записи: %d, хочу %d", dead.Domains, len(big.Domains))
	}
	if len(dead.Payload) != 0 {
		t.Errorf("payload перенесён в q:dead: %d байт", len(dead.Payload))
	}

	// Исходная запись покидает pending: XACK из стрима-источника.
	if want := []ackCall{{StreamDNS, "dns", []string{"1718-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK: %+v, хочу %+v", fake.acks, want)
	}
}

// TestReliabClaimDeadPoisonBody (T-105, P2 ревью) — выжимка из чужого
// тела не должна делать запись q:dead невалидной: иначе Publish отклонил
// бы Dead, XACK не состоялся бы, и «ядовитая» задача навсегда застряла
// бы в pending вместо карантина. Идентификаторы отбрасываются, перенос
// идёт: task_id/стрим/запись/попытки/причина всегда достаточны разбору.
func TestReliabClaimDeadPoisonBody(t *testing.T) {
	poison := `{"task_id":"t1","zone_id":3,"domain_id":42,"subdomain_id":1,"variant":5,"name":"` +
		strings.Repeat("a", 254) + `"}` // вариант вне 0/1, имя длиннее 253
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: []byte(poison)}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 9}}, // лимит 3 исчерпан
		},
	}

	got, _, err := newTestReliab(fake, ReliabOptions{}).Claim(
		context.Background(), StreamRender, "render", "recover-1", time.Minute, 5, "0-0")
	if err != nil {
		t.Fatalf("Claim: %v — перенос не должен падать на чужом теле", err)
	}
	if got != nil {
		t.Fatalf("исчерпавшая попытки запись возвращена в работу: %+v", got)
	}
	if len(fake.adds) != 1 || fake.adds[0].Stream != StreamDead {
		t.Fatalf("Dead не опубликован: %+v", fake.adds)
	}
	data := fake.adds[0].Values.(map[string]any)[fieldData].([]byte)
	dead, err := Decode[Dead](Delivery{Stream: StreamDead, ID: fake.adds[0].ID, Payload: data})
	if err != nil {
		t.Fatalf("поле data не декодируется как Dead: %v\n%s", err, data)
	}
	want := Dead{
		TaskID:   "t1",
		Stream:   StreamRender,
		EntryID:  "1718-0",
		Attempts: 9,
		Reason:   "исчерпан лимит попыток 3: доставок без подтверждения — 9",
	}
	if !reflect.DeepEqual(dead, want) {
		t.Errorf("Dead с чужим телом:\n получил  %+v\n хочу     %+v (без идентификаторов предмета)", dead, want)
	}
	if want := []ackCall{{StreamRender, "render", []string{"1718-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK: %+v, хочу %+v", fake.acks, want)
	}
}

// TestReliabClaimBoundary — граница счётчика: MaxAttempts — ещё в работу,
// больше — в q:dead.
func TestReliabClaimBoundary(t *testing.T) {
	cases := []struct {
		name    string
		attempt int64
		wantNil bool
	}{
		{"первая доставка", 1, false},
		{"последняя разрешённая", 3, false},
		{"исчерпаны", 4, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeClaimRedis{
				claimMsgs: []redis.XMessage{
					{ID: "1-0", Values: map[string]any{fieldData: []byte(`{"task_id":"t1"}`)}},
				},
				pendRes: map[string][]redis.XPendingExt{
					"1-0": {{ID: "1-0", RetryCount: c.attempt}},
				},
			}
			got, _, err := newTestReliab(fake, ReliabOptions{}).Claim(
				context.Background(), StreamDNS, "dns", "recover-1", time.Minute, 1, "0-0")
			if err != nil {
				t.Fatalf("Claim: %v", err)
			}
			if c.wantNil && got != nil {
				t.Errorf("ожидается перевод в q:dead, получена работа: %+v", got)
			}
			if !c.wantNil {
				if len(got) != 1 || got[0].Attempts != int(c.attempt) {
					t.Errorf("ожидались записи с попыткой %d, получено %+v", c.attempt, got)
				}
				if len(fake.adds) != 0 {
					t.Errorf("рабочая запись опубликована в q:dead: %+v", fake.adds)
				}
			}
		})
	}
}

// TestReliabClaimSkips — записи без тела (усечены MAXLEN — XAUTOCLAIM уже
// вычистил их из PEL) и записи, покинувшие PEL между командами, не
// обрабатываются и не переводятся в q:dead.
func TestReliabClaimSkips(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0"}, // усечена MAXLEN: значений нет
			{ID: "1719-0", Values: map[string]any{fieldData: `{"task_id":"t2"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			// "1718-0" — XPENDING не вызывается вовсе (тело пустое)
			"1719-0": nil, // запись уже покинула PEL — подтверждена другим
		},
	}
	got, _, err := newTestReliab(fake, ReliabOptions{}).Claim(
		context.Background(), StreamHTTP, "http", "recover-1", time.Minute, 5, "0-0")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got != nil {
		t.Errorf("нечего возвращать, получено %+v", got)
	}
	if len(fake.pends) != 1 || fake.pends[0].Start != "1719-0" {
		t.Errorf("XPENDING должен спросить только запись с телом: %+v", fake.pends)
	}
	if len(fake.adds) != 0 || len(fake.acks) != 0 {
		t.Errorf("пропуски не публикуются и не подтверждаются: %+v %+v", fake.adds, fake.acks)
	}
}

// TestReliabClaimErrors — ошибки команд и аргументов; сбой публикации в
// q:dead не подтверждает исходную запись (XACK только после Dead).
func TestReliabClaimErrors(t *testing.T) {
	ctx := context.Background()

	// Плохие аргументы — без обращения к Redis.
	for name, call := range map[string]func(r *Reliab) error{
		"неизвестный стрим": func(r *Reliab) error {
			_, _, err := r.Claim(ctx, "q:typo", "g", "c", time.Minute, 1, "0-0")
			return err
		},
		"без группы": func(r *Reliab) error {
			_, _, err := r.Claim(ctx, StreamDNS, "", "c", time.Minute, 1, "0-0")
			return err
		},
		"без потребителя": func(r *Reliab) error {
			_, _, err := r.Claim(ctx, StreamDNS, "g", "", time.Minute, 1, "0-0")
			return err
		},
		"пачка 0": func(r *Reliab) error {
			_, _, err := r.Claim(ctx, StreamDNS, "g", "c", time.Minute, 0, "0-0")
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			fake := &fakeClaimRedis{}
			if err := call(newTestReliab(fake, ReliabOptions{})); err == nil {
				t.Fatal("ожидалась ошибка аргументов")
			}
			if len(fake.claims)+len(fake.pends) != 0 {
				t.Errorf("Redis вызван с плохими аргументами: %+v %+v", fake.claims, fake.pends)
			}
		})
	}

	// Ошибка XAUTOCLAIM.
	fake := &fakeClaimRedis{claimErr: errors.New("ERR connection refused")}
	if _, _, err := newTestReliab(fake, ReliabOptions{}).Claim(ctx, StreamDNS, "dns", "recover-1", time.Minute, 1, "0-0"); err == nil ||
		!contains(err, "claim") {
		t.Errorf("ошибка XAUTOCLAIM: %v", err)
	}

	// Ошибка XPENDING.
	fake = &fakeClaimRedis{
		claimMsgs: []redis.XMessage{{ID: "1-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}}},
		pendErr:   errors.New("ERR connection refused"),
	}
	if _, _, err := newTestReliab(fake, ReliabOptions{}).Claim(ctx, StreamDNS, "dns", "recover-1", time.Minute, 1, "0-0"); err == nil ||
		!contains(err, "счётчик доставок") {
		t.Errorf("ошибка XPENDING: %v", err)
	}

	// Сбой публикации в q:dead — запись остаётся в pending (без XACK):
	// перевод повторится на следующем проходе.
	fake = &fakeClaimRedis{
		fakeRedis: fakeRedis{addErr: errors.New("connection refused")},
		claimMsgs: []redis.XMessage{{ID: "1-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}}},
		pendRes:   map[string][]redis.XPendingExt{"1-0": {{ID: "1-0", RetryCount: 9}}},
	}
	if _, _, err := newTestReliab(fake, ReliabOptions{}).Claim(ctx, StreamDNS, "dns", "recover-1", time.Minute, 1, "0-0"); err == nil ||
		!contains(err, "q:dead") {
		t.Errorf("ошибка публикации Dead: %v", err)
	}
	if len(fake.acks) != 0 {
		t.Errorf("XACK после сбоя публикации Dead: %+v", fake.acks)
	}

	// Сбой XACK перенесённой записи — Dead уже опубликован (дубль на
	// следующем проходе предпочтительнее потери), запись в pending.
	fake = &fakeClaimRedis{
		fakeRedis: fakeRedis{ackErr: errors.New("ERR connection refused")},
		claimMsgs: []redis.XMessage{{ID: "1-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}}},
		pendRes:   map[string][]redis.XPendingExt{"1-0": {{ID: "1-0", RetryCount: 9}}},
	}
	if _, _, err := newTestReliab(fake, ReliabOptions{}).Claim(ctx, StreamDNS, "dns", "recover-1", time.Minute, 1, "0-0"); err == nil ||
		!contains(err, "подтверждение") {
		t.Errorf("ошибка XACK: %v", err)
	}
	if len(fake.adds) != 1 {
		t.Errorf("Dead должен быть опубликован до XACK: %+v", fake.adds)
	}

	// Запись вне форматов пакета (без task_id) не переводится в q:dead
	// молча — ошибка ветки bury для разбора оператором (точная подстрока
	// отличает её от ошибки валидации Dead в публикации).
	fake = &fakeClaimRedis{
		claimMsgs: []redis.XMessage{{ID: "1-0", Values: map[string]any{fieldData: `{"no_task":"id"}`}}},
		pendRes:   map[string][]redis.XPendingExt{"1-0": {{ID: "1-0", RetryCount: 9}}},
	}
	if _, _, err := newTestReliab(fake, ReliabOptions{}).Claim(ctx, StreamDNS, "dns", "recover-1", time.Minute, 1, "0-0"); err == nil ||
		!contains(err, "разберите вручную") {
		t.Errorf("запись без task_id: %v", err)
	}
	if len(fake.adds) != 0 || len(fake.acks) != 0 {
		t.Errorf("неформатная запись не публикуется и не подтверждается: %+v %+v", fake.adds, fake.acks)
	}
}

// TestReliabClaimEmpty — зависших записей нет: пустой результат без
// ошибки, курсор следующего прохода — с начала PEL.
func TestReliabClaimEmpty(t *testing.T) {
	fake := &fakeClaimRedis{}
	got, next, err := newTestReliab(fake, ReliabOptions{}).Claim(
		context.Background(), StreamAnalyze, "analyzer", "recover-1", time.Minute, 5, "0-0")
	if err != nil || got != nil {
		t.Errorf("пустой claim: (%v, %v), хочу (nil, nil)", got, err)
	}
	if next != "0-0" {
		t.Errorf("next пустого обхода = %q, хочу 0-0 (обзор PEL завершён — с начала)", next)
	}
	if len(fake.pends) != 0 {
		t.Errorf("XPENDING без клейм не нужен: %+v", fake.pends)
	}
}

// TestReliabClaimContinuesAfterSpecial — специальная запись в начале
// пачки (сбой счётчика, уход из PEL, исчерпание попыток) не обрывает
// обход: валидные записи следом возвращаются в работу.
func TestReliabClaimContinuesAfterSpecial(t *testing.T) {
	msgs := []redis.XMessage{
		{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
		{ID: "1719-0", Values: map[string]any{fieldData: `{"task_id":"t2"}`}},
	}
	cases := []struct {
		name     string
		pendRes  map[string][]redis.XPendingExt
		pendErr  map[string]error
		wantErr  string // подстрока ошибки по первой записи ("" — без ошибки)
		wantBury bool   // первая запись переводится в q:dead
	}{
		{
			name: "сбой счётчика первой записи",
			pendRes: map[string][]redis.XPendingExt{
				"1719-0": {{ID: "1719-0", RetryCount: 2}},
			},
			pendErr: map[string]error{"1718-0": errors.New("ERR connection refused")},
			wantErr: "счётчик доставок",
		},
		{
			name: "первая покинула PEL",
			pendRes: map[string][]redis.XPendingExt{
				"1718-0": nil, // подтверждена между командами
				"1719-0": {{ID: "1719-0", RetryCount: 2}},
			},
		},
		{
			name: "первая исчерпала попытки",
			pendRes: map[string][]redis.XPendingExt{
				"1718-0": {{ID: "1718-0", RetryCount: 9}}, // лимит 3 исчерпан
				"1719-0": {{ID: "1719-0", RetryCount: 2}},
			},
			wantBury: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeClaimRedis{claimMsgs: msgs, pendRes: c.pendRes, pendOneErr: c.pendErr}
			got, _, err := newTestReliab(fake, ReliabOptions{}).Claim(
				context.Background(), StreamHTTP, "http", "recover-1", time.Minute, 5, "0-0")

			want := []Claimed{
				{Delivery{StreamHTTP, "1719-0", []byte(`{"task_id":"t2"}`)}, 2},
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("Claim после специальной записи:\n получил  %+v\n хочу     %+v", got, want)
			}
			if c.wantErr != "" && (err == nil || !contains(err, c.wantErr)) {
				t.Errorf("ожидалась ошибка %q, получено: %v", c.wantErr, err)
			}
			if c.wantErr == "" && err != nil {
				t.Errorf("ожидалось отсутствие ошибки, получено: %v", err)
			}
			if c.wantBury {
				if len(fake.adds) != 1 || fake.adds[0].Stream != StreamDead {
					t.Errorf("первая запись не перенесена в q:dead: %+v", fake.adds)
				}
				if want := []ackCall{{StreamHTTP, "http", []string{"1718-0"}}}; !reflect.DeepEqual(fake.acks, want) {
					t.Errorf("XACK перенесённой записи: %+v, хочу %+v", fake.acks, want)
				}
			} else if len(fake.adds) != 0 || len(fake.acks) != 0 {
				t.Errorf("специальная запись не должна публиковаться и подтверждаться: %+v %+v", fake.adds, fake.acks)
			}
		})
	}
}

// TestReliabClaimPartialError — сбой XPENDING одной записи не рушит
// пачку: записи с известным счётчиком возвращаются, ошибки собраны
// в возврат (Q-04: XAUTOCLAIM уже закрепил записи — терять пачку на
// сбое одной проверки значило бы лишний MinIdle простоя всем).
func TestReliabClaimPartialError(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
			{ID: "1719-0", Values: map[string]any{fieldData: `{"task_id":"t2"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 2}},
			// "1719-0" — счётчик недоступен (сбой чтения по записи).
		},
		pendOneErr: map[string]error{
			"1719-0": errors.New("ERR connection refused"),
		},
	}

	got, _, err := newTestReliab(fake, ReliabOptions{}).Claim(
		context.Background(), StreamHTTP, "http", "recover-1", time.Minute, 5, "0-0")
	if err == nil || !contains(err, "счётчик доставок") || !contains(err, "1719-0") {
		t.Fatalf("частичный сбой: err = %v, want ошибки счётчика записи 1719-0", err)
	}
	want := []Claimed{
		{Delivery{StreamHTTP, "1718-0", []byte(`{"task_id":"t1"}`)}, 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("валидная часть:\n получил  %+v\n хочу     %+v", got, want)
	}
	if len(fake.acks) != 0 || len(fake.adds) != 0 {
		t.Errorf("клейм не подтверждает и не хоронит: %+v %+v", fake.acks, fake.adds)
	}
}

// TestReliabClaimDeadOnBury (T-154) — хук OnBury зовётся на завершённый
// перевод (Dead опубликован и исходная запись подтверждена) с именем
// стрима-источника; записи в работе и незавершённые переводы (сбой
// публикации Dead, сбой XACK, запись без task_id) хук не трогают.
func TestReliabClaimDeadOnBury(t *testing.T) {
	var buried []string
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}}, // ещё в работу
			{ID: "1719-0", Values: map[string]any{fieldData: `{"task_id":"t2"}`}}, // смерть
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 2}},
			"1719-0": {{ID: "1719-0", RetryCount: 9}}, // лимит 3 исчерпан
		},
	}
	r := NewReliab(fake, NewRedisQueue(fake, RedisOptions{}), ReliabOptions{
		OnBury: func(stream string) { buried = append(buried, stream) },
	})

	if _, _, err := r.Claim(context.Background(), StreamHTTP, "http", "recover-1", time.Minute, 5, "0-0"); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if want := []string{StreamHTTP}; !reflect.DeepEqual(buried, want) {
		t.Errorf("OnBury: %v, хочу ровно один вызов с именем стрима-источника %q", buried, StreamHTTP)
	}

	// Сбой публикации Dead — перевода не было (запись в pending,
	// повторится на следующем проходе): хук молчит.
	fake = &fakeClaimRedis{
		fakeRedis: fakeRedis{addErr: errors.New("connection refused")},
		claimMsgs: []redis.XMessage{{ID: "1-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}}},
		pendRes:   map[string][]redis.XPendingExt{"1-0": {{ID: "1-0", RetryCount: 9}}},
	}
	buried = nil
	if _, _, err := NewReliab(fake, NewRedisQueue(fake, RedisOptions{}), ReliabOptions{
		OnBury: func(stream string) { buried = append(buried, stream) },
	}).Claim(context.Background(), StreamDNS, "dns", "recover-1", time.Minute, 1, "0-0"); err == nil {
		t.Fatal("сбой публикации Dead: ожидалась ошибка")
	}
	if len(buried) != 0 {
		t.Errorf("OnBury при сбое публикации Dead: %v, хочу пусто", buried)
	}

	// Сбой XACK — Dead опубликован, но исходная запись не покинула pending
	// (перевод повторится, дав дубль в q:dead): смерти ещё нет, хук молчит.
	fake = &fakeClaimRedis{
		fakeRedis: fakeRedis{ackErr: errors.New("connection refused")},
		claimMsgs: []redis.XMessage{{ID: "1-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}}},
		pendRes:   map[string][]redis.XPendingExt{"1-0": {{ID: "1-0", RetryCount: 9}}},
	}
	buried = nil
	if _, _, err := NewReliab(fake, NewRedisQueue(fake, RedisOptions{}), ReliabOptions{
		OnBury: func(stream string) { buried = append(buried, stream) },
	}).Claim(context.Background(), StreamDNS, "dns", "recover-1", time.Minute, 1, "0-0"); err == nil {
		t.Fatal("сбой XACK: ожидалась ошибка")
	}
	if len(fake.adds) != 1 {
		t.Fatalf("Dead должен быть опубликован до XACK: %+v", fake.adds)
	}
	if len(buried) != 0 {
		t.Errorf("OnBury при сбое XACK: %v, хочу пусто (перевод не завершён)", buried)
	}

	// Запись без task_id — в q:dead не переводится вовсе.
	fake = &fakeClaimRedis{
		claimMsgs: []redis.XMessage{{ID: "1-0", Values: map[string]any{fieldData: `{"no_task":"id"}`}}},
		pendRes:   map[string][]redis.XPendingExt{"1-0": {{ID: "1-0", RetryCount: 9}}},
	}
	buried = nil
	if _, _, err := NewReliab(fake, NewRedisQueue(fake, RedisOptions{}), ReliabOptions{
		OnBury: func(stream string) { buried = append(buried, stream) },
	}).Claim(context.Background(), StreamDNS, "dns", "recover-1", time.Minute, 1, "0-0"); err == nil {
		t.Fatal("запись без task_id: ожидалась ошибка разбора")
	}
	if len(buried) != 0 {
		t.Errorf("OnBury неформатной записи: %v, хочу пусто", buried)
	}
}

// TestReliabClaimDeadNoHook — nil-хук (изолированные сборки и тесты без
// реестра): bury работает как раньше, без паники.
func TestReliabClaimDeadNoHook(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{{ID: "1-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}}},
		pendRes:   map[string][]redis.XPendingExt{"1-0": {{ID: "1-0", RetryCount: 9}}},
	}
	got, _, err := newTestReliab(fake, ReliabOptions{}).Claim(
		context.Background(), StreamAnalyze, "analyzer", "recover-1", time.Minute, 1, "0-0")
	if err != nil || got != nil {
		t.Fatalf("bury без хука: (%v, %v), хочу (nil, nil) — тихий перевод", got, err)
	}
	if len(fake.adds) != 1 || len(fake.acks) != 1 {
		t.Errorf("перевод без хука не завершён: %+v %+v", fake.adds, fake.acks)
	}
}

// TestProcessing — XACK только после успешной обработки и записи в БД.
func TestProcessing(t *testing.T) {
	ctx := context.Background()
	d := Delivery{Stream: StreamDNS, ID: "1718-0", Payload: []byte(`{"task_id":"t1"}`)}

	// Успех: обработчик записал результат — запись подтверждена.
	fake := &fakeRedis{addID: "1-1"}
	q := NewRedisQueue(fake, RedisOptions{})
	dbWritten := false
	err := Processing(ctx, q, "dns", d, func(_ context.Context, got Delivery) error {
		if got.ID != d.ID {
			t.Errorf("обработчику передана чужая запись: %+v", got)
		}
		dbWritten = true // «запись результата в БД»
		return nil
	})
	if err != nil {
		t.Fatalf("Processing: %v", err)
	}
	if !dbWritten {
		t.Fatal("обработчик не вызван")
	}
	if want := []ackCall{{StreamDNS, "dns", []string{"1718-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK после обработки: %+v, хочу %+v", fake.acks, want)
	}

	// Ошибка обработки (БД не записана) — без XACK: запись вернётся
	// повторной доставкой (Claim).
	fake = &fakeRedis{}
	q = NewRedisQueue(fake, RedisOptions{})
	err = Processing(ctx, q, "dns", d, func(context.Context, Delivery) error {
		return errors.New("db: write result: timeout")
	})
	if err == nil || !contains(err, "db: write result") {
		t.Fatalf("ошибка обработки не возвращена: %v", err)
	}
	if len(fake.acks) != 0 {
		t.Errorf("XACK после сбоя обработки: %+v", fake.acks)
	}

	// Ошибка XACK возвращается: результат в БД, запись в pending —
	// повторная доставка пройдёт по идемпотентности (ТЗ 3.4).
	fake = &fakeRedis{ackErr: errors.New("ERR connection refused")}
	err = Processing(ctx, NewRedisQueue(fake, RedisOptions{}), "dns", d,
		func(context.Context, Delivery) error { return nil })
	if err == nil || !contains(err, "ack") {
		t.Fatalf("ошибка XACK не возвращена: %v", err)
	}

	// Без обработчика — программная ошибка, Redis не трогается.
	fake = &fakeRedis{}
	if err := Processing(ctx, NewRedisQueue(fake, RedisOptions{}), "dns", d, nil); err == nil {
		t.Fatal("ожидалась ошибка отсутствия обработчика")
	}
	if len(fake.acks) != 0 {
		t.Errorf("XACK без обработчика: %+v", fake.acks)
	}
}

// TestPublishMaxLenPolicy — без явного предела каждый стрим усекается
// своим пределом политики T-025, явный предел перекрывает все стримы.
func TestPublishMaxLenPolicy(t *testing.T) {
	msgs := []struct {
		stream string
		msg    Message
	}{
		{StreamDNS, DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: "a.test"}}}},
		{StreamHTTP, HTTPTask{ZoneID: 1, DomainID: 1, Name: "a.test",
			Subdomains: []HTTPSubdomain{{SubdomainID: 0, Addrs: []string{"192.0.2.1"}}}}},
		{StreamRender, Result{ZoneID: 1, DomainID: 1, SubdomainID: 0}},
		{StreamAnalyze, Result{ZoneID: 1, DomainID: 1, SubdomainID: 0}},
		{StreamDead, Dead{TaskID: "t", Stream: StreamDNS, EntryID: "1-0", Attempts: 1,
			Payload: json.RawMessage(`{}`)}},
	}

	fake := &fakeRedis{}
	q := NewRedisQueue(fake, RedisOptions{}) // предел не задан — политика
	for _, m := range msgs {
		if _, err := q.Publish(context.Background(), m.stream, m.msg); err != nil {
			t.Fatalf("Publish в %s: %v", m.stream, err)
		}
	}
	for i, m := range msgs {
		add := fake.adds[i]
		if want := DefaultStreamMaxLen(m.stream); add.MaxLen != want || !add.Approx {
			t.Errorf("%s: MAXLEN ~ %d не применён: MaxLen=%d Approx=%v", m.stream, want, add.MaxLen, add.Approx)
		}
	}

	// Явный предел — один на все стримы.
	fake = &fakeRedis{}
	q = NewRedisQueue(fake, RedisOptions{MaxLen: 50000})
	for _, m := range msgs {
		if _, err := q.Publish(context.Background(), m.stream, m.msg); err != nil {
			t.Fatalf("Publish в %s: %v", m.stream, err)
		}
	}
	for i := range msgs {
		if add := fake.adds[i]; add.MaxLen != 50000 || !add.Approx {
			t.Errorf("явный предел проигнорирован: MaxLen=%d Approx=%v", add.MaxLen, add.Approx)
		}
	}
}

func contains(err error, substr string) bool {
	return err != nil && strings.Contains(err.Error(), substr)
}

// Тесты цикла возврата зависших доставок (RecoverLoop, T-025) без живого
// Redis: фейк reliab_test.go (одноразовый клейм — как живой PEL, idle
// сбрасывается). Проверяются клейм → обработка → XACK, ядовитая обработка
// без подтверждения, дренаж полной пачки без ожидания тика, перевод
// исчерпавших попытки в q:dead (обработчик не зовётся), дефолты опций,
// чистый выход по отмене ctx.

package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// runRecoverLoop — RecoverLoop в горутине; возвращает канал обработанных
// доставок, канал результата и функцию остановки.
func runRecoverLoop(t *testing.T, fake *fakeClaimRedis, opt RecoverOptions, process func(context.Context, Delivery) error) (processed chan Delivery, done chan error, stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	processed = make(chan Delivery, 16)
	if process == nil {
		process = func(_ context.Context, d Delivery) error {
			processed <- d
			return nil
		}
	}
	done = make(chan error, 1)
	r := newTestReliab(fake, ReliabOptions{MinIdle: time.Minute})
	go func() { done <- RecoverLoop(ctx, r, process, opt) }()
	return processed, done, func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("RecoverLoop не остановился по отмене ctx")
		}
	}
}

// sameAcks — сравнение протокола XACK (срез в структуре не сравним
// напрямую).
func sameAcks(got, want []ackCall) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i].stream != want[i].stream || got[i].group != want[i].group {
			return false
		}
		if len(got[i].ids) != len(want[i].ids) {
			return false
		}
		for j := range got[i].ids {
			if got[i].ids[j] != want[i].ids[j] {
				return false
			}
		}
	}
	return true
}

// waitProcessed — первая обработанная доставка или таймаут.
func waitProcessed(t *testing.T, processed chan Delivery) Delivery {
	t.Helper()
	select {
	case d := <-processed:
		return d
	case <-time.After(5 * time.Second):
		t.Fatal("обработка зависшей доставки не началась")
		return Delivery{}
	}
}

func TestRecoverLoopClaimProcess(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 2}},
		},
	}

	processed, _, stop := runRecoverLoop(t, fake, RecoverOptions{
		Stream: StreamHTTP, Group: "http", Consumer: "recover-1",
		Batch: 4, Interval: time.Hour,
	}, nil)
	d := waitProcessed(t, processed)
	if d.ID != "1718-0" || d.Stream != StreamHTTP {
		t.Errorf("обработана чужая доставка: %+v", d)
	}
	stop() // ожидание выхода — внутри runRecoverLoop

	// Клейм от имени потребителя цикла, XACK — после обработки.
	if len(fake.claims) != 1 {
		t.Fatalf("вызовов XAUTOCLAIM: %d, хочу 1", len(fake.claims))
	}
	if c := fake.claims[0]; c.Stream != StreamHTTP || c.Group != "http" || c.Consumer != "recover-1" || c.Count != 4 {
		t.Errorf("XAUTOCLAIM: стрим/группа/потребитель/пачка: %+v", c)
	}
	if want := []ackCall{{StreamHTTP, "http", []string{"1718-0"}}}; !sameAcks(fake.acks, want) {
		t.Errorf("XACK после обработки: %+v, хочу %+v", fake.acks, want)
	}
	// Рабочая запись не уходит в q:dead.
	if len(fake.adds) != 0 {
		t.Errorf("публикаций в q:dead быть не должно: %+v", fake.adds)
	}
}

// TestRecoverLoopProcessError — ошибка обработки: без XACK (запись
// вернётся следующим клеймом), цикл жив, чистый выход по отмене.
func TestRecoverLoopProcessError(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 1}},
		},
	}

	_, _, stop := runRecoverLoop(t, fake, RecoverOptions{
		Stream: StreamHTTP, Group: "http", Consumer: "recover-1",
		Batch: 4, Interval: time.Hour,
	}, func(context.Context, Delivery) error { return errors.New("db: write result: timeout") })
	// Дать циклу обработать запись (клейм + Processing).
	time.Sleep(50 * time.Millisecond)
	stop()

	if len(fake.acks) != 0 {
		t.Errorf("XACK после сбоя обработки: %+v", fake.acks)
	}
	if len(fake.adds) != 0 {
		t.Errorf("сбой обработки не переводит запись в q:dead: %+v", fake.adds)
	}
}

// TestRecoverLoopDrainsFullBatch — полная пачка клейма дренируется без
// ожидания тика: при Batch 1 и Interval час обе записи обрабатываются
// подряд (вторая — без тика), пустой клейм — затем ожидание.
func TestRecoverLoopDrainsFullBatch(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
			{ID: "1719-0", Values: map[string]any{fieldData: `{"task_id":"t2"}`}},
		},
		claimNext: "1720-0", // курсор следующего сканирования из ответа
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 1}},
			"1719-0": {{ID: "1719-0", RetryCount: 1}},
		},
	}

	processed, _, stop := runRecoverLoop(t, fake, RecoverOptions{
		Stream: StreamDNS, Group: "dns", Consumer: "recover-1",
		Batch: 1, Interval: time.Hour,
	}, nil)
	first := waitProcessed(t, processed)
	second := waitProcessed(t, processed) // тик — час: вторая только дренажом
	stop()
	if first.ID != "1718-0" || second.ID != "1719-0" {
		t.Errorf("дренаж: %+v затем %+v, хочу 1718-0 и 1719-0", first, second)
	}
	// Второй клейм продолжает с курсора первого, а не с начала PEL.
	if len(fake.claims) < 2 {
		t.Fatalf("клеймов %d, хочу ≥ 2", len(fake.claims))
	}
	if got := fake.claims[1].Start; got != "1720-0" {
		t.Errorf("курсор второго клейма = %q, хочу 1720-0 (перенос между проходами)", got)
	}
	if !sameAcks(fake.acks, []ackCall{
		{StreamDNS, "dns", []string{"1718-0"}},
		{StreamDNS, "dns", []string{"1719-0"}},
	}) {
		t.Errorf("XACK дренированных: %+v", fake.acks)
	}
}

// TestRecoverLoopDead — запись, исчерпавшая попытки, в обработку не
// попадает: Claim переводит её в q:dead и подтверждает в исходном стриме.
func TestRecoverLoopDead(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 9}}, // лимит 3 исчерпан
		},
	}

	processed, _, stop := runRecoverLoop(t, fake, RecoverOptions{
		Stream: StreamRender, Group: "render", Consumer: "recover-1",
		Batch: 4, Interval: time.Hour,
	}, nil)
	// Дать циклу клейм (перевод в q:dead происходит в Claim).
	time.Sleep(50 * time.Millisecond)
	stop()

	select {
	case d := <-processed:
		t.Errorf("исчерпавшая попытки запись обработана: %+v", d)
	default:
	}
	if len(fake.adds) != 1 || fake.adds[0].Stream != StreamDead {
		t.Errorf("Dead не опубликован: %+v", fake.adds)
	}
	if want := []ackCall{{StreamRender, "render", []string{"1718-0"}}}; !sameAcks(fake.acks, want) {
		t.Errorf("XACK перенесённой записи: %+v, хочу %+v", fake.acks, want)
	}
}

// TestRecoverLoopValidation — nil слоя и обработчика: ошибка до старта
// (сборка сервиса падает внятно, Redis не трогается).
func TestRecoverLoopValidation(t *testing.T) {
	fake := &fakeClaimRedis{}
	if err := RecoverLoop(context.Background(), nil, func(context.Context, Delivery) error { return nil }, RecoverOptions{
		Stream: StreamDNS, Group: "dns", Consumer: "c",
	}); err == nil || !contains(err, "надёжности") {
		t.Errorf("nil Reliab: %v", err)
	}
	r := newTestReliab(fake, ReliabOptions{})
	if err := RecoverLoop(context.Background(), r, nil, RecoverOptions{
		Stream: StreamDNS, Group: "dns", Consumer: "c",
	}); err == nil || !contains(err, "обработчик") {
		t.Errorf("nil обработчик: %v", err)
	}
	if len(fake.claims) != 0 {
		t.Errorf("Redis вызван с плохими аргументами: %+v", fake.claims)
	}
}

// TestRecoverOptionsDefaults — нулевые опции: пачка и интервал дефолтные.
func TestRecoverOptionsDefaults(t *testing.T) {
	o := RecoverOptions{}.withDefaults()
	if o.Batch != DefaultRecoverBatch {
		t.Errorf("Batch = %d, хочу %d", o.Batch, DefaultRecoverBatch)
	}
	if o.Interval != DefaultRecoverInterval {
		t.Errorf("Interval = %s, хочу %s", o.Interval, DefaultRecoverInterval)
	}
}

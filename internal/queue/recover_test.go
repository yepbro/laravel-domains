// Тесты цикла возврата зависших доставок (RecoverLoop, T-025) без живого
// Redis: фейк reliab_test.go (одноразовый клейм — как живой PEL, idle
// сбрасывается). Проверяются клейм → обработка → XACK, ядовитая обработка
// без подтверждения, дренаж полной пачки без ожидания тика, перевод
// исчерпавших попытки в q:dead (обработчик не зовётся), дефолты опций,
// чистый выход по отмене ctx.

package queue

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
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
	c := fake.claims[0]
	if c.Stream != StreamHTTP || c.Group != "http" || c.Consumer != "recover-1" || c.Count != 4 {
		t.Errorf("XAUTOCLAIM: стрим/группа/потребитель/пачка: %+v", c)
	}
	// minIdle 0 из цикла — опция слоя надёжности (MinIdle воркера).
	if c.MinIdle != time.Minute {
		t.Errorf("XAUTOCLAIM MinIdle: %s, хочу 1m (из ReliabOptions теста)", c.MinIdle)
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

// recoverLogOpts — опции цикла с логгером в буфер (проверка записей).
func recoverLogOpts(buf *bytes.Buffer, interval time.Duration) RecoverOptions {
	return RecoverOptions{
		Stream: StreamHTTP, Group: "http", Consumer: "recover-1",
		Batch: 4, Interval: interval,
		Log: slog.New(slog.NewTextHandler(buf, nil)),
	}
}

// TestRecoverLoopLogs — успешный проход: стартовая запись Info есть,
// предупреждений нет (ошибок не было — Warn с err=nil не пишется).
func TestRecoverLoopLogs(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 1}},
		},
	}
	var buf bytes.Buffer
	processed, _, stop := runRecoverLoop(t, fake, recoverLogOpts(&buf, time.Hour), nil)
	waitProcessed(t, processed)
	stop()

	out := buf.String()
	if !strings.Contains(out, "цикл возврата зависших доставок") {
		t.Errorf("стартовая запись цикла не оставлена: %q", out)
	}
	if strings.Contains(out, "клейм") {
		t.Errorf("успешный клейм записан предупреждением: %q", out)
	}
	if strings.Contains(out, "не подтверждена") {
		t.Errorf("успешная обработка записана предупреждением: %q", out)
	}
}

// TestRecoverLoopLogsErrors — ошибки клейма и обработки логируются
// Warn (очередной тик повторит), цикл не падает.
func TestRecoverLoopLogsErrors(t *testing.T) {
	// Ошибка клейма: тик 1 мс, даём циклу несколько проходов (сигнал из
	// фейка потребовал бы его расширения; стиль — как в соседних
	// TestRecoverLoopProcessError/TestRecoverLoopDead).
	fake := &fakeClaimRedis{claimErr: errors.New("ERR connection refused")}
	var buf bytes.Buffer
	_, _, stop := runRecoverLoop(t, fake, recoverLogOpts(&buf, time.Millisecond), nil)
	time.Sleep(50 * time.Millisecond) // хотя бы один тик с ошибкой
	stop()
	if !strings.Contains(buf.String(), "возврат зависших доставок: клейм") {
		t.Errorf("ошибка клейма не записана: %q", buf.String())
	}

	// Ошибка обработки: без XACK, с записью о возврате клеймом. Warn
	// пишется только при живом ctx — «сигнал → отмена» пропускала бы
	// его по условию ctx.Err() == nil; даём циклу отработать (первый
	// клейм и Warn — в первые миллисекунды), стиль — как в соседних
	// TestRecoverLoopProcessError/TestRecoverLoopDead.
	fake = &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 1}},
		},
	}
	buf.Reset()
	_, _, stop = runRecoverLoop(t, fake, recoverLogOpts(&buf, time.Hour),
		func(context.Context, Delivery) error { return errors.New("db: write result: timeout") })
	time.Sleep(50 * time.Millisecond)
	stop()
	if !strings.Contains(buf.String(), "не подтверждена") {
		t.Errorf("ошибка обработки не записана: %q", buf.String())
	}
	if len(fake.acks) != 0 {
		t.Errorf("XACK после сбоя обработки: %+v", fake.acks)
	}
}

// TestRecoverLoopCanceledContext — отмена ctx до старта: клейм
// с ошибкой при уже отменённом контексте не пишется предупреждением
// (выполнено указание остановки, а не сбой), выход чистый.
func TestRecoverLoopCanceledContext(t *testing.T) {
	fake := &fakeClaimRedis{claimErr: errors.New("ERR connection refused")}
	var buf bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := newTestReliab(fake, ReliabOptions{MinIdle: time.Minute})
	err := RecoverLoop(ctx, r, func(context.Context, Delivery) error { return nil },
		recoverLogOpts(&buf, time.Hour))
	if err != nil {
		t.Fatalf("выход по отменённому ctx: %v, хочу nil", err)
	}
	if strings.Contains(buf.String(), "клейм") {
		t.Errorf("клейм при отменённом ctx записан предупреждением: %q", buf.String())
	}
}

// TestRecoverLoopCanceledMidBatch — отмена ctx посреди пачки: текущая
// запись дорабатывает или возвращается клейму, остальные не начинаются;
// ошибка обработки при отменённом ctx не пишется предупреждением.
func TestRecoverLoopCanceledMidBatch(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
			{ID: "1719-0", Values: map[string]any{fieldData: `{"task_id":"t2"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 1}},
			"1719-0": {{ID: "1719-0", RetryCount: 1}},
		},
	}
	var mu sync.Mutex
	handled := make([]string, 0, 2)
	started := make(chan struct{}, 2)
	// release не закрывается: обработка снимается только отменой ctx —
	// детерминированно вторая запись не начинается до отмены.
	release := make(chan struct{})
	process := func(ctx context.Context, d Delivery) error {
		mu.Lock()
		handled = append(handled, d.ID)
		mu.Unlock()
		started <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	var buf bytes.Buffer
	_, _, stop := runRecoverLoop(t, fake, recoverLogOpts(&buf, time.Hour), process)
	<-started // первая запись в работе
	stop()    // отмена ctx: первая возвращается ошибкой, вторая не начинается

	mu.Lock()
	defer mu.Unlock()
	if want := []string{"1718-0"}; !reflect.DeepEqual(handled, want) {
		t.Errorf("обработаны %v, хочу %v (после отмены пачка не продолжается)", handled, want)
	}
	if strings.Contains(buf.String(), "не подтверждена") {
		t.Errorf("ошибка обработки при отменённом ctx записана предупреждением: %q", buf.String())
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

// TestRecoverOptionsDefaults — нулевые опции: пачка и интервал дефолтные
// (литеральные пины констант — сравнение с самой константой мутанта
// не замечает), единичные значения не заменяются.
func TestRecoverOptionsDefaults(t *testing.T) {
	o := RecoverOptions{}.withDefaults()
	if o.Batch != 16 {
		t.Errorf("Batch = %d, хочу 16 (DefaultRecoverBatch, литерал — пин константы)", o.Batch)
	}
	if o.Interval != 30*time.Second {
		t.Errorf("Interval = %s, хочу 30s (DefaultRecoverInterval, литерал — пин константы)", o.Interval)
	}
	o = RecoverOptions{Batch: 1, Interval: time.Nanosecond}.withDefaults()
	if o.Batch != 1 {
		t.Errorf("Batch = %d, хочу 1 (положительная пачка валидна)", o.Batch)
	}
	if o.Interval != time.Nanosecond {
		t.Errorf("Interval = %s, хочу 1нс (положительный интервал валиден)", o.Interval)
	}
}

// --- обработчик с номером попытки доставки (T-148) ---------------------------

// TestProcessingAttempt — паттерн Processing для обработчика с номером
// попытки: число доставляется как есть, успешная обработка → XACK, ошибка
// → без XACK (повторная доставка), nil-обработчик — ошибка.
func TestProcessingAttempt(t *testing.T) {
	fake := &fakeClaimRedis{}
	r := newTestReliab(fake, ReliabOptions{})
	d := Delivery{Stream: StreamRender, ID: "1-1", Payload: []byte(`{"task_id":"t1"}`)}

	var got int
	err := r.ProcessAttempt(context.Background(), "render-worker", d, 3,
		func(_ context.Context, _ Delivery, attempts int) error {
			got = attempts
			return nil
		})
	if err != nil {
		t.Fatalf("ProcessAttempt: %v", err)
	}
	if got != 3 {
		t.Errorf("обработчик видел attempts=%d, хочу 3 (счётчик доставок PEL)", got)
	}
	if want := []ackCall{{StreamRender, "render-worker", []string{"1-1"}}}; !sameAcks(fake.acks, want) {
		t.Errorf("XACK после успешной обработки: %+v, хочу %+v", fake.acks, want)
	}

	// Ошибка обработки — без XACK, номер попытки не теряется в сообщении.
	fake2 := &fakeClaimRedis{}
	r2 := newTestReliab(fake2, ReliabOptions{})
	err = r2.ProcessAttempt(context.Background(), "render-worker", d, 2,
		func(context.Context, Delivery, int) error { return errors.New("таймаут") })
	if err == nil || !strings.Contains(err.Error(), "таймаут") {
		t.Fatalf("ошибка обработки: %v", err)
	}
	if len(fake2.acks) != 0 {
		t.Errorf("XACK при ошибке: %+v", fake2.acks)
	}

	// nil-обработчик — ошибка до Redis.
	if err := ProcessingAttempt(context.Background(), NewRedisQueue(&fakeClaimRedis{}, RedisOptions{}), "g", d, 1, nil); err == nil {
		t.Error("nil обработчик: ожидалась ошибка")
	}
}

// TestRecoverLoopAttempt — RecoverLoopAttempt передаёт обработчику
// Claimed.Attempts (счётчик доставок PEL записи): обработчик видит,
// какая это по счёту доставка, без собственного состояния.
func TestRecoverLoopAttempt(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 2}},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	type rec struct {
		id       string
		attempts int
	}
	got := make(chan rec, 1)
	done := make(chan error, 1)
	r := newTestReliab(fake, ReliabOptions{MinIdle: time.Minute})
	go func() {
		done <- RecoverLoopAttempt(ctx, r, func(_ context.Context, d Delivery, attempts int) error {
			got <- rec{d.ID, attempts}
			return nil
		}, RecoverOptions{Stream: StreamRender, Group: "render-worker", Consumer: "c", Batch: 4, Interval: time.Hour})
	}()

	select {
	case g := <-got:
		if g.id != "1718-0" || g.attempts != 2 {
			t.Errorf("обработчик видел (%s, попытка %d), хочу (1718-0, 2)", g.id, g.attempts)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("обработка клеймнутой доставки не началась")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RecoverLoopAttempt после отмены: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RecoverLoopAttempt не остановился по отмене ctx")
	}
	if want := []ackCall{{StreamRender, "render-worker", []string{"1718-0"}}}; !sameAcks(fake.acks, want) {
		t.Errorf("XACK после обработки: %+v, хочу %+v", fake.acks, want)
	}
}

// TestRecoverLoopAttemptValidation — nil обработчика RecoverLoopAttempt
// отклоняет до старта; RecoverLoop (адаптер) — по-прежнему тоже.
func TestRecoverLoopAttemptValidation(t *testing.T) {
	fake := &fakeClaimRedis{}
	r := newTestReliab(fake, ReliabOptions{})
	if err := RecoverLoopAttempt(context.Background(), r, nil, RecoverOptions{
		Stream: StreamRender, Group: "g", Consumer: "c",
	}); err == nil || !contains(err, "обработчик") {
		t.Errorf("nil обработчик RecoverLoopAttempt: %v", err)
	}
	if err := RecoverLoop(context.Background(), r, nil, RecoverOptions{
		Stream: StreamRender, Group: "g", Consumer: "c",
	}); err == nil || !contains(err, "обработчик") {
		t.Errorf("nil обработчик RecoverLoop: %v", err)
	}
	if len(fake.claims) != 0 {
		t.Errorf("Redis вызван с плохими аргументами: %+v", fake.claims)
	}
}

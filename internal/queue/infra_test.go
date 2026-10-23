// Тесты инфраструктурных ошибок очереди (T-152) без живого Redis:
// классификация сбоев транспорта/сервера (infraCause), маркер InfraError
// через %w-обёртки, повторы публикации XADD при инфраструктурном сбое,
// классификация Ack, проба записи Reliab.Writable (SET) и гейт клейма
// RecoverLoop по пробе. Реальная семантика OOM (noeviction, XPENDING
// RetryCount) — интеграционный тест TestResilience_OOMDoesNotBurnAttempts.

package queue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// testRedisError — ошибка сервера Redis как тип (redis.Error): реальные
// ответы go-redis отдаёт типизированными proto.RedisError, строковый
// fallback Is*-хелперов — только для голых строк.
type testRedisError string

func (e testRedisError) Error() string { return string(e) }
func (e testRedisError) RedisError()   {}

// oomErr — ошибка OOM как из журналов инцидента 22–23.10 (тип
// redis.Error, как отдаёт go-redis).
var oomErr error = testRedisError("OOM command not allowed when used memory > 'maxmemory'")

// TestInfraCause — классификация сбоев инфраструктуры: ответы сервера о
// собственной недоступности (OOM/LOADING/READONLY), транспорт (обрыв
// соединения, таймаут сокета, пул) — инфраструктурные; ошибки обработки,
// ответы о самой команде, отмена/дедлайн контекста вызывающего — нет.
func TestInfraCause(t *testing.T) {
	netTimeout := &net.DNSError{Err: "i/o timeout", IsTimeout: true} // net.Error
	connReset := &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"OOM", oomErr, true},
		{"LOADING", testRedisError("LOADING Redis is loading the dataset in memory"), true},
		{"READONLY", testRedisError("READONLY You can't write against a read only replica"), true},
		{"таймаут сокета", netTimeout, true},
		{"сброс соединения", connReset, true},
		{"обрыв соединения", io.EOF, true},
		{"пул соединений", fmt.Errorf("redis: %w", redis.ErrPoolTimeout), true},
		{"обёртка OOM", fmt.Errorf("queue: publish в q:http: %w", oomErr), true},
		{"ошибка обработки", errors.New("db: write result: timeout"), false},
		{"ответ о команде", errors.New("ERR wrong number of arguments"), false},
		{"отмена ctx", fmt.Errorf("обработка: %w", context.Canceled), false},
		{"дедлайн ctx вызывающего", fmt.Errorf("обработка: %w", context.DeadlineExceeded), false},
		{"nil", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := infraCause(c.err); got != c.want {
				t.Errorf("infraCause(%v) = %v, хочу %v", c.err, got, c.want)
			}
		})
	}
}

// TestInfraErrorIsInfra — маркер проходит через цепочки %w-обёрток
// воркеров (Processing → OnResult → Publish) и не возникает у ошибок
// обработки.
func TestInfraErrorIsInfra(t *testing.T) {
	infra := &InfraError{Op: "publish в q:analyze", Err: oomErr}
	if !IsInfra(infra) {
		t.Fatal("IsInfra(InfraError) = false")
	}
	if !IsInfra(fmt.Errorf("queue: запись 1-1 стрима q:render не подтверждена: %w",
		fmt.Errorf("renderworker: домен a.test: публикация q:analyze: %w", infra))) {
		t.Error("маркер не прошёл через двойную %w-обёртку")
	}
	if IsInfra(errors.New("db: write result: timeout")) {
		t.Error("ошибка обработки классифицирована как инфраструктурная")
	}
	if !strings.Contains(infra.Error(), "publish в q:analyze") ||
		!strings.Contains(infra.Error(), "OOM command not allowed") {
		t.Errorf("сообщение InfraError без операции/причины: %q", infra.Error())
	}
	if !errors.Is(infra, oomErr) {
		t.Error("Unwrap не отдаёт исходную ошибку")
	}
}

// TestPublishRetryDefaults — литеральные пины констант и нормализация
// опций (сравнение с самой константой мутанта не замечает).
func TestPublishRetryDefaults(t *testing.T) {
	if DefaultPublishRetries != 2 {
		t.Errorf("DefaultPublishRetries = %d, хочу 2 (T-152: короткая серия повторов)", DefaultPublishRetries)
	}
	if DefaultPublishRetryPause != 250*time.Millisecond {
		t.Errorf("DefaultPublishRetryPause = %s, хочу 250ms", DefaultPublishRetryPause)
	}
	q := NewRedisQueue(&fakeRedis{}, RedisOptions{})
	if q.opt.PublishRetries != DefaultPublishRetries || q.opt.PublishRetryPause != DefaultPublishRetryPause {
		t.Errorf("нулевые опции повторов не заменены дефолтами: %+v", q.opt)
	}
	q = NewRedisQueue(&fakeRedis{}, RedisOptions{PublishRetries: 1, PublishRetryPause: time.Nanosecond})
	if q.opt.PublishRetries != 1 || q.opt.PublishRetryPause != time.Nanosecond {
		t.Errorf("единичные опции заменены дефолтами: %+v", q.opt)
	}
}

// TestPublishRetriesInfra — повтор XADD при инфраструктурном сбое
// (T-152): результат уже вычислен, короткая серия повторов закрывает
// флап; устойчивый сбой — InfraError после исчерпания повторов;
// не-инфраструктурная ошибка не повторяется; отмена ctx прерывает паузу.
func TestPublishRetriesInfra(t *testing.T) {
	msg := DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: "a.test"}}}

	// Два OOM, третий вызов проходит: публикация успешна, всего 3 XADD.
	fake := &fakeRedis{addErrs: []error{oomErr, oomErr}}
	q := NewRedisQueue(fake, RedisOptions{PublishRetryPause: time.Nanosecond})
	if _, err := q.Publish(context.Background(), StreamDNS, msg); err != nil {
		t.Fatalf("Publish после повторов: %v", err)
	}
	if len(fake.adds) != 3 {
		t.Errorf("вызовов XADD: %d, хочу 3 (1 + 2 повтора)", len(fake.adds))
	}

	// Устойчивый OOM: InfraError, повторы исчерпаны (1 + PublishRetries).
	fake = &fakeRedis{addErr: oomErr}
	q = NewRedisQueue(fake, RedisOptions{PublishRetries: 2, PublishRetryPause: time.Nanosecond})
	_, err := q.Publish(context.Background(), StreamDNS, msg)
	if !IsInfra(err) {
		t.Fatalf("устойчивый сбой: %v, хочу InfraError", err)
	}
	if !strings.Contains(err.Error(), "publish в q:dns") {
		t.Errorf("ошибка без операции: %q", err.Error())
	}
	if len(fake.adds) != 3 {
		t.Errorf("вызовов XADD: %d, хочу 3 (1 + PublishRetries 2)", len(fake.adds))
	}

	// Не-инфраструктурная ошибка XADD — без повторов (повтор дал бы тот
	// же ответ).
	fake = &fakeRedis{addErr: errors.New("ERR wrong number of arguments")}
	q = NewRedisQueue(fake, RedisOptions{PublishRetryPause: time.Nanosecond})
	_, err = q.Publish(context.Background(), StreamDNS, msg)
	if err == nil || IsInfra(err) {
		t.Fatalf("не-инфраструктурная ошибка: %v", err)
	}
	if len(fake.adds) != 1 {
		t.Errorf("вызовов XADD: %d, хочу 1 (повтора нет)", len(fake.adds))
	}

	// Отмена ctx в паузе между повторами: без нового XADD, ошибка
	// инфраструктурная (публикация не состоялась).
	fake = &fakeRedis{addErr: oomErr}
	q = NewRedisQueue(fake, RedisOptions{PublishRetryPause: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := q.Publish(ctx, StreamDNS, msg)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond) // первый XADD упал, цикл в паузе
	cancel()
	select {
	case err := <-done:
		if !IsInfra(err) {
			t.Fatalf("отмена в паузе: %v, хочу InfraError", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Publish не вышел по отмене ctx")
	}
	if len(fake.adds) != 1 {
		t.Errorf("вызовов XADD после отмены: %d, хочу 1", len(fake.adds))
	}
}

// TestAckInfraError — XACK при инфраструктурном сбое классифицируется
// маркером (результат уже записан — это не ошибка обработки), прочая
// ошибка — прежним текстом без маркера.
func TestAckInfraError(t *testing.T) {
	fake := &fakeRedis{ackErr: oomErr}
	q := NewRedisQueue(fake, RedisOptions{})
	err := q.Ack(context.Background(), StreamHTTP, "http-worker", "1-1")
	if !IsInfra(err) {
		t.Fatalf("Ack при OOM: %v, хочу InfraError", err)
	}
	if !strings.Contains(err.Error(), "ack q:http") {
		t.Errorf("ошибка без операции: %q", err.Error())
	}

	fake = &fakeRedis{ackErr: errors.New("ERR no such key")}
	q = NewRedisQueue(fake, RedisOptions{})
	err = q.Ack(context.Background(), StreamHTTP, "http-worker", "1-1")
	if err == nil || IsInfra(err) {
		t.Fatalf("Ack с рядовой ошибкой: %v", err)
	}
}

// probeClaimRedis — fakeClaimRedis плюс SET-проба записи (T-152):
// переключаемая ошибка SET и потокобезопасное наблюдение клеймов
// (цикл клеймит в своей горутине — доступ под мьютексом, стиль
// pacingClaimRedis).
type probeClaimRedis struct {
	*fakeClaimRedis
	mu     sync.Mutex
	setErr error
	sets   int
}

func (f *probeClaimRedis) XAutoClaim(ctx context.Context, a *redis.XAutoClaimArgs) *redis.XAutoClaimCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fakeClaimRedis.XAutoClaim(ctx, a)
}

func (f *probeClaimRedis) XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fakeClaimRedis.XPendingExt(ctx, a)
}

func (f *probeClaimRedis) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	f.mu.Lock()
	f.sets++
	err := f.setErr
	f.mu.Unlock()
	return redis.NewStatusResult("OK", err)
}

func (f *probeClaimRedis) claimCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.claims)
}

func (f *probeClaimRedis) setCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sets
}

func (f *probeClaimRedis) setProbeErr(err error) {
	f.mu.Lock()
	f.setErr = err
	f.mu.Unlock()
}

// TestReliabWritable — проба записи: клиент с SET отвечает по ошибке
// пробы, клиент без SET (фейки юнит-тестов) — true (гейт выключен,
// поведение до T-152).
func TestReliabWritable(t *testing.T) {
	fake := &probeClaimRedis{fakeClaimRedis: &fakeClaimRedis{}}
	r := NewReliab(fake, NewRedisQueue(fake, RedisOptions{}), ReliabOptions{})

	fake.setProbeErr(oomErr)
	if r.Writable(context.Background()) {
		t.Error("Writable при падающей пробе = true")
	}
	fake.setProbeErr(nil)
	if !r.Writable(context.Background()) {
		t.Error("Writable при проходящей пробе = false")
	}
	if fake.setCount() != 2 {
		t.Errorf("проб выполнено %d, хочу 2", fake.setCount())
	}

	// Без пробы (клиент не умеет SET) гейт прозрачен.
	r2 := NewReliab(&fakeClaimRedis{}, NewRedisQueue(&fakeClaimRedis{}, RedisOptions{}), ReliabOptions{})
	if !r2.Writable(context.Background()) {
		t.Error("Writable без пробы = false (гейт обязан быть прозрачным)")
	}
}

// TestRecoverLoopGatedByProbe — гейт клейма (T-152): пока проба записи
// падает (OOM), RecoverLoop не клеймит — попытки доставок не расходуются;
// запись восстановилась — клейм и обработка продолжаются.
func TestRecoverLoopGatedByProbe(t *testing.T) {
	fake := &probeClaimRedis{fakeClaimRedis: &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 1}},
		},
	}}
	fake.setProbeErr(oomErr)
	q := NewRedisQueue(fake, RedisOptions{PublishRetryPause: time.Nanosecond})
	r := NewReliab(fake, q, ReliabOptions{MinIdle: time.Minute})

	ctx, cancel := context.WithCancel(context.Background())
	processed := make(chan Delivery, 4)
	done := make(chan error, 1)
	go func() {
		done <- RecoverLoop(ctx, r, func(_ context.Context, d Delivery) error {
			processed <- d
			return nil
		}, RecoverOptions{
			Stream: StreamHTTP, Group: "http", Consumer: "recover-1",
			Batch: 4, Interval: 40 * time.Millisecond,
		})
	}()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("RecoverLoop не остановился по отмене ctx")
		}
	}
	defer stop()

	// OOM-окно: несколько тиков — пробы падают, клейма нет, обработка
	// не зовётся.
	time.Sleep(250 * time.Millisecond)
	if n := fake.setCount(); n < 2 {
		t.Fatalf("проб записи %d, хочу ≥2 (гейт вообще не опрашивает пробу)", n)
	}
	if n := fake.claimCount(); n != 0 {
		t.Fatalf("клеймов при недоступной записи: %d, хочу 0 (попытки не расходуются)", n)
	}
	select {
	case d := <-processed:
		t.Fatalf("обработка при недоступной записи: %+v", d)
	default:
	}

	// Запись восстановилась: клейм берёт запись, обработка завершается
	// XACK — цикл доводит зависшие доставки.
	fake.setProbeErr(nil)
	select {
	case d := <-processed:
		if d.ID != "1718-0" {
			t.Errorf("обработана чужая доставка: %+v", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("после восстановления записи клейм не вернул доставку в работу")
	}
	if n := fake.claimCount(); n == 0 {
		t.Error("клейм после восстановления не выполнен")
	}
}

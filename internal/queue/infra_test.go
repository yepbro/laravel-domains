// Queue infrastructure error tests (T-152) without a live Redis: the
// classification of transport/server failures (infraCause), the
// InfraError marker through %w wrappers, XADD publication retries on an
// infrastructure failure, the Ack classification, the Reliab.Writable
// write probe (SET) and the RecoverLoop claim gate by the probe. The
// real OOM semantics (noeviction, XPENDING RetryCount) — the integration
// test TestResilience_OOMDoesNotBurnAttempts.

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

// testRedisError — a Redis server error as a type (redis.Error): real
// go-redis answers are typed proto.RedisError, the string fallback of
// the Is* helpers is only for bare strings.
type testRedisError string

func (e testRedisError) Error() string { return string(e) }
func (e testRedisError) RedisError()   {}

// oomErr — an OOM error as from the incident journals of Oct 22–23 (the
// redis.Error type, as go-redis returns it).
var oomErr error = testRedisError("OOM command not allowed when used memory > 'maxmemory'")

// TestInfraCause — the infrastructure failure classification: server
// answers about its own unavailability (OOM/LOADING/READONLY) and
// transport (a connection drop, a socket timeout, the pool) are
// infrastructure; processing errors, answers about the command itself,
// the caller's context cancellation/deadline are not.
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

// TestInfraErrorIsInfra — the marker passes through the workers' %w
// wrapper chains (Processing → OnResult → Publish) and does not arise
// for processing errors.
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

// TestPublishRetryDefaults — literal pins of the constants and the
// normalization of options (a comparison against the constant does not
// notice the mutant).
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

// TestPublishRetriesInfra — an XADD retry on an infrastructure failure
// (T-152): the result is already computed, a short retry series closes
// the flap; a persistent failure — InfraError after the retries are
// exhausted; a non-infrastructure error is not retried; a ctx
// cancellation interrupts the pause.
func TestPublishRetriesInfra(t *testing.T) {
	msg := DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: "a.test"}}}

	// Two OOMs, the third call passes: the publication succeeds, 3 XADD
	// in total.
	fake := &fakeRedis{addErrs: []error{oomErr, oomErr}}
	q := NewRedisQueue(fake, RedisOptions{PublishRetryPause: time.Nanosecond})
	if _, err := q.Publish(context.Background(), StreamDNS, msg); err != nil {
		t.Fatalf("Publish после повторов: %v", err)
	}
	if len(fake.adds) != 3 {
		t.Errorf("вызовов XADD: %d, хочу 3 (1 + 2 повтора)", len(fake.adds))
	}

	// A persistent OOM: InfraError, the retries are exhausted
	// (1 + PublishRetries).
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

	// A non-infrastructure XADD error — without retries (a retry would
	// give the same answer).
	fake = &fakeRedis{addErr: errors.New("ERR wrong number of arguments")}
	q = NewRedisQueue(fake, RedisOptions{PublishRetryPause: time.Nanosecond})
	_, err = q.Publish(context.Background(), StreamDNS, msg)
	if err == nil || IsInfra(err) {
		t.Fatalf("не-инфраструктурная ошибка: %v", err)
	}
	if len(fake.adds) != 1 {
		t.Errorf("вызовов XADD: %d, хочу 1 (повтора нет)", len(fake.adds))
	}

	// A ctx cancellation in the pause between retries: no new XADD, the
	// error is infrastructure (the publication did not happen).
	fake = &fakeRedis{addErr: oomErr}
	q = NewRedisQueue(fake, RedisOptions{PublishRetryPause: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := q.Publish(ctx, StreamDNS, msg)
		done <- err
	}()
	time.Sleep(50 * time.Millisecond) // the first XADD failed, the loop is in the pause
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

// TestAckInfraError — an XACK on an infrastructure failure is
// classified by the marker (the result is already written — this is not
// a processing error), any other error keeps the old text without the
// marker.
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

// probeClaimRedis — fakeClaimRedis plus a SET write probe (T-152): a
// switchable SET error and a thread-safe observation of claims (the loop
// claims in its own goroutine — access under a mutex, the
// pacingClaimRedis style).
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

// TestReliabWritable — the write probe: a client with SET answers by the
// probe's error, a client without SET (the unit-test fakes) — true (the
// gate is off, the pre-T-152 behavior).
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

	// Without the probe (the client cannot SET) the gate is transparent.
	r2 := NewReliab(&fakeClaimRedis{}, NewRedisQueue(&fakeClaimRedis{}, RedisOptions{}), ReliabOptions{})
	if !r2.Writable(context.Background()) {
		t.Error("Writable без пробы = false (гейт обязан быть прозрачным)")
	}
}

// TestRecoverLoopGatedByProbe — the claim gate (T-152): while the write
// probe fails (OOM), RecoverLoop does not claim — delivery attempts are
// not spent; Redis recovered — the claim and processing continue.
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

	// The OOM window: several ticks — the probes fail, no claims,
	// processing is not called.
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

	// Redis recovered: the claim takes the entry, the processing
	// finishes with an XACK — the loop carries the stalled deliveries
	// through.
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

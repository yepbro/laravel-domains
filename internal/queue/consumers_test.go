// Consumer group hygiene tests (T-155) without a live Redis (the
// trim_test.go style): a fake with prepared XINFO CONSUMERS / XPENDING
// EXTENDED / XGROUP DELCONSUMER answers. Checked: the victim selection
// invariants (a consumer with pending is not deleted regardless of
// idleness; an empty inactive one is deleted; an active one is not
// touched), the pending re-check before deletion (the revival race), the
// reaction to a missing stream and group, a partial failure (valid
// groups are cleaned), the carried-away PEL detector, the option
// defaults and the loop termination on ctx cancellation. The real
// DELCONSUMER semantics — the integration tests T-032.

package queue

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// fakeConsumers — the consumerGroups window with prepared answers: the
// group's consumer list, the XPENDING answer for a candidate
// ("stream|group|name") and the number of PEL entries the deletion
// "carried away". The calls are recorded for argument checks; the
// methods are also called from the background loop — access to the
// journals under a mutex (the fakeTrim style, correct under -race).
type fakeConsumers struct {
	lists    map[string][]redis.XInfoConsumer // the "stream|group" key
	listsErr map[string]error
	pendRes  map[string][]redis.XPendingExt // the XPENDING answer for a candidate
	pendErr  map[string]error
	delPEL   int64 // the DELCONSUMER answer: PEL entries carried away with the consumer

	mu       sync.Mutex
	pendArgs []redis.XPendingExtArgs
	dels     []string // "stream|group|consumer" in deletion order
}

func (f *fakeConsumers) XInfoConsumers(ctx context.Context, key, group string) *redis.XInfoConsumersCmd {
	cmd := redis.NewXInfoConsumersCmd(ctx, key, group)
	cmd.SetVal(f.lists[key+"|"+group])
	if err, ok := f.listsErr[key+"|"+group]; ok {
		cmd.SetErr(err)
	}
	return cmd
}

func (f *fakeConsumers) XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	f.mu.Lock()
	f.pendArgs = append(f.pendArgs, *a)
	f.mu.Unlock()
	cmd := redis.NewXPendingExtCmd(ctx, "xpending")
	key := a.Stream + "|" + a.Group + "|" + a.Consumer
	cmd.SetVal(f.pendRes[key])
	if err, ok := f.pendErr[key]; ok {
		cmd.SetErr(err)
	}
	return cmd
}

func (f *fakeConsumers) XGroupDelConsumer(ctx context.Context, stream, group, consumer string) *redis.IntCmd {
	f.mu.Lock()
	f.dels = append(f.dels, stream+"|"+group+"|"+consumer)
	f.mu.Unlock()
	return redis.NewIntResult(f.delPEL, nil)
}

// delCalls — a snapshot of the deletions (a copy — for checks outside
// the mutex).
func (f *fakeConsumers) delCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.dels))
	copy(out, f.dels)
	return out
}

// Compile-time checks: the fake is interchangeable with the real client
// in the hygiene window.
var _ consumerGroups = (*redis.Client)(nil)
var _ consumerGroups = (*fakeConsumers)(nil)

// TestCleanupConsumersOnceSelection — the victim selection invariants of
// one pass: a consumer with pending is not deleted regardless of
// idleness (DELCONSUMER with pending loses the PEL — an undelivery
// forever); an empty one, inactive longer than the threshold — deleted;
// an active one (idle below the threshold) — not touched; before the
// deletion the candidate's PEL emptiness is confirmed.
func TestCleanupConsumersOnceSelection(t *testing.T) {
	idle := 2 * time.Hour
	f := &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamHTTP + "|http-worker": {
				{Name: "http-worker-455745", Pending: 430, Idle: 72 * time.Hour}, // dead with dangling pending
				{Name: "http-worker-461002", Pending: 0, Idle: 26 * time.Hour},   // dead and empty
				{Name: "http-worker-461003", Pending: 0, Idle: 30 * time.Second}, // alive and active
			},
		},
		pendRes: map[string][]redis.XPendingExt{},
	}
	removed, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{StreamHTTP: "http-worker"},
		Idle:   idle,
	})
	if err != nil {
		t.Fatalf("CleanupConsumersOnce: %v", err)
	}
	if removed != 1 {
		t.Errorf("удалено %d, хочу 1 (только пустой неактивный)", removed)
	}
	dels := f.delCalls()
	if len(dels) != 1 || dels[0] != StreamHTTP+"|http-worker|http-worker-461002" {
		t.Fatalf("DELCONSUMER %v, хочу только пустой неактивный http-worker-461002", dels)
	}
	if len(f.pendArgs) != 1 || f.pendArgs[0].Consumer != "http-worker-461002" {
		t.Errorf("перепроверка pending: %+v, хочу одну по кандидату http-worker-461002", f.pendArgs)
	}
}

// TestCleanupConsumersOnceRaceyPEL — the PEL emptiness re-check before
// deletion: a consumer that received deliveries between XINFO CONSUMERS
// (empty) and XPENDING (already holding entries) is not deleted — the
// revival race does not lose the PEL.
func TestCleanupConsumersOnceRaceyPEL(t *testing.T) {
	f := &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamRender + "|render-worker": {
				{Name: "render-worker-101", Pending: 0, Idle: 3 * time.Hour},
			},
		},
		// A "revival" between XINFO and the re-check: the PEL is no
		// longer empty.
		pendRes: map[string][]redis.XPendingExt{
			StreamRender + "|render-worker|render-worker-101": {{ID: "5-0"}},
		},
	}
	removed, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{StreamRender: "render-worker"},
		Idle:   2 * time.Hour,
	})
	if err != nil {
		t.Fatalf("CleanupConsumersOnce: %v", err)
	}
	if removed != 0 {
		t.Errorf("удалено %d, хочу 0 (перепроверка увидела pending)", removed)
	}
	if dels := f.delCalls(); len(dels) != 0 {
		t.Errorf("DELCONSUMER при непустой перепроверке: %v", dels)
	}
}

// TestCleanupConsumersOnceCarriedPEL — the detector of a fired race:
// DELCONSUMER answered with the number of carried-away PEL entries
// (> 0) — the pass returns an error for the loop's Warn log, the
// consumer is nevertheless deleted.
func TestCleanupConsumersOnceCarriedPEL(t *testing.T) {
	f := &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamDNS + "|dns-worker": {
				{Name: "dns-worker-9", Pending: 0, Idle: 5 * time.Hour},
			},
		},
		pendRes: map[string][]redis.XPendingExt{},
		delPEL:  2,
	}
	removed, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{StreamDNS: "dns-worker"},
		Idle:   2 * time.Hour,
	})
	if err == nil {
		t.Fatal("унесённый DELCONSUMER-ом pending не возвращён ошибкой")
	}
	if removed != 1 {
		t.Errorf("удалено %d, хочу 1 (консьюмер удалён, ошибка — сигнал)", removed)
	}
}

// TestCleanupConsumersOnceNoStreamAndNoGroup — a stream without a key
// (not created yet) and a group without a key (lazy creation at the
// first read): an empty pass without an error, DELCONSUMER is not
// executed.
func TestCleanupConsumersOnceNoStreamAndNoGroup(t *testing.T) {
	f := &fakeConsumers{
		listsErr: map[string]error{
			StreamHTTP + "|http-worker": redis.Nil,
			StreamDNS + "|dns-worker":   errors.New("NOGROUP No such consumer group 'dns-worker' for key name 'q:dns'"),
		},
	}
	removed, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{
			StreamHTTP: "http-worker",
			StreamDNS:  "dns-worker",
		},
	})
	if err != nil {
		t.Fatalf("CleanupConsumersOnce: %v (нет стрима/группы — не ошибка)", err)
	}
	if removed != 0 {
		t.Errorf("удалено %d, хочу 0", removed)
	}
	if dels := f.delCalls(); len(dels) != 0 {
		t.Errorf("DELCONSUMER без стрима/группы: %v", dels)
	}
}

// TestCleanupConsumersOncePartialFailure — one group's error (a list
// read, a pending re-check, a deletion) does not cancel the others: the
// valid ones are cleaned, the errors are collected into the return.
func TestCleanupConsumersOncePartialFailure(t *testing.T) {
	f := &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamHTTP + "|http-worker": {
				{Name: "http-worker-1", Pending: 0, Idle: 3 * time.Hour},
			},
			StreamRender + "|render-worker": {
				{Name: "render-worker-1", Pending: 0, Idle: 3 * time.Hour},
			},
			StreamAnalyze + "|analyzer": {
				{Name: "analyzer-1", Pending: 0, Idle: 3 * time.Hour},
			},
		},
		listsErr: map[string]error{
			StreamDNS + "|dns-worker": errors.New("connection reset"),
		},
		pendErr: map[string]error{
			StreamRender + "|render-worker|render-worker-1": errors.New("LOADING Redis is loading the dataset in memory"),
		},
		delPEL: 0,
	}
	removed, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{})
	if err == nil {
		t.Fatal("частичный сбой не возвращён ошибкой")
	}
	if removed != 2 {
		t.Errorf("удалено %d, хочу 2 (q:http и q:analyze; q:dns сбой, q:render не подтверждён)", removed)
	}
	dels := f.delCalls()
	want := []string{
		StreamAnalyze + "|analyzer|analyzer-1",
		StreamHTTP + "|http-worker|http-worker-1",
	}
	if !reflect.DeepEqual(dels, want) {
		t.Fatalf("DELCONSUMER %v, хочу %v (сортировка стримов, сбойные пропущены)", dels, want)
	}
}

// TestCleanupConsumersOnceUnknownStreamAndNilClient — an unknown stream
// name in the group map and an empty group — errors; a nil client — an
// error.
func TestCleanupConsumersOnceUnknownStreamAndNilClient(t *testing.T) {
	f := &fakeConsumers{}
	if _, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{"q:unknown": "g"},
	}); err == nil {
		t.Error("неизвестный стрим не отвергнут")
	}
	if _, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{StreamDNS: ""},
	}); err == nil {
		t.Error("пустая группа не отвергнута")
	}
	if _, err := CleanupConsumersOnce(context.Background(), nil, ConsumerOptions{}); err == nil {
		t.Error("nil-клиент не отвергнут")
	}
	if dels := f.delCalls(); len(dels) != 0 {
		t.Errorf("DELCONSUMER при ошибках аргументов: %v", dels)
	}
}

// TestConsumerOptionsDefaults — zero Groups/Idle/Interval are replaced
// with defaults: the working task pairs (WorkingGroups — all five
// working streams, without q:dead), DefaultConsumerIdle and
// DefaultConsumerInterval. The pin: a new working stream must get into
// WorkingGroups, otherwise its groups are silently not cleaned; the
// match of the group names to the worker constants is checked by
// consumers_pin_test.go.
func TestConsumerOptionsDefaults(t *testing.T) {
	o := ConsumerOptions{}.withDefaults()
	if !reflect.DeepEqual(o.Groups, WorkingGroups()) {
		t.Errorf("Groups по умолчанию = %v, хочу %v (WorkingGroups)", o.Groups, WorkingGroups())
	}
	if len(o.Groups) != len(workingStreams()) {
		t.Errorf("WorkingGroups покрывает %d стримов, рабочих — %d: новый стрим добавьте в WorkingGroups", len(o.Groups), len(workingStreams()))
	}
	for s := range o.Groups {
		if !knownStream(s) || s == StreamDead {
			t.Errorf("WorkingGroups содержит неподходящий стрим %q", s)
		}
	}
	if o.Idle != DefaultConsumerIdle {
		t.Errorf("Idle по умолчанию = %s, хочу %s", o.Idle, DefaultConsumerIdle)
	}
	if o.Interval != DefaultConsumerInterval {
		t.Errorf("Interval по умолчанию = %s, хочу %s", o.Interval, DefaultConsumerInterval)
	}
	o = ConsumerOptions{
		Groups:   map[string]string{StreamHTTP: "http-worker"},
		Idle:     time.Minute,
		Interval: time.Hour,
	}.withDefaults()
	if o.Idle != time.Minute || o.Interval != time.Hour || !reflect.DeepEqual(o.Groups, map[string]string{StreamHTTP: "http-worker"}) {
		t.Errorf("явные значения заменены дефолтами: %+v", o)
	}
}

// TestCleanupConsumersLoopCancel — the loop makes the first pass
// immediately (the deletion is recorded in the fake's journal) and
// finishes with nil on ctx cancellation (the OnRun contract, T-012),
// without waiting for a tick.
func TestCleanupConsumersLoopCancel(t *testing.T) {
	f := &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamDNS + "|dns-worker": {
				{Name: "dns-worker-1", Pending: 0, Idle: 3 * time.Hour},
			},
		},
		pendRes: map[string][]redis.XPendingExt{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- CleanupConsumersLoop(ctx, f, ConsumerOptions{
			Groups:   map[string]string{StreamDNS: "dns-worker"},
			Idle:     2 * time.Hour,
			Interval: time.Hour,
		})
	}()
	// The first pass of the asynchronous loop: recorded by a DELCONSUMER
	// call.
	deadline := time.After(2 * time.Second)
	for len(f.delCalls()) == 0 {
		select {
		case <-deadline:
			t.Fatal("первый проход CleanupConsumersLoop не выполнен")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("CleanupConsumersLoop после отмены: %v, хочу nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CleanupConsumersLoop не завершился после отмены ctx")
	}
}

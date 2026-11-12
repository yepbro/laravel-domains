// Tests of the stuck delivery return loop (RecoverLoop, T-025) without a
// live Redis: the reliab_test.go fake (a one-shot claim — like a live
// PEL, the idle is reset). Covered: claim → processing → XACK, poisonous
// processing without acknowledgement, the drain of a full batch without
// waiting for a tick, moving the attempts-exhausted entries to q:dead
// (the handler is not called), the batch/interval walk pace (T-153), the
// option defaults, a clean exit on ctx cancel.

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

// runRecoverLoop — RecoverLoop in a goroutine; returns the channel of
// processed deliveries, the result channel and the stop function.
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

// sameAcks — a comparison of the XACK protocol (a slice inside a struct
// is not directly comparable).
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

// waitProcessed — the first processed delivery or a timeout.
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
	stop() // waiting for the exit — inside runRecoverLoop

	// The claim on behalf of the loop's consumer, the XACK — after
	// processing.
	if len(fake.claims) != 1 {
		t.Fatalf("вызовов XAUTOCLAIM: %d, хочу 1", len(fake.claims))
	}
	c := fake.claims[0]
	if c.Stream != StreamHTTP || c.Group != "http" || c.Consumer != "recover-1" || c.Count != 4 {
		t.Errorf("XAUTOCLAIM: стрим/группа/потребитель/пачка: %+v", c)
	}
	// A minIdle of 0 from the loop — the reliability layer's option (the
	// worker's MinIdle).
	if c.MinIdle != time.Minute {
		t.Errorf("XAUTOCLAIM MinIdle: %s, хочу 1m (из ReliabOptions теста)", c.MinIdle)
	}
	if want := []ackCall{{StreamHTTP, "http", []string{"1718-0"}}}; !sameAcks(fake.acks, want) {
		t.Errorf("XACK после обработки: %+v, хочу %+v", fake.acks, want)
	}
	// A working entry does not go to q:dead.
	if len(fake.adds) != 0 {
		t.Errorf("публикаций в q:dead быть не должно: %+v", fake.adds)
	}
}

// TestRecoverLoopProcessError — a processing error: without an XACK (the
// entry will come back with the next claim), the loop is alive, a clean
// exit on cancel.
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
	// Let the loop process the entry (the claim + Processing).
	time.Sleep(50 * time.Millisecond)
	stop()

	if len(fake.acks) != 0 {
		t.Errorf("XACK после сбоя обработки: %+v", fake.acks)
	}
	if len(fake.adds) != 0 {
		t.Errorf("сбой обработки не переводит запись в q:dead: %+v", fake.adds)
	}
}

// TestRecoverLoopDrainsFullBatch — a full claim batch is drained without
// waiting for a tick: at Batch 1 and an Interval of an hour both entries
// are processed back to back (the second — without a tick), an empty
// claim — then the wait.
func TestRecoverLoopDrainsFullBatch(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
			{ID: "1719-0", Values: map[string]any{fieldData: `{"task_id":"t2"}`}},
		},
		claimNext: "1720-0", // the next-scan cursor from the answer
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
	second := waitProcessed(t, processed) // the tick — an hour: the second only via the drain
	stop()
	if first.ID != "1718-0" || second.ID != "1719-0" {
		t.Errorf("дренаж: %+v затем %+v, хочу 1718-0 и 1719-0", first, second)
	}
	// The second claim continues from the first one's cursor, not from
	// the beginning of the PEL.
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

// TestRecoverLoopDead — an entry that exhausted its attempts does not
// get into processing: Claim moves it to q:dead and acknowledges it in
// the source stream.
func TestRecoverLoopDead(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 9}}, // the limit of 3 is exhausted
		},
	}

	processed, _, stop := runRecoverLoop(t, fake, RecoverOptions{
		Stream: StreamRender, Group: "render", Consumer: "recover-1",
		Batch: 4, Interval: time.Hour,
	}, nil)
	// Let the loop claim (the move to q:dead happens in Claim).
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

// recoverLogOpts — the loop options with a logger into a buffer (record
// checks).
func recoverLogOpts(buf *bytes.Buffer, interval time.Duration) RecoverOptions {
	return RecoverOptions{
		Stream: StreamHTTP, Group: "http", Consumer: "recover-1",
		Batch: 4, Interval: interval,
		Log: slog.New(slog.NewTextHandler(buf, nil)),
	}
}

// TestRecoverLoopLogs — a successful pass: the startup Info record is
// there, no warnings (there were no errors — a Warn with err=nil is not
// written).
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

// TestRecoverLoopLogsErrors — claim and processing errors are logged as
// Warn (the next tick will retry), the loop does not crash.
func TestRecoverLoopLogsErrors(t *testing.T) {
	// A claim error: a tick of 1 ms, let the loop make several passes (a
	// signal from the fake would require extending it; the style — as in
	// the neighboring TestRecoverLoopProcessError/TestRecoverLoopDead).
	fake := &fakeClaimRedis{claimErr: errors.New("ERR connection refused")}
	var buf bytes.Buffer
	_, _, stop := runRecoverLoop(t, fake, recoverLogOpts(&buf, time.Millisecond), nil)
	time.Sleep(50 * time.Millisecond) // at least one tick with an error
	stop()
	if !strings.Contains(buf.String(), "возврат зависших доставок: клейм") {
		t.Errorf("ошибка клейма не записана: %q", buf.String())
	}

	// A processing error: without an XACK, with a record about the return
	// by a claim. The Warn is written only with a live ctx — "signal →
	// cancel" would skip it under the ctx.Err() == nil condition; let the
	// loop run (the first claim and the Warn — in the first milliseconds),
	// the style — as in the neighboring
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

// TestRecoverLoopCanceledContext — a ctx cancel before the start: a
// claim error under an already canceled context is not logged as a
// warning (the shutdown instruction was carried out, not a failure), the
// exit is clean.
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

// TestRecoverLoopCanceledMidBatch — a ctx cancel in the middle of a
// batch: the current entry finishes its work or returns to the claim,
// the others do not start; a processing error under a canceled ctx is
// not logged as a warning.
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
	// release is never closed: the processing is released only by a ctx
	// cancel — deterministically the second entry does not start before
	// the cancel.
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
	<-started // the first entry in the work
	stop()    // a ctx cancel: the first returns with an error, the second does not start

	mu.Lock()
	defer mu.Unlock()
	if want := []string{"1718-0"}; !reflect.DeepEqual(handled, want) {
		t.Errorf("обработаны %v, хочу %v (после отмены пачка не продолжается)", handled, want)
	}
	if strings.Contains(buf.String(), "не подтверждена") {
		t.Errorf("ошибка обработки при отменённом ctx записана предупреждением: %q", buf.String())
	}
}

// TestRecoverLoopCancelRefundsUnprocessed — a ctx cancel in the middle
// of a batch (T-222, the task's criterion): XAUTOCLAIM increments the
// delivery counter of the whole batch at the moment of the claim, not
// the start of processing; the entries the walk did not reach get their
// attempt back — XCLAIM JUSTID RETRYCOUNT <attempts-1> (before T-222 the
// batch tail lost an attempt on every restart — the graceful shutdown
// budget is less than the batch walk time — and after max_attempts went
// to q:dead without a single processing).
func TestRecoverLoopCancelRefundsUnprocessed(t *testing.T) {
	// A batch of 4 entries, each counter after the claim — 2 (a delivery
	// + a claim): before the claim it was 1, the refund — RETRYCOUNT 1.
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1-1", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
			{ID: "1-2", Values: map[string]any{fieldData: `{"task_id":"t2"}`}},
			{ID: "1-3", Values: map[string]any{fieldData: `{"task_id":"t3"}`}},
			{ID: "1-4", Values: map[string]any{fieldData: `{"task_id":"t4"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1-1": {{ID: "1-1", RetryCount: 2}},
			"1-2": {{ID: "1-2", RetryCount: 2}},
			"1-3": {{ID: "1-3", RetryCount: 2}},
			"1-4": {{ID: "1-4", RetryCount: 2}},
		},
	}

	// The handler reworks two entries, the second one cancels the ctx
	// ("a SIGTERM between the batch entries"): the third and the fourth
	// do not start.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	handled := make([]string, 0, 4)
	done := make(chan error, 1)
	r := newTestReliab(fake, ReliabOptions{MinIdle: time.Minute})
	go func() {
		done <- RecoverLoop(ctx, r, func(_ context.Context, d Delivery) error {
			mu.Lock()
			handled = append(handled, d.ID)
			n := len(handled)
			mu.Unlock()
			if n == 2 {
				cancel()
			}
			return nil
		}, RecoverOptions{
			Stream: StreamHTTP, Group: "http", Consumer: "recover-1",
			Batch: 4, Interval: time.Hour,
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RecoverLoop после отмены: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RecoverLoop не остановился по отмене ctx")
	}

	mu.Lock()
	defer mu.Unlock()
	if want := []string{"1-1", "1-2"}; !reflect.DeepEqual(handled, want) {
		t.Fatalf("обработаны %v, хочу %v (после отмены пачка не продолжается)", handled, want)
	}
	// The T-222 criterion: the attempts of the unprocessed (4 − 2)
	// entries do not grow — one refund command for the batch tail with
	// the pre-claim counter.
	if len(fake.dos) != 1 {
		t.Fatalf("команд возврата: %d, хочу 1 (хвост 1-3, 1-4 одной группой)", len(fake.dos))
	}
	want := []interface{}{"xclaim", StreamHTTP, "http", "recover-1", int64(0), "1-3", "1-4", "justid", "retrycount", int64(1)}
	if got := fake.dos[0]; !reflect.DeepEqual(got, want) {
		t.Errorf("возврат:\n получил  %v\n хочу     %v", got, want)
	}
	// The entries reworked before the cancel: the first is acknowledged
	// before the cancel. The acknowledgement of the second goes over the
	// already canceled ctx and is not sent (the live client — like the
	// fake, the P3-4 review): the entry stays in the PEL unacknowledged —
	// the redelivery is idempotent (spec 3.4).
	if !sameAcks(fake.acks, []ackCall{
		{StreamHTTP, "http", []string{"1-1"}},
	}) {
		t.Errorf("XACK до отмены: %+v", fake.acks)
	}
}

// TestRecoverLoopRefundsUnclassified — a classification failure of one
// batch entry (T-260, FR-6.2): the XAUTOCLAIM claim spent the attempt,
// while the XPENDING counter read failed (transient). RecoverLoop
// refunds the attempt immediately in the same pass (RefundUnclassified:
// a repeated counter read + XCLAIM JUSTID RETRYCOUNT to the pre-claim
// value), the valid part of the batch is processed as usual. Before
// T-260 the attempt of such an entry burned without a trace — the T-222
// window ("ctx cancel") did not cover an unfinished classification.
func TestRecoverLoopRefundsUnclassified(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1-1", Values: map[string]any{fieldData: `{"task_id":"t1"}`}}, // the classification fails (once)
			{ID: "1-2", Values: map[string]any{fieldData: `{"task_id":"t2"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1-1": {{ID: "1-1", RetryCount: 2}}, // the counter after the claim
			"1-2": {{ID: "1-2", RetryCount: 2}},
		},
		pendFailOnce: map[string]error{
			"1-1": errors.New("ERR connection refused"), // only the classification
		},
	}

	processed, _, stop := runRecoverLoop(t, fake, RecoverOptions{
		Stream: StreamHTTP, Group: "http", Consumer: "recover-1",
		Batch: 4, Interval: time.Hour,
	}, nil)
	d := waitProcessed(t, processed) // the valid part of the batch — into the work
	if d.ID != "1-2" {
		t.Errorf("обработана доставка %s, хочу 1-2 (валидная часть пачки)", d.ID)
	}
	stop()

	// The attempt of 1-1 is refunded: the counter after the claim is 2,
	// the rollback to 1 — one command for the same group/consumer.
	if len(fake.dos) != 1 {
		t.Fatalf("команд возврата: %d, хочу 1 (запись без классификации)", len(fake.dos))
	}
	want := []interface{}{"xclaim", StreamHTTP, "http", "recover-1", int64(0), "1-1", "justid", "retrycount", int64(1)}
	if got := fake.dos[0]; !reflect.DeepEqual(got, want) {
		t.Errorf("возврат:\n получил  %v\n хочу     %v", got, want)
	}
	// The counter of 1-1 was read twice: the classification (a failure)
	// and the refund (a success).
	reads := 0
	for _, p := range fake.pends {
		if p.Start == "1-1" {
			reads++
		}
	}
	if reads != 2 {
		t.Errorf("чтений счётчика 1-1: %d, хочу 2 (классификация + возврат)", reads)
	}
	// The valid entry is processed and acknowledged; the unclassified
	// entry gets its attempt back — nothing goes to q:dead.
	if want := []ackCall{{StreamHTTP, "http", []string{"1-2"}}}; !sameAcks(fake.acks, want) {
		t.Errorf("XACK валидной записи: %+v, хочу %+v", fake.acks, want)
	}
	if len(fake.adds) != 0 {
		t.Errorf("публикаций в q:dead быть не должно: %+v", fake.adds)
	}
}

// TestRecoverLoopUnclassifiedOutOfInvariantSkipsRefund — the
// classification takes longer than claim_min_idle (the T-153 invariant
// violated): an entry without a counter could have been re-claimed by a
// neighboring worker during the classification, and the refund would
// undo a foreign increment — the refund is skipped, a Warn (the mirror
// of TestRecoverLoopCancelOutOfInvariantSkipsRefund for the FR-6.2
// branch). A MinIdle of a nanosecond makes the "overtake"
// deterministic.
func TestRecoverLoopUnclassifiedOutOfInvariantSkipsRefund(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1-1", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1-1": {{ID: "1-1", RetryCount: 2}},
		},
		// The classification fails constantly: the refund must not happen
		// at all (the invariant guard — before the refund counter read).
		pendOneErr: map[string]error{"1-1": errors.New("ERR connection refused")},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var buf bytes.Buffer
	done := make(chan error, 1)
	r := newTestReliab(fake, ReliabOptions{MinIdle: time.Nanosecond})
	go func() {
		done <- RecoverLoop(ctx, r, func(context.Context, Delivery) error { return nil },
			recoverLogOpts(&buf, time.Hour))
	}()
	time.Sleep(50 * time.Millisecond) // the claim and the classification are done
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RecoverLoop после отмены: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RecoverLoop не остановился по отмене ctx")
	}
	if len(fake.dos) != 0 {
		t.Errorf("возврат вне инварианта: %+v, хочу 0 команд (чужой клейм не гасим)", fake.dos)
	}
	if !strings.Contains(buf.String(), "классификация дольше claim_min_idle") {
		t.Errorf("пропуск возврата вне инварианта не записан: %q", buf.String())
	}
}

// TestRecoverLoopCancelOutOfInvariantSkipsRefund — the batch walk takes
// longer than claim_min_idle (the T-153 invariant violated — a batch of
// render tarpits): the tail could have been re-claimed by a neighboring
// worker's RecoverLoop, and the refund would undo a foreign counter
// increment — the refund is skipped, a Warn, the behavior before T-222
// (the P2-1 review). A minimal MinIdle makes the "overtake"
// deterministic: the walk time is always above it.
func TestRecoverLoopCancelOutOfInvariantSkipsRefund(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1-1", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
			{ID: "1-2", Values: map[string]any{fieldData: `{"task_id":"t2"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1-1": {{ID: "1-1", RetryCount: 2}},
			"1-2": {{ID: "1-2", RetryCount: 2}},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var buf bytes.Buffer
	done := make(chan error, 1)
	// A MinIdle of a nanosecond: Claim is called with minIdle 0 → the
	// option is in effect, and time.Since(claimAt) by the cancel is
	// always above a nanosecond.
	r := newTestReliab(fake, ReliabOptions{MinIdle: time.Nanosecond})
	go func() {
		done <- RecoverLoop(ctx, r, func(_ context.Context, d Delivery) error {
			cancel() // the cancel on the first entry: the tail — the whole second entry
			return nil
		}, recoverLogOpts(&buf, time.Hour))
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RecoverLoop после отмены вне инварианта: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RecoverLoop не остановился по отмене ctx")
	}
	if len(fake.dos) != 0 {
		t.Errorf("возврат вне инварианта: %+v, хочу 0 команд (чужой клейм не гасим)", fake.dos)
	}
	if !strings.Contains(buf.String(), "возврат попыток пропущен") {
		t.Errorf("пропуск возврата вне инварианта не записан: %q", buf.String())
	}
}

// TestRecoverLoopCancelRefundFails — a refund failure (Redis unavailable
// at shutdown) does not crash the loop: a Warn with context, a clean
// exit; the tail's attempts stay spent — best effort, no worse than the
// behavior before T-222.
func TestRecoverLoopCancelRefundFails(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1-1", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
			{ID: "1-2", Values: map[string]any{fieldData: `{"task_id":"t2"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1-1": {{ID: "1-1", RetryCount: 2}},
			"1-2": {{ID: "1-2", RetryCount: 2}},
		},
		doErr: errors.New("ERR connection refused"),
	}

	ctx, cancel := context.WithCancel(context.Background())
	var buf bytes.Buffer
	done := make(chan error, 1)
	r := newTestReliab(fake, ReliabOptions{MinIdle: time.Minute})
	go func() {
		done <- RecoverLoop(ctx, r, func(_ context.Context, d Delivery) error {
			cancel() // the cancel on the first entry: the tail — the whole second entry
			return nil
		}, recoverLogOpts(&buf, time.Hour))
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RecoverLoop после отмены со сбойным возвратом: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RecoverLoop не остановился по отмене ctx")
	}
	if !strings.Contains(buf.String(), "не возвращены") {
		t.Errorf("сбой возврата попыток не записан: %q", buf.String())
	}
	if len(fake.dos) != 1 {
		t.Errorf("попыток возврата: %d, хочу 1 (best effort — одна команда)", len(fake.dos))
	}
}

// TestRecoverLoopValidation — a nil layer and handler: an error before
// the start (the service assembly fails clearly, Redis is not touched).
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

// TestRecoverOptionsDefaults — zero options: the default batch and
// interval (literal pins of the constants — comparing with the constant
// itself, the mutant goes unnoticed), single values are not replaced.
func TestRecoverOptionsDefaults(t *testing.T) {
	o := RecoverOptions{}.withDefaults()
	if DefaultRecoverBatch != 64 {
		t.Errorf("DefaultRecoverBatch = %d, хочу 64 (T-153: темп обхода PEL; литерал — пин константы)", DefaultRecoverBatch)
	}
	if DefaultRecoverInterval != 30*time.Second {
		t.Errorf("DefaultRecoverInterval = %s, хочу 30s (литерал — пин константы)", DefaultRecoverInterval)
	}
	if o.Batch != 64 {
		t.Errorf("Batch = %d, хочу 64 (DefaultRecoverBatch, литерал — пин константы)", o.Batch)
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

// --- a handler with the delivery attempt number (T-148) ----------------------

// TestProcessingAttempt — the Processing pattern for a handler with the
// attempt number: the number is delivered as is, successful processing →
// XACK, an error → without an XACK (redelivery), a nil handler — an
// error.
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

	// A processing error — without an XACK, the attempt number is not
	// lost in the message.
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

	// A nil handler — an error before Redis.
	if err := ProcessingAttempt(context.Background(), NewRedisQueue(&fakeClaimRedis{}, RedisOptions{}), "g", d, 1, nil); err == nil {
		t.Error("nil обработчик: ожидалась ошибка")
	}
}

// TestRecoverLoopAttempt — RecoverLoopAttempt passes Claimed.Attempts
// (the entry's PEL delivery counter) to the handler: the handler sees
// which delivery this is by count, without its own state.
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

// TestRecoverLoopAttemptValidation — a nil handler is rejected by
// RecoverLoopAttempt before the start; RecoverLoop (the adapter) — still
// too.
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

// pacingClaimRedis — a fakeClaimRedis with thread-safe observation of
// the claims: the loop claims in its own goroutine, the pace test reads
// the counter on the fly (T-153) without stopping the loop (the
// observation style — like the processed channel of runRecoverLoop, not
// after stop). The embedded claims protocol is touched only by the
// loop's goroutine — the test reads only the fields under the mutex.
type pacingClaimRedis struct {
	fakeClaimRedis
	mu     sync.Mutex
	nclaim int
	count  int64 // the Count of the last XAUTOCLAIM (the batch from the options)
}

func (f *pacingClaimRedis) XAutoClaim(ctx context.Context, a *redis.XAutoClaimArgs) *redis.XAutoClaimCmd {
	f.mu.Lock()
	f.nclaim++
	f.count = a.Count
	f.mu.Unlock()
	return f.fakeClaimRedis.XAutoClaim(ctx, a)
}

func (f *pacingClaimRedis) claims() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.nclaim
}

// batch — the actual claim batch (the XAUTOCLAIM Count from the
// options).
func (f *pacingClaimRedis) batch() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.count
}

// TestRecoverLoopIntervalPaces — the walk pace (T-153): an incomplete
// batch — the next claim no earlier than the Interval (a tick), a full
// one — drained without a pause (TestRecoverLoopDrainsFullBatch). The
// loop is assembled manually, not by runRecoverLoop: the helper is
// typed for *fakeClaimRedis, and observing the claims on the fly
// requires overriding XAutoClaim (the pacing fake).
func TestRecoverLoopIntervalPaces(t *testing.T) {
	fake := &pacingClaimRedis{fakeClaimRedis: fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 1}},
		},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	processed := make(chan Delivery, 2)
	done := make(chan error, 1)
	r := NewReliab(fake, NewRedisQueue(fake, RedisOptions{}), ReliabOptions{MinIdle: time.Minute})
	go func() {
		done <- RecoverLoop(ctx, r, func(_ context.Context, d Delivery) error {
			processed <- d
			return nil
		}, RecoverOptions{
			Stream: StreamHTTP, Group: "http", Consumer: "recover-1",
			Batch: 2, Interval: 120 * time.Millisecond,
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

	waitProcessed(t, processed) // the first batch: 1 entry < Batch 2 → a tick
	// The batch from the options is applied to the first claim
	// (Count = Batch).
	if c := fake.batch(); c != 2 {
		t.Errorf("XAUTOCLAIM Count = %d, хочу 2 (Batch из опций)", c)
	}
	time.Sleep(40 * time.Millisecond)
	if n := fake.claims(); n != 1 {
		t.Errorf("клеймов до тика: %d, хочу 1 (неполная пачка ждёт Interval, не клейм сразу)", n)
	}
	time.Sleep(400 * time.Millisecond) // > 2 ticks of 120 ms each
	if n := fake.claims(); n < 2 {
		t.Errorf("клеймов после тиков: %d, хочу ≥ 2 (тик задаёт темп, обход не прекращается)", n)
	}
}

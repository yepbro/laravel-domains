// Tests of releasing the acknowledged prefix of streams (T-141) without
// a live Redis (the redisq_test.go style): a fake with stubbed XINFO
// GROUPS / XPENDING / XTRIM responses. Covered: the choice of the trim
// point (the group's oldest pending; with an empty pending —
// last-delivered-id; the minimum across several groups with a numeric id
// comparison), skipping a stream without groups and without a key (the
// q:dead case), read errors (an unsafe pass is skipped entirely, the
// other streams are released), the removed total and the option
// defaults. The real XTRIM MINID semantics and the memory degradation —
// the integration test TestResilience_TrimConfirmedPrefix (T-141).

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

// fakeTrim — a trimStreams window with stubbed responses: the stream's
// groups, the per-group XPENDING summary (the oldest pending) and the
// XTRIM result. Calls are recorded for argument checks; the methods are
// also invoked from the background TrimLoop loop — access to the call
// logs is under a mutex (the recover_test.go style, correct under
// -race).
type fakeTrim struct {
	groups    map[string][]redis.XInfoGroup
	groupsErr map[string]error
	pend      map[string]redis.XPending // the "stream|group" key
	pendErr   map[string]error
	xlen      map[string]int64 // the stream length (the stuck prefix signal)
	trimCount int64            // the XTRIM response (the removed count)

	mu    sync.Mutex
	infos []string            // the XINFO GROUPS keys in call order
	pends []string            // the XPENDING keys in call order
	trims []map[string]string // XTRIM: {"stream": …, "minid": …}
}

func (f *fakeTrim) XInfoGroups(ctx context.Context, key string) *redis.XInfoGroupsCmd {
	f.mu.Lock()
	f.infos = append(f.infos, key)
	f.mu.Unlock()
	cmd := redis.NewXInfoGroupsCmd(ctx, key)
	cmd.SetVal(f.groups[key])
	if err, ok := f.groupsErr[key]; ok {
		cmd.SetErr(err)
	}
	return cmd
}

func (f *fakeTrim) XPending(ctx context.Context, stream, group string) *redis.XPendingCmd {
	f.mu.Lock()
	f.pends = append(f.pends, stream+"|"+group)
	f.mu.Unlock()
	cmd := redis.NewXPendingCmd(ctx, "xpending", stream, group)
	pend := f.pend[stream+"|"+group]
	cmd.SetVal(&pend)
	if err, ok := f.pendErr[stream+"|"+group]; ok {
		cmd.SetErr(err)
	}
	return cmd
}

func (f *fakeTrim) XTrimMinID(ctx context.Context, key, minID string) *redis.IntCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.trims == nil {
		f.trims = []map[string]string{}
	}
	f.trims = append(f.trims, map[string]string{"stream": key, "minid": minID})
	return redis.NewIntResult(f.trimCount, nil)
}

// trimCalls — a snapshot of the XTRIM calls (a copy — for checks outside
// the mutex).
func (f *fakeTrim) trimCalls() []map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]map[string]string, len(f.trims))
	copy(out, f.trims)
	return out
}

func (f *fakeTrim) XLen(ctx context.Context, key string) *redis.IntCmd {
	return redis.NewIntResult(f.xlen[key], nil)
}

// Compile-time checks: the fake is interchangeable with the real client
// in the release window.
var _ trimStreams = (*redis.Client)(nil)
var _ trimStreams = (*fakeTrim)(nil)

// TestTrimOncePendingKept — the trim point for a group with stuck
// entries: XTRIM MINID — the oldest pending (everything below is
// delivered and acknowledged, the pending itself and the tail above
// stay).
func TestTrimOncePendingKept(t *testing.T) {
	f := &fakeTrim{
		groups: map[string][]redis.XInfoGroup{
			StreamHTTP: {{Name: "http", Pending: 3, LastDeliveredID: "105-0"}},
		},
		pend: map[string]redis.XPending{
			StreamHTTP + "|http": {Count: 3, Lower: "101-0", Higher: "103-0"},
		},
		trimCount: 7,
	}
	n, _, err := TrimOnce(context.Background(), f, TrimOptions{Streams: []string{StreamHTTP}})
	if err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if n != 7 {
		t.Errorf("удалено %d, хочу 7 (ответ XTRIM)", n)
	}
	if len(f.trims) != 1 || f.trims[0]["stream"] != StreamHTTP || f.trims[0]["minid"] != "101-0" {
		t.Fatalf("XTRIM %+v, хочу MINID 101-0 в %s (старейшая pending)", f.trims, StreamHTTP)
	}
}

// TestTrimOnceEmptyPEL — an empty group pending: the point —
// last-delivered-id (everything below is delivered and acknowledged, the
// undelivered tail above stays).
func TestTrimOnceEmptyPEL(t *testing.T) {
	f := &fakeTrim{
		groups: map[string][]redis.XInfoGroup{
			StreamDNS: {{Name: "dns", Pending: 0, LastDeliveredID: "42-1"}},
		},
	}
	if _, _, err := TrimOnce(context.Background(), f, TrimOptions{Streams: []string{StreamDNS}}); err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if len(f.trims) != 1 || f.trims[0]["minid"] != "42-1" {
		t.Fatalf("XTRIM %+v, хочу MINID 42-1 (last-delivered-id)", f.trims)
	}
	if len(f.pends) != 0 {
		t.Errorf("XPENDING при пустом pending группы не нужен: %v", f.pends)
	}
}

// TestTrimOnceMinAcrossGroups — several groups: the point — the numeric
// minimum across groups ("999-0" < "1000-0", the lexicographic order is
// deceptive). A fresh group started at "0" (last-delivered-id "0-0", it
// has not read yet) holds the point at "0-0" — conservatively: trimming
// above would be a history loss for it (XTRIM MINID 0-0 removes nothing,
// TrimOnce simply does not release such a stream until the group makes
// progress).
func TestTrimOnceMinAcrossGroups(t *testing.T) {
	f := &fakeTrim{
		groups: map[string][]redis.XInfoGroup{
			StreamAnalyze: {
				{Name: "a", Pending: 1, LastDeliveredID: "1000-0"},
				{Name: "b", Pending: 0, LastDeliveredID: "999-0"},
			},
		},
		pend: map[string]redis.XPending{
			StreamAnalyze + "|a": {Count: 1, Lower: "1000-0"},
		},
	}
	if _, _, err := TrimOnce(context.Background(), f, TrimOptions{Streams: []string{StreamAnalyze}}); err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if len(f.trims) != 1 || f.trims[0]["minid"] != "999-0" {
		t.Fatalf("XTRIM %+v, хочу MINID 999-0 (минимум по группам)", f.trims)
	}

	f = &fakeTrim{
		groups: map[string][]redis.XInfoGroup{
			StreamAnalyze: {
				{Name: "a", Pending: 0, LastDeliveredID: "50-0"},
				{Name: "fresh", Pending: 0, LastDeliveredID: "0-0"},
			},
		},
	}
	if _, _, err := TrimOnce(context.Background(), f, TrimOptions{Streams: []string{StreamAnalyze}}); err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if len(f.trims) != 1 || f.trims[0]["minid"] != "0-0" {
		t.Fatalf("XTRIM %+v, хочу MINID 0-0 (свежая группа держит точку — история ей ещё не доставлена)", f.trims)
	}
}

// TestTrimOnceNoGroupsAndNoKey — a stream without groups (the q:dead
// case: no groups — there is no acknowledged prefix) and a nonexistent
// key: an empty pass without an error, XTRIM is not run.
func TestTrimOnceNoGroupsAndNoKey(t *testing.T) {
	f := &fakeTrim{
		groups: map[string][]redis.XInfoGroup{StreamDead: {}},
		groupsErr: map[string]error{
			StreamRender: redis.Nil,
			StreamDNS:    errors.New("ERR no such key"),
		},
	}
	n, stuck, err := TrimOnce(context.Background(), f, TrimOptions{
		Streams: []string{StreamDead, StreamRender, StreamDNS},
	})
	if err != nil {
		t.Fatalf("TrimOnce: %v (стрим без групп/ключа — не ошибка)", err)
	}
	if n != 0 {
		t.Errorf("удалено %d, хочу 0", n)
	}
	if len(stuck) != 0 {
		t.Errorf("застрявшие без групп/ключа: %v", stuck)
	}
	if len(f.trims) != 0 {
		t.Errorf("XTRIM по стриму без групп/ключа: %+v", f.trims)
	}
}

// TestTrimOncePendingErrorSkipsStream — a PEL read error makes trimming
// by last-delivered-id unsafe (pending may sit below it): the stream is
// skipped with an error, the others are released.
func TestTrimOncePendingErrorSkipsStream(t *testing.T) {
	f := &fakeTrim{
		groups: map[string][]redis.XInfoGroup{
			StreamHTTP: {{Name: "http", Pending: 2, LastDeliveredID: "10-0"}},
			StreamAnalyze: {
				{Name: "an", Pending: 0, LastDeliveredID: "5-0"},
			},
		},
		pendErr: map[string]error{
			StreamHTTP + "|http": errors.New("connection reset"),
		},
	}
	n, _, err := TrimOnce(context.Background(), f, TrimOptions{
		Streams: []string{StreamHTTP, StreamAnalyze},
	})
	if err == nil {
		t.Fatal("ошибка XPENDING не возвращена")
	}
	if n != 0 {
		t.Errorf("удалено %d, хочу 0 (сбойный стрим пропущен)", n)
	}
	if len(f.trims) != 1 || f.trims[0]["stream"] != StreamAnalyze {
		t.Fatalf("XTRIM %+v, хочу только %s (валидная часть отрабатывает)", f.trims, StreamAnalyze)
	}
}

// TestTrimOnceUnknownStreamAndNilClient — an unknown stream name and a
// nil client — errors; XTRIM is not run.
func TestTrimOnceUnknownStreamAndNilClient(t *testing.T) {
	f := &fakeTrim{}
	if _, _, err := TrimOnce(context.Background(), f, TrimOptions{Streams: []string{"q:unknown"}}); err == nil {
		t.Error("неизвестный стрим не отвергнут")
	}
	if _, _, err := TrimOnce(context.Background(), nil, TrimOptions{}); err == nil {
		t.Error("nil-клиент не отвергнут")
	}
	if len(f.trims) != 0 {
		t.Errorf("XTRIM при ошибках аргументов: %+v", f.trims)
	}
}

// TestTrimOnceStuckSignal — the stuck prefix signal: a stream length
// (XLEN) ≥ 3/4 of the limit with live groups — the trim point is frozen
// (the minimum across groups is held by an orphaned group), the
// acknowledged prefix accumulates, while the lag+pending depth is
// silent; below the threshold and with an explicit MaxLen from the
// options — no signal / computed from the explicit limit.
func TestTrimOnceStuckSignal(t *testing.T) {
	f := &fakeTrim{
		groups: map[string][]redis.XInfoGroup{
			StreamAnalyze: {{Name: "an", Pending: 0, LastDeliveredID: "5-0"}},
		},
		pend: map[string]redis.XPending{},
		xlen: map[string]int64{StreamAnalyze: 120_000}, // 3/4 of 150,000 = 112,500
	}
	_, stuck, err := TrimOnce(context.Background(), f, TrimOptions{Streams: []string{StreamAnalyze}})
	if err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if len(stuck) != 1 || stuck[0] != StreamAnalyze {
		t.Fatalf("застрявшие = %v, хочу [%s] (XLEN 120 000 ≥ ¾ предела 150 000)", stuck, StreamAnalyze)
	}

	// Below the threshold — no signal.
	f.xlen[StreamAnalyze] = 100_000
	if _, stuck, _ := TrimOnce(context.Background(), f, TrimOptions{Streams: []string{StreamAnalyze}}); len(stuck) != 0 {
		t.Errorf("застрявшие при XLEN 100 000 < 112 500: %v", stuck)
	}

	// An explicit limit from the options: the threshold is computed from
	// it (gate-like, BackpressureOptions.MaxLen).
	if _, stuck, _ := TrimOnce(context.Background(), f, TrimOptions{
		Streams: []string{StreamAnalyze}, MaxLen: 120_000,
	}); len(stuck) != 1 || stuck[0] != StreamAnalyze {
		t.Errorf("застрявшие при явном MaxLen 120 000 и XLEN 100 000: %v, хочу [%s]", stuck, StreamAnalyze)
	}
}

// TestTrimOptionsDefaults — zero Streams/Interval are replaced with the
// defaults: the working task streams (workingStreams — everything
// knownStream knows, except q:dead) and DefaultTrimInterval. A pin test:
// a new working stream must land in workingStreams, otherwise it is
// silently never released.
func TestTrimOptionsDefaults(t *testing.T) {
	o := TrimOptions{}.withDefaults()
	want := workingStreams()
	if !reflect.DeepEqual(o.Streams, want) {
		t.Errorf("Streams по умолчанию = %v, хочу %v (без q:dead)", o.Streams, want)
	}
	for _, s := range want {
		if !knownStream(s) {
			t.Errorf("workingStreams содержит неизвестный стрим %q", s)
		}
	}
	if len(want) != 5 {
		t.Errorf("рабочих стримов задач %d (%v), хочу 5 — новый стрим добавьте в workingStreams", len(want), want)
	}
	if o.Interval != DefaultTrimInterval {
		t.Errorf("Interval по умолчанию = %s, хочу %s", o.Interval, DefaultTrimInterval)
	}
	o = TrimOptions{Streams: []string{StreamDead}, Interval: time.Nanosecond}.withDefaults()
	if !reflect.DeepEqual(o.Streams, []string{StreamDead}) || o.Interval != time.Nanosecond {
		t.Errorf("явные значения заменены дефолтами: %+v", o)
	}
}

// TestTrimLoopCancel — the loop makes the first pass immediately and
// finishes with nil on ctx cancellation (the OnRun contract, T-012),
// without waiting for a tick. The call log is read through the
// fakeTrim.trimCalls snapshot — the loop lives in another goroutine
// (access under the fake's mutex).
func TestTrimLoopCancel(t *testing.T) {
	f := &fakeTrim{
		groups: map[string][]redis.XInfoGroup{
			StreamDNS: {{Name: "dns", Pending: 0, LastDeliveredID: "1-0"}},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- TrimLoop(ctx, f, TrimOptions{Interval: time.Hour}) }()
	// The first pass of the asynchronous loop: the point is fixed by an
	// XTRIM call.
	deadline := time.After(2 * time.Second)
	for len(f.trimCalls()) == 0 {
		select {
		case <-deadline:
			t.Fatal("первый проход TrimLoop не выполнен")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("TrimLoop после отмены: %v, хочу nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("TrimLoop не завершился после отмены ctx")
	}
}

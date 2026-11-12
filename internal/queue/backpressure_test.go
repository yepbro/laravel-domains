// Backpressure gate tests (T-026) without a live Redis (the
// redisq_test.go style): the fake extends fakeRedis with the depth window
// redisDepth — XINFO GROUPS (group lag and pending) and XLEN (the
// fallback for "no groups" and the lag clamp, T-168). Checked: depth
// summing (lag + pending, an undefined lag, no groups → XLEN, the
// phantom lag clamp by XLEN — T-168), threshold boundaries
// (below/exactly/above), the threshold cap at half of the stream's
// MAXLEN, waiting with a retry after the interval, batch publication with
// a check before each message, ctx cancellation, read errors, q:dead and
// the disabled gate. The real XINFO GROUPS semantics — the integration
// tests T-032/T-168 (resilience_test.go).

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

// fakeBackpressure — fakeRedis (redisq_test.go) plus the depth window:
// seq — the XINFO GROUPS answers in the order of the checks (the last one
// repeats; a nil element — no groups), xlen — the XLEN answer (the depth
// without groups and the group lag clamp, T-168), log — the order of the
// Redis accesses (depth before xadd — the very subject of the check).
type fakeBackpressure struct {
	*fakeRedis
	seq       [][]redis.XInfoGroup
	groupsErr error
	xlen      int64
	xlenErr   error

	infos []string // the XINFO GROUPS keys
	xlens []string // the XLEN keys
	log   []string // the command order: "xinfo:<stream>" / "xlen:<stream>" / "xadd:<stream>"
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

// Compile-time checks: the fake is interchangeable with the real client
// in the depth window and in the queue window, the wrapper is a full
// Queue.
var (
	_ redisDepth   = (*redis.Client)(nil)
	_ redisDepth   = (*fakeBackpressure)(nil)
	_ redisStreams = (*fakeBackpressure)(nil)
	_ Queue        = (*WatermarkPublisher)(nil)
)

// grp — an XINFO GROUPS answer with one group (lag, pending).
func grp(lag, pending int64) []redis.XInfoGroup {
	return []redis.XInfoGroup{{Name: "g", Lag: lag, Pending: pending}}
}

// newFakeBackpressure — a fake with an initialized queue and a prepared
// sequence of depth answers (the last one repeats; nil — no groups).
func newFakeBackpressure(seq ...[]redis.XInfoGroup) *fakeBackpressure {
	return &fakeBackpressure{fakeRedis: &fakeRedis{}, seq: seq}
}

// newTestGate — a gate over the fake: the watermark threshold, a 1 ms
// retry interval.
func newTestGate(f *fakeBackpressure, watermark int64) *BackpressureGate {
	return NewBackpressureGate(f, BackpressureOptions{Watermark: watermark, Interval: time.Millisecond})
}

// newTestPublisher — a RedisQueue wrapper over the same fake: a gate with
// the threshold 100 against the depths from f.seq.
func newTestPublisher(f *fakeBackpressure) *WatermarkPublisher {
	return NewWatermarkPublisher(NewRedisQueue(f, RedisOptions{}), newTestGate(f, 100))
}

// dnsBatch — a valid q:dns message for one domain.
func dnsBatch() DNSBatch {
	return DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: "a.test"}}}
}

// TestNewBackpressureGateIntervalDefault — the retry interval: 0 and
// negative — DefaultBackpressureInterval, a positive nanosecond wait is
// valid and is not replaced.
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

// TestBackpressureStreamMaxLen — the stream cap: 0 and negative MaxLen —
// the DefaultStreamMaxLen policy, an explicit cap of 1 is valid and is
// not substituted by the policy.
func TestBackpressureStreamMaxLen(t *testing.T) {
	g := NewBackpressureGate(nil, BackpressureOptions{})
	if got := g.streamMaxLen(StreamDNS); got != 250 {
		t.Errorf("streamMaxLen без MaxLen = %d, хочу 250 (политика q:dns, T-141)", got)
	}
	if got := g.streamMaxLen(StreamHTTP); got != 150_000 {
		t.Errorf("streamMaxLen без MaxLen = %d, хочу 150000 (политика штучных стримов, T-141)", got)
	}
	g = NewBackpressureGate(nil, BackpressureOptions{MaxLen: 1})
	if got := g.streamMaxLen(StreamHTTP); got != 1 {
		t.Errorf("streamMaxLen при MaxLen=1 = %d, хочу 1 (явный предел валиден)", got)
	}
	g = NewBackpressureGate(nil, BackpressureOptions{MaxLen: -7})
	if got := g.streamMaxLen(StreamHTTP); got != 150_000 {
		t.Errorf("streamMaxLen при отрицательном MaxLen = %d, хочу 150000 (политика)", got)
	}
}

// TestStreamWatermark — the effective threshold: ≤0 disables the gate
// (0), maxLen ≤ 0 — the stream's policy cap, the threshold is the
// minimum of the watermark and half of the cap.
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
		{"предел политики q:dns", 100_000, 0, StreamDNS, 125},
		{"предел политики штучных", 600_000, 0, StreamHTTP, 75_000},
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

// TestGroupDepth — the common source of truth of the group depth
// (T-188): a pure function of lag/pending/XLEN; the gate (Len), the
// T-162 alert (monitor.RedisStreamGroups) and the "Queues" line of the
// load report (monitor.RedisQueues) sum it — the formulas must not
// diverge.
func TestGroupDepth(t *testing.T) {
	cases := []struct {
		name string
		lag  int64
		pend int64
		xlen int64
		want int64
	}{
		{"лаг с pending", 27409, 120, 136500, 27529},
		{"кламп фантомного лага по XLEN (T-168)", 1753, 0, 1, 1},
		{"лаг 0 — прибавка нуля", 0, 5, 100, 5},
		{"lag −1 (неопределён) — только pending", -1, 8, 3, 8},
		{"пустая группа", 0, 0, 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := GroupDepth(redis.XInfoGroup{Name: "g", Lag: c.lag, Pending: c.pend}, c.xlen); got != c.want {
				t.Errorf("GroupDepth(lag=%d, pending=%d, xlen=%d) = %d, хочу %d",
					c.lag, c.pend, c.xlen, got, c.want)
			}
		})
	}
}

func TestBackpressureLen(t *testing.T) {
	// The group depth — the lag (clamped by the stream's XLEN, T-168)
	// plus unacknowledged; several groups — summed (working streams have
	// one group, generalized — all of them). XLEN above the lags — the
	// clamp does not change the sum (the subject of this case is
	// summing).
	fake := newFakeBackpressure(
		[]redis.XInfoGroup{{Name: "a", Lag: 30, Pending: 12}, {Name: "b", Lag: 7, Pending: 3}},
	)
	fake.xlen = 1000
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
	if want := []string{StreamHTTP}; !reflect.DeepEqual(fake.xlens, want) {
		t.Errorf("XLEN ключи: %v, хочу %v (один вызов — кламп лага групп, T-168)", fake.xlens, want)
	}

	// lag is undefined (−1: the trim cut off the undelivered) — only
	// pending is counted, XLEN does not participate in the clamp.
	fake = newFakeBackpressure(grp(-1, 8))
	fake.xlen = 3
	gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(context.Background(), StreamHTTP); err != nil || n != 8 {
		t.Errorf("Len при неопределённом lag: (%d, %v), хочу (8, nil)", n, err)
	}

	// No groups — no entry has been delivered: the depth equals XLEN.
	fake = newFakeBackpressure(nil)
	fake.xlen = 34
	gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(context.Background(), StreamHTTP); err != nil || n != 34 {
		t.Errorf("Len без групп: (%d, %v), хочу XLEN (34, nil)", n, err)
	}
	if want := []string{StreamHTTP}; !reflect.DeepEqual(fake.xlens, want) {
		t.Errorf("XLEN ключи: %v, хочу %v", fake.xlens, want)
	}

	// The stream does not exist yet (redis.Nil / "no such key") — no
	// groups: depth without deliveries equals XLEN, the XINFO GROUPS
	// value on error is not used.
	for name, groupsErr := range map[string]error{
		"redis.Nil":   redis.Nil,
		"no such key": errors.New("ERR no such key"),
	} {
		fake = &fakeBackpressure{
			fakeRedis: &fakeRedis{},
			seq:       [][]redis.XInfoGroup{grp(5, 7)}, // the val is not used on err
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

	// Errors — a zero depth and the exact text.
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

	// An XLEN error with no groups is returned (the gate does not guess
	// the depth of a nonexistent key).
	fake = newFakeBackpressure(nil)
	fake.xlenErr = errors.New("ERR connection refused")
	gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(context.Background(), StreamDNS); err == nil || n != 0 ||
		!strings.Contains(err.Error(), "групп нет") {
		t.Errorf("Len с ошибкой XLEN: (%d, %v), хочу (0, ошибка XLEN)", n, err)
	}
}

// TestBackpressureLenClampsTrimmedLag — the lag clamp by XLEN (T-168):
// a group's lag (entries_added − entries_read) does not subtract entries
// cut off by XTRIM/XDEL while undelivered — on Redis 7.0.x (production
// 7.0.15) the counter freezes positive forever. The incident of
// Oct 22–23, 2026: after the T-149 drain (XTRIM q:dns MAXLEN) the
// dns-worker lag froze at 1753 with XLEN 1 — the q:dns gate (effective
// threshold 125) was frozen for ~15.5 h with 40.3M pending. Undelivered
// entries are a subset of the stream: depth = min(lag, XLEN) + pending,
// the real backlog is never understated (while the undelivered are in
// the stream, lag ≤ XLEN — the clamp is inactive).
func TestBackpressureLenClampsTrimmedLag(t *testing.T) {
	ctx := context.Background()

	// The production phantom: a lag of 1753 with a single entry in the
	// stream — the depth equals XLEN, not the lag.
	fake := newFakeBackpressure(grp(1753, 0))
	fake.xlen = 1
	gate := NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(ctx, StreamDNS); err != nil || n != 1 {
		t.Errorf("Len фантома: (%d, %v), хочу (1, nil) — min(1753, XLEN 1)", n, err)
	}

	// The real backlog is not distorted: the live q:http measurement of
	// Oct 23 — lag 98931 with XLEN 107367, the clamp is inactive.
	fake = newFakeBackpressure(grp(98_931, 0))
	fake.xlen = 107_367
	gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(ctx, StreamHTTP); err != nil || n != 98_931 {
		t.Errorf("Len реального бэклога: (%d, %v), хочу (98931, nil)", n, err)
	}

	// The clamp does not touch pending: the unacknowledged-delivery
	// counter is always exact — it is delivered work regardless of the
	// stream length.
	fake = newFakeBackpressure(grp(1753, 30))
	fake.xlen = 1
	gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(ctx, StreamDNS); err != nil || n != 31 {
		t.Errorf("Len фантома с pending: (%d, %v), хочу (31, nil) — min(1753,1)+30", n, err)
	}

	// lag 0 — clamping zero changes nothing: the depth = pending.
	fake = newFakeBackpressure(grp(0, 12))
	fake.xlen = 5
	gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(ctx, StreamRender); err != nil || n != 12 {
		t.Errorf("Len при lag 0: (%d, %v), хочу (12, nil)", n, err)
	}

	// Several groups — the clamp per each with the stream's common XLEN:
	// the undelivered of different groups may overlap (the sum above
	// XLEN — an honest backlog of each group), but none exceeds the
	// stream.
	fake = newFakeBackpressure([]redis.XInfoGroup{
		{Name: "a", Lag: 2000, Pending: 0},
		{Name: "b", Lag: 5, Pending: 2},
	})
	fake.xlen = 10
	gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(ctx, StreamAnalyze); err != nil || n != 17 {
		t.Errorf("Len нескольких групп: (%d, %v), хочу (17, nil) — min(2000,10)+min(5,10)+2", n, err)
	}

	// The gate with the phantom is open: the q:dns threshold =
	// min(100000, 250/2) = 125, the clamped depth of 1 is below —
	// publication passes without waiting (the incident freeze does not
	// reproduce).
	fake = newFakeBackpressure(grp(1753, 0))
	fake.xlen = 1
	g := newTestGate(fake, 100000)
	if err := g.AwaitCapacity(ctx, StreamDNS); err != nil {
		t.Fatalf("AwaitCapacity с фантомным лагом: %v", err)
	}
	if len(fake.infos) != 1 {
		t.Errorf("проверок %d, хочу 1 (клампнутая глубина ниже порога): %v", len(fake.infos), fake.infos)
	}

	// An XLEN error with live groups — the gate does not guess the
	// depth: the clamp is impossible without XLEN, the error returns
	// immediately.
	fake = newFakeBackpressure(grp(5, 0))
	fake.xlenErr = errors.New("ERR connection refused")
	gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(ctx, StreamDNS); err == nil || n != 0 ||
		!strings.Contains(err.Error(), "кламп лага") {
		t.Errorf("Len с ошибкой XLEN: (%d, %v), хочу (0, ошибка клампа)", n, err)
	}
}

// TestBackpressureWatermarkCappedByMaxLen — the effective stream
// threshold is no higher than half of its MAXLEN cap: the gate must fire
// before the trim. For q:dns the configured 100,000 entries are
// unreachable (MAXLEN 250, T-141) — the threshold is capped at 125 and
// the gate becomes functional; for one-off streams half of the policy
// cap (150,000 / 2 = 75,000) is below the watermark — the threshold is
// capped too.
func TestBackpressureWatermarkCappedByMaxLen(t *testing.T) {
	// q:dns: MAXLEN by policy 250 → threshold 125. The stream's XLEN —
	// realistically at the policy cap (the XADD MAXLEN ~ trim keeps the
	// length at the cap).
	fake := newFakeBackpressure(grp(124, 0))
	fake.xlen = 125
	gate := newTestGate(fake, 100000)
	if err := gate.AwaitCapacity(context.Background(), StreamDNS); err != nil {
		t.Fatalf("AwaitCapacity ниже ограниченного порога: %v", err)
	}

	fake = newFakeBackpressure(grp(125, 0), grp(124, 0))
	fake.xlen = 125
	gate = newTestGate(fake, 100000)
	if err := gate.AwaitCapacity(context.Background(), StreamDNS); err != nil {
		t.Fatalf("AwaitCapacity q:dns: %v", err)
	}
	if len(fake.infos) != 2 {
		t.Errorf("проверок %d, хочу 2 (125 ≥ порог 125 — гейт закрыт): %v", len(fake.infos), fake.infos)
	}

	// A one-off stream: half of the policy cap (150,000 / 2) is below
	// the watermark of 100,000 — the threshold is capped at 75,000.
	fake = newFakeBackpressure(grp(74_999, 0))
	fake.xlen = 75_000
	gate = newTestGate(fake, 100000)
	if err := gate.AwaitCapacity(context.Background(), StreamHTTP); err != nil {
		t.Fatalf("AwaitCapacity ниже порога 75 000: %v", err)
	}
	fake = newFakeBackpressure(grp(75_000, 0), grp(74_999, 0))
	fake.xlen = 75_000
	gate = newTestGate(fake, 100000)
	if err := gate.AwaitCapacity(context.Background(), StreamHTTP); err != nil {
		t.Fatalf("AwaitCapacity штучного стрима: %v", err)
	}
	if len(fake.infos) != 2 {
		t.Errorf("проверок %d, хочу 2 (75 000 ≥ порог 75 000 — гейт закрыт): %v", len(fake.infos), fake.infos)
	}

	// An explicit cap from the options (redis.maxlen, Q-03) with a half
	// above the watermark — the threshold is not capped (100,000).
	fake = newFakeBackpressure(grp(99_999, 0))
	fake.xlen = 100_000
	gate = NewBackpressureGate(fake, BackpressureOptions{
		Watermark: 100000, MaxLen: 400000, Interval: time.Millisecond,
	})
	if err := gate.AwaitCapacity(context.Background(), StreamHTTP); err != nil {
		t.Fatalf("AwaitCapacity ниже порога 100 000: %v", err)
	}

	// An explicit cap from the options caps it too.
	fake = newFakeBackpressure(grp(2000, 0), grp(1999, 0))
	fake.xlen = 2000
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
	fake.xlen = 99
	gate := newTestGate(fake, 100)

	if err := gate.AwaitCapacity(context.Background(), StreamDNS); err != nil {
		t.Fatalf("AwaitCapacity ниже порога: %v", err)
	}
	if want := []string{StreamDNS}; !reflect.DeepEqual(fake.infos, want) {
		t.Errorf("XINFO GROUPS: %d вызовов (%v), хочу 1 — ниже порога проходит сразу", len(fake.infos), fake.infos)
	}
}

func TestBackpressureGateAtWatermark(t *testing.T) {
	// depth == watermark — "strictly below the threshold" is not met: the
	// gate is closed, publication waits until the depth drops (99 < 100).
	fake := newFakeBackpressure(grp(100, 0), grp(100, 0), grp(99, 0))
	fake.xlen = 100
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
	fake.xlen = 150
	gate := newTestGate(fake, 100)

	if err := gate.AwaitCapacity(context.Background(), StreamRender); err != nil {
		t.Fatalf("AwaitCapacity выше порога: %v", err)
	}
	if len(fake.infos) != 2 {
		t.Errorf("проверок %d, хочу 2 (ожидание и повтор): %v", len(fake.infos), fake.infos)
	}
}

func TestBackpressureGateRetriesOverInterval(t *testing.T) {
	// The gate is closed (5 ≥ 1 the whole time): the checks repeat with
	// the interval; ctx expires while waiting — a context error. In
	// 100 ms with a 40 ms interval there must be at least two checks.
	fake := newFakeBackpressure(grp(5, 0))
	fake.xlen = 5
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
	// Cancelled before the call: the first check, then an exit by ctx
	// without waiting for the interval (a clean exit, spec 3.4).
	fake := newFakeBackpressure(grp(100, 0))
	fake.xlen = 100
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
	// A disabled gate, q:dead and a foreign stream — without touching
	// Redis.
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

	// q:render-fallback — publication from render finalization without
	// waiting for capacity (spec 4.3 p.3): waiting would delay the XACK
	// of a q:render task (the T-066 pathology); the flow is held by the
	// share budget and the pace of worker B.
	if err := gate.AwaitCapacity(context.Background(), StreamRenderFallback); err != nil {
		t.Errorf("q:render-fallback: %v", err)
	}
	if len(fake.infos)+len(fake.xlens) != 0 {
		t.Errorf("q:render-fallback не должен гейтироваться: %v %v", fake.infos, fake.xlens)
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
	// Two closed passes (100 and 100 against a threshold of 100) — one
	// suspension entry: the log is not spammed on every retry.
	var buf bytes.Buffer
	fake := newFakeBackpressure(grp(100, 0), grp(100, 0), grp(50, 0))
	fake.xlen = 100
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
	fake.xlen = 99
	fake.addID = "1718-0"
	pub := newTestPublisher(fake)

	id, err := pub.Publish(context.Background(), StreamDNS, dnsBatch())
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if id != "1718-0" {
		t.Errorf("Publish вернул %q, хочу %q", id, fake.addID)
	}
	want := []string{"xinfo:" + StreamDNS, "xlen:" + StreamDNS, "xadd:" + StreamDNS}
	if !reflect.DeepEqual(fake.log, want) {
		t.Errorf("порядок команд: %v, хочу %v (глубина — затем публикация)", fake.log, want)
	}
	if len(fake.adds) != 1 || fake.adds[0].Stream != StreamDNS {
		t.Errorf("XADD: %+v", fake.adds)
	}
}

func TestWatermarkPublisherWaitsThenPublishes(t *testing.T) {
	fake := newFakeBackpressure(grp(100, 0), grp(3, 0))
	fake.xlen = 100
	fake.addID = "1719-0"
	pub := newTestPublisher(fake)

	if _, err := pub.Publish(context.Background(), StreamDNS, dnsBatch()); err != nil {
		t.Fatalf("Publish после ожидания: %v", err)
	}
	want := []string{
		"xinfo:" + StreamDNS, "xlen:" + StreamDNS, // closed: depth 100 ≥ 100
		"xinfo:" + StreamDNS, "xlen:" + StreamDNS, // retry: 3 < 100
		"xadd:" + StreamDNS,
	}
	if !reflect.DeepEqual(fake.log, want) {
		t.Errorf("порядок команд: %v, хочу %v (публикация только после ёмкости)", fake.log, want)
	}
}

func TestWatermarkPublisherChecksBeforeEveryBatch(t *testing.T) {
	// Batch publication: each message (for q:dns a message is a batch,
	// spec 4.2) gets its own capacity check — "publishes new tasks only
	// if the queue length is below the threshold" (spec 5.3), not once
	// before the series.
	fake := newFakeBackpressure(grp(7, 0)) // the last answer repeats
	fake.xlen = 7
	fake.addID = "1720-0"
	pub := newTestPublisher(fake)

	want := make([]string, 0, 9)
	for i := 0; i < 3; i++ {
		if _, err := pub.Publish(context.Background(), StreamDNS, dnsBatch()); err != nil {
			t.Fatalf("Publish пачки №%d: %v", i+1, err)
		}
		want = append(want, "xinfo:"+StreamDNS, "xlen:"+StreamDNS, "xadd:"+StreamDNS)
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

	// A gate error — the publication does not reach XADD.
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

	// A ctx cancellation in the gate — a clean exit without publication.
	fake = newFakeBackpressure(grp(100, 0))
	fake.xlen = 100
	pub = newTestPublisher(fake)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := pub.Publish(canceled, StreamDNS, dnsBatch()); !errors.Is(err, context.Canceled) {
		t.Errorf("отмена ctx: %v", err)
	}
	if len(fake.adds) != 0 {
		t.Errorf("при отмене ctx публикация не выполняется: %+v", fake.adds)
	}

	// An error of the publication itself — after the passed gate.
	fake = &fakeBackpressure{
		fakeRedis: &fakeRedis{addErr: errors.New("connection refused")},
		seq:       [][]redis.XInfoGroup{grp(5, 0)},
		xlen:      5,
	}
	pub = newTestPublisher(fake)
	if _, err := pub.Publish(ctx, StreamDNS, dnsBatch()); err == nil {
		t.Error("ошибка XADD не возвращена")
	}

	// An invalid message — an error of the wrapped queue (the gate
	// passed).
	fake = newFakeBackpressure(grp(5, 0))
	fake.xlen = 5
	pub = newTestPublisher(fake)
	if _, err := pub.Publish(ctx, StreamDNS, DNSBatch{}); err == nil {
		t.Error("невалидное сообщение опубликовано")
	}
	if len(fake.adds) != 0 {
		t.Errorf("невалидное сообщение дошло до XADD: %+v", fake.adds)
	}

	// A wrapper without a gate — an explicit error: flow control is not
	// lost silently.
	fake = newFakeBackpressure(grp(5, 0))
	pub = NewWatermarkPublisher(NewRedisQueue(fake, RedisOptions{}), nil)
	if _, err := pub.Publish(ctx, StreamDNS, dnsBatch()); err == nil ||
		!strings.Contains(err.Error(), "гейт") {
		t.Errorf("обёртка без гейта: %v", err)
	}
}

func TestWatermarkPublisherPassthrough(t *testing.T) {
	// Consume and Ack do not concern the producer: the gate opens only
	// for publications.
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

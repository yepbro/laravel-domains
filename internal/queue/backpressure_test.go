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
			t.Errorf("Interval %s not replaced by the default: %s", interval, g.opt.Interval)
		}
	}
	g := NewBackpressureGate(nil, BackpressureOptions{Watermark: 1, Interval: time.Nanosecond})
	if g.opt.Interval != time.Nanosecond {
		t.Errorf("Interval 1ns replaced by the default: %s, want 1ns (a positive interval is valid)", g.opt.Interval)
	}
}

// TestBackpressureStreamMaxLen — the stream cap: 0 and negative MaxLen —
// the DefaultStreamMaxLen policy, an explicit cap of 1 is valid and is
// not substituted by the policy.
func TestBackpressureStreamMaxLen(t *testing.T) {
	g := NewBackpressureGate(nil, BackpressureOptions{})
	if got := g.streamMaxLen(StreamDNS); got != 250 {
		t.Errorf("streamMaxLen without MaxLen = %d, want 250 (the q:dns policy, T-141)", got)
	}
	if got := g.streamMaxLen(StreamHTTP); got != 150_000 {
		t.Errorf("streamMaxLen without MaxLen = %d, want 150000 (the unit-stream policy, T-141)", got)
	}
	g = NewBackpressureGate(nil, BackpressureOptions{MaxLen: 1})
	if got := g.streamMaxLen(StreamHTTP); got != 1 {
		t.Errorf("streamMaxLen with MaxLen=1 = %d, want 1 (an explicit limit is valid)", got)
	}
	g = NewBackpressureGate(nil, BackpressureOptions{MaxLen: -7})
	if got := g.streamMaxLen(StreamHTTP); got != 150_000 {
		t.Errorf("streamMaxLen with negative MaxLen = %d, want 150000 (the policy)", got)
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
		{"zero watermark disables", 0, 1000, StreamHTTP, 0},
		{"negative watermark disables", -5, 1000, StreamHTTP, 0},
		{"the q:dns policy cap", 100_000, 0, StreamDNS, 125},
		{"the unit-stream policy cap", 600_000, 0, StreamHTTP, 75_000},
		{"explicit limit 1 — threshold 0", 100_000, 1, StreamDNS, 0},
		{"the minimum is the watermark", 7, 100, StreamHTTP, 7},
		{"the minimum is half the limit", 200, 100, StreamHTTP, 50},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := StreamWatermark(c.watermark, c.maxLen, c.stream); got != c.want {
				t.Errorf("StreamWatermark(%d, %d, %s) = %d, want %d",
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
		{"lag with pending", 27409, 120, 136500, 27529},
		{"phantom lag clamped by XLEN (T-168)", 1753, 0, 1, 1},
		{"lag 0 — a zero increment", 0, 5, 100, 5},
		{"lag −1 (undefined) — pending only", -1, 8, 3, 8},
		{"empty group", 0, 0, 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := GroupDepth(redis.XInfoGroup{Name: "g", Lag: c.lag, Pending: c.pend}, c.xlen); got != c.want {
				t.Errorf("GroupDepth(lag=%d, pending=%d, xlen=%d) = %d, want %d",
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
		t.Errorf("Len = %d, want 52 (lag 30+7 + pending 12+3)", n)
	}
	if want := []string{StreamHTTP}; !reflect.DeepEqual(fake.infos, want) {
		t.Errorf("XINFO GROUPS keys: %v, want %v", fake.infos, want)
	}
	if want := []string{StreamHTTP}; !reflect.DeepEqual(fake.xlens, want) {
		t.Errorf("XLEN keys: %v, want %v (one call — the group lag clamp, T-168)", fake.xlens, want)
	}

	// lag is undefined (−1: the trim cut off the undelivered) — only
	// pending is counted, XLEN does not participate in the clamp.
	fake = newFakeBackpressure(grp(-1, 8))
	fake.xlen = 3
	gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(context.Background(), StreamHTTP); err != nil || n != 8 {
		t.Errorf("Len with undefined lag: (%d, %v), want (8, nil)", n, err)
	}

	// No groups — no entry has been delivered: the depth equals XLEN.
	fake = newFakeBackpressure(nil)
	fake.xlen = 34
	gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(context.Background(), StreamHTTP); err != nil || n != 34 {
		t.Errorf("Len without groups: (%d, %v), want XLEN (34, nil)", n, err)
	}
	if want := []string{StreamHTTP}; !reflect.DeepEqual(fake.xlens, want) {
		t.Errorf("XLEN keys: %v, want %v", fake.xlens, want)
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
			t.Errorf("Len of a nonexistent stream (%s): (%d, %v), want XLEN (34, nil)", name, n, err)
		}
		if want := []string{StreamHTTP}; !reflect.DeepEqual(fake.xlens, want) {
			t.Errorf("Len of a nonexistent stream (%s): XLEN keys %v, want %v", name, fake.xlens, want)
		}
	}

	// Errors — a zero depth and the exact text.
	if n, err := gate.Len(context.Background(), "q:typo"); err == nil || n != 0 ||
		!strings.Contains(err.Error(), "unknown stream") {
		t.Errorf("Len of a foreign stream: (%d, %v), want (0, unknown stream)", n, err)
	}

	gate = NewBackpressureGate(nil, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(context.Background(), StreamDNS); err == nil || n != 0 ||
		!strings.Contains(err.Error(), "Redis client not set") {
		t.Errorf("Len without a client: (%d, %v), want (0, client not set)", n, err)
	}

	fake = &fakeBackpressure{
		fakeRedis: &fakeRedis{},
		groupsErr: errors.New("ERR connection refused"),
	}
	gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(context.Background(), StreamDNS); err == nil || n != 0 ||
		!strings.Contains(err.Error(), "depth") {
		t.Errorf("Len with Redis unavailable: (%d, %v), want (0, a depth error)", n, err)
	}

	// An XLEN error with no groups is returned (the gate does not guess
	// the depth of a nonexistent key).
	fake = newFakeBackpressure(nil)
	fake.xlenErr = errors.New("ERR connection refused")
	gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(context.Background(), StreamDNS); err == nil || n != 0 ||
		!strings.Contains(err.Error(), "no groups") {
		t.Errorf("Len with an XLEN error: (%d, %v), want (0, an XLEN error)", n, err)
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
		t.Errorf("Len of the phantom: (%d, %v), want (1, nil) — min(1753, XLEN 1)", n, err)
	}

	// The real backlog is not distorted: the live q:http measurement of
	// Oct 23 — lag 98931 with XLEN 107367, the clamp is inactive.
	fake = newFakeBackpressure(grp(98_931, 0))
	fake.xlen = 107_367
	gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(ctx, StreamHTTP); err != nil || n != 98_931 {
		t.Errorf("Len of the real backlog: (%d, %v), want (98931, nil)", n, err)
	}

	// The clamp does not touch pending: the unacknowledged-delivery
	// counter is always exact — it is delivered work regardless of the
	// stream length.
	fake = newFakeBackpressure(grp(1753, 30))
	fake.xlen = 1
	gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(ctx, StreamDNS); err != nil || n != 31 {
		t.Errorf("Len of the phantom with pending: (%d, %v), want (31, nil) — min(1753,1)+30", n, err)
	}

	// lag 0 — clamping zero changes nothing: the depth = pending.
	fake = newFakeBackpressure(grp(0, 12))
	fake.xlen = 5
	gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(ctx, StreamRender); err != nil || n != 12 {
		t.Errorf("Len at lag 0: (%d, %v), want (12, nil)", n, err)
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
		t.Errorf("Len of several groups: (%d, %v), want (17, nil) — min(2000,10)+min(5,10)+2", n, err)
	}

	// The gate with the phantom is open: the q:dns threshold =
	// min(100000, 250/2) = 125, the clamped depth of 1 is below —
	// publication passes without waiting (the incident freeze does not
	// reproduce).
	fake = newFakeBackpressure(grp(1753, 0))
	fake.xlen = 1
	g := newTestGate(fake, 100000)
	if err := g.AwaitCapacity(ctx, StreamDNS); err != nil {
		t.Fatalf("AwaitCapacity with a phantom lag: %v", err)
	}
	if len(fake.infos) != 1 {
		t.Errorf("checks %d, want 1 (the clamped depth is below the threshold): %v", len(fake.infos), fake.infos)
	}

	// An XLEN error with live groups — the gate does not guess the
	// depth: the clamp is impossible without XLEN, the error returns
	// immediately.
	fake = newFakeBackpressure(grp(5, 0))
	fake.xlenErr = errors.New("ERR connection refused")
	gate = NewBackpressureGate(fake, BackpressureOptions{Watermark: 100})
	if n, err := gate.Len(ctx, StreamDNS); err == nil || n != 0 ||
		!strings.Contains(err.Error(), "lag clamp") {
		t.Errorf("Len with an XLEN error: (%d, %v), want (0, a clamp error)", n, err)
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
		t.Fatalf("AwaitCapacity below the clamped threshold: %v", err)
	}

	fake = newFakeBackpressure(grp(125, 0), grp(124, 0))
	fake.xlen = 125
	gate = newTestGate(fake, 100000)
	if err := gate.AwaitCapacity(context.Background(), StreamDNS); err != nil {
		t.Fatalf("AwaitCapacity q:dns: %v", err)
	}
	if len(fake.infos) != 2 {
		t.Errorf("checks %d, want 2 (125 ≥ threshold 125 — the gate is closed): %v", len(fake.infos), fake.infos)
	}

	// A one-off stream: half of the policy cap (150,000 / 2) is below
	// the watermark of 100,000 — the threshold is capped at 75,000.
	fake = newFakeBackpressure(grp(74_999, 0))
	fake.xlen = 75_000
	gate = newTestGate(fake, 100000)
	if err := gate.AwaitCapacity(context.Background(), StreamHTTP); err != nil {
		t.Fatalf("AwaitCapacity below the threshold 75,000: %v", err)
	}
	fake = newFakeBackpressure(grp(75_000, 0), grp(74_999, 0))
	fake.xlen = 75_000
	gate = newTestGate(fake, 100000)
	if err := gate.AwaitCapacity(context.Background(), StreamHTTP); err != nil {
		t.Fatalf("AwaitCapacity of a unit stream: %v", err)
	}
	if len(fake.infos) != 2 {
		t.Errorf("checks %d, want 2 (75,000 ≥ threshold 75,000 — the gate is closed): %v", len(fake.infos), fake.infos)
	}

	// An explicit cap from the options (redis.maxlen, Q-03) with a half
	// above the watermark — the threshold is not capped (100,000).
	fake = newFakeBackpressure(grp(99_999, 0))
	fake.xlen = 100_000
	gate = NewBackpressureGate(fake, BackpressureOptions{
		Watermark: 100000, MaxLen: 400000, Interval: time.Millisecond,
	})
	if err := gate.AwaitCapacity(context.Background(), StreamHTTP); err != nil {
		t.Fatalf("AwaitCapacity below the threshold 100,000: %v", err)
	}

	// An explicit cap from the options caps it too.
	fake = newFakeBackpressure(grp(2000, 0), grp(1999, 0))
	fake.xlen = 2000
	gate = NewBackpressureGate(fake, BackpressureOptions{
		Watermark: 100000, MaxLen: 4000, Interval: time.Millisecond,
	})
	if err := gate.AwaitCapacity(context.Background(), StreamHTTP); err != nil {
		t.Fatalf("AwaitCapacity with an explicit MaxLen: %v", err)
	}
	if len(fake.infos) != 2 {
		t.Errorf("checks %d, want 2 (the threshold is clamped to 2,000): %v", len(fake.infos), fake.infos)
	}
}

func TestBackpressureGateBelowWatermark(t *testing.T) {
	fake := newFakeBackpressure(grp(99, 0))
	fake.xlen = 99
	gate := newTestGate(fake, 100)

	if err := gate.AwaitCapacity(context.Background(), StreamDNS); err != nil {
		t.Fatalf("AwaitCapacity below the threshold: %v", err)
	}
	if want := []string{StreamDNS}; !reflect.DeepEqual(fake.infos, want) {
		t.Errorf("XINFO GROUPS: %d calls (%v), want 1 — below the threshold it passes immediately", len(fake.infos), fake.infos)
	}
}

func TestBackpressureGateAtWatermark(t *testing.T) {
	// depth == watermark — "strictly below the threshold" is not met: the
	// gate is closed, publication waits until the depth drops (99 < 100).
	fake := newFakeBackpressure(grp(100, 0), grp(100, 0), grp(99, 0))
	fake.xlen = 100
	gate := newTestGate(fake, 100)

	if err := gate.AwaitCapacity(context.Background(), StreamDNS); err != nil {
		t.Fatalf("AwaitCapacity at the threshold: %v", err)
	}
	if len(fake.infos) != 3 {
		t.Errorf("checks %d, want 3 (two rejections, the third below the threshold): %v", len(fake.infos), fake.infos)
	}
}

func TestBackpressureGateAboveWatermark(t *testing.T) {
	fake := newFakeBackpressure(grp(150, 0), grp(7, 0))
	fake.xlen = 150
	gate := newTestGate(fake, 100)

	if err := gate.AwaitCapacity(context.Background(), StreamRender); err != nil {
		t.Fatalf("AwaitCapacity above the threshold: %v", err)
	}
	if len(fake.infos) != 2 {
		t.Errorf("checks %d, want 2 (a wait and a retry): %v", len(fake.infos), fake.infos)
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
		t.Fatal("AwaitCapacity with a closed gate passed without an error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("wait error: %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Errorf("returned earlier than two wait intervals: %s", elapsed)
	}
	if len(fake.infos) < 2 {
		t.Errorf("checks %d, want ≥ 2 (retries via the interval): %v", len(fake.infos), fake.infos)
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
		t.Errorf("error: %v, want context.Canceled", err)
	}
	if len(fake.infos) != 1 {
		t.Errorf("checks %d, want 1", len(fake.infos))
	}
}

func TestBackpressureGateDepthError(t *testing.T) {
	fake := &fakeBackpressure{
		fakeRedis: &fakeRedis{},
		groupsErr: errors.New("ERR connection refused"),
	}
	gate := newTestGate(fake, 10)

	if err := gate.AwaitCapacity(context.Background(), StreamDNS); err == nil ||
		!strings.Contains(err.Error(), "depth") {
		t.Errorf("the depth read error not returned: %v", err)
	}
	if len(fake.infos) != 1 {
		t.Errorf("checks %d, want 1 — the gate does not retry an error", len(fake.infos))
	}
}

func TestBackpressureGateUngated(t *testing.T) {
	// A disabled gate, q:dead and a foreign stream — without touching
	// Redis.
	fake := newFakeBackpressure(grp(1000, 0))
	disabled := NewBackpressureGate(fake, BackpressureOptions{Watermark: 0})
	if err := disabled.AwaitCapacity(context.Background(), StreamDNS); err != nil {
		t.Errorf("disabled gate: %v", err)
	}
	if len(fake.infos)+len(fake.xlens) != 0 {
		t.Errorf("the disabled gate touched Redis: %v %v", fake.infos, fake.xlens)
	}

	gate := newTestGate(fake, 10)
	if err := gate.AwaitCapacity(context.Background(), StreamDead); err != nil {
		t.Errorf("q:dead: %v", err)
	}
	if len(fake.infos)+len(fake.xlens) != 0 {
		t.Errorf("q:dead must not be gated: %v %v", fake.infos, fake.xlens)
	}

	// q:render-fallback — publication from render finalization without
	// waiting for capacity (spec 4.3 p.3): waiting would delay the XACK
	// of a q:render task (the T-066 pathology); the flow is held by the
	// share budget and the pace of worker B.
	if err := gate.AwaitCapacity(context.Background(), StreamRenderFallback); err != nil {
		t.Errorf("q:render-fallback: %v", err)
	}
	if len(fake.infos)+len(fake.xlens) != 0 {
		t.Errorf("q:render-fallback must not be gated: %v %v", fake.infos, fake.xlens)
	}

	if err := gate.AwaitCapacity(context.Background(), "q:typo"); err == nil ||
		!strings.Contains(err.Error(), "unknown stream") {
		t.Errorf("foreign stream: %v", err)
	}
	if len(fake.infos)+len(fake.xlens) != 0 {
		t.Errorf("a foreign stream must not be checked: %v %v", fake.infos, fake.xlens)
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
		t.Fatalf("checks %d, want 3 (two rejections, the third below the threshold): %v", len(fake.infos), fake.infos)
	}
	out := buf.String()
	if !strings.Contains(out, StreamDNS) || !strings.Contains(out, "100") {
		t.Errorf("the pause entry not logged or without context: %q", out)
	}
	if n := strings.Count(out, "paused"); n != 1 {
		t.Errorf("pause entries %d, want 1 (no spam on every retry): %q", n, out)
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
		t.Errorf("Publish returned %q, want %q", id, fake.addID)
	}
	want := []string{"xinfo:" + StreamDNS, "xlen:" + StreamDNS, "xadd:" + StreamDNS}
	if !reflect.DeepEqual(fake.log, want) {
		t.Errorf("command order: %v, want %v (depth — then publication)", fake.log, want)
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
		t.Fatalf("Publish after waiting: %v", err)
	}
	want := []string{
		"xinfo:" + StreamDNS, "xlen:" + StreamDNS, // closed: depth 100 ≥ 100
		"xinfo:" + StreamDNS, "xlen:" + StreamDNS, // retry: 3 < 100
		"xadd:" + StreamDNS,
	}
	if !reflect.DeepEqual(fake.log, want) {
		t.Errorf("command order: %v, want %v (publication only after capacity)", fake.log, want)
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
			t.Fatalf("Publish of batch #%d: %v", i+1, err)
		}
		want = append(want, "xinfo:"+StreamDNS, "xlen:"+StreamDNS, "xadd:"+StreamDNS)
	}
	if !reflect.DeepEqual(fake.log, want) {
		t.Errorf("command order:\n got  %v\n want %v", fake.log, want)
	}
	if len(fake.adds) != 3 {
		t.Errorf("XADD calls %d, want 3", len(fake.adds))
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
		t.Fatalf("Publish to q:dead: %v", err)
	}
	if len(fake.infos)+len(fake.xlens) != 0 {
		t.Errorf("q:dead must not be gated: %v %v", fake.infos, fake.xlens)
	}
	if len(fake.adds) != 1 {
		t.Errorf("XADD to q:dead: %+v", fake.adds)
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
		!strings.Contains(err.Error(), "depth") {
		t.Errorf("gate error: %v", err)
	}
	if len(fake.adds) != 0 {
		t.Errorf("on a gate error the publication must not happen: %+v", fake.adds)
	}

	// A ctx cancellation in the gate — a clean exit without publication.
	fake = newFakeBackpressure(grp(100, 0))
	fake.xlen = 100
	pub = newTestPublisher(fake)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := pub.Publish(canceled, StreamDNS, dnsBatch()); !errors.Is(err, context.Canceled) {
		t.Errorf("ctx cancel: %v", err)
	}
	if len(fake.adds) != 0 {
		t.Errorf("on ctx cancel the publication must not happen: %+v", fake.adds)
	}

	// An error of the publication itself — after the passed gate.
	fake = &fakeBackpressure{
		fakeRedis: &fakeRedis{addErr: errors.New("connection refused")},
		seq:       [][]redis.XInfoGroup{grp(5, 0)},
		xlen:      5,
	}
	pub = newTestPublisher(fake)
	if _, err := pub.Publish(ctx, StreamDNS, dnsBatch()); err == nil {
		t.Error("the XADD error not returned")
	}

	// An invalid message — an error of the wrapped queue (the gate
	// passed).
	fake = newFakeBackpressure(grp(5, 0))
	fake.xlen = 5
	pub = newTestPublisher(fake)
	if _, err := pub.Publish(ctx, StreamDNS, DNSBatch{}); err == nil {
		t.Error("an invalid message was published")
	}
	if len(fake.adds) != 0 {
		t.Errorf("the invalid message reached XADD: %+v", fake.adds)
	}

	// A wrapper without a gate — an explicit error: flow control is not
	// lost silently.
	fake = newFakeBackpressure(grp(5, 0))
	pub = NewWatermarkPublisher(NewRedisQueue(fake, RedisOptions{}), nil)
	if _, err := pub.Publish(ctx, StreamDNS, dnsBatch()); err == nil ||
		!strings.Contains(err.Error(), "gate") {
		t.Errorf("wrapper without a gate: %v", err)
	}
}

func TestWatermarkPublisherPassthrough(t *testing.T) {
	// Consume and Ack do not concern the producer: the gate opens only
	// for publications.
	fake := &fakeBackpressure{fakeRedis: &fakeRedis{readErr: redis.Nil}}
	pub := newTestPublisher(fake)

	got, err := pub.Consume(context.Background(), StreamDNS, "dns", "dns-1", 1)
	if err != nil || got != nil {
		t.Errorf("Consume: (%v, %v), want (nil, nil)", got, err)
	}
	if err := pub.Ack(context.Background(), StreamDNS, "dns", "1718-0"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if len(fake.infos)+len(fake.xlens) != 0 {
		t.Errorf("Consume/Ack must not measure depth: %v %v", fake.infos, fake.xlens)
	}
	if want := []ackCall{{StreamDNS, "dns", []string{"1718-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK: %+v, want %+v", fake.acks, want)
	}
}

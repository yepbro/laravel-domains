// Tests of delivery reliability (T-025) without a live Redis: the fake
// extends the redisq_test.go command window with the claimStreams
// commands — XAUTOCLAIM and XPENDING EXTENDED. go-redis has no
// New*Result factories for these commands, so the fake builds the
// redis.NewXAutoClaimCmd/NewXPendingExtCmd commands and fills them with
// SetVal/SetErr. Covered: the command arguments, the attempt counter,
// the move to q:dead (the Dead format), the "processing → DB → XACK"
// order. The real Redis semantics — the integration tests of T-032.

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

// fakeClaimRedis — the redisq_test.go fake plus the stuck-entry return
// commands.
type fakeClaimRedis struct {
	fakeRedis

	claims    []redis.XAutoClaimArgs
	claimMsgs []redis.XMessage
	claimNext string // the next-scan cursor from the answer
	claimErr  error

	pends      []redis.XPendingExtArgs
	pendRes    map[string][]redis.XPendingExt // the XPENDING answer by the Start identifier
	pendErr    error
	pendOneErr map[string]error // an XPENDING error for a specific Start identifier
	// pendFailOnce — a one-shot XPENDING error by the Start identifier:
	// the first call fails, the subsequent ones answer with the stub. A
	// model of the transient classification failure (FR-6.2): the counter
	// read at the claim failed, the repeated read at the attempt refund —
	// succeeded.
	pendFailOnce map[string]error
	// pendCtxFail — XPENDING on a canceled ctx answers with the ctx error
	// (like the live client): the tests of classification after a cancel
	// (T-222).
	pendCtxFail bool

	// dos — the recorded Do calls (attempt refunds, T-222): the raw
	// arguments of the XCLAIM … JUSTID RETRYCOUNT command; doErr — the
	// stubbed error of all calls.
	dos   [][]interface{}
	doErr error
}

func (f *fakeClaimRedis) XAutoClaim(ctx context.Context, a *redis.XAutoClaimArgs) *redis.XAutoClaimCmd {
	f.claims = append(f.claims, *a)
	msgs := f.claimMsgs
	if f.claimErr == nil && len(msgs) > 0 {
		// A live claim returns up to Count entries and assigns them (the
		// idle is reset — a repeated call will not return them); the rest
		// of the PEL — to the next calls.
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
	if f.pendCtxFail && ctx.Err() != nil {
		cmd.SetVal(nil)
		cmd.SetErr(ctx.Err())
		return cmd
	}
	if err, ok := f.pendOneErr[a.Start]; ok {
		cmd.SetVal(nil)
		cmd.SetErr(err)
		return cmd
	}
	if err, ok := f.pendFailOnce[a.Start]; ok {
		delete(f.pendFailOnce, a.Start) // one-shot: the next call succeeds
		cmd.SetVal(nil)
		cmd.SetErr(err)
		return cmd
	}
	cmd.SetVal(f.pendRes[a.Start])
	cmd.SetErr(f.pendErr)
	return cmd
}

// Do — the refund window (claimRefunder, T-222): records the raw
// arguments of the command (XCLAIM … JUSTID RETRYCOUNT) and answers with
// the stub (the refund only needs Err).
func (f *fakeClaimRedis) Do(ctx context.Context, args ...interface{}) *redis.Cmd {
	f.dos = append(f.dos, args)
	cmd := redis.NewCmd(ctx)
	cmd.SetErr(f.doErr)
	return cmd
}

// Compile-time checks: the extended fake is interchangeable with the
// real client, Reliab is assembled from the client and the Queue.
var (
	_ claimStreams  = (*fakeClaimRedis)(nil)
	_ claimStreams  = (*redis.Client)(nil)
	_ claimRefunder = (*fakeClaimRedis)(nil)
	_ claimRefunder = (*redis.Client)(nil)
)

// newTestReliab — the wiring as in production: the queue and the
// reliability layer on one fake client.
func newTestReliab(fake *fakeClaimRedis, opt ReliabOptions) *Reliab {
	return NewReliab(fake, NewRedisQueue(fake, RedisOptions{}), opt)
}

func TestNewReliabDefaults(t *testing.T) {
	if DefaultMaxAttempts != 3 {
		t.Errorf("DefaultMaxAttempts = %d, want 3 (spec 4.2; the literal pins the constant)", DefaultMaxAttempts)
	}
	if DefaultClaimMinIdle != 10*time.Minute {
		t.Errorf("DefaultClaimMinIdle = %s, want 10m (the literal pins the constant)", DefaultClaimMinIdle)
	}
	r := newTestReliab(&fakeClaimRedis{}, ReliabOptions{})
	if r.opt.MaxAttempts != DefaultMaxAttempts {
		t.Errorf("MaxAttempts: %d, want %d", r.opt.MaxAttempts, DefaultMaxAttempts)
	}
	if r.opt.MinIdle != DefaultClaimMinIdle {
		t.Errorf("MinIdle: %s, want %s", r.opt.MinIdle, DefaultClaimMinIdle)
	}
	// Single values are valid and are not replaced with the defaults.
	r = newTestReliab(&fakeClaimRedis{}, ReliabOptions{MaxAttempts: 1, MinIdle: time.Nanosecond})
	if r.opt.MaxAttempts != 1 {
		t.Errorf("MaxAttempts = %d, want 1 (a single attempt is a valid setting)", r.opt.MaxAttempts)
	}
	if r.opt.MinIdle != time.Nanosecond {
		t.Errorf("MinIdle = %s, want 1ns (a positive idle is valid)", r.opt.MinIdle)
	}
}

// TestStreamPolicyFitsBudget — the stream budget invariant (T-141,
// spec 3.2): the sum of the MAXLEN policy emergency caps must fit the
// Redis budget of ≤ 512 MiB (with a margin for the overhead memory) even
// with a large q:dns batch — with noeviction a cap above the budget
// would mean a write rejection (OOM) before the trim, as in the T-093
// incident. The batch literal here — the top of the normative range of
// spec 5.3; the link with the actual configuration validation boundary —
// by the pin test config.TestSchedulerBatchFitsStreamBudget
// (config_test.go), which computes from maxSchedulerBatchSize. The
// default (a batch of 1000): q:dns 250 × 1000 × 66 B ≈ 16 MiB + 3 ×
// 103 MiB + the fallback 15,000 ≈ 10 MiB + 2 MiB ≈ 337 MiB; the worst
// case (a batch of 10,000) ≈ 478 MiB — both below the 480 MiB limit.
func TestStreamPolicyFitsBudget(t *testing.T) {
	const (
		worstBatch  = 10_000 // the top of the normative range of spec 5.3
		streamLimit = 480 << 20
	)
	total := DefaultStreamMaxLen(StreamDNS)*worstBatch*DNSStreamBytesPerDomain +
		3*DefaultStreamMaxLen(StreamHTTP)*UnitStreamBytesWorst +
		DefaultStreamMaxLen(StreamRenderFallback)*UnitStreamBytesWorst +
		DefaultStreamMaxLen(StreamDead)*DeadStreamBytesWorst
	if total > streamLimit {
		t.Errorf("the sum of stream caps at batch %d ≈ %d MiB > %d MiB — the Redis budget of 512 MiB (spec 3.2) is broken: renegotiate defaultMaxLen* / scheduler.batch_size (docs/redis.md §3.1)",
			worstBatch, total>>20, streamLimit>>20)
	}
	// The default batch has an order of magnitude more headroom: check
	// the margin.
	def := DefaultStreamMaxLen(StreamDNS)*1000*DNSStreamBytesPerDomain +
		3*DefaultStreamMaxLen(StreamHTTP)*UnitStreamBytesWorst +
		DefaultStreamMaxLen(StreamRenderFallback)*UnitStreamBytesWorst +
		DefaultStreamMaxLen(StreamDead)*DeadStreamBytesWorst
	if def > 400<<20 {
		t.Errorf("the cap sum at the default batch ≈ %d MiB — it eats the budget headroom (the TrimLoop between-trims remainder, service memory)", def>>20)
	}
}

// TestReliabClaim — the XAUTOCLAIM and XPENDING arguments, the return of
// entries with their attempt counters.
func TestReliabClaim(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: []byte(`{"task_id":"t1"}`)}},
			{ID: "1719-0", Values: map[string]any{fieldData: `{"task_id":"t2"}`}},
		},
		claimNext: "1720-0",
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", Consumer: "dead-1", RetryCount: 2}},
			"1719-0": {{ID: "1719-0", Consumer: "dead-1", RetryCount: 3}}, // the last attempt — still into the work
		},
	}
	r := newTestReliab(fake, ReliabOptions{})

	got, _, next, err := r.Claim(context.Background(), StreamHTTP, "http", "recover-1", 15*time.Minute, 10, "0-0")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if next != "1720-0" {
		t.Errorf("the next scan cursor = %q, want %q (from the XAUTOCLAIM answer)", next, "1720-0")
	}

	// XAUTOCLAIM: the same stream/group, assignment to the recovery loop,
	// the idle filter, the PEL scan from the beginning, the batch limit.
	if len(fake.claims) != 1 {
		t.Fatalf("XAUTOCLAIM calls: %d, want 1", len(fake.claims))
	}
	args := fake.claims[0]
	if args.Stream != StreamHTTP || args.Group != "http" || args.Consumer != "recover-1" {
		t.Errorf("XAUTOCLAIM stream/group/consumer: %q/%q/%q", args.Stream, args.Group, args.Consumer)
	}
	if args.MinIdle != 15*time.Minute {
		t.Errorf("XAUTOCLAIM MinIdle: %s, want 15m", args.MinIdle)
	}
	if args.Start != "0-0" || args.Count != 10 {
		t.Errorf("XAUTOCLAIM Start/Count: %q/%d, want 0-0/10", args.Start, args.Count)
	}

	// The explicit cursor is passed to XAUTOCLAIM undistorted (Q-07): the
	// calling loop continues the PEL walk from the kept position.
	if _, _, _, err := r.Claim(context.Background(), StreamHTTP, "http", "recover-1", 15*time.Minute, 10, "1750-3"); err != nil {
		t.Fatalf("Claim with a cursor: %v", err)
	}
	if got := fake.claims[1].Start; got != "1750-3" {
		t.Errorf("XAUTOCLAIM Start with a cursor: %q, want 1750-3", got)
	}

	// The attempt counter — XPENDING EXTENDED per entry (exactly its
	// range), the RetryCount field (the entry's PEL deliveries counter).
	if len(fake.pends) != 2 {
		t.Fatalf("XPENDING calls: %d, want 2", len(fake.pends))
	}
	for i, id := range []string{"1718-0", "1719-0"} {
		p := fake.pends[i]
		if p.Stream != StreamHTTP || p.Group != "http" {
			t.Errorf("XPENDING #%d stream/group: %q/%q", i+1, p.Stream, p.Group)
		}
		if p.Start != id || p.End != id || p.Count != 1 {
			t.Errorf("XPENDING #%d range: %q..%q count=%d, want exactly %q", i+1, p.Start, p.End, p.Count, id)
		}
	}

	want := []Claimed{
		{Delivery{StreamHTTP, "1718-0", []byte(`{"task_id":"t1"}`)}, 2},
		{Delivery{StreamHTTP, "1719-0", []byte(`{"task_id":"t2"}`)}, 3},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Claim:\n got  %+v\n want %+v", got, want)
	}

	// The processed entries do not touch q:dead and are not acknowledged
	// by the claim: the XACK is Processing's job after the result is
	// written to the DB.
	if len(fake.adds) != 0 || len(fake.acks) != 0 {
		t.Errorf("Claim does not publish or acknowledge working entries: %+v %+v", fake.adds, fake.acks)
	}
}

// TestReliabClaimMinIdleDefault — a minIdle of 0 is replaced by the
// option, an option of 0 — by the default; an empty start is normalized
// into "0-0", an explicit minimum minIdle is not replaced.
func TestReliabClaimMinIdleDefault(t *testing.T) {
	fake := &fakeClaimRedis{}
	r := newTestReliab(fake, ReliabOptions{})
	if _, _, _, err := r.Claim(context.Background(), StreamDNS, "dns", "recover-1", 0, 1, ""); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got := fake.claims[0].MinIdle; got != DefaultClaimMinIdle {
		t.Errorf("MinIdle: %s, want %s", got, DefaultClaimMinIdle)
	}
	if got := fake.claims[0].Start; got != "0-0" {
		t.Errorf("Start: %q, want 0-0 (an empty start — from the beginning of the PEL)", got)
	}

	fake = &fakeClaimRedis{}
	r = newTestReliab(fake, ReliabOptions{MinIdle: 2 * time.Minute})
	if _, _, _, err := r.Claim(context.Background(), StreamDNS, "dns", "recover-1", 0, 1, ""); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got := fake.claims[0].MinIdle; got != 2*time.Minute {
		t.Errorf("MinIdle: %s, want 2m from the option", got)
	}

	// An explicit minIdle of 1ns is valid and is not replaced by the
	// layer's option.
	fake = &fakeClaimRedis{}
	r = newTestReliab(fake, ReliabOptions{MinIdle: time.Hour})
	if _, _, _, err := r.Claim(context.Background(), StreamDNS, "dns", "recover-1", time.Nanosecond, 1, ""); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got := fake.claims[0].MinIdle; got != time.Nanosecond {
		t.Errorf("MinIdle: %s, want 1ns (a positive explicit minIdle is valid)", got)
	}
}

// TestReliabClaimDead — an entry whose counter is above the limit is not
// returned but moved to q:dead (a Dead with the task's subject
// identifiers extracted from the body, without payload — T-105) and is
// acknowledged in the source stream.
func TestReliabClaimDead(t *testing.T) {
	origPayload := `{"task_id":"t1","zone_id":3,"domain_id":42,"name":"example.com"}`
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: []byte(origPayload)}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", Consumer: "old", RetryCount: 4}}, // the limit of 3 is exhausted
		},
	}
	r := newTestReliab(fake, ReliabOptions{})

	got, _, _, err := r.Claim(context.Background(), StreamRender, "render", "recover-1", time.Minute, 5, "0-0")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got != nil {
		t.Errorf("the attempt-exhausted entry returned to work: %+v", got)
	}

	// The Dead is published to q:dead before the XACK of the original
	// entry.
	if len(fake.adds) != 1 {
		t.Fatalf("publications to q:dead: %d, want 1", len(fake.adds))
	}
	add := fake.adds[0]
	if add.Stream != StreamDead || add.MaxLen != DefaultStreamMaxLen(StreamDead) || !add.Approx {
		t.Errorf("XADD: stream %q MaxLen=%d Approx=%v, want %q MAXLEN ~ %d",
			add.Stream, add.MaxLen, add.Approx, StreamDead, DefaultStreamMaxLen(StreamDead))
	}
	data := add.Values.(map[string]any)[fieldData].([]byte)
	dead, err := Decode[Dead](Delivery{Stream: StreamDead, ID: add.ID, Payload: data})
	if err != nil {
		t.Fatalf("the data field does not decode as Dead: %v\n%s", err, data)
	}
	wantDead := Dead{
		TaskID:   "t1",
		Stream:   StreamRender,
		EntryID:  "1718-0",
		Attempts: 4,
		Reason:   "attempt limit 3 exhausted: 4 deliveries without ack",
		ZoneID:   3,
		DomainID: 42,
		Name:     "example.com",
	}
	if !reflect.DeepEqual(dead, wantDead) {
		t.Errorf("Dead:\n got  %+v\n want %+v", dead, wantDead)
	}

	// The original entry leaves the pending: XACK from the source stream.
	if want := []ackCall{{StreamRender, "render", []string{"1718-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK: %+v, want %+v", fake.acks, want)
	}
}

// TestReliabClaimDeadByteGuard (T-105) — a q:dead entry does not balloon
// with the original task's payload: a q:dns batch (~250 KB worst-case)
// is carried by identifiers (task_id, stream, entry, batch size), the
// entry's bytes — hundreds, not hundreds of thousands. A cap of 10,000
// entries with batch payloads would give a worst-case ~2.5 GB against
// the Redis budget of 0.5 GB (docs/redis.md §3); the identifiers are
// enough for the operator's triage (docs/runbooks.md §2).
func TestReliabClaimDeadByteGuard(t *testing.T) {
	big := DNSBatch{TaskID: "big", Domains: make([]DNSDomain, 2000)}
	for i := range big.Domains {
		big.Domains[i] = DNSDomain{
			ZoneID:   int16(i%3 + 1), // the batch is multi-zone (T-027)
			DomainID: int64(i + 1),
			Name:     fmt.Sprintf("domain%04d.example.com", i),
		}
	}
	payload, err := json.Marshal(big)
	if err != nil {
		t.Fatalf("batch serialization: %v", err)
	}
	if len(payload) < 100_000 { // ~70 B per domain with the JSON envelope
		t.Fatalf("the batch is unexpectedly small: %d bytes — the test is not about the worst case", len(payload))
	}
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: payload}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 9}}, // the limit of 3 is exhausted
		},
	}

	got, _, _, err := newTestReliab(fake, ReliabOptions{}).Claim(
		context.Background(), StreamDNS, "dns", "recover-1", time.Minute, 5, "0-0")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got != nil {
		t.Fatalf("the attempt-exhausted entry returned to work: %+v", got)
	}
	if len(fake.adds) != 1 || fake.adds[0].Stream != StreamDead {
		t.Fatalf("the Dead not published: %+v", fake.adds)
	}
	data := fake.adds[0].Values.(map[string]any)[fieldData].([]byte)
	if bound := 512; len(data) > bound {
		t.Errorf("the q:dead entry is %d bytes > %d — the original task's payload carried (T-105):\n%s",
			len(data), bound, data)
	}

	dead, err := Decode[Dead](Delivery{Stream: StreamDead, ID: fake.adds[0].ID, Payload: data})
	if err != nil {
		t.Fatalf("the data field does not decode as Dead: %v\n%s", err, data)
	}
	// The identifiers for the operator's triage are in place, no payload.
	if dead.TaskID != "big" || dead.Stream != StreamDNS || dead.EntryID != "1718-0" || dead.Attempts != 9 {
		t.Errorf("the q:dead entry's identifiers: %+v", dead)
	}
	if dead.Domains != len(big.Domains) {
		t.Errorf("the batch size in the entry: %d, want %d", dead.Domains, len(big.Domains))
	}
	if len(dead.Payload) != 0 {
		t.Errorf("the payload carried to q:dead: %d bytes", len(dead.Payload))
	}

	// The original entry leaves the pending: XACK from the source stream.
	if want := []ackCall{{StreamDNS, "dns", []string{"1718-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK: %+v, want %+v", fake.acks, want)
	}
}

// TestReliabClaimDeadPoisonBody (T-105, the P2 review) — an extraction
// from a foreign body must not make the q:dead entry invalid: otherwise
// Publish would reject the Dead, the XACK would not happen, and the
// "poisonous" task would stay stuck in pending forever instead of the
// quarantine. The identifiers are dropped, the move goes through:
// task_id/stream/entry/attempts/reason are always enough for triage.
func TestReliabClaimDeadPoisonBody(t *testing.T) {
	poison := `{"task_id":"t1","zone_id":3,"domain_id":42,"subdomain_id":1,"variant":5,"name":"` +
		strings.Repeat("a", 254) + `"}` // a variant outside 0/1, a name longer than 253
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: []byte(poison)}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 9}}, // the limit of 3 is exhausted
		},
	}

	got, _, _, err := newTestReliab(fake, ReliabOptions{}).Claim(
		context.Background(), StreamRender, "render", "recover-1", time.Minute, 5, "0-0")
	if err != nil {
		t.Fatalf("Claim: %v — the move must not fail on a foreign body", err)
	}
	if got != nil {
		t.Fatalf("the attempt-exhausted entry returned to work: %+v", got)
	}
	if len(fake.adds) != 1 || fake.adds[0].Stream != StreamDead {
		t.Fatalf("the Dead not published: %+v", fake.adds)
	}
	data := fake.adds[0].Values.(map[string]any)[fieldData].([]byte)
	dead, err := Decode[Dead](Delivery{Stream: StreamDead, ID: fake.adds[0].ID, Payload: data})
	if err != nil {
		t.Fatalf("the data field does not decode as Dead: %v\n%s", err, data)
	}
	want := Dead{
		TaskID:   "t1",
		Stream:   StreamRender,
		EntryID:  "1718-0",
		Attempts: 9,
		Reason:   "attempt limit 3 exhausted: 9 deliveries without ack",
	}
	if !reflect.DeepEqual(dead, want) {
		t.Errorf("the Dead with a foreign body:\n got  %+v\n want %+v (without subject identifiers)", dead, want)
	}
	if want := []ackCall{{StreamRender, "render", []string{"1718-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK: %+v, want %+v", fake.acks, want)
	}
}

// TestReliabClaimDeadUnidentified (T-260, FR-6.1) — the terminal branch
// of a PEL entry without a task_id: before T-260 bury rejected the move
// with an error, the entry stayed in pending and spun in an eternal
// claim loop (every RecoverLoop pass — a claim +1 to the counter and the
// same error, the XACK never happened). Now the Dead is published with
// the synthetic identifier unknown:<stream>:<entry-id>, without subject
// fields (the body is outside the package formats — do not trust the
// subject extraction), the original entry is acknowledged: the entry
// leaves the pending, the triage stays manual (q:dead — quarantine).
func TestReliabClaimDeadUnidentified(t *testing.T) {
	// The body carries a zone_id — a field foreign to identification that
	// must stay unmoved: the entry's subject is not identified.
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: `{"no_task":"id","zone_id":3}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 9}}, // the limit of 3 is exhausted
		},
	}
	got, _, _, err := newTestReliab(fake, ReliabOptions{}).Claim(
		context.Background(), StreamHTTP, "http", "recover-1", time.Minute, 5, "0-0")
	if err != nil {
		t.Fatalf("Claim of an entry without task_id: %v — the terminal branch is not an error (FR-6.1)", err)
	}
	if got != nil {
		t.Errorf("the attempt-exhausted entry returned to work: %+v", got)
	}

	// The Dead in q:dead: a synthetic task_id, without subject fields.
	if len(fake.adds) != 1 || fake.adds[0].Stream != StreamDead {
		t.Fatalf("publications to q:dead: %d, want 1", len(fake.adds))
	}
	data := fake.adds[0].Values.(map[string]any)[fieldData].([]byte)
	dead, err := Decode[Dead](Delivery{Stream: StreamDead, ID: fake.adds[0].ID, Payload: data})
	if err != nil {
		t.Fatalf("the data field does not decode as Dead: %v\n%s", err, data)
	}
	want := Dead{
		TaskID:   "unknown:q:http:1718-0",
		Stream:   StreamHTTP,
		EntryID:  "1718-0",
		Attempts: 9,
		Reason:   "entry outside package formats (no task_id), attempt limit 3 exhausted: 9 deliveries without ack",
	}
	if !reflect.DeepEqual(dead, want) {
		t.Errorf("the Dead of an unidentified entry:\n got  %+v\n want %+v (a synthetic id, no subject fields)", dead, want)
	}

	// The entry leaves the pending: XACK of the original — the claim loop
	// is broken open.
	if want := []ackCall{{StreamHTTP, "http", []string{"1718-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK: %+v, want %+v", fake.acks, want)
	}

	// A mixed batch: the entry with a task_id is buried as before (by its
	// own branch, OnBury), the entry without a task_id — by the synthetic
	// one; both leave the pending in one pass.
	fake = &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1-1", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
			{ID: "1-2", Values: map[string]any{fieldData: `{"no_task":"id"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1-1": {{ID: "1-1", RetryCount: 9}},
			"1-2": {{ID: "1-2", RetryCount: 9}},
		},
	}
	var buried, unidentified []string
	r := NewReliab(fake, NewRedisQueue(fake, RedisOptions{}), ReliabOptions{
		OnBury:             func(stream string) { buried = append(buried, stream) },
		OnBuryUnidentified: func(stream string) { unidentified = append(unidentified, stream) },
	})
	if _, _, _, err := r.Claim(context.Background(), StreamDNS, "dns", "recover-1", time.Minute, 5, "0-0"); err != nil {
		t.Fatalf("the mixed batch: %v", err)
	}
	if want := []string{StreamDNS}; !reflect.DeepEqual(buried, want) {
		t.Errorf("OnBury of the mixed batch: %v, want one call %q (only the entry with a task_id)", buried, StreamDNS)
	}
	if want := []string{StreamDNS}; !reflect.DeepEqual(unidentified, want) {
		t.Errorf("OnBuryUnidentified of the mixed batch: %v, want one call %q", unidentified, StreamDNS)
	}
	if len(fake.adds) != 2 || len(fake.acks) != 2 {
		t.Errorf("the mixed batch not moved as a whole: %+v %+v", fake.adds, fake.acks)
	}
}

// TestReliabClaimBoundary — the counter boundary: MaxAttempts — still
// into the work, more — to q:dead.
func TestReliabClaimBoundary(t *testing.T) {
	cases := []struct {
		name    string
		attempt int64
		wantNil bool
	}{
		{"the first delivery", 1, false},
		{"the last allowed one", 3, false},
		{"exhausted", 4, true},
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
			got, _, _, err := newTestReliab(fake, ReliabOptions{}).Claim(
				context.Background(), StreamDNS, "dns", "recover-1", time.Minute, 1, "0-0")
			if err != nil {
				t.Fatalf("Claim: %v", err)
			}
			if c.wantNil && got != nil {
				t.Errorf("a move to q:dead expected, work returned: %+v", got)
			}
			if !c.wantNil {
				if len(got) != 1 || got[0].Attempts != int(c.attempt) {
					t.Errorf("entries with attempt %d expected, got %+v", c.attempt, got)
				}
				if len(fake.adds) != 0 {
					t.Errorf("a working entry published to q:dead: %+v", fake.adds)
				}
			}
		})
	}
}

// TestReliabClaimSkips — entries without a body (truncated by MAXLEN —
// XAUTOCLAIM has already purged them from the PEL) and entries that left
// the PEL between the commands are not processed and are not moved to
// q:dead.
func TestReliabClaimSkips(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0"}, // truncated by MAXLEN: no values
			{ID: "1719-0", Values: map[string]any{fieldData: `{"task_id":"t2"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			// "1718-0" — XPENDING is not called at all (the body is empty)
			"1719-0": nil, // the entry has already left the PEL — acknowledged by someone else
		},
	}
	got, _, _, err := newTestReliab(fake, ReliabOptions{}).Claim(
		context.Background(), StreamHTTP, "http", "recover-1", time.Minute, 5, "0-0")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got != nil {
		t.Errorf("nothing to refund, got %+v", got)
	}
	if len(fake.pends) != 1 || fake.pends[0].Start != "1719-0" {
		t.Errorf("XPENDING must ask about the entry with a body only: %+v", fake.pends)
	}
	if len(fake.adds) != 0 || len(fake.acks) != 0 {
		t.Errorf("skips are not published or acknowledged: %+v %+v", fake.adds, fake.acks)
	}
}

// TestReliabClaimErrors — command and argument errors; a publication
// failure to q:dead does not acknowledge the original entry (the XACK
// only after the Dead).
func TestReliabClaimErrors(t *testing.T) {
	ctx := context.Background()

	// Bad arguments — without touching Redis.
	for name, call := range map[string]func(r *Reliab) error{
		"unknown stream": func(r *Reliab) error {
			_, _, _, err := r.Claim(ctx, "q:typo", "g", "c", time.Minute, 1, "0-0")
			return err
		},
		"no group": func(r *Reliab) error {
			_, _, _, err := r.Claim(ctx, StreamDNS, "", "c", time.Minute, 1, "0-0")
			return err
		},
		"no consumer": func(r *Reliab) error {
			_, _, _, err := r.Claim(ctx, StreamDNS, "g", "", time.Minute, 1, "0-0")
			return err
		},
		"batch 0": func(r *Reliab) error {
			_, _, _, err := r.Claim(ctx, StreamDNS, "g", "c", time.Minute, 0, "0-0")
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			fake := &fakeClaimRedis{}
			if err := call(newTestReliab(fake, ReliabOptions{})); err == nil {
				t.Fatal("an argument error expected")
			}
			if len(fake.claims)+len(fake.pends) != 0 {
				t.Errorf("Redis called with bad arguments: %+v %+v", fake.claims, fake.pends)
			}
		})
	}

	// An XAUTOCLAIM error.
	fake := &fakeClaimRedis{claimErr: errors.New("ERR connection refused")}
	if _, _, _, err := newTestReliab(fake, ReliabOptions{}).Claim(ctx, StreamDNS, "dns", "recover-1", time.Minute, 1, "0-0"); err == nil ||
		!contains(err, "claim") {
		t.Errorf("the XAUTOCLAIM error: %v", err)
	}

	// An XPENDING error.
	fake = &fakeClaimRedis{
		claimMsgs: []redis.XMessage{{ID: "1-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}}},
		pendErr:   errors.New("ERR connection refused"),
	}
	if _, _, _, err := newTestReliab(fake, ReliabOptions{}).Claim(ctx, StreamDNS, "dns", "recover-1", time.Minute, 1, "0-0"); err == nil ||
		!contains(err, "delivery counter") {
		t.Errorf("the XPENDING error: %v", err)
	}

	// A publication failure to q:dead — the entry stays in pending
	// (without an XACK): the move will repeat on the next pass.
	fake = &fakeClaimRedis{
		fakeRedis: fakeRedis{addErr: errors.New("connection refused")},
		claimMsgs: []redis.XMessage{{ID: "1-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}}},
		pendRes:   map[string][]redis.XPendingExt{"1-0": {{ID: "1-0", RetryCount: 9}}},
	}
	if _, _, _, err := newTestReliab(fake, ReliabOptions{}).Claim(ctx, StreamDNS, "dns", "recover-1", time.Minute, 1, "0-0"); err == nil ||
		!contains(err, "q:dead") {
		t.Errorf("the Dead publication error: %v", err)
	}
	if len(fake.acks) != 0 {
		t.Errorf("XACK after a Dead publication failure: %+v", fake.acks)
	}

	// An XACK failure of the moved entry — the Dead is already published
	// (a duplicate on the next pass is preferable to a loss), the entry
	// in pending.
	fake = &fakeClaimRedis{
		fakeRedis: fakeRedis{ackErr: errors.New("ERR connection refused")},
		claimMsgs: []redis.XMessage{{ID: "1-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}}},
		pendRes:   map[string][]redis.XPendingExt{"1-0": {{ID: "1-0", RetryCount: 9}}},
	}
	if _, _, _, err := newTestReliab(fake, ReliabOptions{}).Claim(ctx, StreamDNS, "dns", "recover-1", time.Minute, 1, "0-0"); err == nil ||
		!contains(err, "acknowledging") {
		t.Errorf("the XACK error: %v", err)
	}
	if len(fake.adds) != 1 {
		t.Errorf("the Dead must be published before the XACK: %+v", fake.adds)
	}

	// An entry without a task_id is no longer a bury error — the terminal
	// branch of FR-6.1 (a synthetic identifier, XACK) is covered by the
	// separate TestReliabClaimDeadUnidentified.
}

// TestReliabRefund — refunding attempts to unprocessed entries (T-222):
// XCLAIM … JUSTID RETRYCOUNT <attempts-1> to the same consumer, entries
// with different counters — by separate commands (one RETRYCOUNT per
// command); a client without the Do window — a transparent nil; command
// errors are returned, the other groups are refunded anyway.
func TestReliabRefund(t *testing.T) {
	fake := &fakeClaimRedis{}
	r := newTestReliab(fake, ReliabOptions{})
	claimed := []Claimed{
		{Delivery{StreamHTTP, "1-1", []byte(`{"task_id":"t1"}`)}, 2},
		{Delivery{StreamHTTP, "1-2", []byte(`{"task_id":"t2"}`)}, 2},
		{Delivery{StreamHTTP, "1-3", []byte(`{"task_id":"t3"}`)}, 3},
	}
	if err := r.Refund(context.Background(), "http", "recover-1", claimed); err != nil {
		t.Fatalf("Refund: %v", err)
	}
	// Two groups by counter: {1-1, 1-2} → retrycount 1, {1-3} → 2. The
	// order of the groups is undefined (map) — matched by content.
	if len(fake.dos) != 2 {
		t.Fatalf("refund commands: %d, want 2 (groups by counter)", len(fake.dos))
	}
	byRC := make(map[int64][]interface{})
	for _, args := range fake.dos {
		if got := args[0]; got != "xclaim" {
			t.Errorf("the refund command: %v, want xclaim", got)
		}
		if stream, group, consumer := args[1], args[2], args[3]; stream != StreamHTTP || group != "http" || consumer != "recover-1" {
			t.Errorf("xclaim stream/group/consumer: %v/%v/%v", stream, group, consumer)
		}
		if idle := args[4]; idle != int64(0) {
			t.Errorf("xclaim min-idle: %v, want 0 (the entries were just claimed by this consumer)", idle)
		}
		ids := append([]interface{}{}, args[5:len(args)-3]...)
		rc := args[len(args)-1].(int64)
		if args[len(args)-3] != "justid" || args[len(args)-2] != "retrycount" {
			t.Errorf("xclaim without JUSTID RETRYCOUNT: %v", args)
		}
		byRC[rc] = ids
	}
	if ids := byRC[1]; !reflect.DeepEqual(ids, []interface{}{"1-1", "1-2"}) {
		t.Errorf("the retrycount-1 group: %v, want [1-1 1-2] (counter 2 before the claim −1)", ids)
	}
	if ids := byRC[2]; !reflect.DeepEqual(ids, []interface{}{"1-3"}) {
		t.Errorf("the retrycount-2 group: %v, want [1-3] (counter 3 before the claim −1)", ids)
	}

	// An empty refund — no commands; a nil window (a client without Do)
	// is transparent.
	fake = &fakeClaimRedis{}
	if err := newTestReliab(fake, ReliabOptions{}).Refund(context.Background(), "g", "c", nil); err != nil {
		t.Errorf("Refund without entries: %v, want nil", err)
	}
	if len(fake.dos) != 0 {
		t.Errorf("commands without entries: %d, want 0", len(fake.dos))
	}
	claimOnly := struct{ claimStreams }{fake}
	ro := NewReliab(claimOnly, NewRedisQueue(fake, RedisOptions{}), ReliabOptions{})
	if err := ro.Refund(context.Background(), "g", "c", claimed); err != nil {
		t.Errorf("Refund without a Do window: %v, want a transparent nil (the pre-T-222 behavior)", err)
	}
	if len(fake.dos) != 0 {
		t.Errorf("commands without a Do window: %d, want 0", len(fake.dos))
	}

	// A command error — in the return with the entries' context; it does
	// not cancel the second group (here there is only one).
	fake = &fakeClaimRedis{doErr: errors.New("ERR no such key")}
	err := newTestReliab(fake, ReliabOptions{}).Refund(context.Background(), "http", "recover-1", claimed[:1])
	if err == nil || !contains(err, "attempt refund") || !contains(err, "1-1") {
		t.Errorf("the refund error: %v, want an error naming entry 1-1", err)
	}

	// The zero counter clamp: Attempts 0 → retrycount 0, not −1 (a
	// negative RETRYCOUNT — a server error).
	fake = &fakeClaimRedis{}
	zero := []Claimed{{Delivery{StreamHTTP, "9-9", []byte(`{"task_id":"t9"}`)}, 0}}
	if err := newTestReliab(fake, ReliabOptions{}).Refund(context.Background(), "http", "recover-1", zero); err != nil {
		t.Fatalf("Refund of a zero counter: %v", err)
	}
	if rc := fake.dos[0][len(fake.dos[0])-1]; rc != int64(0) {
		t.Errorf("the zero-counter retrycount: %v, want 0", rc)
	}

	// An unknown stream — an error before Redis (the Claim style).
	fake = &fakeClaimRedis{}
	bad := []Claimed{{Delivery{"q:typo", "1-1", []byte(`{"task_id":"t1"}`)}, 2}}
	if err := newTestReliab(fake, ReliabOptions{}).Refund(context.Background(), "http", "recover-1", bad); err == nil || !contains(err, "unknown stream") {
		t.Errorf("Refund of an unknown stream: %v, want an error", err)
	}
	if len(fake.dos) != 0 {
		t.Errorf("Redis called with a bad stream: %+v", fake.dos)
	}
}

// TestReliabRefundUnclassified (T-260, FR-6.2) — refunding attempts to
// entries without classification (the XAUTOCLAIM claim spent the
// attempt, the XPENDING counter read failed): the counter is read at the
// moment of the refund (a repeated read per entry), the rollback — by
// the same XCLAIM JUSTID RETRYCOUNT <counter-1> mechanism, entries with
// different counters — by groups; an entry that left the PEL needs no
// refund; a repeated read failure — an error without commands; the Do
// window unavailable — a transparent nil.
func TestReliabRefundUnclassified(t *testing.T) {
	ctx := context.Background()

	// Two entries with different counters: 1-1 → retrycount 1, 1-2 → 4.
	fake := &fakeClaimRedis{
		pendRes: map[string][]redis.XPendingExt{
			"1-1": {{ID: "1-1", RetryCount: 2}},
			"1-2": {{ID: "1-2", RetryCount: 5}},
		},
	}
	ds := []Delivery{
		{Stream: StreamHTTP, ID: "1-1", Payload: []byte(`{"task_id":"t1"}`)},
		{Stream: StreamHTTP, ID: "1-2", Payload: []byte(`{"task_id":"t2"}`)},
	}
	if err := newTestReliab(fake, ReliabOptions{}).RefundUnclassified(ctx, "http", "recover-1", ds); err != nil {
		t.Fatalf("RefundUnclassified: %v", err)
	}
	// Each entry's counter is read again — the range is exactly its id
	// (like the Claim classification).
	if len(fake.pends) != 2 {
		t.Fatalf("XPENDING reads: %d, want 2 (one per entry)", len(fake.pends))
	}
	for i, id := range []string{"1-1", "1-2"} {
		p := fake.pends[i]
		if p.Stream != StreamHTTP || p.Group != "http" || p.Start != id || p.End != id || p.Count != 1 {
			t.Errorf("XPENDING #%d: %+v, want exactly the entry %s", i+1, p, id)
		}
	}
	if len(fake.dos) != 2 {
		t.Fatalf("refund commands: %d, want 2 (groups by counter)", len(fake.dos))
	}
	byRC := make(map[int64][]interface{})
	for _, args := range fake.dos {
		if got := args[0]; got != "xclaim" {
			t.Errorf("the refund command: %v, want xclaim", got)
		}
		if stream, group, consumer := args[1], args[2], args[3]; stream != StreamHTTP || group != "http" || consumer != "recover-1" {
			t.Errorf("xclaim stream/group/consumer: %v/%v/%v", stream, group, consumer)
		}
		if idle := args[4]; idle != int64(0) {
			t.Errorf("xclaim min-idle: %v, want 0", idle)
		}
		ids := append([]interface{}{}, args[5:len(args)-3]...)
		if args[len(args)-3] != "justid" || args[len(args)-2] != "retrycount" {
			t.Errorf("xclaim without JUSTID RETRYCOUNT: %v", args)
		}
		byRC[args[len(args)-1].(int64)] = ids
	}
	if ids := byRC[1]; !reflect.DeepEqual(ids, []interface{}{"1-1"}) {
		t.Errorf("the retrycount-1 group: %v, want [1-1] (counter 2 − the claim)", ids)
	}
	if ids := byRC[4]; !reflect.DeepEqual(ids, []interface{}{"1-2"}) {
		t.Errorf("the retrycount-4 group: %v, want [1-2] (counter 5 − the claim)", ids)
	}

	// An entry that left the PEL between the claim and the refund needs
	// no refund: no command for it, no error.
	fake = &fakeClaimRedis{
		pendRes: map[string][]redis.XPendingExt{
			"1-1": {{ID: "1-1", RetryCount: 2}},
			"1-2": nil, // acknowledged by whoever processed it
		},
	}
	if err := newTestReliab(fake, ReliabOptions{}).RefundUnclassified(ctx, "http", "recover-1", ds); err != nil {
		t.Fatalf("RefundUnclassified with an entry that left the PEL: %v", err)
	}
	if len(fake.dos) != 1 {
		t.Fatalf("refund commands: %d, want 1 (only the entry alive in the PEL)", len(fake.dos))
	}
	if ids := fake.dos[0][5 : len(fake.dos[0])-3]; !reflect.DeepEqual(ids, []interface{}{"1-1"}) {
		t.Errorf("a different entry returned: %v, want [1-1]", ids)
	}

	// A repeated counter read failure — an error with the entry's
	// context, no refund commands (the attempt stays spent — no worse
	// than the behavior before T-260).
	fake = &fakeClaimRedis{
		pendOneErr: map[string]error{"1-1": errors.New("ERR connection refused")},
	}
	if err := newTestReliab(fake, ReliabOptions{}).RefundUnclassified(ctx, "http", "recover-1", ds[:1]); err == nil ||
		!contains(err, "delivery counter") || !contains(err, "1-1") {
		t.Errorf("the repeated read failure: %v, want an error naming entry 1-1", err)
	}
	if len(fake.dos) != 0 {
		t.Errorf("commands on the repeated failure: %d, want 0", len(fake.dos))
	}

	// An empty list — nil; the Do window unavailable — a transparent nil
	// (the Refund style: the attempts spent, as before T-222).
	fake = &fakeClaimRedis{}
	if err := newTestReliab(fake, ReliabOptions{}).RefundUnclassified(ctx, "g", "c", nil); err != nil {
		t.Errorf("RefundUnclassified without entries: %v, want nil", err)
	}
	if len(fake.pends) != 0 || len(fake.dos) != 0 {
		t.Errorf("Redis called without entries: %+v %+v", fake.pends, fake.dos)
	}
	claimOnly := struct{ claimStreams }{fake}
	ro := NewReliab(claimOnly, NewRedisQueue(fake, RedisOptions{}), ReliabOptions{})
	if err := ro.RefundUnclassified(ctx, "http", "recover-1", ds); err != nil {
		t.Errorf("RefundUnclassified without a Do window: %v, want a transparent nil", err)
	}
	if len(fake.dos) != 0 {
		t.Errorf("commands without a Do window: %d, want 0", len(fake.dos))
	}

	// Bad arguments — an error before Redis (the Refund/Claim style).
	fake = &fakeClaimRedis{}
	if err := newTestReliab(fake, ReliabOptions{}).RefundUnclassified(ctx, "", "c", ds); err == nil || !contains(err, "attempt refund") {
		t.Errorf("RefundUnclassified without a group: %v, want an error", err)
	}
	badStream := []Delivery{{Stream: "q:typo", ID: "1-1", Payload: []byte(`{}`)}}
	if err := newTestReliab(fake, ReliabOptions{}).RefundUnclassified(ctx, "http", "recover-1", badStream); err == nil || !contains(err, "unknown stream") {
		t.Errorf("RefundUnclassified of an unknown stream: %v, want an error", err)
	}
	if len(fake.pends) != 0 || len(fake.dos) != 0 {
		t.Errorf("Redis called with bad arguments: %+v %+v", fake.pends, fake.dos)
	}
}

// TestReliabClaimClassifiesAfterCancel — a ctx cancel after a successful
// claim does not interrupt the counter classification (T-222, the P2-2
// review): XAUTOCLAIM has already assigned the entries and spent the
// attempts — RecoverLoop will not be able to return the entries without
// a read counter (Refund does not know their IDs). The classification
// runs under a separate ctx without the caller's cancel; XAUTOCLAIM
// itself respects the cancel (a new taking out of the PEL after the
// shutdown does not start — TestRecoverLoopCanceledContext).
func TestReliabClaimClassifiesAfterCancel(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1-1", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
			{ID: "1-2", Values: map[string]any{fieldData: `{"task_id":"t2"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1-1": {{ID: "1-1", RetryCount: 2}},
			"1-2": {{ID: "1-2", RetryCount: 2}},
		},
		pendCtxFail: true, // XPENDING on a canceled ctx would fail (the live client)
	}
	// The claim "made it" before the cancel: the fake XAUTOCLAIM does not
	// depend on ctx — a model of the race "the server executed the
	// command, the cancel arrived after it".
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, _, _, err := newTestReliab(fake, ReliabOptions{}).Claim(ctx, StreamHTTP, "http", "recover-1", time.Minute, 5, "0-0")
	if err != nil {
		t.Fatalf("Claim after the cancel: %v — the classification must survive the cancel", err)
	}
	want := []Claimed{
		{Delivery{StreamHTTP, "1-1", []byte(`{"task_id":"t1"}`)}, 2},
		{Delivery{StreamHTTP, "1-2", []byte(`{"task_id":"t2"}`)}, 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Claim after the cancel:\n got  %+v\n want %+v (the counters read, the entries refundable)", got, want)
	}
}

// TestReliabClaimEmpty — no stuck entries: an empty result without an
// error, the next pass's cursor — from the beginning of the PEL.
func TestReliabClaimEmpty(t *testing.T) {
	fake := &fakeClaimRedis{}
	got, _, next, err := newTestReliab(fake, ReliabOptions{}).Claim(
		context.Background(), StreamAnalyze, "analyzer", "recover-1", time.Minute, 5, "0-0")
	if err != nil || got != nil {
		t.Errorf("an empty claim: (%v, %v), want (nil, nil)", got, err)
	}
	if next != "0-0" {
		t.Errorf("the empty traversal's next = %q, want 0-0 (the PEL walk is finished — from the beginning)", next)
	}
	if len(fake.pends) != 0 {
		t.Errorf("XPENDING without a claim is not needed: %+v", fake.pends)
	}
}

// TestReliabClaimContinuesAfterSpecial — a special entry at the start of
// the batch (a counter failure, leaving the PEL, exhausted attempts)
// does not break the walk: the valid entries after it return into the
// work.
func TestReliabClaimContinuesAfterSpecial(t *testing.T) {
	msgs := []redis.XMessage{
		{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
		{ID: "1719-0", Values: map[string]any{fieldData: `{"task_id":"t2"}`}},
	}
	cases := []struct {
		name     string
		pendRes  map[string][]redis.XPendingExt
		pendErr  map[string]error
		wantErr  string // a substring of the error for the first entry ("" — without an error)
		wantBury bool   // the first entry is moved to q:dead
	}{
		{
			name: "the first entry's counter fails",
			pendRes: map[string][]redis.XPendingExt{
				"1719-0": {{ID: "1719-0", RetryCount: 2}},
			},
			pendErr: map[string]error{"1718-0": errors.New("ERR connection refused")},
			wantErr: "delivery counter",
		},
		{
			name: "the first left the PEL",
			pendRes: map[string][]redis.XPendingExt{
				"1718-0": nil, // acknowledged between the commands
				"1719-0": {{ID: "1719-0", RetryCount: 2}},
			},
		},
		{
			name: "the first exhausted its attempts",
			pendRes: map[string][]redis.XPendingExt{
				"1718-0": {{ID: "1718-0", RetryCount: 9}}, // the limit of 3 is exhausted
				"1719-0": {{ID: "1719-0", RetryCount: 2}},
			},
			wantBury: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeClaimRedis{claimMsgs: msgs, pendRes: c.pendRes, pendOneErr: c.pendErr}
			got, _, _, err := newTestReliab(fake, ReliabOptions{}).Claim(
				context.Background(), StreamHTTP, "http", "recover-1", time.Minute, 5, "0-0")

			want := []Claimed{
				{Delivery{StreamHTTP, "1719-0", []byte(`{"task_id":"t2"}`)}, 2},
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("Claim after the special entry:\n got  %+v\n want %+v", got, want)
			}
			if c.wantErr != "" && (err == nil || !contains(err, c.wantErr)) {
				t.Errorf("the error %q expected, got: %v", c.wantErr, err)
			}
			if c.wantErr == "" && err != nil {
				t.Errorf("no error expected, got: %v", err)
			}
			if c.wantBury {
				if len(fake.adds) != 1 || fake.adds[0].Stream != StreamDead {
					t.Errorf("the first entry not moved to q:dead: %+v", fake.adds)
				}
				if want := []ackCall{{StreamHTTP, "http", []string{"1718-0"}}}; !reflect.DeepEqual(fake.acks, want) {
					t.Errorf("XACK of the moved entry: %+v, want %+v", fake.acks, want)
				}
			} else if len(fake.adds) != 0 || len(fake.acks) != 0 {
				t.Errorf("the special entry must not be published or acknowledged: %+v %+v", fake.adds, fake.acks)
			}
		})
	}
}

// TestReliabClaimPartialError — an XPENDING failure for one entry does
// not wreck the batch: the entries with a known counter are returned,
// the errors are collected into the return (Q-04: XAUTOCLAIM has already
// assigned the entries — losing the batch over one failed check would
// mean an extra MinIdle of downtime for everyone).
func TestReliabClaimPartialError(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
			{ID: "1719-0", Values: map[string]any{fieldData: `{"task_id":"t2"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 2}},
			// "1719-0" — the counter is unavailable (a per-entry read
			// failure).
		},
		pendOneErr: map[string]error{
			"1719-0": errors.New("ERR connection refused"),
		},
	}

	got, unclassified, _, err := newTestReliab(fake, ReliabOptions{}).Claim(
		context.Background(), StreamHTTP, "http", "recover-1", time.Minute, 5, "0-0")
	if err == nil || !contains(err, "delivery counter") || !contains(err, "1719-0") {
		t.Fatalf("a partial failure: err = %v, want the counter error of entry 1719-0", err)
	}
	want := []Claimed{
		{Delivery{StreamHTTP, "1718-0", []byte(`{"task_id":"t1"}`)}, 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("the valid part:\n got  %+v\n want %+v", got, want)
	}
	// The entry without a counter is returned as the second result for
	// the attempt refund (FR-6.2): the claim already spent it — it is
	// neither lost nor let into the work with an unknown counter.
	wantUnclassified := []Delivery{
		{Stream: StreamHTTP, ID: "1719-0", Payload: []byte(`{"task_id":"t2"}`)},
	}
	if !reflect.DeepEqual(unclassified, wantUnclassified) {
		t.Errorf("entries without classification:\n got  %+v\n want %+v", unclassified, wantUnclassified)
	}
	if len(fake.acks) != 0 || len(fake.adds) != 0 {
		t.Errorf("the claim does not acknowledge or bury: %+v %+v", fake.acks, fake.adds)
	}
}

// TestReliabClaimDeadOnBury (T-154) — the OnBury hook is called on a
// completed move (the Dead published and the original entry
// acknowledged) with the source stream name; entries in the work and
// unfinished moves (a Dead publication failure, an XACK failure) are not
// touched by the hook. An entry without a task_id — a separate
// OnBuryUnidentified event (T-260, FR-6.1): OnBury stays silent.
func TestReliabClaimDeadOnBury(t *testing.T) {
	var buried []string
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}}, // still into the work
			{ID: "1719-0", Values: map[string]any{fieldData: `{"task_id":"t2"}`}}, // death
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 2}},
			"1719-0": {{ID: "1719-0", RetryCount: 9}}, // the limit of 3 is exhausted
		},
	}
	r := NewReliab(fake, NewRedisQueue(fake, RedisOptions{}), ReliabOptions{
		OnBury: func(stream string) { buried = append(buried, stream) },
	})

	if _, _, _, err := r.Claim(context.Background(), StreamHTTP, "http", "recover-1", time.Minute, 5, "0-0"); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if want := []string{StreamHTTP}; !reflect.DeepEqual(buried, want) {
		t.Errorf("OnBury: %v, want exactly one call with the source stream name %q", buried, StreamHTTP)
	}

	// A Dead publication failure — no move happened (the entry in
	// pending, it will repeat on the next pass): the hook stays silent.
	fake = &fakeClaimRedis{
		fakeRedis: fakeRedis{addErr: errors.New("connection refused")},
		claimMsgs: []redis.XMessage{{ID: "1-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}}},
		pendRes:   map[string][]redis.XPendingExt{"1-0": {{ID: "1-0", RetryCount: 9}}},
	}
	buried = nil
	if _, _, _, err := NewReliab(fake, NewRedisQueue(fake, RedisOptions{}), ReliabOptions{
		OnBury: func(stream string) { buried = append(buried, stream) },
	}).Claim(context.Background(), StreamDNS, "dns", "recover-1", time.Minute, 1, "0-0"); err == nil {
		t.Fatal("a Dead publication failure: an error expected")
	}
	if len(buried) != 0 {
		t.Errorf("OnBury on a Dead publication failure: %v, want empty", buried)
	}

	// An XACK failure — the Dead is published, but the original entry did
	// not leave the pending (the move will repeat, giving a duplicate in
	// q:dead): no death yet, the hook stays silent.
	fake = &fakeClaimRedis{
		fakeRedis: fakeRedis{ackErr: errors.New("connection refused")},
		claimMsgs: []redis.XMessage{{ID: "1-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}}},
		pendRes:   map[string][]redis.XPendingExt{"1-0": {{ID: "1-0", RetryCount: 9}}},
	}
	buried = nil
	if _, _, _, err := NewReliab(fake, NewRedisQueue(fake, RedisOptions{}), ReliabOptions{
		OnBury: func(stream string) { buried = append(buried, stream) },
	}).Claim(context.Background(), StreamDNS, "dns", "recover-1", time.Minute, 1, "0-0"); err == nil {
		t.Fatal("an XACK failure: an error expected")
	}
	if len(fake.adds) != 1 {
		t.Fatalf("the Dead must be published before the XACK: %+v", fake.adds)
	}
	if len(buried) != 0 {
		t.Errorf("OnBury on an XACK failure: %v, want empty (the move not finished)", buried)
	}

	// An entry without a task_id — the terminal branch of FR-6.1: the
	// move completes (the Dead + the XACK), but the event is different —
	// OnBury stays silent, OnBuryUnidentified is called (separate events:
	// the death of a task and the quarantine of an unidentifiable entry).
	fake = &fakeClaimRedis{
		claimMsgs: []redis.XMessage{{ID: "1-0", Values: map[string]any{fieldData: `{"no_task":"id"}`}}},
		pendRes:   map[string][]redis.XPendingExt{"1-0": {{ID: "1-0", RetryCount: 9}}},
	}
	buried = nil
	var unidentified []string
	if _, _, _, err := NewReliab(fake, NewRedisQueue(fake, RedisOptions{}), ReliabOptions{
		OnBury:             func(stream string) { buried = append(buried, stream) },
		OnBuryUnidentified: func(stream string) { unidentified = append(unidentified, stream) },
	}).Claim(context.Background(), StreamDNS, "dns", "recover-1", time.Minute, 1, "0-0"); err != nil {
		t.Fatalf("an entry without task_id: %v — the terminal branch is not an error (FR-6.1)", err)
	}
	if len(buried) != 0 {
		t.Errorf("OnBury of a foreign-format entry: %v, want empty (no task)", buried)
	}
	if want := []string{StreamDNS}; !reflect.DeepEqual(unidentified, want) {
		t.Errorf("OnBuryUnidentified: %v, want exactly one call %q", unidentified, StreamDNS)
	}
	if len(fake.adds) != 1 || len(fake.acks) != 1 {
		t.Errorf("the move of an unidentified entry not finished: %+v %+v", fake.adds, fake.acks)
	}
}

// TestReliabClaimDeadNoHook — a nil hook (isolated builds and tests
// without the registry): bury works as before, without a panic.
func TestReliabClaimDeadNoHook(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{{ID: "1-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}}},
		pendRes:   map[string][]redis.XPendingExt{"1-0": {{ID: "1-0", RetryCount: 9}}},
	}
	got, _, _, err := newTestReliab(fake, ReliabOptions{}).Claim(
		context.Background(), StreamAnalyze, "analyzer", "recover-1", time.Minute, 1, "0-0")
	if err != nil || got != nil {
		t.Fatalf("bury without a hook: (%v, %v), want (nil, nil) — a silent move", got, err)
	}
	if len(fake.adds) != 1 || len(fake.acks) != 1 {
		t.Errorf("the move without a hook not finished: %+v %+v", fake.adds, fake.acks)
	}
}

// TestProcessing — the XACK only after successful processing and the DB
// write.
func TestProcessing(t *testing.T) {
	ctx := context.Background()
	d := Delivery{Stream: StreamDNS, ID: "1718-0", Payload: []byte(`{"task_id":"t1"}`)}

	// Success: the handler wrote the result — the entry is acknowledged.
	fake := &fakeRedis{addID: "1-1"}
	q := NewRedisQueue(fake, RedisOptions{})
	dbWritten := false
	err := Processing(ctx, q, "dns", d, func(_ context.Context, got Delivery) error {
		if got.ID != d.ID {
			t.Errorf("a foreign entry passed to the handler: %+v", got)
		}
		dbWritten = true // "writing the result to the DB"
		return nil
	})
	if err != nil {
		t.Fatalf("Processing: %v", err)
	}
	if !dbWritten {
		t.Fatal("the handler not called")
	}
	if want := []ackCall{{StreamDNS, "dns", []string{"1718-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK after processing: %+v, want %+v", fake.acks, want)
	}

	// A processing error (the DB not written) — without an XACK: the
	// entry will come back via redelivery (Claim).
	fake = &fakeRedis{}
	q = NewRedisQueue(fake, RedisOptions{})
	err = Processing(ctx, q, "dns", d, func(context.Context, Delivery) error {
		return errors.New("db: write result: timeout")
	})
	if err == nil || !contains(err, "db: write result") {
		t.Fatalf("the processing error not returned: %v", err)
	}
	if len(fake.acks) != 0 {
		t.Errorf("XACK after a processing failure: %+v", fake.acks)
	}

	// An XACK error is returned: the result in the DB, the entry in
	// pending — the redelivery will go through by idempotency (spec 3.4).
	fake = &fakeRedis{ackErr: errors.New("ERR connection refused")}
	err = Processing(ctx, NewRedisQueue(fake, RedisOptions{}), "dns", d,
		func(context.Context, Delivery) error { return nil })
	if err == nil || !contains(err, "ack") {
		t.Fatalf("the XACK error not returned: %v", err)
	}

	// Without a handler — a programming error, Redis is not touched.
	fake = &fakeRedis{}
	if err := Processing(ctx, NewRedisQueue(fake, RedisOptions{}), "dns", d, nil); err == nil {
		t.Fatal("the missing-handler error expected")
	}
	if len(fake.acks) != 0 {
		t.Errorf("XACK without a handler: %+v", fake.acks)
	}
}

// TestPublishMaxLenPolicy — without an explicit limit every stream is
// truncated by its own T-025 policy limit, an explicit limit overrides
// all streams.
func TestPublishMaxLenPolicy(t *testing.T) {
	msgs := []struct {
		stream string
		msg    Message
	}{
		{StreamDNS, DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: "a.test"}}}},
		{StreamHTTP, HTTPTask{ZoneID: 1, DomainID: 1, Name: "a.test",
			Subdomains: []HTTPSubdomain{{SubdomainID: 0, Addrs: []string{"192.0.2.1"}}}}},
		{StreamRender, Result{ZoneID: 1, DomainID: 1, SubdomainID: 0}},
		{StreamRenderFallback, Result{ZoneID: 1, DomainID: 1, SubdomainID: 0}},
		{StreamAnalyze, Result{ZoneID: 1, DomainID: 1, SubdomainID: 0}},
		{StreamDead, Dead{TaskID: "t", Stream: StreamDNS, EntryID: "1-0", Attempts: 1,
			Payload: json.RawMessage(`{}`)}},
	}

	fake := &fakeRedis{}
	q := NewRedisQueue(fake, RedisOptions{}) // no limit set — the policy
	for _, m := range msgs {
		if _, err := q.Publish(context.Background(), m.stream, m.msg); err != nil {
			t.Fatalf("Publish to %s: %v", m.stream, err)
		}
	}
	for i, m := range msgs {
		add := fake.adds[i]
		if want := DefaultStreamMaxLen(m.stream); add.MaxLen != want || !add.Approx {
			t.Errorf("%s: MAXLEN ~ %d not applied: MaxLen=%d Approx=%v", m.stream, want, add.MaxLen, add.Approx)
		}
	}

	// An explicit limit — one for all streams.
	fake = &fakeRedis{}
	q = NewRedisQueue(fake, RedisOptions{MaxLen: 50000})
	for _, m := range msgs {
		if _, err := q.Publish(context.Background(), m.stream, m.msg); err != nil {
			t.Fatalf("Publish to %s: %v", m.stream, err)
		}
	}
	for i := range msgs {
		if add := fake.adds[i]; add.MaxLen != 50000 || !add.Approx {
			t.Errorf("the explicit limit ignored: MaxLen=%d Approx=%v", add.MaxLen, add.Approx)
		}
	}
}

func contains(err error, substr string) bool {
	return err != nil && strings.Contains(err.Error(), substr)
}

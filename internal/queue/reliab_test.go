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
		t.Errorf("DefaultMaxAttempts = %d, хочу 3 (ТЗ 4.2; литерал — пин константы)", DefaultMaxAttempts)
	}
	if DefaultClaimMinIdle != 10*time.Minute {
		t.Errorf("DefaultClaimMinIdle = %s, хочу 10m (литерал — пин константы)", DefaultClaimMinIdle)
	}
	r := newTestReliab(&fakeClaimRedis{}, ReliabOptions{})
	if r.opt.MaxAttempts != DefaultMaxAttempts {
		t.Errorf("MaxAttempts: %d, хочу %d", r.opt.MaxAttempts, DefaultMaxAttempts)
	}
	if r.opt.MinIdle != DefaultClaimMinIdle {
		t.Errorf("MinIdle: %s, хочу %s", r.opt.MinIdle, DefaultClaimMinIdle)
	}
	// Single values are valid and are not replaced with the defaults.
	r = newTestReliab(&fakeClaimRedis{}, ReliabOptions{MaxAttempts: 1, MinIdle: time.Nanosecond})
	if r.opt.MaxAttempts != 1 {
		t.Errorf("MaxAttempts = %d, хочу 1 (одна попытка — валидная настройка)", r.opt.MaxAttempts)
	}
	if r.opt.MinIdle != time.Nanosecond {
		t.Errorf("MinIdle = %s, хочу 1нс (положительный простой валиден)", r.opt.MinIdle)
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
		t.Errorf("сумма потолков стримов при пачке %d ≈ %d МиБ > %d МиБ — бюджет Redis 512 МиБ (ТЗ 3.2) пробит: пересогласуйте defaultMaxLen* / scheduler.batch_size (docs/redis.md §3.1)",
			worstBatch, total>>20, streamLimit>>20)
	}
	// The default batch has an order of magnitude more headroom: check
	// the margin.
	def := DefaultStreamMaxLen(StreamDNS)*1000*DNSStreamBytesPerDomain +
		3*DefaultStreamMaxLen(StreamHTTP)*UnitStreamBytesWorst +
		DefaultStreamMaxLen(StreamRenderFallback)*UnitStreamBytesWorst +
		DefaultStreamMaxLen(StreamDead)*DeadStreamBytesWorst
	if def > 400<<20 {
		t.Errorf("сумма потолков при дефолтной пачке ≈ %d МиБ — съедает запас бюджета (межобрезочный остаток TrimLoop, служебная память)", def>>20)
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
		t.Errorf("курсор следующего сканирования = %q, хочу %q (из ответа XAUTOCLAIM)", next, "1720-0")
	}

	// XAUTOCLAIM: the same stream/group, assignment to the recovery loop,
	// the idle filter, the PEL scan from the beginning, the batch limit.
	if len(fake.claims) != 1 {
		t.Fatalf("вызовов XAUTOCLAIM: %d, хочу 1", len(fake.claims))
	}
	args := fake.claims[0]
	if args.Stream != StreamHTTP || args.Group != "http" || args.Consumer != "recover-1" {
		t.Errorf("XAUTOCLAIM стрим/группа/потребитель: %q/%q/%q", args.Stream, args.Group, args.Consumer)
	}
	if args.MinIdle != 15*time.Minute {
		t.Errorf("XAUTOCLAIM MinIdle: %s, хочу 15m", args.MinIdle)
	}
	if args.Start != "0-0" || args.Count != 10 {
		t.Errorf("XAUTOCLAIM Start/Count: %q/%d, хочу 0-0/10", args.Start, args.Count)
	}

	// The explicit cursor is passed to XAUTOCLAIM undistorted (Q-07): the
	// calling loop continues the PEL walk from the kept position.
	if _, _, _, err := r.Claim(context.Background(), StreamHTTP, "http", "recover-1", 15*time.Minute, 10, "1750-3"); err != nil {
		t.Fatalf("Claim с курсором: %v", err)
	}
	if got := fake.claims[1].Start; got != "1750-3" {
		t.Errorf("XAUTOCLAIM Start с курсором: %q, хочу 1750-3", got)
	}

	// The attempt counter — XPENDING EXTENDED per entry (exactly its
	// range), the RetryCount field (the entry's PEL deliveries counter).
	if len(fake.pends) != 2 {
		t.Fatalf("вызовов XPENDING: %d, хочу 2", len(fake.pends))
	}
	for i, id := range []string{"1718-0", "1719-0"} {
		p := fake.pends[i]
		if p.Stream != StreamHTTP || p.Group != "http" {
			t.Errorf("XPENDING №%d стрим/группа: %q/%q", i+1, p.Stream, p.Group)
		}
		if p.Start != id || p.End != id || p.Count != 1 {
			t.Errorf("XPENDING №%d диапазон: %q..%q count=%d, хочу ровно %q", i+1, p.Start, p.End, p.Count, id)
		}
	}

	want := []Claimed{
		{Delivery{StreamHTTP, "1718-0", []byte(`{"task_id":"t1"}`)}, 2},
		{Delivery{StreamHTTP, "1719-0", []byte(`{"task_id":"t2"}`)}, 3},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Claim:\n получил  %+v\n хочу     %+v", got, want)
	}

	// The processed entries do not touch q:dead and are not acknowledged
	// by the claim: the XACK is Processing's job after the result is
	// written to the DB.
	if len(fake.adds) != 0 || len(fake.acks) != 0 {
		t.Errorf("Claim не публикует и не подтверждает рабочие записи: %+v %+v", fake.adds, fake.acks)
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
		t.Errorf("MinIdle: %s, хочу %s", got, DefaultClaimMinIdle)
	}
	if got := fake.claims[0].Start; got != "0-0" {
		t.Errorf("Start: %q, хочу 0-0 (пустой старт — с начала PEL)", got)
	}

	fake = &fakeClaimRedis{}
	r = newTestReliab(fake, ReliabOptions{MinIdle: 2 * time.Minute})
	if _, _, _, err := r.Claim(context.Background(), StreamDNS, "dns", "recover-1", 0, 1, ""); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got := fake.claims[0].MinIdle; got != 2*time.Minute {
		t.Errorf("MinIdle: %s, хочу 2m из опции", got)
	}

	// An explicit minIdle of 1ns is valid and is not replaced by the
	// layer's option.
	fake = &fakeClaimRedis{}
	r = newTestReliab(fake, ReliabOptions{MinIdle: time.Hour})
	if _, _, _, err := r.Claim(context.Background(), StreamDNS, "dns", "recover-1", time.Nanosecond, 1, ""); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if got := fake.claims[0].MinIdle; got != time.Nanosecond {
		t.Errorf("MinIdle: %s, хочу 1нс (положительный явный minIdle валиден)", got)
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
		t.Errorf("исчерпавшая попытки запись возвращена в работу: %+v", got)
	}

	// The Dead is published to q:dead before the XACK of the original
	// entry.
	if len(fake.adds) != 1 {
		t.Fatalf("публикаций в q:dead: %d, хочу 1", len(fake.adds))
	}
	add := fake.adds[0]
	if add.Stream != StreamDead || add.MaxLen != DefaultStreamMaxLen(StreamDead) || !add.Approx {
		t.Errorf("XADD: стрим %q MaxLen=%d Approx=%v, хочу %q MAXLEN ~ %d",
			add.Stream, add.MaxLen, add.Approx, StreamDead, DefaultStreamMaxLen(StreamDead))
	}
	data := add.Values.(map[string]any)[fieldData].([]byte)
	dead, err := Decode[Dead](Delivery{Stream: StreamDead, ID: add.ID, Payload: data})
	if err != nil {
		t.Fatalf("поле data не декодируется как Dead: %v\n%s", err, data)
	}
	wantDead := Dead{
		TaskID:   "t1",
		Stream:   StreamRender,
		EntryID:  "1718-0",
		Attempts: 4,
		Reason:   "исчерпан лимит попыток 3: доставок без подтверждения — 4",
		ZoneID:   3,
		DomainID: 42,
		Name:     "example.com",
	}
	if !reflect.DeepEqual(dead, wantDead) {
		t.Errorf("Dead:\n получил  %+v\n хочу     %+v", dead, wantDead)
	}

	// The original entry leaves the pending: XACK from the source stream.
	if want := []ackCall{{StreamRender, "render", []string{"1718-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK: %+v, хочу %+v", fake.acks, want)
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
		t.Fatalf("сериализация пачки: %v", err)
	}
	if len(payload) < 100_000 { // ~70 B per domain with the JSON envelope
		t.Fatalf("пачка неожиданно мала: %d байт — тест не про worst-case", len(payload))
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
		t.Fatalf("исчерпавшая попытки запись возвращена в работу: %+v", got)
	}
	if len(fake.adds) != 1 || fake.adds[0].Stream != StreamDead {
		t.Fatalf("Dead не опубликован: %+v", fake.adds)
	}
	data := fake.adds[0].Values.(map[string]any)[fieldData].([]byte)
	if bound := 512; len(data) > bound {
		t.Errorf("запись q:dead %d байт > %d — payload исходной задачи перенесён (T-105):\n%s",
			len(data), bound, data)
	}

	dead, err := Decode[Dead](Delivery{Stream: StreamDead, ID: fake.adds[0].ID, Payload: data})
	if err != nil {
		t.Fatalf("поле data не декодируется как Dead: %v\n%s", err, data)
	}
	// The identifiers for the operator's triage are in place, no payload.
	if dead.TaskID != "big" || dead.Stream != StreamDNS || dead.EntryID != "1718-0" || dead.Attempts != 9 {
		t.Errorf("идентификаторы записи q:dead: %+v", dead)
	}
	if dead.Domains != len(big.Domains) {
		t.Errorf("размер пачки в записи: %d, хочу %d", dead.Domains, len(big.Domains))
	}
	if len(dead.Payload) != 0 {
		t.Errorf("payload перенесён в q:dead: %d байт", len(dead.Payload))
	}

	// The original entry leaves the pending: XACK from the source stream.
	if want := []ackCall{{StreamDNS, "dns", []string{"1718-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK: %+v, хочу %+v", fake.acks, want)
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
		t.Fatalf("Claim: %v — перенос не должен падать на чужом теле", err)
	}
	if got != nil {
		t.Fatalf("исчерпавшая попытки запись возвращена в работу: %+v", got)
	}
	if len(fake.adds) != 1 || fake.adds[0].Stream != StreamDead {
		t.Fatalf("Dead не опубликован: %+v", fake.adds)
	}
	data := fake.adds[0].Values.(map[string]any)[fieldData].([]byte)
	dead, err := Decode[Dead](Delivery{Stream: StreamDead, ID: fake.adds[0].ID, Payload: data})
	if err != nil {
		t.Fatalf("поле data не декодируется как Dead: %v\n%s", err, data)
	}
	want := Dead{
		TaskID:   "t1",
		Stream:   StreamRender,
		EntryID:  "1718-0",
		Attempts: 9,
		Reason:   "исчерпан лимит попыток 3: доставок без подтверждения — 9",
	}
	if !reflect.DeepEqual(dead, want) {
		t.Errorf("Dead с чужим телом:\n получил  %+v\n хочу     %+v (без идентификаторов предмета)", dead, want)
	}
	if want := []ackCall{{StreamRender, "render", []string{"1718-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK: %+v, хочу %+v", fake.acks, want)
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
		t.Fatalf("Claim записи без task_id: %v — терминальная ветка не ошибка (ФТ-6.1)", err)
	}
	if got != nil {
		t.Errorf("исчерпавшая попытки запись возвращена в работу: %+v", got)
	}

	// The Dead in q:dead: a synthetic task_id, without subject fields.
	if len(fake.adds) != 1 || fake.adds[0].Stream != StreamDead {
		t.Fatalf("публикаций в q:dead: %d, хочу 1", len(fake.adds))
	}
	data := fake.adds[0].Values.(map[string]any)[fieldData].([]byte)
	dead, err := Decode[Dead](Delivery{Stream: StreamDead, ID: fake.adds[0].ID, Payload: data})
	if err != nil {
		t.Fatalf("поле data не декодируется как Dead: %v\n%s", err, data)
	}
	want := Dead{
		TaskID:   "unknown:q:http:1718-0",
		Stream:   StreamHTTP,
		EntryID:  "1718-0",
		Attempts: 9,
		Reason:   "запись вне форматов пакета (без task_id), лимит попыток 3: доставок без подтверждения — 9",
	}
	if !reflect.DeepEqual(dead, want) {
		t.Errorf("Dead неидентифицируемой записи:\n получил  %+v\n хочу     %+v (синтетический id, без предметных полей)", dead, want)
	}

	// The entry leaves the pending: XACK of the original — the claim loop
	// is broken open.
	if want := []ackCall{{StreamHTTP, "http", []string{"1718-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK: %+v, хочу %+v", fake.acks, want)
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
		t.Fatalf("смешанная пачка: %v", err)
	}
	if want := []string{StreamDNS}; !reflect.DeepEqual(buried, want) {
		t.Errorf("OnBury смешанной пачки: %v, хочу один вызов %q (только запись с task_id)", buried, StreamDNS)
	}
	if want := []string{StreamDNS}; !reflect.DeepEqual(unidentified, want) {
		t.Errorf("OnBuryUnidentified смешанной пачки: %v, хочу один вызов %q", unidentified, StreamDNS)
	}
	if len(fake.adds) != 2 || len(fake.acks) != 2 {
		t.Errorf("смешанная пачка не переведена целиком: %+v %+v", fake.adds, fake.acks)
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
		{"первая доставка", 1, false},
		{"последняя разрешённая", 3, false},
		{"исчерпаны", 4, true},
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
				t.Errorf("ожидается перевод в q:dead, получена работа: %+v", got)
			}
			if !c.wantNil {
				if len(got) != 1 || got[0].Attempts != int(c.attempt) {
					t.Errorf("ожидались записи с попыткой %d, получено %+v", c.attempt, got)
				}
				if len(fake.adds) != 0 {
					t.Errorf("рабочая запись опубликована в q:dead: %+v", fake.adds)
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
		t.Errorf("нечего возвращать, получено %+v", got)
	}
	if len(fake.pends) != 1 || fake.pends[0].Start != "1719-0" {
		t.Errorf("XPENDING должен спросить только запись с телом: %+v", fake.pends)
	}
	if len(fake.adds) != 0 || len(fake.acks) != 0 {
		t.Errorf("пропуски не публикуются и не подтверждаются: %+v %+v", fake.adds, fake.acks)
	}
}

// TestReliabClaimErrors — command and argument errors; a publication
// failure to q:dead does not acknowledge the original entry (the XACK
// only after the Dead).
func TestReliabClaimErrors(t *testing.T) {
	ctx := context.Background()

	// Bad arguments — without touching Redis.
	for name, call := range map[string]func(r *Reliab) error{
		"неизвестный стрим": func(r *Reliab) error {
			_, _, _, err := r.Claim(ctx, "q:typo", "g", "c", time.Minute, 1, "0-0")
			return err
		},
		"без группы": func(r *Reliab) error {
			_, _, _, err := r.Claim(ctx, StreamDNS, "", "c", time.Minute, 1, "0-0")
			return err
		},
		"без потребителя": func(r *Reliab) error {
			_, _, _, err := r.Claim(ctx, StreamDNS, "g", "", time.Minute, 1, "0-0")
			return err
		},
		"пачка 0": func(r *Reliab) error {
			_, _, _, err := r.Claim(ctx, StreamDNS, "g", "c", time.Minute, 0, "0-0")
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			fake := &fakeClaimRedis{}
			if err := call(newTestReliab(fake, ReliabOptions{})); err == nil {
				t.Fatal("ожидалась ошибка аргументов")
			}
			if len(fake.claims)+len(fake.pends) != 0 {
				t.Errorf("Redis вызван с плохими аргументами: %+v %+v", fake.claims, fake.pends)
			}
		})
	}

	// An XAUTOCLAIM error.
	fake := &fakeClaimRedis{claimErr: errors.New("ERR connection refused")}
	if _, _, _, err := newTestReliab(fake, ReliabOptions{}).Claim(ctx, StreamDNS, "dns", "recover-1", time.Minute, 1, "0-0"); err == nil ||
		!contains(err, "claim") {
		t.Errorf("ошибка XAUTOCLAIM: %v", err)
	}

	// An XPENDING error.
	fake = &fakeClaimRedis{
		claimMsgs: []redis.XMessage{{ID: "1-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}}},
		pendErr:   errors.New("ERR connection refused"),
	}
	if _, _, _, err := newTestReliab(fake, ReliabOptions{}).Claim(ctx, StreamDNS, "dns", "recover-1", time.Minute, 1, "0-0"); err == nil ||
		!contains(err, "счётчик доставок") {
		t.Errorf("ошибка XPENDING: %v", err)
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
		t.Errorf("ошибка публикации Dead: %v", err)
	}
	if len(fake.acks) != 0 {
		t.Errorf("XACK после сбоя публикации Dead: %+v", fake.acks)
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
		!contains(err, "подтверждение") {
		t.Errorf("ошибка XACK: %v", err)
	}
	if len(fake.adds) != 1 {
		t.Errorf("Dead должен быть опубликован до XACK: %+v", fake.adds)
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
		t.Fatalf("команд возврата: %d, хочу 2 (группы по счётчику)", len(fake.dos))
	}
	byRC := make(map[int64][]interface{})
	for _, args := range fake.dos {
		if got := args[0]; got != "xclaim" {
			t.Errorf("команда возврата: %v, хочу xclaim", got)
		}
		if stream, group, consumer := args[1], args[2], args[3]; stream != StreamHTTP || group != "http" || consumer != "recover-1" {
			t.Errorf("xclaim стрим/группа/потребитель: %v/%v/%v", stream, group, consumer)
		}
		if idle := args[4]; idle != int64(0) {
			t.Errorf("xclaim min-idle: %v, хочу 0 (записи только что закреплены этим потребителем)", idle)
		}
		ids := append([]interface{}{}, args[5:len(args)-3]...)
		rc := args[len(args)-1].(int64)
		if args[len(args)-3] != "justid" || args[len(args)-2] != "retrycount" {
			t.Errorf("xclaim без JUSTID RETRYCOUNT: %v", args)
		}
		byRC[rc] = ids
	}
	if ids := byRC[1]; !reflect.DeepEqual(ids, []interface{}{"1-1", "1-2"}) {
		t.Errorf("группа retrycount 1: %v, хочу [1-1 1-2] (счётчик 2 до клейма-1)", ids)
	}
	if ids := byRC[2]; !reflect.DeepEqual(ids, []interface{}{"1-3"}) {
		t.Errorf("группа retrycount 2: %v, хочу [1-3] (счётчик 3 до клейма-1)", ids)
	}

	// An empty refund — no commands; a nil window (a client without Do)
	// is transparent.
	fake = &fakeClaimRedis{}
	if err := newTestReliab(fake, ReliabOptions{}).Refund(context.Background(), "g", "c", nil); err != nil {
		t.Errorf("Refund без записей: %v, хочу nil", err)
	}
	if len(fake.dos) != 0 {
		t.Errorf("команд без записей: %d, хочу 0", len(fake.dos))
	}
	claimOnly := struct{ claimStreams }{fake}
	ro := NewReliab(claimOnly, NewRedisQueue(fake, RedisOptions{}), ReliabOptions{})
	if err := ro.Refund(context.Background(), "g", "c", claimed); err != nil {
		t.Errorf("Refund без окна Do: %v, хочу прозрачный nil (поведение до T-222)", err)
	}
	if len(fake.dos) != 0 {
		t.Errorf("команд без окна Do: %d, хочу 0", len(fake.dos))
	}

	// A command error — in the return with the entries' context; it does
	// not cancel the second group (here there is only one).
	fake = &fakeClaimRedis{doErr: errors.New("ERR no such key")}
	err := newTestReliab(fake, ReliabOptions{}).Refund(context.Background(), "http", "recover-1", claimed[:1])
	if err == nil || !contains(err, "возврат попыток") || !contains(err, "1-1") {
		t.Errorf("ошибка возврата: %v, хочу ошибку с записью 1-1", err)
	}

	// The zero counter clamp: Attempts 0 → retrycount 0, not −1 (a
	// negative RETRYCOUNT — a server error).
	fake = &fakeClaimRedis{}
	zero := []Claimed{{Delivery{StreamHTTP, "9-9", []byte(`{"task_id":"t9"}`)}, 0}}
	if err := newTestReliab(fake, ReliabOptions{}).Refund(context.Background(), "http", "recover-1", zero); err != nil {
		t.Fatalf("Refund нулевого счётчика: %v", err)
	}
	if rc := fake.dos[0][len(fake.dos[0])-1]; rc != int64(0) {
		t.Errorf("retrycount нулевого счётчика: %v, хочу 0", rc)
	}

	// An unknown stream — an error before Redis (the Claim style).
	fake = &fakeClaimRedis{}
	bad := []Claimed{{Delivery{"q:typo", "1-1", []byte(`{"task_id":"t1"}`)}, 2}}
	if err := newTestReliab(fake, ReliabOptions{}).Refund(context.Background(), "http", "recover-1", bad); err == nil || !contains(err, "неизвестный стрим") {
		t.Errorf("Refund неизвестного стрима: %v, хочу ошибку", err)
	}
	if len(fake.dos) != 0 {
		t.Errorf("Redis вызван с плохим стримом: %+v", fake.dos)
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
		t.Fatalf("чтений XPENDING: %d, хочу 2 (по каждой записи)", len(fake.pends))
	}
	for i, id := range []string{"1-1", "1-2"} {
		p := fake.pends[i]
		if p.Stream != StreamHTTP || p.Group != "http" || p.Start != id || p.End != id || p.Count != 1 {
			t.Errorf("XPENDING №%d: %+v, хочу ровно запись %s", i+1, p, id)
		}
	}
	if len(fake.dos) != 2 {
		t.Fatalf("команд возврата: %d, хочу 2 (группы по счётчику)", len(fake.dos))
	}
	byRC := make(map[int64][]interface{})
	for _, args := range fake.dos {
		if got := args[0]; got != "xclaim" {
			t.Errorf("команда возврата: %v, хочу xclaim", got)
		}
		if stream, group, consumer := args[1], args[2], args[3]; stream != StreamHTTP || group != "http" || consumer != "recover-1" {
			t.Errorf("xclaim стрим/группа/потребитель: %v/%v/%v", stream, group, consumer)
		}
		if idle := args[4]; idle != int64(0) {
			t.Errorf("xclaim min-idle: %v, хочу 0", idle)
		}
		ids := append([]interface{}{}, args[5:len(args)-3]...)
		if args[len(args)-3] != "justid" || args[len(args)-2] != "retrycount" {
			t.Errorf("xclaim без JUSTID RETRYCOUNT: %v", args)
		}
		byRC[args[len(args)-1].(int64)] = ids
	}
	if ids := byRC[1]; !reflect.DeepEqual(ids, []interface{}{"1-1"}) {
		t.Errorf("группа retrycount 1: %v, хочу [1-1] (счётчик 2 − клейм)", ids)
	}
	if ids := byRC[4]; !reflect.DeepEqual(ids, []interface{}{"1-2"}) {
		t.Errorf("группа retrycount 4: %v, хочу [1-2] (счётчик 5 − клейм)", ids)
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
		t.Fatalf("RefundUnclassified с покинувшей PEL записью: %v", err)
	}
	if len(fake.dos) != 1 {
		t.Fatalf("команд возврата: %d, хочу 1 (только живая в PEL запись)", len(fake.dos))
	}
	if ids := fake.dos[0][5 : len(fake.dos[0])-3]; !reflect.DeepEqual(ids, []interface{}{"1-1"}) {
		t.Errorf("возвращена не та запись: %v, хочу [1-1]", ids)
	}

	// A repeated counter read failure — an error with the entry's
	// context, no refund commands (the attempt stays spent — no worse
	// than the behavior before T-260).
	fake = &fakeClaimRedis{
		pendOneErr: map[string]error{"1-1": errors.New("ERR connection refused")},
	}
	if err := newTestReliab(fake, ReliabOptions{}).RefundUnclassified(ctx, "http", "recover-1", ds[:1]); err == nil ||
		!contains(err, "счётчик доставок") || !contains(err, "1-1") {
		t.Errorf("повторный сбой чтения: %v, хочу ошибку с записью 1-1", err)
	}
	if len(fake.dos) != 0 {
		t.Errorf("команд при повторном сбое: %d, хочу 0", len(fake.dos))
	}

	// An empty list — nil; the Do window unavailable — a transparent nil
	// (the Refund style: the attempts spent, as before T-222).
	fake = &fakeClaimRedis{}
	if err := newTestReliab(fake, ReliabOptions{}).RefundUnclassified(ctx, "g", "c", nil); err != nil {
		t.Errorf("RefundUnclassified без записей: %v, хочу nil", err)
	}
	if len(fake.pends) != 0 || len(fake.dos) != 0 {
		t.Errorf("Redis вызван без записей: %+v %+v", fake.pends, fake.dos)
	}
	claimOnly := struct{ claimStreams }{fake}
	ro := NewReliab(claimOnly, NewRedisQueue(fake, RedisOptions{}), ReliabOptions{})
	if err := ro.RefundUnclassified(ctx, "http", "recover-1", ds); err != nil {
		t.Errorf("RefundUnclassified без окна Do: %v, хочу прозрачный nil", err)
	}
	if len(fake.dos) != 0 {
		t.Errorf("команд без окна Do: %d, хочу 0", len(fake.dos))
	}

	// Bad arguments — an error before Redis (the Refund/Claim style).
	fake = &fakeClaimRedis{}
	if err := newTestReliab(fake, ReliabOptions{}).RefundUnclassified(ctx, "", "c", ds); err == nil || !contains(err, "возврат попыток") {
		t.Errorf("RefundUnclassified без группы: %v, хочу ошибку", err)
	}
	badStream := []Delivery{{Stream: "q:typo", ID: "1-1", Payload: []byte(`{}`)}}
	if err := newTestReliab(fake, ReliabOptions{}).RefundUnclassified(ctx, "http", "recover-1", badStream); err == nil || !contains(err, "неизвестный стрим") {
		t.Errorf("RefundUnclassified неизвестного стрима: %v, хочу ошибку", err)
	}
	if len(fake.pends) != 0 || len(fake.dos) != 0 {
		t.Errorf("Redis вызван с плохими аргументами: %+v %+v", fake.pends, fake.dos)
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
		t.Fatalf("Claim после отмены: %v — классификация обязана переживать отмену", err)
	}
	want := []Claimed{
		{Delivery{StreamHTTP, "1-1", []byte(`{"task_id":"t1"}`)}, 2},
		{Delivery{StreamHTTP, "1-2", []byte(`{"task_id":"t2"}`)}, 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Claim после отмены:\n получил  %+v\n хочу     %+v (счётчики прочитаны, записи возвратимы)", got, want)
	}
}

// TestReliabClaimEmpty — no stuck entries: an empty result without an
// error, the next pass's cursor — from the beginning of the PEL.
func TestReliabClaimEmpty(t *testing.T) {
	fake := &fakeClaimRedis{}
	got, _, next, err := newTestReliab(fake, ReliabOptions{}).Claim(
		context.Background(), StreamAnalyze, "analyzer", "recover-1", time.Minute, 5, "0-0")
	if err != nil || got != nil {
		t.Errorf("пустой claim: (%v, %v), хочу (nil, nil)", got, err)
	}
	if next != "0-0" {
		t.Errorf("next пустого обхода = %q, хочу 0-0 (обзор PEL завершён — с начала)", next)
	}
	if len(fake.pends) != 0 {
		t.Errorf("XPENDING без клейм не нужен: %+v", fake.pends)
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
			name: "сбой счётчика первой записи",
			pendRes: map[string][]redis.XPendingExt{
				"1719-0": {{ID: "1719-0", RetryCount: 2}},
			},
			pendErr: map[string]error{"1718-0": errors.New("ERR connection refused")},
			wantErr: "счётчик доставок",
		},
		{
			name: "первая покинула PEL",
			pendRes: map[string][]redis.XPendingExt{
				"1718-0": nil, // acknowledged between the commands
				"1719-0": {{ID: "1719-0", RetryCount: 2}},
			},
		},
		{
			name: "первая исчерпала попытки",
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
				t.Errorf("Claim после специальной записи:\n получил  %+v\n хочу     %+v", got, want)
			}
			if c.wantErr != "" && (err == nil || !contains(err, c.wantErr)) {
				t.Errorf("ожидалась ошибка %q, получено: %v", c.wantErr, err)
			}
			if c.wantErr == "" && err != nil {
				t.Errorf("ожидалось отсутствие ошибки, получено: %v", err)
			}
			if c.wantBury {
				if len(fake.adds) != 1 || fake.adds[0].Stream != StreamDead {
					t.Errorf("первая запись не перенесена в q:dead: %+v", fake.adds)
				}
				if want := []ackCall{{StreamHTTP, "http", []string{"1718-0"}}}; !reflect.DeepEqual(fake.acks, want) {
					t.Errorf("XACK перенесённой записи: %+v, хочу %+v", fake.acks, want)
				}
			} else if len(fake.adds) != 0 || len(fake.acks) != 0 {
				t.Errorf("специальная запись не должна публиковаться и подтверждаться: %+v %+v", fake.adds, fake.acks)
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
	if err == nil || !contains(err, "счётчик доставок") || !contains(err, "1719-0") {
		t.Fatalf("частичный сбой: err = %v, want ошибки счётчика записи 1719-0", err)
	}
	want := []Claimed{
		{Delivery{StreamHTTP, "1718-0", []byte(`{"task_id":"t1"}`)}, 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("валидная часть:\n получил  %+v\n хочу     %+v", got, want)
	}
	// The entry without a counter is returned as the second result for
	// the attempt refund (FR-6.2): the claim already spent it — it is
	// neither lost nor let into the work with an unknown counter.
	wantUnclassified := []Delivery{
		{Stream: StreamHTTP, ID: "1719-0", Payload: []byte(`{"task_id":"t2"}`)},
	}
	if !reflect.DeepEqual(unclassified, wantUnclassified) {
		t.Errorf("записи без классификации:\n получил  %+v\n хочу     %+v", unclassified, wantUnclassified)
	}
	if len(fake.acks) != 0 || len(fake.adds) != 0 {
		t.Errorf("клейм не подтверждает и не хоронит: %+v %+v", fake.acks, fake.adds)
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
		t.Errorf("OnBury: %v, хочу ровно один вызов с именем стрима-источника %q", buried, StreamHTTP)
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
		t.Fatal("сбой публикации Dead: ожидалась ошибка")
	}
	if len(buried) != 0 {
		t.Errorf("OnBury при сбое публикации Dead: %v, хочу пусто", buried)
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
		t.Fatal("сбой XACK: ожидалась ошибка")
	}
	if len(fake.adds) != 1 {
		t.Fatalf("Dead должен быть опубликован до XACK: %+v", fake.adds)
	}
	if len(buried) != 0 {
		t.Errorf("OnBury при сбое XACK: %v, хочу пусто (перевод не завершён)", buried)
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
		t.Fatalf("запись без task_id: %v — терминальная ветка не ошибка (ФТ-6.1)", err)
	}
	if len(buried) != 0 {
		t.Errorf("OnBury неформатной записи: %v, хочу пусто (задачи нет)", buried)
	}
	if want := []string{StreamDNS}; !reflect.DeepEqual(unidentified, want) {
		t.Errorf("OnBuryUnidentified: %v, хочу ровно один вызов %q", unidentified, StreamDNS)
	}
	if len(fake.adds) != 1 || len(fake.acks) != 1 {
		t.Errorf("перевод неидентифицируемой записи не завершён: %+v %+v", fake.adds, fake.acks)
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
		t.Fatalf("bury без хука: (%v, %v), хочу (nil, nil) — тихий перевод", got, err)
	}
	if len(fake.adds) != 1 || len(fake.acks) != 1 {
		t.Errorf("перевод без хука не завершён: %+v %+v", fake.adds, fake.acks)
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
			t.Errorf("обработчику передана чужая запись: %+v", got)
		}
		dbWritten = true // "writing the result to the DB"
		return nil
	})
	if err != nil {
		t.Fatalf("Processing: %v", err)
	}
	if !dbWritten {
		t.Fatal("обработчик не вызван")
	}
	if want := []ackCall{{StreamDNS, "dns", []string{"1718-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK после обработки: %+v, хочу %+v", fake.acks, want)
	}

	// A processing error (the DB not written) — without an XACK: the
	// entry will come back via redelivery (Claim).
	fake = &fakeRedis{}
	q = NewRedisQueue(fake, RedisOptions{})
	err = Processing(ctx, q, "dns", d, func(context.Context, Delivery) error {
		return errors.New("db: write result: timeout")
	})
	if err == nil || !contains(err, "db: write result") {
		t.Fatalf("ошибка обработки не возвращена: %v", err)
	}
	if len(fake.acks) != 0 {
		t.Errorf("XACK после сбоя обработки: %+v", fake.acks)
	}

	// An XACK error is returned: the result in the DB, the entry in
	// pending — the redelivery will go through by idempotency (spec 3.4).
	fake = &fakeRedis{ackErr: errors.New("ERR connection refused")}
	err = Processing(ctx, NewRedisQueue(fake, RedisOptions{}), "dns", d,
		func(context.Context, Delivery) error { return nil })
	if err == nil || !contains(err, "ack") {
		t.Fatalf("ошибка XACK не возвращена: %v", err)
	}

	// Without a handler — a programming error, Redis is not touched.
	fake = &fakeRedis{}
	if err := Processing(ctx, NewRedisQueue(fake, RedisOptions{}), "dns", d, nil); err == nil {
		t.Fatal("ожидалась ошибка отсутствия обработчика")
	}
	if len(fake.acks) != 0 {
		t.Errorf("XACK без обработчика: %+v", fake.acks)
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
			t.Fatalf("Publish в %s: %v", m.stream, err)
		}
	}
	for i, m := range msgs {
		add := fake.adds[i]
		if want := DefaultStreamMaxLen(m.stream); add.MaxLen != want || !add.Approx {
			t.Errorf("%s: MAXLEN ~ %d не применён: MaxLen=%d Approx=%v", m.stream, want, add.MaxLen, add.Approx)
		}
	}

	// An explicit limit — one for all streams.
	fake = &fakeRedis{}
	q = NewRedisQueue(fake, RedisOptions{MaxLen: 50000})
	for _, m := range msgs {
		if _, err := q.Publish(context.Background(), m.stream, m.msg); err != nil {
			t.Fatalf("Publish в %s: %v", m.stream, err)
		}
	}
	for i := range msgs {
		if add := fake.adds[i]; add.MaxLen != 50000 || !add.Approx {
			t.Errorf("явный предел проигнорирован: MaxLen=%d Approx=%v", add.MaxLen, add.Approx)
		}
	}
}

func contains(err error, substr string) bool {
	return err != nil && strings.Contains(err.Error(), substr)
}

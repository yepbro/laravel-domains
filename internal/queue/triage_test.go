// Tests of the triage of the surviving mutants of the CI run of
// Nov 10, 2026 (part B, score 0.857): coverage top-ups of queue by
// triage groups — the exact values of the constants and the withDefaults
// boundaries (literal pins, comparing with the constant itself the
// mutant goes unnoticed), pass step errors (muting returns, the count on
// an error must be zero), loop breaks (break instead of continue), the
// consumer cleanup threshold boundaries, textual Redis errors ("no such
// key", server answer prefixes on bare strings — the string fallback of
// the Is* helpers), stopping the XADD retries after a success or a
// non-infrastructure error, the PEL recheck arguments (the race window
// shrunk to a single entry), the clamps and the guard branches of the
// attempt refunds, the trim point boundaries (the "XINFO→XPENDING"
// race, exactly 3/4 of the cap), the stuck prefix signals, the zero
// tarpit counter count on validation/command errors and the RecoverLoop
// exit without a claim after a ctx cancel. Logging and equivalent
// mutants are out of scope of the tests (the blacklist,
// docs/mutation-testing.md §4).
package queue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// --- consumers.go ----------------------------------------------------------

// triConsumersFake — a fakeConsumers with stubbed DELCONSUMER errors by
// the "stream|group|consumer" candidate (the base fake always answers
// with success): muting the deletion error return and breaking the loop
// per candidate.
type triConsumersFake struct {
	*fakeConsumers
	delErrs map[string]error
}

func (f *triConsumersFake) XGroupDelConsumer(ctx context.Context, stream, group, consumer string) *redis.IntCmd {
	f.fakeConsumers.XGroupDelConsumer(ctx, stream, group, consumer) // the protocol recording
	if err, ok := f.delErrs[stream+"|"+group+"|"+consumer]; ok {
		return redis.NewIntResult(0, err)
	}
	return redis.NewIntResult(f.delPEL, nil)
}

// TestConsumersTriagePins — literal pins of the hygiene constants and
// the withDefaults boundaries: the DefaultConsumerIdle threshold —
// exactly 2 h; single (1 ns) Idle/Interval — legitimate explicit values,
// the default does not replace them.
func TestConsumersTriagePins(t *testing.T) {
	if DefaultConsumerIdle != 2*time.Hour {
		t.Errorf("DefaultConsumerIdle = %s, want exactly 2h (T-155: harsh restarts of waves)", DefaultConsumerIdle)
	}
	o := ConsumerOptions{Idle: time.Nanosecond, Interval: time.Nanosecond}.withDefaults()
	if o.Idle != time.Nanosecond || o.Interval != time.Nanosecond {
		t.Errorf("unit Idle/Interval replaced by the defaults: %+v (the default only for ≤ 0)", o)
	}
}

// TestConsumersTriageIdleBoundary — inactivity exactly at the threshold:
// a consumer with idle == Idle is a candidate (the "idle ≥" threshold),
// it is removed.
func TestConsumersTriageIdleBoundary(t *testing.T) {
	f := &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamHTTP + "|http-worker": {
				{Name: "http-worker-1", Pending: 0, Idle: 2 * time.Hour}, // exactly the threshold
			},
		},
		pendRes: map[string][]redis.XPendingExt{},
	}
	removed, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{StreamHTTP: "http-worker"},
		Idle:   2 * time.Hour,
	})
	if err != nil {
		t.Fatalf("CleanupConsumersOnce: %v", err)
	}
	if removed != 1 {
		t.Errorf("removed %d, want 1 (idle == the threshold — already dead)", removed)
	}
}

// TestConsumersTriageXInfoErrors — an error reading the group's consumer
// list: no stream/group (all three forms — redis.Nil, the "no such key"
// text, NOGROUP) — an empty pass without an error; any other error — a
// pass error (a muted return would stay silent: a count of 0, no
// DELCONSUMER).
func TestConsumersTriageXInfoErrors(t *testing.T) {
	quiet := map[string]error{
		"redis.Nil":            redis.Nil,
		"the no such key text": errors.New("ERR no such key"), // the textual form of the Redis answer
		"NOGROUP":              errors.New("NOGROUP No such consumer group 'x' for key name 'y'"),
	}
	for name, err := range quiet {
		f := &fakeConsumers{listsErr: map[string]error{StreamHTTP + "|http-worker": err}}
		removed, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
			Groups: map[string]string{StreamHTTP: "http-worker"},
		})
		if err != nil {
			t.Errorf("%s: %v (no stream/group — not an error)", name, err)
		}
		if removed != 0 {
			t.Errorf("%s: removed %d, want 0", name, removed)
		}
	}

	f := &fakeConsumers{listsErr: map[string]error{StreamDNS + "|dns-worker": errors.New("connection reset")}}
	removed, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{StreamDNS: "dns-worker"},
	})
	if err == nil {
		t.Fatal("the consumer list read error swallowed")
	}
	if removed != 0 {
		t.Errorf("removed %d on error, want 0", removed)
	}
}

// TestConsumersTriageNilClientCounts — a nil client: an error with a
// ZERO removal count (Once) and an immediate loop error (Loop).
func TestConsumersTriageNilClientCounts(t *testing.T) {
	removed, err := CleanupConsumersOnce(context.Background(), nil, ConsumerOptions{})
	if err == nil {
		t.Fatal("a nil client not rejected")
	}
	if removed != 0 {
		t.Errorf("removed %d with a nil client, want 0", removed)
	}
	loopErr := make(chan error, 1)
	go func() { loopErr <- CleanupConsumersLoop(context.Background(), nil, ConsumerOptions{}) }()
	select {
	case err := <-loopErr:
		if err == nil {
			t.Error("the loop with a nil client did not return an error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CleanupConsumersLoop with a nil client did not finish with an error")
	}
}

// TestConsumersTriageStreamLoopBreaks — a pair error (an unknown stream,
// an empty group) and a candidate failure (a pending recheck, a revival
// race, a DELCONSUMER failure) do not break the pass: the groups after
// the failed one are cleaned up, the second candidate of the failed
// group is processed too.
func TestConsumersTriageStreamLoopBreaks(t *testing.T) {
	// The unknown stream sorts BEFORE the working one: a break would
	// stop the pass before the working group.
	f := &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamDNS + "|dns-worker": {{Name: "dns-worker-1", Pending: 0, Idle: 3 * time.Hour}},
		},
		pendRes: map[string][]redis.XPendingExt{},
	}
	removed, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{"q:aaa": "g", StreamDNS: "dns-worker"},
		Idle:   time.Hour,
	})
	if err == nil {
		t.Fatal("an unknown stream not returned as an error")
	}
	if removed != 1 || len(f.delCalls()) != 1 {
		t.Errorf("after the failing pair: removed %d, DELCONSUMER %v — a working stream must be cleaned", removed, f.delCalls())
	}

	// An empty group of the same map — also does not break the walk.
	f = &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamDNS + "|dns-worker": {{Name: "dns-worker-1", Pending: 0, Idle: 3 * time.Hour}},
		},
		pendRes: map[string][]redis.XPendingExt{},
	}
	removed, err = CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{"q:aaa": "", StreamDNS: "dns-worker"},
		Idle:   time.Hour,
	})
	if err == nil {
		t.Fatal("an empty group not returned as an error")
	}
	if removed != 1 {
		t.Errorf("after the empty group: removed %d, want 1", removed)
	}

	// Inside a group: a recheck failure at the first candidate — the
	// second is removed anyway; the first one's error is returned.
	f = &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamHTTP + "|http-worker": {
				{Name: "http-worker-1", Pending: 0, Idle: 3 * time.Hour},
				{Name: "http-worker-2", Pending: 0, Idle: 3 * time.Hour},
			},
		},
		pendErr: map[string]error{StreamHTTP + "|http-worker|http-worker-1": errors.New("LOADING")},
		pendRes: map[string][]redis.XPendingExt{},
	}
	removed, err = CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{StreamHTTP: "http-worker"},
		Idle:   time.Hour,
	})
	if err == nil {
		t.Fatal("the pending re-check failure not returned as an error")
	}
	if removed != 1 {
		t.Errorf("removed %d, want 1 (the second candidate after the failing re-check)", removed)
	}

	// A revival race at the first one (the recheck saw pending) — the
	// second candidate is removed.
	f = &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamHTTP + "|http-worker": {
				{Name: "http-worker-1", Pending: 0, Idle: 3 * time.Hour},
				{Name: "http-worker-2", Pending: 0, Idle: 3 * time.Hour},
			},
		},
		pendRes: map[string][]redis.XPendingExt{
			StreamHTTP + "|http-worker|http-worker-1": {{ID: "5-0"}},
		},
	}
	removed, err = CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{StreamHTTP: "http-worker"},
		Idle:   time.Hour,
	})
	if err != nil {
		t.Fatalf("the revival race — not an error: %v", err)
	}
	if removed != 1 {
		t.Errorf("removed %d, want 1 (after the first one's revival race)", removed)
	}

	// A DELCONSUMER failure of the first one — an error with a zero count
	// of this attempt; the second candidate is removed (continue, not
	// break).
	tf := &triConsumersFake{
		fakeConsumers: &fakeConsumers{
			lists: map[string][]redis.XInfoConsumer{
				StreamDNS + "|dns-worker": {
					{Name: "dns-worker-1", Pending: 0, Idle: 3 * time.Hour},
					{Name: "dns-worker-2", Pending: 0, Idle: 3 * time.Hour},
				},
			},
			pendRes: map[string][]redis.XPendingExt{},
		},
		delErrs: map[string]error{
			StreamDNS + "|dns-worker|dns-worker-1": errors.New("OOM command not allowed"),
		},
	}
	removed, err = CleanupConsumersOnce(context.Background(), tf, ConsumerOptions{
		Groups: map[string]string{StreamDNS: "dns-worker"},
		Idle:   time.Hour,
	})
	if err == nil {
		t.Fatal("the DELCONSUMER failure not returned as an error")
	}
	if removed != 1 {
		t.Errorf("removed %d, want 1 (the first fails — not removed, the second removed)", removed)
	}
	dels := tf.delCalls()
	if len(dels) != 2 || dels[1] != StreamDNS+"|dns-worker|dns-worker-2" {
		t.Errorf("DELCONSUMER %v, want both attempts", dels)
	}

	// A single candidate with a DELCONSUMER failure: an error, the count
	// is strictly 0 (the increment — only after a success).
	tf = &triConsumersFake{
		fakeConsumers: &fakeConsumers{
			lists: map[string][]redis.XInfoConsumer{
				StreamDNS + "|dns-worker": {{Name: "dns-worker-1", Pending: 0, Idle: 3 * time.Hour}},
			},
			pendRes: map[string][]redis.XPendingExt{},
		},
		delErrs: map[string]error{
			StreamDNS + "|dns-worker|dns-worker-1": errors.New("OOM command not allowed"),
		},
	}
	removed, err = CleanupConsumersOnce(context.Background(), tf, ConsumerOptions{
		Groups: map[string]string{StreamDNS: "dns-worker"},
		Idle:   time.Hour,
	})
	if err == nil || removed != 0 {
		t.Errorf("a DELCONSUMER failure: (%d, %v), want (0, an error)", removed, err)
	}
}

// TestConsumersTriageEmptyGroupKnownStream — an empty group of a KNOWN
// stream (the group == "" branch — the known-ness is checked earlier):
// the pair is skipped with an error, the walk continues (a break would
// tear the tail — the "q:aaa" case above covered only the unknown stream
// branch).
func TestConsumersTriageEmptyGroupKnownStream(t *testing.T) {
	f := &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamHTTP + "|http-worker": {{Name: "http-worker-1", Pending: 0, Idle: 3 * time.Hour}},
		},
		pendRes: map[string][]redis.XPendingExt{},
	}
	removed, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		// q:dns (an empty group) sorts before q:http: a break would stop
		// the pass before the working pair.
		Groups: map[string]string{StreamDNS: "", StreamHTTP: "http-worker"},
		Idle:   time.Hour,
	})
	if err == nil || !strings.Contains(err.Error(), StreamDNS) {
		t.Fatalf("the empty q:dns group: %v, want an error about q:dns", err)
	}
	if removed != 1 || len(f.delCalls()) != 1 {
		t.Errorf("the tail after the empty group: removed %d, DELCONSUMER %v — want 1/1 (continue, not break)", removed, f.delCalls())
	}
}

// TestConsumersTriageSortedStreamOrder — the pairs are walked in
// ascending stream name order (a deterministic order of the pass errors
// and logs): a Go map iterates randomly, without sorting the order would
// jump. The pass is repeated: a random order fails at least once (the
// miss probability (1/6)^number of repeats).
func TestConsumersTriageSortedStreamOrder(t *testing.T) {
	groups := map[string]string{StreamHTTP: "", StreamDNS: "", StreamAnalyze: ""}
	for i := 0; i < 50; i++ {
		_, err := CleanupConsumersOnce(context.Background(), &fakeConsumers{}, ConsumerOptions{Groups: groups})
		if err == nil {
			t.Fatal("three empty groups: want a pass error")
		}
		msg := err.Error()
		ia, id, ih := strings.Index(msg, StreamAnalyze), strings.Index(msg, StreamDNS), strings.Index(msg, StreamHTTP)
		if ia < 0 || id < 0 || ih < 0 || ia >= id || id >= ih {
			t.Fatalf("the error order not ascending by stream: %q", msg)
		}
	}
}

// TestConsumersTriagePendingWindow — the candidate's PEL recheck: the
// race window is shrunk to ONE entry (Count 1, the "-"…"+" range,
// exactly this consumer); a pending of one entry carried away by a
// DELCONSUMER is already a race (an error to the operator).
func TestConsumersTriagePendingWindow(t *testing.T) {
	f := &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamDNS + "|dns-worker": {{Name: "dns-worker-1", Pending: 0, Idle: 3 * time.Hour}},
		},
		pendRes: map[string][]redis.XPendingExt{},
	}
	if _, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{StreamDNS: "dns-worker"},
		Idle:   time.Hour,
	}); err != nil {
		t.Fatalf("CleanupConsumersOnce: %v", err)
	}
	if len(f.pendArgs) != 1 {
		t.Fatalf("re-checks %d, want 1", len(f.pendArgs))
	}
	a := f.pendArgs[0]
	if a.Count != 1 || a.Start != "-" || a.End != "+" {
		t.Errorf("XPENDING EXTENDED %+v: want Count=1 Start=- End=+ (the minimal race window)", a)
	}

	// One PEL entry carried away is already an undeliverable-forever
	// case: an error.
	f = &fakeConsumers{
		lists: map[string][]redis.XInfoConsumer{
			StreamDNS + "|dns-worker": {{Name: "dns-worker-1", Pending: 0, Idle: 3 * time.Hour}},
		},
		pendRes: map[string][]redis.XPendingExt{},
		delPEL:  1,
	}
	if _, err := CleanupConsumersOnce(context.Background(), f, ConsumerOptions{
		Groups: map[string]string{StreamDNS: "dns-worker"},
		Idle:   time.Hour,
	}); err == nil {
		t.Error("DELCONSUMER removed 1 pending entry — want an error (the boundary is > 0, not > 1)")
	}
}

// --- infra.go --------------------------------------------------------------

// TestInfraCauseTriagePlainStrings — server answers on BARE strings: the
// go-redis Is* helpers have a string fallback, while the prefix walk in
// infraCause requires the redis.Error interface — the classification of
// bare strings rests exactly on the disjuncts of the Is* chain (removing
// them would stay silent).
func TestInfraCauseTriagePlainStrings(t *testing.T) {
	cases := []struct {
		text string
		want bool
	}{
		{"OOM command not allowed when used memory > 'maxmemory'", true},
		{"LOADING Redis is loading the dataset in memory", true},
		{"READONLY You can't write against a read only replica", true},
		{"MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'", true},
		{"CLUSTERDOWN The cluster is down", true},
		{"TRYAGAIN Multiple keys request during rehashing of slot", true},
		{"ERR wrong number of arguments", false},
	}
	for _, c := range cases {
		if got := infraCause(errors.New(c.text)); got != c.want {
			t.Errorf("infraCause(%q) = %v, want %v", c.text, got, c.want)
		}
	}
	// Transport inside a wrapper: an unexpected EOF and a closed client.
	if !infraCause(fmt.Errorf("queue: publish: %w", io.ErrUnexpectedEOF)) {
		t.Error("io.ErrUnexpectedEOF in a wrap — an infrastructure failure")
	}
	if !infraCause(fmt.Errorf("queue: publish: %w", redis.ErrClosed)) {
		t.Error("redis.ErrClosed in a wrap — an infrastructure failure")
	}
}

// --- redisq.go -------------------------------------------------------------

// TestPublishRetryTriageStops — the XADD retries: a success before the
// retries are exhausted stops the series (no extra XADDs); a
// non-infrastructure error in the middle of the series — a stop without
// an attempt burn-down.
func TestPublishRetryTriageStops(t *testing.T) {
	msg := DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: "a.test"}}}

	// The first XADD — OOM, the second goes through: the series is
	// finished, the third call is not needed (a stop by success, not by
	// exhaustion).
	fake := &fakeRedis{addErrs: []error{oomErr}}
	q := NewRedisQueue(fake, RedisOptions{PublishRetries: 2, PublishRetryPause: time.Nanosecond})
	if _, err := q.Publish(context.Background(), StreamDNS, msg); err != nil {
		t.Fatalf("Publish after the retry: %v", err)
	}
	if len(fake.adds) != 2 {
		t.Errorf("XADD calls %d, want 2 (the second succeeds — the series stops)", len(fake.adds))
	}

	// OOM, then a non-infrastructure error: a retry is pointless — the
	// series is stopped on it, the result — the same error without the
	// InfraError marker.
	fake = &fakeRedis{addErrs: []error{oomErr, errors.New("ERR wrong number of arguments")}}
	q = NewRedisQueue(fake, RedisOptions{PublishRetries: 2, PublishRetryPause: time.Nanosecond})
	_, err := q.Publish(context.Background(), StreamDNS, msg)
	if err == nil || IsInfra(err) {
		t.Fatalf("a non-infrastructure error mid-series: %v", err)
	}
	if len(fake.adds) != 2 {
		t.Errorf("XADD calls %d, want 2 (a non-infra error — no attempt burn-down)", len(fake.adds))
	}
}

// --- reliab.go -------------------------------------------------------------

// TestReliabTriagePins — literal pins of the constants: the write probe
// TTL (with a margin above the redis.recover_interval maximum of 1 h)
// and the budget per-entry byte estimates (the MEMORY USAGE measurement
// of T-141/T-092 — the basis of the DefaultStreamMaxLen caps,
// docs/redis.md §3.1).
func TestReliabTriagePins(t *testing.T) {
	if writeProbeTTL != 2*time.Hour {
		t.Errorf("writeProbeTTL = %s, want exactly 2h", writeProbeTTL)
	}
	if DNSStreamBytesPerDomain != 66 {
		t.Errorf("DNSStreamBytesPerDomain = %d, want 66 (the T-141 measurement)", DNSStreamBytesPerDomain)
	}
	if UnitStreamBytesWorst != 717 {
		t.Errorf("UnitStreamBytesWorst = %d, want 717 (the upper bound of the T-092 measurement)", UnitStreamBytesWorst)
	}
	if DeadStreamBytesWorst != 205 {
		t.Errorf("DeadStreamBytesWorst = %d, want 205 (~150–300 B, T-105)", DeadStreamBytesWorst)
	}
}

// TestReliabTriageRefundArgs — the argument guards of the attempt
// refund: an empty group/consumer with a non-empty batch — an error
// BEFORE Redis; an empty batch — a transparent nil regardless of the
// group; Refund without the Do window and RefundUnclassified without it
// — a transparent nil without commands.
func TestReliabTriageRefundArgs(t *testing.T) {
	claimed := []Claimed{{Delivery{StreamHTTP, "1-1", []byte(`{"task_id":"t1"}`)}, 2}}

	fake := &fakeClaimRedis{}
	r := newTestReliab(fake, ReliabOptions{})

	if err := r.Refund(context.Background(), "", "recover-1", claimed); err == nil || len(fake.dos) != 0 {
		t.Errorf("Refund with an empty group: (%v, %d commands), want an error without commands", err, len(fake.dos))
	}
	if err := r.Refund(context.Background(), "http", "", claimed); err == nil || len(fake.dos) != 0 {
		t.Errorf("Refund with an empty consumer: (%v, %d commands), want an error without commands", err, len(fake.dos))
	}
	if err := r.Refund(context.Background(), "", "c", nil); err != nil {
		t.Errorf("Refund of an empty batch with an empty group: %v, want a transparent nil", err)
	}
	if err := newTestReliab(&fakeClaimRedis{}, ReliabOptions{}).Refund(context.Background(), "", "c", nil); err != nil {
		t.Errorf("Refund(nil batch): %v, want nil", err)
	}

	// RefundUnclassified: the same guards.
	ds := []Delivery{{Stream: StreamHTTP, ID: "1-1", Payload: []byte(`{"task_id":"t1"}`)}}
	fake = &fakeClaimRedis{}
	r = newTestReliab(fake, ReliabOptions{})
	if err := r.RefundUnclassified(context.Background(), "", "recover-1", ds); err == nil || len(fake.dos) != 0 {
		t.Errorf("RefundUnclassified with an empty group: (%v, %d commands), want an error without commands", err, len(fake.dos))
	}
	if err := r.RefundUnclassified(context.Background(), "http", "", ds); err == nil || len(fake.pends) != 0 {
		t.Errorf("RefundUnclassified with an empty consumer: (%v, XPENDING %d), want an error before the reads", err, len(fake.pends))
	}
	if err := r.RefundUnclassified(context.Background(), "", "c", nil); err != nil {
		t.Errorf("RefundUnclassified of an empty batch: %v, want a transparent nil", err)
	}

	// A client without the Do window: both refunds are transparent, the
	// attempts are spent as before T-222 — no commands, no reads.
	claimOnly := struct{ claimStreams }{&fakeClaimRedis{}}
	ro := NewReliab(claimOnly, NewRedisQueue(claimOnly.claimStreams.(*fakeClaimRedis), RedisOptions{}), ReliabOptions{})
	emptyFake := claimOnly.claimStreams.(*fakeClaimRedis)
	if err := ro.Refund(context.Background(), "g", "c", claimed); err != nil {
		t.Errorf("Refund without a Do window: %v, want nil", err)
	}
	if err := ro.RefundUnclassified(context.Background(), "g", "c", ds); err != nil {
		t.Errorf("RefundUnclassified without a Do window: %v, want nil", err)
	}
	if len(emptyFake.pends) != 0 || len(emptyFake.dos) != 0 {
		t.Errorf("commands without a Do window: XPENDING %d, Do %d — the gate must stay silent", len(emptyFake.pends), len(emptyFake.dos))
	}
}

// TestReliabTriageUnclassifiedContinue — the classification does not
// break at the first entry: a counter read failure or an entry that left
// the PEL is skipped, the other entries get their attempt refund.
func TestReliabTriageUnclassifiedContinue(t *testing.T) {
	// The first — a counter read failure: an error in the return, the
	// second is refunded anyway (continue, not break).
	fake := &fakeClaimRedis{
		pendOneErr: map[string]error{"1-1": errors.New("connection reset")},
		pendRes:    map[string][]redis.XPendingExt{"1-2": {{ID: "1-2", RetryCount: 2}}},
	}
	ds := []Delivery{
		{Stream: StreamHTTP, ID: "1-1", Payload: []byte(`{"task_id":"t1"}`)},
		{Stream: StreamHTTP, ID: "1-2", Payload: []byte(`{"task_id":"t2"}`)},
	}
	err := newTestReliab(fake, ReliabOptions{}).RefundUnclassified(context.Background(), "http", "recover-1", ds)
	if err == nil || !contains(err, "delivery counter") {
		t.Fatalf("the classification error: %v, want the counter context", err)
	}
	if len(fake.dos) != 1 || fake.dos[0][5] != "1-2" {
		t.Errorf("the refund %v, want a command for 1-2 only", fake.dos)
	}

	// The first entry left the PEL (an empty answer): it needs no refund,
	// the second is refunded.
	fake = &fakeClaimRedis{
		pendRes: map[string][]redis.XPendingExt{"1-2": {{ID: "1-2", RetryCount: 2}}},
	}
	if err := newTestReliab(fake, ReliabOptions{}).RefundUnclassified(context.Background(), "http", "recover-1", ds); err != nil {
		t.Fatalf("RefundUnclassified: %v", err)
	}
	if len(fake.dos) != 1 || fake.dos[0][5] != "1-2" {
		t.Errorf("the refund %v, want a command for 1-2 only (1-1 outside the PEL)", fake.dos)
	}
}

// --- trim.go ---------------------------------------------------------------

// triTrimFake — a fakeTrim with XTRIM/XLEN errors and an XLEN call count
// (the base fake can do neither): muting the command errors and extra
// length reads.
type triTrimFake struct {
	*fakeTrim
	trimErr   error
	xlenErr   error
	xlenCalls int
}

func (f *triTrimFake) XTrimMinID(ctx context.Context, key, minID string) *redis.IntCmd {
	f.fakeTrim.XTrimMinID(ctx, key, minID)
	if f.trimErr != nil {
		cmd := redis.NewIntCmd(ctx, "xtrim")
		cmd.SetErr(f.trimErr)
		return cmd
	}
	return redis.NewIntResult(f.trimCount, nil)
}

func (f *triTrimFake) XLen(ctx context.Context, key string) *redis.IntCmd {
	f.xlenCalls++
	if f.xlenErr != nil {
		cmd := redis.NewIntCmd(ctx, "xlen")
		cmd.SetErr(f.xlenErr)
		return cmd
	}
	return redis.NewIntResult(f.xlen[key], nil)
}

// triTrimStreamOnce — TrimOnce over a single stream (a shorthand).
func triTrimStreamOnce(t *testing.T, rdb trimStreams, opts TrimOptions) (int64, []string, error) {
	t.Helper()
	o := opts
	o.Streams = []string{StreamDNS}
	return TrimOnce(context.Background(), rdb, o)
}

// TestTrimTriageErrors — pass step errors: reading the groups (except
// "no key"), pending, XTRIM, XLEN — pass errors with a zero count (or
// the exact one for XLEN); after an XTRIM failure the length is not
// read.
func TestTrimTriageErrors(t *testing.T) {
	// Any other XINFO GROUPS error — an error (no key — no, see below).
	f := &triTrimFake{fakeTrim: &fakeTrim{
		groupsErr: map[string]error{StreamDNS: errors.New("connection reset")},
	}}
	n, _, err := triTrimStreamOnce(t, f, TrimOptions{})
	if err == nil {
		t.Fatal("the group list read error swallowed")
	}
	if n != 0 {
		t.Errorf("removed %d on error, want 0", n)
	}

	// An XPENDING error of the group — the stream is skipped with an
	// error, the count is 0.
	f = &triTrimFake{fakeTrim: &fakeTrim{
		groups:  map[string][]redis.XInfoGroup{StreamDNS: {{Name: "dns", Pending: 3, LastDeliveredID: "105-0"}}},
		pendErr: map[string]error{StreamDNS + "|dns": errors.New("connection reset")},
	}}
	n, _, err = triTrimStreamOnce(t, f, TrimOptions{})
	if err == nil {
		t.Fatal("the XPENDING error swallowed")
	}
	if n != 0 {
		t.Errorf("removed %d on a pending error, want 0", n)
	}

	// An XTRIM error — a pass error, the count is 0, the XLEN is not
	// read (returning before the signal read).
	f = &triTrimFake{fakeTrim: &fakeTrim{
		groups: map[string][]redis.XInfoGroup{StreamDNS: {{Name: "dns", Pending: 0, LastDeliveredID: "42-1"}}},
	}, trimErr: errors.New("OOM command not allowed")}
	n, _, err = triTrimStreamOnce(t, f, TrimOptions{})
	if err == nil {
		t.Fatal("the XTRIM error swallowed")
	}
	if n != 0 || f.xlenCalls != 0 {
		t.Errorf("an XTRIM failure: (removed %d, XLEN %d), want (0, 0)", n, f.xlenCalls)
	}

	// An XLEN error — a pass error (the stream's count is not accepted
	// by the pass on an error: trimmed does not grow, the length is a
	// signal, not an invariant).
	f = &triTrimFake{fakeTrim: &fakeTrim{
		groups:    map[string][]redis.XInfoGroup{StreamDNS: {{Name: "dns", Pending: 0, LastDeliveredID: "42-1"}}},
		trimCount: 7,
	}, xlenErr: errors.New("connection reset")}
	n, _, err = triTrimStreamOnce(t, f, TrimOptions{})
	if err == nil {
		t.Fatal("the XLEN error swallowed")
	}
	if n != 0 {
		t.Errorf("removed %d on an XLEN error, want 0 (a failing stream's count is not accepted)", n)
	}
	// The XTRIM itself ran before the length error.
	if len(f.trims) != 1 {
		t.Errorf("XTRIM before the XLEN error: %+v, want one trim", f.trims)
	}

	// A nil client: an error with a zero count; the loop — an immediate
	// error.
	n, _, err = TrimOnce(context.Background(), nil, TrimOptions{})
	if err == nil || n != 0 {
		t.Errorf("TrimOnce with a nil client: (%d, %v), want (0, an error)", n, err)
	}
	loopErr := make(chan error, 1)
	go func() { loopErr <- TrimLoop(context.Background(), nil, TrimOptions{}) }()
	select {
	case err := <-loopErr:
		if err == nil {
			t.Error("TrimLoop with a nil client did not return an error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("TrimLoop with a nil client did not finish with an error")
	}
}

// TestTrimTriagePointAndStuck — the trim point at the boundaries: a
// pending of exactly one entry (XINFO Pending 1 and XPENDING Count 1) —
// the point at the oldest pending; the race "everything was acknowledged
// between XINFO and XPENDING" (Count 0) — the point stays at the group's
// last-delivered-id; the trim total — across all streams; the stuck
// prefix signal — exactly at 3/4 of the limit and with an explicit
// limit of 1.
func TestTrimTriagePointAndStuck(t *testing.T) {
	// Pending == 1 (borders on skipping the PEL check).
	f := &fakeTrim{
		groups: map[string][]redis.XInfoGroup{StreamDNS: {{Name: "dns", Pending: 1, LastDeliveredID: "105-0"}}},
		pend:   map[string]redis.XPending{StreamDNS + "|dns": {Count: 1, Lower: "101-0"}},
	}
	if _, _, err := triTrimStreamOnce(t, f, TrimOptions{}); err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if len(f.trims) != 1 || f.trims[0]["minid"] != "101-0" {
		t.Errorf("XTRIM %+v, want MINID 101-0 (a single-entry pending is a PEL too)", f.trims)
	}

	// A race: XINFO showed pending, XPENDING — already empty (Count 0):
	// cand stays at the racing group's last-delivered-id (the smaller id
	// — exactly it must win the minimum; an empty Lower does not wipe
	// it).
	f = &fakeTrim{
		groups: map[string][]redis.XInfoGroup{StreamDNS: {
			{Name: "a", Pending: 1, LastDeliveredID: "50-0"},
			{Name: "b", Pending: 0, LastDeliveredID: "105-0"},
		}},
		pend: map[string]redis.XPending{StreamDNS + "|a": {}}, // Count 0 — the race
	}
	if _, _, err := triTrimStreamOnce(t, f, TrimOptions{}); err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if len(f.trims) != 1 || f.trims[0]["minid"] != "50-0" {
		t.Errorf("XTRIM %+v, want MINID 50-0 (an empty re-check does not reset the point)", f.trims)
	}

	// The sum across streams: two streams of 7 removed each — 14 in
	// total.
	f = &fakeTrim{
		groups: map[string][]redis.XInfoGroup{
			StreamDNS:    {{Name: "dns", Pending: 0, LastDeliveredID: "42-1"}},
			StreamHTTP:   {{Name: "http", Pending: 0, LastDeliveredID: "43-1"}},
			StreamRender: {{Name: "render", Pending: 0, LastDeliveredID: "44-1"}},
		},
		trimCount: 7,
	}
	n, _, err := TrimOnce(context.Background(), f, TrimOptions{
		Streams: []string{StreamDNS, StreamHTTP, StreamRender},
	})
	if err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if n != 21 {
		t.Errorf("removed %d, want 21 (the sum over three streams)", n)
	}

	// Exactly 3/4 of the limit — already a stuck prefix
	// (length*4 == limit*3).
	f = &fakeTrim{
		groups: map[string][]redis.XInfoGroup{StreamDNS: {{Name: "dns", Pending: 0, LastDeliveredID: "42-1"}}},
		xlen:   map[string]int64{StreamDNS: 3},
	}
	_, stuck, err := triTrimStreamOnce(t, f, TrimOptions{MaxLen: 4})
	if err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if len(stuck) != 1 {
		t.Errorf("stuck %v at XLEN 3/4 = the cap 4, want the signal", stuck)
	}

	// An explicit limit of 1 — a legitimate value (the default only for
	// 0).
	f = &fakeTrim{
		groups: map[string][]redis.XInfoGroup{StreamDNS: {{Name: "dns", Pending: 0, LastDeliveredID: "42-1"}}},
		xlen:   map[string]int64{StreamDNS: 2},
	}
	_, stuck, err = triTrimStreamOnce(t, f, TrimOptions{MaxLen: 1})
	if err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if len(stuck) != 1 {
		t.Errorf("stuck %v at XLEN 2 > the limit 1: an explicit limit must take effect", stuck)
	}
}

// TestTrimTriageNoGroupsNoXLen — a stream without groups (like q:dead):
// an empty pass WITHOUT a length read — the signaling XLEN is not called
// (after removing the early return the pass would go read the length of
// an empty stream).
func TestTrimTriageNoGroupsNoXLen(t *testing.T) {
	f := &triTrimFake{fakeTrim: &fakeTrim{groups: map[string][]redis.XInfoGroup{}}}
	n, stuck, err := triTrimStreamOnce(t, f, TrimOptions{})
	if err != nil {
		t.Fatalf("TrimOnce without groups: %v", err)
	}
	if n != 0 || len(stuck) != 0 {
		t.Errorf("without groups: (%d, %v), want (0, empty)", n, stuck)
	}
	if f.xlenCalls != 0 {
		t.Errorf("XLEN without groups: %d calls, want 0", f.xlenCalls)
	}

	// An unknown stream — an error with a zero count.
	f2 := &triTrimFake{fakeTrim: &fakeTrim{}}
	n, _, err = TrimOnce(context.Background(), f2, TrimOptions{Streams: []string{"q:typo"}})
	if err == nil || n != 0 {
		t.Errorf("an unknown stream: (%d, %v), want (0, an error)", n, err)
	}
}

// TestTrimTriageLateGroupContributes — a group without deliveries
// (cand == "") does not constrain the point and does not stop the walk:
// a later group gives an XTRIM at its point (a break after an empty cand
// would stay silent).
func TestTrimTriageLateGroupContributes(t *testing.T) {
	f := &fakeTrim{
		groups: map[string][]redis.XInfoGroup{StreamDNS: {
			{Name: "a", Pending: 0, LastDeliveredID: ""}, // delivered nothing
			{Name: "b", Pending: 0, LastDeliveredID: "5-0"},
		}},
	}
	if _, _, err := triTrimStreamOnce(t, f, TrimOptions{}); err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if len(f.trims) != 1 || f.trims[0]["minid"] != "5-0" {
		t.Errorf("XTRIM %+v, want MINID 5-0 (an empty group is not the end of the traversal)", f.trims)
	}
}

// --- tarpit.go -------------------------------------------------------------

// TestTarpitTriageZeroCountOnErrors — all Track errors (validation and
// commands) return a strictly zero cardinality: the consumer decides by
// the PEL delivery number, a positive count on an error would lie.
func TestTarpitTriageZeroCountOnErrors(t *testing.T) {
	ctx := context.Background()
	validation := []struct {
		name     string
		taskID   string
		delivery string
		ttl      time.Duration
	}{
		{"empty task_id", "", "e1#1", time.Hour},
		{"empty delivery", "t1", "", time.Hour},
		{"zero TTL", "t1", "e1#1", 0},
		{"negative TTL", "t1", "e1#1", -time.Second},
	}
	for _, cs := range validation {
		n, err := NewTarpitCounter(newFakeTarpitRedis()).Track(ctx, cs.taskID, cs.delivery, cs.ttl)
		if err == nil || n != 0 {
			t.Errorf("%s: (%d, %v), want (0, an error)", cs.name, n, err)
		}
	}
	commandErrs := []struct {
		name   string
		inject func(f *fakeTarpitRedis)
	}{
		{"SADD", func(f *fakeTarpitRedis) { f.saddErr = errors.New("redis: connection closed") }},
		{"EXPIRE", func(f *fakeTarpitRedis) { f.expireErr = errors.New("redis: OOM command not allowed") }},
		{"SCARD", func(f *fakeTarpitRedis) { f.scardErr = errors.New("redis: loading") }},
	}
	for _, cs := range commandErrs {
		f := newFakeTarpitRedis()
		cs.inject(f)
		n, err := NewTarpitCounter(f).Track(ctx, "t1", "e1#1", time.Hour)
		if err == nil || n != 0 {
			t.Errorf("a failure of %s: (%d, %v), want (0, an error)", cs.name, n, err)
		}
	}

	// A TTL of 1 ns — a legitimate positive value: the mark is set.
	f := newFakeTarpitRedis()
	if n, err := NewTarpitCounter(f).Track(ctx, "t1", "e1#1", time.Nanosecond); err != nil || n != 1 {
		t.Errorf("TTL 1ns: (%d, %v), want (1, nil) — a positive TTL is not rejected", n, err)
	}
}

// --- recover.go ------------------------------------------------------------

// TestRecoverLoopTriageNoClaimAfterCancel — a full batch (n == Batch)
// leads the loop straight to the next pass: after the handler cancels
// the ctx, the next claim is NOT executed — the return by ctx before the
// probe and the claim (muting the check would drive a claim on a
// canceled context).
func TestRecoverLoopTriageNoClaimAfterCancel(t *testing.T) {
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
		},
		pendRes: map[string][]redis.XPendingExt{
			"1718-0": {{ID: "1718-0", RetryCount: 1}},
		},
	}
	r := NewReliab(fake, NewRedisQueue(fake, RedisOptions{}), ReliabOptions{MinIdle: time.Hour})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		done <- RecoverLoop(ctx, r, func(c context.Context, _ Delivery) error {
			close(started) // the only delivery has started
			<-c.Done()     // block until the cancel — the loop is in a full-batch pass
			return nil
		}, RecoverOptions{
			Stream: StreamHTTP, Group: "http", Consumer: "recover-1",
			Batch: 1, Interval: time.Hour, // the tick will not fire: the exit — only via ctx
		})
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RecoverLoop after the cancel: %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RecoverLoop did not finish after the ctx cancel")
	}
	// Exactly one claim: no repeated claim on the canceled ctx.
	time.Sleep(50 * time.Millisecond)
	if n := fake.claimsCount(); n != 1 {
		t.Errorf("claims after the ctx cancel: %d, want 1 (exit before the write probe and the claim)", n)
	}
	if len(fake.dos) != 0 {
		t.Errorf("refunds after the cancel: %d, want 0 (the batch tail not started)", len(fake.dos))
	}
}

// claimsCount — a snapshot of the claim count (fakeClaimRedis.claims
// without a mutex is read only after the loop goroutine finishes — no
// race).
func (f *fakeClaimRedis) claimsCount() int { return len(f.claims) }

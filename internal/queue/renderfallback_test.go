// Tests of the render fallback queue stream q:render-fallback (T-237,
// spec 4.3, FR-6) without a live Redis: the pin of the allStreams
// invariant (the stream is known to the whole queue —
// knownStream/workingStreams/WorkingGroups), the policy MAXLEN cap (its
// own, not the unit hundred-thousands — the budget, docs/redis.md §3.1),
// the release of the acknowledged prefix (TrimOnce, trim_test.go) and
// the return of stuck entries with a move to q:dead (Reliab.Claim,
// reliab_test.go). The message format — the existing Result contract, no
// separate table is needed: the q:render-fallback roundtrip is in
// message_test.go.

package queue

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestRenderFallbackAllStreamsInvariant — the allStreams invariant: the
// composition of the queue streams is exactly the package constants,
// q:render-fallback included (spec 4.1 item 3: landing in allStreams
// gives trim/claim/reliability automatically). A new stream without
// allStreams silently drops out of the release and the claim — the pin
// catches that.
func TestRenderFallbackAllStreamsInvariant(t *testing.T) {
	want := []string{StreamDNS, StreamHTTP, StreamRender, StreamRenderFallback, StreamAnalyze, StreamDead}
	if !reflect.DeepEqual(allStreams, want) {
		t.Errorf("allStreams = %v, хочу %v (новый стрим добавьте в allStreams — queue.go)", allStreams, want)
	}
	if !knownStream(StreamRenderFallback) {
		t.Errorf("knownStream(%q) = false: стрим вне allStreams — Publish/Consume/Claim его отвергнут", StreamRenderFallback)
	}
	// A working stream (without q:dead): released by TrimLoop and cleaned
	// up by groups — the fallback group exists (worker B, FR-12).
	working := false
	for _, s := range workingStreams() {
		if s == StreamRenderFallback {
			working = true
		}
	}
	if !working {
		t.Errorf("workingStreams = %v: нет %q — подтверждённый префикс не освобождается", workingStreams(), StreamRenderFallback)
	}
	if _, ok := WorkingGroups()[StreamRenderFallback]; !ok {
		t.Errorf("WorkingGroups: нет пары для %q — консьюмеры группы не чистятся (T-155)", StreamRenderFallback)
	}
}

// TestRenderFallbackMaxLen — the T-025 policy cap for the fallback
// stream: its own (the budget share of the unit flow, docs/redis.md
// §3.1), not the unit 150,000 (it would break the Redis budget with the
// worst q:dns batch) and not the q:dead cap; Publish trims the stream
// with it (XADD MAXLEN ~).
func TestRenderFallbackMaxLen(t *testing.T) {
	if got := DefaultStreamMaxLen(StreamRenderFallback); got != defaultMaxLenRenderFallback {
		t.Errorf("DefaultStreamMaxLen(%s) = %d, хочу %d (политика T-025)", StreamRenderFallback, got, defaultMaxLenRenderFallback)
	}
	if defaultMaxLenRenderFallback >= defaultMaxLenUnit {
		t.Errorf("потолок фолбэка %d ≥ штучному %d — фолбэк обязан быть долей потока этапа (бюджет, ТЗ 3.2)",
			defaultMaxLenRenderFallback, defaultMaxLenUnit)
	}

	fake := &fakeRedis{}
	q := NewRedisQueue(fake, RedisOptions{})
	res := Result{ZoneID: 3, DomainID: 42, SubdomainID: 1, Variant: 1, Name: "example.com"}
	if _, err := q.Publish(context.Background(), StreamRenderFallback, res); err != nil {
		t.Fatalf("Publish Result в %s: %v", StreamRenderFallback, err)
	}
	add := fake.adds[0]
	if add.Stream != StreamRenderFallback {
		t.Errorf("XADD стрим %q, хочу %q", add.Stream, StreamRenderFallback)
	}
	if want := DefaultStreamMaxLen(StreamRenderFallback); add.MaxLen != want || !add.Approx {
		t.Errorf("XADD MAXLEN ~ %d не применён: MaxLen=%d Approx=%v, хочу %d (политика T-025)",
			want, add.MaxLen, add.Approx, want)
	}
}

// TestRenderFallbackTrim — the release of the acknowledged prefix of the
// fallback stream (modeled after TestTrimOncePendingKept): the trim
// point — the group's oldest pending (a stuck PEL entry is not removed),
// the default Streams of TrimOptions cover the stream (workingStreams).
func TestRenderFallbackTrim(t *testing.T) {
	f := &fakeTrim{
		groups: map[string][]redis.XInfoGroup{
			StreamRenderFallback: {{Name: "render-fallback-worker", Pending: 2, LastDeliveredID: "205-0"}},
		},
		pend: map[string]redis.XPending{
			StreamRenderFallback + "|render-fallback-worker": {Count: 2, Lower: "201-0", Higher: "204-0"},
		},
		trimCount: 4,
	}
	n, _, err := TrimOnce(context.Background(), f, TrimOptions{Streams: []string{StreamRenderFallback}})
	if err != nil {
		t.Fatalf("TrimOnce: %v", err)
	}
	if n != 4 {
		t.Errorf("удалено %d, хочу 4 (ответ XTRIM)", n)
	}
	if len(f.trims) != 1 || f.trims[0]["stream"] != StreamRenderFallback || f.trims[0]["minid"] != "201-0" {
		t.Fatalf("XTRIM %+v, хочу MINID 201-0 в %s (старейшая pending группы)", f.trims, StreamRenderFallback)
	}

	covered := false
	for _, s := range (TrimOptions{}).withDefaults().Streams {
		if s == StreamRenderFallback {
			covered = true
		}
	}
	if !covered {
		t.Errorf("дефолтные Streams TrimOptions не покрывают %s (workingStreams)", StreamRenderFallback)
	}
}

// TestRenderFallbackClaim — the return of stuck fallback entries
// (modeled after TestReliabClaim/TestReliabClaimDead): the attempt
// counter from the PEL goes back into the work; an entry that exhausted
// its attempts moves to q:dead — the Dead is valid with the source
// q:render-fallback and the subject identifiers extracted from the
// Result body (deadSubjectOf), the original entry is acknowledged.
func TestRenderFallbackClaim(t *testing.T) {
	fresh := `{"task_id":"t1"}`
	subject := `{"task_id":"t2","zone_id":3,"domain_id":42,"subdomain_id":1,"variant":1,"name":"www.example.com"}`
	fake := &fakeClaimRedis{
		claimMsgs: []redis.XMessage{
			{ID: "301-0", Values: map[string]any{fieldData: []byte(fresh)}},
			{ID: "302-0", Values: map[string]any{fieldData: []byte(subject)}},
		},
		claimNext: "303-0",
		pendRes: map[string][]redis.XPendingExt{
			"301-0": {{ID: "301-0", RetryCount: 2}}, // attempts remain — back into the work
			"302-0": {{ID: "302-0", RetryCount: 4}}, // the limit of 3 is exhausted — q:dead
		},
	}
	r := newTestReliab(fake, ReliabOptions{})

	got, _, next, err := r.Claim(context.Background(), StreamRenderFallback,
		"render-fallback-worker", "recover-1", time.Minute, 5, "0-0")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if next != "303-0" {
		t.Errorf("курсор = %q, хочу 303-0", next)
	}
	want := []Claimed{
		{Delivery{StreamRenderFallback, "301-0", []byte(fresh)}, 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Claim:\n получил  %+v\n хочу     %+v", got, want)
	}

	// The exhausted one — q:dead: the fallback stream as the source, the
	// subject from the Result body.
	if len(fake.adds) != 1 || fake.adds[0].Stream != StreamDead {
		t.Fatalf("публикаций в q:dead: %+v, хочу одну", fake.adds)
	}
	data := fake.adds[0].Values.(map[string]any)[fieldData].([]byte)
	dead, err := Decode[Dead](Delivery{Stream: StreamDead, ID: fake.adds[0].ID, Payload: data})
	if err != nil {
		t.Fatalf("поле data не декодируется как Dead: %v\n%s", err, data)
	}
	wantDead := Dead{
		TaskID:      "t2",
		Stream:      StreamRenderFallback,
		EntryID:     "302-0",
		Attempts:    4,
		Reason:      "исчерпан лимит попыток 3: доставок без подтверждения — 4",
		ZoneID:      3,
		DomainID:    42,
		SubdomainID: 1,
		Variant:     1,
		Name:        "www.example.com",
	}
	if !reflect.DeepEqual(dead, wantDead) {
		t.Errorf("Dead:\n получил  %+v\n хочу     %+v", dead, wantDead)
	}

	// The original entry leaves the pending: XACK from the fallback
	// stream.
	if want := []ackCall{{StreamRenderFallback, "render-fallback-worker", []string{"302-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK: %+v, хочу %+v", fake.acks, want)
	}
}

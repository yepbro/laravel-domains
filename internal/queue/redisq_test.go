// Tests of the RedisQueue implementation without a live Redis (T-024):
// the fake implements the same narrow redisStreams command window as
// *redis.Client — the go-redis command results are built by the
// New*Result factories. Covered: the mapping of arguments and results —
// XADD (the data field, MAXLEN ~), lazy group creation with BUSYGROUP
// absorption, XREADGROUP ">", XACK. The real Redis semantics — the
// integration tests of T-032.

package queue

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// fakeRedis — a record of command calls + stubbed results.
type fakeRedis struct {
	adds   []redis.XAddArgs
	addID  string
	addErr error
	// addErrs — XADD errors call after call (a queue); empty — the
	// permanent addErr. Needed by the publish-retry scenarios on an
	// infrastructure failure (T-152): the first calls fail with OOM, the
	// next one goes through.
	addErrs []error

	creates   []string // "stream group start" on every call
	createErr error
	// createErrs — group creation errors call after call (a queue);
	// empty — the permanent createErr. Needed by the scenarios where the
	// recreation after NOGROUP fails while the first creation does not.
	createErrs []error

	reads   []redis.XReadGroupArgs
	readRes []redis.XStream
	readErr error
	// readErrs — read errors call after call (a queue); empty — the
	// permanent readErr. Needed by the scenarios with a changing Redis
	// answer (NOGROUP after a stream loss, T-032).
	readErrs []error

	acks     []ackCall
	ackCount int64
	ackErr   error
}

// ackCall — a recorded XACK call.
type ackCall struct {
	stream, group string
	ids           []string
}

func (f *fakeRedis) XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd {
	f.adds = append(f.adds, *a)
	err := f.addErr
	if len(f.addErrs) > 0 {
		err = f.addErrs[0]
		f.addErrs = f.addErrs[1:]
	}
	return redis.NewStringResult(f.addID, err)
}

func (f *fakeRedis) XGroupCreateMkStream(ctx context.Context, stream, group, start string) *redis.StatusCmd {
	f.creates = append(f.creates, stream+" "+group+" "+start)
	err := f.createErr
	if len(f.createErrs) > 0 {
		err = f.createErrs[0]
		f.createErrs = f.createErrs[1:]
	}
	return redis.NewStatusResult("OK", err)
}

func (f *fakeRedis) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	f.reads = append(f.reads, *a)
	err := f.readErr
	if len(f.readErrs) > 0 {
		err = f.readErrs[0]
		f.readErrs = f.readErrs[1:]
	}
	return redis.NewXStreamSliceCmdResult(f.readRes, err)
}

func (f *fakeRedis) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	if err := ctx.Err(); err != nil {
		// The live client does not send the command on a canceled ctx
		// (T-222, the P3-4 review) — the fake does not record it in the
		// protocol either.
		return redis.NewIntResult(0, err)
	}
	f.acks = append(f.acks, ackCall{stream, group, ids})
	return redis.NewIntResult(f.ackCount, f.ackErr)
}

// Compile-time checks: the fake and the real client are interchangeable,
// RedisQueue implements Queue.
var (
	_ redisStreams = (*fakeRedis)(nil)
	_ redisStreams = (*redis.Client)(nil)
	_ Queue        = (*RedisQueue)(nil)
)

// TestRedisQueueBlockDefault — the constant and the boundaries of the
// Block replacement in the constructor: 0 and negatives —
// DefaultRedisBlock (a literal pin of the constant: comparing with the
// constant itself, the mutant goes unnoticed), a single nanosecond
// interval — a valid wait, not "zero".
func TestRedisQueueBlockDefault(t *testing.T) {
	if DefaultRedisBlock != 2*time.Second {
		t.Errorf("DefaultRedisBlock = %s, want 2s (the literal pins the constant)", DefaultRedisBlock)
	}
	for _, block := range []time.Duration{0, -time.Second} {
		q := NewRedisQueue(&fakeRedis{}, RedisOptions{Block: block})
		if q.opt.Block != DefaultRedisBlock {
			t.Errorf("Block %s not replaced by the default: %s", block, q.opt.Block)
		}
	}
	q := NewRedisQueue(&fakeRedis{}, RedisOptions{Block: time.Nanosecond})
	if q.opt.Block != time.Nanosecond {
		t.Errorf("Block 1ns replaced by the default: %s, want 1ns (a positive wait is valid)", q.opt.Block)
	}
}

func TestRedisQueuePublish(t *testing.T) {
	fake := &fakeRedis{addID: "1718-0"}
	q := NewRedisQueue(fake, RedisOptions{})

	id, err := q.Publish(context.Background(), StreamDNS, DNSBatch{
		Domains: []DNSDomain{{ZoneID: 3, DomainID: 42, Name: "example.com"}},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if id != "1718-0" {
		t.Errorf("Publish returned %q, want the entry id %q", id, fake.addID)
	}
	if len(fake.adds) != 1 {
		t.Fatalf("XADD calls: %d, want 1", len(fake.adds))
	}
	add := fake.adds[0]
	if add.Stream != StreamDNS {
		t.Errorf("XADD to stream %q, want %q", add.Stream, StreamDNS)
	}
	// The T-025 policy: without an explicit limit the stream limit
	// applies.
	if add.MaxLen != DefaultStreamMaxLen(StreamDNS) || !add.Approx {
		t.Errorf("XADD without MaxLen in options: MaxLen=%d Approx=%v, want MAXLEN ~ %d",
			add.MaxLen, add.Approx, DefaultStreamMaxLen(StreamDNS))
	}
	values, ok := add.Values.(map[string]any)
	if !ok {
		t.Fatalf("XADD values: %T, want map[string]any", add.Values)
	}
	data, ok := values[fieldData].([]byte)
	if !ok {
		t.Fatalf("XADD values[%q]: %T, want []byte", fieldData, values[fieldData])
	}
	var probe struct {
		TaskID  string      `json:"task_id"`
		Domains []DNSDomain `json:"domains"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		t.Fatalf("the data field is not JSON: %v", err)
	}
	if len(probe.TaskID) != 32 {
		t.Errorf("the empty task_id not filled on publication: %q", probe.TaskID)
	}
	if len(probe.Domains) != 1 || probe.Domains[0].Name != "example.com" {
		t.Errorf("the data field: %+v", probe)
	}
}

func TestRedisQueuePublishMaxLen(t *testing.T) {
	fake := &fakeRedis{}
	q := NewRedisQueue(fake, RedisOptions{MaxLen: 100000})
	if _, err := q.Publish(context.Background(), StreamRender,
		Result{ZoneID: 1, DomainID: 1, SubdomainID: 0}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	add := fake.adds[0]
	if add.MaxLen != 100000 || !add.Approx {
		t.Errorf("MAXLEN ~ not passed: MaxLen=%d Approx=%v", add.MaxLen, add.Approx)
	}
}

func TestRedisQueuePublishError(t *testing.T) {
	// wantErr — an exact substring: the error branches are distinguishable
	// by text (the stream check versus the type and values check).
	cases := []struct {
		name    string
		stream  string
		msg     Message
		wantErr string
	}{
		{"a foreign stream", StreamHTTP, DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: "a.test"}}}, "published to"},
		{"an invalid message", StreamDNS, DNSBatch{}, "empty batch"},
		{"an unknown stream", "q:typo", DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: "a.test"}}}, "unknown stream"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeRedis{}
			q := NewRedisQueue(fake, RedisOptions{})
			_, err := q.Publish(context.Background(), c.stream, c.msg)
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("Publish: %v, want an error with %q", err, c.wantErr)
			}
			if len(fake.adds) != 0 {
				t.Errorf("the invalid message reached XADD: %+v", fake.adds)
			}
		})
	}

	fake := &fakeRedis{addErr: errors.New("connection refused")}
	q := NewRedisQueue(fake, RedisOptions{})
	if _, err := q.Publish(context.Background(), StreamDNS, DNSBatch{Domains: []DNSDomain{{ZoneID: 1, DomainID: 1, Name: "a.test"}}}); err == nil {
		t.Fatal("the XADD error not returned")
	}
}

// alienMessage — a message outside the package formats: the Message
// interface is sealed by the validate method, so an implementation is
// possible only inside the package. It passes the stream and values
// checks but is not serialized by marshalMessage — covering the
// serialization error branch in Publish.
type alienMessage struct{}

func (alienMessage) Streams() []string { return []string{StreamDNS} }
func (alienMessage) validate() error   { return nil }

// TestRedisQueuePublishMarshalError — an unserializable message: an
// error with the type in the text, before XADD.
func TestRedisQueuePublishMarshalError(t *testing.T) {
	fake := &fakeRedis{}
	q := NewRedisQueue(fake, RedisOptions{})
	_, err := q.Publish(context.Background(), StreamDNS, alienMessage{})
	if err == nil || !strings.Contains(err.Error(), "serialize") ||
		!strings.Contains(err.Error(), "unknown message type") {
		t.Fatalf("Publish: %v, want a serialization error naming the message type", err)
	}
	if len(fake.adds) != 0 {
		t.Errorf("the non-serializable message reached XADD: %+v", fake.adds)
	}
}

func TestRedisQueueConsume(t *testing.T) {
	fake := &fakeRedis{
		readRes: []redis.XStream{{
			Stream: StreamDNS,
			Messages: []redis.XMessage{
				{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
				{ID: "1719-0", Values: map[string]any{fieldData: []byte(`{"task_id":"t2"}`)}},
				{ID: "1720-0", Values: map[string]any{"other": "x"}}, // without the data field
			},
		}},
	}
	q := NewRedisQueue(fake, RedisOptions{})

	got, err := q.Consume(context.Background(), StreamDNS, "dns", "dns-1", 10)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// The group is created on the first read: from the start of the
	// stream, MKSTREAM.
	if want := []string{StreamDNS + " dns 0"}; !reflect.DeepEqual(fake.creates, want) {
		t.Errorf("XGROUP CREATE: %v, want %v", fake.creates, want)
	}

	// The XREADGROUP arguments: only the group's new entries, the batch
	// and the wait.
	if len(fake.reads) != 1 {
		t.Fatalf("XREADGROUP calls: %d, want 1", len(fake.reads))
	}
	args := fake.reads[0]
	if args.Group != "dns" || args.Consumer != "dns-1" {
		t.Errorf("group/consumer: %q/%q", args.Group, args.Consumer)
	}
	if want := []string{StreamDNS, ">"}; !reflect.DeepEqual(args.Streams, want) {
		t.Errorf("Streams: %v, want %v", args.Streams, want)
	}
	if args.Count != 10 {
		t.Errorf("Count: %d, want 10", args.Count)
	}
	if args.Block != DefaultRedisBlock {
		t.Errorf("Block: %s, want the default %s", args.Block, DefaultRedisBlock)
	}

	want := []Delivery{
		{Stream: StreamDNS, ID: "1718-0", Payload: []byte(`{"task_id":"t1"}`)},
		{Stream: StreamDNS, ID: "1719-0", Payload: []byte(`{"task_id":"t2"}`)},
		{Stream: StreamDNS, ID: "1720-0", Payload: nil},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Consume:\n got  %+v\n want %+v", got, want)
	}
}

func TestRedisQueueConsumeGroupCached(t *testing.T) {
	fake := &fakeRedis{readErr: redis.Nil}
	q := NewRedisQueue(fake, RedisOptions{})
	for i := 0; i < 3; i++ {
		if _, err := q.Consume(context.Background(), StreamHTTP, "http", "http-1", 5); err != nil {
			t.Fatalf("Consume №%d: %v", i+1, err)
		}
	}
	if len(fake.creates) != 1 {
		t.Errorf("XGROUP CREATE called %d times, want 1 (the group is remembered)", len(fake.creates))
	}
	if len(fake.reads) != 3 {
		t.Errorf("XREADGROUP called %d times, want 3", len(fake.reads))
	}

	// Another group of the same stream — a separate creation.
	if _, err := q.Consume(context.Background(), StreamHTTP, "http-2", "http-1", 5); err != nil {
		t.Fatalf("Consume of another group: %v", err)
	}
	if len(fake.creates) != 2 {
		t.Errorf("XGROUP CREATE called %d times, want 2", len(fake.creates))
	}
}

func TestRedisQueueConsumeBusyGroup(t *testing.T) {
	fake := &fakeRedis{
		createErr: errors.New("BUSYGROUP Consumer Group name already exists"),
		readErr:   redis.Nil,
	}
	q := NewRedisQueue(fake, RedisOptions{})
	if _, err := q.Consume(context.Background(), StreamAnalyze, "analyzer", "analyzer-1", 1); err != nil {
		t.Fatalf("BUSYGROUP — not an error: %v", err)
	}
}

// TestRedisQueueConsumeGroupLost — losing Redis with a live worker
// (spec 3.4, T-032): the group disappeared together with the stream, the
// cache of created groups is stale — XREADGROUP answers NOGROUP. Consume
// recreates the group (XGROUP CREATE MKSTREAM) and repeats the read
// instead of failing until a process restart; in the meantime the tasks
// are re-published by the scheduler via lease.
func TestRedisQueueConsumeGroupLost(t *testing.T) {
	fake := &fakeRedis{readErrs: []error{
		nil, // the read before the loss: empty, without an error
		errors.New("NOGROUP No such consumer group 'dns' for key name 'q:dns'"),
		redis.Nil, // the repeated read: no new entries
	}}
	q := NewRedisQueue(fake, RedisOptions{})

	// The first read creates the group and lands in the cache.
	if _, err := q.Consume(context.Background(), StreamDNS, "dns", "dns-1", 1); err != nil {
		t.Fatalf("Consume before the loss: %v", err)
	}
	// The second — after the stream "loss": NOGROUP, recreation, retry.
	got, err := q.Consume(context.Background(), StreamDNS, "dns", "dns-1", 1)
	if err != nil || got != nil {
		t.Fatalf("Consume after the loss: (%v, %v), want (nil, nil)", got, err)
	}
	if len(fake.creates) != 2 {
		t.Errorf("XGROUP CREATE called %d times, want 2 (recreation after NOGROUP)", len(fake.creates))
	}
	if len(fake.reads) != 3 {
		t.Errorf("XREADGROUP called %d times, want 3", len(fake.reads))
	}
}

// TestRedisQueueConsumeGroupLostAtProgress — NOGROUP after successfully
// read entries: the group is recreated from the last read id, not from
// "0" — replaying the processed history of the stream (up to MAXLEN
// entries) as an avalanche of duplicates is not needed (Q-06).
func TestRedisQueueConsumeGroupLostAtProgress(t *testing.T) {
	fake := &fakeRedis{
		readRes: []redis.XStream{{
			Stream: StreamDNS,
			Messages: []redis.XMessage{
				{ID: "1718-0", Values: map[string]any{fieldData: `{"task_id":"t1"}`}},
				{ID: "1719-0", Values: map[string]any{fieldData: `{"task_id":"t2"}`}},
			},
		}},
		readErrs: []error{
			nil, // the first read: two entries (the progress — 1719-0)
			errors.New("NOGROUP No such consumer group 'dns' for key name 'q:dns'"),
			redis.Nil, // the repeated read: no new entries
		},
	}
	q := NewRedisQueue(fake, RedisOptions{})

	if got, err := q.Consume(context.Background(), StreamDNS, "dns", "dns-1", 10); err != nil || len(got) != 2 {
		t.Fatalf("the first read: (%v, %v)", got, err)
	}
	// The group "loss": recreation from the progress of the first read.
	if got, err := q.Consume(context.Background(), StreamDNS, "dns", "dns-1", 10); err != nil || got != nil {
		t.Fatalf("the read after the loss: (%v, %v), want (nil, nil)", got, err)
	}
	want := []string{
		StreamDNS + " dns 0",      // the first creation — from the start of the stream
		StreamDNS + " dns 1719-0", // the recovery — from the last read
	}
	if !reflect.DeepEqual(fake.creates, want) {
		t.Errorf("XGROUP CREATE: %v, want %v", fake.creates, want)
	}
}

// TestRedisQueueConsumeGroupLostNoCache — NOGROUP without progress in
// the cache (a blank spot: the group cache has no "read up to here"
// mark) — the group is recreated from "0", as the first creation, not
// from an empty start.
func TestRedisQueueConsumeGroupLostNoCache(t *testing.T) {
	fake := &fakeRedis{readErrs: []error{
		errors.New("NOGROUP No such consumer group 'dns' for key name 'q:dns'"),
		redis.Nil, // the repeated read: no new entries
	}}
	q := NewRedisQueue(fake, RedisOptions{})
	// An empty progress mark: the group is considered created, but "read
	// up to here" is unknown (the guard value — see trackProgress).
	q.groups.Store(groupKey(StreamDNS, "dns"), "")

	got, err := q.Consume(context.Background(), StreamDNS, "dns", "dns-1", 1)
	if err != nil || got != nil {
		t.Fatalf("Consume after the loss: (%v, %v), want (nil, nil)", got, err)
	}
	if want := []string{StreamDNS + " dns 0"}; !reflect.DeepEqual(fake.creates, want) {
		t.Errorf("XGROUP CREATE: %v, want %v (recreation from the beginning)", fake.creates, want)
	}
	if len(fake.reads) != 2 {
		t.Errorf("XREADGROUP called %d times, want 2 (a read and a retry after the recreation)", len(fake.reads))
	}
}

// TestRedisQueueConsumeGroupRecreateError — a group recreation error
// after NOGROUP is returned to the caller, not swallowed: the consumer
// sees the Redis unavailability, not an empty result.
func TestRedisQueueConsumeGroupRecreateError(t *testing.T) {
	fake := &fakeRedis{
		createErrs: []error{nil, errors.New("ERR connection refused")},
		readErrs: []error{
			errors.New("NOGROUP No such consumer group 'dns' for key name 'q:dns'"),
		},
	}
	q := NewRedisQueue(fake, RedisOptions{})

	_, err := q.Consume(context.Background(), StreamDNS, "dns", "dns-1", 1)
	if err == nil || !strings.Contains(err.Error(), "create group") {
		t.Fatalf("Consume: %v, want a group-recreation error", err)
	}
	if len(fake.reads) != 1 {
		t.Errorf("XREADGROUP called %d times, want 1 (no repeated read after the failure)", len(fake.reads))
	}
}

func TestRedisQueueConsumeErrors(t *testing.T) {
	ctx := context.Background()

	fake := &fakeRedis{createErr: errors.New("ERR connection refused")}
	if _, err := NewRedisQueue(fake, RedisOptions{}).Consume(ctx, StreamDNS, "dns", "dns-1", 1); err == nil ||
		!strings.Contains(err.Error(), "create group") {
		t.Errorf("the group creation error: %v", err)
	}

	fake = &fakeRedis{readErr: errors.New("ERR bad group")}
	if _, err := NewRedisQueue(fake, RedisOptions{}).Consume(ctx, StreamDNS, "dns", "dns-1", 1); err == nil ||
		!strings.Contains(err.Error(), "read") {
		t.Errorf("the read error: %v", err)
	}
}

func TestRedisQueueConsumeBadArgs(t *testing.T) {
	cases := []struct {
		name                    string
		stream, group, consumer string
		n                       int
	}{
		{"an unknown stream", "q:typo", "g", "c", 1},
		{"no group", StreamDNS, "", "c", 1},
		{"no consumer", StreamDNS, "g", "", 1},
		{"batch 0", StreamDNS, "g", "c", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake := &fakeRedis{}
			if _, err := NewRedisQueue(fake, RedisOptions{}).Consume(context.Background(), c.stream, c.group, c.consumer, c.n); err == nil {
				t.Fatal("an argument error expected")
			}
			if len(fake.reads)+len(fake.creates) != 0 {
				t.Errorf("Redis called with bad arguments: %+v %+v", fake.creates, fake.reads)
			}
		})
	}
}

func TestRedisQueueConsumeBlockOption(t *testing.T) {
	fake := &fakeRedis{readErr: redis.Nil}
	q := NewRedisQueue(fake, RedisOptions{Block: 500 * time.Millisecond})
	if _, err := q.Consume(context.Background(), StreamDNS, "dns", "dns-1", 1); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if got := fake.reads[0].Block; got != 500*time.Millisecond {
		t.Errorf("Block: %s, want 500ms", got)
	}
}

func TestRedisQueueConsumeEmpty(t *testing.T) {
	fake := &fakeRedis{readErr: redis.Nil}
	got, err := NewRedisQueue(fake, RedisOptions{}).Consume(context.Background(), StreamDNS, "dns", "dns-1", 1)
	if err != nil || got != nil {
		t.Errorf("an empty read: (%v, %v), want (nil, nil)", got, err)
	}
}

func TestRedisQueueAck(t *testing.T) {
	fake := &fakeRedis{ackCount: 2}
	q := NewRedisQueue(fake, RedisOptions{})
	if err := q.Ack(context.Background(), StreamDNS, "dns", "1718-0", "1719-0"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if want := []ackCall{{StreamDNS, "dns", []string{"1718-0", "1719-0"}}}; !reflect.DeepEqual(fake.acks, want) {
		t.Errorf("XACK: %+v, want %+v", fake.acks, want)
	}

	// Without identifiers — no Redis call is needed.
	fake.acks = nil
	if err := q.Ack(context.Background(), StreamDNS, "dns"); err != nil {
		t.Fatalf("Ack without ids: %v", err)
	}
	if len(fake.acks) != 0 {
		t.Errorf("XACK with an empty ids list: %+v", fake.acks)
	}

	fake.ackErr = errors.New("ERR no such group")
	if err := q.Ack(context.Background(), StreamDNS, "dns", "1-0"); err == nil || !strings.Contains(err.Error(), "ack") {
		t.Errorf("the XACK error: %v", err)
	}
}

// TestRedisQueueTrackProgress — the progress cache contract: an empty
// batch does not touch the cache (no "0" record at all); start marks
// ("", "0") are normalized into "0-0", the progress is the maximum of
// the read ids.
func TestRedisQueueTrackProgress(t *testing.T) {
	// An empty batch — no records in the cache at all.
	q := NewRedisQueue(&fakeRedis{}, RedisOptions{})
	q.trackProgress(StreamDNS, "dns", nil)
	if _, ok := q.groups.Load(groupKey(StreamDNS, "dns")); ok {
		t.Error("an empty batch created an entry in the group cache")
	}

	// An empty start mark (a blank spot: the cache guard value) —
	// normalization into "0-0" and progress from it, not a silent skip.
	q.groups.Store(groupKey(StreamDNS, "dns"), "")
	q.trackProgress(StreamDNS, "dns", []redis.XMessage{
		{ID: "1718-0", Values: map[string]any{fieldData: "x"}},
		{ID: "1719-5", Values: map[string]any{fieldData: "x"}},
	})
	v, _ := q.groups.Load(groupKey(StreamDNS, "dns"))
	if got, _ := v.(string); got != "1719-5" {
		t.Errorf("the group progress = %q, want 1719-5 (the maximum read id)", got)
	}
}

// TestStreamIDGreater — the numeric comparison of "ms-seq" identifiers:
// with equal ms the seq decides, fully equal — not greater, non-numeric
// parts — not greater (never), the lexicographic order "999-*" >
// "1000-*" is wrong.
func TestStreamIDGreater(t *testing.T) {
	cases := []struct {
		name string
		a, b string
		want bool
	}{
		{"greater by ms, lexicographically smaller", "1000-0", "999-0", true},
		{"smaller by ms, lexicographically greater", "999-0", "1000-0", false},
		{"equal ms, greater by seq", "5-2", "5-1", true},
		{"equal ms, smaller by seq", "5-0", "5-1", false},
		{"fully equal", "5-0", "5-0", false},
		// b with a negative seq — the only form where skipping the a check
		// is distinguishable: ams/aseq are zero, the ms equality sends the
		// comparison into seq (0 > −1), whereas the correct code with an
		// unparseable a is always false.
		{"a is not of the ms-seq form", "junk", "0--1", false},
		{"b is not of the ms-seq form", "5-0", "junk", false},
		{"both are not of the ms-seq form", "junk", "junk", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := streamIDGreater(c.a, c.b); got != c.want {
				t.Errorf("streamIDGreater(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
			}
		})
	}
}

// TestSplitStreamID — parsing "ms-seq": the decimal base (not 9/11),
// the int64 width (2⁶³−1 parses), a bad format and non-numbers — an
// error with zero parts and the exact text.
func TestSplitStreamID(t *testing.T) {
	cases := []struct {
		name    string
		id      string
		ms, seq int64
		wantErr string // "" — without an error
	}{
		{"a zero seq", "1718-0", 1718, 0, ""},
		{"a non-zero seq", "1718-42", 1718, 42, ""},
		{"decimal ms", "11-0", 11, 0, ""},
		{"decimal seq", "0-11", 0, 11, ""},
		{"the digit 9 in seq", "0-9", 0, 9, ""},
		{"ms 2⁶³−1", "9223372036854775807-0", 9223372036854775807, 0, ""},
		{"seq 2⁶³−1", "0-9223372036854775807", 0, 9223372036854775807, ""},
		{"no dash", "junk", 0, 0, "ms-seq"},
		{"ms is not a number", "abc-1", 0, 0, "invalid syntax"},
		{"seq is not a number", "1-xyz", 0, 0, "invalid syntax"},
		{"a hex entry is not decimal", "0x10-0", 0, 0, "invalid syntax"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ms, seq, err := splitStreamID(c.id)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("splitStreamID(%q): %v", c.id, err)
				}
				if ms != c.ms || seq != c.seq {
					t.Errorf("splitStreamID(%q) = %d-%d, want %d-%d", c.id, ms, seq, c.ms, c.seq)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("splitStreamID(%q) = %d-%d, %v; want an error with %q", c.id, ms, seq, err, c.wantErr)
			}
			if ms != 0 || seq != 0 {
				t.Errorf("splitStreamID(%q) on error = %d-%d, want 0-0", c.id, ms, seq)
			}
		})
	}
}

// TestIsBusyGroup — nil is not a BUSYGROUP (and does not panic): the
// only caller — ensureGroup — has nothing to protect.
func TestIsBusyGroup(t *testing.T) {
	if isBusyGroup(nil) {
		t.Error("isBusyGroup(nil) = true, want false")
	}
	if !isBusyGroup(errors.New("BUSYGROUP Consumer Group name already exists")) {
		t.Error("BUSYGROUP not recognized")
	}
	if isBusyGroup(errors.New("ERR connection refused")) {
		t.Error("a foreign error recognized as BUSYGROUP")
	}
	if isNoGroup(nil) {
		t.Error("isNoGroup(nil) = true, want false")
	}
}

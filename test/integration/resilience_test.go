// Integration tests of the queue and scheduler resilience (T-032;
// spec 11.2 "Resilience", 3.4, 11.4 item 9) against the live Redis and
// PostgreSQL dev environment: the internal/queue components
// (RedisQueue, Processing, Reliab.Claim with q:dead,
// BackpressureGate/WatermarkPublisher, MAXLEN) and internal/scheduler
// (DispatchBatch/EnqueueBatch/LeaseOnce).
//
// The scenarios:
//   - publication → consumption → the result record to the DB → XACK
//     (Processing, T-025): a processing failure does not acknowledge
//     the record, a new consumer after a break reads only the new
//     records (">"), and the hung one is returned by the claim — the
//     repeated processing closes the loop;
//   - a worker stop in the middle of a batch (spec 11.2): the
//     unacknowledged records are returned by XAUTOCLAIM with the
//     original body, the attempt counter grows, after MaxAttempts —
//     q:dead (the Dead format) and XACK of the originals; all the
//     task_id survive the cycle;
//   - the loss of Redis (spec 3.4, 11.4 item 9): the tasks disappear
//     together with the streams, the domains "hang" in queued — the
//     lease pass (T-029) returns them to pending, the dispatcher
//     (T-027) re-selects, the enqueue (T-028) re-publishes the batch;
//     a live worker restores the group after NOGROUP; the count and
//     the id set of the domains do not change;
//   - backpressure (T-026): below the threshold the publication goes
//     through, at the threshold it blocks until the capacity is
//     freed, a context cancellation — a clean exit (graceful shutdown,
//     spec 3.4);
//   - the phantom lag after the trim of the undelivered (T-168): an
//     XTRIM MAXLEN below the group position leaves a positive lag in
//     XINFO GROUPS (Redis 7.0.x does not subtract the trimmed — the
//     production phantom; 7.4+ corrects itself), the gate measures the
//     real backlog — min(lag, XLEN) + pending — and the publication
//     goes through; the frozen production counter is reproduced
//     explicitly (XGROUP … ENTRIESREAD);
//   - MAXLEN (T-025): a publication over the limit trims the stream —
//     the length does not grow with the number of publications, the
//     old records leave;
//   - freeing the acknowledged prefix (T-141): after "publication →
//     consumption → XACK" TrimOnce returns the stream length to the
//     boundary record, the key memory does not grow with the number
//     of processed ones; the hung PEL records and the undelivered tail
//     are not lost, a stream without a group is not broken by the
//     trim.
//
// The stream names — the production queue.Stream* constants: the queue
// transport accepts only known streams (knownStream), aliases would
// require editing the production code for the sake of a test.
// Isolation — a dedicated Redis DB number (redis.dev.conf: databases
// 16, the application services work in db 0): the tests do not see
// foreign keys and do not disturb the running stack, the stream key
// cleanup runs before and after each check inside the test DB.
// PostgreSQL — an isolated schema with migrations (migratedSchema),
// like the ingest tests.
package integration

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"crawler/internal/model"
	"crawler/internal/queue"
	"crawler/internal/scheduler"
)

// resilienceRedisDB — a dedicated Redis DB for the resilience tests:
// not the db 0 of the running services (the dev config allows 16 DBs).
const resilienceRedisDB = 15

// resilienceStreams — the production queue streams (queue.go), fully
// wiped between the checks inside the test DB (see the file comment).
var resilienceStreams = []string{
	queue.StreamDNS, queue.StreamHTTP, queue.StreamRender,
	queue.StreamRenderFallback, queue.StreamAnalyze, queue.StreamDead,
}

// resilienceRedisEnv — a client of the live testbed Redis (a dedicated
// DB) with a full cleanup of the queue streams before and after the
// test.
func resilienceRedisEnv(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, DB: resilienceRedisDB})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Fatalf("connecting to Redis %s: %v", redisAddr, err)
	}
	wipeQueueStreams(t, rdb)
	t.Cleanup(func() {
		wipeQueueStreams(t, rdb)
		_ = rdb.Close()
	})
	return rdb
}

// wipeQueueStreams removes the queue stream keys together with their
// groups.
func wipeQueueStreams(t *testing.T, rdb *redis.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Del(ctx, resilienceStreams...).Err(); err != nil {
		t.Errorf("clearing the streams %v: %v", resilienceStreams, err)
	}
}

// streamLen — XLEN: all the records of the stream, including the
// delivered and not XACK-acknowledged ones (only the MAXLEN trim
// removes them, T-025).
func streamLen(t *testing.T, rdb *redis.Client, stream string) int64 {
	t.Helper()
	n, err := rdb.XLen(context.Background(), stream).Result()
	if err != nil {
		t.Fatalf("XLEN %s: %v", stream, err)
	}
	return n
}

// pendingCount — the number of unacknowledged records of a group.
// XPENDING EXTENDED, not the summary form: an empty PEL — an empty
// slice without a nil answer, the number does not depend on the Redis
// version.
func pendingCount(t *testing.T, rdb *redis.Client, stream, group string) int {
	t.Helper()
	res, err := rdb.XPendingExt(context.Background(), &redis.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  "-",
		End:    "+",
		Count:  1000,
	}).Result()
	if err != nil {
		t.Fatalf("XPENDING %s/%s: %v", stream, group, err)
	}
	return len(res)
}

// waitPendingIdle — a deterministic wait for the "aging" of the hung
// records of a group: polling XPENDING EXTENDED (a read without side
// effects — the delivery counter and the consumer do not change) until
// the idle of each of the n records exceeds minIdle. By the moment of
// return an XAUTOCLAIM with this minIdle must return the records. A
// blind Sleep over MinIdle gave a 10–20 ms margin and flapped on a
// loaded runner: a sleep eaten by the scheduler left the records
// "unripe", Claim returned 0 — a false failure.
func waitPendingIdle(t *testing.T, rdb *redis.Client, stream, group string, n int, minIdle time.Duration) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		res, err := rdb.XPendingExt(context.Background(), &redis.XPendingExtArgs{
			Stream: stream,
			Group:  group,
			Start:  "-",
			End:    "+",
			Count:  1000,
		}).Result()
		if err != nil {
			t.Fatalf("XPENDING %s/%s: %v", stream, group, err)
		}
		ready := 0
		for _, p := range res {
			if p.Idle > minIdle {
				ready++
			}
		}
		if ready >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the %s/%s records did not age to %s within 5 s (%d of %d ready)", stream, group, minIdle, ready, n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// entryIDs — the identifiers of all the stream records (XRANGE).
func entryIDs(t *testing.T, rdb *redis.Client, stream string) []string {
	t.Helper()
	msgs, err := rdb.XRange(context.Background(), stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRANGE %s: %v", stream, err)
	}
	ids := make([]string, 0, len(msgs))
	for _, m := range msgs {
		ids = append(ids, m.ID)
	}
	return ids
}

// consumeTasks — reads up to n records of the stream by the consumer
// consumer (the Block is shortened in the test RedisOptions: an empty
// read does not hang for seconds).
func consumeTasks(t *testing.T, q *queue.RedisQueue, stream, group, consumer string, n int) []queue.Delivery {
	t.Helper()
	ds, err := q.Consume(context.Background(), stream, group, consumer, n)
	if err != nil {
		t.Fatalf("Consume %s (the group %s, the consumer %s): %v", stream, group, consumer, err)
	}
	return ds
}

// insertPendingDomains — a zone and pending domains with the labels
// labels by direct inserts into the isolated test schema (the
// end-to-end ingest pipeline — the T-023 territory); returns the
// domain ids. The zone counters are kept as apply_add
// (domains_total/domains_pending +n, T-178): the dispatcher selection
// filters the zones by domains_pending > 0 (T-183) — an insert
// bypassing the counters would leave the zone invisible to
// DispatchBatch.
func insertPendingDomains(t *testing.T, pool *pgxpool.Pool, zone string, labels []string) []int64 {
	t.Helper()
	ctx := context.Background()
	var zoneID int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name, domains_total, domains_pending) VALUES ($1, $2, $2) RETURNING id",
		zone, len(labels),
	).Scan(&zoneID); err != nil {
		t.Fatalf("inserting the zone %s: %v", zone, err)
	}
	rows, err := pool.Query(ctx,
		"INSERT INTO domains (zone_id, label) SELECT $1, unnest($2::text[]) RETURNING id",
		zoneID, labels)
	if err != nil {
		t.Fatalf("inserting the domains of the zone %s: %v", zone, err)
	}
	defer rows.Close()
	ids := make([]int64, 0, len(labels))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("reading the domain id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("inserting the domains of the zone %s: %v", zone, err)
	}
	return ids
}

// countDomains — the number of domains rows by the where condition.
func countDomains(t *testing.T, pool *pgxpool.Pool, where string) int {
	t.Helper()
	var n int
	query := "SELECT count(*) FROM domains"
	if where != "" {
		query += " WHERE " + where
	}
	if err := pool.QueryRow(context.Background(), query).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

// stateWhere — a countDomains condition by check_state.
func stateWhere(s model.CheckState) string {
	return fmt.Sprintf("check_state = %d", int16(s))
}

// idSet — a set for an order-free comparison.
func idSet[T comparable](xs []T) map[T]bool {
	m := make(map[T]bool, len(xs))
	for _, x := range xs {
		m[x] = true
	}
	return m
}

// TestResilience_ProcessingAckAfterDB — "publication → consumption →
// the result record to the DB → XACK" (spec 4.2, 3.4; Processing,
// T-025): a successful processing is acknowledged only after the DB
// record, a processing failure does not acknowledge the record; a
// restart-like break (a new consumer of the group reads only ">") does
// not re-give the records, and the hung one is returned by the claim —
// the repeated processing closes the loop. Not a single domain is
// lost: the final DB state covers all the batches.
func TestResilience_ProcessingAckAfterDB(t *testing.T) {
	skipUnlessIntegration(t)
	rdb := resilienceRedisEnv(t)
	pool := migratedSchema(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})
	reliab := queue.NewReliab(rdb, q, queue.ReliabOptions{MinIdle: 30 * time.Millisecond})
	ctx := context.Background()
	const group = "resil-dns"

	// 6 pending domains in two batches of 3 — the way EnqueueLoop
	// selects: after transitioning the first batch to queued the
	// dispatcher gives the next one.
	labels := []string{"h01", "h02", "h03", "h04", "h05", "h06"}
	if ids := insertPendingDomains(t, pool, "resil.test", labels); len(ids) != len(labels) {
		t.Fatalf("domains inserted: %d, want %d", len(ids), len(labels))
	}
	for i := 0; i < 2; i++ {
		batch, err := scheduler.DispatchBatch(ctx, pool, scheduler.DispatchQuery{Limit: 3})
		if err != nil {
			t.Fatalf("selecting batch #%d: %v", i+1, err)
		}
		if len(batch) != 3 {
			t.Fatalf("batch #%d: %d domains selected, want 3", i+1, len(batch))
		}
		if _, err := scheduler.EnqueueBatch(ctx, pool, q, batch); err != nil {
			t.Fatalf("enqueuing batch #%d: %v", i+1, err)
		}
	}

	// Both messages are published before the group is created: a lazy
	// XGROUP CREATE with the "0" start delivers the history too — the
	// first consumer receives both batches.
	got := consumeTasks(t, q, queue.StreamDNS, group, "w1", 10)
	if len(got) != 2 {
		t.Fatalf("messages consumed: %d, want 2 (both batches, the history before the group)", len(got))
	}

	// The result record is idempotent (spec 3.4): the queued → done
	// transition guarded by the current state — a repeated delivery
	// changes nothing.
	writeResult := func(ctx context.Context, d queue.Delivery) error {
		batch, err := queue.Decode[queue.DNSBatch](d)
		if err != nil {
			return err
		}
		domainIDs := make([]int64, len(batch.Domains))
		for i, dom := range batch.Domains {
			domainIDs[i] = dom.DomainID
		}
		_, err = pool.Exec(ctx, `
			UPDATE domains SET check_state = $1, last_checked_at = now()
			WHERE zone_id = $2 AND id = ANY($3) AND check_state = $4`,
			int16(model.CheckStateDone), batch.Domains[0].ZoneID, domainIDs,
			int16(model.CheckStateQueued))
		return err
	}

	// The first batch is processed: the DB record → XACK.
	if err := queue.Processing(ctx, q, group, got[0], writeResult); err != nil {
		t.Fatalf("Processing of the first batch: %v", err)
	}
	if n := countDomains(t, pool, stateWhere(model.CheckStateDone)); n != 3 {
		t.Fatalf("done after the first batch: %d, want 3 (XACK only after the DB write)", n)
	}

	// The second batch: "the worker died" before the result record —
	// Processing returns an error, no XACK, the record stays in the
	// group pending.
	workerDied := errors.New("the worker died before writing the result")
	if err := queue.Processing(ctx, q, group, got[1],
		func(context.Context, queue.Delivery) error { return workerDied }); err == nil {
		t.Fatal("Processing of the failing handling returned nil")
	}
	if n := pendingCount(t, rdb, queue.StreamDNS, group); n != 1 {
		t.Fatalf("unacknowledged records: %d, want 1", n)
	}

	// A restart-like break: the new consumer reads only the new records
	// (">") — neither the acknowledged nor the hung ones are re-given.
	if got := consumeTasks(t, q, queue.StreamDNS, group, "w2", 10); len(got) != 0 {
		t.Fatalf("the new consumer received %d records, want 0", len(got))
	}

	// The claim returns the hung record (attempts = 2: the first
	// delivery + the claim), the repeated processing closes the loop.
	waitPendingIdle(t, rdb, queue.StreamDNS, group, 1, 30*time.Millisecond)
	claimed, _, _, err := reliab.Claim(ctx, queue.StreamDNS, group, "w2", 30*time.Millisecond, 10, "0-0")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != got[1].ID {
		t.Fatalf("the claim returned %+v, want the record %s", claimed, got[1].ID)
	}
	if claimed[0].Attempts != 2 {
		t.Errorf("attempts after the claim: %d, want 2 (a delivery + a claim)", claimed[0].Attempts)
	}
	if err := queue.Processing(ctx, q, group, claimed[0].Delivery, writeResult); err != nil {
		t.Fatalf("Processing of the recovered batch: %v", err)
	}

	// The data is not lost: all 6 domains passed the check, the pending
	// is empty, nothing landed in q:dead.
	if n := countDomains(t, pool, stateWhere(model.CheckStateDone)); n != 6 {
		t.Errorf("done in the end: %d, want 6", n)
	}
	if n := pendingCount(t, rdb, queue.StreamDNS, group); n != 0 {
		t.Errorf("unacknowledged records in the end: %d, want 0", n)
	}
	if n := streamLen(t, rdb, queue.StreamDead); n != 0 {
		t.Errorf("records in q:dead: %d, want 0", n)
	}
}

// TestResilience_WorkerDiesMidBatch — a worker stop in the middle of a
// batch (spec 11.2): the consumer received the records (XREADGROUP),
// processed and acknowledged the first ones, the rest — not ("died").
// A new consumer with ">" does not receive them: the records are
// returned by XAUTOCLAIM (Reliab.Claim) with the original body; the
// recovery restores all the task_id, not a single task is lost and
// none lands in q:dead.
func TestResilience_WorkerDiesMidBatch(t *testing.T) {
	skipUnlessIntegration(t)
	rdb := resilienceRedisEnv(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})
	reliab := queue.NewReliab(rdb, q, queue.ReliabOptions{MinIdle: 30 * time.Millisecond})
	ctx := context.Background()
	const group = "resil-http"
	const total = 6

	// A batch of individual q:http tasks with known task_id.
	wantTasks := make(map[string]bool, total)
	for i := 0; i < total; i++ {
		task := fmt.Sprintf("t-mid-%02d", i+1)
		wantTasks[task] = true
		if _, err := q.Publish(ctx, queue.StreamHTTP, queue.HTTPTask{
			TaskID:     task,
			ZoneID:     7,
			DomainID:   int64(100 + i),
			Name:       fmt.Sprintf("mid%02d.resil.test", i+1),
			Subdomains: []queue.HTTPSubdomain{{SubdomainID: 0, Addrs: []string{"192.0.2.1"}}},
		}); err != nil {
			t.Fatalf("publishing the task %s: %v", task, err)
		}
	}

	batch := consumeTasks(t, q, queue.StreamHTTP, group, "w1", 10)
	if len(batch) != total {
		t.Fatalf("tasks consumed: %d, want %d", len(batch), total)
	}
	origPayload := make(map[string][]byte, total)
	for _, d := range batch {
		origPayload[d.ID] = d.Payload
	}

	// The first two are processed (the DB record → XACK), on the third
	// the worker died: the rest of the batch is not acknowledged.
	if err := q.Ack(ctx, queue.StreamHTTP, group, batch[0].ID, batch[1].ID); err != nil {
		t.Fatalf("XACK of the finished ones: %v", err)
	}

	// A new consumer does not receive the foreign unacknowledged records.
	if got := consumeTasks(t, q, queue.StreamHTTP, group, "w2", 10); len(got) != 0 {
		t.Fatalf("the new consumer received %d foreign records, want 0", len(got))
	}

	// The recovery pass: XAUTOCLAIM returns the rest of the batch
	// (2 of 6 acknowledged above).
	waitPendingIdle(t, rdb, queue.StreamHTTP, group, total-2, 30*time.Millisecond)
	claimed, _, _, err := reliab.Claim(ctx, queue.StreamHTTP, group, "recover", 30*time.Millisecond, total, "0-0")
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != total-2 {
		t.Fatalf("the claim returned %d records, want %d (the unfinished remainder of the batch)",
			len(claimed), total-2)
	}
	doneTasks := make(map[string]bool, total)
	for _, d := range batch[:2] { // the processed ones before the fall
		task, err := queue.Decode[queue.HTTPTask](d)
		if err != nil {
			t.Fatalf("decoding the record %s: %v", d.ID, err)
		}
		doneTasks[task.TaskID] = true
	}
	for _, c := range claimed {
		if c.Attempts != 2 {
			t.Errorf("record %s: attempts %d, want 2 (a delivery + a claim)", c.ID, c.Attempts)
		}
		if string(c.Payload) != string(origPayload[c.ID]) {
			t.Errorf("record %s: the body changed by the claim:\n got  %s\n want %s",
				c.ID, c.Payload, origPayload[c.ID])
		}
		task, err := queue.Decode[queue.HTTPTask](c.Delivery)
		if err != nil {
			t.Fatalf("decoding the record %s: %v", c.ID, err)
		}
		doneTasks[task.TaskID] = true
		// The recoverer finishes up: processing → XACK.
		if err := queue.Processing(ctx, q, group, c.Delivery,
			func(context.Context, queue.Delivery) error { return nil }); err != nil {
			t.Fatalf("Processing of the recovered record %s: %v", c.ID, err)
		}
	}

	// All the task_id of the batch survived the fall: each is processed
	// exactly once, none is lost and none flew to q:dead.
	if len(doneTasks) != total {
		t.Fatalf("unique task_ids after the recovery: %d, want %d", len(doneTasks), total)
	}
	for task := range wantTasks {
		if !doneTasks[task] {
			t.Errorf("the task %s lost during the recovery", task)
		}
	}
	if n := pendingCount(t, rdb, queue.StreamHTTP, group); n != 0 {
		t.Errorf("unacknowledged records in the end: %d, want 0", n)
	}
	if n := streamLen(t, rdb, queue.StreamDead); n != 0 {
		t.Errorf("records in q:dead: %d, want 0", n)
	}
}

// TestResilience_DeadLetterAfterMaxAttempts — the attempt counter and
// the transition to q:dead (spec 4.2, T-025): a record nobody
// acknowledges grows its PEL deliveries counter at every claim;
// MaxAttempts attempts (3 by default) — still in work, the next one —
// Dead (the task_id, the original stream/record, attempts, the subject
// identifiers without a payload — T-105) and an XACK of the original:
// the record leaves the pending. All the task_id survive the cycle —
// alive in q:dead.
func TestResilience_DeadLetterAfterMaxAttempts(t *testing.T) {
	skipUnlessIntegration(t)
	rdb := resilienceRedisEnv(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})
	// MaxAttempts — the default 3 (DefaultMaxAttempts); a small MinIdle:
	// there is no need to wait 5 minutes of DefaultClaimMinIdle in a
	// test.
	reliab := queue.NewReliab(rdb, q, queue.ReliabOptions{MinIdle: 20 * time.Millisecond})
	ctx := context.Background()
	const group = "resil-render"
	const total = 3

	wantRes := make(map[string]queue.Result, total)
	for i := 0; i < total; i++ {
		res := queue.Result{
			TaskID:      fmt.Sprintf("t-dead-%02d", i+1),
			ZoneID:      9,
			DomainID:    int64(200 + i),
			SubdomainID: 1,
			Name:        fmt.Sprintf("dead%02d.resil.test", i+1),
		}
		wantRes[res.TaskID] = res
		if _, err := q.Publish(ctx, queue.StreamRender, res); err != nil {
			t.Fatalf("publishing %s: %v", res.TaskID, err)
		}
	}
	batch := consumeTasks(t, q, queue.StreamRender, group, "w1", 10)
	if len(batch) != total {
		t.Fatalf("tasks consumed: %d, want %d", len(batch), total)
	}

	// The worker died without acknowledging anything. The "recoverers"
	// fail too: each claim pass returns the records to work, the
	// counter grows (a delivery + claim #1 = 2, #2 = 3 — the last
	// allowed).
	for round, wantAttempts := range []int{2, 3} {
		// The claim of the previous round pinned the records to the
		// "recoverer" and reset their idle — the aging is awaited anew.
		waitPendingIdle(t, rdb, queue.StreamRender, group, total, 20*time.Millisecond)
		claimed, _, _, err := reliab.Claim(ctx, queue.StreamRender, group, "recover", 20*time.Millisecond, total, "0-0")
		if err != nil {
			t.Fatalf("claim #%d: %v", round+1, err)
		}
		if len(claimed) != total {
			t.Fatalf("claim #%d returned %d records, want %d (the limit is not exhausted yet)",
				round+1, len(claimed), total)
		}
		for _, c := range claimed {
			if c.Attempts != wantAttempts {
				t.Errorf("claim #%d, record %s: attempts %d, want %d",
					round+1, c.ID, c.Attempts, wantAttempts)
			}
		}
		// No acknowledgement — the records hang again.
	}

	// Claim #3: the counter 4 > MaxAttempts 3 — the records are not
	// returned to work but transitioned to q:dead and acknowledged in
	// the original stream.
	waitPendingIdle(t, rdb, queue.StreamRender, group, total, 20*time.Millisecond)
	if claimed, _, _, err := reliab.Claim(ctx, queue.StreamRender, group, "recover", 20*time.Millisecond, total, "0-0"); err != nil {
		t.Fatalf("claim #3: %v", err)
	} else if len(claimed) != 0 {
		t.Fatalf("claim #3 returned work %+v, want the transition to q:dead", claimed)
	}
	if n := pendingCount(t, rdb, queue.StreamRender, group); n != 0 {
		t.Fatalf("the original records did not leave pending: %d, want 0", n)
	}
	if n := streamLen(t, rdb, queue.StreamDead); n != total {
		t.Fatalf("records in q:dead: %d, want %d", n, total)
	}

	// The manual q:dead triage (T-024/T-025/T-105): the Dead format
	// keeps the task_id, the source, the attempts and the subject
	// identifiers of the task — the key of the subdomain_results row
	// (zone/domain/subdomain/variant/name); the payload of the original
	// message is not carried by the records (the byte guard: the q:dns
	// batches up to ~250 KB × the 10,000 cap ≈ 2.5 GB — see T-105).
	deadMsgs := consumeTasks(t, q, queue.StreamDead, "resil-dead", "operator", 10)
	if len(deadMsgs) != total {
		t.Fatalf("read from q:dead: %d, want %d", len(deadMsgs), total)
	}
	seen := make(map[string]bool, total)
	for _, d := range deadMsgs {
		dead, err := queue.Decode[queue.Dead](d)
		if err != nil {
			t.Fatalf("decoding Dead: %v", err)
		}
		res, ok := wantRes[dead.TaskID]
		if !ok || seen[dead.TaskID] {
			t.Errorf("Dead.task_id = %q — outside the original batch or a duplicate", dead.TaskID)
			continue
		}
		seen[dead.TaskID] = true
		if dead.Stream != queue.StreamRender {
			t.Errorf("Dead.stream = %q, want %q", dead.Stream, queue.StreamRender)
		}
		if dead.ZoneID != res.ZoneID || dead.DomainID != res.DomainID ||
			dead.SubdomainID != res.SubdomainID || dead.Variant != res.Variant || dead.Name != res.Name {
			t.Errorf("the Dead of the record %s: the subject fields %+v, want the task subject %+v",
				dead.EntryID, dead, res)
		}
		if len(dead.Payload) != 0 {
			t.Errorf("the Dead.payload of the record %s carried over (T-105 — records without a payload): %d bytes",
				dead.EntryID, len(dead.Payload))
		}
		if dead.Attempts != 4 {
			t.Errorf("Dead.attempts = %d, want 4 (a delivery + 3 claims)", dead.Attempts)
		}
	}
	if len(seen) != total {
		t.Errorf("unique task_ids in q:dead: %d, want %d", len(seen), total)
	}
}

// TestResilience_DeadUnidentifiedNoTaskID — the terminal branch of a
// record without a task_id (T-260, FR-6.1): before T-260 bury rejected
// the transition with an error, and the record spun in the PEL in an
// eternal claim loop. A record published into a working stream
// bypassing the packet contract (a manual XADD without a task_id),
// after exhausting the attempts leaves the PEL and is visible in
// q:dead with the synthetic identifier unknown:<stream>:<entry-id>,
// without the subject fields.
func TestResilience_DeadUnidentifiedNoTaskID(t *testing.T) {
	skipUnlessIntegration(t)
	rdb := resilienceRedisEnv(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})
	// MaxAttempts — the default 3; a small MinIdle: no need to wait 5
	// minutes.
	reliab := queue.NewReliab(rdb, q, queue.ReliabOptions{MinIdle: 20 * time.Millisecond})
	ctx := context.Background()
	const group = "resil-unidentified"

	// A record outside the packet formats: the data envelope is there,
	// no task_id — published by a direct XADD (queue.Publish would
	// require a task_id).
	id, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: queue.StreamHTTP,
		Values: map[string]any{"data": `{"no_task":"id","zone_id":3}`},
	}).Result()
	if err != nil {
		t.Fatalf("XADD of an unidentifiable record: %v", err)
	}
	batch := consumeTasks(t, q, queue.StreamHTTP, group, "w1", 10)
	if len(batch) != 1 || batch[0].ID != id {
		t.Fatalf("%d records consumed, want 1 (%s)", len(batch), id)
	}

	// Claims #1–#2 return the record to work (the counters 2 and 3 —
	// the last allowed), no processing runs — the record hangs again;
	// claim #3 (the counter 4 > the limit 3) — the terminal branch.
	for round, wantAttempts := range []int64{2, 3} {
		waitPendingIdle(t, rdb, queue.StreamHTTP, group, 1, 20*time.Millisecond)
		claimed, _, _, err := reliab.Claim(ctx, queue.StreamHTTP, group, "recover", 20*time.Millisecond, 10, "0-0")
		if err != nil {
			t.Fatalf("claim #%d: %v", round+1, err)
		}
		if len(claimed) != 1 || claimed[0].Attempts != int(wantAttempts) {
			t.Fatalf("claim #%d returned %+v, want the record %s with the attempt %d",
				round+1, claimed, id, wantAttempts)
		}
	}
	waitPendingIdle(t, rdb, queue.StreamHTTP, group, 1, 20*time.Millisecond)
	if claimed, _, _, err := reliab.Claim(ctx, queue.StreamHTTP, group, "recover", 20*time.Millisecond, 10, "0-0"); err != nil {
		t.Fatalf("claim #3: %v", err)
	} else if len(claimed) != 0 {
		t.Fatalf("claim #3 returned work %+v, want the terminal branch", claimed)
	}

	// The record left the PEL (the claim loop is broken) and is visible
	// in q:dead.
	if n := pendingCount(t, rdb, queue.StreamHTTP, group); n != 0 {
		t.Fatalf("the record without task_id did not leave pending: %d, want 0 (FR-6.1)", n)
	}
	if n := streamLen(t, rdb, queue.StreamDead); n != 1 {
		t.Fatalf("records in q:dead: %d, want 1", n)
	}
	deadMsgs := consumeTasks(t, q, queue.StreamDead, "resil-dead", "operator", 10)
	if len(deadMsgs) != 1 {
		t.Fatalf("read from q:dead: %d, want 1", len(deadMsgs))
	}
	dead, err := queue.Decode[queue.Dead](deadMsgs[0])
	if err != nil {
		t.Fatalf("decoding Dead: %v", err)
	}
	if dead.TaskID != "unknown:q:http:"+id {
		t.Errorf("Dead.task_id = %q, want the synthetic unknown:q:http:%s", dead.TaskID, id)
	}
	if dead.Stream != queue.StreamHTTP || dead.EntryID != id {
		t.Errorf("Dead.stream/entry_id = %q/%q, want %q/%q", dead.Stream, dead.EntryID, queue.StreamHTTP, id)
	}
	if dead.Attempts != 4 {
		t.Errorf("Dead.attempts = %d, want 4 (a delivery + 3 claims)", dead.Attempts)
	}
	// No subject fields: the body is outside the packet formats, the
	// extraction is not trusted (the zone_id of the original body is
	// not carried).
	if dead.ZoneID != 0 || dead.DomainID != 0 || dead.Name != "" || dead.Domains != 0 {
		t.Errorf("the Dead of an unidentifiable record carries the subject fields: %+v", dead)
	}
}

// pendingFlakyRedis — a live Redis client with a counter read failure
// injection: the first XPENDINGs over the given records fail (FR-6.2 —
// a live Redis does not fail XPENDING by itself). Embedding
// *redis.Client promotes the other commands (XAUTOCLAIM, XCLAIM via
// Do) unchanged.
type pendingFlakyRedis struct {
	*redis.Client
	failOnce map[string]int // the record ID → how many first XPENDINGs fail
}

// XPendingExt — the injection wrapper: returning the command with an
// error before the live call (like the reliab_test.go fake).
func (f *pendingFlakyRedis) XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	if n, ok := f.failOnce[a.Start]; ok && n > 0 {
		f.failOnce[a.Start] = n - 1
		cmd := redis.NewXPendingExtCmd(ctx, "xpending")
		cmd.SetErr(errors.New("counter read failure injection (the FR-6.2 test)"))
		return cmd
	}
	return f.Client.XPendingExt(ctx, a)
}

// TestResilience_PendingFailureDoesNotBurnAttempts — an XPENDING
// classification failure after a claim does not burn an attempt
// (T-260, FR-6.2): the XAUTOCLAIM claim has spent the counter, reading
// it by XPENDING failed — Claim returns the record for a refund,
// RefundUnclassified reads the counter at the refund moment and rolls
// RETRYCOUNT back to the pre-claim value. A repeated claim returns the
// record to work with an honest attempt number, the processing closes
// the loop; before T-260 the attempt burned silently (Refund required
// a known counter) and the record burned down to q:dead.
func TestResilience_PendingFailureDoesNotBurnAttempts(t *testing.T) {
	skipUnlessIntegration(t)
	rdb := resilienceRedisEnv(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})
	ctx := context.Background()
	const group = "resil-unclassified"

	// Two tasks: the classification of A fails once (after the claim),
	// B is classified normally.
	var ids []string
	for _, task := range []string{"t-unclass-a", "t-unclass-b"} {
		id, err := q.Publish(ctx, queue.StreamRender, queue.Result{
			TaskID: task, ZoneID: 13, DomainID: 777,
		})
		if err != nil {
			t.Fatalf("publishing %s: %v", task, err)
		}
		ids = append(ids, id)
	}
	batch := consumeTasks(t, q, queue.StreamRender, group, "w1", len(ids))
	if len(batch) != len(ids) {
		t.Fatalf("tasks consumed: %d, want %d", len(batch), len(ids))
	}
	waitPendingIdle(t, rdb, queue.StreamRender, group, len(ids), 30*time.Millisecond)

	// The claim with a failure injection: XAUTOCLAIM pinned both (the
	// counters 1→2), the classification of A fails — the record is
	// returned by the second result for an attempt refund; B is
	// classified (2 ≤ 3) and returned to work.
	flaky := &pendingFlakyRedis{Client: rdb, failOnce: map[string]int{ids[0]: 1}}
	reliab := queue.NewReliab(flaky, q, queue.ReliabOptions{MinIdle: 30 * time.Millisecond})
	claimed, unclassified, _, err := reliab.Claim(ctx, queue.StreamRender, group, "recover", 30*time.Millisecond, 10, "0-0")
	if err == nil {
		t.Fatal("Claim with the failing classification: a counter read error expected")
	}
	if len(claimed) != 1 || claimed[0].ID != ids[1] {
		t.Fatalf("the valid part of the claim: %+v, want the record %s", claimed, ids[1])
	}
	if len(unclassified) != 1 || unclassified[0].ID != ids[0] {
		t.Fatalf("the records without classification: %+v, want %s", unclassified, ids[0])
	}

	// B is processed normally (the result "to the DB" → XACK) —
	// classified and processed, the attempt is spent by the delivery as
	// usual.
	if err := queue.Processing(ctx, q, group, claimed[0].Delivery,
		func(context.Context, queue.Delivery) error { return nil }); err != nil {
		t.Fatalf("Processing of the record %s: %v", ids[1], err)
	}

	// The refund of A's attempt: the counter is read at the refund
	// moment (2 after the claim) and rolled back to 1 — RETRYCOUNT is
	// back, the attempt did not burn.
	if err := reliab.RefundUnclassified(ctx, group, "recover", unclassified); err != nil {
		t.Fatalf("RefundUnclassified: %v", err)
	}
	pending := pendingExt(t, rdb, queue.StreamRender, group)
	if len(pending) != 1 || pending[0].ID != ids[0] {
		t.Fatalf("the PEL after the return: %+v, want only the record %s (B acknowledged)", pending, ids[0])
	}
	if pending[0].RetryCount != 1 {
		t.Errorf("the counter A after the return: %d, want 1 (RETRYCOUNT rolled back to the pre-claim value, FR-6.2)", pending[0].RetryCount)
	}

	// A repeated delivery of A: the claim reads the counter 2 (1 + the
	// claim) — the attempts did not burn (without the refund it would
	// be 3: the next delivery — q:dead without processing). The
	// processing closes the loop.
	waitPendingIdle(t, rdb, queue.StreamRender, group, 1, 30*time.Millisecond)
	plain := queue.NewReliab(rdb, q, queue.ReliabOptions{MinIdle: 30 * time.Millisecond})
	reclaimed, _, _, err := plain.Claim(ctx, queue.StreamRender, group, "recover", 30*time.Millisecond, 10, "0-0")
	if err != nil {
		t.Fatalf("a repeated claim of A: %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0].ID != ids[0] {
		t.Fatalf("the repeated claim: %+v, want the record %s", reclaimed, ids[0])
	}
	if reclaimed[0].Attempts != 2 {
		t.Errorf("attempts of A after the return: %d, want 2 (the return rolled back the claim with the failing classification)", reclaimed[0].Attempts)
	}
	if err := queue.Processing(ctx, q, group, reclaimed[0].Delivery,
		func(context.Context, queue.Delivery) error { return nil }); err != nil {
		t.Fatalf("Processing of the record %s: %v", ids[0], err)
	}

	if n := pendingCount(t, rdb, queue.StreamRender, group); n != 0 {
		t.Errorf("unacknowledged in the end: %d, want 0", n)
	}
	if n := streamLen(t, rdb, queue.StreamDead); n != 0 {
		t.Errorf("records in q:dead: %d, want 0 (the attempts did not burn, the records processed)", n)
	}
}

// TestResilience_RedisLossSchedulerRecovery — the loss of Redis (spec
// 3.4, 11.4 item 9): the tasks disappear together with the streams and
// the groups, the domains "hang" in queued. The expired lease return
// (T-029) returns them to pending, the dispatcher (T-027) re-selects,
// the enqueue (T-028) re-publishes the batch; a live worker recreates
// the group after NOGROUP and consumes the re-published. The source of
// truth is PostgreSQL: the count and the id set of the domains before
// and after match.
func TestResilience_RedisLossSchedulerRecovery(t *testing.T) {
	skipUnlessIntegration(t)
	rdb := resilienceRedisEnv(t)
	pool := migratedSchema(t)
	// One queue instance for the whole scenario — like a live worker
	// process that survived the Redis loss (the created-groups cache
	// goes stale).
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})
	ctx := context.Background()
	const group = "resil-dns"

	labels := make([]string, 0, 10)
	for i := 1; i <= 10; i++ {
		labels = append(labels, fmt.Sprintf("d%03d", i))
	}
	wantIDs := idSet(insertPendingDomains(t, pool, "resil.test", labels))
	if len(wantIDs) != 10 {
		t.Fatalf("domains inserted: %d, want 10", len(wantIDs))
	}

	// The scheduler cycle: the selection → the transition to queued
	// with a lease → the batch to q:dns (T-027/T-028).
	batch, err := scheduler.DispatchBatch(ctx, pool, scheduler.DispatchQuery{Limit: 10})
	if err != nil {
		t.Fatalf("selecting the batch: %v", err)
	}
	if len(batch) != 10 {
		t.Fatalf("domains selected: %d, want 10", len(batch))
	}
	res, err := scheduler.EnqueueBatch(ctx, pool, q, batch)
	if err != nil {
		t.Fatalf("enqueuing the batch: %v", err)
	}
	if res.Queued != 10 || res.Published != 10 {
		t.Fatalf("enqueue: queued %d, published %d; want 10/10", res.Queued, res.Published)
	}
	if n := countDomains(t, pool, stateWhere(model.CheckStateQueued)); n != 10 {
		t.Fatalf("queued after the enqueue: %d, want 10", n)
	}
	if n := streamLen(t, rdb, queue.StreamDNS); n != 1 {
		t.Fatalf("records in q:dns: %d, want 1 (one batch)", n)
	}

	// The worker took the task (the group is created), no
	// acknowledgement.
	if got := consumeTasks(t, q, queue.StreamDNS, group, "w1", 5); len(got) != 1 {
		t.Fatalf("the worker received tasks: %d, want 1", len(got))
	}

	// The Redis loss: the stream keys (and the groups in them) are
	// destroyed — like a flush of the lost instance. DEL of only the
	// test keys: the foreign data is not touched (the full cleanup —
	// resilienceRedisEnv).
	if err := rdb.Del(ctx, resilienceStreams...).Err(); err != nil {
		t.Fatalf("deleting the streams (losing Redis): %v", err)
	}
	if n := streamLen(t, rdb, queue.StreamDNS); n != 0 {
		t.Fatalf("after losing Redis, records in q:dns: %d, want 0", n)
	}

	// The domains "hang" in queued — the task is gone, no result.
	if n := countDomains(t, pool, stateWhere(model.CheckStateQueued)); n != 10 {
		t.Fatalf("domains stuck in queued: %d, want 10", n)
	}

	// The lease return (T-029): the time until expiry is shortened by
	// a direct queued_at update into the past (deterministically,
	// without waiting for the scheduler.lease of 30 minutes) — an hour
	// ago is certainly older than the threshold under any clock skews
	// of the client and the DB server.
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET queued_at = now() - interval '1 hour'"); err != nil {
		t.Fatalf("aging queued_at: %v", err)
	}
	lease, err := scheduler.LeaseOnce(ctx, pool, nil, scheduler.LeaseOptions{})
	if err != nil {
		t.Fatalf("LeaseOnce: %v", err)
	}
	if lease.Domains != 10 {
		t.Fatalf("the lease return: %d domains, want 10", lease.Domains)
	}
	if n := countDomains(t, pool, stateWhere(model.CheckStatePending)); n != 10 {
		t.Fatalf("pending after the lease return: %d, want 10", n)
	}
	if n := countDomains(t, pool, "queued_at IS NOT NULL"); n != 0 {
		t.Errorf("queued_at not reset on the returned ones: %d, want 0", n)
	}

	// The full scheduler cycle after the loss: the re-selection and the
	// re-publishing.
	batch2, err := scheduler.DispatchBatch(ctx, pool, scheduler.DispatchQuery{Limit: 10})
	if err != nil {
		t.Fatalf("re-selecting the batch: %v", err)
	}
	if len(batch2) != 10 {
		t.Fatalf("re-selected domains: %d, want 10", len(batch2))
	}
	res2, err := scheduler.EnqueueBatch(ctx, pool, q, batch2)
	if err != nil {
		t.Fatalf("re-publishing the batch: %v", err)
	}
	if res2.Queued != 10 || res2.Published != 10 {
		t.Fatalf("re-publishing: queued %d, published %d; want 10/10", res2.Queued, res2.Published)
	}
	if n := streamLen(t, rdb, queue.StreamDNS); n != 1 {
		t.Fatalf("records in q:dns after the re-publishing: %d, want 1", n)
	}

	// A live worker (the same process) reads the re-published: the
	// group is recreated after NOGROUP — no process restart needed.
	msgs := consumeTasks(t, q, queue.StreamDNS, group, "w1", 5)
	if len(msgs) != 1 {
		t.Fatalf("the worker received the task after the loss: %d, want 1", len(msgs))
	}
	dnsBatch, err := queue.Decode[queue.DNSBatch](msgs[0])
	if err != nil {
		t.Fatalf("decoding the re-published batch: %v", err)
	}
	gotIDs := make(map[int64]bool, len(dnsBatch.Domains))
	for _, dom := range dnsBatch.Domains {
		gotIDs[dom.DomainID] = true
		if dom.Name == "" {
			t.Errorf("domain %d in the re-published batch without a name", dom.DomainID)
		}
	}
	if len(gotIDs) != len(wantIDs) {
		t.Fatalf("domains in the re-published batch: %d, want %d", len(gotIDs), len(wantIDs))
	}
	for id := range wantIDs {
		if !gotIDs[id] {
			t.Errorf("domain %d lost during the recovery", id)
		}
	}

	// The data did not get lost: the domain count did not change, all
	// are in work (queued) again.
	if n := countDomains(t, pool, ""); n != 10 {
		t.Errorf("domains in the DB: %d, want 10", n)
	}
	if n := countDomains(t, pool, stateWhere(model.CheckStateQueued)); n != 10 {
		t.Errorf("queued after the re-publishing: %d, want 10", n)
	}
}

// TestResilience_BackpressureGate — the capacity gate (T-026; the spec
// 4.2/5.3 "strictly below the threshold"): below the threshold the
// publication goes through, at the threshold — it blocks until the
// capacity is freed (an XACK does not reduce the length — the records
// leave the stream only by a trim/XDEL), a context cancellation while
// waiting — a clean exit with a context error (graceful shutdown,
// spec 3.4).
func TestResilience_BackpressureGate(t *testing.T) {
	skipUnlessIntegration(t)
	rdb := resilienceRedisEnv(t)
	const watermark = 5
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: int64(watermark),
		Interval:  50 * time.Millisecond,
	})
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})
	pub := queue.NewWatermarkPublisher(q, gate)
	ctx := context.Background()

	task := func(i int) queue.Result {
		return queue.Result{
			TaskID:      fmt.Sprintf("t-bp-%03d", i),
			ZoneID:      3,
			DomainID:    int64(i),
			SubdomainID: 0,
		}
	}

	// Below the threshold — the publications go through; each check
	// runs before adding a record, so the fifth publication passes at
	// the length 4 and brings the stream exactly to the threshold.
	for i := 1; i <= watermark; i++ {
		if _, err := pub.Publish(ctx, queue.StreamRender, task(i)); err != nil {
			t.Fatalf("publication #%d below the threshold: %v", i, err)
		}
	}
	if n := streamLen(t, rdb, queue.StreamRender); n != watermark {
		t.Fatalf("the stream length: %d, want %d (the threshold)", n, watermark)
	}

	// The threshold is reached (len == watermark): the next publication
	// blocks — "strictly below the threshold".
	blocked := make(chan error, 1)
	go func() {
		_, err := pub.Publish(ctx, queue.StreamRender, task(watermark+1))
		blocked <- err
	}()
	select {
	case err := <-blocked:
		t.Fatalf("the publication at the threshold went through without waiting: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	// The capacity is freed (the records left the stream) — the
	// publication goes through.
	if err := rdb.XDel(ctx, queue.StreamRender,
		entryIDs(t, rdb, queue.StreamRender)...).Err(); err != nil {
		t.Fatalf("XDEL: %v", err)
	}
	select {
	case err := <-blocked:
		if err != nil {
			t.Fatalf("publishing after the capacity freed up: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the publication did not unblock after the capacity freed up")
	}
	if n := streamLen(t, rdb, queue.StreamRender); n != 1 {
		t.Errorf("the length after the publication: %d, want 1", n)
	}

	// A cancellation during the wait — a clean exit (the graceful
	// shutdown of the producer does not hang in the gate). Filling up
	// to the threshold again.
	for i := 1; i < watermark; i++ {
		if _, err := q.Publish(ctx, queue.StreamRender, task(100+i)); err != nil {
			t.Fatalf("filling to the threshold: %v", err)
		}
	}
	cancelCtx, cancel := context.WithCancel(context.Background())
	shutdown := make(chan error, 1)
	go func() {
		_, err := pub.Publish(cancelCtx, queue.StreamRender, task(999))
		shutdown <- err
	}()
	select {
	case err := <-shutdown:
		t.Fatalf("the publication went through before the cancel: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-shutdown:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceling the gate wait: %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the ctx cancel did not interrupt the gate wait")
	}
}

// TestResilience_BackpressureTrimmedLag — the phantom lag after the
// trim of the undelivered (T-168): the group lag (entries_added −
// entries_read, Redis ≥ 7.0) does not subtract the records trimmed by
// XTRIM/XDEL while undelivered — the gate must measure the real
// backlog (the records living in the stream), not the server counter.
// The 2026-10-22–23 incident (production, Redis 7.0.15): after the
// T-149 drain (XTRIM q:dns MAXLEN 125) the dns-worker lag froze at
// 1753 with an XLEN of 1 — the effective q:dns gate threshold of 125
// was closed forever, the scheduler was silent for ~15.5 h at 40.3M
// pending. Here both sides are checked on live Redis: the server
// semantics after an XTRIM of the undelivered (7.0.x keeps a phantom
// lag > XLEN, 7.4+ corrects itself — the gate depth does not depend on
// the version) and the protection against the frozen counter — the
// production state is reproduced explicitly (XGROUP … ENTRIESREAD
// below the reality, the same state the drain left on 7.0.15).
func TestResilience_BackpressureTrimmedLag(t *testing.T) {
	skipUnlessIntegration(t)
	rdb := resilienceRedisEnv(t)
	// An explicitly large MAXLEN: the trim here is manual (XTRIM
	// MAXLEN, like the T-149 drain), not the XADD cap.
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{MaxLen: 100000, Block: 150 * time.Millisecond})
	ctx := context.Background()
	const group = "resil-t168"
	const total, keep = 30, 5
	// The watermark between the real backlog (keep) and the phantom lag
	// (total): the "raw" lag would keep the gate closed forever, the
	// clamped depth opens the publication.
	const watermark = int64(10)
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{
		Watermark: watermark,
		Interval:  50 * time.Millisecond,
	})
	pub := queue.NewWatermarkPublisher(q, gate)

	task := func(i int) queue.Result {
		return queue.Result{
			TaskID:      fmt.Sprintf("t-tlag-%05d", i),
			ZoneID:      6,
			DomainID:    int64(i + 1),
			SubdomainID: 0,
		}
	}

	// The server semantics: XADD total → XTRIM MAXLEN keep trims the
	// undelivered (the group read nothing) → XINFO GROUPS gives a
	// positive lag: Redis 7.0.x does not subtract the trimmed (a lag of
	// 30 at an XLEN of 5 — a phantom), 7.4+ corrects itself (a lag of
	// 5). The gate sees the same in both cases: min(lag, XLEN) = 5.
	if err := rdb.XGroupCreateMkStream(ctx, queue.StreamAnalyze, group, "0").Err(); err != nil {
		t.Fatalf("XGROUP CREATE: %v", err)
	}
	for i := 0; i < total; i++ {
		if _, err := q.Publish(ctx, queue.StreamAnalyze, task(i)); err != nil {
			t.Fatalf("publication #%d: %v", i+1, err)
		}
	}
	if err := rdb.XTrimMaxLen(ctx, queue.StreamAnalyze, keep).Err(); err != nil {
		t.Fatalf("XTRIM MAXLEN %d: %v", keep, err)
	}
	xlen := streamLen(t, rdb, queue.StreamAnalyze)
	if xlen != keep {
		t.Fatalf("XLEN after the XTRIM: %d, want %d", xlen, keep)
	}
	groups, err := rdb.XInfoGroups(ctx, queue.StreamAnalyze).Result()
	if err != nil {
		t.Fatalf("XINFO GROUPS: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("groups on the stream: %d, want 1", len(groups))
	}
	if groups[0].Lag <= 0 {
		t.Fatalf("the group lag after the XTRIM of the undelivered: %d, want positive (the server either keeps the trimmed undelivered in the counter or corrected it — but not NULL)", groups[0].Lag)
	}
	n, err := gate.Len(ctx, queue.StreamAnalyze)
	if err != nil {
		t.Fatalf("Len after the XTRIM: %v", err)
	}
	// The group at "0": all the remaining records are undelivered — the
	// real backlog equals XLEN, the clamp guarantees it on any server
	// version.
	if n != keep {
		t.Fatalf("the depth after the XTRIM: %d, want %d (min(lag %d, XLEN %d) + pending 0)",
			n, keep, groups[0].Lag, xlen)
	}
	if _, err := pub.Publish(ctx, queue.StreamAnalyze, task(total)); err != nil {
		t.Fatalf("publishing after the XTRIM of the undelivered: %v", err)
	}

	// The 7.0.15 production state — a frozen entries-read counter: the
	// lag is a multiple above the stream length. On 7.4 the XTRIM
	// corrects the lag itself, so the counter is set explicitly
	// (XGROUP CREATE … ENTRIESREAD 0 — Redis ≥ 7.0; go-redis v9.23
	// cannot express the argument, a raw command). The group is placed
	// on the first record of the stream: the position is valid, the lag
	// is computed from the counter — a "raw" depth ≥ the watermark
	// would close the gate forever, the clamp opens the publication.
	if err := rdb.Del(ctx, queue.StreamAnalyze).Err(); err != nil {
		t.Fatalf("clearing the stream: %v", err)
	}
	for i := 0; i < total; i++ {
		if _, err := q.Publish(ctx, queue.StreamAnalyze, task(100+i)); err != nil {
			t.Fatalf("publication (the frozen counter) #%d: %v", i+1, err)
		}
	}
	if err := rdb.XTrimMaxLen(ctx, queue.StreamAnalyze, keep).Err(); err != nil {
		t.Fatalf("XTRIM MAXLEN (the frozen counter): %v", err)
	}
	ids := entryIDs(t, rdb, queue.StreamAnalyze)
	if len(ids) != keep {
		t.Fatalf("records after the XTRIM (the frozen counter): %d, want %d", len(ids), keep)
	}
	if err := rdb.Do(ctx, "XGROUP", "CREATE", queue.StreamAnalyze, group+"-stuck",
		ids[0], "ENTRIESREAD", "0").Err(); err != nil {
		t.Fatalf("XGROUP CREATE ENTRIESREAD: %v", err)
	}
	groups, err = rdb.XInfoGroups(ctx, queue.StreamAnalyze).Result()
	if err != nil {
		t.Fatalf("XINFO GROUPS (the frozen counter): %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("groups on the stream after the recreation: %d, want 1", len(groups))
	}
	if lag := groups[0].Lag; lag < watermark {
		t.Fatalf("the group lag %d below the watermark %d — the phantom not reproduced (the raw lag must close the gate)", lag, watermark)
	}
	n, err = gate.Len(ctx, queue.StreamAnalyze)
	if err != nil {
		t.Fatalf("Len (the frozen counter): %v", err)
	}
	if n != keep {
		t.Fatalf("the depth with the frozen counter: %d, want %d (the clamped min(lag %d, XLEN %d))",
			n, keep, groups[0].Lag, keep)
	}
	if _, err := pub.Publish(ctx, queue.StreamAnalyze, task(200)); err != nil {
		t.Fatalf("publishing with the frozen counter: %v", err)
	}
	if n := streamLen(t, rdb, queue.StreamAnalyze); n != keep+1 {
		t.Fatalf("XLEN after the publication: %d, want %d", n, keep+1)
	}
}

// TestResilience_TrimConfirmedPrefix — freeing the acknowledged prefix
// of a stream (T-141, spec 4.2 "Addendum 2026-10-23"): an XACK removes
// the record from the group PEL but not from the stream — TrimOnce
// (an XTRIM MINID by the oldest record that must stay) returns the
// length to the boundary record, and the key memory does not grow with
// the number of processed ones. The unprocessed tasks (the undelivered
// tail) and the hung PEL records are not lost; a stream without a group
// (q:dead) is not broken by the trim.
func TestResilience_TrimConfirmedPrefix(t *testing.T) {
	skipUnlessIntegration(t)
	rdb := resilienceRedisEnv(t)
	// An explicitly large MAXLEN — the prefix freeing is checked, not
	// the XADD trim cap.
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{MaxLen: 100000, Block: 150 * time.Millisecond})
	ctx := context.Background()
	const group = "resil-an"

	task := func(i int) queue.Result {
		return queue.Result{
			TaskID:      fmt.Sprintf("t-trim-%05d", i),
			ZoneID:      4,
			DomainID:    int64(i + 1),
			SubdomainID: 0,
		}
	}

	// The rounds "publication → consumption → XACK → TrimOnce":
	// without the freeing the acknowledged history accumulates up to
	// MAXLEN and the Redis budget (the T-093 incident) — with TrimOnce
	// the length returns to ~0 (the boundary record of last-delivered-id
	// stays: MINID is not strict), the key memory does not grow from
	// round to round.
	const perRound = 400
	const rounds = 5
	firstMem := int64(0)
	for r := 0; r < rounds; r++ {
		for i := 0; i < perRound; i++ {
			if _, err := q.Publish(ctx, queue.StreamAnalyze, task(r*perRound+i)); err != nil {
				t.Fatalf("round %d: publication #%d: %v", r+1, i+1, err)
			}
		}
		batch := consumeTasks(t, q, queue.StreamAnalyze, group, "w1", perRound)
		if len(batch) != perRound {
			t.Fatalf("round %d: %d consumed, want %d", r+1, len(batch), perRound)
		}
		ids := make([]string, len(batch))
		for i, d := range batch {
			ids[i] = d.ID
		}
		if err := q.Ack(ctx, queue.StreamAnalyze, group, ids...); err != nil {
			t.Fatalf("round %d: XACK: %v", r+1, err)
		}
		// An XACK frees only the PEL: the history in the stream
		// accumulates — that is the very subject of the fix (after the
		// first round — all the records).
		if n := streamLen(t, rdb, queue.StreamAnalyze); n < int64(perRound) {
			t.Fatalf("round %d: after the XACK the stream holds %d records < %d — is the history not accumulating?",
				r+1, n, perRound)
		}
		trimmed, _, err := queue.TrimOnce(ctx, rdb, queue.TrimOptions{Streams: []string{queue.StreamAnalyze}})
		if err != nil {
			t.Fatalf("round %d: TrimOnce: %v", r+1, err)
		}
		if trimmed == 0 {
			t.Fatalf("round %d: TrimOnce did not delete a single record", r+1)
		}
		if n := streamLen(t, rdb, queue.StreamAnalyze); n != 1 {
			t.Fatalf("round %d: after TrimOnce the stream holds %d records, want 1 (the boundary last-delivered-id, everything acknowledged)",
				r+1, n)
		}
		mem, err := rdb.MemoryUsage(ctx, queue.StreamAnalyze).Result()
		if err != nil {
			t.Fatalf("round %d: MEMORY USAGE: %v", r+1, err)
		}
		if r == 0 {
			firstMem = mem
		}
		// The key memory after a round — the metadata of the empty stream
		// and the group: it does not grow with the number of processed
		// records (2000 by the last round against 400 by the first).
		if grown := mem - firstMem; grown > 4096 {
			t.Fatalf("the key memory grows with the processed count: %d → %d bytes (+%d)",
				firstMem, mem, grown)
		}
	}
	if mem, _ := rdb.MemoryUsage(ctx, queue.StreamAnalyze).Result(); mem > 16*1024 {
		t.Errorf("the memory of the \"empty\" stream after %d processed records: %d bytes, want ≤ 16 KiB",
			rounds*perRound, mem)
	}

	// The hung PEL records are not lost: the trim up to the oldest
	// pending, the pending ones themselves stay in the stream and in
	// the PEL (they will come back by a claim).
	const hungTotal = 6
	for i := 0; i < hungTotal; i++ {
		if _, err := q.Publish(ctx, queue.StreamRender, task(10000+i)); err != nil {
			t.Fatalf("publishing the hanging one %d: %v", i+1, err)
		}
	}
	hung := consumeTasks(t, q, queue.StreamRender, "resil-render", "w1", hungTotal)
	if len(hung) != hungTotal {
		t.Fatalf("hung records consumed: %d, want %d", len(hung), hungTotal)
	}
	if err := q.Ack(ctx, queue.StreamRender, "resil-render", hung[0].ID, hung[1].ID, hung[2].ID); err != nil {
		t.Fatalf("XACK of the acknowledged prefix: %v", err)
	}
	if _, _, err := queue.TrimOnce(ctx, rdb, queue.TrimOptions{Streams: []string{queue.StreamRender}}); err != nil {
		t.Fatalf("TrimOnce with the hung ones: %v", err)
	}
	if n := streamLen(t, rdb, queue.StreamRender); n != hungTotal-3 {
		t.Fatalf("after TrimOnce the stream holds %d records, want %d (the acknowledged removed, the hung stayed)",
			n, hungTotal-3)
	}
	if n := pendingCount(t, rdb, queue.StreamRender, "resil-render"); n != hungTotal-3 {
		t.Fatalf("hung ones in the PEL: %d, want %d (the PEL trim does not touch them)", n, hungTotal-3)
	}
	left := entryIDs(t, rdb, queue.StreamRender)
	for i, d := range hung[3:] {
		if left[i] != d.ID {
			t.Errorf("the record %s lost/shifted (in the stream %s)", d.ID, left[i])
		}
	}

	// The undelivered tail is not lost: the point is last-delivered-id,
	// the records above stay and are delivered to the consumer.
	for i := 0; i < 5; i++ {
		if _, err := q.Publish(ctx, queue.StreamHTTP, queue.HTTPTask{
			TaskID:     fmt.Sprintf("t-trim-tail-%02d", i),
			ZoneID:     5,
			DomainID:   int64(300 + i),
			Name:       fmt.Sprintf("tail%02d.resil.test", i),
			Subdomains: []queue.HTTPSubdomain{{Addrs: []string{"192.0.2.1"}}},
		}); err != nil {
			t.Fatalf("publishing the tail %d: %v", i+1, err)
		}
	}
	head := consumeTasks(t, q, queue.StreamHTTP, "resil-http", "w1", 3)
	if len(head) != 3 {
		t.Fatalf("tail records consumed: %d, want 3", len(head))
	}
	if err := q.Ack(ctx, queue.StreamHTTP, "resil-http", head[0].ID, head[1].ID, head[2].ID); err != nil {
		t.Fatalf("XACK of the head: %v", err)
	}
	if _, _, err := queue.TrimOnce(ctx, rdb, queue.TrimOptions{Streams: []string{queue.StreamHTTP}}); err != nil {
		t.Fatalf("TrimOnce with an undelivered tail: %v", err)
	}
	if n := streamLen(t, rdb, queue.StreamHTTP); n != 3 {
		t.Fatalf("after TrimOnce the stream holds %d records, want 3 (the boundary + 2 undelivered)", n)
	}
	rest := consumeTasks(t, q, queue.StreamHTTP, "resil-http", "w2", 5)
	if len(rest) != 2 {
		t.Fatalf("the undelivered tail after the trim: %d records, want 2", len(rest))
	}

	// A stream without a group (q:dead: the manual triage, no consumer
	// groups) — TrimOnce skips it without an error, the records are not
	// touched.
	for i := 0; i < 3; i++ {
		if _, err := q.Publish(ctx, queue.StreamDead, queue.Dead{
			TaskID:   fmt.Sprintf("t-trim-dead-%02d", i),
			Stream:   queue.StreamAnalyze,
			EntryID:  fmt.Sprintf("%d-1", 1700000000000+i),
			Attempts: 4,
		}); err != nil {
			t.Fatalf("publishing to q:dead %d: %v", i+1, err)
		}
	}
	if _, _, err := queue.TrimOnce(ctx, rdb, queue.TrimOptions{Streams: []string{queue.StreamDead}}); err != nil {
		t.Fatalf("TrimOnce on a stream without a group: %v", err)
	}
	if n := streamLen(t, rdb, queue.StreamDead); n != 3 {
		t.Fatalf("records in q:dead after TrimOnce: %d, want 3 (without a group the stream is not freed)", n)
	}
}

// TestResilience_MaxLenTrims — MAXLEN ~ (spec 4.2, T-025): a
// publication over the limit trims the stream — the length stays near
// the threshold and does not grow with the number of publications, the
// old records leave (the first published record is not in the
// stream).
func TestResilience_MaxLenTrims(t *testing.T) {
	skipUnlessIntegration(t)
	rdb := resilienceRedisEnv(t)
	const maxLen = 40
	// An explicit limit instead of the stream policy: the XADD MAXLEN ~
	// trim itself is checked at small numbers, not the megarecords of
	// q:analyze.
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{MaxLen: maxLen, Block: 150 * time.Millisecond})
	ctx := context.Background()

	const published = 300
	first := ""
	for i := 0; i < published; i++ {
		id, err := q.Publish(ctx, queue.StreamAnalyze, queue.Result{
			TaskID:      fmt.Sprintf("t-ml-%04d", i+1),
			ZoneID:      2,
			DomainID:    int64(i + 1),
			SubdomainID: 0,
		})
		if err != nil {
			t.Fatalf("publication #%d: %v", i+1, err)
		}
		if first == "" {
			first = id
		}
	}

	// MAXLEN ~ trims with the precision of a records-list node: the
	// length is near the threshold but certainly bounded — it does not
	// grow with the publications. The overshoot of up to one listpack
	// macro node (~30–40 such messages in a 4 KB node; on the live
	// bench the length after 300 publications was 57), the maxLen*2 cap
	// cuts off "no trim" (there it would be 300) with a margin for the
	// node size.
	n := streamLen(t, rdb, queue.StreamAnalyze)
	if n < 1 || n > maxLen*2 {
		t.Fatalf("the q:analyze length after %d publications: %d, want 1..%d (MAXLEN ~ %d, the margin — one list-record macro-node)",
			published, n, maxLen*2, maxLen)
	}

	// The old records are cut: the earliest record of the stream is
	// newer than the first published one.
	oldest, err := rdb.XRangeN(ctx, queue.StreamAnalyze, "-", "+", 1).Result()
	if err != nil || len(oldest) != 1 {
		t.Fatalf("XRANGE q:analyze: %d records, err=%v", len(oldest), err)
	}
	if oldest[0].ID <= first {
		t.Errorf("the earliest record %s is not newer than the first published %s — no trim happened",
			oldest[0].ID, first)
	}
}

// --- OOM Redis does not burn the attempts (T-152) -----------------------------

// oomGuard — the maxmemory control of the test Redis for the OOM
// scenario (T-152): it saves the original maxmemory/maxmemory-policy
// (CONFIG GET), enterOOM lowers the cap below the current used_memory
// — all the writes are rejected with "OOM command not allowed" (the
// policy is forced to noeviction, as in production — docs/redis.md
// §4); release returns the original settings. release is registered in
// t.Cleanup STRICTLY before the first change: the settings come back
// on a test failure too. The window is held for a minimum of time: the
// server maxmemory is global for all the DBs of the instance.
type oomGuard struct {
	t        *testing.T
	rdb      *redis.Client
	maxmem   string
	policy   string
	released bool
}

func newOOMGuard(t *testing.T, rdb *redis.Client) *oomGuard {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	maxmem, err := rdb.ConfigGet(ctx, "maxmemory").Result()
	if err != nil {
		t.Fatalf("CONFIG GET maxmemory: %v", err)
	}
	policy, err := rdb.ConfigGet(ctx, "maxmemory-policy").Result()
	if err != nil {
		t.Fatalf("CONFIG GET maxmemory-policy: %v", err)
	}
	g := &oomGuard{t: t, rdb: rdb, maxmem: maxmem["maxmemory"], policy: policy["maxmemory-policy"]}
	t.Cleanup(g.release)
	return g
}

// usedMemory — the current memory consumption of the server (INFO memory).
func (g *oomGuard) usedMemory() int64 {
	g.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	info, err := g.rdb.Info(ctx, "memory").Result()
	if err != nil {
		g.t.Fatalf("INFO memory: %v", err)
	}
	for _, line := range strings.Split(info, "\r\n") {
		if v, ok := strings.CutPrefix(line, "used_memory:"); ok {
			n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			if err != nil {
				g.t.Fatalf("used_memory %q: %v", v, err)
			}
			return n
		}
	}
	g.t.Fatal("INFO memory without the used_memory field")
	return 0
}

func (g *oomGuard) configSet(param, value string) {
	g.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := g.rdb.ConfigSet(ctx, param, value).Err(); err != nil {
		g.t.Fatalf("CONFIG SET %s %s: %v", param, value, err)
	}
}

// enterOOM lowers the memory cap below the current consumption: the
// writes (SET/XADD) are rejected with OOM, the reads work. Skips the
// test if the write rejection could not be imitated (the CONFIG
// rights/the environment).
func (g *oomGuard) enterOOM() {
	g.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	g.configSet("maxmemory-policy", "noeviction")
	cap := g.usedMemory() - 64*1024
	if cap < 1 {
		cap = 1
	}
	g.configSet("maxmemory", strconv.FormatInt(cap, 10))
	if err := g.rdb.Set(ctx, "t152-oom-precondition", 1, time.Second).Err(); err == nil {
		g.release()
		g.t.Skipf("SET went through under the cap %d — OOM not simulated, the scenario not reproducible", cap)
	}
}

// release returns the original settings; it is safe on a repeated call
// (t.Cleanup + an explicit call by the test).
func (g *oomGuard) release() {
	if g.released {
		return
	}
	g.released = true
	g.configSet("maxmemory", g.maxmem)
	g.configSet("maxmemory-policy", g.policy)
}

// pendingExt — a PEL summary of the group with the delivery counters
// (RetryCount) — like the XPENDING from runbooks §2.6.
func pendingExt(t *testing.T, rdb *redis.Client, stream, group string) []redis.XPendingExt {
	t.Helper()
	res, err := rdb.XPendingExt(context.Background(), &redis.XPendingExtArgs{
		Stream: stream, Group: group, Start: "-", End: "+", Count: 1000,
	}).Result()
	if err != nil {
		t.Fatalf("XPENDING %s/%s: %v", stream, group, err)
	}
	return res
}

// TestResilience_OOMDoesNotBurnAttempts — an OOM Redis does not burn
// the task attempts (T-152; the load-measurements.md §11.1
// post-mortem, the recipe — runbooks §2.6): the result publications
// (XADD) fail with "OOM command not allowed", the deliveries stay
// without an XACK — the PEL delivery counter (the XPENDING RetryCount)
// does not grow: RecoverLoop does not claim without a chance to record
// the result (the write gate by a probe). The memory is freed (in the
// incident — the drain of the legacy q:dns) — the tasks are carried by
// the claim to the result publication and the XACK, q:dead is empty.
func TestResilience_OOMDoesNotBurnAttempts(t *testing.T) {
	skipUnlessIntegration(t)
	rdb := resilienceRedisEnv(t)
	// The XADD retries are short: the gate and the carrying are checked
	// here, not the retry pause (the latter — TestPublishRetriesInfra).
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{
		Block:             150 * time.Millisecond,
		PublishRetries:    1,
		PublishRetryPause: time.Millisecond,
	})
	reliab := queue.NewReliab(rdb, q, queue.ReliabOptions{MinIdle: 30 * time.Millisecond})
	ctx := context.Background()
	const group = "resil-oom"
	const total = 3

	// A q:render batch — like the render cohort of the incident;
	// published and consumed before the OOM window (the first delivery:
	// RetryCount = 1).
	for i := 0; i < total; i++ {
		if _, err := q.Publish(ctx, queue.StreamRender, queue.Result{
			TaskID:      fmt.Sprintf("t-oom-%02d", i+1),
			ZoneID:      11,
			DomainID:    int64(400 + i),
			SubdomainID: 0,
		}); err != nil {
			t.Fatalf("publishing %d: %v", i+1, err)
		}
	}
	batch := consumeTasks(t, q, queue.StreamRender, group, "w1", total)
	if len(batch) != total {
		t.Fatalf("tasks consumed: %d, want %d", len(batch), total)
	}

	// The handler — a model of the incident worker: the processing is
	// healthy, the result is published to q:analyze; the publication is
	// the very failure.
	process := func(c context.Context, d queue.Delivery) error {
		res, err := queue.Decode[queue.Result](d)
		if err != nil {
			return err
		}
		res.TaskID += "-done"
		_, err = q.Publish(c, queue.StreamAnalyze, res)
		return err
	}

	// The OOM window and the hung-records return loop: without the gate
	// it would claim in circles.
	guard := newOOMGuard(t, rdb)
	guard.enterOOM()
	loopCtx, cancelLoop := context.WithCancel(ctx)
	defer cancelLoop()
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		_ = queue.RecoverLoop(loopCtx, reliab, process, queue.RecoverOptions{
			Stream: queue.StreamRender, Group: group, Consumer: "recover",
			Batch: total, Interval: 100 * time.Millisecond,
		})
	}()

	// Several loop ticks (700 ms ≥ 6 intervals): the write probe fails
	// — no claim, the delivery counter is stable at the first delivery.
	time.Sleep(700 * time.Millisecond)
	pending := pendingExt(t, rdb, queue.StreamRender, group)
	if len(pending) != total {
		t.Fatalf("records in the PEL: %d, want %d", len(pending), total)
	}
	for _, p := range pending {
		if p.RetryCount != 1 {
			t.Errorf("record %s: deliveries %d, want 1 — claims on an unavailable record burn attempts (T-152)",
				p.ID, p.RetryCount)
		}
	}
	if n := streamLen(t, rdb, queue.StreamAnalyze); n != 0 {
		t.Errorf("q:analyze publications under OOM: %d, want 0", n)
	}

	// The memory is freed: the records go through — the claim returns
	// the deliveries, the processing carries them to the XACK (the
	// publication + the acknowledgement).
	guard.release()
	deadline := time.Now().Add(10 * time.Second)
	for pendingCount(t, rdb, queue.StreamRender, group) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("after the memory freed up the PEL is not empty: %d records",
				pendingCount(t, rdb, queue.StreamRender, group))
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancelLoop()
	<-loopDone
	if n := streamLen(t, rdb, queue.StreamAnalyze); n != total {
		t.Errorf("results in q:analyze: %d, want %d (the processing is healthy, only the publication died)", n, total)
	}
	if n := streamLen(t, rdb, queue.StreamDead); n != 0 {
		t.Errorf("records in q:dead: %d, want 0 (T-152: OOM does not burn attempts)", n)
	}
}

// TestResilience_CancelMidBatchDoesNotBurnAttempts — a RecoverLoop
// cancellation in the middle of a claimed batch does not burn the
// attempts of the unprocessed records (T-222): an XAUTOCLAIM
// increments the PEL deliveries counter (the XPENDING RetryCount) of
// the whole batch at the claim moment, so the records the traversal
// did not reach get their attempt back — XCLAIM … JUSTID RETRYCOUNT
// <attempts-1> (the semantics is checked on live Redis: RETRYCOUNT
// sets the counter to the exact value; JUSTID without RETRYCOUNT does
// not touch the counter — a separate pass below). The record that
// entered the processing spends the attempt: its "delivery + claim"
// counter stays. The scenario — a graceful shutdown (the stop budget
// is less than the render batch traversal time): before T-222 the
// batch tail lost an attempt on every restart, after max_attempts the
// tasks went to q:dead without a single processing.
func TestResilience_CancelMidBatchDoesNotBurnAttempts(t *testing.T) {
	skipUnlessIntegration(t)
	rdb := resilienceRedisEnv(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})
	// MinIdle 500 ms: Mature for a claim in half a second and a margin
	// for the refund guard (the batch traversal is shorter than
	// claim_min_idle — the T-153 invariant; at 30 ms the test would
	// flap on a loaded runner).
	reliab := queue.NewReliab(rdb, q, queue.ReliabOptions{MinIdle: 500 * time.Millisecond})
	ctx := context.Background()
	const group = "resil-refund"
	const total = 4

	// A q:render batch read and not acknowledged: RetryCount = 1 (the
	// first delivery) — a model of the hung batch handled by the
	// RecoverLoop.
	for i := 0; i < total; i++ {
		if _, err := q.Publish(ctx, queue.StreamRender, queue.Result{
			TaskID:   fmt.Sprintf("t-refund-%02d", i+1),
			ZoneID:   12,
			DomainID: int64(500 + i),
		}); err != nil {
			t.Fatalf("publishing %d: %v", i, err)
		}
	}
	batch := consumeTasks(t, q, queue.StreamRender, group, "w1", total)
	if len(batch) != total {
		t.Fatalf("tasks consumed: %d, want %d", len(batch), total)
	}
	waitPendingIdle(t, rdb, queue.StreamRender, group, total, 500*time.Millisecond)

	// The return loop: the first record enters the processing and
	// "hangs on the stop budget" — a ctx cancellation in the middle of
	// the batch.
	loopCtx, cancelLoop := context.WithCancel(ctx)
	defer cancelLoop()
	// A buffer: the start signal is not lost if the handler raced the
	// reception (the P3-1 review) — otherwise the test would hang until
	// the package timeout.
	started := make(chan struct{}, 1)
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		_ = queue.RecoverLoop(loopCtx, reliab, func(c context.Context, d queue.Delivery) error {
			started <- struct{}{}
			<-c.Done() // "the graceful shutdown budget ran out mid-record"
			return c.Err()
		}, queue.RecoverOptions{
			Stream: queue.StreamRender, Group: group, Consumer: "recover",
			Batch: total, Interval: 100 * time.Millisecond,
		})
	}()

	<-started    // the first record in work
	cancelLoop() // the graceful shutdown
	<-loopDone

	// The unprocessed tail (3 records) got its attempt back: the
	// counter as before the claim (1). The started record — "delivery
	// + claim" (2): the attempt is spent by the delivery that started
	// the processing.
	assertRefundCounts := func() {
		t.Helper()
		pending := pendingExt(t, rdb, queue.StreamRender, group)
		if len(pending) != total {
			t.Fatalf("records in the PEL: %d, want %d", len(pending), total)
		}
		for _, p := range pending {
			want := int64(1)
			if p.ID == batch[0].ID {
				want = 2
			}
			if p.RetryCount != want {
				t.Errorf("record %s: deliveries %d, want %d (a cancel mid-batch does not burn the attempts of the unprocessed ones, T-222)",
					p.ID, p.RetryCount, want)
			}
		}
	}
	assertRefundCounts()

	// The JUSTID-without-RETRYCOUNT semantics on live Redis (the
	// redis.md doc §4.2): a claim of the identifiers by itself does not
	// increment the counter.
	if err := rdb.Do(ctx, "xclaim", queue.StreamRender, group, "recover", 0, batch[0].ID, "justid").Err(); err != nil {
		t.Fatalf("xclaim justid: %v", err)
	}
	assertRefundCounts()

	if n := streamLen(t, rdb, queue.StreamDead); n != 0 {
		t.Errorf("records in q:dead: %d, want 0", n)
	}
}

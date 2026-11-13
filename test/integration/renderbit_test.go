// Integration tests of the render_pending bit × debt counter automaton
// (T-258, the TZ-fsm spec tasks/TZ-fsm.md §3 FR-4) against live
// PostgreSQL/Redis: the scenario chain of all the specification
// transitions (internal/httpworker/renderq.go, the table at
// renderSetPendingSQL — the single point of truth) by the real writers
// on one domain: D1 the debt is set by a result record with a full
// queue (+1), D2 a repeated delivery with the bit already set (the
// guard: a second +1 is forbidden), D8 a fresh episode with a free
// queue (the bit is not erased, the pair is untouched), D3 the
// rework pass "published → cleared" (−1) and the mirror clearing
// guard D7 (a second −1 is forbidden), D4 the draft flag (a clear
// without a publication, −1), D5 the keep of unreadable content (the
// pair is preserved), D6 the clearing by a delete-file (−1, T-186).
// The bit-counter pairing (the I2 invariant) after every step is
// checked by the monitor reconciliation (VerifyCounters — the counter
// against the live number of rows with the bit); surviving the "other"
// bits (is_idn) is checked on every transition (the httpbit_test.go
// style, T-257). The q:render overflow is deterministic: watermark 1
// and one live message in the stream (no groups — the XLEN depth);
// resetting the depth between steps — a DEL of the stream (the
// render_late_test.go style). The desync branch "counter > 0, the
// selection is empty" (the orphan bits diagnostics) is already covered
// by TestRenderRequeuePass ("a repeated pass does not select the
// cleared ones"); the deleted-orphans themselves are a special state
// of the specification, not required by the FR-4 tests. The call level
// — library-level, without starting services; isolation —
// withTestSchema + Redis DB 15.
package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"crawler/internal/content"
	"crawler/internal/httpfetch"
	"crawler/internal/httpworker"
	"crawler/internal/ingest"
	"crawler/internal/model"
	"crawler/internal/monitor"
	"crawler/internal/queue"
)

// renderBitStaticBody — a static page (non-SPA: the text is abundant,
// no container and no noscript) — the DetectSPA verdict is false, no
// render is needed (T-057).
var renderBitStaticBody = []byte(`<html><body>plain static page with plenty of visible text</body></html>`)

// renderBitDebt — the render debt counter crawler_counters['render_pending'].
func renderBitDebt(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx,
		"SELECT value FROM crawler_counters WHERE name = 'render_pending'").Scan(&n); err != nil {
		t.Fatalf("the render debt counter: %v", err)
	}
	return n
}

// renderBitFailContent — a RenderContent with a transient read failure
// (the keep scenario, D5): any Get — a storage error, not a ctx
// cancellation — renderDomainKeys keeps the flag until the next tick.
type renderBitFailContent struct{ err error }

func (f renderBitFailContent) Get(context.Context, content.Hash) ([]byte, error) {
	return nil, f.err
}

// TestRenderPendingBit_SpecTransitions — the chain of the specification
// transitions on one domain: D0 steady state → D1 debt (+1) → D2 the
// setting guard → D8 a fresh episode without a transition → D3
// "published → cleared" (−1) + D7 the clearing guard → D4 the draft
// flag (−1 without a publication) → D5 the keep → D6 the clearing by a
// delete-file (−1). The counter value walks 0 → 1 → 0 exactly by the
// ±1 steps of the transitions, repeated deliveries/clearings do not
// move it.
func TestRenderPendingBit_SpecTransitions(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t) // a shared package helper
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// q:render: the publication is real (XADD); the stream has no
	// groups — the XLEN depth, resetting the depth between steps — a
	// DEL (a consumption emulation). The stream cleanup and the client
	// closing are done by resilienceRedisEnv (wipeQueueStreams with a
	// fresh context; the test ctx by the time of t.Cleanup is already
	// cancelled by defer cancel — review T-258 P1-1).
	rdb := resilienceRedisEnv(t)
	q := queue.NewRedisQueue(rdb, queue.RedisOptions{Block: 150 * time.Millisecond})
	store, err := content.OpenPackStore(pool, content.PackOptions{
		Dir:           t.TempDir(),
		PackSize:      8 << 10,
		MaxStoreBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("OpenPackStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	// watermark 1: an empty stream — free, one live message — already
	// an overflow ("strictly below the threshold", spec 4.2).
	gate := queue.NewBackpressureGate(rdb, queue.BackpressureOptions{})
	renderQ, err := httpworker.NewRenderQueue(q, gate, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := httpworker.NewRenderRecorder(pool, q, renderQ, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	zone := countersSeedZone(t, pool, "fsm-render.test")
	// countersSeedDomains numbers the labels: prefix "spec" → label "spec0".
	ids := countersSeedDomains(t, pool, zone, "spec", 1, model.CheckStateQueued, model.StatusNew)
	id := ids[0]
	const name = "spec0.fsm-render.test"

	// The "other bit" is_idn (unrelated to the counter): the D1–D8
	// specification requires keeping it — OR/AND ~bit do not touch
	// foreign digits.
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET flags = flags | $1 WHERE zone_id = $2 AND id = $3",
		int16(model.FlagIDN), zone, id); err != nil {
		t.Fatalf("seeding is_idn: %v", err)
	}
	// A manual insert bypassing the incremental deltas — the
	// reconciliation is the authority (the reference point of the zone
	// counters, the httpbit_test.go style).
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("reconciliation: %v", err)
	}

	debt := func() int64 { return renderBitDebt(ctx, t, pool) }
	flags := func() int16 { return httpBitFlags(ctx, t, pool, zone, id) }
	renderLen := func() int64 { return streamLen(t, rdb, queue.StreamRender) }
	wantFlags := func(want int16, what string) {
		t.Helper()
		if f := flags(); f != want {
			t.Fatalf("%s: flags = %b, want %b", what, f, want)
		}
	}
	full := func() {
		t.Helper()
		if free, err := renderQ.HasCapacity(ctx); err != nil || free {
			t.Fatalf("HasCapacity = %v err=%v, want false (q:render is full, depth %d)", free, err, renderLen())
		}
	}
	drain := func() {
		t.Helper()
		if err := rdb.Del(ctx, queue.StreamRender).Err(); err != nil {
			t.Fatalf("resetting the q:render depth: %v", err)
		}
	}

	// episode — an HTTP episode by the real recording chain: the early
	// Put of the content (T-147, like the ContentSaver T-056) →
	// RecordAndPublish (T-058/T-066). The SPA verdict — DetectSPA over
	// the same bytes (deterministic, T-057).
	episode := func(body []byte) {
		t.Helper()
		put, err := store.Put(ctx, body)
		if err != nil {
			t.Fatalf("storing the content: %v", err)
		}
		h := put.Hash
		v := httpfetch.DetectSPA(body)
		res := &httpworker.DomainResult{
			ZoneID: zone, DomainID: id, Name: name,
			Subdomains: []httpworker.SubdomainResult{{
				SubdomainID: 0,
				Records: []httpworker.Record{{
					Variant:     0,
					Scheme:      model.SchemeHTTPS,
					FinalURL:    "https://" + name + "/",
					HTTPStatus:  200,
					ContentHash: &h,
					SPA:         &v,
				}},
			}},
		}
		if ok, err := rec.RecordAndPublish(ctx, res); err != nil || !ok {
			t.Fatalf("RecordAndPublish: ok=%v err=%v", ok, err)
		}
	}

	// armFull — fill q:render up to the threshold (one live message).
	armFull := func() {
		t.Helper()
		if renderLen() == 0 {
			if _, err := q.Publish(ctx, queue.StreamRender, queue.Result{
				ZoneID: zone, DomainID: id, SubdomainID: 0, Name: name,
			}); err != nil {
				t.Fatalf("filling q:render: %v", err)
			}
		}
		full()
	}

	const debtFlags = int16(model.FlagIDN | model.FlagRenderPending)
	const cleanFlags = int16(model.FlagIDN)

	// D0 "no debt": the steady state — the bit is clean, the counter
	// 0; the T-181 guard does not run the heavy selection (an empty
	// outcome without an error).
	if debt() != 0 {
		t.Fatalf("D0: debt counter = %d, want 0", debt())
	}
	if res, err := httpworker.RequeueOnce(ctx, pool, store, renderQ, httpworker.RequeueOptions{}); err != nil || res.Selected != 0 {
		t.Fatalf("D0: RequeueOnce = %+v err=%v, want selected=0 (steady state)", res, err)
	}

	// D1: a result record with a full queue — the bit and the counter
	// +1 by the same transaction, no q:render publication happens, the
	// episode finishes without a delay (done/alive).
	armFull()
	episode(rqBody)
	wantFlags(debtFlags, "after D1")
	if debt() != 1 {
		t.Fatalf("D1: counter = %d, want 1 (the set +1)", debt())
	}
	if n := renderLen(); n != 1 {
		t.Fatalf("D1: q:render = %d, want 1 (only the fill — no publication happened)", n)
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.state != int16(model.CheckStateDone) || r.status != int16(model.StatusAlive) {
		t.Fatalf("after D1 = %+v, want done/alive (the flag does not delay the episode, §5.6)", r)
	}
	countersVerifyEmpty(t, pool, "after D1 (the set +1)")

	// D2: a repeated delivery of the same episode — the queue is still
	// full, the bit is already set: the setting guard gives RowsAffected
	// 0, a second +1 is FORBIDDEN (the pairing is not doubled); the rows
	// are rewritten idempotently.
	full()
	episode(rqBody)
	wantFlags(debtFlags, "after D2")
	if debt() != 1 {
		t.Fatalf("D2: counter = %d, want 1 (the bit guard: a second +1 is forbidden)", debt())
	}
	if n := renderLen(); n != 1 {
		t.Fatalf("D2: q:render = %d, want 1 (still no publication)", n)
	}
	countersVerifyEmpty(t, pool, "after D2 (a repeated delivery — no delta)")

	// D8: a fresh episode with a free queue — the result record does
	// NOT erase the bit and does not touch the counter: the tasks are
	// published by the episode itself (the duplication is bounded), the
	// holder stays the rework pass.
	drain()
	episode(rqBody)
	wantFlags(debtFlags, "after D8")
	if debt() != 1 {
		t.Fatalf("D8: counter = %d, want 1 (a fresh episode does not touch the pair)", debt())
	}
	if n := renderLen(); n != 1 {
		t.Fatalf("D8: q:render = %d, want 1 (the fresh episode publishes itself)", n)
	}
	countersVerifyEmpty(t, pool, "after D8 (a publication without a transition)")

	// D3: the rework pass — "published → cleared": re-publishing the
	// SPA task to q:render, then clearing the bit with the counter −1
	// in one transaction.
	drain()
	res, err := httpworker.RequeueOnce(ctx, pool, store, renderQ, httpworker.RequeueOptions{})
	if err != nil {
		t.Fatalf("D3: RequeueOnce: %v", err)
	}
	if res.Selected != 1 || res.Cleared != 1 || res.Published != 1 {
		t.Fatalf("D3: outcome %+v, want selected=1 cleared=1 published=1", res)
	}
	if n := renderLen(); n != 1 {
		t.Fatalf("D3: q:render = %d, want 1 (re-publishing the SPA task)", n)
	}
	wantFlags(cleanFlags, "after D3")
	if debt() != 0 {
		t.Fatalf("D3: counter = %d, want 0 (the clear −1)", debt())
	}
	countersVerifyEmpty(t, pool, "after D3 (the clear −1)")

	// D7 (the mirror clearing guard): a competitor has already cleared
	// the bit — a repeated CLEAR by the guard gives RowsAffected 0, a
	// second −1 is FORBIDDEN (the raw SQL does not touch the counter by
	// definition — the whole point is in RowsAffected = 0: exactly that
	// is checked here; the pass operator renderClearPendingTx skips the
	// decrement on this zero — the TestRequeueOnceClearRace unit test).
	// The SQL — a literal of renderClearPendingSQL (the duplication —
	// the schema_test.go style, the constant is not exported; a literal
	// desync with the code is caught by TestSchema_RenderDebtCounter —
	// the guard contract).
	const clearSQL = "UPDATE domains AS t SET flags = (t.flags & -3)::int2 " +
		"WHERE t.zone_id = $1::int2 AND t.id = $2::int8 AND (t.flags & 2) <> 0"
	tag, err := pool.Exec(ctx, clearSQL, zone, id)
	if err != nil {
		t.Fatalf("D7: the repeated clear: %v", err)
	}
	if tag.RowsAffected() != 0 {
		t.Fatalf("D7: the repeated clear: RowsAffected = %d, want 0 (the bit guard)", tag.RowsAffected())
	}
	if debt() != 0 {
		t.Fatalf("D7: counter = %d, want 0 (a second −1 is forbidden)", debt())
	}
	countersVerifyEmpty(t, pool, "after D7 (the clearing guard)")

	// D4 "the draft flag": the debt is re-armed (D1 — the queue holds
	// the message published by the D3 pass), then the page stopped
	// being SPA (a fresh episode with a static body with a free queue —
	// nothing to render, the bit survived the record, D8) — the pass
	// clears the flag WITHOUT a publication, the counter −1.
	full()
	episode(rqBody)
	wantFlags(debtFlags, "after re-arming D4")
	if debt() != 1 {
		t.Fatalf("re-arming D4: counter = %d, want 1", debt())
	}
	drain()
	episode(renderBitStaticBody)
	wantFlags(debtFlags, "after the non-SPA episode (the bit survived the record)")
	if debt() != 1 {
		t.Fatalf("non-SPA episode: counter = %d, want 1 (the pair untouched)", debt())
	}
	if n := renderLen(); n != 0 {
		t.Fatalf("non-SPA episode: q:render = %d, want 0 (non-SPA is not published)", n)
	}
	res, err = httpworker.RequeueOnce(ctx, pool, store, renderQ, httpworker.RequeueOptions{})
	if err != nil {
		t.Fatalf("D4: RequeueOnce: %v", err)
	}
	if res.Selected != 1 || res.Cleared != 1 || res.Published != 0 {
		t.Fatalf("D4: outcome %+v, want selected=1 cleared=1 published=0 (the draft flag)", res)
	}
	wantFlags(cleanFlags, "after D4")
	if debt() != 0 {
		t.Fatalf("D4: counter = %d, want 0 (the draft clear −1)", debt())
	}
	countersVerifyEmpty(t, pool, "after D4 (the draft clear −1)")

	// D5 the keep: the record content is unreadable (a transient
	// storage failure) — the pass keeps the "bit × counter" pair until
	// the next tick; a tick with a readable storage closes the debt by
	// the usual D3.
	armFull()
	episode(rqBody)
	wantFlags(debtFlags, "after re-arming D5")
	if debt() != 1 {
		t.Fatalf("re-arming D5: counter = %d, want 1", debt())
	}
	drain()
	res, err = httpworker.RequeueOnce(ctx, pool, renderBitFailContent{
		err: errors.New("transient storage failure (the scenario)"),
	}, renderQ, httpworker.RequeueOptions{})
	if err != nil {
		t.Fatalf("D5: RequeueOnce: %v", err)
	}
	if res.Selected != 1 || res.Cleared != 0 || res.Published != 0 {
		t.Fatalf("D5: outcome %+v, want selected=1 cleared=0 published=0 (keep)", res)
	}
	wantFlags(debtFlags, "after D5")
	if debt() != 1 {
		t.Fatalf("D5: counter = %d, want 1 (the pair kept)", debt())
	}
	countersVerifyEmpty(t, pool, "after D5 (keep — no delta)")

	res, err = httpworker.RequeueOnce(ctx, pool, store, renderQ, httpworker.RequeueOptions{})
	if err != nil {
		t.Fatalf("closing D5: RequeueOnce: %v", err)
	}
	if res.Selected != 1 || res.Cleared != 1 || res.Published != 1 {
		t.Fatalf("closing D5: outcome %+v, want selected=1 cleared=1 published=1", res)
	}
	if debt() != 0 {
		t.Fatalf("closing D5: counter = %d, want 0", debt())
	}
	countersVerifyEmpty(t, pool, "after closing D5 (the clear −1)")

	// D6: a delete-file clears the bit by the same status transition
	// transaction and decrements the counter (T-186): the deleted
	// domain will not pass the pass selection anymore, a bit without an
	// owner does not hold the debt; is_idn is preserved; a repeated
	// batch apply (at-least-once) does not double the −1 (the status
	// guard; the full batch — TestCounters_DeleteWithRenderPending).
	full() // the queue holds the message published by the D5 closure
	episode(rqBody)
	wantFlags(debtFlags, "after re-arming D6")
	if debt() != 1 {
		t.Fatalf("re-arming D6: counter = %d, want 1", debt())
	}
	batch := mustBatchID(t)
	if _, err := pool.Exec(ctx,
		"INSERT INTO ingest_stage (batch_id, zone_id, label, action, line_no) VALUES ($1, $2, $3, $4, 1)",
		batch[:], zone, "spec0", int16(model.ActionDelete)); err != nil {
		t.Fatalf("staging delete: %v", err)
	}
	dres, err := ingest.ApplyDelete(ctx, pool, ingest.Batch{ID: batch, ZoneID: zone, Action: model.ActionDelete})
	if err != nil || dres.Deleted != 1 || dres.NotFound != 0 {
		t.Fatalf("D6: ApplyDelete = %+v err=%v, want {1 0}", dres, err)
	}
	wantFlags(cleanFlags, "after D6")
	if debt() != 0 {
		t.Fatalf("D6: counter = %d, want 0 (the delete decrement −1)", debt())
	}
	if r := checkStateOf(ctx, t, pool, zone, id); r.status != int16(model.StatusDeleted) {
		t.Fatalf("after D6: status = %d, want deleted", r.status)
	}
	countersVerifyEmpty(t, pool, "after D6 (the delete-file clear −1)")
}

// --- the 2026-11-08 incident (T-269) ----------------------------------------
//
// The investigation of the "−36 in one pass" desync from prod-reports/
// 2026-11-08-010800.md: clearing 18 flags by the rework pass gave
// −18, an expected 0, and the next guard tick already read −18 — a
// "silent" (the counter row's updated_at did not move) second −18 on
// top of the single observed set of clearings. The operator's
// suspicion — the monitor reconciliation (T-178) with a "stale
// expectation". The audit outcome of all the writers of
// crawler_counters['render_pending']:
//
//   - httpworker (renderq.go) — the ±1 deltas in a pair with the bit,
//     the RowsAffected guards (D1–D8, the tests above) — a double −1 is
//     impossible;
//   - monitor (zonestats.go, recountCountersSQL) — an ABSOLUTE upsert
//     with the aggregate value (value = EXCLUDED.value), it does not
//     write deltas; every rewrite moves updated_at — it cannot leave a
//     "silent" trace. The real race is different: the value is computed
//     by a snapshot of the pass's early operator (the aggregate) but
//     applied by the late one (the upsert) — the concurrent transitions
//     in the window are overwritten by the stale absolute (a false
//     debt/a false zero), transiently, it self-heals by the next pass —
//     reproduced by the refutation test below;
//   - ingest (apply_delete.go, T-186 before the fix) — the ONLY
//     "silent" delta writer with a snapshot: the CTE rd decremented
//     −count(had_render) by the SNAPSHOT pre, while the bit clearing
//     was done by a CASE over the CURRENT row version. A finish-up
//     pass that cleared the bits between the snapshot and the lock was
//     already doing −1 each — rd added a second −n by the snapshot:
//     exactly the −36 of the incident with the bits 0 and the counter
//     −18.
//
// The T-269 fix: the delete apply — two operators of one transaction
// (the status transition; the guarded "bit clearing × −count(fact)"
// pair by the bit-row keys), the snapshot window is closed by the
// current-version guard (mirroring D7). Below — the reproduction of the
// 11-08 scenario (phase 1: the T-186 operator verbatim, BEFORE the fix
// — it must give −18; phase 2: the same interleaving with the fix — the
// invariant |counter − count(bits)| = 0 on every step) and the
// refutation of the reconciliation hypothesis.

// renderBitLegacyDeleteSQL — the delete apply BEFORE T-269 (the T-186
// operator verbatim, the literals expanded): the snapshot had_render
// from pre feeds the CTE rd, the bit clearing — a CASE of the current
// version in moved. The incident reproducibility contract: the desync
// shows up only in THIS operator shape — a literal divergence from the
// code history would be noticed by a text review, the live operator in
// internal/ingest/apply_delete.go no longer contains it.
const renderBitLegacyDeleteSQL = `WITH pre AS MATERIALIZED (
	SELECT d.zone_id, d.label, (d.flags & 2) <> 0 AS had_render
	FROM ingest_stage AS s
	JOIN domains AS d ON d.zone_id = s.zone_id AND d.label = s.label
	WHERE s.batch_id = $1
	  AND s.action = $3
	  AND d.status <> $2
), moved AS (
	UPDATE domains AS d
	SET status = $2,
	    flags = CASE WHEN (d.flags & 2) <> 0
	                 THEN (d.flags & -3)::int2
	                 ELSE d.flags END
	FROM pre AS p
	WHERE d.zone_id = p.zone_id
	  AND d.label = p.label
	  AND d.status <> $2
	RETURNING d.zone_id, d.label, d.check_state, p.had_render
), zc AS (
	UPDATE zones AS z
	SET domains_total = z.domains_total - c.n_total,
	    domains_pending = z.domains_pending - c.n_pending,
	    domains_queued = z.domains_queued - c.n_queued,
	    domains_checked = z.domains_checked - c.n_checked
	FROM (SELECT zone_id,
	             count(*) AS n_total,
	             count(*) FILTER (WHERE check_state = 0) AS n_pending,
	             count(*) FILTER (WHERE check_state = 1) AS n_queued,
	             count(*) FILTER (WHERE check_state = 2) AS n_checked
	      FROM moved GROUP BY zone_id) AS c
	WHERE z.id = c.zone_id
), rd AS (
	INSERT INTO crawler_counters (name, value)
	SELECT 'render_pending', -count(*) FROM moved WHERE had_render
	HAVING count(*) > 0
	ON CONFLICT (name) DO UPDATE
	SET value = crawler_counters.value + EXCLUDED.value
)
SELECT
	(SELECT count(*) FROM ingest_stage AS s
	WHERE s.batch_id = $1 AND s.action = $3
	  AND (EXISTS (SELECT 1 FROM moved AS m
	               WHERE m.zone_id = s.zone_id AND m.label = s.label)
	       OR EXISTS (SELECT 1 FROM domains AS d2
	                  WHERE d2.zone_id = s.zone_id AND d2.label = s.label
	                    AND d2.status = $2))) AS deleted,
	(SELECT count(*) FROM ingest_stage AS s
	WHERE s.batch_id = $1 AND s.action = $3
	  AND NOT EXISTS (SELECT 1 FROM domains AS d
	                  WHERE d.zone_id = s.zone_id AND d.label = s.label)) AS not_found`

// renderBitClearPairSQL — the "rework pass" of the scenario: the
// bit clearing in a pair with the counter per the runbooks §6 sample
// (the current-version guard, the delta by the fact of the cleared)
// as a batch by the keys. In the reproduction it plays the role of the
// T-066 pass from the blocker transaction: a real RequeueOnce would
// lock the counter row bypassing the controlled interleaving order,
// and the D3 transition semantics (the publication was done in
// advance — the content seeding is not part of the counter race) is
// the same here: the bit → 0 and −1 per each fact.
const renderBitClearPairSQL = `WITH cleared AS (
	UPDATE domains SET flags = (flags & -3)::int2
	WHERE zone_id = $1 AND id = ANY($2) AND (flags & 2) <> 0
	RETURNING 1
)
UPDATE crawler_counters
SET value = value - (SELECT count(*) FROM cleared)
WHERE name = 'render_pending' AND (SELECT count(*) FROM cleared) > 0`

// renderBitSeedDebt — the "11:48:04" state of the incident: n live
// domains with the render_pending bit and the counter n (a manual §6
// installation atomically with the counter), the zones/statuses are
// aligned by the reconciliation (the reference point of the
// cross-check). Returns the labels in the id order.
func renderBitSeedDebt(ctx context.Context, t *testing.T, pool *pgxpool.Pool, zone int16, prefix string, n int) []string {
	t.Helper()
	ids := countersSeedDomains(t, pool, zone, prefix, n, model.CheckStateDone, model.StatusAlive)
	for _, id := range ids {
		if _, err := pool.Exec(ctx,
			"UPDATE domains SET flags = flags | $1 WHERE zone_id = $2 AND id = $3",
			int16(model.FlagRenderPending), zone, id); err != nil {
			t.Fatalf("domain %d bit: %v", id, err)
		}
	}
	labels := make([]string, len(ids))
	for i := range ids {
		labels[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	if _, err := pool.Exec(ctx,
		"INSERT INTO crawler_counters (name, value) VALUES ('render_pending', $1) "+
			"ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value", int64(n)); err != nil {
		t.Fatalf("the debt counter: %v", err)
	}
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("the baseline reconciliation: %v", err)
	}
	return labels
}

// renderBitStageDelete — a delete batch by the labels into staging.
func renderBitStageDelete(ctx context.Context, t *testing.T, pool *pgxpool.Pool, zone int16, labels []string) ingest.BatchID {
	t.Helper()
	batch := mustBatchID(t)
	for i, label := range labels {
		if _, err := pool.Exec(ctx,
			"INSERT INTO ingest_stage (batch_id, zone_id, label, action, line_no) VALUES ($1, $2, $3, $4, $5)",
			batch[:], zone, label, int16(model.ActionDelete), int16(i+1)); err != nil {
			t.Fatalf("staging %s: %v", label, err)
		}
	}
	return batch
}

// renderBitBitCount — the number of rows with the bit by the keys (the
// pairing fact).
func renderBitBitCount(ctx context.Context, t *testing.T, pool *pgxpool.Pool, zone int16, ids []int64) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM domains WHERE zone_id = $1 AND id = ANY($2) AND (flags & $3) <> 0",
		zone, ids, int16(model.FlagRenderPending)).Scan(&n); err != nil {
		t.Fatalf("counting the bits: %v", err)
	}
	return n
}

// renderBitWaitBlocked — waits until the competitor goroutine blocks
// exactly on the blocker transaction xid (pg_locks: a waiting
// transactionid with the target xid; the target defines the wait
// SPECIFICALLY — a global "some wait exists" predicate would catch
// foreign packets of the parallel test-integration, review T-269
// P2-2) — the point of the deterministic interleaving. A goroutine
// error before the blocking surfaces immediately.
func renderBitWaitBlocked(t *testing.T, pool *pgxpool.Pool, errc chan error, xid, what string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		select {
		case err := <-errc:
			t.Fatalf("%s: the competitor failed before blocking: %v", what, err)
		default:
		}
		var n int
		err := pool.QueryRow(context.Background(),
			"SELECT count(*) FROM pg_locks WHERE locktype = 'transactionid' AND granted = false AND transactionid = $1::xid",
			xid).Scan(&n)
		if err == nil && n > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: the competitor did not block within 15 s (pg_locks: err=%v n=%d)", what, err, n)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// renderBitXID — the transaction identifier of the blocker (txid_current
// assigns an xid if the record has not assigned one yet) — the wait
// target.
func renderBitXID(ctx context.Context, t *testing.T, tx pgx.Tx) string {
	t.Helper()
	var xid string
	if err := tx.QueryRow(ctx, "SELECT txid_current()::text").Scan(&xid); err != nil {
		t.Fatalf("txid_current: %v", err)
	}
	return xid
}

// renderBitInTxDebt — the debt counter from the open pass transaction
// (its own writes are visible, the competitor is still blocked — a
// read without racing with its commit, review T-269 P2-1).
func renderBitInTxDebt(ctx context.Context, t *testing.T, tx pgx.Tx) int64 {
	t.Helper()
	var n int64
	if err := tx.QueryRow(ctx,
		"SELECT value FROM crawler_counters WHERE name = 'render_pending'").Scan(&n); err != nil {
		t.Fatalf("the debt counter in the transaction: %v", err)
	}
	return n
}

// renderBitInTxBits — the number of rows with the bit by the keys from
// the open pass transaction (see renderBitInTxDebt).
func renderBitInTxBits(ctx context.Context, t *testing.T, tx pgx.Tx, zone int16, ids []int64) int64 {
	t.Helper()
	var n int64
	if err := tx.QueryRow(ctx,
		"SELECT count(*) FROM domains WHERE zone_id = $1 AND id = ANY($2) AND (flags & $3) <> 0",
		zone, ids, int16(model.FlagRenderPending)).Scan(&n); err != nil {
		t.Fatalf("counting the bits in the transaction: %v", err)
	}
	return n
}

// renderBitJoinErr — the result of the competitor goroutine (one error).
func renderBitJoinErr(t *testing.T, errc chan error, what string) {
	t.Helper()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("%s: did not finish within 30 s", what)
	}
}

// TestRenderPendingBit_DeleteClearRace — the reproduction of the
// 2026-11-08 scenario: in parallel, the flag clearing by the finish-up
// pass × the delete-file apply over the same domains. Phase 1 (the
// T-186 operator before the fix, verbatim): the pass clears the bits
// between the pre snapshot and the moved lock — rd decrements −18 by
// the snapshot SECOND, the counter goes to −18 with a zero bit (the
// total shift −36 in one "pass", the counter's updated_at is silent —
// the delta writer does not write it). Phase 2 (after the T-269 fix):
// the same interleaving — the status transition does not touch the
// bit, the "clearing × decrement" pair goes by the transition fact
// with the current-version guard, the invariant |counter −
// count(bits)| = 0 holds on every step. Phase 2b — the mirrored order
// (the apply won the race): the pair is executed by the apply, the
// pairing is the same.
func TestRenderPendingBit_DeleteClearRace(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const n = 18 // the incident scale: 18 domains / 18 flags / a −36 shift
	zone := countersSeedZone(t, pool, "fsm-delrace.test")
	raceIDs := renderBitIDsOfLabels(ctx, t, pool, zone, renderBitSeedDebt(ctx, t, pool, zone, "race", n))

	// --- Phase 1: BEFORE the fix — the −36 reproduction -------------------
	batch := renderBitStageDelete(ctx, t, pool, zone, renderBitLabels("race", n))
	if debt := renderBitDebt(ctx, t, pool); debt != n {
		t.Fatalf("phase 1: baseline counter = %d, want %d", debt, n)
	}

	// The "rework pass" (L): clears all 18 by the pair and KEEPS the
	// transaction uncommitted — the rows are locked, the changes are
	// invisible.
	lx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("the pass transaction: %v", err)
	}
	if _, err := lx.Exec(ctx, renderBitClearPairSQL, zone, raceIDs); err != nil {
		t.Fatalf("the pass: clearing by the pair: %v", err)
	}
	xid := renderBitXID(ctx, t, lx)

	// The delete apply (D) by the T-186 operator: the pre snapshot sees
	// the bits (the L clearing is uncommitted), moved locks on the L
	// rows.
	errc := make(chan error, 1)
	go func() {
		rows, err := pool.Query(ctx, renderBitLegacyDeleteSQL, batch[:], int16(model.StatusDeleted), int16(model.ActionDelete))
		if err != nil {
			errc <- err
			return
		}
		defer rows.Close()
		var deleted, notFound int64
		if !rows.Next() {
			errc <- fmt.Errorf("legacy apply: the outcome not returned: %w", rows.Err())
			return
		}
		if err := rows.Scan(&deleted, &notFound); err != nil {
			errc <- fmt.Errorf("legacy apply: reading the outcome: %w", err)
			return
		}
		if err := rows.Err(); err != nil {
			errc <- err
			return
		}
		if deleted != n || notFound != 0 {
			errc <- fmt.Errorf("legacy apply: the outcome {deleted %d not_found %d}, want {%d 0}", deleted, notFound, n)
			return
		}
		errc <- nil
	}()
	renderBitWaitBlocked(t, pool, errc, xid, "phase 1: the apply")

	// The "after the pass" step — inside the pass transaction (its own
	// writes are visible, the competitor is still blocked): the bits 0,
	// the counter 0.
	if debt := renderBitInTxDebt(ctx, t, lx); debt != 0 {
		t.Fatalf("phase 1: after the pass counter = %d, want 0", debt)
	}
	if bits := renderBitInTxBits(ctx, t, lx, zone, raceIDs); bits != 0 {
		t.Fatalf("phase 1: after the pass bit = %d, want 0", bits)
	}
	// The pass commit releases D — as at 11:50:39.982; there are no
	// intermediate reads from another session anymore (a race with the
	// D commit, review T-269 P2-1).
	if err := lx.Commit(ctx); err != nil {
		t.Fatalf("committing the pass: %v", err)
	}

	// D continues on the committed version: the CASE sees the bit
	// cleared, rd still decrements −18 by the snapshot — the counter
	// −18.
	renderBitJoinErr(t, errc, "phase 1: the apply")
	if debt := renderBitDebt(ctx, t, pool); debt != -n {
		t.Fatalf("phase 1: REPRODUCTION: counter = %d, want −%d (a second −n by the snapshot, the total shift −%d)", debt, n, 2*n)
	}
	if bits := renderBitBitCount(ctx, t, pool, zone, raceIDs); bits != 0 {
		t.Fatalf("phase 1: bits = %d, want 0 (the debt closed by the pass)", bits)
	}
	// The pairing is broken exactly by −n: the cross-check sees the
	// position.
	mismatch, err := monitor.VerifyCounters(ctx, pool, monitor.VerifyOptions{})
	if err != nil {
		t.Fatalf("phase 1: VerifyCounters: %v", err)
	}
	if len(mismatch) != 1 || mismatch[0].Counter != "global:render_pending" ||
		mismatch[0].Old != -n || mismatch[0].New != 0 {
		t.Fatalf("phase 1: desync = %+v, want [global:render_pending −%d → 0]", mismatch, n)
	}

	// The operator-level leveling ~11:59 (runbooks §6): the counter :=
	// the fact.
	if _, err := pool.Exec(ctx,
		"UPDATE crawler_counters SET value = $1 WHERE name = 'render_pending'", int64(0)); err != nil {
		t.Fatalf("leveling: %v", err)
	}

	// --- Phase 2: AFTER the fix — the same interleaving, the invariant on every step
	fixIDs := renderBitIDsOfLabels(ctx, t, pool, zone, renderBitSeedDebt(ctx, t, pool, zone, "fix", n))
	batch2 := renderBitStageDelete(ctx, t, pool, zone, renderBitLabels("fix", n))
	if debt := renderBitDebt(ctx, t, pool); debt != n {
		t.Fatalf("phase 2: baseline counter = %d, want %d", debt, n)
	}

	lx2, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("phase 2: the pass transaction: %v", err)
	}
	if _, err := lx2.Exec(ctx, renderBitClearPairSQL, zone, fixIDs); err != nil {
		t.Fatalf("phase 2: the pass: clearing by the pair: %v", err)
	}
	xid2 := renderBitXID(ctx, t, lx2)

	resc := make(chan ingest.DeleteResult, 1)
	errc2 := make(chan error, 1)
	go func() {
		res, err := ingest.ApplyDelete(ctx, pool, ingest.Batch{ID: batch2, ZoneID: zone, Action: model.ActionDelete})
		resc <- res
		errc2 <- err
	}()
	renderBitWaitBlocked(t, pool, errc2, xid2, "phase 2: the apply")

	// The "after the pass" step — inside the pass transaction: the
	// invariant 0.
	if debt := renderBitInTxDebt(ctx, t, lx2); debt != 0 {
		t.Fatalf("phase 2: after the pass counter = %d, want 0", debt)
	}
	if bits := renderBitInTxBits(ctx, t, lx2, zone, fixIDs); bits != 0 {
		t.Fatalf("phase 2: after the pass bits = %d, want 0", bits)
	}
	if err := lx2.Commit(ctx); err != nil {
		t.Fatalf("phase 2: committing the pass: %v", err)
	}

	renderBitJoinErr(t, errc2, "phase 2: the apply")
	var res ingest.DeleteResult
	select {
	case res = <-resc:
	default:
		t.Fatal("phase 2: the apply outcome not returned")
	}
	if res.Deleted != n || res.NotFound != 0 {
		t.Fatalf("phase 2: ApplyDelete = %+v, want deleted %d", res, n)
	}
	// The "after the apply" step: the status transition landed, the bit
	// was already cleared by the pass — the pair did NOT run (no keys),
	// the counter is untouched.
	if debt := renderBitDebt(ctx, t, pool); debt != 0 {
		t.Fatalf("phase 2: AFTER THE FIX counter = %d, want 0 (the invariant holds)", debt)
	}
	if bits := renderBitBitCount(ctx, t, pool, zone, fixIDs); bits != 0 {
		t.Fatalf("phase 2: bits = %d, want 0", bits)
	}
	for i, id := range fixIDs {
		if r := checkStateOf(ctx, t, pool, zone, id); r.status != int16(model.StatusDeleted) {
			t.Fatalf("phase 2: the fix%d domain: status = %d, want deleted", i, r.status)
		}
	}
	countersVerifyEmpty(t, pool, "phase 2 (the pass × the apply, after the fix)")

	// --- Phase 2b: the apply wins the race (the bit is set) — the pair by the fact
	winIDs := renderBitIDsOfLabels(ctx, t, pool, zone, renderBitSeedDebt(ctx, t, pool, zone, "win", 1))
	batch3 := renderBitStageDelete(ctx, t, pool, zone, renderBitLabels("win", 1))

	res3, err := ingest.ApplyDelete(ctx, pool, ingest.Batch{ID: batch3, ZoneID: zone, Action: model.ActionDelete})
	if err != nil || res3.Deleted != 1 {
		t.Fatalf("phase 2b: ApplyDelete = %+v err=%v, want deleted 1", res3, err)
	}
	if bits := renderBitBitCount(ctx, t, pool, zone, winIDs); bits != 0 {
		t.Fatalf("phase 2b: the bit not cleared by the apply (%d)", bits)
	}
	if debt := renderBitDebt(ctx, t, pool); debt != 0 {
		t.Fatalf("phase 2b: counter = %d, want 0 (the pair −1 by the fact)", debt)
	}
	countersVerifyEmpty(t, pool, "phase 2b (the apply without the pass)")
}

// renderBitLabels — the labels prefix0..prefix(n−1) in the seed order.
func renderBitLabels(prefix string, n int) []string {
	labels := make([]string, n)
	for i := range labels {
		labels[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return labels
}

// renderBitIDsOfLabels — the domain ids by the labels (the seed order).
func renderBitIDsOfLabels(ctx context.Context, t *testing.T, pool *pgxpool.Pool, zone int16, labels []string) []int64 {
	t.Helper()
	ids := make([]int64, len(labels))
	for i, label := range labels {
		if err := pool.QueryRow(ctx,
			"SELECT id FROM domains WHERE zone_id = $1 AND label = $2", zone, label).Scan(&ids[i]); err != nil {
			t.Fatalf("the %s domain id: %v", label, err)
		}
	}
	return ids
}

// TestRenderPendingBit_ReconciliationStaleSnapshot — the refutation of
// the incident hypothesis: "the monitor reconciliation applies a delta
// from the stale expectation — a second −18". A reconciliation pass
// with a stale snapshot (the aggregate saw the bits BEFORE the
// clearing by the rework pass, the upsert executed AFTER) writes
// the stale ABSOLUTE with the aggregate value, not a delta: the
// counter becomes +18 with a zero bit (a false debt), NOT −18; the
// rewrite moves updated_at (a "loud" trace — the reconciliation cannot
// leave a "silent" second −n) and self-heals by the next pass (the
// pass is the authority, ≤ zone_stats_interval). The drift of the pass
// itself is zero here (the old = the new = 18): the pairing desync is
// caught by VerifyCounters, not by the drift gauge.
func TestRenderPendingBit_ReconciliationStaleSnapshot(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const n = 18
	zone := countersSeedZone(t, pool, "fsm-monrace.test")
	ids := renderBitIDsOfLabels(ctx, t, pool, zone, renderBitSeedDebt(ctx, t, pool, zone, "mon", n))
	if debt := renderBitDebt(ctx, t, pool); debt != n {
		t.Fatalf("baseline counter = %d, want %d", debt, n)
	}
	// The "manual operation" mark: updated_at into the past — after the
	// race it must shift (the loudness of the rewrite).
	if _, err := pool.Exec(ctx,
		"UPDATE crawler_counters SET updated_at = now() - interval '1 hour' WHERE name = 'render_pending'"); err != nil {
		t.Fatalf("the updated_at mark: %v", err)
	}
	var t0 time.Time
	if err := pool.QueryRow(ctx,
		"SELECT updated_at FROM crawler_counters WHERE name = 'render_pending'").Scan(&t0); err != nil {
		t.Fatalf("reading updated_at: %v", err)
	}

	// L holds the counter row — the reconciliation upsert will block
	// ABOVE the snapshot (the aggregate has already counted 18 bits),
	// the rework pass will have time to close the debt.
	lx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("the blocker transaction: %v", err)
	}
	if _, err := lx.Exec(ctx,
		"SELECT value FROM crawler_counters WHERE name = 'render_pending' FOR UPDATE"); err != nil {
		t.Fatalf("locking the counter row: %v", err)
	}
	xid := renderBitXID(ctx, t, lx)

	// M — a real reconciliation pass (T-178): the aggregate sees 18
	// bits, the upsert (the value as an ABSOLUTE from the snapshot)
	// blocks on L.
	resc := make(chan monitor.ZoneStatsResult, 1)
	errc := make(chan error, 1)
	go func() {
		res, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{})
		resc <- res
		errc <- err
	}()
	renderBitWaitBlocked(t, pool, errc, xid, "reconciliation")

	// The "rework pass" inside L: the bits → 0, the counter −18 (L
	// already holds the row — the decrement without waiting). The "debt
	// closed" step — inside the transaction (its own writes are
	// visible, M is still blocked): 0/0.
	if _, err := lx.Exec(ctx, renderBitClearPairSQL, zone, ids); err != nil {
		t.Fatalf("the pass: clearing by the pair: %v", err)
	}
	if debt := renderBitInTxDebt(ctx, t, lx); debt != 0 {
		t.Fatalf("after the pass counter = %d, want 0", debt)
	}
	if bits := renderBitInTxBits(ctx, t, lx, zone, ids); bits != 0 {
		t.Fatalf("after the pass bits = %d, want 0", bits)
	}
	if err := lx.Commit(ctx); err != nil {
		t.Fatalf("committing the pass: %v", err)
	}

	// M is released and applies the stale ABSOLUTE: +18 with a zero bit
	// — NOT −18 (no delta from the expectation), updated_at shifted
	// (loudly).
	renderBitJoinErr(t, errc, "reconciliation")
	var mres monitor.ZoneStatsResult
	select {
	case mres = <-resc:
	default:
		t.Fatal("the reconciliation outcome not returned")
	}
	// The drift of the pass itself — zero (the counter old = the new =
	// 18): the crawler_counters_drift gauge does not measure the
	// pairing, only the cross-check below sees the pairing break.
	sawPos := false
	for _, p := range mres.Positions {
		if p.Counter != "global:render_pending" {
			continue
		}
		sawPos = true
		if p.Old != n || p.New != n || p.Drift() != 0 {
			t.Fatalf("the pass drift = %+v, want old=new=%d drift 0 (the drift is a counter about counters, it does not see the pairing)", p, n)
		}
	}
	if !sawPos {
		t.Fatal("the global:render_pending position missing from the pass outcome")
	}
	if debt := renderBitDebt(ctx, t, pool); debt != n {
		t.Fatalf("REFUTATION: the reconciliation with a stale snapshot left the counter %d, want +%d (the stale ABSOLUTE; a \"second −18\" does not exist)", debt, n)
	}
	if bits := renderBitBitCount(ctx, t, pool, zone, ids); bits != 0 {
		t.Fatalf("bits = %d, want 0", bits)
	}
	var at time.Time
	if err := pool.QueryRow(ctx,
		"SELECT updated_at FROM crawler_counters WHERE name = 'render_pending'").Scan(&at); err != nil {
		t.Fatalf("reading updated_at: %v", err)
	}
	if !at.After(t0) {
		t.Fatalf("updated_at not moved (%v ≤ %v): the reconciliation rewrite must be \"loud\" — it does not write a silent delta", at, t0)
	}

	// The pairing is broken transiently (+18/0) — the cross-check sees
	// it; the drift of the pass itself is zero (the old = the new =
	// 18): the drift gauge does not measure the pairing, the next pass
	// heals it (the pass is the authority).
	mismatch, err := monitor.VerifyCounters(ctx, pool, monitor.VerifyOptions{})
	if err != nil {
		t.Fatalf("VerifyCounters: %v", err)
	}
	if len(mismatch) != 1 || mismatch[0].Counter != "global:render_pending" ||
		mismatch[0].Old != n || mismatch[0].New != 0 {
		t.Fatalf("desync = %+v, want [global:render_pending %d → 0]", mismatch, n)
	}

	// Self-healing: the next pass writes a fresh absolute (no bits).
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("the healing pass: %v", err)
	}
	if debt := renderBitDebt(ctx, t, pool); debt != 0 {
		t.Fatalf("after the next pass counter = %d, want 0 (self-healing ≤ zone_stats_interval)", debt)
	}
	countersVerifyEmpty(t, pool, "after the reconciliation self-healing")
}

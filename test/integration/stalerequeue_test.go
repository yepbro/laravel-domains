// Integration test of the "stale status" compensator (T-270, TZ-fsm
// spec tasks/TZ-fsm.md §3 FR-2.2) against the live PostgreSQL and
// Redis testbed: a complete loss-class scenario — O6 is recorded
// (DNS "has addresses", the q:http task is published to the live
// Redis), the mark is aged out beyond the 24 h window, the undelivered
// q:http record is lost (XTRIM — a Redis restart without persistence
// loses the same way, docs/redis.md §5) — reconciliation raises the
// new_done_stale level (gauge), StaleRequeueOnce returns the domain to
// pending (the R15 transition of the check_state specification, zone
// deltas are handled by the transition operator, the
// crawler_scheduler_stale_requeued_total counter grows), a repeated
// pass is a no-op (idempotency), the episode is finished by the
// pipeline NORMALLY (enqueue → the terminal DNS branch), the gauge is
// zeroed. Cost contracts: the guard — a point PK lookup of
// crawler_counters (no seq scan over domains), the batch selection —
// a seq scan over domains with the early LIMIT stop under the
// SET LOCAL pinning (T-191). The call level — library-level, without
// starting services; isolation — withTestSchema and a dedicated Redis
// DB (resilienceRedisEnv).
package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"crawler/internal/dnsworker"
	"crawler/internal/metrics"
	"crawler/internal/model"
	"crawler/internal/monitor"
	"crawler/internal/queue"
	"crawler/internal/scheduler"
)

// TestStaleRequeue_LostHTTPTask — a scenario of losing a q:http task
// with done recorded: the gauge sees the cohort, the compensator
// returns the domain to pending, the pipeline finishes the episode,
// the gauge is zeroed.
func TestStaleRequeue_LostHTTPTask(t *testing.T) {
	skipUnlessIntegration(t)
	pool := countersSchema(t)
	rdb := resilienceRedisEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	zone := countersSeedZone(t, pool, "stale.test")
	q := &countersStubQueue{}

	// R10 + R1: insert and enqueue — the domain is queued for the DNS stage.
	add := checkStateStageAdd(ctx, t, pool, zone, "lost")
	if add.Inserted != 1 {
		t.Fatalf("ApplyAdd = %+v, хочу Inserted=1", add)
	}
	var id int64
	if err := pool.QueryRow(ctx,
		"SELECT id FROM domains WHERE zone_id = $1 AND label = 'lost'", zone,
	).Scan(&id); err != nil {
		t.Fatalf("id вставленного: %v", err)
	}
	if _, err := scheduler.EnqueueBatch(ctx, pool, q, []scheduler.DispatchRow{
		{ZoneID: zone, DomainID: id, Name: "lost.stale.test"},
	}); err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}

	// O6: the DNS episode "has addresses" — publishing q:http to the
	// LIVE Redis, setting the http_published bit (T-108, worker order:
	// publication → bit → result record), recording the keep-status
	// branch: check_state = done, the status stays new (the HTTP stage
	// decides the outcome).
	httpq := queue.NewRedisQueue(rdb, queue.RedisOptions{})
	task := queue.HTTPTask{
		TaskID:     "stale-requeue-test",
		ZoneID:     zone,
		DomainID:   id,
		Name:       "lost.stale.test",
		Subdomains: []queue.HTTPSubdomain{{SubdomainID: 1, Label: "lost", Addrs: []string{"127.0.0.1"}}},
	}
	if _, err := httpq.Publish(ctx, queue.StreamHTTP, task); err != nil {
		t.Fatalf("публикация q:http: %v", err)
	}
	if n := streamLen(t, rdb, queue.StreamHTTP); n != 1 {
		t.Fatalf("XLEN q:http = %d, хочу 1 — задача опубликована", n)
	}
	if _, err := dnsworker.MarkHTTPPublished(ctx, pool, zone, id); err != nil {
		t.Fatalf("MarkHTTPPublished: %v", err)
	}
	ok, err := dnsworker.RecordEpisode(ctx, pool, zone, id, dnsworker.EpisodeResult{Mask: 1}, nil)
	if err != nil || !ok {
		t.Fatalf("RecordEpisode: ok=%v err=%v (О6)", ok, err)
	}
	r := checkStateOf(ctx, t, pool, zone, id)
	if r.state != int16(model.CheckStateDone) || r.status != int16(model.StatusNew) || r.httpPublished {
		t.Fatalf("после О6 = %+v, хочу done/new/бит снят — «статус повис» без HTTP-итога", r)
	}

	// The cohort is aged out beyond the 24 h window (incident week:
	// last_checked_at long ago), the undelivered q:http record is lost
	// — XTRIM of the whole stream (a Redis restart without persistence
	// loses the same).
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET last_checked_at = current_date - 2 WHERE zone_id = $1 AND id = $2",
		zone, id); err != nil {
		t.Fatalf("возраст проверки: %v", err)
	}
	if err := rdb.XTrimMaxLen(ctx, queue.StreamHTTP, 0).Err(); err != nil {
		t.Fatalf("XTRIM q:http (потеря недоставленного): %v", err)
	}
	if n := streamLen(t, rdb, queue.StreamHTTP); n != 0 {
		t.Fatalf("XLEN q:http после потери = %d, хочу 0", n)
	}

	// Reconciliation raises the level: the new_done_stale gauge = 1.
	st, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{})
	if err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	if st.NewDoneStale != 1 {
		t.Fatalf("NewDoneStale = %d, хочу 1 — gauge видит когорту", st.NewDoneStale)
	}
	countersVerifyEmpty(t, pool, "после реконсиляции (уровень new_done_stale)")

	// The compensator (R15): the domain is pending, the status is kept,
	// no lease; zone deltas are handled by the transition operator; the
	// counter grows.
	reg := metrics.New("scheduler")
	res, err := scheduler.StaleRequeueOnce(ctx, pool, scheduler.StaleRequeueOptions{Metrics: reg})
	if err != nil || res.Domains != 1 || res.Selected != 1 {
		t.Fatalf("StaleRequeueOnce = %+v err=%v, хочу Domains=Selected=1 (Р15)", res, err)
	}
	r = checkStateOf(ctx, t, pool, zone, id)
	if r.state != int16(model.CheckStatePending) || r.status != int16(model.StatusNew) ||
		!r.queuedAtNull || !r.lastChecked || r.httpPublished {
		t.Fatalf("после компенсатора = %+v, хочу pending/new/queued_at NULL/отметка есть/бит снят (Р15)", r)
	}
	countersVerifyEmpty(t, pool, "после компенсатора (Р15) — дельты вёл оператор перевода")
	if got := staleRequeuedTotal(t, reg); got != 1 {
		t.Fatalf("счётчик компенсатора = %v, хочу 1", got)
	}

	// A repeated pass is a no-op (idempotency: the transitioned rows are
	// not selected), the counter does not grow.
	repeat, err := scheduler.StaleRequeueOnce(ctx, pool, scheduler.StaleRequeueOptions{Metrics: reg})
	if err != nil || repeat != (scheduler.StaleRequeueResult{}) {
		t.Fatalf("повторный StaleRequeueOnce = %+v err=%v, хочу нулевой (идемпотентность)", repeat, err)
	}
	if got := staleRequeuedTotal(t, reg); got != 1 {
		t.Fatalf("счётчик после повтора = %v, хочу 1 (пустой проход не считается)", got)
	}

	// The episode is finished by the pipeline NORMALLY: enqueue (R1) and
	// the terminal DNS branch (R2) — the domain leaves the anomaly with
	// a final status.
	if _, err := scheduler.EnqueueBatch(ctx, pool, q, []scheduler.DispatchRow{
		{ZoneID: zone, DomainID: id, Name: "lost.stale.test"},
	}); err != nil {
		t.Fatalf("EnqueueBatch (штатный конвейер): %v", err)
	}
	ok, err = dnsworker.RecordEpisode(ctx, pool, zone, id, dnsworker.EpisodeResult{
		SetStatus: true,
		Status:    model.StatusNotDelegated,
	}, nil)
	if err != nil || !ok {
		t.Fatalf("RecordEpisode терминальная: ok=%v err=%v (Р2)", ok, err)
	}
	r = checkStateOf(ctx, t, pool, zone, id)
	if r.state != int16(model.CheckStateDone) || r.status != int16(model.StatusNotDelegated) || !r.queuedAtNull {
		t.Fatalf("после штатного завершения = %+v, хочу done/not_delegated/queued_at NULL (Р2)", r)
	}
	countersVerifyEmpty(t, pool, "после штатного завершения эпизода (Р2)")

	// The gauge is zeroed: the cohort is healed, the signal about new losses is clean.
	if st, err = monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция повторно: %v", err)
	}
	if st.NewDoneStale != 0 {
		t.Fatalf("NewDoneStale после лечения = %d, хочу 0 (gauge обнулён)", st.NewDoneStale)
	}
	var level int64
	if err := pool.QueryRow(ctx,
		"SELECT value FROM crawler_counters WHERE name = 'new_done_stale'",
	).Scan(&level); err != nil || level != 0 {
		t.Fatalf("строка new_done_stale = (%d, %v), хочу 0", level, err)
	}
	countersVerifyEmpty(t, pool, "после обнуления gauge")

	// The selection shape contract under pinning (T-191): SET LOCAL ×3 —
	// a seq scan over domains with the early LIMIT stop (the marathon
	// over domains_recheck_idx does not come back).
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, pin := range []string{
		"SET LOCAL enable_indexscan = off",
		"SET LOCAL enable_bitmapscan = off",
		"SET LOCAL enable_indexonlyscan = off",
	} {
		if _, err := tx.Exec(ctx, pin); err != nil {
			t.Fatalf("%s: %v", pin, err)
		}
	}
	plan := checkStateExplain(ctx, t, tx, "SELECT zone_id, id FROM domains AS d "+
		"WHERE d.status = 0 AND d.check_state = 2 AND d.last_checked_at < now() - interval '24 hours' "+
		"LIMIT 1000")
	if !strings.Contains(plan, "Seq Scan on domains") || !strings.Contains(plan, "Limit") {
		t.Errorf("выборка пачки не seq scan с ранним остановом:\n%s", plan)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	// The guard cost contract: a point PK lookup of crawler_counters —
	// no seq scan over domains (an empty set — a cheap probe); seqscan
	// is disabled so that the tiny counter table does not break the
	// contract by choosing a trivial scan — the
	// TestCheckState_LeaseNullRepair style.
	tx2, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx2.Rollback(ctx) }()
	if _, err := tx2.Exec(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
		t.Fatalf("SET LOCAL enable_seqscan: %v", err)
	}
	plan = checkStateExplain(ctx, t, tx2,
		"SELECT coalesce((SELECT value FROM crawler_counters WHERE name = 'new_done_stale'), 0)")
	if !strings.Contains(plan, "crawler_counters_pkey") {
		t.Errorf("guard не идёт по PK crawler_counters:\n%s", plan)
	}
	if strings.Contains(plan, "domains") {
		t.Errorf("guard трогает domains:\n%s", plan)
	}
}

// staleRequeuedTotal — the compensator counter value in the registry
// (the checkStateNullReturned style).
func staleRequeuedTotal(t *testing.T, reg *metrics.Registry) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != "crawler_scheduler_stale_requeued_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			return m.GetCounter().GetValue()
		}
	}
	return 0
}

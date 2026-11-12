// Integration tests for the manual recheck transitions of the check_state
// FSM spec (T-255, TZ-fsm spec §3 FR-1: rows R11/R12; the single source of
// truth — internal/scheduler/checkstate.go) against live PostgreSQL:
// crawlerctl recheck moves found domains of any check_state (done / queued
// / pending) to pending with a queued_at reset and zone deltas by the old
// state; deleted — a rejection; crawlerctl subdomains recheck moves
// done → pending by draining. The SQL contracts and the move semantics —
// the recheck_test.go/subdomains_test.go unit tests on fakes; here — the
// real operator behavior on a migrated schema (the sample —
// fixtures_import_integration_test.go; the spec requires integration tests
// for ALL transitions, and crawlerctl is package main, not importable from
// test/integration). Run: make test-integration (CRAWLER_INTEGRATION=1,
// the same package list in the Makefile); without the variable the test
// is skipped.
package main

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"crawler/internal/model"
	"crawler/internal/monitor"
)

// TestRecheckSpecTransitionsLive — the R11 transitions (manual recheck:
// done / queued / pending → pending, deleted — a rejection) and R12 (the
// mass recheck of a subdomain: done → pending by draining): queued_at is
// reset, the zone deltas reconcile with the T-178 reconciliation after
// each episode.
func TestRecheckSpecTransitionsLive(t *testing.T) {
	if os.Getenv("CRAWLER_INTEGRATION") != "1" {
		t.Skip("интеграционный тест recheck: запустите make test-integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, cleanup := t243Schema(t)
	defer cleanup()

	var zone int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name, is_active) VALUES ('recheck.test', true) RETURNING id",
	).Scan(&zone); err != nil {
		t.Fatalf("зона: %v", err)
	}
	seed := func(label string, state model.CheckState, status model.Status, queued bool) {
		t.Helper()
		var id int64
		q := "NULL"
		if queued {
			q = "now()"
		}
		if err := pool.QueryRow(ctx,
			`INSERT INTO domains (zone_id, label, check_state, status, queued_at, last_checked_at)
			 VALUES ($1, $2, $3, $4, `+q+`, current_date) RETURNING id`,
			zone, label, int16(state), int16(status)).Scan(&id); err != nil {
			t.Fatalf("домен %s: %v", label, err)
		}
	}
	seed("done", model.CheckStateDone, model.StatusAlive, false)
	seed("queued", model.CheckStateQueued, model.StatusNew, true)
	seed("pending", model.CheckStatePending, model.StatusNew, false)
	seed("del", model.CheckStateDone, model.StatusDeleted, false)

	// A manual INSERT bypassing the deltas: the reconciliation is the authority.
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция: %v", err)
	}
	if mismatch, err := monitor.VerifyCounters(ctx, pool, monitor.VerifyOptions{}); err != nil || len(mismatch) > 0 {
		t.Fatalf("после реконсиляции: mismatch=%v err=%v", mismatch, err)
	}

	// R11: the found ones (done/queued/pending) are moved to pending, del — a rejection.
	var stdout bytes.Buffer
	err := recheckRun(ctx, pool,
		namesOf("done.recheck.test", "queued.recheck.test", "pending.recheck.test", "del.recheck.test"),
		&stdout)
	if err == nil || !strings.Contains(err.Error(), "del.recheck.test") {
		t.Fatalf("ошибка = %v, хочу отказ по del.recheck.test (частичный успех)", err)
	}
	got := stdout.String()
	for name, was := range map[string]string{
		"done.recheck.test":    "done",
		"queued.recheck.test":  "queued",
		"pending.recheck.test": "pending",
	} {
		if !strings.Contains(got, name+": поставлен на проверку (было check_state: "+was+")") {
			t.Errorf("вывод без «%s … было %s»:\n%s", name, was, got)
		}
	}
	if !strings.Contains(got, "del.recheck.test: удалён") {
		t.Errorf("вывод без отказа deleted:\n%s", got)
	}

	states := func() map[string]struct {
		state    int16
		queuedAt bool
	} {
		rows, err := pool.Query(ctx,
			"SELECT label, check_state, queued_at IS NOT NULL FROM domains WHERE zone_id = $1", zone)
		if err != nil {
			t.Fatalf("чтение состояний: %v", err)
		}
		defer rows.Close()
		out := map[string]struct {
			state    int16
			queuedAt bool
		}{}
		for rows.Next() {
			var label string
			var s struct {
				state    int16
				queuedAt bool
			}
			if err := rows.Scan(&label, &s.state, &s.queuedAt); err != nil {
				t.Fatalf("чтение состояний: строка: %v", err)
			}
			out[label] = s
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("чтение состояний: %v", err)
		}
		return out
	}
	for label, want := range map[string]int16{
		"done":    int16(model.CheckStatePending),
		"queued":  int16(model.CheckStatePending),
		"pending": int16(model.CheckStatePending),
		"del":     int16(model.CheckStateDone),
	} {
		if s := states()[label]; s.state != want {
			t.Errorf("домен %s: check_state = %d, хочу %d (Р11)", label, s.state, want)
		}
	}
	for label := range map[string]bool{"done": true, "queued": true, "pending": true} {
		if s := states()[label]; s.queuedAt {
			t.Errorf("домен %s: queued_at не сброшен (Р11)", label)
		}
	}
	if mismatch, err := monitor.VerifyCounters(ctx, pool, monitor.VerifyOptions{}); err != nil || len(mismatch) > 0 {
		t.Fatalf("после ручного recheck: mismatch=%v err=%v (дельты по старым состояниям)", mismatch, err)
	}

	// R12: the mass recheck of a subdomain — done → pending by draining;
	// non-done ones are not touched. Make del a done candidate again? No:
	// deleted is excluded by the guard — we check on the only done.
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET check_state = $1 WHERE zone_id = $2 AND label = 'done'",
		int16(model.CheckStateDone), zone); err != nil {
		t.Fatalf("возврат в done: %v", err)
	}
	// The subdomain reference table is not seeded: the migration 000001
	// seed already contains (1, 'www') with enabled = true —
	// lookupSubdomainLabel will find it.
	// A manual UPDATE bypassing the deltas — the reconciliation aligns
	// the counters before the drain.
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("реконсиляция (перед Р12): %v", err)
	}
	var out2 bytes.Buffer
	if err := subdomainsRecheckRun(ctx, pool, "www", nil, "все зоны", &out2); err != nil {
		t.Fatalf("subdomainsRecheckRun: %v", err)
	}
	if !strings.Contains(out2.String(), "переведено в проверку (pending): 1") {
		t.Errorf("вывод массового recheck без итога 1:\n%s", out2.String())
	}
	if s := states()["done"]; s.state != int16(model.CheckStatePending) {
		t.Errorf("done-домен после массового recheck: check_state = %d, хочу pending (Р12)", s.state)
	}
	if s := states()["pending"]; s.state != int16(model.CheckStatePending) {
		t.Errorf("pending-домен тронут массовым recheck: %+v", s)
	}
	if s := states()["del"]; s.state != int16(model.CheckStateDone) {
		t.Errorf("deleted-домен тронут массовым recheck: %+v (охрана status <> deleted)", s)
	}
	if mismatch, err := monitor.VerifyCounters(ctx, pool, monitor.VerifyOptions{}); err != nil || len(mismatch) > 0 {
		t.Fatalf("после массового recheck: mismatch=%v err=%v", mismatch, err)
	}
}

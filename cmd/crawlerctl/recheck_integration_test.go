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
		t.Skip("the recheck integration test: run make test-integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, cleanup := t243Schema(t)
	defer cleanup()

	var zone int16
	if err := pool.QueryRow(ctx,
		"INSERT INTO zones (name, is_active) VALUES ('recheck.test', true) RETURNING id",
	).Scan(&zone); err != nil {
		t.Fatalf("the zone: %v", err)
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
			t.Fatalf("the domain %s: %v", label, err)
		}
	}
	seed("done", model.CheckStateDone, model.StatusAlive, false)
	seed("queued", model.CheckStateQueued, model.StatusNew, true)
	seed("pending", model.CheckStatePending, model.StatusNew, false)
	seed("del", model.CheckStateDone, model.StatusDeleted, false)

	// A manual INSERT bypassing the deltas: the reconciliation is the authority.
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("the reconciliation: %v", err)
	}
	if mismatch, err := monitor.VerifyCounters(ctx, pool, monitor.VerifyOptions{}); err != nil || len(mismatch) > 0 {
		t.Fatalf("after the reconciliation: mismatch=%v err=%v", mismatch, err)
	}

	// R11: the found ones (done/queued/pending) are moved to pending, del — a rejection.
	var stdout bytes.Buffer
	err := recheckRun(ctx, pool,
		namesOf("done.recheck.test", "queued.recheck.test", "pending.recheck.test", "del.recheck.test"),
		&stdout)
	if err == nil || !strings.Contains(err.Error(), "del.recheck.test") {
		t.Fatalf("err = %v, want a refusal for del.recheck.test (a partial success)", err)
	}
	got := stdout.String()
	for name, was := range map[string]string{
		"done.recheck.test":    "done",
		"queued.recheck.test":  "queued",
		"pending.recheck.test": "pending",
	} {
		if !strings.Contains(got, name+": scheduled for a check (was check_state: "+was+")") {
			t.Errorf("the output without \"%s ... was %s\":\n%s", name, was, got)
		}
	}
	if !strings.Contains(got, "del.recheck.test: deleted") {
		t.Errorf("the output without the deleted refusal:\n%s", got)
	}

	states := func() map[string]struct {
		state    int16
		queuedAt bool
	} {
		rows, err := pool.Query(ctx,
			"SELECT label, check_state, queued_at IS NOT NULL FROM domains WHERE zone_id = $1", zone)
		if err != nil {
			t.Fatalf("reading states: %v", err)
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
				t.Fatalf("reading states: row: %v", err)
			}
			out[label] = s
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("reading states: %v", err)
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
			t.Errorf("domain %s: check_state = %d, want %d (R11)", label, s.state, want)
		}
	}
	for label := range map[string]bool{"done": true, "queued": true, "pending": true} {
		if s := states()[label]; s.queuedAt {
			t.Errorf("domain %s: queued_at is not reset (R11)", label)
		}
	}
	if mismatch, err := monitor.VerifyCounters(ctx, pool, monitor.VerifyOptions{}); err != nil || len(mismatch) > 0 {
		t.Fatalf("after the manual recheck: mismatch=%v err=%v (the deltas over the old states)", mismatch, err)
	}

	// R12: the mass recheck of a subdomain — done → pending by draining;
	// non-done ones are not touched. Make del a done candidate again? No:
	// deleted is excluded by the guard — we check on the only done.
	if _, err := pool.Exec(ctx,
		"UPDATE domains SET check_state = $1 WHERE zone_id = $2 AND label = 'done'",
		int16(model.CheckStateDone), zone); err != nil {
		t.Fatalf("the refund to done: %v", err)
	}
	// The subdomain reference table is not seeded: the migration 000001
	// seed already contains (1, 'www') with enabled = true —
	// lookupSubdomainLabel will find it.
	// A manual UPDATE bypassing the deltas — the reconciliation aligns
	// the counters before the drain.
	if _, err := monitor.ZoneStatsOnce(ctx, pool, monitor.ZoneStatsOptions{}); err != nil {
		t.Fatalf("the reconciliation (before R12): %v", err)
	}
	var out2 bytes.Buffer
	if err := subdomainsRecheckRun(ctx, pool, "www", nil, "all zones", &out2); err != nil {
		t.Fatalf("subdomainsRecheckRun: %v", err)
	}
	if !strings.Contains(out2.String(), "moved to pending: 1") {
		t.Errorf("the bulk recheck output without the total 1:\n%s", out2.String())
	}
	if s := states()["done"]; s.state != int16(model.CheckStatePending) {
		t.Errorf("the done domain after the bulk recheck: check_state = %d, want pending (R12)", s.state)
	}
	if s := states()["pending"]; s.state != int16(model.CheckStatePending) {
		t.Errorf("the pending domain touched by the bulk recheck: %+v", s)
	}
	if s := states()["del"]; s.state != int16(model.CheckStateDone) {
		t.Errorf("the deleted domain touched by the bulk recheck: %+v (the status <> deleted guard)", s)
	}
	if mismatch, err := monitor.VerifyCounters(ctx, pool, monitor.VerifyOptions{}); err != nil || len(mismatch) > 0 {
		t.Fatalf("after the bulk recheck: mismatch=%v err=%v", mismatch, err)
	}
}

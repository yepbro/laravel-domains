// The alert state machine tests (T-264, spec tasks/TZ-fsm.md §3 FR-10): the driver
// of the transition (the single state-writing point) and the table tests
// of the machines "a sequence of observations → a sequence of statuses".
// The sequential tests of the other machines were migrated in place —
// alerts_test.go / badhashes_test.go / fallbackalerts_test.go (the
// expectations are the same); here — the shared observation constructors and the coverage of machines without
// their own sequential test (unit_oom). The observation
// sources (the observe* functions) and the full pass stack are covered
// by the Pass tests of the same files.
package monitor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// --- observation constructors and sequence runs -------------------------------

// obs — a measurement (the source is alive, the observation is valid).
func obs[V any](v V) observation[V] {
	return observation[V]{value: v}
}

// obsErr — a source error (a Warn every pass).
func obsErr[V any](err error) observation[V] {
	return observation[V]{err: err}
}

// obsReason — a quiet "not measured" degradation with a reason (without a Warn).
func obsReason[V any](reason string) observation[V] {
	return observation[V]{reason: reason}
}

// runSequence runs a sequence of observations through the machine and
// returns the sequence of statuses — the format of the FR-10 table tests.
func runSequence[V any](a *Alerts, m *alertMachine[V], list ...observation[V]) []string {
	out := make([]string, 0, len(list))
	for _, o := range list {
		out = append(out, m.transition(a, context.Background(), o).Status)
	}
	return out
}

// wantSequence compares the sequence of statuses with the expected one.
func wantSequence(t *testing.T, name string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d statuses (%v), want %d (%v)", name, len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s: step %d: status = %s, want %s (the sequence: %v)",
				name, i, got[i], want[i], got)
		}
	}
}

// --- the transition driver ----------------------------------------------------

// TestAlertMachineTransition — the single state-writing point
// (the invariants I1/I2/I3 of the specification): a source error — a Warn and "not
// measured" with the reason in detail, the track memory untouched (the
// series survives the failure); a quiet reason — the same without a Warn; a ctx cancellation — without
// a Warn (part of the shutdown); the status is written only by transition and is visible
// to the next pass (the hysteresis/hold read it as prev).
func TestAlertMachineTransition(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	ctx := context.Background()
	m := env.newDeadMachine() // series: the memory is visible in the detail ("N/3")

	// A source error: "not measured" with a Warn; the series is not reset —
	// the next live observation continues the count (not "the first").
	s := m.transition(env.Alerts, ctx, obs(int64(5)))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "1/3") {
		t.Fatalf("the first observation: %s (%s), want ok with the streak 1/3", s.Status, s.Detail)
	}
	before := env.buf.String()
	s = m.transition(env.Alerts, ctx, obsErr[int64](errors.New("connection refused")))
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "connection refused") {
		t.Fatalf("a source error: %s (%s), want not_measured with the reason", s.Status, s.Detail)
	}
	if !strings.Contains(env.buf.String()[len(before):], "level=WARN") ||
		!strings.Contains(env.buf.String()[len(before):], "connection refused") {
		t.Errorf("a source error without a Warn:\n%s", env.buf.String()[len(before):])
	}
	s = m.transition(env.Alerts, ctx, obs(int64(5)))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "2/3") {
		t.Fatalf("after the failure: %s (%s), want ok with the streak 2/3 (the memory survived the degradation)", s.Status, s.Detail)
	}

	// A quiet reason — "not measured" without a Warn; the series, as with
	// an error, is not reset (a degradation does not touch the memory).
	env2 := newAlertsTestEnv(t, nil)
	m2 := env2.newDeadMachine()
	_ = m2.transition(env2.Alerts, ctx, obs(int64(5))) // series 1/3
	before = env2.buf.String()
	s = m2.transition(env2.Alerts, ctx, obsReason[int64]("Redis not set"))
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "Redis not set") {
		t.Fatalf("a quiet reason: %s (%s), want not_measured with the reason", s.Status, s.Detail)
	}
	if strings.Contains(env2.buf.String()[len(before):], "level=WARN") {
		t.Errorf("a quiet reason produced a Warn:\n%s", env2.buf.String()[len(before):])
	}
	s = m2.transition(env2.Alerts, ctx, obs(int64(5)))
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "2/3") {
		t.Fatalf("after the quiet reason: %s (%s), want ok with the streak 2/3 (the memory survived)", s.Status, s.Detail)
	}

	// A ctx cancellation during the transition — the Warn is suppressed (part of the shutdown).
	env3 := newAlertsTestEnv(t, nil)
	m3 := env3.newDeadMachine()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	before = env3.buf.String()
	_ = m3.transition(env3.Alerts, cancelled, obsErr[int64](errors.New("db unavailable")))
	if strings.Contains(env3.buf.String()[len(before):], "level=WARN") {
		t.Errorf("the ctx cancellation produced a Warn:\n%s", env3.buf.String()[len(before):])
	}

	// The status is written by the transition: the next pass's prev is the past
	// machine status (the series hold by prev = firing).
	env4 := newAlertsTestEnv(t, nil)
	m4 := env4.newDeadMachine()
	got := runSequence(env4.Alerts, m4,
		obs(int64(5)), obs(int64(5)), obs(int64(5)), obs(int64(1)), obs(int64(0)))
	wantSequence(t, "an episode by prev", got, []string{
		AlertStatusOK, AlertStatusOK, AlertStatusFiring, AlertStatusFiring, AlertStatusOK,
	})
}

// TestAlertMachineUnitOOM — the unit_oom machine (the level threshold family, without
// memory): each kind of event alone fires the alert; the fired
// state is held until the unit counters reset; all zeros — ok.
func TestAlertMachineUnitOOM(t *testing.T) {
	env := newAlertsTestEnv(t, nil)
	m := env.newUnitOOMMachine()

	events := []struct {
		name  string
		units []UnitEvents
		want  string
	}{
		{"oom", []UnitEvents{{Unit: "crawler-http-worker.service", OOM: 3}}, AlertStatusFiring},
		{"oom_kill", []UnitEvents{{Unit: "crawler-render-worker.service", OOMKill: 1}}, AlertStatusFiring},
		{"restarts", []UnitEvents{{Unit: "crawler-chromium.service", NRestarts: 2}}, AlertStatusFiring},
		{"two hot + one healthy", []UnitEvents{
			{Unit: "crawler-http-worker.service", OOMKill: 1},
			{Unit: "crawler-monitor.service"},
			{Unit: "crawler-chromium.service", NRestarts: 4},
		}, AlertStatusFiring},
		{"all zeros", []UnitEvents{
			{Unit: "crawler-http-worker.service"},
			{Unit: "crawler-monitor.service"},
		}, AlertStatusOK},
		{"a unit state reset", []UnitEvents{{Unit: "crawler-http-worker.service"}}, AlertStatusOK},
	}
	for _, c := range events {
		s := m.transition(env.Alerts, context.Background(), obs(c.units))
		if s.Status != c.want {
			t.Errorf("%s: status = %s (%s), want %s", c.name, s.Status, s.Detail, c.want)
		}
	}
	// The fired detail names every hot unit and its counters,
	// the healthy one is not mentioned; the ok detail — the number of units.
	env2 := newAlertsTestEnv(t, nil)
	m2 := env2.newUnitOOMMachine()
	s := m2.transition(env2.Alerts, context.Background(), obs(events[3].units))
	for _, want := range []string{"crawler-http-worker.service", "oom_kill=1",
		"crawler-chromium.service", "restarts=4", "episode until the unit restart"} {
		if !strings.Contains(s.Detail, want) {
			t.Errorf("the detail %q does not contain %q", s.Detail, want)
		}
	}
	if strings.Contains(s.Detail, "crawler-monitor.service:") {
		t.Errorf("the detail %q names a healthy unit", s.Detail)
	}
	s = m2.transition(env2.Alerts, context.Background(), obs(events[4].units))
	if !strings.Contains(s.Detail, "2 units") {
		t.Errorf("the ok detail = %q, want the unit count", s.Detail)
	}
}

// TestAlertMachineClockUsesDriver — the time windows (psi/freeze) take
// "now" from the driver's clock: only the clock moves between passes —
// the window closes; the clock stands — the hold does not grow (a check
// of passing now from transition into decide).
func TestAlertMachineClockUsesDriver(t *testing.T) {
	base := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	over := obs(PSIStat{Avg10: 95, Avg60: 95, Avg300: 95})

	// The driver's clock moves 31 min between passes — the window closed.
	now := base
	moved := NewAlerts(AlertsDeps{}, AlertsOptions{Now: func() time.Time { return now }})
	mMoved := moved.newPSIMachine()
	_ = mMoved.transition(moved, context.Background(), over) // the window base at t0
	now = base.Add(31 * time.Minute)
	if s := mMoved.transition(moved, context.Background(), over); s.Status != AlertStatusFiring {
		t.Errorf("the clock +31m: %s (%s), want firing (the window closed by the driver clock)", s.Status, s.Detail)
	}

	// The clock stands — the window does not tick, the hold does not grow.
	still := base
	fixed := NewAlerts(AlertsDeps{}, AlertsOptions{Now: func() time.Time { return still }})
	mFixed := fixed.newPSIMachine()
	_ = mFixed.transition(fixed, context.Background(), over)
	if s := mFixed.transition(fixed, context.Background(), over); s.Status != AlertStatusOK {
		t.Errorf("the clock stands still: %s (%s), want ok (a hold of 0 — now from the driver clock)", s.Status, s.Detail)
	}
}

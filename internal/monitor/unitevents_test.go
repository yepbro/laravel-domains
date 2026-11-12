// Crawler unit OOM/restart monitoring unit tests (T-145, spec 5.9) on
// fake sources: the alert thresholds (oom/oom_kill/NRestarts separately
// and together), the episode (one Warn per series — the front transition
// mechanics), quiet degradations (no source, a systemd environment
// error, an empty unit list — without a Warn, like SMART), the parsing
// of systemctl show output and memory.events (tables, including
// "[not set]" and junk lines), OSUnitEvents against stubs
// (a script-systemctl in TempDir + a cgroupfs tree; a unit without a
// cgroup — with zeros), the metrics wrapper (gauges per unit,
// hold-last on an error).
// Real reads of the host cgroupfs/exec systemctl are outside the tests
// (hermeticity, docs/mutation-testing.md §4).
package monitor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"crawler/internal/metrics"
)

// --- fake source --------------------------------------------------------

// fakeUnitEvents — unit snapshots from a preset, an error queue, the protocol.
type fakeUnitEvents struct {
	mu    sync.Mutex
	units []UnitEvents
	errs  []error
	calls int
}

func (f *fakeUnitEvents) UnitEvents(context.Context) ([]UnitEvents, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if err := popErr(&f.errs); err != nil {
		return nil, err
	}
	return f.units, nil
}

// newUnitEnv — a test evaluator with a live unit source: the default is
// two healthy units (all counters zero).
func newUnitEnv(t *testing.T, units []UnitEvents) (*alertsTestEnv, *fakeUnitEvents) {
	t.Helper()
	env := newAlertsTestEnv(t, nil)
	src := &fakeUnitEvents{units: units}
	env.deps.UnitEvents = src
	return env, src
}

// --- the unit_oom alert -------------------------------------------------------------

// TestAlertsUnitOOMFire — each kind of event alone turns the alert on:
// oom, oom_kill, NRestarts; together — one detail with each unit and its
// counters; all zeros — ok with the number of units.
func TestAlertsUnitOOMFire(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name  string
		units []UnitEvents
	}{
		{"oom", []UnitEvents{{Unit: "crawler-http-worker.service", OOM: 3}}},
		{"oom_kill", []UnitEvents{{Unit: "crawler-render-worker.service", OOMKill: 1}}},
		{"restarts", []UnitEvents{{Unit: "crawler-chromium.service", NRestarts: 2}}},
	}
	for _, c := range cases {
		env, _ := newUnitEnv(t, c.units)
		s := snapshotOf(t, env.Pass(ctx), AlertUnitOOM)
		if s.Status != AlertStatusFiring {
			t.Errorf("%s: status = %s (%s), want firing", c.name, s.Status, s.Detail)
		}
		if !strings.Contains(s.Detail, c.units[0].Unit) {
			t.Errorf("%s: the detail %q does not name the unit %s", c.name, s.Detail, c.units[0].Unit)
		}
	}

	// Two hot units — both in the detail, the healthy third not mentioned.
	env, _ := newUnitEnv(t, []UnitEvents{
		{Unit: "crawler-http-worker.service", OOMKill: 1},
		{Unit: "crawler-monitor.service"},
		{Unit: "crawler-chromium.service", NRestarts: 4},
	})
	s := snapshotOf(t, env.Pass(ctx), AlertUnitOOM)
	if s.Status != AlertStatusFiring {
		t.Fatalf("two hot ones: status = %s (%s), want firing", s.Status, s.Detail)
	}
	for _, want := range []string{"crawler-http-worker.service", "oom_kill=1", "crawler-chromium.service", "restarts=4"} {
		if !strings.Contains(s.Detail, want) {
			t.Errorf("the detail %q does not contain %q", s.Detail, want)
		}
	}
	if strings.Contains(s.Detail, "crawler-monitor.service") {
		t.Errorf("the detail %q names a healthy unit", s.Detail)
	}

	// All zeros — ok; the detail names the number of units.
	env, _ = newUnitEnv(t, []UnitEvents{
		{Unit: "crawler-http-worker.service"},
		{Unit: "crawler-monitor.service"},
	})
	s = snapshotOf(t, env.Pass(ctx), AlertUnitOOM)
	if s.Status != AlertStatusOK || !strings.Contains(s.Detail, "2 units") {
		t.Errorf("healthy units: %s (%s), want ok with the unit count", s.Status, s.Detail)
	}
}

// TestAlertsUnitOOMEpisode — one Warn per episode: a firing — a Warn,
// a sustained firing — silence, a counter reset (a unit restart/
// reset-failed) — a clearing Info.
func TestAlertsUnitOOMEpisode(t *testing.T) {
	env, src := newUnitEnv(t, []UnitEvents{{Unit: "crawler-http-worker.service", OOMKill: 1}})
	ctx := context.Background()

	env.Pass(ctx)
	if !strings.Contains(env.buf.String(), "level=WARN") ||
		!strings.Contains(env.buf.String(), "ALERT: OOM kills and crash restarts of crawler units") {
		t.Fatalf("a firing without a Warn:\n%s", env.buf.String())
	}

	// An episode continuation (counters not reset) — silence.
	before := env.buf.String()
	env.Pass(ctx)
	if delta := env.buf.String()[len(before):]; strings.Contains(delta, "level=WARN") || strings.Contains(delta, "level=INFO") {
		t.Errorf("a sustained firing repeated the signal:\n%s", delta)
	}

	// A new event of the same unit inside the episode — without a second Warn.
	src.units = []UnitEvents{{Unit: "crawler-http-worker.service", OOMKill: 2, NRestarts: 1}}
	env.Pass(ctx)
	delta := env.buf.String()[len(before):]
	if strings.Contains(delta, "level=WARN") || strings.Contains(delta, "level=INFO") {
		t.Errorf("a new event inside the episode produced a second signal:\n%s", delta)
	}

	// A unit state reset (the cgroup recreated, NRestarts zeroed) — an Info.
	src.units = []UnitEvents{{Unit: "crawler-http-worker.service"}}
	before = env.buf.String()
	env.Pass(ctx)
	if delta := env.buf.String()[len(before):]; !strings.Contains(delta, "alert cleared") || !strings.Contains(delta, "level=INFO") {
		t.Errorf("an episode reset without a clearing Info:\n%s", delta)
	}
}

// TestAlertsUnitOOMDegradation — source degradations of two classes:
// an environment without systemd (errNoSystemd — a quiet "not measured",
// no Warn, the SMART contract) and a production source failure (a Warn
// of each pass, the ingest probe style); plus the source not set and an
// empty unit list.
func TestAlertsUnitOOMDegradation(t *testing.T) {
	env := newAlertsTestEnv(t, nil) // UnitEvents not set
	env.deps.UnitEvents = nil       // an explicit absence of a source (T-162 gave env a healthy default)
	s := snapshotOf(t, env.Pass(context.Background()), AlertUnitOOM)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "source not set") {
		t.Errorf("without a source: %s (%s), want not_measured \"source not set\"", s.Status, s.Detail)
	}

	// A dev container without systemd — quiet: a Debug transition, no Warn.
	env, src := newUnitEnv(t, nil)
	src.errs = []error{fmt.Errorf("monitor: units: %w: systemctl show: %w",
		errNoSystemd, errors.New("exec: systemctl: executable file not found"))}
	s = snapshotOf(t, env.Pass(context.Background()), AlertUnitOOM)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "systemd is missing") {
		t.Errorf("without systemd: %s (%s), want not_measured with the errNoSystemd class", s.Status, s.Detail)
	}
	if strings.Contains(env.buf.String(), "level=WARN") {
		t.Errorf("the missing systemd produced a Warn (it must be quiet, like SMART):\n%s", env.buf.String())
	}

	// A production source failure — a Warn of each pass, "not measured"
	// with the reason; the next pass against a live source works.
	env, src = newUnitEnv(t, []UnitEvents{{Unit: "crawler-http-worker.service"}})
	src.errs = []error{errors.New("monitor: units: memory.events: EIO")}
	s = snapshotOf(t, env.Pass(context.Background()), AlertUnitOOM)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "EIO") {
		t.Errorf("a hard failure: %s (%s), want not_measured with the reason", s.Status, s.Detail)
	}
	if !strings.Contains(env.buf.String(), "level=WARN") || !strings.Contains(env.buf.String(), "EIO") {
		t.Errorf("a hard failure without a Warn:\n%s", env.buf.String())
	}
	if s := snapshotOf(t, env.Pass(context.Background()), AlertUnitOOM); s.Status != AlertStatusOK {
		t.Errorf("after the failure: %s (%s), want ok (the loop lives)", s.Status, s.Detail)
	}

	// systemd alive, no units loaded — an empty list without an error.
	src.units = nil
	s = snapshotOf(t, env.Pass(context.Background()), AlertUnitOOM)
	if s.Status != AlertStatusNotMeasured || !strings.Contains(s.Detail, "not found") {
		t.Errorf("without units: %s (%s), want not_measured \"not found\"", s.Status, s.Detail)
	}
}

// --- parsing of systemctl show output ----------------------------------------------

// TestParseSystemctlShow — the Id/NRestarts blocks per unit, "[not set]"
// and junk values — zeros (like t092-sample.sh), an NRestarts before an
// Id is discarded, an empty output — an empty list.
func TestParseSystemctlShow(t *testing.T) {
	units := parseSystemctlShow("Id=crawler-http-worker.service\nNRestarts=2\n\n" +
		"Id=crawler-monitor.service\nNRestarts=0\n\n")
	if len(units) != 2 {
		t.Fatalf("units = %d, want 2: %+v", len(units), units)
	}
	if units[0].Unit != "crawler-http-worker.service" || units[0].NRestarts != 2 {
		t.Errorf("the first unit = %+v, want crawler-http-worker.service NRestarts=2", units[0])
	}
	if units[1].Unit != "crawler-monitor.service" || units[1].NRestarts != 0 {
		t.Errorf("the second unit = %+v, want crawler-monitor.service NRestarts=0", units[1])
	}

	// "[not set]" (the unit never started) and a non-number — zeros, not an error.
	for _, val := range []string{"[not set]", "junk"} {
		units = parseSystemctlShow("Id=crawler-web.service\nNRestarts=" + val + "\n")
		if len(units) != 1 || units[0].NRestarts != 0 {
			t.Errorf("NRestarts=%q: %+v, want 0", val, units)
		}
	}

	// An NRestarts before the first Id is discarded; foreign lines do not pass.
	units = parseSystemctlShow("NRestarts=9\nId=crawler-dns-worker.service\nLoadState=loaded\nNRestarts=1\n")
	if len(units) != 1 || units[0].Unit != "crawler-dns-worker.service" || units[0].NRestarts != 1 {
		t.Errorf("a weird block: %+v, want dns-worker NRestarts=1 (an early NRestarts=9 discarded)", units)
	}

	if got := parseSystemctlShow(""); got != nil {
		t.Errorf("an empty output = %+v, want nil", got)
	}
	if got := parseSystemctlShow("\n\n"); got != nil {
		t.Errorf("only empty lines = %+v, want nil", got)
	}
}

// --- parsing of memory.events --------------------------------------------------------

// TestParseMemoryEvents — oom/oom_kill/high from a full file, foreign
// keys and missing lines do not pass / zeros, a non-numeric value is an
// error.
func TestParseMemoryEvents(t *testing.T) {
	ev, err := parseMemoryEvents("low 0\nhigh 42\nmax 7\noom 3\noom_kill 1\noom_group 0\n")
	if err != nil || ev.OOM != 3 || ev.OOMKill != 1 || ev.High != 42 {
		t.Errorf("a full file = (%d, %d, %d, %v), want (3, 1, 42, nil)", ev.OOM, ev.OOMKill, ev.High, err)
	}

	ev, err = parseMemoryEvents("low 5\nmax 1\n")
	if err != nil || ev.OOM != 0 || ev.OOMKill != 0 || ev.High != 0 {
		t.Errorf("without oom/high lines = (%d, %d, %d, %v), want zeros without an error", ev.OOM, ev.OOMKill, ev.High, err)
	}

	ev, err = parseMemoryEvents("")
	if err != nil || ev.OOM != 0 || ev.OOMKill != 0 || ev.High != 0 {
		t.Errorf("an empty file = (%d, %d, %d, %v), want zeros without an error", ev.OOM, ev.OOMKill, ev.High, err)
	}

	// A line without a space — not a key-value, skipped.
	if _, err := parseMemoryEvents("oomkill\n"); err != nil {
		t.Errorf("a line without a value: error = %v, want a skip", err)
	}

	if ev, err := parseMemoryEvents("oom -1\n"); err == nil || ev.OOM != 0 {
		t.Errorf("a non-numeric oom: (%d, %v), want an error and zero", ev.OOM, err)
	}
	if ev, err := parseMemoryEvents("oom_kill x\n"); err == nil || ev.OOMKill != 0 {
		t.Errorf("a non-numeric oom_kill: (%d, %v), want an error and zero", ev.OOMKill, err)
	}
	if ev, err := parseMemoryEvents("high y\n"); err == nil || ev.High != 0 {
		t.Errorf("a non-numeric high: (%d, %v), want an error and zero (the memory_high alert, T-162)", ev.High, err)
	}
}

// --- OSUnitEvents against stubs ---------------------------------------------

// fakeSystemctl — an executable systemctl stub script in the dir
// directory: prints out to stdout; on fail — stderr and exit code 1.
func fakeSystemctl(t *testing.T, dir, out, failMsg string) string {
	t.Helper()
	script := "#!/bin/sh\n"
	if failMsg != "" {
		script += "echo '" + failMsg + "' >&2\nexit 1\n"
	} else {
		// A quoted heredoc — no substitutions, verbatim output.
		script += "cat <<'SYSTEMCTL_SHOW_EOF'\n" + out + "\nSYSTEMCTL_SHOW_EOF\n"
	}
	path := filepath.Join(dir, "systemctl-fake")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

// fakeCgroup — a unit cgroupfs tree with memory.events in the root
// directory; events == "" — a unit without a cgroup (stopped).
func fakeCgroup(t *testing.T, root, unit, events string) {
	t.Helper()
	if events == "" {
		return // the unit directory is not created
	}
	dir := filepath.Join(root, "system.slice", unit)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.events"), []byte(events), 0o444); err != nil {
		t.Fatal(err)
	}
}

// TestOSUnitEvents — systemctl show gives the units and NRestarts,
// memory.events — oom/oom_kill/high (high — MemoryHigh throttling, the
// memory_high alert T-162); a unit without a cgroup — zero memory
// events (stopped, restarts accounted by NRestarts).
func TestOSUnitEvents(t *testing.T) {
	root := t.TempDir()
	cg := filepath.Join(root, "cg")
	show := "Id=crawler-http-worker.service\nNRestarts=2\n\n" +
		"Id=crawler-analyzer.service\nNRestarts=0\n"
	fakeCgroup(t, cg, "crawler-http-worker.service", "low 0\nhigh 12\noom 3\noom_kill 1\n")
	fakeCgroup(t, cg, "crawler-analyzer.service", "low 0\noom 0\noom_kill 0\n")
	// monitor.service has no directory — like a stopped unit.
	show += "\nId=crawler-monitor.service\nNRestarts=1\n"

	units, err := (OSUnitEvents{
		CgroupFS:  cg,
		Systemctl: fakeSystemctl(t, root, show, ""),
	}).UnitEvents(context.Background())
	if err != nil {
		t.Fatalf("UnitEvents: %v", err)
	}
	if len(units) != 3 {
		t.Fatalf("units = %d, want 3: %+v", len(units), units)
	}
	want := []UnitEvents{
		{Unit: "crawler-http-worker.service", OOM: 3, OOMKill: 1, High: 12, NRestarts: 2},
		{Unit: "crawler-analyzer.service"},
		{Unit: "crawler-monitor.service", NRestarts: 1},
	}
	for i, w := range want {
		if units[i] != w {
			t.Errorf("unit[%d] = %+v, want %+v", i, units[i], w)
		}
	}
}

// TestOSUnitEventsEmptyShow — systemd alive, the pattern found nothing:
// an empty list without an error (the alert will switch to "not
// measured" by itself).
func TestOSUnitEventsEmptyShow(t *testing.T) {
	root := t.TempDir()
	units, err := (OSUnitEvents{
		CgroupFS:  filepath.Join(root, "cg"),
		Systemctl: fakeSystemctl(t, root, "", ""),
	}).UnitEvents(context.Background())
	if err != nil {
		t.Fatalf("UnitEvents: %v", err)
	}
	if len(units) != 0 {
		t.Errorf("units = %+v, want empty", units)
	}
}

// TestOSUnitEventsErrors — the failure classes: "not systemd" (no
// binary, PID 1 not systemd — errNoSystemd, the quiet alert class) and
// production errors (systemctl failed for another reason, a corrupted
// memory.events — the names of all failed units, the traversal without
// fail-fast), a hung systemctl — the timeout budget.
func TestOSUnitEventsErrors(t *testing.T) {
	root := t.TempDir()

	// PID 1 is not systemd — an expected absence of the environment.
	errNoBoot := "System has not been booted with systemd (PID 1)"
	_, err := (OSUnitEvents{
		CgroupFS:  filepath.Join(root, "cg"),
		Systemctl: fakeSystemctl(t, root, "", errNoBoot),
	}).UnitEvents(context.Background())
	if err == nil || !strings.Contains(err.Error(), errNoBoot) {
		t.Errorf("PID 1 is not systemd: error = %v, want the systemctl reason", err)
	}
	if !errors.Is(err, errNoSystemd) {
		t.Errorf("PID 1 is not systemd: error = %v, want the errNoSystemd class", err)
	}

	// The binary is not in PATH (a dev container, Systemctl="" →
	// "systemctl") — the quiet errNoSystemd class.
	_, err = (OSUnitEvents{
		CgroupFS:  filepath.Join(root, "cg"),
		Systemctl: "systemctl-definitely-missing-t145",
	}).UnitEvents(context.Background())
	if !errors.Is(err, errNoSystemd) {
		t.Errorf("systemctl is missing from PATH: error = %v, want the errNoSystemd class", err)
	}

	// The explicitly given path vanished (a production package breakage,
	// not a container) — NOT the quiet class: staying silent about a
	// broken systemctl is not allowed.
	_, err = (OSUnitEvents{
		CgroupFS:  filepath.Join(root, "cg"),
		Systemctl: filepath.Join(root, "vanished-systemctl"),
	}).UnitEvents(context.Background())
	if err == nil || errors.Is(err, errNoSystemd) {
		t.Errorf("the given path is missing: error = %v, want a hard error (not errNoSystemd)", err)
	}

	// A production systemctl error (not "no systemd") — not the quiet class.
	_, err = (OSUnitEvents{
		CgroupFS:  filepath.Join(root, "cg"),
		Systemctl: fakeSystemctl(t, root, "", "Failed to connect to bus"),
	}).UnitEvents(context.Background())
	if err == nil || !strings.Contains(err.Error(), "Failed to connect to bus") {
		t.Errorf("a systemctl crash: error = %v, want the reason with stderr", err)
	}
	if errors.Is(err, errNoSystemd) {
		t.Errorf("a bus failure: error = %v, it must not be the errNoSystemd class", err)
	}

	// A corrupted memory.events of two units: the traversal continues the
	// list, the reason names each failed one (no fail-fast on the first).
	cg := filepath.Join(root, "cg")
	show := "Id=crawler-http-worker.service\nNRestarts=0\n\nId=crawler-dns-worker.service\nNRestarts=0\n"
	fakeCgroup(t, cg, "crawler-http-worker.service", "oom many\n")
	fakeCgroup(t, cg, "crawler-dns-worker.service", "oom_kill too\n")
	_, err = (OSUnitEvents{CgroupFS: cg, Systemctl: fakeSystemctl(t, root, show, "")}).
		UnitEvents(context.Background())
	for _, unit := range []string{"crawler-http-worker.service", "crawler-dns-worker.service"} {
		if err == nil || !strings.Contains(err.Error(), unit) {
			t.Errorf("two corrupted memory.events: error = %v, want the name %s", err, unit)
		}
	}

	// A ctx cancellation — before the systemctl access.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = (OSUnitEvents{Systemctl: fakeSystemctl(t, root, "", "")}).UnitEvents(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("a ctx cancellation: error = %v, want context.Canceled", err)
	}
}

// TestOSUnitEventsTimeout — a hung systemctl is cut off by the source
// budget (the alert passes are sequential — a hang must not stop the
// loop): a short Timeout + a sleeping stub. exec — so that the kill
// lands in the process itself instead of waiting for an orphaned child
// on the pipe descriptors (the direct-child model, like the real
// systemctl). The marker file proves the process started: the error is
// then exactly a budget cut-off, not a launch refusal.
func TestOSUnitEventsTimeout(t *testing.T) {
	root := t.TempDir()
	marker := filepath.Join(root, "started")
	script := "#!/bin/sh\n touch " + marker + "\n exec sleep 30\n"
	path := filepath.Join(root, "systemctl-hang")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err := (OSUnitEvents{
		CgroupFS:  filepath.Join(root, "cg"),
		Systemctl: path,
		Timeout:   50 * time.Millisecond,
	}).UnitEvents(context.Background())
	if err == nil {
		t.Fatal("a hung systemctl: no error, want the timeout budget")
	}
	if _, merr := os.Stat(marker); merr != nil {
		t.Fatalf("the stub did not start (the marker was not created) — the timeout was not exercised: %v", merr)
	}
	// The error identity is not pinned (the kill by ctx arrives as
	// "signal: killed", not a DeadlineExceeded wrapper) — what matters is
	// the budget cut-off itself and the non-quiet class (a production
	// failure, not "no systemd").
	if errors.Is(err, errNoSystemd) {
		t.Errorf("a hung systemctl: error = %v, not the errNoSystemd class", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("the budget did not cut the hang: %s", elapsed)
	}
}

// TestOSUnitEventsNoCgroupRoot — systemd answers, the units are there,
// but there is no system.slice root (cgroup v1/hybrid, a foreign
// CgroupFS) — a source error, not a false "ok" by ENOENT zeros
// (review round 2, P3).
func TestOSUnitEventsNoCgroupRoot(t *testing.T) {
	root := t.TempDir()
	show := "Id=crawler-http-worker.service\nNRestarts=0\n"
	// The cg directory is not created: there is no system.slice root.
	_, err := (OSUnitEvents{
		CgroupFS:  filepath.Join(root, "cg"),
		Systemctl: fakeSystemctl(t, root, show, ""),
	}).UnitEvents(context.Background())
	if err == nil || !strings.Contains(err.Error(), "system.slice") {
		t.Errorf("the cgroup v2 root is missing: error = %v, want an error about system.slice", err)
	}
	if errors.Is(err, errNoSystemd) {
		t.Errorf("the cgroup v2 root is missing: error = %v, not the errNoSystemd class (systemctl did answer)", err)
	}

	// An empty unit list with the same missing root — without an error:
	// there is nothing to read, the alert will say "not found" by itself.
	_, err = (OSUnitEvents{
		CgroupFS:  filepath.Join(root, "cg"),
		Systemctl: fakeSystemctl(t, root, "", ""),
	}).UnitEvents(context.Background())
	if err != nil {
		t.Errorf("an empty list without a cgroup root: error = %v, want nil", err)
	}
}

// --- metrics wrapper -------------------------------------------------------------

// gaugeLabels — a gauge value by name and the label=value pairs.
// The service utility label (substituted by the registry factory) is
// not counted by the criterion: a series matches when all the want
// labels coincide and the rest reduce to service.
func gaugeLabels(t *testing.T, g prometheus.Gatherer, name string, want map[string]string) (float64, bool) {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			got := make(map[string]string, len(m.GetLabel()))
			for _, l := range m.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			equal := true
			for k, v := range want {
				if got[k] != v {
					equal = false
				}
			}
			for k := range got {
				if _, ok := want[k]; !ok && k != metrics.LabelService {
					equal = false
				}
			}
			if equal {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// TestUnitEventsMetrics — the wrapper mirrors the snapshot into the
// gauges of both instruments (the unit/kind labels), a source error
// keeps the last values (hold-last), a nil registry — pure transit.
func TestUnitEventsMetrics(t *testing.T) {
	reg := metrics.New("monitor")
	src := &fakeUnitEvents{units: []UnitEvents{
		{Unit: "crawler-http-worker.service", OOM: 3, OOMKill: 1, NRestarts: 2},
		{Unit: "crawler-monitor.service"},
	}}
	m := NewUnitEventsMetrics(reg, src)

	if _, err := m.UnitEvents(context.Background()); err != nil {
		t.Fatalf("UnitEvents: %v", err)
	}
	cases := []struct {
		metric string
		labels map[string]string
		want   float64
	}{
		{"crawler_unit_oom_events", map[string]string{"unit": "crawler-http-worker.service", "kind": "oom"}, 3},
		{"crawler_unit_oom_events", map[string]string{"unit": "crawler-http-worker.service", "kind": "oom_kill"}, 1},
		{"crawler_unit_oom_events", map[string]string{"unit": "crawler-monitor.service", "kind": "oom_kill"}, 0},
		{"crawler_unit_restarts", map[string]string{"unit": "crawler-http-worker.service"}, 2},
		{"crawler_unit_restarts", map[string]string{"unit": "crawler-monitor.service"}, 0},
	}
	for _, c := range cases {
		if v, ok := gaugeLabels(t, reg, c.metric, c.labels); !ok || v != c.want {
			t.Errorf("%s%v = (%v, %v), want %v", c.metric, c.labels, v, ok, c.want)
		}
	}

	// A source error — the error transit, the metrics untouched (hold-last).
	src.errs = []error{errors.New("container without systemd")}
	if _, err := m.UnitEvents(context.Background()); err == nil {
		t.Error("the source error is not propagated")
	}
	if v, ok := gaugeLabels(t, reg, "crawler_unit_restarts", map[string]string{"unit": "crawler-http-worker.service"}); !ok || v != 2 {
		t.Errorf("after the error restarts = (%v, %v), want the previous 2", v, ok)
	}

	// New values overwrite the gauge (they do not accumulate as rows).
	src.errs = nil
	src.units = []UnitEvents{{Unit: "crawler-http-worker.service", OOM: 4, NRestarts: 3}}
	if _, err := m.UnitEvents(context.Background()); err != nil {
		t.Fatalf("UnitEvents: %v", err)
	}
	if v, _ := gaugeLabels(t, reg, "crawler_unit_oom_events", map[string]string{"unit": "crawler-http-worker.service", "kind": "oom"}); v != 4 {
		t.Errorf("the gauge overwrite: oom = %v, want 4", v)
	}
	if v, _ := gaugeLabels(t, reg, "crawler_unit_restarts", map[string]string{"unit": "crawler-http-worker.service"}); v != 3 {
		t.Errorf("the gauge overwrite: restarts = %v, want 3", v)
	}

	// A nil registry — transit without instruments (isolated builds).
	plain := NewUnitEventsMetrics(nil, &fakeUnitEvents{units: []UnitEvents{{Unit: "u.service"}}})
	units, err := plain.UnitEvents(context.Background())
	if err != nil || len(units) != 1 || units[0].Unit != "u.service" {
		t.Errorf("transit without a registry = (%+v, %v), want the source as is", units, err)
	}
}

// A source of crawler unit OOM and restart monitoring (T-145, spec 5.9):
// the memory.events counters of cgroup v2 (oom/oom_kill/high) and the
// NRestarts of systemd. oom/oom_kill/NRestarts feed the unit_oom alert,
// the growth of the high counter between passes — the MemoryHigh
// throttling alert (memory_high, T-162: a precursor of the http-worker
// fatals of T-157 — both read one source snapshot).
// Unit memory caps are set by MemoryMax (T-085/T-143; the http-worker's
// soft MemoryHigh — by the same T-145), but a cap is a guard, not a
// guarantee: an OOM-kill or a crash restart is unscheduled pipeline
// downtime (spec 3.4) and the first symptom of the budget not being met
// (docs/deploy.md §2); the unit_oom alert (alerts.go) hits a Warn in the
// monitor's journald, one Warn per episode (the T-080 transition
// mechanics).
//
// The sources avoid new dependencies (no DBus client is introduced):
//
//   - the unit list and NRestarts — `systemctl show 'crawler-*.service'
//     -p Id -p NRestarts`: the manager properties are read
//     unprivileged (like `systemctl status` under any user — polkit does
//     not ask for property reads), the pattern covers stopped/failed
//     units too (NRestarts of a crash loop by StartLimitBurst is visible
//     although the cgroup is already removed);
//   - oom/oom_kill — a direct read of
//     /sys/fs/cgroup/system.slice/<unit>/memory.events: the cgroup files
//     are world-readable (0444 root), the ProtectControlGroups of the
//     monitor unit mounts cgroupfs read-only — reads are unaffected.
//     The direct read is not a choice among equals: systemd does not
//     export the memory.events counters as properties (systemd 259 does
//     not know -p MemoryEvents), the file is the only path; the t092
//     sampler read the same as root.
//
// The dev container has neither systemd (the distroless runtime image
// without systemctl) nor system.slice — the source returns an
// errNoSystemd class error, the alert degrades quietly to "not measured"
// on it (Debug, no Warn and no false firing): the same contract as SMART
// NVMe (alerts.go) — an environment without systemd for dev is the norm,
// not a failure. Other errors (a production host but systemd/cgroupfs
// unavailable) — the alert is "not measured" with a Warn of each pass,
// the loop keeps living: the failure of already working monitoring must
// not be invisible. On the production host systemctl and cgroupfs are
// available to the monitor unit without extra privileges.
package monitor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// unitEventsPattern — the pattern of monitored units: all crawler
// services of the system slice. crawler.target does not match the
// .service suffix, and the .service suffix cuts off possible
// non-service units of the set.
const unitEventsPattern = "crawler-*.service"

// errNoSystemd — an environment without systemd (a container): systemctl
// is not installed or PID 1 is not systemd. An expected state of the dev
// environment, not a failure: the alert degrades quietly on this class
// (like SMART), the other source errors (a production host but a broken
// systemd/cgroupfs) — a Warn of each pass, the style of the other
// sources (the ingest probe).
var errNoSystemd = errors.New("systemd is missing (container/dev)")

// Values of the kind label of the crawler_unit_oom_events metric (uniteventsmetrics).
const (
	// unitEventOOM is memory.events oom: the OOM-killer ran in the cgroup.
	unitEventOOM = "oom"
	// unitEventOOMKill is memory.events oom_kill: processes were killed.
	unitEventOOMKill = "oom_kill"
)

// UnitEvents is a snapshot of one crawler unit's events over the life of
// its cgroup (memory.events) and the unit (NRestarts).
type UnitEvents struct {
	// Unit is the unit name (crawler-http-worker.service).
	Unit string
	// OOM is memory.events oom: the number of OOM-killer firings in the
	// cgroup (attempts, including ones fended off by reclaim).
	OOM uint64
	// OOMKill is memory.events oom_kill: processes killed by the OOM-killer.
	OOMKill uint64
	// High is memory.events high: the number of hits against the soft
	// MemoryHigh cap — each one is a forced reclaim/delays
	// (throttling). The absolute value accumulates over the cgroup's
	// life (an http-worker with MemoryHigh=850M gains >100 k per day —
	// deploy/systemd/crawler-http-worker.service): the memory_high alert
	// event is the counter's growth between passes, not the value
	// itself (T-162).
	High uint64
	// NRestarts is the systemctl NRestarts: crash restarts since the
	// last start/unit state reset (reset-failed/stop).
	NRestarts uint64
}

// hot — the unit has events (the alert only looks at this).
func (e UnitEvents) hot() bool {
	return e.OOM > 0 || e.OOMKill > 0 || e.NRestarts > 0
}

// UnitEventsSource is a unit events source; an interface for
// substitution in unit tests (the real read is systemctl + cgroupfs,
// which the test container does not have).
// SpaceChecker/WearSource/BadHashesSource style.
type UnitEventsSource interface {
	// UnitEvents returns snapshots of all crawler-*.service units.
	// An error means "not measured", with the reason attached (a
	// container without systemd); an empty list without an error —
	// systemd is alive, no units are loaded.
	UnitEvents(ctx context.Context) ([]UnitEvents, error)
}

// OSUnitEvents is a UnitEventsSource on top of systemctl show + cgroup
// v2: one exec per pass (the pattern is expanded by the manager for all
// units at once) and one memory.events read per unit.
type OSUnitEvents struct {
	// CgroupFS is the cgroup v2 root ("" — "/sys/fs/cgroup"); tests
	// substitute it with a directory (the SysFS/DevFS style of OSWear).
	CgroupFS string
	// Systemctl is the path to systemctl ("" — "systemctl" from PATH);
	// tests substitute it with a stub script.
	Systemctl string
	// Timeout is the budget of one source pass, exec included
	// (0 — unitEventsProbeTimeout); tests substitute it with a small
	// value so as not to wait real seconds on a hung stub.
	Timeout time.Duration
}

// unitEventsProbeTimeout is the budget of a source pass: the alert
// passes are sequential (Pass), a hung systemctl/D-Bus must not stop the
// evaluation of the other alerts for more than a couple of loop periods.
// 10 s — with a margin against a cold manager start and at the same time
// ≪ the 60 s loop period.
//
// The exact value is only distinguishable by a seconds-long pause
// against a hung process — it contradicts the hermeticity of unit tests
// (docs/mutation-testing.md §4): the value is a configuration budget,
// not logic (the ingestProbeTimeout style).
// mutator-disable-next-line arithmetic/base, numbers/decrementer, numbers/incrementer
const unitEventsProbeTimeout = 10 * time.Second

// compile-time: OSUnitEvents is a UnitEventsSource.
var _ UnitEventsSource = OSUnitEvents{}

// UnitEvents collects the events of all crawler units: systemctl show
// gives the list and NRestarts, memory.events of each unit —
// oom/oom_kill. A unit without a cgroup (stopped: failed after the
// StartLimit is exhausted or before the first start) is read with zero
// memory events — the NRestarts of such a unit stays in the snapshot and
// alerts. The reading of all units is traversed in full: each error is
// collected into one reason with names (diagnostics sees all the failed
// ones), but the snapshot is not returned — an unread counter is not
// substituted with zero, otherwise a read failure would falsely clear
// the alert.
func (s OSUnitEvents) UnitEvents(ctx context.Context) ([]UnitEvents, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout())
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	systemctl := s.Systemctl
	// The PATH default lives only on the production host: unit tests
	// always set Systemctl with a script, a real exec in tests is not
	// allowed.
	// mutator-disable-next-line branch/if
	if systemctl == "" {
		// mutator-disable-next-line statement/remove
		systemctl = "systemctl"
	}
	cgroupFS := s.CgroupFS
	// The "/sys/fs/cgroup" default — like systemctl above.
	// mutator-disable-next-line branch/if
	if cgroupFS == "" {
		// mutator-disable-next-line statement/remove
		cgroupFS = "/sys/fs/cgroup"
	}

	var out, errOut bytes.Buffer
	cmd := exec.CommandContext(ctx, systemctl,
		"show", unitEventsPattern, "--property=Id", "--property=NRestarts")
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		reason := fmt.Errorf("systemctl show: %w", err)
		if stderr := strings.TrimSpace(errOut.String()); stderr != "" {
			reason = fmt.Errorf("systemctl show: %w: %s", err, stderr)
		}
		// An environment without systemd — an expected degradation: the
		// binary is not in PATH (a distroless dev container) or PID 1 is
		// not systemd — the quiet errNoSystemd class; everything else
		// (a production host with a broken systemd — including the given
		// path vanished, a hung manager cut off by the timeout) — the
		// error as is, the alert will answer with a Warn.
		if errors.Is(err, exec.ErrNotFound) ||
			strings.Contains(errOut.String(), "System has not been booted with systemd") {
			return nil, fmt.Errorf("monitor: units: %w: %w", errNoSystemd, reason)
		}
		return nil, fmt.Errorf("monitor: units: %w", reason)
	}
	units := parseSystemctlShow(out.String())
	if len(units) > 0 {
		// The system slice root must exist on a cgroup v2 host
		// (systemctl has already answered — systemd is there). Its absence
		// is not "units without events" but a non-working source
		// (cgroup v1/hybrid, a foreign root): without the check all the
		// ENOENTs below would fold into a false "ok" by zeros.
		sliceDir := filepath.Join(cgroupFS, "system.slice")
		if _, err := os.Stat(sliceDir); err != nil {
			return nil, fmt.Errorf("monitor: units: cgroup v2 %s is unavailable: %w", sliceDir, err)
		}
	}
	var errs []string
	for i := range units {
		ev, err := readMemoryEvents(filepath.Join(
			cgroupFS, "system.slice", units[i].Unit, "memory.events"))
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", units[i].Unit, err))
			continue
		}
		units[i].OOM, units[i].OOMKill, units[i].High = ev.OOM, ev.OOMKill, ev.High
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("monitor: units: memory.events: %s", strings.Join(errs, "; "))
	}
	return units, nil
}

// timeout — the pass budget with the default.
func (s OSUnitEvents) timeout() time.Duration {
	if s.Timeout > 0 {
		return s.Timeout
	}
	return unitEventsProbeTimeout
}

// parseSystemctlShow parses the output of `systemctl show -p Id
// -p NRestarts <pattern>`: the "Id=…\nNRestarts=…" blocks per unit (the
// line order in a block is fixed by the manager, the separator is an
// empty line, but the parser does not depend on the order). A
// non-numeric NRestarts ("[not set]" — the unit has never started) is
// read as zero, as in the t092-sample.sh sampler; an NRestarts before
// the first Id is discarded. Extracted as a pure function for a table
// test without a process.
func parseSystemctlShow(out string) []UnitEvents {
	var units []UnitEvents
	// cur is the index of the current unit; −1 — no Id seen yet.
	cur := -1
	for _, line := range strings.Split(out, "\n") {
		switch {
		case strings.HasPrefix(line, "Id="):
			units = append(units, UnitEvents{Unit: strings.TrimPrefix(line, "Id=")})
			cur = len(units) - 1
		case strings.HasPrefix(line, "NRestarts="):
			if cur < 0 {
				continue
			}
			n, err := strconv.ParseUint(strings.TrimPrefix(line, "NRestarts="), 10, 64)
			if err != nil {
				n = 0
			}
			units[cur].NRestarts = n
		}
	}
	return units
}

// memoryEvents — the memory.events file counters the alerts need:
// oom/oom_kill (unit_oom, T-145) and high (memory_high, T-162).
type memoryEvents struct {
	OOM     uint64
	OOMKill uint64
	High    uint64
}

// readMemoryEvents reads oom/oom_kill/high of one unit from
// memory.events. No file (a unit without a cgroup — stopped) — zeros:
// the counters live in the cgroup and are zeroed by its recreation,
// a restart of such a unit is already accounted in NRestarts.
func readMemoryEvents(path string) (memoryEvents, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return memoryEvents{}, nil
	}
	if err != nil {
		return memoryEvents{}, err
	}
	return parseMemoryEvents(string(b))
}

// parseMemoryEvents parses the memory.events file: "key value" lines
// (low/high/max/oom/oom_kill/oom_group); the alerts need oom/oom_kill/
// high, the other keys are skipped, missing ones — zeros. A value that
// is not a number is an error (the format is fixed by the kernel, a
// malformed file must not be silently ignored).
// Extracted as a pure function for a table test.
func parseMemoryEvents(data string) (memoryEvents, error) {
	var ev memoryEvents
	for _, line := range strings.Split(data, "\n") {
		key, val, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		var dst *uint64
		switch key {
		case "oom":
			dst = &ev.OOM
		case "oom_kill":
			dst = &ev.OOMKill
		case "high":
			dst = &ev.High
		default:
			continue
		}
		n, perr := strconv.ParseUint(val, 10, 64)
		if perr != nil {
			return memoryEvents{}, fmt.Errorf("monitor: memory.events: %q: the value %q is not a number", key, val)
		}
		*dst = n
	}
	return ev, nil
}

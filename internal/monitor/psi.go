// The CPU pressure source via PSI (Pressure Stall Information, a kernel ≥ 4.20 with
// CONFIG_PSI) for the psi_cpu alert (T-162, spec 5.9): /proc/pressure/cpu on
// the host is the same kernel the measurement sampler of §12 read (some 91–95% in
// CPU starvation episodes). PSI some — the share of time when at least one
// task waited for CPU: a direct metric of scheduler starvation that load
// average does not distinguish (iowait and high utilization without a queue).
//
// The degradation contract — like NVMe SMART (alerts.go): kernels/VPS without PSI
// (/proc/pressure missing — not every host enables CONFIG_PSI=y or the psi=1
// parameter) is an expected environment, not an incident: errNoPSI; the alert
// is quietly "not measured" without a Warn and without a false firing. In Docker
// /proc is the host pseudo-fs; in a dev container the values are the same as on
// the CI host: the source unit tests substitute the path with a temporary file.
package monitor

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
)

// errNoPSI — a kernel without PSI: /proc/pressure missing (CONFIG_PSI not
// built in or disabled by the psi parameter). An expected degradation of some
// environments, not an incident — the psi_cpu alert is quietly "not measured"
// on it (the errNoSystemd style of unit events, T-145).
var errNoPSI = errors.New("PSI не доступен (ядро без CONFIG_PSI или psi=0)")

// PSIStat — the CPU pressure via the some line of /proc/pressure/cpu:
// the average share of stall time (%) over 10 s / 60 s / 300 s.
type PSIStat struct {
	// Avg10, Avg60, Avg300 — some avg10/avg60/avg300, % (0–100).
	Avg10  float64
	Avg60  float64
	Avg300 float64
}

// PSISource — a CPU pressure source; an interface for substitution in unit tests
// (the real read is the /proc file; the values in the test container are not
// enough). The WearSource/UnitEventsSource style.
type PSISource interface {
	// PSI returns the some snapshot. An error = "not measured" with a reason.
	PSI(ctx context.Context) (PSIStat, error)
}

// OSPSI — a PSISource on top of /proc/pressure/cpu.
type OSPSI struct {
	// Path — the CPU pressure file path ("" — "/proc/pressure/cpu"); the tests
	// substitute a temporary file (the SysFS/DevFS style of OSWear).
	Path string
}

// compile-time: OSPSI is a PSISource.
var _ PSISource = OSPSI{}

// PSI reads and parses the CPU pressure file.
func (s OSPSI) PSI(ctx context.Context) (PSIStat, error) {
	if err := ctx.Err(); err != nil {
		return PSIStat{}, err
	}
	path := s.Path
	// The default lives only on the production host: unit tests always set
	// Path to a file; reading the real /proc is not hermetic (the CI host pressure
	// changes between runs).
	// mutator-disable-next-line branch/if
	if path == "" {
		// mutator-disable-next-line statement/remove
		path = "/proc/pressure/cpu"
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return PSIStat{}, fmt.Errorf("monitor: PSI: %w", errNoPSI)
	}
	if err != nil {
		return PSIStat{}, fmt.Errorf("monitor: PSI: %w", err)
	}
	return parsePSI(string(b))
}

// parsePSI parses a PSI file: the line "some avg10=0.00 avg60=0.00
// avg300=0.00 total=0" (CPU has no full line — tasks do not wait for each other).
// The full line and the total field (microseconds, unneeded for the alert)
// are skipped. A missing some or a non-numeric/out-of-0–100 value —
// an error: the format is fixed by the kernel; do not stay silent on a corrupt file
// (the parseMemoryEvents style). Factored out as a pure function for a table test.
func parsePSI(data string) (PSIStat, error) {
	return parsePSIKind(data, "some")
}

// parsePSIKind — parsePSI with a given PSI line kind (some/full): the full
// pressure line of /proc/pressure/memory contains both — some waits for
// "at least one task", full — "all tasks" (the T-173 load report).
func parsePSIKind(data, kind string) (PSIStat, error) {
	for _, line := range strings.Split(data, "\n") {
		prefix, rest, ok := strings.Cut(strings.TrimSpace(line), " ")
		if !ok || prefix != kind {
			continue
		}
		fields := make(map[string]string)
		for _, f := range strings.Fields(rest) {
			if k, v, ok := strings.Cut(f, "="); ok {
				fields[k] = v
			}
		}
		var st PSIStat
		for _, f := range []struct {
			key string
			val *float64
		}{
			{"avg10", &st.Avg10},
			{"avg60", &st.Avg60},
			{"avg300", &st.Avg300},
		} {
			v, ok := fields[f.key]
			if !ok {
				return PSIStat{}, fmt.Errorf("monitor: PSI: поле %s отсутствует", f.key)
			}
			n, err := strconv.ParseFloat(v, 64)
			if err != nil || n < 0 || n > 100 {
				return PSIStat{}, fmt.Errorf("monitor: PSI: %s=%q не доля процента", f.key, v)
			}
			*f.val = n
		}
		return st, nil
	}
	return PSIStat{}, fmt.Errorf("monitor: PSI: строка %s отсутствует", kind)
}
